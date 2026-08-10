-- EuroLeague shadow schema -- migration 014: lineup-unit read layer.
--
-- Season roll-up plus the filtered dynamic function, matching the
-- default-fast-path / filtered-path split the player and team surfaces use.
--
-- No stored ratios. AGENTS.md requires additive counts and seconds only; the
-- app derives PPP and the four factors after aggregation. This is a deliberate
-- deviation from the Israeli sub_lineups_stats, which stores rounded PPP.
--
-- The materialized view is created unconditionally rather than replaced: it is
-- new in this migration, and apply_shadow_schema() refuses any destructive
-- statement, which is the safety property we want here. A later migration that
-- changes its definition needs its own reviewed applier.

BEGIN;

SET LOCAL search_path TO euroleague, public;

CREATE MATERIALIZED VIEW euroleague.sub_lineups_stats_mv AS
WITH unit_totals AS (
  SELECT
    sl.competition,
    sl.game_year,
    sl.team_id,
    sl.unit_key,
    sl.unit_size,
    sl.player_ids,
    sum(l.possessions)        FILTER (WHERE l.type_lineup = 'offense') AS off_poss,
    sum(l.points)             FILTER (WHERE l.type_lineup = 'offense') AS off_pts,
    sum(l.fg2_made)           FILTER (WHERE l.type_lineup = 'offense') AS off_fg2_made,
    sum(l.fg2_att)            FILTER (WHERE l.type_lineup = 'offense') AS off_fg2_att,
    sum(l.fg3_made)           FILTER (WHERE l.type_lineup = 'offense') AS off_fg3_made,
    sum(l.fg3_att)            FILTER (WHERE l.type_lineup = 'offense') AS off_fg3_att,
    sum(l.ts_possessions)     FILTER (WHERE l.type_lineup = 'offense') AS off_ts_poss,
    sum(l.fgm)                FILTER (WHERE l.type_lineup = 'offense') AS off_fgm,
    sum(l.fga)                FILTER (WHERE l.type_lineup = 'offense') AS off_fga,
    sum(l.ft_attempts)        FILTER (WHERE l.type_lineup = 'offense') AS off_fta,
    sum(l.orebounds)          FILTER (WHERE l.type_lineup = 'offense') AS off_oreb,
    sum(l.oreb_opportunities) FILTER (WHERE l.type_lineup = 'offense') AS off_oreb_opp,
    sum(l.turnovers)          FILTER (WHERE l.type_lineup = 'offense') AS off_tov,
    sum(l.steals)             FILTER (WHERE l.type_lineup = 'offense') AS off_steals,
    sum(l.possessions)        FILTER (WHERE l.type_lineup = 'defense') AS def_poss,
    sum(l.points)             FILTER (WHERE l.type_lineup = 'defense') AS def_pts,
    sum(l.fg2_made)           FILTER (WHERE l.type_lineup = 'defense') AS def_fg2_made,
    sum(l.fg2_att)            FILTER (WHERE l.type_lineup = 'defense') AS def_fg2_att,
    sum(l.fg3_made)           FILTER (WHERE l.type_lineup = 'defense') AS def_fg3_made,
    sum(l.fg3_att)            FILTER (WHERE l.type_lineup = 'defense') AS def_fg3_att,
    sum(l.ts_possessions)     FILTER (WHERE l.type_lineup = 'defense') AS def_ts_poss,
    sum(l.fgm)                FILTER (WHERE l.type_lineup = 'defense') AS def_fgm,
    sum(l.fga)                FILTER (WHERE l.type_lineup = 'defense') AS def_fga,
    sum(l.ft_attempts)        FILTER (WHERE l.type_lineup = 'defense') AS def_fta,
    sum(l.orebounds)          FILTER (WHERE l.type_lineup = 'defense') AS def_oreb,
    sum(l.oreb_opportunities) FILTER (WHERE l.type_lineup = 'defense') AS def_oreb_opp,
    sum(l.turnovers)          FILTER (WHERE l.type_lineup = 'defense') AS def_tov,
    sum(l.steals)             FILTER (WHERE l.type_lineup = 'defense') AS def_steals,
    -- seconds live on offense rows only, so this cannot double-count
    sum(l.seconds)            FILTER (WHERE l.type_lineup = 'offense') AS seconds
  FROM euroleague.sub_lineups sl
  JOIN euroleague.lineup_totals_by_game l
    ON l.competition = sl.competition
   AND l.game_year   = sl.game_year
   AND l.team_id     = sl.team_id
   AND l.lineup_key  = sl.lineup_key
  GROUP BY 1, 2, 3, 4, 5, 6
)
SELECT
  ut.competition, ut.game_year, ut.team_id, ut.unit_key, ut.unit_size,
  ut.player_ids,
  names.player_names,
  names.player_names_str,
  ut.off_poss, ut.off_pts, ut.off_fg2_made, ut.off_fg2_att,
  ut.off_fg3_made, ut.off_fg3_att, ut.off_ts_poss, ut.off_fgm, ut.off_fga,
  ut.off_fta, ut.off_oreb, ut.off_oreb_opp, ut.off_tov, ut.off_steals,
  ut.def_poss, ut.def_pts, ut.def_fg2_made, ut.def_fg2_att,
  ut.def_fg3_made, ut.def_fg3_att, ut.def_ts_poss, ut.def_fgm, ut.def_fga,
  ut.def_fta, ut.def_oreb, ut.def_oreb_opp, ut.def_tov, ut.def_steals,
  round(coalesce(ut.seconds, 0) / 60.0, 1) AS minutes
FROM unit_totals ut
CROSS JOIN LATERAL (
  SELECT
    array_agg(coalesce(p.display_name, '#' || u.pid::text) ORDER BY u.ord)
      AS player_names,
    string_agg(coalesce(p.display_name, '#' || u.pid::text), ', ' ORDER BY u.ord)
      AS player_names_str
  FROM unnest(ut.player_ids) WITH ORDINALITY AS u(pid, ord)
  LEFT JOIN euroleague.players p ON p.player_id = u.pid
) names
WITH NO DATA;

CREATE UNIQUE INDEX euroleague_sub_lineups_stats_mv_pk
  ON euroleague.sub_lineups_stats_mv
     (competition, game_year, team_id, unit_key);

CREATE INDEX euroleague_sub_lineups_stats_mv_size_idx
  ON euroleague.sub_lineups_stats_mv
     (competition, game_year, unit_size, team_id);

-- ---------------------------------------------------------------------------
-- fetch_lineups_dynamic -- filtered path for 2-5 player units.
-- ---------------------------------------------------------------------------
--
-- sub_lineups -> lineup_totals_by_game -> the verified schedule filter.
--
-- sub_lineups's primary key means one unit_key has at most one row per
-- lineup_key, so this join can never duplicate a lineup_totals_by_game row
-- into a unit's sum. Without that key the whole surface would double-count.
--
-- p_players_on_csv / p_players_off_csv read sub_lineups.player_ids, not the
-- hash: "these two players together" is an array question.
--
-- The parameter preamble and the schedule_ranked/team_ranked/games CTEs are
-- copied unchanged from get_team_four_factors_dynamic in
-- 006_team_four_factors.sql. Only the aggregation after `games` differs.

CREATE OR REPLACE FUNCTION euroleague.fetch_lineups_dynamic(
    p_competition          TEXT,
    p_game_year            INTEGER,
    p_start_date           DATE    DEFAULT NULL,
    p_end_date             DATE    DEFAULT NULL,
    p_team_ids_csv         TEXT    DEFAULT NULL,
    p_phase_csv            TEXT    DEFAULT NULL,
    p_opp_ids_csv          TEXT    DEFAULT NULL,
    p_home_away            TEXT    DEFAULT 'all',
    p_outcome              TEXT    DEFAULT 'all',
    p_opp_rank_side        TEXT    DEFAULT NULL,
    p_opp_rank_n           INTEGER DEFAULT NULL,
    p_opp_rank_metric      TEXT    DEFAULT NULL,
    p_min_gn               INTEGER DEFAULT NULL,
    p_max_gn               INTEGER DEFAULT NULL,
    p_last_n_games         INTEGER DEFAULT NULL,
    p_num_starters_off_min INTEGER DEFAULT NULL,
    p_num_starters_off_max INTEGER DEFAULT NULL,
    p_num_starters_def_min INTEGER DEFAULT NULL,
    p_num_starters_def_max INTEGER DEFAULT NULL,
    p_unit_size            INTEGER DEFAULT 5,
    p_players_on_csv       TEXT    DEFAULT NULL,
    p_players_off_csv      TEXT    DEFAULT NULL,
    p_min_poss             INTEGER DEFAULT 0
)
RETURNS TABLE (
    team_id          BIGINT,
    unit_key         TEXT,
    unit_size        SMALLINT,
    player_ids       BIGINT[],
    player_names     TEXT[],
    player_names_str TEXT,
    off_poss         BIGINT, off_pts       BIGINT,
    off_fg2_made     BIGINT, off_fg2_att   BIGINT,
    off_fg3_made     BIGINT, off_fg3_att   BIGINT,
    off_ts_poss      BIGINT, off_fgm       BIGINT, off_fga BIGINT,
    off_fta          BIGINT, off_oreb      BIGINT, off_oreb_opp BIGINT,
    off_tov          BIGINT, off_steals    BIGINT,
    def_poss         BIGINT, def_pts       BIGINT,
    def_fg2_made     BIGINT, def_fg2_att   BIGINT,
    def_fg3_made     BIGINT, def_fg3_att   BIGINT,
    def_ts_poss      BIGINT, def_fgm       BIGINT, def_fga BIGINT,
    def_fta          BIGINT, def_oreb      BIGINT, def_oreb_opp BIGINT,
    def_tov          BIGINT, def_steals    BIGINT,
    minutes          NUMERIC
)
LANGUAGE plpgsql
STABLE
SET plan_cache_mode = force_custom_plan
AS $function$
DECLARE
  v_competition text;
  v_team_ids    bigint[];
  v_opp_ids     bigint[];
  v_phases      text[];
  v_home_away   text;
  v_outcome     text;
  v_rank_side   text;
  v_rank_metric text;
  v_players_on  bigint[];
  v_players_off bigint[];
BEGIN
  v_competition := COALESCE(NULLIF(btrim(p_competition), ''), 'E');
  v_home_away   := COALESCE(NULLIF(btrim(p_home_away), ''), 'all');
  v_outcome     := COALESCE(NULLIF(btrim(p_outcome), ''), 'all');
  v_rank_side   := NULLIF(btrim(p_opp_rank_side), '');
  v_rank_metric := COALESCE(NULLIF(btrim(p_opp_rank_metric), ''), 'net');

  IF p_team_ids_csv IS NOT NULL AND length(btrim(p_team_ids_csv)) > 0 THEN
    v_team_ids := ARRAY(SELECT DISTINCT x::bigint
      FROM unnest(string_to_array(regexp_replace(p_team_ids_csv, '\s+', '', 'g'), ',')) x
      WHERE x <> '' ORDER BY 1);
  END IF;
  IF p_opp_ids_csv IS NOT NULL AND length(btrim(p_opp_ids_csv)) > 0 THEN
    v_opp_ids := ARRAY(SELECT DISTINCT x::bigint
      FROM unnest(string_to_array(regexp_replace(p_opp_ids_csv, '\s+', '', 'g'), ',')) x
      WHERE x <> '' ORDER BY 1);
  END IF;
  IF p_phase_csv IS NOT NULL AND length(btrim(p_phase_csv)) > 0 THEN
    v_phases := ARRAY(SELECT DISTINCT btrim(x)
      FROM unnest(string_to_array(p_phase_csv, ',')) x WHERE btrim(x) <> '' ORDER BY 1);
  END IF;
  IF p_players_on_csv IS NOT NULL AND length(btrim(p_players_on_csv)) > 0 THEN
    v_players_on := ARRAY(SELECT DISTINCT x::bigint
      FROM unnest(string_to_array(regexp_replace(p_players_on_csv, '\s+', '', 'g'), ',')) x
      WHERE x <> '' ORDER BY 1);
  END IF;
  IF p_players_off_csv IS NOT NULL AND length(btrim(p_players_off_csv)) > 0 THEN
    v_players_off := ARRAY(SELECT DISTINCT x::bigint
      FROM unnest(string_to_array(regexp_replace(p_players_off_csv, '\s+', '', 'g'), ',')) x
      WHERE x <> '' ORDER BY 1);
  END IF;

  RETURN QUERY
  WITH schedule_ranked AS (
    SELECT
      g.game_id, g.team_id, g.opp_team_id, g.is_home, g.has_won,
      g.round_number, g.phase, g.game_date,
      row_number() OVER (PARTITION BY g.team_id ORDER BY g.game_date DESC, g.game_id DESC)
        AS team_game_rank
    FROM euroleague.team_game_ratings_mv g
    WHERE g.competition = v_competition AND g.game_year = p_game_year
  ),
  team_ranked AS (
    SELECT r.team_id, r.off_rank, r.def_rank, r.net_rank
    FROM euroleague.team_ppp_ratings_mv r
    WHERE r.competition = v_competition AND r.game_year = p_game_year
  ),
  games AS (
    SELECT sr.*
    FROM schedule_ranked sr
    LEFT JOIN team_ranked tr ON tr.team_id = sr.opp_team_id
    WHERE (p_start_date IS NULL OR sr.game_date >= p_start_date)
      AND (p_end_date   IS NULL OR sr.game_date <= p_end_date)
      AND (v_phases   IS NULL OR sr.phase = ANY(v_phases))
      AND (v_team_ids IS NULL OR sr.team_id = ANY(v_team_ids))
      AND (v_opp_ids  IS NULL OR sr.opp_team_id = ANY(v_opp_ids))
      AND (p_min_gn   IS NULL OR sr.round_number >= p_min_gn)
      AND (p_max_gn   IS NULL OR sr.round_number <= p_max_gn)
      AND (p_last_n_games IS NULL OR sr.team_game_rank <= p_last_n_games)
      AND (v_home_away = 'all'
           OR (v_home_away = 'home' AND sr.is_home)
           OR (v_home_away = 'away' AND NOT sr.is_home))
      AND (v_outcome = 'all'
           OR (v_outcome = 'win'  AND sr.has_won)
           OR (v_outcome = 'loss' AND NOT sr.has_won))
      AND (
        v_rank_side IS NULL OR p_opp_rank_n IS NULL
        OR (v_rank_side = 'top' AND
            CASE v_rank_metric WHEN 'off' THEN tr.off_rank
                               WHEN 'def' THEN tr.def_rank
                               ELSE tr.net_rank END <= p_opp_rank_n)
        OR (v_rank_side = 'bottom' AND
            CASE v_rank_metric WHEN 'off' THEN tr.off_rank
                               WHEN 'def' THEN tr.def_rank
                               ELSE tr.net_rank END
            > (SELECT count(*) FROM team_ranked) - p_opp_rank_n)
      )
  ),
  unit_rows AS (
    SELECT
      sl.team_id      AS u_team_id,
      sl.unit_key     AS u_unit_key,
      sl.unit_size    AS u_unit_size,
      sl.player_ids   AS u_player_ids,
      l.type_lineup,
      l.possessions, l.points, l.fg2_made, l.fg2_att, l.fg3_made, l.fg3_att,
      l.ts_possessions, l.fgm, l.fga, l.ft_attempts,
      l.orebounds, l.oreb_opportunities, l.turnovers, l.steals, l.seconds
    FROM euroleague.sub_lineups sl
    JOIN euroleague.lineup_totals_by_game l
      ON l.competition = sl.competition
     AND l.game_year   = sl.game_year
     AND l.team_id     = sl.team_id
     AND l.lineup_key  = sl.lineup_key
    JOIN games g ON g.game_id = l.game_id AND g.team_id = l.team_id
    WHERE sl.competition = v_competition
      AND sl.game_year   = p_game_year
      AND sl.unit_size   = p_unit_size::smallint
      AND (v_players_on  IS NULL OR sl.player_ids @> v_players_on)
      AND (v_players_off IS NULL OR NOT (sl.player_ids && v_players_off))
      AND (p_num_starters_off_min IS NULL OR l.own_starters >= p_num_starters_off_min)
      AND (p_num_starters_off_max IS NULL OR l.own_starters <= p_num_starters_off_max)
      AND (p_num_starters_def_min IS NULL OR l.opp_starters >= p_num_starters_def_min)
      AND (p_num_starters_def_max IS NULL OR l.opp_starters <= p_num_starters_def_max)
  ),
  agg AS (
    SELECT
      u.u_team_id, u.u_unit_key, u.u_unit_size, u.u_player_ids,
      sum(u.possessions)        FILTER (WHERE u.type_lineup = 'offense') AS a_off_poss,
      sum(u.points)             FILTER (WHERE u.type_lineup = 'offense') AS a_off_pts,
      sum(u.fg2_made)           FILTER (WHERE u.type_lineup = 'offense') AS a_off_fg2_made,
      sum(u.fg2_att)            FILTER (WHERE u.type_lineup = 'offense') AS a_off_fg2_att,
      sum(u.fg3_made)           FILTER (WHERE u.type_lineup = 'offense') AS a_off_fg3_made,
      sum(u.fg3_att)            FILTER (WHERE u.type_lineup = 'offense') AS a_off_fg3_att,
      sum(u.ts_possessions)     FILTER (WHERE u.type_lineup = 'offense') AS a_off_ts_poss,
      sum(u.fgm)                FILTER (WHERE u.type_lineup = 'offense') AS a_off_fgm,
      sum(u.fga)                FILTER (WHERE u.type_lineup = 'offense') AS a_off_fga,
      sum(u.ft_attempts)        FILTER (WHERE u.type_lineup = 'offense') AS a_off_fta,
      sum(u.orebounds)          FILTER (WHERE u.type_lineup = 'offense') AS a_off_oreb,
      sum(u.oreb_opportunities) FILTER (WHERE u.type_lineup = 'offense') AS a_off_oreb_opp,
      sum(u.turnovers)          FILTER (WHERE u.type_lineup = 'offense') AS a_off_tov,
      sum(u.steals)             FILTER (WHERE u.type_lineup = 'offense') AS a_off_steals,
      sum(u.possessions)        FILTER (WHERE u.type_lineup = 'defense') AS a_def_poss,
      sum(u.points)             FILTER (WHERE u.type_lineup = 'defense') AS a_def_pts,
      sum(u.fg2_made)           FILTER (WHERE u.type_lineup = 'defense') AS a_def_fg2_made,
      sum(u.fg2_att)            FILTER (WHERE u.type_lineup = 'defense') AS a_def_fg2_att,
      sum(u.fg3_made)           FILTER (WHERE u.type_lineup = 'defense') AS a_def_fg3_made,
      sum(u.fg3_att)            FILTER (WHERE u.type_lineup = 'defense') AS a_def_fg3_att,
      sum(u.ts_possessions)     FILTER (WHERE u.type_lineup = 'defense') AS a_def_ts_poss,
      sum(u.fgm)                FILTER (WHERE u.type_lineup = 'defense') AS a_def_fgm,
      sum(u.fga)                FILTER (WHERE u.type_lineup = 'defense') AS a_def_fga,
      sum(u.ft_attempts)        FILTER (WHERE u.type_lineup = 'defense') AS a_def_fta,
      sum(u.orebounds)          FILTER (WHERE u.type_lineup = 'defense') AS a_def_oreb,
      sum(u.oreb_opportunities) FILTER (WHERE u.type_lineup = 'defense') AS a_def_oreb_opp,
      sum(u.turnovers)          FILTER (WHERE u.type_lineup = 'defense') AS a_def_tov,
      sum(u.steals)             FILTER (WHERE u.type_lineup = 'defense') AS a_def_steals,
      sum(u.seconds)            FILTER (WHERE u.type_lineup = 'offense') AS a_seconds
    FROM unit_rows u
    GROUP BY u.u_team_id, u.u_unit_key, u.u_unit_size, u.u_player_ids
  )
  SELECT
    a.u_team_id, a.u_unit_key, a.u_unit_size, a.u_player_ids,
    names.p_names, names.p_names_str,
    a.a_off_poss, a.a_off_pts, a.a_off_fg2_made, a.a_off_fg2_att,
    a.a_off_fg3_made, a.a_off_fg3_att, a.a_off_ts_poss, a.a_off_fgm, a.a_off_fga,
    a.a_off_fta, a.a_off_oreb, a.a_off_oreb_opp, a.a_off_tov, a.a_off_steals,
    a.a_def_poss, a.a_def_pts, a.a_def_fg2_made, a.a_def_fg2_att,
    a.a_def_fg3_made, a.a_def_fg3_att, a.a_def_ts_poss, a.a_def_fgm, a.a_def_fga,
    a.a_def_fta, a.a_def_oreb, a.a_def_oreb_opp, a.a_def_tov, a.a_def_steals,
    round(coalesce(a.a_seconds, 0) / 60.0, 1)
  FROM agg a
  CROSS JOIN LATERAL (
    SELECT
      array_agg(coalesce(p.display_name, '#' || x.pid::text) ORDER BY x.ord)
        AS p_names,
      string_agg(coalesce(p.display_name, '#' || x.pid::text), ', ' ORDER BY x.ord)
        AS p_names_str
    FROM unnest(a.u_player_ids) WITH ORDINALITY AS x(pid, ord)
    LEFT JOIN euroleague.players p ON p.player_id = x.pid
  ) names
  WHERE coalesce(a.a_off_poss, 0) + coalesce(a.a_def_poss, 0)
        >= coalesce(p_min_poss, 0);
END;
$function$;

-- ---------------------------------------------------------------------------
-- Refresh entry point.
-- ---------------------------------------------------------------------------
--
-- NOT concurrent. refresh_app_materialized_views() runs inside the publication
-- transaction so a load cannot be marked completed with a stale snapshot, and
-- REFRESH ... CONCURRENTLY cannot run in a transaction block. Fail-closed
-- publication and concurrent refresh are mutually exclusive; this project has
-- already chosen fail-closed.
--
-- CREATE OR REPLACE, so this function's existing EXECUTE grants survive.

CREATE OR REPLACE FUNCTION euroleague.refresh_app_materialized_views()
RETURNS void
LANGUAGE plpgsql
AS $function$
BEGIN
  REFRESH MATERIALIZED VIEW euroleague.final_schedule_mv;
  REFRESH MATERIALIZED VIEW euroleague.team_game_ratings_mv;
  REFRESH MATERIALIZED VIEW euroleague.team_ppp_ratings_mv;
  REFRESH MATERIALIZED VIEW euroleague.team_four_factors_mv;
  REFRESH MATERIALIZED VIEW euroleague.player_onoff_default_mv;
  REFRESH MATERIALIZED VIEW euroleague.player_advanced_stats_mv;
  REFRESH MATERIALIZED VIEW euroleague.sub_lineups_stats_mv;
END;
$function$;

GRANT SELECT ON euroleague.sub_lineups_stats_mv TO app_readonly;

GRANT EXECUTE ON FUNCTION
  euroleague.fetch_lineups_dynamic(
    text, int4, date, date, text, text, text, text, text, text, int4, text,
    int4, int4, int4, int4, int4, int4, int4, int4, text, text, int4)
TO app_readonly;

COMMIT;
