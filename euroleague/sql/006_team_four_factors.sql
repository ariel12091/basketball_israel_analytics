-- ===========================================================================
-- 006: EuroLeague team four factors + the filtered (dynamic) team path.
--
-- Team-grain four-factor counts, derived from actions the same way
-- refresh_player_four_factors_by_game_for_games() derives the player-grain
-- ones. That function computes a team-perspective row per (event, team) in its
-- team_event_context CTE and only fans out to players afterwards, in
-- player_exposure; this reproduces the chain up to that point and aggregates by
-- team instead. So the metric DEFINITIONS here are copied verbatim from the
-- player refresh -- same TS-possession rule (FGA plus the last FT of a
-- committed-foul trip), same OREB-opportunity rule, same play_type mapping.
--
-- The duplication is deliberate and was approved: the alternative was dividing
-- the player fact by 5, which is correct only while every possession is
-- attributed to exactly 5 players and fails silently otherwise. If a third
-- consumer ever needs this, promote team_event_context to a persisted relation
-- (euroleague/CLAUDE.md item 7) rather than copying it again.
--
-- Grain is (game_id, team_id, own_starters, opp_starters) -- matching the
-- player fact -- so the lineup-starters filters work on the team surfaces too.
--
-- NOT included: clutch. The four clutch params on the Israeli team functions
-- need a running score and time-remaining per action. actions_raw carries
-- points_home/points_away/period/marker_time so it is buildable, but it is its
-- own correctness surface (pre-shot margin, OT bypass) and was deferred.
--
-- Apply order: 001 -> 002 -> 004 -> 005 -> 006.
-- ===========================================================================

-- ---------------------------------------------------------------------------
-- 1. Team-grain fact.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS euroleague.team_four_factors_by_game (
  game_id           bigint   NOT NULL,
  team_id           bigint   NOT NULL,
  game_year         smallint NOT NULL,
  own_starters      smallint NOT NULL,
  opp_starters      smallint NOT NULL,
  off_pts           numeric  NOT NULL DEFAULT 0,
  off_poss          bigint   NOT NULL DEFAULT 0,
  off_ts_poss       bigint   NOT NULL DEFAULT 0,
  off_oreb          bigint   NOT NULL DEFAULT 0,
  off_oreb_opp      bigint   NOT NULL DEFAULT 0,
  off_tov           bigint   NOT NULL DEFAULT 0,
  off_fta           bigint   NOT NULL DEFAULT 0,
  off_fga           bigint   NOT NULL DEFAULT 0,
  off_fgm           bigint   NOT NULL DEFAULT 0,
  off_fg3m          bigint   NOT NULL DEFAULT 0,
  def_pts           numeric  NOT NULL DEFAULT 0,
  def_poss          bigint   NOT NULL DEFAULT 0,
  def_ts_poss       bigint   NOT NULL DEFAULT 0,
  def_oreb          bigint   NOT NULL DEFAULT 0,
  def_oreb_opp      bigint   NOT NULL DEFAULT 0,
  def_tov           bigint   NOT NULL DEFAULT 0,
  def_fta           bigint   NOT NULL DEFAULT 0,
  def_fga           bigint   NOT NULL DEFAULT 0,
  def_fgm           bigint   NOT NULL DEFAULT 0,
  def_fg3m          bigint   NOT NULL DEFAULT 0,
  def_steals        bigint   NOT NULL DEFAULT 0,
  derivation_version text,
  PRIMARY KEY (game_id, team_id, own_starters, opp_starters)
);

CREATE INDEX IF NOT EXISTS euroleague_team_ff_by_game_team_idx
  ON euroleague.team_four_factors_by_game (game_id, team_id);

CREATE OR REPLACE FUNCTION euroleague.refresh_team_four_factors_by_game_for_games(
  game_ids bigint[]
)
RETURNS bigint
LANGUAGE plpgsql
AS $function$
DECLARE
  inserted_count bigint := 0;
BEGIN
  IF game_ids IS NULL OR array_length(game_ids, 1) IS NULL THEN
    DELETE FROM euroleague.team_four_factors_by_game;
  ELSE
    DELETE FROM euroleague.team_four_factors_by_game WHERE game_id = ANY(game_ids);
  END IF;

  INSERT INTO euroleague.team_four_factors_by_game (
    game_id, team_id, game_year, own_starters, opp_starters,
    off_pts, off_poss, off_ts_poss, off_oreb, off_oreb_opp, off_tov,
    off_fta, off_fga, off_fgm, off_fg3m,
    def_pts, def_poss, def_ts_poss, def_oreb, def_oreb_opp, def_tov,
    def_fta, def_fga, def_fgm, def_fg3m, def_steals,
    derivation_version
  )
  WITH target_games AS (
    SELECT s.*
    FROM euroleague.schedule s
    WHERE game_ids IS NULL OR s.game_id = ANY(game_ids)
  ),
  event_base AS (
    SELECT
      ar.game_id,
      ar.source_event_order,
      tg.season AS game_year,
      ar.team_id AS event_team_id,
      ar.play_type,
      ac.synthetic_ft_trip_id,
      root.play_type AS parent_play_type,
      al.home_lineup_id,
      al.away_lineup_id,
      tg.home_team_id,
      tg.away_team_id,
      p.offense_team_id AS endpoint_offense_team_id,
      row_number() OVER (
        PARTITION BY ar.game_id, ac.synthetic_ft_trip_id
        ORDER BY ar.source_event_order DESC
      ) AS ft_reverse_order
    FROM target_games tg
    JOIN euroleague.actions_raw ar ON ar.game_id = tg.game_id
    JOIN euroleague.actions_clean ac
      ON ac.game_id = ar.game_id AND ac.source_event_order = ar.source_event_order
    JOIN euroleague.actions_raw root
      ON root.game_id = ac.game_id AND root.source_event_order = ac.synthetic_parent_order
    JOIN euroleague.action_lineups al
      ON al.game_id = ar.game_id AND al.source_event_order = ar.source_event_order
    LEFT JOIN euroleague.possessions p
      ON p.game_id = ar.game_id AND p.endpoint_source_event_order = ar.source_event_order
  ),
  -- Verbatim from refresh_player_four_factors_by_game_for_games().
  event_metrics AS (
    SELECT
      eb.*,
      CASE eb.play_type
        WHEN '2FGM' THEN 2 WHEN '3FGM' THEN 3 WHEN 'FTM' THEN 1 ELSE 0
      END::integer AS points,
      CASE WHEN eb.play_type IN ('2FGM', '2FGA', '3FGM', '3FGA') THEN 1
           WHEN eb.play_type IN ('FTM', 'FTA')
            AND eb.synthetic_ft_trip_id IS NOT NULL
            AND eb.parent_play_type = 'CM'
            AND eb.ft_reverse_order = 1 THEN 1 ELSE 0 END::integer AS ts_possessions,
      CASE WHEN eb.play_type = 'O' THEN 1 ELSE 0 END::integer AS orebounds,
      CASE WHEN eb.play_type IN ('2FGA', '3FGA') THEN 1
           WHEN eb.play_type = 'FTA'
            AND eb.synthetic_ft_trip_id IS NOT NULL
            AND eb.parent_play_type = 'CM'
            AND eb.ft_reverse_order = 1 THEN 1 ELSE 0 END::integer AS oreb_opportunities,
      CASE WHEN eb.play_type = 'TO' THEN 1 ELSE 0 END::integer AS turnovers,
      CASE WHEN eb.play_type = 'ST' THEN 1 ELSE 0 END::integer AS steals,
      CASE WHEN eb.play_type IN ('FTM', 'FTA') THEN 1 ELSE 0 END::integer AS ft_attempts,
      CASE WHEN eb.play_type IN ('2FGM', '2FGA', '3FGM', '3FGA') THEN 1 ELSE 0 END::integer AS fga,
      CASE WHEN eb.play_type IN ('2FGM', '3FGM') THEN 1 ELSE 0 END::integer AS fgm,
      CASE WHEN eb.play_type = '3FGM' THEN 1 ELSE 0 END::integer AS fg3_made
    FROM event_base eb
  ),
  team_event_context AS (
    SELECT
      em.game_id,
      em.game_year,
      side.team_id,
      own_lineup.starter_count AS own_starters,
      opp_lineup.starter_count AS opp_starters,
      CASE WHEN em.event_team_id = side.team_id THEN em.points ELSE 0 END AS off_points,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.points ELSE 0 END AS def_points,
      CASE WHEN em.endpoint_offense_team_id = side.team_id THEN 1 ELSE 0 END AS off_possessions,
      CASE WHEN em.endpoint_offense_team_id = side.opponent_team_id THEN 1 ELSE 0 END AS def_possessions,
      CASE WHEN em.event_team_id = side.team_id THEN em.ts_possessions ELSE 0 END AS off_ts_possessions,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.ts_possessions ELSE 0 END AS def_ts_possessions,
      CASE WHEN em.event_team_id = side.team_id THEN em.orebounds ELSE 0 END AS off_orebounds,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.orebounds ELSE 0 END AS def_orebounds,
      CASE WHEN em.event_team_id = side.team_id THEN em.oreb_opportunities ELSE 0 END AS off_oreb_opportunities,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.oreb_opportunities ELSE 0 END AS def_oreb_opportunities,
      CASE WHEN em.event_team_id = side.team_id THEN em.turnovers ELSE 0 END AS off_turnovers,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.turnovers ELSE 0 END AS def_turnovers,
      CASE WHEN em.event_team_id = side.team_id THEN em.steals ELSE 0 END AS def_steals,
      CASE WHEN em.event_team_id = side.team_id THEN em.ft_attempts ELSE 0 END AS off_ft_attempts,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.ft_attempts ELSE 0 END AS def_ft_attempts,
      CASE WHEN em.event_team_id = side.team_id THEN em.fga ELSE 0 END AS off_fga,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.fga ELSE 0 END AS def_fga,
      CASE WHEN em.event_team_id = side.team_id THEN em.fgm ELSE 0 END AS off_fgm,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.fgm ELSE 0 END AS def_fgm,
      CASE WHEN em.event_team_id = side.team_id THEN em.fg3_made ELSE 0 END AS off_fg3_made,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.fg3_made ELSE 0 END AS def_fg3_made
    FROM event_metrics em
    CROSS JOIN LATERAL (
      VALUES
        (em.home_team_id, em.away_team_id, em.home_lineup_id, em.away_lineup_id),
        (em.away_team_id, em.home_team_id, em.away_lineup_id, em.home_lineup_id)
    ) AS side(team_id, opponent_team_id, own_lineup_id, opp_lineup_id)
    JOIN euroleague.lineups own_lineup ON own_lineup.lineup_id = side.own_lineup_id
    JOIN euroleague.lineups opp_lineup ON opp_lineup.lineup_id = side.opp_lineup_id
  )
  SELECT
    tec.game_id, tec.team_id, tec.game_year, tec.own_starters, tec.opp_starters,
    sum(tec.off_points)::numeric, sum(tec.off_possessions)::bigint,
    sum(tec.off_ts_possessions)::bigint, sum(tec.off_orebounds)::bigint,
    sum(tec.off_oreb_opportunities)::bigint, sum(tec.off_turnovers)::bigint,
    sum(tec.off_ft_attempts)::bigint, sum(tec.off_fga)::bigint,
    sum(tec.off_fgm)::bigint, sum(tec.off_fg3_made)::bigint,
    sum(tec.def_points)::numeric, sum(tec.def_possessions)::bigint,
    sum(tec.def_ts_possessions)::bigint, sum(tec.def_orebounds)::bigint,
    sum(tec.def_oreb_opportunities)::bigint, sum(tec.def_turnovers)::bigint,
    sum(tec.def_ft_attempts)::bigint, sum(tec.def_fga)::bigint,
    sum(tec.def_fgm)::bigint, sum(tec.def_fg3_made)::bigint,
    sum(tec.def_steals)::bigint,
    '006-team-ff'
  FROM team_event_context tec
  GROUP BY tec.game_id, tec.team_id, tec.game_year, tec.own_starters, tec.opp_starters;

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$function$;

-- ---------------------------------------------------------------------------
-- 2. Rename the ratings MV columns onto the Israeli contract, so the ported
--    Tab 3 renderer needs no rename layer. team_game_ratings_mv keeps its own
--    names; only the season-level surface the app reads is aligned.
-- ---------------------------------------------------------------------------

DROP MATERIALIZED VIEW IF EXISTS euroleague.team_ppp_ratings_mv;

CREATE MATERIALIZED VIEW euroleague.team_ppp_ratings_mv AS
WITH agg AS (
  SELECT
    g.competition, g.game_year, g.team_id,
    count(*)                              AS games_played,
    count(*) FILTER (WHERE g.has_won)     AS wins,
    count(*) FILTER (WHERE NOT g.has_won) AS losses,
    sum(g.off_pts)::bigint                AS off_pts,
    sum(g.off_poss)::bigint               AS off_poss,
    sum(g.def_pts)::bigint                AS def_pts,
    sum(g.def_poss)::bigint               AS def_poss
  FROM euroleague.team_game_ratings_mv g
  GROUP BY g.competition, g.game_year, g.team_id
),
rated AS (
  SELECT
    a.*,
    round(100.0 * a.off_pts / NULLIF(a.off_poss, 0), 1) AS off_ppp,
    round(100.0 * a.def_pts / NULLIF(a.def_poss, 0), 1) AS def_ppp
  FROM agg a
)
SELECT
  r.competition, r.game_year, r.team_id, t.display_name AS team_name,
  r.off_ppp, r.def_ppp,
  round(r.off_ppp - r.def_ppp, 1) AS net_rtg,
  r.games_played, r.wins, r.losses,
  r.off_pts, r.def_pts, r.off_poss, r.def_poss,
  dense_rank() OVER (PARTITION BY r.competition, r.game_year ORDER BY r.off_ppp - r.def_ppp DESC) AS rank_net_rtg,
  dense_rank() OVER (PARTITION BY r.competition, r.game_year ORDER BY r.off_ppp DESC)             AS rank_off_ppp,
  dense_rank() OVER (PARTITION BY r.competition, r.game_year ORDER BY r.def_ppp ASC)              AS rank_def_ppp,
  -- Kept so migration 005's consumers (onoff_compute / four_factors_compute)
  -- keep working unchanged; same values, the names the compute functions use.
  dense_rank() OVER (PARTITION BY r.competition, r.game_year ORDER BY r.off_ppp DESC)             AS off_rank,
  dense_rank() OVER (PARTITION BY r.competition, r.game_year ORDER BY r.def_ppp ASC)              AS def_rank,
  dense_rank() OVER (PARTITION BY r.competition, r.game_year ORDER BY r.off_ppp - r.def_ppp DESC) AS net_rank
FROM rated r
JOIN euroleague.teams t ON t.team_id = r.team_id
WITH NO DATA;

CREATE UNIQUE INDEX euroleague_team_ppp_ratings_mv_pk
  ON euroleague.team_ppp_ratings_mv (competition, game_year, team_id);

-- ---------------------------------------------------------------------------
-- 3. Season-level team four factors. Rates are computed once from summed raw
--    counts, never averaged across games.
-- ---------------------------------------------------------------------------

DROP MATERIALIZED VIEW IF EXISTS euroleague.team_four_factors_mv;

CREATE MATERIALIZED VIEW euroleague.team_four_factors_mv AS
WITH agg AS (
  SELECT
    s.competition, f.game_year, f.team_id,
    sum(f.off_pts)      AS off_pts,      sum(f.off_poss)     AS off_poss,
    sum(f.off_ts_poss)  AS off_ts_poss,  sum(f.off_oreb)     AS off_oreb,
    sum(f.off_oreb_opp) AS off_oreb_opp, sum(f.off_tov)      AS off_tov,
    sum(f.off_fta)      AS off_fta,      sum(f.off_fga)      AS off_fga,
    sum(f.off_fgm)      AS off_fgm,      sum(f.off_fg3m)     AS off_fg3m,
    sum(f.def_pts)      AS def_pts,      sum(f.def_poss)     AS def_poss,
    sum(f.def_ts_poss)  AS def_ts_poss,  sum(f.def_oreb)     AS def_oreb,
    sum(f.def_oreb_opp) AS def_oreb_opp, sum(f.def_tov)      AS def_tov,
    sum(f.def_fta)      AS def_fta,      sum(f.def_fga)      AS def_fga,
    sum(f.def_fgm)      AS def_fgm,      sum(f.def_fg3m)     AS def_fg3m,
    sum(f.def_steals)   AS def_steals
  FROM euroleague.team_four_factors_by_game f
  JOIN euroleague.schedule s ON s.game_id = f.game_id
  GROUP BY s.competition, f.game_year, f.team_id
)
SELECT
  a.competition, a.game_year, a.team_id, t.display_name AS team_name,
  round(100.0 * a.off_pts / NULLIF(a.off_poss, 0), 1)                       AS off_ppp,
  round(100.0 * a.def_pts / NULLIF(a.def_poss, 0), 1)                       AS def_ppp,
  round(100.0 * a.off_pts / NULLIF(a.off_poss, 0)
      - 100.0 * a.def_pts / NULLIF(a.def_poss, 0), 1)                       AS net_rtg,
  round(100.0 * (a.off_fgm + 0.5 * a.off_fg3m) / NULLIF(a.off_fga, 0), 1)   AS off_efg,
  round(100.0 * (a.def_fgm + 0.5 * a.def_fg3m) / NULLIF(a.def_fga, 0), 1)   AS def_efg,
  round(100.0 * a.off_pts / NULLIF(2 * a.off_ts_poss, 0), 1)                AS off_ts,
  round(100.0 * a.def_pts / NULLIF(2 * a.def_ts_poss, 0), 1)                AS def_ts,
  round(100.0 * a.off_oreb / NULLIF(a.off_oreb_opp, 0), 1)                  AS off_oreb,
  round(100.0 * a.def_oreb / NULLIF(a.def_oreb_opp, 0), 1)                  AS def_oreb,
  round(100.0 * a.off_tov / NULLIF(a.off_poss, 0), 1)                       AS off_tov,
  round(100.0 * a.def_tov / NULLIF(a.def_poss, 0), 1)                       AS def_tov,
  round(100.0 * a.off_fta / NULLIF(a.off_fga, 0), 1)                        AS off_ftr,
  round(100.0 * a.def_fta / NULLIF(a.def_fga, 0), 1)                        AS def_ftr,
  a.off_poss, a.def_poss
FROM agg a
JOIN euroleague.teams t ON t.team_id = a.team_id
WITH NO DATA;

CREATE UNIQUE INDEX euroleague_team_four_factors_mv_pk
  ON euroleague.team_four_factors_mv (competition, game_year, team_id);

-- ---------------------------------------------------------------------------
-- 4. Filtered path. Mirrors the Israeli get_team_*_dynamic contract minus the
--    four clutch params (deferred) and with phase instead of game_type.
-- ---------------------------------------------------------------------------

DROP FUNCTION IF EXISTS euroleague.get_team_ratings_dynamic(
  text, int4, date, date, text, text, text, text, text, int4, text,
  int4, int4, int4, int4, int4, int4, int4);

CREATE OR REPLACE FUNCTION euroleague.get_team_ratings_dynamic(
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
    p_num_starters_def_max INTEGER DEFAULT NULL
)
RETURNS TABLE (
    game_year INT, team_id BIGINT, team_name TEXT,
    off_ppp NUMERIC, def_ppp NUMERIC, net_rtg NUMERIC,
    games_played BIGINT, wins BIGINT, losses BIGINT,
    off_poss BIGINT, def_poss BIGINT,
    rank_net_rtg BIGINT, rank_off_ppp BIGINT, rank_def_ppp BIGINT
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
  -- Season-wide opponent ranks, same source as onoff_compute.
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
  agg AS (
    SELECT
      f.team_id,
      count(DISTINCT f.game_id)                                        AS games_played,
      count(DISTINCT f.game_id) FILTER (WHERE g.has_won)               AS wins,
      count(DISTINCT f.game_id) FILTER (WHERE NOT g.has_won)           AS losses,
      sum(f.off_pts)                                                   AS off_pts,
      sum(f.off_poss)                                                  AS off_poss,
      sum(f.def_pts)                                                   AS def_pts,
      sum(f.def_poss)                                                  AS def_poss
    FROM euroleague.team_four_factors_by_game f
    JOIN games g ON g.game_id = f.game_id AND g.team_id = f.team_id
    WHERE (p_num_starters_off_min IS NULL OR f.own_starters >= p_num_starters_off_min)
      AND (p_num_starters_off_max IS NULL OR f.own_starters <= p_num_starters_off_max)
      AND (p_num_starters_def_min IS NULL OR f.opp_starters >= p_num_starters_def_min)
      AND (p_num_starters_def_max IS NULL OR f.opp_starters <= p_num_starters_def_max)
    GROUP BY f.team_id
  ),
  rated AS (
    SELECT
      a.*,
      round(100.0 * a.off_pts / NULLIF(a.off_poss, 0), 1) AS off_ppp,
      round(100.0 * a.def_pts / NULLIF(a.def_poss, 0), 1) AS def_ppp
    FROM agg a
  )
  -- sum(bigint) is numeric in Postgres; the casts keep the row type matching
  -- the RETURNS TABLE declaration.
  SELECT
    p_game_year, r.team_id, t.display_name,
    r.off_ppp, r.def_ppp, round(r.off_ppp - r.def_ppp, 1),
    r.games_played::bigint, r.wins::bigint, r.losses::bigint,
    r.off_poss::bigint, r.def_poss::bigint,
    dense_rank() OVER (ORDER BY r.off_ppp - r.def_ppp DESC),
    dense_rank() OVER (ORDER BY r.off_ppp DESC),
    dense_rank() OVER (ORDER BY r.def_ppp ASC)
  FROM rated r
  JOIN euroleague.teams t ON t.team_id = r.team_id
  ORDER BY r.off_ppp - r.def_ppp DESC NULLS LAST;
END;
$function$;

DROP FUNCTION IF EXISTS euroleague.get_team_four_factors_dynamic(
  text, int4, date, date, text, text, text, text, text, int4, text,
  int4, int4, int4, int4, int4, int4, int4);

CREATE OR REPLACE FUNCTION euroleague.get_team_four_factors_dynamic(
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
    p_num_starters_def_max INTEGER DEFAULT NULL
)
RETURNS TABLE (
    game_year INT, team_id BIGINT, team_name TEXT,
    off_ppp NUMERIC, def_ppp NUMERIC, net_rtg NUMERIC,
    off_efg NUMERIC, def_efg NUMERIC, off_ts NUMERIC, def_ts NUMERIC,
    off_oreb NUMERIC, def_oreb NUMERIC, off_tov NUMERIC, def_tov NUMERIC,
    off_ftr NUMERIC, def_ftr NUMERIC,
    off_poss BIGINT, def_poss BIGINT
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
  agg AS (
    SELECT
      f.team_id,
      sum(f.off_pts) AS off_pts, sum(f.off_poss) AS off_poss,
      sum(f.off_ts_poss) AS off_ts_poss, sum(f.off_oreb) AS off_oreb,
      sum(f.off_oreb_opp) AS off_oreb_opp, sum(f.off_tov) AS off_tov,
      sum(f.off_fta) AS off_fta, sum(f.off_fga) AS off_fga,
      sum(f.off_fgm) AS off_fgm, sum(f.off_fg3m) AS off_fg3m,
      sum(f.def_pts) AS def_pts, sum(f.def_poss) AS def_poss,
      sum(f.def_ts_poss) AS def_ts_poss, sum(f.def_oreb) AS def_oreb,
      sum(f.def_oreb_opp) AS def_oreb_opp, sum(f.def_tov) AS def_tov,
      sum(f.def_fta) AS def_fta, sum(f.def_fga) AS def_fga,
      sum(f.def_fgm) AS def_fgm, sum(f.def_fg3m) AS def_fg3m
    FROM euroleague.team_four_factors_by_game f
    JOIN games g ON g.game_id = f.game_id AND g.team_id = f.team_id
    WHERE (p_num_starters_off_min IS NULL OR f.own_starters >= p_num_starters_off_min)
      AND (p_num_starters_off_max IS NULL OR f.own_starters <= p_num_starters_off_max)
      AND (p_num_starters_def_min IS NULL OR f.opp_starters >= p_num_starters_def_min)
      AND (p_num_starters_def_max IS NULL OR f.opp_starters <= p_num_starters_def_max)
    GROUP BY f.team_id
  )
  SELECT
    p_game_year, a.team_id, t.display_name,
    round(100.0 * a.off_pts / NULLIF(a.off_poss, 0), 1),
    round(100.0 * a.def_pts / NULLIF(a.def_poss, 0), 1),
    round(100.0 * a.off_pts / NULLIF(a.off_poss, 0)
        - 100.0 * a.def_pts / NULLIF(a.def_poss, 0), 1),
    round(100.0 * (a.off_fgm + 0.5 * a.off_fg3m) / NULLIF(a.off_fga, 0), 1),
    round(100.0 * (a.def_fgm + 0.5 * a.def_fg3m) / NULLIF(a.def_fga, 0), 1),
    round(100.0 * a.off_pts / NULLIF(2 * a.off_ts_poss, 0), 1),
    round(100.0 * a.def_pts / NULLIF(2 * a.def_ts_poss, 0), 1),
    round(100.0 * a.off_oreb / NULLIF(a.off_oreb_opp, 0), 1),
    round(100.0 * a.def_oreb / NULLIF(a.def_oreb_opp, 0), 1),
    round(100.0 * a.off_tov / NULLIF(a.off_poss, 0), 1),
    round(100.0 * a.def_tov / NULLIF(a.def_poss, 0), 1),
    round(100.0 * a.off_fta / NULLIF(a.off_fga, 0), 1),
    round(100.0 * a.def_fta / NULLIF(a.def_fga, 0), 1),
    a.off_poss::bigint, a.def_poss::bigint
  FROM agg a
  JOIN euroleague.teams t ON t.team_id = a.team_id
  ORDER BY 100.0 * a.off_pts / NULLIF(a.off_poss, 0)
         - 100.0 * a.def_pts / NULLIF(a.def_poss, 0) DESC NULLS LAST;
END;
$function$;

-- ---------------------------------------------------------------------------
-- 5. Refresh entry point + grants.
-- ---------------------------------------------------------------------------

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
END;
$function$;

-- team_ppp_ratings_mv is DROPped and recreated above, which wipes the grant
-- migration 005 made -- exactly as DROP FUNCTION wipes EXECUTE grants. It must
-- be re-granted here or every reader gets "permission denied".
GRANT SELECT ON
  euroleague.team_four_factors_by_game,
  euroleague.team_four_factors_mv,
  euroleague.team_ppp_ratings_mv
TO app_readonly;

GRANT EXECUTE ON FUNCTION
  euroleague.get_team_ratings_dynamic(
    text, int4, date, date, text, text, text, text, text, text, int4, text,
    int4, int4, int4, int4, int4, int4, int4)
TO app_readonly;

GRANT EXECUTE ON FUNCTION
  euroleague.get_team_four_factors_dynamic(
    text, int4, date, date, text, text, text, text, text, text, int4, text,
    int4, int4, int4, int4, int4, int4, int4)
TO app_readonly;
