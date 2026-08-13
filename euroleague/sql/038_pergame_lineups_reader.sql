-- EuroLeague migration 038: per-game Lineups reader for the
-- EUROLEAGUE SHADOW SCHEMA.
--
-- Why this exists
-- ---------------
-- Tab 10 routes every non-custom-clutch request -- including requests with no
-- clutch predicate at all -- to `fetch_lineups_dynamic` (migration 029), which
-- costs 21 s warm for a broad season request. Migration 037 fixed the same
-- routing gap on the Team tabs.
--
-- The cause here is NOT the one 037 fixed. A probe of the live schema showed
-- `fetch_lineups_dynamic` never touches the action fact on a non-clutch
-- request: `select_team_game_facts` (migration 020) already branches to
-- `lineup_totals_by_game` when margin and time are absent, and returns that
-- table row for row (8,440 rows over a 40-game sample, zero rows differing in
-- either direction). The 21 s is query *shape*, not data volume:
--
--   1. the fact arrives through two nested analytical function boundaries
--      (fetch_lineups_dynamic -> filtered_team_game_facts ->
--      select_team_game_facts), the parameter-planning failure PROJECT.md
--      documents;
--   2. `lineup_identity` then joins `lineup_totals_by_game` a SECOND time,
--      on a five-element text[] equality, purely to recover `lineup_key` and
--      `player_ids` -- two columns the fact rows already carried;
--   3. the result is expanded through `sub_lineups` even at unit size 5.
--
-- This reader reads the fact once and groups it. Measured on the broad
-- season preset at size 5: 8,240 units in 0.19 s versus 21.23 s, with zero
-- differing rows and zero differing values.
--
-- Deliberately NOT parameterised for clutch
-- -----------------------------------------
-- As in 037: `lineup_totals_by_game` has no time or margin dimension, so the
-- four clutch parameters are absent from the signature rather than accepted
-- and ignored. This reader takes 23 parameters where the clutch-capable
-- readers take 27, so a mis-routed clutch request fails loudly at the call
-- site instead of silently returning unfiltered numbers.
--
-- Grain, verified before this was written
-- ---------------------------------------
-- `lineup_totals_by_game` is keyed (game_id, team_id, lineup_key, type_lineup,
-- opp_starters), so one lineup instance spans 2-12 rows. `own_starters` is
-- functionally determined by (game_id, team_id, lineup_key) -- zero violating
-- instances in the season -- so both starter bounds are plain row predicates,
-- exactly as `filtered_team_game_facts` applies them.
--
-- Unit size 5 bypasses `sub_lineups` (PROJECT.md lesson 10). That is a row-set
-- identity, not an approximation: all 8,240 season lineups have exactly one
-- size-5 `sub_lineups` row, and in every one `unit_key = lineup_key` and
-- `player_ids` are identical. Sizes 2-4 use the mapping, whose primary key
-- keeps one unit at most one row per lineup and so cannot double-count.
--
-- Floor time still comes only from offense rows: `seconds` is NULL on defense
-- rows by CHECK constraint (migration 013), so the FILTER is belt and braces.
--
-- Adds no fact table, no backfill, and no index.

BEGIN;
SET LOCAL search_path TO euroleague, public;

CREATE OR REPLACE FUNCTION euroleague.fetch_lineups_pergame(
    p_competition TEXT, p_game_year INTEGER,
    p_start_date DATE DEFAULT NULL, p_end_date DATE DEFAULT NULL,
    p_team_ids_csv TEXT DEFAULT NULL, p_phase_csv TEXT DEFAULT NULL,
    p_opp_ids_csv TEXT DEFAULT NULL, p_home_away TEXT DEFAULT 'all',
    p_outcome TEXT DEFAULT 'all', p_opp_rank_side TEXT DEFAULT NULL,
    p_opp_rank_n INTEGER DEFAULT NULL, p_opp_rank_metric TEXT DEFAULT NULL,
    p_min_gn INTEGER DEFAULT NULL, p_max_gn INTEGER DEFAULT NULL,
    p_last_n_games INTEGER DEFAULT NULL,
    p_num_starters_off_min INTEGER DEFAULT NULL,
    p_num_starters_off_max INTEGER DEFAULT NULL,
    p_num_starters_def_min INTEGER DEFAULT NULL,
    p_num_starters_def_max INTEGER DEFAULT NULL,
    p_unit_size INTEGER DEFAULT 5,
    p_players_on_csv TEXT DEFAULT NULL,
    p_players_off_csv TEXT DEFAULT NULL,
    p_min_poss INTEGER DEFAULT 0
)
RETURNS TABLE (
    team_id BIGINT, unit_key TEXT, unit_size SMALLINT,
    player_ids BIGINT[], player_names_str TEXT,
    off_poss BIGINT, off_pts BIGINT, off_fg2_made BIGINT, off_fg2_att BIGINT,
    off_fg3_made BIGINT, off_fg3_att BIGINT, off_ts_poss BIGINT,
    off_fgm BIGINT, off_fga BIGINT, off_fta BIGINT,
    off_oreb BIGINT, off_oreb_opp BIGINT, off_tov BIGINT, off_steals BIGINT,
    def_poss BIGINT, def_pts BIGINT, def_fg2_made BIGINT, def_fg2_att BIGINT,
    def_fg3_made BIGINT, def_fg3_att BIGINT, def_ts_poss BIGINT,
    def_fgm BIGINT, def_fga BIGINT, def_fta BIGINT,
    def_oreb BIGINT, def_oreb_opp BIGINT, def_tov BIGINT, def_steals BIGINT,
    minutes NUMERIC
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, euroleague, public
SET plan_cache_mode = force_custom_plan
AS $function$
  -- n / sr / ranks / games are the schedule filter of migration 035, minus the
  -- margin_status normalisation this reader has no parameter for.
  WITH n AS (
    SELECT coalesce(nullif(btrim(p_competition), ''), 'E') AS competition,
      CASE WHEN nullif(btrim(p_team_ids_csv), '') IS NULL THEN NULL::bigint[]
           ELSE string_to_array(regexp_replace(p_team_ids_csv, '\s+', '', 'g'), ',')::bigint[] END AS team_ids,
      CASE WHEN nullif(btrim(p_phase_csv), '') IS NULL THEN NULL::text[]
           ELSE string_to_array(p_phase_csv, ',') END AS phases,
      CASE WHEN nullif(btrim(p_opp_ids_csv), '') IS NULL THEN NULL::bigint[]
           ELSE string_to_array(regexp_replace(p_opp_ids_csv, '\s+', '', 'g'), ',')::bigint[] END AS opp_ids,
      CASE WHEN nullif(btrim(p_players_on_csv), '') IS NULL THEN NULL::bigint[]
           ELSE string_to_array(regexp_replace(p_players_on_csv, '\s+', '', 'g'), ',')::bigint[] END AS players_on,
      CASE WHEN nullif(btrim(p_players_off_csv), '') IS NULL THEN NULL::bigint[]
           ELSE string_to_array(regexp_replace(p_players_off_csv, '\s+', '', 'g'), ',')::bigint[] END AS players_off,
      coalesce(nullif(btrim(p_home_away), ''), 'all') AS home_away,
      coalesce(nullif(btrim(p_outcome), ''), 'all') AS outcome,
      nullif(btrim(p_opp_rank_side), '') AS rank_side,
      coalesce(nullif(btrim(p_opp_rank_metric), ''), 'net') AS rank_metric
  ),
  sr AS (
    SELECT fs.*, row_number() OVER (
             PARTITION BY fs.team_id ORDER BY fs.game_date DESC, fs.game_id DESC
           ) AS recent
    FROM euroleague.final_schedule_mv fs CROSS JOIN n
    WHERE fs.competition = n.competition AND fs.game_year = p_game_year
  ),
  ranks AS (
    SELECT r.team_id, r.off_rank, r.def_rank, r.net_rank, count(*) OVER () AS team_count
    FROM euroleague.team_ppp_ratings_mv r CROSS JOIN n
    WHERE r.competition = n.competition AND r.game_year = p_game_year
  ),
  games AS MATERIALIZED (
    SELECT sr.game_id, sr.team_id
    FROM sr CROSS JOIN n LEFT JOIN ranks r ON r.team_id = sr.opp_team_id
    WHERE (p_start_date IS NULL OR sr.game_date >= p_start_date)
      AND (p_end_date IS NULL OR sr.game_date <= p_end_date)
      AND (n.team_ids IS NULL OR sr.team_id = ANY(n.team_ids))
      AND (n.phases IS NULL OR sr.phase = ANY(n.phases))
      AND (n.opp_ids IS NULL OR sr.opp_team_id = ANY(n.opp_ids))
      AND (n.home_away = 'all' OR n.home_away = 'home' AND sr.is_home
           OR n.home_away = 'away' AND NOT sr.is_home)
      AND (n.outcome = 'all' OR n.outcome = 'win' AND sr.has_won
           OR n.outcome = 'loss' AND NOT sr.has_won)
      AND (n.rank_side IS NULL OR p_opp_rank_n IS NULL
        OR n.rank_side = 'top' AND CASE n.rank_metric WHEN 'off' THEN r.off_rank
             WHEN 'def' THEN r.def_rank ELSE r.net_rank END <= p_opp_rank_n
        OR n.rank_side = 'bottom' AND CASE n.rank_metric WHEN 'off' THEN r.off_rank
             WHEN 'def' THEN r.def_rank ELSE r.net_rank END > r.team_count - p_opp_rank_n)
      AND (p_min_gn IS NULL OR sr.round_number >= p_min_gn)
      AND (p_max_gn IS NULL OR sr.round_number <= p_max_gn)
      AND (p_last_n_games IS NULL OR sr.recent <= p_last_n_games)
  ),
  -- The whole fact read. Starter bounds are the same predicates
  -- filtered_team_game_facts applies, against the same two columns.
  lineup_rows AS MATERIALIZED (
    SELECT l.team_id, l.lineup_key, l.player_ids, l.type_lineup,
      l.possessions, l.points, l.fg2_made, l.fg2_att, l.fg3_made, l.fg3_att,
      l.ts_possessions, l.fgm, l.fga, l.ft_attempts,
      l.orebounds, l.oreb_opportunities, l.turnovers, l.steals, l.seconds
    FROM euroleague.lineup_totals_by_game l
    JOIN games g USING (game_id, team_id)
    CROSS JOIN n
    WHERE l.competition = n.competition AND l.game_year = p_game_year
      AND (p_num_starters_off_min IS NULL OR l.own_starters >= p_num_starters_off_min)
      AND (p_num_starters_off_max IS NULL OR l.own_starters <= p_num_starters_off_max)
      AND (p_num_starters_def_min IS NULL OR l.opp_starters >= p_num_starters_def_min)
      AND (p_num_starters_def_max IS NULL OR l.opp_starters <= p_num_starters_def_max)
  ),
  -- Size 5 is the lineup itself; sizes 2-4 come from the season mapping.
  unit_rows AS (
    SELECT lr.team_id, lr.lineup_key AS unit_key, 5::smallint AS unit_size,
      lr.player_ids, lr.type_lineup, lr.possessions, lr.points,
      lr.fg2_made, lr.fg2_att, lr.fg3_made, lr.fg3_att, lr.ts_possessions,
      lr.fgm, lr.fga, lr.ft_attempts, lr.orebounds, lr.oreb_opportunities,
      lr.turnovers, lr.steals, lr.seconds
    FROM lineup_rows lr CROSS JOIN n
    WHERE p_unit_size = 5
      AND (n.players_on IS NULL OR lr.player_ids @> n.players_on)
      AND (n.players_off IS NULL OR NOT (lr.player_ids && n.players_off))
    UNION ALL
    SELECT lr.team_id, sl.unit_key, sl.unit_size,
      sl.player_ids, lr.type_lineup, lr.possessions, lr.points,
      lr.fg2_made, lr.fg2_att, lr.fg3_made, lr.fg3_att, lr.ts_possessions,
      lr.fgm, lr.fga, lr.ft_attempts, lr.orebounds, lr.oreb_opportunities,
      lr.turnovers, lr.steals, lr.seconds
    FROM lineup_rows lr
    JOIN euroleague.sub_lineups sl
      ON sl.competition = (SELECT competition FROM n)
     AND sl.game_year = p_game_year
     AND sl.team_id = lr.team_id
     AND sl.lineup_key = lr.lineup_key
    CROSS JOIN n
    WHERE p_unit_size BETWEEN 2 AND 4
      AND sl.unit_size = p_unit_size::smallint
      AND (n.players_on IS NULL OR sl.player_ids @> n.players_on)
      AND (n.players_off IS NULL OR NOT (sl.player_ids && n.players_off))
  ),
  agg AS (
    SELECT u.team_id, u.unit_key, u.unit_size, u.player_ids,
      sum(u.possessions) FILTER (WHERE u.type_lineup = 'offense') AS off_poss,
      sum(u.points) FILTER (WHERE u.type_lineup = 'offense') AS off_pts,
      sum(u.fg2_made) FILTER (WHERE u.type_lineup = 'offense') AS off_fg2_made,
      sum(u.fg2_att) FILTER (WHERE u.type_lineup = 'offense') AS off_fg2_att,
      sum(u.fg3_made) FILTER (WHERE u.type_lineup = 'offense') AS off_fg3_made,
      sum(u.fg3_att) FILTER (WHERE u.type_lineup = 'offense') AS off_fg3_att,
      sum(u.ts_possessions) FILTER (WHERE u.type_lineup = 'offense') AS off_ts_poss,
      sum(u.fgm) FILTER (WHERE u.type_lineup = 'offense') AS off_fgm,
      sum(u.fga) FILTER (WHERE u.type_lineup = 'offense') AS off_fga,
      sum(u.ft_attempts) FILTER (WHERE u.type_lineup = 'offense') AS off_fta,
      sum(u.orebounds) FILTER (WHERE u.type_lineup = 'offense') AS off_oreb,
      sum(u.oreb_opportunities) FILTER (WHERE u.type_lineup = 'offense') AS off_oreb_opp,
      sum(u.turnovers) FILTER (WHERE u.type_lineup = 'offense') AS off_tov,
      sum(u.steals) FILTER (WHERE u.type_lineup = 'offense') AS off_steals,
      sum(u.possessions) FILTER (WHERE u.type_lineup = 'defense') AS def_poss,
      sum(u.points) FILTER (WHERE u.type_lineup = 'defense') AS def_pts,
      sum(u.fg2_made) FILTER (WHERE u.type_lineup = 'defense') AS def_fg2_made,
      sum(u.fg2_att) FILTER (WHERE u.type_lineup = 'defense') AS def_fg2_att,
      sum(u.fg3_made) FILTER (WHERE u.type_lineup = 'defense') AS def_fg3_made,
      sum(u.fg3_att) FILTER (WHERE u.type_lineup = 'defense') AS def_fg3_att,
      sum(u.ts_possessions) FILTER (WHERE u.type_lineup = 'defense') AS def_ts_poss,
      sum(u.fgm) FILTER (WHERE u.type_lineup = 'defense') AS def_fgm,
      sum(u.fga) FILTER (WHERE u.type_lineup = 'defense') AS def_fga,
      sum(u.ft_attempts) FILTER (WHERE u.type_lineup = 'defense') AS def_fta,
      sum(u.orebounds) FILTER (WHERE u.type_lineup = 'defense') AS def_oreb,
      sum(u.oreb_opportunities) FILTER (WHERE u.type_lineup = 'defense') AS def_oreb_opp,
      sum(u.turnovers) FILTER (WHERE u.type_lineup = 'defense') AS def_tov,
      sum(u.steals) FILTER (WHERE u.type_lineup = 'defense') AS def_steals,
      sum(u.seconds) FILTER (WHERE u.type_lineup = 'offense') AS seconds
    FROM unit_rows u
    GROUP BY u.team_id, u.unit_key, u.unit_size, u.player_ids
  )
  SELECT
    a.team_id, a.unit_key, a.unit_size, a.player_ids,
    names.player_names_str,
    a.off_poss::bigint, a.off_pts::bigint,
    a.off_fg2_made::bigint, a.off_fg2_att::bigint,
    a.off_fg3_made::bigint, a.off_fg3_att::bigint,
    a.off_ts_poss::bigint, a.off_fgm::bigint, a.off_fga::bigint,
    a.off_fta::bigint, a.off_oreb::bigint, a.off_oreb_opp::bigint,
    a.off_tov::bigint, a.off_steals::bigint,
    a.def_poss::bigint, a.def_pts::bigint,
    a.def_fg2_made::bigint, a.def_fg2_att::bigint,
    a.def_fg3_made::bigint, a.def_fg3_att::bigint,
    a.def_ts_poss::bigint, a.def_fgm::bigint, a.def_fga::bigint,
    a.def_fta::bigint, a.def_oreb::bigint, a.def_oreb_opp::bigint,
    a.def_tov::bigint, a.def_steals::bigint,
    round(coalesce(a.seconds, 0) / 60.0, 1)
  FROM agg a
  CROSS JOIN LATERAL (
    SELECT string_agg(
      coalesce(euroleague.person_display_name(p.display_name), '#' || x.pid::text),
      ', ' ORDER BY x.ord
    ) AS player_names_str
    FROM unnest(a.player_ids) WITH ORDINALITY x(pid, ord)
    LEFT JOIN euroleague.players p ON p.player_id = x.pid
  ) names
  WHERE coalesce(a.off_poss, 0) + coalesce(a.def_poss, 0)
        >= coalesce(p_min_poss, 0)
$function$;

REVOKE ALL ON FUNCTION euroleague.fetch_lineups_pergame(
  text, integer, date, date, text, text, text, text, text, text, integer,
  text, integer, integer, integer, integer, integer, integer, integer,
  integer, text, text, integer
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION euroleague.fetch_lineups_pergame(
  text, integer, date, date, text, text, text, text, text, text, integer,
  text, integer, integer, integer, integer, integer, integer, integer,
  integer, text, text, integer
) TO app_readonly;

COMMIT;
