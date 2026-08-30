-- Combined Israeli filtered Four Factors app reader. Additive function only.
-- Match the EuroLeague dashboard query shape: filter eligible games once,
-- aggregate the additive player-game fact once, then derive factors and ratings.
-- The older four_factors_compute remains unchanged for its other consumers.
CREATE OR REPLACE FUNCTION basketball_test.four_factors_dashboard_compute(
  p_game_year int, p_start_date date DEFAULT NULL, p_end_date date DEFAULT NULL,
  p_team_ids_csv text DEFAULT NULL, p_game_type_csv text DEFAULT NULL,
  p_opp_ids_csv text DEFAULT NULL, p_home_away text DEFAULT 'all',
  p_outcome text DEFAULT 'all', p_opp_rank_side text DEFAULT 'all',
  p_opp_rank_n int DEFAULT NULL, p_opp_rank_metric text DEFAULT 'net',
  p_min_gn int DEFAULT NULL, p_max_gn int DEFAULT NULL,
  p_last_n_games int DEFAULT NULL, p_num_starters_off int DEFAULT NULL,
  p_num_starters_def int DEFAULT NULL, p_num_starters_off_min int DEFAULT NULL,
  p_num_starters_off_max int DEFAULT NULL, p_num_starters_def_min int DEFAULT NULL,
  p_num_starters_def_max int DEFAULT NULL
)
RETURNS TABLE (
  player_id int, team_id int, firstname text, lastname text, team_name text,
  game_year int, off_on_ts numeric, off_off_ts numeric, def_on_ts numeric,
  def_off_ts numeric, off_on_efg numeric, off_off_efg numeric, def_on_efg numeric,
  def_off_efg numeric, off_on_oreb numeric, off_off_oreb numeric,
  def_on_oreb numeric, def_off_oreb numeric, off_on_tov numeric,
  off_off_tov numeric, def_on_tov numeric, def_off_tov numeric,
  def_on_disruptions numeric, def_off_disruptions numeric, off_on_ftr numeric,
  off_off_ftr numeric, def_on_ftr numeric, def_off_ftr numeric,
  off_on_poss bigint, off_off_poss bigint, def_on_poss bigint, def_off_poss bigint,
  "Off eFG% Diff" numeric, "Off TS% Diff" numeric, "Off OREB% Diff" numeric,
  "Off TOV% Diff" numeric, "Off FTR Diff" numeric, "Def eFG% Diff" numeric,
  "Def TS% Diff" numeric, "Def OREB% Diff" numeric, "Def TOV% Diff" numeric,
  "Def FTR Diff" numeric, "Def Disruptions/100 Diff" numeric,
  "Net RTG Diff" numeric, "Off ON Diff" numeric, "Def ON Diff" numeric,
  minutes numeric
)
LANGUAGE sql STABLE
SET search_path TO 'pg_catalog', 'basketball_test', 'public'
SET plan_cache_mode TO 'force_custom_plan'
AS $function$
WITH n AS (
  SELECT
    CASE WHEN nullif(btrim(p_team_ids_csv), '') IS NULL THEN NULL::int[]
      ELSE string_to_array(regexp_replace(p_team_ids_csv, '\s+', '', 'g'), ',')::int[] END team_ids,
    CASE WHEN nullif(btrim(p_game_type_csv), '') IS NULL THEN NULL::int[]
      ELSE string_to_array(regexp_replace(p_game_type_csv, '\s+', '', 'g'), ',')::int[] END game_types,
    CASE WHEN nullif(btrim(p_opp_ids_csv), '') IS NULL THEN NULL::int[]
      ELSE string_to_array(regexp_replace(p_opp_ids_csv, '\s+', '', 'g'), ',')::int[] END opp_ids,
    coalesce(nullif(btrim(p_home_away), ''), 'all') home_away,
    coalesce(nullif(btrim(p_outcome), ''), 'all') outcome,
    coalesce(nullif(btrim(p_opp_rank_side), ''), 'all') rank_side,
    coalesce(nullif(btrim(p_opp_rank_metric), ''), 'net') rank_metric
), schedule_ranked AS (
  SELECT fs.game_id, fs.team_id, fs.game_year, fs.opp_team_id, fs.game_date,
    fs.game_type, fs.is_home, fs.has_won, fs.gn,
    row_number() OVER (
      PARTITION BY fs.team_id, fs.game_year
      ORDER BY fs.game_date DESC NULLS LAST, fs.game_id DESC
    ) recent
  FROM basketball_test.final_schedule_mv fs
  WHERE fs.game_year = p_game_year
), games_base AS (
  SELECT sr.game_id, sr.team_id, sr.game_year, sr.opp_team_id
  FROM schedule_ranked sr CROSS JOIN n
  WHERE (p_start_date IS NULL OR sr.game_date >= p_start_date)
    AND (p_end_date IS NULL OR sr.game_date <= p_end_date)
    AND (n.game_types IS NULL OR sr.game_type = ANY(n.game_types))
    AND (n.opp_ids IS NULL OR sr.opp_team_id = ANY(n.opp_ids))
    AND (n.team_ids IS NULL OR sr.team_id = ANY(n.team_ids))
    AND (n.home_away = 'all' OR n.home_away = 'home' AND sr.is_home
      OR n.home_away = 'away' AND NOT sr.is_home)
    AND (n.outcome = 'all' OR n.outcome = 'win' AND sr.has_won IS TRUE
      OR n.outcome = 'loss' AND sr.has_won IS FALSE)
    AND (p_min_gn IS NULL OR sr.gn >= p_min_gn)
    AND (p_max_gn IS NULL OR sr.gn <= p_max_gn)
    AND (p_last_n_games IS NULL OR sr.recent <= p_last_n_games)
), games_ranked AS (
  SELECT gb.*,
    CASE n.rank_metric WHEN 'off' THEN r.rank_off_ppp
      WHEN 'def' THEN r.rank_def_ppp ELSE r.rank_net_rtg END opp_rank,
    max(CASE n.rank_metric WHEN 'off' THEN r.rank_off_ppp
      WHEN 'def' THEN r.rank_def_ppp ELSE r.rank_net_rtg END)
      OVER (PARTITION BY gb.game_year) max_rank,
    n.rank_side
  FROM games_base gb CROSS JOIN n
  LEFT JOIN basketball_test.team_ppp_ratings_mv r
    ON r.game_year::int = gb.game_year AND r.team_id::int = gb.opp_team_id
), games AS MATERIALIZED (
  SELECT game_id, team_id
  FROM games_ranked
  WHERE rank_side = 'all' OR p_opp_rank_n IS NULL
    OR rank_side = 'top' AND opp_rank <= p_opp_rank_n
    OR rank_side = 'bottom' AND opp_rank >= max_rank - p_opp_rank_n + 1
), agg AS (
  SELECT pf.player_id, pf.team_id, pf.is_on_key, pf.type_lineup,
    sum(pf.total_points)::numeric pts,
    sum(pf.total_poss)::bigint poss,
    sum(coalesce(pf.onoff_minutes, 0))::numeric mins,
    sum(pf.ts_poss_count)::bigint ts_poss,
    sum(pf.oreb_count)::bigint oreb,
    sum(pf.oreb_opportunities)::bigint oreb_opp,
    sum(pf.tov_count)::bigint tov,
    sum(pf.steal_count)::bigint steals,
    sum(pf.deflection_count)::bigint deflections,
    sum(pf.total_ft_attempts)::bigint fta,
    sum(pf.total_fga)::bigint fga,
    sum(pf.total_fgm)::bigint fgm,
    sum(pf.total_fg3_made)::bigint fg3m
  FROM basketball_test.player_four_factors_by_game pf
  JOIN games g USING (game_id, team_id)
  WHERE (coalesce(p_num_starters_off_min, p_num_starters_off) IS NULL
      OR pf.own_starters >= coalesce(p_num_starters_off_min, p_num_starters_off))
    AND (coalesce(p_num_starters_off_max, p_num_starters_off) IS NULL
      OR pf.own_starters <= coalesce(p_num_starters_off_max, p_num_starters_off))
    AND (coalesce(p_num_starters_def_min, p_num_starters_def) IS NULL
      OR pf.opp_starters >= coalesce(p_num_starters_def_min, p_num_starters_def))
    AND (coalesce(p_num_starters_def_max, p_num_starters_def) IS NULL
      OR pf.opp_starters <= coalesce(p_num_starters_def_max, p_num_starters_def))
  GROUP BY pf.player_id, pf.team_id, pf.is_on_key, pf.type_lineup
), rates AS (
  SELECT a.*,
    a.pts / nullif(2 * a.ts_poss, 0)::numeric ts_pct,
    (a.fgm + 0.5 * a.fg3m) / nullif(a.fga, 0)::numeric efg_pct,
    a.oreb / nullif(a.oreb_opp, 0)::numeric oreb_pct,
    a.tov / nullif(a.poss, 0)::numeric tov_pct,
    (a.steals + a.deflections) / nullif(a.poss, 0)::numeric disruption_rate,
    a.fta / nullif(a.fga, 0)::numeric ftr,
    round(100.0 * a.pts / nullif(a.poss, 0), 1) ppp
  FROM agg a
), p AS (
  SELECT r.player_id, r.team_id,
    max(ts_pct) FILTER (WHERE type_lineup='offense' AND is_on_key=1) off_on_ts,
    max(ts_pct) FILTER (WHERE type_lineup='offense' AND is_on_key=0) off_off_ts,
    max(ts_pct) FILTER (WHERE type_lineup='defense' AND is_on_key=1) def_on_ts,
    max(ts_pct) FILTER (WHERE type_lineup='defense' AND is_on_key=0) def_off_ts,
    max(efg_pct) FILTER (WHERE type_lineup='offense' AND is_on_key=1) off_on_efg,
    max(efg_pct) FILTER (WHERE type_lineup='offense' AND is_on_key=0) off_off_efg,
    max(efg_pct) FILTER (WHERE type_lineup='defense' AND is_on_key=1) def_on_efg,
    max(efg_pct) FILTER (WHERE type_lineup='defense' AND is_on_key=0) def_off_efg,
    max(oreb_pct) FILTER (WHERE type_lineup='offense' AND is_on_key=1) off_on_oreb,
    max(oreb_pct) FILTER (WHERE type_lineup='offense' AND is_on_key=0) off_off_oreb,
    max(oreb_pct) FILTER (WHERE type_lineup='defense' AND is_on_key=1) def_on_oreb,
    max(oreb_pct) FILTER (WHERE type_lineup='defense' AND is_on_key=0) def_off_oreb,
    max(tov_pct) FILTER (WHERE type_lineup='offense' AND is_on_key=1) off_on_tov,
    max(tov_pct) FILTER (WHERE type_lineup='offense' AND is_on_key=0) off_off_tov,
    max(tov_pct) FILTER (WHERE type_lineup='defense' AND is_on_key=1) def_on_tov,
    max(tov_pct) FILTER (WHERE type_lineup='defense' AND is_on_key=0) def_off_tov,
    max(disruption_rate) FILTER (WHERE type_lineup='defense' AND is_on_key=1) def_on_disr,
    max(disruption_rate) FILTER (WHERE type_lineup='defense' AND is_on_key=0) def_off_disr,
    max(ftr) FILTER (WHERE type_lineup='offense' AND is_on_key=1) off_on_ftr,
    max(ftr) FILTER (WHERE type_lineup='offense' AND is_on_key=0) off_off_ftr,
    max(ftr) FILTER (WHERE type_lineup='defense' AND is_on_key=1) def_on_ftr,
    max(ftr) FILTER (WHERE type_lineup='defense' AND is_on_key=0) def_off_ftr,
    max(poss) FILTER (WHERE type_lineup='offense' AND is_on_key=1) off_on_poss,
    max(poss) FILTER (WHERE type_lineup='offense' AND is_on_key=0) off_off_poss,
    max(poss) FILTER (WHERE type_lineup='defense' AND is_on_key=1) def_on_poss,
    max(poss) FILTER (WHERE type_lineup='defense' AND is_on_key=0) def_off_poss,
    max(ppp) FILTER (WHERE type_lineup='offense' AND is_on_key=1) off_on_ppp,
    max(ppp) FILTER (WHERE type_lineup='offense' AND is_on_key=0) off_off_ppp,
    max(ppp) FILTER (WHERE type_lineup='defense' AND is_on_key=1) def_on_ppp,
    max(ppp) FILTER (WHERE type_lineup='defense' AND is_on_key=0) def_off_ppp,
    max(mins) FILTER (WHERE type_lineup='offense' AND is_on_key=1) mins_on
  FROM rates r
  GROUP BY r.player_id, r.team_id
), roster AS (
  SELECT fr.player_id, fr.team_id,
    min(fr.firstname) firstname, min(fr.lastname) lastname, min(fr.team_name) team_name
  FROM basketball_test.full_rosters fr
  WHERE fr.game_year = p_game_year
  GROUP BY fr.player_id, fr.team_id
)
SELECT p.player_id, p.team_id, r.firstname, r.lastname, r.team_name, p_game_year,
  p.off_on_ts, p.off_off_ts, p.def_on_ts, p.def_off_ts,
  p.off_on_efg, p.off_off_efg, p.def_on_efg, p.def_off_efg,
  p.off_on_oreb, p.off_off_oreb, p.def_on_oreb, p.def_off_oreb,
  p.off_on_tov, p.off_off_tov, p.def_on_tov, p.def_off_tov,
  p.def_on_disr, p.def_off_disr,
  p.off_on_ftr, p.off_off_ftr, p.def_on_ftr, p.def_off_ftr,
  p.off_on_poss, p.off_off_poss, p.def_on_poss, p.def_off_poss,
  round(100 * (p.off_on_efg - p.off_off_efg), 1),
  round(100 * (p.off_on_ts - p.off_off_ts), 1),
  round(100 * (p.off_on_oreb - p.off_off_oreb), 1),
  round(100 * (p.off_on_tov - p.off_off_tov), 1),
  round(100 * (p.off_on_ftr - p.off_off_ftr), 1),
  round(100 * (p.def_on_efg - p.def_off_efg), 1),
  round(100 * (p.def_on_ts - p.def_off_ts), 1),
  round(100 * (p.def_on_oreb - p.def_off_oreb), 1),
  round(100 * (p.def_on_tov - p.def_off_tov), 1),
  round(100 * (p.def_on_ftr - p.def_off_ftr), 1),
  round(100 * (p.def_on_disr - p.def_off_disr), 1),
  CASE WHEN (p.off_on_ppp-p.off_off_ppp)-(p.def_on_ppp-p.def_off_ppp) IS NOT NULL
    THEN (p.off_on_ppp-p.off_off_ppp)-(p.def_on_ppp-p.def_off_ppp) END,
  CASE WHEN (p.off_on_ppp-p.off_off_ppp)-(p.def_on_ppp-p.def_off_ppp) IS NOT NULL
    THEN p.off_on_ppp-p.off_off_ppp END,
  CASE WHEN (p.off_on_ppp-p.off_off_ppp)-(p.def_on_ppp-p.def_off_ppp) IS NOT NULL
    THEN p.def_on_ppp-p.def_off_ppp END,
  CASE WHEN (p.off_on_ppp-p.off_off_ppp)-(p.def_on_ppp-p.def_off_ppp) IS NOT NULL
    THEN p.mins_on END
FROM p JOIN roster r USING (player_id, team_id)
ORDER BY round(100 * (p.off_on_ts - p.off_off_ts), 1) DESC NULLS LAST
$function$;

REVOKE ALL ON FUNCTION basketball_test.four_factors_dashboard_compute(int,date,date,text,text,text,text,text,text,int,text,int,int,int,int,int,int,int,int,int) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION basketball_test.four_factors_dashboard_compute(int,date,date,text,text,text,text,text,text,int,text,int,int,int,int,int,int,int,int,int) TO app_readonly;
