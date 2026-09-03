-- EUROLEAGUE SHADOW SCHEMA -- migration 052: expose the on/off rating
-- components the dashboard reader already computes.
--
-- 046 returns "Net RTG Diff", "Off ON Diff" and "Def ON Diff" -- the results of
-- subtracting four values its own p CTE builds and then discards. Returning the
-- four lets the app draw the on-court and off-court rating as a range instead of
-- only their difference. No new scan, join or index: the query does exactly the
-- work it already did and returns four more numbers.
--
-- The Israeli twin is sql/functions/four_factors_dashboard_compute.sql. Apply
-- both before editing either tab: a running app holds the pre-edit closure, so
-- only the live path notices a mismatch.
BEGIN;
SET LOCAL search_path TO euroleague, public;

-- Four new output columns change the return type, which CREATE OR REPLACE
-- cannot do. Dropping wipes app_readonly's EXECUTE grant; the GRANT at the
-- foot of this file restores it.
DROP FUNCTION IF EXISTS euroleague.four_factors_dashboard_compute(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,integer,integer,integer,integer,integer,integer);

CREATE OR REPLACE FUNCTION euroleague.four_factors_dashboard_compute(
  p_competition text, p_game_year integer, p_start_date date DEFAULT NULL,
  p_end_date date DEFAULT NULL, p_team_ids_csv text DEFAULT NULL,
  p_phase_csv text DEFAULT NULL, p_opp_ids_csv text DEFAULT NULL,
  p_home_away text DEFAULT 'all', p_outcome text DEFAULT 'all',
  p_opp_rank_side text DEFAULT NULL, p_opp_rank_n integer DEFAULT NULL,
  p_opp_rank_metric text DEFAULT NULL, p_min_gn integer DEFAULT NULL,
  p_max_gn integer DEFAULT NULL, p_last_n_games integer DEFAULT NULL,
  p_num_starters_off_min integer DEFAULT NULL,
  p_num_starters_off_max integer DEFAULT NULL,
  p_num_starters_def_min integer DEFAULT NULL,
  p_num_starters_def_max integer DEFAULT NULL
)
RETURNS TABLE (
  player_id bigint, team_id bigint, firstname text, lastname text,
  team_name text, game_year integer,
  off_on_ts numeric, off_off_ts numeric, def_on_ts numeric, def_off_ts numeric,
  off_on_efg numeric, off_off_efg numeric, def_on_efg numeric, def_off_efg numeric,
  off_on_oreb numeric, off_off_oreb numeric, def_on_oreb numeric, def_off_oreb numeric,
  off_on_tov numeric, off_off_tov numeric, def_on_tov numeric, def_off_tov numeric,
  def_on_disruptions numeric, def_off_disruptions numeric,
  off_on_ftr numeric, off_off_ftr numeric, def_on_ftr numeric, def_off_ftr numeric,
  off_on_poss bigint, off_off_poss bigint, def_on_poss bigint, def_off_poss bigint,
  "Off eFG% Diff" numeric, "Off TS% Diff" numeric, "Off OREB% Diff" numeric,
  "Off TOV% Diff" numeric, "Off FTR Diff" numeric,
  "Def eFG% Diff" numeric, "Def TS% Diff" numeric, "Def OREB% Diff" numeric,
  "Def TOV% Diff" numeric, "Def FTR Diff" numeric,
  "Def Disruptions/100 Diff" numeric,
  "Net RTG Diff" numeric, "Off ON Diff" numeric, "Def ON Diff" numeric,
  minutes numeric,
  -- Appended, so every existing column keeps its position.
  off_on_ppp numeric, off_off_ppp numeric, def_on_ppp numeric, def_off_ppp numeric
)
LANGUAGE sql STABLE
SET search_path TO 'pg_catalog', 'euroleague', 'public'
SET plan_cache_mode TO 'force_custom_plan'
AS $function$
WITH n AS (
  SELECT coalesce(nullif(btrim(p_competition), ''), 'E') competition,
    CASE WHEN nullif(btrim(p_team_ids_csv), '') IS NULL THEN NULL::bigint[]
      ELSE string_to_array(regexp_replace(p_team_ids_csv, '\s+', '', 'g'), ',')::bigint[] END team_ids,
    CASE WHEN nullif(btrim(p_phase_csv), '') IS NULL THEN NULL::text[]
      ELSE string_to_array(p_phase_csv, ',') END phases,
    CASE WHEN nullif(btrim(p_opp_ids_csv), '') IS NULL THEN NULL::bigint[]
      ELSE string_to_array(regexp_replace(p_opp_ids_csv, '\s+', '', 'g'), ',')::bigint[] END opp_ids,
    coalesce(nullif(btrim(p_home_away), ''), 'all') home_away,
    coalesce(nullif(btrim(p_outcome), ''), 'all') outcome,
    nullif(btrim(p_opp_rank_side), '') rank_side,
    coalesce(nullif(btrim(p_opp_rank_metric), ''), 'net') rank_metric
), schedule_ranked AS (
  SELECT fs.game_id, fs.team_id, fs.opp_team_id, fs.is_home, fs.has_won,
    s.round_number, s.phase, s.scheduled_at::date game_date,
    row_number() OVER (PARTITION BY fs.team_id ORDER BY s.scheduled_at DESC, fs.game_id DESC) team_game_rank
  FROM euroleague.final_schedule fs JOIN euroleague.schedule s USING (game_id)
  CROSS JOIN n WHERE s.competition=n.competition AND s.season=p_game_year
), team_ranked AS (
  SELECT r.team_id,r.off_rank,r.def_rank,r.net_rank,count(*) OVER() team_count
  FROM euroleague.team_ppp_ratings_mv r CROSS JOIN n
  WHERE r.competition=n.competition AND r.game_year=p_game_year
), games AS MATERIALIZED (
  SELECT sr.game_id,sr.team_id FROM schedule_ranked sr CROSS JOIN n
  LEFT JOIN team_ranked tr ON tr.team_id=sr.opp_team_id
  WHERE (p_start_date IS NULL OR sr.game_date>=p_start_date)
    AND (p_end_date IS NULL OR sr.game_date<=p_end_date)
    AND (n.phases IS NULL OR sr.phase=ANY(n.phases))
    AND (n.opp_ids IS NULL OR sr.opp_team_id=ANY(n.opp_ids))
    AND (p_min_gn IS NULL OR sr.round_number>=p_min_gn)
    AND (p_max_gn IS NULL OR sr.round_number<=p_max_gn)
    AND (p_last_n_games IS NULL OR sr.team_game_rank<=p_last_n_games)
    AND (n.home_away='all' OR n.home_away='home' AND sr.is_home OR n.home_away='away' AND NOT sr.is_home)
    AND (n.outcome='all' OR n.outcome='win' AND sr.has_won OR n.outcome='loss' AND NOT sr.has_won)
    AND (n.rank_side IS NULL OR p_opp_rank_n IS NULL
      OR n.rank_side='top' AND CASE n.rank_metric WHEN 'off' THEN tr.off_rank WHEN 'def' THEN tr.def_rank ELSE tr.net_rank END<=p_opp_rank_n
      OR n.rank_side='bottom' AND CASE n.rank_metric WHEN 'off' THEN tr.off_rank WHEN 'def' THEN tr.def_rank ELSE tr.net_rank END>tr.team_count-p_opp_rank_n)
), agg AS (
  SELECT c.player_id,c.team_id,c.is_on_key,c.type_lineup,
    sum(c.total_points)::numeric pts,sum(c.total_poss)::bigint poss,
    sum(c.onoff_minutes)::numeric mins,sum(c.ts_poss_count)::bigint ts_poss,
    sum(c.oreb_count)::bigint oreb,sum(c.oreb_opportunities)::bigint oreb_opp,
    sum(c.tov_count)::bigint tov,sum(c.steal_count)::bigint steals,
    sum(c.deflection_count)::bigint deflections,sum(c.total_ft_attempts)::bigint fta,
    sum(c.total_fga)::bigint fga,sum(c.total_fgm)::bigint fgm,
    sum(c.total_fg3_made)::bigint fg3m
  FROM euroleague.player_four_factors_by_game c JOIN games g USING(game_id,team_id) CROSS JOIN n
  WHERE (p_num_starters_off_min IS NULL OR c.own_starters>=p_num_starters_off_min)
    AND (p_num_starters_off_max IS NULL OR c.own_starters<=p_num_starters_off_max)
    AND (p_num_starters_def_min IS NULL OR c.opp_starters>=p_num_starters_def_min)
    AND (p_num_starters_def_max IS NULL OR c.opp_starters<=p_num_starters_def_max)
    AND (n.team_ids IS NULL OR c.team_id=ANY(n.team_ids))
  GROUP BY c.player_id,c.team_id,c.is_on_key,c.type_lineup
), rates AS (
  SELECT a.*,
    a.pts/nullif(2*a.ts_poss,0)::numeric ts_pct,
    (a.fgm+0.5*a.fg3m)/nullif(a.fga,0)::numeric efg_pct,
    a.oreb/nullif(a.oreb_opp,0)::numeric oreb_pct,
    a.tov/nullif(a.poss,0)::numeric tov_pct,
    (a.steals+a.deflections)/nullif(a.poss,0)::numeric disruption_rate,
    a.fta/nullif(a.fga,0)::numeric ftr,
    round(100.0*a.pts/nullif(a.poss,0),1) ppp
  FROM agg a
), p AS (
  SELECT r.player_id,r.team_id,
    max(ts_pct) FILTER(WHERE type_lineup='offense' AND is_on_key=1) off_on_ts,max(ts_pct) FILTER(WHERE type_lineup='offense' AND is_on_key=0) off_off_ts,max(ts_pct) FILTER(WHERE type_lineup='defense' AND is_on_key=1) def_on_ts,max(ts_pct) FILTER(WHERE type_lineup='defense' AND is_on_key=0) def_off_ts,
    max(efg_pct) FILTER(WHERE type_lineup='offense' AND is_on_key=1) off_on_efg,max(efg_pct) FILTER(WHERE type_lineup='offense' AND is_on_key=0) off_off_efg,max(efg_pct) FILTER(WHERE type_lineup='defense' AND is_on_key=1) def_on_efg,max(efg_pct) FILTER(WHERE type_lineup='defense' AND is_on_key=0) def_off_efg,
    max(oreb_pct) FILTER(WHERE type_lineup='offense' AND is_on_key=1) off_on_oreb,max(oreb_pct) FILTER(WHERE type_lineup='offense' AND is_on_key=0) off_off_oreb,max(oreb_pct) FILTER(WHERE type_lineup='defense' AND is_on_key=1) def_on_oreb,max(oreb_pct) FILTER(WHERE type_lineup='defense' AND is_on_key=0) def_off_oreb,
    max(tov_pct) FILTER(WHERE type_lineup='offense' AND is_on_key=1) off_on_tov,max(tov_pct) FILTER(WHERE type_lineup='offense' AND is_on_key=0) off_off_tov,max(tov_pct) FILTER(WHERE type_lineup='defense' AND is_on_key=1) def_on_tov,max(tov_pct) FILTER(WHERE type_lineup='defense' AND is_on_key=0) def_off_tov,
    max(disruption_rate) FILTER(WHERE type_lineup='defense' AND is_on_key=1) def_on_disr,max(disruption_rate) FILTER(WHERE type_lineup='defense' AND is_on_key=0) def_off_disr,
    max(ftr) FILTER(WHERE type_lineup='offense' AND is_on_key=1) off_on_ftr,max(ftr) FILTER(WHERE type_lineup='offense' AND is_on_key=0) off_off_ftr,max(ftr) FILTER(WHERE type_lineup='defense' AND is_on_key=1) def_on_ftr,max(ftr) FILTER(WHERE type_lineup='defense' AND is_on_key=0) def_off_ftr,
    max(poss) FILTER(WHERE type_lineup='offense' AND is_on_key=1) off_on_poss,max(poss) FILTER(WHERE type_lineup='offense' AND is_on_key=0) off_off_poss,max(poss) FILTER(WHERE type_lineup='defense' AND is_on_key=1) def_on_poss,max(poss) FILTER(WHERE type_lineup='defense' AND is_on_key=0) def_off_poss,
    max(ppp) FILTER(WHERE type_lineup='offense' AND is_on_key=1) off_on_ppp,max(ppp) FILTER(WHERE type_lineup='offense' AND is_on_key=0) off_off_ppp,max(ppp) FILTER(WHERE type_lineup='defense' AND is_on_key=1) def_on_ppp,max(ppp) FILTER(WHERE type_lineup='defense' AND is_on_key=0) def_off_ppp,
    max(mins) FILTER(WHERE type_lineup='offense' AND is_on_key=1) mins_on
  FROM rates r GROUP BY r.player_id,r.team_id
)
SELECT p.player_id,p.team_id,btrim(split_part(pl.display_name,',',2)),btrim(split_part(pl.display_name,',',1)),t.display_name,p_game_year,
  p.off_on_ts,p.off_off_ts,p.def_on_ts,p.def_off_ts,p.off_on_efg,p.off_off_efg,p.def_on_efg,p.def_off_efg,
  p.off_on_oreb,p.off_off_oreb,p.def_on_oreb,p.def_off_oreb,p.off_on_tov,p.off_off_tov,p.def_on_tov,p.def_off_tov,
  p.def_on_disr,p.def_off_disr,p.off_on_ftr,p.off_off_ftr,p.def_on_ftr,p.def_off_ftr,
  p.off_on_poss,p.off_off_poss,p.def_on_poss,p.def_off_poss,
  round(100*(p.off_on_efg-p.off_off_efg),1),round(100*(p.off_on_ts-p.off_off_ts),1),round(100*(p.off_on_oreb-p.off_off_oreb),1),round(100*(p.off_on_tov-p.off_off_tov),1),round(100*(p.off_on_ftr-p.off_off_ftr),1),
  round(100*(p.def_on_efg-p.def_off_efg),1),round(100*(p.def_on_ts-p.def_off_ts),1),round(100*(p.def_on_oreb-p.def_off_oreb),1),round(100*(p.def_on_tov-p.def_off_tov),1),round(100*(p.def_on_ftr-p.def_off_ftr),1),round(100*(p.def_on_disr-p.def_off_disr),1),
  CASE WHEN (p.off_on_ppp-p.off_off_ppp)-(p.def_on_ppp-p.def_off_ppp) IS NOT NULL
    THEN (p.off_on_ppp-p.off_off_ppp)-(p.def_on_ppp-p.def_off_ppp) END,
  CASE WHEN (p.off_on_ppp-p.off_off_ppp)-(p.def_on_ppp-p.def_off_ppp) IS NOT NULL
    THEN p.off_on_ppp-p.off_off_ppp END,
  CASE WHEN (p.off_on_ppp-p.off_off_ppp)-(p.def_on_ppp-p.def_off_ppp) IS NOT NULL
    THEN p.def_on_ppp-p.def_off_ppp END,
  CASE WHEN (p.off_on_ppp-p.off_off_ppp)-(p.def_on_ppp-p.def_off_ppp) IS NOT NULL
    THEN round(p.mins_on,1) END,
  -- Ungated, unlike the diffs above: a rating that exists is reportable even
  -- when its opposite side is missing and the difference is therefore NULL.
  p.off_on_ppp, p.off_off_ppp, p.def_on_ppp, p.def_off_ppp
FROM p JOIN euroleague.players pl USING(player_id) JOIN euroleague.teams t USING(team_id)
$function$;

REVOKE ALL ON FUNCTION euroleague.four_factors_dashboard_compute(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,integer,integer,integer,integer,integer,integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION euroleague.four_factors_dashboard_compute(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,integer,integer,integer,integer,integer,integer) TO app_readonly;

COMMIT;
