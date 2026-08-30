-- Combined Israeli filtered Four Factors app reader. Additive function only.
-- The trusted Four Factors reader remains unchanged; the former second full
-- onoff_compute call is replaced by a narrow ratings/minutes aggregation.
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
  SELECT CASE WHEN nullif(btrim(p_team_ids_csv),'') IS NULL THEN NULL::int[] ELSE string_to_array(regexp_replace(p_team_ids_csv,'\s+','','g'),',')::int[] END team_ids,
    CASE WHEN nullif(btrim(p_game_type_csv),'') IS NULL THEN NULL::int[] ELSE string_to_array(regexp_replace(p_game_type_csv,'\s+','','g'),',')::int[] END game_types,
    CASE WHEN nullif(btrim(p_opp_ids_csv),'') IS NULL THEN NULL::int[] ELSE string_to_array(regexp_replace(p_opp_ids_csv,'\s+','','g'),',')::int[] END opp_ids,
    coalesce(nullif(btrim(p_home_away),''),'all') home_away,
    coalesce(nullif(btrim(p_outcome),''),'all') outcome,
    coalesce(nullif(btrim(p_opp_rank_side),''),'all') rank_side,
    coalesce(nullif(btrim(p_opp_rank_metric),''),'net') rank_metric
), ff AS MATERIALIZED (
  SELECT * FROM basketball_test.four_factors_compute(
    p_game_year,p_start_date,p_end_date,p_team_ids_csv,p_game_type_csv,
    p_opp_ids_csv,p_home_away,p_outcome,p_opp_rank_side,p_opp_rank_n,
    p_opp_rank_metric,p_min_gn,p_max_gn,p_last_n_games,p_num_starters_off,
    p_num_starters_def,p_num_starters_off_min,p_num_starters_off_max,
    p_num_starters_def_min,p_num_starters_def_max)
), schedule_ranked AS (
  SELECT fs.*,row_number() OVER(PARTITION BY fs.team_id,fs.game_year ORDER BY fs.game_date DESC NULLS LAST,fs.game_id DESC) recent
  FROM basketball_test.final_schedule_mv fs WHERE fs.game_year=p_game_year
), games_base AS (
  SELECT sr.game_id,sr.team_id,sr.game_year,sr.opp_team_id FROM schedule_ranked sr CROSS JOIN n
  WHERE (p_start_date IS NULL OR sr.game_date>=p_start_date) AND (p_end_date IS NULL OR sr.game_date<=p_end_date)
    AND (n.game_types IS NULL OR sr.game_type=ANY(n.game_types))
    AND (n.opp_ids IS NULL OR sr.opp_team_id=ANY(n.opp_ids))
    AND (n.team_ids IS NULL OR sr.team_id=ANY(n.team_ids))
    AND (n.home_away='all' OR n.home_away='home' AND sr.is_home OR n.home_away='away' AND NOT sr.is_home)
    AND (n.outcome='all' OR n.outcome='win' AND sr.has_won IS TRUE OR n.outcome='loss' AND sr.has_won IS FALSE)
    AND (p_min_gn IS NULL OR sr.gn>=p_min_gn) AND (p_max_gn IS NULL OR sr.gn<=p_max_gn)
    AND (p_last_n_games IS NULL OR sr.recent<=p_last_n_games)
), games_ranked AS (
  SELECT gb.*,
    CASE n.rank_metric WHEN 'off' THEN r.rank_off_ppp WHEN 'def' THEN r.rank_def_ppp ELSE r.rank_net_rtg END opp_rank,
    max(CASE n.rank_metric WHEN 'off' THEN r.rank_off_ppp WHEN 'def' THEN r.rank_def_ppp ELSE r.rank_net_rtg END) OVER(PARTITION BY gb.game_year) max_rank,
    n.rank_side
  FROM games_base gb CROSS JOIN n LEFT JOIN basketball_test.team_ppp_ratings_mv r
    ON r.game_year::int=gb.game_year AND r.team_id::int=gb.opp_team_id
), games AS MATERIALIZED (
  SELECT game_id,team_id FROM games_ranked
  WHERE rank_side='all' OR p_opp_rank_n IS NULL OR rank_side='top' AND opp_rank<=p_opp_rank_n
    OR rank_side='bottom' AND opp_rank>=max_rank-p_opp_rank_n+1
), a AS (
  SELECT pf.player_id,pf.team_id,pf.is_on_key,pf.type_lineup,
    round(sum(pf.total_points)::numeric/nullif(sum(pf.total_poss),0)::numeric*100,1) ppp,
    sum(coalesce(pf.onoff_minutes,0))::numeric mins
  FROM basketball_test.player_four_factors_by_game pf JOIN games g USING(game_id,team_id)
  WHERE (coalesce(p_num_starters_off_min,p_num_starters_off) IS NULL OR pf.own_starters>=coalesce(p_num_starters_off_min,p_num_starters_off))
    AND (coalesce(p_num_starters_off_max,p_num_starters_off) IS NULL OR pf.own_starters<=coalesce(p_num_starters_off_max,p_num_starters_off))
    AND (coalesce(p_num_starters_def_min,p_num_starters_def) IS NULL OR pf.opp_starters>=coalesce(p_num_starters_def_min,p_num_starters_def))
    AND (coalesce(p_num_starters_def_max,p_num_starters_def) IS NULL OR pf.opp_starters<=coalesce(p_num_starters_def_max,p_num_starters_def))
  GROUP BY pf.player_id,pf.team_id,pf.is_on_key,pf.type_lineup
), r AS (
  SELECT player_id,team_id,
    max(ppp) FILTER(WHERE type_lineup='offense' AND is_on_key=1) off_on,
    max(ppp) FILTER(WHERE type_lineup='offense' AND is_on_key=0) off_off,
    max(ppp) FILTER(WHERE type_lineup='defense' AND is_on_key=1) def_on,
    max(ppp) FILTER(WHERE type_lineup='defense' AND is_on_key=0) def_off,
    max(mins) FILTER(WHERE type_lineup='offense' AND is_on_key=1) mins
  FROM a GROUP BY player_id,team_id
), rated AS (
  SELECT r.*,(off_on-off_off)-(def_on-def_off) net_diff,off_on-off_off off_diff,def_on-def_off def_diff
  FROM r
)
SELECT ff.*,
  CASE WHEN rated.net_diff IS NOT NULL THEN rated.net_diff END,
  CASE WHEN rated.net_diff IS NOT NULL THEN rated.off_diff END,
  CASE WHEN rated.net_diff IS NOT NULL THEN rated.def_diff END,
  CASE WHEN rated.net_diff IS NOT NULL THEN rated.mins END
FROM ff LEFT JOIN rated USING(player_id,team_id)
$function$;

REVOKE ALL ON FUNCTION basketball_test.four_factors_dashboard_compute(int,date,date,text,text,text,text,text,text,int,text,int,int,int,int,int,int,int,int,int) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION basketball_test.four_factors_dashboard_compute(int,date,date,text,text,text,text,text,text,int,text,int,int,int,int,int,int,int,int,int) TO app_readonly;
