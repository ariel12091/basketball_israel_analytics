-- Additive Israeli standard-clutch Four Factors + Minutes reader.
-- Exact measured body promoted after rollback and fresh-backend gates.

BEGIN;

CREATE OR REPLACE FUNCTION basketball_test.get_team_four_factors_dashboard_dynamic(
    p_game_year INT,
    p_start_date DATE DEFAULT NULL, p_end_date DATE DEFAULT NULL,
    p_game_type_csv TEXT DEFAULT NULL, p_opp_team_ids_csv TEXT DEFAULT NULL,
    p_home_away TEXT DEFAULT 'all', p_outcome TEXT DEFAULT 'all',
    p_opp_rank_side TEXT DEFAULT 'all', p_opp_rank_n INT DEFAULT NULL,
    p_opp_rank_metric TEXT DEFAULT 'net',
    p_max_margin INT DEFAULT 5, p_margin_status TEXT DEFAULT 'all',
    p_max_time_remaining INT DEFAULT 300, p_ot_margin_filter BOOLEAN DEFAULT FALSE,
    p_min_gn INT DEFAULT NULL, p_max_gn INT DEFAULT NULL,
    p_last_n_games INT DEFAULT NULL,
    p_num_starters_off INT DEFAULT NULL, p_num_starters_def INT DEFAULT NULL,
    p_num_starters_off_min INT DEFAULT NULL, p_num_starters_off_max INT DEFAULT NULL,
    p_num_starters_def_min INT DEFAULT NULL, p_num_starters_def_max INT DEFAULT NULL
)
RETURNS TABLE (
    team_id INT, game_year INT, team_name TEXT,
    off_ts NUMERIC, off_efg NUMERIC, off_oreb NUMERIC, off_tov NUMERIC,
    off_ftr NUMERIC, off_ppp NUMERIC, off_poss INT, off_pts INT,
    off_ts_poss INT, off_oreb_cnt INT, off_oreb_opps INT, off_tov_cnt INT,
    off_fta INT, off_fga_cnt INT, off_fgm_cnt INT, off_fg3m_cnt INT,
    def_ts NUMERIC, def_efg NUMERIC, def_oreb NUMERIC, def_tov NUMERIC,
    def_ftr NUMERIC, def_ppp NUMERIC, def_poss INT, def_pts INT,
    def_ts_poss INT, def_oreb_cnt INT, def_oreb_opps INT, def_tov_cnt INT,
    def_fta INT, def_fga_cnt INT, def_fgm_cnt INT, def_fg3m_cnt INT,
    net_rtg NUMERIC, minutes NUMERIC
)
LANGUAGE sql STABLE
SET search_path = pg_catalog, basketball_test, public
SET plan_cache_mode = force_custom_plan
AS $function$
WITH params AS (
  SELECT
    CASE WHEN p_game_type_csv IS NULL OR btrim(p_game_type_csv)='' THEN NULL::int4[]
      ELSE ARRAY(SELECT DISTINCT x::int4 FROM unnest(string_to_array(regexp_replace(p_game_type_csv,'\s+','','g'),',')) x WHERE x<>'' ORDER BY 1) END game_types,
    CASE WHEN p_opp_team_ids_csv IS NULL OR btrim(p_opp_team_ids_csv)='' THEN NULL::int4[]
      ELSE ARRAY(SELECT DISTINCT x::int4 FROM unnest(string_to_array(regexp_replace(p_opp_team_ids_csv,'\s+','','g'),',')) x WHERE x<>'' ORDER BY 1) END opp_ids,
    coalesce(nullif(btrim(p_home_away),''),'all') home_away,
    coalesce(nullif(btrim(p_outcome),''),'all') outcome,
    coalesce(nullif(btrim(p_opp_rank_side),''),'all') rank_side,
    coalesce(nullif(btrim(p_opp_rank_metric),''),'net') rank_metric,
    coalesce(nullif(btrim(p_margin_status),''),'all') margin_status
), schedule_ranked AS (
  SELECT fs.game_id,fs.team_id,fs.game_year,
    row_number() over(partition by fs.team_id,fs.game_year order by fs.game_date desc nulls last,fs.game_id desc) rn_recent
  FROM basketball_test.final_schedule_mv fs WHERE fs.game_year=p_game_year
), games_base AS (
  SELECT fs.game_id,fs.team_id,fs.game_year,fs.opp_team_id
  FROM basketball_test.final_schedule_mv fs
  JOIN schedule_ranked sr USING(game_id,team_id,game_year)
  CROSS JOIN params p
  WHERE fs.game_year=p_game_year
    AND (p_start_date IS NULL OR fs.game_date>=p_start_date)
    AND (p_end_date IS NULL OR fs.game_date<=p_end_date)
    AND (p.game_types IS NULL OR fs.game_type=ANY(p.game_types))
    AND (p.opp_ids IS NULL OR fs.opp_team_id=ANY(p.opp_ids))
    AND (p.home_away='all' OR (p.home_away='home' AND fs.is_home) OR (p.home_away='away' AND NOT fs.is_home))
    AND (p.outcome='all' OR (p.outcome='win' AND fs.has_won) OR (p.outcome='loss' AND NOT fs.has_won))
    AND (p_min_gn IS NULL OR fs.gn>=p_min_gn)
    AND (p_max_gn IS NULL OR fs.gn<=p_max_gn)
    AND (p_last_n_games IS NULL OR sr.rn_recent<=p_last_n_games)
), games_ranked AS (
  SELECT gb.*,
    CASE WHEN p.rank_side IN('top','bottom') THEN CASE p.rank_metric
      WHEN 'off' THEN r.rank_off_ppp WHEN 'def' THEN r.rank_def_ppp ELSE r.rank_net_rtg END END opp_rank,
    CASE WHEN p.rank_side='bottom' THEN max(CASE p.rank_metric
      WHEN 'off' THEN r.rank_off_ppp WHEN 'def' THEN r.rank_def_ppp ELSE r.rank_net_rtg END)
      over(partition by gb.game_year) END max_rank,
    p.margin_status,p.rank_side
  FROM games_base gb CROSS JOIN params p
  LEFT JOIN basketball_test.team_ppp_ratings_mv r
    ON r.game_year::int=gb.game_year AND r.team_id::int=gb.opp_team_id
    AND p.rank_side IN('top','bottom')
), games_filtered AS (
  SELECT gr.game_id,gr.team_id,gr.game_year,gr.margin_status
  FROM games_ranked gr
  WHERE gr.rank_side='all' OR p_opp_rank_n IS NULL
    OR (gr.rank_side='top' AND gr.opp_rank<=p_opp_rank_n)
    OR (gr.rank_side='bottom' AND gr.opp_rank>=(gr.max_rank-p_opp_rank_n+1))
), facts AS MATERIALIZED (
  SELECT d.id,d.game_id,d.team_id,gf.game_year,d.team_score,d.type,
    d.parameters_type,d.parameters_made,d.parameters_points,d.pct_ft,
    d.parent_action_id,d.type_lineup,d.final_end_poss,d.lineup_hash,d.segment_id,
    d.event_elapsed_seconds,parent.type parent_type,parent.parameters_type parent_param
  FROM basketball_test.df_pts_poss_lineups_longer_mv d
  JOIN games_filtered gf ON gf.game_id=d.game_id AND gf.team_id=d.team_id
  LEFT JOIN LATERAL (
    SELECT p0.type,p0.parameters_type
    FROM basketball_test.df_pts_poss_lineups_longer_mv p0
    WHERE p0.game_id=d.game_id AND p0.id=d.parent_action_id AND p0.type='foul'
    LIMIT 1
  ) parent ON true
  WHERE (p_max_margin IS NULL OR abs(CASE WHEN d.type_lineup='offense'
      THEN (d.own_team_score-coalesce(d.team_score,0))-d.opp_team_score
      ELSE d.own_team_score-(d.opp_team_score-coalesce(d.team_score,0)) END)<=p_max_margin
      OR (d.quarter>4 AND NOT coalesce(p_ot_margin_filter,false)))
    AND (gf.margin_status='all'
      OR (gf.margin_status='leading' AND CASE WHEN d.type_lineup='offense'
        THEN (d.own_team_score-coalesce(d.team_score,0))>d.opp_team_score
        ELSE d.own_team_score>(d.opp_team_score-coalesce(d.team_score,0)) END)
      OR (gf.margin_status='trailing' AND CASE WHEN d.type_lineup='offense'
        THEN (d.own_team_score-coalesce(d.team_score,0))<d.opp_team_score
        ELSE d.own_team_score<(d.opp_team_score-coalesce(d.team_score,0)) END)
      OR (gf.margin_status='tied' AND CASE WHEN d.type_lineup='offense'
        THEN (d.own_team_score-coalesce(d.team_score,0))=d.opp_team_score
        ELSE d.own_team_score=(d.opp_team_score-coalesce(d.team_score,0)) END)
      OR (d.quarter>4 AND NOT coalesce(p_ot_margin_filter,false)))
    AND (p_max_time_remaining IS NULL OR d.end_game_seconds_remaining<=p_max_time_remaining OR d.quarter>4)
    AND (coalesce(p_num_starters_off_min,p_num_starters_off) IS NULL OR d.own_starters>=coalesce(p_num_starters_off_min,p_num_starters_off))
    AND (coalesce(p_num_starters_off_max,p_num_starters_off) IS NULL OR d.own_starters<=coalesce(p_num_starters_off_max,p_num_starters_off))
    AND (coalesce(p_num_starters_def_min,p_num_starters_def) IS NULL OR d.opp_starters>=coalesce(p_num_starters_def_min,p_num_starters_def))
    AND (coalesce(p_num_starters_def_max,p_num_starters_def) IS NULL OR d.opp_starters<=coalesce(p_num_starters_def_max,p_num_starters_def))
), team_agg AS (
  SELECT f.team_id,max(f.game_year)::int game_year,f.type_lineup,
    sum(f.team_score)::int total_points,
    count(*) filter(where f.final_end_poss)::int total_poss,
    (count(*) filter(where f.type='shot') + count(distinct f.parent_action_id) filter(
      where f.type='freeThrow' and f.parent_type='foul' and f.parent_param='personal'))::int ts_poss_count,
    count(*) filter(where f.type='rebound' and f.parameters_type='offensive')::int oreb_count,
    count(*) filter(where (f.type='shot' and f.parameters_made in('missed','blocked')) or
      (f.type='freeThrow' and f.parameters_made='missed' and f.pct_ft=1::numeric
       and f.parent_type='foul' and f.parent_param='personal'))::int oreb_opportunities,
    count(*) filter(where f.type='turnover')::int tov_count,
    count(*) filter(where f.type='freeThrow')::int total_ft_attempts,
    count(*) filter(where f.type='shot')::int total_fga,
    count(*) filter(where f.type='shot' and f.parameters_made='made')::int total_fgm,
    count(*) filter(where f.type='shot' and f.parameters_made='made' and f.parameters_points=3)::int total_fg3_made
  FROM facts f GROUP BY f.team_id,f.type_lineup
), segments AS (
  SELECT f.team_id,f.game_id,f.lineup_hash,f.segment_id,
    greatest((array_agg(f.event_elapsed_seconds order by f.id desc))[1]-
             (array_agg(f.event_elapsed_seconds order by f.id))[1],0)::numeric seconds
  FROM facts f WHERE f.lineup_hash IS NOT NULL AND f.segment_id IS NOT NULL
    AND f.event_elapsed_seconds IS NOT NULL
  GROUP BY f.team_id,f.game_id,f.lineup_hash,f.segment_id
), durations AS (
  SELECT team_id,round(sum(seconds)/60.0,3)::numeric minutes
  FROM segments GROUP BY team_id
), wide AS (
  SELECT a.team_id,max(a.game_year)::int game_year,
    max(a.total_points) filter(where a.type_lineup='offense')::int off_pts,
    max(a.total_poss) filter(where a.type_lineup='offense')::int off_poss,
    max(a.ts_poss_count) filter(where a.type_lineup='offense')::int off_ts_poss,
    max(a.oreb_count) filter(where a.type_lineup='offense')::int off_oreb_cnt,
    max(a.oreb_opportunities) filter(where a.type_lineup='offense')::int off_oreb_opps,
    max(a.tov_count) filter(where a.type_lineup='offense')::int off_tov_cnt,
    max(a.total_ft_attempts) filter(where a.type_lineup='offense')::int off_fta,
    max(a.total_fga) filter(where a.type_lineup='offense')::int off_fga_cnt,
    max(a.total_fgm) filter(where a.type_lineup='offense')::int off_fgm_cnt,
    max(a.total_fg3_made) filter(where a.type_lineup='offense')::int off_fg3m_cnt,
    max(a.total_points) filter(where a.type_lineup='defense')::int def_pts,
    max(a.total_poss) filter(where a.type_lineup='defense')::int def_poss,
    max(a.ts_poss_count) filter(where a.type_lineup='defense')::int def_ts_poss,
    max(a.oreb_count) filter(where a.type_lineup='defense')::int def_oreb_cnt,
    max(a.oreb_opportunities) filter(where a.type_lineup='defense')::int def_oreb_opps,
    max(a.tov_count) filter(where a.type_lineup='defense')::int def_tov_cnt,
    max(a.total_ft_attempts) filter(where a.type_lineup='defense')::int def_fta,
    max(a.total_fga) filter(where a.type_lineup='defense')::int def_fga_cnt,
    max(a.total_fgm) filter(where a.type_lineup='defense')::int def_fgm_cnt,
    max(a.total_fg3_made) filter(where a.type_lineup='defense')::int def_fg3m_cnt
  FROM team_agg a GROUP BY a.team_id
), named AS (
  SELECT w.*,min(fr.team_name)::text team_name,d.minutes
  FROM wide w JOIN basketball_test.full_rosters fr
    ON fr.game_year=p_game_year AND fr.team_id=w.team_id
  LEFT JOIN durations d ON d.team_id=w.team_id
  GROUP BY w.team_id,w.game_year,w.off_pts,w.off_poss,w.off_ts_poss,w.off_oreb_cnt,
    w.off_oreb_opps,w.off_tov_cnt,w.off_fta,w.off_fga_cnt,w.off_fgm_cnt,w.off_fg3m_cnt,
    w.def_pts,w.def_poss,w.def_ts_poss,w.def_oreb_cnt,w.def_oreb_opps,w.def_tov_cnt,
    w.def_fta,w.def_fga_cnt,w.def_fgm_cnt,w.def_fg3m_cnt,d.minutes
)
SELECT n.team_id,n.game_year,n.team_name,
  round(100.0*n.off_pts/nullif(2*n.off_ts_poss,0),1),
  round(100.0*(n.off_fgm_cnt+0.5*n.off_fg3m_cnt)/nullif(n.off_fga_cnt,0),1),
  round(100.0*n.off_oreb_cnt/nullif(n.off_oreb_opps,0),1),
  round(100.0*n.off_tov_cnt/nullif(n.off_poss,0),1),
  round(100.0*n.off_fta/nullif(n.off_fga_cnt,0),1),
  round(100.0*n.off_pts/nullif(n.off_poss,0),1),n.off_poss,n.off_pts,
  n.off_ts_poss,n.off_oreb_cnt,n.off_oreb_opps,n.off_tov_cnt,n.off_fta,
  n.off_fga_cnt,n.off_fgm_cnt,n.off_fg3m_cnt,
  round(100.0*n.def_pts/nullif(2*n.def_ts_poss,0),1),
  round(100.0*(n.def_fgm_cnt+0.5*n.def_fg3m_cnt)/nullif(n.def_fga_cnt,0),1),
  round(100.0*n.def_oreb_cnt/nullif(n.def_oreb_opps,0),1),
  round(100.0*n.def_tov_cnt/nullif(n.def_poss,0),1),
  round(100.0*n.def_fta/nullif(n.def_fga_cnt,0),1),
  round(100.0*n.def_pts/nullif(n.def_poss,0),1),n.def_poss,n.def_pts,
  n.def_ts_poss,n.def_oreb_cnt,n.def_oreb_opps,n.def_tov_cnt,n.def_fta,
  n.def_fga_cnt,n.def_fgm_cnt,n.def_fg3m_cnt,
  round(100.0*n.off_pts/nullif(n.off_poss,0)-100.0*n.def_pts/nullif(n.def_poss,0),1),
  n.minutes
FROM named n
$function$;

REVOKE ALL ON FUNCTION basketball_test.get_team_four_factors_dashboard_dynamic(integer,date,date,text,text,text,text,text,integer,text,integer,text,integer,boolean,integer,integer,integer,integer,integer,integer,integer,integer,integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION basketball_test.get_team_four_factors_dashboard_dynamic(integer,date,date,text,text,text,text,text,integer,text,integer,text,integer,boolean,integer,integer,integer,integer,integer,integer,integer,integer,integer) TO app_readonly;

COMMIT;
