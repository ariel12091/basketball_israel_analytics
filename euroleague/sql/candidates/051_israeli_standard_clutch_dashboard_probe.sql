-- Rollback-only Israeli standard-clutch Team dashboard probe.
-- This is measurement input, not a deployable migration.

CREATE OR REPLACE FUNCTION basketball_test.get_team_dashboard_standard_clutch_probe_20260901(
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
  game_year INT, team_id INT, team_name TEXT,
  off_ppp NUMERIC, def_ppp NUMERIC, net_rtg NUMERIC,
  games_played INT, wins INT, losses INT, off_poss INT, def_poss INT,
  rank_net_rtg BIGINT, rank_off_ppp BIGINT, rank_def_ppp BIGINT,
  off_fga INT, off_layup_att INT, off_dunk_att INT, off_fg3_att INT,
  off_c3_att INT, off_c3_known_att INT,
  def_fga INT, def_layup_att INT, def_dunk_att INT, def_fg3_att INT,
  def_c3_att INT, def_c3_known_att INT,
  off_ts NUMERIC, off_efg NUMERIC, off_oreb NUMERIC, off_tov NUMERIC, off_ftr NUMERIC,
  off_pts INT, off_ts_poss INT, off_oreb_cnt INT, off_oreb_opps INT,
  off_tov_cnt INT, off_fta INT, off_fga_cnt INT, off_fgm_cnt INT, off_fg3m_cnt INT,
  def_ts NUMERIC, def_efg NUMERIC, def_oreb NUMERIC, def_tov NUMERIC, def_ftr NUMERIC,
  def_pts INT, def_ts_poss INT, def_oreb_cnt INT, def_oreb_opps INT,
  def_tov_cnt INT, def_fta INT, def_fga_cnt INT, def_fgm_cnt INT, def_fg3m_cnt INT,
  minutes NUMERIC
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
  SELECT fs.game_id,fs.team_id,fs.game_year,fs.opp_team_id,fs.has_won
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
    p.margin_status
  FROM games_base gb CROSS JOIN params p
  LEFT JOIN basketball_test.team_ppp_ratings_mv r
    ON r.game_year::int=gb.game_year AND r.team_id::int=gb.opp_team_id
    AND p.rank_side IN('top','bottom')
), games_filtered AS (
  SELECT gr.game_id,gr.team_id,gr.game_year,gr.has_won,gr.margin_status
  FROM games_ranked gr CROSS JOIN params p
  WHERE p.rank_side='all' OR p_opp_rank_n IS NULL
    OR (p.rank_side='top' AND gr.opp_rank<=p_opp_rank_n)
    OR (p.rank_side='bottom' AND gr.opp_rank>=(gr.max_rank-p_opp_rank_n+1))
), facts AS MATERIALIZED (
  SELECT d.id,d.game_id,d.team_id,gf.game_year,gf.has_won,d.type_lineup,
    d.team_score,d.final_end_poss,d.type,d.parameters_type,d.parameters_made,
    d.parameters_points,d.pct_ft,d.parent_action_id,d.lineup_hash,d.segment_id,
    d.event_elapsed_seconds,z.is_corner3,
    parent.type parent_type,parent.parameters_type parent_param
  FROM basketball_test.df_pts_poss_lineups_longer_mv d
  JOIN games_filtered gf ON gf.game_id=d.game_id AND gf.team_id=d.team_id
  LEFT JOIN basketball_test.shot_zones z ON z.game_id=d.game_id AND z.id=d.id
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
), agg AS (
  SELECT f.team_id,max(f.game_year) game_year,f.type_lineup,
    count(distinct f.game_id)::int games_count,
    sum(f.team_score)::int pts,count(*) filter(where f.final_end_poss)::int poss,
    count(*) filter(where f.type='shot')::int fga,
    count(*) filter(where f.type='shot' and f.parameters_points=2 and f.parameters_type='lay-up')::int layup_att,
    count(*) filter(where f.type='shot' and f.parameters_points=2 and f.parameters_type in('dunk','allyhoop'))::int dunk_att,
    count(*) filter(where f.type='shot' and f.parameters_points=3)::int fg3_att,
    count(*) filter(where f.type='shot' and f.parameters_points=3 and f.is_corner3)::int c3_att,
    count(*) filter(where f.type='shot' and f.parameters_points=3 and f.is_corner3 is not null)::int c3_known_att,
    (count(*) filter(where f.type='shot') + count(distinct f.parent_action_id) filter(
      where f.type='freeThrow' and f.parent_type='foul' and f.parent_param='personal'))::int ts_poss,
    count(*) filter(where f.type='rebound' and f.parameters_type='offensive')::int oreb,
    count(*) filter(where (f.type='shot' and f.parameters_made in('missed','blocked')) or
      (f.type='freeThrow' and f.parameters_made='missed' and f.pct_ft=1::numeric and f.parent_type='foul' and f.parent_param='personal'))::int oreb_opps,
    count(*) filter(where f.type='turnover')::int tov,
    count(*) filter(where f.type='freeThrow')::int fta,
    count(*) filter(where f.type='shot' and f.parameters_made='made')::int fgm,
    count(*) filter(where f.type='shot' and f.parameters_made='made' and f.parameters_points=3)::int fg3m
  FROM facts f GROUP BY f.team_id,f.type_lineup
), records AS (
  SELECT f.team_id,count(distinct f.game_id)::int games_played,
    count(distinct f.game_id) filter(where f.has_won)::int wins,
    count(distinct f.game_id) filter(where not f.has_won)::int losses
  FROM facts f GROUP BY f.team_id
), segments AS (
  SELECT f.team_id,f.game_id,f.lineup_hash,f.segment_id,
    greatest((array_agg(f.event_elapsed_seconds order by f.id desc))[1]-
             (array_agg(f.event_elapsed_seconds order by f.id))[1],0)::numeric seconds
  FROM facts f WHERE f.lineup_hash IS NOT NULL AND f.segment_id IS NOT NULL
    AND f.event_elapsed_seconds IS NOT NULL
  GROUP BY f.team_id,f.game_id,f.lineup_hash,f.segment_id
), durations AS (
  SELECT team_id,round(sum(seconds)/60.0,3)::numeric minutes FROM segments GROUP BY team_id
), wide AS (
  SELECT a.team_id,max(a.game_year)::int game_year,
    max(a.games_count)::int games_played,
    max(a.pts) filter(where a.type_lineup='offense')::int off_pts,
    max(a.poss) filter(where a.type_lineup='offense')::int off_poss,
    max(a.ts_poss) filter(where a.type_lineup='offense')::int off_ts_poss,
    max(a.oreb) filter(where a.type_lineup='offense')::int off_oreb_cnt,
    max(a.oreb_opps) filter(where a.type_lineup='offense')::int off_oreb_opps,
    max(a.tov) filter(where a.type_lineup='offense')::int off_tov_cnt,
    max(a.fta) filter(where a.type_lineup='offense')::int off_fta,
    max(a.fga) filter(where a.type_lineup='offense')::int off_fga_cnt,
    max(a.fgm) filter(where a.type_lineup='offense')::int off_fgm_cnt,
    max(a.fg3m) filter(where a.type_lineup='offense')::int off_fg3m_cnt,
    max(a.layup_att) filter(where a.type_lineup='offense')::int off_layup_att,
    max(a.dunk_att) filter(where a.type_lineup='offense')::int off_dunk_att,
    max(a.fg3_att) filter(where a.type_lineup='offense')::int off_fg3_att,
    max(a.c3_att) filter(where a.type_lineup='offense')::int off_c3_att,
    max(a.c3_known_att) filter(where a.type_lineup='offense')::int off_c3_known_att,
    max(a.pts) filter(where a.type_lineup='defense')::int def_pts,
    max(a.poss) filter(where a.type_lineup='defense')::int def_poss,
    max(a.ts_poss) filter(where a.type_lineup='defense')::int def_ts_poss,
    max(a.oreb) filter(where a.type_lineup='defense')::int def_oreb_cnt,
    max(a.oreb_opps) filter(where a.type_lineup='defense')::int def_oreb_opps,
    max(a.tov) filter(where a.type_lineup='defense')::int def_tov_cnt,
    max(a.fta) filter(where a.type_lineup='defense')::int def_fta,
    max(a.fga) filter(where a.type_lineup='defense')::int def_fga_cnt,
    max(a.fgm) filter(where a.type_lineup='defense')::int def_fgm_cnt,
    max(a.fg3m) filter(where a.type_lineup='defense')::int def_fg3m_cnt,
    max(a.layup_att) filter(where a.type_lineup='defense')::int def_layup_att,
    max(a.dunk_att) filter(where a.type_lineup='defense')::int def_dunk_att,
    max(a.fg3_att) filter(where a.type_lineup='defense')::int def_fg3_att,
    max(a.c3_att) filter(where a.type_lineup='defense')::int def_c3_att,
    max(a.c3_known_att) filter(where a.type_lineup='defense')::int def_c3_known_att
  FROM agg a GROUP BY a.team_id
), named AS (
  SELECT w.*,min(fr.team_name)::text team_name,r.wins,r.losses,d.minutes,
    round(100.0*w.off_pts/nullif(w.off_poss,0),1) off_rating,
    round(100.0*w.def_pts/nullif(w.def_poss,0),1) def_rating,
    round(100.0*w.off_pts/nullif(w.off_poss,0)-100.0*w.def_pts/nullif(w.def_poss,0),1) net_rating
  FROM wide w JOIN records r USING(team_id) LEFT JOIN durations d USING(team_id)
  JOIN basketball_test.full_rosters fr ON fr.game_year=p_game_year AND fr.team_id=w.team_id
  GROUP BY w.team_id,w.game_year,w.games_played,w.off_pts,w.off_poss,w.off_ts_poss,w.off_oreb_cnt,w.off_oreb_opps,w.off_tov_cnt,w.off_fta,w.off_fga_cnt,w.off_fgm_cnt,w.off_fg3m_cnt,w.off_layup_att,w.off_dunk_att,w.off_fg3_att,w.off_c3_att,w.off_c3_known_att,w.def_pts,w.def_poss,w.def_ts_poss,w.def_oreb_cnt,w.def_oreb_opps,w.def_tov_cnt,w.def_fta,w.def_fga_cnt,w.def_fgm_cnt,w.def_fg3m_cnt,w.def_layup_att,w.def_dunk_att,w.def_fg3_att,w.def_c3_att,w.def_c3_known_att,r.wins,r.losses,d.minutes
)
SELECT n.game_year,n.team_id,n.team_name,n.off_rating,n.def_rating,n.net_rating,
  n.games_played,n.wins,n.losses,n.off_poss,n.def_poss,
  dense_rank() over(order by n.net_rating desc nulls last),
  dense_rank() over(order by n.off_rating desc nulls last),
  dense_rank() over(order by n.def_rating asc nulls last),
  n.off_fga_cnt,n.off_layup_att,n.off_dunk_att,n.off_fg3_att,n.off_c3_att,n.off_c3_known_att,
  n.def_fga_cnt,n.def_layup_att,n.def_dunk_att,n.def_fg3_att,n.def_c3_att,n.def_c3_known_att,
  round(100.0*n.off_pts/nullif(2*n.off_ts_poss,0),1),
  round(100.0*(n.off_fgm_cnt+0.5*n.off_fg3m_cnt)/nullif(n.off_fga_cnt,0),1),
  round(100.0*n.off_oreb_cnt/nullif(n.off_oreb_opps,0),1),
  round(100.0*n.off_tov_cnt/nullif(n.off_poss,0),1),
  round(100.0*n.off_fta/nullif(n.off_fga_cnt,0),1),
  n.off_pts,n.off_ts_poss,n.off_oreb_cnt,n.off_oreb_opps,n.off_tov_cnt,n.off_fta,n.off_fga_cnt,n.off_fgm_cnt,n.off_fg3m_cnt,
  round(100.0*n.def_pts/nullif(2*n.def_ts_poss,0),1),
  round(100.0*(n.def_fgm_cnt+0.5*n.def_fg3m_cnt)/nullif(n.def_fga_cnt,0),1),
  round(100.0*n.def_oreb_cnt/nullif(n.def_oreb_opps,0),1),
  round(100.0*n.def_tov_cnt/nullif(n.def_poss,0),1),
  round(100.0*n.def_fta/nullif(n.def_fga_cnt,0),1),
  n.def_pts,n.def_ts_poss,n.def_oreb_cnt,n.def_oreb_opps,n.def_tov_cnt,n.def_fta,n.def_fga_cnt,n.def_fgm_cnt,n.def_fg3m_cnt,n.minutes
FROM named n ORDER BY n.net_rating DESC NULLS LAST
$function$;
