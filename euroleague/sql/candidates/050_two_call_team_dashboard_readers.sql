-- Migration 050: combined EuroLeague Team dashboard readers for the
-- EUROLEAGUE SHADOW SCHEMA.
--
-- Additive only. Standard clutch returns Ratings, Four Factors, and Minutes in
-- one call. Per-game and custom clutch combine Ratings/Four Factors while
-- retaining the established Minutes reader as a second call.

BEGIN;
SET LOCAL search_path TO euroleague, public;

CREATE OR REPLACE FUNCTION euroleague.get_team_metrics_pergame(
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
    p_num_starters_def_max INTEGER DEFAULT NULL
)
RETURNS TABLE (
    game_year INT, team_id BIGINT, team_name TEXT,
    off_ppp NUMERIC, def_ppp NUMERIC, net_rtg NUMERIC,
    games_played BIGINT, wins BIGINT, losses BIGINT,
    off_poss BIGINT, def_poss BIGINT,
    rank_net_rtg BIGINT, rank_off_ppp BIGINT, rank_def_ppp BIGINT,
    off_efg NUMERIC, def_efg NUMERIC, off_ts NUMERIC, def_ts NUMERIC,
    off_oreb NUMERIC, def_oreb NUMERIC, off_tov NUMERIC, def_tov NUMERIC,
    off_ftr NUMERIC, def_ftr NUMERIC
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, euroleague, public
SET plan_cache_mode = force_custom_plan
AS $function$
WITH normalized AS (
  SELECT coalesce(nullif(btrim(p_competition),''),'E') competition,
    CASE WHEN nullif(btrim(p_team_ids_csv),'') IS NULL THEN NULL::bigint[]
      ELSE string_to_array(regexp_replace(p_team_ids_csv,'\s+','','g'),',')::bigint[] END team_ids,
    CASE WHEN nullif(btrim(p_phase_csv),'') IS NULL THEN NULL::text[]
      ELSE string_to_array(p_phase_csv,',') END phases,
    CASE WHEN nullif(btrim(p_opp_ids_csv),'') IS NULL THEN NULL::bigint[]
      ELSE string_to_array(regexp_replace(p_opp_ids_csv,'\s+','','g'),',')::bigint[] END opp_ids,
    coalesce(nullif(btrim(p_home_away),''),'all') home_away,
    coalesce(nullif(btrim(p_outcome),''),'all') outcome,
    nullif(btrim(p_opp_rank_side),'') rank_side,
    coalesce(nullif(btrim(p_opp_rank_metric),''),'net') rank_metric
), schedule_ranked AS (
  SELECT fs.*, row_number() OVER (
    PARTITION BY fs.team_id ORDER BY fs.game_date DESC, fs.game_id DESC
  ) team_game_rank
  FROM euroleague.final_schedule_mv fs CROSS JOIN normalized n
  WHERE fs.competition=n.competition AND fs.game_year=p_game_year
), opponent_ranks AS (
  SELECT r.team_id,r.off_rank,r.def_rank,r.net_rank,count(*) OVER() team_count
  FROM euroleague.team_ppp_ratings_mv r CROSS JOIN normalized n
  WHERE r.competition=n.competition AND r.game_year=p_game_year
), games_filtered AS MATERIALIZED (
  SELECT sr.game_id,sr.team_id,sr.team_name,sr.has_won
  FROM schedule_ranked sr CROSS JOIN normalized n
  LEFT JOIN opponent_ranks r ON r.team_id=sr.opp_team_id
  WHERE (p_start_date IS NULL OR sr.game_date>=p_start_date)
    AND (p_end_date IS NULL OR sr.game_date<=p_end_date)
    AND (n.team_ids IS NULL OR sr.team_id=ANY(n.team_ids))
    AND (n.phases IS NULL OR sr.phase=ANY(n.phases))
    AND (n.opp_ids IS NULL OR sr.opp_team_id=ANY(n.opp_ids))
    AND (n.home_away='all' OR (n.home_away='home' AND sr.is_home)
      OR (n.home_away='away' AND NOT sr.is_home))
    AND (n.outcome='all' OR (n.outcome='win' AND sr.has_won)
      OR (n.outcome='loss' AND NOT sr.has_won))
    AND (n.rank_side IS NULL OR p_opp_rank_n IS NULL
      OR (n.rank_side='top' AND CASE n.rank_metric WHEN 'off' THEN r.off_rank
        WHEN 'def' THEN r.def_rank ELSE r.net_rank END<=p_opp_rank_n)
      OR (n.rank_side='bottom' AND CASE n.rank_metric WHEN 'off' THEN r.off_rank
        WHEN 'def' THEN r.def_rank ELSE r.net_rank END>r.team_count-p_opp_rank_n))
    AND (p_min_gn IS NULL OR sr.round_number>=p_min_gn)
    AND (p_max_gn IS NULL OR sr.round_number<=p_max_gn)
    AND (p_last_n_games IS NULL OR sr.team_game_rank<=p_last_n_games)
), metric_games AS (
  SELECT gf.game_id,gf.team_id,max(gf.team_name) team_name,bool_or(gf.has_won) has_won,
    sum(f.off_pts) off_pts,sum(f.off_poss) off_poss,
    sum(f.off_ts_poss) off_ts_poss,sum(f.off_oreb) off_oreb,
    sum(f.off_oreb_opp) off_oreb_opp,sum(f.off_tov) off_tov,
    sum(f.off_fta) off_fta,sum(f.off_fga) off_fga,
    sum(f.off_fgm) off_fgm,sum(f.off_fg3m) off_fg3m,
    sum(f.def_pts) def_pts,sum(f.def_poss) def_poss,
    sum(f.def_ts_poss) def_ts_poss,sum(f.def_oreb) def_oreb,
    sum(f.def_oreb_opp) def_oreb_opp,sum(f.def_tov) def_tov,
    sum(f.def_fta) def_fta,sum(f.def_fga) def_fga,
    sum(f.def_fgm) def_fgm,sum(f.def_fg3m) def_fg3m
  FROM euroleague.team_four_factors_by_game f
  JOIN games_filtered gf USING(game_id,team_id)
  WHERE f.game_year=p_game_year
    AND (p_num_starters_off_min IS NULL OR f.own_starters>=p_num_starters_off_min)
    AND (p_num_starters_off_max IS NULL OR f.own_starters<=p_num_starters_off_max)
    AND (p_num_starters_def_min IS NULL OR f.opp_starters>=p_num_starters_def_min)
    AND (p_num_starters_def_max IS NULL OR f.opp_starters<=p_num_starters_def_max)
  GROUP BY gf.game_id,gf.team_id
), metric_agg AS (
  SELECT team_id,max(team_name) team_name,count(DISTINCT game_id) games_played,
    count(DISTINCT game_id) FILTER(WHERE has_won) wins,
    count(DISTINCT game_id) FILTER(WHERE NOT has_won) losses,
    sum(off_pts) off_pts,sum(off_poss) off_poss,sum(off_ts_poss) off_ts_poss,
    sum(off_oreb) off_oreb,sum(off_oreb_opp) off_oreb_opp,sum(off_tov) off_tov,
    sum(off_fta) off_fta,sum(off_fga) off_fga,sum(off_fgm) off_fgm,sum(off_fg3m) off_fg3m,
    sum(def_pts) def_pts,sum(def_poss) def_poss,sum(def_ts_poss) def_ts_poss,
    sum(def_oreb) def_oreb,sum(def_oreb_opp) def_oreb_opp,sum(def_tov) def_tov,
    sum(def_fta) def_fta,sum(def_fga) def_fga,sum(def_fgm) def_fgm,sum(def_fg3m) def_fg3m
  FROM metric_games GROUP BY team_id
), rated AS (
  SELECT a.*,round(100.0*a.off_pts/nullif(a.off_poss,0),1) off_rating,
    round(100.0*a.def_pts/nullif(a.def_poss,0),1) def_rating
  FROM metric_agg a
)
SELECT p_game_year,r.team_id,r.team_name,r.off_rating,r.def_rating,
  round(100.0*r.off_pts/nullif(r.off_poss,0)-100.0*r.def_pts/nullif(r.def_poss,0),1),
  r.games_played::bigint,r.wins::bigint,r.losses::bigint,
  r.off_poss::bigint,r.def_poss::bigint,
  dense_rank() OVER(ORDER BY r.off_rating-r.def_rating DESC),
  dense_rank() OVER(ORDER BY r.off_rating DESC),dense_rank() OVER(ORDER BY r.def_rating ASC),
  round(100.0*(r.off_fgm+0.5*r.off_fg3m)/nullif(r.off_fga,0),1),
  round(100.0*(r.def_fgm+0.5*r.def_fg3m)/nullif(r.def_fga,0),1),
  round(100.0*r.off_pts/nullif(2*r.off_ts_poss,0),1),
  round(100.0*r.def_pts/nullif(2*r.def_ts_poss,0),1),
  round(100.0*r.off_oreb/nullif(r.off_oreb_opp,0),1),
  round(100.0*r.def_oreb/nullif(r.def_oreb_opp,0),1),
  round(100.0*r.off_tov/nullif(r.off_poss,0),1),
  round(100.0*r.def_tov/nullif(r.def_poss,0),1),
  round(100.0*r.off_fta/nullif(r.off_fga,0),1),
  round(100.0*r.def_fta/nullif(r.def_fga,0),1)
FROM rated r
ORDER BY r.off_rating-r.def_rating DESC NULLS LAST
$function$;

CREATE OR REPLACE FUNCTION euroleague.get_team_dashboard_dynamic(
    p_competition TEXT, p_game_year INTEGER,
    p_start_date DATE DEFAULT NULL, p_end_date DATE DEFAULT NULL,
    p_team_ids_csv TEXT DEFAULT NULL, p_phase_csv TEXT DEFAULT NULL,
    p_opp_ids_csv TEXT DEFAULT NULL, p_home_away TEXT DEFAULT 'all',
    p_outcome TEXT DEFAULT 'all', p_opp_rank_side TEXT DEFAULT NULL,
    p_opp_rank_n INTEGER DEFAULT NULL, p_opp_rank_metric TEXT DEFAULT NULL,
    p_max_margin INTEGER DEFAULT NULL, p_margin_status TEXT DEFAULT NULL,
    p_max_time_remaining INTEGER DEFAULT NULL,
    p_ot_margin_filter BOOLEAN DEFAULT FALSE,
    p_min_gn INTEGER DEFAULT NULL, p_max_gn INTEGER DEFAULT NULL,
    p_last_n_games INTEGER DEFAULT NULL,
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
    rank_net_rtg BIGINT, rank_off_ppp BIGINT, rank_def_ppp BIGINT,
    off_efg NUMERIC, def_efg NUMERIC, off_ts NUMERIC, def_ts NUMERIC,
    off_oreb NUMERIC, def_oreb NUMERIC, off_tov NUMERIC, def_tov NUMERIC,
    off_ftr NUMERIC, def_ftr NUMERIC, minutes NUMERIC
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, euroleague, public
SET plan_cache_mode = force_custom_plan
AS $function$
WITH facts AS MATERIALIZED (
  SELECT * FROM euroleague.filtered_team_game_facts(
    p_competition,p_game_year,p_start_date,p_end_date,
    p_team_ids_csv,p_phase_csv,p_opp_ids_csv,p_home_away,p_outcome,
    p_opp_rank_side,p_opp_rank_n,p_opp_rank_metric,
    p_max_margin,p_margin_status,p_max_time_remaining,p_ot_margin_filter,
    p_min_gn,p_max_gn,p_last_n_games,
    p_num_starters_off_min,p_num_starters_off_max,
    p_num_starters_def_min,p_num_starters_def_max
  )
), agg AS (
  SELECT f.team_id,max(f.team_name) team_name,
    count(DISTINCT f.game_id) games_played,
    count(DISTINCT f.game_id) FILTER(WHERE f.has_won) wins,
    count(DISTINCT f.game_id) FILTER(WHERE NOT f.has_won) losses,
    sum(f.points) FILTER(WHERE f.type_lineup='offense') off_pts,
    sum(f.possessions) FILTER(WHERE f.type_lineup='offense') off_poss,
    sum(f.ts_possessions) FILTER(WHERE f.type_lineup='offense') off_ts_poss,
    sum(f.orebounds) FILTER(WHERE f.type_lineup='offense') off_oreb,
    sum(f.oreb_opportunities) FILTER(WHERE f.type_lineup='offense') off_oreb_opp,
    sum(f.turnovers) FILTER(WHERE f.type_lineup='offense') off_tov,
    sum(f.ft_attempts) FILTER(WHERE f.type_lineup='offense') off_fta,
    sum(f.fga) FILTER(WHERE f.type_lineup='offense') off_fga,
    sum(f.fgm) FILTER(WHERE f.type_lineup='offense') off_fgm,
    sum(f.fg3_made) FILTER(WHERE f.type_lineup='offense') off_fg3m,
    sum(f.points) FILTER(WHERE f.type_lineup='defense') def_pts,
    sum(f.possessions) FILTER(WHERE f.type_lineup='defense') def_poss,
    sum(f.ts_possessions) FILTER(WHERE f.type_lineup='defense') def_ts_poss,
    sum(f.orebounds) FILTER(WHERE f.type_lineup='defense') def_oreb,
    sum(f.oreb_opportunities) FILTER(WHERE f.type_lineup='defense') def_oreb_opp,
    sum(f.turnovers) FILTER(WHERE f.type_lineup='defense') def_tov,
    sum(f.ft_attempts) FILTER(WHERE f.type_lineup='defense') def_fta,
    sum(f.fga) FILTER(WHERE f.type_lineup='defense') def_fga,
    sum(f.fgm) FILTER(WHERE f.type_lineup='defense') def_fgm,
    sum(f.fg3_made) FILTER(WHERE f.type_lineup='defense') def_fg3m,
    sum(f.seconds) FILTER(WHERE f.type_lineup='offense') seconds
  FROM facts f GROUP BY f.team_id
), rated AS (
  SELECT a.*,round(100.0*a.off_pts/nullif(a.off_poss,0),1) off_rating,
    round(100.0*a.def_pts/nullif(a.def_poss,0),1) def_rating FROM agg a
)
SELECT p_game_year,r.team_id,r.team_name,r.off_rating,r.def_rating,
  round(100.0*r.off_pts/nullif(r.off_poss,0)-100.0*r.def_pts/nullif(r.def_poss,0),1),
  r.games_played::bigint,r.wins::bigint,r.losses::bigint,
  r.off_poss::bigint,r.def_poss::bigint,
  dense_rank() OVER(ORDER BY r.off_rating-r.def_rating DESC),
  dense_rank() OVER(ORDER BY r.off_rating DESC),dense_rank() OVER(ORDER BY r.def_rating ASC),
  round(100.0*(r.off_fgm+0.5*r.off_fg3m)/nullif(r.off_fga,0),1),
  round(100.0*(r.def_fgm+0.5*r.def_fg3m)/nullif(r.def_fga,0),1),
  round(100.0*r.off_pts/nullif(2*r.off_ts_poss,0),1),
  round(100.0*r.def_pts/nullif(2*r.def_ts_poss,0),1),
  round(100.0*r.off_oreb/nullif(r.off_oreb_opp,0),1),
  round(100.0*r.def_oreb/nullif(r.def_oreb_opp,0),1),
  round(100.0*r.off_tov/nullif(r.off_poss,0),1),
  round(100.0*r.def_tov/nullif(r.def_poss,0),1),
  round(100.0*r.off_fta/nullif(r.off_fga,0),1),
  round(100.0*r.def_fta/nullif(r.def_fga,0),1),round(r.seconds/60.0,3)::numeric
FROM rated r ORDER BY r.off_rating-r.def_rating DESC NULLS LAST
$function$;

CREATE OR REPLACE FUNCTION euroleague.get_team_metrics_direct(
    p_competition TEXT, p_game_year INTEGER,
    p_start_date DATE DEFAULT NULL, p_end_date DATE DEFAULT NULL,
    p_team_ids_csv TEXT DEFAULT NULL, p_phase_csv TEXT DEFAULT NULL,
    p_opp_ids_csv TEXT DEFAULT NULL, p_home_away TEXT DEFAULT 'all',
    p_outcome TEXT DEFAULT 'all', p_opp_rank_side TEXT DEFAULT NULL,
    p_opp_rank_n INTEGER DEFAULT NULL, p_opp_rank_metric TEXT DEFAULT NULL,
    p_max_margin INTEGER DEFAULT NULL, p_margin_status TEXT DEFAULT NULL,
    p_max_time_remaining INTEGER DEFAULT NULL,
    p_ot_margin_filter BOOLEAN DEFAULT FALSE,
    p_min_gn INTEGER DEFAULT NULL, p_max_gn INTEGER DEFAULT NULL,
    p_last_n_games INTEGER DEFAULT NULL,
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
    rank_net_rtg BIGINT, rank_off_ppp BIGINT, rank_def_ppp BIGINT,
    off_efg NUMERIC, def_efg NUMERIC, off_ts NUMERIC, def_ts NUMERIC,
    off_oreb NUMERIC, def_oreb NUMERIC, off_tov NUMERIC, def_tov NUMERIC,
    off_ftr NUMERIC, def_ftr NUMERIC
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, euroleague, public
SET plan_cache_mode = force_custom_plan
AS $function$
WITH normalized AS (
  SELECT coalesce(nullif(btrim(p_competition),''),'E') competition,
    CASE WHEN nullif(btrim(p_team_ids_csv),'') IS NULL THEN NULL::bigint[] ELSE string_to_array(regexp_replace(p_team_ids_csv,'\s+','','g'),',')::bigint[] END team_ids,
    CASE WHEN nullif(btrim(p_phase_csv),'') IS NULL THEN NULL::text[] ELSE string_to_array(p_phase_csv,',') END phases,
    CASE WHEN nullif(btrim(p_opp_ids_csv),'') IS NULL THEN NULL::bigint[] ELSE string_to_array(regexp_replace(p_opp_ids_csv,'\s+','','g'),',')::bigint[] END opp_ids,
    coalesce(nullif(btrim(p_home_away),''),'all') home_away,
    coalesce(nullif(btrim(p_outcome),''),'all') outcome,
    nullif(btrim(p_opp_rank_side),'') rank_side,
    coalesce(nullif(btrim(p_opp_rank_metric),''),'net') rank_metric,
    coalesce(nullif(btrim(p_margin_status),''),'all') margin_status
), schedule_ranked AS (
  SELECT fs.*,row_number() OVER(PARTITION BY fs.team_id ORDER BY fs.game_date DESC,fs.game_id DESC) team_game_rank
  FROM euroleague.final_schedule_mv fs CROSS JOIN normalized n
  WHERE fs.competition=n.competition AND fs.game_year=p_game_year
), opponent_ranks AS (
  SELECT r.team_id,r.off_rank,r.def_rank,r.net_rank,count(*) OVER() team_count
  FROM euroleague.team_ppp_ratings_mv r CROSS JOIN normalized n
  WHERE r.competition=n.competition AND r.game_year=p_game_year
), games_filtered AS MATERIALIZED (
  SELECT sr.game_id,sr.team_id,sr.team_name,sr.has_won
  FROM schedule_ranked sr CROSS JOIN normalized n LEFT JOIN opponent_ranks r ON r.team_id=sr.opp_team_id
  WHERE (p_start_date IS NULL OR sr.game_date>=p_start_date) AND (p_end_date IS NULL OR sr.game_date<=p_end_date)
    AND (n.team_ids IS NULL OR sr.team_id=ANY(n.team_ids)) AND (n.phases IS NULL OR sr.phase=ANY(n.phases))
    AND (n.opp_ids IS NULL OR sr.opp_team_id=ANY(n.opp_ids))
    AND (n.home_away='all' OR (n.home_away='home' AND sr.is_home) OR (n.home_away='away' AND NOT sr.is_home))
    AND (n.outcome='all' OR (n.outcome='win' AND sr.has_won) OR (n.outcome='loss' AND NOT sr.has_won))
    AND (n.rank_side IS NULL OR p_opp_rank_n IS NULL
      OR (n.rank_side='top' AND CASE n.rank_metric WHEN 'off' THEN r.off_rank WHEN 'def' THEN r.def_rank ELSE r.net_rank END<=p_opp_rank_n)
      OR (n.rank_side='bottom' AND CASE n.rank_metric WHEN 'off' THEN r.off_rank WHEN 'def' THEN r.def_rank ELSE r.net_rank END>r.team_count-p_opp_rank_n))
    AND (p_min_gn IS NULL OR sr.round_number>=p_min_gn) AND (p_max_gn IS NULL OR sr.round_number<=p_max_gn)
    AND (p_last_n_games IS NULL OR sr.team_game_rank<=p_last_n_games)
), acts AS (
  SELECT a.game_id,a.team_id,a.type_lineup,a.possession_flag,a.points,a.ts_possessions,
    a.orebounds,a.oreb_opportunities,a.turnovers,a.ft_attempts,
    a.fga,a.fgm,a.fg3_made
  FROM euroleague.player_stats_actions_by_game a
  JOIN games_filtered gf USING(game_id,team_id) CROSS JOIN normalized n
  WHERE (p_max_time_remaining IS NULL OR a.regulation_seconds_remaining<=p_max_time_remaining OR a.is_overtime)
    AND (p_max_margin IS NULL OR a.pre_abs_margin<=p_max_margin OR (a.is_overtime AND NOT coalesce(p_ot_margin_filter,false)))
    AND (n.margin_status='all' OR (n.margin_status='leading' AND a.pre_status>0)
      OR (n.margin_status='trailing' AND a.pre_status<0) OR (n.margin_status='tied' AND a.pre_status=0)
      OR (a.is_overtime AND NOT coalesce(p_ot_margin_filter,false)))
    AND (p_num_starters_off_min IS NULL OR a.own_starters>=p_num_starters_off_min)
    AND (p_num_starters_off_max IS NULL OR a.own_starters<=p_num_starters_off_max)
    AND (p_num_starters_def_min IS NULL OR a.opp_starters>=p_num_starters_def_min)
    AND (p_num_starters_def_max IS NULL OR a.opp_starters<=p_num_starters_def_max)
), metric_games AS (
  SELECT gf.game_id,gf.team_id,max(gf.team_name) team_name,bool_or(gf.has_won) has_won,
    sum(a.points) FILTER(WHERE a.type_lineup='offense') off_pts,
    sum(a.possession_flag) FILTER(WHERE a.type_lineup='offense') off_poss,
    sum(a.ts_possessions) FILTER(WHERE a.type_lineup='offense') off_ts_poss,
    sum(a.orebounds) FILTER(WHERE a.type_lineup='offense') off_oreb,
    sum(a.oreb_opportunities) FILTER(WHERE a.type_lineup='offense') off_oreb_opp,
    sum(a.turnovers) FILTER(WHERE a.type_lineup='offense') off_tov,
    sum(a.ft_attempts) FILTER(WHERE a.type_lineup='offense') off_fta,
    sum(a.fga) FILTER(WHERE a.type_lineup='offense') off_fga,
    sum(a.fgm) FILTER(WHERE a.type_lineup='offense') off_fgm,
    sum(a.fg3_made) FILTER(WHERE a.type_lineup='offense') off_fg3m,
    sum(a.points) FILTER(WHERE a.type_lineup='defense') def_pts,
    sum(a.possession_flag) FILTER(WHERE a.type_lineup='defense') def_poss,
    sum(a.ts_possessions) FILTER(WHERE a.type_lineup='defense') def_ts_poss,
    sum(a.orebounds) FILTER(WHERE a.type_lineup='defense') def_oreb,
    sum(a.oreb_opportunities) FILTER(WHERE a.type_lineup='defense') def_oreb_opp,
    sum(a.turnovers) FILTER(WHERE a.type_lineup='defense') def_tov,
    sum(a.ft_attempts) FILTER(WHERE a.type_lineup='defense') def_fta,
    sum(a.fga) FILTER(WHERE a.type_lineup='defense') def_fga,
    sum(a.fgm) FILTER(WHERE a.type_lineup='defense') def_fgm,
    sum(a.fg3_made) FILTER(WHERE a.type_lineup='defense') def_fg3m
  FROM acts a JOIN games_filtered gf USING(game_id,team_id)
  WHERE a.type_lineup IS NOT NULL GROUP BY gf.game_id,gf.team_id
), metric_agg AS (
  SELECT team_id,max(team_name) team_name,count(DISTINCT game_id) games_played,
    count(DISTINCT game_id) FILTER(WHERE has_won) wins,count(DISTINCT game_id) FILTER(WHERE NOT has_won) losses,
    sum(off_pts) off_pts,sum(off_poss) off_poss,sum(off_ts_poss) off_ts_poss,sum(off_oreb) off_oreb,
    sum(off_oreb_opp) off_oreb_opp,sum(off_tov) off_tov,sum(off_fta) off_fta,sum(off_fga) off_fga,sum(off_fgm) off_fgm,sum(off_fg3m) off_fg3m,
    sum(def_pts) def_pts,sum(def_poss) def_poss,sum(def_ts_poss) def_ts_poss,sum(def_oreb) def_oreb,
    sum(def_oreb_opp) def_oreb_opp,sum(def_tov) def_tov,sum(def_fta) def_fta,sum(def_fga) def_fga,sum(def_fgm) def_fgm,sum(def_fg3m) def_fg3m
  FROM metric_games GROUP BY team_id
), rated AS (
  SELECT a.*,round(100.0*a.off_pts/nullif(a.off_poss,0),1) off_rating,
    round(100.0*a.def_pts/nullif(a.def_poss,0),1) def_rating FROM metric_agg a
)
SELECT p_game_year,r.team_id,r.team_name,r.off_rating,r.def_rating,
  round(100.0*r.off_pts/nullif(r.off_poss,0)-100.0*r.def_pts/nullif(r.def_poss,0),1),
  r.games_played::bigint,r.wins::bigint,r.losses::bigint,r.off_poss::bigint,r.def_poss::bigint,
  dense_rank() OVER(ORDER BY r.off_rating-r.def_rating DESC),dense_rank() OVER(ORDER BY r.off_rating DESC),
  dense_rank() OVER(ORDER BY r.def_rating ASC),
  round(100.0*(r.off_fgm+0.5*r.off_fg3m)/nullif(r.off_fga,0),1),round(100.0*(r.def_fgm+0.5*r.def_fg3m)/nullif(r.def_fga,0),1),
  round(100.0*r.off_pts/nullif(2*r.off_ts_poss,0),1),round(100.0*r.def_pts/nullif(2*r.def_ts_poss,0),1),
  round(100.0*r.off_oreb/nullif(r.off_oreb_opp,0),1),round(100.0*r.def_oreb/nullif(r.def_oreb_opp,0),1),
  round(100.0*r.off_tov/nullif(r.off_poss,0),1),round(100.0*r.def_tov/nullif(r.def_poss,0),1),
  round(100.0*r.off_fta/nullif(r.off_fga,0),1),round(100.0*r.def_fta/nullif(r.def_fga,0),1)
FROM rated r
ORDER BY r.off_rating-r.def_rating DESC NULLS LAST
$function$;

REVOKE ALL ON FUNCTION euroleague.get_team_metrics_pergame(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,integer,integer,integer,integer,integer,integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION euroleague.get_team_dashboard_dynamic(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,text,integer,boolean,integer,integer,integer,integer,integer,integer,integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION euroleague.get_team_metrics_direct(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,text,integer,boolean,integer,integer,integer,integer,integer,integer,integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION euroleague.get_team_metrics_pergame(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,integer,integer,integer,integer,integer,integer) TO app_readonly;
GRANT EXECUTE ON FUNCTION euroleague.get_team_dashboard_dynamic(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,text,integer,boolean,integer,integer,integer,integer,integer,integer,integer) TO app_readonly;
GRANT EXECUTE ON FUNCTION euroleague.get_team_metrics_direct(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,text,integer,boolean,integer,integer,integer,integer,integer,integer,integer) TO app_readonly;

COMMIT;
