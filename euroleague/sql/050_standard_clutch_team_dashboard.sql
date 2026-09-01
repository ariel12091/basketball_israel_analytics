-- Migration 050: one-call standard-clutch Team dashboard reader for the
-- EUROLEAGUE SHADOW SCHEMA.
--
-- This is the retained slice of the combined Team-reader experiment. The
-- standard-clutch route's three existing readers all consume the same filtered
-- fact, so materializing it once removes two complete evaluations. Per-game
-- and custom-clutch candidates remain rejected because their warm gains did
-- not pass the committed fresh-backend cold gate.

BEGIN;
SET LOCAL search_path TO euroleague, public;

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

REVOKE ALL ON FUNCTION euroleague.get_team_dashboard_dynamic(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,text,integer,boolean,integer,integer,integer,integer,integer,integer,integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION euroleague.get_team_dashboard_dynamic(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,text,integer,boolean,integer,integer,integer,integer,integer,integer,integer) TO app_readonly;

COMMIT;
