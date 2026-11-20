-- 068_team_ratings_luck_counts_before.sql -- EUROLEAGUE SHADOW SCHEMA
-- Live definitions replaced by migration 068 (dumped 2026-11-20), for rollback.

CREATE OR REPLACE FUNCTION euroleague.refresh_team_four_factors_by_game_for_games(game_ids bigint[])
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
  SELECT
    atc.game_id,
    atc.team_id,
    s.season::smallint,
    atc.credit_own_starters,
    atc.credit_opp_starters,
    coalesce(sum(atc.points) FILTER (WHERE atc.type_lineup = 'offense'), 0)::numeric,
    coalesce(sum(atc.possession_flag) FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.ts_possessions) FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.orebounds) FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.oreb_opportunities) FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.turnovers) FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.ft_attempts) FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.fga) FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.fgm) FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.fg3_made) FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.points) FILTER (WHERE atc.type_lineup = 'defense'), 0)::numeric,
    coalesce(sum(atc.possession_flag) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.ts_possessions) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.orebounds) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.oreb_opportunities) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.turnovers) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.ft_attempts) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.fga) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.fgm) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.fg3_made) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.steals) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    'actions-v1'
  FROM euroleague.action_team_context_actions atc
  JOIN euroleague.schedule s ON s.game_id = atc.game_id
 WHERE game_ids IS NULL OR atc.game_id = ANY(game_ids)
 GROUP BY atc.game_id, atc.team_id, s.season,
          atc.credit_own_starters, atc.credit_opp_starters;

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$function$;


CREATE OR REPLACE FUNCTION euroleague.get_team_ratings_pergame(p_competition text, p_game_year integer, p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date, p_team_ids_csv text DEFAULT NULL::text, p_phase_csv text DEFAULT NULL::text, p_opp_ids_csv text DEFAULT NULL::text, p_home_away text DEFAULT 'all'::text, p_outcome text DEFAULT 'all'::text, p_opp_rank_side text DEFAULT NULL::text, p_opp_rank_n integer DEFAULT NULL::integer, p_opp_rank_metric text DEFAULT NULL::text, p_min_gn integer DEFAULT NULL::integer, p_max_gn integer DEFAULT NULL::integer, p_last_n_games integer DEFAULT NULL::integer, p_num_starters_off_min integer DEFAULT NULL::integer, p_num_starters_off_max integer DEFAULT NULL::integer, p_num_starters_def_min integer DEFAULT NULL::integer, p_num_starters_def_max integer DEFAULT NULL::integer)
 RETURNS TABLE(game_year integer, team_id bigint, team_name text, off_ppp numeric, def_ppp numeric, net_rtg numeric, games_played bigint, wins bigint, losses bigint, off_poss bigint, def_poss bigint, rank_net_rtg bigint, rank_off_ppp bigint, rank_def_ppp bigint)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'euroleague', 'public'
 SET plan_cache_mode TO 'force_custom_plan'
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
  SELECT fs.*,row_number() OVER(
    PARTITION BY fs.team_id ORDER BY fs.game_date DESC,fs.game_id DESC) team_game_rank
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
), game_agg AS (
  SELECT gf.game_id,gf.team_id,max(gf.team_name) team_name,bool_or(gf.has_won) has_won,
    sum(f.off_pts) off_pts,sum(f.off_poss) off_poss,
    sum(f.def_pts) def_pts,sum(f.def_poss) def_poss
  FROM euroleague.team_four_factors_by_game f
  JOIN games_filtered gf USING(game_id,team_id)
  WHERE f.game_year=p_game_year
    AND (p_num_starters_off_min IS NULL OR f.own_starters>=p_num_starters_off_min)
    AND (p_num_starters_off_max IS NULL OR f.own_starters<=p_num_starters_off_max)
    AND (p_num_starters_def_min IS NULL OR f.opp_starters>=p_num_starters_def_min)
    AND (p_num_starters_def_max IS NULL OR f.opp_starters<=p_num_starters_def_max)
  GROUP BY gf.game_id,gf.team_id
), agg AS (
  SELECT team_id,max(team_name) team_name,count(DISTINCT game_id) games_played,
    count(DISTINCT game_id) FILTER(WHERE has_won) wins,
    count(DISTINCT game_id) FILTER(WHERE NOT has_won) losses,
    sum(off_pts) off_pts,sum(off_poss) off_poss,
    sum(def_pts) def_pts,sum(def_poss) def_poss
  FROM game_agg GROUP BY team_id
), rated AS (
  SELECT a.*,round(100.0*a.off_pts/nullif(a.off_poss,0),1) off_ppp,
    round(100.0*a.def_pts/nullif(a.def_poss,0),1) def_ppp FROM agg a
)
SELECT p_game_year,r.team_id,r.team_name,r.off_ppp,r.def_ppp,
  round(100.0*r.off_pts/nullif(r.off_poss,0)
      - 100.0*r.def_pts/nullif(r.def_poss,0),1),r.games_played,r.wins,r.losses,
  r.off_poss::bigint,r.def_poss::bigint,
  dense_rank() OVER(ORDER BY r.off_ppp-r.def_ppp DESC),
  dense_rank() OVER(ORDER BY r.off_ppp DESC),dense_rank() OVER(ORDER BY r.def_ppp ASC)
FROM rated r ORDER BY r.off_ppp-r.def_ppp DESC NULLS LAST
$function$;


CREATE OR REPLACE FUNCTION euroleague.get_team_ratings_dynamic(p_competition text, p_game_year integer, p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date, p_team_ids_csv text DEFAULT NULL::text, p_phase_csv text DEFAULT NULL::text, p_opp_ids_csv text DEFAULT NULL::text, p_home_away text DEFAULT 'all'::text, p_outcome text DEFAULT 'all'::text, p_opp_rank_side text DEFAULT NULL::text, p_opp_rank_n integer DEFAULT NULL::integer, p_opp_rank_metric text DEFAULT NULL::text, p_max_margin integer DEFAULT NULL::integer, p_margin_status text DEFAULT NULL::text, p_max_time_remaining integer DEFAULT NULL::integer, p_ot_margin_filter boolean DEFAULT false, p_min_gn integer DEFAULT NULL::integer, p_max_gn integer DEFAULT NULL::integer, p_last_n_games integer DEFAULT NULL::integer, p_num_starters_off_min integer DEFAULT NULL::integer, p_num_starters_off_max integer DEFAULT NULL::integer, p_num_starters_def_min integer DEFAULT NULL::integer, p_num_starters_def_max integer DEFAULT NULL::integer)
 RETURNS TABLE(game_year integer, team_id bigint, team_name text, off_ppp numeric, def_ppp numeric, net_rtg numeric, games_played bigint, wins bigint, losses bigint, off_poss bigint, def_poss bigint, rank_net_rtg bigint, rank_off_ppp bigint, rank_def_ppp bigint)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'euroleague', 'public'
AS $function$
  WITH facts AS (
    SELECT * FROM euroleague.filtered_team_game_facts(
      p_competition, p_game_year, p_start_date, p_end_date,
      p_team_ids_csv, p_phase_csv, p_opp_ids_csv, p_home_away, p_outcome,
      p_opp_rank_side, p_opp_rank_n, p_opp_rank_metric,
      p_max_margin, p_margin_status, p_max_time_remaining, p_ot_margin_filter,
      p_min_gn, p_max_gn, p_last_n_games,
      p_num_starters_off_min, p_num_starters_off_max,
      p_num_starters_def_min, p_num_starters_def_max
    )
  ),
  agg AS (
    SELECT
      f.team_id, max(f.team_name) AS team_name,
      count(DISTINCT f.game_id) AS games_played,
      count(DISTINCT f.game_id) FILTER (WHERE f.has_won) AS wins,
      count(DISTINCT f.game_id) FILTER (WHERE NOT f.has_won) AS losses,
      sum(f.points) FILTER (WHERE f.type_lineup = 'offense') AS off_pts,
      sum(f.possessions) FILTER (WHERE f.type_lineup = 'offense') AS off_poss,
      sum(f.points) FILTER (WHERE f.type_lineup = 'defense') AS def_pts,
      sum(f.possessions) FILTER (WHERE f.type_lineup = 'defense') AS def_poss
    FROM facts f GROUP BY f.team_id
  ),
  rated AS (
    SELECT a.*,
      round(100.0 * a.off_pts / nullif(a.off_poss, 0), 1) AS off_ppp,
      round(100.0 * a.def_pts / nullif(a.def_poss, 0), 1) AS def_ppp
    FROM agg a
  )
  SELECT
    p_game_year, r.team_id, r.team_name,
    r.off_ppp, r.def_ppp,
    round(100.0 * r.off_pts / nullif(r.off_poss, 0)
        - 100.0 * r.def_pts / nullif(r.def_poss, 0), 1),
    r.games_played::bigint, r.wins::bigint, r.losses::bigint,
    r.off_poss::bigint, r.def_poss::bigint,
    dense_rank() OVER (ORDER BY r.off_ppp - r.def_ppp DESC),
    dense_rank() OVER (ORDER BY r.off_ppp DESC),
    dense_rank() OVER (ORDER BY r.def_ppp ASC)
  FROM rated r
  ORDER BY r.off_ppp - r.def_ppp DESC NULLS LAST
$function$;


CREATE OR REPLACE FUNCTION euroleague.get_team_ratings_direct(p_competition text, p_game_year integer, p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date, p_team_ids_csv text DEFAULT NULL::text, p_phase_csv text DEFAULT NULL::text, p_opp_ids_csv text DEFAULT NULL::text, p_home_away text DEFAULT 'all'::text, p_outcome text DEFAULT 'all'::text, p_opp_rank_side text DEFAULT NULL::text, p_opp_rank_n integer DEFAULT NULL::integer, p_opp_rank_metric text DEFAULT NULL::text, p_max_margin integer DEFAULT NULL::integer, p_margin_status text DEFAULT NULL::text, p_max_time_remaining integer DEFAULT NULL::integer, p_ot_margin_filter boolean DEFAULT false, p_min_gn integer DEFAULT NULL::integer, p_max_gn integer DEFAULT NULL::integer, p_last_n_games integer DEFAULT NULL::integer, p_num_starters_off_min integer DEFAULT NULL::integer, p_num_starters_off_max integer DEFAULT NULL::integer, p_num_starters_def_min integer DEFAULT NULL::integer, p_num_starters_def_max integer DEFAULT NULL::integer)
 RETURNS TABLE(game_year integer, team_id bigint, team_name text, off_ppp numeric, def_ppp numeric, net_rtg numeric, games_played bigint, wins bigint, losses bigint, off_poss bigint, def_poss bigint, rank_net_rtg bigint, rank_off_ppp bigint, rank_def_ppp bigint)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'euroleague', 'public'
 SET plan_cache_mode TO 'force_custom_plan'
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
    coalesce(nullif(btrim(p_opp_rank_metric),''),'net') rank_metric,
    coalesce(nullif(btrim(p_margin_status),''),'all') margin_status
), schedule_ranked AS (
  SELECT fs.*,row_number() OVER(
    PARTITION BY fs.team_id ORDER BY fs.game_date DESC,fs.game_id DESC) team_game_rank
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
), acts AS (
  SELECT a.game_id,a.team_id,a.type_lineup,a.possession_flag,a.points
  FROM euroleague.player_stats_actions_by_game a
  JOIN games_filtered gf USING(game_id,team_id) CROSS JOIN normalized n
  WHERE (p_max_time_remaining IS NULL OR a.regulation_seconds_remaining<=p_max_time_remaining OR a.is_overtime)
    AND (p_max_margin IS NULL OR a.pre_abs_margin<=p_max_margin
      OR (a.is_overtime AND NOT coalesce(p_ot_margin_filter,false)))
    AND (n.margin_status='all' OR (n.margin_status='leading' AND a.pre_status>0)
      OR (n.margin_status='trailing' AND a.pre_status<0)
      OR (n.margin_status='tied' AND a.pre_status=0)
      OR (a.is_overtime AND NOT coalesce(p_ot_margin_filter,false)))
    AND (p_num_starters_off_min IS NULL OR a.own_starters>=p_num_starters_off_min)
    AND (p_num_starters_off_max IS NULL OR a.own_starters<=p_num_starters_off_max)
    AND (p_num_starters_def_min IS NULL OR a.opp_starters>=p_num_starters_def_min)
    AND (p_num_starters_def_max IS NULL OR a.opp_starters<=p_num_starters_def_max)
), game_agg AS (
  SELECT gf.game_id,gf.team_id,max(gf.team_name) team_name,bool_or(gf.has_won) has_won,
    a.type_lineup,sum(a.points)::bigint points,sum(a.possession_flag)::bigint possessions
  FROM acts a JOIN games_filtered gf USING(game_id,team_id)
  WHERE a.type_lineup IS NOT NULL
  GROUP BY gf.game_id,gf.team_id,a.type_lineup
), agg AS (
  SELECT team_id,max(team_name) team_name,count(DISTINCT game_id) games_played,
    count(DISTINCT game_id) FILTER(WHERE has_won) wins,
    count(DISTINCT game_id) FILTER(WHERE NOT has_won) losses,
    sum(points) FILTER(WHERE type_lineup='offense') off_pts,
    sum(possessions) FILTER(WHERE type_lineup='offense') off_poss,
    sum(points) FILTER(WHERE type_lineup='defense') def_pts,
    sum(possessions) FILTER(WHERE type_lineup='defense') def_poss
  FROM game_agg GROUP BY team_id
), rated AS (
  SELECT a.*,round(100.0*a.off_pts/nullif(a.off_poss,0),1) off_ppp,
    round(100.0*a.def_pts/nullif(a.def_poss,0),1) def_ppp FROM agg a
)
SELECT p_game_year,r.team_id,r.team_name,r.off_ppp,r.def_ppp,
  round(100.0*r.off_pts/nullif(r.off_poss,0)
      - 100.0*r.def_pts/nullif(r.def_poss,0),1),
  r.games_played,r.wins,r.losses,r.off_poss,r.def_poss,
  dense_rank() OVER(ORDER BY r.off_ppp-r.def_ppp DESC),
  dense_rank() OVER(ORDER BY r.off_ppp DESC),dense_rank() OVER(ORDER BY r.def_ppp ASC)
FROM rated r ORDER BY r.off_ppp-r.def_ppp DESC NULLS LAST
$function$;


CREATE OR REPLACE FUNCTION euroleague.get_team_dashboard_dynamic(p_competition text, p_game_year integer, p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date, p_team_ids_csv text DEFAULT NULL::text, p_phase_csv text DEFAULT NULL::text, p_opp_ids_csv text DEFAULT NULL::text, p_home_away text DEFAULT 'all'::text, p_outcome text DEFAULT 'all'::text, p_opp_rank_side text DEFAULT NULL::text, p_opp_rank_n integer DEFAULT NULL::integer, p_opp_rank_metric text DEFAULT NULL::text, p_max_margin integer DEFAULT NULL::integer, p_margin_status text DEFAULT NULL::text, p_max_time_remaining integer DEFAULT NULL::integer, p_ot_margin_filter boolean DEFAULT false, p_min_gn integer DEFAULT NULL::integer, p_max_gn integer DEFAULT NULL::integer, p_last_n_games integer DEFAULT NULL::integer, p_num_starters_off_min integer DEFAULT NULL::integer, p_num_starters_off_max integer DEFAULT NULL::integer, p_num_starters_def_min integer DEFAULT NULL::integer, p_num_starters_def_max integer DEFAULT NULL::integer)
 RETURNS TABLE(game_year integer, team_id bigint, team_name text, off_ppp numeric, def_ppp numeric, net_rtg numeric, games_played bigint, wins bigint, losses bigint, off_poss bigint, def_poss bigint, rank_net_rtg bigint, rank_off_ppp bigint, rank_def_ppp bigint, off_efg numeric, def_efg numeric, off_ts numeric, def_ts numeric, off_oreb numeric, def_oreb numeric, off_tov numeric, def_tov numeric, off_ftr numeric, def_ftr numeric, minutes numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'euroleague', 'public'
 SET plan_cache_mode TO 'force_custom_plan'
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


CREATE MATERIALIZED VIEW euroleague.team_ppp_ratings_mv AS
 WITH game_agg AS (
         SELECT s.competition,
            f.game_year,
            f.game_id,
            f.team_id,
            bool_or(fs.has_won) AS has_won,
            (sum(f.off_pts))::bigint AS off_pts,
            (sum(f.off_poss))::bigint AS off_poss,
            (sum(f.def_pts))::bigint AS def_pts,
            (sum(f.def_poss))::bigint AS def_poss
           FROM ((euroleague.team_four_factors_by_game f
             JOIN euroleague.final_schedule_mv fs ON (((fs.game_id = f.game_id) AND (fs.team_id = f.team_id))))
             JOIN euroleague.schedule s ON ((s.game_id = f.game_id)))
          GROUP BY s.competition, f.game_year, f.game_id, f.team_id
        ), agg AS (
         SELECT g.competition,
            g.game_year,
            g.team_id,
            count(DISTINCT g.game_id) AS games_played,
            count(DISTINCT g.game_id) FILTER (WHERE g.has_won) AS wins,
            count(DISTINCT g.game_id) FILTER (WHERE (NOT g.has_won)) AS losses,
            (sum(g.off_pts))::bigint AS off_pts,
            (sum(g.off_poss))::bigint AS off_poss,
            (sum(g.def_pts))::bigint AS def_pts,
            (sum(g.def_poss))::bigint AS def_poss
           FROM game_agg g
          GROUP BY g.competition, g.game_year, g.team_id
        ), rated AS (
         SELECT a.competition,
            a.game_year,
            a.team_id,
            a.games_played,
            a.wins,
            a.losses,
            a.off_pts,
            a.off_poss,
            a.def_pts,
            a.def_poss,
            round(((100.0 * (a.off_pts)::numeric) / (NULLIF(a.off_poss, 0))::numeric), 1) AS off_ppp,
            round(((100.0 * (a.def_pts)::numeric) / (NULLIF(a.def_poss, 0))::numeric), 1) AS def_ppp
           FROM agg a
        )
 SELECT r.competition,
    r.game_year,
    r.team_id,
    t.display_name AS team_name,
    r.off_ppp,
    r.def_ppp,
    round((((100.0 * (r.off_pts)::numeric) / (NULLIF(r.off_poss, 0))::numeric) - ((100.0 * (r.def_pts)::numeric) / (NULLIF(r.def_poss, 0))::numeric)), 1) AS net_rtg,
    r.games_played,
    r.wins,
    r.losses,
    r.off_pts,
    r.def_pts,
    r.off_poss,
    r.def_poss,
    dense_rank() OVER (PARTITION BY r.competition, r.game_year ORDER BY (r.off_ppp - r.def_ppp) DESC) AS rank_net_rtg,
    dense_rank() OVER (PARTITION BY r.competition, r.game_year ORDER BY r.off_ppp DESC) AS rank_off_ppp,
    dense_rank() OVER (PARTITION BY r.competition, r.game_year ORDER BY r.def_ppp) AS rank_def_ppp,
    dense_rank() OVER (PARTITION BY r.competition, r.game_year ORDER BY r.off_ppp DESC) AS off_rank,
    dense_rank() OVER (PARTITION BY r.competition, r.game_year ORDER BY r.def_ppp) AS def_rank,
    dense_rank() OVER (PARTITION BY r.competition, r.game_year ORDER BY (r.off_ppp - r.def_ppp) DESC) AS net_rank
   FROM (rated r
     JOIN euroleague.teams t ON ((t.team_id = r.team_id)))
;
CREATE UNIQUE INDEX euroleague_team_ppp_ratings_mv_pk ON euroleague.team_ppp_ratings_mv USING btree (competition, game_year, team_id);
