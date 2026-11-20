-- 068_team_ratings_luck_counts.sql
--
-- EUROLEAGUE SHADOW SCHEMA. Team Ratings shooting luck (2026-11-20; spec
-- docs/superpowers/specs/2026-11-20-team-ratings-shooting-luck-design.md):
-- every Team Ratings Summary reader appends eight counts, per side,
-- off_/def_ fg3_made, fg3_att, ftm, fta. The web derives the luck from them
-- (teamShootingLuck, web/src/rules/teamRatings.ts). Israeli twin: the same
-- columns on basketball_test.get_team_ratings_dynamic / team_ppp_ratings_mv.
--
-- FTM: play_type = 'FTM', as get_team_traditional_direct counts it. The
-- standard-clutch cache (filtered_team_game_facts) has no FT-made column, so
-- the readers on it take FTM = points - 2 x fg2_made - 3 x fg3_made: every
-- point that is not a field goal is a free throw. Pinned against the
-- Traditional reader by app/tests/testthat/test-team-shooting-luck-db.R.
--
-- Applied by scripts/apply_068_team_ratings_luck_counts.py (dry run by
-- default). Existing columns of every object are unchanged (gated there).
-- Rollback: 068_team_ratings_luck_counts_before.sql.

ALTER TABLE euroleague.team_four_factors_by_game
  ADD COLUMN IF NOT EXISTS off_fg3a bigint NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS off_ftm  bigint NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS def_fg3a bigint NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS def_ftm  bigint NOT NULL DEFAULT 0;


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
    derivation_version,
    off_fg3a, off_ftm, def_fg3a, def_ftm
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
    'actions-v1',
    coalesce(sum(atc.fg3_att) FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum((atc.play_type = 'FTM')::int) FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.fg3_att) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum((atc.play_type = 'FTM')::int) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint
  FROM euroleague.action_team_context_actions atc
  JOIN euroleague.schedule s ON s.game_id = atc.game_id
 WHERE game_ids IS NULL OR atc.game_id = ANY(game_ids)
 GROUP BY atc.game_id, atc.team_id, s.season,
          atc.credit_own_starters, atc.credit_opp_starters;

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$function$;

-- Backfill: rebuild every game's rows with the new columns (the loader's own refresh).
SELECT euroleague.refresh_team_four_factors_by_game_for_games(NULL::bigint[]);



-- Return type changes: replace, then restore the grants.
DROP FUNCTION euroleague.get_team_ratings_pergame(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,integer,integer,integer,integer,integer,integer);
CREATE OR REPLACE FUNCTION euroleague.get_team_ratings_pergame(p_competition text, p_game_year integer, p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date, p_team_ids_csv text DEFAULT NULL::text, p_phase_csv text DEFAULT NULL::text, p_opp_ids_csv text DEFAULT NULL::text, p_home_away text DEFAULT 'all'::text, p_outcome text DEFAULT 'all'::text, p_opp_rank_side text DEFAULT NULL::text, p_opp_rank_n integer DEFAULT NULL::integer, p_opp_rank_metric text DEFAULT NULL::text, p_min_gn integer DEFAULT NULL::integer, p_max_gn integer DEFAULT NULL::integer, p_last_n_games integer DEFAULT NULL::integer, p_num_starters_off_min integer DEFAULT NULL::integer, p_num_starters_off_max integer DEFAULT NULL::integer, p_num_starters_def_min integer DEFAULT NULL::integer, p_num_starters_def_max integer DEFAULT NULL::integer)
 RETURNS TABLE(game_year integer, team_id bigint, team_name text, off_ppp numeric, def_ppp numeric, net_rtg numeric, games_played bigint, wins bigint, losses bigint, off_poss bigint, def_poss bigint, rank_net_rtg bigint, rank_off_ppp bigint, rank_def_ppp bigint, off_fg3_made bigint, off_fg3_att bigint, def_fg3_made bigint, def_fg3_att bigint, off_ftm bigint, off_fta bigint, def_ftm bigint, def_fta bigint)
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
    sum(f.def_pts) def_pts,sum(f.def_poss) def_poss,
    sum(f.off_fg3m) off_fg3_made,sum(f.off_fg3a) off_fg3_att,sum(f.def_fg3m) def_fg3_made,sum(f.def_fg3a) def_fg3_att,
    sum(f.off_ftm) off_ftm,sum(f.off_fta) off_fta,sum(f.def_ftm) def_ftm,sum(f.def_fta) def_fta
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
    sum(def_pts) def_pts,sum(def_poss) def_poss,
    sum(off_fg3_made) off_fg3_made,sum(off_fg3_att) off_fg3_att,sum(def_fg3_made) def_fg3_made,sum(def_fg3_att) def_fg3_att,
    sum(off_ftm) off_ftm,sum(off_fta) off_fta,sum(def_ftm) def_ftm,sum(def_fta) def_fta
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
  dense_rank() OVER(ORDER BY r.off_ppp DESC),dense_rank() OVER(ORDER BY r.def_ppp ASC),
  r.off_fg3_made::bigint,r.off_fg3_att::bigint,r.def_fg3_made::bigint,r.def_fg3_att::bigint,
  r.off_ftm::bigint,r.off_fta::bigint,r.def_ftm::bigint,r.def_fta::bigint
FROM rated r ORDER BY r.off_ppp-r.def_ppp DESC NULLS LAST
$function$;
REVOKE ALL ON FUNCTION euroleague.get_team_ratings_pergame(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,integer,integer,integer,integer,integer,integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION euroleague.get_team_ratings_pergame(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,integer,integer,integer,integer,integer,integer) TO app_readonly;


-- Return type changes: replace, then restore the grants.
DROP FUNCTION euroleague.get_team_ratings_dynamic(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,text,integer,boolean,integer,integer,integer,integer,integer,integer,integer);
CREATE OR REPLACE FUNCTION euroleague.get_team_ratings_dynamic(p_competition text, p_game_year integer, p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date, p_team_ids_csv text DEFAULT NULL::text, p_phase_csv text DEFAULT NULL::text, p_opp_ids_csv text DEFAULT NULL::text, p_home_away text DEFAULT 'all'::text, p_outcome text DEFAULT 'all'::text, p_opp_rank_side text DEFAULT NULL::text, p_opp_rank_n integer DEFAULT NULL::integer, p_opp_rank_metric text DEFAULT NULL::text, p_max_margin integer DEFAULT NULL::integer, p_margin_status text DEFAULT NULL::text, p_max_time_remaining integer DEFAULT NULL::integer, p_ot_margin_filter boolean DEFAULT false, p_min_gn integer DEFAULT NULL::integer, p_max_gn integer DEFAULT NULL::integer, p_last_n_games integer DEFAULT NULL::integer, p_num_starters_off_min integer DEFAULT NULL::integer, p_num_starters_off_max integer DEFAULT NULL::integer, p_num_starters_def_min integer DEFAULT NULL::integer, p_num_starters_def_max integer DEFAULT NULL::integer)
 RETURNS TABLE(game_year integer, team_id bigint, team_name text, off_ppp numeric, def_ppp numeric, net_rtg numeric, games_played bigint, wins bigint, losses bigint, off_poss bigint, def_poss bigint, rank_net_rtg bigint, rank_off_ppp bigint, rank_def_ppp bigint, off_fg3_made bigint, off_fg3_att bigint, def_fg3_made bigint, def_fg3_att bigint, off_ftm bigint, off_fta bigint, def_ftm bigint, def_fta bigint)
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
      sum(f.possessions) FILTER (WHERE f.type_lineup = 'defense') AS def_poss,
      sum(f.fg3_made) FILTER (WHERE f.type_lineup = 'offense') AS off_fg3_made,
      sum(f.fg3_att) FILTER (WHERE f.type_lineup = 'offense') AS off_fg3_att,
      sum(f.fg3_made) FILTER (WHERE f.type_lineup = 'defense') AS def_fg3_made,
      sum(f.fg3_att) FILTER (WHERE f.type_lineup = 'defense') AS def_fg3_att,
      sum(f.points - 2 * f.fg2_made - 3 * f.fg3_made) FILTER (WHERE f.type_lineup = 'offense') AS off_ftm,
      sum(f.ft_attempts) FILTER (WHERE f.type_lineup = 'offense') AS off_fta,
      sum(f.points - 2 * f.fg2_made - 3 * f.fg3_made) FILTER (WHERE f.type_lineup = 'defense') AS def_ftm,
      sum(f.ft_attempts) FILTER (WHERE f.type_lineup = 'defense') AS def_fta
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
    dense_rank() OVER (ORDER BY r.def_ppp ASC),
    r.off_fg3_made::bigint, r.off_fg3_att::bigint, r.def_fg3_made::bigint, r.def_fg3_att::bigint,
    r.off_ftm::bigint, r.off_fta::bigint, r.def_ftm::bigint, r.def_fta::bigint
  FROM rated r
  ORDER BY r.off_ppp - r.def_ppp DESC NULLS LAST
$function$;
REVOKE ALL ON FUNCTION euroleague.get_team_ratings_dynamic(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,text,integer,boolean,integer,integer,integer,integer,integer,integer,integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION euroleague.get_team_ratings_dynamic(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,text,integer,boolean,integer,integer,integer,integer,integer,integer,integer) TO app_readonly;


-- Return type changes: replace, then restore the grants.
DROP FUNCTION euroleague.get_team_ratings_direct(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,text,integer,boolean,integer,integer,integer,integer,integer,integer,integer);
CREATE OR REPLACE FUNCTION euroleague.get_team_ratings_direct(p_competition text, p_game_year integer, p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date, p_team_ids_csv text DEFAULT NULL::text, p_phase_csv text DEFAULT NULL::text, p_opp_ids_csv text DEFAULT NULL::text, p_home_away text DEFAULT 'all'::text, p_outcome text DEFAULT 'all'::text, p_opp_rank_side text DEFAULT NULL::text, p_opp_rank_n integer DEFAULT NULL::integer, p_opp_rank_metric text DEFAULT NULL::text, p_max_margin integer DEFAULT NULL::integer, p_margin_status text DEFAULT NULL::text, p_max_time_remaining integer DEFAULT NULL::integer, p_ot_margin_filter boolean DEFAULT false, p_min_gn integer DEFAULT NULL::integer, p_max_gn integer DEFAULT NULL::integer, p_last_n_games integer DEFAULT NULL::integer, p_num_starters_off_min integer DEFAULT NULL::integer, p_num_starters_off_max integer DEFAULT NULL::integer, p_num_starters_def_min integer DEFAULT NULL::integer, p_num_starters_def_max integer DEFAULT NULL::integer)
 RETURNS TABLE(game_year integer, team_id bigint, team_name text, off_ppp numeric, def_ppp numeric, net_rtg numeric, games_played bigint, wins bigint, losses bigint, off_poss bigint, def_poss bigint, rank_net_rtg bigint, rank_off_ppp bigint, rank_def_ppp bigint, off_fg3_made bigint, off_fg3_att bigint, def_fg3_made bigint, def_fg3_att bigint, off_ftm bigint, off_fta bigint, def_ftm bigint, def_fta bigint)
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
  SELECT a.game_id,a.team_id,a.type_lineup,a.possession_flag,a.points,
    a.fg3_made,a.fg3_att,(a.play_type='FTM')::int ftm,a.ft_attempts
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
    a.type_lineup,sum(a.points)::bigint points,sum(a.possession_flag)::bigint possessions,
    sum(a.fg3_made)::bigint fg3_made,sum(a.fg3_att)::bigint fg3_att,sum(a.ftm)::bigint ftm,sum(a.ft_attempts)::bigint fta
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
    sum(possessions) FILTER(WHERE type_lineup='defense') def_poss,
    sum(fg3_made) FILTER(WHERE type_lineup='offense') off_fg3_made,sum(fg3_att) FILTER(WHERE type_lineup='offense') off_fg3_att,
    sum(fg3_made) FILTER(WHERE type_lineup='defense') def_fg3_made,sum(fg3_att) FILTER(WHERE type_lineup='defense') def_fg3_att,
    sum(ftm) FILTER(WHERE type_lineup='offense') off_ftm,sum(fta) FILTER(WHERE type_lineup='offense') off_fta,
    sum(ftm) FILTER(WHERE type_lineup='defense') def_ftm,sum(fta) FILTER(WHERE type_lineup='defense') def_fta
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
  dense_rank() OVER(ORDER BY r.off_ppp DESC),dense_rank() OVER(ORDER BY r.def_ppp ASC),
  r.off_fg3_made::bigint,r.off_fg3_att::bigint,r.def_fg3_made::bigint,r.def_fg3_att::bigint,
  r.off_ftm::bigint,r.off_fta::bigint,r.def_ftm::bigint,r.def_fta::bigint
FROM rated r ORDER BY r.off_ppp-r.def_ppp DESC NULLS LAST
$function$;
REVOKE ALL ON FUNCTION euroleague.get_team_ratings_direct(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,text,integer,boolean,integer,integer,integer,integer,integer,integer,integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION euroleague.get_team_ratings_direct(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,text,integer,boolean,integer,integer,integer,integer,integer,integer,integer) TO app_readonly;


-- Return type changes: replace, then restore the grants.
DROP FUNCTION euroleague.get_team_dashboard_dynamic(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,text,integer,boolean,integer,integer,integer,integer,integer,integer,integer);
CREATE OR REPLACE FUNCTION euroleague.get_team_dashboard_dynamic(p_competition text, p_game_year integer, p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date, p_team_ids_csv text DEFAULT NULL::text, p_phase_csv text DEFAULT NULL::text, p_opp_ids_csv text DEFAULT NULL::text, p_home_away text DEFAULT 'all'::text, p_outcome text DEFAULT 'all'::text, p_opp_rank_side text DEFAULT NULL::text, p_opp_rank_n integer DEFAULT NULL::integer, p_opp_rank_metric text DEFAULT NULL::text, p_max_margin integer DEFAULT NULL::integer, p_margin_status text DEFAULT NULL::text, p_max_time_remaining integer DEFAULT NULL::integer, p_ot_margin_filter boolean DEFAULT false, p_min_gn integer DEFAULT NULL::integer, p_max_gn integer DEFAULT NULL::integer, p_last_n_games integer DEFAULT NULL::integer, p_num_starters_off_min integer DEFAULT NULL::integer, p_num_starters_off_max integer DEFAULT NULL::integer, p_num_starters_def_min integer DEFAULT NULL::integer, p_num_starters_def_max integer DEFAULT NULL::integer)
 RETURNS TABLE(game_year integer, team_id bigint, team_name text, off_ppp numeric, def_ppp numeric, net_rtg numeric, games_played bigint, wins bigint, losses bigint, off_poss bigint, def_poss bigint, rank_net_rtg bigint, rank_off_ppp bigint, rank_def_ppp bigint, off_efg numeric, def_efg numeric, off_ts numeric, def_ts numeric, off_oreb numeric, def_oreb numeric, off_tov numeric, def_tov numeric, off_ftr numeric, def_ftr numeric, minutes numeric, off_fg3_made bigint, off_fg3_att bigint, def_fg3_made bigint, def_fg3_att bigint, off_ftm bigint, off_fta bigint, def_ftm bigint, def_fta bigint)
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
    sum(f.seconds) FILTER(WHERE f.type_lineup='offense') seconds,
    sum(f.fg3_made) FILTER (WHERE f.type_lineup = 'offense') AS off_fg3_made,
    sum(f.fg3_att) FILTER (WHERE f.type_lineup = 'offense') AS off_fg3_att,
    sum(f.fg3_made) FILTER (WHERE f.type_lineup = 'defense') AS def_fg3_made,
    sum(f.fg3_att) FILTER (WHERE f.type_lineup = 'defense') AS def_fg3_att,
    sum(f.points - 2 * f.fg2_made - 3 * f.fg3_made) FILTER (WHERE f.type_lineup = 'offense') AS off_ftm,
    sum(f.points - 2 * f.fg2_made - 3 * f.fg3_made) FILTER (WHERE f.type_lineup = 'defense') AS def_ftm
    -- off_fta / def_fta: already summed above (ft_attempts)
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
  round(100.0*r.def_fta/nullif(r.def_fga,0),1),round(r.seconds/60.0,3)::numeric,
  r.off_fg3_made::bigint,r.off_fg3_att::bigint,r.def_fg3_made::bigint,r.def_fg3_att::bigint,
  r.off_ftm::bigint,r.off_fta::bigint,r.def_ftm::bigint,r.def_fta::bigint
FROM rated r ORDER BY r.off_rating-r.def_rating DESC NULLS LAST
$function$;
REVOKE ALL ON FUNCTION euroleague.get_team_dashboard_dynamic(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,text,integer,boolean,integer,integer,integer,integer,integer,integer,integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION euroleague.get_team_dashboard_dynamic(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,text,integer,boolean,integer,integer,integer,integer,integer,integer,integer) TO app_readonly;


DROP MATERIALIZED VIEW euroleague.team_ppp_ratings_mv;
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
            (sum(f.def_poss))::bigint AS def_poss,
            (sum(f.off_fg3m))::bigint AS off_fg3_made,
            (sum(f.off_fg3a))::bigint AS off_fg3_att,
            (sum(f.def_fg3m))::bigint AS def_fg3_made,
            (sum(f.def_fg3a))::bigint AS def_fg3_att,
            (sum(f.off_ftm))::bigint AS off_ftm,
            (sum(f.off_fta))::bigint AS off_fta,
            (sum(f.def_ftm))::bigint AS def_ftm,
            (sum(f.def_fta))::bigint AS def_fta
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
            (sum(g.def_poss))::bigint AS def_poss,
            (sum(g.off_fg3_made))::bigint AS off_fg3_made,
            (sum(g.off_fg3_att))::bigint AS off_fg3_att,
            (sum(g.def_fg3_made))::bigint AS def_fg3_made,
            (sum(g.def_fg3_att))::bigint AS def_fg3_att,
            (sum(g.off_ftm))::bigint AS off_ftm,
            (sum(g.off_fta))::bigint AS off_fta,
            (sum(g.def_ftm))::bigint AS def_ftm,
            (sum(g.def_fta))::bigint AS def_fta
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
            round(((100.0 * (a.def_pts)::numeric) / (NULLIF(a.def_poss, 0))::numeric), 1) AS def_ppp,
            a.off_fg3_made,
            a.off_fg3_att,
            a.def_fg3_made,
            a.def_fg3_att,
            a.off_ftm,
            a.off_fta,
            a.def_ftm,
            a.def_fta
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
    dense_rank() OVER (PARTITION BY r.competition, r.game_year ORDER BY (r.off_ppp - r.def_ppp) DESC) AS net_rank,
    r.off_fg3_made,
    r.off_fg3_att,
    r.def_fg3_made,
    r.def_fg3_att,
    r.off_ftm,
    r.off_fta,
    r.def_ftm,
    r.def_fta
   FROM (rated r
     JOIN euroleague.teams t ON ((t.team_id = r.team_id)))
;
CREATE UNIQUE INDEX euroleague_team_ppp_ratings_mv_pk ON euroleague.team_ppp_ratings_mv USING btree (competition, game_year, team_id);
GRANT SELECT ON euroleague.team_ppp_ratings_mv TO app_readonly;
