-- EuroLeague migration 037: per-game Team Ratings and Four Factors readers
-- for the EUROLEAGUE SHADOW SCHEMA.
--
-- Why this exists
-- ---------------
-- Migration 031 added Israeli-shaped `_direct` readers that scan the
-- action/team-perspective fact once. That shape is correct and fast *when a
-- clutch predicate narrows the action set* -- which is what the 031/032 audit
-- measured. The app, however, routes EVERY non-standard preset to `_direct`,
-- including requests that carry no clutch predicate at all (a phase, an
-- opponent, a last-N, or a narrowed date range). Those requests scan the whole
-- 494 MB action fact to produce a 20-row answer:
--
--   full-season, no clutch : get_team_four_factors_direct  12.58 s warm
--                            get_team_ratings_direct       12.61 s warm
--   same answer from the per-game fact                      0.37 s
--
-- A non-clutch request does not need action grain. `team_four_factors_by_game`
-- already stores every additive count these two readers need, keyed
-- (game_id, team_id, own_starters, opp_starters) -- 21,204 rows, 6.4 MB -- so
-- the starter-context filters are still answerable without touching actions.
--
-- This follows the Israeli companion, which branches internally on
-- `v_clutch_active` and reads the pre-aggregated per-game fact when no clutch
-- filter is present (see the Israeli get_team_four_factors_dynamic, in
-- sql/functions/). EuroLeague keeps explicit per-preset routing instead of one selector
-- (PROJECT.md technique 8), so the branch is a third public reader rather than
-- an IF inside an existing one.
--
-- Deliberately NOT parameterised for clutch
-- -----------------------------------------
-- The per-game fact has no time or margin dimension, so these readers
-- structurally cannot answer a clutch request. The four clutch parameters are
-- therefore absent from the signature rather than accepted and ignored: the
-- arity itself documents the capability, and a mis-routed clutch request fails
-- loudly at the call site instead of silently returning unfiltered numbers.
--
-- Starter semantics are those of the `_direct` readers, NOT the Israeli ones:
-- `p_num_starters_off_*` bounds the team's OWN starters and
-- `p_num_starters_def_*` bounds the OPPONENT's. Parity here is against current
-- EuroLeague output (PROJECT.md rule 6), not against Israel.
--
-- Adds no fact table, no backfill, and no index.

BEGIN;
SET LOCAL search_path TO euroleague, public;

CREATE OR REPLACE FUNCTION euroleague.get_team_ratings_pergame(
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
    rank_net_rtg BIGINT, rank_off_ppp BIGINT, rank_def_ppp BIGINT
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
  round(r.off_ppp-r.def_ppp,1),r.games_played,r.wins,r.losses,
  r.off_poss::bigint,r.def_poss::bigint,
  dense_rank() OVER(ORDER BY r.off_ppp-r.def_ppp DESC),
  dense_rank() OVER(ORDER BY r.off_ppp DESC),dense_rank() OVER(ORDER BY r.def_ppp ASC)
FROM rated r ORDER BY r.off_ppp-r.def_ppp DESC NULLS LAST
$function$;

CREATE OR REPLACE FUNCTION euroleague.get_team_four_factors_pergame(
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
 game_year INT,team_id BIGINT,team_name TEXT,off_ppp NUMERIC,def_ppp NUMERIC,
 net_rtg NUMERIC,off_efg NUMERIC,def_efg NUMERIC,off_ts NUMERIC,def_ts NUMERIC,
 off_oreb NUMERIC,def_oreb NUMERIC,off_tov NUMERIC,def_tov NUMERIC,
 off_ftr NUMERIC,def_ftr NUMERIC,off_poss BIGINT,def_poss BIGINT)
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
    coalesce(nullif(btrim(p_opp_rank_metric),''),'net') rank_metric
), schedule_ranked AS (
  SELECT fs.*,row_number() OVER(PARTITION BY fs.team_id ORDER BY fs.game_date DESC,fs.game_id DESC) team_game_rank
  FROM euroleague.final_schedule_mv fs CROSS JOIN normalized n
  WHERE fs.competition=n.competition AND fs.game_year=p_game_year
), opponent_ranks AS (
  SELECT r.team_id,r.off_rank,r.def_rank,r.net_rank,count(*) OVER() team_count
  FROM euroleague.team_ppp_ratings_mv r CROSS JOIN normalized n
  WHERE r.competition=n.competition AND r.game_year=p_game_year
), games_filtered AS MATERIALIZED (
  SELECT sr.game_id,sr.team_id,sr.team_name
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
), agg AS (
 SELECT f.team_id,max(gf.team_name) team_name,
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
 GROUP BY f.team_id
)
SELECT p_game_year,a.team_id,a.team_name,
 round(100.0*a.off_pts/nullif(a.off_poss,0),1),round(100.0*a.def_pts/nullif(a.def_poss,0),1),
 round(100.0*a.off_pts/nullif(a.off_poss,0)-100.0*a.def_pts/nullif(a.def_poss,0),1),
 round(100.0*(a.off_fgm+0.5*a.off_fg3m)/nullif(a.off_fga,0),1),round(100.0*(a.def_fgm+0.5*a.def_fg3m)/nullif(a.def_fga,0),1),
 round(100.0*a.off_pts/nullif(2*a.off_ts_poss,0),1),round(100.0*a.def_pts/nullif(2*a.def_ts_poss,0),1),
 round(100.0*a.off_oreb/nullif(a.off_oreb_opp,0),1),round(100.0*a.def_oreb/nullif(a.def_oreb_opp,0),1),
 round(100.0*a.off_tov/nullif(a.off_poss,0),1),round(100.0*a.def_tov/nullif(a.def_poss,0),1),
 round(100.0*a.off_fta/nullif(a.off_fga,0),1),round(100.0*a.def_fta/nullif(a.def_fga,0),1),
 a.off_poss::bigint,a.def_poss::bigint
FROM agg a ORDER BY 100.0*a.off_pts/nullif(a.off_poss,0)-100.0*a.def_pts/nullif(a.def_poss,0) DESC NULLS LAST
$function$;

REVOKE ALL ON FUNCTION euroleague.get_team_ratings_pergame(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,integer,integer,integer,integer,integer,integer) FROM PUBLIC;
REVOKE ALL ON FUNCTION euroleague.get_team_four_factors_pergame(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,integer,integer,integer,integer,integer,integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION euroleague.get_team_ratings_pergame(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,integer,integer,integer,integer,integer,integer) TO app_readonly;
GRANT EXECUTE ON FUNCTION euroleague.get_team_four_factors_pergame(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,integer,integer,integer,integer,integer,integer) TO app_readonly;
COMMIT;
