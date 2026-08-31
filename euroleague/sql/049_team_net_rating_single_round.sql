-- Migration 049: canonical single-round Net Rating for EuroLeague Team Ratings.
--
-- Defect: each reader computed off_ppp and def_ppp rounded to 1dp, then
-- subtracted those rounded values. The Four Factors readers already round
-- once from additive counts, so Summary and Four Factors disagreed by 0.1 for
-- 8 of 20 teams (broad) and 5 of 20 (last-10) in the 2026-08-29 audit.
--
-- Scope: the net_rtg VALUE ONLY. off_ppp/def_ppp outputs are unchanged, and
-- every dense_rank() ORDER BY is carried over exactly as-is -- those ranks are
-- read as the opponent-strength filter by Tabs 8, 9 and 10, so changing them
-- would move teams between top-N/bottom-N bands. That is deferred.
--
-- CREATE OR REPLACE (never DROP) so app_readonly EXECUTE grants survive.

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
  round(100.0*r.off_pts/nullif(r.off_poss,0)
      - 100.0*r.def_pts/nullif(r.def_poss,0),1),r.games_played,r.wins,r.losses,
  r.off_poss::bigint,r.def_poss::bigint,
  dense_rank() OVER(ORDER BY r.off_ppp-r.def_ppp DESC),
  dense_rank() OVER(ORDER BY r.off_ppp DESC),dense_rank() OVER(ORDER BY r.def_ppp ASC)
FROM rated r ORDER BY r.off_ppp-r.def_ppp DESC NULLS LAST
$function$;

REVOKE ALL ON FUNCTION euroleague.get_team_ratings_pergame(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,integer,integer,integer,integer,integer,integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION euroleague.get_team_ratings_pergame(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,integer,integer,integer,integer,integer,integer) TO app_readonly;

CREATE OR REPLACE FUNCTION euroleague.get_team_ratings_dynamic(
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
    rank_net_rtg BIGINT, rank_off_ppp BIGINT, rank_def_ppp BIGINT
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, euroleague, public
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

REVOKE ALL ON FUNCTION euroleague.get_team_ratings_dynamic(
  text, integer, date, date, text, text, text, text, text, text, integer,
  text, integer, text, integer, boolean, integer, integer, integer,
  integer, integer, integer, integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION euroleague.get_team_ratings_dynamic(
  text, integer, date, date, text, text, text, text, text, text, integer,
  text, integer, text, integer, boolean, integer, integer, integer,
  integer, integer, integer, integer) TO app_readonly;

CREATE OR REPLACE FUNCTION euroleague.get_team_ratings_direct(
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

REVOKE ALL ON FUNCTION euroleague.get_team_ratings_direct(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,text,integer,boolean,integer,integer,integer,integer,integer,integer,integer) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION euroleague.get_team_ratings_direct(text,integer,date,date,text,text,text,text,text,text,integer,text,integer,text,integer,boolean,integer,integer,integer,integer,integer,integer,integer) TO app_readonly;

-- -----------------------------------------------------------------------------
-- The season Ratings materialized view carries the same defect. The Four
-- Factors MV already has the canonical formula and is deliberately untouched.
-- A materialized view cannot be
-- CREATE OR REPLACE'd -- it must be DROP + CREATE, which wipes its GRANTs and
-- drops its unique index. Both properties are restored below.
--
-- Step 1 introspection (read-only, run against the live db on 2026-08-29,
-- via euroleague/src/euroleague_possessions/postgres_backend.connect_from_env_file):
--
--   team_ppp_ratings_mv dependents -> []
--   team_ppp_ratings_mv grants (pg_class.relacl) ->
--     ['postgres=arwdDxtm/postgres', 'app_readonly=r/postgres']
--     (has_table_privilege('app_readonly', 'euroleague.team_ppp_ratings_mv',
--      'SELECT') -> True; information_schema.role_table_grants returned zero
--      rows for both MVs under the connecting role -- that view is
--      role-visibility filtered, not authoritative here, so relacl/
--      has_table_privilege were used instead)
--   team_ppp_ratings_mv indexes -> one unique btree index,
--     euroleague_team_ppp_ratings_mv_pk, on
--     (competition, game_year, team_id)
--
-- The target MV has no dependent view -- the plan's blast-radius assumption holds,
-- so this proceeds without stopping for a human decision.
--
-- team_ppp_ratings_mv also used team_game_ratings_mv, whose direct actions
-- source includes QA-blocked games that cannot produce the lineup-aware
-- consumer facts used by every filtered Team reader. Rebuilding the old
-- definition therefore introduced games 246, 493, 549 and 650 into Ratings
-- but not Four Factors. Aggregate the established per-game Four Factors fact
-- instead, preserving the current published-game eligibility and additive
-- counts. The net_rtg projection then rounds those raw counts once.
-- every dense_rank() OVER clause (rank_net_rtg, rank_off_ppp, rank_def_ppp,
-- off_rank, def_rank, net_rank) is carried over unchanged -- those ranks are
-- read as the opponent-strength filter by Tabs 8, 9 and 10 via
-- get_team_ratings_pergame/_direct and onoff_compute/four_factors_compute.
-- -----------------------------------------------------------------------------

DROP MATERIALIZED VIEW IF EXISTS euroleague.team_ppp_ratings_mv;

CREATE MATERIALIZED VIEW euroleague.team_ppp_ratings_mv AS
WITH game_agg AS (
  SELECT
    s.competition, f.game_year, f.game_id, f.team_id,
    bool_or(fs.has_won) AS has_won,
    sum(f.off_pts)::bigint AS off_pts,
    sum(f.off_poss)::bigint AS off_poss,
    sum(f.def_pts)::bigint AS def_pts,
    sum(f.def_poss)::bigint AS def_poss
  FROM euroleague.team_four_factors_by_game f
  JOIN euroleague.final_schedule_mv fs
    ON fs.game_id = f.game_id AND fs.team_id = f.team_id
  JOIN euroleague.schedule s ON s.game_id = f.game_id
  GROUP BY s.competition, f.game_year, f.game_id, f.team_id
),
agg AS (
  SELECT
    g.competition, g.game_year, g.team_id,
    count(DISTINCT g.game_id) AS games_played,
    count(DISTINCT g.game_id) FILTER (WHERE g.has_won) AS wins,
    count(DISTINCT g.game_id) FILTER (WHERE NOT g.has_won) AS losses,
    sum(g.off_pts)::bigint AS off_pts,
    sum(g.off_poss)::bigint AS off_poss,
    sum(g.def_pts)::bigint AS def_pts,
    sum(g.def_poss)::bigint AS def_poss
  FROM game_agg g
  GROUP BY g.competition, g.game_year, g.team_id
),
rated AS (
  SELECT
    a.*,
    round(100.0 * a.off_pts / NULLIF(a.off_poss, 0), 1) AS off_ppp,
    round(100.0 * a.def_pts / NULLIF(a.def_poss, 0), 1) AS def_ppp
  FROM agg a
)
SELECT
  r.competition, r.game_year, r.team_id, t.display_name AS team_name,
  r.off_ppp, r.def_ppp,
  round(100.0 * r.off_pts / NULLIF(r.off_poss, 0)
      - 100.0 * r.def_pts / NULLIF(r.def_poss, 0), 1) AS net_rtg,
  r.games_played, r.wins, r.losses,
  r.off_pts, r.def_pts, r.off_poss, r.def_poss,
  dense_rank() OVER (
    PARTITION BY r.competition, r.game_year
    ORDER BY r.off_ppp - r.def_ppp DESC
  ) AS rank_net_rtg,
  dense_rank() OVER (
    PARTITION BY r.competition, r.game_year ORDER BY r.off_ppp DESC
  ) AS rank_off_ppp,
  dense_rank() OVER (
    PARTITION BY r.competition, r.game_year ORDER BY r.def_ppp ASC
  ) AS rank_def_ppp,
  dense_rank() OVER (
    PARTITION BY r.competition, r.game_year ORDER BY r.off_ppp DESC
  ) AS off_rank,
  dense_rank() OVER (
    PARTITION BY r.competition, r.game_year ORDER BY r.def_ppp ASC
  ) AS def_rank,
  dense_rank() OVER (
    PARTITION BY r.competition, r.game_year
    ORDER BY r.off_ppp - r.def_ppp DESC
  ) AS net_rank
FROM rated r
JOIN euroleague.teams t ON t.team_id = r.team_id
WITH NO DATA;

-- Exact name/columns from the Step 1 pg_indexes output above.
CREATE UNIQUE INDEX euroleague_team_ppp_ratings_mv_pk
  ON euroleague.team_ppp_ratings_mv (competition, game_year, team_id);

-- DROP wiped this; the app cannot read the MV until it is back.
GRANT SELECT ON euroleague.team_ppp_ratings_mv TO app_readonly;

REFRESH MATERIALIZED VIEW euroleague.team_ppp_ratings_mv;

-- Israeli companion correction: Four Factors only; Ratings is canonical.

DROP MATERIALIZED VIEW IF EXISTS basketball_test.team_four_factors_mv;

-- Team-level four-factor rates aggregated from lineup_four_factors_by_game.
-- Each row in the source MV is unique per (lineup_hash, team_id, game_id, type_lineup),
-- so summing by (team_id, game_year, type_lineup) avoids double-counting.
-- Pivots offense/defense into a single row per (team_id, game_year).

CREATE MATERIALIZED VIEW basketball_test.team_four_factors_mv
TABLESPACE pg_default
AS
WITH team_agg AS (
  SELECT
    lf.team_id,
    lf.game_year,
    lf.type_lineup,
    SUM(lf.total_points)       AS total_points,
    SUM(lf.total_poss)         AS total_poss,
    SUM(lf.ts_poss_count)      AS ts_poss_count,
    SUM(lf.oreb_count)         AS oreb_count,
    SUM(lf.oreb_opportunities) AS oreb_opportunities,
    SUM(lf.tov_count)          AS tov_count,
    SUM(lf.total_ft_attempts)  AS total_ft_attempts,
    SUM(lf.total_fga)          AS total_fga,
    SUM(lf.total_fgm)          AS total_fgm,
    SUM(lf.total_fg3_made)     AS total_fg3_made
  FROM basketball_test.lineup_four_factors_by_game lf
  GROUP BY lf.team_id, lf.game_year, lf.type_lineup
),
pivoted AS (
  SELECT
    ta.team_id,
    ta.game_year,
    -- Offense rates
    ROUND(
      SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'offense')::numeric
      / (2.0 * NULLIF(SUM(ta.ts_poss_count) FILTER (WHERE ta.type_lineup = 'offense'), 0)::numeric)
    * 100, 1) AS off_ts,
    ROUND(
      (
        SUM(ta.total_fgm) FILTER (WHERE ta.type_lineup = 'offense')::numeric
        + 0.5 * SUM(ta.total_fg3_made) FILTER (WHERE ta.type_lineup = 'offense')::numeric
      )
      / NULLIF(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'offense'), 0)::numeric
    * 100, 1) AS off_efg,
    ROUND(
      SUM(ta.oreb_count) FILTER (WHERE ta.type_lineup = 'offense')::numeric
      / NULLIF(SUM(ta.oreb_opportunities) FILTER (WHERE ta.type_lineup = 'offense'), 0)::numeric
    * 100, 1) AS off_oreb,
    ROUND(
      SUM(ta.tov_count) FILTER (WHERE ta.type_lineup = 'offense')::numeric
      / NULLIF(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'offense'), 0)::numeric
    * 100, 1) AS off_tov,
    ROUND(
      SUM(ta.total_ft_attempts) FILTER (WHERE ta.type_lineup = 'offense')::numeric
      / NULLIF(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'offense'), 0)::numeric
    * 100, 1) AS off_ftr,
    ROUND(
      SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'offense')::numeric
      / NULLIF(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'offense'), 0) * 100
    , 1) AS off_ppp,
    COALESCE(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_poss,
    -- Offense raw counts
    COALESCE(SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_pts,
    COALESCE(SUM(ta.ts_poss_count) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_ts_poss,
    COALESCE(SUM(ta.oreb_count) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_oreb_cnt,
    COALESCE(SUM(ta.oreb_opportunities) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_oreb_opps,
    COALESCE(SUM(ta.tov_count) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_tov_cnt,
    COALESCE(SUM(ta.total_ft_attempts) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_fta,
    COALESCE(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_fga_cnt,
    COALESCE(SUM(ta.total_fgm) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_fgm_cnt,
    COALESCE(SUM(ta.total_fg3_made) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_fg3m_cnt,
    -- Defense rates
    ROUND(
      SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'defense')::numeric
      / (2.0 * NULLIF(SUM(ta.ts_poss_count) FILTER (WHERE ta.type_lineup = 'defense'), 0)::numeric)
    * 100, 1) AS def_ts,
    ROUND(
      (
        SUM(ta.total_fgm) FILTER (WHERE ta.type_lineup = 'defense')::numeric
        + 0.5 * SUM(ta.total_fg3_made) FILTER (WHERE ta.type_lineup = 'defense')::numeric
      )
      / NULLIF(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'defense'), 0)::numeric
    * 100, 1) AS def_efg,
    ROUND(
      SUM(ta.oreb_count) FILTER (WHERE ta.type_lineup = 'defense')::numeric
      / NULLIF(SUM(ta.oreb_opportunities) FILTER (WHERE ta.type_lineup = 'defense'), 0)::numeric
    * 100, 1) AS def_oreb,
    ROUND(
      SUM(ta.tov_count) FILTER (WHERE ta.type_lineup = 'defense')::numeric
      / NULLIF(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'defense'), 0)::numeric
    * 100, 1) AS def_tov,
    ROUND(
      SUM(ta.total_ft_attempts) FILTER (WHERE ta.type_lineup = 'defense')::numeric
      / NULLIF(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'defense'), 0)::numeric
    * 100, 1) AS def_ftr,
    ROUND(
      SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'defense')::numeric
      / NULLIF(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'defense'), 0) * 100
    , 1) AS def_ppp,
    COALESCE(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_poss,
    -- Defense raw counts
    COALESCE(SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_pts,
    COALESCE(SUM(ta.ts_poss_count) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_ts_poss,
    COALESCE(SUM(ta.oreb_count) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_oreb_cnt,
    COALESCE(SUM(ta.oreb_opportunities) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_oreb_opps,
    COALESCE(SUM(ta.tov_count) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_tov_cnt,
    COALESCE(SUM(ta.total_ft_attempts) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_fta,
    COALESCE(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_fga_cnt,
    COALESCE(SUM(ta.total_fgm) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_fgm_cnt,
    COALESCE(SUM(ta.total_fg3_made) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_fg3m_cnt
  FROM team_agg ta
  GROUP BY ta.team_id, ta.game_year
),
teams AS (
  SELECT DISTINCT full_rosters.game_year, full_rosters.team_id, full_rosters.team_name
  FROM basketball_test.full_rosters
)
SELECT
  p.team_id,
  p.game_year,
  t.team_name,
  p.off_ts, p.off_efg, p.off_oreb, p.off_tov, p.off_ftr, p.off_ppp, p.off_poss,
  p.off_pts, p.off_ts_poss, p.off_oreb_cnt, p.off_oreb_opps, p.off_tov_cnt, p.off_fta, p.off_fga_cnt, p.off_fgm_cnt, p.off_fg3m_cnt,
  p.def_ts, p.def_efg, p.def_oreb, p.def_tov, p.def_ftr, p.def_ppp, p.def_poss,
  p.def_pts, p.def_ts_poss, p.def_oreb_cnt, p.def_oreb_opps, p.def_tov_cnt, p.def_fta, p.def_fga_cnt, p.def_fgm_cnt, p.def_fg3m_cnt,
  -- Round once, from the additive counts. Subtracting two values that were
  -- each already rounded to 1dp disagreed with Ratings by 0.1 for 4 of 14
  -- teams in the 2026-08-29 audit.
  ROUND(
    100.0 * p.off_pts / NULLIF(p.off_poss, 0)
    - 100.0 * p.def_pts / NULLIF(p.def_poss, 0)
  , 1) AS net_rtg
FROM pivoted p
JOIN teams t ON t.game_year = p.game_year AND t.team_id = p.team_id
WITH DATA;

-- Indexes
CREATE INDEX idx_tffmv_gy ON basketball_test.team_four_factors_mv USING btree (game_year);
CREATE UNIQUE INDEX idx_tffmv_pk ON basketball_test.team_four_factors_mv USING btree (team_id, game_year);

GRANT SELECT ON basketball_test.team_four_factors_mv TO app_readonly;

CREATE OR REPLACE FUNCTION basketball_test.get_team_four_factors_dynamic(
    p_game_year        INT,
    p_start_date       DATE DEFAULT NULL,
    p_end_date         DATE DEFAULT NULL,
    p_game_type_csv    TEXT DEFAULT NULL,
    p_opp_team_ids_csv TEXT DEFAULT NULL,
    p_home_away        TEXT DEFAULT 'all',
    p_outcome          TEXT DEFAULT 'all',
    p_opp_rank_side    TEXT DEFAULT 'all',
    p_opp_rank_n       INT  DEFAULT NULL,
    p_opp_rank_metric  TEXT DEFAULT 'net',
    p_max_margin       INT  DEFAULT NULL,
    p_margin_status    TEXT DEFAULT 'all',
    p_max_time_remaining INT DEFAULT NULL,
    p_ot_margin_filter BOOLEAN DEFAULT FALSE,
    p_min_gn           INT DEFAULT NULL,
    p_max_gn           INT DEFAULT NULL,
    p_last_n_games     INT DEFAULT NULL,
    p_num_starters_off INT DEFAULT NULL,
    p_num_starters_def INT DEFAULT NULL,
    p_num_starters_off_min INT DEFAULT NULL,
    p_num_starters_off_max INT DEFAULT NULL,
    p_num_starters_def_min INT DEFAULT NULL,
    p_num_starters_def_max INT DEFAULT NULL
)
RETURNS TABLE (
    team_id        INT,
    game_year      INT,
    team_name      TEXT,
    off_ts         NUMERIC,
    off_efg        NUMERIC,
    off_oreb       NUMERIC,
    off_tov        NUMERIC,
    off_ftr        NUMERIC,
    off_ppp        NUMERIC,
    off_poss       INT,
    off_pts        INT,
    off_ts_poss    INT,
    off_oreb_cnt   INT,
    off_oreb_opps  INT,
    off_tov_cnt    INT,
    off_fta        INT,
    off_fga_cnt    INT,
    off_fgm_cnt    INT,
    off_fg3m_cnt   INT,
    def_ts         NUMERIC,
    def_efg        NUMERIC,
    def_oreb       NUMERIC,
    def_tov        NUMERIC,
    def_ftr        NUMERIC,
    def_ppp        NUMERIC,
    def_poss       INT,
    def_pts        INT,
    def_ts_poss    INT,
    def_oreb_cnt   INT,
    def_oreb_opps  INT,
    def_tov_cnt    INT,
    def_fta        INT,
    def_fga_cnt    INT,
    def_fgm_cnt    INT,
    def_fg3m_cnt   INT,
    net_rtg        NUMERIC
)
LANGUAGE plpgsql
STABLE
SET plan_cache_mode = force_custom_plan
AS $$
DECLARE
  v_game_types      int4[];
  v_opp_ids         int4[];
  v_home_away       text;
  v_outcome         text;
  v_opp_rank_side   text;
  v_opp_rank_metric text;
  v_margin_status   text;
  v_clutch_active   boolean;
BEGIN
  -- [Input Normalization]
  v_home_away       := COALESCE(NULLIF(btrim(p_home_away), ''), 'all');
  v_outcome         := COALESCE(NULLIF(btrim(p_outcome), ''), 'all');
  v_opp_rank_side   := COALESCE(NULLIF(btrim(p_opp_rank_side), ''), 'all');
  v_opp_rank_metric := COALESCE(NULLIF(btrim(p_opp_rank_metric), ''), 'net');
  v_margin_status   := COALESCE(NULLIF(btrim(p_margin_status), ''), 'all');
  v_clutch_active   := (p_max_margin IS NOT NULL OR v_margin_status <> 'all' OR p_max_time_remaining IS NOT NULL
                        OR p_num_starters_off IS NOT NULL OR p_num_starters_def IS NOT NULL
                        OR p_num_starters_off_min IS NOT NULL OR p_num_starters_off_max IS NOT NULL
                        OR p_num_starters_def_min IS NOT NULL OR p_num_starters_def_max IS NOT NULL);

  -- Parse CSVs
  IF p_game_type_csv IS NOT NULL AND length(btrim(p_game_type_csv)) > 0 THEN
      v_game_types := ARRAY(SELECT DISTINCT x::int4 FROM unnest(string_to_array(regexp_replace(p_game_type_csv, '\s+', '', 'g'), ',')) x WHERE x <> '' ORDER BY 1);
  END IF;

  IF p_opp_team_ids_csv IS NOT NULL AND length(btrim(p_opp_team_ids_csv)) > 0 THEN
      v_opp_ids := ARRAY(SELECT DISTINCT x::int4 FROM unnest(string_to_array(regexp_replace(p_opp_team_ids_csv, '\s+', '', 'g'), ',')) x WHERE x <> '' ORDER BY 1);
  END IF;

  IF v_clutch_active THEN
  -- ============================================================
  -- CLUTCH PATH: Inline four-factor CASE logic from raw MV
  -- ============================================================
  RETURN QUERY
  WITH
  schedule_ranked AS (
    SELECT
      fsr.game_id,
      fsr.team_id,
      fsr.game_year,
      ROW_NUMBER() OVER (
        PARTITION BY fsr.team_id, fsr.game_year
        ORDER BY fsr.game_date DESC NULLS LAST, fsr.game_id DESC
      ) AS rn_recent
    FROM basketball_test.final_schedule_mv fsr
    WHERE fsr.game_year = p_game_year
  ),
  games_base AS (
    SELECT fs.game_id, fs.team_id, fs.game_year, fs.opp_team_id
    FROM basketball_test.final_schedule_mv fs
    JOIN schedule_ranked sr
      ON sr.game_id = fs.game_id
     AND sr.team_id = fs.team_id
     AND sr.game_year = fs.game_year
    WHERE fs.game_year = p_game_year
      AND (p_start_date IS NULL OR fs.game_date >= p_start_date)
      AND (p_end_date   IS NULL OR fs.game_date <= p_end_date)
      AND (v_game_types IS NULL OR fs.game_type = ANY(v_game_types))
      AND (v_opp_ids    IS NULL OR fs.opp_team_id = ANY(v_opp_ids))
      AND (v_home_away = 'all' OR (v_home_away = 'home' AND fs.is_home) OR (v_home_away = 'away' AND NOT fs.is_home))
      AND (v_outcome = 'all'   OR (v_outcome = 'win' AND fs.has_won IS TRUE) OR (v_outcome = 'loss' AND fs.has_won IS FALSE))
      AND (p_min_gn IS NULL OR fs.gn >= p_min_gn)
      AND (p_max_gn IS NULL OR fs.gn <= p_max_gn)
      AND (p_last_n_games IS NULL OR sr.rn_recent <= p_last_n_games)
  ),
  games_ranked AS (
    SELECT gb.game_id, gb.team_id, gb.game_year,
           CASE WHEN v_opp_rank_side IN ('top', 'bottom') THEN
             CASE v_opp_rank_metric
               WHEN 'off' THEN r.rank_off_ppp
               WHEN 'def' THEN r.rank_def_ppp
               ELSE r.rank_net_rtg
             END
           ELSE NULL END AS opp_rank,
           CASE WHEN v_opp_rank_side = 'bottom' THEN
             MAX(CASE v_opp_rank_metric
                   WHEN 'off' THEN r.rank_off_ppp
                   WHEN 'def' THEN r.rank_def_ppp
                   ELSE r.rank_net_rtg
                 END) OVER (PARTITION BY gb.game_year)
           ELSE NULL END AS max_rank
    FROM games_base gb
    LEFT JOIN basketball_test.team_ppp_ratings_mv r
      ON r.game_year::integer = gb.game_year
      AND r.team_id::integer  = gb.opp_team_id
      AND v_opp_rank_side IN ('top', 'bottom')
  ),
  games_filtered AS (
    SELECT gr.game_id, gr.team_id, gr.game_year
    FROM games_ranked gr
    WHERE v_opp_rank_side = 'all' OR p_opp_rank_n IS NULL
       OR (v_opp_rank_side = 'top'    AND gr.opp_rank <= p_opp_rank_n)
       OR (v_opp_rank_side = 'bottom' AND gr.opp_rank >= (gr.max_rank - p_opp_rank_n + 1))
  ),
  -- Clutch-filtered raw data from df_pts_poss_lineups_longer_mv
  -- NOTE: Use pre-shot margin (subtract points scored from current score)
  clean_stats AS (
    SELECT
      d.id, d.game_id, d.team_id, d.team_score, d.type,
      d.parameters_type, d.parameters_made, d.parameters_points, d.pct_ft,
      d.parent_action_id, d.type_lineup,
      CASE WHEN d.final_end_poss IS TRUE THEN 1 ELSE 0 END AS final_end_flag
    FROM basketball_test.df_pts_poss_lineups_longer_mv d
    JOIN games_filtered gf ON gf.game_id = d.game_id AND gf.team_id = d.team_id
    WHERE (p_max_margin IS NULL
           OR ABS(CASE WHEN d.type_lineup = 'offense'
                       THEN (d.own_team_score - COALESCE(d.team_score, 0)) - d.opp_team_score
                       ELSE d.own_team_score - (d.opp_team_score - COALESCE(d.team_score, 0))
                  END) <= p_max_margin
           OR (d.quarter > 4 AND NOT COALESCE(p_ot_margin_filter, FALSE)))
      AND (v_margin_status = 'all'
           OR (v_margin_status = 'leading'  AND
               CASE WHEN d.type_lineup = 'offense'
                    THEN (d.own_team_score - COALESCE(d.team_score, 0)) > d.opp_team_score
                    ELSE d.own_team_score > (d.opp_team_score - COALESCE(d.team_score, 0))
               END)
           OR (v_margin_status = 'trailing' AND
               CASE WHEN d.type_lineup = 'offense'
                    THEN (d.own_team_score - COALESCE(d.team_score, 0)) < d.opp_team_score
                    ELSE d.own_team_score < (d.opp_team_score - COALESCE(d.team_score, 0))
               END)
           OR (v_margin_status = 'tied'     AND
               CASE WHEN d.type_lineup = 'offense'
                    THEN (d.own_team_score - COALESCE(d.team_score, 0)) = d.opp_team_score
                    ELSE d.own_team_score = (d.opp_team_score - COALESCE(d.team_score, 0))
               END)
           OR (d.quarter > 4 AND NOT COALESCE(p_ot_margin_filter, FALSE)))
      AND (p_max_time_remaining IS NULL OR d.end_game_seconds_remaining <= p_max_time_remaining OR d.quarter > 4)
      AND (COALESCE(p_num_starters_off_min, p_num_starters_off) IS NULL OR d.own_starters >= COALESCE(p_num_starters_off_min, p_num_starters_off))
      AND (COALESCE(p_num_starters_off_max, p_num_starters_off) IS NULL OR d.own_starters <= COALESCE(p_num_starters_off_max, p_num_starters_off))
      AND (COALESCE(p_num_starters_def_min, p_num_starters_def) IS NULL OR d.opp_starters >= COALESCE(p_num_starters_def_min, p_num_starters_def))
      AND (COALESCE(p_num_starters_def_max, p_num_starters_def) IS NULL OR d.opp_starters <= COALESCE(p_num_starters_def_max, p_num_starters_def))
  ),
  -- complex_flags scoped to clutch-filtered rows; parent foul can still precede
  -- clutch window because we only scope the child rows (clean_stats), not parent lookup.
  complex_flags AS (
    SELECT DISTINCT ON (cs.id)
      cs.id AS main_id,
      t2.type AS parent_type,
      t2.parameters_type AS parent_param
    FROM clean_stats cs
    JOIN basketball_test.df_pts_poss_lineups_longer_mv t2
      ON t2.id = cs.parent_action_id
      AND t2.game_id = cs.game_id
      AND t2.type = 'foul'::text
    WHERE cs.parent_action_id IS NOT NULL
    ORDER BY cs.id
  ),
  combined_data AS (
    SELECT
      cs.team_id,
      cs.game_id,
      p_game_year AS game_year,
      cs.type_lineup,
      cs.team_score,
      cs.final_end_flag,
      cs.type,
      cs.parameters_type,
      cs.parameters_made,
      cs.parameters_points,
      cs.pct_ft,
      cs.parent_action_id,
      cf.parent_type,
      cf.parent_param
    FROM clean_stats cs
    LEFT JOIN complex_flags cf ON cs.id = cf.main_id
  ),
  team_agg AS (
    SELECT
      cd.team_id,
      cd.game_year,
      cd.type_lineup,
      SUM(cd.team_score)       AS total_points,
      SUM(cd.final_end_flag)   AS total_poss,
      COUNT(CASE WHEN cd.type = 'shot' THEN 1 END)
        + COUNT(DISTINCT CASE
            WHEN cd.type = 'freeThrow'
              AND cd.parent_type = 'foul'
              AND cd.parent_param = 'personal'
            THEN cd.parent_action_id
          END)                 AS ts_poss_count,
      COUNT(CASE WHEN cd.type = 'rebound' AND cd.parameters_type = 'offensive' THEN 1 END) AS oreb_count,
      COUNT(CASE
        WHEN cd.type = 'shot' AND cd.parameters_made IN ('missed', 'blocked') THEN 1
        WHEN cd.type = 'freeThrow' AND cd.parameters_made = 'missed'
          AND cd.pct_ft = 1::numeric
          AND cd.parent_type = 'foul' AND cd.parent_param = 'personal' THEN 1
      END)                     AS oreb_opportunities,
      COUNT(CASE WHEN cd.type = 'turnover' THEN 1 END) AS tov_count,
      COUNT(CASE WHEN cd.type = 'freeThrow' THEN 1 END) AS total_ft_attempts,
      COUNT(CASE WHEN cd.type = 'shot' THEN 1 END) AS total_fga,
      COUNT(CASE WHEN cd.type = 'shot' AND cd.parameters_made = 'made' THEN 1 END) AS total_fgm,
      COUNT(CASE WHEN cd.type = 'shot' AND cd.parameters_made = 'made' AND cd.parameters_points = 3 THEN 1 END) AS total_fg3_made
    FROM combined_data cd
    GROUP BY cd.team_id, cd.game_year, cd.type_lineup
  ),
  pivoted AS (
    SELECT
      ta.team_id, ta.game_year,
      ROUND(SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'offense')::numeric / (2.0 * NULLIF(SUM(ta.ts_poss_count) FILTER (WHERE ta.type_lineup = 'offense'), 0)::numeric) * 100, 1) AS off_ts,
      ROUND(((SUM(ta.total_fgm) FILTER (WHERE ta.type_lineup = 'offense')::numeric) + 0.5 * (SUM(ta.total_fg3_made) FILTER (WHERE ta.type_lineup = 'offense')::numeric)) / NULLIF(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'offense'), 0)::numeric * 100, 1) AS off_efg,
      ROUND(SUM(ta.oreb_count) FILTER (WHERE ta.type_lineup = 'offense')::numeric / NULLIF(SUM(ta.oreb_opportunities) FILTER (WHERE ta.type_lineup = 'offense'), 0)::numeric * 100, 1) AS off_oreb,
      ROUND(SUM(ta.tov_count) FILTER (WHERE ta.type_lineup = 'offense')::numeric / NULLIF(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'offense'), 0)::numeric * 100, 1) AS off_tov,
      ROUND(SUM(ta.total_ft_attempts) FILTER (WHERE ta.type_lineup = 'offense')::numeric / NULLIF(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'offense'), 0)::numeric * 100, 1) AS off_ftr,
      ROUND(SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'offense')::numeric / NULLIF(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'offense'), 0) * 100, 1) AS off_ppp,
      COALESCE(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_poss,
      COALESCE(SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_pts,
      COALESCE(SUM(ta.ts_poss_count) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_ts_poss,
      COALESCE(SUM(ta.oreb_count) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_oreb_cnt,
      COALESCE(SUM(ta.oreb_opportunities) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_oreb_opps,
      COALESCE(SUM(ta.tov_count) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_tov_cnt,
      COALESCE(SUM(ta.total_ft_attempts) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_fta,
      COALESCE(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_fga_cnt,
      COALESCE(SUM(ta.total_fgm) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_fgm_cnt,
      COALESCE(SUM(ta.total_fg3_made) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_fg3m_cnt,
      ROUND(SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'defense')::numeric / (2.0 * NULLIF(SUM(ta.ts_poss_count) FILTER (WHERE ta.type_lineup = 'defense'), 0)::numeric) * 100, 1) AS def_ts,
      ROUND(((SUM(ta.total_fgm) FILTER (WHERE ta.type_lineup = 'defense')::numeric) + 0.5 * (SUM(ta.total_fg3_made) FILTER (WHERE ta.type_lineup = 'defense')::numeric)) / NULLIF(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'defense'), 0)::numeric * 100, 1) AS def_efg,
      ROUND(SUM(ta.oreb_count) FILTER (WHERE ta.type_lineup = 'defense')::numeric / NULLIF(SUM(ta.oreb_opportunities) FILTER (WHERE ta.type_lineup = 'defense'), 0)::numeric * 100, 1) AS def_oreb,
      ROUND(SUM(ta.tov_count) FILTER (WHERE ta.type_lineup = 'defense')::numeric / NULLIF(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'defense'), 0)::numeric * 100, 1) AS def_tov,
      ROUND(SUM(ta.total_ft_attempts) FILTER (WHERE ta.type_lineup = 'defense')::numeric / NULLIF(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'defense'), 0)::numeric * 100, 1) AS def_ftr,
      ROUND(SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'defense')::numeric / NULLIF(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'defense'), 0) * 100, 1) AS def_ppp,
      COALESCE(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_poss,
      COALESCE(SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_pts,
      COALESCE(SUM(ta.ts_poss_count) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_ts_poss,
      COALESCE(SUM(ta.oreb_count) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_oreb_cnt,
      COALESCE(SUM(ta.oreb_opportunities) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_oreb_opps,
      COALESCE(SUM(ta.tov_count) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_tov_cnt,
      COALESCE(SUM(ta.total_ft_attempts) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_fta,
      COALESCE(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_fga_cnt,
      COALESCE(SUM(ta.total_fgm) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_fgm_cnt,
      COALESCE(SUM(ta.total_fg3_made) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_fg3m_cnt
    FROM team_agg ta
    GROUP BY ta.team_id, ta.game_year
  ),
  team_names AS (
    SELECT fr.team_id, MIN(fr.team_name) AS team_name
    FROM basketball_test.full_rosters fr
    WHERE fr.game_year = p_game_year
    GROUP BY fr.team_id
  ),
  final_calc AS (
    SELECT
      p.team_id, p.game_year, tn.team_name,
      p.off_ts, p.off_efg, p.off_oreb, p.off_tov, p.off_ftr, p.off_ppp, p.off_poss,
      p.off_pts, p.off_ts_poss, p.off_oreb_cnt, p.off_oreb_opps, p.off_tov_cnt, p.off_fta, p.off_fga_cnt, p.off_fgm_cnt, p.off_fg3m_cnt,
      p.def_ts, p.def_efg, p.def_oreb, p.def_tov, p.def_ftr, p.def_ppp, p.def_poss,
      p.def_pts, p.def_ts_poss, p.def_oreb_cnt, p.def_oreb_opps, p.def_tov_cnt, p.def_fta, p.def_fga_cnt, p.def_fgm_cnt, p.def_fg3m_cnt,
      ROUND(
        100.0 * p.off_pts / NULLIF(p.off_poss, 0)
        - 100.0 * p.def_pts / NULLIF(p.def_poss, 0)
      , 1) AS net_rtg
    FROM pivoted p
    JOIN team_names tn ON tn.team_id = p.team_id
  )
  SELECT
    fc.team_id, fc.game_year, fc.team_name,
    fc.off_ts, fc.off_efg, fc.off_oreb, fc.off_tov, fc.off_ftr, fc.off_ppp, fc.off_poss,
    fc.off_pts, fc.off_ts_poss, fc.off_oreb_cnt, fc.off_oreb_opps, fc.off_tov_cnt, fc.off_fta, fc.off_fga_cnt, fc.off_fgm_cnt, fc.off_fg3m_cnt,
    fc.def_ts, fc.def_efg, fc.def_oreb, fc.def_tov, fc.def_ftr, fc.def_ppp, fc.def_poss,
    fc.def_pts, fc.def_ts_poss, fc.def_oreb_cnt, fc.def_oreb_opps, fc.def_tov_cnt, fc.def_fta, fc.def_fga_cnt, fc.def_fgm_cnt, fc.def_fg3m_cnt,
    fc.net_rtg
  FROM final_calc fc;

  ELSE
  -- ============================================================
  -- NON-CLUTCH PATH: Use pre-aggregated lineup_four_factors_by_game MV
  -- ============================================================
  RETURN QUERY
  WITH
  schedule_ranked AS (
    SELECT
      fsr.game_id,
      fsr.team_id,
      fsr.game_year,
      ROW_NUMBER() OVER (
        PARTITION BY fsr.team_id, fsr.game_year
        ORDER BY fsr.game_date DESC NULLS LAST, fsr.game_id DESC
      ) AS rn_recent
    FROM basketball_test.final_schedule_mv fsr
    WHERE fsr.game_year = p_game_year
  ),
  games_base AS (
    SELECT fs.game_id, fs.team_id, fs.game_year, fs.opp_team_id
    FROM basketball_test.final_schedule_mv fs
    JOIN schedule_ranked sr
      ON sr.game_id = fs.game_id
     AND sr.team_id = fs.team_id
     AND sr.game_year = fs.game_year
    WHERE fs.game_year = p_game_year
      AND (p_start_date IS NULL OR fs.game_date >= p_start_date)
      AND (p_end_date   IS NULL OR fs.game_date <= p_end_date)
      AND (v_game_types IS NULL OR fs.game_type = ANY(v_game_types))
      AND (v_opp_ids    IS NULL OR fs.opp_team_id = ANY(v_opp_ids))
      AND (v_home_away = 'all' OR (v_home_away = 'home' AND fs.is_home) OR (v_home_away = 'away' AND NOT fs.is_home))
      AND (v_outcome = 'all'   OR (v_outcome = 'win' AND fs.has_won IS TRUE) OR (v_outcome = 'loss' AND fs.has_won IS FALSE))
      AND (p_min_gn IS NULL OR fs.gn >= p_min_gn)
      AND (p_max_gn IS NULL OR fs.gn <= p_max_gn)
      AND (p_last_n_games IS NULL OR sr.rn_recent <= p_last_n_games)
  ),
  games_ranked AS (
    SELECT gb.game_id, gb.team_id, gb.game_year,
           CASE WHEN v_opp_rank_side IN ('top', 'bottom') THEN
             CASE v_opp_rank_metric
               WHEN 'off' THEN r.rank_off_ppp
               WHEN 'def' THEN r.rank_def_ppp
               ELSE r.rank_net_rtg
             END
           ELSE NULL END AS opp_rank,
           CASE WHEN v_opp_rank_side = 'bottom' THEN
             MAX(CASE v_opp_rank_metric
                   WHEN 'off' THEN r.rank_off_ppp
                   WHEN 'def' THEN r.rank_def_ppp
                   ELSE r.rank_net_rtg
                 END) OVER (PARTITION BY gb.game_year)
           ELSE NULL END AS max_rank
    FROM games_base gb
    LEFT JOIN basketball_test.team_ppp_ratings_mv r
      ON r.game_year::integer = gb.game_year
      AND r.team_id::integer  = gb.opp_team_id
      AND v_opp_rank_side IN ('top', 'bottom')
  ),
  games_filtered AS (
    SELECT gr.game_id, gr.team_id, gr.game_year
    FROM games_ranked gr
    WHERE v_opp_rank_side = 'all' OR p_opp_rank_n IS NULL
       OR (v_opp_rank_side = 'top'    AND gr.opp_rank <= p_opp_rank_n)
       OR (v_opp_rank_side = 'bottom' AND gr.opp_rank >= (gr.max_rank - p_opp_rank_n + 1))
  ),
  team_agg AS (
    SELECT
      gf.team_id,
      gf.game_year,
      lf.type_lineup,
      SUM(lf.total_points)       AS total_points,
      SUM(lf.total_poss)         AS total_poss,
      SUM(lf.ts_poss_count)      AS ts_poss_count,
      SUM(lf.oreb_count)         AS oreb_count,
      SUM(lf.oreb_opportunities) AS oreb_opportunities,
      SUM(lf.tov_count)          AS tov_count,
      SUM(lf.total_ft_attempts)  AS total_ft_attempts,
      SUM(lf.total_fga)          AS total_fga,
      SUM(lf.total_fgm)          AS total_fgm,
      SUM(lf.total_fg3_made)     AS total_fg3_made
    FROM basketball_test.lineup_four_factors_by_game lf
    JOIN games_filtered gf ON gf.game_id = lf.game_id AND gf.team_id = lf.team_id
    WHERE lf.game_year = p_game_year
      AND (
        (lf.type_lineup = 'offense'
          AND (COALESCE(p_num_starters_off_min, p_num_starters_off) IS NULL OR lf.num_starters >= COALESCE(p_num_starters_off_min, p_num_starters_off))
          AND (COALESCE(p_num_starters_off_max, p_num_starters_off) IS NULL OR lf.num_starters <= COALESCE(p_num_starters_off_max, p_num_starters_off)))
        OR
        (lf.type_lineup = 'defense'
          AND (COALESCE(p_num_starters_def_min, p_num_starters_def) IS NULL OR lf.num_starters >= COALESCE(p_num_starters_def_min, p_num_starters_def))
          AND (COALESCE(p_num_starters_def_max, p_num_starters_def) IS NULL OR lf.num_starters <= COALESCE(p_num_starters_def_max, p_num_starters_def)))
      )
    GROUP BY gf.team_id, gf.game_year, lf.type_lineup
  ),
  pivoted AS (
    SELECT
      ta.team_id, ta.game_year,
      ROUND(SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'offense')::numeric / (2.0 * NULLIF(SUM(ta.ts_poss_count) FILTER (WHERE ta.type_lineup = 'offense'), 0)::numeric) * 100, 1) AS off_ts,
      ROUND(((SUM(ta.total_fgm) FILTER (WHERE ta.type_lineup = 'offense')::numeric) + 0.5 * (SUM(ta.total_fg3_made) FILTER (WHERE ta.type_lineup = 'offense')::numeric)) / NULLIF(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'offense'), 0)::numeric * 100, 1) AS off_efg,
      ROUND(SUM(ta.oreb_count) FILTER (WHERE ta.type_lineup = 'offense')::numeric / NULLIF(SUM(ta.oreb_opportunities) FILTER (WHERE ta.type_lineup = 'offense'), 0)::numeric * 100, 1) AS off_oreb,
      ROUND(SUM(ta.tov_count) FILTER (WHERE ta.type_lineup = 'offense')::numeric / NULLIF(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'offense'), 0)::numeric * 100, 1) AS off_tov,
      ROUND(SUM(ta.total_ft_attempts) FILTER (WHERE ta.type_lineup = 'offense')::numeric / NULLIF(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'offense'), 0)::numeric * 100, 1) AS off_ftr,
      ROUND(SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'offense')::numeric / NULLIF(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'offense'), 0) * 100, 1) AS off_ppp,
      COALESCE(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_poss,
      COALESCE(SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_pts,
      COALESCE(SUM(ta.ts_poss_count) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_ts_poss,
      COALESCE(SUM(ta.oreb_count) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_oreb_cnt,
      COALESCE(SUM(ta.oreb_opportunities) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_oreb_opps,
      COALESCE(SUM(ta.tov_count) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_tov_cnt,
      COALESCE(SUM(ta.total_ft_attempts) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_fta,
      COALESCE(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_fga_cnt,
      COALESCE(SUM(ta.total_fgm) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_fgm_cnt,
      COALESCE(SUM(ta.total_fg3_made) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_fg3m_cnt,
      ROUND(SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'defense')::numeric / (2.0 * NULLIF(SUM(ta.ts_poss_count) FILTER (WHERE ta.type_lineup = 'defense'), 0)::numeric) * 100, 1) AS def_ts,
      ROUND(((SUM(ta.total_fgm) FILTER (WHERE ta.type_lineup = 'defense')::numeric) + 0.5 * (SUM(ta.total_fg3_made) FILTER (WHERE ta.type_lineup = 'defense')::numeric)) / NULLIF(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'defense'), 0)::numeric * 100, 1) AS def_efg,
      ROUND(SUM(ta.oreb_count) FILTER (WHERE ta.type_lineup = 'defense')::numeric / NULLIF(SUM(ta.oreb_opportunities) FILTER (WHERE ta.type_lineup = 'defense'), 0)::numeric * 100, 1) AS def_oreb,
      ROUND(SUM(ta.tov_count) FILTER (WHERE ta.type_lineup = 'defense')::numeric / NULLIF(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'defense'), 0)::numeric * 100, 1) AS def_tov,
      ROUND(SUM(ta.total_ft_attempts) FILTER (WHERE ta.type_lineup = 'defense')::numeric / NULLIF(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'defense'), 0)::numeric * 100, 1) AS def_ftr,
      ROUND(SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'defense')::numeric / NULLIF(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'defense'), 0) * 100, 1) AS def_ppp,
      COALESCE(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_poss,
      COALESCE(SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_pts,
      COALESCE(SUM(ta.ts_poss_count) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_ts_poss,
      COALESCE(SUM(ta.oreb_count) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_oreb_cnt,
      COALESCE(SUM(ta.oreb_opportunities) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_oreb_opps,
      COALESCE(SUM(ta.tov_count) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_tov_cnt,
      COALESCE(SUM(ta.total_ft_attempts) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_fta,
      COALESCE(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_fga_cnt,
      COALESCE(SUM(ta.total_fgm) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_fgm_cnt,
      COALESCE(SUM(ta.total_fg3_made) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_fg3m_cnt
    FROM team_agg ta
    GROUP BY ta.team_id, ta.game_year
  ),
  team_names AS (
    SELECT fr.team_id, MIN(fr.team_name) AS team_name
    FROM basketball_test.full_rosters fr
    WHERE fr.game_year = p_game_year
    GROUP BY fr.team_id
  ),
  final_calc AS (
    SELECT
      p.team_id, p.game_year, tn.team_name,
      p.off_ts, p.off_efg, p.off_oreb, p.off_tov, p.off_ftr, p.off_ppp, p.off_poss,
      p.off_pts, p.off_ts_poss, p.off_oreb_cnt, p.off_oreb_opps, p.off_tov_cnt, p.off_fta, p.off_fga_cnt, p.off_fgm_cnt, p.off_fg3m_cnt,
      p.def_ts, p.def_efg, p.def_oreb, p.def_tov, p.def_ftr, p.def_ppp, p.def_poss,
      p.def_pts, p.def_ts_poss, p.def_oreb_cnt, p.def_oreb_opps, p.def_tov_cnt, p.def_fta, p.def_fga_cnt, p.def_fgm_cnt, p.def_fg3m_cnt,
      ROUND(
        100.0 * p.off_pts / NULLIF(p.off_poss, 0)
        - 100.0 * p.def_pts / NULLIF(p.def_poss, 0)
      , 1) AS net_rtg
    FROM pivoted p
    JOIN team_names tn ON tn.team_id = p.team_id
  )
  SELECT
    fc.team_id, fc.game_year, fc.team_name,
    fc.off_ts, fc.off_efg, fc.off_oreb, fc.off_tov, fc.off_ftr, fc.off_ppp, fc.off_poss,
    fc.off_pts, fc.off_ts_poss, fc.off_oreb_cnt, fc.off_oreb_opps, fc.off_tov_cnt, fc.off_fta, fc.off_fga_cnt, fc.off_fgm_cnt, fc.off_fg3m_cnt,
    fc.def_ts, fc.def_efg, fc.def_oreb, fc.def_tov, fc.def_ftr, fc.def_ppp, fc.def_poss,
    fc.def_pts, fc.def_ts_poss, fc.def_oreb_cnt, fc.def_oreb_opps, fc.def_tov_cnt, fc.def_fta, fc.def_fga_cnt, fc.def_fgm_cnt, fc.def_fg3m_cnt,
    fc.net_rtg
  FROM final_calc fc;

  END IF;
END;
$$;
