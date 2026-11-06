-- Game Logs readers for the web app (plan Task 5).
--
-- Requires the publication markers (sql/publication/001_publication.sql,
-- deployed by scripts/apply_publication.R) -- apply those first.
--
-- Each reader returns ONE jsonb envelope built by ONE statement (review R6,
-- gap G7): the rows, their completeness controls (row_count, ctl_poss -- the
-- count and possession total of the very rows returned) and the league's
-- (state, version) from publication_status(). Functions here are STABLE, so
-- every relation read, including the nested publication_status() and
-- scoreless_games_il() calls, sees the calling statement's snapshot: the
-- markers can never describe different data than the rows. An empty season
-- still returns the envelope, with rows = [] and zero controls.
--
-- Sources of truth (Shiny). Every column below must exist; the catalogue test
-- test-app-api-sql-catalog-db.R deploys these files into a scratch schema and
-- calls them, and LANGUAGE sql bodies are validated at CREATE.
--   game_logs_il       gl_query_schedule / gl_query_lineup_totals /
--                      gl_query_lineup_ff (app/R/svc_game_logs.R)
--   scoreless_games_il fetch_scoreless_games (app/R/global.R)
--   game_logs_euro     euro_fetch_game_log_rows (app/R/global_euro.R)
--
-- R / TypeScript own derived rates and percentiles; nothing is derived here.
--
-- CONSUMERS of game_logs_il: its rows FULL JOIN the Summary and Four Factors
-- facts, so a row can carry only one view's columns (the other side all
-- NULL) where a key exists in one fact and not the other. Shiny reads each
-- view's fact on its own, so a consumer must keep only the rows that carry
-- the active view's side (Summary: total_poss .. minutes; Four Factors:
-- ff_total_poss .. ff_minutes) and treat a NULL sum within that side as 0
-- (SQL sum() of an all-NULL group is NULL where Shiny's na.rm sum is 0).

-- The (game, team) pairs whose lineup rows hold no score at all, which Game
-- Logs shows without a score. Shiny's query is the same aggregate without a
-- season predicate; df_pts_poss_lineups_longer_mv has no game_year, so the
-- season is the set of final_schedule_mv game ids (game ids are unique across
-- seasons, so the flags on a season's games are identical to Shiny's).
CREATE OR REPLACE FUNCTION app_api.scoreless_games_il(p_season integer)
RETURNS TABLE(game_id integer, team_id integer)
LANGUAGE sql STABLE
SET search_path = '' SET plan_cache_mode = force_custom_plan
AS $fn$
  SELECT d.game_id, d.team_id
  FROM basketball_test.df_pts_poss_lineups_longer_mv d
  WHERE d.game_id IN (
    SELECT s.game_id FROM basketball_test.final_schedule_mv s WHERE s.game_year = p_season
  )
  GROUP BY d.game_id, d.team_id
  HAVING count(d.own_team_score) = 0
$fn$;

-- Israeli season: the schedule rows as Shiny reads them, the Summary and Four
-- Factors facts summed to (game_id, team_id, type_lineup, num_starters) --
-- the only keys Game Logs filters or groups on -- and the scoreless pairs.
-- The two facts are summed BEFORE they are joined, so neither duplicates the
-- other; FF columns that share a Summary name carry an ff_ prefix.
CREATE OR REPLACE FUNCTION app_api.game_logs_il(p_season integer)
RETURNS jsonb
LANGUAGE sql STABLE
SET search_path = '' SET plan_cache_mode = force_custom_plan
AS $fn$
WITH schedule AS MATERIALIZED (
  SELECT s.*
  FROM basketball_test.final_schedule_mv s
  WHERE s.game_year = p_season
), summary AS MATERIALIZED (
  SELECT t.game_id, t.team_id, t.type_lineup, t.num_starters,
    sum(t.total_poss) AS total_poss, sum(t.total_pts) AS total_pts,
    sum(t.fg2_made) AS fg2_made, sum(t.fg2_att) AS fg2_att,
    sum(t.fg3_made) AS fg3_made, sum(t.fg3_att) AS fg3_att,
    sum(t.minutes) AS minutes
  FROM basketball_test.mv_lineup_totals_by_day t
  WHERE t.game_year = p_season
  GROUP BY t.game_id, t.team_id, t.type_lineup, t.num_starters
), ff AS MATERIALIZED (
  SELECT f.game_id, f.team_id, f.type_lineup, f.num_starters,
    sum(f.total_poss) AS ff_total_poss, sum(f.total_points) AS total_points,
    sum(f.ts_poss_count) AS ts_poss_count, sum(f.oreb_count) AS oreb_count,
    sum(f.oreb_opportunities) AS oreb_opportunities, sum(f.tov_count) AS tov_count,
    sum(f.total_ft_attempts) AS total_ft_attempts, sum(f.total_fga) AS total_fga,
    sum(f.total_fgm) AS total_fgm, sum(f.total_fg3_made) AS total_fg3_made,
    sum(f.minutes) AS ff_minutes
  FROM basketball_test.lineup_four_factors_by_game f
  WHERE f.game_year = p_season
  GROUP BY f.game_id, f.team_id, f.type_lineup, f.num_starters
), fact AS MATERIALIZED (
  SELECT coalesce(a.game_id, b.game_id) AS game_id,
    coalesce(a.team_id, b.team_id) AS team_id,
    coalesce(a.type_lineup, b.type_lineup) AS type_lineup,
    coalesce(a.num_starters, b.num_starters) AS num_starters,
    a.total_poss, a.total_pts, a.fg2_made, a.fg2_att, a.fg3_made, a.fg3_att, a.minutes,
    b.ff_total_poss, b.total_points, b.ts_poss_count, b.oreb_count, b.oreb_opportunities,
    b.tov_count, b.total_ft_attempts, b.total_fga, b.total_fgm, b.total_fg3_made,
    b.ff_minutes
  FROM summary a
  FULL JOIN ff b
    ON a.game_id = b.game_id AND a.team_id = b.team_id
   AND a.type_lineup IS NOT DISTINCT FROM b.type_lineup
   AND a.num_starters IS NOT DISTINCT FROM b.num_starters
), scoreless AS MATERIALIZED (
  SELECT z.game_id, z.team_id FROM app_api.scoreless_games_il(p_season) z
), envelope AS (
  SELECT
    (SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY r.game_id, r.team_id, r.type_lineup,
                               r.num_starters), '[]'::jsonb) FROM fact r) AS rows,
    (SELECT count(*) FROM fact) AS row_count,
    (SELECT coalesce(sum(r.total_poss), 0) FROM fact r) AS ctl_poss,
    (SELECT coalesce(sum(r.ff_total_poss), 0) FROM fact r) AS ctl_ff_poss,
    (SELECT coalesce(jsonb_agg(to_jsonb(s) ORDER BY s.game_id, s.team_id), '[]'::jsonb)
       FROM schedule s) AS schedule,
    (SELECT count(*) FROM schedule) AS schedule_count,
    (SELECT coalesce(jsonb_agg(to_jsonb(z) ORDER BY z.game_id, z.team_id), '[]'::jsonb)
       FROM scoreless z) AS scoreless
)
SELECT jsonb_build_object(
  'league', 'il', 'season', p_season,
  'state', p.state, 'version', p.version,
  'row_count', e.row_count, 'ctl_poss', e.ctl_poss, 'ctl_ff_poss', e.ctl_ff_poss,
  'schedule_count', e.schedule_count,
  'rows', e.rows, 'schedule', e.schedule, 'scoreless', e.scoreless)
FROM envelope e
LEFT JOIN LATERAL basketball_test.publication_status('il') p ON true
$fn$;

-- EuroLeague / EuroCup season: the native rows, exactly what
-- euro_fetch_game_log_rows() returns -- one row per team_four_factors_by_game
-- record with own_starters and opp_starters, off_/def_ counts side by side,
-- schedule columns and minutes -- with no aggregation and no offense/defense
-- expansion, because Shiny filters these native rows before expanding them
-- (review R10). Row order is not part of the contract (D24 fixed).
--
-- plpgsql only to fail closed on an unknown competition (security rule 4):
-- a LANGUAGE sql body cannot raise, and would return no publication state.
-- The single RETURN query still runs on the calling statement's snapshot.
CREATE OR REPLACE FUNCTION app_api.game_logs_euro(p_competition text, p_season integer)
RETURNS jsonb
LANGUAGE plpgsql STABLE
SET search_path = '' SET plan_cache_mode = force_custom_plan
AS $fn$
DECLARE
  v_league text := CASE p_competition WHEN 'E' THEN 'euroleague' WHEN 'U' THEN 'eurocup' END;
BEGIN
  IF v_league IS NULL THEN
    RAISE EXCEPTION 'unknown competition: %', p_competition USING ERRCODE = '22023';
  END IF;
  RETURN (
    WITH fact AS MATERIALIZED (
      SELECT f.*, s.round_number, s.phase, s.game_date, s.opp_team_id,
             s.is_home, s.has_won, s.team_points, s.opp_points,
             s.team_name, s.opp_team_name,
             m.seconds / 60.0 AS minutes
      FROM euroleague.team_four_factors_by_game f
      JOIN euroleague.final_schedule_mv s
        ON s.game_id = f.game_id AND s.team_id = f.team_id
      LEFT JOIN (
        SELECT t.game_id, t.team_id, t.own_starters, t.opp_starters,
               sum(t.seconds) AS seconds
        FROM euroleague.lineup_totals_by_game t
        WHERE t.type_lineup = 'offense'
          AND t.competition = p_competition AND t.game_year = p_season
        GROUP BY t.game_id, t.team_id, t.own_starters, t.opp_starters
      ) m
        ON m.game_id = f.game_id AND m.team_id = f.team_id
       AND m.own_starters = f.own_starters
       AND m.opp_starters = f.opp_starters
      WHERE s.competition = p_competition AND s.game_year = p_season
    ), envelope AS (
      SELECT
        (SELECT coalesce(jsonb_agg(to_jsonb(r) ORDER BY r.team_id, r.game_date DESC,
                                   r.game_id DESC, r.own_starters, r.opp_starters),
                         '[]'::jsonb) FROM fact r) AS rows,
        (SELECT count(*) FROM fact) AS row_count,
        (SELECT coalesce(sum(r.off_poss), 0) FROM fact r) AS ctl_poss
    )
    SELECT jsonb_build_object(
      'league', v_league, 'competition', p_competition, 'season', p_season,
      'state', p.state, 'version', p.version,
      'row_count', e.row_count, 'ctl_poss', e.ctl_poss,
      'rows', e.rows, 'scoreless', '[]'::jsonb)
    FROM envelope e
    LEFT JOIN LATERAL basketball_test.publication_status(v_league) p ON true
  );
END
$fn$;

REVOKE ALL ON FUNCTION app_api.scoreless_games_il(integer), app_api.game_logs_il(integer),
  app_api.game_logs_euro(text, integer) FROM PUBLIC, anon, authenticated;
