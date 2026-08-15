-- 2026-08-15 — real "# Starters" on the lineup fast path
--
-- basketball_test.fetch_lineups_all returned s.num_lineup::numeric AS
-- num_starters on its fast path -- the group size, a constant -- while both of
-- its filtered branches returned the genuine possession-weighted own-starters
-- average (lines ~315 and ~485). The column therefore meant one thing when the
-- query hit sub_lineups_stats and another whenever any filter forced a dynamic
-- path, which is visible today: applying a starter filter shows 5-player units
-- with values below 5.
--
-- This adds the numerator to the stats TABLE so the fast path can compute the
-- same expression. The denominator is the existing off_poss + def_poss: the
-- average is weighted by offensive AND defensive possessions, matching the
-- filtered branches exactly. num_starters is always own-perspective on
-- df_pts_poss_lineups_longer_mv (aliased from pws.num_starters_offense on
-- offense rows and pws.num_starters_defense on defense rows), so both sides are
-- the same quantity.
--
-- sub_lineups_stats is a TABLE, not a materialized view, so ADD COLUMN
-- preserves its grants -- no re-grant is required for this statement. The three
-- accompanying functions are deployed with CREATE OR REPLACE and unchanged
-- signatures, so their EXECUTE grants survive too.
--
-- Order of operations:
--   1. this file
--   2. scripts/deploy_sql_functions.R sql/functions/refresh_sub_lineups.sql
--        sql/functions/refresh_sub_lineups_incremental.sql
--        sql/functions/fetch_lineups_all.sql
--   3. SELECT basketball_test.refresh_sub_lineups_stats();   -- backfill
--
-- Until step 3 runs, starters_poss_num is NULL and the fast path returns NULL
-- for num_starters rather than a wrong number.

ALTER TABLE basketball_test.sub_lineups_stats
  ADD COLUMN IF NOT EXISTS starters_poss_num numeric;

COMMENT ON COLUMN basketball_test.sub_lineups_stats.starters_poss_num IS
  'Sum of own starters on court over each possession, across offensive and defensive possessions. Divide by (off_poss + def_poss) for the possession-weighted average shown as "# Starters".';
