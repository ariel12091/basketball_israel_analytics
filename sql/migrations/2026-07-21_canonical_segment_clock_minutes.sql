-- Add canonical timing alongside the untouched provider clock fields.
-- Populate with refresh_segment_clock_fields_for_games() after deploying that
-- function, then refresh minutes-dependent tables/materialized views.

ALTER TABLE basketball_test.df_pts_poss_lineups_longer_mv
  ADD COLUMN IF NOT EXISTS event_elapsed_seconds numeric,
  ADD COLUMN IF NOT EXISTS clock_regression_seconds numeric,
  ADD COLUMN IF NOT EXISTS segment_start_elapsed_seconds numeric,
  ADD COLUMN IF NOT EXISTS segment_end_elapsed_seconds numeric,
  ADD COLUMN IF NOT EXISTS segment_seconds numeric;

-- On an existing populated deployment, create the supporting index with
-- CREATE INDEX CONCURRENTLY outside a transaction before backfilling these
-- fields in game batches. The full table build also creates the index.
