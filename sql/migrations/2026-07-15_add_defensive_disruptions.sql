-- Store defensive activity event counts in the player on/off game aggregate.
-- Deploy the refreshed function definition, then backfill all rows:
--   SELECT basketball_test.refresh_player_four_factors_by_game_for_games(NULL);

ALTER TABLE basketball_test.player_four_factors_by_game
  ADD COLUMN IF NOT EXISTS steal_count bigint NOT NULL DEFAULT 0,
  ADD COLUMN IF NOT EXISTS deflection_count bigint NOT NULL DEFAULT 0;

ALTER TABLE basketball_test.player_advanced_stats_mv
  ADD COLUMN IF NOT EXISTS def_on_disruptions numeric,
  ADD COLUMN IF NOT EXISTS def_off_disruptions numeric,
  ADD COLUMN IF NOT EXISTS "Def Disruptions/100 Diff" numeric;
