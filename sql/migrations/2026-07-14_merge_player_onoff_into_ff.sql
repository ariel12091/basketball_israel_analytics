-- Merge basketball_test.player_onoff_by_game payload into
-- basketball_test.player_four_factors_by_game.
--
-- Apply sequence:
-- 1. Add the merged on/off payload columns to the existing physical table.
-- 2. Deploy sql/functions/refresh_player_four_factors_by_game_for_games.sql.
-- 3. Run a full refresh with refresh_player_four_factors_by_game_for_games(NULL).
-- 4. Run parity gates against the still-live player_onoff_by_game MV.
-- 5. Deploy sql/functions/onoff_compute.sql with CREATE OR REPLACE only.
-- 6. Re-run baseline output diffs.
-- 7. Drop the obsolete MV.
-- 8. Rebuild the season-safe primary keys.

ALTER TABLE basketball_test.player_four_factors_by_game
  ADD COLUMN IF NOT EXISTS fg2_made int,
  ADD COLUMN IF NOT EXISTS fg2_att int,
  ADD COLUMN IF NOT EXISTS fg3_made int,
  ADD COLUMN IF NOT EXISTS fg3_att int,
  ADD COLUMN IF NOT EXISTS onoff_minutes numeric;

-- Deploy:
--   sql/functions/refresh_player_four_factors_by_game_for_games.sql
-- Then:
--   SELECT basketball_test.refresh_player_four_factors_by_game_for_games(NULL);

-- After parity gates and onoff_compute output diffs pass:
-- Deploy sql/functions/onoff_compute.sql using only the CREATE OR REPLACE statement.

DROP MATERIALIZED VIEW basketball_test.player_onoff_by_game;

BEGIN;
ALTER TABLE basketball_test.lineups_lookup_on
  DROP CONSTRAINT lineups_lookup_on_pkey,
  ADD CONSTRAINT lineups_lookup_on_pkey
    PRIMARY KEY (player_id, lineup_hash, team_id, game_year);
COMMIT;

BEGIN;
ALTER TABLE basketball_test.sub_lineups
  DROP CONSTRAINT sub_lineups_pkey,
  ADD CONSTRAINT sub_lineups_pkey
    PRIMARY KEY (team_id, lineup_hash, sub_lineup_hash, game_year);
COMMIT;
