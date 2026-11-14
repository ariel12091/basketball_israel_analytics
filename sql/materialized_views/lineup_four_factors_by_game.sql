-- basketball_test.lineup_four_factors_by_game: a TABLE since 2026-11-14 (was a materialized view).
-- Definition: basketball_test.compute_lineup_four_factors_by_game() (sql/functions/). Refreshed per
-- game by refresh_lineup_four_factors_by_game_for_games() from etl/etl_full.R Phase 4.
-- The _mv-style name stays so readers are unchanged.

DROP TABLE IF EXISTS basketball_test.lineup_four_factors_by_game;

CREATE TABLE basketball_test.lineup_four_factors_by_game AS
SELECT * FROM basketball_test.compute_lineup_four_factors_by_game(NULL::int4[]);

CREATE INDEX idx_lff_game_id ON basketball_test.lineup_four_factors_by_game USING btree (game_id);

CREATE INDEX idx_lff_lineup_hash ON basketball_test.lineup_four_factors_by_game USING btree (lineup_hash);

CREATE INDEX idx_lff_starters ON basketball_test.lineup_four_factors_by_game USING btree (game_year, num_starters, opp_starters, team_id, lineup_hash, type_lineup);

CREATE UNIQUE INDEX idx_lff_pk ON basketball_test.lineup_four_factors_by_game USING btree (lineup_hash, team_id, game_id, type_lineup, num_starters, opp_starters) NULLS NOT DISTINCT;

ANALYZE basketball_test.lineup_four_factors_by_game;
