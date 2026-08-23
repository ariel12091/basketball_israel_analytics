DROP TABLE IF EXISTS basketball_test.default_clutch_player_totals_by_game;

CREATE TABLE basketball_test.default_clutch_player_totals_by_game AS
SELECT *
FROM basketball_test.compute_player_traditional_by_game(NULL::int4[], TRUE);

CREATE UNIQUE INDEX default_clutch_player_totals_by_game_pk
  ON basketball_test.default_clutch_player_totals_by_game
  (game_year, game_id, team_id, player_id);

CREATE INDEX default_clutch_player_totals_by_game_filter_idx
  ON basketball_test.default_clutch_player_totals_by_game
  (game_year, team_id, game_id, player_id);

ANALYZE basketball_test.default_clutch_player_totals_by_game;
