DROP TABLE IF EXISTS basketball_test.player_stats_actions_by_game;

CREATE TABLE basketball_test.player_stats_actions_by_game AS
SELECT *
FROM basketball_test.compute_player_stats_actions_by_game(NULL::int4[]);

CREATE UNIQUE INDEX player_stats_actions_by_game_pk
  ON basketball_test.player_stats_actions_by_game
  (game_id, team_id, action_id);

CREATE INDEX player_stats_actions_by_game_filter_idx
  ON basketball_test.player_stats_actions_by_game
  (game_year, game_id, team_id, is_overtime,
   regulation_seconds_remaining, pre_abs_margin, pre_status);

ANALYZE basketball_test.player_stats_actions_by_game;
