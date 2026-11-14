-- basketball_test.player_traditional_stats_mv: a TABLE since 2026-11-14 (was a materialized view).
-- Definition: basketball_test.compute_player_traditional_stats() (sql/functions/). Refreshed per
-- season by refresh_player_traditional_stats_for_games() from etl/etl_full.R Phase 4.
-- The _mv-style name stays so readers are unchanged.

DROP TABLE IF EXISTS basketball_test.player_traditional_stats_mv;

CREATE TABLE basketball_test.player_traditional_stats_mv AS
SELECT * FROM basketball_test.compute_player_traditional_stats(NULL::int4[]);

CREATE UNIQUE INDEX player_traditional_stats_mv_uq ON basketball_test.player_traditional_stats_mv USING btree (game_year, team_id, player_id);

CREATE INDEX player_traditional_stats_mv_year_team_name_idx ON basketball_test.player_traditional_stats_mv USING btree (game_year, team_name);

ANALYZE basketball_test.player_traditional_stats_mv;
