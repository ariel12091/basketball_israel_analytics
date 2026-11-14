-- basketball_test.mv_lineup_totals_by_day: a TABLE since 2026-11-14 (was a materialized view).
-- Definition: basketball_test.compute_mv_lineup_totals_by_day() (sql/functions/). Refreshed per
-- game by refresh_mv_lineup_totals_by_day_for_games() from etl/etl_full.R Phase 4.
-- The _mv-style name stays so readers are unchanged.

DROP TABLE IF EXISTS basketball_test.mv_lineup_totals_by_day;

CREATE TABLE basketball_test.mv_lineup_totals_by_day AS
SELECT * FROM basketball_test.compute_mv_lineup_totals_by_day(NULL::int4[]);

CREATE INDEX idx_mv_ltotals_day_game_id ON basketball_test.mv_lineup_totals_by_day USING btree (game_id);

CREATE INDEX idx_mv_ltotals_day_date ON basketball_test.mv_lineup_totals_by_day USING btree (g_date, lineup_hash, type_lineup);

CREATE INDEX idx_mv_ltotals_day_starters ON basketball_test.mv_lineup_totals_by_day USING btree (game_year, num_starters, opp_starters, team_id, lineup_hash, type_lineup);

CREATE UNIQUE INDEX idx_mv_ltotals_day_pk ON basketball_test.mv_lineup_totals_by_day USING btree (lineup_hash, type_lineup, g_date, num_starters, opp_starters) NULLS NOT DISTINCT;

ANALYZE basketball_test.mv_lineup_totals_by_day;
