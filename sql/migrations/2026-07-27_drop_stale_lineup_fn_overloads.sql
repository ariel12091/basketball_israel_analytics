-- Drop obsolete lineup-function overloads (finding 3 of the 2026-07-27 perf
-- review; also item 3 of the 2026-07-22 catalog audit in database_context.md).
-- Older 23-arg and 25-arg generations of fetch_lineups_all and
-- fetch_lineups_csv_v2 remained deployed alongside the current 29-arg
-- versions. Both app callers (app/R/server_tab2.R, frontend-v2/server/
-- plumber.R) pass all 29 args positionally, and fetch_lineups_csv_v2 passes
-- all 29 through to fetch_lineups_all — the old signatures are unreachable
-- from the app but make sparse named-arg calls fail with "function is not
-- unique" and could silently serve an outdated return contract to manual
-- callers. fetch_lineups_four_factors / _csv have no stale overloads.

DROP FUNCTION IF EXISTS basketball_test.fetch_lineups_all(
  smallint, integer[], integer[], integer[], boolean, date, date, integer,
  integer, text, text, text, text, text, integer, text, integer, text,
  integer, boolean, integer, integer, integer);

DROP FUNCTION IF EXISTS basketball_test.fetch_lineups_all(
  smallint, integer[], integer[], integer[], boolean, date, date, integer,
  integer, text, text, text, text, text, integer, text, integer, text,
  integer, boolean, integer, integer, integer, integer, integer);

DROP FUNCTION IF EXISTS basketball_test.fetch_lineups_csv_v2(
  integer, text, text, text, boolean, date, date, integer,
  integer, text, text, text, text, text, integer, text, integer, text,
  integer, boolean, integer, integer, integer);

DROP FUNCTION IF EXISTS basketball_test.fetch_lineups_csv_v2(
  integer, text, text, text, boolean, date, date, integer,
  integer, text, text, text, text, text, integer, text, integer, text,
  integer, boolean, integer, integer, integer, integer, integer);
