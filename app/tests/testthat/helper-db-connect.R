# Every live-database test connects through db_test_connect(), never
# DBI::dbConnect() directly (test-db-test-connect.R enforces it).
#
# Why: from about 2026-11-10 the nightly suite ran a Game Flow query that
# scanned every EuroLeague game (float8 ids, see
# docs/verification/supabase_disk_io_2026-11-13.md) 15 times a night at
# ~55 s each, and stayed green while it used up Supabase's disk I/O budget.
# A test connection had no statement timeout, so a slow plan could not fail.
#
# 30 s: the slowest legitimate app_readonly statement in pg_stat_statements
# (2026-11-03 .. 11-14) peaked at 17.7 s, under load. A test that needs longer
# should set its own timeout after connecting and say why.
DB_TEST_STATEMENT_TIMEOUT <- "30s"

db_test_connect <- function(drv, ...) {
  con <- DBI::dbConnect(drv, ...)
  DBI::dbExecute(con, sprintf("SET statement_timeout = '%s'",
                              DB_TEST_STATEMENT_TIMEOUT))
  con
}
