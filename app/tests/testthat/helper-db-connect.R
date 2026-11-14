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

# Which seasons a live test covers. The nightly run checks the current season:
# its data changes every night. A finished season changes only when code or
# SQL does, so the weekly full run (DB_TEST_SCOPE=full, Sundays, set in
# r-tests.yml) checks it as well. Until 2026-11-14 every night re-checked the
# finished 2025-26 season too: about 2 GB of Supabase temp writes a night.
db_test_full_scope <- function() identical(Sys.getenv("DB_TEST_SCOPE"), "full")

# Israeli seasons are season-ending years; the weekly run adds the finished ones.
DB_TEST_FINISHED_SEASONS <- 2026L
db_test_seasons <- function() {
  current <- as.integer(global_defs("DEFAULT_GAME_YEAR")$DEFAULT_GAME_YEAR)
  if (db_test_full_scope()) sort(unique(c(DB_TEST_FINISHED_SEASONS, current))) else current
}

# EuroLeague provider seasons are starting years (euro_default_season()).
DB_TEST_FINISHED_EURO_SEASONS <- 2025L
db_test_euro_seasons <- function() {
  current <- as.integer(global_defs("DEFAULT_GAME_YEAR")$DEFAULT_GAME_YEAR) - 1L
  if (db_test_full_scope()) sort(unique(c(DB_TEST_FINISHED_EURO_SEASONS, current))) else current
}
