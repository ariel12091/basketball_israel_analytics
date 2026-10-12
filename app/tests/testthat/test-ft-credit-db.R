library(testthat)

# Free throws are credited to the lineup that drew the foul. The base table
# records that in credit_* columns next to the physical lineup columns; every
# aggregation of events groups on the credit columns, floor time on the
# physical ones. See docs/superpowers/plans/2026-10-12-free-throw-foul-lineup-attribution.md.

skip_if_not(Sys.getenv("RUN_DB_TESTS", "0") == "1")
skip_if_not_installed("DBI")
skip_if_not_installed("RPostgres")

pg_env <- c("PG_HOST", "PG_PORT", "PG_DB", "PG_USER", "PG_PASS")
missing_env <- pg_env[!nzchar(Sys.getenv(pg_env))]
if (length(missing_env)) {
  skip(paste("Missing DB env:", paste(missing_env, collapse = ", ")))
}

con <- DBI::dbConnect(
  RPostgres::Postgres(),
  host = Sys.getenv("PG_HOST"),
  port = as.integer(Sys.getenv("PG_PORT")),
  dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE", unset = "require")
)
on.exit(DBI::dbDisconnect(con), add = TRUE)

test_that("every base-table row carries credit columns", {
  bad <- DBI::dbGetQuery(con, "
    SELECT COUNT(*) AS n FROM basketball_test.df_pts_poss_lineups_longer_mv
    WHERE credit_lineup_hash IS NULL OR credit_segment_id IS NULL")
  expect_equal(as.numeric(bad$n), 0)
})

test_that("all free throws of one trip are credited to one segment", {
  split <- DBI::dbGetQuery(con, "
    SELECT COUNT(*) AS n FROM (
      SELECT 1 FROM basketball_test.df_pts_poss_lineups_longer_mv
      WHERE type = 'freeThrow'
      GROUP BY game_id, team_id, type_lineup, parent_action_id
      HAVING COUNT(DISTINCT (credit_lineup_hash, credit_segment_id)) > 1) x")
  expect_equal(as.numeric(split$n), 0)
})
