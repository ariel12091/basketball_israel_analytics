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

test_that("team Four Factors counts each free-throw trip once", {
  bad <- DBI::dbGetQuery(con, "
    WITH expected AS (
      SELECT a.game_id, a.team_id,
             COUNT(*) FILTER (WHERE a.type = 'shot')
             + COUNT(DISTINCT a.parent_action_id) FILTER (
                 WHERE a.type = 'freeThrow' AND p.id IS NOT NULL) AS ts_poss
      FROM basketball_test.df_pts_poss_lineups_longer_mv a
      LEFT JOIN basketball_test.df_pts_poss_lineups_longer_mv p
        ON p.game_id = a.game_id AND p.id = a.parent_action_id
       AND p.type = 'foul' AND p.parameters_type = 'personal'
       AND p.team_id = a.team_id AND p.type_lineup = a.type_lineup
      WHERE a.type_lineup = 'offense'
      GROUP BY a.game_id, a.team_id
    )
    SELECT COUNT(*) AS n
    FROM basketball_test.team_metrics_by_game_mv tm
    JOIN expected e ON e.game_id = tm.game_id AND e.team_id = tm.team_id
    WHERE tm.off_ts_poss_raw <> e.ts_poss")
  expect_equal(as.numeric(bad$n), 0)
})
