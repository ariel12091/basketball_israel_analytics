library(testthat)

# Two implementations must agree on a made and-one the team rebounds itself (D30):
# compute_possessions() writes each new game's ends, and the targets SQL selects the
# same shape from stored rows. Once the ETL rule has run, the SQL must find nothing in
# the current season. A target here means the two disagree on a new game, or that a
# game was stored without the rule. Past seasons were backfilled and do not change.

skip_if_not(Sys.getenv("RUN_DB_TESTS", "0") == "1")
skip_if_not_installed("DBI")
skip_if_not_installed("RPostgres")

pg_env <- c("PG_HOST", "PG_PORT", "PG_DB", "PG_USER", "PG_PASS")
missing_env <- pg_env[!nzchar(Sys.getenv(pg_env))]
if (length(missing_env)) {
  skip(paste("Missing DB env:", paste(missing_env, collapse = ", ")))
}

con <- db_test_connect(
  RPostgres::Postgres(),
  host = Sys.getenv("PG_HOST"),
  port = as.integer(Sys.getenv("PG_PORT")),
  dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE", unset = "require"),
  bigint = "numeric"
)
on.exit(DBI::dbDisconnect(con), add = TRUE)

test_that("the and-one targets SQL finds nothing in the current season", {
  sql_file <- testthat::test_path("..", "..", "..", "sql", "migrations",
                                  "2026-11-03_and_one_possession_targets.sql")
  sql <- paste(readLines(sql_file), collapse = "\n")
  season <- DBI::dbGetQuery(con, "SELECT max(game_year) AS y FROM basketball_test.final_schedule_mv")$y
  targets <- DBI::dbGetQuery(con, sql, params = list(sprintf("{%d}", as.integer(season))))
  expect_identical(
    nrow(targets), 0L,
    info = paste("game/shot:", paste(targets$game_id, targets$id, sep = "/", collapse = ", "))
  )
})
