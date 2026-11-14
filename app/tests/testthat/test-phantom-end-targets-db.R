library(testthat)

# Two implementations must agree on phantom possession ends (D31): the rebound of a put-back and
# the foul turnovers the fouling team did not have the ball for. compute_possessions() writes each
# new game's ends, and the targets SQL selects the same shape from stored rows. Once the ETL rule
# has run, the SQL must find nothing in the current season. A target here means the rule failed to
# remove an end on a new game, or that a game was stored without the rule. Past seasons were
# backfilled (scripts/apply_possession_end_backfill.R --rule phantom-ends) and do not change.
# A removal can expose a further target in a rare shape (a brawl removal followed by the same team's
# later foul turnover, or a removed foul turnover before a put-back's rebound); none exists today,
# and the nightly test then fails until someone rules on it. The test sees ends the rule failed to
# remove, not ends it removed wrongly.

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

test_that("the phantom-end targets SQL finds nothing in the current season", {
  sql_file <- testthat::test_path("..", "..", "..", "sql", "migrations",
                                  "2026-11-03_phantom_possession_end_targets.sql")
  sql <- paste(readLines(sql_file), collapse = "\n")
  season <- DBI::dbGetQuery(con, "SELECT max(game_year) AS y FROM basketball_test.final_schedule_mv")$y
  targets <- DBI::dbGetQuery(con, sql, params = list(sprintf("{%d}", as.integer(season))))
  expect_identical(
    nrow(targets), 0L,
    info = paste("game/id:", paste(targets$game_id, targets$id, sep = "/", collapse = ", "))
  )
})

test_that("the ruled games hold the agreed ends", {
  # (game, id, is it an end?) on the offense row
  rows <- list(c(62, 620961, TRUE), c(62, 620962, FALSE), c(55, 550125, TRUE), c(55, 550131, FALSE),
               c(358, 3580302, TRUE), c(358, 3580307, FALSE), c(358, 3580312, FALSE),
               c(108, 1080817, TRUE), c(108, 1080820, TRUE))
  for (r in rows) {
    got <- DBI::dbGetQuery(con, "
      SELECT bool_or(coalesce(final_end_poss, FALSE)) AS is_end, count(*) AS n
      FROM basketball_test.df_pts_poss_lineups_longer_mv
      WHERE game_id = $1 AND id = $2 AND type_lineup = 'offense'",
      params = list(as.integer(r[[1]]), as.integer(r[[2]])))
    expect_equal(as.numeric(got$n) > 0, TRUE, info = paste("no offense row for game", r[[1]], "id", r[[2]]))
    expect_identical(isTRUE(got$is_end), as.logical(r[[3]]),
                     info = paste("game", r[[1]], "id", r[[2]], "should", if (r[[3]]) "be an end" else "not be an end"))
  }
})
