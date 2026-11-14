library(testthat)

# Three Israeli tables are refreshed per game or season since 2026-11-14
# (scripts/apply_il_per_game_tables.R); each one's definition is a compute_*
# function. For the latest processed games the stored rows must equal what the
# function returns now. A failure means the table missed a refresh (e.g. base
# data corrected outside the ETL: run the ETL with game_ids=) or the function
# and the table's history diverged. Cheap: two games and one season.

skip_if_not(identical(Sys.getenv("RUN_DB_TESTS"), "1"), "RUN_DB_TESTS not enabled")
pg_env <- c("PG_HOST", "PG_DB", "PG_USER", "PG_PASS")
skip_if_not(all(nzchar(Sys.getenv(pg_env))), "no database configured")

con <- db_test_connect(RPostgres::Postgres(), host = Sys.getenv("PG_HOST"),
  port = 5432L, dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE", "require"),
  bigint = "numeric")
on.exit(DBI::dbDisconnect(con), add = TRUE)

latest <- DBI::dbGetQuery(con, "SELECT game_id FROM basketball_test.etl_processed_games
  ORDER BY processed_at DESC, game_id DESC LIMIT 2")$game_id
ids <- sprintf("ARRAY[%s]::int4[]", paste(as.integer(latest), collapse = ","))
season <- DBI::dbGetQuery(con, sprintf("SELECT max(game_year) AS gy FROM basketball_test.final_schedule_mv
  WHERE game_id = ANY (%s)", ids))$gy

differs <- function(stored, computed) {
  n <- function(a, b) as.numeric(DBI::dbGetQuery(con, sprintf(
    "SELECT count(*) AS n FROM ((%s) EXCEPT ALL (%s)) x", a, b))$n)
  c(n(stored, computed), n(computed, stored))
}

test_that("the per-game tables equal their compute functions for the latest games", {
  expect_length(latest, 2)
  for (tbl in c("lineup_four_factors_by_game", "mv_lineup_totals_by_day")) {
    kind <- DBI::dbGetQuery(con, sprintf("SELECT relkind FROM pg_class WHERE oid = 'basketball_test.%s'::regclass", tbl))$relkind
    expect_identical(kind, "r", label = paste(tbl, "relkind"))
    gap <- differs(sprintf("SELECT * FROM basketball_test.%s WHERE game_id = ANY (%s)", tbl, ids),
                   sprintf("SELECT * FROM basketball_test.compute_%s(%s)", tbl, ids))
    expect_identical(gap, c(0, 0), label = paste(tbl, "stored-computed, computed-stored"))
  }
})

test_that("the season table equals its compute function for the latest season", {
  gap <- differs(sprintf("SELECT * FROM basketball_test.player_traditional_stats_mv WHERE game_year = %d", season),
                 sprintf("SELECT * FROM basketball_test.compute_player_traditional_stats(ARRAY[%d]::int4[])", season))
  expect_identical(gap, c(0, 0), label = paste(season, "stored-computed, computed-stored"))
})
