library(testthat)

# basket_player_registration has two writers -- the stats pages
# (backfill_basket_shooting_history.R) and the team pages (basket_rosters.R) --
# that share registration_upsert_sql(). Whatever order they run in, a stats
# row wins and the identity link the matcher wrote survives. Runs in one
# transaction that is rolled back, on a season no real row uses (1991).
#
# Needs a role that can write the table (the ETL role, etl/.Renviron) and
# RUN_DB_TESTS=1; the nightly app_readonly run skips it.

skip_if_not(identical(Sys.getenv("RUN_DB_TESTS"), "1"))
skip_if_not_installed("DBI")
skip_if_not_installed("RPostgres")
source(file.path("..", "..", "scripts", "backfill_basket_shooting_history.R"), local = TRUE)

con <- DBI::dbConnect(RPostgres::Postgres(), host = Sys.getenv("PG_HOST"),
  port = as.integer(Sys.getenv("PG_PORT")), dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"), password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE", unset = "require"), bigint = "numeric")
on.exit(DBI::dbDisconnect(con), add = TRUE)
can_write <- DBI::dbGetQuery(con, sprintf(
  "SELECT has_table_privilege('%s', 'INSERT') AS ok", BASKET_REGISTRATION_TABLE))$ok
skip_if_not(isTRUE(can_write), "role cannot write basket_player_registration")

test_that("a stats row wins and the identity link survives, in either writer order", {
  q <- function(sql, ...) DBI::dbGetQuery(con, sql, ...)
  x <- function(sql, ...) DBI::dbExecute(con, sql, ...)
  x("BEGIN")
  on.exit(x("ROLLBACK"), add = TRUE)
  run_id <- q("SELECT min(run_id) AS r FROM basketball_test.basket_shooting_import_runs")$r
  identity <- q(paste("SELECT identity_id, canonical_player_id FROM", BASKET_REGISTRATION_TABLE,
                      "WHERE identity_match_status = 'verified' LIMIT 1"))
  x(paste0("CREATE TEMP TABLE stage_stats (LIKE ", BASKET_REGISTRATION_TABLE, " INCLUDING DEFAULTS) ON COMMIT DROP"))
  x(paste0("CREATE TEMP TABLE stage_roster (LIKE ", BASKET_REGISTRATION_TABLE, " INCLUDING DEFAULTS) ON COMMIT DROP"))
  x(paste("INSERT INTO stage_stats (game_year, basket_player_id, source, player_name, player_name_en,",
          "source_url, source_content_md5, fetched_at, run_id) VALUES",
          "(1991, 1, 'stats', 'א סטטס', 'A Stats', 'u-stats', md5('s'), now(), $1)"),
    params = list(run_id))
  x(paste("INSERT INTO stage_roster (game_year, basket_player_id, source, player_name, player_name_en,",
          "basket_team_id, height_m, source_url, source_content_md5, fetched_at) VALUES",
          "(1991, 1, 'roster', 'א רוסטר', NULL, 2111, 2.01, 'u-team', md5('r'), now()),",
          "(1991, 2, 'roster', 'ב רוסטר', NULL, 2111, 1.90, 'u-team', md5('r'), now())"))
  stats_cols <- c("game_year", "basket_player_id", "player_name", "player_name_en",
                  "source_url", "source_content_md5", "fetched_at", "run_id")
  roster_cols <- c("game_year", "basket_player_id", "player_name", "player_name_en",
                   "basket_team_id", "height_m", "source_url", "source_content_md5", "fetched_at")
  run_order <- function(writers) {
    x("SAVEPOINT order_start")
    # Player A was a roster-only player the matcher had already linked.
    x(paste("INSERT INTO", BASKET_REGISTRATION_TABLE, "(game_year, basket_player_id, source,",
            "player_name, source_url, source_content_md5, fetched_at, identity_id,",
            "canonical_player_id, identity_match_status, identity_matched_on)",
            "VALUES (1991, 1, 'roster', 'א רוסטר', 'u-team', md5('r'), now(), $1, $2,",
            "'verified', 'name_he')"),
      params = list(identity$identity_id, identity$canonical_player_id))
    for (w in writers) {
      if (w == "stats") x(registration_upsert_sql("stats", "stage_stats", stats_cols))
      else x(registration_upsert_sql("roster", "stage_roster", roster_cols))
    }
    out <- q(paste("SELECT basket_player_id, source, player_name, player_name_en, height_m,",
                   "source_url, run_id, identity_id FROM", BASKET_REGISTRATION_TABLE,
                   "WHERE game_year = 1991 ORDER BY basket_player_id"))
    x("ROLLBACK TO SAVEPOINT order_start")
    out
  }
  roster_first <- run_order(c("roster", "stats"))
  stats_first <- run_order(c("stats", "roster"))

  expect_equal(roster_first, stats_first)
  expect_equal(roster_first$source, c("stats", "roster"))
  expect_equal(roster_first$player_name_en, c("A Stats", NA))
  expect_equal(roster_first$source_url, c("u-stats", "u-team"))
  # A's roster height is gone with his roster row; his profile refills it.
  expect_equal(as.numeric(roster_first$height_m), c(NA, 1.90))
  expect_equal(as.numeric(roster_first$run_id[1]), as.numeric(run_id))
  expect_equal(as.numeric(roster_first$identity_id[1]), as.numeric(identity$identity_id))
  expect_true(is.na(roster_first$identity_id[2]))
})
