library(testthat)

# Team floor time has one owner, get_team_minutes_dynamic (docs/shared_definitions.md,
# "Minutes"). Two other readers return team minutes under the same filters;
# this holds all three together, and checks the app calls the owner.

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

minutes_of <- function(fn, args) {
  DBI::dbGetQuery(con, sprintf("SELECT team_id, minutes FROM basketball_test.%s(2026%s)", fn, args))
}
largest_gap <- function(a, b) {
  m <- merge(a, b, by = "team_id", all = TRUE)
  d <- abs(as.numeric(m$minutes.x) - as.numeric(m$minutes.y))
  if (anyNA(d)) Inf else max(d)
}

test_that("team minutes equal the Traditional reader's, to its one decimal", {
  for (args in c("", ", p_home_away := 'home'", ", p_last_n_games := 5",
                 ", p_opp_rank_side := 'top', p_opp_rank_n := 4",
                 ", p_max_margin := 5, p_max_time_remaining := 300")) {
    owner <- minutes_of("get_team_minutes_dynamic", args)
    expect_equal(nrow(owner), 14, label = paste("teams for", args))
    expect_lte(largest_gap(owner, minutes_of("get_team_traditional_dynamic", args)), 0.0501, label = args)
  }
})

test_that("team minutes equal the clutch dashboard reader's exactly, with and without starters", {
  for (args in c(", p_max_margin := 5, p_max_time_remaining := 300",
                 ", p_max_margin := 5, p_max_time_remaining := 300, p_num_starters_off := 5")) {
    expect_lte(largest_gap(minutes_of("get_team_minutes_dynamic", args),
                           minutes_of("get_team_four_factors_dashboard_dynamic", args)), 1e-9, label = args)
  }
})

test_that("unfiltered team minutes are measured floor time, not nominal game length", {
  measured <- DBI::dbGetQuery(con, "
    SELECT team_id, SUM(secs) / 60.0 AS minutes FROM (
      SELECT d.team_id, d.game_id, d.lineup_hash, d.segment_id, MAX(d.segment_seconds) AS secs
      FROM basketball_test.df_pts_poss_lineups_longer_mv d
      JOIN basketball_test.final_schedule_mv s USING (game_id, team_id)
      WHERE s.game_year = 2026 AND d.lineup_hash IS NOT NULL AND d.segment_id IS NOT NULL
      GROUP BY 1, 2, 3, 4) x GROUP BY 1")
  expect_lte(largest_gap(minutes_of("get_team_minutes_dynamic", ""), measured), 0.001)
})
