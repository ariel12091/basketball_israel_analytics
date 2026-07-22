clock_contract_file <- function(...) {
  paste(readLines(repo_file(...), warn = FALSE), collapse = "\n")
}

test_that("canonical segment timing preserves raw clocks and uses lineup boundaries", {
  helper_sql <- clock_contract_file(
    "..", "sql", "functions", "refresh_segment_clock_fields_for_games.sql"
  )
  base_sql <- clock_contract_file(
    "..", "sql", "materialized_views", "df_pts_poss_longer.sql"
  )

  expect_true(grepl("event_elapsed_seconds", helper_sql, fixed = TRUE))
  expect_true(grepl("lead(ss.segment_start_elapsed_seconds)", helper_sql, fixed = TRUE))
  expect_true(grepl("segment_seconds", base_sql, fixed = TRUE))
  expect_false(grepl("UPDATE basketball_test.actions_clean", helper_sql, fixed = TRUE))
  expect_false(grepl("UPDATE basketball_test.possessions", helper_sql, fixed = TRUE))
})

test_that("production minute SQL no longer uses action-clock extrema", {
  paths <- c(
    list.files(repo_file("..", "sql"), pattern = "\\.sql$", recursive = TRUE, full.names = TRUE),
    repo_file("..", "app", "R", "server_tab3.R"),
    repo_file("..", "app", "R", "server_tab6_team_stats.R")
  )
  sql_text <- paste(
    unlist(lapply(paths, readLines, warn = FALSE), use.names = FALSE),
    collapse = "\n"
  )

  forbidden <- "max\\s*\\([^)]*end_game_seconds_remaining[^)]*\\)\\s*-\\s*min\\s*\\([^)]*end_game_seconds_remaining"
  expect_false(grepl(forbidden, sql_text, ignore.case = TRUE, perl = TRUE))
})

test_that("data-quality report covers source clocks and canonical durations", {
  dq_r <- clock_contract_file("..", "etl", "run_data_quality_report.R")

  expect_true(grepl("AA_material_clock_order_anomalies", dq_r, fixed = TRUE))
  expect_true(grepl("AD_clutch_clock_exposure", dq_r, fixed = TRUE))
  expect_true(grepl("AH_canonical_segment_timing", dq_r, fixed = TRUE))
})
