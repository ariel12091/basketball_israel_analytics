# Unit tests for the per-run game attribution in etl/etl_run_report.R (no DB).
# Run: Rscript -e "testthat::test_file('etl/tests/test_etl_run_report.R')"
library(testthat)
source("../etl_run_report.R")

fake_dq_result <- function() {
  dir <- tempfile("dq_")
  dir.create(file.path(dir, "details"), recursive = TRUE)
  write_detail <- function(check_id, df) {
    utils::write.csv(df, file.path(dir, "details", paste0(check_id, ".csv")), row.names = FALSE)
    file.path("details", paste0(check_id, ".csv"))
  }
  summary <- data.frame(
    check_id = c("AK_misplaced_clock_runs", "T_invalid_team_minutes", "G_dataset_level",
                 "P1_reviewed_data_quality_exceptions", "B_passing"),
    severity = c("warning", "error", "error", "warning", "error"),
    status = c("warning", "fail", "fail", "warning", "pass"),
    issue_count = c(2, 1, 3, 1, 0),
    title = c("Misplaced clock runs", "Team minutes", "Dataset | level", "Reviewed", "Passing"),
    detail_file = c(
      write_detail("AK_misplaced_clock_runs", data.frame(game_id = c(398, 399), period = c("Q2", "Q3"))),
      write_detail("T_invalid_team_minutes", data.frame(game_id = c(398, 398, 500))),
      write_detail("G_dataset_level", data.frame(note = "no game column")),
      write_detail("P1_reviewed_data_quality_exceptions", data.frame(game_id = 398)),
      write_detail("B_passing", data.frame(game_id = 398))
    ),
    stringsAsFactors = FALSE
  )
  list(status = "FAIL", summary = summary, latest_path = file.path(dir, "latest.md"))
}

test_that("findings are attributed only to the run's games, from open checks with a game_id", {
  findings <- dq_game_findings(fake_dq_result(), c(398L, 64942L))
  expect_identical(findings$game_id, c(398L, 398L))
  expect_identical(findings$check_id, c("AK_misplaced_clock_runs", "T_invalid_team_minutes"))
  # Two detail rows name game 398 in the minutes check.
  expect_identical(findings$detail_rows, c(1L, 2L))
})

test_that("no published games means no findings", {
  expect_identical(nrow(dq_game_findings(fake_dq_result(), integer(0))), 0L)
})

test_that("every published game is listed, clean games included", {
  dq <- fake_dq_result()
  games <- published_games_table(c(64942, 398, 398), dq_game_findings(dq, c(398, 64942)))
  expect_identical(games$game_id, c(398L, 64942L))
  expect_identical(games$open_findings, c(2L, 0L))
  expect_identical(games$checks[[1]], "AK_misplaced_clock_runs (warning), T_invalid_team_minutes (error)")
  expect_identical(games$checks[[2]], "none")
})

test_that("the run summary lists published games and escapes table pipes", {
  dq <- fake_dq_result()
  games <- published_games_table(c(398, 64942), dq_game_findings(dq, c(398, 64942)))
  path <- write_etl_run_summary_md(list(success = TRUE), dq, games, tempfile(fileext = ".md"))
  md <- readLines(path)
  expect_true("- ETL: **PASS**" %in% md)
  expect_true("| 64942 | 0 | none |" %in% md)
  expect_true(any(grepl("^\\| 398 \\| 2 \\| AK_misplaced_clock_runs \\(warning\\)", md)))
  expect_true(any(grepl("Dataset \\| level", md, fixed = TRUE)))
  # The reviewed-exceptions register is not an open check.
  expect_false(any(grepl("P1_reviewed_data_quality_exceptions", md, fixed = TRUE)))
})

test_that("a run with no published games says so", {
  path <- write_etl_run_summary_md(list(success = TRUE), fake_dq_result(),
                                   published_games_table(integer(0), data.frame()),
                                   tempfile(fileext = ".md"))
  expect_true("No games were published in this run." %in% readLines(path))
})
