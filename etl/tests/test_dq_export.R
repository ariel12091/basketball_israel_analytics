library(testthat)
source("../dq_export.R")

make_export_input <- function() {
  dir <- tempfile("dq_export_")
  dir.create(file.path(dir, "details"), recursive = TRUE)
  utils::write.csv(data.frame(game_id = c(398L, 399L), note = c("one", "two")),
                   file.path(dir, "details", "A.csv"), row.names = FALSE)
  writeLines("report", file.path(dir, "report.md"))
  writeLines("findings", file.path(dir, "findings.html"))
  writeLines("summary", file.path(dir, "summary.csv"))
  list(dir = dir, dq = list(
    status = "FAIL", latest_path = file.path(dir, "latest.md"),
    report_path = file.path(dir, "report.md"), findings_path = file.path(dir, "findings.html"),
    summary_path = file.path(dir, "summary.csv"),
    summary = data.frame(check_id = "A", title = "Example", severity = "error", status = "fail",
                         row_count = 2L, issue_count = 2L, error_message = NA_character_,
                         detail_file = "details/A.csv")
  ))
}

test_that("exports are immutable and preserve empty and singleton arrays", {
  input <- make_export_input()
  on.exit(unlink(input$dir, recursive = TRUE))
  path <- write_dq_export(input$dq, input$dir, "basketball_test", Sys.time(), list(
    success = FALSE, published_game_ids = 398L, failed_game_ids = integer(),
    base_loaded_game_ids = 398L, phase_failures = "validation failed"
  ))
  raw <- jsonlite::read_json(path, simplifyVector = FALSE)
  expect_length(raw$checks, 1L)
  expect_identical(raw$etl$published_game_ids, list(398L))
  expect_identical(raw$etl$failed_game_ids, list())
  expect_false(raw$etl$success)
  expect_null(raw$checks[[1]]$error_message)
  expect_identical(raw$scope, "whole_dataset")
  expect_identical(raw$checks[[1]]$evidence_game_ids, list(398L, 399L))
  snapshot <- raw$checks[[1]]$detail_path
  unlink(file.path(input$dir, "details", "A.csv"))
  expect_true(file.exists(snapshot))
  expect_identical(nrow(utils::read.csv(snapshot)), 2L)
  expect_identical(readLines(raw$artifacts$findings), "findings")
})

test_that("a standalone report has no invented ETL success", {
  input <- make_export_input()
  on.exit(unlink(input$dir, recursive = TRUE))
  path <- write_dq_export(input$dq, input$dir, "basketball", Sys.time())
  raw <- jsonlite::read_json(path)
  expect_null(raw$etl)
  expect_identical(raw$league, "il")
})

test_that("missing evidence cannot publish a complete bundle", {
  input <- make_export_input()
  on.exit(unlink(input$dir, recursive = TRUE))
  unlink(file.path(input$dir, "details", "A.csv"))
  expect_error(write_dq_export(input$dq, input$dir, "basketball_test", Sys.time()),
               "Missing data-quality evidence")
  expect_length(list.files(file.path(input$dir, "dagster"), recursive = TRUE), 0)
})
