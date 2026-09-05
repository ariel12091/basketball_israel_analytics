# Tab 4 (Israeli game logs) had no CSV export while its EuroLeague companion
# (Tab 11) has had one all along -- a parity gap running the opposite way to
# the Min column.
#
# Tabs 3, 5 and 6 each spelled out the same export contract separately. Tab 4
# has TWO tables, so inlining it again would make copies four and five;
# csv_export_button() in helpers.R is the single definition instead.
#
# The contract that matters is `columns = ":visible"`. Both game-log tables
# hide internals -- the eight raw fg2/fg3 columns and every pr_* percentile
# rank -- and DataTables exports hidden columns unless told otherwise, so
# without this the CSV leaks fields the reader never asked for.

test_that("csv_export_button emits the shared export contract", {
  btn <- csv_export_button("game_logs_summary", now = as.POSIXct("2026-09-05 14:30:15", tz = "UTC"))

  expect_length(btn, 1)
  b <- btn[[1]]
  expect_equal(b$extend, "csv")
  expect_equal(b$text, "Download CSV")
  expect_equal(b$filename, "game_logs_summary_20260905_143015")
  expect_equal(b$exportOptions$columns, ":visible")
  expect_true(b$exportOptions$stripHtml)
  expect_true(b$exportOptions$stripNewlines)
  expect_true(b$exportOptions$trim)
  expect_equal(b$exportOptions$modifier$search, "applied")
  expect_equal(b$exportOptions$modifier$order, "applied")
})

test_that("csv_export_button takes a custom label", {
  btn <- csv_export_button("x", text = "Export", now = as.POSIXct("2026-01-02 03:04:05", tz = "UTC"))
  expect_equal(btn[[1]]$text, "Export")
  expect_equal(btn[[1]]$filename, "x_20260102_030405")
})

test_that("both Tab 4 tables expose a CSV button", {
  src <- paste(readLines(repo_file("R", "server_tab4.R"), warn = FALSE), collapse = "\n")

  # Two tables: Summary and Four Factors.
  expect_equal(length(gregexpr('extensions = "Buttons"', src, fixed = TRUE)[[1]]), 2L)
  expect_equal(length(gregexpr('dom = "Btip"', src, fixed = TRUE)[[1]]), 2L)
  # "B" must be in the dom string or the button never renders.
  expect_false(grepl('dom = "tip"', src, fixed = TRUE))

  expect_true(grepl('csv_export_button("game_logs_summary")', src, fixed = TRUE))
  expect_true(grepl('csv_export_button("game_logs_four_factors")', src, fixed = TRUE))
})

test_that("Tab 4 does not inline its own export options", {
  src <- paste(readLines(repo_file("R", "server_tab4.R"), warn = FALSE), collapse = "\n")

  # The contract lives in helpers.R; a copy here is the thing this change exists
  # to prevent.
  expect_false(grepl("exportOptions", src, fixed = TRUE))
})
