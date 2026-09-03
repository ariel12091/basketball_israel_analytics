source(repo_file("R", "helpers.R"), local = TRUE)

# These assertions are chosen to fail on the ways an "extraction" can quietly
# lose something: the baseline track, the weighting that separates the on-court
# value from the off-court one, and the impact estimate that only one caller
# passes. A test that merely checks the container classes are present would
# pass while all three were gone.

test_that("range_cell_js emits both branches with the full markup", {
  js <- range_cell_js("diffVal", "onVal", "offVal")

  # unranked branch
  expect_true(grepl("diff-val unranked", js, fixed = TRUE))
  expect_true(grepl("rank-bar-container hidden", js, fixed = TRUE))

  # ranked branch
  expect_true(grepl("rank-bar-container", js, fixed = TRUE))
  expect_true(grepl("rank-track", js, fixed = TRUE))
  expect_true(grepl("range-connect", js, fixed = TRUE))
  expect_true(grepl("dot-off", js, fixed = TRUE))
  expect_true(grepl("dot-on", js, fixed = TRUE))

  # The on dot must paint after the off dot so it wins the overlap.
  expect_lt(
    regexpr("dot-off", js, fixed = TRUE),
    regexpr("dot-on", js, fixed = TRUE)
  )
})

test_that("the sub-text keeps on-court primary and off-court secondary", {
  js <- range_cell_js("diffVal", "onVal", "offVal")

  # Flattening this to plain text would render both values with equal weight
  # and lose which one is the on-court figure.
  expect_true(grepl("font-weight:700", js, fixed = TRUE))
  expect_true(grepl("opacity:0.6", js, fixed = TRUE))
  # Both values are drawn in --ibpl-cell-text; the hierarchy is carried by
  # weight alone, because only pure white clears AA on every step of the
  # ramp (4.93 on the greenest cell, against 4.33 at 90% opacity).
  expect_equal(length(gregexpr("var(--ibpl-cell-text)", js, fixed = TRUE)[[1]]), 2)
  expect_false(grepl("#666", js, fixed = TRUE))
  expect_false(grepl("#222", js, fixed = TRUE))
})

test_that("extra_expr reaches both branches, and is absent when not passed", {
  with_extra <- range_cell_js("diffVal", "onVal", "offVal", extra_expr = "estLine")
  without <- range_cell_js("head", "onTxt", "offTxt")

  # Four Factors appends its est. +/-X pts line to the unranked branch as well
  # as the ranked one; dropping either would blank the annotation on some rows.
  expect_equal(length(gregexpr("estLine", with_extra, fixed = TRUE)[[1]]), 2)
  expect_false(grepl("estLine", without, fixed = TRUE))
})

test_that("the unranked test keys on the on-court percentile alone", {
  js <- range_cell_js("diffVal", "onVal", "offVal")

  # A row with a known on-court rank and a missing off-court rank renders the
  # ranked branch. Widening this condition would change which rows show dots.
  expect_true(grepl("onPct === null || onPct === undefined", js, fixed = TRUE))
  expect_false(grepl("isNaN(offPct)", js, fixed = TRUE))
})

test_that("both call sites go through the shared builder", {
  helpers_txt <- read_repo_txt("R", "helpers.R")
  tab1_txt <- read_repo_txt("R", "server_tab1.R")

  expect_true(grepl("range_cell_js <- function", helpers_txt, fixed = TRUE))
  expect_true(grepl("range_cell_js(", tab1_txt, fixed = TRUE))

  # The markup exists once: inside the builder, and nowhere by hand.
  expect_equal(length(gregexpr("rank-track", helpers_txt, fixed = TRUE)[[1]]), 1)
  expect_false(grepl("rank-track", tab1_txt, fixed = TRUE))
})

# --- Summary verdict columns -------------------------------------------------

# Isolate onoff_summary_datatable's body so these assertions cannot be
# satisfied by the four-factors renderer further down the same file.
summary_body <- function() {
  helpers_txt <- read_repo_txt("R", "helpers.R")
  start <- regexpr("onoff_summary_datatable <- function", helpers_txt, fixed = TRUE)
  end <- regexpr("onoff_four_factors_datatable <- function", helpers_txt, fixed = TRUE)
  substring(helpers_txt, start, end)
}

test_that("the summary verdict column carries a non-colour rank cue", {
  expect_true(grepl("range_cell_js(", summary_body(), fixed = TRUE))
})

test_that("summary background colour is confined to the verdict columns", {
  body <- summary_body()

  styled <- regmatches(body, gregexpr('formatStyle\\(dt, "[^"]+"', body))[[1]]
  styled <- sub('formatStyle\\(dt, "', "", styled)
  styled <- sub('"$', "", styled)

  # Net RTG Diff, Off ON Diff and Def ON Diff are the verdict. The on/off PPP
  # and net-rating columns are context and read through position instead.
  expect_setequal(styled, c("Net RTG Diff", "Off ON Diff", "Def ON Diff"))
})

test_that("the summary escape allowlist stays narrow", {
  body <- summary_body()

  # Exactly one column emits HTML, and it is the one that renders the range
  # cell. Anything wider would put database text through an unescaped column.
  expect_true(grepl('dt_escape_except(df, "Net RTG Diff")', body, fixed = TRUE))
  expect_false(grepl("escape = FALSE", body, fixed = TRUE))
})

test_that("Net RTG Diff no longer runs through the plain diff formatter", {
  body <- summary_body()

  # Two renderers targeting one column would leave the outcome to DT's
  # columnDefs precedence; the verdict column is excluded explicitly instead.
  expect_true(grepl('setdiff(diff_cols, "Net RTG Diff")', body, fixed = TRUE))
})

test_that("percent signs match how the call site consumes the JS", {
  # A call site that passes the result through sprintf() needs %% ; one that
  # concatenates straight into DT::JS() needs % , or the browser is handed
  # "left:42%%" and positions nothing.
  escaped <- range_cell_js("v", "on", "off")
  plain <- range_cell_js("v", "on", "off", sprintf_escaped = FALSE)

  expect_true(grepl("left:' + rangeLineLeft + '%%;", escaped, fixed = TRUE))
  expect_true(grepl("left:' + rangeLineLeft + '%;", plain, fixed = TRUE))
  expect_false(grepl("%%", plain, fixed = TRUE))
})

test_that("the Summary verdict renderer emits single percent signs", {
  # It concatenates into DT::JS() rather than sprintf(), unlike the other two.
  expect_true(grepl("sprintf_escaped = FALSE", summary_body(), fixed = TRUE))
})
