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
  expect_true(grepl("color:#666", js, fixed = TRUE))
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
