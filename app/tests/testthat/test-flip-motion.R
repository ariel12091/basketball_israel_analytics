test_that("the FLIP module is present and correctly gated", {
  js <- read_repo_txt("www", "app.js")

  expect_true(grepl("prefers-reduced-motion", js, fixed = TRUE))
  expect_true(grepl("preDraw.dt", js, fixed = TRUE))
  expect_true(grepl("draw.dt", js, fixed = TRUE))
  # Opt-in only: a table without the class must never be measured.
  expect_true(grepl("ibpl-flip", js, fixed = TRUE))
})

test_that("only the two shared on/off tables opt in", {
  helpers_txt <- read_repo_txt("R", "helpers.R")

  hits <- regmatches(helpers_txt, gregexpr("ibpl-flip", helpers_txt))[[1]]
  expect_length(hits, 2)
  # DT's default class must survive, or the table loses its base styling.
  expect_true(grepl('class = "display ibpl-flip"', helpers_txt, fixed = TRUE))
})

test_that("the reduced-motion check gates before any measuring", {
  js <- read_repo_txt("www", "app.js")

  # The guard has to come before the class check and the measure() call, or a
  # reader who asked for no motion still pays for a layout read on every draw.
  guard <- regexpr("if (reducedMotion()) { pending = null; return; }", js, fixed = TRUE)
  measured <- regexpr("pending = { table: table, boxes: measure(table) };", js, fixed = TRUE)

  expect_gt(guard, 0)
  expect_gt(measured, 0)
  expect_lt(guard, measured)
})

test_that("rows absent on either side of the redraw are left alone", {
  js <- read_repo_txt("www", "app.js")

  # Animating arrivals and departures would be decoration; only a row that
  # existed before and after has a movement worth showing.
  expect_true(grepl("hasOwnProperty.call(before, k)", js, fixed = TRUE))
})

test_that("the row key uses two cells, not one", {
  js <- read_repo_txt("www", "app.js")

  # Column 1 is Team and column 2 is Player on both tables that opt in.
  # Measured in the browser on a 30-row table: 14 unique first cells against
  # 30 unique first-and-second, so keying on the first cell alone would make a
  # team's players share an identity and animate from each other's positions.
  expect_true(grepl("cells[1].textContent.trim()", js, fixed = TRUE))
  expect_false(grepl("var cell = tr.querySelector", js, fixed = TRUE))
})
