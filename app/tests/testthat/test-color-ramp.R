# global.R cannot be sourced from a test: at source time it builds the DB pool,
# registers onStop() and schedules a later() prewarm. The ramp is a pure
# constant, so evaluate just its two definition lines out of the real file --
# this still reads the committed source rather than a copy of it.
ramp_src <- readLines(repo_file("R", "global.R"), warn = FALSE)
ramp_lines <- ramp_src[startsWith(ramp_src, "COLS_GRAD") |
                       startsWith(ramp_src, "COLS_REV")]
ramp_env <- new.env()
eval(parse(text = ramp_lines), envir = ramp_env)
COLS_GRAD <- ramp_env$COLS_GRAD
COLS_REV  <- ramp_env$COLS_REV

test_that("the percentile ramp is strictly monotonic in luminance", {
  # Colour is the only encoding on most heat cells, and red-green deficiency
  # removes hue. What survives is luminance, so luminance alone has to carry
  # the ordering. The pre-2026-09 ramp did not: its minimum adjacent step was
  # negative, so "good" could be dimmer than "average".
  lum <- rel_luminance(COLS_GRAD)

  expect_length(COLS_GRAD, 20)
  expect_true(all(diff(lum) > 0))
})

test_that("the ramp separates its quintiles by a usable margin", {
  lum <- rel_luminance(COLS_GRAD)
  quintiles <- lum[c(1, 5, 10, 15, 20)]
  ratios <- quintiles[-1] / quintiles[-length(quintiles)]

  # The old ramp's worst quintile-to-quintile ratio was 1.02x: two adjacent
  # fifths of the scale that a deuteranope cannot tell apart at all.
  expect_gt(min(ratios), 1.2)
  expect_gt(lum[20] / lum[1], 3)
})

test_that("the ramp keeps the green-good red-bad convention", {
  low <- grDevices::col2rgb(COLS_GRAD[1])[, 1]
  high <- grDevices::col2rgb(COLS_GRAD[20])[, 1]

  expect_gt(as.integer(low[["red"]]), as.integer(low[["green"]]))
  expect_gt(as.integer(high[["green"]]), as.integer(high[["red"]]))
})

test_that("COLS_REV is COLS_GRAD reversed", {
  expect_equal(COLS_REV, rev(COLS_GRAD))
})

test_that("the shot-accuracy renderer varies all three channels", {
  # accColor() built rgb(r, g, 60): blue pinned, so only the red-green axis
  # moved -- the exact axis red-green deficiency removes -- and its luminance
  # was not monotonic (a -0.098 step at the midpoint). It exists twice in this
  # file, byte-identical, so both copies must carry the fix.
  src <- read_repo_txt("R", "helpers.R")

  n_defs <- length(gregexpr("function accColor", src, fixed = TRUE)[[1]])
  expect_equal(n_defs, 2)
  expect_false(grepl("var r, g;", src, fixed = TRUE))
  n_fixed <- length(gregexpr("var r, g, b;", src, fixed = TRUE)[[1]])
  expect_equal(n_fixed, n_defs)
})
