# The Per 60 / Per 30 small-sample trim (drop the bottom (1 - keep_pct) by
# possessions) should only engage once the population is large; on small/filtered
# result sets it must be disabled (threshold 0) so every player is shown.

test_that("ts_rate_threshold disables the trim below min_n players", {
  expect_equal(ts_rate_threshold(rep(100, 119), keep_pct = 0.85, min_n = 120), 0)
})

test_that("ts_rate_threshold returns the (1 - keep_pct) possession quantile at/above min_n", {
  v <- 1:120
  expect_equal(
    ts_rate_threshold(v, keep_pct = 0.85, min_n = 120),
    as.numeric(stats::quantile(v, probs = 0.15, na.rm = TRUE, type = 7))
  )
})

test_that("ts_rate_threshold counts only finite, positive possessions", {
  # 120 valid possessions plus junk that must be ignored -> trim engages.
  v <- c(rep(0, 40), rep(NA_real_, 40), 1:120)
  expect_equal(
    ts_rate_threshold(v, keep_pct = 0.85, min_n = 120),
    as.numeric(stats::quantile(1:120, probs = 0.15, na.rm = TRUE, type = 7))
  )
  # Only 119 valid -> still disabled.
  expect_equal(ts_rate_threshold(c(rep(0, 10), 1:119), keep_pct = 0.85, min_n = 120), 0)
})

test_that("ts_rate_threshold handles empty / all-invalid input", {
  expect_equal(ts_rate_threshold(numeric(0)), 0)
  expect_equal(ts_rate_threshold(c(NA_real_, 0, -5)), 0)
})
