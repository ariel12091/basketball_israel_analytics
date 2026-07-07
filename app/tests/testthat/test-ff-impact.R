# Unit tests for the four-factor point-impact helpers (helpers.R).
# Weights are league-calibrated (see scripts/fit_ff_impact_weights.R);
# these tests pin the published values and the conversion semantics.

test_that("FF_IMPACT_WEIGHTS holds the published league weights", {
  expect_equal(
    FF_IMPACT_WEIGHTS,
    c(efg = 1.45, tov = -1.36, oreb = 0.63, ftr = 0.13)
  )
})

test_that("ff_impact_pts converts factor deltas to est. pts/100", {
  expect_equal(ff_impact_pts(2.1, "efg"), 2.1 * 1.45)
  # TOV carries a negative weight: more turnovers -> fewer points.
  expect_equal(ff_impact_pts(1, "tov"), -1.36)
  expect_equal(ff_impact_pts(-1.5, "tov"), 2.04)
  expect_equal(ff_impact_pts(3, "oreb"), 1.89)
  expect_equal(ff_impact_pts(0.8, "ftr"), 0.104)
})

test_that("ff_impact_pts is vectorized over delta and factor", {
  expect_equal(
    ff_impact_pts(c(1, 1, 1, 1), c("efg", "tov", "oreb", "ftr")),
    c(1.45, -1.36, 0.63, 0.13)
  )
  expect_equal(ff_impact_pts(c(2, -2), "efg"), c(2.9, -2.9))
})

test_that("ff_impact_pts propagates NA and fails closed on unknown factor", {
  expect_true(is.na(ff_impact_pts(NA_real_, "efg")))
  expect_equal(ff_impact_pts(c(1, NA), "tov"), c(-1.36, NA_real_))
  expect_error(ff_impact_pts(1, "ts"), "Unknown four-factor")
})

test_that("ff_impact_tooltip leads with Estimated and includes the weight", {
  tip <- ff_impact_tooltip("efg")
  expect_match(tip, "^Estimated")
  expect_match(tip, "1.45", fixed = TRUE)
  expect_match(tip, "100 poss", fixed = TRUE)
  expect_error(ff_impact_tooltip("nope"), "Unknown four-factor")
})
