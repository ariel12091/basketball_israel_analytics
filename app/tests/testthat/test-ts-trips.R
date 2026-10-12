library(testthat)

# TS% is points per two true-shooting attempts, where an attempt is a
# field-goal attempt or a trip to the line for a personal foul. The database
# readers return it computed from real trips; the app must not fall back to
# the FGA + 0.44 x FTA estimate when it recombines rows.

test_that("ts_pct_from_trips divides points by twice the true-shooting attempts", {
  expect_equal(ts_pct_from_trips(20, 10), 100)
  expect_equal(ts_pct_from_trips(c(11, 0), c(10, 5)), c(55, 0))
  expect_true(is.na(ts_pct_from_trips(4, 0)))
  expect_true(is.na(ts_pct_from_trips(4, NA)))
})

test_that("ts_implied_attempts recovers the attempts behind a row's TS%", {
  # 100 points at 50% is 100 attempts; 50 points at 62.5% is 40.
  expect_equal(ts_implied_attempts(pts = c(100, 50), ts = c(50, 62.5), fga = c(80, 30)), c(100, 40))
  # No points: the attempts cannot be implied, so field-goal attempts stand in.
  expect_equal(ts_implied_attempts(pts = 0, ts = 0, fga = 7), 7)
  expect_equal(ts_implied_attempts(pts = NA, ts = NA, fga = 3), 3)
})

test_that("a TOTAL row's TS% is the attempts-weighted combination of its rows", {
  rows <- data.frame(pts = c(100, 50), ts = c(50, 62.5), fga = c(80, 30))
  attempts <- ts_implied_attempts(rows$pts, rows$ts, rows$fga)
  expect_equal(ts_pct_from_trips(sum(rows$pts), sum(attempts)), round(100 * 150 / 280, 1))
})

test_that("the app's R code no longer estimates trips as 0.44 x FTA", {
  for (f in c("helpers.R", "server_tab3.R", "server_tab5_traditional.R", "server_tab7_compare.R")) {
    expect_false(grepl("0.44", read_repo_txt("R", f), fixed = TRUE), info = f)
  }
})
