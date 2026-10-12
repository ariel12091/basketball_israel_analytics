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

# Recovering attempts from a row's own TS% works only when the row scored. A
# scoreless row's missed free-throw trips are invisible to it, so the readers
# return the attempt count itself (ts_att) and the app sums that.
test_that("ts_attempts uses the count the reader returns, including for scoreless rows", {
  rows <- data.frame(pts = c(0, 10), ts = c(0, 50), fga = c(0, 10), ts_att = c(10, 10))
  expect_equal(ts_attempts(rows), c(10, 10))
  # ten missed trips for no points plus ten points on ten attempts is 25%, not 50%
  expect_equal(ts_pct_from_trips(sum(rows$pts), sum(ts_attempts(rows))), 25)
})

test_that("ts_attempts falls back to the implied count where a reader returns none", {
  rows <- data.frame(pts = c(100, 50), ts = c(50, 62.5), fga = c(80, 30))
  expect_equal(ts_attempts(rows), c(100, 40))
  rows$ts_att <- c(NA, 44)
  expect_equal(ts_attempts(rows), c(100, 44))
})

test_that("the multi-team TOTAL row sums returned attempts", {
  rows <- data.frame(
    team_id = c(1L, 2L), player_id = c(7L, 7L), Player = "A B", team_name = c("X", "Y"),
    gp = c(2, 3), poss_on_floor = c(40, 60), minutes = c(20, 30),
    pts = c(0, 10), reb = 0, oreb = 0, dreb = 0, ast = 0, stl = 0, blk = 0, dfl = 0, tov = 0,
    fgm = c(0, 4), fga = c(0, 10), `3pm` = 0, `3pa` = 0, ftm = c(0, 2), fta = c(20, 2),
    ts = c(0, 50), usg_pct = c(10, 10), ts_att = c(10, 10), check.names = FALSE)
  lookup <- data.frame(team_id = c(1L, 2L), player_id = c(7L, 7L), identity_id = "p7")
  out <- add_ts_multi_team_totals(rows, lookup)
  expect_equal(out$ts[out$is_multi_team_total], 25)
  expect_equal(out$ts_att[out$is_multi_team_total], 20)
})

test_that("the usage fallback counts a scoreless player's free-throw trips", {
  df <- data.frame(team_id = 1L, pts = c(0, 10), ts = c(0, 50), fga = c(0, 10), fta = c(20, 0),
                   tov = 0, poss_on_floor = 100, ts_att = c(10, 10), usg_pct = NA_real_)
  expect_equal(add_ts_usage_pct(df)$usg_pct, c(20, 20))
})
