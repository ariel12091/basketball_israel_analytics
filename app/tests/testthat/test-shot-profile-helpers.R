# Pure share-math contracts for the Shot Profile display mode (Plan C).
# helpers.R is sourced by helper-server-mocks.R, so the real implementations run.

test_that("shot_profile_metric_cols maps labels to suffixed column names", {
  cols <- shot_profile_metric_cols("Off", "off_on")
  expect_equal(unname(cols["Off Lay+Dunk%"]), "off_on_rim_share")
  expect_equal(unname(cols["Off Corner 3 Share"]), "off_on_c3_pct3")
  expect_length(cols, 6L)
})

test_that("add_shot_profile_metrics computes shares of FGA with mid as remainder", {
  df <- data.frame(
    la = c(20, 0), du = c(5, 0), fga = c(100, 80), f3 = c(40, 30),
    c3 = c(10, 0), c3k = c(35, 0)
  )
  out <- add_shot_profile_metrics(df, list(p = c("la", "du", "fga", "f3", "c3", "c3k")))
  expect_equal(out$p_layup_share, c(20, 0))
  expect_equal(out$p_dunk_share, c(5, 0))
  expect_equal(out$p_rim_share, c(25, 0))
  expect_equal(out$p_fg3_share, c(40, 37.5))
  expect_equal(out$p_mid_share, c(35, 62.5))
  # rim + fg3 + mid partitions FGA
  expect_equal(out$p_rim_share + out$p_fg3_share + out$p_mid_share, c(100, 100))
  expect_equal(out$p_fga, c(100, 80))
})

test_that("corner share divides by known 3PA and fails open to NA", {
  df <- data.frame(la = 1, du = 0, fga = 50, f3 = 20, c3 = 6, c3k = 15)
  out <- add_shot_profile_metrics(df, list(p = c("la", "du", "fga", "f3", "c3", "c3k")))
  expect_equal(out$p_c3_pct3, 40)  # 6/15, NOT 6/20
  df0 <- data.frame(la = 1, du = 0, fga = 50, f3 = 20, c3 = 0, c3k = 0)
  out0 <- add_shot_profile_metrics(df0, list(p = c("la", "du", "fga", "f3", "c3", "c3k")))
  expect_true(is.na(out0$p_c3_pct3))  # unknown != 0%
})

test_that("zero FGA yields NA shares and NA counts are treated as 0", {
  df <- data.frame(la = c(0, NA), du = c(0, 2), fga = c(0, 10), f3 = c(0, 4), c3 = c(0, 1), c3k = c(0, 3))
  out <- add_shot_profile_metrics(df, list(p = c("la", "du", "fga", "f3", "c3", "c3k")))
  expect_true(is.na(out$p_rim_share[1]))
  expect_true(is.na(out$p_mid_share[1]))
  expect_equal(out$p_rim_share[2], 20)  # NA layup -> 0, dunk 2 of 10
})

test_that("missing spec columns leave df untouched", {
  df <- data.frame(x = 1)
  out <- add_shot_profile_metrics(df, list(p = c("a", "b", "c", "d", "e", "f")))
  expect_identical(names(out), "x")
  expect_identical(add_shot_profile_metrics(NULL, list()), NULL)
})
