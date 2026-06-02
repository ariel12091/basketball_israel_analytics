test_that("tab5 calculates two-point columns from field-goal and three-point columns", {
  df <- data.frame(
    fgm = c(10, 3),
    fga = c(20, 5),
    `3pm` = c(4, 3),
    `3pa` = c(10, 5),
    check.names = FALSE
  )

  out <- add_ts_two_point_stats(df)

  expect_equal(out$`2pm`, c(6, 0))
  expect_equal(out$`2pa`, c(10, 0))
  expect_equal(out$two_pct, c(60, NA_real_))
})
