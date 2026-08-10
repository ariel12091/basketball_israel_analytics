test_that("auto_minposs_from_df reproduces the Tab 2 behaviour it replaces", {
  # Fewer rows than the target: no threshold is needed.
  small <- data.frame(total_poss = c(500, 400, 300))
  expect_identical(auto_minposs_from_df(small, target_rows = 150L), 0L)

  # More rows than the target: the kth largest value, rounded up to the step.
  many <- data.frame(total_poss = seq(1000, 1, by = -1))
  expect_identical(
    auto_minposs_from_df(many, target_rows = 10L, step = 10L),
    as.integer(ceiling(991 / 10) * 10)
  )

  # Empty, NULL, and missing-column inputs are NA, never an error.
  expect_true(is.na(auto_minposs_from_df(NULL)))
  expect_true(is.na(auto_minposs_from_df(data.frame())))
  expect_true(is.na(auto_minposs_from_df(data.frame(other = 1:3))))

  # Non-finite values are dropped before ranking.
  mixed <- data.frame(total_poss = c(100, NA, Inf, 50, 25))
  expect_identical(auto_minposs_from_df(mixed, target_rows = 2L, step = 10L), 50L)

  # The Tab 2 default target must survive the move to helpers.R.
  expect_identical(formals(auto_minposs_from_df)$target_rows, 150L)
})
