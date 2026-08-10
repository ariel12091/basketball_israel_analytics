# Helpers extracted from the per-league tab files so both leagues share one
# implementation. These tests exist because the extractions are byte-identical
# MOVES: they must keep behaving exactly as they did inside the server bodies,
# including when called from file scope rather than from inside a server
# function (where dplyr was already attached by global.R).

test_that("auto_min_on_from_df keeps the top AUTO_TOP_PCT by usage", {
  expect_identical(AUTO_TOP_PCT, 0.35)

  # 10 rows, top 35% = ceiling(3.5) = 4 rows -> lowest of those is 70,
  # floored to the step.
  df <- data.frame(poss = c(100, 90, 80, 70, 60, 50, 40, 30, 20, 10))
  expect_identical(auto_min_on_from_df(df, "poss", step = 10L), 70L)

  # Floors to the step rather than rounding up.
  df2 <- data.frame(poss = c(100, 90, 80, 77, 60, 50, 40, 30, 20, 10))
  expect_identical(auto_min_on_from_df(df2, "poss", step = 10L), 70L)

  # Degenerate inputs return NA, never an error.
  expect_true(is.na(auto_min_on_from_df(NULL, "poss")))
  expect_true(is.na(auto_min_on_from_df(data.frame(), "poss")))
  expect_true(is.na(auto_min_on_from_df(data.frame(other = 1:3), "poss")))
})

test_that("auto_min_all_from_df requires both on and off to clear the bar", {
  # Ordered by usage; the top 4 rows' pmin(on, off) low-water mark is 20.
  df <- data.frame(
    poss = c(100, 90, 80, 70, 60, 50, 40, 30, 20, 10),
    on   = c(100, 90, 80, 70, 60, 50, 40, 30, 20, 10),
    off  = c( 50, 40, 30, 20, 60, 50, 40, 30, 20, 10)
  )
  expect_identical(auto_min_all_from_df(df, "poss", "on", "off", step = 10L), 20L)

  # A missing column is NA, not an error.
  expect_true(is.na(auto_min_all_from_df(df, "poss", "on", "nope")))
})

test_that("resolve_poss_cols picks the columns for the active view mode", {
  summary_df <- data.frame(`ON Poss` = 1, `OFF Poss` = 1, check.names = FALSE)
  expect_identical(resolve_poss_cols(summary_df, "Summary"),
                   list(on = "ON Poss", off = "OFF Poss"))

  ff_df <- data.frame(off_on_poss = 1, off_off_poss = 1)
  expect_identical(resolve_poss_cols(ff_df, "Four Factors"),
                   list(on = "off_on_poss", off = "off_off_poss"))

  # Summary falls back to the raw column names when the display ones are absent.
  expect_identical(resolve_poss_cols(ff_df, "Summary"),
                   list(on = "off_on_poss", off = "off_off_poss"))

  # Four Factors does NOT fall back to the display names.
  expect_identical(resolve_poss_cols(summary_df, "Four Factors"),
                   list(on = NA_character_, off = NA_character_))
})
