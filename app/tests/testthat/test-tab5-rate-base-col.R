# In rate modes the per-rate column is normalized to a constant (Poss On Floor=60,
# Min=30), so the table must surface the player's ACTUAL per-game base: Min/G in
# minutes modes, Poss/G in possession modes. Per Game already shows per-game Min
# and Poss On Floor, so it needs no extra column; Totals has no rate.

test_that("ts_rate_base_col surfaces per-game minutes in minutes modes", {
  expect_equal(ts_rate_base_col("Per 30 Minutes")$label, "Min/G")
  expect_equal(ts_rate_base_col("Per 30 Minutes")$src, "base_min_pg")
  expect_equal(ts_rate_base_col("Per 30 Minutes")$after, "Min")
  expect_equal(ts_rate_base_col("Per X Minutes")$src, "base_min_pg")
})

test_that("ts_rate_base_col surfaces per-game possessions in possession modes", {
  expect_equal(ts_rate_base_col("Per 60 Possessions")$label, "Poss/G")
  expect_equal(ts_rate_base_col("Per 60 Possessions")$src, "base_poss_pg")
  expect_equal(ts_rate_base_col("Per 60 Possessions")$after, "Poss On Floor")
  expect_equal(ts_rate_base_col("Per X Possessions")$src, "base_poss_pg")
})

test_that("ts_rate_base_col shows no extra base column for Per Game / Totals", {
  expect_null(ts_rate_base_col("Per Game"))
  expect_null(ts_rate_base_col("Totals"))
})
