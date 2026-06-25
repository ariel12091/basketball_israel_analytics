# Tab 5 percentile coloring must be computed over the full population so the
# color of a row does not depend on how many rows are displayed (player
# selection, Min GP, stat filters). Regression guard for the bug where selecting
# one player collapsed the ranking population to a single row.

make_pop <- function() {
  data.frame(
    team_id = 1:5, player_id = 1:5,
    Player = paste0("P", 1:5), team_name = "T",
    poss_on_floor = c(500, 400, 300, 200, 100),
    `.poss_rank_base` = c(500, 400, 300, 200, 100),
    pts = c(25, 20, 15, 10, 5),
    tov = c(1, 2, 3, 4, 5),
    check.names = FALSE, stringsAsFactors = FALSE
  )
}

test_that("add_ts_percentile_cols ranks over the full population", {
  out <- add_ts_percentile_cols(make_pop())
  # percent_rank is league-relative: top scorer = 1, bottom = 0.
  expect_equal(out[[ts_pr_colname("PTS")]], c(1, 0.75, 0.5, 0.25, 0))
  # TOV column gets its own pr (reverse polarity is applied later at render time).
  expect_equal(out[[ts_pr_colname("TOV")]], c(0, 0.25, 0.5, 0.75, 1))
})

test_that("a player's percentile is stable when the table is narrowed to one row", {
  full <- add_ts_percentile_cols(make_pop())
  one <- full[full$Player == "P3", , drop = FALSE]
  # Narrowing the DISPLAY to one row keeps the league-relative color.
  expect_equal(one[[ts_pr_colname("PTS")]], 0.5)

  # Contrast: ranking a single-row population (the old bug) yields no usable rank.
  buggy <- add_ts_percentile_cols(make_pop()[3, , drop = FALSE])
  expect_true(is.nan(buggy[[ts_pr_colname("PTS")]]) || is.na(buggy[[ts_pr_colname("PTS")]]))
})

test_that("add_ts_percentile_cols is a no-op on empty/NULL input", {
  expect_null(add_ts_percentile_cols(NULL))
  empty <- make_pop()[0, , drop = FALSE]
  out <- add_ts_percentile_cols(empty)
  expect_equal(nrow(out), 0L)
})
