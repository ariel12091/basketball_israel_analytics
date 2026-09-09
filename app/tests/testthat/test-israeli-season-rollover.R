.rollover_defs <- global_defs(
  "DEFAULT_GAME_YEAR",
  "STATIC_TEAM_ROSTERS",
  "static_team_roster",
  "SEASON_DATE_BOUNDS",
  "season_date_bounds_for_year"
)

test_that("Israeli 2026-27 season is the default with its provider teams", {
  expect_identical(.rollover_defs$DEFAULT_GAME_YEAR, "2027")

  roster <- .rollover_defs$static_team_roster(2027L)
  expect_equal(nrow(roster), 14L)
  expect_setequal(roster$team_id, c(2L, 3L, 4L, 5L, 6L, 7L, 8L, 9L, 10L, 11L, 12L, 14L, 17L, 33L))
})

test_that("Israeli 2026-27 date bounds include the September opening games", {
  bounds <- .rollover_defs$season_date_bounds_for_year(2027L)
  expect_identical(bounds$start, as.Date("2026-09-01"))
  expect_identical(bounds$end, as.Date("2027-07-01"))

  # Historical seasons retain the established fallback window.
  old_bounds <- .rollover_defs$season_date_bounds_for_year(2026L)
  expect_identical(old_bounds$start, as.Date("2025-10-01"))
  expect_identical(old_bounds$end, as.Date("2026-07-01"))
})
