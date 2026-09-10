.rollover_defs <- global_defs(
  "DEFAULT_GAME_YEAR",
  "STATIC_TEAM_ROSTERS",
  "static_team_roster",
  "SEASON_DATE_BOUNDS",
  "season_date_bounds_for_year",
  "GAME_TYPE_LABELS",
  "GAME_TYPE_CHOICES_UI"
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

  bounds_2026 <- .rollover_defs$season_date_bounds_for_year(2026L)
  expect_identical(bounds_2026$start, as.Date("2025-09-01"))
  expect_identical(bounds_2026$end, as.Date("2026-07-01"))

  bounds_2025 <- .rollover_defs$season_date_bounds_for_year(2025L)
  expect_identical(bounds_2025$start, as.Date("2024-09-01"))
  expect_identical(bounds_2025$end, as.Date("2025-07-01"))
})

test_that("Winner Cup filters include both legacy and 2027 provider game types", {
  expect_identical(unname(.rollover_defs$GAME_TYPE_CHOICES_UI[["Winner Cup"]]), "10,34")
  expect_identical(unname(.rollover_defs$GAME_TYPE_LABELS[c("10", "34")]),
                   c("Winner Cup", "Winner Cup"))
})
