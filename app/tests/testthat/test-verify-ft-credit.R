library(testthat)

# The gate script for the free-throw credit migration must FAIL on an
# unexpected change, not merely print it. These pin the pure comparison logic
# in scripts/verify_ft_credit_helpers.R (no database).

source(repo_file("..", "scripts", "verify_ft_credit_helpers.R"), local = TRUE)

teams <- function(pts = c(10, 20), ts = c(50, 55), minutes = c(40, 40)) {
  data.frame(team_id = c(1L, 2L), pts = pts, ts = ts, minutes = minutes)
}

test_that("a changed value in a column that is not permitted is a failure", {
  out <- fn_result_failures(teams(), teams(pts = c(999, 20)), key = "team_id",
                            spec = list(allowed = "ts"))
  expect_length(out, 1L)
  expect_match(out, "pts")
})

test_that("a change confined to permitted columns passes", {
  expect_length(fn_result_failures(teams(), teams(ts = c(51, 56)), key = "team_id",
                                   spec = list(allowed = "ts")), 0L)
})

test_that("no permitted columns means nothing may change", {
  expect_length(fn_result_failures(teams(), teams(), key = "team_id", spec = list()), 0L)
  expect_length(fn_result_failures(teams(), teams(ts = c(51, 55)), key = "team_id", spec = list()), 1L)
})

test_that("a frozen column may not change even when every column is permitted", {
  spec <- list(allowed = "*", frozen = "minutes")
  expect_length(fn_result_failures(teams(), teams(pts = c(11, 20)), key = "team_id", spec = spec), 0L)
  out <- fn_result_failures(teams(), teams(minutes = c(39, 40)), key = "team_id", spec = spec)
  expect_length(out, 1L)
  expect_match(out, "minutes")
})

test_that("a column that appears or disappears is a failure", {
  new <- teams()
  new$extra <- 1
  expect_match(fn_result_failures(teams(), new, key = "team_id", spec = list(allowed = "*")), "extra")
  expect_match(fn_result_failures(teams(), teams()[, c("team_id", "pts", "ts")], key = "team_id",
                                  spec = list(allowed = "*")), "minutes")
})

test_that("a different row count fails unless the reader is declared row-unstable", {
  old <- teams()
  new <- old[1, , drop = FALSE]
  expect_match(fn_result_failures(old, new, key = "team_id", spec = list(allowed = "*")), "rows")
  expect_length(fn_result_failures(old, new, key = "team_id",
                                   spec = list(allowed = "*", rows = "any")), 0L)
})

test_that("frozen columns are still compared on the rows both results share", {
  old <- teams()
  new <- teams(minutes = c(39, 40))[1, , drop = FALSE]
  out <- fn_result_failures(old, new, key = "team_id",
                            spec = list(allowed = "*", frozen = "minutes", rows = "any"))
  expect_match(out, "minutes")
})

test_that("a frozen column shown only when another column exists is compared where both exist", {
  old <- data.frame(team_id = 1:2, net = c(1.5, 2), minutes = c(10, 0.3))
  spec <- list(allowed = "*", frozen = "minutes", frozen_where = c(minutes = "net"))
  # the reader stopped reporting a diff for team 2, and with it the minutes
  hidden <- data.frame(team_id = 1:2, net = c(1.7, NA), minutes = c(10, NA))
  expect_length(fn_result_failures(old, hidden, key = "team_id", spec = spec), 0L)
  # minutes moved on a row where the diff is still reported
  moved <- data.frame(team_id = 1:2, net = c(1.7, 2), minutes = c(9, 0.3))
  expect_match(fn_result_failures(old, moved, key = "team_id", spec = spec), "minutes")
  # minutes vanished although the diff is still reported
  lost <- data.frame(team_id = 1:2, net = c(1.7, 2), minutes = c(10, NA))
  expect_match(fn_result_failures(old, lost, key = "team_id", spec = spec), "minutes")
  # a condition naming a column the reader does not return compares nothing
  typo <- list(allowed = "*", frozen = "minutes", frozen_where = c(minutes = "nett"))
  expect_match(fn_result_failures(old, moved, key = "team_id", spec = typo), "nett")
})

test_that("a conserved column may move between rows but its total may not change", {
  spec <- list(allowed = "*", rows = "any", conserved = "pts")
  expect_length(fn_result_failures(teams(), teams(pts = c(12, 18)), key = "team_id", spec = spec), 0L)
  out <- fn_result_failures(teams(), teams(pts = c(12, 17)), key = "team_id", spec = spec)
  expect_match(out, "pts")
  expect_match(out, "30")
  expect_match(out, "29")
  # a dropped row takes its points with it
  expect_match(fn_result_failures(teams(), teams()[1, , drop = FALSE], key = "team_id", spec = spec), "pts")
})

test_that("a case with no snapshot is a failure unless it is declared unsupported", {
  expect_match(missing_case_failures(c("none", "clutch"), available = "none", unsupported = character(0)),
               "clutch")
  expect_length(missing_case_failures(c("none", "clutch"), available = "none", unsupported = "clutch"), 0L)
})

test_that("trip coverage fails when a team-game is missing, extra or wrong, or nothing was compared", {
  expected <- data.frame(game_id = c(1, 1), team_id = c(1, 2), expected = c(80, 90))
  ok <- data.frame(game_id = c(1, 1), team_id = c(1, 2), got = c(80, 90))
  expect_length(trip_coverage_failures(ok, expected), 0L)
  expect_match(trip_coverage_failures(ok[1, ], expected), "missing")
  expect_match(trip_coverage_failures(rbind(ok, data.frame(game_id = 2, team_id = 1, got = 5)), expected), "not expected")
  expect_match(trip_coverage_failures(transform(ok, got = c(80, 91)), expected), "differ")
  expect_match(trip_coverage_failures(ok[0, ], expected[0, ]), "nothing")
})

test_that("a team-game with no actions is acceptable only with an empty trip count", {
  expected <- data.frame(game_id = 1, team_id = 1, expected = 80)
  no_pbp <- data.frame(game_id = c(1, 9), team_id = c(1, 1), got = c(80, NA))
  expect_length(trip_coverage_failures(no_pbp, expected), 0L)
  expect_length(trip_coverage_failures(transform(no_pbp, got = c(80, 0)), expected), 0L)
  expect_match(trip_coverage_failures(transform(no_pbp, got = c(80, 12)), expected), "not expected")
})
