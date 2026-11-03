# compute_possessions(): a free throw awarded for a technical, unsportsmanlike or
# disqualifying foul does not end the possession; the shooting team keeps the ball
# (docs/shared_definitions.md, Possession).
# Run: Rscript -e "testthat::test_file('etl/tests/test_possession_rules.R')"
library(testthat)
suppressPackageStartupMessages({ library(dplyr); library(dbplyr) })
source("../possession_rules.R")

test_that("every special-foul label keeps the possession, in every season's spelling", {
  for (kind in c("technical", "technical-behavioral", "technical-administrative",
                 "unsportsmanlike", "unsportsmanlike-flagrant", "unsportsmanlike-disruptive",
                 "OffensiveFoulUnsportsmanlike", "disqualifying")) {
    expect_equal(ends(trip(kind)), 4L, info = kind)
  }
})

test_that("a personal-foul trip still ends the possession", {
  expect_equal(ends(trip("personal")), c(3L, 4L))
})

test_that("a special trip that is the last event of a period still ends it", {
  rows <- dplyr::bind_rows(foul(1, "unsportsmanlike-flagrant"), ft(2, 1, 1, 2), ft(3, 1, 2, 2),
                           act(4, "period", ptype = "end-of-quarter"))
  expect_equal(ends(rows), 3L)
})

test_that("an unsportsmanlike foul on a made basket: neither the basket nor the free throw ends it", {
  rows <- dplyr::bind_rows(shot(1), foul(2, "unsportsmanlike-flagrant", parent = 1),
                           ft(3, 1, 1, 1), shot(4, ptype = "jump-shot"))
  expect_equal(ends(rows), 4L)
})

test_that("the turnover of an offensive unsportsmanlike foul still ends the offense's possession", {
  # Team 1 has the ball and commits the foul; team 2 shoots, keeps the ball and scores.
  rows <- dplyr::bind_rows(foul(1, "OffensiveFoulUnsportsmanlike", team = 1L),
                           act(2, "turnover", parent = 1, ptype = "offensive-foul", team = 1L),
                           ft(3, 1, 1, 2, team = 2L), ft(4, 1, 2, 2, team = 2L),
                           shot(5, team = 2L))
  expect_equal(ends(rows), c(2L, 5L))
})

test_that("substitutions and timeouts between the trip and the period end do not hide the period end", {
  rows <- dplyr::bind_rows(foul(1, "unsportsmanlike-flagrant"), ft(2, 1, 1, 2), ft(3, 1, 2, 2),
                           act(4, "substitution"), act(5, "timeout"),
                           act(6, "quarter", ptype = "end-of-quarter"))
  expect_equal(ends(rows), 3L)
})

test_that("a technical free throw shot by the team without the ball is not an end", {
  # Team 1 has the ball; team 1 commits a technical, team 2 shoots, then team 1 scores.
  rows <- dplyr::bind_rows(foul(1, "technical-behavioral", team = 1L),
                           ft(2, 1, 1, 1, team = 2L), shot(3, team = 1L))
  expect_equal(ends(rows), 3L)
})

# Fixtures live in helper-possession-fixtures.R so other scripts can reuse the rows.
test_that("a made and-one the team rebounds keeps the possession open", {
  for (nm in names(POSSESSION_FIXTURES)) {
    fx <- POSSESSION_FIXTURES[[nm]]
    expect_equal(ends(fx$rows), fx$ends, info = nm)
  }
})

test_that("a put-back's own rebound and a mislabelled foul turnover are not possession ends", {
  for (nm in names(PHANTOM_END_FIXTURES)) {
    fx <- PHANTOM_END_FIXTURES[[nm]]
    expect_equal(ends(fx$rows), fx$ends, info = nm)
  }
})
