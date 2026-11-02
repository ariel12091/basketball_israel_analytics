# compute_possessions(): a free throw awarded for a technical, unsportsmanlike or
# disqualifying foul does not end the possession; the shooting team keeps the ball
# (docs/shared_definitions.md, Possession).
# Run: Rscript -e "testthat::test_file('etl/tests/test_special_foul_possession.R')"
library(testthat)
suppressPackageStartupMessages({ library(dplyr); library(dbplyr) })
source("../possession_rules.R")

con <- DBI::dbConnect(duckdb::duckdb())
withr::defer(DBI::dbDisconnect(con, shutdown = TRUE), teardown_env())

# One row per action, in the actions_clean columns compute_possessions() reads.
act <- function(id, type, parent = id, ptype = NA_character_, made = NA_character_,
                pts = NA_real_, ft_no = NA_integer_, ft_of = NA_integer_, team = 1L, q = 1L) {
  tibble::tibble(game_id = 1L, id = as.integer(id), parent_action_id = as.integer(parent),
                 quarter = as.integer(q), end_game_seconds_remaining = 2400 - as.numeric(id),
                 user_time = sprintf("20:%02d:00", as.integer(id)), team_id = as.integer(team),
                 type = type, parameters_type = ptype, parameters_made = made,
                 parameters_points = pts, parameters_free_throw_number = as.integer(ft_no),
                 parameters_free_throws_awarded = as.integer(ft_of))
}
ft <- function(id, parent, no, of, made = "made", team = 1L)
  act(id, "freeThrow", parent, made = made, pts = if (made == "made") 1 else NA_real_,
      ft_no = no, ft_of = of, team = team)
shot <- function(id, parent = id, made = "made", ptype = "lay-up", team = 1L)
  act(id, "shot", parent, ptype = ptype, made = made, pts = 2, team = team)
foul <- function(id, kind, parent = id, team = 2L) act(id, "foul", parent, ptype = kind, team = team)

# Ids whose final_end_poss is TRUE (NA and FALSE are both "not an end").
ends <- function(rows) {
  name <- paste0("a", sample.int(1e9, 1))
  dplyr::copy_to(con, rows, name, overwrite = TRUE)
  out <- compute_possessions(dplyr::tbl(con, name)) |> dplyr::collect()
  sort(out$id[out$final_end_poss %in% TRUE])
}

# Team 2 fouls team 1, two made free throws, then team 1 scores.
trip <- function(kind) dplyr::bind_rows(foul(1, kind), ft(2, 1, 1, 2), ft(3, 1, 2, 2), shot(4))

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
