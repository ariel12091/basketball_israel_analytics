# compute_possessions(): a free throw awarded for a technical, unsportsmanlike or
# disqualifying foul does not end the possession from 2026-27 (game_year 2027);
# earlier seasons derive exactly what is stored (docs/shared_definitions.md D29).
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
ends <- function(rows, game_year) {
  name <- paste0("a", sample.int(1e9, 1))
  dplyr::copy_to(con, rows, name, overwrite = TRUE)
  out <- compute_possessions(dplyr::tbl(con, name), game_year) |> dplyr::collect()
  sort(out$id[out$final_end_poss %in% TRUE])
}

# Team 2 fouls team 1, two made free throws, then team 1 scores.
trip <- function(kind) dplyr::bind_rows(foul(1, kind), ft(2, 1, 1, 2), ft(3, 1, 2, 2), shot(4))

test_that("an unsportsmanlike trip keeps the possession from 2027, not before", {
  expect_equal(ends(trip("unsportsmanlike-flagrant"), 2027L), 4L)
  expect_equal(ends(trip("unsportsmanlike-flagrant"), 2026L), c(3L, 4L))
  expect_equal(ends(trip("unsportsmanlike"), 2026L), c(3L, 4L))
})

test_that("every 2026-27 special-foul label keeps the possession", {
  for (kind in c("technical-behavioral", "technical-administrative",
                 "unsportsmanlike-disruptive", "unsportsmanlike", "disqualifying")) {
    expect_equal(ends(trip(kind), 2027L), 4L, info = kind)
  }
})

test_that("a personal-foul trip still ends the possession", {
  expect_equal(ends(trip("personal"), 2027L), c(3L, 4L))
})

test_that("a plain technical never ended the possession", {
  expect_equal(ends(trip("technical"), 2026L), 4L)
  expect_equal(ends(trip("technical"), 2027L), 4L)
})

test_that("a special trip that is the last event of a period still ends it", {
  rows <- dplyr::bind_rows(foul(1, "unsportsmanlike-flagrant"), ft(2, 1, 1, 2), ft(3, 1, 2, 2),
                           act(4, "period", ptype = "end-of-quarter"))
  expect_equal(ends(rows, 2027L), 3L)
})

test_that("an unsportsmanlike foul on a made basket: neither the basket nor the free throw ends it", {
  rows <- dplyr::bind_rows(shot(1), foul(2, "unsportsmanlike-flagrant", parent = 1),
                           ft(3, 1, 1, 1), shot(4, ptype = "jump-shot"))
  expect_equal(ends(rows, 2027L), 4L)
})

test_that("one season per call", {
  expect_error(ends(trip("personal"), c(2026L, 2027L)))
  expect_error(ends(trip("personal"), NA_integer_))
})
