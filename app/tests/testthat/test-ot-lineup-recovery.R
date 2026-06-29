library(testthat)

source(repo_file("..", "etl", "ot_lineup_recovery.R"), local = TRUE)

make_ot_roster <- function(game_id = 900L) {
  data.frame(
    game_id = game_id,
    team_id = rep(c(10L, 20L), each = 7L),
    player_id = c(1:7, 101:107),
    game_year = 2026L,
    starter = c(rep(TRUE, 5), FALSE, FALSE, rep(TRUE, 5), FALSE, FALSE),
    stringsAsFactors = FALSE
  )
}

make_action <- function(
  id,
  team_id,
  player_id,
  clock,
  type = "shot",
  game_id = 900L,
  quarter = 5L,
  player_in = NA,
  player_out = NA,
  fouled_on = NA
) {
  data.frame(
    id = as.integer(id),
    game_id = as.integer(game_id),
    team_id = as.integer(team_id),
    player_id = as.integer(player_id),
    quarter = as.integer(quarter),
    quarter_time = sprintf("%02d:%02d", floor(clock / 60), clock %% 60),
    end_quarter_seconds_remaining = as.numeric(clock),
    end_game_seconds_remaining = as.numeric(clock),
    type = type,
    parameters_player_in = player_in,
    parameters_player_out = player_out,
    parameters_fouled_on = fouled_on,
    parameters_is_coach_foul = FALSE,
    parameters_is_bench_foul = FALSE,
    stringsAsFactors = FALSE
  )
}

make_previous_lineups <- function(roster, game_id = 900L, quarter = 4L) {
  rbind(
    ot_make_lineup_state_rows(
      roster[roster$team_id == 10L, ],
      game_id, 10L, quarter, 800L, 0, 1:5
    ),
    ot_make_lineup_state_rows(
      roster[roster$team_id == 20L, ],
      game_id, 20L, quarter, 801L, 0, 101:105
    )
  )
}

test_that("OT carry-forward accepts explained participation", {
  roster <- make_ot_roster()
  previous <- make_previous_lineups(roster)
  actions <- rbind(
    make_action(901L, 10L, 1L, 295),
    make_action(902L, 20L, 101L, 292, type = "rebound")
  )

  recovered <- recover_ot_lineup_periods(
    actions,
    roster,
    previous,
    data.frame(game_id = 900L, quarter = 5L)
  )

  expect_true(all(recovered$audit$recovery_status == "accepted_carry_forward"))
  expect_equal(nrow(recovered$replacement_rows), 14L)
  expect_equal(
    ot_latest_valid_lineup(recovered$lineups, 900L, 10L, 5L),
    1:5
  )
})

test_that("unexplained OT participant rejects recovery", {
  roster <- make_ot_roster()
  previous <- make_previous_lineups(roster)
  actions <- rbind(
    make_action(901L, 10L, 6L, 295),
    make_action(902L, 20L, 101L, 292)
  )

  recovered <- recover_ot_lineup_periods(
    actions,
    roster,
    previous,
    data.frame(game_id = 900L, quarter = 5L)
  )

  team_result <- recovered$audit[recovered$audit$team_id == 10L, ]
  expect_equal(team_result$recovery_status, "rejected_unexplained_participant")
  expect_equal(team_result$unexplained_event_count, 1L)
  expect_equal(nrow(recovered$replacement_rows), 0L)
})

test_that("same-clock substitution ordering is accepted with a warning", {
  roster <- make_ot_roster()
  previous <- make_previous_lineups(roster)
  actions <- rbind(
    make_action(901L, 10L, 1L, 280, type = "turnover"),
    make_action(902L, 10L, 1L, 280, type = "substitution", player_out = TRUE),
    make_action(903L, 10L, 6L, 280, type = "substitution", player_in = TRUE),
    make_action(904L, 20L, 101L, 278)
  )

  recovered <- recover_ot_lineup_periods(
    actions,
    roster,
    previous,
    data.frame(game_id = 900L, quarter = 5L)
  )

  team_result <- recovered$audit[recovered$audit$team_id == 10L, ]
  expect_match(team_result$recovery_status, "^accepted_")
  expect_equal(team_result$ordering_warning_count, 1L)
  expect_equal(
    ot_latest_valid_lineup(recovered$lineups, 900L, 10L, 5L),
    c(2:6)
  )
})

test_that("later substitution does not explain earlier participation", {
  roster <- make_ot_roster()
  previous <- make_previous_lineups(roster)
  actions <- rbind(
    make_action(901L, 10L, 6L, 280, type = "foul"),
    make_action(902L, 10L, 1L, 270, type = "substitution", player_out = TRUE),
    make_action(903L, 10L, 6L, 270, type = "substitution", player_in = TRUE),
    make_action(904L, 20L, 101L, 268)
  )

  recovered <- recover_ot_lineup_periods(
    actions,
    roster,
    previous,
    data.frame(game_id = 900L, quarter = 5L)
  )

  team_result <- recovered$audit[recovered$audit$team_id == 10L, ]
  expect_equal(team_result$recovery_status, "rejected_unexplained_participant")
  expect_equal(team_result$unexplained_event_ids, "901")
})

test_that("five-IN provider declaration is treated as an atomic reset", {
  roster <- make_ot_roster()
  previous <- make_previous_lineups(roster)
  team_10_reset <- do.call(rbind, lapply(seq_along(2:6), function(i) {
    make_action(
      900L + i,
      10L,
      (2:6)[[i]],
      300,
      type = "substitution",
      player_in = TRUE
    )
  }))
  actions <- rbind(
    team_10_reset,
    make_action(910L, 20L, 101L, 295)
  )

  recovered <- recover_ot_lineup_periods(
    actions,
    roster,
    previous,
    data.frame(game_id = 900L, quarter = 5L)
  )

  team_result <- recovered$audit[recovered$audit$team_id == 10L, ]
  expect_equal(team_result$period_start_reset_type, "atomic_five_in_declaration")
  expect_match(team_result$recovery_status, "^accepted_")
  expect_equal(
    ot_latest_valid_lineup(recovered$lineups, 900L, 10L, 5L),
    2:6
  )
})

test_that("temporary period-start player can enter and exit inside a full reset", {
  roster <- make_ot_roster()
  start <- 1:5
  rows <- rbind(
    do.call(rbind, lapply(seq_along(start), function(i) {
      make_action(900L + i, 10L, start[[i]], 300, "substitution", player_out = TRUE)
    })),
    make_action(906L, 10L, 7L, 300, "substitution", player_in = TRUE),
    make_action(907L, 10L, 7L, 300, "substitution", player_out = TRUE),
    do.call(rbind, lapply(seq_along(2:6), function(i) {
      make_action(907L + i, 10L, (2:6)[[i]], 300, "substitution", player_in = TRUE)
    }))
  )
  result <- ot_reconstruct_team_period(
    rows,
    roster,
    normal_lineups = data.frame(),
    previous_players = start,
    team_id = 10L
  )

  expect_true(result$ok)
  expect_equal(result$reset_type, "full_out_in_reset")
  expect_equal(ot_latest_valid_lineup(result$rows, 900L, 10L, 5L), 2:6)
})

test_that("leading OT gap detection ignores periods whose first row is complete", {
  pws <- data.frame(
    game_id = c(900L, 901L),
    quarter = c(5L, 5L),
    id = c(1L, 1L),
    segment_id = c(NA_integer_, 1L),
    team_id_defense = c(20L, 20L),
    lineup_hash_offense = c(NA_character_, "a"),
    lineup_hash_defense = c(NA_character_, "b")
  )

  gaps <- detect_ot_leading_lineup_gaps(pws)
  expect_equal(gaps$game_id, 900L)
  expect_equal(gaps$quarter, 5L)
})
