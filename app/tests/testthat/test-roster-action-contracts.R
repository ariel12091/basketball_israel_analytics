roster_contract_functions <- function() {
  exprs <- parse(file = repo_file("..", "etl", "etl_onoff.R"))
  wanted <- c("action_player_roster_gaps", "assert_action_players_in_roster")
  assignments <- Filter(function(expr) {
    is.call(expr) && identical(expr[[1]], as.name("<-")) &&
      as.character(expr[[2]]) %in% wanted
  }, exprs)
  expect_equal(length(assignments), length(wanted))

  env <- new.env(parent = globalenv())
  invisible(lapply(assignments, eval, envir = env))
  env
}

test_that("action-player roster guard rejects a missing participant", {
  env <- roster_contract_functions()
  actions <- tibble::tibble(
    game_id = c(62461L, 62461L),
    team_id = c(8L, 8L),
    player_id = c(2543L, 2543L),
    type = c("substitution", "shot")
  )
  roster <- tibble::tibble(
    game_id = 62461L,
    team_id = 8L,
    player_id = 2655L
  )

  gaps <- env$action_player_roster_gaps(actions, roster)
  expect_equal(nrow(gaps), 1L)
  expect_equal(gaps$player_id, 2543L)
  expect_equal(gaps$action_rows, 2L)
  expect_error(
    env$assert_action_players_in_roster(actions, roster, function(...) NULL),
    "game=62461 team=8 player=2543"
  )
})

test_that("action-player roster guard passes recovered players and ignores sentinels", {
  env <- roster_contract_functions()
  actions <- tibble::tibble(
    game_id = c(62461L, 62461L, 62461L),
    team_id = c(8L, 8L, 0L),
    player_id = c(2543L, NA_integer_, 0L),
    type = c("shot", "substitution", "substitution")
  )
  roster <- tibble::tibble(
    game_id = 62461L,
    team_id = 8L,
    player_id = 2543L
  )

  expect_equal(nrow(env$action_player_roster_gaps(actions, roster)), 0L)
  expect_invisible(
    env$assert_action_players_in_roster(actions, roster, function(...) NULL)
  )
})
