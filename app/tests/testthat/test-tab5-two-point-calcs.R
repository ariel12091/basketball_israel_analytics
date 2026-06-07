test_that("tab5 calculates two-point columns from field-goal and three-point columns", {
  df <- data.frame(
    fgm = c(10, 3),
    fga = c(20, 5),
    `3pm` = c(4, 3),
    `3pa` = c(10, 5),
    check.names = FALSE
  )

  out <- add_ts_two_point_stats(df)

  expect_equal(out$`2pm`, c(6, 0))
  expect_equal(out$`2pa`, c(10, 0))
  expect_equal(out$two_pct, c(60, NA_real_))
})

test_that("tab5 normalizes player traditional rebound split columns", {
  split_df <- data.frame(
    player_name = c("Player A", "Player B"),
    oreb = c(3, 5),
    dreb = c(7, 6),
    check.names = FALSE
  )
  split_out <- normalize_ts_result_cols(split_df)

  expect_equal(split_out$Player, c("Player A", "Player B"))
  expect_equal(split_out$reb, c(10, 11))
  expect_equal(split_out$oreb, c(3, 5))
  expect_equal(split_out$dreb, c(7, 6))

  legacy_df <- data.frame(reb = c(8, 4), check.names = FALSE)
  legacy_out <- normalize_ts_result_cols(legacy_df)

  expect_true(all(c("oreb", "dreb") %in% names(legacy_out)))
  expect_equal(legacy_out$reb, c(8, 4))
  expect_true(all(is.na(legacy_out$oreb)))
  expect_true(all(is.na(legacy_out$dreb)))
})

test_that("tab5 player choices narrow to selected teams", {
  players <- data.frame(
    team_id = c(1L, 1L, 2L),
    player_id = c(11L, 12L, 21L),
    name = c("Player A", "Player C", "Player B"),
    stringsAsFactors = FALSE
  )
  teams <- data.frame(
    team_id = c(1L, 2L),
    team_name = c("Team A", "Team B"),
    stringsAsFactors = FALSE
  )

  all_choices <- ts_player_choices(players, teams)
  team_a_choices <- ts_player_choices(players, teams, team_ids = 1L)

  expect_equal(unname(all_choices), c("1:11", "2:21", "1:12"))
  expect_equal(names(all_choices), c("Player A (Team A)", "Player B (Team B)", "Player C (Team A)"))
  expect_equal(unname(team_a_choices), c("1:11", "1:12"))
  expect_equal(names(team_a_choices), c("Player A (Team A)", "Player C (Team A)"))
})

test_that("tab5 player filter uses team-player keys", {
  df <- data.frame(
    team_id = c(1L, 1L, 2L),
    player_id = c(11L, 12L, 11L),
    Player = c("Player A", "Player C", "Player A"),
    pts = c(10, 20, 30),
    stringsAsFactors = FALSE
  )

  filtered <- filter_ts_players(df, c("1:11", "2:11"))

  expect_equal(filtered$pts, c(10, 30))
})

test_that("tab5 empty state calls out selected players without data", {
  expect_equal(ts_no_data_message(character(0)), "No data for current filters")
  expect_equal(ts_no_data_message("1:99"), "No data for current filters")
})
