source(repo_file("..", "etl", "onoff_roster_refresh.R"), local = TRUE)

test_that("a new player refreshes earlier games for his team and season only", {
  before <- data.frame(game_year = c(2026L, 2027L, 2027L),
    team_id = c(6L, 6L, 9L), player_id = c(100L, 100L, 200L))
  after <- rbind(before, data.frame(game_year = 2027L, team_id = 6L, player_id = 1384L))
  games <- data.frame(game_year = c(2026L, 2027L, 2027L, 2027L),
    team_id = c(6L, 6L, 6L, 9L), game_id = c(1L, 393L, 413L, 414L))
  expect_identical(onoff_roster_refresh_ids(413L, before, after, games), c(393L, 413L))
  # Existing roster / no roster growth keeps the ordinary incremental path.
  expect_identical(onoff_roster_refresh_ids(413L, after, after, games), 413L)
  # Both teams' new games are still processed, even if only one gains a player.
  expect_identical(onoff_roster_refresh_ids(c(414L, 413L, 413L), before, after, games),
    c(393L, 413L, 414L))
  # An id already on another team or in another season is a new team member.
  moved <- rbind(before, data.frame(game_year = 2027L, team_id = 6L, player_id = 200L))
  expect_identical(onoff_roster_refresh_ids(413L, before, moved, games), c(393L, 413L))
  expect_identical(onoff_roster_refresh_ids(integer(), before, before, games), integer())
})
