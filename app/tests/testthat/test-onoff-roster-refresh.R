source(repo_file("..", "etl", "onoff_roster_refresh.R"), local = TRUE)

pairs <- function(team_id, player_id, lineup_hash, is_on_key)
  data.frame(team_id = team_id, player_id = player_id, lineup_hash = lineup_hash,
             is_on_key = is_on_key)

# Team 6 played lineup A in games 393 and 413, B only in 413; team 9 played C.
games <- data.frame(game_id = c(393L, 413L, 413L, 414L), team_id = c(6L, 6L, 6L, 9L),
                    lineup_hash = c("A", "A", "B", "C"))
before <- pairs(c(6L, 6L, 9L), c(100L, 100L, 200L), c("A", "B", "C"), c(1L, 1L, 1L))

test_that("a new player refreshes the earlier games of the lineups he is paired with", {
  after <- rbind(before, pairs(6L, 1384L, c("A", "B"), 0L))
  expect_identical(onoff_roster_refresh_ids(413L, before, after, games), c(393L, 413L))
  # Nothing gained keeps the ordinary incremental path.
  expect_identical(onoff_roster_refresh_ids(413L, after, after, games), 413L)
  # Every processed game is kept, deduplicated, whichever team gained.
  expect_identical(onoff_roster_refresh_ids(c(414L, 413L, 413L), before, after, games),
                   c(393L, 413L, 414L))
  expect_identical(onoff_roster_refresh_ids(integer(), before, before, games), integer())
})

# 2026-11-14: the old trigger fired only for a player new to the team.
test_that("an existing player's new pairing for an old lineup refreshes its earlier games", {
  existing <- rbind(before, pairs(6L, 1384L, "B", 0L))
  gained_a <- rbind(existing, pairs(6L, 1384L, "A", 0L))
  expect_identical(onoff_roster_refresh_ids(413L, existing, gained_a, games), c(393L, 413L))
  # A pairing for a lineup no earlier game played needs no historical game.
  only_b <- rbind(before, pairs(6L, 1384L, "B", 0L))
  expect_identical(onoff_roster_refresh_ids(413L, before, only_b, games), 413L)
  # A flipped on/off verdict is a new pairing as well.
  flipped <- rbind(before, pairs(6L, 100L, "A", 0L))
  expect_identical(onoff_roster_refresh_ids(413L, before, flipped, games), c(393L, 413L))
})

test_that("a pairing on one team never refreshes another team's games", {
  after <- rbind(before, pairs(9L, 1384L, "A", 0L))  # same hash, other team
  expect_identical(onoff_roster_refresh_ids(414L, before, after, games), 414L)
})
