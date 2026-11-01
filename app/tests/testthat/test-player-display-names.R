# The "Player display name" owner (docs/shared_definitions.md, D27):
# owner_player_names() applies player_display_names_v by (team_id, player_id),
# keeping each page's own spelling where the owner has no name.

test_that("owner names replace the page spelling by (team, player)", {
  names <- data.frame(team_id = c(1L, 1L, 2L), player_id = c(10L, 11L, 10L),
                      display_name = c("OWNER TEN", "", "OTHER TEAM TEN"))
  got <- owner_player_names(
    team_id = c(1, 1, 2, 1, 3),
    player_id = c("10", 11, 10, 12, 10),
    names = names,
    fallback = c("Roster Ten", "Roster Eleven", "Roster B", "Roster Twelve", "Roster C")
  )
  expect_identical(got, c("OWNER TEN", "Roster Eleven", "OTHER TEAM TEN",
                          "Roster Twelve", "Roster C"))
})

test_that("an empty or malformed owner read leaves every name unchanged", {
  fb <- c("A", "B")
  expect_identical(owner_player_names(1:2, 1:2, data.frame(), fb), fb)
  expect_identical(owner_player_names(1:2, 1:2, NULL, fb), fb)
  expect_identical(owner_player_names(1:2, 1:2, data.frame(team_id = 1L), fb), fb)
  expect_identical(owner_player_names(integer(0), integer(0), data.frame(), character(0)),
                   character(0))
})

test_that("the lineup and Compare picker pool takes the owner's names", {
  env <- environment(fetch_players_named)
  stub <- get("fetch_player_display_names", envir = env)
  assign("fetch_player_display_names", function(gy) {
    data.frame(team_id = 1L, player_id = 12L, display_name = "OWNER C")
  }, envir = env)
  on.exit(assign("fetch_player_display_names", stub, envir = env), add = TRUE)
  players <- fetch_players_named(2026)
  expect_identical(players$name, c("Player A", "OWNER C", "Player B"))
  expect_identical(players$player_id, c(11L, 12L, 21L))
})
