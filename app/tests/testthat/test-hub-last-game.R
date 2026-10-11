# Home team hub: "Last game" line with its Gameflow link. Pure helpers in
# R/helpers.R -- no DB, no Shiny session.

source(repo_file("R", "mod_team_hub.R"), local = TRUE)

last_games <- data.frame(
  game_id = c(419L, 419L, 658L),
  game_date = as.Date(c("2026-10-10", "2026-10-10", "2026-10-05")),
  team_id = c(2L, 10L, 7L),
  team_name = c("Maccabi Tel-Aviv", "Hapoel Galil Elion", "Maccabi Ramat Gan"),
  opp_team_name = c("Hapoel Galil Elion", "Maccabi Tel-Aviv", "Maccabi Ashdod"),
  team_score = c(120L, 93L, 84L),
  opp_score = c(93L, 120L, 89L),
  has_won = c(TRUE, FALSE, FALSE),
  is_home = c(TRUE, FALSE, TRUE),
  has_gameflow = c(TRUE, TRUE, FALSE),
  stringsAsFactors = FALSE
)

test_that("hub_last_game picks the selected team's row", {
  g <- hub_last_game(last_games, "10")

  expect_identical(g$game_id, 419L)
  expect_identical(g$team_score, 93L)
  expect_identical(g$opp_team_name, "Maccabi Tel-Aviv")
})

test_that("hub_last_game is NULL when there is nothing to show", {
  expect_null(hub_last_game(last_games, "999"))
  expect_null(hub_last_game(last_games, ""))
  expect_null(hub_last_game(last_games, "abc"))
  expect_null(hub_last_game(NULL, "2"))
  expect_null(hub_last_game(last_games[0, ], "2"))
})

test_that("the result reads from the selected team's side", {
  expect_identical(hub_last_game_result(hub_last_game(last_games, "2")), "W 120-93")
  expect_identical(hub_last_game_result(hub_last_game(last_games, "10")), "L 93-120")
})

test_that("home and away are told apart, with the date short and locale-free", {
  expect_identical(hub_last_game_context(hub_last_game(last_games, "2")),
                   paste("vs Hapoel Galil Elion", intToUtf8(183L), "Oct 10"))
  expect_identical(hub_last_game_context(hub_last_game(last_games, "10")),
                   paste("at Maccabi Tel-Aviv", intToUtf8(183L), "Oct 10"))
})

test_that("the line carries a Gameflow link wired to the hub's own opener", {
  html <- as.character(hub_last_game_ui(hub_last_game(last_games, "2")))

  expect_match(html, "Last game", fixed = TRUE)
  expect_match(html, "W 120-93", fixed = TRUE)
  expect_match(html, 'class="ribbon-link"', fixed = TRUE)
  expect_match(html, 'data-game-id="419"', fixed = TRUE)
  expect_match(html, 'data-team-id="2"', fixed = TRUE)
  expect_match(html, 'data-input-id="hub_ribbon_click"', fixed = TRUE)
  expect_match(html, 'data-own-team="Maccabi Tel-Aviv"', fixed = TRUE)
  expect_match(html, "View Gameflow", fixed = TRUE)
  # A win and a loss are coloured through the existing hub classes.
  expect_match(html, "hub-pos", fixed = TRUE)
  expect_match(as.character(hub_last_game_ui(hub_last_game(last_games, "10"))), "hub-neg", fixed = TRUE)
})

test_that("a game with no Gameflow data shows its score but offers no link", {
  html <- as.character(hub_last_game_ui(hub_last_game(last_games, "7")))

  expect_match(html, "L 84-89", fixed = TRUE)
  expect_false(grepl("ribbon-link", html, fixed = TRUE))
  expect_false(grepl("View Gameflow", html, fixed = TRUE))
})

test_that("no game renders nothing at all", {
  expect_null(hub_last_game_ui(NULL))
})

test_that("team names reach the page escaped", {
  g <- hub_last_game(last_games, "2")
  g$opp_team_name <- '<img src=x onerror="alert(1)">'
  html <- as.character(hub_last_game_ui(g))

  expect_false(grepl("<img", html, fixed = TRUE))
})

test_that("the hub is wired for Gameflow on desktop and on touch", {
  hub <- read_repo_txt("R", "mod_team_hub.R")
  mobile_js <- read_repo_txt("www", "mobile.js")

  expect_true(grepl('uiOutput("hub_last_game")', hub, fixed = TRUE))
  # Touch devices draw Gameflow inline, in a panel the hub must own.
  expect_true(grepl('ribbon_inline_ui("hub")', hub, fixed = TRUE))
  expect_true(grepl('ribbon_modal_server(input, output, session, "hub", "israel"', hub, fixed = TRUE))
  expect_match(mobile_js, 'INLINE_PREFIXES = \\["gl", "eurogl", "hub"\\]')
  # The last game is one season-level query shared by every session.
  expect_true(grepl("hub_fetch_last_games <- function(gy, ver)", hub, fixed = TRUE))
  expect_true(grepl("DISTINCT ON (s.team_id)", hub, fixed = TRUE))
})

test_that("the Gameflow link sits outside the clickable identity card", {
  # The identity card is one big js-shiny-event button that opens Team
  # Ratings; a link inside it would fire both.
  html <- htmltools::renderTags(team_hub_ui())$html
  identity_at <- regexpr('id="hub_identity"', html, fixed = TRUE)
  last_at <- regexpr('id="hub_last_game"', html, fixed = TRUE)

  expect_gt(last_at, identity_at)
  expect_false(grepl('js-shiny-event[^>]*>\\s*<div[^>]*id="hub_last_game"', html))
})
