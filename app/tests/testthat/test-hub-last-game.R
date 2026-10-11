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

test_that("on wide screens the hub lays its boxes side by side", {
  css <- read_repo_txt("www", "app.css")
  wide <- regmatches(css, regexpr("@media \\(min-width: 1200px\\) \\{\\s*/\\* Home hub: wide layout[\\s\\S]*?\\n\\}\\r?\\n", css, perl = TRUE))

  expect_length(wide, 1L)
  # The grid is the wrapper that holds the hub AND the question links, so the
  # links can sit in the hub's own row; the hub section dissolves into it.
  expect_match(wide, "\\.home-hub-layout\\s*\\{[^}]*display:\\s*grid")
  expect_match(wide, "#team_hub_section\\s*\\{[^}]*display:\\s*contents")
  # Key players and lineups, then the question links beside them.
  expect_match(wide, "#team_hub_section > \\.row\\s*\\{[^}]*grid-column:\\s*1 / span 8")
  expect_match(wide, "\\.home-nav-group\\s*\\{[^}]*grid-column:\\s*9 / -1")
  # Team card and last game on the left, storylines beside them.
  expect_match(wide, "#hub_identity\\s*\\{[^}]*grid-column:\\s*1 / span 7")
  expect_match(wide, "#hub_last_game\\s*\\{[^}]*grid-column:\\s*1 / span 7")
  expect_match(wide, "\\.hub-storylines-shell\\s*\\{[^}]*grid-row:\\s*1 / span 2")
  # Shiny suspends an output it believes is hidden, so an empty output must
  # never be display:none or it would never receive its first value.
  expect_false(grepl("hub-storylines-output:empty\\s*\\{[^}]*display:\\s*none", css))
  expect_false(grepl("#hub_[a-z_]+:empty\\s*\\{[^}]*display:\\s*none", css))
})

test_that("a hub box with nothing to show gives its width to its neighbour", {
  css <- read_repo_txt("www", "app.css")

  # Early in a season a team has no 100-possession lineup, so the Lineups box
  # is empty; Key players must then fill the row instead of leaving a hole.
  expect_match(css, "#team_hub_section > \\.row > \\[class\\*=\"col-\"\\]\\s*\\{[^}]*flex:\\s*1 1 0")
  empty_rule <- regmatches(css, regexpr(
    "#team_hub_section > \\.row > \\[class\\*=\"col-\"\\]:has\\(> \\.shiny-html-output:empty\\)\\s*\\{[^}]*\\}", css))
  expect_length(empty_rule, 1L)
  expect_match(empty_rule, "flex:\\s*0 0 0")
  # Collapsed to zero width, never display:none -- Shiny would suspend it.
  expect_false(grepl("display:\\s*none", empty_rule))
})

test_that("the Israeli question cards are one titled group inside the hub layout", {
  home <- read_repo_txt("R", "ui_tab0_home.R")

  expect_true(grepl('class = "league-only-il home-hub-layout"', home, fixed = TRUE))
  expect_true(grepl('class = "home-nav-group"', home, fixed = TRUE))
  expect_true(grepl('"Investigate further"', home, fixed = TRUE))
  # Still the shared card builder: the group wraps it, it does not replace it.
  layout_at <- regexpr('class = "league-only-il home-hub-layout"', home, fixed = TRUE)
  group_at <- regexpr('class = "home-nav-group"', home, fixed = TRUE)
  cards_at <- regexpr("home_nav_cards(list(", substr(home, group_at, nchar(home)), fixed = TRUE)
  expect_gt(group_at, layout_at)
  expect_gt(cards_at, 0)
  expect_lt(cards_at, 400)
})

test_that("the question links stay cards with their answers below 1200px", {
  css <- read_repo_txt("www", "app.css")
  wide_at <- regexpr("/* Home hub: wide layout", css, fixed = TRUE)

  # The compact, answer-less look exists only inside the wide-layout block.
  first_hide <- regexpr("\\.home-nav-group \\.home-nav-card-sub\\s*\\{[^}]*display:\\s*none", css)
  expect_gt(first_hide, wide_at)
  expect_match(css, "\\.home-nav-group-title\\s*\\{[^}]*display:\\s*none")
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
