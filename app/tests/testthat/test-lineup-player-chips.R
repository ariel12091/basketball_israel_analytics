# The chip roster (lineup_player_filter_ui(layout = "chips")) and the group
# count it adds: "at least k of" the any-of players, where k = 1 is the plain
# any-of every other surface still sends.

chips_fixture <- function() {
  data.frame(
    team_id = c(1L, 1L, 1L, 1L, 2L),
    player_ids = c("{10,11,12,13,14}",
                   "{10,11,20,21,22}",
                   "{10,30,31,32,33}",
                   "{11,40,41,42,43}",
                   "{50,51,52,53,54}"),
    total_poss = c(100L, 90L, 80L, 70L, 60L),
    stringsAsFactors = FALSE
  )
}

chips_poss <- function(...) {
  p <- modifyList(
    list(team_csv = NA_character_, player_csv = NA_character_,
         player_off_csv = NA_character_),
    list(...)
  )
  apply_local_lineup_filters(chips_fixture(), p)$total_poss
}

# ---- the count --------------------------------------------------------------

test_that("at least 2 of a group needs two of them on together", {
  # {10, 11, 40}: rows 1-2 hold 10 and 11, row 4 holds 11 and 40; row 3 holds
  # only 10, which satisfies "one of" but not "two of".
  expect_equal(chips_poss(player_csv = "10,11,40", player_required_csv = ""),
               c(100L, 90L, 80L, 70L))
  expect_equal(chips_poss(player_csv = "10,11,40", player_required_csv = "",
                          player_any_min = 2L),
               c(100L, 90L, 70L))
})

test_that("the count applies to the group, on top of the required players", {
  # 10 required; at least 2 of {11, 12, 30}. Row 1 holds 11 and 12; row 2 only
  # 11 and row 3 only 30, so both fall to the count, not to the requirement.
  expect_equal(chips_poss(player_csv = "10,11,12,30", player_required_csv = "10",
                          player_any_min = 2L),
               100L)
})

test_that("an absent count is the plain any-of", {
  expect_equal(chips_poss(player_csv = "10,11,40", player_required_csv = ""),
               chips_poss(player_csv = "10,11,40", player_required_csv = "",
                          player_any_min = 1L))
})

test_that("a count larger than the group is clamped to the whole group", {
  # A stale count left from a bigger group must not empty the table.
  expect_equal(chips_poss(player_csv = "10,11", player_required_csv = "",
                          player_any_min = 5L),
               c(100L, 90L))
})

test_that("an unusable count falls back to 1", {
  one <- chips_poss(player_csv = "10,11,40", player_required_csv = "")
  for (bad in list(NA, "abc", 0L, -3L, c(2L, 3L), NULL)) {
    expect_equal(chips_poss(player_csv = "10,11,40", player_required_csv = "",
                            player_any_min = bad), one)
  }
})

test_that("parse_any_min clamps into [1, group size]", {
  expect_equal(parse_any_min(2L, 3L), 2L)
  expect_equal(parse_any_min("2", 3L), 2L)
  expect_equal(parse_any_min(9L, 3L), 3L)
  expect_equal(parse_any_min(NULL, 3L), 1L)
  expect_equal(parse_any_min(2L, 0L), 1L)
})

# ---- roster order -----------------------------------------------------------

test_that("season minutes join onto the roster by team and player", {
  roster <- data.frame(team_id = c(1L, 1L, 2L), player_id = c(10L, 11L, 10L),
                       name = c("A", "B", "C"))
  season <- data.frame(team_id = c(1L, 2L), player_id = c(11L, 10L),
                       minutes = c(300, 120))

  out <- with_season_minutes(roster, season)

  # Keyed on the pair: player 10 is on both teams and only team 2's row has
  # minutes. A roster player with no season row stays, with NA.
  expect_equal(out$minutes, c(NA, 300, 120))
  expect_equal(out$name, roster$name)
})

test_that("a missing or failed stats pull leaves the roster untouched", {
  roster <- data.frame(team_id = 1L, player_id = 10L, name = "A")

  expect_identical(with_season_minutes(roster, NULL), roster)
  expect_identical(with_season_minutes(roster, data.frame(team_id = 1L)), roster)
})

# ---- wiring -----------------------------------------------------------------

count_hits <- function(txt, needle) {
  m <- gregexpr(needle, txt, fixed = TRUE)[[1]]
  if (length(m) == 1L && m[[1]] == -1L) 0L else length(m)
}

test_that("the chip layout keeps the three boxes as its hidden model", {
  # tt() lives in global.R, which the tests do not source; the chip layout
  # never shows these labels anyway.
  html <- as.character(lineup_player_filter_ui(
    "ld_x", layout = "chips", players_on_label = NULL, players_on_any_label = NULL
  ))

  expect_true(grepl('id="ld_x-chips"', html, fixed = TRUE))
  expect_true(grepl('data-ns="ld_x-"', html, fixed = TRUE))
  expect_true(grepl("lineup-chips-model", html, fixed = TRUE))
  for (box in c("players_on", "players_on_any", "players_off")) {
    expect_true(grepl(sprintf('id="ld_x-%s"', box), html, fixed = TRUE), info = box)
  }
  for (mode in c("on", "any", "off")) {
    expect_true(grepl(sprintf('data-mode="%s"', mode), html, fixed = TRUE), info = mode)
  }
})

test_that("Tabs 2 and 10 use the chips; Compare keeps its dropdowns", {
  for (f in c("ui_tab2_lineup.R", "ui_tab10_euro_lineups.R")) {
    expect_true(grepl('layout = "chips"', read_repo_txt("R", f), fixed = TRUE), info = f)
  }
  for (f in c("server_tab2.R", "server_tab10_euro_lineups.R")) {
    expect_true(grepl("chips = TRUE", read_repo_txt("R", f), fixed = TRUE), info = f)
  }
  expect_false(grepl('layout = "chips"', read_repo_txt("R", "ui_tab7_compare.R"), fixed = TRUE))
  expect_false(grepl("chips = TRUE", read_repo_txt("R", "server_tab7_compare.R"), fixed = TRUE))
})

test_that("the count reaches the filter and every trigger that must see it", {
  tab2 <- read_repo_txt("R", "server_tab2.R")
  # ld_params(), its bindEvent, both auto-min observers, and the chip bar.
  expect_equal(count_hits(tab2, "ld_lineup_filter$players_on_any_min()"), 5L)
  expect_true(grepl("player_any_min = ld_lineup_filter$players_on_any_min()", tab2, fixed = TRUE))

  tab10 <- read_repo_txt("R", "server_tab10_euro_lineups.R")
  # The local filter, the auto-min inputs, the auto-min observer, the chip bar.
  expect_equal(count_hits(tab10, "ld_filter$players_on_any_min()"), 4L)
  expect_true(grepl("player_any_min = ld_filter$players_on_any_min()", tab10, fixed = TRUE))
})

test_that("the server message and the client handler agree on a name", {
  expect_true(grepl('"lineup-chips-roster"', read_repo_txt("R", "mod_lineup_player_filter.R"), fixed = TRUE))
  js <- read_repo_txt("www", "app.js")
  expect_true(grepl('addCustomMessageHandler("lineup-chips-roster"', js, fixed = TRUE))
  # ... and on the input only the widget sets.
  expect_true(grepl('"players_on_any_min"', js, fixed = TRUE))
})
