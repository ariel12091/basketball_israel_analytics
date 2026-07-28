# Pure team-hub helpers (R/helpers.R) — no DB, no Shiny session.

teams_df <- data.frame(
  team_id = c(10L, 20L, 30L),
  team_name = c("Alpha", "Beta", "Gamma")
)

ratings_df <- data.frame(
  team_id = c(10L, 20L, 30L),
  team_name = c("Alpha", "Beta", "Gamma"),
  off_ppp = c(110, 105, 100),
  def_ppp = c(100, 104, 108),
  net_rtg = c(10, 1, -8),
  games_played = c(10L, 10L, 10L),
  wins = c(8L, 5L, 2L),
  losses = c(2L, 5L, 8L),
  off_poss = c(800L, 810L, 790L),
  def_poss = c(805L, 800L, 795L),
  rank_net_rtg = c(1L, 2L, 3L),
  rank_off_ppp = c(1L, 2L, 3L),
  rank_def_ppp = c(1L, 2L, 3L)
)

ff_df <- data.frame(
  team_id = c(10L, 20L, 30L),
  off_efg = c(55, 52, 49),
  off_tov = c(12, 14, 16),
  off_oreb = c(30, 28, 26),
  off_ftr = c(25, 22, 20)
)

test_that("hub_default_team prefers a valid remembered id", {
  expect_equal(hub_default_team("20", teams_df, ratings_df), "20")
})

test_that("hub_default_team falls back to net-rating leader", {
  expect_equal(hub_default_team("", teams_df, ratings_df), "10")
  expect_equal(hub_default_team("999", teams_df, ratings_df), "10")
  expect_equal(hub_default_team(NULL, teams_df, ratings_df), "10")
})

test_that("hub_default_team falls back to first team without ratings", {
  expect_equal(hub_default_team("", teams_df, NULL), "10")
  expect_equal(hub_default_team("", teams_df, ratings_df[0, ]), "10")
})

test_that("hub_default_team returns empty string with no teams", {
  expect_equal(hub_default_team("20", teams_df[0, ], ratings_df), "")
})

test_that("hub_identity_data returns team row, league size and ff row", {
  info <- hub_identity_data(ratings_df, ff_df, "20")
  expect_equal(info$row$team_name, "Beta")
  expect_equal(info$n_teams, 3L)
  expect_equal(info$ff$off_efg, 52)
})

test_that("hub_identity_data is NULL for unknown team or empty data", {
  expect_null(hub_identity_data(ratings_df, ff_df, "999"))
  expect_null(hub_identity_data(NULL, ff_df, "10"))
  expect_null(hub_identity_data(ratings_df[0, ], ff_df, "10"))
})

test_that("hub_ff_mini ranks offense factors with TOV inverted", {
  mini <- hub_ff_mini(ff_df, "10")
  expect_equal(nrow(mini), 4L)
  expect_true(all(mini$rank == 1L))
  mini30 <- hub_ff_mini(ff_df, "30")
  expect_true(all(mini30$rank == 3L))
})

test_that("hub_key_players filters team + min poss, sorts by diff, caps at top_n", {
  onoff_df <- data.frame(
    team_id = c(10L, 10L, 10L, 20L),
    `First Name` = c("A", "B", "C", "D"),
    `Last Name` = c("One", "Two", "Three", "Four"),
    `ON Poss` = c(500, 150, 50, 400),
    `Net RTG Diff` = c(5, 12, 99, 3),
    check.names = FALSE
  )
  out <- hub_key_players(onoff_df, "10", min_on_poss = 100, top_n = 5)
  expect_equal(out[["Last Name"]], c("Two", "One"))
  out1 <- hub_key_players(onoff_df, "10", min_on_poss = 100, top_n = 1)
  expect_equal(nrow(out1), 1L)
  expect_null(hub_key_players(onoff_df, "30"))
})

test_that("hub_top_scorer picks highest ppg with min games", {
  ts_df <- data.frame(
    team_id = c(10L, 10L, 10L),
    player_id = 1:3,
    player_name = c("Low GP", "Scorer", "Role"),
    pts = c(60, 200, 100),
    gp = c(2L, 10L, 10L)
  )
  out <- hub_top_scorer(ts_df, "10", min_gp = 3)
  expect_equal(out$player_name, "Scorer")
  expect_equal(out$ppg, 20)
  expect_null(hub_top_scorer(ts_df, "20"))
})

test_that("hub_best_worst_lineups computes total_poss and extremes", {
  lu <- data.frame(
    player_names_str = c("L1", "L2", "L3"),
    net_rtg = c(12.5, -8.0, 3.0),
    off_poss = c(100L, 120L, 90L),
    def_poss = c(110L, 100L, 95L)
  )
  bw <- hub_best_worst_lineups(lu)
  expect_equal(bw$best$player_names_str, "L1")
  expect_equal(bw$worst$player_names_str, "L2")
  expect_equal(bw$best$total_poss, 210)
  expect_null(hub_best_worst_lineups(lu[0, ]))
  expect_null(hub_best_worst_lineups(NULL))
})

test_that("hub_ordinal formats English ordinals", {
  expect_equal(hub_ordinal(1), "1st")
  expect_equal(hub_ordinal(2), "2nd")
  expect_equal(hub_ordinal(3), "3rd")
  expect_equal(hub_ordinal(4), "4th")
  expect_equal(hub_ordinal(11), "11th")
  expect_equal(hub_ordinal(12), "12th")
  expect_equal(hub_ordinal(13), "13th")
  expect_equal(hub_ordinal(22), "22nd")
})

test_that("hub_league_net_rtg uses possession-weighted league average", {
  expected <- stats::weighted.mean(
    ratings_df$net_rtg,
    ratings_df$off_poss + ratings_df$def_poss
  )
  expect_equal(hub_league_net_rtg(ratings_df), expected)
  expect_true(is.na(hub_league_net_rtg(NULL)))
  expect_true(is.na(hub_league_net_rtg(ratings_df[0, ])))
})

test_that("hub_storyline_specs returns all hub storylines", {
  specs <- hub_storyline_specs()
  expect_equal(
    vapply(specs, `[[`, "", "id"),
    c("starters_bench", "clutch", "last10", "top_bottom_4")
  )
  expect_equal(specs[[1]]$preset, "starters_bench")
  expect_equal(specs[[2]]$preset, "clutch")
  expect_equal(specs[[3]]$preset, "")
  expect_equal(specs[[4]]$preset, "top_bottom_rank")
})

test_that("hub_storyline_lines qualifies on both sides' possessions and skips failures", {
  row_ok <- data.frame(net_rtg = 8, off_poss = 100L, def_poss = 100L)
  row_thin <- data.frame(net_rtg = 8, off_poss = 20L, def_poss = 20L)
  specs <- list(
    list(
      id = "a",
      preset = "p",
      min_poss = 100,
      sentence = function(a, b) sprintf("diff %+.1f", a$net_rtg - b$net_rtg)
    ),
    list(
      id = "b",
      preset = "",
      min_poss = 100,
      sentence = function(a, b) stop("boom")
    )
  )
  fetch_pair <- function(id) {
    if (id == "a") {
      list(a = row_ok, b = row_ok)
    } else {
      list(a = row_ok, b = row_ok)
    }
  }
  out <- hub_storyline_lines(specs, fetch_pair)
  expect_equal(length(out), 1L)
  expect_equal(out[[1]]$id, "a")
  expect_equal(out[[1]]$text, "diff +0.0")

  fetch_thin <- function(id) list(a = row_ok, b = row_thin)
  expect_equal(length(hub_storyline_lines(specs[1], fetch_thin)), 0L)
  fetch_null <- function(id) NULL
  expect_equal(length(hub_storyline_lines(specs[1], fetch_null)), 0L)
})

test_that("v1 storyline sentences read correctly in both directions", {
  specs <- hub_storyline_specs()
  a <- data.frame(
    net_rtg = 6.0,
    off_poss = 200L,
    def_poss = 200L,
    league_net_rtg = 2.5
  )
  b <- data.frame(
    net_rtg = 1.9,
    off_poss = 200L,
    def_poss = 200L,
    league_net_rtg = 1.0
  )
  sb <- specs[[1]]$sentence(a, b)
  expect_match(sb, "Starter-heavy")
  expect_match(sb, "4.1", fixed = TRUE)
  expect_match(sb, "league")
  sb_rev <- specs[[1]]$sentence(b, a)
  expect_match(sb_rev, "Bench-heavy")
  cl <- specs[[2]]$sentence(a, b)
  expect_match(cl, "Clutch")
  expect_match(cl, "league")
  l10 <- specs[[3]]$sentence(a, b)
  expect_match(l10, "Last 10")
  expect_match(l10, "league")
  top_bottom <- specs[[4]]$sentence(a, b)
  expect_match(top_bottom, "Top 4")
  expect_match(top_bottom, "Bottom 4")
  expect_match(top_bottom, "league")
  expect_match(top_bottom, "+6.0", fixed = TRUE)
  expect_match(top_bottom, "+1.9", fixed = TRUE)
})
