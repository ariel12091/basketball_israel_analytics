test_that("EuroLeague clutch controls reuse the Israeli UI and parameter helpers", {
  team_ui <- read_repo_txt("R", "ui_tab9_euro_team.R")
  lineup_ui <- read_repo_txt("R", "ui_tab10_euro_lineups.R")
  team_server <- read_repo_txt("R", "server_tab9_euro_team.R")
  lineup_server <- read_repo_txt("R", "server_tab10_euro_lineups.R")

  expect_match(team_ui, 'clutch_filter_ui("euroteam")', fixed = TRUE)
  expect_match(lineup_ui, 'clutch_filter_ui("euro_ld")', fixed = TRUE)
  expect_match(team_server, "resolve_clutch_params(", fixed = TRUE)
  expect_match(lineup_server, "resolve_clutch_params(", fixed = TRUE)
  expect_match(team_server, 'clutch_enabled_id = "euroteam_clutch_enabled"', fixed = TRUE)
  expect_match(lineup_server, 'clutch_enabled_id = "euro_ld_clutch_enabled"', fixed = TRUE)
})

test_that("EuroLeague clutch parameters reach ratings, factors, minutes, and lineups", {
  team_server <- read_repo_txt("R", "server_tab9_euro_team.R")
  lineup_server <- read_repo_txt("R", "server_tab10_euro_lineups.R")

  for (parameter in c(
    "max_margin", "margin_status", "max_time_remaining", "ot_margin_filter"
  )) {
    expect_match(team_server, paste0("p$", parameter), fixed = TRUE)
    expect_match(lineup_server, paste0("a$", parameter), fixed = TRUE)
  }
  expect_match(team_server, "get_team_ratings_dynamic", fixed = TRUE)
  expect_match(team_server, "get_team_ratings_direct", fixed = TRUE)
  expect_match(team_server, "get_team_four_factors_dynamic", fixed = TRUE)
  expect_match(team_server, "get_team_four_factors_direct", fixed = TRUE)
  expect_match(team_server, "get_team_minutes_dynamic", fixed = TRUE)
  expect_match(team_server, "get_team_minutes_direct", fixed = TRUE)
  expect_match(lineup_server, "fetch_lineups_dynamic", fixed = TRUE)
  expect_match(lineup_server, "fetch_lineups_direct", fixed = TRUE)
  expect_match(lineup_server, "isTRUE(input$euro_ld_clutch_enabled)", fixed = TRUE)
})
