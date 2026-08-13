test_that("Player Stats is shared by the Israeli and EuroLeague sections", {
  app_js <- read_repo_txt("www", "app.js")
  ui <- read_repo_txt("R", "ui_tab5_traditional.R")
  server <- read_repo_txt("R", "server_tab5_traditional.R")

  expect_false(grepl('traditional_stats: "il"', app_js, fixed = TRUE))
  expect_match(ui, 'uiOutput("ts_game_context_filters")', fixed = TRUE)
  expect_match(server, "ts_is_euro <- reactive", fixed = TRUE)
  expect_match(server, "game_context_descriptor(", fixed = TRUE)
  expect_match(server, "euro_fetch_players_basic", fixed = TRUE)
})

test_that("EuroLeague Player Stats uses the indexed and dynamic read paths", {
  server <- read_repo_txt("R", "server_tab5_traditional.R")

  expect_match(server, "euroleague.player_traditional_stats_mv", fixed = TRUE)
  expect_match(server, "euroleague.get_player_traditional_dynamic", fixed = TRUE)
  expect_match(server, "euroleague.get_player_traditional_standard_clutch", fixed = TRUE)
  expect_match(server, "euroleague.get_player_traditional_custom_clutch", fixed = TRUE)
  expect_match(server, "standard_clutch <-", fixed = TRUE)
  expect_match(server, "clutch_active <-", fixed = TRUE)
  expect_match(server, "resolve_clutch_params(", fixed = TRUE)
  expect_match(server, "integerize = !ts_is_euro()", fixed = TRUE)
  expect_match(server, 'phase_csv = db_args$game_type_csv', fixed = TRUE)
  expect_match(server, 'if (ts_is_euro()) disp$DFL <- NULL', fixed = TRUE)
})

test_that("shared chip clearing accepts a reactive season adapter", {
  global <- read_repo_txt("R", "global.R")

  expect_match(global, "season_value_fn = NULL", fixed = TRUE)
  expect_match(global, "if (is.function(season_value_fn)) season_value_fn()", fixed = TRUE)
  expect_match(global, "if (is.function(game_type_id)) game_type_id()", fixed = TRUE)
})
