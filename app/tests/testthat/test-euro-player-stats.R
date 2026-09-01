test_that("Player Stats is shared by the Israeli and EuroLeague sections", {
  app_js <- read_repo_txt("www", "app.js")
  ui <- read_repo_txt("R", "ui_tab5_traditional.R")
  server <- read_repo_txt("R", "server_tab5_traditional.R")

  expect_false(grepl('traditional_stats: "il"', app_js, fixed = TRUE))
  expect_match(ui, 'uiOutput("ts_game_context_filters")', fixed = TRUE)
  expect_match(server, "ts_is_euro <- reactive", fixed = TRUE)
  expect_match(server, "game_context_descriptor(", fixed = TRUE)
  # 019d8c7 centralized the EuroLeague reference lookups: tabs no longer call
  # euro_fetch_*() directly, they read the shared reactives that wrap them
  # (test-euro-shared-initialization.R pins that). Assert Tab 5 sources
  # EuroLeague players through that reactive, not by naming the fetcher.
  expect_match(server, "shared$euro$players_df()", fixed = TRUE)
})

test_that("EuroLeague Player Stats uses the indexed and dynamic read paths", {
  server <- read_repo_txt("R", "server_tab5_traditional.R")

  expect_match(server, "euroleague.player_traditional_stats_mv", fixed = TRUE)
  # The live path composes its reader name from the kind clutch_reader_kind()
  # picks, so no euroleague.get_player_traditional_* literal appears any more.
  # The three reader names and the kind -> reader map are pinned in
  # test-euro-clutch.R; this test only asserts the read paths exist.
  expect_match(server, 'paste0("SELECT * FROM euroleague.", reader, "("', fixed = TRUE)
  expect_match(server, "clutch_reader_kind(list(", fixed = TRUE)
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
