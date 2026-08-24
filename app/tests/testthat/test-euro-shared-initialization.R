test_that("EuroLeague reference data has one shared lazy owner", {
  app_txt <- read_repo_txt("app.R")
  global_txt <- read_repo_txt("R", "global_euro.R")

  expect_match(global_txt, "make_euro_shared_context <- function(input)", fixed = TRUE)
  for (name in c("seasons_df", "teams_df", "players_df", "phase_choices",
                 "round_values", "date_bounds")) {
    expect_match(global_txt, paste0(name, " = reactive({"), fixed = TRUE)
  }
  expect_match(app_txt, "euro_context <- make_euro_shared_context(input)", fixed = TRUE)
  expect_match(app_txt, "euro = euro_context", fixed = TRUE)
})

test_that("hidden EuroLeague tabs do not initialize their filters", {
  global_txt <- read_repo_txt("R", "global_euro.R")
  lineup_txt <- read_repo_txt("R", "server_tab10_euro_lineups.R")

  expect_match(
    global_txt,
    "if (!identical(input$main_tabs, tab_id)) return(invisible(NULL))",
    fixed = TRUE
  )
  expect_match(
    lineup_txt,
    'if (!identical(input$main_tabs, "euro_lineups")) return(invisible(NULL))',
    fixed = TRUE
  )
})

test_that("EuroLeague tabs consume shared reference reactives", {
  files <- c(
    "server_tab5_traditional.R", "server_tab8_euro.R",
    "server_tab9_euro_team.R", "server_tab10_euro_lineups.R",
    "server_tab11_euro_gamelogs.R"
  )
  texts <- lapply(files, function(file) read_repo_txt("R", file))

  for (txt in texts) expect_match(txt, "shared$euro$", fixed = TRUE)
  for (txt in texts) {
    expect_false(grepl("euro_fetch_teams(", txt, fixed = TRUE))
    expect_false(grepl("euro_fetch_players_basic(", txt, fixed = TRUE))
    expect_false(grepl("euro_fetch_round_values(", txt, fixed = TRUE))
    expect_false(grepl("euro_phase_choices(", txt, fixed = TRUE))
  }
})
