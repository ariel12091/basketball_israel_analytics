# A newly loaded EuroLeague season must become the default. The navbar select is
# built with the static EURO_DEFAULT_SEASON, and that placeholder used to count
# as the user's choice, so 25-26 stayed selected after 26-27 was loaded.

run_euro_season_init <- function(seasons, leagues) {
  env <- new.env(parent = globalenv())
  sys.source(repo_file("R", "helpers.R"), envir = env)
  sys.source(repo_file("R", "global_euro.R"), envir = env)
  picks <- character(0)
  env$updateSelectInput <- function(session, inputId, choices = NULL, selected = NULL, ...) {
    picks <<- c(picks, selected)
  }
  ctx <- list(seasons_df = function() data.frame(game_year = seasons))
  shiny::testServer(function(input, output, session) {
    env$euro_init_season_inputs(input, session, ctx)
  }, {
    session$setInputs(euro_game_year = env$EURO_DEFAULT_SEASON,
                      league_select = leagues[[1]])
    for (lg in leagues[-1]) {
      # the user picks the older season between league switches
      session$setInputs(euro_game_year = "2025")
      session$setInputs(league_select = lg)
    }
  })
  picks
}

test_that("first EuroLeague visit selects the newest loaded season", {
  expect_equal(run_euro_season_init(c(2026L, 2025L), "E"), "2026")
})

test_that("a season the user picked survives a competition switch", {
  expect_equal(run_euro_season_init(c(2026L, 2025L), c("E", "U")), c("2026", "2025"))
})

test_that("only one loaded season still works", {
  expect_equal(run_euro_season_init(2025L, "E"), "2025")
})
