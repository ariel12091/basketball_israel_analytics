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

# The dropdown labels the whole season vector at once; a scalar-only label
# function left every option showing the raw provider year ("2026", "2025").
test_that("season choices carry the Israeli-style label", {
  env <- new.env(parent = globalenv())
  sys.source(repo_file("R", "helpers.R"), envir = env)
  sys.source(repo_file("R", "global_euro.R"), envir = env)
  expect_equal(env$euro_season_label(c("2026", "2025")), c("26-27", "25-26"))
  expect_equal(env$euro_season_label(2025L), "25-26")
  expect_equal(env$euro_season_label("x"), "x")
})

test_that("Israeli and EuroLeague seasons share one label format", {
  env <- new.env(parent = globalenv())
  sys.source(repo_file("R", "helpers.R"), envir = env)
  # Israeli game_year is the ending year, EuroLeague the starting year.
  expect_equal(env$season_label(c("2027", "2026", "2025")), c("26-27", "25-26", "24-25"))
  expect_equal(env$season_label(2025L, ending_year = FALSE), "25-26")
  expect_equal(env$season_label(2000L), "99-00")
})
