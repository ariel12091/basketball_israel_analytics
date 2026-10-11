# A newly loaded EuroLeague season must become the default. The navbar select is
# built with the static EURO_DEFAULT_SEASON, and that placeholder used to count
# as the user's choice, so 25-26 stayed selected after 26-27 was loaded.

run_euro_season_init <- function(seasons, leagues) {
  env <- new.env(parent = globalenv())
  # global_euro.R derives its default season from the app-wide one; "2026"
  # (25-26) makes "2025" the static placeholder these cases are written for.
  env$DEFAULT_GAME_YEAR <- "2026"
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
  # global_euro.R derives its default season from the app-wide one; "2026"
  # (25-26) makes "2025" the static placeholder these cases are written for.
  env$DEFAULT_GAME_YEAR <- "2026"
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

# ---- The static default must already be the newest season -------------------
# Until the loaded seasons arrive from the database, the navbar select, the
# date inputs and every EuroLeague query run on the static default. It was a
# hard-coded "2025" that nobody bumped when 26-27 was loaded, so the page was
# served showing 25-26 and only switched after a round trip (2026-10-11).

euro_env_for <- function(game_year) {
  env <- new.env(parent = globalenv())
  env$DEFAULT_GAME_YEAR <- game_year
  sys.source(repo_file("R", "helpers.R"), envir = env)
  sys.source(repo_file("R", "global_euro.R"), envir = env)
  env
}

test_that("the EuroLeague default follows the app-wide default season", {
  # Israeli game_year is the season-ENDING year, a provider season the
  # STARTING year: 2027 and 2026 are both 26-27.
  env <- euro_env_for("2027")

  expect_identical(env$EURO_DEFAULT_SEASON, "2026")
  expect_identical(env$euro_season_label(env$EURO_DEFAULT_SEASON), "26-27")
  expect_identical(env$EURO_DEFAULT_START, as.Date("2026-09-01"))
  expect_identical(env$EURO_DEFAULT_END, as.Date("2027-07-01"))
  expect_identical(euro_env_for("2028")$EURO_DEFAULT_SEASON, "2027")
})

test_that("the default is derived, never a year typed into global_euro.R", {
  src <- read_repo_txt("R", "global_euro.R")

  expect_false(grepl("EURO_DEFAULT_SEASON[ ]*<-[ ]*\"[0-9]{4}\"", src))
  # And the real constants agree today.
  real <- euro_env_for(global_defs("DEFAULT_GAME_YEAR")$DEFAULT_GAME_YEAR)
  expect_identical(real$euro_season_label(real$EURO_DEFAULT_SEASON),
                   real$season_label(global_defs("DEFAULT_GAME_YEAR")$DEFAULT_GAME_YEAR))
})

test_that("the navbar is served with the newest season selected and the previous one offered", {
  env <- euro_env_for("2027")
  html <- as.character(env$euro_navbar_season_ui())

  expect_match(html, '<option value="2026" selected>26-27</option>', fixed = TRUE)
  expect_match(html, '<option value="2025">25-26</option>', fixed = TRUE)
})

test_that("a season picked before the loaded list arrives is kept", {
  env <- euro_env_for("2027")
  picks <- character(0)
  env$updateSelectInput <- function(session, inputId, choices = NULL, selected = NULL, ...) {
    picks <<- c(picks, selected)
  }
  ctx <- list(seasons_df = function() data.frame(game_year = c(2026L, 2025L)))
  shiny::testServer(function(input, output, session) {
    env$euro_init_season_inputs(input, session, ctx)
  }, {
    # The previous season is offered from the first paint, so the user can
    # choose it before the first population runs.
    session$setInputs(euro_game_year = "2025", league_select = "E")
  })

  expect_equal(picks, "2025")
})
