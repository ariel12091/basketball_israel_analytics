# svc_game_logs(): the one Game Logs service for both leagues. Each preset in
# tests/goldens/presets/game_logs.R is turned into the service's `p` by hand
# (Task 8's svc_game_logs_params() will build the same shape from API input)
# and run against the recorded database answers; the rows must equal what
# Shiny rendered for that preset -- the golden -- on every column the golden
# has except the Gameflow link, which Shiny adds after the service.

source(repo_file("tests", "goldens", "harness.R"), local = TRUE)
source(repo_file("tests", "goldens", "presets", "game_logs.R"), local = TRUE)

svc_gl_fixture_dir <- repo_file("tests", "goldens", "fixtures", "game_logs")
svc_gl_golden <- function(name) {
  readRDS(repo_file("tests", "goldens", "out", "game_logs", paste0(name, ".rds")))
}

# The preset's Shiny inputs, all steps merged, as one named list.
preset_inputs <- function(preset) {
  Reduce(function(acc, step) utils::modifyList(acc, step), golden_input_steps(preset$inputs), list())
}

# The service's `p` for a preset, read off the same inputs Shiny reads. `env`
# is the app environment, so the resolvers are the app's own.
preset_to_p <- function(preset, env) {
  inp <- preset_inputs(preset)
  view <- if (identical(inp[[if (preset$league == "il") "gl_view_mode" else "eurogl_view_mode"]],
                        "Four Factors")) "four_factors" else "summary"
  if (identical(preset$league, "il")) {
    filterable <- if (view == "four_factors") env$GL_FF_FILTERABLE_COLS else env$GL_SUMMARY_FILTERABLE_COLS
    stat_filters <- if (!is.null(inp$gl_add_stat_filter)) {
      list(list(col = unname(filterable[[inp$gl_stat_filter_col]]),
                op = inp$gl_stat_filter_op, value = inp$gl_stat_filter_value))
    } else list()
    list(
      league = "il", competition = NULL, season = as.integer(inp$game_year), view = view,
      team = inp$gl_team %||% "", teams = NULL, dates = inp$gl_dates,
      game_type = inp$gl_game_type, phase = NULL, opponents = inp$gl_opponents,
      home_away = inp$gl_home_away %||% "", outcome = inp$gl_outcome %||% "",
      starters = env$gl_starters_bounds(inp$gl_num_starters_off_mode, inp$gl_num_starters_off,
                                        inp$gl_num_starters_def_mode, inp$gl_num_starters_def),
      gn = env$resolve_gn_last_n_values(inp$gl_gn_min, inp$gl_gn_max, inp$gl_last_n),
      stat_filters = stat_filters, data_version = "golden"
    )
  } else {
    sb <- env$resolve_starters_bounds(inp$eurogl_num_starters_off_mode, inp$eurogl_num_starters_off,
                                      inp$eurogl_num_starters_def_mode, inp$eurogl_num_starters_def)
    teams <- suppressWarnings(as.numeric(inp$eurogl_teams))
    list(
      league = "euroleague", competition = inp$league_select, season = inp$euro_game_year,
      view = view, team = NULL, teams = teams[is.finite(teams)], dates = inp$eurogl_dates,
      game_type = NULL, phase = inp$eurogl_phase, opponents = inp$eurogl_opponents,
      home_away = inp$eurogl_home_away %||% "", outcome = inp$eurogl_outcome %||% "",
      starters = list(off_min = sb$num_starters_off_min, off_max = sb$num_starters_off_max,
                      def_min = sb$num_starters_def_min, def_max = sb$num_starters_def_max),
      gn = env$resolve_gn_last_n_values(inp$eurogl_gn_min, inp$eurogl_gn_max, inp$eurogl_last_n),
      stat_filters = list(), data_version = "golden"
    )
  }
}

# Golden columns minus the link, as a plain data frame with fresh row names.
svc_gl_comparable <- function(df, cols) {
  out <- as.data.frame(df)[, cols, drop = FALSE]
  rownames(out) <- NULL
  out
}

svc_gl_env <- golden_app_env(golden_replay_conn(svc_gl_fixture_dir))

test_that("svc_game_logs reproduces every Game Logs golden", {
  # Ruling R5: the union of the presets' ids, written as literals.
  covers("GL-01", "GL-02", "GL-03", "GL-04", "GL-05", "GL-06", "GL-07", "X-09")
  literal <- c("GL-01", "GL-02", "GL-03", "GL-04", "GL-05", "GL-06", "GL-07", "X-09")
  expect_setequal(literal, unique(unlist(lapply(GAME_LOGS_PRESETS, `[[`, "covers"))))

  for (name in names(GAME_LOGS_PRESETS)) {
    preset <- GAME_LOGS_PRESETS[[name]]
    p <- preset_to_p(preset, svc_gl_env)
    res <- svc_gl_env$svc_game_logs(p)
    golden <- svc_gl_golden(name)

    if (isTRUE(preset$empty)) {
      expect_null(golden, info = name)
      expect_null(res$rows, info = name)
    } else {
      cols <- setdiff(names(golden$data), "gameflow")
      expect_true(all(cols %in% names(res$rows)), info = name)
      expect_identical(svc_gl_comparable(res$rows, cols),
                       svc_gl_comparable(golden$data, cols), info = name)
    }

    round_col <- if (identical(p$league, "il")) "gn" else "round_number"
    expect_identical(res$meta$default_sort, list(
      list(column = "game_date", direction = "desc"),
      list(column = round_col, direction = "desc")
    ), info = name)
    expect_identical(res$meta$path, "fast", info = name)
    expect_identical(res$meta$percentile_population, "season", info = name)
    expect_identical(res$meta$filters, p, info = name)
  }
})

test_that("shot averages are reported for Israeli Summary only", {
  covers("GL-07")
  for (name in names(GAME_LOGS_PRESETS)) {
    preset <- GAME_LOGS_PRESETS[[name]]
    p <- preset_to_p(preset, svc_gl_env)
    res <- svc_gl_env$svc_game_logs(p)
    if (identical(p$league, "il") && p$view == "summary" && !is.null(res$rows)) {
      expect_identical(res$meta$shot_averages, svc_gl_env$gl_shot_averages(res$rows), info = name)
      expect_named(res$meta$shot_averages, c("off", "def"))
    } else {
      expect_null(res$meta$shot_averages, info = name)
    }
  }
})

test_that("the Shiny defaults match the descriptor", {
  covers("GL-01")
  expect_named(svc_gl_env$GL_LEAGUE, c("il", "euroleague"))
  expect_identical(svc_gl_env$GL_LEAGUE$il$round_col, "gn")
  expect_identical(svc_gl_env$GL_LEAGUE$euroleague$round_col, "round_number")
  expect_named(svc_gl_env$gl_default_readers(),
               c("schedule", "lineup_totals", "lineup_ff", "euro_rows", "scoreless"))
})

rm(svc_gl_env)
