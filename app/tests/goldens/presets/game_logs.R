# Game Logs golden presets: Israeli Tab 4 (game_year 2026 = 2025-26) and
# EuroLeague Tab 11 (competition E, provider season 2025 = 2025-26).
#
# Each preset is list(league, covers, inputs, slug[, empty]). `inputs` are
# Shiny input ids; a named list is applied in one setInputs() call, an unnamed
# list of named lists in successive steps (see golden_input_steps()). `slug`
# is the CSV-button slug that identifies the table to capture. `empty = TRUE`
# marks the one preset whose renderer returns NULL, captured as a NULL golden.
#
# Every preset sets the tab, the league and the season explicitly, and the
# date range to the season's own bounds: in a browser the app moves the date
# input to those bounds as soon as the season is chosen, but under testServer
# updateDateRangeInput() is not applied, so it is written in here. Nothing a
# preset queries depends on today's date.
#
# Team ids are literals resolved once from the recorded reference data:
# Israeli fetch_teams_min(2026), first two by team_id = 2 (MACCABI TEL AVIV)
# and 3 (HAPOEL TEL AVIV); EuroLeague euro_fetch_teams("E", 2025), first two
# by team_id = 1 (Anadolu Efes Istanbul) and 2 (Maccabi Rapyd Tel Aviv).
# Israeli game type 5 is the regular season ("League"). Team 2's highest 2026
# GN is 26, so GN 30-31 matches nothing.

gl_il_base <- function(view, ...) {
  c(list(
    main_tabs = "game_logs", league_select = "il", game_year = "2026",
    gl_view_mode = view,
    gl_dates = as.Date(c("2025-09-01", "2026-07-01"))
  ), list(...))
}

gl_el_base <- function(view, ...) {
  c(list(
    main_tabs = "euro_game_logs", league_select = "E", euro_game_year = "2025",
    eurogl_view_mode = view,
    eurogl_dates = as.Date(c("2025-09-01", "2026-07-01"))
  ), list(...))
}

GL_IL_SUMMARY_SLUG <- "game_logs_summary"
GL_IL_FF_SLUG <- "game_logs_four_factors"
GL_EL_SUMMARY_SLUG <- "euroleague_game_logs_summary"
GL_EL_FF_SLUG <- "euroleague_game_logs_four_factors"

GAME_LOGS_PRESETS <- list(
  il_summary_default = list(
    league = "il", covers = c("GL-01", "GL-03", "GL-04", "GL-05", "GL-06", "GL-07"),
    inputs = gl_il_base("Summary"), slug = GL_IL_SUMMARY_SLUG
  ),
  il_ff_default = list(
    league = "il", covers = c("GL-01", "GL-04", "GL-05"),
    inputs = gl_il_base("Four Factors"), slug = GL_IL_FF_SLUG
  ),
  il_summary_team = list(
    league = "il", covers = c("GL-01", "GL-02"),
    inputs = gl_il_base("Summary", gl_team = "2"), slug = GL_IL_SUMMARY_SLUG
  ),
  il_summary_last5_home = list(
    league = "il", covers = "GL-02",
    inputs = gl_il_base("Summary", gl_last_n = "5", gl_home_away = "home"),
    slug = GL_IL_SUMMARY_SLUG
  ),
  il_ff_gn_win = list(
    league = "il", covers = "GL-02",
    inputs = gl_il_base("Four Factors", gl_gn_min = "5", gl_gn_max = "15", gl_outcome = "win"),
    slug = GL_IL_FF_SLUG
  ),
  il_summary_dates_type = list(
    league = "il", covers = "GL-02",
    inputs = utils::modifyList(gl_il_base("Summary", gl_game_type = "5"),
                               list(gl_dates = as.Date(c("2025-12-01", "2026-01-31")))),
    slug = GL_IL_SUMMARY_SLUG
  ),
  il_ff_starters = list(
    league = "il", covers = "GL-02",
    inputs = gl_il_base("Four Factors",
                        gl_num_starters_off_mode = "gte", gl_num_starters_off = "4",
                        gl_num_starters_def_mode = "lte", gl_num_starters_def = "2"),
    slug = GL_IL_FF_SLUG
  ),
  # Two steps: the view-mode observer clears stat filters when the view is
  # first set, so the filter is added only after the view has settled.
  il_summary_opps_statfilter = list(
    league = "il", covers = c("GL-02", "X-09"),
    inputs = list(
      gl_il_base("Summary", gl_opponents = c("2", "3")),
      list(gl_stat_filter_col = "Net", gl_stat_filter_op = "ge", gl_stat_filter_value = 0),
      list(gl_add_stat_filter = 1)
    ),
    slug = GL_IL_SUMMARY_SLUG
  ),
  il_ff_away_loss_last10 = list(
    league = "il", covers = "GL-02",
    inputs = gl_il_base("Four Factors", gl_home_away = "away", gl_outcome = "loss",
                        gl_last_n = "10"),
    slug = GL_IL_FF_SLUG
  ),
  il_summary_empty = list(
    league = "il", covers = "GL-02",
    inputs = gl_il_base("Summary", gl_team = "2", gl_gn_min = "30", gl_gn_max = "31"),
    slug = GL_IL_SUMMARY_SLUG, empty = TRUE
  ),
  el_summary_default = list(
    league = "euroleague", covers = c("GL-01", "GL-03", "GL-04", "GL-05", "GL-07"),
    inputs = gl_el_base("Summary"), slug = GL_EL_SUMMARY_SLUG
  ),
  el_ff_default = list(
    league = "euroleague", covers = c("GL-01", "GL-04"),
    inputs = gl_el_base("Four Factors"), slug = GL_EL_FF_SLUG
  ),
  el_ff_teams_phase = list(
    league = "euroleague", covers = "GL-02",
    inputs = gl_el_base("Four Factors", eurogl_teams = c("1", "2"), eurogl_phase = "RS"),
    slug = GL_EL_FF_SLUG
  ),
  el_summary_round_starters = list(
    league = "euroleague", covers = "GL-02",
    inputs = gl_el_base("Summary", eurogl_gn_min = "10", eurogl_gn_max = "20",
                        eurogl_num_starters_off_mode = "gte", eurogl_num_starters_off = "4"),
    slug = GL_EL_SUMMARY_SLUG
  ),
  el_summary_last5_home = list(
    league = "euroleague", covers = "GL-02",
    inputs = gl_el_base("Summary", eurogl_last_n = "5", eurogl_home_away = "home"),
    slug = GL_EL_SUMMARY_SLUG
  )
)

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
    teams <- suppressWarnings(as.numeric(inp$eurogl_teams))
    list(
      league = "euroleague", competition = inp$league_select, season = inp$euro_game_year,
      view = view, team = NULL, teams = teams[is.finite(teams)], dates = inp$eurogl_dates,
      game_type = NULL, phase = inp$eurogl_phase, opponents = inp$eurogl_opponents,
      home_away = inp$eurogl_home_away %||% "", outcome = inp$eurogl_outcome %||% "",
      starters = env$gl_starters_bounds(inp$eurogl_num_starters_off_mode, inp$eurogl_num_starters_off,
                                        inp$eurogl_num_starters_def_mode, inp$eurogl_num_starters_def,
                                        keep_empty = TRUE),
      gn = env$resolve_gn_last_n_values(inp$eurogl_gn_min, inp$eurogl_gn_max, inp$eurogl_last_n),
      stat_filters = list(), data_version = "golden"
    )
  }
}
