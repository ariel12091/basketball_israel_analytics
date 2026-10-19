# server_tab11_euro_gamelogs.R - Tab 11: EuroLeague / EuroCup Game Logs

server_tab11_euro_gamelogs <- function(input, output, session, shared) {
  empty_game_log <- function(message) DT::datatable(
    data.frame(Info = message, check.names = FALSE),
    rownames = FALSE,
    options = list(headerCallback = HEADER_TOOLTIP_JS, dom = "t")
  )

  competition <- shared$euro$competition
  season <- shared$euro$season
  teams_df <- shared$euro$teams_df

  setup_euro_section_filters(input, session, "eurogl", tab_id = "euro_game_logs",
                             euro_context = shared$euro, date_id = "eurogl_dates",
                             shared = shared, teams_id = "eurogl_teams")
  setup_gn_last_n_sync(session, input, "eurogl")

  observeEvent(input$eurogl_reset, {
    apply_season_date_bounds(session, "eurogl_dates", euro_season_date_bounds(isolate(season())))
    for (id in c("eurogl_teams", "eurogl_phase", "eurogl_opponents"))
      updateSelectizeInput(session, id, selected = character(0))
    for (id in c("eurogl_home_away", "eurogl_outcome",
                 "eurogl_num_starters_off_mode", "eurogl_num_starters_off",
                 "eurogl_num_starters_def_mode", "eurogl_num_starters_def"))
      updateSelectInput(session, id, selected = "")
    for (id in c("eurogl_gn_min", "eurogl_gn_max", "eurogl_last_n"))
      updateSelectizeInput(session, id, selected = "")
    updateRadioButtons(session, "eurogl_view_mode", selected = "Summary")
  })

  ribbon_modal_server(input, output, session, "eurogl", "euroleague", euro_data_version, svg_id_prefix = "eugl")

  season_rows <- reactive({
    req(identical(input$main_tabs, "euro_game_logs"))
    gl_default_readers()$euro_rows(competition(), season(), euro_data_version())
  })

  selected_team_ids <- reactive({
    ids <- suppressWarnings(as.numeric(input$eurogl_teams))
    ids[is.finite(ids)]
  })

  # Match the Israeli companion: filters decide which games are displayed,
  # but their percentiles stay anchored to the season's whole game population.
  # Team, date, opponent, phase, result, round, last-N, and starter filters
  # therefore cannot rescale the heat cells.
  percentile_games <- reactive(gl_el_baseline(season_rows()))

  filtered_games <- reactive({
    df <- season_rows()
    if (is.null(df) || !nrow(df)) return(df)
    f <- game_context_filter_values(input, "eurogl", game_type_id = "eurogl_phase")
    # The filter and starters fields of svc_game_logs()'s `p`.
    gl_el_games(df, list(
      teams = selected_team_ids(), dates = input$eurogl_dates, phase = f$game_type,
      opponents = f$opp_ids, home_away = f$home_away, outcome = f$outcome,
      gn = resolve_gn_last_n_values(f$gn_min, f$gn_max, f$last_n),
      starters = gl_starters_bounds(
        input$eurogl_num_starters_off_mode, input$eurogl_num_starters_off,
        input$eurogl_num_starters_def_mode, input$eurogl_num_starters_def,
        keep_empty = TRUE
      )
    ), percentile_games)
  })

  output$eurogl_table <- renderDT({
    df <- filtered_games()
    if (is.null(df) || !nrow(df)) return(empty_game_log("No games match the current filters"))
    ff <- identical(input$eurogl_view_mode, "Four Factors")
    # Column sets, heat polarity and number formats mirror Tab 4 exactly; only
    # the two league-varying header labels differ (Rd/Phase for GN/Game Type).
    # Names stay snake_case rather than being renamed to display labels: the
    # shared header container supplies the labels, and gl_apply_heat_styles()
    # keys on the frame's own names. Net sits in the leading context block, not
    # in Offense or Defense: it is their difference, and it is there so the
    # table can be ordered by it.
    if (ff) {
      cols <- c("round_number", "phase_label", "game_date", "gameflow", "team_name", "opp_team_name",
                "result", "score", "minutes", "net_rtg",
                "off_ppp", "off_efg_pct", "off_oreb_pct", "off_tov_pct", "off_ftr_pct",
                "def_ppp", "def_efg_pct", "def_oreb_pct", "def_tov_pct", "def_ftr_pct",
                "off_poss", "def_poss")
      heat_reverse <- GL_HEAT_REVERSE_FF
      round_cols <- GL_ROUND_1$four_factors
      sketch <- gl_header("euroleague", "four_factors", show_gameflow = TRUE)
    } else {
      cols <- c("round_number", "phase_label", "game_date", "gameflow", "team_name", "opp_team_name",
                "result", "score", "minutes",
                "off_ppp", "def_ppp", "net_rtg", "off_poss", "def_poss")
      heat_reverse <- GL_HEAT_REVERSE_SUMMARY
      round_cols <- GL_ROUND_1$summary
      sketch <- gl_header("euroleague", "summary", has_shots = FALSE, show_gameflow = TRUE)
    }

    pr_cols <- intersect(
      unname(vapply(names(heat_reverse), gl_pr_col_name, character(1))), names(df))
    # EuroLeague has nothing to gate today: euroleague.ribbon_margin_v is
    # asserted NULL-free by a live-DB regression test (see
    # test-stint-ribbon-readers.R, "has no NULL margin"), so pass an empty
    # scoreless set rather than inventing a second query against this
    # league's schema for a condition it does not have.
    df <- attach_has_scores(df, NULL)
    df <- add_ribbon_link_column(
      df, input_id = "eurogl_ribbon_click",
      output_col = "gameflow", link_label = "View"
    )
    disp <- df[, c(cols, pr_cols), drop = FALSE]

    result_idx <- which(names(disp) == "result") - 1L
    hidden_idx <- which(names(disp) %in% pr_cols) - 1L
    date_idx <- which(names(disp) == "game_date") - 1L
    off_ppp_idx <- which(names(disp) == "off_ppp") - 1L
    def_ppp_idx <- which(names(disp) == "def_ppp") - 1L
    off_poss_idx <- which(names(disp) == "off_poss") - 1L

    col_defs <- list(
      list(className = "dt-center", targets = "_all"),
      list(targets = result_idx, render = gl_result_cell_renderer()),
      list(targets = hidden_idx, visible = FALSE),
      list(targets = date_idx, type = "date")
    )
    # Section separators under the Offense / Defense / Usage group headings.
    if (length(off_ppp_idx)) col_defs[[length(col_defs) + 1]] <-
      list(targets = off_ppp_idx, className = "section-left-border dt-center")
    if (ff && length(def_ppp_idx)) col_defs[[length(col_defs) + 1]] <-
      list(targets = def_ppp_idx, className = "section-left-border dt-center")
    if (length(off_poss_idx)) col_defs[[length(col_defs) + 1]] <-
      list(targets = off_poss_idx, className = "section-left-border dt-center")

    dt <- DT::datatable(disp, container = sketch, rownames = FALSE,
      extensions = "Buttons",
      escape = dt_escape_except(disp, "gameflow"),
      options = list(headerCallback = HEADER_TOOLTIP_JS, dom = "Btip",
        buttons = csv_export_button(if (ff) "euroleague_game_logs_four_factors"
                                    else "euroleague_game_logs_summary"),
        pageLength = 50, scrollX = TRUE, scrollY = "70vh", scrollCollapse = TRUE,
        order = list(list(date_idx, "desc"), list(0, "desc")),
        columnDefs = col_defs)) %>%
      DT::formatRound(intersect(round_cols, names(disp)), 1)

    if ("minutes" %in% names(disp)) dt <- DT::formatRound(dt, "minutes", 1)
    if (all(c("off_poss", "def_poss") %in% names(disp))) {
      dt <- DT::formatCurrency(dt, c("off_poss", "def_poss"), currency = "",
                               interval = 3, mark = ",", digits = 0)
    }

    gl_apply_heat_styles(dt, disp,
                         stats::setNames(names(heat_reverse), names(heat_reverse)),
                         heat_reverse)
  }, server = FALSE)

  output$eurogl_filter_chips <- renderUI({
    td <- teams_df()
    team_map <- if (!is.null(td) && nrow(td)) stats::setNames(td$team_name, td$team_id) else NULL
    build_filter_chips("eurogl", input, euro_season_date_bounds,
      reset_btn_id = "eurogl_reset", team_label_map = team_map, opponent_label_map = team_map,
      season_value = season(),
      season_label = paste(EURO_COMPETITION_LABELS[[competition()]] %||% competition(), euro_season_label(season())),
      date_input_id = "eurogl_dates", game_type_input_id = "eurogl_phase",
      game_type_labeller = euro_phase_label, gn_label = "Rd")
  })

  setup_chip_clears("eurogl", session, input, shared,
    game_type_id = "eurogl_phase", opponents_id = "eurogl_opponents",
    home_away_id = "eurogl_home_away", outcome_id = "eurogl_outcome",
    gn_min_id = "eurogl_gn_min", gn_max_id = "eurogl_gn_max", last_n_id = "eurogl_last_n",
    opp_rank_ids = character(0),
    date_id = "eurogl_dates", gy_input_id = "euro_game_year",
    teams_ids = "eurogl_teams", teams_multiple = TRUE,
    starters_ids = c("eurogl_num_starters_off_mode", "eurogl_num_starters_off",
                     "eurogl_num_starters_def_mode", "eurogl_num_starters_def"),
    bounds_fn = euro_season_date_bounds)

  invisible(list(
    filtered_games = filtered_games,
    percentile_games = percentile_games
  ))
}
