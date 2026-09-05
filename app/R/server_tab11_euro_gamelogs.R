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

  season_rows <- reactive({
    req(identical(input$main_tabs, "euro_game_logs"))
    cached_season_df(
      list("euro_game_logs", competition(), season(), euro_data_version()),
      function() db_get_query(pg_pool,
        "SELECT f.*, s.round_number, s.phase, s.game_date, s.opp_team_id,
                s.is_home, s.has_won, s.team_points, s.opp_points,
                s.team_name, s.opp_team_name,
                m.seconds / 60.0 AS minutes
           FROM euroleague.team_four_factors_by_game f
           JOIN euroleague.final_schedule_mv s
             ON s.game_id = f.game_id AND s.team_id = f.team_id
           LEFT JOIN (
             SELECT game_id, team_id, own_starters, opp_starters,
                    SUM(seconds) AS seconds
               FROM euroleague.lineup_totals_by_game
              WHERE type_lineup = 'offense'
                AND competition = $1::text AND game_year = $2::int4
              GROUP BY game_id, team_id, own_starters, opp_starters
           ) m
             ON m.game_id = f.game_id AND m.team_id = f.team_id
            AND m.own_starters = f.own_starters
            AND m.opp_starters = f.opp_starters
          WHERE s.competition = $1::text AND s.game_year = $2::int4",
        params = list(competition(), as.integer(season())))
    )
  })

  percentile_metrics <- c(
    "off_ppp", "def_ppp", "net_rtg",
    "off_efg_pct", "off_oreb_pct", "off_tov_pct", "off_ftr_pct",
    "def_efg_pct", "def_oreb_pct", "def_tov_pct", "def_ftr_pct"
  )

  selected_team_ids <- reactive({
    ids <- suppressWarnings(as.numeric(input$eurogl_teams))
    ids[is.finite(ids)]
  })

  build_games <- function(df, starters_bounds = NULL, apply_starters = TRUE) {
    if (is.null(df) || !nrow(df)) return(NULL)

    schedule_rows <- df %>%
      distinct(game_id, team_id, round_number, phase, game_date, opp_team_id,
               is_home, has_won, team_points, opp_points, team_name, opp_team_name)

    # Schema adapter only: Tab 4's shared calculator consumes one offense or
    # defense row at a time, while the EuroLeague fact stores both perspectives
    # on one row. All aggregation and basketball formulas remain in
    # gl_build_ff_metrics().
    metric_rows <- bind_rows(
      df %>% transmute(
        game_id, team_id, type_lineup = "offense", num_starters = own_starters,
        total_points = off_pts, total_poss = off_poss,
        ts_poss_count = off_ts_poss, oreb_count = off_oreb,
        oreb_opportunities = off_oreb_opp, tov_count = off_tov,
        total_ft_attempts = off_fta, total_fga = off_fga,
        total_fgm = off_fgm, total_fg3_made = off_fg3m, minutes
      ),
      df %>% transmute(
        game_id, team_id, type_lineup = "defense", num_starters = opp_starters,
        total_points = def_pts, total_poss = def_poss,
        ts_poss_count = def_ts_poss, oreb_count = def_oreb,
        oreb_opportunities = def_oreb_opp, tov_count = def_tov,
        total_ft_attempts = def_fta, total_fga = def_fga,
        total_fgm = def_fgm, total_fg3_made = def_fg3m, minutes = NA_real_
      )
    )

    games <- gl_build_ff_metrics(
      metric_rows, schedule_rows,
      starters_bounds = starters_bounds,
      apply_starters = apply_starters
    )
    if (is.null(games) || !nrow(games)) return(NULL)

    games %>%
      inner_join(schedule_rows, by = c("game_id", "team_id")) %>%
      mutate(result = ifelse(has_won, "W", "L"),
             score = paste0(team_points, "-", opp_points),
             phase_label = euro_phase_label(phase)) %>%
      arrange(desc(game_date), desc(round_number), team_name)
  }

  # Match the Israeli companion: filters decide which games are displayed,
  # but their percentiles stay anchored to the season's whole game population.
  # Team, date, opponent, phase, result, round, last-N, and starter filters
  # therefore cannot rescale the heat cells.
  percentile_games <- reactive({
    df <- season_rows()
    if (is.null(df) || !nrow(df)) return(df)
    build_games(df, apply_starters = FALSE)
  })

  filtered_games <- reactive({
    df <- season_rows()
    if (is.null(df) || !nrow(df)) return(df)
    f <- game_context_filter_values(input, "eurogl", game_type_id = "eurogl_phase")
    rng <- input$eurogl_dates
    if (length(rng) == 2L) df <- df %>% filter(game_date >= as.Date(rng[1]), game_date <= as.Date(rng[2]))
    ids <- selected_team_ids()
    if (length(ids)) df <- df %>% filter(team_id %in% ids)
    opp <- suppressWarnings(as.numeric(f$opp_ids)); opp <- opp[is.finite(opp)]
    if (length(opp)) df <- df %>% filter(opp_team_id %in% opp)
    if (length(f$game_type) && any(nzchar(f$game_type))) df <- df %>% filter(phase %in% f$game_type[nzchar(f$game_type)])
    if (nzchar(f$home_away %||% "")) df <- df %>% filter(is_home == (f$home_away == "home"))
    if (nzchar(f$outcome %||% "")) df <- df %>% filter(has_won == (f$outcome == "win"))

    starter_bounds <- resolve_starters_bounds(
      input$eurogl_num_starters_off_mode, input$eurogl_num_starters_off,
      input$eurogl_num_starters_def_mode, input$eurogl_num_starters_def
    )
    gn <- resolve_gn_last_n_values(f$gn_min, f$gn_max, f$last_n)
    if (!is.na(gn$min_gn)) df <- df %>% filter(round_number >= gn$min_gn)
    if (!is.na(gn$max_gn)) df <- df %>% filter(round_number <= gn$max_gn)
    if (!is.na(gn$last_n)) df <- df %>% group_by(team_id) %>%
      arrange(desc(game_date), desc(game_id), .by_group = TRUE) %>%
      slice_head(n = gn$last_n) %>% ungroup()

    games <- build_games(df, starters_bounds = list(
      off_min = starter_bounds$num_starters_off_min,
      off_max = starter_bounds$num_starters_off_max,
      def_min = starter_bounds$num_starters_def_min,
      def_max = starter_bounds$num_starters_def_max
    ))
    if (is.null(games) || !nrow(games)) return(games)

    gl_attach_percentiles(games, percentile_games(), percentile_metrics)
  })

  output$eurogl_table <- renderDT({
    df <- filtered_games()
    if (is.null(df) || !nrow(df)) return(empty_game_log("No games match the current filters"))
    ff <- identical(input$eurogl_view_mode, "Four Factors")
    # Column sets, heat polarity and number formats mirror Tab 4 exactly; only
    # the two league-varying header labels differ (Rd/Phase for GN/Game Type).
    # Names stay snake_case rather than being renamed to display labels: the
    # shared header container supplies the labels, and gl_apply_heat_styles()
    # keys on the frame's own names. Net is deliberately absent from Four
    # Factors -- it lives in the Summary view on both tabs, and the Usage group
    # is the possession pair.
    if (ff) {
      cols <- c("round_number", "phase_label", "game_date", "team_name", "opp_team_name",
                "result", "score", "minutes",
                "off_ppp", "off_efg_pct", "off_oreb_pct", "off_tov_pct", "off_ftr_pct",
                "def_ppp", "def_efg_pct", "def_oreb_pct", "def_tov_pct", "def_ftr_pct",
                "off_poss", "def_poss")
      heat_reverse <- c(
        off_ppp = FALSE, off_efg_pct = FALSE, off_oreb_pct = FALSE,
        off_tov_pct = TRUE, off_ftr_pct = FALSE,
        def_ppp = TRUE, def_efg_pct = TRUE, def_oreb_pct = TRUE,
        def_tov_pct = FALSE, def_ftr_pct = TRUE
      )
      round_cols <- names(heat_reverse)
      sketch <- gamelog_ff_header("Rd", "Phase")
    } else {
      cols <- c("round_number", "phase_label", "game_date", "team_name", "opp_team_name",
                "result", "score", "minutes",
                "off_ppp", "def_ppp", "net_rtg", "off_poss", "def_poss")
      heat_reverse <- c(off_ppp = FALSE, def_ppp = TRUE)
      round_cols <- c("off_ppp", "def_ppp", "net_rtg")
      sketch <- gamelog_summary_header("Rd", "Phase", has_shots = FALSE)
    }

    pr_cols <- intersect(
      unname(vapply(names(heat_reverse), gl_pr_col_name, character(1))), names(df))
    disp <- df[, c(cols, pr_cols), drop = FALSE]

    result_idx <- which(names(disp) == "result") - 1L
    hidden_idx <- which(names(disp) %in% pr_cols) - 1L
    off_ppp_idx <- which(names(disp) == "off_ppp") - 1L
    def_ppp_idx <- which(names(disp) == "def_ppp") - 1L
    off_poss_idx <- which(names(disp) == "off_poss") - 1L

    col_defs <- list(
      list(className = "dt-center", targets = "_all"),
      list(targets = result_idx, render = gl_result_cell_renderer()),
      list(targets = hidden_idx, visible = FALSE)
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
      options = list(headerCallback = HEADER_TOOLTIP_JS, dom = "Btip",
        buttons = csv_export_button(if (ff) "euroleague_game_logs_four_factors"
                                    else "euroleague_game_logs_summary"),
        pageLength = 50, scrollX = TRUE, scrollY = "70vh", scrollCollapse = TRUE,
        order = list(list(2, "desc"), list(0, "desc")),
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
