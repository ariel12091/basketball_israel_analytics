# server_tab4.R - Tab 4: Game Logs server logic

gl_result_cell_renderer <- function() DT::JS(
  "function(data, type, row, meta) {
     if (type !== 'display' || !row) return data;
     var color = data === 'W' ? 'var(--ibpl-pos)' : 'var(--ibpl-neg)';
     return '<span style=\"font-weight:700; color:' + color + ';\">' + data + '</span>';
   }"
)

# The date becomes an HTML link after the display frame is built. DataTables
# otherwise compares that markup as text. Return a numeric YYYYMMDD key for
# sort/type operations so ordering cannot depend on HTML or locale; keep the
# display/export value unchanged.
gl_date_cell_renderer <- function() DT::JS(
  "function(data, type, row, meta) {
     if (type !== 'sort' && type !== 'type') return data;
     var node = document.createElement('span');
     node.innerHTML = data == null ? '' : String(data);
     var text = (node.textContent || node.innerText || '').trim();
     var iso = text.match(/^(\\d{4})[-/.](\\d{1,2})[-/.](\\d{1,2})/);
     if (iso) return Number(iso[1]) * 10000 + Number(iso[2]) * 100 + Number(iso[3]);
     var dmy = text.match(/^(\\d{1,2})[-/.](\\d{1,2})[-/.](\\d{4})/);
     if (dmy) return Number(dmy[3]) * 10000 + Number(dmy[2]) * 100 + Number(dmy[1]);
     return 0;
   }"
)

gl_apply_heat_styles <- function(dt, display_df, metric_map, heat_reverse) {
  for (display_name in names(metric_map)) {
    metric_name <- unname(metric_map[[display_name]])
    pr_col <- gl_pr_col_name(metric_name)
    if (!(display_name %in% names(display_df)) || !(pr_col %in% names(display_df))) next
    dt <- DT::formatStyle(
      dt, display_name,
      backgroundColor = DT::styleInterval(
        CUTS, if (isTRUE(heat_reverse[[display_name]])) COLS_REV else COLS_GRAD
      ),
      valueColumns = pr_col
    )
  }
  dt
}

server_tab4 <- function(input, output, session, shared) {

  gl_ref <- reactiveValues(teams = NULL)
  gl_data_version <- reactive(shared_data_version(shared))
  gl_stat_filter_state <- make_stat_filter_state()
  gl_stat_filter_cols <- reactive({
    if (identical(input$gl_view_mode, "Four Factors")) GL_FF_FILTERABLE_COLS else GL_SUMMARY_FILTERABLE_COLS
  })

  setup_stat_filter_handlers("gl", input, session, gl_stat_filter_cols, gl_stat_filter_state)

  # --- Team list for the season ---
  # ignoreInit = FALSE: a restored session lands here with the tab already
  # selected, and the restore bridges below only run inside this observer.
  observeEvent(list(input$main_tabs, input$game_year), ignoreInit = FALSE, {
    if (!identical(input$main_tabs, "game_logs")) return(NULL)
    gy_int <- as.integer(input$game_year)
    teams_gl <- fetch_teams_min(gy_int)
    gl_ref$teams <- teams_gl
    nav <- consume_pending_nav(shared, "game_logs")
    pending_team <- (if (!is.null(nav)) nav$team_id else NULL) %||% shared$pending_gl_team()
    if (!is.null(pending_team) && nzchar(pending_team)) {
      shared$pending_gl_team(NULL)
      update_single_team_selectize(session, "gl_team", teams_gl, selected = pending_team)
    } else {
      team_choices <- team_select_choices_with_all(teams_gl)
      restored_team <- restore_once_selection(session, "gl_team", NULL, team_choices)
      update_single_team_selectize(
        session, "gl_team", teams_gl,
        selected = if (length(restored_team)) restored_team[[1]] else ""
      )
    }
    opponent_choices <- stats::setNames(
      as.character(teams_gl$team_id),
      as.character(teams_gl$team_name)
    )
    updateSelectizeInput(
      session, "gl_opponents",
      choices = opponent_choices,
      selected = restore_aware_selection(
        session, "gl_opponents", isolate(input$gl_opponents), opponent_choices
      ),
      server = TRUE
    )

    gn_df <- fetch_gn_values(gy_int)
    gn_vals <- if (nrow(gn_df)) as.integer(gn_df$gn) else integer(0)
    update_gn_last_n_choices(session, "gl", gn_vals)
  })

  setup_gn_last_n_sync(session, input, "gl")

  observeEvent(input$game_year, {
    b <- shared$season_date_bounds(input$game_year)
    apply_season_date_bounds(session, "gl_dates", b)
  }, ignoreInit = FALSE)

  gl_gn_params <- reactive({
    resolve_gn_last_n_params(input, "gl")
  }) %>% debounce(150)

  # --- Reset ---
  observeEvent(input$gl_reset, {
    updateRadioButtons(session, "gl_view_mode", selected = "Summary")
    b <- shared$season_date_bounds(input$game_year %||% DEFAULT_GAME_YEAR)
    updateDateRangeInput(session, "gl_dates", start = b$start, end = b$end, min = b$start, max = b$end)
    if (!is.null(gl_ref$teams)) {
      update_single_team_selectize(session, "gl_team", gl_ref$teams, selected = "")
    } else {
      updateSelectizeInput(session, "gl_team", selected = "", server = TRUE)
    }
    updateSelectizeInput(session, "gl_game_type", selected = character(0))
    updateSelectizeInput(session, "gl_opponents", selected = character(0))
    updateSelectInput(session, "gl_home_away", selected = "")
    updateSelectInput(session, "gl_outcome", selected = "")
    reset_starters_inputs(session, "gl")
    reset_gn_last_n_inputs(session, "gl")
    reset_stat_filters(gl_stat_filter_state)
  })

  observeEvent(input$gl_view_mode, {
    reset_stat_filters(gl_stat_filter_state)
  }, ignoreInit = TRUE)

  ribbon_modal_server(input, output, session, "gl", "israel", function() shared_data_version(shared))

  # --- Schedule cache per season ---
  gl_schedule <- reactive({
    req(identical(input$main_tabs, "game_logs"))
    gy_int <- as.integer(input$game_year)
    req(gy_int)
    gl_query_schedule(gy_int)
  }) %>% bindEvent(input$game_year, input$main_tabs)

  # --- Filtered schedule rows (team is optional) ---
  gl_filtered_schedule <- reactive({
    sched <- gl_schedule()
    req(nrow(sched) > 0)

    # The filter fields of svc_game_logs()'s `p`.
    gl_filter_games(sched, list(
      team = input$gl_team,
      dates = input$gl_dates,
      game_type = input$gl_game_type,
      opponents = input$gl_opponents,
      home_away = input$gl_home_away,
      outcome = input$gl_outcome,
      gn = gl_gn_params()
    ), "il")
  })

  # Percentile baseline: the season's whole game population, deliberately
  # unfiltered. Filters -- the team selector included -- decide which rows are
  # DISPLAYED, never how the heat cells are scaled, so a game keeps the same
  # colour whether it is read in a league-wide table or a single team's log.
  # Same contract as Tab 5 (see test-tab5-percentile-population.R).
  gl_percentile_schedule <- reactive({
    sched <- gl_schedule()
    req(nrow(sched) > 0)
    sched
  })

  gl_starters_filter <- reactive({
    gl_starters_bounds(
      input$gl_num_starters_off_mode, input$gl_num_starters_off,
      input$gl_num_starters_def_mode, input$gl_num_starters_def
    )
  })

  # --- Lineup totals cache per season ---
  gl_lineup_totals <- reactive({
    req(identical(input$main_tabs, "game_logs"))
    gy_int <- as.integer(input$game_year)
    req(gy_int)
    gl_query_lineup_totals(gy_int)
  }) %>%
    bindCache(as.integer(input$game_year), gl_data_version(), cache = GL_DATA_CACHE) %>%
    bindEvent(input$game_year, input$main_tabs, gl_data_version())

  # --- Lineup FF cache per season ---
  gl_lineup_ff <- reactive({
    req(identical(input$main_tabs, "game_logs"))
    gy_int <- as.integer(input$game_year)
    req(gy_int)
    gl_query_lineup_ff(gy_int)
  }) %>%
    bindCache(as.integer(input$game_year), gl_data_version(), cache = GL_DATA_CACHE) %>%
    bindEvent(input$game_year, input$main_tabs, gl_data_version())

  # ============================================================
  # TEAMS SUMMARY
  # ============================================================
  gl_teams_summary_baseline <- reactive({
    gl_il_baseline("summary", gl_lineup_totals(), gl_percentile_schedule())
  })

  gl_teams_summary <- reactive({
    sched <- gl_filtered_schedule()
    req(nrow(sched) > 0)
    gl_il_games("summary", gl_lineup_totals(), sched, gl_starters_filter(),
                gl_teams_summary_baseline)
  })

  # ============================================================
  # TEAMS FOUR FACTORS
  # ============================================================
  gl_teams_ff_baseline <- reactive({
    gl_il_baseline("four_factors", gl_lineup_ff(), gl_percentile_schedule())
  })

  gl_teams_ff <- reactive({
    sched <- gl_filtered_schedule()
    req(nrow(sched) > 0)
    gl_il_games("four_factors", gl_lineup_ff(), sched, gl_starters_filter(),
                gl_teams_ff_baseline)
  })

  # ============================================================
  # RENDER TABLE
  # ============================================================
  output$gl_table <- DT::renderDataTable({
    req(identical(input$main_tabs, "game_logs"))

    view <- input$gl_view_mode

    if (identical(view, "Summary")) {
      # ------- TEAMS SUMMARY -------
      df <- gl_teams_summary()
      if (is.null(df) || nrow(df) == 0) return(NULL)

      shot_raw_cols <- c("off_fg2m", "off_fg2a", "off_fg3m", "off_fg3a",
                         "def_fg2m", "def_fg2a", "def_fg3m", "def_fg3a")
      has_shots <- all(c("off_fg2a", "off_fg3a") %in% names(df))
      df <- gl_summary_display(df, gl_stat_filter_state$filters(), fetch_scoreless_games(gl_data_version()))
      if (is.null(df)) return(NULL)
      df <- add_ribbon_link_column(df, output_col = "gameflow", link_label = "View")
      disp <- df %>% select(
        gn, game_type_label, game_date, gameflow, team_name, opp_team_name, result, score_display,
        minutes,
        off_ppp, def_ppp, net_rtg,
        any_of(c("Off Shot", "Def Shot")),
        off_poss, def_poss,
        any_of(shot_raw_cols),
        any_of(c("pr_off_ppp", "pr_def_ppp"))
      )

      hide_idx <- which(names(disp) %in% c(shot_raw_cols, "pr_off_ppp", "pr_def_ppp")) - 1L
      date_idx <- which(names(disp) == "game_date") - 1L

      # Shooting column JS render
      make_shot_render_gl <- function(fg2m_col, fg2a_col, fg3m_col, fg3a_col,
                                      is_defense = FALSE, min_fga = 10, avg2 = 53, avg3 = 34) {
        fg2m_idx <- which(names(disp) == fg2m_col) - 1
        fg2a_idx <- which(names(disp) == fg2a_col) - 1
        fg3m_idx <- which(names(disp) == fg3m_col) - 1
        fg3a_idx <- which(names(disp) == fg3a_col) - 1
        sign_mult <- if (is_defense) -1 else 1
        js_str <- sprintf(
          "function(data, type, row, meta) {
             if (type !== 'display' || !row) return data;
             var fg2m = row[%d] || 0, fg2a = row[%d] || 0;
             var fg3m = row[%d] || 0, fg3a = row[%d] || 0;
             var totalFGA = fg2a + fg3a;
             if (!totalFGA) return '<div class=\"shot-acc-label\" style=\"color:#aaa;\">-</div>';
             var fg2pct = fg2a ? Math.round(fg2m / fg2a * 100) : 0;
             var fg3pct = fg3a ? Math.round(fg3m / fg3a * 100) : 0;
             var fg2freq = Math.round(fg2a / totalFGA * 100);
             var fg3freq = 100 - fg2freq;
             var minFGA = %d;
             var sign = %d;
             var avg2 = %d, avg3 = %d;
             function accColor(pct, avg) {
               var d = sign * (pct - avg) / avg;
               d = Math.max(-1, Math.min(1, d * 3));
               var r, g;
               if (d < 0) { r = 200; g = Math.round(200 + d * 120); }
               else       { g = 170; r = Math.round(200 - d * 150); }
               return 'rgb(' + r + ',' + g + ',60)';
             }
             var muted = totalFGA < minFGA;
             var c2 = muted ? '#bbb' : accColor(fg2pct, avg2);
             var c3 = muted ? '#bbb' : accColor(fg3pct, avg3);
             var barOpacity = muted ? 'opacity:0.3;' : '';
             var title2pct = '2PT accuracy: ' + fg2pct + '%% (' + fg2m + '/' + fg2a + ')';
             var title3pct = '3PT accuracy: ' + fg3pct + '%% (' + fg3m + '/' + fg3a + ')';
             var title2freq = '2PT frequency: ' + fg2freq + '%% of FGA (' + fg2a + '/' + totalFGA + ')';
             var title3freq = '3PT frequency: ' + fg3freq + '%% of FGA (' + fg3a + '/' + totalFGA + ')';
             return '<div class=\"shot-acc-label\">' +
               '<span title=\"' + title2pct + '\" style=\"color:' + c2 + '; font-weight:' + (muted ? '400' : '700') + '; cursor:help;\">' + fg2pct + '%%</span>' +
               ' <span style=\"opacity:0.3;\">|</span> ' +
               '<span title=\"' + title3pct + '\" style=\"color:' + c3 + '; font-weight:' + (muted ? '400' : '700') + '; cursor:help;\">' + fg3pct + '%%</span>' +
               '</div>' +
               '<div class=\"shot-bar-container\" style=\"' + barOpacity + '\">' +
               '<div class=\"shot-bar-2pt\" title=\"' + title2freq + '\" style=\"width:' + fg2freq + '%%; cursor:help;\">' + fg2freq + '%%</div>' +
               '<div class=\"shot-bar-3pt\" title=\"' + title3freq + '\" style=\"width:' + fg3freq + '%%; cursor:help;\">' + fg3freq + '%%</div>' +
               '</div>';
           }", fg2m_idx, fg2a_idx, fg3m_idx, fg3a_idx, min_fga, sign_mult, avg2, avg3
        )
        DT::JS(js_str)
      }

      # Dynamic shot averages from the data
      shot_col_defs <- list()
      if (has_shots) {
        shot_col_map <- list(
          "Off Shot" = GL_SHOT_COLS$off,
          "Def Shot" = GL_SHOT_COLS$def
        )
        SHOT_MIN_FGA <- 10L
        shot_avgs <- gl_shot_averages(disp)
        for (disp_name in names(shot_col_map)) {
          cols <- shot_col_map[[disp_name]]
          target_idx <- which(names(disp) == disp_name) - 1
          is_def <- grepl("^Def", disp_name)
          a2 <- shot_avgs[[if (is_def) "def" else "off"]][["fg2"]]
          a3 <- shot_avgs[[if (is_def) "def" else "off"]][["fg3"]]
          if (length(target_idx) && all(cols %in% names(disp))) {
            shot_col_defs[[length(shot_col_defs) + 1]] <- list(
              targets = target_idx,
              render = make_shot_render_gl(cols[1], cols[2], cols[3], cols[4],
                                           is_defense = is_def, min_fga = SHOT_MIN_FGA,
                                           avg2 = a2, avg3 = a3)
            )
          }
        }
      }

      # Result column color
      result_idx <- which(names(disp) == "result") - 1L
      result_render <- gl_result_cell_renderer()

      col_defs <- c(
        list(
          list(targets = hide_idx, visible = FALSE),
          list(targets = "_all", className = "dt-center"),
          list(targets = result_idx, render = result_render),
          list(targets = date_idx, type = "date")
        ),
        shot_col_defs
      )

      sketch <- gamelog_summary_header(has_shots = has_shots, show_gameflow = TRUE)

      off_ppp_idx <- which(names(disp) == "off_ppp") - 1L
      off_poss_idx <- which(names(disp) == "off_poss") - 1L
      off_shot_idx <- if (has_shots) which(names(disp) == "Off Shot") - 1L else integer(0)

      if (length(off_ppp_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = off_ppp_idx, className = "section-left-border dt-center")
      if (length(off_poss_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = off_poss_idx, className = "section-left-border dt-center")
      if (length(off_shot_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = off_shot_idx, className = "section-left-border dt-center")

      dt <- DT::datatable(disp, container = sketch, rownames = FALSE,
                          escape = dt_escape_except(disp, "gameflow"),
                          extensions = "Buttons",
                          options = list(
                            headerCallback = HEADER_TOOLTIP_JS,
                            dom = "Btip", pageLength = 50,
                            buttons = csv_export_button("game_logs_summary"),
                            deferRender = TRUE, scrollX = TRUE,
                            scrollY = "70vh", scrollCollapse = TRUE,
                            order = list(list(date_idx, "desc"), list(0, "desc")),
                            columnDefs = col_defs
                          ))

      dt <- DT::formatRound(dt, c("off_ppp", "def_ppp", "net_rtg"), 1)
      if ("minutes" %in% names(disp)) dt <- DT::formatRound(dt, "minutes", 1)
      dt <- DT::formatCurrency(dt, c("off_poss", "def_poss"), currency = "", interval = 3, mark = ",", digits = 0)
      dt <- gl_apply_heat_styles(
        dt, disp,
        c(off_ppp = "off_ppp", def_ppp = "def_ppp"),
        c(off_ppp = FALSE, def_ppp = TRUE)
      )

      return(dt)

    } else {
      # ------- TEAMS FOUR FACTORS -------
      df <- gl_teams_ff()
      if (is.null(df) || nrow(df) == 0) return(NULL)
      df <- gl_ff_display(df, gl_stat_filter_state$filters(), fetch_scoreless_games(gl_data_version()))
      if (is.null(df)) return(NULL)
      df <- add_ribbon_link_column(df, output_col = "gameflow", link_label = "View")
      disp <- df %>% select(
        gn, game_type_label, game_date, gameflow, team_name, opp_team_name, result, score_display,
        minutes, net_rtg,
        off_ppp, off_efg_pct, off_oreb_pct, off_tov_pct, off_ftr_pct,
        def_ppp, def_efg_pct, def_oreb_pct, def_tov_pct, def_ftr_pct,
        off_poss, def_poss,
        any_of(c(
          "pr_off_ppp", "pr_off_efg_pct", "pr_off_oreb_pct", "pr_off_tov_pct", "pr_off_ftr_pct",
          "pr_def_ppp", "pr_def_efg_pct", "pr_def_oreb_pct", "pr_def_tov_pct", "pr_def_ftr_pct"
        ))
      )

      hidden_pr_cols <- names(disp)[grepl("^pr_", names(disp))]

      # Result column color
      result_idx <- which(names(disp) == "result") - 1L
      result_render <- gl_result_cell_renderer()
      date_idx <- which(names(disp) == "game_date") - 1L

      off_ppp_idx <- which(names(disp) == "off_ppp") - 1L
      def_ppp_idx <- which(names(disp) == "def_ppp") - 1L
      off_poss_idx <- which(names(disp) == "off_poss") - 1L

      col_defs <- list(
        list(targets = "_all", className = "dt-center"),
        list(targets = result_idx, render = result_render),
        list(targets = which(names(disp) %in% hidden_pr_cols) - 1L, visible = FALSE),
        list(targets = date_idx, type = "date")
      )
      if (length(off_ppp_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = off_ppp_idx, className = "section-left-border dt-center")
      if (length(def_ppp_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = def_ppp_idx, className = "section-left-border dt-center")
      if (length(off_poss_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = off_poss_idx, className = "section-left-border dt-center")

      sketch <- gamelog_ff_header(show_gameflow = TRUE)

      dt <- DT::datatable(disp, container = sketch, rownames = FALSE,
                          escape = dt_escape_except(disp, "gameflow"),
                          extensions = "Buttons",
                          options = list(
                            headerCallback = HEADER_TOOLTIP_JS,
                            dom = "Btip", pageLength = 50,
                            buttons = csv_export_button("game_logs_four_factors"),
                            deferRender = TRUE, scrollX = TRUE,
                            scrollY = "70vh", scrollCollapse = TRUE,
                            order = list(list(date_idx, "desc"), list(0, "desc")),
                            columnDefs = col_defs
                          ))

      rate_cols <- c("off_efg_pct", "off_oreb_pct", "off_tov_pct", "off_ftr_pct",
                     "def_efg_pct", "def_oreb_pct", "def_tov_pct", "def_ftr_pct")
      ppp_cols <- c("off_ppp", "def_ppp", "net_rtg")

      dt <- DT::formatRound(dt, intersect(c(rate_cols, ppp_cols), names(disp)), 1)
      if ("minutes" %in% names(disp)) dt <- DT::formatRound(dt, "minutes", 1)
      dt <- DT::formatCurrency(dt, c("off_poss", "def_poss"), currency = "", interval = 3, mark = ",", digits = 0)
      heat_reverse <- c(
        off_ppp = FALSE,
        off_efg_pct = FALSE,
        off_oreb_pct = FALSE,
        off_tov_pct = TRUE,
        off_ftr_pct = FALSE,
        def_ppp = TRUE,
        def_efg_pct = TRUE,
        def_oreb_pct = TRUE,
        def_tov_pct = FALSE,
        def_ftr_pct = TRUE
      )
      dt <- gl_apply_heat_styles(
        dt, disp,
        stats::setNames(names(heat_reverse), names(heat_reverse)),
        heat_reverse
      )

      return(dt)
    }
  })

  # ---- Filter Chips ----
  output$gl_filter_chips <- renderUI({
    team_map <- NULL
    tdf <- gl_ref$teams
    if (is.data.frame(tdf) && nrow(tdf) > 0 &&
        all(c("team_id", "team_name") %in% names(tdf))) {
      ids <- as.character(tdf$team_id)
      lbls <- as.character(tdf$team_name)
      keep <- !is.na(ids) & nzchar(ids)
      ids <- ids[keep]
      lbls <- lbls[keep]
      if (length(ids) > 0) {
        team_map <- stats::setNames(lbls, ids)
      }
    }
    build_filter_chips(
      "gl", input, shared$season_date_bounds,
      reset_btn_id = "gl_reset",
      team_label_map = team_map,
      opponent_label_map = team_map,
      input_ids = list(teams = "gl_team"),
      extra_children = stat_filter_chips_ui("gl", gl_stat_filter_state, gl_stat_filter_cols)
    )
  })
  setup_chip_clears("gl", session, input, shared,
    game_type_id = "gl_game_type", opponents_id = "gl_opponents",
    home_away_id = "gl_home_away", outcome_id = "gl_outcome",
    gn_min_id = "gl_gn_min", gn_max_id = "gl_gn_max", last_n_id = "gl_last_n",
    opp_rank_ids = c(),
    date_id = "gl_dates", gy_input_id = "game_year",
    teams_ids = "gl_team",
    starters_ids = c("gl_num_starters_off_mode", "gl_num_starters_off",
                     "gl_num_starters_def_mode", "gl_num_starters_def"))

  invisible(list(
    gl_filtered_schedule = gl_filtered_schedule,
    gl_teams_summary = gl_teams_summary,
    gl_teams_ff = gl_teams_ff
  ))
}

