# server_tab4.R - Tab 4: Game Logs server logic

server_tab4 <- function(input, output, session, shared) {

  gl_ref <- reactiveValues(teams = NULL)

  # --- Team list for the season ---
  observeEvent(list(input$main_tabs, input$gl_game_year), ignoreInit = TRUE, {
    if (!identical(input$main_tabs, "game_logs")) return(NULL)
    gy_int <- as.integer(input$gl_game_year)
    teams_gl <- DBI::dbGetQuery(pg_pool,
      "SELECT DISTINCT team_id, MIN(team_name) AS team_name
       FROM basketball_test.full_rosters
       WHERE game_year = $1
       GROUP BY team_id ORDER BY MIN(team_name)",
      params = list(gy_int))
    gl_ref$teams <- teams_gl
    team_values <- c("", as.character(teams_gl$team_id))
    names(team_values) <- c("\u2014 All teams \u2014", teams_gl$team_name)
    updateSelectizeInput(session, "gl_team", choices = team_values, selected = "", server = TRUE)
    updateSelectizeInput(session, "gl_opponents", choices = teams_gl$team_name,
                         selected = character(0), server = TRUE)
  })

  # --- Reset ---
  observeEvent(input$gl_reset, {
    updateRadioButtons(session, "gl_view_mode", selected = "Summary")
    updateDateRangeInput(session, "gl_dates", start = NA, end = NA)
    if (!is.null(gl_ref$teams)) {
      team_values <- c("", as.character(gl_ref$teams$team_id))
      names(team_values) <- c("\u2014 All teams \u2014", gl_ref$teams$team_name)
      updateSelectizeInput(session, "gl_team", choices = team_values, selected = "", server = TRUE)
    } else {
      updateSelectizeInput(session, "gl_team", selected = "", server = TRUE)
    }
    updateSelectizeInput(session, "gl_game_type", selected = "")
    updateSelectizeInput(session, "gl_opponents", selected = character(0))
    updateSelectInput(session, "gl_home_away", selected = "")
    updateSelectInput(session, "gl_outcome", selected = "")
  })

  # --- Schedule cache per season ---
  gl_schedule <- reactive({
    req(identical(input$main_tabs, "game_logs"))
    gy_int <- as.integer(input$gl_game_year)
    req(gy_int)
    gl_schedule_mv %>%
      filter(game_year == !!gy_int) %>%
      collect()
  }) %>% bindEvent(input$gl_game_year, input$main_tabs)

  # --- Filtered schedule rows (team is optional) ---
  gl_filtered_schedule <- reactive({
    sched <- gl_schedule()
    req(nrow(sched) > 0)

    df <- sched

    # Team filter (optional)
    team_id_str <- input$gl_team
    if (!is.null(team_id_str) && nzchar(team_id_str)) {
      team_id_val <- as.integer(team_id_str)
      df <- df %>% filter(team_id == !!team_id_val)
    }

    # Date filter
    start_d <- input$gl_dates[1]
    end_d <- input$gl_dates[2]
    if (!is.null(start_d) && !is.na(start_d)) df <- df %>% filter(game_date >= !!as.Date(start_d))
    if (!is.null(end_d) && !is.na(end_d)) df <- df %>% filter(game_date <= !!as.Date(end_d))

    # Game type filter
    gt <- input$gl_game_type
    if (!is.null(gt) && length(gt) > 0 && any(nzchar(gt))) {
      gt_vals <- as.integer(gt[nzchar(gt)])
      df <- df %>% filter(game_type %in% !!gt_vals)
    }

    # Opponent filter
    opp_names <- input$gl_opponents
    if (!is.null(opp_names) && length(opp_names) > 0) {
      df <- df %>% filter(opp_team_name %in% !!opp_names)
    }

    # Home/Away filter
    ha <- input$gl_home_away
    if (!is.null(ha) && nzchar(ha)) {
      if (ha == "home") df <- df %>% filter(is_home == TRUE)
      else df <- df %>% filter(is_home == FALSE)
    }

    # Outcome filter
    outcome <- input$gl_outcome
    if (!is.null(outcome) && nzchar(outcome)) {
      if (outcome == "win") df <- df %>% filter(has_won == TRUE)
      else df <- df %>% filter(has_won == FALSE)
    }

    df
  })

  # --- Lineup totals cache per season ---
  gl_lineup_totals <- reactive({
    req(identical(input$main_tabs, "game_logs"))
    gy_int <- as.integer(input$gl_game_year)
    req(gy_int)
    DBI::dbGetQuery(pg_pool, sprintf(
      "SELECT team_id, lineup_hash, type_lineup, g_date, game_id, game_year,
              total_poss, total_pts, fg2_made, fg2_att, fg3_made, fg3_att, minutes
       FROM basketball_test.mv_lineup_totals_by_day
       WHERE game_year = %d", gy_int))
  }) %>% bindEvent(input$gl_game_year, input$main_tabs)

  # --- Lineup FF cache per season ---
  gl_lineup_ff <- reactive({
    req(identical(input$main_tabs, "game_logs"))
    gy_int <- as.integer(input$gl_game_year)
    req(gy_int)
    DBI::dbGetQuery(pg_pool, sprintf(
      "SELECT lineup_hash, team_id, game_id, game_year, type_lineup,
              total_points, total_poss, ts_poss_count, oreb_count,
              oreb_opportunities, tov_count, total_ft_attempts, total_fga, minutes
       FROM basketball_test.lineup_four_factors_by_game
       WHERE game_year = %d", gy_int))
  }) %>% bindEvent(input$gl_game_year, input$main_tabs)

  # ============================================================
  # TEAMS SUMMARY
  # ============================================================
  gl_teams_summary <- reactive({
    sched <- gl_filtered_schedule()
    req(nrow(sched) > 0)

    # Build a set of (game_id, team_id) pairs from schedule
    sched_pairs <- sched %>% select(game_id, team_id) %>% distinct()

    lt <- gl_lineup_totals()
    lt <- lt %>% inner_join(sched_pairs, by = c("game_id", "team_id"))

    if (nrow(lt) == 0) return(NULL)

    # Aggregate per (game_id, team_id, type_lineup)
    game_stats <- lt %>%
      group_by(game_id, team_id, type_lineup) %>%
      summarise(
        poss = sum(total_poss, na.rm = TRUE),
        pts = sum(total_pts, na.rm = TRUE),
        fg2m = sum(fg2_made, na.rm = TRUE),
        fg2a = sum(fg2_att, na.rm = TRUE),
        fg3m = sum(fg3_made, na.rm = TRUE),
        fg3a = sum(fg3_att, na.rm = TRUE),
        mins = sum(minutes, na.rm = TRUE),
        .groups = "drop"
      )

    off <- game_stats %>% filter(type_lineup == "offense") %>%
      rename(off_poss = poss, off_pts = pts,
             off_fg2m = fg2m, off_fg2a = fg2a, off_fg3m = fg3m, off_fg3a = fg3a,
             off_mins = mins) %>%
      select(-type_lineup)
    def <- game_stats %>% filter(type_lineup == "defense") %>%
      rename(def_poss = poss, def_pts = pts,
             def_fg2m = fg2m, def_fg2a = fg2a, def_fg3m = fg3m, def_fg3a = fg3a) %>%
      select(game_id, team_id, def_poss, def_pts, def_fg2m, def_fg2a, def_fg3m, def_fg3a)

    combined <- off %>% left_join(def, by = c("game_id", "team_id"))

    combined <- combined %>% mutate(
      off_ppp = ifelse(off_poss > 0, round(off_pts / off_poss * 100, 1), NA_real_),
      def_ppp = ifelse(def_poss > 0, round(def_pts / def_poss * 100, 1), NA_real_),
      net_rtg = round(coalesce(off_ppp, 0) - coalesce(def_ppp, 0), 1),
      minutes = round(off_mins, 1)
    )

    # Join schedule info (includes gn, team_name)
    sched_info <- sched %>%
      select(game_id, team_id, team_name, gn, game_date, opp_team_name, team_score, opp_score, has_won) %>%
      mutate(
        result = ifelse(has_won, "W", "L"),
        score_display = paste0(team_score, "-", opp_score)
      )

    combined %>%
      inner_join(sched_info, by = c("game_id", "team_id")) %>%
      arrange(gn, game_id, game_date, team_name)
  })

  # ============================================================
  # TEAMS FOUR FACTORS
  # ============================================================
  gl_teams_ff <- reactive({
    sched <- gl_filtered_schedule()
    req(nrow(sched) > 0)

    sched_pairs <- sched %>% select(game_id, team_id) %>% distinct()

    ff <- gl_lineup_ff()
    ff <- ff %>% inner_join(sched_pairs, by = c("game_id", "team_id"))

    if (nrow(ff) == 0) return(NULL)

    # Aggregate per (game_id, team_id, type_lineup)
    game_ff <- ff %>%
      group_by(game_id, team_id, type_lineup) %>%
      summarise(
        total_points = sum(total_points, na.rm = TRUE),
        total_poss = sum(total_poss, na.rm = TRUE),
        ts_poss_count = sum(ts_poss_count, na.rm = TRUE),
        oreb_count = sum(oreb_count, na.rm = TRUE),
        oreb_opportunities = sum(oreb_opportunities, na.rm = TRUE),
        tov_count = sum(tov_count, na.rm = TRUE),
        total_ft_attempts = sum(total_ft_attempts, na.rm = TRUE),
        total_fga = sum(total_fga, na.rm = TRUE),
        mins = sum(minutes, na.rm = TRUE),
        .groups = "drop"
      )

    off <- game_ff %>% filter(type_lineup == "offense") %>%
      rename(off_pts = total_points, off_poss = total_poss,
             off_ts_poss = ts_poss_count, off_oreb = oreb_count,
             off_oreb_opp = oreb_opportunities, off_tov = tov_count,
             off_fta = total_ft_attempts, off_fga = total_fga,
             off_mins = mins) %>%
      select(-type_lineup)
    def <- game_ff %>% filter(type_lineup == "defense") %>%
      rename(def_pts = total_points, def_poss = total_poss,
             def_ts_poss = ts_poss_count, def_oreb = oreb_count,
             def_oreb_opp = oreb_opportunities, def_tov = tov_count,
             def_fta = total_ft_attempts, def_fga = total_fga) %>%
      select(game_id, team_id, def_pts, def_poss, def_ts_poss, def_oreb, def_oreb_opp,
             def_tov, def_fta, def_fga)

    combined <- off %>% left_join(def, by = c("game_id", "team_id"))

    combined <- combined %>% mutate(
      off_ppp = ifelse(off_poss > 0, round(off_pts / off_poss * 100, 1), NA_real_),
      def_ppp = ifelse(def_poss > 0, round(def_pts / def_poss * 100, 1), NA_real_),
      off_ts_pct = ifelse(off_ts_poss > 0, round(off_pts / (2 * off_ts_poss) * 100, 1), NA_real_),
      off_oreb_pct = ifelse(off_oreb_opp > 0, round(off_oreb / off_oreb_opp * 100, 1), NA_real_),
      off_tov_pct = ifelse(off_poss > 0, round(off_tov / off_poss * 100, 1), NA_real_),
      off_ftr_pct = ifelse(off_fga > 0, round(off_fta / off_fga * 100, 1), NA_real_),
      def_ts_pct = ifelse(def_ts_poss > 0, round(def_pts / (2 * def_ts_poss) * 100, 1), NA_real_),
      def_oreb_pct = ifelse(def_oreb_opp > 0, round(def_oreb / def_oreb_opp * 100, 1), NA_real_),
      def_tov_pct = ifelse(def_poss > 0, round(def_tov / def_poss * 100, 1), NA_real_),
      def_ftr_pct = ifelse(def_fga > 0, round(def_fta / def_fga * 100, 1), NA_real_),
      minutes = round(off_mins, 1)
    )

    # Join schedule info (includes gn, team_name)
    sched_info <- sched %>%
      select(game_id, team_id, team_name, gn, game_date, opp_team_name, team_score, opp_score, has_won) %>%
      mutate(
        result = ifelse(has_won, "W", "L"),
        score_display = paste0(team_score, "-", opp_score)
      )

    combined %>%
      inner_join(sched_info, by = c("game_id", "team_id")) %>%
      arrange(gn, game_id, game_date, team_name)
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
      if (has_shots) {
        df[["Off Shot"]] <- coalesce(df$off_fg2a, 0) + coalesce(df$off_fg3a, 0)
        df[["Def Shot"]] <- coalesce(df$def_fg2a, 0) + coalesce(df$def_fg3a, 0)
      }

      disp <- df %>% select(
        gn, game_date, team_name, opp_team_name, result, score_display,
        off_ppp, def_ppp, net_rtg,
        any_of(c("Off Shot", "Def Shot")),
        off_poss, def_poss, minutes,
        any_of(shot_raw_cols)
      )

      hide_idx <- which(names(disp) %in% shot_raw_cols) - 1L

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
             return '<div class=\"shot-acc-label\">' +
               '<span style=\"color:' + c2 + '; font-weight:' + (muted ? '400' : '700') + ';\">' + fg2pct + '%%</span>' +
               ' <span style=\"opacity:0.3;\">|</span> ' +
               '<span style=\"color:' + c3 + '; font-weight:' + (muted ? '400' : '700') + ';\">' + fg3pct + '%%</span>' +
               '</div>' +
               '<div class=\"shot-bar-container\" style=\"' + barOpacity + '\">' +
               '<div class=\"shot-bar-2pt\" style=\"width:' + fg2freq + '%%\">' + fg2freq + '%%</div>' +
               '<div class=\"shot-bar-3pt\" style=\"width:' + fg3freq + '%%\">' + fg3freq + '%%</div>' +
               '</div>';
           }", fg2m_idx, fg2a_idx, fg3m_idx, fg3a_idx, min_fga, sign_mult, avg2, avg3
        )
        DT::JS(js_str)
      }

      # Dynamic shot averages from the data
      shot_col_defs <- list()
      if (has_shots) {
        shot_col_map <- list(
          "Off Shot" = c("off_fg2m", "off_fg2a", "off_fg3m", "off_fg3a"),
          "Def Shot" = c("def_fg2m", "def_fg2a", "def_fg3m", "def_fg3a")
        )
        SHOT_MIN_FGA <- 10L
        for (disp_name in names(shot_col_map)) {
          cols <- shot_col_map[[disp_name]]
          target_idx <- which(names(disp) == disp_name) - 1
          is_def <- grepl("^Def", disp_name)
          fg2a_sum <- sum(disp[[cols[2]]], na.rm = TRUE)
          fg3a_sum <- sum(disp[[cols[4]]], na.rm = TRUE)
          a2 <- if (fg2a_sum > 0) as.integer(round(sum(disp[[cols[1]]], na.rm = TRUE) / fg2a_sum * 100)) else 53L
          a3 <- if (fg3a_sum > 0) as.integer(round(sum(disp[[cols[3]]], na.rm = TRUE) / fg3a_sum * 100)) else 34L
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
      result_render <- DT::JS(
        "function(data, type, row, meta) {
           if (type !== 'display' || !row) return data;
           var color = data === 'W' ? '#1a9850' : '#d73027';
           return '<span style=\"font-weight:700; color:' + color + ';\">' + data + '</span>';
         }")

      col_defs <- c(
        list(
          list(targets = hide_idx, visible = FALSE),
          list(targets = "_all", className = "dt-center"),
          list(targets = result_idx, render = result_render)
        ),
        shot_col_defs
      )

      sketch <- htmltools::withTags(table(class = 'display', thead(
        tr(
          th(class = "sub-head", "GN"),
          th(class = "sub-head", "Date"),
          th(class = "sub-head", "Team"),
          th(class = "sub-head", "Opponent"),
          th(class = "sub-head", "W/L"),
          th(class = "sub-head", "Score"),
          th(class = "sub-head section-left-border", "Off PPP"),
          th(class = "sub-head", "Def PPP"),
          th(class = "sub-head", "Net"),
          if (has_shots) th(class = "sub-head section-left-border", "Off Shot"),
          if (has_shots) th(class = "sub-head", "Def Shot"),
          th(class = "sub-head section-left-border", "Off Poss"),
          th(class = "sub-head", "Def Poss"),
          th(class = "sub-head", "Min")
        )
      )))

      off_ppp_idx <- which(names(disp) == "off_ppp") - 1L
      off_poss_idx <- which(names(disp) == "off_poss") - 1L
      off_shot_idx <- if (has_shots) which(names(disp) == "Off Shot") - 1L else integer(0)

      if (length(off_ppp_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = off_ppp_idx, className = "section-left-border dt-center")
      if (length(off_poss_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = off_poss_idx, className = "section-left-border dt-center")
      if (length(off_shot_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = off_shot_idx, className = "section-left-border dt-center")

      dt <- DT::datatable(disp, container = sketch, rownames = FALSE, escape = FALSE,
                          options = list(
                            dom = "tip", pageLength = 50,
                            deferRender = TRUE, scrollX = TRUE,
                            scrollY = "70vh", scrollCollapse = TRUE,
                            order = list(list(0, "asc")),
                            columnDefs = col_defs
                          ))

      dt <- DT::formatRound(dt, c("off_ppp", "def_ppp", "net_rtg", "minutes"), 1)
      dt <- DT::formatCurrency(dt, c("off_poss", "def_poss"), currency = "", interval = 3, mark = ",", digits = 0)

      return(dt)

    } else {
      # ------- TEAMS FOUR FACTORS -------
      df <- gl_teams_ff()
      if (is.null(df) || nrow(df) == 0) return(NULL)

      disp <- df %>% select(
        gn, game_date, team_name, opp_team_name, result, score_display,
        off_ppp, off_ts_pct, off_oreb_pct, off_tov_pct, off_ftr_pct,
        def_ppp, def_ts_pct, def_oreb_pct, def_tov_pct, def_ftr_pct,
        off_poss, def_poss, minutes
      )

      # Result column color
      result_idx <- which(names(disp) == "result") - 1L
      result_render <- DT::JS(
        "function(data, type, row, meta) {
           if (type !== 'display' || !row) return data;
           var color = data === 'W' ? '#1a9850' : '#d73027';
           return '<span style=\"font-weight:700; color:' + color + ';\">' + data + '</span>';
         }")

      off_ppp_idx <- which(names(disp) == "off_ppp") - 1L
      def_ppp_idx <- which(names(disp) == "def_ppp") - 1L
      off_poss_idx <- which(names(disp) == "off_poss") - 1L

      col_defs <- list(
        list(targets = "_all", className = "dt-center"),
        list(targets = result_idx, render = result_render)
      )
      if (length(off_ppp_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = off_ppp_idx, className = "section-left-border dt-center")
      if (length(def_ppp_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = def_ppp_idx, className = "section-left-border dt-center")
      if (length(off_poss_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = off_poss_idx, className = "section-left-border dt-center")

      sketch <- htmltools::withTags(table(class = 'display', thead(
        tr(
          th(class = "group-head", colspan = 6, ""),
          th(class = "group-head section-left-border", colspan = 5, "Offense"),
          th(class = "group-head section-left-border", colspan = 5, "Defense"),
          th(class = "group-head section-left-border", colspan = 3, "Usage")
        ),
        tr(
          th(class = "sub-head", "GN"),
          th(class = "sub-head", "Date"),
          th(class = "sub-head", "Team"),
          th(class = "sub-head", "Opponent"),
          th(class = "sub-head", "W/L"),
          th(class = "sub-head", "Score"),
          th(class = "sub-head section-left-border", "PPP"),
          th(class = "sub-head", "TS%"),
          th(class = "sub-head", "OREB%"),
          th(class = "sub-head", "TOV%"),
          th(class = "sub-head", "FTR"),
          th(class = "sub-head section-left-border", "PPP"),
          th(class = "sub-head", "TS%"),
          th(class = "sub-head", "OREB%"),
          th(class = "sub-head", "TOV%"),
          th(class = "sub-head", "FTR"),
          th(class = "sub-head section-left-border", "Off Poss"),
          th(class = "sub-head", "Def Poss"),
          th(class = "sub-head", "Min")
        )
      )))

      dt <- DT::datatable(disp, container = sketch, rownames = FALSE, escape = FALSE,
                          options = list(
                            dom = "tip", pageLength = 50,
                            deferRender = TRUE, scrollX = TRUE,
                            scrollY = "70vh", scrollCollapse = TRUE,
                            order = list(list(0, "asc")),
                            columnDefs = col_defs
                          ))

      rate_cols <- c("off_ts_pct", "off_oreb_pct", "off_tov_pct", "off_ftr_pct",
                     "def_ts_pct", "def_oreb_pct", "def_tov_pct", "def_ftr_pct")
      ppp_cols <- c("off_ppp", "def_ppp")

      dt <- DT::formatRound(dt, intersect(c(rate_cols, ppp_cols, "minutes"), names(disp)), 1)
      dt <- DT::formatCurrency(dt, c("off_poss", "def_poss"), currency = "", interval = 3, mark = ",", digits = 0)

      return(dt)
    }
  })
}
