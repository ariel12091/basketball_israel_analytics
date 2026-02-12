# server_tab3.R - Tab 3: Team Ratings server logic

server_tab3 <- function(input, output, session, shared) {

  # -------------------------------------------------------------
  # Tab 3: Team Ratings (Fully Expanded Logic)
  # -------------------------------------------------------------
  observeEvent(input$tr_reset, {
    updateRadioButtons(session, "tr_view_mode", selected = "Summary")
    updateDateRangeInput(session, "tr_dates", start = NA, end = NA)
    updateSelectizeInput(session, "tr_game_type", selected = "")
    updateSelectizeInput(session, "tr_opponents", selected = character(0))
    updateSelectInput(session, "tr_home_away", selected = "")
    updateSelectInput(session, "tr_outcome", selected = "")
    updateSelectInput(session, "tr_opp_rank_side", selected = "")
    updateSelectInput(session, "tr_opp_rank_n", selected = "")
    updateSelectInput(session, "tr_opp_rank_metric", selected = "")
    updateCheckboxInput(session, "tr_clutch_enabled", value = FALSE)
    updateSliderInput(session, "tr_clutch_margin", value = 5)
    updateSelectInput(session, "tr_clutch_status", selected = "all")
    updateSliderInput(session, "tr_clutch_minutes", value = 5)
    updateCheckboxInput(session, "tr_clutch_ot_margin", value = FALSE)
    updateSelectizeInput(session, "tr_gn_min", selected = "")
    updateSelectizeInput(session, "tr_gn_max", selected = "")
    updateSelectizeInput(session, "tr_last_n", selected = "")
  })

  tr_teams_for_year <- reactive({
    gy_int <- as.integer(input$tr_game_year)
    req(gy_int)
    cached_ref_query(
      key = sprintf("tr_teams_%d", gy_int),
      query_fun = function() {
        DBI::dbGetQuery(
          pg_pool,
          "SELECT DISTINCT team_id, team_name FROM basketball_test.full_rosters WHERE game_year = $1 ORDER BY team_name",
          params = list(gy_int)
        )
      }
    )
  })

  observeEvent(list(input$tr_game_year, input$main_tabs), ignoreInit = TRUE, {
    if (!identical(input$main_tabs, "team_ratings")) return(NULL)
    req(input$tr_game_year)
    td <- tr_teams_for_year()
    updateSelectizeInput(session, "tr_opponents", choices = td$team_name, selected = character(0))

    gy_int <- as.integer(input$tr_game_year)
    gn_df <- cached_ref_query(
      key = sprintf("tr_gn_%d", gy_int),
      query_fun = function() {
        DBI::dbGetQuery(
          pg_pool,
          "SELECT DISTINCT gn FROM basketball_test.final_schedule_mv WHERE game_year = $1 ORDER BY gn",
          params = list(gy_int)
        )
      }
    )
    gn_vals <- if (nrow(gn_df)) as.integer(gn_df$gn) else integer(0)
    gn_choices <- c("", as.character(gn_vals))
    last_choices <- if (length(gn_vals)) c("", as.character(seq_len(max(gn_vals, na.rm = TRUE)))) else ""
    updateSelectizeInput(session, "tr_gn_min", choices = gn_choices, selected = "")
    updateSelectizeInput(session, "tr_gn_max", choices = gn_choices, selected = "")
    updateSelectizeInput(session, "tr_last_n", choices = last_choices, selected = "")
  })

  run_team_ratings_dynamic <- function(pool, game_year, start_d, end_d, game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric, max_margin, margin_status, max_time_remaining, ot_margin_filter, min_gn = NA_integer_, max_gn = NA_integer_, last_n_games = NA_integer_) {
    DBI::dbGetQuery(pool, paste0("SELECT * FROM basketball_test.get_team_ratings_dynamic(", "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,$7::text,$8::text,$9::int4,$10::text,$11::int4,$12::text,$13::int4,$14::bool,$15::int4,$16::int4,$17::int4", ")"), params = list(as.integer(game_year), if (!is.na(start_d)) as.Date(start_d) else NA, if (!is.na(end_d)) as.Date(end_d) else NA, game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric, max_margin, margin_status, max_time_remaining, ot_margin_filter, min_gn, max_gn, last_n_games))
  }

  run_team_ff_dynamic <- function(pool, game_year, start_d, end_d, game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric, max_margin, margin_status, max_time_remaining, ot_margin_filter, min_gn = NA_integer_, max_gn = NA_integer_, last_n_games = NA_integer_) {
    DBI::dbGetQuery(pool, paste0("SELECT * FROM basketball_test.get_team_four_factors_dynamic(", "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,$7::text,$8::text,$9::int4,$10::text,$11::int4,$12::text,$13::int4,$14::bool,$15::int4,$16::int4,$17::int4", ")"), params = list(as.integer(game_year), if (!is.na(start_d)) as.Date(start_d) else NA, if (!is.na(end_d)) as.Date(end_d) else NA, game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric, max_margin, margin_status, max_time_remaining, ot_margin_filter, min_gn, max_gn, last_n_games))
  }

  tr_params <- reactive({
    gy <- as.integer(input$tr_game_year)
    req(gy)
    start_d <- if (!is.null(input$tr_dates[1]) && !is.na(input$tr_dates[1])) as.Date(input$tr_dates[1]) else NA
    end_d <- if (!is.null(input$tr_dates[2]) && !is.na(input$tr_dates[2])) as.Date(input$tr_dates[2]) else NA
    tr_game_type_csv <- {
      x <- input$tr_game_type
      if (is.null(x) || !length(x) || !any(nzchar(x))) NA_character_ else paste(x[nzchar(x)], collapse = ",")
    }
    td_map <- tr_teams_for_year()
    tr_opp_ids_csv <- {
      sel <- input$tr_opponents
      if (is.null(sel) || !length(sel)) NA_character_ else {
        ids <- td_map %>% filter(team_name %in% sel) %>% pull(team_id)
        paste(ids, collapse = ",")
      }
    }
    tr_home_away <- if (!nzchar(input$tr_home_away %||% "")) NA_character_ else input$tr_home_away
    tr_outcome <- if (!nzchar(input$tr_outcome %||% "")) NA_character_ else input$tr_outcome
    tr_rank_side <- if (!nzchar(input$tr_opp_rank_side %||% "")) NA_character_ else input$tr_opp_rank_side
    tr_rank_n <- suppressWarnings(as.integer(if (!nzchar(input$tr_opp_rank_n %||% "")) NA_character_ else input$tr_opp_rank_n))
    tr_metric <- if (!nzchar(input$tr_opp_rank_metric %||% "")) NA_character_ else input$tr_opp_rank_metric

    min_gn <- if (!is.null(input$tr_gn_min) && nzchar(input$tr_gn_min)) as.integer(input$tr_gn_min) else NA_integer_
    max_gn <- if (!is.null(input$tr_gn_max) && nzchar(input$tr_gn_max)) as.integer(input$tr_gn_max) else NA_integer_
    last_n <- if (!is.null(input$tr_last_n) && nzchar(input$tr_last_n)) as.integer(input$tr_last_n) else NA_integer_
    if (!is.na(last_n)) {
      min_gn <- NA_integer_
      max_gn <- NA_integer_
    }
    if (!is.na(min_gn) || !is.na(max_gn)) {
      last_n <- NA_integer_
    }
    if (!is.na(min_gn) && !is.na(max_gn) && min_gn > max_gn) {
      tmp <- min_gn; min_gn <- max_gn; max_gn <- tmp
    }

    # Extract clutch params
    clutch_enabled <- isTRUE(input$tr_clutch_enabled)
    max_margin <- if (clutch_enabled) as.integer(input$tr_clutch_margin) else NA_integer_
    margin_status <- if (clutch_enabled) input$tr_clutch_status else NA_character_
    max_time_remaining <- if (clutch_enabled) as.integer(input$tr_clutch_minutes) * 60L else NA_integer_
    ot_margin_filter <- if (clutch_enabled) isTRUE(input$tr_clutch_ot_margin) else FALSE

    list(game_year = gy, start_d = start_d, end_d = end_d, game_type_csv = tr_game_type_csv, opp_ids_csv = tr_opp_ids_csv, home_away = tr_home_away, outcome = tr_outcome, rank_side = tr_rank_side, rank_n = tr_rank_n, metric = tr_metric, max_margin = max_margin, margin_status = margin_status, max_time_remaining = max_time_remaining, ot_margin_filter = ot_margin_filter, min_gn = min_gn, max_gn = max_gn, last_n_games = last_n)
  }) %>% debounce(300)

  observeEvent(input$tr_last_n, {
    if (!is.null(input$tr_last_n) && nzchar(input$tr_last_n)) {
      updateSelectizeInput(session, "tr_gn_min", selected = "")
      updateSelectizeInput(session, "tr_gn_max", selected = "")
    }
  }, ignoreInit = TRUE)

  observeEvent(list(input$tr_gn_min, input$tr_gn_max), {
    if ((nzchar(input$tr_gn_min %||% "") || nzchar(input$tr_gn_max %||% "")) &&
        nzchar(input$tr_last_n %||% "")) {
      updateSelectizeInput(session, "tr_last_n", selected = "")
    }
  }, ignoreInit = TRUE)

  tr_fallback_needed <- reactive({
    p <- tr_params()
    has_dates <- !is.na(p$start_d) || !is.na(p$end_d)
    has_gt <- !is.na(p$game_type_csv)
    has_opp <- !is.na(p$opp_ids_csv)
    has_ha <- !is.na(p$home_away)
    has_out <- !is.na(p$outcome)
    has_rank <- !is.na(p$rank_side) || !is.na(p$rank_n)
    has_clutch <- !is.na(p$max_margin) || (!is.na(p$margin_status) && p$margin_status != "all") || !is.na(p$max_time_remaining)
    has_gn <- !is.na(p$min_gn) || !is.na(p$max_gn) || !is.na(p$last_n_games)
    has_dates || has_gt || has_opp || has_ha || has_out || has_rank || has_clutch || has_gn
  })

  tr_data <- reactive({
    p <- tr_params()
    if (tr_fallback_needed()) {
      run_team_ratings_dynamic(pg_pool, game_year = p$game_year, start_d = p$start_d, end_d = p$end_d, game_type_csv = p$game_type_csv, opp_ids_csv = p$opp_ids_csv, home_away = p$home_away, outcome = p$outcome, opp_rank_side = p$rank_side, opp_rank_n = p$rank_n, opp_rank_metric = p$metric, max_margin = p$max_margin, margin_status = p$margin_status, max_time_remaining = p$max_time_remaining, ot_margin_filter = p$ot_margin_filter, min_gn = p$min_gn, max_gn = p$max_gn, last_n_games = p$last_n_games)
    } else {
      DBI::dbGetQuery(pg_pool,
        "SELECT game_year, team_name, off_ppp, def_ppp, net_rtg, games_played, wins, losses, off_poss, def_poss, rank_net_rtg, rank_off_ppp, rank_def_ppp FROM basketball_test.team_ppp_ratings_mv WHERE game_year = $1 ORDER BY rank_net_rtg",
        params = list(p$game_year))
    }
  })

  tr_ff_data <- reactive({
    p <- tr_params()
    if (tr_fallback_needed()) {
      df <- run_team_ff_dynamic(pg_pool, game_year = p$game_year, start_d = p$start_d, end_d = p$end_d, game_type_csv = p$game_type_csv, opp_ids_csv = p$opp_ids_csv, home_away = p$home_away, outcome = p$outcome, opp_rank_side = p$rank_side, opp_rank_n = p$rank_n, opp_rank_metric = p$metric, max_margin = p$max_margin, margin_status = p$margin_status, max_time_remaining = p$max_time_remaining, ot_margin_filter = p$ot_margin_filter, min_gn = p$min_gn, max_gn = p$max_gn, last_n_games = p$last_n_games)
    } else {
      df <- DBI::dbGetQuery(pg_pool,
        "SELECT * FROM basketball_test.team_four_factors_mv WHERE game_year = $1",
        params = list(p$game_year))
    }

    if (is.null(df) || nrow(df) == 0) return(df)

    # Compute percentile ranks — all teams qualify (>>100 poss)
    pr_vec <- function(x, invert = FALSE) {
      n <- sum(!is.na(x))
      if (n <= 1) return(rep(NA_real_, length(x)))
      r <- rank(x, na.last = "keep", ties.method = "average")
      p <- (r - 1) / (n - 1)
      if (invert) p <- 1 - p
      as.numeric(p)
    }

    df$pr_off_ppp  <- pr_vec(df$off_ppp)
    df$pr_off_ts   <- pr_vec(df$off_ts)
    df$pr_off_oreb <- pr_vec(df$off_oreb)
    df$pr_off_tov  <- pr_vec(df$off_tov, invert = TRUE)
    df$pr_off_ftr  <- pr_vec(df$off_ftr)
    df$pr_def_ppp  <- pr_vec(df$def_ppp, invert = TRUE)
    df$pr_def_ts   <- pr_vec(df$def_ts, invert = TRUE)
    df$pr_def_oreb <- pr_vec(df$def_oreb, invert = TRUE)
    df$pr_def_tov  <- pr_vec(df$def_tov)
    df$pr_def_ftr  <- pr_vec(df$def_ftr, invert = TRUE)
    df$pr_net      <- pr_vec(df$net_rtg)

    df
  })

  output$tr_table <- renderDT({
    mode <- input$tr_view_mode

    if (identical(mode, "Four Factors")) {
      # ============================================================
      # FOUR FACTORS TEAM TABLE
      # ============================================================
      df <- tr_ff_data()
      if (is.null(df) || nrow(df) == 0) return(NULL)

      pr_cols <- c("pr_off_ppp", "pr_off_ts", "pr_off_oreb", "pr_off_tov", "pr_off_ftr",
                   "pr_def_ppp", "pr_def_ts", "pr_def_oreb", "pr_def_tov", "pr_def_ftr", "pr_net")

      keep_cols <- c("team_name",
                     "off_ppp", "off_ts", "off_oreb", "off_tov", "off_ftr", "off_poss",
                     "def_ppp", "def_ts", "def_oreb", "def_tov", "def_ftr", "def_poss",
                     "net_rtg")
      df <- df %>% select(any_of(c(keep_cols, pr_cols)))
      df <- df %>% arrange(desc(net_rtg))

      sketch_ff <- htmltools::withTags(table(class = 'display', thead(
        tr(
          th(class = "group-head", ""),
          th(class = "group-head section-left-border", colspan = 6, "Offense"),
          th(class = "group-head section-left-border", colspan = 6, "Defense"),
          th(class = "group-head section-left-border", "")
        ),
        tr(
          th(class = "sub-head", "Team"),
          th(class = "sub-head section-left-border", "PPP"), th(class = "sub-head", "TS%"),
          th(class = "sub-head", "OREB%"), th(class = "sub-head", "TOV%"),
          th(class = "sub-head", "FTR"), th(class = "sub-head", "Poss"),
          th(class = "sub-head section-left-border", "PPP"), th(class = "sub-head", "TS%"),
          th(class = "sub-head", "OREB%"), th(class = "sub-head", "TOV%"),
          th(class = "sub-head", "FTR"), th(class = "sub-head", "Poss"),
          th(class = "sub-head section-left-border", "Net")
        )
      )))

      hide_idx <- which(colnames(df) %in% pr_cols) - 1L
      off_ppp_idx <- which(names(df) == "off_ppp") - 1L
      def_ppp_idx <- which(names(df) == "def_ppp") - 1L
      net_idx     <- which(names(df) == "net_rtg") - 1L

      col_defs <- list(
        list(targets = hide_idx, visible = FALSE),
        list(targets = "_all", className = "dt-center")
      )
      if (length(off_ppp_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = off_ppp_idx, className = "section-left-border dt-center")
      if (length(def_ppp_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = def_ppp_idx, className = "section-left-border dt-center")
      if (length(net_idx))     col_defs[[length(col_defs) + 1]] <- list(targets = net_idx, className = "section-left-border dt-center")

      dt <- DT::datatable(df, container = sketch_ff, rownames = FALSE,
                          options = list(
                            dom = "t", pageLength = 50,
                            deferRender = TRUE, scrollX = TRUE,
                            scrollY = "70vh", scrollCollapse = TRUE,
                            order = list(list(net_idx, "desc")),
                            columnDefs = col_defs
                          ))

      rate_cols <- intersect(c("off_ts", "off_oreb", "off_tov", "off_ftr", "def_ts", "def_oreb", "def_tov", "def_ftr"), names(df))
      ppp_cols  <- intersect(c("off_ppp", "def_ppp", "net_rtg"), names(df))
      poss_cols <- intersect(c("off_poss", "def_poss"), names(df))

      if (length(rate_cols)) dt <- DT::formatRound(dt, rate_cols, 1)
      if (length(ppp_cols))  dt <- DT::formatRound(dt, ppp_cols, 1)
      if (length(poss_cols)) dt <- DT::formatCurrency(dt, poss_cols, currency = "", interval = 3, mark = ",", digits = 0)

      # Color logic — same polarity as Tab 2 FF
      if ("pr_off_ppp"  %in% names(df)) dt <- DT::formatStyle(dt, "off_ppp",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_ppp")
      if ("pr_off_ts"   %in% names(df)) dt <- DT::formatStyle(dt, "off_ts",   backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_ts")
      if ("pr_off_oreb" %in% names(df)) dt <- DT::formatStyle(dt, "off_oreb", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_oreb")
      if ("pr_off_tov"  %in% names(df)) dt <- DT::formatStyle(dt, "off_tov",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_tov")
      if ("pr_off_ftr"  %in% names(df)) dt <- DT::formatStyle(dt, "off_ftr",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_ftr")
      if ("pr_def_ppp"  %in% names(df)) dt <- DT::formatStyle(dt, "def_ppp",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_def_ppp")
      if ("pr_def_ts"   %in% names(df)) dt <- DT::formatStyle(dt, "def_ts",   backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_def_ts")
      if ("pr_def_oreb" %in% names(df)) dt <- DT::formatStyle(dt, "def_oreb", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_def_oreb")
      if ("pr_def_tov"  %in% names(df)) dt <- DT::formatStyle(dt, "def_tov",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_def_tov")
      if ("pr_def_ftr"  %in% names(df)) dt <- DT::formatStyle(dt, "def_ftr",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_def_ftr")
      if ("pr_net"      %in% names(df)) dt <- DT::formatStyle(dt, "net_rtg",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_net")

      return(dt)

    } else {
      # ============================================================
      # SUMMARY TEAM TABLE (existing behavior)
      # ============================================================
      df <- tr_data()
      if (is.null(df) || nrow(df) == 0) return(NULL)
      pretty_names <- c("Season", "Team", "GP", "W", "L", "Off PPP", "Def PPP", "Net Rtg", "Net Rank", "Off Rank", "Def Rank", "Off Poss", "Def Poss")
      disp_df <- df %>% select(game_year, team_name, games_played, wins, losses, off_ppp, def_ppp, net_rtg, rank_net_rtg, rank_off_ppp, rank_def_ppp, off_poss, def_poss)
      max_rank <- max(c(disp_df$rank_net_rtg, disp_df$rank_off_ppp, disp_df$rank_def_ppp), na.rm = TRUE)
      if (max_rank < 2) max_rank <- 2
      cuts <- seq(1.5, max_rank - 0.5, 1)
      cols_rank <- colorRampPalette(c("#1a9850", "#fee08b", "#d73027"))(length(cuts) + 1)

      dt <- datatable(disp_df, colnames = pretty_names, rownames = FALSE, options = list(dom = "t", pageLength = 50, scrollX = TRUE, scrollY = "70vh", scrollCollapse = TRUE, columnDefs = list(list(className = 'dt-center', targets = "_all")))) %>%
        formatRound(c("off_ppp", "def_ppp", "net_rtg"), 1) %>%
        formatCurrency(c("off_poss", "def_poss"), currency = "", interval = 3, mark = ",", digits = 0) %>%
        formatStyle(columns = c("rank_net_rtg", "rank_off_ppp", "rank_def_ppp"), backgroundColor = styleInterval(cuts, cols_rank))
      return(dt)
    }
  })
}

