# server_tab5_traditional.R - Tab 5: Traditional Player Stats server logic

server_tab5_traditional <- function(input, output, session, shared) {

  TS_NORM_MIN_GP <- 3L
  TS_NORM_PCT <- 75
  TS_RATE_KEEP_PCT <- 0.85

  clean_ts_rows <- function(df) {
    if (is.null(df) || !nrow(df)) return(df)
    df %>%
      filter(
        !is.na(Player), nzchar(trimws(Player)),
        !is.na(team_name), nzchar(trimws(team_name))
      ) %>%
      filter(
        coalesce(gp, 0) > 0 |
          coalesce(poss_on_floor, 0) > 0 |
          coalesce(minutes, 0) > 0
      )
  }

  ts_ref <- reactiveValues(teams = NULL)

  observeEvent(list(input$main_tabs, input$ts_game_year), ignoreInit = TRUE, {
    if (!identical(input$main_tabs, "traditional_stats")) return(NULL)
    gy_int <- as.integer(input$ts_game_year)
    req(gy_int)

    teams_df <- cached_ref_query(
      key = sprintf("ts_teams_%d", gy_int),
      query_fun = function() {
        DBI::dbGetQuery(
          pg_pool,
          "SELECT DISTINCT team_id, team_name
           FROM basketball_test.full_rosters
           WHERE game_year = $1
           ORDER BY team_name",
          params = list(gy_int)
        )
      }
    )
    ts_ref$teams <- teams_df
    updateSelectizeInput(session, "ts_teams", choices = teams_df$team_name, selected = character(0), server = TRUE)
    updateSelectizeInput(session, "ts_opponents", choices = teams_df$team_name, selected = character(0), server = TRUE)

    gn_df <- cached_ref_query(
      key = sprintf("ts_gn_%d", gy_int),
      query_fun = function() {
        DBI::dbGetQuery(
          pg_pool,
          "SELECT DISTINCT gn
           FROM basketball_test.final_schedule_mv
           WHERE game_year = $1
           ORDER BY gn",
          params = list(gy_int)
        )
      }
    )
    gn_vals <- if (nrow(gn_df)) as.integer(gn_df$gn) else integer(0)
    gn_choices <- c("", as.character(gn_vals))
    last_choices <- if (length(gn_vals)) c("", as.character(seq_len(max(gn_vals, na.rm = TRUE)))) else ""
    updateSelectizeInput(session, "ts_gn_min", choices = gn_choices, selected = "")
    updateSelectizeInput(session, "ts_gn_max", choices = gn_choices, selected = "")
    updateSelectizeInput(session, "ts_last_n", choices = last_choices, selected = "")
  })

  observeEvent(input$ts_game_year, {
    b <- shared$season_date_bounds(input$ts_game_year)
    updateDateRangeInput(session, "ts_dates", start = b$start, end = b$end, min = b$start, max = b$end)
  }, ignoreInit = FALSE)

  observeEvent(input$ts_last_n, {
    if (!is.null(input$ts_last_n) && nzchar(input$ts_last_n)) {
      updateSelectizeInput(session, "ts_gn_min", selected = "")
      updateSelectizeInput(session, "ts_gn_max", selected = "")
    }
  }, ignoreInit = TRUE)

  observeEvent(list(input$ts_gn_min, input$ts_gn_max), {
    if ((nzchar(input$ts_gn_min %||% "") || nzchar(input$ts_gn_max %||% "")) &&
        nzchar(input$ts_last_n %||% "")) {
      updateSelectizeInput(session, "ts_last_n", selected = "")
    }
  }, ignoreInit = TRUE)

  observeEvent(input$ts_reset, {
    b <- shared$season_date_bounds(input$ts_game_year %||% DEFAULT_GAME_YEAR)
    updateDateRangeInput(session, "ts_dates", start = b$start, end = b$end, min = b$start, max = b$end)
    updateSelectizeInput(session, "ts_teams", selected = character(0))
    updateSelectizeInput(session, "ts_game_type", selected = "")
    updateSelectizeInput(session, "ts_opponents", selected = character(0))
    updateSelectInput(session, "ts_home_away", selected = "")
    updateSelectInput(session, "ts_outcome", selected = "")
    updateSelectInput(session, "ts_opp_rank_side", selected = "")
    updateSelectInput(session, "ts_opp_rank_n", selected = "")
    updateSelectInput(session, "ts_opp_rank_metric", selected = "")
    updateSelectInput(session, "ts_display_mode", selected = "Totals")
    updateSliderInput(session, "ts_min_gp_slider", value = 1, min = 1, max = 40)
    updateNumericInput(session, "ts_min_gp", value = 1, min = 1, max = 40)
    updateCheckboxInput(session, "ts_show_ineligible", value = FALSE)
    updateCheckboxInput(session, "ts_clutch_enabled", value = FALSE)
    updateSliderInput(session, "ts_clutch_margin", value = 5)
    updateSelectInput(session, "ts_clutch_status", selected = "all")
    updateSliderInput(session, "ts_clutch_minutes", value = 5)
    updateCheckboxInput(session, "ts_clutch_ot_margin", value = FALSE)
    updateSelectizeInput(session, "ts_gn_min", selected = "")
    updateSelectizeInput(session, "ts_gn_max", selected = "")
    updateSelectizeInput(session, "ts_last_n", selected = "")
  })

  observeEvent(input$ts_min_gp_slider, ignoreInit = TRUE, {
    s <- suppressWarnings(as.integer(input$ts_min_gp_slider))
    n <- suppressWarnings(as.integer(input$ts_min_gp))
    if (is.na(s)) return(NULL)
    if (is.na(n) || s != n) {
      updateNumericInput(session, "ts_min_gp", value = s)
    }
  })

  observeEvent(input$ts_min_gp, ignoreInit = TRUE, {
    n <- suppressWarnings(as.integer(input$ts_min_gp))
    s <- suppressWarnings(as.integer(input$ts_min_gp_slider))
    if (is.na(n)) return(NULL)
    if (is.na(s) || n != s) {
      updateSliderInput(session, "ts_min_gp_slider", value = n)
    }
  })

  apply_ts_mode <- function(df, mode, x_poss = NA_real_, x_min = NA_real_) {
    if (is.null(df) || !nrow(df)) return(df)

    count_cols <- c("pts", "reb", "ast", "stl", "blk", "tov", "fgm", "fga", "3pm", "3pa", "ftm", "fta")
    mode <- mode %||% "Totals"

    if (identical(mode, "Per Game")) {
      for (col in count_cols) {
        if (col %in% names(df)) df[[col]] <- ifelse(df$gp > 0, df[[col]] / df$gp, NA_real_)
      }
      if ("poss_on_floor" %in% names(df)) df$poss_on_floor <- ifelse(df$gp > 0, df$poss_on_floor / df$gp, NA_real_)
      if ("minutes" %in% names(df)) df$minutes <- ifelse(df$gp > 0, df$minutes / df$gp, NA_real_)
      return(df)
    }

    if (identical(mode, "Per 60 Possessions")) {
      for (col in count_cols) {
        if (col %in% names(df)) df[[col]] <- ifelse(df$poss_on_floor > 0, df[[col]] / df$poss_on_floor * 60, NA_real_)
      }
      if ("minutes" %in% names(df)) df$minutes <- ifelse(df$poss_on_floor > 0, df$minutes / df$poss_on_floor * 60, NA_real_)
      if ("poss_on_floor" %in% names(df)) df$poss_on_floor <- ifelse(df$poss_on_floor > 0, df$poss_on_floor / df$poss_on_floor * 60, NA_real_)
      return(df)
    }

    if (identical(mode, "Per 30 Minutes")) {
      for (col in count_cols) {
        if (col %in% names(df)) df[[col]] <- ifelse(df$minutes > 0, df[[col]] / df$minutes * 30, NA_real_)
      }
      if ("poss_on_floor" %in% names(df)) df$poss_on_floor <- ifelse(df$minutes > 0, df$poss_on_floor / df$minutes * 30, NA_real_)
      return(df)
    }

    if (identical(mode, "Per X Possessions")) {
      if (!is.finite(x_poss) || x_poss <= 0) return(df)
      for (col in count_cols) {
        if (col %in% names(df)) df[[col]] <- ifelse(df$poss_on_floor > 0, df[[col]] / df$poss_on_floor * x_poss, NA_real_)
      }
      if ("minutes" %in% names(df)) df$minutes <- ifelse(df$poss_on_floor > 0, df$minutes / df$poss_on_floor * x_poss, NA_real_)
      if ("poss_on_floor" %in% names(df)) df$poss_on_floor <- ifelse(df$poss_on_floor > 0, df$poss_on_floor / df$poss_on_floor * x_poss, NA_real_)
      return(df)
    }

    if (identical(mode, "Per X Minutes")) {
      if (!is.finite(x_min) || x_min <= 0) return(df)
      for (col in count_cols) {
        if (col %in% names(df)) df[[col]] <- ifelse(df$minutes > 0, df[[col]] / df$minutes * x_min, NA_real_)
      }
      if ("poss_on_floor" %in% names(df)) df$poss_on_floor <- ifelse(df$minutes > 0, df$poss_on_floor / df$minutes * x_min, NA_real_)
      return(df)
    }

    df
  }

  debounced_range <- reactive(input$ts_dates) %>% debounce(300)
  debounced_teams <- reactive(input$ts_teams) %>% debounce(300)
  debounced_ts_filters <- reactive(list(
    game_type = input$ts_game_type,
    opp_names = input$ts_opponents,
    home_away = input$ts_home_away,
    outcome = input$ts_outcome,
    rank_side = input$ts_opp_rank_side,
    rank_n = input$ts_opp_rank_n,
    metric = input$ts_opp_rank_metric,
    clutch_enabled = input$ts_clutch_enabled,
    clutch_margin = input$ts_clutch_margin,
    clutch_status = input$ts_clutch_status,
    clutch_minutes = input$ts_clutch_minutes,
    clutch_ot_margin = input$ts_clutch_ot_margin
  )) %>% debounce(300)

  gn_params <- reactive({
    min_gn <- if (!is.null(input$ts_gn_min) && nzchar(input$ts_gn_min)) as.integer(input$ts_gn_min) else NA_integer_
    max_gn <- if (!is.null(input$ts_gn_max) && nzchar(input$ts_gn_max)) as.integer(input$ts_gn_max) else NA_integer_
    last_n <- if (!is.null(input$ts_last_n) && nzchar(input$ts_last_n)) as.integer(input$ts_last_n) else NA_integer_
    if (!is.na(last_n)) {
      min_gn <- NA_integer_
      max_gn <- NA_integer_
    }
    if (!is.na(min_gn) || !is.na(max_gn)) {
      last_n <- NA_integer_
    }
    if (!is.na(min_gn) && !is.na(max_gn) && min_gn > max_gn) {
      tmp <- min_gn
      min_gn <- max_gn
      max_gn <- tmp
    }
    list(min_gn = min_gn, max_gn = max_gn, last_n = last_n)
  }) %>% debounce(150)

  selected_team_ids <- reactive({
    td <- ts_ref$teams
    teams_in <- debounced_teams()
    if (is.null(td) || !nrow(td) || is.null(teams_in) || !length(teams_in)) return(NULL)
    td %>% filter(team_name %in% teams_in) %>% pull(team_id)
  })

  selected_opp_ids <- reactive({
    td <- ts_ref$teams
    opp_names <- debounced_ts_filters()$opp_names
    if (is.null(td) || !nrow(td) || is.null(opp_names) || !length(opp_names)) return(NULL)
    td %>% filter(team_name %in% opp_names) %>% pull(team_id)
  })

  run_player_traditional_dynamic <- function(pool, game_year, start_d, end_d,
                                             team_ids_csv, game_type_csv, opp_ids_csv,
                                             home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric,
                                             max_margin, margin_status, max_time_remaining, ot_margin_filter,
                                             min_gn, max_gn, last_n_games) {
    DBI::dbGetQuery(
      pool,
      paste0(
        "SELECT * FROM basketball_test.get_player_traditional_dynamic(",
        "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,$7::text,$8::text,$9::text,$10::int4,$11::text,",
        "$12::int4,$13::text,$14::int4,$15::bool,$16::int4,$17::int4,$18::int4",
        ")"
      ),
      params = list(
        as.integer(game_year),
        if (!is.na(start_d)) as.Date(start_d) else NA,
        if (!is.na(end_d)) as.Date(end_d) else NA,
        team_ids_csv,
        game_type_csv,
        opp_ids_csv,
        home_away,
        outcome,
        opp_rank_side,
        opp_rank_n,
        opp_rank_metric,
        max_margin,
        margin_status,
        max_time_remaining,
        ot_margin_filter,
        min_gn,
        max_gn,
        last_n_games
      )
    )
  }

  fallback_needed <- reactive({
    rng <- debounced_range()
    if (is.null(rng)) return(FALSE)

    start_d <- as.Date(rng[1])
    end_d <- as.Date(rng[2])
    if (is.na(start_d) || is.na(end_d)) return(FALSE)

    gy <- as.integer(input$ts_game_year)
    season_bounds <- shared$season_date_bounds(gy)
    date_changed <- (start_d != season_bounds$start) || (end_d != season_bounds$end)

    f <- debounced_ts_filters()
    extra_filters <- (!is.null(f$game_type) && any(nzchar(f$game_type))) ||
      (!is.null(f$opp_names) && length(f$opp_names) > 0) ||
      nzchar(f$home_away %||% "") ||
      nzchar(f$outcome %||% "") ||
      nzchar(f$rank_side %||% "") ||
      isTRUE(f$clutch_enabled)

    gp <- gn_params()
    gn_active <- !is.na(gp$min_gn) || !is.na(gp$max_gn) || !is.na(gp$last_n)
    gn_raw_active <- nzchar(input$ts_gn_min %||% "") ||
      nzchar(input$ts_gn_max %||% "") ||
      nzchar(input$ts_last_n %||% "")

    date_changed || extra_filters || gn_active || gn_raw_active
  })

  mv_result_df <- reactive({
    req(identical(input$main_tabs, "traditional_stats"))
    gy_int <- as.integer(input$ts_game_year)
    req(gy_int)

    out <- tryCatch(
      DBI::dbGetQuery(
        pg_pool,
        "SELECT player_id, team_id, team_name, player_name AS \"Player\",
                gp, poss_on_floor, minutes,
                pts, reb, ast, stl, blk, tov, fgm, fga, \"3pm\", \"3pa\", ftm, fta,
                fg_pct, tp_pct, ft_pct, efg, ts
         FROM basketball_test.player_traditional_stats_mv
         WHERE game_year = $1",
        params = list(gy_int)
      ),
      error = function(e) NULL
    )
    if (is.null(out)) return(NULL)

    sel_names <- debounced_teams()
    if (!is.null(sel_names) && length(sel_names) > 0) {
      out <- out %>% filter(team_name %in% sel_names)
    }

    out %>%
      clean_ts_rows() %>%
      arrange(desc(pts), desc(minutes), team_name, Player)
  }) %>% bindEvent(input$main_tabs, input$ts_game_year, debounced_teams())

  live_result_df <- reactive({
    req(identical(input$main_tabs, "traditional_stats"))

    gy_int <- as.integer(input$ts_game_year)
    req(gy_int)
    rng <- debounced_range()
    req(rng)
    req(!is.na(rng[1]), !is.na(rng[2]))

    f <- debounced_ts_filters()
    tids <- selected_team_ids()
    opp_ids <- selected_opp_ids()
    gp <- gn_params()
    clutch_enabled <- isTRUE(f$clutch_enabled)
    max_margin <- if (clutch_enabled) suppressWarnings(as.integer(f$clutch_margin)) else NA_integer_
    margin_status <- if (clutch_enabled) (f$clutch_status %||% "all") else NA_character_
    max_time_remaining <- if (clutch_enabled) suppressWarnings(as.integer(f$clutch_minutes)) * 60L else NA_integer_
    ot_margin_filter <- if (clutch_enabled) isTRUE(f$clutch_ot_margin) else FALSE
    team_ids_csv <- if (!is.null(tids) && length(tids) > 0) paste(as.integer(tids), collapse = ",") else NA_character_
    game_type_csv <- if (!is.null(f$game_type) && any(nzchar(f$game_type))) {
      paste(as.integer(f$game_type[nzchar(f$game_type)]), collapse = ",")
    } else {
      NA_character_
    }
    opp_ids_csv <- if (!is.null(opp_ids) && length(opp_ids) > 0) paste(as.integer(opp_ids), collapse = ",") else NA_character_
    rank_side <- if (nzchar(f$rank_side %||% "")) f$rank_side else NA_character_
    rank_n <- suppressWarnings(as.integer(if (!nzchar(f$rank_n %||% "")) NA_character_ else f$rank_n))
    rank_metric <- if (nzchar(f$metric %||% "")) f$metric else NA_character_
    home_away <- if (nzchar(f$home_away %||% "")) f$home_away else NA_character_
    outcome <- if (nzchar(f$outcome %||% "")) f$outcome else NA_character_

    out <- tryCatch(
      run_player_traditional_dynamic(
        pg_pool,
        game_year = gy_int,
        start_d = as.Date(rng[1]),
        end_d = as.Date(rng[2]),
        team_ids_csv = team_ids_csv,
        game_type_csv = game_type_csv,
        opp_ids_csv = opp_ids_csv,
        home_away = home_away,
        outcome = outcome,
        opp_rank_side = rank_side,
        opp_rank_n = rank_n,
        opp_rank_metric = rank_metric,
        max_margin = max_margin,
        margin_status = margin_status,
        max_time_remaining = max_time_remaining,
        ot_margin_filter = ot_margin_filter,
        min_gn = gp$min_gn,
        max_gn = gp$max_gn,
        last_n_games = gp$last_n
      ),
      error = function(e) NULL
    )

    if (is.null(out) || !nrow(out)) return(NULL)

    out %>%
      rename(Player = player_name) %>%
      clean_ts_rows() %>%
      arrange(desc(pts), desc(minutes), team_name, Player)
    }) %>% bindEvent(
    input$main_tabs,
    input$ts_game_year,
    debounced_range(),
    debounced_teams(),
    debounced_ts_filters(),
    gn_params()
  )

  result_df <- reactive({
    req(identical(input$main_tabs, "traditional_stats"))
    if (!isTRUE(fallback_needed())) {
      mv_df <- mv_result_df()
      if (!is.null(mv_df)) return(mv_df)
    }
    live_result_df()
  }) %>% bindEvent(
    input$main_tabs,
    input$ts_game_year,
    debounced_range(),
    debounced_teams(),
    debounced_ts_filters(),
    gn_params()
  )

  observeEvent(result_df(), ignoreInit = FALSE, {
    df <- result_df()
    max_gp <- 1L
    if (!is.null(df) && nrow(df) && "gp" %in% names(df)) {
      max_gp <- suppressWarnings(as.integer(max(df$gp, na.rm = TRUE)))
      if (!is.finite(max_gp) || is.na(max_gp) || max_gp < 1L) max_gp <- 1L
    }
    cur_num <- suppressWarnings(as.integer(input$ts_min_gp))
    cur_sld <- suppressWarnings(as.integer(input$ts_min_gp_slider))
    target <- max(1L, min(max_gp, dplyr::coalesce(cur_num, cur_sld, 1L)))
    updateSliderInput(session, "ts_min_gp_slider", min = 1, max = max_gp, value = target)
    updateNumericInput(session, "ts_min_gp", min = 1, max = max_gp, value = target)
  })

  ts_mode_context <- reactive({
    base_df <- result_df()
    if (is.null(base_df) || !nrow(base_df)) {
      return(list(df = base_df, x_poss = NA_real_, x_min = NA_real_, rate_threshold = 0))
    }

    min_gp <- suppressWarnings(as.integer(TS_NORM_MIN_GP))
    if (!is.finite(min_gp) || min_gp < 1) min_gp <- 1L
    pct <- suppressWarnings(as.numeric(TS_NORM_PCT))
    if (!is.finite(pct)) pct <- 75
    pct <- max(70, min(90, pct))

    df0 <- base_df %>%
      mutate(
        poss_pg = ifelse(gp > 0, poss_on_floor / gp, NA_real_),
        min_pg = ifelse(gp > 0, minutes / gp, NA_real_)
      )

    eligible <- df0 %>%
      filter(gp >= min_gp, !is.na(poss_pg), !is.na(min_pg), poss_pg > 0, min_pg > 0)

    if (!nrow(eligible)) {
      eligible <- df0 %>%
        filter(gp > 0, !is.na(poss_pg), !is.na(min_pg), poss_pg > 0, min_pg > 0)
    }

    x_poss <- if (nrow(eligible)) as.numeric(stats::quantile(eligible$poss_pg, probs = pct / 100, na.rm = TRUE, type = 7)) else NA_real_
    x_min <- if (nrow(eligible)) as.numeric(stats::quantile(eligible$min_pg, probs = pct / 100, na.rm = TRUE, type = 7)) else NA_real_
    poss_vec <- base_df$poss_on_floor
    poss_vec <- poss_vec[is.finite(poss_vec) & poss_vec > 0]
    rate_threshold <- if (length(poss_vec)) {
      as.numeric(stats::quantile(poss_vec, probs = 1 - TS_RATE_KEEP_PCT, na.rm = TRUE, type = 7))
    } else 0

    list(
      df = base_df,
      x_poss = x_poss,
      x_min = x_min,
      rate_threshold = rate_threshold
    )
  }) %>% bindEvent(result_df())

  ts_display_context <- reactive({
    ctx <- ts_mode_context()
    df <- ctx$df
    mode <- input$ts_display_mode %||% "Totals"
    show_ineligible <- isTRUE(input$ts_show_ineligible)
    poss_threshold <- as.numeric(ctx$rate_threshold %||% 0)
    if (is.null(df) || !nrow(df)) {
      return(list(df = df, mode = mode, removed = 0L, ineligible = 0L, threshold = poss_threshold, show_ineligible = show_ineligible))
    }

    min_gp <- suppressWarnings(as.integer(input$ts_min_gp))
    if (!is.finite(min_gp) || is.na(min_gp) || min_gp < 1L) min_gp <- 1L
    df <- df %>% filter(coalesce(gp, 0L) >= min_gp)
    if (is.null(df) || !nrow(df)) {
      return(list(df = df, mode = mode, removed = 0L, ineligible = 0L, threshold = poss_threshold, show_ineligible = show_ineligible))
    }

    removed <- 0L
    ineligible <- 0L
    df$rate_eligible <- TRUE
    if (identical(mode, "Per 60 Possessions") || identical(mode, "Per 30 Minutes")) {
      keep <- !is.na(df$poss_on_floor) & df$poss_on_floor >= poss_threshold
      ineligible <- sum(!keep, na.rm = TRUE)
      df$rate_eligible <- keep
      if (!show_ineligible) {
        removed <- ineligible
        df <- df[keep, , drop = FALSE]
      }
    }

    df <- apply_ts_mode(df, mode, x_poss = ctx$x_poss, x_min = ctx$x_min)

    list(df = df, mode = mode, removed = removed, ineligible = ineligible, threshold = poss_threshold, show_ineligible = show_ineligible)
  }) %>% bindEvent(ts_mode_context(), input$ts_display_mode, input$ts_show_ineligible, input$ts_min_gp, input$ts_min_gp_slider)

  output$ts_mode_warning <- renderUI({
    disp_ctx <- ts_display_context()
    req(!is.null(disp_ctx$mode))
    if (!(identical(disp_ctx$mode, "Per 60 Possessions") || identical(disp_ctx$mode, "Per 30 Minutes"))) return(NULL)
    if (isTRUE(disp_ctx$show_ineligible)) {
      if (!isTRUE(disp_ctx$ineligible > 0)) return(NULL)
      return(
        div(
          class = "alert alert-info py-2 mb-2",
          sprintf(
            "%d non-eligible players shown in gray in %s (below %s total possessions).",
            as.integer(disp_ctx$ineligible),
            disp_ctx$mode,
            format(as.integer(disp_ctx$threshold), big.mark = ",")
          )
        )
      )
    }
    if (!isTRUE(disp_ctx$removed > 0)) return(NULL)
    div(class = "alert alert-warning py-2 mb-2",
        sprintf("%d players hidden in %s (below %s total possessions).",
                as.integer(disp_ctx$removed), disp_ctx$mode, format(as.integer(disp_ctx$threshold), big.mark = ",")))
  }) %>% bindEvent(ts_display_context(), input$main_tabs)

  output$ts_table <- DT::renderDataTable({
    req(identical(input$main_tabs, "traditional_stats"))
    disp_ctx <- ts_display_context()
    df <- disp_ctx$df
    if (is.null(df) || nrow(df) == 0) return(NULL)
    mode <- disp_ctx$mode

    disp <- df %>%
      transmute(
        Team = team_name,
        Player,
        GP = gp,
        `Poss On Floor` = poss_on_floor,
        Min = minutes,
        PTS = pts,
        REB = reb,
        AST = ast,
        STL = stl,
        BLK = blk,
        TOV = tov,
        FGM = fgm,
        FGA = fga,
        `FG%` = fg_pct,
        `3PM` = `3pm`,
        `3PA` = `3pa`,
        `3P%` = tp_pct,
        FTM = ftm,
        FTA = fta,
        `FT%` = ft_pct,
        `eFG%` = efg,
        `TS%` = ts,
        `.eligible_rate` = coalesce(rate_eligible, TRUE)
      )

    order_col <- which(grepl("^PTS", names(disp)))
    if (!length(order_col)) order_col <- 6L
    round_cols <- setdiff(names(disp), c("Team", "Player", "GP", ".eligible_rate"))
    style_cols <- setdiff(names(disp), ".eligible_rate")

    DT::datatable(
      disp,
      rownames = FALSE,
      options = list(
        dom = "tip",
        pageLength = 50,
        deferRender = TRUE,
        scrollX = TRUE,
        scrollY = "70vh",
        scrollCollapse = TRUE,
        order = list(list(order_col - 1L, "desc")),
        columnDefs = list(
          list(className = "dt-center", targets = "_all"),
          list(visible = FALSE, targets = which(names(disp) == ".eligible_rate") - 1L)
        )
      )
    ) %>%
      DT::formatRound(intersect(round_cols, names(disp)), 1) %>%
      DT::formatStyle(
        columns = style_cols,
        valueColumns = ".eligible_rate",
        color = DT::styleEqual(c(TRUE, FALSE), c("inherit", "#9aa0a6")),
        backgroundColor = DT::styleEqual(c(TRUE, FALSE), c(NA, "#f8f9fb"))
      )
  }) %>% bindEvent(ts_display_context(), input$main_tabs)

  output$ts_download_csv <- downloadHandler(
    filename = function() sprintf("traditional_player_stats_%s.csv", Sys.Date()),
    content = function(file) {
      disp_ctx <- ts_display_context()
      df <- disp_ctx$df
      if (is.null(df) || !nrow(df)) {
        write.csv(data.frame(), file, row.names = FALSE)
        return()
      }
      out <- df %>%
        transmute(
          team = team_name,
          player = Player,
          gp = gp,
          poss_on_floor = poss_on_floor,
          minutes = minutes,
          pts = pts,
          reb = reb,
          ast = ast,
          stl = stl,
          blk = blk,
          tov = tov,
          fgm = fgm,
          fga = fga,
          x3pm = `3pm`,
          x3pa = `3pa`,
          ftm = ftm,
          fta = fta,
          fg_pct = fg_pct,
          tp_pct = tp_pct,
          ft_pct = ft_pct,
          efg = efg,
          ts = ts
        )
      write.csv(out, file, row.names = FALSE)
    }
  )
}

