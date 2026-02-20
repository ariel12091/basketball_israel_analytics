# server_tab1.R - Tab 1: On/Off Impact server logic

server_tab1 <- function(input, output, session, shared) {
  auto_min_state <- reactiveValues(
    last_auto = NA_integer_,
    last_auto_all = NA_integer_,
    updating = FALSE
  )
  auto_enabled <- reactiveVal(TRUE)
  resetting <- reactiveVal(FALSE)

  AUTO_TOP_PCT <- 0.35

  auto_min_on_from_df <- function(df, usage_col, step = 10L) {
    if (is.null(df) || !NROW(df)) return(NA_integer_)
    if (!usage_col %in% names(df)) return(NA_integer_)
    n <- nrow(df)
    top_n <- max(1L, ceiling(n * AUTO_TOP_PCT))
    df_ord <- df %>% arrange(desc(.data[[usage_col]]))
    df_top <- df_ord[seq_len(min(top_n, n)), , drop = FALSE]
    min_needed <- suppressWarnings(min(df_top[[usage_col]], na.rm = TRUE))
    if (!is.finite(min_needed)) return(NA_integer_)
    as.integer(floor(min_needed / step) * step)
  }

  auto_min_all_from_df <- function(df, usage_col, on_col, off_col, step = 10L) {
    if (is.null(df) || !NROW(df)) return(NA_integer_)
    if (!usage_col %in% names(df) || !on_col %in% names(df) || !off_col %in% names(df)) return(NA_integer_)
    n <- nrow(df)
    top_n <- max(1L, ceiling(n * AUTO_TOP_PCT))
    df_ord <- df %>% arrange(desc(.data[[usage_col]]))
    df_top <- df_ord[seq_len(min(top_n, n)), , drop = FALSE]
    poss_min <- pmin(df_top[[on_col]], df_top[[off_col]])
    min_needed <- suppressWarnings(min(poss_min, na.rm = TRUE))
    if (!is.finite(min_needed)) return(NA_integer_)
    as.integer(floor(min_needed / step) * step)
  }

  resolve_poss_cols <- function(df, mode) {
    if (identical(mode, "Four Factors")) {
      if (all(c("off_on_poss", "off_off_poss") %in% names(df))) {
        return(list(on = "off_on_poss", off = "off_off_poss"))
      }
    } else {
      if (all(c("ON Poss", "OFF Poss") %in% names(df))) {
        return(list(on = "ON Poss", off = "OFF Poss"))
      }
      if (all(c("off_on_poss", "off_off_poss") %in% names(df))) {
        return(list(on = "off_on_poss", off = "off_off_poss"))
      }
    }
    list(on = NA_character_, off = NA_character_)
  }

  # ======== On/Off tab Logic ===================================
  observeEvent(shared$selected_game_year(), {
    bounds <- shared$season_date_bounds(shared$selected_game_year())
    updateDateRangeInput(session, "date_range",
                         start = bounds$start, end = bounds$end,
                         min = bounds$start, max = bounds$end)

    gy_int <- as.integer(shared$selected_game_year())
    gn_df <- cached_ref_query(
      key = sprintf("on_gn_%d", gy_int),
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
    updateSelectizeInput(session, "on_gn_min", choices = gn_choices, selected = "")
    updateSelectizeInput(session, "on_gn_max", choices = gn_choices, selected = "")
    updateSelectizeInput(session, "on_last_n", choices = last_choices, selected = "")
  }, ignoreInit = FALSE)


  # --- Reset Logic ---
  observeEvent(input$reset_defaults, {
    resetting(TRUE)
    updateSelectInput(session, "game_year", selected = DEFAULT_GAME_YEAR)
    bounds <- shared$season_date_bounds(DEFAULT_GAME_YEAR)
    updateDateRangeInput(session, "date_range",
                         start = bounds$start, end = bounds$end,
                         min = bounds$start, max = bounds$end)
    updateSelectizeInput(session, "on_game_type", selected = "")
    updateSelectizeInput(session, "on_opponents", selected = character(0))
    updateSelectInput(session, "on_home_away", selected = "")
    updateSelectInput(session, "on_outcome", selected = "")
    updateSelectInput(session, "on_opp_rank_side", selected = "")
    updateSelectInput(session, "on_opp_rank_n", selected = "")
    updateSelectInput(session, "on_opp_rank_metric", selected = "")
    updateSliderInput(session, "min_all_poss", value = DEFAULT_MIN_ALL)
    updateSliderInput(session, "min_on_poss", value = DEFAULT_MIN_ON)
    updateSelectizeInput(session, "on_gn_min", selected = "")
    updateSelectizeInput(session, "on_gn_max", selected = "")
    updateSelectizeInput(session, "on_last_n", selected = "")
    auto_min_state$last_auto <- as.integer(DEFAULT_MIN_ON)
    auto_min_state$last_auto_all <- as.integer(DEFAULT_MIN_ALL)
    auto_enabled(FALSE)
    # Clear teams
    updateSelectizeInput(session, "teams", selected = character(0))
    session$onFlushed(function() resetting(FALSE), once = TRUE)
  })

  debounced_range <- reactive(input$date_range) %>% debounce(300)
  debounced_teams <- reactive(input$teams) %>% debounce(300)
  debounced_on_filters <- reactive(list(
    game_type = input$on_game_type,
    opp_names = input$on_opponents,
    home_away = input$on_home_away,
    outcome = input$on_outcome,
    rank_side = input$on_opp_rank_side,
    rank_n = input$on_opp_rank_n,
    metric = input$on_opp_rank_metric
  )) %>% debounce(300)

  gn_params <- reactive({
    min_gn <- if (!is.null(input$on_gn_min) && nzchar(input$on_gn_min)) as.integer(input$on_gn_min) else NA_integer_
    max_gn <- if (!is.null(input$on_gn_max) && nzchar(input$on_gn_max)) as.integer(input$on_gn_max) else NA_integer_
    last_n <- if (!is.null(input$on_last_n) && nzchar(input$on_last_n)) as.integer(input$on_last_n) else NA_integer_
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
    list(min_gn = min_gn, max_gn = max_gn, last_n = last_n)
  }) %>% debounce(150)

  observeEvent(input$min_on_poss, {
    if (isTRUE(auto_min_state$updating)) return(invisible(NULL))
    cur_val <- as.integer(input$min_on_poss)
    last_auto <- as.integer(auto_min_state$last_auto)
    if (!is.na(cur_val) && !is.na(last_auto) && cur_val == last_auto) {
      return(invisible(NULL))
    }
    auto_enabled(FALSE)
  }, ignoreInit = TRUE)

  observeEvent(input$min_all_poss, {
    if (isTRUE(auto_min_state$updating)) return(invisible(NULL))
    cur_val <- as.integer(input$min_all_poss)
    last_auto <- as.integer(auto_min_state$last_auto_all)
    if (!is.na(cur_val) && !is.na(last_auto) && cur_val == last_auto) {
      return(invisible(NULL))
    }
    auto_enabled(FALSE)
  }, ignoreInit = TRUE)

  observeEvent(list(debounced_range(), debounced_teams(), debounced_on_filters(),
                    gn_params(), input$game_year, input$onoff_view_mode), {
    if (isTRUE(resetting())) return(invisible(NULL))
    auto_enabled(TRUE)
  }, ignoreInit = TRUE)

  observeEvent(list(debounced_range(), debounced_teams(), debounced_on_filters(),
                    gn_params(), input$game_year, input$onoff_view_mode, input$min_all_poss), {
    if (!isTRUE(auto_enabled())) return(invisible(NULL))

    mode <- input$onoff_view_mode

    df_base <- NULL
    if (identical(mode, "Four Factors")) {
      df_base <- ff_ranked_df()
      tids <- selected_team_ids()
      if (!is.null(tids) && length(tids) > 0) df_base <- df_base %>% filter(team_id %in% !!tids)
    } else {
      if (isTRUE(fallback_needed())) {
        rng <- debounced_range()
        req(rng)
        tids <- selected_team_ids()
        gy <- shared$selected_game_year()
        f <- debounced_on_filters()
        game_type_csv <- if (is.null(f$game_type) || !any(nzchar(f$game_type))) NA_character_ else paste(f$game_type[nzchar(f$game_type)], collapse = ",")
        opp_ids_csv <- {
          ids <- shared$selected_opp_ids_on()
          if (is.null(ids)) NA_character_ else paste(ids, collapse = ",")
        }
        home_away <- if (!nzchar(f$home_away %||% "")) NA_character_ else f$home_away
        outcome <- if (!nzchar(f$outcome %||% "")) NA_character_ else f$outcome
        gp <- gn_params()

        df_base <- run_onoff_compute_14(
          pg_pool,
          start_d = as.Date(rng[1]), end_d = as.Date(rng[2]),
          team_ids = tids, min_all = 0L, min_on = 0L,
          min_net = DEFAULT_MIN_NET, game_year = gy,
          game_type_csv = game_type_csv, opp_ids_csv = opp_ids_csv,
          home_away = home_away, outcome = outcome,
          opp_rank_side = if (!nzchar(f$rank_side %||% "")) NA else f$rank_side,
          opp_rank_n = suppressWarnings(as.integer(if (!nzchar(f$rank_n %||% "")) NA else f$rank_n)),
          opp_rank_metric = if (!nzchar(f$metric %||% "")) NA else f$metric,
          min_gn = gp$min_gn, max_gn = gp$max_gn, last_n_games = gp$last_n
        )
      } else {
        df_base <- mv_result_df()
        tids_names <- input$teams
        if (!is.null(tids_names) && length(tids_names) > 0) {
          df_base <- df_base %>% filter(Team %in% tids_names)
        }
      }
    }

    poss_cols <- resolve_poss_cols(df_base, mode)
    if (is.na(poss_cols$on)) return(invisible(NULL))
    min_needed <- auto_min_on_from_df(df_base, usage_col = poss_cols$on, step = 10L)
    cur_val <- as.integer(input$min_on_poss)
    if (is.na(min_needed) || is.na(cur_val)) return(invisible(NULL))
    if (cur_val <= min_needed) return(invisible(NULL))

    auto_min_state$updating <- TRUE
    updateSliderInput(session, "min_on_poss", value = min_needed)
    auto_min_state$updating <- FALSE
    auto_min_state$last_auto <- min_needed
  }, ignoreInit = TRUE)

  observeEvent(list(debounced_range(), debounced_teams(), debounced_on_filters(),
                    gn_params(), input$game_year, input$onoff_view_mode, input$min_on_poss), {
    if (!isTRUE(auto_enabled())) return(invisible(NULL))

    mode <- input$onoff_view_mode
    df_base <- NULL
    if (identical(mode, "Four Factors")) {
      df_base <- ff_ranked_df()
      tids <- selected_team_ids()
      if (!is.null(tids) && length(tids) > 0) df_base <- df_base %>% filter(team_id %in% !!tids)
      if ("off_on_poss" %in% names(df_base)) {
        df_base <- df_base %>% filter(off_on_poss >= !!input$min_on_poss)
      }
    } else {
      if (isTRUE(fallback_needed())) {
        rng <- debounced_range()
        req(rng)
        tids <- selected_team_ids()
        gy <- shared$selected_game_year()
        f <- debounced_on_filters()
        game_type_csv <- if (is.null(f$game_type) || !any(nzchar(f$game_type))) NA_character_ else paste(f$game_type[nzchar(f$game_type)], collapse = ",")
        opp_ids_csv <- {
          ids <- shared$selected_opp_ids_on()
          if (is.null(ids)) NA_character_ else paste(ids, collapse = ",")
        }
        home_away <- if (!nzchar(f$home_away %||% "")) NA_character_ else f$home_away
        outcome <- if (!nzchar(f$outcome %||% "")) NA_character_ else f$outcome
        gp <- gn_params()

        df_base <- run_onoff_compute_14(
          pg_pool,
          start_d = as.Date(rng[1]), end_d = as.Date(rng[2]),
          team_ids = tids, min_all = 0L, min_on = 0L,
          min_net = DEFAULT_MIN_NET, game_year = gy,
          game_type_csv = game_type_csv, opp_ids_csv = opp_ids_csv,
          home_away = home_away, outcome = outcome,
          opp_rank_side = if (!nzchar(f$rank_side %||% "")) NA else f$rank_side,
          opp_rank_n = suppressWarnings(as.integer(if (!nzchar(f$rank_n %||% "")) NA else f$rank_n)),
          opp_rank_metric = if (!nzchar(f$metric %||% "")) NA else f$metric,
          min_gn = gp$min_gn, max_gn = gp$max_gn, last_n_games = gp$last_n
        )
      } else {
        df_base <- mv_result_df()
        tids_names <- input$teams
        if (!is.null(tids_names) && length(tids_names) > 0) {
          df_base <- df_base %>% filter(Team %in% tids_names)
        }
      }
    }

    poss_cols <- resolve_poss_cols(df_base, mode)
    if (is.na(poss_cols$on) || is.na(poss_cols$off)) return(invisible(NULL))

    min_needed <- auto_min_all_from_df(df_base, usage_col = poss_cols$on, on_col = poss_cols$on, off_col = poss_cols$off, step = 10L)
    cur_val <- as.integer(input$min_all_poss)
    if (is.na(min_needed) || is.na(cur_val)) return(invisible(NULL))
    if (cur_val <= min_needed) return(invisible(NULL))

    auto_min_state$updating <- TRUE
    updateSliderInput(session, "min_all_poss", value = min_needed)
    auto_min_state$updating <- FALSE
    auto_min_state$last_auto_all <- min_needed
  }, ignoreInit = TRUE)

  observeEvent(input$on_last_n, {
    if (!is.null(input$on_last_n) && nzchar(input$on_last_n)) {
      updateSelectizeInput(session, "on_gn_min", selected = "")
      updateSelectizeInput(session, "on_gn_max", selected = "")
    }
  }, ignoreInit = TRUE)

  observeEvent(list(input$on_gn_min, input$on_gn_max), {
    if ((nzchar(input$on_gn_min %||% "") || nzchar(input$on_gn_max %||% "")) &&
        nzchar(input$on_last_n %||% "")) {
      updateSelectizeInput(session, "on_last_n", selected = "")
    }
  }, ignoreInit = TRUE)

  selected_team_ids <- reactive({
    td <- shared$teams_for_year_df()
    teams_in <- debounced_teams()
    if (is.null(teams_in) || !length(teams_in)) return(NULL)
    td %>% filter(team_name %in% teams_in) %>% pull(team_id)
  })

  # --- Fallback Logic ---
  # We do NOT return true if only team/min_poss changed.
  fallback_needed <- reactive({
    rng <- debounced_range()
    if (is.null(rng)) return(FALSE)
    start_d <- as.Date(rng[1])
    end_d <- as.Date(rng[2])
    if (is.na(start_d) || is.na(end_d)) return(FALSE)
    gy <- shared$selected_game_year()
    season_bounds <- shared$season_date_bounds(gy)

    date_changed <- (start_d != season_bounds$start) || (end_d != season_bounds$end)

    f <- debounced_on_filters()
    extra_filters <- (!is.null(f$game_type) && any(nzchar(f$game_type))) ||
      (!is.null(f$opp_names) && length(f$opp_names) > 0) ||
      nzchar(f$home_away %||% "") ||
      nzchar(f$outcome %||% "") ||
      nzchar(f$rank_side %||% "")

    gp <- gn_params()
    gn_active <- !is.na(gp$min_gn) || !is.na(gp$max_gn) || !is.na(gp$last_n)
    gn_raw_active <- nzchar(input$on_gn_min %||% "") ||
      nzchar(input$on_gn_max %||% "") ||
      nzchar(input$on_last_n %||% "")
    gn_active <- gn_active || gn_raw_active

    date_changed || extra_filters || gn_active
  })

  # --- On/Off Compute Function ---
  run_onoff_compute_14 <- function(pool, start_d, end_d, team_ids, min_all, min_on, min_net, game_year, game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric, min_gn = NA_integer_, max_gn = NA_integer_, last_n_games = NA_integer_) {
    team_csv <- if (is.null(team_ids) || !length(team_ids)) NA_character_ else paste(team_ids, collapse = ",")
    DBI::dbGetQuery(pool, paste0("SELECT * FROM basketball_test.onoff_compute(", "$1::date,$2::date,$3::text,$4::int4,$5::int4,$6::numeric,$7::text,", "$8::text,$9::text,$10::text,$11::text,$12::text,$13::int4,$14::text,", "$15::int4,$16::int4,$17::int4", ")"),
                    params = list(as.Date(start_d), as.Date(end_d), team_csv, as.integer(min_all), as.integer(min_on), as.numeric(min_net), as.character(game_year), game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric, min_gn, max_gn, last_n_games))
  }

  # --- Four Factors Compute Function ---
  run_four_factors_compute <- function(pool, game_year, start_d, end_d, team_ids,
                                       game_type_csv, opp_ids_csv, home_away, outcome,
                                       opp_rank_side, opp_rank_n, opp_rank_metric,
                                       min_gn = NA_integer_, max_gn = NA_integer_, last_n_games = NA_integer_) {
    team_csv <- if (is.null(team_ids) || !length(team_ids)) NA_character_ else paste(team_ids, collapse = ",")
    DBI::dbGetQuery(pool,
                    paste0("SELECT * FROM basketball_test.four_factors_compute(",
                           "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,",
                           "$7::text,$8::text,$9::text,$10::int4,$11::text,",
                           "$12::int4,$13::int4,$14::int4",
                           ")"),
                    params = list(as.integer(game_year), start_d, end_d, team_csv,
                                  game_type_csv, opp_ids_csv, home_away, outcome,
                                  opp_rank_side, opp_rank_n, opp_rank_metric,
                                  min_gn, max_gn, last_n_games))
  }

  # --- Live Calculation (Summary) ---
  live_result_df <- reactive({
    req(!is.null(input$min_all_poss), !is.null(input$min_on_poss))
    rng <- debounced_range()
    req(rng)
    req(!is.na(rng[1]), !is.na(rng[2]))
    tids <- selected_team_ids()
    gy <- shared$selected_game_year()
    f <- debounced_on_filters()
    game_type_csv <- if (is.null(f$game_type) || !any(nzchar(f$game_type))) NA_character_ else paste(f$game_type[nzchar(f$game_type)], collapse = ",")
    opp_ids_csv <- {
      ids <- shared$selected_opp_ids_on()
      if (is.null(ids)) NA_character_ else paste(ids, collapse = ",")
    }
    home_away <- if (!nzchar(f$home_away %||% "")) NA_character_ else f$home_away
    outcome <- if (!nzchar(f$outcome %||% "")) NA_character_ else f$outcome

    gp <- gn_params()
    run_onoff_compute_14(pg_pool, start_d = as.Date(rng[1]), end_d = as.Date(rng[2]), team_ids = tids, min_all = input$min_all_poss, min_on = input$min_on_poss, min_net = DEFAULT_MIN_NET, game_year = gy, game_type_csv = game_type_csv, opp_ids_csv = opp_ids_csv, home_away = home_away, outcome = outcome, opp_rank_side = if (!nzchar(f$rank_side %||% "")) NA else f$rank_side, opp_rank_n = suppressWarnings(as.integer(if (!nzchar(f$rank_n %||% "")) NA else f$rank_n)), opp_rank_metric = if (!nzchar(f$metric %||% "")) NA else f$metric, min_gn = gp$min_gn, max_gn = gp$max_gn, last_n_games = gp$last_n)
  })

  # --- Live Calculation (Four Factors) ---
  live_ff_result_df <- reactive({
    rng <- debounced_range()
    req(rng)
    gy <- shared$selected_game_year()
    f <- debounced_on_filters()

    game_type_csv <- if (is.null(f$game_type) || !any(nzchar(f$game_type))) NA_character_ else paste(f$game_type[nzchar(f$game_type)], collapse = ",")
    opp_ids_csv <- {
      ids <- shared$selected_opp_ids_on()
      if (is.null(ids)) NA_character_ else paste(ids, collapse = ",")
    }
    home_away <- if (!nzchar(f$home_away %||% "")) NA_character_ else f$home_away
    outcome <- if (!nzchar(f$outcome %||% "")) NA_character_ else f$outcome

    gp <- gn_params()
    run_four_factors_compute(pg_pool,
                             game_year = gy,
                             start_d = as.Date(rng[1]),
                             end_d = as.Date(rng[2]),
                             team_ids = NULL,
                             game_type_csv = game_type_csv,
                             opp_ids_csv = opp_ids_csv,
                             home_away = home_away,
                             outcome = outcome,
                             opp_rank_side = if (!nzchar(f$rank_side %||% "")) NA else f$rank_side,
                             opp_rank_n = suppressWarnings(as.integer(if (!nzchar(f$rank_n %||% "")) NA else f$rank_n)),
                             opp_rank_metric = if (!nzchar(f$metric %||% "")) NA else f$metric,
                             min_gn = gp$min_gn, max_gn = gp$max_gn, last_n_games = gp$last_n)
  })

  # --- MV Fetch (Summary - LOAD FULL DATA) ---
  # Only load raw MV here. Filtering happens later in result_df.
  mv_result_df <- reactive({
    gy <- as.integer(shared$selected_game_year())
    DBI::dbGetQuery(pg_pool,
      sprintf('SELECT * FROM basketball_test.onoff_default_mv WHERE "Year" = %d ORDER BY "Net RTG Diff" DESC, "Team", "Last Name", "First Name"', gy))
  })

  # --- MV Fetch (Four Factors - LOAD FULL DATA) ---
  advanced_result_df <- reactive({
    gy <- as.integer(shared$selected_game_year())
    DBI::dbGetQuery(pg_pool,
      "SELECT * FROM basketball_test.player_advanced_stats_mv WHERE game_year = $1",
      params = list(gy))
  })

  # --- Full ranked Four Factors data (ranks computed BEFORE any user filtering) ---
  ff_ranked_df <- reactive({
    if (isTRUE(fallback_needed())) {
      # Dynamic SQL path: use four_factors_compute + onoff_compute for rating diffs
      df_adv <- live_ff_result_df()

      # Get RTG diffs for ALL players (no min_poss or team filter)
      # Min-poss and team filtering is applied later in result_df()
      rng <- debounced_range()
      gy <- shared$selected_game_year()
      f <- debounced_on_filters()
      game_type_csv <- if (is.null(f$game_type) || !any(nzchar(f$game_type))) NA_character_ else paste(f$game_type[nzchar(f$game_type)], collapse = ",")
      opp_ids_csv <- {
        ids <- shared$selected_opp_ids_on()
        if (is.null(ids)) NA_character_ else paste(ids, collapse = ",")
      }
      home_away <- if (!nzchar(f$home_away %||% "")) NA_character_ else f$home_away
      outcome <- if (!nzchar(f$outcome %||% "")) NA_character_ else f$outcome

      gp <- gn_params()
      df_sum <- run_onoff_compute_14(pg_pool,
                                     start_d = as.Date(rng[1]), end_d = as.Date(rng[2]),
                                     team_ids = NULL, min_all = 0L, min_on = 0L, min_net = DEFAULT_MIN_NET,
                                     game_year = gy, game_type_csv = game_type_csv, opp_ids_csv = opp_ids_csv,
                                     home_away = home_away, outcome = outcome,
                                     opp_rank_side = if (!nzchar(f$rank_side %||% "")) NA else f$rank_side,
                                     opp_rank_n = suppressWarnings(as.integer(if (!nzchar(f$rank_n %||% "")) NA else f$rank_n)),
                                     opp_rank_metric = if (!nzchar(f$metric %||% "")) NA else f$metric,
                                     min_gn = gp$min_gn, max_gn = gp$max_gn, last_n_games = gp$last_n) %>%
        select(player_id, team_id, `Net RTG Diff`, `Off ON Diff`, `Def ON Diff`)

      df <- df_adv %>%
        left_join(df_sum, by = c("player_id", "team_id"))
    } else {
      # MV path (existing behavior)
      df_adv <- advanced_result_df()

      # Join with Summary Stats to get Ratings (Net/Off/Def Diff)
      if (!"Net RTG Diff" %in% names(df_adv)) {
        df_sum <- mv_result_df() %>%
          select(player_id, team_id, "Year", `Net RTG Diff`, `Off ON Diff`, `Def ON Diff`)

        df <- df_adv %>%
          left_join(df_sum, by = c("player_id", "team_id", "game_year" = "Year"))
      } else {
        df <- df_adv
      }
    }

    # Derived display columns
    df <- df %>% mutate(
      `Off Rtg Diff` = as.numeric(`Off ON Diff`),
      `Def Rtg Diff` = as.numeric(`Def ON Diff`),
      `Net Diff`     = round(`Net RTG Diff`, 1)
    )

    # Calculate ALL ranks on full unfiltered dataset
    # Adaptive baseline: lower threshold when data is sparse (narrow date ranges)
    rank_thresh <- adaptive_baseline(df$off_on_poss)

    # Background color ranks (pr_ prefix)
    df <- df %>% mutate(
      pr_net_diff = percent_rank(if_else(off_on_poss >= rank_thresh, coalesce(`Net Diff`, -999), NA_real_)),
      pr_off_rtg  = percent_rank(if_else(off_on_poss >= rank_thresh, coalesce(`Off Rtg Diff`, -999), NA_real_)),
      pr_def_rtg  = percent_rank(if_else(off_on_poss >= rank_thresh, coalesce(`Def Rtg Diff`, 999), NA_real_)),

      pr_diff_off_ts   = percent_rank(if_else(off_on_poss >= rank_thresh, off_on_ts - off_off_ts, NA_real_)),
      pr_diff_off_oreb = percent_rank(if_else(off_on_poss >= rank_thresh, off_on_oreb - off_off_oreb, NA_real_)),
      pr_diff_off_ftr  = percent_rank(if_else(off_on_poss >= rank_thresh, off_on_ftr - off_off_ftr, NA_real_)),
      pr_diff_off_tov  = percent_rank(if_else(off_on_poss >= rank_thresh, off_on_tov - off_off_tov, NA_real_)),

      pr_diff_def_ts   = percent_rank(if_else(off_on_poss >= rank_thresh, def_on_ts - def_off_ts, NA_real_)),
      pr_diff_def_oreb = percent_rank(if_else(off_on_poss >= rank_thresh, def_on_oreb - def_off_oreb, NA_real_)),
      pr_diff_def_ftr  = percent_rank(if_else(off_on_poss >= rank_thresh, def_on_ftr - def_off_ftr, NA_real_)),
      pr_diff_def_tov  = percent_rank(if_else(off_on_poss >= rank_thresh, def_on_tov - def_off_tov, NA_real_))
    )

    # Dot position ranks (_rank suffix) for range bar visuals
    raw_cols <- c("off_on_ts", "off_off_ts", "off_on_oreb", "off_off_oreb",
                  "off_on_tov", "off_off_tov", "off_on_ftr", "off_off_ftr",
                  "def_on_ts", "def_off_ts", "def_on_oreb", "def_off_oreb",
                  "def_on_tov", "def_off_tov", "def_on_ftr", "def_off_ftr")
    for (col in intersect(raw_cols, names(df))) {
      vals <- if_else(df$off_on_poss >= rank_thresh, coalesce(df[[col]], 0), NA_real_)
      df[[paste0(col, "_rank")]] <- percent_rank(vals) * 100
    }

    df
  })

  # --- Final Switcher ---
  result_df <- reactive({
    mode <- input$onoff_view_mode
    if (identical(mode, "Four Factors")) {

      df <- ff_ranked_df()

      # Filter LOCALLY (ranks already computed on full data)
      tids <- selected_team_ids()
      if (!is.null(tids) && length(tids) > 0) {
        df <- df %>% filter(team_id %in% !!tids)
      }
      df <- df %>% filter(off_on_poss >= !!input$min_on_poss)

      return(df)

    } else {
      # Summary Mode
      if (isTRUE(fallback_needed())) {
        return(live_result_df())
      } else {
        # Standard View = Use MV
        df <- mv_result_df()

        # --- FILTERING for Summary (Local) ---
        tids_names <- input$teams
        if (!is.null(tids_names) && length(tids_names) > 0) {
          df <- df %>% filter(Team %in% tids_names)
        }

        # Filter Min Poss (Summary MV uses 'ON Poss')
        df <- df %>% filter(`ON Poss` >= !!input$min_on_poss)

        return(df)
      }
    }
  }) %>% bindEvent(debounced_range(), debounced_teams(), debounced_on_filters(), gn_params(), input$min_all_poss, input$min_on_poss, input$game_year, input$onoff_view_mode)

  # --- Render Table ---
  output$onoff_dt <- renderDT({
    df <- result_df()
    mode <- input$onoff_view_mode

    # Standard Name Cleanup
    if (!"Player" %in% names(df) && all(c("First Name", "Last Name") %in% names(df))) {
      df <- df %>% mutate(Player = paste(`First Name`, `Last Name`))
    } else if (!"Player" %in% names(df) && all(c("firstname", "lastname") %in% names(df))) {
      df <- df %>% mutate(Player = paste(firstname, lastname))
    }
    if ("team_name" %in% names(df)) df <- df %>% rename(Team = team_name)

    if (identical(mode, "Summary")) {
      # Shooting split column names (16 raw + 4 display)
      shot_raw_cols <- c(
        "off_on_fg2_made", "off_on_fg2_att", "off_on_fg3_made", "off_on_fg3_att",
        "off_off_fg2_made", "off_off_fg2_att", "off_off_fg3_made", "off_off_fg3_att",
        "def_on_fg2_made", "def_on_fg2_att", "def_on_fg3_made", "def_on_fg3_att",
        "def_off_fg2_made", "def_off_fg2_att", "def_off_fg3_made", "def_off_fg3_att"
      )
      shot_display_cols <- c("Off Shot ON", "Def Shot ON", "Off Shot OFF", "Def Shot OFF")

      # Create display columns (sortable value = total FGA)
      has_shots <- all(c("off_on_fg2_att", "off_on_fg3_att") %in% names(df))
      if (has_shots) {
        df <- df %>% mutate(
          `Off Shot ON`  = coalesce(off_on_fg2_att, 0L) + coalesce(off_on_fg3_att, 0L),
          `Def Shot ON`  = coalesce(def_on_fg2_att, 0L) + coalesce(def_on_fg3_att, 0L),
          `Off Shot OFF` = coalesce(off_off_fg2_att, 0L) + coalesce(off_off_fg3_att, 0L),
          `Def Shot OFF` = coalesce(def_off_fg2_att, 0L) + coalesce(def_off_fg3_att, 0L)
        )
      }

      keep_cols <- c(
        "Team", "Player",
        "Net RTG Diff", "Off ON Diff", "Def ON Diff",
        "Off ON PPP", "Def ON PPP", "On Net RTG", "Off Shot ON", "Def Shot ON",
        "Off OFF PPP", "Def OFF PPP", "Off Net RTG", "Off Shot OFF", "Def Shot OFF",
        "ON Poss", "OFF Poss",
        shot_raw_cols,
        "pr_net", "pr_off_on_d", "pr_def_on_d", "pr_off_on", "pr_def_on_inv", "pr_on_net", "pr_off_off", "pr_def_off_inv", "pr_off_net", "pr_def_on_d_inv"
      )
      df <- df[, intersect(keep_cols, names(df))]

      idx_net <- which(names(df) == "Net RTG Diff") - 1
      idx_on  <- which(names(df) == "Off ON PPP") - 1
      idx_off <- which(names(df) == "Off OFF PPP") - 1
      idx_use <- which(names(df) == "ON Poss") - 1

      diff_cols <- c("Net RTG Diff", "Off ON Diff", "Def ON Diff", "On Net RTG", "Off Net RTG")
      idx_diff <- which(names(df) %in% diff_cols) - 1

      pr_cols <- names(df)[grep("^pr_", names(df))]
      hide_idx <- which(names(df) %in% c(pr_cols, shot_raw_cols)) - 1

      # Shooting column JS render function factory
      make_shot_render <- function(fg2m_col, fg2a_col, fg3m_col, fg3a_col,
                                   is_defense = FALSE, min_fga = 50, avg2 = 53, avg3 = 34) {
        fg2m_idx <- which(names(df) == fg2m_col) - 1
        fg2a_idx <- which(names(df) == fg2a_col) - 1
        fg3m_idx <- which(names(df) == fg3m_col) - 1
        fg3a_idx <- which(names(df) == fg3a_col) - 1
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

      # Build shot column defs with dynamic thresholds
      shot_col_defs <- list()
      if (has_shots) {
        shot_col_map <- list(
          "Off Shot ON"  = c("off_on_fg2_made", "off_on_fg2_att", "off_on_fg3_made", "off_on_fg3_att"),
          "Def Shot ON"  = c("def_on_fg2_made", "def_on_fg2_att", "def_on_fg3_made", "def_on_fg3_att"),
          "Off Shot OFF" = c("off_off_fg2_made", "off_off_fg2_att", "off_off_fg3_made", "off_off_fg3_att"),
          "Def Shot OFF" = c("def_off_fg2_made", "def_off_fg2_att", "def_off_fg3_made", "def_off_fg3_att")
        )
        # Compute per-column weighted averages from qualifying players (>= 50 FGA)
        SHOT_MIN_FGA <- 50L
        shot_avgs <- list()
        for (dn in names(shot_col_map)) {
          cols <- shot_col_map[[dn]]
          fga <- df[[dn]]
          qual <- if (is.null(fga)) rep(FALSE, nrow(df)) else (!is.na(fga) & fga >= SHOT_MIN_FGA)
          fg2a_sum <- sum(df[[cols[2]]][qual], na.rm = TRUE)
          fg3a_sum <- sum(df[[cols[4]]][qual], na.rm = TRUE)
          a2 <- if (fg2a_sum > 0) as.integer(round(sum(df[[cols[1]]][qual], na.rm = TRUE) / fg2a_sum * 100)) else 53L
          a3 <- if (fg3a_sum > 0) as.integer(round(sum(df[[cols[3]]][qual], na.rm = TRUE) / fg3a_sum * 100)) else 34L
          shot_avgs[[dn]] <- list(avg2 = a2, avg3 = a3)
        }
        for (disp_name in names(shot_col_map)) {
          cols <- shot_col_map[[disp_name]]
          target_idx <- which(names(df) == disp_name) - 1
          is_def <- grepl("^Def", disp_name)
          avgs <- shot_avgs[[disp_name]]
          if (length(target_idx) && all(cols %in% names(df))) {
            shot_col_defs[[length(shot_col_defs) + 1]] <- list(
              targets = target_idx,
              render = make_shot_render(cols[1], cols[2], cols[3], cols[4],
                                        is_defense = is_def, min_fga = SHOT_MIN_FGA,
                                        avg2 = avgs$avg2, avg3 = avgs$avg3)
            )
          }
        }
      }

      # Section border indices for shooting columns
      idx_shot_on  <- which(names(df) == "Off Shot ON") - 1
      idx_shot_off <- which(names(df) == "Off Shot OFF") - 1
      section_borders <- c(idx_net, idx_on, idx_off, idx_use)
      # Don't add shot borders — they sit inside on/off court groups

      # Header: On Court = Off PPP, Def PPP, Net Rtg, Off Shot, Def Shot (5 cols)
      # Off Court = Off PPP, Def PPP, Net Rtg, Off Shot, Def Shot (5 cols)
      sketch_summary <- htmltools::withTags(table(class = 'display', thead(
        tr(
          th(class="group-head", colspan=2, ""),
          th(class="group-head section-left-border", colspan=3, "Net Impact"),
          th(class="group-head section-left-border", colspan=5, "On Court Stats"),
          th(class="group-head section-left-border", colspan=5, "Off Court Stats"),
          th(class="group-head section-left-border", colspan=2, "Usage")
        ),
        tr(
          th(class="sub-head", "Team"), th(class="sub-head", "Player"),
          th(class="sub-head section-left-border", "Net"), th(class="sub-head", "Off"), th(class="sub-head", "Def"),
          th(class="sub-head section-left-border", "Off PPP"), th(class="sub-head", "Def PPP"), th(class="sub-head", "Net Rtg"), th(class="sub-head", "Off Shot"), th(class="sub-head", "Def Shot"),
          th(class="sub-head section-left-border", "Off PPP"), th(class="sub-head", "Def PPP"), th(class="sub-head", "Net Rtg"), th(class="sub-head", "Off Shot"), th(class="sub-head", "Def Shot"),
          th(class="sub-head section-left-border", "On Poss"), th(class="sub-head", "Off Poss")
        )
      )))

      dt <- datatable(df, container = sketch_summary, rownames = FALSE,
                      options = list(dom = "tip", pageLength = 30, scrollX = TRUE,
                                     scrollY = "70vh", scrollCollapse = TRUE,
                                     order = list(list(which(names(df) == "Net RTG Diff") - 1, "desc")),
                                     columnDefs = c(list(
                                       list(targets = section_borders, className = "section-left-border"),
                                       list(targets = hide_idx, visible = FALSE),
                                       list(targets = "_all", className = "dt-center"),
                                       list(targets = idx_diff, render = DT::JS(
                                         "function(data, type, row, meta) {",
                                         "  if (type !== 'display' || data === null) return data;",
                                         "  var val = parseFloat(data);",
                                         "  if (isNaN(val)) return data;",
                                         "  var formatted = val.toFixed(2);",
                                         "  return val > 0 ? '+' + formatted : formatted;",
                                         "}"
                                       ))
                                     ), shot_col_defs))) |>
        formatRound(c("Off ON PPP", "Def ON PPP", "Off OFF PPP", "Def OFF PPP"), 1) |>
        formatCurrency(c("ON Poss", "OFF Poss"), currency = "", interval = 3, mark = ",", digits = 0)

      if("pr_net" %in% names(df)) dt <- formatStyle(dt, "Net RTG Diff", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_net")
      if("pr_off_on_d" %in% names(df)) dt <- formatStyle(dt, "Off ON Diff", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_on_d")
      if("pr_def_on_d" %in% names(df)) dt <- formatStyle(dt, "Def ON Diff", backgroundColor = styleInterval(CUTS, COLS_REV), valueColumns = "pr_def_on_d")

      if("pr_off_on" %in% names(df)) dt <- formatStyle(dt, "Off ON PPP", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_on")
      if("pr_def_on_inv" %in% names(df)) dt <- formatStyle(dt, "Def ON PPP", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_def_on_inv")
      if("pr_on_net" %in% names(df)) dt <- formatStyle(dt, "On Net RTG", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_on_net")

      if("pr_off_off" %in% names(df)) dt <- formatStyle(dt, "Off OFF PPP", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_off")
      if("pr_def_off_inv" %in% names(df)) dt <- formatStyle(dt, "Def OFF PPP", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_def_off_inv")
      if("pr_off_net" %in% names(df)) dt <- formatStyle(dt, "Off Net RTG", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_net")

      return(dt)

    } else {
      # === MODE 2: FOUR FACTORS ===

      metric_map <- list(
        "Off TS% Diff"   = c("off_on_ts", "off_off_ts"),
        "Off OREB% Diff" = c("off_on_oreb", "off_off_oreb"),
        "Off TOV% Diff"  = c("off_on_tov", "off_off_tov"),
        "Off FTR Diff"   = c("off_on_ftr", "off_off_ftr"),
        "Def TS% Diff"   = c("def_on_ts", "def_off_ts"),
        "Def OREB% Diff" = c("def_on_oreb", "def_off_oreb"),
        "Def TOV% Diff"  = c("def_on_tov", "def_off_tov"),
        "Def FTR Diff"   = c("def_on_ftr", "def_off_ftr")
      )

      raw_cols_all <- unique(unlist(metric_map))

      # Rounding
      df <- df %>% mutate(across(all_of(intersect(raw_cols_all, names(df))), ~ round(as.numeric(.) * 100, 1)))
      df <- df %>% mutate(across(all_of(intersect(names(metric_map), names(df))), ~ round(as.numeric(.), 1)))

      # Dot position ranks (_rank columns) already computed in ff_ranked_df()

      # Rename poss columns for display
      df <- df %>% rename(`ON Poss` = off_on_poss, `OFF Poss` = off_off_poss)

      # 3. SELECT & ORDER COLUMNS
      vis_cols <- c("Team", "Player", "Net Diff", "Off Rtg Diff", "Def Rtg Diff", intersect(names(metric_map), names(df)), "ON Poss", "OFF Poss")

      rank_cols <- intersect(c(
        "pr_net_diff", "pr_off_rtg", "pr_def_rtg",
        "pr_diff_off_ts", "pr_diff_off_oreb", "pr_diff_off_tov", "pr_diff_off_ftr",
        "pr_diff_def_ts", "pr_diff_def_oreb", "pr_diff_def_tov", "pr_diff_def_ftr"
      ), names(df))

      df_final <- df %>% select(all_of(vis_cols), any_of(rank_cols), ends_with("_rank"), all_of(raw_cols_all))

      final_vis_order <- c(
        "Team", "Player", "Net Diff",
        "Off Rtg Diff", "Off TS% Diff", "Off OREB% Diff", "Off TOV% Diff", "Off FTR Diff",
        "Def Rtg Diff", "Def TS% Diff", "Def OREB% Diff", "Def TOV% Diff", "Def FTR Diff",
        "ON Poss", "OFF Poss"
      )

      final_vis_order <- intersect(final_vis_order, names(df_final))
      final_col_order <- c(final_vis_order, setdiff(names(df_final), final_vis_order))
      df_final <- df_final %>% select(all_of(final_col_order))

      defs <- list()

      for (i in seq_along(metric_map)) {
        diff_name <- names(metric_map)[i]
        if (!diff_name %in% names(df_final)) next
        target_idx <- which(names(df_final) == diff_name) - 1L

        on_col <- metric_map[[i]][1]
        off_col <- metric_map[[i]][2]

        if (on_col %in% names(df_final) && off_col %in% names(df_final)) {
          on_val_idx <- which(names(df_final) == on_col) - 1L
          off_val_idx <- which(names(df_final) == off_col) - 1L
          on_rank_idx <- which(names(df_final) == paste0(on_col, "_rank")) - 1L
          off_rank_idx <- which(names(df_final) == paste0(off_col, "_rank")) - 1L

          js_func <- JS(sprintf(
            "function(data, type, row, meta) {
               if (type === 'display') {
                 var diffVal = (data === null) ? '-' : (parseFloat(data) > 0 ? '+' + data : data);
                 var onVal   = row[%d] || '-';
                 var offVal  = row[%d] || '-';
                 var onPct   = row[%d];
                 var offPct  = row[%d];

                 if (onPct === null || onPct === undefined) {
                    return '<div class=\"diff-val unranked\">' + diffVal + '</div>' +
                           '<div class=\"rank-bar-container hidden\"></div>' +
                           '<div class=\"sub-text\" style=\"opacity:0.5;\">' + onVal + ' | ' + offVal + '</div>';
                 }

                 var rangeLineLeft  = Math.min(onPct, offPct);
                 var rangeLineWidth = Math.abs(onPct - offPct);

                 return '<div class=\"diff-val\">' + diffVal + '</div>' +
                        '<div class=\"rank-bar-container\">' +
                          '<div class=\"rank-track\"></div>' +
                          '<div class=\"range-connect\" style=\"left:' + rangeLineLeft + '%%; width:' + rangeLineWidth + '%%;\"></div>' +
                          '<div class=\"dot-off\" style=\"left:' + offPct + '%%;\" title=\"Off: ' + offVal + '\"></div>' +
                          '<div class=\"dot-on\" style=\"left:' + onPct + '%%;\" title=\"On: ' + onVal + '\"></div>' +
                        '</div>' +
                        '<div class=\"sub-text\">' +
                          '<span style=\"font-weight:700; color:#222;\">' + onVal + '</span>' +
                          ' <span style=\"opacity:0.6;\">|</span> ' +
                          '<span style=\"color:#666;\">' + offVal + '</span>' +
                        '</div>';
               }
               return data;
             }", on_val_idx, off_val_idx, on_rank_idx, off_rank_idx
          ))
          defs[[length(defs) + 1]] <- list(targets = target_idx, render = js_func)
        }
      }

      # Hide auxiliary columns
      hide_cols <- c(rank_cols, raw_cols_all, names(df)[grep("_rank$", names(df))])
      hide_idx <- which(names(df_final) %in% hide_cols) - 1L
      if (length(hide_idx)) defs[[length(defs) + 1]] <- list(targets = hide_idx, visible = FALSE)

      # --- SEPARATORS (Thick borders for 3 sections) ---

      off_rtg_idx <- which(names(df_final) == "Off Rtg Diff") - 1L
      if(length(off_rtg_idx)) defs[[length(defs) + 1]] <- list(targets = off_rtg_idx, className = "section-left-border")

      def_rtg_idx <- which(names(df_final) == "Def Rtg Diff") - 1L
      if(length(def_rtg_idx)) defs[[length(defs) + 1]] <- list(targets = def_rtg_idx, className = "section-left-border")

      on_poss_idx <- which(names(df_final) == "ON Poss") - 1L
      if(length(on_poss_idx)) defs[[length(defs) + 1]] <- list(targets = on_poss_idx, className = "section-left-border")

      # Net Diff Style
      net_diff_idx <- which(names(df_final) == "Net Diff") - 1L
      if(length(net_diff_idx)) {
        defs[[length(defs) + 1]] <- list(targets = net_diff_idx, className = "dt-center",
                                         render = JS("function(data, type, row) {
                                            if(type === 'display') {
                                              var v = (data !== null && parseFloat(data) > 0) ? '+' + data : data;
                                              return '<div style=\"font-weight:800; font-size:1.05em;\">' + v + '</div>';
                                            }
                                            return data;
                                         }"))
      }

      # '+' sign for Off Rtg Diff and Def Rtg Diff
      plus_sign_js <- JS(
        "function(data, type, row, meta) {",
        "  if (type !== 'display' || data === null) return data;",
        "  var val = parseFloat(data);",
        "  if (isNaN(val)) return data;",
        "  return val > 0 ? '+' + data : data;",
        "}"
      )
      off_rtg_diff_idx <- which(names(df_final) == "Off Rtg Diff") - 1L
      def_rtg_diff_idx <- which(names(df_final) == "Def Rtg Diff") - 1L
      rtg_diff_idx <- c(off_rtg_diff_idx, def_rtg_diff_idx)
      if (length(rtg_diff_idx)) defs[[length(defs) + 1]] <- list(targets = rtg_diff_idx, render = plus_sign_js)

      defs[[length(defs) + 1]] <- list(targets = "_all", className = "dt-center")

      sketch_ff <- htmltools::withTags(table(class = 'display', thead(
        tr(
          th(class = "group-head", colspan = 2, ""),
          th(class = "group-head", "Total"),
          th(class = "group-head section-left-border", colspan = 5, "Offense Impact (On-Off)"),
          th(class = "group-head section-left-border", colspan = 5, "Defense Impact (On-Off)"),
          th(class = "group-head section-left-border", colspan = 2, "Usage")
        ),
        tr(
          th(class = "sub-head", "Team"), th(class = "sub-head", "Player"),
          th(class = "sub-head", "Diff"),
          th(class = "sub-head section-left-border", "Diff"), th(class = "sub-head", "TS%"), th(class = "sub-head", "OREB%"), th(class = "sub-head", "TOV%"), th(class = "sub-head", "FTR"),
          th(class = "sub-head section-left-border", "Diff"), th(class = "sub-head", "TS%"), th(class = "sub-head", "OREB%"), th(class = "sub-head", "TOV%"), th(class = "sub-head", "FTR"),
          th(class = "sub-head section-left-border", "On Poss"), th(class = "sub-head", "Off Poss")
        )
      )))

      dt <- datatable(df_final,
                      container = sketch_ff, rownames = FALSE, escape = FALSE,
                      options = list(
                        dom = "t", pageLength = 50, deferRender = TRUE, scrollX = TRUE,
                        scrollY = "70vh", scrollCollapse = TRUE,
                        order = list(list(2, "desc")),
                        columnDefs = defs
                      )
      )

      # --- FORMAT POSS COLUMNS ---
      dt <- formatCurrency(dt, c("ON Poss", "OFF Poss"), currency = "", interval = 3, mark = ",", digits = 0)

      # --- COLOR LOGIC ---
      if ("pr_net_diff" %in% names(df_final)) dt <- formatStyle(dt, "Net Diff", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_net_diff")

      # Offense Ratings (High Diff = Good)
      if ("pr_off_rtg" %in% names(df_final)) dt <- formatStyle(dt, "Off Rtg Diff", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_rtg")

      # Defense Ratings (High Diff = Bad -> Reverse)
      if ("pr_def_rtg" %in% names(df_final)) dt <- formatStyle(dt, "Def Rtg Diff", backgroundColor = styleInterval(CUTS, COLS_REV), valueColumns = "pr_def_rtg")

      # Offense Factors
      if ("pr_diff_off_ts" %in% names(df_final)) dt <- formatStyle(dt, "Off TS% Diff", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_diff_off_ts")
      if ("pr_diff_off_oreb" %in% names(df_final)) dt <- formatStyle(dt, "Off OREB% Diff", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_diff_off_oreb")
      if ("pr_diff_off_ftr" %in% names(df_final)) dt <- formatStyle(dt, "Off FTR Diff", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_diff_off_ftr")
      if ("pr_diff_off_tov" %in% names(df_final)) dt <- formatStyle(dt, "Off TOV% Diff", backgroundColor = styleInterval(CUTS, COLS_REV), valueColumns = "pr_diff_off_tov")

      # Defense Factors
      if ("pr_diff_def_ts" %in% names(df_final)) dt <- formatStyle(dt, "Def TS% Diff", backgroundColor = styleInterval(CUTS, COLS_REV), valueColumns = "pr_diff_def_ts")
      if ("pr_diff_def_oreb" %in% names(df_final)) dt <- formatStyle(dt, "Def OREB% Diff", backgroundColor = styleInterval(CUTS, COLS_REV), valueColumns = "pr_diff_def_oreb")
      if ("pr_diff_def_ftr" %in% names(df_final)) dt <- formatStyle(dt, "Def FTR Diff", backgroundColor = styleInterval(CUTS, COLS_REV), valueColumns = "pr_diff_def_ftr")
      if ("pr_diff_def_tov" %in% names(df_final)) dt <- formatStyle(dt, "Def TOV% Diff", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_diff_def_tov")

      return(dt)
    }
  }) %>% bindEvent(debounced_range(), debounced_teams(), debounced_on_filters(), gn_params(), input$min_all_poss, input$min_on_poss, input$game_year, input$onoff_view_mode)
}
