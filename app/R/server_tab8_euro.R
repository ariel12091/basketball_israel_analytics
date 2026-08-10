# server_tab8_euro.R - Tab 8: EuroLeague On/Off Impact server logic
# Derived from server_tab1.R. Summary + Four Factors only (no Shot
# Profile); reads euroleague.* via migration 004.

# The Summary and Four Factors filter menus live in helpers.R as
# ONOFF_SUMMARY_FILTERABLE_COLS / ONOFF_FF_FILTERABLE_COLS, shared with the
# other league's on/off tab.

server_tab8_euro <- function(input, output, session, shared) {
  auto_min_state <- reactiveValues(
    last_auto = NA_integer_,
    last_auto_all = NA_integer_,
    updating = FALSE
  )
  auto_enabled <- reactiveVal(TRUE)
  resetting <- reactiveVal(FALSE)
  euro_stat_filter_state <- make_stat_filter_state()

  # Competition and season come from the section-wide navbar selectors, shared
  # with every other EuroLeague tab (populated once in app.R). They are NOT the
  # navbar's input$game_year, which is the Israeli season-ending year
  # (2026 = 2025-26) while EuroLeague uses the provider season (2025 = 2025-26).
  # One selector value must never mean two seasons.
  euro_competition <- reactive(euro_selected_competition(input))
  euro_selected_season <- reactive(euro_selected_game_year(input))
  euro_teams_df <- reactive({
    euro_fetch_teams(euro_competition(), euro_selected_season())
  })

  # Teams / opponents / phase dropdowns follow competition + season.
  observeEvent(list(euro_competition(), euro_selected_season()), {
    td <- euro_teams_df()
    choices <- if (!is.null(td) && nrow(td)) {
      stats::setNames(as.character(td$team_id), as.character(td$team_name))
    } else {
      character(0)
    }
    updateSelectizeInput(session, "euro_teams", choices = choices,
                         selected = character(0), server = TRUE)
    updateSelectizeInput(session, "euro_opponents", choices = choices,
                         selected = character(0), server = TRUE)

    ph <- tryCatch(euro_fetch_phases(euro_competition(), euro_selected_season()),
                   error = function(e) NULL)
    ph_vals <- if (!is.null(ph) && nrow(ph)) as.character(ph$phase) else character(0)
    updateSelectizeInput(session, "euro_phase",
                         choices = stats::setNames(ph_vals, euro_phase_label(ph_vals)),
                         selected = character(0))
  }, ignoreInit = FALSE)

  euro_stat_filter_cols <- reactive({
    switch(input$euro_view_mode %||% "Summary",
      "Four Factors" = ONOFF_FF_FILTERABLE_COLS,
      ONOFF_SUMMARY_FILTERABLE_COLS
    )
  })

  setup_stat_filter_handlers("euro", input, session, euro_stat_filter_cols, euro_stat_filter_state)

  # AUTO_TOP_PCT and the auto-min helpers (auto_min_on_from_df,
  # auto_min_all_from_df, resolve_poss_cols) now live in helpers.R,
  # shared with the other league's on/off tab.
  # ======== On/Off tab Logic ===================================
  observeEvent(euro_selected_season(), {
    bounds <- euro_season_date_bounds(euro_selected_season())
    updateDateRangeInput(session, "euro_date_range",
                         start = bounds$start, end = bounds$end,
                         min = bounds$start, max = bounds$end)

    # GN here means ROUND number, not gamecode -- a gamecode range would mean
    # "league games 5-10", not "rounds 5-10".
    gn_df <- euro_fetch_round_values(euro_competition(), euro_selected_season())
    gn_vals <- if (!is.null(gn_df) && nrow(gn_df)) as.integer(gn_df$gn) else integer(0)
    update_gn_last_n_choices(session, "euro", gn_vals)
  }, ignoreInit = FALSE)


  # --- Reset Logic ---
  observeEvent(input$euro_reset_defaults, {
    resetting(TRUE)
    # Reset to the newest loaded season for this competition, not to the
    # Israeli DEFAULT_GAME_YEAR, which is a different numbering entirely.
    seasons <- tryCatch(euro_fetch_seasons(euro_competition()), error = function(e) NULL)
    default_season <- if (!is.null(seasons) && nrow(seasons)) {
      as.character(seasons$game_year[[1]])
    } else {
      EURO_DEFAULT_SEASON
    }
    updateSelectInput(session, "euro_game_year", selected = default_season)
    bounds <- euro_season_date_bounds(default_season)
    updateDateRangeInput(session, "euro_date_range",
                         start = bounds$start, end = bounds$end,
                         min = bounds$start, max = bounds$end)
    updateSelectizeInput(session, "euro_phase", selected = character(0))
    updateSelectizeInput(session, "euro_opponents", selected = character(0))
    updateSelectInput(session, "euro_home_away", selected = "")
    updateSelectInput(session, "euro_outcome", selected = "")
    updateSelectInput(session, "euro_opp_rank_side", selected = "")
    updateSelectInput(session, "euro_opp_rank_n", selected = "")
    updateSelectInput(session, "euro_opp_rank_metric", selected = "")
    updateSelectInput(session, "euro_num_starters_off_mode", selected = "")
    updateSelectInput(session, "euro_num_starters_off", selected = "")
    updateSelectInput(session, "euro_num_starters_def_mode", selected = "")
    updateSelectInput(session, "euro_num_starters_def", selected = "")
    updateSliderInput(session, "euro_min_all_poss", value = DEFAULT_MIN_ALL)
    updateSliderInput(session, "euro_min_on_poss", value = DEFAULT_MIN_ON)
    updateSelectizeInput(session, "euro_gn_min", selected = "")
    updateSelectizeInput(session, "euro_gn_max", selected = "")
    updateSelectizeInput(session, "euro_last_n", selected = "")
    reset_stat_filters(euro_stat_filter_state)
    auto_min_state$last_auto <- as.integer(DEFAULT_MIN_ON)
    auto_min_state$last_auto_all <- as.integer(DEFAULT_MIN_ALL)
    auto_enabled(FALSE)
    # Clear teams
    updateSelectizeInput(session, "euro_teams", selected = character(0))
    session$onFlushed(function() resetting(FALSE), once = TRUE)
  })

  observeEvent(input$euro_view_mode, {
    reset_stat_filters(euro_stat_filter_state)
  }, ignoreInit = TRUE)

  debounced_range <- reactive(input$euro_date_range) %>% debounce(300)
  debounced_teams <- reactive(input$euro_teams) %>% debounce(300)
  debounced_on_filters <- reactive(list(
    game_type = input$euro_phase,
    opp_ids = input$euro_opponents,
    home_away = input$euro_home_away,
    outcome = input$euro_outcome,
    rank_side = input$euro_opp_rank_side,
    rank_n = input$euro_opp_rank_n,
    metric = input$euro_opp_rank_metric,
    num_starters_off_mode = input$euro_num_starters_off_mode,
    num_starters_off = input$euro_num_starters_off,
    num_starters_def_mode = input$euro_num_starters_def_mode,
    num_starters_def = input$euro_num_starters_def
  )) %>% debounce(300)

  # Same GN/last-N resolution as every other tab; "GN" here is the round
  # number, which is what euro_fetch_round_values() populates the choices from.
  gn_params <- reactive(resolve_gn_last_n_params(input, "euro")) %>% debounce(150)

  build_onoff_db_args <- function() {
    f <- debounced_on_filters()
    gp <- gn_params()

    phase_csv <- csv_if_any(f$game_type)
    opp_ids_csv <- csv_if_any(input$euro_opponents)

    home_away <- blank_to_na_character(f$home_away)
    outcome <- blank_to_na_character(f$outcome)
    opp_rank_side <- blank_to_na_character(f$rank_side)
    opp_rank_n <- blank_to_na_integer(f$rank_n)
    opp_rank_metric <- blank_to_na_character(f$metric)
    starters <- resolve_starters_bounds(
      off_mode = f$num_starters_off_mode,
      off_val = f$num_starters_off,
      def_mode = f$num_starters_def_mode,
      def_val = f$num_starters_def
    )

    list(
      phase_csv = phase_csv,
      opp_ids_csv = opp_ids_csv,
      home_away = home_away,
      outcome = outcome,
      opp_rank_side = opp_rank_side,
      opp_rank_n = opp_rank_n,
      opp_rank_metric = opp_rank_metric,
      min_gn = gp$min_gn,
      max_gn = gp$max_gn,
      last_n_games = gp$last_n,
      num_starters_off_min = starters$num_starters_off_min,
      num_starters_off_max = starters$num_starters_off_max,
      num_starters_def_min = starters$num_starters_def_min,
      num_starters_def_max = starters$num_starters_def_max
    )
  }

  observeEvent(input$euro_min_on_poss, {
    if (isTRUE(auto_min_state$updating)) return(invisible(NULL))
    cur_val <- as.integer(input$euro_min_on_poss)
    last_auto <- as.integer(auto_min_state$last_auto)
    if (!is.na(cur_val) && !is.na(last_auto) && cur_val == last_auto) {
      return(invisible(NULL))
    }
    auto_enabled(FALSE)
  }, ignoreInit = TRUE)

  observeEvent(input$euro_min_all_poss, {
    if (isTRUE(auto_min_state$updating)) return(invisible(NULL))
    cur_val <- as.integer(input$euro_min_all_poss)
    last_auto <- as.integer(auto_min_state$last_auto_all)
    if (!is.na(cur_val) && !is.na(last_auto) && cur_val == last_auto) {
      return(invisible(NULL))
    }
    auto_enabled(FALSE)
  }, ignoreInit = TRUE)

  observeEvent(list(debounced_range(), debounced_teams(), debounced_on_filters(),
                    gn_params(), input$euro_game_year, input$euro_view_mode), {
    if (isTRUE(resetting())) return(invisible(NULL))
    auto_enabled(TRUE)
  }, ignoreInit = TRUE)

  observeEvent(list(debounced_range(), debounced_teams(), debounced_on_filters(),
                    gn_params(), input$euro_game_year, input$euro_view_mode, input$euro_min_all_poss), {
    if (!isTRUE(auto_enabled())) return(invisible(NULL))

    mode <- input$euro_view_mode

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
        gy <- euro_selected_season()
        db_args <- build_onoff_db_args()

        df_base <- run_onoff_compute_14(
          pg_pool,
          start_d = as.Date(rng[1]), end_d = as.Date(rng[2]),
          team_ids = tids, min_all = 0L, min_on = 0L,
          min_net = DEFAULT_MIN_NET, game_year = gy,
          phase_csv = db_args$phase_csv, opp_ids_csv = db_args$opp_ids_csv,
          home_away = db_args$home_away, outcome = db_args$outcome,
          opp_rank_side = db_args$opp_rank_side, opp_rank_n = db_args$opp_rank_n, opp_rank_metric = db_args$opp_rank_metric,
          min_gn = db_args$min_gn, max_gn = db_args$max_gn, last_n_games = db_args$last_n_games,
          num_starters_off_min = db_args$num_starters_off_min, num_starters_off_max = db_args$num_starters_off_max,
          num_starters_def_min = db_args$num_starters_def_min, num_starters_def_max = db_args$num_starters_def_max
        )
      } else {
        df_base <- mv_result_df()
        tids <- selected_team_ids()
        if (!is.null(tids) && length(tids) > 0) {
          df_base <- df_base %>% filter(team_id %in% !!tids)
        }
      }
    }

    poss_cols <- resolve_poss_cols(df_base, mode)
    if (is.na(poss_cols$on)) return(invisible(NULL))
    min_needed <- auto_min_on_from_df(df_base, usage_col = poss_cols$on, step = 10L)
    cur_val <- as.integer(input$euro_min_on_poss)
    if (is.na(min_needed) || is.na(cur_val)) return(invisible(NULL))
    if (cur_val <= min_needed) return(invisible(NULL))

    auto_min_state$updating <- TRUE
    updateSliderInput(session, "euro_min_on_poss", value = min_needed)
    auto_min_state$updating <- FALSE
    auto_min_state$last_auto <- min_needed
  }, ignoreInit = TRUE)

  observeEvent(list(debounced_range(), debounced_teams(), debounced_on_filters(),
                    gn_params(), input$euro_game_year, input$euro_view_mode, input$euro_min_on_poss), {
    if (!isTRUE(auto_enabled())) return(invisible(NULL))

    mode <- input$euro_view_mode
    df_base <- NULL
    if (identical(mode, "Four Factors")) {
      df_base <- ff_ranked_df()
      tids <- selected_team_ids()
      if (!is.null(tids) && length(tids) > 0) df_base <- df_base %>% filter(team_id %in% !!tids)
      if ("off_on_poss" %in% names(df_base)) {
        df_base <- df_base %>% filter(off_on_poss >= !!input$euro_min_on_poss)
      }
    } else {
      if (isTRUE(fallback_needed())) {
        rng <- debounced_range()
        req(rng)
        tids <- selected_team_ids()
        gy <- euro_selected_season()
        db_args <- build_onoff_db_args()

        df_base <- run_onoff_compute_14(
          pg_pool,
          start_d = as.Date(rng[1]), end_d = as.Date(rng[2]),
          team_ids = tids, min_all = 0L, min_on = 0L,
          min_net = DEFAULT_MIN_NET, game_year = gy,
          phase_csv = db_args$phase_csv, opp_ids_csv = db_args$opp_ids_csv,
          home_away = db_args$home_away, outcome = db_args$outcome,
          opp_rank_side = db_args$opp_rank_side, opp_rank_n = db_args$opp_rank_n, opp_rank_metric = db_args$opp_rank_metric,
          min_gn = db_args$min_gn, max_gn = db_args$max_gn, last_n_games = db_args$last_n_games,
          num_starters_off_min = db_args$num_starters_off_min, num_starters_off_max = db_args$num_starters_off_max,
          num_starters_def_min = db_args$num_starters_def_min, num_starters_def_max = db_args$num_starters_def_max
        )
      } else {
        df_base <- mv_result_df()
        tids <- selected_team_ids()
        if (!is.null(tids) && length(tids) > 0) {
          df_base <- df_base %>% filter(team_id %in% !!tids)
        }
      }
    }

    poss_cols <- resolve_poss_cols(df_base, mode)
    if (is.na(poss_cols$on) || is.na(poss_cols$off)) return(invisible(NULL))

    min_needed <- auto_min_all_from_df(df_base, usage_col = poss_cols$on, on_col = poss_cols$on, off_col = poss_cols$off, step = 10L)
    cur_val <- as.integer(input$euro_min_all_poss)
    if (is.na(min_needed) || is.na(cur_val)) return(invisible(NULL))
    if (cur_val <= min_needed) return(invisible(NULL))

    auto_min_state$updating <- TRUE
    updateSliderInput(session, "euro_min_all_poss", value = min_needed)
    auto_min_state$updating <- FALSE
    auto_min_state$last_auto_all <- min_needed
  }, ignoreInit = TRUE)

  setup_gn_last_n_sync(session, input, "euro")

  selected_team_ids <- reactive({
    ids <- suppressWarnings(as.integer(debounced_teams()))
    ids <- ids[is.finite(ids)]
    if (length(ids)) ids else NULL
  })

  # --- Fallback Logic ---
  # We do NOT return true if only team/min_poss changed.
  fallback_needed <- reactive({
    rng <- debounced_range()
    if (is.null(rng)) return(FALSE)
    start_d <- as.Date(rng[1])
    end_d <- as.Date(rng[2])
    if (is.na(start_d) || is.na(end_d)) return(FALSE)
    gy <- euro_selected_season()
    season_bounds <- euro_season_date_bounds(gy)

    date_changed <- (start_d != season_bounds$start) || (end_d != season_bounds$end)

    f <- debounced_on_filters()
    extra_filters <- (!is.null(f$game_type) && any(nzchar(f$game_type))) ||
      (!is.null(f$opp_ids) && length(f$opp_ids) > 0) ||
      nzchar(f$home_away %||% "") ||
      nzchar(f$outcome %||% "") ||
      nzchar(f$rank_side %||% "") ||
      (nzchar(f$num_starters_off_mode %||% "") && nzchar(f$num_starters_off %||% "")) ||
      (nzchar(f$num_starters_def_mode %||% "") && nzchar(f$num_starters_def %||% ""))

    gp <- gn_params()
    gn_active <- !is.na(gp$min_gn) || !is.na(gp$max_gn) || !is.na(gp$last_n)
    gn_raw_active <- nzchar(input$euro_gn_min %||% "") ||
      nzchar(input$euro_gn_max %||% "") ||
      nzchar(input$euro_last_n %||% "")
    gn_active <- gn_active || gn_raw_active

    date_changed || extra_filters || gn_active
  })

  # --- On/Off Compute Function ---
  # euroleague.onoff_compute takes 22 params in a different order from the
  # Israeli 23-param version: competition first, season second, and the two
  # legacy scalar starter params do not exist (only the min/max pairs).
  run_onoff_compute_14 <- function(pool, start_d, end_d, team_ids, min_all, min_on, min_net, game_year, phase_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric, min_gn = NA_integer_, max_gn = NA_integer_, last_n_games = NA_integer_, num_starters_off_min = NA_integer_, num_starters_off_max = NA_integer_, num_starters_def_min = NA_integer_, num_starters_def_max = NA_integer_) {
    allowed <- guard_heavy_request(
      session, key = "tab8_euro_onoff_compute",
      start_d = start_d, end_d = end_d,
      min_gn = min_gn, max_gn = max_gn, last_n = last_n_games,
      max_calls = 35L, window_sec = 60L
    )
    if (!isTRUE(allowed)) return(data.frame())
    team_csv <- if (is.null(team_ids) || !length(team_ids)) NA_character_ else paste(team_ids, collapse = ",")
    db_get_query(pool,
                 paste0("SELECT * FROM euroleague.onoff_compute(",
                        "$1::text,$2::int4,$3::date,$4::date,$5::text,$6::text,",
                        "$7::text,$8::text,$9::text,$10::text,$11::int4,$12::text,",
                        "$13::int4,$14::int4,$15::int4,$16::int4,$17::int4,$18::int4,",
                        "$19::int4,$20::numeric,$21::int4,$22::int4",
                        ")"),
                 params = list(euro_competition(), as.integer(game_year),
                               as.Date(start_d), as.Date(end_d), team_csv,
                               phase_csv, opp_ids_csv, home_away, outcome,
                               opp_rank_side, opp_rank_n, opp_rank_metric,
                               min_gn, max_gn, last_n_games,
                               num_starters_off_min, num_starters_off_max,
                               num_starters_def_min, num_starters_def_max,
                               as.numeric(min_net), as.integer(min_all), as.integer(min_on)))
  }

  # --- Four Factors Compute Function ---
  run_four_factors_compute <- function(pool, game_year, start_d, end_d, team_ids,
                                       phase_csv, opp_ids_csv, home_away, outcome,
                                       opp_rank_side, opp_rank_n, opp_rank_metric,
                                       min_gn = NA_integer_, max_gn = NA_integer_, last_n_games = NA_integer_,
                                       num_starters_off_min = NA_integer_, num_starters_off_max = NA_integer_,
                                       num_starters_def_min = NA_integer_, num_starters_def_max = NA_integer_) {
    allowed <- guard_heavy_request(
      session, key = "tab8_euro_ff_compute",
      start_d = start_d, end_d = end_d,
      min_gn = min_gn, max_gn = max_gn, last_n = last_n_games,
      max_calls = 35L, window_sec = 60L
    )
    if (!isTRUE(allowed)) return(data.frame())
    team_csv <- if (is.null(team_ids) || !length(team_ids)) NA_character_ else paste(team_ids, collapse = ",")
    db_get_query(pool,
                 paste0("SELECT * FROM euroleague.four_factors_compute(",
                        "$1::text,$2::int4,$3::date,$4::date,$5::text,$6::text,",
                        "$7::text,$8::text,$9::text,$10::text,$11::int4,$12::text,",
                        "$13::int4,$14::int4,$15::int4,$16::int4,$17::int4,$18::int4,$19::int4",
                        ")"),
                 params = list(euro_competition(), as.integer(game_year),
                               as.Date(start_d), as.Date(end_d), team_csv,
                               phase_csv, opp_ids_csv, home_away, outcome,
                               opp_rank_side, opp_rank_n, opp_rank_metric,
                               min_gn, max_gn, last_n_games,
                               num_starters_off_min, num_starters_off_max,
                               num_starters_def_min, num_starters_def_max))
  }

  # --- Live Calculation (Summary) ---
  live_result_df <- reactive({
    req(!is.null(input$euro_min_all_poss), !is.null(input$euro_min_on_poss))
    rng <- debounced_range()
    req(rng)
    req(!is.na(rng[1]), !is.na(rng[2]))
    tids <- selected_team_ids()
    gy <- euro_selected_season()
    db_args <- build_onoff_db_args()
    df_live <- run_onoff_compute_14(
      pg_pool,
      start_d = as.Date(rng[1]),
      end_d = as.Date(rng[2]),
      team_ids = tids,
      min_all = 0L,
      min_on = 0L,
      min_net = DEFAULT_MIN_NET,
      game_year = gy,
      phase_csv = db_args$phase_csv,
      opp_ids_csv = db_args$opp_ids_csv,
      home_away = db_args$home_away,
      outcome = db_args$outcome,
      opp_rank_side = db_args$opp_rank_side,
      opp_rank_n = db_args$opp_rank_n,
      opp_rank_metric = db_args$opp_rank_metric,
      min_gn = db_args$min_gn,
      max_gn = db_args$max_gn,
      last_n_games = db_args$last_n_games,
      num_starters_off_min = db_args$num_starters_off_min,
      num_starters_off_max = db_args$num_starters_off_max,
      num_starters_def_min = db_args$num_starters_def_min,
      num_starters_def_max = db_args$num_starters_def_max
    )

    # Keep fallback filtering behavior consistent with the MV path.
    if (all(c("ON Poss", "OFF Poss") %in% names(df_live))) {
      df_live <- df_live %>%
        filter(
          pmin(
            dplyr::coalesce(`ON Poss`, 0),
            dplyr::coalesce(`OFF Poss`, 0)
          ) >= !!input$euro_min_all_poss
        )
    }
    if ("ON Poss" %in% names(df_live)) {
      df_live <- df_live %>% filter(`ON Poss` >= !!input$euro_min_on_poss)
    }

    df_live
  })

  # --- Live Calculation (Four Factors) ---
  live_ff_result_df <- reactive({
    rng <- debounced_range()
    req(rng)
    gy <- euro_selected_season()
    db_args <- build_onoff_db_args()
    run_four_factors_compute(pg_pool,
                             game_year = gy,
                             start_d = as.Date(rng[1]),
                             end_d = as.Date(rng[2]),
                             team_ids = NULL,
                             phase_csv = db_args$phase_csv,
                             opp_ids_csv = db_args$opp_ids_csv,
                             home_away = db_args$home_away,
                             outcome = db_args$outcome,
                             opp_rank_side = db_args$opp_rank_side,
                             opp_rank_n = db_args$opp_rank_n,
                             opp_rank_metric = db_args$opp_rank_metric,
                             min_gn = db_args$min_gn, max_gn = db_args$max_gn, last_n_games = db_args$last_n_games,
                             num_starters_off_min = db_args$num_starters_off_min, num_starters_off_max = db_args$num_starters_off_max,
                             num_starters_def_min = db_args$num_starters_def_min, num_starters_def_max = db_args$num_starters_def_max)
  })

  # --- MV Fetch (Summary - LOAD FULL DATA) ---
  # Only load raw MV here. Filtering happens later in result_df.
  # Cache keys carry the competition and the EuroLeague load-run version, not
  # the Israeli ETL version: an Israeli ETL must not invalidate these, and a
  # EuroLeague publication must.
  on_data_version <- reactive(euro_data_version())
  mv_result_df <- reactive({
    gy <- as.integer(euro_selected_season())
    req(gy)
    cached_season_df(
      list("euro_player_onoff_default_mv", euro_competition(), gy, on_data_version()),
      function() db_get_query(pg_pool,
        'SELECT * FROM euroleague.player_onoff_default_mv
          WHERE competition = $1::text AND game_year = $2::int4
          ORDER BY "Net RTG Diff" DESC, "Team", "Last Name", "First Name"',
        params = list(euro_competition(), gy))
    )
  })

  # --- MV Fetch (Four Factors - LOAD FULL DATA) ---
  advanced_result_df <- reactive({
    gy <- as.integer(euro_selected_season())
    req(gy)
    cached_season_df(
      list("euro_player_advanced_stats_mv", euro_competition(), gy, on_data_version()),
      function() db_get_query(pg_pool,
        "SELECT *
           FROM euroleague.player_advanced_stats_mv
          WHERE competition = $1::text AND game_year = $2::int4",
        params = list(euro_competition(), gy))
    )
  })

  # --- Full ranked Four Factors data (ranks computed BEFORE any user filtering) ---
  ff_ranked_df <- reactive({
    if (isTRUE(fallback_needed())) {
      # Dynamic SQL path: use four_factors_compute + onoff_compute for rating diffs
      df_adv <- live_ff_result_df()

      # Get RTG diffs for ALL players (no min_poss or team filter)
      # Min-poss and team filtering is applied later in result_df()
      rng <- debounced_range()
      gy <- euro_selected_season()
      db_args <- build_onoff_db_args()
      df_sum <- run_onoff_compute_14(pg_pool,
                                     start_d = as.Date(rng[1]), end_d = as.Date(rng[2]),
                                     team_ids = NULL, min_all = 0L, min_on = 0L, min_net = DEFAULT_MIN_NET,
                                     game_year = gy, phase_csv = db_args$phase_csv, opp_ids_csv = db_args$opp_ids_csv,
                                     home_away = db_args$home_away, outcome = db_args$outcome,
                                     opp_rank_side = db_args$opp_rank_side, opp_rank_n = db_args$opp_rank_n, opp_rank_metric = db_args$opp_rank_metric,
                                     min_gn = db_args$min_gn, max_gn = db_args$max_gn, last_n_games = db_args$last_n_games,
                                     num_starters_off_min = db_args$num_starters_off_min, num_starters_off_max = db_args$num_starters_off_max,
                                     num_starters_def_min = db_args$num_starters_def_min, num_starters_def_max = db_args$num_starters_def_max) %>%
        select(player_id, team_id, `Net RTG Diff`, `Off ON Diff`, `Def ON Diff`, any_of("minutes"))

      df <- df_adv %>%
        left_join(df_sum, by = c("player_id", "team_id"))
    } else {
      # MV path (existing behavior)
      df_adv <- advanced_result_df()

      # Join with Summary Stats to get Ratings (Net/Off/Def Diff).
      # The Israeli onoff_default_mv exposes the season as "Year"; the
      # EuroLeague one uses game_year, and carries a competition column that
      # must be part of the key once EuroCup shares the schema.
      if (!"Net RTG Diff" %in% names(df_adv)) {
        df_sum <- mv_result_df() %>%
          select(player_id, team_id, competition, game_year,
                 `Net RTG Diff`, `Off ON Diff`, `Def ON Diff`, any_of("minutes"))

        df <- df_adv %>%
          left_join(df_sum, by = c("player_id", "team_id", "competition", "game_year"))
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

      pr_diff_off_efg  = percent_rank(if_else(off_on_poss >= rank_thresh, off_on_efg - off_off_efg, NA_real_)),
      pr_diff_off_oreb = percent_rank(if_else(off_on_poss >= rank_thresh, off_on_oreb - off_off_oreb, NA_real_)),
      pr_diff_off_ftr  = percent_rank(if_else(off_on_poss >= rank_thresh, off_on_ftr - off_off_ftr, NA_real_)),
      pr_diff_off_tov  = percent_rank(if_else(off_on_poss >= rank_thresh, off_on_tov - off_off_tov, NA_real_)),

      pr_diff_def_efg  = percent_rank(if_else(off_on_poss >= rank_thresh, def_on_efg - def_off_efg, NA_real_)),
      pr_diff_def_oreb = percent_rank(if_else(off_on_poss >= rank_thresh, def_on_oreb - def_off_oreb, NA_real_)),
      pr_diff_def_ftr  = percent_rank(if_else(off_on_poss >= rank_thresh, def_on_ftr - def_off_ftr, NA_real_)),
      pr_diff_def_tov  = percent_rank(if_else(off_on_poss >= rank_thresh, def_on_tov - def_off_tov, NA_real_))
    )

    # Dot position ranks (_rank suffix) for range bar visuals
    raw_cols <- c("off_on_efg", "off_off_efg", "off_on_oreb", "off_off_oreb",
                  "off_on_tov", "off_off_tov", "off_on_ftr", "off_off_ftr",
                  "def_on_efg", "def_off_efg", "def_on_oreb", "def_off_oreb",
                  "def_on_tov", "def_off_tov", "def_on_ftr", "def_off_ftr")
    for (col in intersect(raw_cols, names(df))) {
      vals <- if_else(df$off_on_poss >= rank_thresh, coalesce(df[[col]], 0), NA_real_)
      df[[paste0(col, "_rank")]] <- percent_rank(vals) * 100
    }

    df
  })

  # --- Final Switcher ---
  result_df <- reactive({
    mode <- input$euro_view_mode
    if (identical(mode, "Four Factors")) {

      df <- ff_ranked_df()

      # Filter LOCALLY (ranks already computed on full data)
      tids <- selected_team_ids()
      if (!is.null(tids) && length(tids) > 0) {
        df <- df %>% filter(team_id %in% !!tids)
      }
      if (all(c("off_on_poss", "off_off_poss", "def_on_poss", "def_off_poss") %in% names(df))) {
        df <- df %>%
          filter(
            pmin(
              dplyr::coalesce(off_on_poss, 0),
              dplyr::coalesce(off_off_poss, 0),
              dplyr::coalesce(def_on_poss, 0),
              dplyr::coalesce(def_off_poss, 0)
            ) >= !!input$euro_min_all_poss
          )
      }
      df <- df %>% filter(off_on_poss >= !!input$euro_min_on_poss)

      return(df)

    } else {
      # Summary Mode
      if (isTRUE(fallback_needed())) {
        return(live_result_df())
      } else {
        # Standard View = Use MV
        df <- mv_result_df()

        # --- FILTERING for Summary (Local) ---
        tids <- selected_team_ids()
        if (!is.null(tids) && length(tids) > 0) {
          df <- df %>% filter(team_id %in% !!tids)
        }

        # Enforce minimum possessions on both ON/OFF sides in MV path
        if (all(c("ON Poss", "OFF Poss") %in% names(df))) {
          df <- df %>%
            filter(
              pmin(
                dplyr::coalesce(`ON Poss`, 0),
                dplyr::coalesce(`OFF Poss`, 0)
              ) >= !!input$euro_min_all_poss
            )
        }

        # Filter Min Poss (Summary MV uses 'ON Poss')
        df <- df %>% filter(`ON Poss` >= !!input$euro_min_on_poss)

        return(df)
      }
    }
  }) %>% bindEvent(debounced_range(), debounced_teams(), debounced_on_filters(), gn_params(), input$euro_min_all_poss, input$euro_min_on_poss, input$euro_game_year, input$euro_view_mode)

  # --- Render Table ---
  output$euro_dt <- renderDT({
    df <- onoff_clean_display_names(result_df())
    mode <- input$euro_view_mode

    if (identical(mode, "Summary")) {
      return(onoff_summary_datatable(df, euro_stat_filter_state$filters()))

    } else if (identical(mode, "Four Factors")) {
      return(onoff_four_factors_datatable(df, euro_stat_filter_state$filters(),
                                          show_impact = FALSE))
    }
  }) %>% bindEvent(debounced_range(), debounced_teams(), debounced_on_filters(), gn_params(), input$euro_min_all_poss, input$euro_min_on_poss, input$euro_game_year, input$euro_view_mode, euro_stat_filter_state$filters())

  # ---- Filter Chips ----
  # build_filter_chips() special-cases the Israeli prefixes and reads the global
  # input$game_year for its season chip, so tab 8 builds its own rather than
  # bending that helper into a league-aware shape.
  output$euro_filter_chips <- renderUI({
    td <- euro_teams_df()
    team_map <- if (!is.null(td) && nrow(td)) {
      stats::setNames(as.character(td$team_name), as.character(td$team_id))
    } else {
      NULL
    }
    map_teams <- function(ids) {
      if (is.null(ids) || !length(ids) || is.null(team_map)) return(as.character(ids))
      out <- unname(team_map[as.character(ids)])
      out[is.na(out)] <- as.character(ids)[is.na(out)]
      out
    }

    chips <- list(
      tags$span(class = "filter-chip chip-season",
                paste(EURO_COMPETITION_LABELS[[euro_competition()]] %||% euro_competition(),
                      euro_season_label(euro_selected_season())))
    )
    add <- function(label, clear_id, cls = "chip-game") {
      chips[[length(chips) + 1L]] <<- make_chip(label, clear_id, cls)
    }

    bounds <- euro_season_date_bounds(euro_selected_season())
    rng <- input$euro_date_range
    if (length(rng) == 2 && !is.na(rng[1]) && !is.na(rng[2]) &&
        (!identical(as.Date(rng[1]), bounds$start) || !identical(as.Date(rng[2]), bounds$end))) {
      add(paste(format(as.Date(rng[1]), "%b %d"), "–", format(as.Date(rng[2]), "%b %d")),
          "euro_clear_dates")
    }
    if (length(input$euro_phase) && any(nzchar(input$euro_phase))) {
      add(paste(euro_phase_label(input$euro_phase), collapse = ", "), "euro_clear_game_type")
    }
    if (length(input$euro_teams) && any(nzchar(input$euro_teams))) {
      lbl <- map_teams(input$euro_teams)
      add(if (length(lbl) == 1) lbl else paste(length(lbl), "teams"), "euro_clear_teams")
    }
    if (length(input$euro_opponents) && any(nzchar(input$euro_opponents))) {
      lbl <- map_teams(input$euro_opponents)
      add(paste("vs", if (length(lbl) == 1) lbl else paste(length(lbl), "teams")),
          "euro_clear_opponents")
    }
    if (nzchar(input$euro_home_away %||% "")) {
      add(if (identical(input$euro_home_away, "home")) "Home" else "Away", "euro_clear_home_away")
    }
    if (nzchar(input$euro_outcome %||% "")) {
      add(if (identical(input$euro_outcome, "win")) "Wins" else "Losses", "euro_clear_outcome")
    }
    gp <- gn_params()
    if (!is.na(gp$last_n)) {
      add(paste("Last", gp$last_n, "games"), "euro_clear_last_n")
    } else if (!is.na(gp$min_gn) || !is.na(gp$max_gn)) {
      lo <- if (is.na(gp$min_gn)) "1" else as.character(gp$min_gn)
      hi <- if (is.na(gp$max_gn)) "∞" else as.character(gp$max_gn)
      add(paste0("Rounds ", lo, "–", hi), "euro_clear_gn")
    }
    for (side in c("off", "def")) {
      mode_v <- input[[paste0("euro_num_starters_", side, "_mode")]]
      val_v <- input[[paste0("euro_num_starters_", side)]]
      if (nzchar(mode_v %||% "") && nzchar(val_v %||% "")) {
        add(paste0(if (side == "off") "Own" else "Opp", " starters ",
                   if (identical(mode_v, "gte")) "≥" else "≤", " ", val_v),
            "euro_clear_starters")
      }
    }
    if (nzchar(input$euro_opp_rank_side %||% "") && nzchar(input$euro_opp_rank_n %||% "")) {
      add(paste(if (identical(input$euro_opp_rank_side, "top")) "vs Top" else "vs Bottom",
                input$euro_opp_rank_n), "euro_clear_opp_rank")
    }

    tagList(
      div(class = "filter-chips-bar", chips),
      stat_filter_chips_ui("euro", euro_stat_filter_state, euro_stat_filter_cols)
    )
  })
  setup_chip_clears("euro", session, input, shared,
    game_type_id = "euro_phase", opponents_id = "euro_opponents",
    home_away_id = "euro_home_away", outcome_id = "euro_outcome",
    gn_min_id = "euro_gn_min", gn_max_id = "euro_gn_max", last_n_id = "euro_last_n",
    opp_rank_ids = c("euro_opp_rank_side", "euro_opp_rank_n", "euro_opp_rank_metric"),
    date_id = "euro_date_range", gy_input_id = "euro_game_year",
    teams_ids = NULL,
    starters_ids = c("euro_num_starters_off_mode", "euro_num_starters_off",
                     "euro_num_starters_def_mode", "euro_num_starters_def"))

  # Teams is handled here rather than via setup_chip_clears' teams_ids: that
  # helper clears anything outside its Israeli id allowlist with "", which
  # leaves a blank option selected in a multi-select.
  observeEvent(input$euro_clear_teams, {
    updateSelectizeInput(session, "euro_teams", selected = character(0))
  }, ignoreInit = TRUE)
}

