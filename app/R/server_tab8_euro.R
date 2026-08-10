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

  # The population the auto-min bars measure on the filtered path: the same
  # query live_result_df() runs, but with both bars at zero so the threshold
  # comes from the whole population.
  auto_min_live_df <- function() {
    rng <- debounced_range()
    req(rng)
    db_args <- build_onoff_db_args()
    run_onoff_compute_14(
      pg_pool,
      start_d = as.Date(rng[1]), end_d = as.Date(rng[2]),
      team_ids = selected_team_ids(), min_all = 0L, min_on = 0L,
      min_net = DEFAULT_MIN_NET, game_year = euro_selected_season(),
      phase_csv = db_args$phase_csv, opp_ids_csv = db_args$opp_ids_csv,
      home_away = db_args$home_away, outcome = db_args$outcome,
      opp_rank_side = db_args$opp_rank_side, opp_rank_n = db_args$opp_rank_n, opp_rank_metric = db_args$opp_rank_metric,
      min_gn = db_args$min_gn, max_gn = db_args$max_gn, last_n_games = db_args$last_n_games,
      num_starters_off_min = db_args$num_starters_off_min, num_starters_off_max = db_args$num_starters_off_max,
      num_starters_def_min = db_args$num_starters_def_min, num_starters_def_max = db_args$num_starters_def_max
    )
  }

  setup_onoff_auto_min(
    input, session,
    min_on_id = "euro_min_on_poss", min_all_id = "euro_min_all_poss",
    state = auto_min_state, auto_enabled = auto_enabled, resetting = resetting,
    mode_r = function() input$euro_view_mode,
    triggers = function() {
      list(debounced_range(), debounced_teams(), debounced_on_filters(),
           gn_params(), input$euro_game_year, input$euro_view_mode)
    },
    sources = list(
      fallback = function() fallback_needed(),
      ff = function() ff_ranked_df(),
      mv = function() mv_result_df(),
      live = auto_min_live_df,
      team_ids = function() selected_team_ids()
    )
  )

  setup_gn_last_n_sync(session, input, "euro")

  selected_team_ids <- reactive({
    ids <- suppressWarnings(as.integer(debounced_teams()))
    ids <- ids[is.finite(ids)]
    if (length(ids)) ids else NULL
  })

  # --- Fallback Logic ---
  # We do NOT return true if only team/min_poss changed. Shared with the other
  # league's on/off tab; see onoff_fallback_needed() in helpers.R.
  fallback_needed <- reactive({
    onoff_fallback_needed(
      debounced_range(),
      euro_season_date_bounds(euro_selected_season()),
      debounced_on_filters(), gn_params(), input, "euro"
    )
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

    # Keep fallback filtering behavior consistent with the MV path. Teams are
    # already filtered in SQL here, so only the possession bars apply.
    onoff_filter_summary_rows(df_live, NULL, input$euro_min_all_poss,
                              input$euro_min_on_poss)
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

    # Derived display columns and full-population percentile ranks (helpers.R,
    # shared with the other league's on/off tab).
    onoff_add_ff_ranks(df)
  })

  # --- Final Switcher ---
  result_df <- reactive({
    mode <- input$euro_view_mode
    if (identical(mode, "Four Factors")) {

      # Filter LOCALLY (ranks already computed on full data)
      return(onoff_filter_ff_rows(ff_ranked_df(), selected_team_ids(),
                                  input$euro_min_all_poss, input$euro_min_on_poss))

    } else {
      # Summary Mode
      if (isTRUE(fallback_needed())) {
        return(live_result_df())
      } else {
        # Standard View = Use MV, filtered locally
        return(onoff_filter_summary_rows(mv_result_df(), selected_team_ids(),
                                         input$euro_min_all_poss,
                                         input$euro_min_on_poss))
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

