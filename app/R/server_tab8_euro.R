# server_tab8_euro.R - Tab 8: EuroLeague On/Off Impact server logic
# Derived from server_tab1.R. Summary + Four Factors only (no Shot
# Profile); reads euroleague.* via migration 004.

# The Summary and Four Factors filter menus live in helpers.R as
# ONOFF_SUMMARY_FILTERABLE_COLS / ONOFF_FF_FILTERABLE_COLS, shared with the
# other league's on/off tab.

server_tab8_euro <- function(input, output, session, shared) {
  onoff_cfg <- onoff_tab_descriptor("euroleague")
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
  euro_competition <- shared$euro$competition
  euro_selected_season <- shared$euro$season
  euro_teams_df <- shared$euro$teams_df

  # Teams / opponents / phase / rounds / dates all follow competition + season.
  # Shared with the other EuroLeague tabs; see setup_euro_section_filters().
  setup_euro_section_filters(input, session, "euro", tab_id = "euro",
                             euro_context = shared$euro,
                             date_id = "euro_date_range")

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

  # --- Reset Logic ---
  observeEvent(input$euro_reset_defaults, {
    resetting(TRUE)
    # Reset to the newest loaded season for this competition, not to the
    # Israeli DEFAULT_GAME_YEAR, which is a different numbering entirely.
    seasons <- shared$euro$seasons_df()
    default_season <- if (!is.null(seasons) && nrow(seasons)) {
      as.character(seasons$game_year[[1]])
    } else {
      EURO_DEFAULT_SEASON
    }
    updateSelectInput(session, "euro_game_year", selected = default_season)
    apply_season_date_bounds(session, "euro_date_range", euro_season_date_bounds(default_season))
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
  debounced_on_filters <- reactive(
    onoff_filter_values(input, onoff_cfg$prefix,
                        game_type_id = onoff_cfg$game_type_id)
  ) %>% debounce(300)

  # Same GN/last-N resolution as every other tab; "GN" here is the round
  # number supplied by the shared EuroLeague reference context.
  gn_params <- reactive(resolve_gn_last_n_params(input, "euro")) %>% debounce(150)

  build_onoff_db_args <- function() {
    args <- onoff_db_args(debounced_on_filters(), gn_params())
    args$phase_csv <- args$game_type_csv
    args
  }

  # The whole filtered-path population, with both possession bars at zero.
  # A reactive, not a function, and that matters: the Summary table and the two
  # auto-min bars all need this exact frame, and each used to issue the query
  # separately -- three identical onoff_compute calls per filter change. As one
  # reactive it is computed once per flush.
  live_unfiltered_df <- reactive({
    rng <- debounced_range()
    req(rng)
    req(!is.na(rng[1]), !is.na(rng[2]))
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
  })

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
      live = function() live_unfiltered_df(),
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
                 paste0("SELECT * FROM euroleague.four_factors_dashboard_compute(",
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
  # Reads the shared unfiltered pull rather than re-issuing the query, then
  # applies the possession bars. Teams are already filtered in SQL there.
  live_result_df <- reactive({
    req(!is.null(input$euro_min_all_poss), !is.null(input$euro_min_on_poss))
    onoff_filter_summary_rows(live_unfiltered_df(), NULL,
                              input$euro_min_all_poss, input$euro_min_on_poss)
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
      # One filtered fact aggregation now supplies Four Factors and the rating
      # differences; the previous second onoff_compute call was redundant.
      df <- live_ff_result_df()
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
                                          show_impact = onoff_cfg$show_impact))
    }
  }) %>% bindEvent(debounced_range(), debounced_teams(), debounced_on_filters(), gn_params(), input$euro_min_all_poss, input$euro_min_on_poss, input$euro_game_year, input$euro_view_mode, euro_stat_filter_state$filters())

  # ---- Filter Chips ----
  # Tab 1's chip bar. Everything league-specific is an argument: the season
  # value and its competition-qualified label, the date input id, the phase
  # input and its labeller, and "Rd" for the schedule position.
  output$euro_filter_chips <- renderUI({
    td <- euro_teams_df()
    team_map <- if (!is.null(td) && nrow(td)) {
      stats::setNames(as.character(td$team_name), as.character(td$team_id))
    } else {
      NULL
    }
    season <- euro_selected_season()
    build_filter_chips(
      "euro", input, euro_season_date_bounds,
      reset_btn_id = "euro_reset_defaults",
      team_label_map = team_map,
      opponent_label_map = team_map,
      season_value = season,
      season_label = paste(EURO_COMPETITION_LABELS[[euro_competition()]] %||% euro_competition(),
                           euro_season_label(season)),
      date_input_id = "euro_date_range",
      game_type_input_id = "euro_phase",
      game_type_labeller = euro_phase_label,
      gn_label = "Rd",
      extra_children = stat_filter_chips_ui("euro", euro_stat_filter_state, euro_stat_filter_cols)
    )
  })
  setup_chip_clears("euro", session, input, shared,
    game_type_id = "euro_phase", opponents_id = "euro_opponents",
    home_away_id = "euro_home_away", outcome_id = "euro_outcome",
    gn_min_id = "euro_gn_min", gn_max_id = "euro_gn_max", last_n_id = "euro_last_n",
    opp_rank_ids = c("euro_opp_rank_side", "euro_opp_rank_n", "euro_opp_rank_metric"),
    date_id = "euro_date_range", gy_input_id = "euro_game_year",
    teams_ids = onoff_cfg$teams_id, teams_multiple = TRUE,
    starters_ids = c("euro_num_starters_off_mode", "euro_num_starters_off",
                     "euro_num_starters_def_mode", "euro_num_starters_def"),
    bounds_fn = euro_season_date_bounds)

}
