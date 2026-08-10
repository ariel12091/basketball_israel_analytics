# server_tab1.R - Tab 1: On/Off Impact server logic

# The Summary and Four Factors filter menus live in helpers.R as
# ONOFF_SUMMARY_FILTERABLE_COLS / ONOFF_FF_FILTERABLE_COLS, shared with the
# other league's on/off tab. Shot Profile is Israeli-only and stays here.
ON_SP_LABELS <- c("eFG%", "Lay-up", "Dunk", "Lay+Dunk", "3PA", "C3", "2PT Jumper")

ON_SP_FILTERABLE_COLS <- c(
  stats::setNames(paste0("Off ", ON_SP_LABELS, " Diff"), paste("Off", ON_SP_LABELS, "Δ")),
  stats::setNames(paste0("Def ", ON_SP_LABELS, " Diff"), paste("Def", ON_SP_LABELS, "Δ")),
  "Min" = "minutes",
  "On Poss" = "ON Poss",
  "Off Poss" = "OFF Poss"
)

server_tab1 <- function(input, output, session, shared) {
  auto_min_state <- reactiveValues(
    last_auto = NA_integer_,
    last_auto_all = NA_integer_,
    updating = FALSE
  )
  auto_enabled <- reactiveVal(TRUE)
  resetting <- reactiveVal(FALSE)
  on_stat_filter_state <- make_stat_filter_state()

  on_stat_filter_cols <- reactive({
    switch(input$onoff_view_mode %||% "Summary",
      "Four Factors" = ONOFF_FF_FILTERABLE_COLS,
      "Shot Profile" = ON_SP_FILTERABLE_COLS,
      ONOFF_SUMMARY_FILTERABLE_COLS
    )
  })

  setup_stat_filter_handlers("on", input, session, on_stat_filter_cols, on_stat_filter_state)

  # AUTO_TOP_PCT and the auto-min helpers (auto_min_on_from_df,
  # auto_min_all_from_df, resolve_poss_cols) now live in helpers.R,
  # shared with the other league's on/off tab.
  # ======== On/Off tab Logic ===================================
  observeEvent(shared$selected_game_year(), {
    bounds <- shared$season_date_bounds(shared$selected_game_year())
    updateDateRangeInput(session, "date_range",
                         start = bounds$start, end = bounds$end,
                         min = bounds$start, max = bounds$end)

    gy_int <- as.integer(shared$selected_game_year())
    gn_df <- fetch_gn_values(gy_int)
    gn_vals <- if (nrow(gn_df)) as.integer(gn_df$gn) else integer(0)
    update_gn_last_n_choices(session, "on", gn_vals)
  }, ignoreInit = FALSE)


  # --- Reset Logic ---
  observeEvent(input$reset_defaults, {
    resetting(TRUE)
    updateSelectInput(session, "game_year", selected = DEFAULT_GAME_YEAR)
    bounds <- shared$season_date_bounds(DEFAULT_GAME_YEAR)
    updateDateRangeInput(session, "date_range",
                         start = bounds$start, end = bounds$end,
                         min = bounds$start, max = bounds$end)
    updateSelectizeInput(session, "on_game_type", selected = character(0))
    updateSelectizeInput(session, "on_opponents", selected = character(0))
    updateSelectInput(session, "on_home_away", selected = "")
    updateSelectInput(session, "on_outcome", selected = "")
    updateSelectInput(session, "on_opp_rank_side", selected = "")
    updateSelectInput(session, "on_opp_rank_n", selected = "")
    updateSelectInput(session, "on_opp_rank_metric", selected = "")
    updateSelectInput(session, "on_num_starters_off_mode", selected = "")
    updateSelectInput(session, "on_num_starters_off", selected = "")
    updateSelectInput(session, "on_num_starters_def_mode", selected = "")
    updateSelectInput(session, "on_num_starters_def", selected = "")
    updateSliderInput(session, "min_all_poss", value = DEFAULT_MIN_ALL)
    updateSliderInput(session, "min_on_poss", value = DEFAULT_MIN_ON)
    updateSelectizeInput(session, "on_gn_min", selected = "")
    updateSelectizeInput(session, "on_gn_max", selected = "")
    updateSelectizeInput(session, "on_last_n", selected = "")
    reset_stat_filters(on_stat_filter_state)
    auto_min_state$last_auto <- as.integer(DEFAULT_MIN_ON)
    auto_min_state$last_auto_all <- as.integer(DEFAULT_MIN_ALL)
    auto_enabled(FALSE)
    # Clear teams
    updateSelectizeInput(session, "teams", selected = character(0))
    session$onFlushed(function() resetting(FALSE), once = TRUE)
  })

  observeEvent(input$onoff_view_mode, {
    reset_stat_filters(on_stat_filter_state)
  }, ignoreInit = TRUE)

  debounced_range <- reactive(input$date_range) %>% debounce(300)
  debounced_teams <- reactive(input$teams) %>% debounce(300)
  debounced_on_filters <- reactive(list(
    game_type = input$on_game_type,
    opp_ids = input$on_opponents,
    home_away = input$on_home_away,
    outcome = input$on_outcome,
    rank_side = input$on_opp_rank_side,
    rank_n = input$on_opp_rank_n,
    metric = input$on_opp_rank_metric,
    num_starters_off_mode = input$on_num_starters_off_mode,
    num_starters_off = input$on_num_starters_off,
    num_starters_def_mode = input$on_num_starters_def_mode,
    num_starters_def = input$on_num_starters_def
  )) %>% debounce(300)

  # resolve_gn_last_n_params() is the same GN/last-N resolution tabs 2-6 use;
  # this tab and tab 8 held their own byte-identical copies of it.
  gn_params <- reactive(resolve_gn_last_n_params(input, "on")) %>% debounce(150)

  build_onoff_db_args <- function() {
    f <- debounced_on_filters()
    gp <- gn_params()

    game_type_csv <- csv_if_any(f$game_type)
    opp_ids_csv <- csv_if_any(shared$selected_opp_ids_on())

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
      game_type_csv = game_type_csv,
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

  # The whole filtered-path population, with both possession bars at zero.
  # A reactive, not a function, and that matters: the Summary table and the two
  # auto-min bars all need this exact frame, and each used to issue the query
  # separately -- three identical onoff_compute calls per filter change, on the
  # heaviest query the tab runs. As one reactive it is computed once per flush.
  live_unfiltered_df <- reactive({
    rng <- debounced_range()
    req(rng)
    req(!is.na(rng[1]), !is.na(rng[2]))
    db_args <- build_onoff_db_args()
    run_onoff_compute_14(
      pg_pool,
      start_d = as.Date(rng[1]), end_d = as.Date(rng[2]),
      team_ids = selected_team_ids(), min_all = 0L, min_on = 0L,
      min_net = DEFAULT_MIN_NET, game_year = shared$selected_game_year(),
      game_type_csv = db_args$game_type_csv, opp_ids_csv = db_args$opp_ids_csv,
      home_away = db_args$home_away, outcome = db_args$outcome,
      opp_rank_side = db_args$opp_rank_side, opp_rank_n = db_args$opp_rank_n, opp_rank_metric = db_args$opp_rank_metric,
      min_gn = db_args$min_gn, max_gn = db_args$max_gn, last_n_games = db_args$last_n_games,
      num_starters_off = NA_integer_, num_starters_def = NA_integer_,
      num_starters_off_min = db_args$num_starters_off_min, num_starters_off_max = db_args$num_starters_off_max,
      num_starters_def_min = db_args$num_starters_def_min, num_starters_def_max = db_args$num_starters_def_max
    )
  })

  setup_onoff_auto_min(
    input, session,
    min_on_id = "min_on_poss", min_all_id = "min_all_poss",
    state = auto_min_state, auto_enabled = auto_enabled, resetting = resetting,
    mode_r = function() input$onoff_view_mode,
    triggers = function() {
      list(debounced_range(), debounced_teams(), debounced_on_filters(),
           gn_params(), input$game_year, input$onoff_view_mode)
    },
    sources = list(
      fallback = function() fallback_needed(),
      ff = function() ff_ranked_df(),
      mv = function() mv_result_df(),
      live = function() live_unfiltered_df(),
      team_ids = function() selected_team_ids()
    )
  )

  setup_gn_last_n_sync(session, input, "on")

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
      shared$season_date_bounds(shared$selected_game_year()),
      debounced_on_filters(), gn_params(), input, "on"
    )
  })

  # --- On/Off Compute Function ---
  run_onoff_compute_14 <- function(pool, start_d, end_d, team_ids, min_all, min_on, min_net, game_year, game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric, min_gn = NA_integer_, max_gn = NA_integer_, last_n_games = NA_integer_, num_starters_off = NA_integer_, num_starters_def = NA_integer_, num_starters_off_min = NA_integer_, num_starters_off_max = NA_integer_, num_starters_def_min = NA_integer_, num_starters_def_max = NA_integer_) {
    allowed <- guard_heavy_request(
      session, key = "tab1_onoff_compute",
      start_d = start_d, end_d = end_d,
      min_gn = min_gn, max_gn = max_gn, last_n = last_n_games,
      max_calls = 35L, window_sec = 60L
    )
    if (!isTRUE(allowed)) return(data.frame())
    team_csv <- if (is.null(team_ids) || !length(team_ids)) NA_character_ else paste(team_ids, collapse = ",")
    db_get_query(pool, paste0("SELECT * FROM basketball_test.onoff_compute(", "$1::date,$2::date,$3::text,$4::int4,$5::int4,$6::numeric,$7::text,", "$8::text,$9::text,$10::text,$11::text,$12::text,$13::int4,$14::text,", "$15::int4,$16::int4,$17::int4,$18::int4,$19::int4,$20::int4,$21::int4,$22::int4,$23::int4", ")"),
                    params = list(as.Date(start_d), as.Date(end_d), team_csv, as.integer(min_all), as.integer(min_on), as.numeric(min_net), as.character(game_year), game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric, min_gn, max_gn, last_n_games, num_starters_off, num_starters_def, num_starters_off_min, num_starters_off_max, num_starters_def_min, num_starters_def_max))
  }

  # --- Four Factors Compute Function ---
  run_four_factors_compute <- function(pool, game_year, start_d, end_d, team_ids,
                                       game_type_csv, opp_ids_csv, home_away, outcome,
                                       opp_rank_side, opp_rank_n, opp_rank_metric,
                                       min_gn = NA_integer_, max_gn = NA_integer_, last_n_games = NA_integer_,
                                       num_starters_off = NA_integer_, num_starters_def = NA_integer_,
                                       num_starters_off_min = NA_integer_, num_starters_off_max = NA_integer_,
                                       num_starters_def_min = NA_integer_, num_starters_def_max = NA_integer_) {
    allowed <- guard_heavy_request(
      session, key = "tab1_ff_compute",
      start_d = start_d, end_d = end_d,
      min_gn = min_gn, max_gn = max_gn, last_n = last_n_games,
      max_calls = 35L, window_sec = 60L
    )
    if (!isTRUE(allowed)) return(data.frame())
    team_csv <- if (is.null(team_ids) || !length(team_ids)) NA_character_ else paste(team_ids, collapse = ",")
    db_get_query(pool,
                    paste0("SELECT * FROM basketball_test.four_factors_compute(",
                           "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,",
                           "$7::text,$8::text,$9::text,$10::int4,$11::text,",
                           "$12::int4,$13::int4,$14::int4,$15::int4,$16::int4,$17::int4,$18::int4,$19::int4,$20::int4",
                           ")"),
                    params = list(as.integer(game_year), start_d, end_d, team_csv,
                                  game_type_csv, opp_ids_csv, home_away, outcome,
                                  opp_rank_side, opp_rank_n, opp_rank_metric,
                                  min_gn, max_gn, last_n_games, num_starters_off, num_starters_def, num_starters_off_min, num_starters_off_max, num_starters_def_min, num_starters_def_max))
  }

  # --- Live Calculation (Summary) ---
  # Reads the shared unfiltered pull rather than re-issuing the query, then
  # applies the possession bars. Teams are already filtered in SQL there.
  live_result_df <- reactive({
    req(!is.null(input$min_all_poss), !is.null(input$min_on_poss))
    onoff_filter_summary_rows(live_unfiltered_df(), NULL,
                              input$min_all_poss, input$min_on_poss)
  })

  # --- Live Calculation (Four Factors) ---
  live_ff_result_df <- reactive({
    rng <- debounced_range()
    req(rng)
    gy <- shared$selected_game_year()
    db_args <- build_onoff_db_args()
    run_four_factors_compute(pg_pool,
                             game_year = gy,
                             start_d = as.Date(rng[1]),
                             end_d = as.Date(rng[2]),
                             team_ids = NULL,
                             game_type_csv = db_args$game_type_csv,
                             opp_ids_csv = db_args$opp_ids_csv,
                             home_away = db_args$home_away,
                             outcome = db_args$outcome,
                             opp_rank_side = db_args$opp_rank_side,
                             opp_rank_n = db_args$opp_rank_n,
                             opp_rank_metric = db_args$opp_rank_metric,
                             min_gn = db_args$min_gn, max_gn = db_args$max_gn, last_n_games = db_args$last_n_games,
                             num_starters_off = NA_integer_, num_starters_def = NA_integer_,
                             num_starters_off_min = db_args$num_starters_off_min, num_starters_off_max = db_args$num_starters_off_max,
                             num_starters_def_min = db_args$num_starters_def_min, num_starters_def_max = db_args$num_starters_def_max)
  })

  # --- MV Fetch (Summary - LOAD FULL DATA) ---
  # Only load raw MV here. Filtering happens later in result_df.
  # Season pulls are shared across sessions via cached_season_df; the
  # data-version key invalidates the cache after each ETL run.
  on_data_version <- reactive(shared_data_version(shared))
  mv_result_df <- reactive({
    gy <- as.integer(shared$selected_game_year())
    req(gy)
    cached_season_df(
      list("onoff_default_mv", gy, on_data_version()),
      function() db_get_query(pg_pool,
        'SELECT * FROM basketball_test.onoff_default_mv WHERE "Year" = $1::int4 ORDER BY "Net RTG Diff" DESC, "Team", "Last Name", "First Name"',
        params = list(gy))
    )
  })

  # --- MV Fetch (Four Factors - LOAD FULL DATA) ---
  advanced_result_df <- reactive({
    gy <- as.integer(shared$selected_game_year())
    req(gy)
    cached_season_df(
      list("player_advanced_stats_mv", gy, on_data_version()),
      function() db_get_query(pg_pool,
        "SELECT *
           FROM basketball_test.player_advanced_stats_mv
          WHERE game_year = $1::int4",
        params = list(gy))
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
      gy <- shared$selected_game_year()
      db_args <- build_onoff_db_args()
      df_sum <- run_onoff_compute_14(pg_pool,
                                     start_d = as.Date(rng[1]), end_d = as.Date(rng[2]),
                                     team_ids = NULL, min_all = 0L, min_on = 0L, min_net = DEFAULT_MIN_NET,
                                     game_year = gy, game_type_csv = db_args$game_type_csv, opp_ids_csv = db_args$opp_ids_csv,
                                     home_away = db_args$home_away, outcome = db_args$outcome,
                                     opp_rank_side = db_args$opp_rank_side, opp_rank_n = db_args$opp_rank_n, opp_rank_metric = db_args$opp_rank_metric,
                                     min_gn = db_args$min_gn, max_gn = db_args$max_gn, last_n_games = db_args$last_n_games,
                                     num_starters_off = NA_integer_, num_starters_def = NA_integer_,
                                     num_starters_off_min = db_args$num_starters_off_min, num_starters_off_max = db_args$num_starters_off_max,
                                     num_starters_def_min = db_args$num_starters_def_min, num_starters_def_max = db_args$num_starters_def_max) %>%
        select(player_id, team_id, `Net RTG Diff`, `Off ON Diff`, `Def ON Diff`, any_of("minutes"))

      df <- df_adv %>%
        left_join(df_sum, by = c("player_id", "team_id"))
    } else {
      # MV path (existing behavior)
      df_adv <- advanced_result_df()

      # Join with Summary Stats to get Ratings (Net/Off/Def Diff)
      if (!"Net RTG Diff" %in% names(df_adv)) {
        df_sum <- mv_result_df() %>%
          select(player_id, team_id, "Year", `Net RTG Diff`, `Off ON Diff`, `Def ON Diff`, any_of("minutes"))

        df <- df_adv %>%
          left_join(df_sum, by = c("player_id", "team_id", "game_year" = "Year"))
      } else {
        df <- df_adv
      }
    }

    # Derived display columns and full-population percentile ranks (helpers.R,
    # shared with the other league's on/off tab).
    onoff_add_ff_ranks(df)
  })

  # --- Full ranked Shot Profile data (ranks computed BEFORE any user filtering,
  # mirroring ff_ranked_df: team/min-poss filters must not reshuffle percentiles) ---
  SP_METRIC_SUFFIX <- c("efg", "layup_share", "dunk_share", "rim_share", "fg3_share", "c3_pct3", "mid_share")
  SP_FGA_GATE <- 50

  sp_ranked_df <- reactive({
    if (isTRUE(fallback_needed())) {
      rng <- debounced_range()
      req(rng)
      gy <- shared$selected_game_year()
      db_args <- build_onoff_db_args()
      df <- run_onoff_compute_14(
        pg_pool,
        start_d = as.Date(rng[1]), end_d = as.Date(rng[2]),
        team_ids = NULL, min_all = 0L, min_on = 0L,
        min_net = DEFAULT_MIN_NET, game_year = gy,
        game_type_csv = db_args$game_type_csv, opp_ids_csv = db_args$opp_ids_csv,
        home_away = db_args$home_away, outcome = db_args$outcome,
        opp_rank_side = db_args$opp_rank_side, opp_rank_n = db_args$opp_rank_n, opp_rank_metric = db_args$opp_rank_metric,
        min_gn = db_args$min_gn, max_gn = db_args$max_gn, last_n_games = db_args$last_n_games,
        num_starters_off = NA_integer_, num_starters_def = NA_integer_,
        num_starters_off_min = db_args$num_starters_off_min, num_starters_off_max = db_args$num_starters_off_max,
        num_starters_def_min = db_args$num_starters_def_min, num_starters_def_max = db_args$num_starters_def_max
      )
    } else {
      df <- mv_result_df()
    }

    sp_prefixes <- c("off_on", "off_off", "def_on", "def_off")
    need_cols <- as.vector(outer(sp_prefixes, c("_layup_att", "_dunk_att", "_fg2_att", "_fg3_att", "_c3_att", "_c3_known_att", "_fg2_made", "_fg3_made"), paste0))
    if (is.null(df) || !nrow(df) || !all(need_cols %in% names(df))) return(df)

    # Total FGA per split (helper takes total FGA, not fg2) + eFG per split
    # (same FGA denominator as the diet shares — efficiency context column)
    for (p in sp_prefixes) {
      fga <- dplyr::coalesce(as.numeric(df[[paste0(p, "_fg2_att")]]), 0) +
        dplyr::coalesce(as.numeric(df[[paste0(p, "_fg3_att")]]), 0)
      df[[paste0(p, "_fga_in")]] <- fga
      made_w <- dplyr::coalesce(as.numeric(df[[paste0(p, "_fg2_made")]]), 0) +
        1.5 * dplyr::coalesce(as.numeric(df[[paste0(p, "_fg3_made")]]), 0)
      df[[paste0(p, "_efg")]] <- ifelse(fga > 0, round(made_w / fga * 100, 1), NA_real_)
    }
    sp_specs <- stats::setNames(lapply(sp_prefixes, function(p) {
      paste0(p, c("_layup_att", "_dunk_att", "_fga_in", "_fg3_att", "_c3_att", "_c3_known_att"))
    }), sp_prefixes)
    df <- add_shot_profile_metrics(df, sp_specs)

    # Diff display columns (ON share - OFF share, pp) + FF-style percentile
    # ranks: pr_sp_* colors the diff cell, *_rank positions the on/off dots.
    # Ranked only at >= SP_FGA_GATE ON-side team FGA (else gray/unranked).
    for (i in seq_along(SP_METRIC_SUFFIX)) {
      m <- SP_METRIC_SUFFIX[i]
      for (side in c("off", "def")) {
        diff_col <- paste0(ifelse(side == "off", "Off ", "Def "), ON_SP_LABELS[i], " Diff")
        df[[diff_col]] <- round(df[[paste0(side, "_on_", m)]] - df[[paste0(side, "_off_", m)]], 1)
        gate <- dplyr::coalesce(as.numeric(df[[paste0(side, "_on_fga")]]), 0) >= SP_FGA_GATE
        df[[paste0("pr_sp_", side, "_", m)]] <- percent_rank(ifelse(gate, df[[diff_col]], NA_real_))
        for (phase in c("on", "off")) {
          sc <- paste0(side, "_", phase, "_", m)
          df[[paste0(sc, "_rank")]] <- percent_rank(ifelse(gate, dplyr::coalesce(df[[sc]], 0), NA_real_)) * 100
        }
      }
    }

    df
  })

  # --- Final Switcher ---
  result_df <- reactive({
    mode <- input$onoff_view_mode
    if (identical(mode, "Four Factors")) {

      # Filter LOCALLY (ranks already computed on full data)
      return(onoff_filter_ff_rows(ff_ranked_df(), selected_team_ids(),
                                  input$min_all_poss, input$min_on_poss))

    } else if (identical(mode, "Shot Profile")) {

      # Filter LOCALLY (ranks already computed on full data, like Four Factors)
      return(onoff_filter_summary_rows(sp_ranked_df(), selected_team_ids(),
                                       input$min_all_poss, input$min_on_poss))

    } else {
      # Summary Mode
      if (isTRUE(fallback_needed())) {
        return(live_result_df())
      } else {
        # Standard View = Use MV, filtered locally
        return(onoff_filter_summary_rows(mv_result_df(), selected_team_ids(),
                                         input$min_all_poss, input$min_on_poss))
      }
    }
  }) %>% bindEvent(debounced_range(), debounced_teams(), debounced_on_filters(), gn_params(), input$min_all_poss, input$min_on_poss, input$game_year, input$onoff_view_mode)

  # --- Render Table ---
  output$onoff_dt <- renderDT({
    df <- onoff_clean_display_names(result_df())
    mode <- input$onoff_view_mode

    if (identical(mode, "Summary")) {
      return(onoff_summary_datatable(df, on_stat_filter_state$filters()))

    } else if (identical(mode, "Four Factors")) {
      return(onoff_four_factors_datatable(df, on_stat_filter_state$filters(),
                                          show_impact = TRUE))
    } else {
      # === MODE 3: SHOT PROFILE (FF-style; shares/diffs/ranks precomputed
      # on the full population in sp_ranked_df, filtered in result_df) ===
      sp_prefixes <- c("off_on", "off_off", "def_on", "def_off")
      sp_metric_suffix <- SP_METRIC_SUFFIX
      if (!all(paste0("Off ", ON_SP_LABELS, " Diff") %in% names(df))) {
        return(DT::datatable(
          data.frame(Info = "Shot Profile columns unavailable for this dataset", check.names = FALSE),
          rownames = FALSE, options = list(dom = "t")
        ))
      }

      if (!"minutes" %in% names(df)) df$minutes <- NA_real_
      sp_diff_cols <- c(paste0("Off ", ON_SP_LABELS, " Diff"), paste0("Def ", ON_SP_LABELS, " Diff"))
      sp_share_cols <- as.vector(outer(sp_prefixes, paste0("_", sp_metric_suffix), paste0))
      sp_fga_cols <- paste0(sp_prefixes, "_fga")
      sp_pr_cols <- as.vector(outer(paste0("pr_sp_", c("off", "def"), "_"), sp_metric_suffix, paste0))
      sp_rank_cols <- paste0(sp_share_cols, "_rank")
      keep_cols <- c("Team", "Player", sp_diff_cols, "minutes", "ON Poss", "OFF Poss",
                     sp_share_cols, sp_fga_cols, sp_pr_cols, sp_rank_cols)
      df_final <- df[, intersect(keep_cols, names(df))]
      df_final <- apply_stat_filters(df_final, on_stat_filter_state$filters())

      # FF-style cell: signed diff headline, on/off percentile dots on a rank
      # bar, "on | off" subtext. Em-dash when the corner flag is unknown;
      # gray/unranked (no dots, dimmed subtext) below the FGA gate.
      make_sp_render <- function(on_col, off_col) {
        on_idx  <- which(names(df_final) == on_col) - 1L
        off_idx <- which(names(df_final) == off_col) - 1L
        on_rank_idx  <- which(names(df_final) == paste0(on_col, "_rank")) - 1L
        off_rank_idx <- which(names(df_final) == paste0(off_col, "_rank")) - 1L
        DT::JS(sprintf(
          "function(data, type, row, meta) {
             if (type !== 'display' || !row) return data;
             var onV = row[%d], offV = row[%d], onPct = row[%d], offPct = row[%d];
             if (data === null || onV === null || offV === null) {
               return '<div class=\"diff-val unranked\">—</div>';
             }
             var d = parseFloat(data);
             var head = (d > 0 ? '+' : '') + d.toFixed(1);
             var onTxt = parseFloat(onV).toFixed(1), offTxt = parseFloat(offV).toFixed(1);
             if (onPct === null || onPct === undefined) {
               return '<div class=\"diff-val unranked\">' + head + '</div>' +
                      '<div class=\"rank-bar-container hidden\"></div>' +
                      '<div class=\"sub-text\" style=\"opacity:0.5;\">' + onTxt + ' | ' + offTxt + '</div>';
             }
             var rangeLineLeft  = Math.min(onPct, offPct);
             var rangeLineWidth = Math.abs(onPct - offPct);
             return '<div class=\"diff-val\">' + head + '</div>' +
                    '<div class=\"rank-bar-container\">' +
                      '<div class=\"rank-track\"></div>' +
                      '<div class=\"range-connect\" style=\"left:' + rangeLineLeft + '%%; width:' + rangeLineWidth + '%%;\"></div>' +
                      '<div class=\"dot-off\" style=\"left:' + offPct + '%%;\" title=\"Off: ' + offTxt + '\"></div>' +
                      '<div class=\"dot-on\" style=\"left:' + onPct + '%%;\" title=\"On: ' + onTxt + '\"></div>' +
                    '</div>' +
                    '<div class=\"sub-text\">' +
                      '<span style=\"font-weight:700; color:#222;\">' + onTxt + '</span>' +
                      ' <span style=\"opacity:0.6;\">|</span> ' +
                      '<span style=\"color:#666;\">' + offTxt + '</span>' +
                    '</div>';
           }", on_idx, off_idx, on_rank_idx, off_rank_idx))
      }

      defs <- list()
      for (i in seq_along(sp_metric_suffix)) {
        m <- sp_metric_suffix[i]
        for (side in c("off", "def")) {
          disp <- paste0(ifelse(side == "off", "Off ", "Def "), ON_SP_LABELS[i], " Diff")
          tgt <- which(names(df_final) == disp) - 1L
          if (!length(tgt)) next
          defs[[length(defs) + 1L]] <- list(
            targets = tgt,
            render = make_sp_render(paste0(side, "_on_", m), paste0(side, "_off_", m))
          )
        }
      }
      hide_idx <- which(names(df_final) %in% c(sp_share_cols, sp_fga_cols, sp_pr_cols, sp_rank_cols)) - 1L
      if (length(hide_idx)) defs[[length(defs) + 1L]] <- list(targets = hide_idx, visible = FALSE)
      sec_idx <- which(names(df_final) %in% c("Off eFG% Diff", "Def eFG% Diff", "minutes")) - 1L
      if (length(sec_idx)) defs[[length(defs) + 1L]] <- list(targets = sec_idx, className = "section-left-border")
      defs[[length(defs) + 1L]] <- list(targets = "_all", className = "dt-center")

      c3_title <- "Corner 3s as % of 3PA with known court location; — = location unknown"
      sketch_sp <- htmltools::withTags(table(class = "display", thead(
        tr(
          th(class = "group-head", colspan = 2, ""),
          th(class = "group-head section-left-border", colspan = 7, "Offense Shot Profile (ON − OFF; eFG% + shares of FGA)"),
          th(class = "group-head section-left-border", colspan = 7, "Defense Shot Profile (ON − OFF; eFG% + shares of FGA)"),
          th(class = "group-head section-left-border", colspan = 3, "Usage")
        ),
        tr(
          th(class = "sub-head", "Team"), th(class = "sub-head", "Player"),
          th(class = "sub-head section-left-border", "eFG%"),
          th(class = "sub-head", "Lay-up"), th(class = "sub-head", "Dunk"),
          th(class = "sub-head", "Lay+Dunk"), th(class = "sub-head", "3PA"),
          th(class = "sub-head", title = c3_title, "Corner 3 Share"), th(class = "sub-head", "2PT Jumper"),
          th(class = "sub-head section-left-border", "eFG%"),
          th(class = "sub-head", "Lay-up"), th(class = "sub-head", "Dunk"),
          th(class = "sub-head", "Lay+Dunk"), th(class = "sub-head", "3PA"),
          th(class = "sub-head", title = c3_title, "Corner 3 Share"), th(class = "sub-head", "2PT Jumper"),
          th(class = "sub-head section-left-border", "Min"), th(class = "sub-head", "On Poss"), th(class = "sub-head", "Off Poss")
        )
      )))

      dt <- datatable(df_final, container = sketch_sp, rownames = FALSE,
                      options = list(headerCallback = HEADER_TOOLTIP_JS, dom = "tip",
                                     pageLength = 30, scrollX = TRUE,
                                     scrollY = "70vh", scrollCollapse = TRUE,
                                     order = list(list(which(names(df_final) == "Off Lay+Dunk Diff") - 1L, "desc")),
                                     columnDefs = defs)) |>
        formatRound(intersect("minutes", names(df_final)), 1) |>
        formatCurrency(intersect(c("ON Poss", "OFF Poss"), names(df_final)),
                       currency = "", interval = 3, mark = ",", digits = 0)

      # Gradient backgrounds by diff percentile: value-hierarchy polarity
      # (eFG/interior/3PA/C3 green-high on offense, red-high on defense; the
      # 2PT Jumper column flips, like TOV% in Four Factors).
      for (i in seq_along(sp_metric_suffix)) {
        m <- sp_metric_suffix[i]
        for (side in c("off", "def")) {
          disp <- paste0(ifelse(side == "off", "Off ", "Def "), ON_SP_LABELS[i], " Diff")
          pr_col <- paste0("pr_sp_", side, "_", m)
          if (!disp %in% names(df_final) || !pr_col %in% names(df_final)) next
          jumper <- identical(m, "mid_share")
          pal <- if ((side == "off") == !jumper) COLS_GRAD else COLS_REV
          dt <- formatStyle(dt, disp, backgroundColor = styleInterval(CUTS, pal), valueColumns = pr_col)
        }
      }
      return(dt)
    }
  }) %>% bindEvent(debounced_range(), debounced_teams(), debounced_on_filters(), gn_params(), input$min_all_poss, input$min_on_poss, input$game_year, input$onoff_view_mode, on_stat_filter_state$filters())

  # ---- Filter Chips ----
  output$on_filter_chips <- renderUI({
    td <- shared$teams_for_year_df()
    team_map <- if (!is.null(td) && nrow(td)) {
      stats::setNames(as.character(td$team_name), as.character(td$team_id))
    } else {
      NULL
    }
    build_filter_chips(
      "on", input, shared$season_date_bounds,
      reset_btn_id = "reset_defaults",
      team_label_map = team_map,
      opponent_label_map = team_map,
      extra_children = stat_filter_chips_ui("on", on_stat_filter_state, on_stat_filter_cols)
    )
  })
  setup_chip_clears("on", session, input, shared,
    game_type_id = "on_game_type", opponents_id = "on_opponents",
    home_away_id = "on_home_away", outcome_id = "on_outcome",
    gn_min_id = "on_gn_min", gn_max_id = "on_gn_max", last_n_id = "on_last_n",
    opp_rank_ids = c("on_opp_rank_side", "on_opp_rank_n", "on_opp_rank_metric"),
    date_id = "date_range", gy_input_id = "game_year",
    teams_ids = "teams",
    starters_ids = c("on_num_starters_off_mode", "on_num_starters_off",
                     "on_num_starters_def_mode", "on_num_starters_def"))
}

