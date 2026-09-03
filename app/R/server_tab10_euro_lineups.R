# server_tab10_euro_lineups.R - Tab 10 server: EuroLeague 2-5 player units.
#
# Tab 8's euro filter plumbing around Tab 2's lineup shape.
#
# Every rate on this tab is derived from summed raw counts AFTER the requested
# games are aggregated. The database stores no ratios by design; never read a
# stored PPP or four-factor value here, and never average per-game rates.

server_tab10_euro_lineups <- function(input, output, session, shared) {

  euro_ld_ref <- reactiveValues(teams = NULL, players = NULL)

  ld_filter <- lineup_player_filter_server(
    "euro_ld_lineup_filter",
    players_ref = reactive(euro_ld_ref$players)
  )

  auto_min_state <- reactiveValues(last_auto = NA_integer_, updating = FALSE)
  auto_enabled <- reactiveVal(TRUE)

  euro_ld_stat_filter_state <- make_stat_filter_state()
  euro_ld_stat_filter_cols <- reactive({
    if (identical(input$euro_ld_view_mode, "Four Factors")) LD_FF_FILTERABLE_COLS
    else LD_SUMMARY_FILTERABLE_COLS
  })
  setup_stat_filter_handlers("euro_ld", input, session,
                             euro_ld_stat_filter_cols, euro_ld_stat_filter_state)

  euro_competition <- shared$euro$competition
  euro_season <- shared$euro$season

  # --- Reference data -------------------------------------------------------
  # Own cache keys throughout. Reusing an Israeli lookup's key would serve one
  # league's teams and players to the other.
  observeEvent(list(input$main_tabs, euro_competition(), euro_season()), {
    if (!identical(input$main_tabs, "euro_lineups")) return(invisible(NULL))
    comp <- euro_competition()
    season <- euro_season()
    if (is.null(comp) || is.null(season) || is.na(season)) return(invisible(NULL))

    teams <- shared$euro$teams_df()
    euro_ld_ref$teams <- teams
    euro_ld_ref$players <- shared$euro$players_df()

    team_choices <- if (!is.null(teams) && nrow(teams)) {
      c(setNames("", "- All teams -"),
        setNames(as.character(teams$team_id), teams$team_name))
    } else {
      setNames("", "- All teams -")
    }
    # A row pivot from EuroLeague On/Off names a team, and for the player
    # action a player as well. NULL means no pivot, so the selection is kept.
    nav <- consume_pending_nav(shared, "euro_lineups")
    nav_team <- if (!is.null(nav)) as.character(nav$team_id %||% "") else ""
    nav_team <- nav_team[nzchar(nav_team)]
    selected_team <- ld_filter$update_team_choices(
      team_choices,
      selected = if (length(nav_team) && nav_team[[1]] %in% unname(team_choices)) nav_team[[1]] else ""
    )
    nav_player <- if (is.null(nav)) NULL else {
      pid <- as.character(nav$player_id %||% "")
      pid[nzchar(pid)]
    }
    ld_filter$refresh_player_choices(team_value = selected_team,
                                     players_on = nav_player)

    opponent_choices <- team_select_choices_with_all(teams, all_label = NULL)
    update_restore_aware_selectize(
      session, input, "euro_ld_opponents", opponent_choices, server = FALSE
    )

    phase_choices <- shared$euro$phase_choices()
    update_restore_aware_selectize(
      session, input, "euro_ld_phase", phase_choices, server = FALSE
    )

    update_gn_last_n_choices(session, "euro_ld", shared$euro$round_values())

    apply_season_date_bounds(session, "euro_ld_date_range", shared$euro$date_bounds())
  }, ignoreInit = FALSE)

  setup_gn_last_n_sync(session, input, "euro_ld")

  observeEvent(input$euro_ld_reset, {
    apply_season_date_bounds(session, "euro_ld_date_range", euro_season_date_bounds(euro_season()))
    updateRadioButtons(session, "euro_ld_group_size", selected = "5")
    ld_filter$reset_inputs(team_selected = "")
    updateSelectizeInput(session, "euro_ld_opponents", selected = character(0))
    updateSelectizeInput(session, "euro_ld_phase", selected = character(0))
    reset_gn_last_n_inputs(session, "euro_ld")
    for (id in c("euro_ld_opp_rank_side", "euro_ld_opp_rank_n",
                 "euro_ld_num_starters_off_mode", "euro_ld_num_starters_off",
                 "euro_ld_num_starters_def_mode", "euro_ld_num_starters_def")) {
      updateSelectInput(session, id, selected = "")
    }
    updateSelectInput(session, "euro_ld_home_away", selected = "")
    updateSelectInput(session, "euro_ld_outcome", selected = "")
    updateSelectInput(session, "euro_ld_opp_rank_metric", selected = "net")
    reset_clutch_inputs(session, "euro_ld")
    auto_enabled(TRUE)
  })

  # --- Filter arguments -----------------------------------------------------
  gn_params <- reactive(resolve_gn_last_n_params(input, "euro_ld"))

  debounced_dates <- reactive(input$euro_ld_date_range) %>% debounce(300)

  build_db_args <- function() {
    filters <- game_context_filter_values(
      input, "euro_ld", game_type_id = "euro_ld_phase"
    )
    context <- game_context_db_args(filters, gn_params())
    clutch <- resolve_clutch_params(
      input$euro_ld_clutch_enabled, input$euro_ld_clutch_margin,
      input$euro_ld_clutch_status, input$euro_ld_clutch_minutes,
      input$euro_ld_clutch_ot_margin
    )
    team_val <- ld_filter$team()
    team_val <- team_val[nzchar(team_val)]
    list(
      team_csv = if (length(team_val)) paste(team_val, collapse = ",") else NA_character_,
      phase_csv = context$game_type_csv,
      opp_ids_csv = context$opp_ids_csv,
      home_away = context$home_away,
      outcome = context$outcome,
      opp_rank_side = context$opp_rank_side,
      opp_rank_n = context$opp_rank_n,
      opp_rank_metric = context$opp_rank_metric,
      max_margin = clutch$max_margin,
      margin_status = clutch$margin_status,
      max_time_remaining = clutch$max_time_remaining,
      ot_margin_filter = clutch$ot_margin_filter,
      min_gn = context$min_gn, max_gn = context$max_gn,
      last_n_games = context$last_n_games,
      num_starters_off_min = context$num_starters_off_min,
      num_starters_off_max = context$num_starters_off_max,
      num_starters_def_min = context$num_starters_def_min,
      num_starters_def_max = context$num_starters_def_max,
      players_on_csv = csv_if_any(ld_filter$players_on()),
      players_off_csv = csv_if_any(ld_filter$players_off()),
      unit_size = as.integer(input$euro_ld_group_size %||% "5")
    )
  }

  # --- Fast path ------------------------------------------------------------
  # sub_lineups_stats_mv can serve competition, season, team, unit size and the
  # players-on/off predicates on its own. What it cannot serve is anything that
  # narrows the SET OF GAMES -- dates, opponent, phase, round range, last-N,
  # home/away, outcome, opponent rank -- or the starter-context bounds, which
  # live on lineup_totals_by_game and never reach the roll-up. Those force the
  # dynamic function.
  #
  # Same gate rule the rest of the project uses: the app always sends dates, so
  # a window equal to the full season counts as "no date filter".
  fallback_needed <- reactive({
    rng <- debounced_dates()
    b <- tryCatch(euro_season_date_bounds(euro_season()), error = function(e) NULL)
    if (is.null(b)) return(FALSE)
    isTRUE(input$euro_ld_clutch_enabled) || onoff_fallback_needed(
      rng, b,
      game_context_filter_values(
        input, "euro_ld", game_type_id = "euro_ld_phase"
      ),
      gn_params(), input, "euro_ld"
    )
  })

  # The whole season's units, cached across sessions on competition + season +
  # the EuroLeague load version -- an Israeli ETL must not invalidate this, and
  # a EuroLeague publication must. Group-size and player filters then apply in
  # R, so switching size costs no query.
  #
  # player_ids arrives as a delimited string rather than an array: PostgreSQL
  # hands arrays back as '{1,2,3}' text that R would have to parse.
  euro_ld_mv <- reactive({
    comp <- euro_competition()
    gy <- as.integer(euro_season())
    req(gy)
    cached_season_df(
      list("euro_sub_lineups_stats_mv", comp, gy, euro_data_version()),
      function() db_get_query(
        pg_pool,
        paste0(
          "SELECT team_id, unit_key, unit_size, player_names_str,",
          " player_ids,",
          " off_poss, off_pts, off_fg2_made, off_fg2_att, off_fg3_made, off_fg3_att,",
          " off_ts_poss, off_fgm, off_fga, off_fta, off_oreb, off_oreb_opp,",
          " off_tov, off_steals,",
          " def_poss, def_pts, def_fg2_made, def_fg2_att, def_fg3_made, def_fg3_att,",
          " def_ts_poss, def_fgm, def_fga, def_fta, def_oreb, def_oreb_opp,",
          " def_tov, def_steals, minutes, starters_poss_num",
          " FROM euroleague.sub_lineups_stats_mv",
          " WHERE competition = $1::text AND game_year = $2::int4"
        ),
        params = list(comp, gy)
      )
    )
  })

  # Delegates to the shared helper Tab 2 already uses in production, so the
  # fast path's player-set semantics are the same implementation rather than a
  # second one that merely looks equivalent: players-on is "contains all"
  # (the SQL @>), players-off is "overlaps none" (NOT &&).
  # Group size is part of the ranking population's definition -- a pair and a
  # quintet are not comparable -- so it narrows BEFORE ranks are computed.
  select_unit_size <- function(df) {
    if (!NROW(df)) return(df)
    df[df$unit_size == as.integer(input$euro_ld_group_size %||% "5"), , drop = FALSE]
  }

  # Team and players-on/off narrow AFTER ranking, so a unit keeps its rank
  # against the whole league rather than against its own team. Delegates to the
  # shared helper Tab 2 already uses in production, so the fast path's set
  # semantics are the same implementation rather than a second one that merely
  # looks equivalent: players-on is "contains all" (the SQL @>), players-off is
  # "overlaps none" (NOT &&).
  apply_local_unit_filters <- function(df) {
    if (!NROW(df)) return(df)
    team_val <- ld_filter$team()
    team_val <- team_val[nzchar(team_val)]
    df <- apply_local_lineup_filters(df, list(
      team_csv       = if (length(team_val)) paste(team_val, collapse = ",") else NA_character_,
      player_csv     = csv_if_any(ld_filter$players_on()),
      player_off_csv = csv_if_any(ld_filter$players_off())
    ))
    df$player_ids_list <- NULL
    df
  }

  # --- Fetch ----------------------------------------------------------------
  # p_min_poss is always 0: ranks and the auto threshold need the complete
  # comparison population. The displayed minimum is applied afterwards.
  #
  # player_ids is deliberately not selected on the filtered path. PostgreSQL
  # returns it as '{1,2,3}' text, and the function already applied the player
  # predicates -- unit_key is the identity, player_names_str is the display.
  euro_ld_filtered <- reactive({
    comp <- euro_competition()
    season <- euro_season()
    dates <- debounced_dates()
    if (is.null(comp) || is.null(season) || is.na(season)) return(data.frame())
    if (is.null(dates) || length(dates) < 2 || any(is.na(dates))) return(data.frame())

    a <- build_db_args()
    allowed <- guard_heavy_request(
      session, key = "tab10_euro_lineups",
      start_d = dates[[1]], end_d = dates[[2]],
      min_gn = a$min_gn, max_gn = a$max_gn, last_n = a$last_n_games,
      max_calls = 35L, window_sec = 60L
    )
    if (!isTRUE(allowed)) return(data.frame())

    # Three readers, chosen by clutch_reader_kind() in helpers.R -- the same
    # classification Tab 9 uses, so the two surfaces cannot drift:
    #   no clutch predicate -> fetch_lineups_pergame (migration 038)
    #   exact 5/all/5:00    -> fetch_lineups_dynamic, which has a cached fact
    #   any other clutch    -> fetch_lineups_direct, one action scan
    #
    # The first branch is new. A filtered but non-clutch request used to reach
    # fetch_lineups_dynamic, which gets the very same per-game fact but through
    # two nested function boundaries, then re-joins that fact on a five-element
    # text[] just to recover columns it already had: 24s for a broad season
    # request the per-game reader answers in 2.1s. Verified identical across 29
    # presets covering all four unit sizes and both player-membership filters.
    kind <- clutch_reader_kind(a)
    reader <- switch(kind,
      pergame = "fetch_lineups_pergame",
      dynamic = "fetch_lineups_dynamic",
      "fetch_lineups_direct"
    )

    # The per-game reader takes 23 parameters because it has no time/margin
    # dimension; the other two keep their existing 27. Signature and parameter
    # list are therefore chosen together, never independently.
    head_params <- list(
      comp, as.integer(season), as.Date(dates[[1]]), as.Date(dates[[2]]),
      NA_character_, a$phase_csv, a$opp_ids_csv, a$home_away, a$outcome,
      a$opp_rank_side, a$opp_rank_n, a$opp_rank_metric
    )
    tail_params <- list(
      a$min_gn, a$max_gn, a$last_n_games,
      a$num_starters_off_min, a$num_starters_off_max,
      a$num_starters_def_min, a$num_starters_def_max,
      a$unit_size,
      # team and players-on/off are deliberately NOT sent: ranks must be
      # computed over the full population for the selected games, exactly as
      # Tab 2 does. They are applied locally afterwards.
      NA_character_, NA_character_,
      0L
    )
    if (identical(kind, "pergame")) {
      sig <- paste0(
        "$1::text,$2::int4,$3::date,$4::date,$5::text,$6::text,$7::text,",
        "$8::text,$9::text,$10::text,$11::int4,$12::text,",
        "$13::int4,$14::int4,$15::int4,",
        "$16::int4,$17::int4,$18::int4,$19::int4,",
        "$20::int4,$21::text,$22::text,$23::int4"
      )
      params <- c(head_params, tail_params)
    } else {
      sig <- paste0(
        "$1::text,$2::int4,$3::date,$4::date,$5::text,$6::text,$7::text,",
        "$8::text,$9::text,$10::text,$11::int4,$12::text,",
        "$13::int4,$14::text,$15::int4,$16::bool,",
        "$17::int4,$18::int4,$19::int4,",
        "$20::int4,$21::int4,$22::int4,$23::int4,",
        "$24::int4,$25::text,$26::text,$27::int4"
      )
      params <- c(
        head_params,
        list(a$max_margin, a$margin_status,
             a$max_time_remaining, a$ot_margin_filter),
        tail_params
      )
    }

    db_get_query(
      pg_pool,
      paste0(
        "SELECT team_id, unit_key, unit_size, player_names_str,",
        " off_poss, off_pts, off_fg2_made, off_fg2_att, off_fg3_made, off_fg3_att,",
        " off_ts_poss, off_fgm, off_fga, off_fta, off_oreb, off_oreb_opp,",
        " off_tov, off_steals,",
        " def_poss, def_pts, def_fg2_made, def_fg2_att, def_fg3_made, def_fg3_att,",
        " def_ts_poss, def_fgm, def_fga, def_fta, def_oreb, def_oreb_opp,",
        " def_tov, def_steals, minutes, starters_poss_num",
        " FROM euroleague.", reader, "(", sig, ")"
      ),
      params = params
    )
  })

  # The branch. Both paths return the same column names, so everything
  # downstream -- rates, ranks, TOTAL row, table -- is identical either way.
  # Both return the FULL population for the selected games and group size.
  euro_ld_raw <- reactive({
    if (isTRUE(fallback_needed())) return(select_unit_size(euro_ld_filtered()))
    select_unit_size(euro_ld_mv())
  })

  # --- Derived rates --------------------------------------------------------
  # Every denominator is guarded: a zero denominator is NA, never 0 and never a
  # midpoint. Computed from summed counts, after aggregation.
  safe_rate <- function(num, den, scale = 100) {
    num <- as.numeric(num); den <- as.numeric(den)
    ifelse(is.na(den) | den <= 0, NA_real_, scale * num / den)
  }

  add_rates <- function(df) {
    if (!NROW(df)) return(df)
    df$total_poss <- as.numeric(df$off_poss) + as.numeric(df$def_poss)
    df$off_ppp <- safe_rate(df$off_pts, df$off_poss)
    df$def_ppp <- safe_rate(df$def_pts, df$def_poss)
    df$net_rtg <- df$off_ppp - df$def_ppp
    df$off_ts  <- safe_rate(df$off_pts, 2 * as.numeric(df$off_ts_poss))
    df$def_ts  <- safe_rate(df$def_pts, 2 * as.numeric(df$def_ts_poss))
    df$off_tov_pct  <- safe_rate(df$off_tov, df$off_poss)
    df$def_tov_pct  <- safe_rate(df$def_tov, df$def_poss)
    df$off_oreb_pct <- safe_rate(df$off_oreb, df$off_oreb_opp)
    df$def_oreb_pct <- safe_rate(df$def_oreb, df$def_oreb_opp)
    df$off_ftr <- safe_rate(df$off_fta, df$off_fga)
    df$def_ftr <- safe_rate(df$def_fta, df$def_fga)
    df$off_efg <- safe_rate(as.numeric(df$off_fgm) + 0.5 * as.numeric(df$off_fg3_made), df$off_fga)
    df$def_efg <- safe_rate(as.numeric(df$def_fgm) + 0.5 * as.numeric(df$def_fg3_made), df$def_fga)
    df
  }

  # Rates and percentile ranks are computed here, on the full unfiltered
  # population, before any team/player/min-poss narrowing -- the same ordering
  # Tab 2 uses. Ranking after filtering would silently re-scale every heat cell
  # to whatever subset happened to be on screen.
  euro_ld_full <- reactive({
    df <- euro_ld_raw()
    if (!NROW(df)) return(df)
    df <- add_rates(df)
    teams <- euro_ld_ref$teams
    if (!is.null(teams) && nrow(teams)) {
      df$team_name <- teams$team_name[match(df$team_id, teams$team_id)]
    } else {
      df$team_name <- as.character(df$team_id)
    }
    df <- add_pct_ranks(df, c(SUMMARY_RANKS, FF_RANKS))
    df[order(-df$total_poss), , drop = FALSE]
  })

  # --- Auto minimum possessions --------------------------------------------
  # Computed on the team/player-filtered population BEFORE the min-poss filter.
  # Manual slider use switches to manual; a filter change returns to auto. The
  # `updating` flag stops an auto-driven slider update reading as a manual one.
  euro_ld_auto_inputs <- reactive({
    list(input$main_tabs, euro_competition(), euro_season(),
         input$euro_ld_group_size, ld_filter$team(),
         ld_filter$players_on(), ld_filter$players_off(),
         debounced_dates(), input$euro_ld_opponents, input$euro_ld_phase,
         input$euro_ld_home_away, input$euro_ld_outcome,
         input$euro_ld_opp_rank_side, input$euro_ld_opp_rank_n,
         input$euro_ld_opp_rank_metric, input$euro_ld_view_mode,
         input$euro_ld_num_starters_off_mode, input$euro_ld_num_starters_off,
         input$euro_ld_num_starters_def_mode, input$euro_ld_num_starters_def,
         input$euro_ld_clutch_enabled, input$euro_ld_clutch_margin,
         input$euro_ld_clutch_status, input$euro_ld_clutch_minutes,
         input$euro_ld_clutch_ot_margin,
         input$euro_ld_gn_min, input$euro_ld_gn_max, input$euro_ld_last_n)
  })

  # Register this before the calculation observer, matching Tab 2: a dataset-
  # shaping filter first returns the control to auto mode, then recalculates it.
  observeEvent(list(input$euro_ld_group_size, ld_filter$team(),
                    ld_filter$players_on(), ld_filter$players_off(),
                    debounced_dates(), input$euro_ld_opponents,
                    input$euro_ld_phase, input$euro_ld_home_away,
                    input$euro_ld_outcome, input$euro_ld_opp_rank_side,
                    input$euro_ld_opp_rank_n, input$euro_ld_opp_rank_metric,
                    input$euro_ld_view_mode,
                    input$euro_ld_num_starters_off_mode,
                    input$euro_ld_num_starters_off,
                    input$euro_ld_num_starters_def_mode,
                    input$euro_ld_num_starters_def,
                    input$euro_ld_clutch_enabled, input$euro_ld_clutch_margin,
                    input$euro_ld_clutch_status, input$euro_ld_clutch_minutes,
                    input$euro_ld_clutch_ot_margin,
                    input$euro_ld_gn_min, input$euro_ld_gn_max,
                    input$euro_ld_last_n), {
    auto_enabled(TRUE)
  }, ignoreInit = TRUE)

  observeEvent(euro_ld_auto_inputs(), {
    req(identical(input$main_tabs, "euro_lineups"))
    df <- apply_local_unit_filters(euro_ld_full())
    if (!NROW(df)) return(invisible(NULL))
    if (!isTRUE(auto_enabled())) return(invisible(NULL))
    target <- auto_minposs_from_df(df, usage_col = "total_poss", step = 10L)
    if (is.na(target)) return(invisible(NULL))
    max_poss <- max(c(as.numeric(df$total_poss), 0), na.rm = TRUE)
    auto_min_state$updating <- TRUE
    auto_min_state$last_auto <- target
    updateSliderInput(session, "euro_ld_minposs",
                      value = target,
                      max = max(as.integer(ceiling(max_poss / 10) * 10), 10L))
    session$onFlushed(function() auto_min_state$updating <- FALSE, once = TRUE)
  })

  observeEvent(input$euro_ld_minposs, {
    if (isTRUE(auto_min_state$updating)) return(invisible(NULL))
    if (identical(as.integer(input$euro_ld_minposs), auto_min_state$last_auto)) {
      return(invisible(NULL))
    }
    auto_enabled(FALSE)
  }, ignoreInit = TRUE)

  # Tab 2's column contract. The count columns are COPIED, not renamed: the
  # Summary view still reads off_fg3_made for its shot splits, and off_fga /
  # off_oreb are still the FTR and OREB denominators. Copying first also makes
  # the ordering constraint structural rather than a rule to remember --
  # off_tov is a raw count here and a rate on Tab 2, so deriving the rate
  # before copying the count would leave the FF TOTAL row dividing a rate by
  # possessions.
  to_tab2_contract <- function(df) {
    if (!NROW(df)) return(df)
    df$Team            <- df$team_name
    df$Players         <- df$player_names_str
    df$sub_lineup_hash <- df$unit_key
    # Possession-weighted mean of own starters on court, weighted by
    # offensive AND defensive possessions -- the same definition the Israeli
    # fetch_lineups_all uses. The read layer returns the numerator, never a
    # stored ratio, so the division happens here with the rest of the rates.
    df$num_starters    <- safe_rate(df$starters_poss_num,
                                    as.numeric(df$off_poss) + as.numeric(df$def_poss),
                                    scale = 1)
    df$plus_minus      <- as.numeric(df$off_pts) - as.numeric(df$def_pts)

    # --- raw counts the FF TOTAL row sums (copies) ---
    df$off_oreb_cnt  <- df$off_oreb
    df$off_oreb_opps <- df$off_oreb_opp
    df$off_tov_cnt   <- df$off_tov
    df$off_fga_cnt   <- df$off_fga
    df$off_fgm_cnt   <- df$off_fgm
    df$off_fg3m_cnt  <- df$off_fg3_made
    df$def_oreb_cnt  <- df$def_oreb
    df$def_oreb_opps <- df$def_oreb_opp
    df$def_tov_cnt   <- df$def_tov
    df$def_fga_cnt   <- df$def_fga
    df$def_fgm_cnt   <- df$def_fgm
    df$def_fg3m_cnt  <- df$def_fg3_made
    # off_ts_poss, off_fta, off_pts, def_pts, minutes already carry Tab 2's names.

    # --- rates, LAST: these overwrite off_oreb / off_tov ---
    df$off_oreb <- df$off_oreb_pct
    df$def_oreb <- df$def_oreb_pct
    df$off_tov  <- df$off_tov_pct
    df$def_tov  <- df$def_tov_pct
    # off_efg, def_efg, off_ftr, def_ftr, off_ppp, def_ppp, net_rtg,
    # total_poss, off_poss, def_poss, minutes already carry Tab 2's names.
    df
  }

  # --- Displayed rows --------------------------------------------------------
  # The shared renderers (lineup_summary_datatable() / lineup_ff_datatable())
  # build their own TOTAL row from the filtered population, exactly as Tab 2
  # does -- this reactive must not build one of its own.
  euro_ld_display <- reactive({
    df <- apply_local_unit_filters(euro_ld_full())
    if (!NROW(df)) return(df)
    threshold <- as.numeric(input$euro_ld_minposs %||% 0)
    df <- df[!is.na(df$total_poss) & df$total_poss >= threshold, , drop = FALSE]
    if (!NROW(df)) return(df)
    to_tab2_contract(df)
  })

  # --- Table ----------------------------------------------------------------
  # Percentile ranks drive the heat colouring, exactly as Tabs 1/3/8 do:
  # a hidden pr_* column in [0,1] feeds formatStyle via valueColumns.
  #
  # Small-sample units are left unranked (NA), so their cells render uncoloured
  # rather than implying a confident reading. The threshold comes from
  # adaptive_baseline(), as Tab 2 uses: a fixed RANKING_BASELINE leaves almost
  # everything grey when the population is sparse, which 2-player units at a
  # single group size can be. The TOTAL row is never ranked.
  add_pct_ranks <- function(df, specs) {
    thresh <- adaptive_baseline(df$total_poss)
    eligible <- !is.na(df$total_poss) & df$total_poss >= thresh &
      !is.na(df$unit_key)
    for (nm in names(specs)) {
      spec <- specs[[nm]]
      vals <- suppressWarnings(as.numeric(df[[spec$src]]))
      vals[!eligible] <- NA_real_
      pr <- rep(NA_real_, length(vals))
      ok <- !is.na(vals)
      if (sum(ok) > 1L) {
        pr[ok] <- (rank(vals[ok], ties.method = "average") - 1) / (sum(ok) - 1)
      } else if (sum(ok) == 1L) {
        pr[ok] <- 0.5
      }
      if (isTRUE(spec$invert)) pr <- 1 - pr
      df[[nm]] <- pr
    }
    df
  }

  # Polarity is baked into the rank, not chosen at render time: the shared
  # renderer applies COLS_GRAD to every pr_* column, exactly as Tab 2 does.
  SUMMARY_RANKS <- list(
    pr_ld_off_ppp   = list(src = "off_ppp"),
    pr_ld_def_ppp_i = list(src = "def_ppp", invert = TRUE),
    pr_ld_net       = list(src = "net_rtg")
  )
  FF_RANKS <- list(
    pr_off_ppp  = list(src = "off_ppp"),
    pr_off_efg  = list(src = "off_efg"),
    pr_off_oreb = list(src = "off_oreb_pct"),
    pr_off_tov  = list(src = "off_tov_pct", invert = TRUE),
    pr_off_ftr  = list(src = "off_ftr"),
    pr_def_ppp  = list(src = "def_ppp",     invert = TRUE),
    pr_def_efg  = list(src = "def_efg",     invert = TRUE),
    pr_def_oreb = list(src = "def_oreb_pct", invert = TRUE),
    pr_def_tov  = list(src = "def_tov_pct"),
    pr_def_ftr  = list(src = "def_ftr",     invert = TRUE),
    pr_net      = list(src = "net_rtg")
  )

  output$euro_ld_dt <- renderDT({
    df <- euro_ld_display()
    if (!NROW(df)) {
      return(datatable(data.frame(Message = "No units match these filters."),
                       rownames = FALSE, options = list(dom = "t")))
    }
    if (identical(input$euro_ld_view_mode, "Four Factors")) {
      # raw = df is explicit, not left to the default: R default-argument
      # promises evaluate lazily in the CALLEE's frame, so an implicit
      # raw = df would be forced only deep inside the TOTAL-row block, by
      # which point lineup_ff_datatable() has already reassigned its own
      # df via select()/arrange()/apply_stat_filters() and the count columns
      # (off_pts, off_ts_poss, off_oreb_cnt, ...) would be gone -- summing
      # them would silently yield 0 instead of erroring. Passing raw = df
      # here creates the promise in THIS frame, where df is still
      # euro_ld_display()'s unmutated output. Mirrors server_tab2.R's
      # lineup_ff_datatable(..., raw = ld_data()) call.
      lineup_ff_datatable(df, euro_ld_stat_filter_state$filters(),
                          EURO_LD_LINEUP_TABLE_SPEC, raw = df)
    } else {
      lineup_summary_datatable(df, euro_ld_stat_filter_state$filters(),
                               EURO_LD_LINEUP_TABLE_SPEC)
    }
  })

  # --- Filter chips ---------------------------------------------------------
  # Tab 2's chip bar. This tab previously showed only a hand-rolled summary
  # line, so the schedule filters it does send to SQL -- dates, phase,
  # opponents, home/away, outcome, rounds, last-N, starters, opponent rank --
  # were never visible. The unit size, row count and min-possessions readout
  # are not filters that clear individually, so they stay as extra children.
  output$euro_ld_filter_chips <- renderUI({
    teams <- euro_ld_ref$teams
    team_map <- if (!is.null(teams) && nrow(teams)) {
      stats::setNames(as.character(teams$team_name), as.character(teams$team_id))
    } else NULL

    player_map <- NULL
    if (!is.null(euro_ld_ref$players) && nrow(euro_ld_ref$players)) {
      tid <- suppressWarnings(as.integer(ld_filter$team()))
      pmap <- euro_ld_ref$players
      if (length(tid) == 1L && !is.na(tid)) pmap <- pmap[pmap$team_id == tid, , drop = FALSE]
      player_map <- stats::setNames(as.character(pmap$name), as.character(pmap$player_id))
    }

    n_units <- NROW(euro_ld_display())
    bits <- c(sprintf("%s-player units", input$euro_ld_group_size %||% "5"),
              sprintf("%d shown", n_units))
    if (!is.null(input$euro_ld_minposs) && input$euro_ld_minposs > 0) {
      bits <- c(bits, sprintf("min %d poss%s", as.integer(input$euro_ld_minposs),
                              if (isTRUE(auto_enabled())) " (auto)" else ""))
    }

    season <- euro_season()
    build_filter_chips(
      "euro_ld", input, euro_season_date_bounds,
      reset_btn_id = "euro_ld_reset",
      team_label_map = team_map,
      opponent_label_map = team_map,
      player_label_map = player_map,
      teams_value = ld_filter$team(),
      players_on_value = ld_filter$players_on(),
      players_off_value = ld_filter$players_off(),
      input_ids = list(
        teams = "euro_ld_filter-team",
        players_on = "euro_ld_filter-players_on",
        players_off = "euro_ld_filter-players_off"
      ),
      season_value = season,
      season_label = paste(EURO_COMPETITION_LABELS[[euro_competition()]] %||% euro_competition(),
                           euro_season_label(season)),
      date_input_id = "euro_ld_date_range",
      game_type_input_id = "euro_ld_phase",
      game_type_labeller = euro_phase_label,
      gn_label = "Rd",
      extra_children = c(
        lapply(bits, function(b) tags$span(class = "filter-chip", b)),
        stat_filter_chips_ui("euro_ld", euro_ld_stat_filter_state,
                             euro_ld_stat_filter_cols)
      )
    )
  })

  setup_chip_clears("euro_ld", session, input, shared,
    game_type_id = "euro_ld_phase", opponents_id = "euro_ld_opponents",
    home_away_id = "euro_ld_home_away", outcome_id = "euro_ld_outcome",
    gn_min_id = "euro_ld_gn_min", gn_max_id = "euro_ld_gn_max",
    last_n_id = "euro_ld_last_n",
    opp_rank_ids = c("euro_ld_opp_rank_side", "euro_ld_opp_rank_n"),
    date_id = "euro_ld_date_range", gy_input_id = "euro_game_year",
    teams_ids = "euro_ld_lineup_filter-team",
    starters_ids = c("euro_ld_num_starters_off_mode", "euro_ld_num_starters_off",
                     "euro_ld_num_starters_def_mode", "euro_ld_num_starters_def"),
    clutch_enabled_id = "euro_ld_clutch_enabled",
    bounds_fn = euro_season_date_bounds)

  observeEvent(input$euro_ld_clear_players_on, {
    updateSelectizeInput(session, "euro_ld_lineup_filter-players_on", selected = character(0))
  }, ignoreInit = TRUE)
  observeEvent(input$euro_ld_clear_players_off, {
    updateSelectizeInput(session, "euro_ld_lineup_filter-players_off", selected = character(0))
  }, ignoreInit = TRUE)

  # --- Lineup game log modal -----------------------------------------------
  # This is what keeping game_id in lineup_totals_by_game's key buys: the
  # per-game rows already exist, so the modal needs no new relation.
  #
  # sub_lineups's primary key gives one row per (lineup_key, unit_key), so this
  # join contributes each of the unit's games exactly once.
  observeEvent(input$euro_ld_clicked_unit, ignoreInit = TRUE, {
    unit <- as.character(input$euro_ld_clicked_unit %||% "")
    if (!nzchar(unit)) return(invisible(NULL))

    rows <- tryCatch(db_get_query(
      pg_pool,
      "SELECT f.game_date, f.round_number, f.opp_team_name, f.is_home,
              sum(l.possessions) FILTER (WHERE l.type_lineup = 'offense') AS off_poss,
              sum(l.points)      FILTER (WHERE l.type_lineup = 'offense') AS off_pts,
              sum(l.possessions) FILTER (WHERE l.type_lineup = 'defense') AS def_poss,
              sum(l.points)      FILTER (WHERE l.type_lineup = 'defense') AS def_pts,
              round(sum(l.seconds) FILTER (WHERE l.type_lineup = 'offense') / 60.0, 1) AS minutes
         FROM euroleague.sub_lineups sl
         JOIN euroleague.lineup_totals_by_game l
           ON l.competition = sl.competition AND l.game_year = sl.game_year
          AND l.team_id = sl.team_id AND l.lineup_key = sl.lineup_key
         JOIN euroleague.final_schedule_mv f
           ON f.game_id = l.game_id AND f.team_id = l.team_id
        WHERE sl.competition = $1::text AND sl.game_year = $2::int4
          AND sl.unit_key = $3::text
        GROUP BY f.game_date, f.round_number, f.opp_team_name, f.is_home
        ORDER BY f.game_date",
      params = list(euro_competition(), as.integer(euro_season()), unit)
    ), error = function(e) NULL)

    if (is.null(rows) || !NROW(rows)) {
      showModal(modalDialog(title = "Lineup game log", easyClose = TRUE,
                            "No games found for this unit."))
      return(invisible(NULL))
    }

    rows$off_ppp <- round(safe_rate(rows$off_pts, rows$off_poss), 1)
    rows$def_ppp <- round(safe_rate(rows$def_pts, rows$def_poss), 1)
    rows$net <- round(rows$off_ppp - rows$def_ppp, 1)
    rows$plus_minus <- as.numeric(rows$off_pts) - as.numeric(rows$def_pts)
    rows$total_poss <- as.numeric(rows$off_poss) + as.numeric(rows$def_poss)
    rows$venue <- ifelse(isTRUE(rows$is_home) | rows$is_home %in% TRUE, "H", "A")
    show <- rows[, c("game_date", "round_number", "opp_team_name", "venue",
                     "minutes", "total_poss", "off_ppp", "def_ppp", "net",
                     "off_poss", "def_poss", "off_pts", "def_pts", "plus_minus")]
    names(show) <- c("Date", "Rd", "Opponent", "H/A", "Min",
                     "Total Poss", "Off PPP", "Def PPP", "Net", "Off Poss",
                     "Def Poss", "Off Pts", "Def Pts", "+/-")

    showModal(modalDialog(
      title = "Lineup game log",
      size = "l",
      easyClose = TRUE,
      renderDT(datatable(show, rownames = FALSE,
                         options = list(pageLength = 25, dom = "t", scrollX = TRUE)))
    ))
  })
}
