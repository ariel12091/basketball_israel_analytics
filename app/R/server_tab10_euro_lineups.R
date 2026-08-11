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

  euro_competition <- reactive(euro_selected_competition(input))
  euro_season <- reactive(euro_selected_game_year(input))

  # --- Reference data -------------------------------------------------------
  # Own cache keys throughout. Reusing an Israeli lookup's key would serve one
  # league's teams and players to the other.
  observeEvent(list(euro_competition(), euro_season()), {
    comp <- euro_competition()
    season <- euro_season()
    if (is.null(comp) || is.null(season) || is.na(season)) return(invisible(NULL))

    teams <- tryCatch(euro_fetch_teams(comp, season), error = function(e) NULL)
    euro_ld_ref$teams <- teams
    euro_ld_ref$players <- tryCatch(euro_fetch_players_basic(comp, season),
                                    error = function(e) NULL)

    team_choices <- if (!is.null(teams) && nrow(teams)) {
      c(setNames("", "- All teams -"),
        setNames(as.character(teams$team_id), teams$team_name))
    } else {
      setNames("", "- All teams -")
    }
    selected_team <- ld_filter$update_team_choices(team_choices)
    ld_filter$refresh_player_choices(team_value = selected_team)

    updateSelectizeInput(session, "euro_ld_opponents",
                         choices = team_select_choices_with_all(teams, all_label = NULL),
                         selected = character(0), server = FALSE)

    updateSelectizeInput(session, "euro_ld_phase",
                         choices = euro_phase_choices(comp, season),
                         selected = character(0), server = FALSE)

    rounds <- tryCatch(euro_fetch_round_values(comp, season), error = function(e) NULL)
    update_gn_last_n_choices(session, "euro_ld", rounds$gn)

    apply_season_date_bounds(session, "euro_ld_date_range", euro_season_date_bounds(season))
  }, ignoreInit = FALSE)

  setup_gn_last_n_sync(session, input, "euro_ld")

  observeEvent(input$euro_ld_reset, {
    apply_season_date_bounds(session, "euro_ld_date_range", euro_season_date_bounds(euro_season()))
    updateSelectInput(session, "euro_ld_group_size", selected = "5")
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
    onoff_fallback_needed(
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
          " def_tov, def_steals, minutes",
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

    db_get_query(
      pg_pool,
      paste0(
        "SELECT team_id, unit_key, unit_size, player_names_str,",
        " off_poss, off_pts, off_fg2_made, off_fg2_att, off_fg3_made, off_fg3_att,",
        " off_ts_poss, off_fgm, off_fga, off_fta, off_oreb, off_oreb_opp,",
        " off_tov, off_steals,",
        " def_poss, def_pts, def_fg2_made, def_fg2_att, def_fg3_made, def_fg3_att,",
        " def_ts_poss, def_fgm, def_fga, def_fta, def_oreb, def_oreb_opp,",
        " def_tov, def_steals, minutes",
        " FROM euroleague.fetch_lineups_dynamic(",
        "$1::text,$2::int4,$3::date,$4::date,$5::text,$6::text,$7::text,",
        "$8::text,$9::text,$10::text,$11::int4,$12::text,",
        "$13::int4,$14::int4,$15::int4,",
        "$16::int4,$17::int4,$18::int4,$19::int4,",
        "$20::int4,$21::text,$22::text,$23::int4)"
      ),
      params = list(
        comp, as.integer(season), as.Date(dates[[1]]), as.Date(dates[[2]]),
        NA_character_, a$phase_csv, a$opp_ids_csv, a$home_away, a$outcome,
        a$opp_rank_side, a$opp_rank_n, a$opp_rank_metric,
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
    list(euro_ld_full(), input$euro_ld_group_size, ld_filter$team(),
         ld_filter$players_on(), ld_filter$players_off(),
         debounced_dates(), input$euro_ld_opponents, input$euro_ld_phase,
         input$euro_ld_home_away, input$euro_ld_outcome,
         input$euro_ld_opp_rank_side, input$euro_ld_opp_rank_n,
         input$euro_ld_opp_rank_metric, input$euro_ld_view_mode,
         input$euro_ld_num_starters_off_mode, input$euro_ld_num_starters_off,
         input$euro_ld_num_starters_def_mode, input$euro_ld_num_starters_def,
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
                    input$euro_ld_gn_min, input$euro_ld_gn_max,
                    input$euro_ld_last_n), {
    auto_enabled(TRUE)
  }, ignoreInit = TRUE)

  observeEvent(euro_ld_auto_inputs(), {
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

  # --- Displayed rows, with the TOTAL row pinned on top ---------------------
  euro_ld_display <- reactive({
    df <- apply_local_unit_filters(euro_ld_full())
    if (!NROW(df)) return(df)
    threshold <- as.numeric(input$euro_ld_minposs %||% 0)
    df <- df[!is.na(df$total_poss) & df$total_poss >= threshold, , drop = FALSE]
    if (!NROW(df)) return(df)

    # TOTAL sums the raw counts and derives its rates from those sums. It is
    # not an average of the rows' rates, and it is not clickable.
    count_cols <- c("off_poss", "off_pts", "off_fg2_made", "off_fg2_att",
                    "off_fg3_made", "off_fg3_att", "off_ts_poss", "off_fgm",
                    "off_fga", "off_fta", "off_oreb", "off_oreb_opp",
                    "off_tov", "off_steals",
                    "def_poss", "def_pts", "def_fg2_made", "def_fg2_att",
                    "def_fg3_made", "def_fg3_att", "def_ts_poss", "def_fgm",
                    "def_fga", "def_fta", "def_oreb", "def_oreb_opp",
                    "def_tov", "def_steals", "minutes")
    total <- df[1, , drop = FALSE]
    for (col in count_cols) total[[col]] <- sum(as.numeric(df[[col]]), na.rm = TRUE)
    total$unit_key <- NA_character_
    total$team_id <- NA_integer_
    total$team_name <- "TOTAL"
    total$player_names_str <- "TOTAL"
    total <- add_rates(total)
    rbind(total, df)
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
      src <- specs[[nm]]
      vals <- suppressWarnings(as.numeric(df[[src]]))
      vals[!eligible] <- NA_real_
      pr <- rep(NA_real_, length(vals))
      ok <- !is.na(vals)
      if (sum(ok) > 1L) {
        pr[ok] <- (rank(vals[ok], ties.method = "average") - 1) / (sum(ok) - 1)
      } else if (sum(ok) == 1L) {
        pr[ok] <- 0.5
      }
      df[[nm]] <- pr
    }
    df
  }

  # Column polarity follows the project's standing rule: offense is green-high
  # except TOV%, defense is red-high except TOV%.
  SUMMARY_RANKS <- list(pr_off_ppp = "off_ppp", pr_def_ppp = "def_ppp",
                        pr_net = "net_rtg", pr_off_efg = "off_efg",
                        pr_def_efg = "def_efg")
  FF_RANKS <- list(pr_off_ts = "off_ts", pr_off_tov = "off_tov_pct",
                   pr_off_oreb = "off_oreb_pct", pr_off_ftr = "off_ftr",
                   pr_def_ts = "def_ts", pr_def_tov = "def_tov_pct",
                   pr_def_oreb = "def_oreb_pct", pr_def_ftr = "def_ftr")

  summary_cols <- c(
    team_name = "Team", player_names_str = "Unit",
    off_ppp = "Off PPP", def_ppp = "Def PPP", net_rtg = "Net Rtg",
    off_efg = "Off eFG%", def_efg = "Def eFG%",
    minutes = "Min", off_poss = "Off Poss", def_poss = "Def Poss"
  )
  ff_cols <- c(
    team_name = "Team", player_names_str = "Unit",
    off_ts = "Off TS%", off_tov_pct = "Off TOV%",
    off_oreb_pct = "Off OREB%", off_ftr = "Off FTR",
    def_ts = "Def TS%", def_tov_pct = "Def TOV%",
    def_oreb_pct = "Def OREB%", def_ftr = "Def FTR",
    minutes = "Min", off_poss = "Off Poss", def_poss = "Def Poss"
  )

  sketch_summary <- htmltools::withTags(table(class = "display", thead(
    tr(
      th(class = "group-head", colspan = 2, ""),
      th(class = "group-head section-left-border", colspan = 3, "Ratings"),
      th(class = "group-head section-left-border", colspan = 2, "Shooting"),
      th(class = "group-head section-left-border", colspan = 3, "Usage")
    ),
    tr(
      th(class = "sub-head", "Team"), th(class = "sub-head", "Unit"),
      th(class = "sub-head section-left-border", "Off PPP"),
      th(class = "sub-head", "Def PPP"), th(class = "sub-head", "Net Rtg"),
      th(class = "sub-head section-left-border", "Off eFG%"),
      th(class = "sub-head", "Def eFG%"),
      th(class = "sub-head section-left-border", "Min"),
      th(class = "sub-head", "Off Poss"), th(class = "sub-head", "Def Poss")
    )
  )))

  sketch_ff <- htmltools::withTags(table(class = "display", thead(
    tr(
      th(class = "group-head", colspan = 2, ""),
      th(class = "group-head section-left-border", colspan = 4, "Offense"),
      th(class = "group-head section-left-border", colspan = 4, "Defense"),
      th(class = "group-head section-left-border", colspan = 3, "Usage")
    ),
    tr(
      th(class = "sub-head", "Team"), th(class = "sub-head", "Unit"),
      th(class = "sub-head section-left-border", "TS%"),
      th(class = "sub-head", "TOV%"), th(class = "sub-head", "OREB%"),
      th(class = "sub-head", "FTR"),
      th(class = "sub-head section-left-border", "TS%"),
      th(class = "sub-head", "TOV%"), th(class = "sub-head", "OREB%"),
      th(class = "sub-head", "FTR"),
      th(class = "sub-head section-left-border", "Min"),
      th(class = "sub-head", "Off Poss"), th(class = "sub-head", "Def Poss")
    )
  )))

  output$euro_ld_dt <- renderDT({
    df <- euro_ld_display()
    is_ff <- identical(input$euro_ld_view_mode, "Four Factors")
    cols   <- if (is_ff) ff_cols   else summary_cols
    sketch <- if (is_ff) sketch_ff else sketch_summary
    ranks  <- if (is_ff) FF_RANKS  else SUMMARY_RANKS

    if (!NROW(df)) {
      return(datatable(data.frame(Message = "No units match these filters."),
                       rownames = FALSE, options = list(dom = "t")))
    }

    out <- df[, names(cols), drop = FALSE]
    names(out) <- unname(cols)
    # unit_key and the pr_* columns ride along hidden. Hidden columns beyond
    # the sketch's th count get auto-generated headers, so they need no entry
    # in the container.
    out$unit_ref <- ifelse(is.na(df$unit_key), "", df$unit_key)
    for (nm in names(ranks)) out[[nm]] <- df[[nm]]

    hide_idx  <- which(names(out) %in% c("unit_ref", names(ranks))) - 1L
    unit_idx  <- which(names(out) == "Unit") - 1L
    key_idx   <- which(names(out) == "unit_ref") - 1L
    border_at <- if (is_ff) c("Off TS%", "Def TS%", "Min") else
                            c("Off PPP", "Off eFG%", "Min")
    section_borders <- which(names(out) %in% border_at) - 1L

    # Escaping stays on. Provider-supplied player names reach this table, so
    # the data must be escaped; the unit link and the bold TOTAL come from the
    # columnDefs render function, whose markup DataTables inserts regardless.
    dt <- datatable(
      out,
      container = sketch,
      rownames = FALSE,
      escape = dt_escape_except(out),
      selection = "none",
      # Delegated on the table, not an inline onclick: DataTables re-creates
      # the row elements on every sort, page and redraw, so a handler bound to
      # the anchors themselves would stop firing after the first interaction.
      callback = DT::JS(
        "table.on('click', 'a.euro-ld-unit', function(e) {",
        "  e.preventDefault();",
        "  var key = this.getAttribute('data-unit');",
        "  if (!key) return;",
        "  Shiny.setInputValue('euro_ld_clicked_unit', key, {priority: 'event'});",
        "});"
      ),
      options = list(
        headerCallback = HEADER_TOOLTIP_JS,
        dom = "tip",
        pageLength = 30,
        scrollX = TRUE,
        scrollY = "70vh",
        scrollCollapse = TRUE,
        order = list(list(which(names(out) == "Off Poss") - 1L, "desc")),
        columnDefs = list(
          list(targets = hide_idx, visible = FALSE),
          list(targets = section_borders, className = "section-left-border"),
          list(targets = "_all", className = "dt-center"),
          list(
            targets = unit_idx,
            className = "dt-left",
            render = DT::JS(
              "function(data, type, row, meta) {",
              "  if (type !== 'display' || !row) return data;",
              sprintf("  var key = row[%d];", key_idx),
              "  if (!key) return '<strong>' + data + '</strong>';",
              "  return '<a href=\"#\" class=\"euro-ld-unit\" data-unit=\"' + key + '\">' + data + '</a>';",
              "}"
            )
          )
        )
      )
    ) |>
      formatRound(intersect(names(out),
                            c("Off PPP", "Def PPP", "Net Rtg", "Off eFG%",
                              "Def eFG%", "Off TS%", "Def TS%", "Off TOV%",
                              "Def TOV%", "Off OREB%", "Def OREB%",
                              "Off FTR", "Def FTR", "Min")), 1) |>
      formatCurrency(intersect(names(out), c("Off Poss", "Def Poss")),
                     currency = "", interval = 3, mark = ",", digits = 0)

    # Offense green-high except TOV%; defense red-high except TOV%.
    heat <- if (is_ff) list(
      list("Off TS%",   "pr_off_ts",   COLS_GRAD),
      list("Off TOV%",  "pr_off_tov",  COLS_REV),
      list("Off OREB%", "pr_off_oreb", COLS_GRAD),
      list("Off FTR",   "pr_off_ftr",  COLS_GRAD),
      list("Def TS%",   "pr_def_ts",   COLS_REV),
      list("Def TOV%",  "pr_def_tov",  COLS_GRAD),
      list("Def OREB%", "pr_def_oreb", COLS_REV),
      list("Def FTR",   "pr_def_ftr",  COLS_REV)
    ) else list(
      list("Off PPP",  "pr_off_ppp", COLS_GRAD),
      list("Def PPP",  "pr_def_ppp", COLS_REV),
      list("Net Rtg",  "pr_net",     COLS_GRAD),
      list("Off eFG%", "pr_off_efg", COLS_GRAD),
      list("Def eFG%", "pr_def_efg", COLS_REV)
    )
    for (h in heat) {
      dt <- formatStyle(dt, h[[1]],
                        backgroundColor = styleInterval(CUTS, h[[3]]),
                        valueColumns = h[[2]])
    }
    dt
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

    n_units <- max(NROW(euro_ld_display()) - 1L, 0L)
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
      season_value = season,
      season_label = paste(EURO_COMPETITION_LABELS[[euro_competition()]] %||% euro_competition(),
                           euro_season_label(season)),
      date_input_id = "euro_ld_date_range",
      game_type_input_id = "euro_ld_phase",
      game_type_labeller = euro_phase_label,
      gn_label = "Rd",
      extra_children = lapply(bits, function(b) tags$span(class = "filter-chip", b))
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
    rows$venue <- ifelse(isTRUE(rows$is_home) | rows$is_home %in% TRUE, "H", "A")
    show <- rows[, c("game_date", "round_number", "opp_team_name", "venue",
                     "minutes", "off_poss", "off_ppp", "def_poss", "def_ppp",
                     "net")]
    names(show) <- c("Date", "Rd", "Opponent", "H/A", "Min",
                     "Off Poss", "Off PPP", "Def Poss", "Def PPP", "Net")

    showModal(modalDialog(
      title = "Lineup game log",
      size = "l",
      easyClose = TRUE,
      renderDT(datatable(show, rownames = FALSE,
                         options = list(pageLength = 25, dom = "t", scrollX = TRUE)))
    ))
  })
}
