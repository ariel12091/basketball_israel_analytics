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

    opp_choices <- if (!is.null(teams) && nrow(teams)) {
      setNames(as.character(teams$team_id), teams$team_name)
    } else {
      character(0)
    }
    updateSelectizeInput(session, "euro_ld_opponents", choices = opp_choices,
                         selected = character(0), server = FALSE)

    phases <- tryCatch(euro_fetch_phases(comp, season), error = function(e) NULL)
    phase_choices <- if (!is.null(phases) && nrow(phases)) {
      setNames(as.character(phases[[1]]), vapply(phases[[1]], euro_phase_label, ""))
    } else {
      character(0)
    }
    updateSelectizeInput(session, "euro_ld_phase", choices = phase_choices,
                         selected = character(0), server = FALSE)

    rounds <- tryCatch(euro_fetch_round_values(comp, season), error = function(e) NULL)
    round_vals <- if (!is.null(rounds) && nrow(rounds)) as.character(rounds$gn) else character(0)
    updateSelectInput(session, "euro_ld_gn_min",
                      choices = c("—" = "", setNames(round_vals, round_vals)), selected = "")
    updateSelectInput(session, "euro_ld_gn_max",
                      choices = c("—" = "", setNames(round_vals, round_vals)), selected = "")
    updateSelectInput(session, "euro_ld_last_n",
                      choices = c("All" = "", setNames(round_vals, round_vals)), selected = "")

    # updateDateRangeInput() with a start outside min yields NA, so the bounds
    # are guarded before they are applied.
    b <- tryCatch(euro_season_date_bounds(season), error = function(e) NULL)
    if (!is.null(b) && !is.na(b$start) && !is.na(b$end)) {
      updateDateRangeInput(session, "euro_ld_date_range",
                           start = b$start, end = b$end,
                           min = b$start, max = b$end)
    }
  }, ignoreInit = FALSE)

  observeEvent(input$euro_ld_reset, {
    b <- tryCatch(euro_season_date_bounds(euro_season()), error = function(e) NULL)
    if (!is.null(b) && !is.na(b$start) && !is.na(b$end)) {
      updateDateRangeInput(session, "euro_ld_date_range",
                           start = b$start, end = b$end,
                           min = b$start, max = b$end)
    }
    updateSelectInput(session, "euro_ld_group_size", selected = "5")
    ld_filter$reset_inputs(team_selected = "")
    updateSelectizeInput(session, "euro_ld_opponents", selected = character(0))
    updateSelectizeInput(session, "euro_ld_phase", selected = character(0))
    for (id in c("euro_ld_gn_min", "euro_ld_gn_max", "euro_ld_last_n",
                 "euro_ld_opp_rank_side", "euro_ld_opp_rank_n",
                 "euro_ld_num_starters_off_mode", "euro_ld_num_starters_off",
                 "euro_ld_num_starters_def_mode", "euro_ld_num_starters_def")) {
      updateSelectInput(session, id, selected = "")
    }
    updateSelectInput(session, "euro_ld_home_away", selected = "all")
    updateSelectInput(session, "euro_ld_outcome", selected = "all")
    updateSelectInput(session, "euro_ld_opp_rank_metric", selected = "net")
    auto_enabled(TRUE)
  })

  # --- Filter arguments -----------------------------------------------------
  gn_params <- reactive({
    to_int <- function(x) if (!is.null(x) && nzchar(x)) as.integer(x) else NA_integer_
    min_gn <- to_int(input$euro_ld_gn_min)
    max_gn <- to_int(input$euro_ld_gn_max)
    last_n <- to_int(input$euro_ld_last_n)
    # GN range and last-N are mutually exclusive.
    if (!is.na(last_n)) { min_gn <- NA_integer_; max_gn <- NA_integer_ }
    if (!is.na(min_gn) || !is.na(max_gn)) last_n <- NA_integer_
    if (!is.na(min_gn) && !is.na(max_gn) && min_gn > max_gn) {
      tmp <- min_gn; min_gn <- max_gn; max_gn <- tmp
    }
    list(min_gn = min_gn, max_gn = max_gn, last_n = last_n)
  })

  debounced_dates <- reactive(input$euro_ld_date_range) %>% debounce(300)

  build_db_args <- function() {
    gp <- gn_params()
    starters <- resolve_starters_bounds(
      off_mode = input$euro_ld_num_starters_off_mode,
      off_val  = input$euro_ld_num_starters_off,
      def_mode = input$euro_ld_num_starters_def_mode,
      def_val  = input$euro_ld_num_starters_def
    )
    team_val <- ld_filter$team()
    team_val <- team_val[nzchar(team_val)]
    list(
      team_csv = if (length(team_val)) paste(team_val, collapse = ",") else NA_character_,
      phase_csv = csv_if_any(input$euro_ld_phase),
      opp_ids_csv = csv_if_any(input$euro_ld_opponents),
      home_away = blank_to_na_character(input$euro_ld_home_away),
      outcome = blank_to_na_character(input$euro_ld_outcome),
      opp_rank_side = blank_to_na_character(input$euro_ld_opp_rank_side),
      opp_rank_n = blank_to_na_integer(input$euro_ld_opp_rank_n),
      opp_rank_metric = blank_to_na_character(input$euro_ld_opp_rank_metric),
      min_gn = gp$min_gn, max_gn = gp$max_gn, last_n_games = gp$last_n,
      num_starters_off_min = starters$num_starters_off_min,
      num_starters_off_max = starters$num_starters_off_max,
      num_starters_def_min = starters$num_starters_def_min,
      num_starters_def_max = starters$num_starters_def_max,
      players_on_csv = csv_if_any(ld_filter$players_on()),
      players_off_csv = csv_if_any(ld_filter$players_off()),
      unit_size = as.integer(input$euro_ld_group_size %||% "5")
    )
  }

  # --- Fetch ----------------------------------------------------------------
  # p_min_poss is always 0: ranks and the auto threshold need the complete
  # comparison population. The displayed minimum is applied afterwards.
  #
  # player_ids / player_names are deliberately not selected. PostgreSQL returns
  # them as '{1,2,3}' text, and nothing here needs them -- unit_key is the
  # identity and player_names_str is the display form.
  euro_ld_raw <- reactive({
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
        a$team_csv, a$phase_csv, a$opp_ids_csv, a$home_away, a$outcome,
        a$opp_rank_side, a$opp_rank_n, a$opp_rank_metric,
        a$min_gn, a$max_gn, a$last_n_games,
        a$num_starters_off_min, a$num_starters_off_max,
        a$num_starters_def_min, a$num_starters_def_max,
        a$unit_size, a$players_on_csv, a$players_off_csv,
        0L
      )
    )
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
    df[order(-df$total_poss), , drop = FALSE]
  })

  # --- Auto minimum possessions --------------------------------------------
  # Computed on the team/player-filtered population BEFORE the min-poss filter.
  # Manual slider use switches to manual; a filter change returns to auto. The
  # `updating` flag stops an auto-driven slider update reading as a manual one.
  observeEvent(euro_ld_full(), {
    df <- euro_ld_full()
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

  observeEvent(list(input$euro_ld_group_size, ld_filter$team(),
                    ld_filter$players_on(), ld_filter$players_off(),
                    debounced_dates(), input$euro_ld_opponents,
                    input$euro_ld_phase, input$euro_ld_home_away,
                    input$euro_ld_outcome, input$euro_ld_gn_min,
                    input$euro_ld_gn_max, input$euro_ld_last_n), {
    auto_enabled(TRUE)
  }, ignoreInit = TRUE)

  # --- Displayed rows, with the TOTAL row pinned on top ---------------------
  euro_ld_display <- reactive({
    df <- euro_ld_full()
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
  summary_cols <- c(
    team_name = "Team", player_names_str = "Unit", minutes = "Min",
    off_poss = "Off Poss", off_ppp = "Off PPP",
    def_poss = "Def Poss", def_ppp = "Def PPP", net_rtg = "Net",
    off_fg2_made = "2PM", off_fg2_att = "2PA",
    off_fg3_made = "3PM", off_fg3_att = "3PA", off_efg = "eFG%"
  )
  ff_cols <- c(
    team_name = "Team", player_names_str = "Unit", minutes = "Min",
    off_poss = "Off Poss", off_ts = "Off TS%", off_tov_pct = "Off TOV%",
    off_oreb_pct = "Off OREB%", off_ftr = "Off FTR",
    def_poss = "Def Poss", def_ts = "Def TS%", def_tov_pct = "Def TOV%",
    def_oreb_pct = "Def OREB%", def_ftr = "Def FTR"
  )

  output$euro_ld_dt <- renderDT({
    df <- euro_ld_display()
    cols <- if (identical(input$euro_ld_view_mode, "Four Factors")) ff_cols else summary_cols
    if (!NROW(df)) {
      return(datatable(data.frame(Message = "No units match these filters."),
                       rownames = FALSE, options = list(dom = "t")))
    }

    out <- df[, names(cols), drop = FALSE]
    names(out) <- unname(cols)
    # unit_key rides along hidden so a click can resolve the row to a unit.
    out$`_unit` <- ifelse(is.na(df$unit_key), "", df$unit_key)

    numeric_cols <- names(out)[vapply(out, is.numeric, logical(1))]
    unit_idx <- which(names(out) == "Unit") - 1L
    key_idx <- which(names(out) == "_unit") - 1L

    # Escaping stays on. Provider-supplied player names reach this table, so
    # the data must be escaped; the unit link and the bold TOTAL are produced
    # by the columnDefs render function below, whose markup is inserted by
    # DataTables regardless of server-side escaping. No column needs raw HTML
    # in its data, so there is no allowlist to grant.
    datatable(
      out,
      rownames = FALSE,
      selection = "none",
      extensions = "FixedHeader",
      options = list(
        pageLength = 25,
        scrollX = TRUE,
        fixedHeader = TRUE,
        order = list(),
        columnDefs = list(
          list(targets = key_idx, visible = FALSE),
          list(
            targets = unit_idx,
            render = DT::JS(
              "function(data, type, row) {",
              "  if (type !== 'display' || !row) return data;",
              sprintf("  var key = row[%d];", key_idx),
              "  if (!key) return '<strong>' + data + '</strong>';",
              "  return '<a href=\"#\" style=\"text-decoration:underline;\" ",
              "onclick=\"Shiny.setInputValue(&quot;euro_ld_clicked_unit&quot;, &quot;' + key + ",
              "'&quot;, {priority: &quot;event&quot;}); return false;\">' + data + '</a>';",
              "}"
            )
          )
        )
      )
    ) %>%
      formatRound(intersect(numeric_cols, c("Min", "Off PPP", "Def PPP", "Net",
                                            "eFG%", "Off TS%", "Def TS%",
                                            "Off TOV%", "Def TOV%",
                                            "Off OREB%", "Def OREB%",
                                            "Off FTR", "Def FTR")), 1)
  })

  # --- Filter chips ---------------------------------------------------------
  output$euro_ld_filter_chips <- renderUI({
    df <- euro_ld_display()
    n_units <- max(NROW(df) - 1L, 0L)
    size <- input$euro_ld_group_size %||% "5"
    bits <- c(sprintf("%s-player units", size), sprintf("%d shown", n_units))
    if (!is.null(input$euro_ld_minposs) && input$euro_ld_minposs > 0) {
      bits <- c(bits, sprintf("min %d poss%s", as.integer(input$euro_ld_minposs),
                              if (isTRUE(auto_enabled())) " (auto)" else ""))
    }
    team_val <- ld_filter$team()
    team_val <- team_val[nzchar(team_val)]
    if (length(team_val)) {
      teams <- euro_ld_ref$teams
      nm <- if (!is.null(teams)) teams$team_name[match(as.integer(team_val), teams$team_id)] else team_val
      bits <- c(bits, paste0("team: ", nm))
    }
    if (length(ld_filter$players_on())) {
      bits <- c(bits, sprintf("%d player(s) on", length(ld_filter$players_on())))
    }
    if (length(ld_filter$players_off())) {
      bits <- c(bits, sprintf("%d player(s) off", length(ld_filter$players_off())))
    }
    div(class = "filter-chips",
        lapply(bits, function(b) span(class = "filter-chip", b)))
  })

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
