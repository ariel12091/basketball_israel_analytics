# server_tab9_euro_team.R - Tab 9: EuroLeague / EuroCup Team Ratings
#
# Purpose-built rather than copied from server_tab3.R: Traditional and Shot
# Profile handling do not apply here. Clutch uses the shared controls and
# parameter resolver, backed by the EuroLeague canonical action facts.
# The logic that DOES apply is mirrored deliberately --
#   * fast path (season MV) vs filtered path (dynamic SQL function), chosen by
#     the same "has the user actually narrowed anything" test,
#   * percentile ranks computed with the same pr_vec helper and polarity,
#   * the same value/#rank/delta cell format and rank-based colour ramp.

server_tab9_euro_team <- function(input, output, session, shared) {

  # ---- Competition + season come from the shared navbar selectors ----
  # Same inputs the On/Off tab reads, populated once in app.R, so changing
  # season once changes it for the whole EuroLeague section.
  et_competition <- reactive(euro_selected_competition(input))
  et_season <- reactive(euro_selected_game_year(input))
  et_teams_df <- reactive(euro_fetch_teams(et_competition(), et_season()))

  # Teams / opponents / phase / rounds / dates all follow competition + season.
  # Shared with the other EuroLeague tabs; see setup_euro_section_filters().
  setup_euro_section_filters(input, session, "euroteam",
                             competition = et_competition,
                             season = et_season,
                             teams_df = et_teams_df,
                             date_id = "euroteam_dates")

  setup_gn_last_n_sync(session, input, "euroteam")

  observeEvent(input$euroteam_reset, {
    apply_season_date_bounds(session, "euroteam_dates", euro_season_date_bounds(isolate(et_season())))
    updateSelectizeInput(session, "euroteam_teams", selected = character(0))
    updateSelectizeInput(session, "euroteam_phase", selected = character(0))
    updateSelectizeInput(session, "euroteam_opponents", selected = character(0))
    for (id in c("euroteam_home_away", "euroteam_outcome", "euroteam_opp_rank_side",
                 "euroteam_opp_rank_n", "euroteam_opp_rank_metric",
                 "euroteam_num_starters_off_mode", "euroteam_num_starters_off",
                 "euroteam_num_starters_def_mode", "euroteam_num_starters_def")) {
      updateSelectInput(session, id, selected = "")
    }
    for (id in c("euroteam_gn_min", "euroteam_gn_max", "euroteam_last_n")) {
      updateSelectizeInput(session, id, selected = "")
    }
    reset_clutch_inputs(session, "euroteam")
  })

  # ---- Parameters ----
  et_debounced <- reactive({
    f <- game_context_filter_values(
      input, "euroteam", game_type_id = "euroteam_phase"
    )
    f$dates <- input$euroteam_dates
    f$teams <- input$euroteam_teams
    f$clutch_enabled <- input$euroteam_clutch_enabled
    f$clutch_margin <- input$euroteam_clutch_margin
    f$clutch_status <- input$euroteam_clutch_status
    f$clutch_minutes <- input$euroteam_clutch_minutes
    f$clutch_ot_margin <- input$euroteam_clutch_ot_margin
    f
  }) %>% debounce(300)

  et_params <- reactive({
    f <- et_debounced()
    rng <- f$dates
    bounds <- euro_season_date_bounds(et_season())
    start_d <- if (length(rng) == 2 && !is.na(rng[1])) as.Date(rng[1]) else bounds$start
    end_d   <- if (length(rng) == 2 && !is.na(rng[2])) as.Date(rng[2]) else bounds$end

    gn <- resolve_gn_last_n_values(f$gn_min, f$gn_max, f$last_n)
    context <- game_context_db_args(f, gn)
    clutch <- resolve_clutch_params(
      f$clutch_enabled, f$clutch_margin, f$clutch_status,
      f$clutch_minutes, f$clutch_ot_margin
    )

    list(
      competition = et_competition(),
      game_year = as.integer(et_season()),
      start_d = start_d, end_d = end_d,
      team_ids_csv = csv_if_any(f$teams),
      phase_csv = context$game_type_csv,
      opp_ids_csv = context$opp_ids_csv,
      home_away = context$home_away,
      outcome = context$outcome,
      rank_side = context$opp_rank_side,
      rank_n = context$opp_rank_n,
      rank_metric = context$opp_rank_metric,
      max_margin = clutch$max_margin,
      margin_status = clutch$margin_status,
      max_time_remaining = clutch$max_time_remaining,
      ot_margin_filter = clutch$ot_margin_filter,
      min_gn = context$min_gn, max_gn = context$max_gn,
      last_n = context$last_n_games,
      st_off_min = context$num_starters_off_min,
      st_off_max = context$num_starters_off_max,
      st_def_min = context$num_starters_def_min,
      st_def_max = context$num_starters_def_max,
      bounds = bounds
    )
  })

  # Fast path only when nothing has actually been narrowed, matching Tab 3.
  et_fallback_needed <- reactive({
    p <- et_params()
    (p$start_d != p$bounds$start) || (p$end_d != p$bounds$end) ||
      !is.na(p$team_ids_csv) || !is.na(p$phase_csv) || !is.na(p$opp_ids_csv) ||
      !is.na(p$home_away) || !is.na(p$outcome) || !is.na(p$rank_side) ||
      !is.na(p$max_margin) || !is.na(p$max_time_remaining) ||
      !is.na(p$min_gn) || !is.na(p$max_gn) || !is.na(p$last_n) ||
      !is.na(p$st_off_min) || !is.na(p$st_off_max) ||
      !is.na(p$st_def_min) || !is.na(p$st_def_max)
  })

  # Match Tab 3: rank movement is meaningful only on the full season date/GN
  # baseline. Other filters are carried into both current and previous calls.
  et_delta_enabled <- reactive({
    p <- et_params()
    if (!is.na(p$min_gn) || !is.na(p$max_gn) || !is.na(p$last_n)) return(FALSE)
    identical(as.Date(p$start_d), as.Date(p$bounds$start)) &&
      identical(as.Date(p$end_d), as.Date(p$bounds$end))
  })

  # ---- Data access ----
  # The default 5/all/5:00 preset stays on the incremental cache behind the
  # dynamic reader. Every other filtered request uses the Israeli-shaped
  # direct action scan, avoiding the lineup/minutes fact pipeline.
  use_direct_team_reader <- function(p) {
    status <- blank_to_na_character(p$margin_status)
    status <- if (length(status) == 1L && !is.na(status)) status else "all"
    standard_clutch <- identical(suppressWarnings(as.integer(p$max_margin)), 5L) &&
      identical(status, "all") &&
      identical(suppressWarnings(as.integer(p$max_time_remaining)), 300L) &&
      !isTRUE(p$ot_margin_filter)
    !isTRUE(standard_clutch)
  }

  run_team_ratings <- function(p, end_override = NULL) {
    allowed <- guard_heavy_request(
      session, key = "tab9_euro_team_ratings",
      start_d = p$start_d, end_d = end_override %||% p$end_d,
      min_gn = p$min_gn, max_gn = p$max_gn, last_n = p$last_n,
      max_calls = 35L, window_sec = 60L
    )
    if (!isTRUE(allowed)) return(data.frame())
    reader <- if (use_direct_team_reader(p)) {
      "get_team_ratings_direct"
    } else {
      "get_team_ratings_dynamic"
    }
    db_get_query(pg_pool,
      paste0("SELECT * FROM euroleague.", reader, "(",
             "$1::text,$2::int4,$3::date,$4::date,$5::text,$6::text,$7::text,",
             "$8::text,$9::text,$10::text,$11::int4,$12::text,",
             "$13::int4,$14::text,$15::int4,$16::bool,",
             "$17::int4,$18::int4,$19::int4,$20::int4,$21::int4,$22::int4,$23::int4)"),
      params = list(p$competition, p$game_year, p$start_d, end_override %||% p$end_d,
                    p$team_ids_csv, p$phase_csv, p$opp_ids_csv, p$home_away, p$outcome,
                    p$rank_side, p$rank_n, p$rank_metric,
                    p$max_margin, p$margin_status, p$max_time_remaining, p$ot_margin_filter,
                    p$min_gn, p$max_gn, p$last_n,
                    p$st_off_min, p$st_off_max, p$st_def_min, p$st_def_max))
  }

  run_team_ff <- function(p, end_override = NULL) {
    allowed <- guard_heavy_request(
      session, key = "tab9_euro_team_ff",
      start_d = p$start_d, end_d = end_override %||% p$end_d,
      min_gn = p$min_gn, max_gn = p$max_gn, last_n = p$last_n,
      max_calls = 35L, window_sec = 60L
    )
    if (!isTRUE(allowed)) return(data.frame())
    reader <- if (use_direct_team_reader(p)) {
      "get_team_four_factors_direct"
    } else {
      "get_team_four_factors_dynamic"
    }
    db_get_query(pg_pool,
      paste0("SELECT * FROM euroleague.", reader, "(",
             "$1::text,$2::int4,$3::date,$4::date,$5::text,$6::text,$7::text,",
             "$8::text,$9::text,$10::text,$11::int4,$12::text,",
             "$13::int4,$14::text,$15::int4,$16::bool,",
             "$17::int4,$18::int4,$19::int4,$20::int4,$21::int4,$22::int4,$23::int4)"),
      params = list(p$competition, p$game_year, p$start_d, end_override %||% p$end_d,
                    p$team_ids_csv, p$phase_csv, p$opp_ids_csv, p$home_away, p$outcome,
                    p$rank_side, p$rank_n, p$rank_metric,
                    p$max_margin, p$margin_status, p$max_time_remaining, p$ot_margin_filter,
                    p$min_gn, p$max_gn, p$last_n,
                    p$st_off_min, p$st_off_max, p$st_def_min, p$st_def_max))
  }

  # Team floor time already exists at canonical segment grain. Keep this query
  # separate from the rating facts so pace remains a ratio calculated only
  # after the selected games and starter contexts have been aggregated.
  run_team_minutes <- function(p) {
    reader <- if (use_direct_team_reader(p)) {
      "get_team_minutes_direct"
    } else {
      "get_team_minutes_dynamic"
    }
    db_get_query(pg_pool,
      paste0("SELECT team_id, minutes AS game_minutes ",
             "FROM euroleague.", reader, "(",
             "$1::text,$2::int4,$3::date,$4::date,$5::text,$6::text,$7::text,",
             "$8::text,$9::text,$10::text,$11::int4,$12::text,",
             "$13::int4,$14::text,$15::int4,$16::bool,",
             "$17::int4,$18::int4,$19::int4,$20::int4,$21::int4,$22::int4,$23::int4)"),
      params = list(
        p$competition, p$game_year, p$start_d, p$end_d,
        p$team_ids_csv, p$phase_csv, p$opp_ids_csv, p$home_away, p$outcome,
        p$rank_side, p$rank_n, p$rank_metric,
        p$max_margin, p$margin_status, p$max_time_remaining, p$ot_margin_filter,
        p$min_gn, p$max_gn, p$last_n,
        p$st_off_min, p$st_off_max, p$st_def_min, p$st_def_max
      )
    )
  }

  et_data_version <- reactive(euro_data_version())

  et_data <- reactive({
    p <- et_params()
    if (et_fallback_needed()) return(run_team_ratings(p))
    cached_season_df(
      list("euro_team_ppp_ratings_mv", p$competition, p$game_year, et_data_version()),
      function() db_get_query(pg_pool,
        "SELECT game_year, team_id, team_name, off_ppp, def_ppp, net_rtg,
                games_played, wins, losses, off_poss, def_poss,
                rank_net_rtg, rank_off_ppp, rank_def_ppp
           FROM euroleague.team_ppp_ratings_mv
          WHERE competition = $1::text AND game_year = $2::int4
          ORDER BY rank_net_rtg",
        params = list(p$competition, p$game_year))
    )
  })

  # Percentile ranks, same helper and polarity as Tab 3.

  et_ff_data <- reactive({
    p <- et_params()
    df <- if (et_fallback_needed()) {
      run_team_ff(p)
    } else {
      cached_season_df(
        list("euro_team_four_factors_mv", p$competition, p$game_year, et_data_version()),
        function() db_get_query(pg_pool,
          "SELECT * FROM euroleague.team_four_factors_mv
            WHERE competition = $1::text AND game_year = $2::int4",
          params = list(p$competition, p$game_year))
      )
    }
    if (is.null(df) || nrow(df) == 0) return(df)

    add_team_metric_ranks(df)
  })

  # ---- Trend arrows: ranks as they stood before the most recent matchday ----
  # With every loaded game on one date there is no earlier matchday, so the
  # delta column renders as an em-dash until more games land. That is correct,
  # not a failure.
  et_prev_end <- reactive({
    p <- et_params()
    res <- tryCatch(db_get_query(pg_pool,
      "SELECT max(game_date) AS d
         FROM euroleague.team_game_ratings_mv
        WHERE competition = $1::text AND game_year = $2::int4
          AND game_date >= $3::date AND game_date < (
            SELECT max(game_date) FROM euroleague.team_game_ratings_mv
             WHERE competition = $1::text AND game_year = $2::int4
               AND game_date <= $4::date)",
      params = list(p$competition, p$game_year, p$start_d, p$end_d)),
      error = function(e) NULL)
    if (is.null(res) || !nrow(res) || is.na(res$d[[1]])) return(as.Date(NA))
    as.Date(res$d[[1]])
  })

  et_prev_data <- reactive({
    if (!isTRUE(et_delta_enabled())) return(NULL)
    prev_end <- et_prev_end()
    if (is.na(prev_end)) return(NULL)
    tryCatch(run_team_ratings(et_params(), end_override = prev_end), error = function(e) NULL)
  })

  et_prev_ff_data <- reactive({
    if (!isTRUE(et_delta_enabled())) return(NULL)
    prev_end <- et_prev_end()
    if (is.na(prev_end)) return(NULL)
    tryCatch(run_team_ff(et_params(), end_override = prev_end), error = function(e) NULL)
  })

  et_game_minutes <- reactive({
    run_team_minutes(et_params())
  })

  # ---- Render ----
  output$euroteam_table <- renderDT({
    mode <- input$euroteam_view_mode %||% "Summary"
    mins_df <- tryCatch(
      et_game_minutes(),
      error = function(e) {
        msg <- paste0("EL Team Ratings minutes error: ", conditionMessage(e))
        app_log("tab9_euro_team", msg, level = "ERROR", session = session)
        showNotification(msg, type = "error", duration = 8)
        NULL
      }
    )
    mins_map <- if (is.data.frame(mins_df) && nrow(mins_df)) {
      stats::setNames(as.numeric(mins_df$game_minutes), as.character(mins_df$team_id))
    } else NULL
    show_delta <- isTRUE(et_delta_enabled())

    empty_dt <- function(msg) DT::datatable(
      data.frame(Info = msg, check.names = FALSE),
      rownames = FALSE, options = list(headerCallback = HEADER_TOOLTIP_JS, dom = "t"))

    tryCatch({
      if (identical(mode, "Four Factors")) {
        df <- et_ff_data()
        if (is.null(df) || nrow(df) == 0) return(empty_dt("Four Factors: no data for current filters"))

        df <- add_team_pace_cols(df, mins_map, fallback_to_regulation = FALSE)
        ranks <- team_rating_rank_deltas(df, et_prev_ff_data(), show_delta)
        metric_cols <- TEAM_RATING_METRICS$metric
        for (metric in intersect(metric_cols, names(df))) {
          df[[paste0(metric, "_label")]] <- fmt_rank_cell(
            df[[metric]], ranks$current[[metric]], ranks$delta[[metric]], 1, show_delta
          )
        }

        pr_cols <- c("pr_off_ppp", "pr_off_efg", "pr_off_oreb", "pr_off_tov", "pr_off_ftr",
                     "pr_def_ppp", "pr_def_efg", "pr_def_oreb", "pr_def_tov", "pr_def_ftr", "pr_net")
        disp <- data.frame(
          team_name = df$team_name, minutes = df$minutes,
          off_ppp = df$off_ppp_label, off_efg = df$off_efg_label,
          off_oreb = df$off_oreb_label, off_tov = df$off_tov_label,
          off_ftr = df$off_ftr_label, off_poss = df$off_poss,
          def_ppp = df$def_ppp_label, def_efg = df$def_efg_label,
          def_oreb = df$def_oreb_label, def_tov = df$def_tov_label,
          def_ftr = df$def_ftr_label, def_poss = df$def_poss,
          net_rtg = df$net_rtg_label,
          df[, pr_cols, drop = FALSE], check.names = FALSE
        )
        sorted <- team_rating_sort_columns(disp, df, metric_cols)
        disp <- sorted$data

        sketch <- htmltools::withTags(table(class = "display", thead(
          tr(
            th(class = "group-head", colspan = 2, ""),
            th(class = "group-head section-left-border", colspan = 6, "Offense"),
            th(class = "group-head section-left-border", colspan = 6, "Defense"),
            th(class = "group-head section-left-border", "")
          ),
          tr(
            th(class = "sub-head", "Team"),
            th(class = "sub-head", "Min"),
            th(class = "sub-head section-left-border", "PPP"), th(class = "sub-head", "eFG%"),
            th(class = "sub-head", "OREB%"), th(class = "sub-head", "TOV%"),
            th(class = "sub-head", "FTR"), th(class = "sub-head", "Poss"),
            th(class = "sub-head section-left-border", "PPP"), th(class = "sub-head", "eFG%"),
            th(class = "sub-head", "OREB%"), th(class = "sub-head", "TOV%"),
            th(class = "sub-head", "FTR"), th(class = "sub-head", "Poss"),
            th(class = "sub-head section-left-border", "Net")
          )
        )))

        hide_idx <- which(names(disp) %in% c(pr_cols, grep("^sort__", names(disp), value = TRUE))) - 1L
        net_idx <- which(names(disp) == "net_rtg") - 1L
        col_defs <- list(
          list(targets = hide_idx, visible = FALSE),
          list(targets = "_all", className = "dt-center")
        )
        col_defs <- c(col_defs, sorted$definitions)
        for (nm in c("off_ppp", "def_ppp", "net_rtg")) {
          i <- which(names(disp) == nm) - 1L
          if (length(i)) col_defs[[length(col_defs) + 1]] <-
            list(targets = i, className = "section-left-border dt-center")
        }

        dt <- DT::datatable(
          disp, container = sketch, rownames = FALSE,
          escape = dt_escape_except(disp, metric_cols),
          extensions = "Buttons",
          options = list(
            headerCallback = HEADER_TOOLTIP_JS, dom = "Btip",
            buttons = list(list(extend = "csv", text = "Download CSV",
                                filename = "euroleague_team_four_factors")),
            pageLength = 50, deferRender = TRUE, scrollX = TRUE,
            scrollY = "70vh", scrollCollapse = TRUE,
            order = if (length(net_idx)) list(list(net_idx, "desc")) else list(),
            columnDefs = col_defs))

        poss_cols <- intersect(c("off_poss", "def_poss"), names(disp))
        if (length(poss_cols)) dt <- DT::formatCurrency(dt, poss_cols, currency = "",
                                                        interval = 3, mark = ",", digits = 0)
        dt <- DT::formatRound(dt, "minutes", digits = 1)
        # Every pr_ vector is already oriented so that high = good, so all
        # columns use the same ramp -- the polarity lives in pr_vec(invert=).
        style_map <- list(
          off_ppp = "pr_off_ppp", off_efg = "pr_off_efg", off_oreb = "pr_off_oreb",
          off_tov = "pr_off_tov", off_ftr = "pr_off_ftr",
          def_ppp = "pr_def_ppp", def_efg = "pr_def_efg", def_oreb = "pr_def_oreb",
          def_tov = "pr_def_tov", def_ftr = "pr_def_ftr", net_rtg = "pr_net")
        for (nm in names(style_map)) {
          pc <- style_map[[nm]]
          if (nm %in% names(disp) && pc %in% names(disp)) {
            dt <- DT::formatStyle(dt, nm, backgroundColor = styleInterval(CUTS, COLS_GRAD),
                                  valueColumns = pc)
          }
        }
        return(dt)
      }

      # ---------------- Summary ----------------
      df <- et_data()
      if (is.null(df) || nrow(df) == 0) return(empty_dt("Summary: no data for current filters"))
      df <- add_team_pace_cols(df, mins_map, fallback_to_regulation = FALSE)
      ranks <- team_rating_rank_deltas(df, et_prev_data(), show_delta)
      rk_net <- ranks$current$net_rtg
      rk_off <- ranks$current$off_ppp
      rk_def <- ranks$current$def_ppp
      d_net <- ranks$delta$net_rtg
      d_off <- ranks$delta$off_ppp
      d_def <- ranks$delta$def_ppp

      disp <- data.frame(
        game_year = df$game_year,
        team_name = df$team_name,
        games_played = df$games_played,
        minutes = df$minutes,
        wins = df$wins,
        losses = df$losses,
        off_ppp = fmt_rank_cell(df$off_ppp, rk_off, d_off, 1, show_delta),
        def_ppp = fmt_rank_cell(df$def_ppp, rk_def, d_def, 1, show_delta),
        net_rtg = fmt_rank_cell(df$net_rtg, rk_net, d_net, 1, show_delta),
        off_pace = df$off_pace,
        def_pace = df$def_pace,
        off_poss = df$off_poss,
        def_poss = df$def_poss,
        check.names = FALSE, stringsAsFactors = FALSE
      )
      disp$rank_net_rtg <- rk_net
      disp$rank_off_ppp <- rk_off
      disp$rank_def_ppp <- rk_def
      disp$sort_off_ppp <- suppressWarnings(as.numeric(df$off_ppp))
      disp$sort_def_ppp <- suppressWarnings(as.numeric(df$def_ppp))
      disp$sort_net_rtg <- suppressWarnings(as.numeric(df$net_rtg))
      disp$sort_off_ppp[is.na(disp$sort_off_ppp)] <- -Inf
      disp$sort_def_ppp[is.na(disp$sort_def_ppp)] <- Inf
      disp$sort_net_rtg[is.na(disp$sort_net_rtg)] <- -Inf

      max_rank <- suppressWarnings(max(c(rk_net, rk_off, rk_def), na.rm = TRUE))
      if (!is.finite(max_rank) || max_rank < 2) max_rank <- 2
      cuts <- seq(1.5, max_rank - 0.5, 1)
      cols_rank <- colorRampPalette(c("#1a6b38", "#6b5a20", "#8b2020"))(length(cuts) + 1)

      hidden <- which(names(disp) %in% c("rank_net_rtg", "rank_off_ppp", "rank_def_ppp",
                                         "sort_off_ppp", "sort_def_ppp", "sort_net_rtg")) - 1L
      order_defs <- list(
        list(targets = which(names(disp) == "off_ppp") - 1L,
             orderData = which(names(disp) == "sort_off_ppp") - 1L,
             orderSequence = list("desc", "asc")),
        list(targets = which(names(disp) == "def_ppp") - 1L,
             orderData = which(names(disp) == "sort_def_ppp") - 1L,
             orderSequence = list("asc", "desc")),
        list(targets = which(names(disp) == "net_rtg") - 1L,
             orderData = which(names(disp) == "sort_net_rtg") - 1L,
             orderSequence = list("desc", "asc"))
      )

      DT::datatable(
        disp,
        colnames = c("Season", "Team", "GP", "Min", "W", "L", "Off PPP", "Def PPP", "Net Rtg",
                     "Off Pace", "Def Pace", "Off Poss", "Def Poss",
                     "rank_net_rtg", "rank_off_ppp", "rank_def_ppp",
                     "sort_off_ppp", "sort_def_ppp", "sort_net_rtg"),
        rownames = FALSE,
        escape = dt_escape_except(disp, c("off_ppp", "def_ppp", "net_rtg")),
        extensions = "Buttons",
        options = list(
          headerCallback = HEADER_TOOLTIP_JS, dom = "Btip",
          buttons = list(list(extend = "csv", text = "Download CSV",
                              filename = "euroleague_team_ratings")),
          pageLength = 50, scrollX = TRUE, scrollY = "70vh", scrollCollapse = TRUE,
          columnDefs = c(list(
            list(className = "dt-center", targets = "_all"),
            list(visible = FALSE, targets = hidden)
          ), order_defs)
        )
      ) %>%
        DT::formatRound(c("minutes", "off_pace", "def_pace"), digits = 1) %>%
        DT::formatCurrency(c("off_poss", "def_poss"), currency = "",
                           interval = 3, mark = ",", digits = 0) %>%
        DT::formatStyle(columns = c("net_rtg", "off_ppp", "def_ppp"),
                        valueColumns = c("rank_net_rtg", "rank_off_ppp", "rank_def_ppp"),
                        backgroundColor = styleInterval(cuts, cols_rank))
    }, error = function(e) {
      msg <- paste0("EL Team Ratings render error: ", conditionMessage(e))
      app_log("tab9_euro_team", msg, level = "ERROR", session = session)
      showNotification(msg, type = "error", duration = 8)
      DT::datatable(data.frame(Error = msg, check.names = FALSE), rownames = FALSE,
                    options = list(headerCallback = HEADER_TOOLTIP_JS, dom = "t"))
    })
  }, server = FALSE)

  # ---- Filter chips ----
  # Tab 3's chip bar; see server_tab8_euro.R for what the league arguments do.
  output$euroteam_filter_chips <- renderUI({
    td <- et_teams_df()
    team_map <- if (!is.null(td) && nrow(td)) {
      stats::setNames(as.character(td$team_name), as.character(td$team_id))
    } else NULL
    season <- et_season()
    build_filter_chips(
      "euroteam", input, euro_season_date_bounds,
      reset_btn_id = "euroteam_reset",
      team_label_map = team_map,
      opponent_label_map = team_map,
      season_value = season,
      season_label = paste(EURO_COMPETITION_LABELS[[et_competition()]] %||% et_competition(),
                           euro_season_label(season)),
      date_input_id = "euroteam_dates",
      game_type_input_id = "euroteam_phase",
      game_type_labeller = euro_phase_label,
      gn_label = "Rd"
    )
  })

  setup_chip_clears("euroteam", session, input, shared,
    game_type_id = "euroteam_phase", opponents_id = "euroteam_opponents",
    home_away_id = "euroteam_home_away", outcome_id = "euroteam_outcome",
    gn_min_id = "euroteam_gn_min", gn_max_id = "euroteam_gn_max",
    last_n_id = "euroteam_last_n",
    opp_rank_ids = c("euroteam_opp_rank_side", "euroteam_opp_rank_n", "euroteam_opp_rank_metric"),
    date_id = "euroteam_dates", gy_input_id = "euro_game_year",
    teams_ids = "euroteam_teams", teams_multiple = TRUE,
    starters_ids = c("euroteam_num_starters_off_mode", "euroteam_num_starters_off",
                     "euroteam_num_starters_def_mode", "euroteam_num_starters_def"),
    clutch_enabled_id = "euroteam_clutch_enabled",
    bounds_fn = euro_season_date_bounds)

}
