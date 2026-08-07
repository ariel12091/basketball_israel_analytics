# server_tab9_euro_team.R - Tab 9: EuroLeague / EuroCup Team Ratings
#
# Purpose-built rather than copied from server_tab3.R: most of that file is
# clutch, Traditional and Shot Profile handling, none of which applies here.
# The logic that DOES apply is mirrored deliberately --
#   * fast path (season MV) vs filtered path (dynamic SQL function), chosen by
#     the same "has the user actually narrowed anything" test,
#   * percentile ranks computed with the same pr_vec helper and polarity,
#   * the same value/#rank/delta cell format and rank-based colour ramp.

server_tab9_euro_team <- function(input, output, session, shared) {

  # ---- Competition + season (this section owns its own selectors) ----
  et_competition <- reactive({
    val <- input$euroteam_competition %||% EURO_DEFAULT_COMPETITION
    if (!nzchar(val)) EURO_DEFAULT_COMPETITION else as.character(val)
  })
  et_season <- reactive({
    val <- input$euroteam_game_year %||% EURO_DEFAULT_SEASON
    if (!nzchar(val)) EURO_DEFAULT_SEASON else as.character(val)
  })
  et_teams_df <- reactive(euro_fetch_teams(et_competition(), et_season()))

  observe({
    comps <- tryCatch(euro_fetch_competitions(), error = function(e) NULL)
    codes <- if (!is.null(comps) && nrow(comps)) as.character(comps$competition) else EURO_DEFAULT_COMPETITION
    labels <- unname(EURO_COMPETITION_LABELS[codes])
    labels[is.na(labels)] <- codes[is.na(labels)]
    updateSelectInput(session, "euroteam_competition",
                      choices = stats::setNames(codes, labels),
                      selected = isolate(et_competition()))
  })

  observeEvent(et_competition(), {
    seasons <- tryCatch(euro_fetch_seasons(et_competition()), error = function(e) NULL)
    vals <- if (!is.null(seasons) && nrow(seasons)) as.character(seasons$game_year) else EURO_DEFAULT_SEASON
    sel <- isolate(et_season())
    if (!sel %in% vals) sel <- vals[[1]]
    updateSelectInput(session, "euroteam_game_year",
                      choices = stats::setNames(vals, euro_season_label(vals)), selected = sel)
  }, ignoreInit = FALSE)

  observeEvent(list(et_competition(), et_season()), {
    bounds <- euro_season_date_bounds(et_season())
    updateDateRangeInput(session, "euroteam_dates",
                         start = bounds$start, end = bounds$end,
                         min = bounds$start, max = bounds$end)

    td <- et_teams_df()
    choices <- if (!is.null(td) && nrow(td)) {
      stats::setNames(as.character(td$team_id), as.character(td$team_name))
    } else character(0)
    updateSelectizeInput(session, "euroteam_teams", choices = choices,
                         selected = character(0), server = TRUE)
    updateSelectizeInput(session, "euroteam_opponents", choices = choices,
                         selected = character(0), server = TRUE)

    ph <- tryCatch(euro_fetch_phases(et_competition(), et_season()), error = function(e) NULL)
    ph_vals <- if (!is.null(ph) && nrow(ph)) as.character(ph$phase) else character(0)
    updateSelectizeInput(session, "euroteam_phase",
                         choices = stats::setNames(ph_vals, euro_phase_label(ph_vals)),
                         selected = character(0))

    # GN here is ROUND number, matching the compute functions.
    rd <- tryCatch(euro_fetch_round_values(et_competition(), et_season()), error = function(e) NULL)
    rd_vals <- if (!is.null(rd) && nrow(rd)) as.integer(rd$gn) else integer(0)
    update_gn_last_n_choices(session, "euroteam", rd_vals)
  }, ignoreInit = FALSE)

  setup_gn_last_n_sync(session, input, "euroteam")

  observeEvent(input$euroteam_reset, {
    bounds <- euro_season_date_bounds(isolate(et_season()))
    updateDateRangeInput(session, "euroteam_dates",
                         start = bounds$start, end = bounds$end,
                         min = bounds$start, max = bounds$end)
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
  })

  # ---- Parameters ----
  et_debounced <- reactive(list(
    dates    = input$euroteam_dates,
    teams    = input$euroteam_teams,
    phase    = input$euroteam_phase,
    opps     = input$euroteam_opponents,
    home     = input$euroteam_home_away,
    outcome  = input$euroteam_outcome,
    rk_side  = input$euroteam_opp_rank_side,
    rk_n     = input$euroteam_opp_rank_n,
    rk_metric = input$euroteam_opp_rank_metric,
    st_off_m = input$euroteam_num_starters_off_mode,
    st_off_v = input$euroteam_num_starters_off,
    st_def_m = input$euroteam_num_starters_def_mode,
    st_def_v = input$euroteam_num_starters_def,
    gn_min   = input$euroteam_gn_min,
    gn_max   = input$euroteam_gn_max,
    last_n   = input$euroteam_last_n
  )) %>% debounce(300)

  et_params <- reactive({
    f <- et_debounced()
    rng <- f$dates
    bounds <- euro_season_date_bounds(et_season())
    start_d <- if (length(rng) == 2 && !is.na(rng[1])) as.Date(rng[1]) else bounds$start
    end_d   <- if (length(rng) == 2 && !is.na(rng[2])) as.Date(rng[2]) else bounds$end

    min_gn <- blank_to_na_integer(f$gn_min)
    max_gn <- blank_to_na_integer(f$gn_max)
    last_n <- blank_to_na_integer(f$last_n)
    if (!is.na(last_n)) { min_gn <- NA_integer_; max_gn <- NA_integer_ }
    if (!is.na(min_gn) || !is.na(max_gn)) last_n <- NA_integer_
    if (!is.na(min_gn) && !is.na(max_gn) && min_gn > max_gn) {
      tmp <- min_gn; min_gn <- max_gn; max_gn <- tmp
    }
    starters <- resolve_starters_bounds(
      off_mode = f$st_off_m, off_val = f$st_off_v,
      def_mode = f$st_def_m, def_val = f$st_def_v
    )

    list(
      competition = et_competition(),
      game_year = as.integer(et_season()),
      start_d = start_d, end_d = end_d,
      team_ids_csv = csv_if_any(f$teams),
      phase_csv = csv_if_any(f$phase),
      opp_ids_csv = csv_if_any(f$opps),
      home_away = blank_to_na_character(f$home),
      outcome = blank_to_na_character(f$outcome),
      rank_side = blank_to_na_character(f$rk_side),
      rank_n = blank_to_na_integer(f$rk_n),
      rank_metric = blank_to_na_character(f$rk_metric),
      min_gn = min_gn, max_gn = max_gn, last_n = last_n,
      st_off_min = starters$num_starters_off_min, st_off_max = starters$num_starters_off_max,
      st_def_min = starters$num_starters_def_min, st_def_max = starters$num_starters_def_max,
      bounds = bounds
    )
  })

  # Fast path only when nothing has actually been narrowed, matching Tab 3.
  et_fallback_needed <- reactive({
    p <- et_params()
    (p$start_d != p$bounds$start) || (p$end_d != p$bounds$end) ||
      !is.na(p$team_ids_csv) || !is.na(p$phase_csv) || !is.na(p$opp_ids_csv) ||
      !is.na(p$home_away) || !is.na(p$outcome) || !is.na(p$rank_side) ||
      !is.na(p$min_gn) || !is.na(p$max_gn) || !is.na(p$last_n) ||
      !is.na(p$st_off_min) || !is.na(p$st_off_max) ||
      !is.na(p$st_def_min) || !is.na(p$st_def_max)
  })

  # ---- Data access ----
  run_team_ratings <- function(p, end_override = NULL) {
    allowed <- guard_heavy_request(
      session, key = "tab9_euro_team_ratings",
      start_d = p$start_d, end_d = end_override %||% p$end_d,
      min_gn = p$min_gn, max_gn = p$max_gn, last_n = p$last_n,
      max_calls = 35L, window_sec = 60L
    )
    if (!isTRUE(allowed)) return(data.frame())
    db_get_query(pg_pool,
      paste0("SELECT * FROM euroleague.get_team_ratings_dynamic(",
             "$1::text,$2::int4,$3::date,$4::date,$5::text,$6::text,$7::text,",
             "$8::text,$9::text,$10::text,$11::int4,$12::text,",
             "$13::int4,$14::int4,$15::int4,$16::int4,$17::int4,$18::int4,$19::int4)"),
      params = list(p$competition, p$game_year, p$start_d, end_override %||% p$end_d,
                    p$team_ids_csv, p$phase_csv, p$opp_ids_csv, p$home_away, p$outcome,
                    p$rank_side, p$rank_n, p$rank_metric,
                    p$min_gn, p$max_gn, p$last_n,
                    p$st_off_min, p$st_off_max, p$st_def_min, p$st_def_max))
  }

  run_team_ff <- function(p) {
    allowed <- guard_heavy_request(
      session, key = "tab9_euro_team_ff",
      start_d = p$start_d, end_d = p$end_d,
      min_gn = p$min_gn, max_gn = p$max_gn, last_n = p$last_n,
      max_calls = 35L, window_sec = 60L
    )
    if (!isTRUE(allowed)) return(data.frame())
    db_get_query(pg_pool,
      paste0("SELECT * FROM euroleague.get_team_four_factors_dynamic(",
             "$1::text,$2::int4,$3::date,$4::date,$5::text,$6::text,$7::text,",
             "$8::text,$9::text,$10::text,$11::int4,$12::text,",
             "$13::int4,$14::int4,$15::int4,$16::int4,$17::int4,$18::int4,$19::int4)"),
      params = list(p$competition, p$game_year, p$start_d, p$end_d,
                    p$team_ids_csv, p$phase_csv, p$opp_ids_csv, p$home_away, p$outcome,
                    p$rank_side, p$rank_n, p$rank_metric,
                    p$min_gn, p$max_gn, p$last_n,
                    p$st_off_min, p$st_off_max, p$st_def_min, p$st_def_max))
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
  pr_vec <- function(x, invert = FALSE) {
    n <- sum(!is.na(x))
    if (n <= 1) return(rep(NA_real_, length(x)))
    r <- rank(x, na.last = "keep", ties.method = "average")
    p <- (r - 1) / (n - 1)
    if (invert) p <- 1 - p
    as.numeric(p)
  }

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

    df$pr_off_ppp  <- pr_vec(df$off_ppp)
    df$pr_off_efg  <- pr_vec(df$off_efg)
    df$pr_off_oreb <- pr_vec(df$off_oreb)
    df$pr_off_tov  <- pr_vec(df$off_tov, invert = TRUE)
    df$pr_off_ftr  <- pr_vec(df$off_ftr)
    df$pr_def_ppp  <- pr_vec(df$def_ppp, invert = TRUE)
    df$pr_def_efg  <- pr_vec(df$def_efg, invert = TRUE)
    df$pr_def_oreb <- pr_vec(df$def_oreb, invert = TRUE)
    df$pr_def_tov  <- pr_vec(df$def_tov)
    df$pr_def_ftr  <- pr_vec(df$def_ftr, invert = TRUE)
    df$pr_net      <- pr_vec(df$net_rtg)
    df
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
    prev_end <- et_prev_end()
    if (is.na(prev_end)) return(NULL)
    tryCatch(run_team_ratings(et_params(), end_override = prev_end), error = function(e) NULL)
  })

  # ---- Render ----
  output$euroteam_table <- renderDT({
    mode <- input$euroteam_view_mode %||% "Summary"

    empty_dt <- function(msg) DT::datatable(
      data.frame(Info = msg, check.names = FALSE),
      rownames = FALSE, options = list(headerCallback = HEADER_TOOLTIP_JS, dom = "t"))

    fmt_rank_cell <- function(value, rank_now, delta = NA_integer_, digits = 1) {
      v <- suppressWarnings(as.numeric(value))
      r <- suppressWarnings(as.integer(rank_now))
      d <- suppressWarnings(as.integer(delta))
      value_txt <- ifelse(is.na(v), "NA", format(round(v, digits), nsmall = digits, trim = TRUE))
      rank_txt <- ifelse(is.na(r), "#NA", paste0("#", r))
      delta_txt <- ifelse(
        is.na(d), "—",
        ifelse(d > 0, paste0("▲", abs(d)),
               ifelse(d < 0, paste0("▼", abs(d)), "↔")))
      paste0(value_txt, "<br>", rank_txt, "<br>", delta_txt)
    }

    tryCatch({
      if (identical(mode, "Four Factors")) {
        df <- et_ff_data()
        if (is.null(df) || nrow(df) == 0) return(empty_dt("Four Factors: no data for current filters"))

        pr_cols <- c("pr_off_ppp", "pr_off_efg", "pr_off_oreb", "pr_off_tov", "pr_off_ftr",
                     "pr_def_ppp", "pr_def_efg", "pr_def_oreb", "pr_def_tov", "pr_def_ftr", "pr_net")
        disp <- df[, intersect(c("team_name",
                                 "off_ppp", "off_efg", "off_oreb", "off_tov", "off_ftr", "off_poss",
                                 "def_ppp", "def_efg", "def_oreb", "def_tov", "def_ftr", "def_poss",
                                 "net_rtg", pr_cols), names(df))]

        sketch <- htmltools::withTags(table(class = "display", thead(
          tr(
            th(class = "group-head", colspan = 1, ""),
            th(class = "group-head section-left-border", colspan = 6, "Offense"),
            th(class = "group-head section-left-border", colspan = 6, "Defense"),
            th(class = "group-head section-left-border", "")
          ),
          tr(
            th(class = "sub-head", "Team"),
            th(class = "sub-head section-left-border", "PPP"), th(class = "sub-head", "eFG%"),
            th(class = "sub-head", "OREB%"), th(class = "sub-head", "TOV%"),
            th(class = "sub-head", "FTR"), th(class = "sub-head", "Poss"),
            th(class = "sub-head section-left-border", "PPP"), th(class = "sub-head", "eFG%"),
            th(class = "sub-head", "OREB%"), th(class = "sub-head", "TOV%"),
            th(class = "sub-head", "FTR"), th(class = "sub-head", "Poss"),
            th(class = "sub-head section-left-border", "Net")
          )
        )))

        hide_idx <- which(names(disp) %in% pr_cols) - 1L
        net_idx <- which(names(disp) == "net_rtg") - 1L
        col_defs <- list(
          list(targets = hide_idx, visible = FALSE),
          list(targets = "_all", className = "dt-center")
        )
        for (nm in c("off_ppp", "def_ppp", "net_rtg")) {
          i <- which(names(disp) == nm) - 1L
          if (length(i)) col_defs[[length(col_defs) + 1]] <-
            list(targets = i, className = "section-left-border dt-center")
        }

        dt <- DT::datatable(
          disp, container = sketch, rownames = FALSE, escape = TRUE,
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

      rk_net <- dplyr::min_rank(dplyr::desc(df$net_rtg))
      rk_off <- dplyr::min_rank(dplyr::desc(df$off_ppp))
      rk_def <- dplyr::min_rank(df$def_ppp)

      d_net <- rep(NA_integer_, nrow(df))
      d_off <- rep(NA_integer_, nrow(df))
      d_def <- rep(NA_integer_, nrow(df))
      prev <- et_prev_data()
      if (!is.null(prev) && nrow(prev)) {
        pkey <- as.character(prev$team_id)
        key <- as.character(df$team_id)
        d_net <- as.integer(stats::setNames(dplyr::min_rank(dplyr::desc(prev$net_rtg)), pkey)[key]) - as.integer(rk_net)
        d_off <- as.integer(stats::setNames(dplyr::min_rank(dplyr::desc(prev$off_ppp)), pkey)[key]) - as.integer(rk_off)
        d_def <- as.integer(stats::setNames(dplyr::min_rank(prev$def_ppp), pkey)[key]) - as.integer(rk_def)
      }

      disp <- data.frame(
        team_name = df$team_name,
        games_played = df$games_played,
        wins = df$wins,
        losses = df$losses,
        off_ppp = fmt_rank_cell(df$off_ppp, rk_off, d_off, 1),
        def_ppp = fmt_rank_cell(df$def_ppp, rk_def, d_def, 1),
        net_rtg = fmt_rank_cell(df$net_rtg, rk_net, d_net, 1),
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
        colnames = c("Team", "GP", "W", "L", "Off PPP", "Def PPP", "Net Rtg",
                     "Off Poss", "Def Poss",
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
  output$euroteam_filter_chips <- renderUI({
    td <- et_teams_df()
    team_map <- if (!is.null(td) && nrow(td)) {
      stats::setNames(as.character(td$team_name), as.character(td$team_id))
    } else NULL
    map_teams <- function(ids) {
      if (is.null(ids) || !length(ids) || is.null(team_map)) return(as.character(ids))
      out <- unname(team_map[as.character(ids)])
      out[is.na(out)] <- as.character(ids)[is.na(out)]
      out
    }

    chips <- list(
      tags$span(class = "filter-chip chip-season",
                paste(EURO_COMPETITION_LABELS[[et_competition()]] %||% et_competition(),
                      euro_season_label(et_season())))
    )
    add <- function(label, clear_id) {
      chips[[length(chips) + 1L]] <<- make_chip(label, clear_id, "chip-game")
    }

    p <- et_params()
    if (p$start_d != p$bounds$start || p$end_d != p$bounds$end) {
      add(paste(format(p$start_d, "%b %d"), "–", format(p$end_d, "%b %d")),
          "euroteam_clear_dates")
    }
    if (length(input$euroteam_phase) && any(nzchar(input$euroteam_phase))) {
      add(paste(euro_phase_label(input$euroteam_phase), collapse = ", "), "euroteam_clear_game_type")
    }
    if (length(input$euroteam_teams) && any(nzchar(input$euroteam_teams))) {
      lbl <- map_teams(input$euroteam_teams)
      add(if (length(lbl) == 1) lbl else paste(length(lbl), "teams"), "euroteam_clear_teams")
    }
    if (length(input$euroteam_opponents) && any(nzchar(input$euroteam_opponents))) {
      lbl <- map_teams(input$euroteam_opponents)
      add(paste("vs", if (length(lbl) == 1) lbl else paste(length(lbl), "teams")),
          "euroteam_clear_opponents")
    }
    if (nzchar(input$euroteam_home_away %||% "")) {
      add(if (identical(input$euroteam_home_away, "home")) "Home" else "Away", "euroteam_clear_home_away")
    }
    if (nzchar(input$euroteam_outcome %||% "")) {
      add(if (identical(input$euroteam_outcome, "win")) "Wins" else "Losses", "euroteam_clear_outcome")
    }
    if (!is.na(p$last_n)) {
      add(paste("Last", p$last_n, "games"), "euroteam_clear_last_n")
    } else if (!is.na(p$min_gn) || !is.na(p$max_gn)) {
      lo <- if (is.na(p$min_gn)) "1" else as.character(p$min_gn)
      hi <- if (is.na(p$max_gn)) "∞" else as.character(p$max_gn)
      add(paste0("Rounds ", lo, "–", hi), "euroteam_clear_gn")
    }
    for (side in c("off", "def")) {
      mode_v <- input[[paste0("euroteam_num_starters_", side, "_mode")]]
      val_v <- input[[paste0("euroteam_num_starters_", side)]]
      if (nzchar(mode_v %||% "") && nzchar(val_v %||% "")) {
        add(paste0(if (side == "off") "Own" else "Opp", " starters ",
                   if (identical(mode_v, "gte")) "≥" else "≤", " ", val_v),
            "euroteam_clear_starters")
      }
    }
    if (nzchar(input$euroteam_opp_rank_side %||% "") && nzchar(input$euroteam_opp_rank_n %||% "")) {
      add(paste(if (identical(input$euroteam_opp_rank_side, "top")) "vs Top" else "vs Bottom",
                input$euroteam_opp_rank_n), "euroteam_clear_opp_rank")
    }

    div(class = "filter-chips-bar", chips)
  })

  setup_chip_clears("euroteam", session, input, shared,
    game_type_id = "euroteam_phase", opponents_id = "euroteam_opponents",
    home_away_id = "euroteam_home_away", outcome_id = "euroteam_outcome",
    gn_min_id = "euroteam_gn_min", gn_max_id = "euroteam_gn_max",
    last_n_id = "euroteam_last_n",
    opp_rank_ids = c("euroteam_opp_rank_side", "euroteam_opp_rank_n", "euroteam_opp_rank_metric"),
    date_id = "euroteam_dates", gy_input_id = "euroteam_game_year",
    teams_ids = NULL,
    starters_ids = c("euroteam_num_starters_off_mode", "euroteam_num_starters_off",
                     "euroteam_num_starters_def_mode", "euroteam_num_starters_def"))

  observeEvent(input$euroteam_clear_teams, {
    updateSelectizeInput(session, "euroteam_teams", selected = character(0))
  }, ignoreInit = TRUE)
}
