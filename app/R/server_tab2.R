# server_tab2.R - Tab 2: Lineup Data server logic

server_tab2 <- function(input, output, session, shared) {

  ld_ref <- reactiveValues(teams = NULL, players = NULL)
  auto_min_state <- reactiveValues(
    last_auto = NA_integer_,
    updating = FALSE
  )
  auto_enabled <- reactiveVal(TRUE)
  resetting <- reactiveVal(FALSE)
  AUTO_TARGET_ROWS <- 150L

  auto_minposs_from_df <- function(df, usage_col = "total_poss", step = 10L, target_rows = AUTO_TARGET_ROWS) {
    if (is.null(df) || !NROW(df)) return(NA_integer_)
    if (!usage_col %in% names(df)) return(NA_integer_)
    vals <- suppressWarnings(as.numeric(df[[usage_col]]))
    vals <- vals[is.finite(vals)]
    if (!length(vals)) return(NA_integer_)
    vals <- sort(vals, decreasing = TRUE)
    n <- length(vals)
    if (n <= target_rows) return(0L)
    kth <- vals[target_rows]
    if (!is.finite(kth)) return(NA_integer_)
    as.integer(ceiling(kth / step) * step)
  }

  observeEvent(list(input$main_tabs, input$game_year), ignoreInit = TRUE, {
    if (!identical(input$main_tabs, "lineup_data")) return(NULL)
    gy_int <- as.integer(input$game_year)

    teams_ld <- DBI::dbGetQuery(pg_pool, sprintf("SELECT DISTINCT team_id, MIN(team_name) AS team_name FROM basketball_test.full_rosters WHERE game_year = %d GROUP BY team_id ORDER BY MIN(team_name)", gy_int))
    ld_ref$teams <- teams_ld
    team_values <- c("", as.character(teams_ld$team_id))
    names(team_values) <- c("— All teams —", teams_ld$team_name)
    updateSelectizeInput(session, "ld_team", choices = team_values, selected = "", server = TRUE)

    players_map <- DBI::dbGetQuery(pg_pool, sprintf("SELECT team_id, player_id, MIN(btrim(firstname)||' '||btrim(lastname)) AS name FROM basketball_test.full_rosters WHERE game_year = %d GROUP BY team_id, player_id ORDER BY MIN(btrim(firstname)||' '||btrim(lastname))", gy_int))
    ld_ref$players <- players_map

    updateSelectizeInput(session, "ld_players_on", choices = setNames(integer(0), character(0)), selected = character(0), server = TRUE)
    updateSelectizeInput(session, "ld_players_off", choices = setNames(integer(0), character(0)), selected = character(0), server = TRUE)

    gn_df <- DBI::dbGetQuery(pg_pool,
      sprintf("SELECT DISTINCT gn FROM basketball_test.final_schedule_mv WHERE game_year = %d ORDER BY gn", gy_int))
    gn_vals <- if (nrow(gn_df)) as.integer(gn_df$gn) else integer(0)
    gn_choices <- c("", as.character(gn_vals))
    last_choices <- if (length(gn_vals)) c("", as.character(seq_len(max(gn_vals, na.rm = TRUE)))) else ""
    updateSelectizeInput(session, "ld_gn_min", choices = gn_choices, selected = "")
    updateSelectizeInput(session, "ld_gn_max", choices = gn_choices, selected = "")
    updateSelectizeInput(session, "ld_last_n", choices = last_choices, selected = "")
  })

  observeEvent(input$ld_team, {
    req(identical(input$main_tabs, "lineup_data"))
    if (is.null(input$ld_team) || is.na(input$ld_team) || !nzchar(input$ld_team)) {
      updateSelectizeInput(session, "ld_players_on", choices = setNames(integer(0), character(0)), selected = character(0), server = TRUE)
      updateSelectizeInput(session, "ld_players_off", choices = setNames(integer(0), character(0)), selected = character(0), server = TRUE)
      return(invisible(NULL))
    }
    team_id <- as.integer(input$ld_team)
    players <- ld_ref$players %>% filter(team_id == !!team_id)
    choices <- setNames(players$player_id, players$name)
    # Clear old tags first, then set new choices
    updateSelectizeInput(session, "ld_players_on", choices = setNames(integer(0), character(0)), selected = character(0), server = TRUE)
    updateSelectizeInput(session, "ld_players_off", choices = setNames(integer(0), character(0)), selected = character(0), server = TRUE)
    updateSelectizeInput(session, "ld_players_on", choices = choices, selected = character(0), server = TRUE)
    updateSelectizeInput(session, "ld_players_off", choices = choices, selected = character(0), server = TRUE)
  }, ignoreInit = TRUE)

  observeEvent(input$ld_players_on, {
    on_sel <- input$ld_players_on %||% character(0)
    off_sel <- input$ld_players_off %||% character(0)
    inter <- intersect(on_sel, off_sel)
    if (length(inter)) updateSelectizeInput(session, "ld_players_off", selected = setdiff(off_sel, inter))
  }, ignoreInit = TRUE)

  observeEvent(input$ld_players_off, {
    on_sel <- input$ld_players_on %||% character(0)
    off_sel <- input$ld_players_off %||% character(0)
    inter <- intersect(on_sel, off_sel)
    if (length(inter)) updateSelectizeInput(session, "ld_players_on", selected = setdiff(on_sel, inter))
  }, ignoreInit = TRUE)

  observeEvent(input$ld_reset, {
    resetting(TRUE)
    updateRadioButtons(session, "ld_view_mode", selected = "Summary")
    updateRadioButtons(session, "ld_num", selected = LD_DEFAULT_NUM)
    updateDateRangeInput(session, "ld_dates", start = NA, end = NA)
    if (!is.null(ld_ref$teams)) {
      team_values <- c("", as.character(ld_ref$teams$team_id))
      names(team_values) <- c("— All teams —", ld_ref$teams$team_name)
      updateSelectizeInput(session, "ld_team", choices = team_values, selected = "", server = TRUE)
    } else {
      updateSelectizeInput(session, "ld_team", selected = "", server = TRUE)
    }
    updateSelectizeInput(session, "ld_players_on", choices = setNames(integer(0), character(0)), selected = character(0), server = TRUE)
    updateSelectizeInput(session, "ld_players_off", choices = setNames(integer(0), character(0)), selected = character(0), server = TRUE)
    updateSelectInput(session, "ld_num_starters_off_mode", selected = "")
    updateSelectInput(session, "ld_num_starters_off", selected = "")
    updateSelectInput(session, "ld_num_starters_def_mode", selected = "")
    updateSelectInput(session, "ld_num_starters_def", selected = "")
    updateSliderInput(session, "ld_minposs", value = LD_DEFAULT_MIN_POSS)
    updateSelectizeInput(session, "ld_game_type", selected = "")
    updateSelectizeInput(session, "ld_opponents", selected = character(0))
    updateSelectInput(session, "ld_home_away", selected = "")
    updateSelectInput(session, "ld_outcome", selected = "")
    updateSelectInput(session, "ld_opp_rank_side", selected = "")
    updateSelectInput(session, "ld_opp_rank_n", selected = "")
    updateSelectInput(session, "ld_opp_rank_metric", selected = "")
    updateCheckboxInput(session, "ld_clutch_enabled", value = FALSE)
    updateSliderInput(session, "ld_clutch_margin", value = 5)
    updateSelectInput(session, "ld_clutch_status", selected = "all")
    updateSliderInput(session, "ld_clutch_minutes", value = 5)
    updateCheckboxInput(session, "ld_clutch_ot_margin", value = FALSE)
    updateSelectizeInput(session, "ld_gn_min", selected = "")
    updateSelectizeInput(session, "ld_gn_max", selected = "")
    updateSelectizeInput(session, "ld_last_n", selected = "")
    auto_min_state$last_auto <- as.integer(LD_DEFAULT_MIN_POSS)
    auto_enabled(FALSE)
    session$onFlushed(function() resetting(FALSE), once = TRUE)
  })

  observeEvent(input$ld_last_n, {
    if (!is.null(input$ld_last_n) && nzchar(input$ld_last_n)) {
      updateSelectizeInput(session, "ld_gn_min", selected = "")
      updateSelectizeInput(session, "ld_gn_max", selected = "")
    }
  }, ignoreInit = TRUE)

  observeEvent(list(input$ld_gn_min, input$ld_gn_max), {
    if ((nzchar(input$ld_gn_min %||% "") || nzchar(input$ld_gn_max %||% "")) &&
        nzchar(input$ld_last_n %||% "")) {
      updateSelectizeInput(session, "ld_last_n", selected = "")
    }
  }, ignoreInit = TRUE)

  run_fetch_lineups_20 <- function(pool, num, team_csv, player_csv, player_off_csv, exact, start_date, end_date, min_poss, game_year, game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric, max_margin, margin_status, max_time_remaining, ot_margin_filter, min_gn = NA_integer_, max_gn = NA_integer_, last_n_games = NA_integer_, num_starters_off = NA_integer_, num_starters_def = NA_integer_, num_starters_off_min = NA_integer_, num_starters_off_max = NA_integer_, num_starters_def_min = NA_integer_, num_starters_def_max = NA_integer_) {
    allowed <- guard_heavy_request(
      session, key = "tab2_lineups_summary",
      start_d = start_date, end_d = end_date,
      min_gn = min_gn, max_gn = max_gn, last_n = last_n_games,
      max_calls = 40L, window_sec = 60L
    )
    if (!isTRUE(allowed)) return(data.frame())
    DBI::dbGetQuery(pool, paste0("SELECT * FROM basketball_test.fetch_lineups_csv_v2(", "$1::int4,$2::text,$3::text,$4::text,$5::bool,$6::date,$7::date,$8::int4,$9::int4,", "$10::text,$11::text,$12::text,$13::text,$14::text,$15::int4,$16::text,$17::int4,$18::text,$19::int4,$20::bool,$21::int4,$22::int4,$23::int4,$24::int4,$25::int4,$26::int4,$27::int4,$28::int4,$29::int4", ")"), params = list(as.integer(num), team_csv, player_csv, player_off_csv, as.logical(exact), as.Date(start_date), as.Date(end_date), as.integer(min_poss), as.integer(game_year), game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric, max_margin, margin_status, max_time_remaining, ot_margin_filter, min_gn, max_gn, last_n_games, num_starters_off, num_starters_def, num_starters_off_min, num_starters_off_max, num_starters_def_min, num_starters_def_max))
  }

  run_fetch_lineups_ff_20 <- function(pool, num, team_csv, player_csv, player_off_csv, exact, start_date, end_date, min_poss, game_year, game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric, max_margin, margin_status, max_time_remaining, ot_margin_filter, min_gn = NA_integer_, max_gn = NA_integer_, last_n_games = NA_integer_, num_starters_off = NA_integer_, num_starters_def = NA_integer_, num_starters_off_min = NA_integer_, num_starters_off_max = NA_integer_, num_starters_def_min = NA_integer_, num_starters_def_max = NA_integer_) {
    allowed <- guard_heavy_request(
      session, key = "tab2_lineups_ff",
      start_d = start_date, end_d = end_date,
      min_gn = min_gn, max_gn = max_gn, last_n = last_n_games,
      max_calls = 40L, window_sec = 60L
    )
    if (!isTRUE(allowed)) return(data.frame())
    DBI::dbGetQuery(pool, paste0("SELECT * FROM basketball_test.fetch_lineups_four_factors_csv(", "$1::int4,$2::text,$3::text,$4::text,$5::bool,$6::date,$7::date,$8::int4,$9::int4,", "$10::text,$11::text,$12::text,$13::text,$14::text,$15::int4,$16::text,$17::int4,$18::text,$19::int4,$20::bool,$21::int4,$22::int4,$23::int4,$24::int4,$25::int4,$26::int4,$27::int4,$28::int4,$29::int4", ")"), params = list(as.integer(num), team_csv, player_csv, player_off_csv, as.logical(exact), as.Date(start_date), as.Date(end_date), as.integer(min_poss), as.integer(game_year), game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric, max_margin, margin_status, max_time_remaining, ot_margin_filter, min_gn, max_gn, last_n_games, num_starters_off, num_starters_def, num_starters_off_min, num_starters_off_max, num_starters_def_min, num_starters_def_max))
  }

  # --- Full ranked FF data (ranks computed BEFORE any local filtering) ---
  # Only re-fetches when game filters or group size change.
  # Team, players on/off, min poss are applied locally afterward.
  ld_ff_ranked_df <- reactive({
    req(identical(input$main_tabs, "lineup_data"))
    gy <- as.integer(input$game_year)
    num <- as.integer(input$ld_num)

    # Extract game filter params (same logic as ld_params)
    game_type_csv <- {
      x <- input$ld_game_type
      if (is.null(x) || !length(x) || !any(nzchar(x))) NA_character_ else paste(x[nzchar(x)], collapse = ",")
    }
    opp_ids_csv <- {
      ids <- shared$selected_opp_ids_ld()
      if (is.null(ids) || !length(ids)) NA_character_ else paste(ids, collapse = ",")
    }
    home_away <- if (!nzchar(input$ld_home_away %||% "")) NA_character_ else input$ld_home_away
    outcome <- if (!nzchar(input$ld_outcome %||% "")) NA_character_ else input$ld_outcome
    rank_side <- if (!nzchar(input$ld_opp_rank_side %||% "")) NA_character_ else input$ld_opp_rank_side
    rank_n <- suppressWarnings(as.integer(if (!nzchar(input$ld_opp_rank_n %||% "")) NA_character_ else input$ld_opp_rank_n))
    metric <- if (!nzchar(input$ld_opp_rank_metric %||% "")) NA_character_ else input$ld_opp_rank_metric

    start_date <- if (!is.null(input$ld_dates[1]) && !is.na(input$ld_dates[1])) as.Date(input$ld_dates[1]) else NA
    end_date <- if (!is.null(input$ld_dates[2]) && !is.na(input$ld_dates[2])) as.Date(input$ld_dates[2]) else NA

    # Extract GN params
    min_gn <- if (!is.null(input$ld_gn_min) && nzchar(input$ld_gn_min)) as.integer(input$ld_gn_min) else NA_integer_
    max_gn <- if (!is.null(input$ld_gn_max) && nzchar(input$ld_gn_max)) as.integer(input$ld_gn_max) else NA_integer_
    last_n <- if (!is.null(input$ld_last_n) && nzchar(input$ld_last_n)) as.integer(input$ld_last_n) else NA_integer_
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

    # Extract clutch params
    clutch_enabled <- isTRUE(input$ld_clutch_enabled)
    max_margin <- if (clutch_enabled) as.integer(input$ld_clutch_margin) else NA_integer_
    margin_status <- if (clutch_enabled) input$ld_clutch_status else NA_character_
    max_time_remaining <- if (clutch_enabled) as.integer(input$ld_clutch_minutes) * 60L else NA_integer_
    ot_margin_filter <- if (clutch_enabled) isTRUE(input$ld_clutch_ot_margin) else FALSE

    # Fetch ALL lineups for group size + game filters (no team/player/min_poss)
    off_mode <- input$ld_num_starters_off_mode %||% ""
    def_mode <- input$ld_num_starters_def_mode %||% ""
    off_val <- if (nzchar(off_mode) && nzchar(input$ld_num_starters_off %||% "")) as.integer(input$ld_num_starters_off) else NA_integer_
    def_val <- if (nzchar(def_mode) && nzchar(input$ld_num_starters_def %||% "")) as.integer(input$ld_num_starters_def) else NA_integer_
    num_starters_off_min <- if (identical(off_mode, "gte")) off_val else NA_integer_
    num_starters_off_max <- if (identical(off_mode, "lte")) off_val else NA_integer_
    num_starters_def_min <- if (identical(def_mode, "gte")) def_val else NA_integer_
    num_starters_def_max <- if (identical(def_mode, "lte")) def_val else NA_integer_

    df <- run_fetch_lineups_ff_20(pg_pool,
                                  num = num, team_csv = NA_character_, player_csv = NA_character_,
                                  player_off_csv = NA_character_, exact = TRUE,
                                  start_date = start_date, end_date = end_date,
                                  min_poss = 0L, game_year = gy,
                                  game_type_csv = game_type_csv, opp_ids_csv = opp_ids_csv,
                                  home_away = home_away, outcome = outcome,
                                  opp_rank_side = rank_side, opp_rank_n = rank_n, opp_rank_metric = metric,
                                  max_margin = max_margin, margin_status = margin_status, max_time_remaining = max_time_remaining,
                                  ot_margin_filter = ot_margin_filter, min_gn = min_gn, max_gn = max_gn, last_n_games = last_n,
                                  num_starters_off = NA_integer_, num_starters_def = NA_integer_,
                                  num_starters_off_min = num_starters_off_min, num_starters_off_max = num_starters_off_max,
                                  num_starters_def_min = num_starters_def_min, num_starters_def_max = num_starters_def_max)

    if (is.null(df) || NROW(df) == 0L) return(df)

    df$total_poss <- dplyr::coalesce(df$off_poss, 0L) + dplyr::coalesce(df$def_poss, 0L)

    # Compute percentile ranks on the FULL unfiltered dataset.
    # Adaptive baseline: lowers threshold when data is sparse.
    rank_thresh <- adaptive_baseline(df$total_poss)
    qualified <- df$total_poss >= rank_thresh

    pr_vec <- function(x, invert = FALSE) {
      vals <- ifelse(qualified, x, NA_real_)
      n <- sum(!is.na(vals))
      if (n <= 1) return(rep(NA_real_, length(vals)))
      r <- rank(vals, na.last = "keep", ties.method = "average")
      p <- (r - 1) / (n - 1)
      if (invert) p <- 1 - p
      as.numeric(p)
    }

    if ("off_ppp"  %in% names(df)) df$pr_off_ppp  <- pr_vec(df$off_ppp)
    if ("off_ts"   %in% names(df)) df$pr_off_ts   <- pr_vec(df$off_ts)
    if ("off_oreb" %in% names(df)) df$pr_off_oreb <- pr_vec(df$off_oreb)
    if ("off_tov"  %in% names(df)) df$pr_off_tov  <- pr_vec(df$off_tov, invert = TRUE)
    if ("off_ftr"  %in% names(df)) df$pr_off_ftr  <- pr_vec(df$off_ftr)
    if ("def_ppp"  %in% names(df)) df$pr_def_ppp  <- pr_vec(df$def_ppp, invert = TRUE)
    if ("def_ts"   %in% names(df)) df$pr_def_ts   <- pr_vec(df$def_ts, invert = TRUE)
    if ("def_oreb" %in% names(df)) df$pr_def_oreb <- pr_vec(df$def_oreb, invert = TRUE)
    if ("def_tov"  %in% names(df)) df$pr_def_tov  <- pr_vec(df$def_tov)
    if ("def_ftr"  %in% names(df)) df$pr_def_ftr  <- pr_vec(df$def_ftr, invert = TRUE)
    if ("net_rtg"  %in% names(df)) df$pr_net      <- pr_vec(df$net_rtg)

    df
  }) %>% bindEvent(input$ld_num, input$ld_dates, input$game_year,
                   input$ld_game_type, input$ld_opponents, input$ld_home_away,
                   input$ld_outcome, input$ld_opp_rank_side, input$ld_opp_rank_n,
                   input$ld_opp_rank_metric, input$main_tabs, input$ld_view_mode,
                   input$ld_clutch_enabled, input$ld_clutch_margin, input$ld_clutch_status, input$ld_clutch_minutes, input$ld_clutch_ot_margin,
                   input$ld_num_starters_off_mode, input$ld_num_starters_off, input$ld_num_starters_def_mode, input$ld_num_starters_def,
                   input$ld_gn_min, input$ld_gn_max, input$ld_last_n)

  # --- Full ranked Summary data (ranks computed BEFORE any local filtering) ---
  # Same pattern as ld_ff_ranked_df but for the Summary view.
  ld_summary_ranked_df <- reactive({
    req(identical(input$main_tabs, "lineup_data"))
    gy <- as.integer(input$game_year)
    num <- as.integer(input$ld_num)

    game_type_csv <- {
      x <- input$ld_game_type
      if (is.null(x) || !length(x) || !any(nzchar(x))) NA_character_ else paste(x[nzchar(x)], collapse = ",")
    }
    opp_ids_csv <- {
      ids <- shared$selected_opp_ids_ld()
      if (is.null(ids) || !length(ids)) NA_character_ else paste(ids, collapse = ",")
    }
    home_away <- if (!nzchar(input$ld_home_away %||% "")) NA_character_ else input$ld_home_away
    outcome <- if (!nzchar(input$ld_outcome %||% "")) NA_character_ else input$ld_outcome
    rank_side <- if (!nzchar(input$ld_opp_rank_side %||% "")) NA_character_ else input$ld_opp_rank_side
    rank_n <- suppressWarnings(as.integer(if (!nzchar(input$ld_opp_rank_n %||% "")) NA_character_ else input$ld_opp_rank_n))
    metric <- if (!nzchar(input$ld_opp_rank_metric %||% "")) NA_character_ else input$ld_opp_rank_metric

    # Extract GN params
    min_gn <- if (!is.null(input$ld_gn_min) && nzchar(input$ld_gn_min)) as.integer(input$ld_gn_min) else NA_integer_
    max_gn <- if (!is.null(input$ld_gn_max) && nzchar(input$ld_gn_max)) as.integer(input$ld_gn_max) else NA_integer_
    last_n <- if (!is.null(input$ld_last_n) && nzchar(input$ld_last_n)) as.integer(input$ld_last_n) else NA_integer_
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

    start_date <- if (!is.null(input$ld_dates[1]) && !is.na(input$ld_dates[1])) as.Date(input$ld_dates[1]) else NA
    end_date <- if (!is.null(input$ld_dates[2]) && !is.na(input$ld_dates[2])) as.Date(input$ld_dates[2]) else NA

    # Extract clutch params
    clutch_enabled <- isTRUE(input$ld_clutch_enabled)
    max_margin <- if (clutch_enabled) as.integer(input$ld_clutch_margin) else NA_integer_
    margin_status <- if (clutch_enabled) input$ld_clutch_status else NA_character_
    max_time_remaining <- if (clutch_enabled) as.integer(input$ld_clutch_minutes) * 60L else NA_integer_
    ot_margin_filter <- if (clutch_enabled) isTRUE(input$ld_clutch_ot_margin) else FALSE

    off_mode <- input$ld_num_starters_off_mode %||% ""
    def_mode <- input$ld_num_starters_def_mode %||% ""
    off_val <- if (nzchar(off_mode) && nzchar(input$ld_num_starters_off %||% "")) as.integer(input$ld_num_starters_off) else NA_integer_
    def_val <- if (nzchar(def_mode) && nzchar(input$ld_num_starters_def %||% "")) as.integer(input$ld_num_starters_def) else NA_integer_
    num_starters_off_min <- if (identical(off_mode, "gte")) off_val else NA_integer_
    num_starters_off_max <- if (identical(off_mode, "lte")) off_val else NA_integer_
    num_starters_def_min <- if (identical(def_mode, "gte")) def_val else NA_integer_
    num_starters_def_max <- if (identical(def_mode, "lte")) def_val else NA_integer_

    df <- run_fetch_lineups_20(pg_pool,
                               num = num, team_csv = NA_character_, player_csv = NA_character_,
                               player_off_csv = NA_character_, exact = TRUE,
                               start_date = start_date, end_date = end_date,
                               min_poss = 0L, game_year = gy,
                               game_type_csv = game_type_csv, opp_ids_csv = opp_ids_csv,
                               home_away = home_away, outcome = outcome,
                               opp_rank_side = rank_side, opp_rank_n = rank_n, opp_rank_metric = metric,
                               max_margin = max_margin, margin_status = margin_status, max_time_remaining = max_time_remaining,
                               ot_margin_filter = ot_margin_filter, min_gn = min_gn, max_gn = max_gn, last_n_games = last_n,
                               num_starters_off = NA_integer_, num_starters_def = NA_integer_,
                               num_starters_off_min = num_starters_off_min, num_starters_off_max = num_starters_off_max,
                               num_starters_def_min = num_starters_def_min, num_starters_def_max = num_starters_def_max)

    if (is.null(df) || NROW(df) == 0L) return(df)

    df$total_poss <- dplyr::coalesce(df$off_poss, 0L) + dplyr::coalesce(df$def_poss, 0L)
    df$plus_minus <- dplyr::coalesce(df$off_pts, 0) - dplyr::coalesce(df$def_pts, 0)

    # Adaptive baseline: lowers threshold when data is sparse
    rank_thresh <- adaptive_baseline(df$total_poss)
    qualified <- df$total_poss >= rank_thresh

    pr_vec <- function(x, invert = FALSE) {
      vals <- ifelse(qualified, x, NA_real_)
      n <- sum(!is.na(vals))
      if (n <= 1) return(rep(NA_real_, length(vals)))
      r <- rank(vals, na.last = "keep", ties.method = "average")
      p <- (r - 1) / (n - 1)
      if (invert) p <- 1 - p
      as.numeric(p)
    }

    if ("net_rtg" %in% names(df)) df$pr_ld_net       <- pr_vec(df$net_rtg)
    if ("off_ppp" %in% names(df)) df$pr_ld_off_ppp   <- pr_vec(df$off_ppp)
    if ("def_ppp" %in% names(df)) df$pr_ld_def_ppp_i <- pr_vec(df$def_ppp, invert = TRUE)

    df
  }) %>% bindEvent(input$ld_num, input$ld_dates, input$game_year,
                   input$ld_game_type, input$ld_opponents, input$ld_home_away,
                   input$ld_outcome, input$ld_opp_rank_side, input$ld_opp_rank_n,
                   input$ld_opp_rank_metric, input$main_tabs, input$ld_view_mode,
                   input$ld_clutch_enabled, input$ld_clutch_margin, input$ld_clutch_status, input$ld_clutch_minutes, input$ld_clutch_ot_margin,
                   input$ld_num_starters_off_mode, input$ld_num_starters_off, input$ld_num_starters_def_mode, input$ld_num_starters_def,
                   input$ld_gn_min, input$ld_gn_max, input$ld_last_n)

  ld_params <- reactive({
    req(identical(input$main_tabs, "lineup_data"))
    team_id <- if (!is.null(input$ld_team) && !is.na(input$ld_team) && nzchar(input$ld_team)) as.integer(input$ld_team) else NA_integer_
    player_on_ids <- if (!is.na(team_id)) as.integer(input$ld_players_on) else integer(0)
    player_off_ids <- if (!is.na(team_id)) as.integer(input$ld_players_off) else integer(0)
    ld_game_type_csv <- {
      x <- input$ld_game_type
      if (is.null(x) || !length(x) || !any(nzchar(x))) NA_character_ else paste(x[nzchar(x)], collapse = ",")
    }
    ld_opp_ids_csv <- {
      ids <- shared$selected_opp_ids_ld()
      if (is.null(ids) || !length(ids)) NA_character_ else paste(ids, collapse = ",")
    }
    ld_home_away <- if (!nzchar(input$ld_home_away %||% "")) NA_character_ else input$ld_home_away
    ld_outcome <- if (!nzchar(input$ld_outcome %||% "")) NA_character_ else input$ld_outcome
    ld_rank_side <- if (!nzchar(input$ld_opp_rank_side %||% "")) NA_character_ else input$ld_opp_rank_side
    ld_rank_n <- suppressWarnings(as.integer(if (!nzchar(input$ld_opp_rank_n %||% "")) NA_character_ else input$ld_opp_rank_n))
    ld_metric <- if (!nzchar(input$ld_opp_rank_metric %||% "")) NA_character_ else input$ld_opp_rank_metric

    min_gn <- if (!is.null(input$ld_gn_min) && nzchar(input$ld_gn_min)) as.integer(input$ld_gn_min) else NA_integer_
    max_gn <- if (!is.null(input$ld_gn_max) && nzchar(input$ld_gn_max)) as.integer(input$ld_gn_max) else NA_integer_
    last_n <- if (!is.null(input$ld_last_n) && nzchar(input$ld_last_n)) as.integer(input$ld_last_n) else NA_integer_
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

    off_mode <- input$ld_num_starters_off_mode %||% ""
    def_mode <- input$ld_num_starters_def_mode %||% ""
    off_val <- if (nzchar(off_mode) && nzchar(input$ld_num_starters_off %||% "")) as.integer(input$ld_num_starters_off) else NA_integer_
    def_val <- if (nzchar(def_mode) && nzchar(input$ld_num_starters_def %||% "")) as.integer(input$ld_num_starters_def) else NA_integer_
    ld_num_starters_off_min <- if (identical(off_mode, "gte")) off_val else NA_integer_
    ld_num_starters_off_max <- if (identical(off_mode, "lte")) off_val else NA_integer_
    ld_num_starters_def_min <- if (identical(def_mode, "gte")) def_val else NA_integer_
    ld_num_starters_def_max <- if (identical(def_mode, "lte")) def_val else NA_integer_

    list(num = as.integer(input$ld_num), team_csv = if (!is.na(team_id)) as.character(team_id) else NA_character_, player_csv = if (length(player_on_ids)) paste(player_on_ids, collapse = ",") else NA_character_, player_off_csv = if (length(player_off_ids)) paste(player_off_ids, collapse = ",") else NA_character_, exact = TRUE, start_date = if (!is.null(input$ld_dates[1]) && !is.na(input$ld_dates[1])) as.Date(input$ld_dates[1]) else NA, end_date = if (!is.null(input$ld_dates[2]) && !is.na(input$ld_dates[2])) as.Date(input$ld_dates[2]) else NA, min_poss = as.integer(input$ld_minposs), game_type_csv = ld_game_type_csv, opp_ids_csv = ld_opp_ids_csv, home_away = ld_home_away, outcome = ld_outcome, opp_rank_side = ld_rank_side, opp_rank_n = ld_rank_n, opp_rank_metric = ld_metric, min_gn = min_gn, max_gn = max_gn, last_n_games = last_n, num_starters_off = NA_integer_, num_starters_def = NA_integer_, num_starters_off_min = ld_num_starters_off_min, num_starters_off_max = ld_num_starters_off_max, num_starters_def_min = ld_num_starters_def_min, num_starters_def_max = ld_num_starters_def_max)
  }) %>% bindEvent(input$ld_num, input$ld_team, input$ld_players_on, input$ld_players_off, input$ld_dates, input$ld_minposs, input$main_tabs, input$ld_game_type, input$ld_opponents, input$ld_home_away, input$ld_outcome, input$ld_opp_rank_side, input$ld_opp_rank_n, input$ld_opp_rank_metric, input$ld_view_mode, input$ld_num_starters_off_mode, input$ld_num_starters_off, input$ld_num_starters_def_mode, input$ld_num_starters_def, input$ld_gn_min, input$ld_gn_max, input$ld_last_n)

  parse_player_ids <- function(x) {
    if (is.null(x)) return(integer(0))
    if (is.list(x)) {
      vals <- suppressWarnings(as.integer(unlist(x, use.names = FALSE)))
      return(vals[!is.na(vals)])
    }
    s <- gsub("[{}\\s]", "", as.character(x))
    if (!nzchar(s)) return(integer(0))
    vals <- suppressWarnings(as.integer(strsplit(s, ",", fixed = TRUE)[[1]]))
    vals[!is.na(vals)]
  }

  ensure_player_ids_list <- function(df) {
    if (is.null(df) || NROW(df) == 0L || !("player_ids" %in% names(df))) return(df)
    if ("player_ids_list" %in% names(df)) return(df)
    df$player_ids_list <- lapply(df$player_ids, parse_player_ids)
    df
  }

  apply_local_lineup_filters <- function(df, p) {
    if (is.null(df) || NROW(df) == 0L) return(df)
    df <- ensure_player_ids_list(df)
    if (!is.na(p$team_csv) && nzchar(p$team_csv)) {
      team_ids <- as.integer(strsplit(p$team_csv, ",")[[1]])
      df <- df %>% filter(team_id %in% team_ids)
    }
    if (!is.na(p$player_csv) && nzchar(p$player_csv)) {
      on_ids <- as.integer(strsplit(p$player_csv, ",")[[1]])
      keep <- vapply(df$player_ids_list, function(x) all(on_ids %in% x), logical(1))
      df <- df[keep, , drop = FALSE]
    }
    if (!is.na(p$player_off_csv) && nzchar(p$player_off_csv)) {
      off_ids <- as.integer(strsplit(p$player_off_csv, ",")[[1]])
      keep <- vapply(df$player_ids_list, function(x) !any(off_ids %in% x), logical(1))
      df <- df[keep, , drop = FALSE]
    }
    df
  }

  observeEvent(input$ld_minposs, {
    if (isTRUE(auto_min_state$updating)) return(invisible(NULL))
    cur_val <- as.integer(input$ld_minposs)
    last_auto <- as.integer(auto_min_state$last_auto)
    if (!is.na(cur_val) && !is.na(last_auto) && cur_val == last_auto) {
      return(invisible(NULL))
    }
    auto_enabled(FALSE)
  }, ignoreInit = TRUE)

  observeEvent(list(input$ld_num, input$ld_team, input$ld_players_on, input$ld_players_off,
                    input$ld_dates, input$main_tabs, input$ld_game_type, input$ld_opponents,
                    input$ld_home_away, input$ld_outcome, input$ld_opp_rank_side,
                    input$ld_opp_rank_n, input$ld_opp_rank_metric, input$ld_view_mode,
                    input$ld_num_starters_off_mode, input$ld_num_starters_off, input$ld_num_starters_def_mode, input$ld_num_starters_def,
                    input$ld_gn_min, input$ld_gn_max, input$ld_last_n,
                    input$ld_clutch_enabled, input$ld_clutch_margin, input$ld_clutch_status,
                    input$ld_clutch_minutes, input$ld_clutch_ot_margin, input$game_year), {
    if (isTRUE(resetting())) return(invisible(NULL))
    auto_enabled(TRUE)
  }, ignoreInit = TRUE)

  observeEvent(list(input$ld_num, input$ld_team, input$ld_players_on, input$ld_players_off,
                    input$ld_dates, input$main_tabs, input$ld_game_type, input$ld_opponents,
                    input$ld_home_away, input$ld_outcome, input$ld_opp_rank_side,
                    input$ld_opp_rank_n, input$ld_opp_rank_metric, input$ld_view_mode,
                    input$ld_num_starters_off_mode, input$ld_num_starters_off, input$ld_num_starters_def_mode, input$ld_num_starters_def,
                    input$ld_gn_min, input$ld_gn_max, input$ld_last_n,
                    input$ld_clutch_enabled, input$ld_clutch_margin, input$ld_clutch_status,
                    input$ld_clutch_minutes, input$ld_clutch_ot_margin, input$game_year), {
    req(identical(input$main_tabs, "lineup_data"))
    if (!isTRUE(auto_enabled())) return(invisible(NULL))
    p <- ld_params()
    if (is.null(p)) return(invisible(NULL))

    mode <- input$ld_view_mode
    df_base <- if (identical(mode, "Four Factors")) ld_ff_ranked_df() else ld_summary_ranked_df()
    df_base <- apply_local_lineup_filters(df_base, p)

    min_needed <- auto_minposs_from_df(df_base, usage_col = "total_poss", step = 10L, target_rows = AUTO_TARGET_ROWS)
    cur_val <- as.integer(input$ld_minposs)
    if (is.na(min_needed)) return(invisible(NULL))
    if (!is.na(cur_val) && cur_val == min_needed) return(invisible(NULL))

    auto_min_state$updating <- TRUE
    updateSliderInput(session, "ld_minposs", value = min_needed)
    auto_min_state$updating <- FALSE
    auto_min_state$last_auto <- min_needed
  }, ignoreInit = TRUE)

  ld_data <- reactive({
    req(ld_params())
    p <- ld_params()
    gy <- as.integer(input$game_year)
    mode <- input$ld_view_mode

    if (identical(mode, "Four Factors")) {
      # Get pre-ranked data (ranks computed on full unfiltered population)
      df <- ld_ff_ranked_df()

      if (is.null(df) || NROW(df) == 0L) {
        return(data.frame(
          team_id = integer(0), player_names_str = character(0),
          off_ts = numeric(0), off_oreb = numeric(0), off_tov = numeric(0), off_ftr = numeric(0),
          off_poss = integer(0), off_pts = integer(0), off_ppp = numeric(0),
          def_ts = numeric(0), def_oreb = numeric(0), def_tov = numeric(0), def_ftr = numeric(0),
          def_poss = integer(0), def_pts = integer(0), def_ppp = numeric(0),
          net_rtg = numeric(0), num_lineup = integer(0), sub_lineup_hash = character(0),
          total_poss = integer(0),
          stringsAsFactors = FALSE
        ))
      }

      # --- Filter LOCALLY (ranks already computed on full data) ---

      # Filter by team
      if (!is.na(p$team_csv) && nzchar(p$team_csv)) {
        team_ids <- as.integer(strsplit(p$team_csv, ",")[[1]])
        df <- df %>% filter(team_id %in% team_ids)
      }

      # Filter by players on (lineup must contain all selected players)
      if (!is.na(p$player_csv) && nzchar(p$player_csv)) {
        df <- ensure_player_ids_list(df)
        on_ids <- as.integer(strsplit(p$player_csv, ",")[[1]])
        keep <- vapply(df$player_ids_list, function(x) all(on_ids %in% x), logical(1))
        df <- df[keep, , drop = FALSE]
      }

      # Filter by players off (lineup must NOT contain any excluded players)
      if (!is.na(p$player_off_csv) && nzchar(p$player_off_csv)) {
        df <- ensure_player_ids_list(df)
        off_ids <- as.integer(strsplit(p$player_off_csv, ",")[[1]])
        keep <- vapply(df$player_ids_list, function(x) !any(off_ids %in% x), logical(1))
        df <- df[keep, , drop = FALSE]
      }

      # Filter by min poss
      df <- df %>% filter(total_poss >= !!p$min_poss)

      df
    } else {
      # Get pre-ranked data (ranks computed on full unfiltered population)
      df <- ld_summary_ranked_df()

      if (is.null(df) || NROW(df) == 0L) {
        return(data.frame(
          team_id = integer(0), player_names_str = character(0),
          total_poss = integer(0), plus_minus = numeric(0),
          off_poss = integer(0), def_poss = integer(0),
          off_pts = numeric(0), def_pts = numeric(0),
          off_ppp = numeric(0), def_ppp = numeric(0),
          net_rtg = numeric(0), num_lineup = integer(0), num_starters = numeric(0),
          sub_lineup_hash = character(0),
          stringsAsFactors = FALSE
        ))
      }

      # --- Filter LOCALLY (ranks already computed on full data) ---

      # Filter by team
      if (!is.na(p$team_csv) && nzchar(p$team_csv)) {
        team_ids <- as.integer(strsplit(p$team_csv, ",")[[1]])
        df <- df %>% filter(team_id %in% team_ids)
      }

      # Filter by players on (lineup must contain all selected players)
      if (!is.na(p$player_csv) && nzchar(p$player_csv)) {
        df <- ensure_player_ids_list(df)
        on_ids <- as.integer(strsplit(p$player_csv, ",")[[1]])
        keep <- vapply(df$player_ids_list, function(x) all(on_ids %in% x), logical(1))
        df <- df[keep, , drop = FALSE]
      }

      # Filter by players off (lineup must NOT contain any excluded players)
      if (!is.na(p$player_off_csv) && nzchar(p$player_off_csv)) {
        df <- ensure_player_ids_list(df)
        off_ids <- as.integer(strsplit(p$player_off_csv, ",")[[1]])
        keep <- vapply(df$player_ids_list, function(x) !any(off_ids %in% x), logical(1))
        df <- df[keep, , drop = FALSE]
      }

      # Filter by min poss
      df <- df %>% filter(total_poss >= !!p$min_poss)

      df
    }
  })

  team_name_vec <- reactive({
    tdf <- isolate(ld_ref$teams)
    if (is.null(tdf)) return(character(0))
    setNames(tdf$team_name, as.character(tdf$team_id))
  })

  output$ld_table <- DT::renderDataTable({
    req(ld_params())
    df <- ld_data()
    mode <- input$ld_view_mode
    tmap <- team_name_vec()

    # Common: Map team names and create Players column
    if ("team_id" %in% names(df)) {
      df$Team <- unname(tmap[as.character(df$team_id)])
      df$Team[is.na(df$Team)] <- as.character(df$team_id[is.na(df$Team)])
    }
    if ("player_names_str" %in% names(df)) df$Players <- df$player_names_str

    if (identical(mode, "Four Factors")) {
      # ============================================================
      # FOUR FACTORS LINEUP TABLE
      # Ranks are pre-computed on the full unfiltered population
      # in ld_ff_ranked_df(), so colors stay stable across local filters.
      # ============================================================

      pr_cols <- c("pr_off_ppp", "pr_off_ts", "pr_off_oreb", "pr_off_tov", "pr_off_ftr",
                   "pr_def_ppp", "pr_def_ts", "pr_def_oreb", "pr_def_tov", "pr_def_ftr", "pr_net")

      keep_cols <- c("Team", "Players",
                     "off_ppp", "off_ts", "off_oreb", "off_tov", "off_ftr", "off_poss",
                     "def_ppp", "def_ts", "def_oreb", "def_tov", "def_ftr", "def_poss",
                     "minutes", "total_poss", "net_rtg", "team_id", "sub_lineup_hash")
      df <- df %>% select(any_of(c(keep_cols, pr_cols)))
      df$is_total <- rep(1, nrow(df))
      df <- df %>% arrange(desc(total_poss))

      # --- TOTAL row (rates from summed raw counts) ---
      if (nrow(df) > 0) {
        raw <- ld_data()
        sum_off_poss <- sum(df$off_poss, na.rm = TRUE)
        sum_def_poss <- sum(df$def_poss, na.rm = TRUE)
        sum_off_pts  <- sum(raw$off_pts, na.rm = TRUE)
        sum_def_pts  <- sum(raw$def_pts, na.rm = TRUE)
        tot_off_ppp <- if (sum_off_poss > 0) round((sum_off_pts / sum_off_poss) * 100, 1) else NA_real_
        tot_def_ppp <- if (sum_def_poss > 0) round((sum_def_pts / sum_def_poss) * 100, 1) else NA_real_
        tot_net_rtg <- if (!is.na(tot_off_ppp) && !is.na(tot_def_ppp)) round(tot_off_ppp - tot_def_ppp, 1) else NA_real_

        # Sum raw counts for four-factor rates
        s_off_ts_poss   <- sum(raw$off_ts_poss, na.rm = TRUE)
        s_off_oreb_cnt  <- sum(raw$off_oreb_cnt, na.rm = TRUE)
        s_off_oreb_opps <- sum(raw$off_oreb_opps, na.rm = TRUE)
        s_off_tov_cnt   <- sum(raw$off_tov_cnt, na.rm = TRUE)
        s_off_fta       <- sum(raw$off_fta, na.rm = TRUE)
        s_off_fga       <- sum(raw$off_fga_cnt, na.rm = TRUE)
        s_def_ts_poss   <- sum(raw$def_ts_poss, na.rm = TRUE)
        s_def_oreb_cnt  <- sum(raw$def_oreb_cnt, na.rm = TRUE)
        s_def_oreb_opps <- sum(raw$def_oreb_opps, na.rm = TRUE)
        s_def_tov_cnt   <- sum(raw$def_tov_cnt, na.rm = TRUE)
        s_def_fta       <- sum(raw$def_fta, na.rm = TRUE)
        s_def_fga       <- sum(raw$def_fga_cnt, na.rm = TRUE)

        tot_off_ts   <- if (s_off_ts_poss > 0) round(sum_off_pts / (2 * s_off_ts_poss) * 100, 1) else NA_real_
        tot_off_oreb <- if (s_off_oreb_opps > 0) round(s_off_oreb_cnt / s_off_oreb_opps * 100, 1) else NA_real_
        tot_off_tov  <- if (sum_off_poss > 0) round(s_off_tov_cnt / sum_off_poss * 100, 1) else NA_real_
        tot_off_ftr  <- if (s_off_fga > 0) round(s_off_fta / s_off_fga * 100, 1) else NA_real_
        tot_def_ts   <- if (s_def_ts_poss > 0) round(sum_def_pts / (2 * s_def_ts_poss) * 100, 1) else NA_real_
        tot_def_oreb <- if (s_def_oreb_opps > 0) round(s_def_oreb_cnt / s_def_oreb_opps * 100, 1) else NA_real_
        tot_def_tov  <- if (sum_def_poss > 0) round(s_def_tov_cnt / sum_def_poss * 100, 1) else NA_real_
        tot_def_ftr  <- if (s_def_fga > 0) round(s_def_fta / s_def_fga * 100, 1) else NA_real_

        sum_minutes <- sum(raw$minutes, na.rm = TRUE)
        total_row <- data.frame(
          Team = "TOTAL", Players = "— All Lineups —",
          off_ppp = tot_off_ppp, off_ts = tot_off_ts, off_oreb = tot_off_oreb, off_tov = tot_off_tov, off_ftr = tot_off_ftr,
          off_poss = sum_off_poss,
          def_ppp = tot_def_ppp, def_ts = tot_def_ts, def_oreb = tot_def_oreb, def_tov = tot_def_tov, def_ftr = tot_def_ftr,
          def_poss = sum_def_poss,
          minutes = sum_minutes,
          total_poss = sum_off_poss + sum_def_poss,
          net_rtg = tot_net_rtg,
          team_id = NA_integer_, sub_lineup_hash = NA_character_,
          is_total = 0, stringsAsFactors = FALSE
        )
        df <- dplyr::bind_rows(total_row, df)
      }

      df <- df %>% select(is_total, everything())

      # Build custom sketch header
      # Note: first th("") in each row accounts for hidden is_total column at position 0
      sketch_ff <- htmltools::withTags(table(class = 'display', thead(
        tr(
          th(""),
          th(class = "group-head", colspan = 2, ""),
          th(class = "group-head section-left-border", colspan = 6, "Offense"),
          th(class = "group-head section-left-border", colspan = 6, "Defense"),
          th(class = "group-head section-left-border", colspan = 3, "Usage")
        ),
        tr(
          th(""),
          th(class = "sub-head", "Team"), th(class = "sub-head", "Players"),
          th(class = "sub-head section-left-border", "PPP"), th(class = "sub-head", "TS%"),
          th(class = "sub-head", "OREB%"), th(class = "sub-head", "TOV%"),
          th(class = "sub-head", "FTR"), th(class = "sub-head", "Poss"),
          th(class = "sub-head section-left-border", "PPP"), th(class = "sub-head", "TS%"),
          th(class = "sub-head", "OREB%"), th(class = "sub-head", "TOV%"),
          th(class = "sub-head", "FTR"), th(class = "sub-head", "Poss"),
          th(class = "sub-head section-left-border", "Min"), th(class = "sub-head", "Poss"), th(class = "sub-head", "Net")
        )
      )))

      # Column indices for section borders
      ff_hash_idx <- which(names(df) == "sub_lineup_hash") - 1L
      ff_tid_idx  <- which(names(df) == "team_id") - 1L
      hide_idx <- c(0, which(colnames(df) %in% pr_cols) - 1L, ff_hash_idx, ff_tid_idx)
      off_ppp_idx  <- which(names(df) == "off_ppp") - 1L
      def_ppp_idx  <- which(names(df) == "def_ppp") - 1L
      minutes_idx  <- which(names(df) == "minutes") - 1L

      # Clickable Players column
      ff_players_idx <- which(names(df) == "Players") - 1L
      ff_players_render <- DT::JS(sprintf(
        "function(data, type, row, meta) {
           if (type !== 'display' || !row) return data;
           if (row[0] === 0) return data;
           var hash = row[%d];
           var tid = row[%d];
           return '<a href=\"#\" style=\"color:#e8a435;text-decoration:underline;cursor:pointer;\" onclick=\"Shiny.setInputValue(\\'ld_lineup_click\\', {hash: \\'' + hash + '\\', team_id: ' + tid + ', ts: Date.now()}, {priority: \\'event\\'}); return false;\">' + data + '</a>';
         }", ff_hash_idx, ff_tid_idx))

      col_defs <- list(
        list(targets = hide_idx, visible = FALSE),
        list(targets = "_all", className = "dt-center"),
        list(targets = ff_players_idx, render = ff_players_render)
      )
      if (length(off_ppp_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = off_ppp_idx, className = "section-left-border dt-center")
      if (length(def_ppp_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = def_ppp_idx, className = "section-left-border dt-center")
      if (length(minutes_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = minutes_idx, className = "section-left-border dt-center")

      dt <- DT::datatable(df, container = sketch_ff, rownames = FALSE, escape = FALSE,
                          options = list(
                            dom = "tip", pageLength = 50,
                            lengthMenu = c(25, 50, 100, 200),
                            orderFixed = list(list(0, 'asc')),
                            deferRender = TRUE, scrollX = TRUE,
                            scrollY = "70vh", scrollCollapse = TRUE,
                            columnDefs = col_defs
                          ))

      # Format numbers
      rate_cols <- intersect(c("off_ts", "off_oreb", "off_tov", "off_ftr", "def_ts", "def_oreb", "def_tov", "def_ftr"), names(df))
      ppp_cols  <- intersect(c("off_ppp", "def_ppp", "net_rtg"), names(df))
      poss_cols <- intersect(c("off_poss", "def_poss", "total_poss"), names(df))
      min_cols  <- intersect(c("minutes"), names(df))

      if (length(rate_cols)) dt <- DT::formatRound(dt, rate_cols, 1)
      if (length(ppp_cols))  dt <- DT::formatRound(dt, ppp_cols, 1)
      if (length(poss_cols)) dt <- DT::formatCurrency(dt, poss_cols, currency = "", interval = 3, mark = ",", digits = 0)
      if (length(min_cols))  dt <- DT::formatRound(dt, min_cols, 1)

      # TOTAL row styling
      dt <- DT::formatStyle(dt, "Team", target = "row",
                            backgroundColor = styleEqual("TOTAL", "#1a1f2b"),
                            fontWeight = styleEqual("TOTAL", "bold"))

      # Color logic
      if ("pr_off_ppp"  %in% names(df)) dt <- DT::formatStyle(dt, "off_ppp",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_ppp")
      if ("pr_off_ts"   %in% names(df)) dt <- DT::formatStyle(dt, "off_ts",   backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_ts")
      if ("pr_off_oreb" %in% names(df)) dt <- DT::formatStyle(dt, "off_oreb", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_oreb")
      if ("pr_off_tov"  %in% names(df)) dt <- DT::formatStyle(dt, "off_tov",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_tov")
      if ("pr_off_ftr"  %in% names(df)) dt <- DT::formatStyle(dt, "off_ftr",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_ftr")
      if ("pr_def_ppp"  %in% names(df)) dt <- DT::formatStyle(dt, "def_ppp",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_def_ppp")
      if ("pr_def_ts"   %in% names(df)) dt <- DT::formatStyle(dt, "def_ts",   backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_def_ts")
      if ("pr_def_oreb" %in% names(df)) dt <- DT::formatStyle(dt, "def_oreb", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_def_oreb")
      if ("pr_def_tov"  %in% names(df)) dt <- DT::formatStyle(dt, "def_tov",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_def_tov")
      if ("pr_def_ftr"  %in% names(df)) dt <- DT::formatStyle(dt, "def_ftr",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_def_ftr")
      if ("pr_net"      %in% names(df)) dt <- DT::formatStyle(dt, "net_rtg",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_net")

      return(dt)

    } else {
      # ============================================================
      # SUMMARY LINEUP TABLE (existing behavior)
      # ============================================================

      pr_cols <- c("pr_ld_net", "pr_ld_off_ppp", "pr_ld_def_ppp_i")
      shot_raw_cols <- c("off_fg2_made", "off_fg2_att", "off_fg3_made", "off_fg3_att",
                         "def_fg2_made", "def_fg2_att", "def_fg3_made", "def_fg3_att")
      has_shots <- all(c("off_fg2_att", "off_fg3_att") %in% names(df))
      if (!("num_starters" %in% names(df)) && ("num_lineup" %in% names(df))) {
        df$num_starters <- df$num_lineup
      }

      # Create display columns for sorting (total FGA)
      if (has_shots) {
        df[["Off Shot"]] <- dplyr::coalesce(df$off_fg2_att, 0L) + dplyr::coalesce(df$off_fg3_att, 0L)
        df[["Def Shot"]] <- dplyr::coalesce(df$def_fg2_att, 0L) + dplyr::coalesce(df$def_fg3_att, 0L)
      }

      keep_cols <- c("Team", "Players", "minutes", "total_poss", "plus_minus",
                     if (has_shots) c("Off Shot", "Def Shot"),
                     "off_poss", "def_poss", "off_pts", "def_pts", "off_ppp", "def_ppp", "net_rtg", "num_starters", "sub_lineup_hash", "team_id")
      df <- df %>% select(any_of(c(keep_cols, shot_raw_cols, pr_cols)))
      df$is_total <- rep(1, nrow(df))
      if ("net_rtg" %in% names(df)) df <- df %>% arrange(desc(total_poss))
      if (nrow(df) > 0) {
        sum_off_poss <- sum(df$off_poss, na.rm = TRUE)
        sum_def_poss <- sum(df$def_poss, na.rm = TRUE)
        sum_off_pts <- sum(df$off_pts, na.rm = TRUE)
        sum_def_pts <- sum(df$def_pts, na.rm = TRUE)
        sum_minutes <- sum(df$minutes, na.rm = TRUE)
        tot_off_ppp <- if (sum_off_poss > 0) (sum_off_pts / sum_off_poss) * 100 else 0
        tot_def_ppp <- if (sum_def_poss > 0) (sum_def_pts / sum_def_poss) * 100 else 0
        tot_net_rtg <- tot_off_ppp - tot_def_ppp
        total_row <- data.frame(Team = "TOTAL", Players = "— All Lineups —", minutes = sum_minutes, total_poss = sum_off_poss + sum_def_poss, off_ppp = tot_off_ppp, def_ppp = tot_def_ppp, net_rtg = tot_net_rtg, plus_minus = sum_off_pts - sum_def_pts, off_poss = sum_off_poss, off_pts = sum_off_pts, def_poss = sum_def_poss, def_pts = sum_def_pts, num_starters = NA_real_, sub_lineup_hash = "TOTAL", team_id = NA_integer_, is_total = 0, stringsAsFactors = FALSE)
        # Add shooting totals
        if (has_shots) {
          for (sc in shot_raw_cols) total_row[[sc]] <- sum(df[[sc]], na.rm = TRUE)
          total_row[["Off Shot"]] <- total_row$off_fg2_att + total_row$off_fg3_att
          total_row[["Def Shot"]] <- total_row$def_fg2_att + total_row$def_fg3_att
        }
        df <- dplyr::bind_rows(total_row, df)
      }
      df <- df %>% select(is_total, everything())
      show_cols <- c("Team", "Players", "minutes", "total_poss", "off_ppp", "def_ppp", "net_rtg", "plus_minus",
                     if (has_shots) c("Off Shot", "Def Shot"),
                     "off_poss", "off_pts", "def_poss", "def_pts", "num_starters", "sub_lineup_hash", "team_id")

      keep <- intersect(show_cols, names(df))
      df <- df[, unique(c("is_total", keep, shot_raw_cols[shot_raw_cols %in% names(df)], pr_cols[pr_cols %in% names(df)])), drop = FALSE]
      pretty_labels <- c(Team = "Team", Players = "Players", minutes = "Min", num_starters = "# Starters", total_poss = "Total Poss", net_rtg = "Net RTG", `plus_minus` = "+/-", off_ppp = "Off PPP", def_ppp = "Def PPP", off_poss = "Off Poss", off_pts = "Off Pts", def_poss = "Def Poss", def_pts = "Def Pts", sub_lineup_hash = "Lineup ID", team_id = "team_id", `Off Shot` = "Off Shot", `Def Shot` = "Def Shot")

      # Shooting column JS render function factory (same pattern as Tab 1)
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
          "Off Shot" = c("off_fg2_made", "off_fg2_att", "off_fg3_made", "off_fg3_att"),
          "Def Shot" = c("def_fg2_made", "def_fg2_att", "def_fg3_made", "def_fg3_att")
        )
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

      data_col_names <- colnames(df)[-1]
      data_col_names <- setdiff(data_col_names, c(pr_cols, shot_raw_cols))
      col_labels <- unname(pretty_labels[data_col_names])
      final_labels <- c("", col_labels)
      pr_indices <- which(colnames(df) %in% pr_cols) - 1L
      shot_raw_indices <- which(colnames(df) %in% shot_raw_cols) - 1L
      sum_hash_idx <- which(names(df) == "sub_lineup_hash") - 1L
      sum_tid_idx <- which(names(df) == "team_id") - 1L
      hidden_indices <- c(0, pr_indices, shot_raw_indices, sum_hash_idx, sum_tid_idx)

      # Clickable Players column
      sum_players_idx <- which(names(df) == "Players") - 1L
      sum_players_render <- DT::JS(sprintf(
        "function(data, type, row, meta) {
           if (type !== 'display' || !row) return data;
           if (row[0] === 0) return data;
           var hash = row[%d];
           var tid = row[%d];
           return '<a href=\"#\" style=\"color:#e8a435;text-decoration:underline;cursor:pointer;\" onclick=\"Shiny.setInputValue(\\'ld_lineup_click\\', {hash: \\'' + hash + '\\', team_id: ' + tid + ', ts: Date.now()}, {priority: \\'event\\'}); return false;\">' + data + '</a>';
         }", sum_hash_idx, sum_tid_idx))

      all_col_defs <- c(list(list(targets = hidden_indices, visible = FALSE),
                             list(targets = sum_players_idx, render = sum_players_render)),
                        shot_col_defs)

      dt <- DT::datatable(df, colnames = final_labels, rownames = FALSE, escape = FALSE, filter = "top", options = list(pageLength = 50, lengthMenu = c(25, 50, 100, 200, 1000), orderFixed = list(list(0, 'asc')), deferRender = TRUE, scrollX = TRUE, scrollY = "70vh", scrollCollapse = TRUE, processing = TRUE, columnDefs = all_col_defs)) |>
        DT::formatRound(c("off_ppp", "def_ppp", "net_rtg", "minutes")[c("off_ppp", "def_ppp", "net_rtg", "minutes") %in% names(df)], 1) |>
        DT::formatCurrency(c("total_poss", "off_poss", "def_poss")[c("total_poss", "off_poss", "def_poss") %in% names(df)], currency = "", interval = 3, mark = ",", digits = 0) |>
        DT::formatCurrency(c("off_pts", "def_pts", "plus_minus")[c("off_pts", "def_pts", "plus_minus") %in% names(df)], currency = "", interval = 3, mark = ",", digits = 0)
      dt <- DT::formatStyle(dt, "Team", target = "row", backgroundColor = styleEqual("TOTAL", "#1a1f2b"), fontWeight = styleEqual("TOTAL", "bold"))
      if (all(c("net_rtg", "pr_ld_net") %in% colnames(df))) dt <- DT::formatStyle(dt, "net_rtg", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_ld_net")
      if (all(c("off_ppp", "pr_ld_off_ppp") %in% colnames(df))) dt <- DT::formatStyle(dt, "off_ppp", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_ld_off_ppp")
      if (all(c("def_ppp", "pr_ld_def_ppp_i") %in% colnames(df))) dt <- DT::formatStyle(dt, "def_ppp", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_ld_def_ppp_i")
      return(dt)
    }
  })

  # ============================================================
  # LINEUP CLICK → MODAL GAME LOG
  # ============================================================
  observeEvent(input$ld_lineup_click, {
    click <- input$ld_lineup_click
    req(click$hash, click$team_id)

    sub_hash <- as.character(click$hash)
    team_id_val <- as.integer(click$team_id)
    gy <- as.integer(input$game_year)
    view_mode <- input$ld_view_mode

    # Resolve sub_lineup_hash → lineup_hash(es)
    lineup_hashes <- DBI::dbGetQuery(pg_pool,
      "SELECT DISTINCT lineup_hash FROM basketball_test.sub_lineups
       WHERE sub_lineup_hash = $1 AND team_id = $2 AND game_year = $3",
      params = list(sub_hash, team_id_val, gy))$lineup_hash

    # If empty (5-man case), the sub_lineup_hash IS the lineup_hash
    if (length(lineup_hashes) == 0) lineup_hashes <- sub_hash

    lineup_hashes <- unique(as.character(lineup_hashes))
    lineup_hashes <- lineup_hashes[!is.na(lineup_hashes) & nzchar(lineup_hashes)]
    req(length(lineup_hashes) > 0)

    hash_placeholders <- paste(sprintf("$%d", seq_along(lineup_hashes)), collapse = ",")
    team_id_idx <- length(lineup_hashes) + 1
    gy_idx <- length(lineup_hashes) + 2
    hash_query_params <- c(as.list(lineup_hashes), list(team_id_val, gy))

    # Join schedule (shared between both views)
    sched <- DBI::dbGetQuery(
      pg_pool,
      "SELECT game_id, gn, game_date, opp_team_name, team_score, opp_score,
              team_score > opp_score AS has_won
       FROM basketball_test.final_schedule_mv
       WHERE team_id = $1 AND game_year = $2",
      params = list(team_id_val, gy)
    )
    sched <- sched %>% mutate(
      result = ifelse(has_won, "W", "L"),
      score_display = paste0(team_score, "-", opp_score)
    )

    # Get lineup name
    lineup_name <- DBI::dbGetQuery(pg_pool,
      "SELECT player_names_str FROM basketball_test.sub_lineups_stats
       WHERE sub_lineup_hash = $1 AND team_id = $2 AND game_year = $3 LIMIT 1",
      params = list(sub_hash, team_id_val, gy))$player_names_str
    if (length(lineup_name) == 0 || is.na(lineup_name)) lineup_name <- sub_hash

    if (identical(view_mode, "Four Factors")) {
      # ============================================================
      # FOUR FACTORS MODAL GAME LOG
      # ============================================================
      ff_query <- sprintf(
        "SELECT game_id, type_lineup,
                SUM(total_points) AS total_points, SUM(total_poss) AS total_poss,
                SUM(ts_poss_count) AS ts_poss_count, SUM(oreb_count) AS oreb_count,
                SUM(oreb_opportunities) AS oreb_opportunities, SUM(tov_count) AS tov_count,
                SUM(total_ft_attempts) AS total_ft_attempts, SUM(total_fga) AS total_fga,
                SUM(minutes) AS mins
         FROM basketball_test.lineup_four_factors_by_game
         WHERE lineup_hash IN (%s) AND team_id = $%d AND game_year = $%d
         GROUP BY game_id, type_lineup",
        hash_placeholders, team_id_idx, gy_idx
      )
      ff_data <- DBI::dbGetQuery(pg_pool, ff_query, params = hash_query_params)

      if (nrow(ff_data) == 0) {
        showModal(modalDialog(title = "No game data", "No games found for this lineup.", easyClose = TRUE))
        return()
      }

      off <- ff_data %>% filter(type_lineup == "offense") %>%
        rename(off_pts = total_points, off_poss = total_poss,
               off_ts_poss = ts_poss_count, off_oreb = oreb_count,
               off_oreb_opp = oreb_opportunities, off_tov = tov_count,
               off_fta = total_ft_attempts, off_fga = total_fga, off_mins = mins) %>%
        select(-type_lineup)
      def <- ff_data %>% filter(type_lineup == "defense") %>%
        rename(def_pts = total_points, def_poss = total_poss,
               def_ts_poss = ts_poss_count, def_oreb = oreb_count,
               def_oreb_opp = oreb_opportunities, def_tov = tov_count,
               def_fta = total_ft_attempts, def_fga = total_fga) %>%
        select(game_id, def_pts, def_poss, def_ts_poss, def_oreb, def_oreb_opp,
               def_tov, def_fta, def_fga)

      combined <- off %>% full_join(def, by = "game_id") %>% mutate(
        off_poss = coalesce(off_poss, 0), def_poss = coalesce(def_poss, 0),
        off_pts = coalesce(off_pts, 0), def_pts = coalesce(def_pts, 0),
        off_ppp = ifelse(off_poss > 0, round(off_pts / off_poss * 100, 1), NA_real_),
        def_ppp = ifelse(def_poss > 0, round(def_pts / def_poss * 100, 1), NA_real_),
        off_ts = ifelse(coalesce(off_ts_poss, 0) > 0, round(off_pts / (2 * off_ts_poss) * 100, 1), NA_real_),
        off_oreb_pct = ifelse(coalesce(off_oreb_opp, 0) > 0, round(off_oreb / off_oreb_opp * 100, 1), NA_real_),
        off_tov_pct = ifelse(off_poss > 0, round(coalesce(off_tov, 0) / off_poss * 100, 1), NA_real_),
        off_ftr = ifelse(coalesce(off_fga, 0) > 0, round(coalesce(off_fta, 0) / off_fga * 100, 1), NA_real_),
        def_ts = ifelse(coalesce(def_ts_poss, 0) > 0, round(def_pts / (2 * def_ts_poss) * 100, 1), NA_real_),
        def_oreb_pct = ifelse(coalesce(def_oreb_opp, 0) > 0, round(coalesce(def_oreb, 0) / def_oreb_opp * 100, 1), NA_real_),
        def_tov_pct = ifelse(def_poss > 0, round(coalesce(def_tov, 0) / def_poss * 100, 1), NA_real_),
        def_ftr = ifelse(coalesce(def_fga, 0) > 0, round(coalesce(def_fta, 0) / def_fga * 100, 1), NA_real_),
        net_rtg = round(coalesce(off_ppp, 0) - coalesce(def_ppp, 0), 1),
        minutes = round(coalesce(off_mins, 0), 1)
      )

      combined <- combined %>%
        inner_join(sched %>% select(game_id, gn, game_date, opp_team_name, result, score_display),
                   by = "game_id") %>%
        arrange(gn)

      disp_ff <- combined %>% select(
        gn, game_date, opp_team_name, result, score_display,
        off_ppp, off_ts, off_oreb_pct, off_tov_pct, off_ftr, off_poss,
        def_ppp, def_ts, def_oreb_pct, def_tov_pct, def_ftr, def_poss,
        minutes
      )

      output$ld_modal_table <- DT::renderDataTable({
        result_idx_ff <- which(names(disp_ff) == "result") - 1L
        result_render_ff <- DT::JS(
          "function(data, type, row, meta) {
             if (type !== 'display' || !row) return data;
             var color = data === 'W' ? '#34d399' : '#f87171';
             return '<span style=\"font-weight:700; color:' + color + ';\">' + data + '</span>';
           }")

        off_ppp_idx_ff <- which(names(disp_ff) == "off_ppp") - 1L
        def_ppp_idx_ff <- which(names(disp_ff) == "def_ppp") - 1L
        minutes_idx_ff <- which(names(disp_ff) == "minutes") - 1L

        col_defs_ff <- list(
          list(targets = "_all", className = "dt-center"),
          list(targets = result_idx_ff, render = result_render_ff)
        )
        if (length(off_ppp_idx_ff)) col_defs_ff[[length(col_defs_ff) + 1]] <- list(targets = off_ppp_idx_ff, className = "section-left-border dt-center")
        if (length(def_ppp_idx_ff)) col_defs_ff[[length(col_defs_ff) + 1]] <- list(targets = def_ppp_idx_ff, className = "section-left-border dt-center")
        if (length(minutes_idx_ff)) col_defs_ff[[length(col_defs_ff) + 1]] <- list(targets = minutes_idx_ff, className = "section-left-border dt-center")

        sketch_ff <- htmltools::withTags(table(class = 'display', thead(
          tr(
            th(class = "group-head", colspan = 5, ""),
            th(class = "group-head section-left-border", colspan = 6, "Offense"),
            th(class = "group-head section-left-border", colspan = 6, "Defense"),
            th(class = "group-head section-left-border", colspan = 1, "")
          ),
          tr(
            th(class = "sub-head", "GN"),
            th(class = "sub-head", "Date"),
            th(class = "sub-head", "Opponent"),
            th(class = "sub-head", "W/L"),
            th(class = "sub-head", "Score"),
            th(class = "sub-head section-left-border", "PPP"),
            th(class = "sub-head", "TS%"),
            th(class = "sub-head", "OREB%"),
            th(class = "sub-head", "TOV%"),
            th(class = "sub-head", "FTR"),
            th(class = "sub-head", "Poss"),
            th(class = "sub-head section-left-border", "PPP"),
            th(class = "sub-head", "TS%"),
            th(class = "sub-head", "OREB%"),
            th(class = "sub-head", "TOV%"),
            th(class = "sub-head", "FTR"),
            th(class = "sub-head", "Poss"),
            th(class = "sub-head section-left-border", "Min")
          )
        )))

        dt_ff <- DT::datatable(disp_ff, container = sketch_ff, rownames = FALSE, escape = FALSE,
                              options = list(
                                dom = "tip", pageLength = 50,
                                deferRender = TRUE, scrollX = TRUE,
                                scrollY = "60vh", scrollCollapse = TRUE,
                                order = list(list(0, "asc")),
                                columnDefs = col_defs_ff
                              ))

        rate_cols_ff <- intersect(c("off_ts", "off_oreb_pct", "off_tov_pct", "off_ftr",
                                    "def_ts", "def_oreb_pct", "def_tov_pct", "def_ftr"), names(disp_ff))
        ppp_cols_ff <- intersect(c("off_ppp", "def_ppp", "net_rtg"), names(disp_ff))
        if (length(rate_cols_ff)) dt_ff <- DT::formatRound(dt_ff, rate_cols_ff, 1)
        if (length(ppp_cols_ff))  dt_ff <- DT::formatRound(dt_ff, ppp_cols_ff, 1)
        dt_ff <- DT::formatRound(dt_ff, "minutes", 1)
        dt_ff <- DT::formatCurrency(dt_ff, c("off_poss", "def_poss"), currency = "", interval = 3, mark = ",", digits = 0)
        dt_ff
      })

    } else {
      # ============================================================
      # SUMMARY MODAL GAME LOG
      # ============================================================
      game_query <- sprintf(
        "SELECT game_id, type_lineup,
                SUM(total_poss) AS poss, SUM(total_pts) AS pts,
                SUM(fg2_made) AS fg2m, SUM(fg2_att) AS fg2a,
                SUM(fg3_made) AS fg3m, SUM(fg3_att) AS fg3a,
                SUM(minutes) AS mins
         FROM basketball_test.mv_lineup_totals_by_day
         WHERE lineup_hash IN (%s) AND team_id = $%d AND game_year = $%d
         GROUP BY game_id, type_lineup",
        hash_placeholders, team_id_idx, gy_idx
      )
      game_data <- DBI::dbGetQuery(pg_pool, game_query, params = hash_query_params)

      if (nrow(game_data) == 0) {
        showModal(modalDialog(title = "No game data", "No games found for this lineup.", easyClose = TRUE))
        return()
      }

      # Pivot off/def
      off <- game_data %>% filter(type_lineup == "offense") %>%
        rename(off_poss = poss, off_pts = pts, off_fg2m = fg2m, off_fg2a = fg2a,
               off_fg3m = fg3m, off_fg3a = fg3a, off_mins = mins) %>%
        select(-type_lineup)
      def <- game_data %>% filter(type_lineup == "defense") %>%
        rename(def_poss = poss, def_pts = pts, def_fg2m = fg2m, def_fg2a = fg2a,
               def_fg3m = fg3m, def_fg3a = fg3a) %>%
        select(game_id, def_poss, def_pts, def_fg2m, def_fg2a, def_fg3m, def_fg3a)

      combined <- off %>% full_join(def, by = "game_id") %>% mutate(
        off_poss = coalesce(off_poss, 0), def_poss = coalesce(def_poss, 0),
        off_pts = coalesce(off_pts, 0), def_pts = coalesce(def_pts, 0),
        off_ppp = ifelse(off_poss > 0, round(off_pts / off_poss * 100, 1), NA_real_),
        def_ppp = ifelse(def_poss > 0, round(def_pts / def_poss * 100, 1), NA_real_),
        net_rtg = round(coalesce(off_ppp, 0) - coalesce(def_ppp, 0), 1),
        minutes = round(coalesce(off_mins, 0), 1)
      )

      combined <- combined %>%
        inner_join(sched %>% select(game_id, gn, game_date, opp_team_name, result, score_display),
                   by = "game_id") %>%
        arrange(gn)

      # Build display table
      shot_raw_cols_m <- c("off_fg2m", "off_fg2a", "off_fg3m", "off_fg3a",
                           "def_fg2m", "def_fg2a", "def_fg3m", "def_fg3a")
      has_shots_m <- all(c("off_fg2a", "off_fg3a") %in% names(combined))
      if (has_shots_m) {
        combined[["Off Shot"]] <- coalesce(combined$off_fg2a, 0) + coalesce(combined$off_fg3a, 0)
        combined[["Def Shot"]] <- coalesce(combined$def_fg2a, 0) + coalesce(combined$def_fg3a, 0)
      }

      disp_m <- combined %>% select(
        gn, game_date, opp_team_name, result, score_display,
        off_ppp, def_ppp, net_rtg,
        any_of(c("Off Shot", "Def Shot")),
        off_poss, def_poss, minutes,
        any_of(shot_raw_cols_m)
      )

      output$ld_modal_table <- DT::renderDataTable({
        hide_idx_m <- which(names(disp_m) %in% shot_raw_cols_m) - 1L

        # Shooting column JS render
        make_shot_render_m <- function(fg2m_col, fg2a_col, fg3m_col, fg3a_col,
                                       is_defense = FALSE, min_fga = 10, avg2 = 53, avg3 = 34) {
          fg2m_i <- which(names(disp_m) == fg2m_col) - 1
          fg2a_i <- which(names(disp_m) == fg2a_col) - 1
          fg3m_i <- which(names(disp_m) == fg3m_col) - 1
          fg3a_i <- which(names(disp_m) == fg3a_col) - 1
          sign_mult <- if (is_defense) -1 else 1
          DT::JS(sprintf(
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
             }", fg2m_i, fg2a_i, fg3m_i, fg3a_i, min_fga, sign_mult, avg2, avg3))
        }

        shot_col_defs_m <- list()
        if (has_shots_m) {
          shot_col_map_m <- list(
            "Off Shot" = c("off_fg2m", "off_fg2a", "off_fg3m", "off_fg3a"),
            "Def Shot" = c("def_fg2m", "def_fg2a", "def_fg3m", "def_fg3a")
          )
          for (dn in names(shot_col_map_m)) {
            cols_m <- shot_col_map_m[[dn]]
            tgt <- which(names(disp_m) == dn) - 1
            is_def <- grepl("^Def", dn)
            fg2a_s <- sum(disp_m[[cols_m[2]]], na.rm = TRUE)
            fg3a_s <- sum(disp_m[[cols_m[4]]], na.rm = TRUE)
            a2 <- if (fg2a_s > 0) as.integer(round(sum(disp_m[[cols_m[1]]], na.rm = TRUE) / fg2a_s * 100)) else 53L
            a3 <- if (fg3a_s > 0) as.integer(round(sum(disp_m[[cols_m[3]]], na.rm = TRUE) / fg3a_s * 100)) else 34L
            if (length(tgt) && all(cols_m %in% names(disp_m))) {
              shot_col_defs_m[[length(shot_col_defs_m) + 1]] <- list(
                targets = tgt,
                render = make_shot_render_m(cols_m[1], cols_m[2], cols_m[3], cols_m[4],
                                            is_defense = is_def, min_fga = 10L, avg2 = a2, avg3 = a3))
            }
          }
        }

        # Result column color
        result_idx_m <- which(names(disp_m) == "result") - 1L
        result_render_m <- DT::JS(
          "function(data, type, row, meta) {
             if (type !== 'display' || !row) return data;
             var color = data === 'W' ? '#34d399' : '#f87171';
             return '<span style=\"font-weight:700; color:' + color + ';\">' + data + '</span>';
           }")

        col_defs_m <- c(
          list(
            list(targets = hide_idx_m, visible = FALSE),
            list(targets = "_all", className = "dt-center"),
            list(targets = result_idx_m, render = result_render_m)
          ),
          shot_col_defs_m
        )

        off_ppp_idx_m <- which(names(disp_m) == "off_ppp") - 1L
        off_poss_idx_m <- which(names(disp_m) == "off_poss") - 1L
        off_shot_idx_m <- if (has_shots_m) which(names(disp_m) == "Off Shot") - 1L else integer(0)
        if (length(off_ppp_idx_m)) col_defs_m[[length(col_defs_m) + 1]] <- list(targets = off_ppp_idx_m, className = "section-left-border dt-center")
        if (length(off_poss_idx_m)) col_defs_m[[length(col_defs_m) + 1]] <- list(targets = off_poss_idx_m, className = "section-left-border dt-center")
        if (length(off_shot_idx_m)) col_defs_m[[length(col_defs_m) + 1]] <- list(targets = off_shot_idx_m, className = "section-left-border dt-center")

        sketch_m <- htmltools::withTags(table(class = 'display', thead(
          tr(
            th(class = "sub-head", "GN"),
            th(class = "sub-head", "Date"),
            th(class = "sub-head", "Opponent"),
            th(class = "sub-head", "W/L"),
            th(class = "sub-head", "Score"),
            th(class = "sub-head section-left-border", "Off PPP"),
            th(class = "sub-head", "Def PPP"),
            th(class = "sub-head", "Net"),
            if (has_shots_m) th(class = "sub-head section-left-border", "Off Shot"),
            if (has_shots_m) th(class = "sub-head", "Def Shot"),
            th(class = "sub-head section-left-border", "Off Poss"),
            th(class = "sub-head", "Def Poss"),
            th(class = "sub-head", "Min")
          )
        )))

        dt_m <- DT::datatable(disp_m, container = sketch_m, rownames = FALSE, escape = FALSE,
                              options = list(
                                dom = "tip", pageLength = 50,
                                deferRender = TRUE, scrollX = TRUE,
                                scrollY = "60vh", scrollCollapse = TRUE,
                                order = list(list(0, "asc")),
                                columnDefs = col_defs_m
                              ))
        dt_m <- DT::formatRound(dt_m, c("off_ppp", "def_ppp", "net_rtg", "minutes"), 1)
        dt_m <- DT::formatCurrency(dt_m, c("off_poss", "def_poss"), currency = "", interval = 3, mark = ",", digits = 0)
        dt_m
      })
    }

    showModal(modalDialog(
      title = lineup_name,
      DTOutput("ld_modal_table"),
      size = "xl",
      easyClose = TRUE
    ))
  })

  # ---- Filter Chips ----
  output$ld_filter_chips <- renderUI({
    team_map <- NULL
    if (!is.null(ld_ref$teams) && nrow(ld_ref$teams)) {
      team_map <- setNames(ld_ref$teams$team_name, as.character(ld_ref$teams$team_id))
    }
    player_map <- NULL
    if (!is.null(ld_ref$players) && nrow(ld_ref$players)) {
      team_id <- suppressWarnings(as.integer(input$ld_team))
      pmap <- ld_ref$players
      if (!is.na(team_id)) pmap <- pmap %>% filter(team_id == !!team_id)
      player_map <- setNames(pmap$name, as.character(pmap$player_id))
    }
    build_filter_chips(
      "ld", input, shared$season_date_bounds,
      reset_btn_id = "ld_reset",
      team_label_map = team_map,
      player_label_map = player_map
    )
  })
  setup_chip_clears("ld", session, input, shared,
    game_type_id = "ld_game_type", opponents_id = "ld_opponents",
    home_away_id = "ld_home_away", outcome_id = "ld_outcome",
    gn_min_id = "ld_gn_min", gn_max_id = "ld_gn_max", last_n_id = "ld_last_n",
    opp_rank_ids = c("ld_opp_rank_side", "ld_opp_rank_n", "ld_opp_rank_metric"),
    date_id = "ld_dates", gy_input_id = "game_year",
    teams_ids = "ld_team",
    starters_ids = c("ld_num_starters_off_mode", "ld_num_starters_off",
                     "ld_num_starters_def_mode", "ld_num_starters_def"),
    clutch_enabled_id = "ld_clutch_enabled")
  # Tab 2 specific: players on/off clear
  observeEvent(input$ld_clear_players_on, {
    updateSelectizeInput(session, "ld_players_on", selected = character(0))
  }, ignoreInit = TRUE)
  observeEvent(input$ld_clear_players_off, {
    updateSelectizeInput(session, "ld_players_off", selected = character(0))
  }, ignoreInit = TRUE)
}
