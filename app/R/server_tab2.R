# server_tab2.R - Tab 2: Lineup Data server logic

server_tab2 <- function(input, output, session, shared) {

  ld_ref <- reactiveValues(teams = NULL, players = NULL)

  observeEvent(list(input$main_tabs, input$game_year_ld), ignoreInit = TRUE, {
    if (!identical(input$main_tabs, "lineup_data")) return(NULL)
    gy_int <- as.integer(input$game_year_ld)

    teams_ld <- DBI::dbGetQuery(pg_pool, "SELECT DISTINCT team_id, MIN(team_name) AS team_name FROM basketball_test.full_rosters WHERE game_year = $1 GROUP BY team_id ORDER BY MIN(team_name)", params = list(gy_int))
    ld_ref$teams <- teams_ld
    team_values <- c("", as.character(teams_ld$team_id))
    names(team_values) <- c("— All teams —", teams_ld$team_name)
    updateSelectizeInput(session, "ld_team", choices = team_values, selected = "", server = TRUE)

    players_map <- DBI::dbGetQuery(pg_pool, "SELECT team_id, player_id, MIN(btrim(firstname)||' '||btrim(lastname)) AS name FROM basketball_test.full_rosters WHERE game_year = $1 GROUP BY team_id, player_id ORDER BY MIN(btrim(firstname)||' '||btrim(lastname))", params = list(gy_int))
    ld_ref$players <- players_map

    updateSelectizeInput(session, "ld_players_on", choices = setNames(integer(0), character(0)), selected = character(0), server = TRUE)
    updateSelectizeInput(session, "ld_players_off", choices = setNames(integer(0), character(0)), selected = character(0), server = TRUE)
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
    updateSelectizeInput(session, "ld_players_on", choices = choices, selected = character(0), server = TRUE)
    updateSelectizeInput(session, "ld_players_off", choices = choices, selected = character(0), server = TRUE)
  }, ignoreInit = TRUE)

  observeEvent(input$ld_players_on, {
    on_sel <- input$ld_players_on %||% character(0)
    off_sel <- input$ld_players_off %||% character(0)
    inter <- intersect(on_sel, off_sel)
    if (length(inter)) updateSelectizeInput(session, "ld_players_off", selected = setdiff(off_sel, inter), server = TRUE)
  }, ignoreInit = TRUE)

  observeEvent(input$ld_players_off, {
    on_sel <- input$ld_players_on %||% character(0)
    off_sel <- input$ld_players_off %||% character(0)
    inter <- intersect(on_sel, off_sel)
    if (length(inter)) updateSelectizeInput(session, "ld_players_on", selected = setdiff(on_sel, inter), server = TRUE)
  }, ignoreInit = TRUE)

  observeEvent(input$ld_reset, {
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
  })

  run_fetch_lineups_20 <- function(pool, num, team_csv, player_csv, player_off_csv, exact, start_date, end_date, min_poss, game_year, game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric, max_margin, margin_status, max_time_remaining, ot_margin_filter) {
    DBI::dbGetQuery(pool, paste0("SELECT * FROM basketball_test.fetch_lineups_csv_v2(", "$1::int4,$2::text,$3::text,$4::text,$5::bool,$6::date,$7::date,$8::int4,$9::int4,", "$10::text,$11::text,$12::text,$13::text,$14::text,$15::int4,$16::text,$17::int4,$18::text,$19::int4,$20::bool", ")"), params = list(as.integer(num), team_csv, player_csv, player_off_csv, as.logical(exact), as.Date(start_date), as.Date(end_date), as.integer(min_poss), as.integer(game_year), game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric, max_margin, margin_status, max_time_remaining, ot_margin_filter))
  }

  run_fetch_lineups_ff_20 <- function(pool, num, team_csv, player_csv, player_off_csv, exact, start_date, end_date, min_poss, game_year, game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric, max_margin, margin_status, max_time_remaining, ot_margin_filter) {
    DBI::dbGetQuery(pool, paste0("SELECT * FROM basketball_test.fetch_lineups_four_factors_csv(", "$1::int4,$2::text,$3::text,$4::text,$5::bool,$6::date,$7::date,$8::int4,$9::int4,", "$10::text,$11::text,$12::text,$13::text,$14::text,$15::int4,$16::text,$17::int4,$18::text,$19::int4,$20::bool", ")"), params = list(as.integer(num), team_csv, player_csv, player_off_csv, as.logical(exact), as.Date(start_date), as.Date(end_date), as.integer(min_poss), as.integer(game_year), game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric, max_margin, margin_status, max_time_remaining, ot_margin_filter))
  }

  # --- Full ranked FF data (ranks computed BEFORE any local filtering) ---
  # Only re-fetches when game filters or group size change.
  # Team, players on/off, min poss are applied locally afterward.
  ld_ff_ranked_df <- reactive({
    req(identical(input$main_tabs, "lineup_data"))
    gy <- as.integer(input$game_year_ld)
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

    # Extract clutch params
    clutch_enabled <- isTRUE(input$ld_clutch_enabled)
    max_margin <- if (clutch_enabled) as.integer(input$ld_clutch_margin) else NA_integer_
    margin_status <- if (clutch_enabled) input$ld_clutch_status else NA_character_
    max_time_remaining <- if (clutch_enabled) as.integer(input$ld_clutch_minutes) * 60L else NA_integer_
    ot_margin_filter <- if (clutch_enabled) isTRUE(input$ld_clutch_ot_margin) else FALSE

    # Fetch ALL lineups for group size + game filters (no team/player/min_poss)
    df <- run_fetch_lineups_ff_20(pg_pool,
                                  num = num, team_csv = NA_character_, player_csv = NA_character_,
                                  player_off_csv = NA_character_, exact = TRUE,
                                  start_date = start_date, end_date = end_date,
                                  min_poss = 0L, game_year = gy,
                                  game_type_csv = game_type_csv, opp_ids_csv = opp_ids_csv,
                                  home_away = home_away, outcome = outcome,
                                  opp_rank_side = rank_side, opp_rank_n = rank_n, opp_rank_metric = metric,
                                  max_margin = max_margin, margin_status = margin_status, max_time_remaining = max_time_remaining,
                                  ot_margin_filter = ot_margin_filter)

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
  }) %>% bindEvent(input$ld_num, input$ld_dates, input$game_year_ld,
                   input$ld_game_type, input$ld_opponents, input$ld_home_away,
                   input$ld_outcome, input$ld_opp_rank_side, input$ld_opp_rank_n,
                   input$ld_opp_rank_metric, input$main_tabs, input$ld_view_mode,
                   input$ld_clutch_enabled, input$ld_clutch_margin, input$ld_clutch_status, input$ld_clutch_minutes, input$ld_clutch_ot_margin)

  # --- Full ranked Summary data (ranks computed BEFORE any local filtering) ---
  # Same pattern as ld_ff_ranked_df but for the Summary view.
  ld_summary_ranked_df <- reactive({
    req(identical(input$main_tabs, "lineup_data"))
    gy <- as.integer(input$game_year_ld)
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

    start_date <- if (!is.null(input$ld_dates[1]) && !is.na(input$ld_dates[1])) as.Date(input$ld_dates[1]) else NA
    end_date <- if (!is.null(input$ld_dates[2]) && !is.na(input$ld_dates[2])) as.Date(input$ld_dates[2]) else NA

    # Extract clutch params
    clutch_enabled <- isTRUE(input$ld_clutch_enabled)
    max_margin <- if (clutch_enabled) as.integer(input$ld_clutch_margin) else NA_integer_
    margin_status <- if (clutch_enabled) input$ld_clutch_status else NA_character_
    max_time_remaining <- if (clutch_enabled) as.integer(input$ld_clutch_minutes) * 60L else NA_integer_
    ot_margin_filter <- if (clutch_enabled) isTRUE(input$ld_clutch_ot_margin) else FALSE

    df <- run_fetch_lineups_20(pg_pool,
                               num = num, team_csv = NA_character_, player_csv = NA_character_,
                               player_off_csv = NA_character_, exact = TRUE,
                               start_date = start_date, end_date = end_date,
                               min_poss = 0L, game_year = gy,
                               game_type_csv = game_type_csv, opp_ids_csv = opp_ids_csv,
                               home_away = home_away, outcome = outcome,
                               opp_rank_side = rank_side, opp_rank_n = rank_n, opp_rank_metric = metric,
                               max_margin = max_margin, margin_status = margin_status, max_time_remaining = max_time_remaining,
                               ot_margin_filter = ot_margin_filter)

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
  }) %>% bindEvent(input$ld_num, input$ld_dates, input$game_year_ld,
                   input$ld_game_type, input$ld_opponents, input$ld_home_away,
                   input$ld_outcome, input$ld_opp_rank_side, input$ld_opp_rank_n,
                   input$ld_opp_rank_metric, input$main_tabs, input$ld_view_mode,
                   input$ld_clutch_enabled, input$ld_clutch_margin, input$ld_clutch_status, input$ld_clutch_minutes, input$ld_clutch_ot_margin)

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

    list(num = as.integer(input$ld_num), team_csv = if (!is.na(team_id)) as.character(team_id) else NA_character_, player_csv = if (length(player_on_ids)) paste(player_on_ids, collapse = ",") else NA_character_, player_off_csv = if (length(player_off_ids)) paste(player_off_ids, collapse = ",") else NA_character_, exact = TRUE, start_date = if (!is.null(input$ld_dates[1]) && !is.na(input$ld_dates[1])) as.Date(input$ld_dates[1]) else NA, end_date = if (!is.null(input$ld_dates[2]) && !is.na(input$ld_dates[2])) as.Date(input$ld_dates[2]) else NA, min_poss = as.integer(input$ld_minposs), game_type_csv = ld_game_type_csv, opp_ids_csv = ld_opp_ids_csv, home_away = ld_home_away, outcome = ld_outcome, opp_rank_side = ld_rank_side, opp_rank_n = ld_rank_n, opp_rank_metric = ld_metric)
  }) %>% bindEvent(input$ld_num, input$ld_team, input$ld_players_on, input$ld_players_off, input$ld_dates, input$ld_minposs, input$main_tabs, input$ld_game_type, input$ld_opponents, input$ld_home_away, input$ld_outcome, input$ld_opp_rank_side, input$ld_opp_rank_n, input$ld_opp_rank_metric, input$ld_view_mode)

  ld_data <- reactive({
    req(ld_params())
    p <- ld_params()
    gy <- as.integer(input$game_year_ld)
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
        on_ids <- as.integer(strsplit(p$player_csv, ",")[[1]])
        pid_list <- if (is.list(df$player_ids)) df$player_ids else lapply(df$player_ids, function(s) as.integer(strsplit(gsub("[{}]", "", as.character(s)), ",")[[1]]))
        keep <- vapply(pid_list, function(x) all(on_ids %in% x), logical(1))
        df <- df[keep, , drop = FALSE]
      }

      # Filter by players off (lineup must NOT contain any excluded players)
      if (!is.na(p$player_off_csv) && nzchar(p$player_off_csv)) {
        off_ids <- as.integer(strsplit(p$player_off_csv, ",")[[1]])
        pid_list <- if (is.list(df$player_ids)) df$player_ids else lapply(df$player_ids, function(s) as.integer(strsplit(gsub("[{}]", "", as.character(s)), ",")[[1]]))
        keep <- vapply(pid_list, function(x) !any(off_ids %in% x), logical(1))
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
          net_rtg = numeric(0), num_lineup = integer(0),
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
        on_ids <- as.integer(strsplit(p$player_csv, ",")[[1]])
        pid_list <- if (is.list(df$player_ids)) df$player_ids else lapply(df$player_ids, function(s) as.integer(strsplit(gsub("[{}]", "", as.character(s)), ",")[[1]]))
        keep <- vapply(pid_list, function(x) all(on_ids %in% x), logical(1))
        df <- df[keep, , drop = FALSE]
      }

      # Filter by players off (lineup must NOT contain any excluded players)
      if (!is.na(p$player_off_csv) && nzchar(p$player_off_csv)) {
        off_ids <- as.integer(strsplit(p$player_off_csv, ",")[[1]])
        pid_list <- if (is.list(df$player_ids)) df$player_ids else lapply(df$player_ids, function(s) as.integer(strsplit(gsub("[{}]", "", as.character(s)), ",")[[1]]))
        keep <- vapply(pid_list, function(x) !any(off_ids %in% x), logical(1))
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
                     "total_poss", "net_rtg")
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

        total_row <- data.frame(
          Team = "TOTAL", Players = "— All Lineups —",
          off_ppp = tot_off_ppp, off_ts = tot_off_ts, off_oreb = tot_off_oreb, off_tov = tot_off_tov, off_ftr = tot_off_ftr,
          off_poss = sum_off_poss,
          def_ppp = tot_def_ppp, def_ts = tot_def_ts, def_oreb = tot_def_oreb, def_tov = tot_def_tov, def_ftr = tot_def_ftr,
          def_poss = sum_def_poss,
          total_poss = sum_off_poss + sum_def_poss,
          net_rtg = tot_net_rtg,
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
          th(class = "group-head section-left-border", colspan = 2, "Usage")
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
          th(class = "sub-head section-left-border", "Total"), th(class = "sub-head", "Net")
        )
      )))

      # Column indices for section borders
      hide_idx <- c(0, which(colnames(df) %in% pr_cols) - 1L)
      off_ppp_idx  <- which(names(df) == "off_ppp") - 1L
      def_ppp_idx  <- which(names(df) == "def_ppp") - 1L
      total_idx    <- which(names(df) == "total_poss") - 1L

      col_defs <- list(
        list(targets = hide_idx, visible = FALSE),
        list(targets = "_all", className = "dt-center")
      )
      if (length(off_ppp_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = off_ppp_idx, className = "section-left-border dt-center")
      if (length(def_ppp_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = def_ppp_idx, className = "section-left-border dt-center")
      if (length(total_idx))   col_defs[[length(col_defs) + 1]] <- list(targets = total_idx, className = "section-left-border dt-center")

      dt <- DT::datatable(df, container = sketch_ff, rownames = FALSE,
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

      if (length(rate_cols)) dt <- DT::formatRound(dt, rate_cols, 1)
      if (length(ppp_cols))  dt <- DT::formatRound(dt, ppp_cols, 1)
      if (length(poss_cols)) dt <- DT::formatCurrency(dt, poss_cols, currency = "", interval = 3, mark = ",", digits = 0)

      # TOTAL row styling
      dt <- DT::formatStyle(dt, "Team", target = "row",
                            backgroundColor = styleEqual("TOTAL", "#f0f0f0"),
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
      keep_cols <- c("Team", "Players", "total_poss", "plus_minus", "off_poss", "def_poss", "off_pts", "def_pts", "off_ppp", "def_ppp", "net_rtg", "num_lineup", "sub_lineup_hash")
      df <- df %>% select(any_of(c(keep_cols, pr_cols)))
      df$is_total <- rep(1, nrow(df))
      if ("net_rtg" %in% names(df)) df <- df %>% arrange(desc(total_poss))
      if (nrow(df) > 0) {
        sum_off_poss <- sum(df$off_poss, na.rm = TRUE)
        sum_def_poss <- sum(df$def_poss, na.rm = TRUE)
        sum_off_pts <- sum(df$off_pts, na.rm = TRUE)
        sum_def_pts <- sum(df$def_pts, na.rm = TRUE)
        tot_off_ppp <- if (sum_off_poss > 0) (sum_off_pts / sum_off_poss) * 100 else 0
        tot_def_ppp <- if (sum_def_poss > 0) (sum_def_pts / sum_def_poss) * 100 else 0
        tot_net_rtg <- tot_off_ppp - tot_def_ppp
        total_row <- data.frame(Team = "TOTAL", Players = "— All Lineups —", total_poss = sum_off_poss + sum_def_poss, off_ppp = tot_off_ppp, def_ppp = tot_def_ppp, net_rtg = tot_net_rtg, plus_minus = sum_off_pts - sum_def_pts, off_poss = sum_off_poss, off_pts = sum_off_pts, def_poss = sum_def_poss, def_pts = sum_def_pts, num_lineup = NA_integer_, sub_lineup_hash = "TOTAL", is_total = 0, stringsAsFactors = FALSE)
        df <- dplyr::bind_rows(total_row, df)
      }
      df <- df %>% select(is_total, everything())
      show_cols <- c("Team", "Players", "total_poss", "off_ppp", "def_ppp", "net_rtg", "plus_minus", "off_poss", "off_pts", "def_poss", "def_pts", "num_lineup", "sub_lineup_hash")

      keep <- intersect(show_cols, names(df))
      df <- df[, unique(c("is_total", keep, pr_cols[pr_cols %in% names(df)])), drop = FALSE]
      pretty_labels <- c(Team = "Team", Players = "Players", num_lineup = "Size", total_poss = "Total Poss", net_rtg = "Net RTG", plus_minus = "+/-", off_ppp = "Off PPP", def_ppp = "Def PPP", off_poss = "Off Poss", off_pts = "Off Pts", def_poss = "Def Poss", def_pts = "Def Pts", sub_lineup_hash = "Lineup ID")
      data_col_names <- colnames(df)[-1]
      data_col_names <- setdiff(data_col_names, pr_cols)
      col_labels <- unname(pretty_labels[data_col_names])
      final_labels <- c("", col_labels)
      pr_indices <- which(colnames(df) %in% pr_cols) - 1L
      hidden_indices <- c(0, pr_indices)

      dt <- DT::datatable(df, colnames = final_labels, rownames = FALSE, filter = "top", options = list(pageLength = 50, lengthMenu = c(25, 50, 100, 200, 1000), orderFixed = list(list(0, 'asc')), deferRender = TRUE, scrollX = TRUE, scrollY = "70vh", scrollCollapse = TRUE, processing = TRUE, columnDefs = list(list(targets = hidden_indices, visible = FALSE)))) |>
        DT::formatRound(c("off_ppp", "def_ppp", "net_rtg")[c("off_ppp", "def_ppp", "net_rtg") %in% names(df)], 1) |>
        DT::formatCurrency(c("total_poss", "off_poss", "def_poss")[c("total_poss", "off_poss", "def_poss") %in% names(df)], currency = "", interval = 3, mark = ",", digits = 0) |>
        DT::formatCurrency(c("off_pts", "def_pts", "plus_minus")[c("off_pts", "def_pts", "plus_minus") %in% names(df)], currency = "", interval = 3, mark = ",", digits = 0)
      dt <- DT::formatStyle(dt, "Team", target = "row", backgroundColor = styleEqual("TOTAL", "#f0f0f0"), fontWeight = styleEqual("TOTAL", "bold"))
      if (all(c("net_rtg", "pr_ld_net") %in% colnames(df))) dt <- DT::formatStyle(dt, "net_rtg", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_ld_net")
      if (all(c("off_ppp", "pr_ld_off_ppp") %in% colnames(df))) dt <- DT::formatStyle(dt, "off_ppp", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_ld_off_ppp")
      if (all(c("def_ppp", "pr_ld_def_ppp_i") %in% colnames(df))) dt <- DT::formatStyle(dt, "def_ppp", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_ld_def_ppp_i")
      return(dt)
    }
  })
}
