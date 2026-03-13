# server_tab7_compare.R - Tab 7: Compare server logic

server_tab7_compare <- function(input, output, session, shared) {

  cmp_ref <- reactiveValues(
    result_a = NULL,
    result_b = NULL,
    joined = NULL,
    teams = NULL,
    players = NULL,
    current_mode = "Teams"
  )
  selected_metric <- reactiveVal("net_rtg")

  # ── Helpers ──

  collect_side_params <- function(side) {
    pfx <- paste0("cmp_", side, "_")
    get_input <- function(name) input[[paste0(pfx, name)]]

    gy <- as.integer(input$game_year)
    b <- shared$season_date_bounds(as.character(gy))

    # Cutoff handling
    cutoff_type <- get_input("cutoff_type") %||% ""
    cutoff_val <- input[[paste0(pfx, "cutoff_value")]]
    start_d <- b$start
    end_d <- b$end
    min_gn <- NA_integer_
    max_gn <- NA_integer_
    last_n <- NA_integer_

    if (nzchar(cutoff_type) && !is.null(cutoff_val) && nzchar(as.character(cutoff_val))) {
      if (cutoff_type == "before_gn") max_gn <- as.integer(cutoff_val)
      else if (cutoff_type == "after_gn") min_gn <- as.integer(cutoff_val)
      else if (cutoff_type == "before_date") end_d <- as.Date(cutoff_val)
      else if (cutoff_type == "after_date") start_d <- as.Date(cutoff_val)
    }

    # Starters
    st_mode <- get_input("starters_mode") %||% ""
    st_val <- get_input("starters_val") %||% ""
    num_starters_off <- NA_integer_
    num_starters_off_min <- NA_integer_
    num_starters_off_max <- NA_integer_
    if (nzchar(st_mode) && nzchar(st_val)) {
      v <- as.integer(st_val)
      if (st_mode == "gte") { num_starters_off_min <- v; num_starters_off_max <- 5L }
      else if (st_mode == "lte") { num_starters_off_min <- 0L; num_starters_off_max <- v }
    }

    # Clutch
    clutch_on <- isTRUE(get_input("clutch"))
    max_margin <- if (clutch_on) as.integer(get_input("clutch_margin")) else NA_integer_
    margin_status <- NA_character_
    max_time_remaining <- if (clutch_on) as.integer(get_input("clutch_minutes")) else NA_integer_
    ot_margin_filter <- FALSE

    # Opponents
    opp_sel <- get_input("opponents") %||% character(0)
    td <- cmp_ref$teams
    opp_ids_csv <- NA_character_
    if (length(opp_sel) && !is.null(td)) {
      ids <- td$team_id[td$team_name %in% opp_sel]
      if (length(ids)) opp_ids_csv <- paste(ids, collapse = ",")
    }

    # Game type
    gt <- get_input("game_type") %||% character(0)
    game_type_csv <- if (length(gt) && any(nzchar(gt))) paste(gt[nzchar(gt)], collapse = ",") else NA_character_

    # Home/away, outcome
    home_away <- get_input("home_away") %||% ""
    if (!nzchar(home_away)) home_away <- NA_character_
    outcome <- get_input("outcome") %||% ""
    if (!nzchar(outcome)) outcome <- NA_character_

    # Opp rank
    opp_rank_side <- get_input("opp_rank_side") %||% ""
    if (!nzchar(opp_rank_side)) opp_rank_side <- NA_character_
    opp_rank_n_val <- get_input("opp_rank_n") %||% ""
    opp_rank_n <- if (nzchar(opp_rank_n_val)) as.integer(opp_rank_n_val) else NA_integer_
    opp_rank_metric <- NA_character_

    list(
      game_year = gy, start_d = start_d, end_d = end_d,
      game_type_csv = game_type_csv, opp_ids_csv = opp_ids_csv,
      home_away = home_away, outcome = outcome,
      opp_rank_side = opp_rank_side, opp_rank_n = opp_rank_n,
      opp_rank_metric = opp_rank_metric,
      max_margin = max_margin, margin_status = margin_status,
      max_time_remaining = max_time_remaining, ot_margin_filter = ot_margin_filter,
      min_gn = min_gn, max_gn = max_gn, last_n_games = last_n,
      num_starters_off = num_starters_off, num_starters_def = NA_integer_,
      num_starters_off_min = num_starters_off_min,
      num_starters_off_max = num_starters_off_max,
      num_starters_def_min = NA_integer_, num_starters_def_max = NA_integer_
    )
  }

  # ── SQL runners ──

  run_team_ratings <- function(p) {
    allowed <- guard_heavy_request(
      session, key = "cmp_team_ratings",
      start_d = p$start_d, end_d = p$end_d,
      min_gn = p$min_gn, max_gn = p$max_gn, last_n = p$last_n_games,
      max_calls = 50L, window_sec = 60L
    )
    if (!isTRUE(allowed)) return(data.frame())
    DBI::dbGetQuery(pg_pool, paste0(
      "SELECT * FROM basketball_test.get_team_ratings_dynamic(",
      "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,$7::text,$8::text,$9::int4,$10::text,",
      "$11::int4,$12::text,$13::int4,$14::bool,$15::int4,$16::int4,$17::int4,",
      "$18::int4,$19::int4,$20::int4,$21::int4,$22::int4,$23::int4",
      ")"), params = list(
      p$game_year, as.Date(p$start_d), as.Date(p$end_d),
      p$game_type_csv, p$opp_ids_csv, p$home_away, p$outcome,
      p$opp_rank_side, p$opp_rank_n, p$opp_rank_metric,
      p$max_margin, p$margin_status, p$max_time_remaining, p$ot_margin_filter,
      p$min_gn, p$max_gn, p$last_n_games,
      p$num_starters_off, p$num_starters_def,
      p$num_starters_off_min, p$num_starters_off_max,
      p$num_starters_def_min, p$num_starters_def_max
    ))
  }

  run_team_ff <- function(p) {
    allowed <- guard_heavy_request(
      session, key = "cmp_team_ff",
      start_d = p$start_d, end_d = p$end_d,
      min_gn = p$min_gn, max_gn = p$max_gn, last_n = p$last_n_games,
      max_calls = 50L, window_sec = 60L
    )
    if (!isTRUE(allowed)) return(data.frame())
    DBI::dbGetQuery(pg_pool, paste0(
      "SELECT * FROM basketball_test.get_team_four_factors_dynamic(",
      "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,$7::text,$8::text,$9::int4,$10::text,",
      "$11::int4,$12::text,$13::int4,$14::bool,$15::int4,$16::int4,$17::int4,",
      "$18::int4,$19::int4,$20::int4,$21::int4,$22::int4,$23::int4",
      ")"), params = list(
      p$game_year, as.Date(p$start_d), as.Date(p$end_d),
      p$game_type_csv, p$opp_ids_csv, p$home_away, p$outcome,
      p$opp_rank_side, p$opp_rank_n, p$opp_rank_metric,
      p$max_margin, p$margin_status, p$max_time_remaining, p$ot_margin_filter,
      p$min_gn, p$max_gn, p$last_n_games,
      p$num_starters_off, p$num_starters_def,
      p$num_starters_off_min, p$num_starters_off_max,
      p$num_starters_def_min, p$num_starters_def_max
    ))
  }

  run_lineups_summary <- function(p) {
    allowed <- guard_heavy_request(
      session, key = "cmp_lineups_summary",
      start_d = p$start_d, end_d = p$end_d,
      min_gn = p$min_gn, max_gn = p$max_gn, last_n = p$last_n_games,
      max_calls = 50L, window_sec = 60L
    )
    if (!isTRUE(allowed)) return(data.frame())
    DBI::dbGetQuery(pg_pool, paste0(
      "SELECT * FROM basketball_test.fetch_lineups_csv_v2(",
      "$1::int4,$2::text,$3::text,$4::text,$5::bool,$6::date,$7::date,$8::int4,$9::int4,",
      "$10::text,$11::text,$12::text,$13::text,$14::text,$15::int4,$16::text,$17::int4,$18::text,$19::int4,$20::bool,",
      "$21::int4,$22::int4,$23::int4,$24::int4,$25::int4,$26::int4,$27::int4,$28::int4,$29::int4",
      ")"), params = list(
      5L,            # num (lineup size)
      NA_character_, # team_csv (all teams)
      NA_character_, # player_csv
      NA_character_, # player_off_csv
      FALSE,         # exact
      as.Date(p$start_d), as.Date(p$end_d),
      0L,            # min_poss
      p$game_year,
      p$game_type_csv, p$opp_ids_csv, p$home_away, p$outcome,
      p$opp_rank_side, p$opp_rank_n, p$opp_rank_metric,
      p$max_margin, p$margin_status, p$max_time_remaining, p$ot_margin_filter,
      p$min_gn, p$max_gn, p$last_n_games,
      p$num_starters_off, p$num_starters_def,
      p$num_starters_off_min, p$num_starters_off_max,
      p$num_starters_def_min, p$num_starters_def_max
    ))
  }

  run_lineups_ff <- function(p) {
    allowed <- guard_heavy_request(
      session, key = "cmp_lineups_ff",
      start_d = p$start_d, end_d = p$end_d,
      min_gn = p$min_gn, max_gn = p$max_gn, last_n = p$last_n_games,
      max_calls = 50L, window_sec = 60L
    )
    if (!isTRUE(allowed)) return(data.frame())
    DBI::dbGetQuery(pg_pool, paste0(
      "SELECT * FROM basketball_test.fetch_lineups_four_factors_csv(",
      "$1::int4,$2::text,$3::text,$4::text,$5::bool,$6::date,$7::date,$8::int4,$9::int4,",
      "$10::text,$11::text,$12::text,$13::text,$14::text,$15::int4,$16::text,$17::int4,$18::text,$19::int4,$20::bool,",
      "$21::int4,$22::int4,$23::int4,$24::int4,$25::int4,$26::int4,$27::int4,$28::int4,$29::int4",
      ")"), params = list(
      5L,            # num
      NA_character_, # team_csv
      NA_character_, # player_csv
      NA_character_, # player_off_csv
      FALSE,         # exact
      as.Date(p$start_d), as.Date(p$end_d),
      0L,            # min_poss
      p$game_year,
      p$game_type_csv, p$opp_ids_csv, p$home_away, p$outcome,
      p$opp_rank_side, p$opp_rank_n, p$opp_rank_metric,
      p$max_margin, p$margin_status, p$max_time_remaining, p$ot_margin_filter,
      p$min_gn, p$max_gn, p$last_n_games,
      p$num_starters_off, p$num_starters_def,
      p$num_starters_off_min, p$num_starters_off_max,
      p$num_starters_def_min, p$num_starters_def_max
    ))
  }

  run_player_traditional <- function(p, team_ids_csv) {
    allowed <- guard_heavy_request(
      session, key = "cmp_player_traditional",
      start_d = p$start_d, end_d = p$end_d,
      min_gn = p$min_gn, max_gn = p$max_gn, last_n = p$last_n_games,
      max_calls = 50L, window_sec = 60L
    )
    if (!isTRUE(allowed)) return(data.frame())
    DBI::dbGetQuery(pg_pool, paste0(
      "SELECT * FROM basketball_test.get_player_traditional_dynamic(",
      "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,$7::text,$8::text,$9::text,$10::int4,$11::text,",
      "$12::int4,$13::text,$14::int4,$15::bool,$16::int4,$17::int4,$18::int4",
      ")"), params = list(
      p$game_year,
      if (!is.na(p$start_d)) as.Date(p$start_d) else NA,
      if (!is.na(p$end_d)) as.Date(p$end_d) else NA,
      team_ids_csv,
      p$game_type_csv, p$opp_ids_csv, p$home_away, p$outcome,
      p$opp_rank_side, p$opp_rank_n, p$opp_rank_metric,
      p$max_margin, p$margin_status, p$max_time_remaining, p$ot_margin_filter,
      p$min_gn, p$max_gn, p$last_n_games
    ))
  }

  # ── Metric chip definitions per mode ──

  TEAM_METRICS <- c(
    "Net Rtg" = "net_rtg", "Offense" = "off_ppp", "Defense" = "def_ppp",
    "TS%" = "off_ts_pct", "TOV%" = "off_tov_pct", "OREB%" = "off_oreb_pct", "FTR" = "off_ftr"
  )

  PLAYER_METRICS <- c(
    "PPG" = "ppg", "RPG" = "rpg", "APG" = "apg", "SPG" = "spg",
    "FG%" = "fg_pct", "3P%" = "fg3_pct", "FT%" = "ft_pct", "TS%" = "ts_pct"
  )

  output$cmp_metric_chips_ui <- renderUI({
    metrics <- if (input$cmp_mode == "Players") PLAYER_METRICS else TEAM_METRICS
    cur <- isolate(selected_metric())
    if (!(cur %in% metrics)) {
      selected_metric(metrics[[1]])
      cur <- metrics[[1]]
    }
    chips <- lapply(seq_along(metrics), function(i) {
      nm <- names(metrics)[i]
      val <- metrics[[i]]
      cls <- if (identical(val, cur)) "btn btn-sm btn-warning" else "btn btn-sm btn-outline-secondary"
      actionButton(
        paste0("cmp_metric_", val), nm,
        class = cls, style = "border-radius: 20px; padding: 2px 12px; font-size: .76rem;"
      )
    })
    do.call(tagList, chips)
  })

  # Register metric chip click observers once at init (not inside observe)
  all_metrics <- unique(c(unname(TEAM_METRICS), unname(PLAYER_METRICS)))
  lapply(all_metrics, function(m) {
    observeEvent(input[[paste0("cmp_metric_", m)]], {
      selected_metric(m)
    }, ignoreInit = TRUE)
  })

  # ── Tab init: load ref data ──

  observeEvent(list(input$main_tabs, input$game_year), ignoreInit = TRUE, {
    if (!identical(input$main_tabs, "compare")) return(NULL)
    gy_int <- as.integer(input$game_year)

    teams_df <- cached_ref_query(
      key = sprintf("cmp_teams_%d", gy_int),
      query_fun = function() DBI::dbGetQuery(pg_pool, sprintf(
        "SELECT DISTINCT team_id, team_name FROM basketball_test.full_rosters WHERE game_year = %d ORDER BY team_name", gy_int))
    )
    cmp_ref$teams <- teams_df
    updateSelectizeInput(session, "cmp_a_opponents", choices = teams_df$team_name, selected = character(0), server = TRUE)
    updateSelectizeInput(session, "cmp_b_opponents", choices = teams_df$team_name, selected = character(0), server = TRUE)

    players_df <- cached_ref_query(
      key = sprintf("cmp_players_%d", gy_int),
      query_fun = function() DBI::dbGetQuery(pg_pool, sprintf(
        "SELECT team_id, player_id, MIN(btrim(firstname)||' '||btrim(lastname)) AS name FROM basketball_test.full_rosters WHERE game_year = %d GROUP BY team_id, player_id ORDER BY MIN(btrim(firstname)||' '||btrim(lastname))", gy_int))
    )
    cmp_ref$players <- players_df
    player_choices <- setNames(players_df$player_id, players_df$name)
    updateSelectizeInput(session, "cmp_player_a", choices = player_choices, selected = character(0), server = TRUE)
    updateSelectizeInput(session, "cmp_player_b", choices = player_choices, selected = character(0), server = TRUE)

    # Apply pending preset from home tab
    pending <- shared$pending_compare_preset()
    if (!is.null(pending) && nzchar(pending)) {
      shared$pending_compare_preset(NULL)
      updateSelectInput(session, "cmp_preset", selected = pending)
    }
  })

  # ── Preset handler ──

  observeEvent(input$cmp_preset, {
    preset <- input$cmp_preset
    if (is.null(preset) || !nzchar(preset)) return()

    # Helper to clear all filters on a side
    clear_side <- function(s) {
      updateSelectInput(session, paste0("cmp_", s, "_starters_mode"), selected = "")
      updateSelectInput(session, paste0("cmp_", s, "_starters_val"), selected = "")
      updateSelectInput(session, paste0("cmp_", s, "_home_away"), selected = "")
      updateSelectInput(session, paste0("cmp_", s, "_outcome"), selected = "")
      updateCheckboxInput(session, paste0("cmp_", s, "_clutch"), value = FALSE)
      updateSelectInput(session, paste0("cmp_", s, "_cutoff_type"), selected = "")
      updateSelectizeInput(session, paste0("cmp_", s, "_opponents"), selected = character(0))
      updateSelectizeInput(session, paste0("cmp_", s, "_game_type"), selected = character(0))
      updateSelectInput(session, paste0("cmp_", s, "_opp_rank_side"), selected = "")
      updateSelectInput(session, paste0("cmp_", s, "_opp_rank_n"), selected = "")
    }

    clear_side("a")
    clear_side("b")

    if (preset == "starters_bench") {
      updateSelectInput(session, "cmp_a_starters_mode", selected = "gte")
      updateSelectInput(session, "cmp_a_starters_val", selected = "3")
      updateSelectInput(session, "cmp_b_starters_mode", selected = "lte")
      updateSelectInput(session, "cmp_b_starters_val", selected = "2")
    } else if (preset == "clutch") {
      updateCheckboxInput(session, "cmp_a_clutch", value = TRUE)
      updateSliderInput(session, "cmp_a_clutch_margin", value = 5)
      updateSliderInput(session, "cmp_a_clutch_minutes", value = 2)
    } else if (preset == "home_away") {
      updateSelectInput(session, "cmp_a_home_away", selected = "home")
      updateSelectInput(session, "cmp_b_home_away", selected = "away")
    } else if (preset == "win_loss") {
      updateSelectInput(session, "cmp_a_outcome", selected = "win")
      updateSelectInput(session, "cmp_b_outcome", selected = "loss")
    }
  }, ignoreInit = TRUE)

  # ── Before/After cutoff dynamic UI ──

  output$cmp_a_cutoff_value_ui <- renderUI({
    ct <- input$cmp_a_cutoff_type %||% ""
    if (!nzchar(ct)) return(NULL)
    if (grepl("gn", ct)) {
      selectizeInput("cmp_a_cutoff_value", "GN", choices = c("", as.character(1:40)),
                     selected = "", options = list(placeholder = "#"))
    } else {
      b <- shared$season_date_bounds(input$game_year %||% DEFAULT_GAME_YEAR)
      dateInput("cmp_a_cutoff_value", "Date", value = b$start, min = b$start, max = b$end)
    }
  })

  output$cmp_b_cutoff_value_ui <- renderUI({
    ct <- input$cmp_b_cutoff_type %||% ""
    if (!nzchar(ct)) return(NULL)
    if (grepl("gn", ct)) {
      selectizeInput("cmp_b_cutoff_value", "GN", choices = c("", as.character(1:40)),
                     selected = "", options = list(placeholder = "#"))
    } else {
      b <- shared$season_date_bounds(input$game_year %||% DEFAULT_GAME_YEAR)
      dateInput("cmp_b_cutoff_value", "Date", value = b$start, min = b$start, max = b$end)
    }
  })

  # ── Compare button ──

  observeEvent(input$cmp_go, {
    req(input$cmp_mode)
    mode <- input$cmp_mode
    cmp_ref$current_mode <- mode

    pa <- collect_side_params("a")
    pb <- collect_side_params("b")
    metric <- selected_metric()

    if (mode == "Teams") {
      is_ff <- metric %in% c("off_ts_pct", "off_tov_pct", "off_oreb_pct", "off_ftr")
      if (is_ff) {
        res_a <- run_team_ff(pa)
        res_b <- run_team_ff(pb)
      } else {
        res_a <- run_team_ratings(pa)
        res_b <- run_team_ratings(pb)
      }
      if (!nrow(res_a) || !nrow(res_b)) { cmp_ref$joined <- data.frame(); return() }

      pick_cols <- function(df, suffix) {
        base <- c("team_id", "team_name")
        poss_col <- if ("off_poss" %in% names(df)) "off_poss" else if ("total_poss" %in% names(df)) "total_poss" else NULL
        metric_col <- if (metric %in% names(df)) metric else NULL
        cols <- c(base, metric_col, poss_col)
        cols <- cols[cols %in% names(df)]
        out <- df[, cols, drop = FALSE]
        if (!is.null(metric_col)) names(out)[names(out) == metric_col] <- paste0("metric_", suffix)
        if (!is.null(poss_col)) names(out)[names(out) == poss_col] <- paste0("poss_", suffix)
        out
      }

      df_a <- pick_cols(res_a, "a")
      df_b <- pick_cols(res_b, "b")
      joined <- merge(df_a, df_b, by = c("team_id", "team_name"), suffixes = c("", ".b"))
      joined$metric_a <- as.numeric(joined$metric_a)
      joined$metric_b <- as.numeric(joined$metric_b)
      joined$gap <- joined$metric_a - joined$metric_b
      joined <- joined[order(-abs(joined$gap)), ]
      joined$rank <- seq_len(nrow(joined))
      joined$entity_name <- joined$team_name
      cmp_ref$joined <- joined

    } else if (mode == "Lineups") {
      is_ff <- metric %in% c("off_ts_pct", "off_tov_pct", "off_oreb_pct", "off_ftr")
      if (is_ff) {
        res_a <- run_lineups_ff(pa)
        res_b <- run_lineups_ff(pb)
      } else {
        res_a <- run_lineups_summary(pa)
        res_b <- run_lineups_summary(pb)
      }
      if (!nrow(res_a) || !nrow(res_b)) { cmp_ref$joined <- data.frame(); return() }

      pick_cols_lu <- function(df, suffix) {
        key <- "sub_lineup_hash"
        name_col <- if ("player_names_str" %in% names(df)) "player_names_str" else NULL
        team_col <- if ("team_name" %in% names(df)) "team_name" else NULL
        poss_col <- if ("off_poss" %in% names(df)) "off_poss" else if ("total_poss" %in% names(df)) "total_poss" else NULL
        metric_col <- if (metric %in% names(df)) metric else NULL
        cols <- c(key, name_col, team_col, metric_col, poss_col)
        cols <- cols[!is.null(cols) & cols %in% names(df)]
        out <- df[, cols, drop = FALSE]
        if (!is.null(metric_col)) names(out)[names(out) == metric_col] <- paste0("metric_", suffix)
        if (!is.null(poss_col)) names(out)[names(out) == poss_col] <- paste0("poss_", suffix)
        out
      }

      df_a <- pick_cols_lu(res_a, "a")
      df_b <- pick_cols_lu(res_b, "b")
      join_by <- "sub_lineup_hash"
      extra <- intersect(c("player_names_str", "team_name"), intersect(names(df_a), names(df_b)))
      joined <- merge(df_a, df_b, by = c(join_by, extra), suffixes = c("", ".b"))
      joined$metric_a <- as.numeric(joined$metric_a)
      joined$metric_b <- as.numeric(joined$metric_b)
      joined$gap <- joined$metric_a - joined$metric_b
      joined <- joined[order(-abs(joined$gap)), ]
      joined$rank <- seq_len(nrow(joined))
      joined$entity_name <- if ("player_names_str" %in% names(joined)) joined$player_names_str else joined$sub_lineup_hash
      cmp_ref$joined <- joined

    } else if (mode == "Players") {
      player_a_id <- input$cmp_player_a
      player_b_id <- input$cmp_player_b
      if (is.null(player_a_id) || !nzchar(player_a_id)) {
        showNotification("Select Player A", type = "warning")
        return()
      }
      if (is.null(player_b_id) || !nzchar(player_b_id)) {
        showNotification("Select Player B", type = "warning")
        return()
      }

      players_df <- cmp_ref$players
      # Look up team for each player
      team_a <- paste(unique(players_df$team_id[players_df$player_id == as.integer(player_a_id)]), collapse = ",")
      team_b <- paste(unique(players_df$team_id[players_df$player_id == as.integer(player_b_id)]), collapse = ",")

      res_a <- run_player_traditional(pa, team_a)
      res_b <- run_player_traditional(pb, team_b)
      if (!nrow(res_a) || !nrow(res_b)) { cmp_ref$joined <- data.frame(); return() }

      # Filter to selected players
      res_a <- res_a[res_a$player_id == as.integer(player_a_id), , drop = FALSE]
      res_b <- res_b[res_b$player_id == as.integer(player_b_id), , drop = FALSE]
      if (!nrow(res_a) || !nrow(res_b)) { cmp_ref$joined <- data.frame(); return() }

      # Map metric names to columns
      rate_mode <- input$cmp_rate_mode %||% "Per Game"
      get_player_metric <- function(row, m, rate) {
        # Standard per-game stats from the SQL function
        col_map <- c(
          "ppg" = "pts_per_game", "rpg" = "reb_per_game", "apg" = "ast_per_game", "spg" = "stl_per_game",
          "fg_pct" = "fg_pct", "fg3_pct" = "fg3_pct", "ft_pct" = "ft_pct", "ts_pct" = "ts_pct"
        )
        # Totals columns
        total_map <- c(
          "ppg" = "total_pts", "rpg" = "total_reb", "apg" = "total_ast", "spg" = "total_stl"
        )
        if (m %in% c("fg_pct", "fg3_pct", "ft_pct", "ts_pct")) {
          # Percentages don't change with rate mode
          cname <- col_map[m]
          if (cname %in% names(row)) return(as.numeric(row[[cname]]))
          return(NA_real_)
        }
        if (rate == "Totals") {
          cname <- total_map[m]
          if (!is.null(cname) && cname %in% names(row)) return(as.numeric(row[[cname]]))
          return(NA_real_)
        }
        if (rate == "Per 75 Possessions") {
          # per 75 = total / total_poss * 75
          cname_total <- total_map[m]
          if (!is.null(cname_total) && cname_total %in% names(row) && "total_poss" %in% names(row)) {
            poss <- as.numeric(row[["total_poss"]])
            if (!is.na(poss) && poss > 0) return(as.numeric(row[[cname_total]]) / poss * 75)
          }
          return(NA_real_)
        }
        # Per Game (default)
        cname <- col_map[m]
        if (!is.null(cname) && cname %in% names(row)) return(as.numeric(row[[cname]]))
        NA_real_
      }

      same_player <- identical(as.character(player_a_id), as.character(player_b_id))
      player_a_name <- players_df$name[players_df$player_id == as.integer(player_a_id)][1]
      player_b_name <- players_df$name[players_df$player_id == as.integer(player_b_id)][1]

      val_a <- get_player_metric(res_a[1, ], metric, rate_mode)
      val_b <- get_player_metric(res_b[1, ], metric, rate_mode)
      poss_a <- if ("total_poss" %in% names(res_a)) as.numeric(res_a$total_poss[1]) else NA_real_
      poss_b <- if ("total_poss" %in% names(res_b)) as.numeric(res_b$total_poss[1]) else NA_real_

      if (same_player) {
        joined <- data.frame(
          rank = 1L,
          entity_name = player_a_name,
          metric_a = val_a, poss_a = poss_a,
          metric_b = val_b, poss_b = poss_b,
          gap = val_a - val_b,
          stringsAsFactors = FALSE
        )
      } else {
        joined <- data.frame(
          rank = 1:2,
          entity_name = c(player_a_name, player_b_name),
          metric_a = c(val_a, val_b),
          poss_a = c(poss_a, poss_b),
          metric_b = c(val_b, val_a),
          poss_b = c(poss_b, poss_a),
          gap = c(val_a - val_b, val_b - val_a),
          stringsAsFactors = FALSE
        )
      }
      cmp_ref$joined <- joined
    }
  })

  # ── Summary cards ──

  metric_label <- reactive({
    m <- selected_metric()
    nms <- c(TEAM_METRICS, PLAYER_METRICS)
    names(nms)[match(m, nms)] %||% m
  })

  output$cmp_summary_a <- renderText({
    df <- cmp_ref$joined
    if (is.null(df) || !nrow(df)) return("\u2014")
    sprintf("%+.1f", mean(df$metric_a, na.rm = TRUE))
  })
  output$cmp_summary_a_label <- renderText({ metric_label() })
  output$cmp_summary_b <- renderText({
    df <- cmp_ref$joined
    if (is.null(df) || !nrow(df)) return("\u2014")
    sprintf("%+.1f", mean(df$metric_b, na.rm = TRUE))
  })
  output$cmp_summary_b_label <- renderText({ metric_label() })
  output$cmp_summary_gap <- renderText({
    df <- cmp_ref$joined
    if (is.null(df) || !nrow(df)) return("\u2014")
    sprintf("%.1f", mean(abs(df$gap), na.rm = TRUE))
  })

  # ── Results table ──

  output$cmp_table <- DT::renderDataTable({
    df <- cmp_ref$joined
    req(df, nrow(df) > 0)

    mode <- cmp_ref$current_mode
    entity_label <- if (mode == "Players") "Player" else if (mode == "Lineups") "Lineup" else "Team"

    show_df <- data.frame(
      `#` = df$rank,
      Entity = df$entity_name,
      `Side A` = sprintf("%+.1f", df$metric_a),
      `Poss A` = if ("poss_a" %in% names(df)) as.integer(df$poss_a) else NA_integer_,
      `Side B` = sprintf("%+.1f", df$metric_b),
      `Poss B` = if ("poss_b" %in% names(df)) as.integer(df$poss_b) else NA_integer_,
      Gap = sprintf("%+.1f", df$gap),
      check.names = FALSE, stringsAsFactors = FALSE
    )
    names(show_df)[2] <- entity_label

    DT::datatable(
      show_df,
      options = list(
        dom = "t", paging = FALSE, ordering = TRUE,
        order = list(list(6, "desc")),
        columnDefs = list(
          list(className = "dt-right", targets = 2:6),
          list(className = "dt-left", targets = 1)
        )
      ),
      rownames = FALSE, selection = "none",
      class = "compact stripe nowrap"
    )
  }, server = FALSE)

  # ── Reset ──

  observeEvent(input$cmp_reset, {
    updateSelectInput(session, "cmp_preset", selected = "")
    for (s in c("a", "b")) {
      updateSelectInput(session, paste0("cmp_", s, "_starters_mode"), selected = "")
      updateSelectInput(session, paste0("cmp_", s, "_starters_val"), selected = "")
      updateSelectInput(session, paste0("cmp_", s, "_home_away"), selected = "")
      updateSelectInput(session, paste0("cmp_", s, "_outcome"), selected = "")
      updateCheckboxInput(session, paste0("cmp_", s, "_clutch"), value = FALSE)
      updateSelectInput(session, paste0("cmp_", s, "_cutoff_type"), selected = "")
      updateSelectizeInput(session, paste0("cmp_", s, "_opponents"), selected = character(0))
      updateSelectizeInput(session, paste0("cmp_", s, "_game_type"), selected = character(0))
      updateSelectInput(session, paste0("cmp_", s, "_opp_rank_side"), selected = "")
      updateSelectInput(session, paste0("cmp_", s, "_opp_rank_n"), selected = "")
    }
    cmp_ref$joined <- NULL
  })

  # ── Filter chips ──

  output$cmp_filter_chips <- renderUI({
    tryCatch(
      build_filter_chips("cmp", input, shared$season_date_bounds, reset_btn_id = "cmp_reset"),
      error = function(e) NULL
    )
  })
}
