# server_tab7_compare.R - Tab 7: Compare server logic

server_tab7_compare <- function(input, output, session, shared) {

  cmp_ref <- reactiveValues(
    teams = NULL,
    players = NULL
  )
  selected_metric <- reactiveVal("net_rtg")
  cmp_auto_default_ids <- reactiveVal(integer(0))
  cmp_defaults_active <- reactiveVal(FALSE)
  selected_detail_entity <- reactiveVal(NULL)
  detail_view_active <- reactiveVal(FALSE)

  # -- Helpers --

  collect_side_params <- function(side) {
    pfx <- paste0("cmp_", side, "_")
    get_input <- function(name) input[[paste0(pfx, name)]]

    gy <- as.integer(input$game_year)
    b <- shared$season_date_bounds(as.character(gy))

    start_d <- b$start
    end_d <- b$end
    min_gn <- NA_integer_
    max_gn <- NA_integer_
    last_n <- NA_integer_

    # Players mode uses shared time filters so both sides are compared on the same window.
    if (identical(input$cmp_mode, "Players")) {
      dr <- input$cmp_players_dates
      if (!is.null(dr) && length(dr) == 2) {
        d1 <- suppressWarnings(as.Date(dr[[1]]))
        d2 <- suppressWarnings(as.Date(dr[[2]]))
        if (!is.na(d1)) start_d <- d1
        if (!is.na(d2)) end_d <- d2
      }
      gn_min <- suppressWarnings(as.integer(input$cmp_players_gn_min %||% ""))
      gn_max <- suppressWarnings(as.integer(input$cmp_players_gn_max %||% ""))
      if (is.finite(gn_min)) min_gn <- gn_min
      if (is.finite(gn_max)) max_gn <- gn_max
    }
    if (!identical(input$cmp_mode, "Players")) {
      preset <- input$cmp_preset %||% ""
      if (identical(preset, "date_split")) {
        split_date <- suppressWarnings(as.Date(input$cmp_split_date))
        if (!is.na(split_date)) {
          if (identical(side, "a")) {
            end_d <- min(as.Date(end_d), split_date)
          } else {
            start_d <- max(as.Date(start_d), split_date + 1L)
          }
        }
      } else if (identical(preset, "gn_split")) {
        split_gn <- suppressWarnings(as.integer(input$cmp_split_gn %||% ""))
        if (is.finite(split_gn)) {
          if (identical(side, "a")) {
            max_gn <- split_gn
          } else {
            min_gn <- split_gn + 1L
          }
        }
      }
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
    max_time_remaining <- if (clutch_on) as.integer(get_input("clutch_minutes")) * 60L else NA_integer_
    ot_margin_filter <- FALSE

    # Teams / opponents
    team_sel <- get_input("teams") %||% character(0)
    opp_sel <- get_input("opponents") %||% character(0)
    td <- cmp_ref$teams
    team_ids_csv <- NA_character_
    opp_ids_csv <- NA_character_
    if (length(team_sel) && !is.null(td)) {
      ids <- td$team_id[td$team_name %in% team_sel]
      if (length(ids)) team_ids_csv <- paste(ids, collapse = ",")
    }
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
      game_type_csv = game_type_csv, team_ids_csv = team_ids_csv, team_names = team_sel, opp_ids_csv = opp_ids_csv,
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

  # Build a short description of what a side's filters mean
  side_summary <- function(side) {
    pfx <- paste0("cmp_", side, "_")
    get_input <- function(name) input[[paste0(pfx, name)]]
    parts <- character(0)

    st_mode <- get_input("starters_mode") %||% ""
    st_val <- get_input("starters_val") %||% ""
    if (nzchar(st_mode) && nzchar(st_val)) {
      op <- if (st_mode == "gte") "\u2265" else "\u2264"
      parts <- c(parts, paste0("Starters ", op, st_val))
    }
    ha <- get_input("home_away") %||% ""
    if (nzchar(ha)) parts <- c(parts, tools::toTitleCase(ha))
    oc <- get_input("outcome") %||% ""
    if (nzchar(oc)) parts <- c(parts, tools::toTitleCase(oc))
    if (isTRUE(get_input("clutch"))) parts <- c(parts, "Clutch")
    if (!identical(input$cmp_mode, "Players")) {
      preset <- input$cmp_preset %||% ""
      if (identical(preset, "date_split")) {
        split_date <- suppressWarnings(as.Date(input$cmp_split_date))
        if (!is.na(split_date)) {
          parts <- c(parts, if (identical(side, "a")) {
            paste0("Before ", format(split_date, "%Y-%m-%d"))
          } else {
            paste0("After ", format(split_date, "%Y-%m-%d"))
          })
        }
      } else if (identical(preset, "gn_split")) {
        split_gn <- suppressWarnings(as.integer(input$cmp_split_gn %||% ""))
        if (is.finite(split_gn)) {
          parts <- c(parts, if (identical(side, "a")) {
            paste0("Before GN ", split_gn)
          } else {
            paste0("After GN ", split_gn)
          })
        }
      }
    }
    if (length(parts)) paste(parts, collapse = ", ") else paste0("Side ", toupper(side))
  }

  apply_side_team_filter <- function(df, p) {
    if (is.null(df) || !nrow(df)) return(df)
    ids <- integer(0)
    if (!is.null(p$team_ids_csv) && !is.na(p$team_ids_csv) && nzchar(p$team_ids_csv)) {
      ids <- suppressWarnings(as.integer(strsplit(p$team_ids_csv, ",", fixed = TRUE)[[1]]))
      ids <- ids[is.finite(ids)]
    }
    if (length(ids) && ("team_id" %in% names(df))) {
      return(df[df$team_id %in% ids, , drop = FALSE])
    }
    teams <- p$team_names %||% character(0)
    if (length(teams) && ("team_name" %in% names(df))) {
      return(df[df$team_name %in% teams, , drop = FALSE])
    }
    df
  }

  apply_min_poss_filter <- function(df, min_poss = 10L) {
    if (is.null(df) || !nrow(df)) return(df)
    keep <- rep(TRUE, nrow(df))
    if ("poss_a" %in% names(df)) {
      pa <- suppressWarnings(as.numeric(df$poss_a))
      keep <- keep & is.finite(pa) & (pa >= min_poss)
    }
    if ("poss_b" %in% names(df)) {
      pb <- suppressWarnings(as.numeric(df$poss_b))
      keep <- keep & is.finite(pb) & (pb >= min_poss)
    }
    df[keep, , drop = FALSE]
  }

  # -- SQL runners --

  run_team_ratings <- function(p) {
    allowed <- guard_heavy_request(
      session, key = "cmp_team_ratings",
      start_d = p$start_d, end_d = p$end_d,
      min_gn = p$min_gn, max_gn = p$max_gn, last_n = p$last_n_games,
      max_calls = 50L, window_sec = 60L
    )
    if (!isTRUE(allowed)) return(data.frame())
    db_get_query(pg_pool, paste0(
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
    db_get_query(pg_pool, paste0(
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
    db_get_query(pg_pool, paste0(
      "SELECT * FROM basketball_test.fetch_lineups_csv_v2(",
      "$1::int4,$2::text,$3::text,$4::text,$5::bool,$6::date,$7::date,$8::int4,$9::int4,",
      "$10::text,$11::text,$12::text,$13::text,$14::text,$15::int4,$16::text,$17::int4,$18::text,$19::int4,$20::bool,",
      "$21::int4,$22::int4,$23::int4,$24::int4,$25::int4,$26::int4,$27::int4,$28::int4,$29::int4",
      ")"), params = list(
      5L,            # num (lineup size)
      p$team_ids_csv, # team_csv
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
    db_get_query(pg_pool, paste0(
      "SELECT * FROM basketball_test.fetch_lineups_four_factors_csv(",
      "$1::int4,$2::text,$3::text,$4::text,$5::bool,$6::date,$7::date,$8::int4,$9::int4,",
      "$10::text,$11::text,$12::text,$13::text,$14::text,$15::int4,$16::text,$17::int4,$18::text,$19::int4,$20::bool,",
      "$21::int4,$22::int4,$23::int4,$24::int4,$25::int4,$26::int4,$27::int4,$28::int4,$29::int4",
      ")"), params = list(
      5L,            # num
      p$team_ids_csv, # team_csv
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
    tryCatch(
      db_get_query(pg_pool, paste0(
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
      )),
      error = function(e) {
        msg <- conditionMessage(e)
        if (grepl("statement timeout", msg, ignore.case = TRUE)) {
          showNotification("Player compare query timed out. Narrow filters or date range.", type = "warning", duration = 5)
        } else {
          message(sprintf("[tab7][player_compare] query failed: %s", msg))
          showNotification("Player compare query failed. Try narrowing filters or retry.", type = "error", duration = 6)
        }
        data.frame()
      }
    )
  }

  run_four_factors <- function(p, team_ids_csv) {
    allowed <- guard_heavy_request(
      session, key = "cmp_four_factors",
      start_d = p$start_d, end_d = p$end_d,
      min_gn = p$min_gn, max_gn = p$max_gn, last_n = p$last_n_games,
      max_calls = 50L, window_sec = 60L
    )
    if (!isTRUE(allowed)) return(data.frame())
    team_csv <- if (is.null(team_ids_csv) || is.na(team_ids_csv) || !nzchar(team_ids_csv)) NA_character_ else team_ids_csv
    db_get_query(pg_pool, paste0(
      "SELECT * FROM basketball_test.four_factors_compute(",
      "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,",
      "$7::text,$8::text,$9::text,$10::int4,$11::text,",
      "$12::int4,$13::int4,$14::int4,$15::int4,$16::int4,$17::int4,$18::int4,$19::int4,$20::int4",
      ")"), params = list(
      p$game_year,
      as.Date(p$start_d), as.Date(p$end_d),
      team_csv,
      p$game_type_csv, p$opp_ids_csv, p$home_away, p$outcome,
      p$opp_rank_side, p$opp_rank_n, p$opp_rank_metric,
      p$min_gn, p$max_gn, p$last_n_games,
      p$num_starters_off, p$num_starters_def,
      p$num_starters_off_min, p$num_starters_off_max,
      p$num_starters_def_min, p$num_starters_def_max
    ))
  }

  run_onoff_impact <- function(p, team_ids_csv) {
    allowed <- guard_heavy_request(
      session, key = "cmp_onoff_impact",
      start_d = p$start_d, end_d = p$end_d,
      min_gn = p$min_gn, max_gn = p$max_gn, last_n = p$last_n_games,
      max_calls = 50L, window_sec = 60L
    )
    if (!isTRUE(allowed)) return(data.frame())
    db_get_query(pg_pool, paste0(
      "SELECT * FROM basketball_test.onoff_compute(",
      "$1::date,$2::date,$3::text,$4::int4,$5::int4,$6::numeric,$7::text,",
      "$8::text,$9::text,$10::text,$11::text,$12::text,$13::int4,$14::text,",
      "$15::int4,$16::int4,$17::int4,$18::int4,$19::int4,$20::int4,$21::int4,$22::int4,$23::int4",
      ")"), params = list(
      as.Date(p$start_d), as.Date(p$end_d), team_ids_csv,
      0L, 0L, as.numeric(DEFAULT_MIN_NET), as.character(p$game_year),
      p$game_type_csv, p$opp_ids_csv, p$home_away, p$outcome,
      p$opp_rank_side, p$opp_rank_n, p$opp_rank_metric,
      p$min_gn, p$max_gn, p$last_n_games,
      p$num_starters_off, p$num_starters_def,
      p$num_starters_off_min, p$num_starters_off_max,
      p$num_starters_def_min, p$num_starters_def_max
    ))
  }

  # -- Metric chip definitions per mode --

  TEAM_METRICS <- c(
    "Net Rtg" = "net_rtg", "Offense" = "off_ppp", "Defense" = "def_ppp",
    "TS%" = "off_ts_pct", "TOV%" = "off_tov_pct", "OREB%" = "off_oreb_pct", "FTR" = "off_ftr"
  )

  PLAYER_METRICS <- c(
    "PPG" = "ppg", "RPG" = "rpg", "APG" = "apg", "SPG" = "spg",
    "FG%" = "fg_pct", "3P%" = "fg3_pct", "FT%" = "ft_pct", "TS%" = "ts_pct"
  )

  PLAYER_VIEWS <- c(
    "Overall" = "overall",
    "Four Factors" = "ff_swing"
  )

  selected_player_view <- reactiveVal("overall")

  normalize_teams_ref <- function(df) {
    if (is.null(df) || !nrow(df) || is.null(names(df))) return(data.frame())
    names(df) <- tolower(names(df))
    if (!("team_id" %in% names(df))) return(data.frame())
    if (!("team_name" %in% names(df))) {
      if ("name" %in% names(df)) {
        df$team_name <- as.character(df$name)
      } else if ("team" %in% names(df)) {
        df$team_name <- as.character(df$team)
      } else {
        return(data.frame())
      }
    }
    out <- df[, c("team_id", "team_name"), drop = FALSE]
    out$team_id <- suppressWarnings(as.integer(out$team_id))
    out$team_name <- as.character(out$team_name)
    out <- out[is.finite(out$team_id) & nzchar(out$team_name), , drop = FALSE]
    if (!nrow(out)) return(data.frame())
    unique(out)
  }

  normalize_players_ref <- function(df) {
    if (is.null(df) || !nrow(df) || is.null(names(df))) return(data.frame())
    names(df) <- tolower(names(df))
    if (!("player_id" %in% names(df)) || !("team_id" %in% names(df))) return(data.frame())
    if (!("name" %in% names(df))) {
      if (all(c("firstname", "lastname") %in% names(df))) {
        df$name <- trimws(paste(df$firstname, df$lastname))
      } else if (all(c("first_name", "last_name") %in% names(df))) {
        df$name <- trimws(paste(df$first_name, df$last_name))
      } else if ("player_name" %in% names(df)) {
        df$name <- as.character(df$player_name)
      } else {
        return(data.frame())
      }
    }
    out <- df[, c("team_id", "player_id", "name"), drop = FALSE]
    out$team_id <- suppressWarnings(as.integer(out$team_id))
    out$player_id <- suppressWarnings(as.integer(out$player_id))
    out$name <- as.character(out$name)
    out <- out[is.finite(out$team_id) & is.finite(out$player_id) & nzchar(out$name), , drop = FALSE]
    if (!nrow(out)) return(data.frame())
    unique(out)
  }

  render_metric_chips <- function(metrics, cur, input_id) {
    chips <- lapply(seq_along(metrics), function(i) {
      nm <- names(metrics)[i]
      val <- metrics[[i]]
      cls <- if (identical(val, cur)) "btn btn-sm btn-warning" else "btn btn-sm btn-outline-secondary"
      tags$button(
        type = "button",
        class = cls,
        style = "border-radius: 20px; padding: 2px 12px; font-size: .76rem;",
        onclick = sprintf("Shiny.setInputValue('%s', '%s', {priority: 'event'})", input_id, val),
        nm
      )
    })
    do.call(tagList, chips)
  }

  output$cmp_metric_chips_ui <- renderUI({
    if (identical(input$cmp_mode, "Players")) return(NULL)
    metrics <- TEAM_METRICS
    cur <- selected_metric()
    if (!(cur %in% metrics)) cur <- metrics[[1]]
    render_metric_chips(metrics, cur, "cmp_metric")
  })

  output$cmp_player_chips_ui <- renderUI({
    req(identical(input$cmp_mode, "Players"))
    cur <- selected_player_view()
    render_metric_chips(PLAYER_VIEWS, cur, "cmp_player_view")
  })

  observeEvent(input$cmp_metric, {
    if (identical(input$cmp_mode, "Players")) return(NULL)
    metrics <- TEAM_METRICS
    m <- input$cmp_metric %||% ""
    if (m %in% unname(metrics)) selected_metric(m)
  }, ignoreInit = TRUE)

  observeEvent(input$cmp_detail_toggle, {
    is_detail <- identical(input$cmp_detail_toggle, "detail")
    detail_view_active(is_detail)
    if (!is_detail) selected_detail_entity(NULL)
  }, ignoreInit = TRUE)

  observe({
    is_detail <- detail_view_active()
    session$sendCustomMessage("toggle_cmp_view", list(detail = is_detail))
  })

  observeEvent(input$cmp_player_view, {
    v <- input$cmp_player_view %||% ""
    if (v %in% unname(PLAYER_VIEWS)) selected_player_view(v)
  }, ignoreInit = TRUE)

  # -- Shared filter reset helper --

  reset_compare_filters <- function() {
    updateSelectInput(session, "cmp_preset", selected = "")
    updateDateInput(session, "cmp_split_date", value = DEFAULT_END)
    updateSelectizeInput(session, "cmp_split_gn", selected = character(0))
    b <- shared$season_date_bounds(input$game_year %||% DEFAULT_GAME_YEAR)
    updateDateRangeInput(session, "cmp_players_dates", start = b$start, end = b$end, min = b$start, max = b$end)
    updateSelectizeInput(session, "cmp_players_gn_min", selected = character(0))
    updateSelectizeInput(session, "cmp_players_gn_max", selected = character(0))
    for (s in c("a", "b")) {
      updateSelectInput(session, paste0("cmp_", s, "_starters_mode"), selected = "")
      updateSelectInput(session, paste0("cmp_", s, "_starters_val"), selected = "")
      updateSelectInput(session, paste0("cmp_", s, "_home_away"), selected = "")
      updateSelectInput(session, paste0("cmp_", s, "_outcome"), selected = "")
      updateCheckboxInput(session, paste0("cmp_", s, "_clutch"), value = FALSE)
      updateSliderInput(session, paste0("cmp_", s, "_clutch_margin"), value = 5)
      updateSliderInput(session, paste0("cmp_", s, "_clutch_minutes"), value = 5)
      updateSelectizeInput(session, paste0("cmp_", s, "_teams"), selected = character(0))
      updateSelectizeInput(session, paste0("cmp_", s, "_opponents"), selected = character(0))
      updateSelectizeInput(session, paste0("cmp_", s, "_game_type"), selected = character(0))
      updateSelectInput(session, paste0("cmp_", s, "_opp_rank_side"), selected = "")
      updateSelectInput(session, paste0("cmp_", s, "_opp_rank_n"), selected = "")
    }
    updateSelectizeInput(session, "cmp_player_a", selected = character(0))
    updateSelectizeInput(session, "cmp_player_b", selected = character(0))
    updateSelectizeInput(session, "cmp_player_a_list_team_filter", selected = character(0))
    updateSelectizeInput(session, "cmp_player_b_list_team_filter", selected = character(0))
    updateSelectInput(session, "cmp_player_a_team", selected = "")
    updateSelectInput(session, "cmp_player_b_team", selected = "")
  }

  refresh_player_choices <- function(side) {
    players_df <- normalize_players_ref(cmp_ref$players)
    teams_df <- normalize_teams_ref(cmp_ref$teams)
    if (is.null(players_df) || !nrow(players_df)) return(NULL)

    side <- match.arg(side, c("a", "b"))
    list_filter_id <- paste0("cmp_player_", side, "_list_team_filter")
    player_id <- paste0("cmp_player_", side)
    keep_val <- input[[player_id]] %||% ""

    team_sel <- as.character(input[[list_filter_id]] %||% character(0))
    team_sel <- team_sel[nzchar(team_sel)]
    filtered <- players_df
    if (length(team_sel) && !is.null(teams_df) && nrow(teams_df)) {
      ids <- teams_df$team_id[teams_df$team_name %in% team_sel]
      if (length(ids)) filtered <- filtered[filtered$team_id %in% ids, , drop = FALSE]
      else filtered <- filtered[0, , drop = FALSE]
    }
    if (!nrow(filtered)) {
      updateSelectizeInput(session, player_id, choices = c(), selected = character(0), server = TRUE)
      return(NULL)
    }

    filtered <- filtered[order(filtered$name), c("player_id", "name"), drop = FALSE]
    filtered <- filtered[!duplicated(filtered$player_id), , drop = FALSE]
    choice_values <- as.character(filtered$player_id)
    player_choices <- setNames(choice_values, filtered$name)
    if (!(keep_val %in% choice_values)) keep_val <- ""

    updateSelectizeInput(session, player_id, choices = player_choices, selected = keep_val, server = TRUE)
  }

  get_default_player_ids <- function() {
    gy_int <- as.integer(input$game_year)
    scorer_df <- cached_ref_query(
      key = sprintf("cmp_default_scorers_%d", gy_int),
      query_fun = function() db_get_query(pg_pool, paste0(
        "SELECT player_id, gp, pts FROM basketball_test.get_player_traditional_dynamic(",
        "$1::int4, NULL::date, NULL::date, NULL::text, NULL::text, NULL::text, NULL::text, NULL::text, ",
        "NULL::text, NULL::int4, NULL::text, NULL::int4, NULL::text, NULL::int4, NULL::bool, NULL::int4, NULL::int4, NULL::int4",
        ")"
      ), params = list(gy_int))
    )
    if (is.null(scorer_df) || !nrow(scorer_df)) return(integer(0))
    scorer_df$gp <- suppressWarnings(as.numeric(scorer_df$gp))
    scorer_df$pts <- suppressWarnings(as.numeric(scorer_df$pts))
    scorer_df <- scorer_df[is.finite(scorer_df$gp) & scorer_df$gp > 0 & is.finite(scorer_df$pts), , drop = FALSE]
    if (!nrow(scorer_df)) return(integer(0))
    scorer_df$ppg <- scorer_df$pts / scorer_df$gp
    scorer_df <- scorer_df[order(-scorer_df$ppg, -scorer_df$pts), , drop = FALSE]
    top3 <- unique(as.integer(scorer_df$player_id[seq_len(min(3L, nrow(scorer_df)))]))
    top3 <- top3[is.finite(top3)]
    if (length(top3) >= 2L) return(sample(top3, size = 2L, replace = FALSE))
    top3
  }

  apply_default_players <- function() {
    if (!identical(input$main_tabs, "compare") || !identical(input$cmp_mode, "Players")) return(invisible(NULL))
    if (!is.null(input$cmp_player_a) && nzchar(input$cmp_player_a %||% "")) return(invisible(NULL))
    if (!is.null(input$cmp_player_b) && nzchar(input$cmp_player_b %||% "")) return(invisible(NULL))

    players_df <- normalize_players_ref(cmp_ref$players)
    if (is.null(players_df) || !nrow(players_df)) return(invisible(NULL))
    players_df <- players_df[order(players_df$name), c("player_id", "name"), drop = FALSE]
    players_df <- players_df[!duplicated(players_df$player_id), , drop = FALSE]

    choice_values <- as.character(players_df$player_id)
    player_choices <- stats::setNames(choice_values, players_df$name)
    available_ids <- unique(suppressWarnings(as.integer(players_df$player_id)))
    available_ids <- available_ids[is.finite(available_ids)]
    if (length(available_ids) < 2L) return(invisible(NULL))

    ids <- get_default_player_ids()
    ids <- ids[ids %in% available_ids]
    if (length(ids) < 2L) {
      # Fallback to first available two players.
      ids <- available_ids[seq_len(min(2L, length(available_ids)))]
    }
    if (length(ids) < 2L) return(invisible(NULL))

    session$onFlushed(function() {
      updateSelectizeInput(session, "cmp_player_a", choices = player_choices, selected = as.character(ids[[1]]), server = TRUE)
      updateSelectizeInput(session, "cmp_player_b", choices = player_choices, selected = as.character(ids[[2]]), server = TRUE)
    }, once = TRUE)
    cmp_auto_default_ids(as.integer(ids))
    cmp_defaults_active(TRUE)
    invisible(NULL)
  }

  # -- Mode change: full filter reset + metric validity --

  observeEvent(input$cmp_mode, {
    reset_compare_filters()
    mode <- input$cmp_mode
    valid <- if (identical(mode, "Players")) PLAYER_METRICS else TEAM_METRICS
    if (!(selected_metric() %in% valid)) selected_metric(valid[[1]])
    selected_player_view("overall")
    selected_detail_entity(NULL)
    detail_view_active(FALSE)
    if (identical(mode, "Players")) apply_default_players()
  }, ignoreInit = TRUE)

  # -- Tab init: load ref data --

  observeEvent(list(input$main_tabs, input$game_year), ignoreInit = FALSE, {
    if (!identical(input$main_tabs, "compare")) return(NULL)
    gy_int <- as.integer(input$game_year)

    teams_df <- cached_ref_query(
      key = sprintf("cmp_teams_%d", gy_int),
      query_fun = function() db_get_query(pg_pool, sprintf(
        "SELECT DISTINCT team_id, team_name FROM basketball_test.full_rosters WHERE game_year = %d ORDER BY team_name", gy_int))
    )
    teams_df <- normalize_teams_ref(teams_df)
    cmp_ref$teams <- teams_df
    team_choices <- if (nrow(teams_df)) teams_df$team_name else character(0)
    updateSelectizeInput(session, "cmp_a_teams", choices = team_choices, selected = character(0), server = TRUE)
    updateSelectizeInput(session, "cmp_b_teams", choices = team_choices, selected = character(0), server = TRUE)
    updateSelectizeInput(session, "cmp_a_opponents", choices = team_choices, selected = character(0), server = TRUE)
    updateSelectizeInput(session, "cmp_b_opponents", choices = team_choices, selected = character(0), server = TRUE)
    updateSelectizeInput(session, "cmp_player_a_list_team_filter", choices = team_choices, selected = character(0), server = TRUE)
    updateSelectizeInput(session, "cmp_player_b_list_team_filter", choices = team_choices, selected = character(0), server = TRUE)
    gn_df <- cached_ref_query(
      key = sprintf("cmp_gn_%d", gy_int),
      query_fun = function() db_get_query(pg_pool, sprintf(
        "SELECT DISTINCT gn FROM basketball_test.final_schedule_mv WHERE game_year = %d ORDER BY gn", gy_int))
    )
    gn_choices <- if (nrow(gn_df)) as.character(gn_df$gn) else character(0)
    updateSelectizeInput(session, "cmp_players_gn_min", choices = c("", gn_choices), selected = "", server = TRUE)
    updateSelectizeInput(session, "cmp_players_gn_max", choices = c("", gn_choices), selected = "", server = TRUE)
    updateSelectizeInput(session, "cmp_split_gn", choices = c("", gn_choices), selected = "", server = TRUE)
    b <- shared$season_date_bounds(as.character(gy_int))
    updateDateRangeInput(session, "cmp_players_dates", start = b$start, end = b$end, min = b$start, max = b$end)
    updateDateInput(session, "cmp_split_date", value = b$end, min = b$start, max = b$end)

    players_df <- cached_ref_query(
      key = sprintf("cmp_players_%d", gy_int),
      query_fun = function() db_get_query(pg_pool, sprintf(
        "SELECT team_id, player_id, MIN(btrim(firstname)||' '||btrim(lastname)) AS name FROM basketball_test.full_rosters WHERE game_year = %d GROUP BY team_id, player_id ORDER BY MIN(btrim(firstname)||' '||btrim(lastname))", gy_int))
    )
    players_df <- normalize_players_ref(players_df)
    cmp_ref$players <- players_df
    refresh_player_choices("a")
    refresh_player_choices("b")
    apply_default_players()

    # Apply pending preset from home tab
    pending <- shared$pending_compare_preset()
    if (!is.null(pending) && nzchar(pending)) {
      shared$pending_compare_preset(NULL)
      updateSelectInput(session, "cmp_preset", selected = pending)
    }
  })

  observeEvent(input$cmp_player_a_list_team_filter, {
    refresh_player_choices("a")
  }, ignoreInit = TRUE)

  observeEvent(input$cmp_player_b_list_team_filter, {
    refresh_player_choices("b")
  }, ignoreInit = TRUE)

  observeEvent(list(input$cmp_player_a, input$cmp_player_b), {
    ids <- cmp_auto_default_ids()
    if (length(ids) < 2L) return()
    cur_a <- suppressWarnings(as.integer(input$cmp_player_a %||% ""))
    cur_b <- suppressWarnings(as.integer(input$cmp_player_b %||% ""))
    if (!((is.finite(cur_a) && cur_a %in% ids) && (is.finite(cur_b) && cur_b %in% ids))) {
      cmp_defaults_active(FALSE)
    }
  }, ignoreInit = TRUE)

  players_filters_pristine <- function() {
    b <- shared$season_date_bounds(input$game_year %||% DEFAULT_GAME_YEAR)
    dr <- input$cmp_players_dates
    same_dates <- TRUE
    if (!is.null(dr) && length(dr) >= 2) {
      d_start <- suppressWarnings(as.Date(dr[[1]]))
      d_end <- suppressWarnings(as.Date(dr[[2]]))
      same_dates <- !is.na(d_start) && !is.na(d_end) && identical(d_start, as.Date(b$start)) && identical(d_end, as.Date(b$end))
      same_dates <- isTRUE(same_dates)
    }

    empty_chr <- function(x) is.null(x) || !length(x) || !any(nzchar(as.character(x)))
    is_false <- function(x) isFALSE(isTRUE(x))

    same_dates &&
      empty_chr(input$cmp_players_gn_min) &&
      empty_chr(input$cmp_players_gn_max) &&
      empty_chr(input$cmp_a_game_type) &&
      empty_chr(input$cmp_b_game_type) &&
      empty_chr(input$cmp_a_opponents) &&
      empty_chr(input$cmp_b_opponents) &&
      empty_chr(input$cmp_a_home_away) &&
      empty_chr(input$cmp_b_home_away) &&
      empty_chr(input$cmp_a_outcome) &&
      empty_chr(input$cmp_b_outcome) &&
      is_false(input$cmp_a_clutch) &&
      is_false(input$cmp_b_clutch)
  }

  # Clear auto-default players once user starts changing filters.
  observeEvent(list(
    input$cmp_players_dates, input$cmp_players_gn_min, input$cmp_players_gn_max,
    input$cmp_a_game_type, input$cmp_b_game_type,
    input$cmp_a_opponents, input$cmp_b_opponents,
    input$cmp_a_home_away, input$cmp_b_home_away,
    input$cmp_a_outcome, input$cmp_b_outcome,
    input$cmp_a_clutch, input$cmp_b_clutch
  ), {
    if (!identical(input$cmp_mode, "Players")) return()
    if (!isTRUE(cmp_defaults_active())) return()
    if (isTRUE(players_filters_pristine())) return()
    ids <- cmp_auto_default_ids()
    if (length(ids) < 2L) return()
    cur_a <- suppressWarnings(as.integer(input$cmp_player_a %||% ""))
    cur_b <- suppressWarnings(as.integer(input$cmp_player_b %||% ""))
    if ((is.finite(cur_a) && cur_a %in% ids) && (is.finite(cur_b) && cur_b %in% ids)) {
      updateSelectizeInput(session, "cmp_player_a", selected = character(0), server = TRUE)
      updateSelectizeInput(session, "cmp_player_b", selected = character(0), server = TRUE)
    }
    cmp_defaults_active(FALSE)
  }, ignoreInit = TRUE)

  # -- Preset handler --

  observeEvent(input$cmp_preset, {
    preset <- input$cmp_preset
    if (is.null(preset) || !nzchar(preset)) {
      return()
    }
    clear_side <- function(s) {
      updateSelectInput(session, paste0("cmp_", s, "_starters_mode"), selected = "")
      updateSelectInput(session, paste0("cmp_", s, "_starters_val"), selected = "")
      updateSelectInput(session, paste0("cmp_", s, "_home_away"), selected = "")
      updateSelectInput(session, paste0("cmp_", s, "_outcome"), selected = "")
      updateCheckboxInput(session, paste0("cmp_", s, "_clutch"), value = FALSE)
      updateSelectizeInput(session, paste0("cmp_", s, "_teams"), selected = character(0))
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
      updateSliderInput(session, "cmp_a_clutch_minutes", value = 5)
    } else if (preset == "home_away") {
      updateSelectInput(session, "cmp_a_home_away", selected = "home")
      updateSelectInput(session, "cmp_b_home_away", selected = "away")
    } else if (preset == "win_loss") {
      updateSelectInput(session, "cmp_a_outcome", selected = "win")
      updateSelectInput(session, "cmp_b_outcome", selected = "loss")
    }
  }, ignoreInit = TRUE)

  # -- PvP Player Comparison (Players mode) --

  player_team_choices <- function(player_id_chr) {
    players_df <- normalize_players_ref(cmp_ref$players)
    teams_df <- normalize_teams_ref(cmp_ref$teams)
    if (is.null(players_df) || is.null(teams_df) || !nrow(players_df) || !nrow(teams_df)) return(data.frame())
    if (is.null(player_id_chr) || !nzchar(player_id_chr)) return(data.frame())
    pid <- suppressWarnings(as.integer(player_id_chr))
    if (!is.finite(pid)) return(data.frame())
    ids <- unique(players_df$team_id[players_df$player_id == pid])
    ids <- ids[is.finite(ids)]
    if (!length(ids)) return(data.frame())
    out <- teams_df[teams_df$team_id %in% ids, c("team_id", "team_name"), drop = FALSE]
    out[order(out$team_name), , drop = FALSE]
  }

  output$cmp_player_a_team_ui <- renderUI({
    req(identical(input$cmp_mode, "Players"))
    td <- player_team_choices(input$cmp_player_a)
    if (nrow(td) <= 1) return(NULL)
    choices <- c("All teams" = "", setNames(as.character(td$team_id), td$team_name))
    selectInput("cmp_player_a_team", "Team (optional)", choices = choices, selected = "")
  })

  output$cmp_player_b_team_ui <- renderUI({
    req(identical(input$cmp_mode, "Players"))
    td <- player_team_choices(input$cmp_player_b)
    if (nrow(td) <= 1) return(NULL)
    choices <- c("All teams" = "", setNames(as.character(td$team_id), td$team_name))
    selectInput("cmp_player_b_team", "Team (optional)", choices = choices, selected = "")
  })

  PVP_STATS <- list(
    list(label = "Points",   col = "pts",    type = "count"),
    list(label = "Rebounds",  col = "reb",    type = "count"),
    list(label = "Assists",   col = "ast",    type = "count"),
    list(label = "Steals",    col = "stl",    type = "count"),
    list(label = "FG%",       col = "fg_pct", type = "pct"),
    list(label = "3P%",       col = "tp_pct", type = "pct"),
    list(label = "FT%",       col = "ft_pct", type = "pct"),
    list(label = "TS%",       col = "ts",     type = "pct")
  )

  cmp_player_raw <- reactive({
    req(identical(input$cmp_mode, "Players"))
    req(identical(input$main_tabs, "compare"))

    # Fallback: ensure refs exist even if tab-init observer didn't run yet.
    if (is.null(cmp_ref$players) || !nrow(cmp_ref$players)) {
      gy_int <- as.integer(input$game_year)
      players_df <- cached_ref_query(
        key = sprintf("cmp_players_%d", gy_int),
        query_fun = function() db_get_query(pg_pool, sprintf(
          "SELECT team_id, player_id, MIN(btrim(firstname)||' '||btrim(lastname)) AS name FROM basketball_test.full_rosters WHERE game_year = %d GROUP BY team_id, player_id ORDER BY MIN(btrim(firstname)||' '||btrim(lastname))", gy_int))
      )
      cmp_ref$players <- normalize_players_ref(players_df)
    }
    if (is.null(cmp_ref$teams) || !nrow(cmp_ref$teams)) {
      gy_int <- as.integer(input$game_year)
      teams_df <- cached_ref_query(
        key = sprintf("cmp_teams_%d", gy_int),
        query_fun = function() db_get_query(pg_pool, sprintf(
          "SELECT DISTINCT team_id, team_name FROM basketball_test.full_rosters WHERE game_year = %d ORDER BY team_name", gy_int))
      )
      cmp_ref$teams <- normalize_teams_ref(teams_df)
    }

    player_a_id <- input$cmp_player_a
    player_b_id <- input$cmp_player_b
    req(player_a_id, nzchar(player_a_id))
    req(player_b_id, nzchar(player_b_id))

    players_df <- normalize_players_ref(cmp_ref$players)
    cmp_ref$players <- players_df
    req(!is.null(players_df), nrow(players_df) > 0)

    pa <- collect_side_params("a")
    pb <- collect_side_params("b")

    team_ids_a <- unique(players_df$team_id[players_df$player_id == as.integer(player_a_id)])
    team_ids_b <- unique(players_df$team_id[players_df$player_id == as.integer(player_b_id)])
    team_sel_a <- suppressWarnings(as.integer(input$cmp_player_a_team %||% ""))
    team_sel_b <- suppressWarnings(as.integer(input$cmp_player_b_team %||% ""))
    if (is.finite(team_sel_a) && (team_sel_a %in% team_ids_a)) team_ids_a <- team_sel_a
    if (is.finite(team_sel_b) && (team_sel_b %in% team_ids_b)) team_ids_b <- team_sel_b
    if (!length(team_ids_a) || !length(team_ids_b)) return(NULL)

    res_a <- run_player_traditional(pa, paste(team_ids_a, collapse = ","))
    res_b <- run_player_traditional(pb, paste(team_ids_b, collapse = ","))
    if (!nrow(res_a) || !nrow(res_b)) return(NULL)

    row_a <- res_a[res_a$player_id == as.integer(player_a_id), , drop = FALSE]
    row_b <- res_b[res_b$player_id == as.integer(player_b_id), , drop = FALSE]
    if (!nrow(row_a) || !nrow(row_b)) return(NULL)

    name_a <- players_df$name[players_df$player_id == as.integer(player_a_id)][1]
    name_b <- players_df$name[players_df$player_id == as.integer(player_b_id)][1]

    teams_df <- normalize_teams_ref(cmp_ref$teams)
    cmp_ref$teams <- teams_df
    team_name_a <- if (!is.null(teams_df)) teams_df$team_name[teams_df$team_id == team_ids_a[1]][1] else ""
    team_name_b <- if (!is.null(teams_df)) teams_df$team_name[teams_df$team_id == team_ids_b[1]][1] else ""

    list(
      row_a = row_a[1, ], row_b = row_b[1, ],
      name_a = name_a, name_b = name_b,
      team_a = team_name_a %||% "", team_b = team_name_b %||% "",
      team_ids_a = team_ids_a, team_ids_b = team_ids_b,
      pa = pa, pb = pb
    )
  })

  cmp_player_ff_raw <- reactive({
    req(identical(input$cmp_mode, "Players"))
    req(identical(selected_player_view(), "ff_swing"))
    data <- cmp_player_raw()
    req(data)

    player_a_id <- input$cmp_player_a
    player_b_id <- input$cmp_player_b

    ff_a <- run_four_factors(data$pa, paste(data$team_ids_a, collapse = ","))
    ff_b <- run_four_factors(data$pb, paste(data$team_ids_b, collapse = ","))
    onoff_a <- run_onoff_impact(data$pa, paste(data$team_ids_a, collapse = ","))
    onoff_b <- run_onoff_impact(data$pb, paste(data$team_ids_b, collapse = ","))
    if (!nrow(ff_a) || !nrow(ff_b)) return(NULL)

    row_a <- ff_a[ff_a$player_id == as.integer(player_a_id), , drop = FALSE]
    row_b <- ff_b[ff_b$player_id == as.integer(player_b_id), , drop = FALSE]
    if (!nrow(row_a) || !nrow(row_b)) return(NULL)
    on_a <- onoff_a[onoff_a$player_id == as.integer(player_a_id), , drop = FALSE]
    on_b <- onoff_b[onoff_b$player_id == as.integer(player_b_id), , drop = FALSE]

    list(
      row_a = row_a[1, ], row_b = row_b[1, ],
      onoff_a = if (nrow(on_a)) on_a[1, ] else NULL,
      onoff_b = if (nrow(on_b)) on_b[1, ] else NULL,
      name_a = data$name_a, name_b = data$name_b,
      team_a = data$team_a, team_b = data$team_b
    )
  })

  # -- Shared PvP UI helpers --

  badge_css <- "background: rgba(232,164,53,.15); color: #e8a435; border: 1px solid rgba(232,164,53,.35); border-radius: 4px; padding: 1px 8px; font-size: .78rem; font-weight: 600; white-space: nowrap;"
  val_win_css <- "font-size: 1.05rem; font-weight: 600; color: #e6edf3;"
  val_lose_css <- "font-size: 1.05rem; font-weight: 600; color: #8b949e;"

  pvp_header <- function(name_a, team_a, info_a, name_b, team_b, info_b) {
    tags$div(
      style = "display: flex; align-items: center; justify-content: center; margin-bottom: 20px; padding: 16px 0; border-bottom: 1px solid rgba(255,255,255,.08);",
      tags$div(
        style = "flex: 1; text-align: center;",
        tags$div(style = "font-size: 1.25rem; font-weight: 700; color: #7b8cde;", name_a),
        tags$div(style = "font-size: .82rem; color: #8b949e; margin-top: 2px;", team_a),
        tags$div(style = "font-size: .78rem; color: #6e7681; margin-top: 2px;", info_a)
      ),
      tags$div(style = "font-size: .9rem; font-weight: 700; color: #484f58; padding: 0 20px;", "vs"),
      tags$div(
        style = "flex: 1; text-align: center;",
        tags$div(style = "font-size: 1.25rem; font-weight: 700; color: #e8a435;", name_b),
        tags$div(style = "font-size: .82rem; color: #8b949e; margin-top: 2px;", team_b),
        tags$div(style = "font-size: .78rem; color: #6e7681; margin-top: 2px;", info_b)
      )
    )
  }

  pvp_stat_row <- function(label, va, vb, fmt_fn, higher_is_better = TRUE) {
    diff <- if (!is.na(va) && !is.na(vb)) abs(va - vb) else NA_real_
    if (higher_is_better) {
      a_better <- !is.na(va) && !is.na(vb) && va > vb
      b_better <- !is.na(va) && !is.na(vb) && vb > va
    } else {
      a_better <- !is.na(va) && !is.na(vb) && va < vb
      b_better <- !is.na(va) && !is.na(vb) && vb < va
    }
    diff_txt <- if (!is.na(diff) && diff > 0.05) sprintf("+%.1f", diff) else NULL
    left_badge <- if (a_better && !is.null(diff_txt)) tags$span(style = badge_css, diff_txt) else NULL
    right_badge <- if (b_better && !is.null(diff_txt)) tags$span(style = badge_css, diff_txt) else NULL

    tags$div(
      style = "display: flex; align-items: center; padding: 10px 0; border-bottom: 1px solid rgba(255,255,255,.06);",
      tags$div(
        style = "flex: 1; display: flex; align-items: center; justify-content: flex-end; gap: 10px;",
        left_badge,
        tags$span(style = if (a_better) val_win_css else val_lose_css, fmt_fn(va))
      ),
      tags$div(
        style = "width: 130px; text-align: center; font-size: .85rem; font-weight: 600; color: #8b949e;",
        label
      ),
      tags$div(
        style = "flex: 1; display: flex; align-items: center; justify-content: flex-start; gap: 10px;",
        tags$span(style = if (b_better) val_win_css else val_lose_css, fmt_fn(vb)),
        right_badge
      )
    )
  }

  pvp_section_header <- function(label) {
    tags$div(
      style = "text-align: center; padding: 14px 0 6px; font-size: .75rem; font-weight: 700; text-transform: uppercase; letter-spacing: .08em; color: #6e7681;",
      label
    )
  }

  # -- FF Swing view --

  FF_SWING_STATS <- list(
    list(label = "Off Diff", col = "Off ON Diff", side = "off"),
    list(label = "TS%",      col = "Off TS% Diff", side = "off"),
    list(label = "OREB%",    col = "Off OREB% Diff", side = "off"),
    list(label = "TOV%",     col = "Off TOV% Diff", side = "off", invert = TRUE),
    list(label = "FTR",      col = "Off FTR Diff", side = "off"),
    list(label = "Def Diff", col = "Def ON Diff", side = "def", invert = TRUE),
    list(label = "TS%",      col = "Def TS% Diff", side = "def", invert = TRUE),
    list(label = "OREB%",    col = "Def OREB% Diff", side = "def", invert = TRUE),
    list(label = "TOV%",     col = "Def TOV% Diff", side = "def"),
    list(label = "FTR",      col = "Def FTR Diff", side = "def", invert = TRUE)
  )

  render_ff_swing_ui <- function() {
    data <- cmp_player_ff_raw()
    req(data)

    trad <- cmp_player_raw()
    req(trad)

    row_a <- data$row_a
    row_b <- data$row_b
    onoff_a <- data$onoff_a
    onoff_b <- data$onoff_b

    # GP / poss info from traditional data
    gp_a <- as.numeric(trad$row_a[["gp"]]); gp_b <- as.numeric(trad$row_b[["gp"]])
    poss_a <- if ("off_on_poss" %in% names(row_a)) as.numeric(row_a[["off_on_poss"]]) else NA_real_
    poss_b <- if ("off_on_poss" %in% names(row_b)) as.numeric(row_b[["off_on_poss"]]) else NA_real_
    info_line_ff <- function(gp, poss) {
      parts <- c()
      if (!is.na(gp)) parts <- c(parts, paste0(gp, " GP"))
      if (!is.na(poss)) parts <- c(parts, paste0(round(poss), " ON Poss"))
      paste(parts, collapse = " \u00b7 ")
    }

    fmt_swing <- function(v) {
      if (is.na(v)) return("\u2014")
      sprintf("%+.1f", v)
    }

    get_swing <- function(ff_row, onoff_row, stat) {
      source_row <- if (grepl("Diff$", stat$label)) onoff_row else ff_row
      if (is.null(source_row) || is.null(stat$col) || !(stat$col %in% names(source_row))) return(NA_real_)
      as.numeric(source_row[[stat$col]])
    }

    # Split stats by side
    off_stats <- FF_SWING_STATS[vapply(FF_SWING_STATS, function(s) s$side == "off", logical(1))]
    def_stats <- FF_SWING_STATS[vapply(FF_SWING_STATS, function(s) s$side == "def", logical(1))]

    make_rows <- function(stats) {
      lapply(stats, function(stat) {
        va <- get_swing(row_a, onoff_a, stat)
        vb <- get_swing(row_b, onoff_b, stat)
        higher_is_better <- !isTRUE(stat$invert)
        pvp_stat_row(stat$label, va, vb, fmt_swing, higher_is_better)
      })
    }

    tagList(
      pvp_header(
        data$name_a, data$team_a, info_line_ff(gp_a, poss_a),
        data$name_b, data$team_b, info_line_ff(gp_b, poss_b)
      ),
      tags$div(
        style = "max-width: 520px; margin: 0 auto;",
        tags$div(
          style = "text-align: center; font-size: .72rem; color: #6e7681; margin-bottom: 8px;",
          "Swing values use the same diffs as Tab 1 Four Factors (plus Off/Def Diff)."
        ),
        pvp_section_header("Offensive Four Factors"),
        do.call(tagList, make_rows(off_stats)),
        pvp_section_header("Defensive Four Factors"),
        do.call(tagList, make_rows(def_stats))
      )
    )
  }

  # -- Overall PvP view --

  output$cmp_pvp_ui <- renderUI({
    if (!nzchar(input$cmp_player_a %||% "") || !nzchar(input$cmp_player_b %||% "")) {
      return(tags$div(
        class = "card bg-dark border-secondary p-3",
        tags$div(class = "small text-muted",
                 "Select Player A and Player B to run Players compare.")
      ))
    }

    view <- selected_player_view()
    if (identical(view, "ff_swing")) {
      return(render_ff_swing_ui())
    }

    data <- cmp_player_raw()
    req(data)

    rate <- input$cmp_rate_mode %||% "Per Game"
    row_a <- data$row_a
    row_b <- data$row_b

    get_val <- function(row, stat) {
      col <- stat$col
      if (!(col %in% names(row))) return(NA_real_)
      raw <- as.numeric(row[[col]])
      if (stat$type == "pct") return(raw)
      if (rate == "Totals") return(raw)
      if (rate == "Per 75 Possessions") {
        poss <- if ("poss_on_floor" %in% names(row)) as.numeric(row[["poss_on_floor"]]) else NA_real_
        if (!is.na(poss) && poss > 0) return(raw / poss * 75)
        return(NA_real_)
      }
      gp <- if ("gp" %in% names(row)) as.numeric(row[["gp"]]) else NA_real_
      if (!is.na(gp) && gp > 0) return(raw / gp)
      NA_real_
    }

    fmt_val <- function(v, stat) {
      if (is.na(v)) return("\u2014")
      if (stat$type == "pct") return(sprintf("%.1f", v))
      if (rate == "Totals") return(sprintf("%.0f", v))
      sprintf("%.1f", v)
    }

    stat_rows <- lapply(PVP_STATS, function(stat) {
      va <- get_val(row_a, stat)
      vb <- get_val(row_b, stat)
      pvp_stat_row(stat$label, va, vb, function(v) fmt_val(v, stat))
    })

    # GP / MPG info
    gp_a <- as.numeric(row_a[["gp"]]); gp_b <- as.numeric(row_b[["gp"]])
    min_a <- if ("minutes" %in% names(row_a) && !is.na(gp_a) && gp_a > 0) as.numeric(row_a[["minutes"]]) / gp_a else NA_real_
    min_b <- if ("minutes" %in% names(row_b) && !is.na(gp_b) && gp_b > 0) as.numeric(row_b[["minutes"]]) / gp_b else NA_real_
    info_line <- function(gp, mpg) {
      parts <- c()
      if (!is.na(gp)) parts <- c(parts, paste0(gp, " GP"))
      if (!is.na(mpg)) parts <- c(parts, paste0(sprintf("%.1f", mpg), " MPG"))
      paste(parts, collapse = " \u00b7 ")
    }

    tagList(
      pvp_header(
        data$name_a, data$team_a, info_line(gp_a, min_a),
        data$name_b, data$team_b, info_line(gp_b, min_b)
      ),
      tags$div(
        style = "max-width: 520px; margin: 0 auto;",
        do.call(tagList, stat_rows)
      )
    )
  })

  # -- Reactive comparison (auto-triggers on filter change) --

  cmp_joined <- reactive({
    req(identical(input$main_tabs, "compare"))
    mode <- input$cmp_mode
    req(mode)

    # Players mode handled by cmp_pvp_ui - skip here
    if (identical(mode, "Players")) return(NULL)

    pa <- collect_side_params("a")
    pb <- collect_side_params("b")
    metric <- selected_metric()

    if (mode == "Teams") {
      req(metric %in% TEAM_METRICS)
      is_ff <- metric %in% c("off_ts_pct", "off_tov_pct", "off_oreb_pct", "off_ftr")
      if (is_ff) {
        res_a <- run_team_ff(pa)
        res_b <- run_team_ff(pb)
      } else {
        res_a <- run_team_ratings(pa)
        res_b <- run_team_ratings(pb)
      }
      res_a <- apply_side_team_filter(res_a, pa)
      res_b <- apply_side_team_filter(res_b, pb)
      if (!nrow(res_a) || !nrow(res_b)) return(NULL)

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
      joined <- dplyr::inner_join(df_a, df_b, by = c("team_id", "team_name"), suffix = c("", ".b"))
      joined$metric_a <- as.numeric(joined$metric_a)
      joined$metric_b <- as.numeric(joined$metric_b)
      joined <- apply_min_poss_filter(joined, min_poss = 10L)
      if (!nrow(joined)) return(NULL)
      joined$gap <- joined$metric_a - joined$metric_b
      joined <- joined[order(-abs(joined$gap)), ]
      joined$rank <- seq_len(nrow(joined))
      joined$entity_name <- joined$team_name
      joined

    } else if (mode == "Lineups") {
      req(metric %in% TEAM_METRICS)
      is_ff <- metric %in% c("off_ts_pct", "off_tov_pct", "off_oreb_pct", "off_ftr")
      if (is_ff) {
        res_a <- run_lineups_ff(pa)
        res_b <- run_lineups_ff(pb)
      } else {
        res_a <- run_lineups_summary(pa)
        res_b <- run_lineups_summary(pb)
      }
      res_a <- apply_side_team_filter(res_a, pa)
      res_b <- apply_side_team_filter(res_b, pb)
      if (!nrow(res_a) || !nrow(res_b)) return(NULL)

      pick_cols_lu <- function(df, suffix) {
        key <- "sub_lineup_hash"
        name_col <- if ("player_names_str" %in% names(df)) "player_names_str" else NULL
        team_col <- if ("team_name" %in% names(df)) "team_name" else NULL
        total_poss_col <- if ("total_poss" %in% names(df)) "total_poss" else NULL
        off_poss_col <- if ("off_poss" %in% names(df)) "off_poss" else NULL
        def_poss_col <- if ("def_poss" %in% names(df)) "def_poss" else NULL
        metric_col <- if (metric %in% names(df)) metric else NULL
        cols <- c(key, name_col, team_col, metric_col, total_poss_col, off_poss_col, def_poss_col)
        cols <- cols[!is.null(cols) & cols %in% names(df)]
        out <- df[, cols, drop = FALSE]
        if (!is.null(metric_col)) names(out)[names(out) == metric_col] <- paste0("metric_", suffix)
        poss_vals <- NULL
        if (!is.null(total_poss_col) && total_poss_col %in% names(out)) {
          poss_vals <- suppressWarnings(as.numeric(out[[total_poss_col]]))
        } else if (!is.null(off_poss_col) && !is.null(def_poss_col) &&
                   off_poss_col %in% names(out) && def_poss_col %in% names(out)) {
          poss_vals <- dplyr::coalesce(suppressWarnings(as.numeric(out[[off_poss_col]])), 0) +
            dplyr::coalesce(suppressWarnings(as.numeric(out[[def_poss_col]])), 0)
        } else if (!is.null(off_poss_col) && off_poss_col %in% names(out)) {
          poss_vals <- suppressWarnings(as.numeric(out[[off_poss_col]]))
        }
        if (!is.null(poss_vals)) out[[paste0("poss_", suffix)]] <- poss_vals
        out
      }

      df_a <- pick_cols_lu(res_a, "a")
      df_b <- pick_cols_lu(res_b, "b")
      join_by <- "sub_lineup_hash"
      extra <- intersect(c("player_names_str", "team_name"), intersect(names(df_a), names(df_b)))
      joined <- dplyr::inner_join(df_a, df_b, by = c(join_by, extra), suffix = c("", ".b"))
      joined$metric_a <- as.numeric(joined$metric_a)
      joined$metric_b <- as.numeric(joined$metric_b)
      joined <- apply_min_poss_filter(joined, min_poss = 10L)
      if (!nrow(joined)) return(NULL)
      joined$gap <- joined$metric_a - joined$metric_b
      joined <- joined[order(-abs(joined$gap)), ]
      joined$rank <- seq_len(nrow(joined))
      joined$entity_name <- if ("player_names_str" %in% names(joined)) joined$player_names_str else joined$sub_lineup_hash
      joined

    } else if (mode == "Players") {
      # Ensure metric is valid for Players mode before querying
      req(metric %in% PLAYER_METRICS)

      player_a_id <- input$cmp_player_a
      player_b_id <- input$cmp_player_b
      if (is.null(player_a_id) || !nzchar(player_a_id)) return(NULL)
      if (is.null(player_b_id) || !nzchar(player_b_id)) return(NULL)

      players_df <- normalize_players_ref(cmp_ref$players)
      cmp_ref$players <- players_df
      req(!is.null(players_df), nrow(players_df) > 0)
      team_ids_a <- unique(players_df$team_id[players_df$player_id == as.integer(player_a_id)])
      team_ids_b <- unique(players_df$team_id[players_df$player_id == as.integer(player_b_id)])
      if (!length(team_ids_a) || !length(team_ids_b)) return(NULL)
      team_a <- paste(team_ids_a, collapse = ",")
      team_b <- paste(team_ids_b, collapse = ",")

      res_a <- run_player_traditional(pa, team_a)
      res_b <- run_player_traditional(pb, team_b)
      if (!nrow(res_a) || !nrow(res_b)) return(NULL)

      res_a <- res_a[res_a$player_id == as.integer(player_a_id), , drop = FALSE]
      res_b <- res_b[res_b$player_id == as.integer(player_b_id), , drop = FALSE]
      if (!nrow(res_a) || !nrow(res_b)) return(NULL)

      rate_mode <- input$cmp_rate_mode %||% "Per Game"

      # SQL returns raw totals: pts, reb, ast, stl, gp, poss_on_floor, minutes
      # Percentages: fg_pct, tp_pct, ft_pct, ts
      get_player_metric <- function(row, m, rate) {
        count_map <- c("ppg" = "pts", "rpg" = "reb", "apg" = "ast", "spg" = "stl")
        pct_map <- c("fg_pct" = "fg_pct", "fg3_pct" = "tp_pct", "ft_pct" = "ft_pct", "ts_pct" = "ts")

        if (m %in% names(pct_map)) {
          cname <- pct_map[m]
          if (cname %in% names(row)) return(as.numeric(row[[cname]]))
          return(NA_real_)
        }

        cname <- count_map[m]
        if (is.null(cname) || !(cname %in% names(row))) return(NA_real_)
        total <- as.numeric(row[[cname]])

        if (rate == "Totals") return(total)
        if (rate == "Per 75 Possessions") {
          poss <- if ("poss_on_floor" %in% names(row)) as.numeric(row[["poss_on_floor"]]) else NA_real_
          if (!is.na(poss) && poss > 0) return(total / poss * 75)
          return(NA_real_)
        }
        # Per Game (default)
        gp <- if ("gp" %in% names(row)) as.numeric(row[["gp"]]) else NA_real_
        if (!is.na(gp) && gp > 0) return(total / gp)
        NA_real_
      }

      player_a_name <- players_df$name[players_df$player_id == as.integer(player_a_id)][1]
      player_b_name <- players_df$name[players_df$player_id == as.integer(player_b_id)][1]

      val_a <- get_player_metric(res_a[1, ], metric, rate_mode)
      val_b <- get_player_metric(res_b[1, ], metric, rate_mode)
      poss_a <- if ("poss_on_floor" %in% names(res_a)) as.numeric(res_a$poss_on_floor[1]) else NA_real_
      poss_b <- if ("poss_on_floor" %in% names(res_b)) as.numeric(res_b$poss_on_floor[1]) else NA_real_

      data.frame(
        rank = 1L,
        entity_name = paste0(player_a_name, " vs ", player_b_name),
        metric_a = val_a, poss_a = poss_a,
        metric_b = val_b, poss_b = poss_b,
        gap = val_a - val_b, stringsAsFactors = FALSE
      )
    } else {
      NULL
    }
  })

  # -- Detail view: fetch all metrics for selected entity --

  cmp_detail_data <- reactive({
    entity <- selected_detail_entity()
    req(entity)
    req(identical(input$main_tabs, "compare"))

    pa <- collect_side_params("a")
    pb <- collect_side_params("b")
    mode <- entity$mode

    if (mode == "Teams") {
      ratings_a <- run_team_ratings(pa)
      ratings_b <- run_team_ratings(pb)
      ff_a <- run_team_ff(pa)
      ff_b <- run_team_ff(pb)

      team_id <- entity$key
      ra <- ratings_a[ratings_a$team_id == team_id, , drop = FALSE]
      rb <- ratings_b[ratings_b$team_id == team_id, , drop = FALSE]
      fa <- ff_a[ff_a$team_id == team_id, , drop = FALSE]
      fb <- ff_b[ff_b$team_id == team_id, , drop = FALSE]

      if (!nrow(ra) && !nrow(fa)) return(NULL)
      if (!nrow(rb) && !nrow(fb)) return(NULL)

      list(
        mode = "Teams",
        entity_name = entity$name,
        ratings_a = if (nrow(ra)) ra[1, ] else NULL,
        ratings_b = if (nrow(rb)) rb[1, ] else NULL,
        ff_a = if (nrow(fa)) fa[1, ] else NULL,
        ff_b = if (nrow(fb)) fb[1, ] else NULL
      )

    } else if (mode == "Lineups") {
      summary_a <- run_lineups_summary(pa)
      summary_b <- run_lineups_summary(pb)
      ff_a <- run_lineups_ff(pa)
      ff_b <- run_lineups_ff(pb)

      hash <- entity$key
      sa <- summary_a[summary_a$sub_lineup_hash == hash, , drop = FALSE]
      sb <- summary_b[summary_b$sub_lineup_hash == hash, , drop = FALSE]
      fa <- ff_a[ff_a$sub_lineup_hash == hash, , drop = FALSE]
      fb <- ff_b[ff_b$sub_lineup_hash == hash, , drop = FALSE]

      if (!nrow(sa) && !nrow(fa)) return(NULL)
      if (!nrow(sb) && !nrow(fb)) return(NULL)

      list(
        mode = "Lineups",
        entity_name = entity$name,
        ratings_a = if (nrow(sa)) sa[1, ] else NULL,
        ratings_b = if (nrow(sb)) sb[1, ] else NULL,
        ff_a = if (nrow(fa)) fa[1, ] else NULL,
        ff_b = if (nrow(fb)) fb[1, ] else NULL
      )
    } else {
      NULL
    }
  })

  # -- Summary cards --

  metric_label <- reactive({
    m <- selected_metric()
    nms <- c(TEAM_METRICS, PLAYER_METRICS)
    names(nms)[match(m, nms)] %||% m
  })

  format_metric_raw <- function(x) {
    if (is.null(x) || !is.finite(x)) return("\u2014")
    sprintf("%.1f", x)
  }

  weighted_or_mean <- function(x, w) {
    xv <- suppressWarnings(as.numeric(x))
    wv <- suppressWarnings(as.numeric(w))
    ok_w <- is.finite(xv) & is.finite(wv) & (wv > 0)
    if (any(ok_w)) return(sum(xv[ok_w] * wv[ok_w]) / sum(wv[ok_w]))
    ok_x <- is.finite(xv)
    if (any(ok_x)) return(mean(xv[ok_x], na.rm = TRUE))
    NA_real_
  }

  cmp_summary_stats <- reactive({
    df <- cmp_joined()
    if (is.null(df) || !nrow(df)) {
      return(list(a = NA_real_, b = NA_real_, delta = NA_real_, gap_abs = NA_real_))
    }

    mode <- input$cmp_mode %||% ""
    if (identical(mode, "Lineups")) {
      a_val <- weighted_or_mean(df$metric_a, df$poss_a)
      b_val <- weighted_or_mean(df$metric_b, df$poss_b)
      delta <- a_val - b_val
      return(list(a = a_val, b = b_val, delta = delta, gap_abs = abs(delta)))
    }

    a_val <- mean(df$metric_a, na.rm = TRUE)
    b_val <- mean(df$metric_b, na.rm = TRUE)
    delta <- mean(df$gap, na.rm = TRUE)
    list(a = a_val, b = b_val, delta = delta, gap_abs = mean(abs(df$gap), na.rm = TRUE))
  })

  output$cmp_summary_a_title <- renderText({ side_summary("a") })
  output$cmp_summary_b_title <- renderText({ side_summary("b") })

  output$cmp_summary_a <- renderText({
    st <- cmp_summary_stats()
    format_metric_raw(st$a)
  })
  output$cmp_summary_a_label <- renderText({ metric_label() })
  output$cmp_summary_a_delta <- renderText({
    st <- cmp_summary_stats()
    if (!is.finite(st$delta)) return("\u0394 vs B: \u2014")
    paste0("\u0394 vs B: ", sprintf("%+.1f", st$delta))
  })
  output$cmp_summary_b <- renderText({
    st <- cmp_summary_stats()
    format_metric_raw(st$b)
  })
  output$cmp_summary_b_label <- renderText({ metric_label() })
  output$cmp_summary_b_delta <- renderText({
    st <- cmp_summary_stats()
    if (!is.finite(st$delta)) return("\u0394 vs A: \u2014")
    paste0("\u0394 vs A: ", sprintf("%+.1f", -st$delta))
  })
  output$cmp_summary_a_poss <- renderText({
    df <- cmp_joined()
    if (is.null(df) || !nrow(df) || !("poss_a" %in% names(df))) return("Poss A: \u2014")
    poss <- suppressWarnings(mean(as.numeric(df$poss_a), na.rm = TRUE))
    if (!is.finite(poss)) return("Poss A: \u2014")
    paste0("Poss A: ", format(round(poss), big.mark = ",", scientific = FALSE))
  })
  output$cmp_summary_b_poss <- renderText({
    df <- cmp_joined()
    if (is.null(df) || !nrow(df) || !("poss_b" %in% names(df))) return("Poss B: \u2014")
    poss <- suppressWarnings(mean(as.numeric(df$poss_b), na.rm = TRUE))
    if (!is.finite(poss)) return("Poss B: \u2014")
    paste0("Poss B: ", format(round(poss), big.mark = ",", scientific = FALSE))
  })
  output$cmp_summary_gap <- renderText({
    st <- cmp_summary_stats()
    if (!is.finite(st$gap_abs)) return("\u2014")
    sprintf("%.1f", st$gap_abs)
  })

  # -- Detail view: row click observer --

  observeEvent(input$cmp_table_row_click, {
    info <- input$cmp_table_row_click
    req(info$entity_name)

    mode <- input$cmp_mode
    df <- cmp_joined()
    req(df, nrow(df) > 0)

    entity_name <- info$entity_name

    if (mode == "Teams") {
      row <- df[df$team_name == entity_name, , drop = FALSE]
      if (nrow(row)) {
        selected_detail_entity(list(
          key = row$team_id[1],
          name = row$team_name[1],
          mode = "Teams"
        ))
      }
    } else if (mode == "Lineups") {
      row <- df[df$entity_name == entity_name, , drop = FALSE]
      if (nrow(row)) {
        selected_detail_entity(list(
          key = row$sub_lineup_hash[1],
          name = row$entity_name[1],
          mode = "Lineups"
        ))
      }
    }
    detail_view_active(TRUE)
  }, ignoreInit = TRUE)

  # -- Detail view: entity dropdown --

  output$cmp_detail_entity_dropdown_ui <- renderUI({
    if (!isTRUE(detail_view_active())) return(NULL)

    df <- cmp_joined()
    if (is.null(df) || !nrow(df)) return(NULL)

    choices <- setNames(df$entity_name, df$entity_name)
    current <- selected_detail_entity()
    sel <- if (!is.null(current)) current$name else ""

    tags$div(
      style = "min-width: 200px;",
      selectizeInput("cmp_detail_entity_select", NULL,
        choices = c("Select..." = "", choices),
        selected = sel,
        options = list(placeholder = "Select entity..."),
        width = "100%"
      )
    )
  })

  observeEvent(input$cmp_detail_entity_select, {
    req(nzchar(input$cmp_detail_entity_select))
    entity_name <- input$cmp_detail_entity_select
    # Guard: skip if already showing this entity (prevents loop when renderUI re-creates the dropdown)
    current <- selected_detail_entity()
    if (!is.null(current) && identical(current$name, entity_name)) return()
    mode <- input$cmp_mode
    df <- cmp_joined()
    req(df, nrow(df) > 0)

    if (mode == "Teams") {
      row <- df[df$team_name == entity_name, , drop = FALSE]
      if (nrow(row)) {
        selected_detail_entity(list(
          key = row$team_id[1],
          name = row$team_name[1],
          mode = "Teams"
        ))
      }
    } else if (mode == "Lineups") {
      row <- df[df$entity_name == entity_name, , drop = FALSE]
      if (nrow(row)) {
        selected_detail_entity(list(
          key = row$sub_lineup_hash[1],
          name = row$entity_name[1],
          mode = "Lineups"
        ))
      }
    }
  }, ignoreInit = TRUE)

  # -- Results table --

  output$cmp_table <- DT::renderDataTable({
    df <- cmp_joined()
    req(df, nrow(df) > 0)

    mode <- input$cmp_mode
    entity_label <- if (mode == "Players") "Player" else if (mode == "Lineups") "Lineup" else "Team"

    side_a_label <- side_summary("a")
    side_b_label <- side_summary("b")

    show_df <- data.frame(
      `#` = df$rank,
      Entity = df$entity_name,
      A = vapply(df$metric_a, format_metric_raw, character(1)),
      `Total Poss A` = if ("poss_a" %in% names(df)) as.integer(df$poss_a) else NA_integer_,
      B = vapply(df$metric_b, format_metric_raw, character(1)),
      `Total Poss B` = if ("poss_b" %in% names(df)) as.integer(df$poss_b) else NA_integer_,
      Gap = sprintf("%+.1f", df$gap),
      check.names = FALSE, stringsAsFactors = FALSE
    )
    names(show_df)[2] <- entity_label
    names(show_df)[3] <- side_a_label
    names(show_df)[5] <- side_b_label

    DT::datatable(
      show_df,
      callback = DT::JS("
        table.on('click', 'tbody tr', function() {
          var data = table.row(this).data();
          if (data) {
            Shiny.setInputValue('cmp_table_row_click', {
              entity_name: data[1],
              rand: Math.random()
            }, {priority: 'event'});
          }
        });
      "),
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

  # -- Reset --

  observeEvent(input$cmp_reset, {
    reset_compare_filters()
  })

  # -- Filter chips --

  output$cmp_filter_chips <- renderUI({
    tryCatch(
      build_filter_chips("cmp", input, shared$season_date_bounds, reset_btn_id = "cmp_reset"),
      error = function(e) NULL
    )
  })
}

