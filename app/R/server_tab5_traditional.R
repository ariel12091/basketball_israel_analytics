# server_tab5_traditional.R - Tab 5: Traditional Player Stats server logic

server_tab5_traditional <- function(input, output, session, shared) {

  TS_NORM_MIN_GP <- 3L
  TS_NORM_PCT <- 75
  TS_RATE_KEEP_PCT <- 0.85

  clean_ts_rows <- function(df) {
    if (is.null(df) || !nrow(df)) return(df)
    df %>%
      filter(
        !is.na(Player), nzchar(trimws(Player)),
        !is.na(team_name), nzchar(trimws(team_name))
      ) %>%
      filter(
        coalesce(gp, 0) > 0 |
          coalesce(poss_on_floor, 0) > 0 |
          coalesce(minutes, 0) > 0
      )
  }

  ts_ref <- reactiveValues(teams = NULL)

  observeEvent(list(input$main_tabs, input$ts_game_year), ignoreInit = TRUE, {
    if (!identical(input$main_tabs, "traditional_stats")) return(NULL)
    gy_int <- as.integer(input$ts_game_year)
    req(gy_int)

    teams_df <- cached_ref_query(
      key = sprintf("ts_teams_%d", gy_int),
      query_fun = function() {
        DBI::dbGetQuery(
          pg_pool,
          "SELECT DISTINCT team_id, team_name
           FROM basketball_test.full_rosters
           WHERE game_year = $1
           ORDER BY team_name",
          params = list(gy_int)
        )
      }
    )
    ts_ref$teams <- teams_df
    updateSelectizeInput(session, "ts_teams", choices = teams_df$team_name, selected = character(0), server = TRUE)
    updateSelectizeInput(session, "ts_opponents", choices = teams_df$team_name, selected = character(0), server = TRUE)

    gn_df <- cached_ref_query(
      key = sprintf("ts_gn_%d", gy_int),
      query_fun = function() {
        DBI::dbGetQuery(
          pg_pool,
          "SELECT DISTINCT gn
           FROM basketball_test.final_schedule_mv
           WHERE game_year = $1
           ORDER BY gn",
          params = list(gy_int)
        )
      }
    )
    gn_vals <- if (nrow(gn_df)) as.integer(gn_df$gn) else integer(0)
    gn_choices <- c("", as.character(gn_vals))
    last_choices <- if (length(gn_vals)) c("", as.character(seq_len(max(gn_vals, na.rm = TRUE)))) else ""
    updateSelectizeInput(session, "ts_gn_min", choices = gn_choices, selected = "")
    updateSelectizeInput(session, "ts_gn_max", choices = gn_choices, selected = "")
    updateSelectizeInput(session, "ts_last_n", choices = last_choices, selected = "")
  })

  observeEvent(input$ts_game_year, {
    b <- shared$season_date_bounds(input$ts_game_year)
    updateDateRangeInput(session, "ts_dates", start = b$start, end = b$end, min = b$start, max = b$end)
  }, ignoreInit = FALSE)

  observeEvent(input$ts_last_n, {
    if (!is.null(input$ts_last_n) && nzchar(input$ts_last_n)) {
      updateSelectizeInput(session, "ts_gn_min", selected = "")
      updateSelectizeInput(session, "ts_gn_max", selected = "")
    }
  }, ignoreInit = TRUE)

  observeEvent(list(input$ts_gn_min, input$ts_gn_max), {
    if ((nzchar(input$ts_gn_min %||% "") || nzchar(input$ts_gn_max %||% "")) &&
        nzchar(input$ts_last_n %||% "")) {
      updateSelectizeInput(session, "ts_last_n", selected = "")
    }
  }, ignoreInit = TRUE)

  observeEvent(input$ts_reset, {
    b <- shared$season_date_bounds(input$ts_game_year %||% DEFAULT_GAME_YEAR)
    updateDateRangeInput(session, "ts_dates", start = b$start, end = b$end, min = b$start, max = b$end)
    updateSelectizeInput(session, "ts_teams", selected = character(0))
    updateSelectizeInput(session, "ts_game_type", selected = "")
    updateSelectizeInput(session, "ts_opponents", selected = character(0))
    updateSelectInput(session, "ts_home_away", selected = "")
    updateSelectInput(session, "ts_outcome", selected = "")
    updateSelectInput(session, "ts_opp_rank_side", selected = "")
    updateSelectInput(session, "ts_opp_rank_n", selected = "")
    updateSelectInput(session, "ts_opp_rank_metric", selected = "")
    updateSelectInput(session, "ts_display_mode", selected = "Totals")
    updateCheckboxInput(session, "ts_show_ineligible", value = FALSE)
    updateCheckboxInput(session, "ts_clutch_enabled", value = FALSE)
    updateSliderInput(session, "ts_clutch_margin", value = 5)
    updateSelectInput(session, "ts_clutch_status", selected = "all")
    updateSliderInput(session, "ts_clutch_minutes", value = 5)
    updateCheckboxInput(session, "ts_clutch_ot_margin", value = FALSE)
    updateSelectizeInput(session, "ts_gn_min", selected = "")
    updateSelectizeInput(session, "ts_gn_max", selected = "")
    updateSelectizeInput(session, "ts_last_n", selected = "")
  })

  apply_ts_mode <- function(df, mode, x_poss = NA_real_, x_min = NA_real_) {
    if (is.null(df) || !nrow(df)) return(df)

    count_cols <- c("pts", "reb", "ast", "stl", "blk", "tov", "fgm", "fga", "3pm", "3pa", "ftm", "fta")
    mode <- mode %||% "Totals"

    if (identical(mode, "Per Game")) {
      for (col in count_cols) {
        if (col %in% names(df)) df[[col]] <- ifelse(df$gp > 0, df[[col]] / df$gp, NA_real_)
      }
      if ("poss_on_floor" %in% names(df)) df$poss_on_floor <- ifelse(df$gp > 0, df$poss_on_floor / df$gp, NA_real_)
      if ("minutes" %in% names(df)) df$minutes <- ifelse(df$gp > 0, df$minutes / df$gp, NA_real_)
      return(df)
    }

    if (identical(mode, "Per 60 Possessions")) {
      for (col in count_cols) {
        if (col %in% names(df)) df[[col]] <- ifelse(df$poss_on_floor > 0, df[[col]] / df$poss_on_floor * 60, NA_real_)
      }
      if ("minutes" %in% names(df)) df$minutes <- ifelse(df$poss_on_floor > 0, df$minutes / df$poss_on_floor * 60, NA_real_)
      if ("poss_on_floor" %in% names(df)) df$poss_on_floor <- ifelse(df$poss_on_floor > 0, df$poss_on_floor / df$poss_on_floor * 60, NA_real_)
      return(df)
    }

    if (identical(mode, "Per 30 Minutes")) {
      for (col in count_cols) {
        if (col %in% names(df)) df[[col]] <- ifelse(df$minutes > 0, df[[col]] / df$minutes * 30, NA_real_)
      }
      if ("poss_on_floor" %in% names(df)) df$poss_on_floor <- ifelse(df$minutes > 0, df$poss_on_floor / df$minutes * 30, NA_real_)
      return(df)
    }

    if (identical(mode, "Per X Possessions")) {
      if (!is.finite(x_poss) || x_poss <= 0) return(df)
      for (col in count_cols) {
        if (col %in% names(df)) df[[col]] <- ifelse(df$poss_on_floor > 0, df[[col]] / df$poss_on_floor * x_poss, NA_real_)
      }
      if ("minutes" %in% names(df)) df$minutes <- ifelse(df$poss_on_floor > 0, df$minutes / df$poss_on_floor * x_poss, NA_real_)
      if ("poss_on_floor" %in% names(df)) df$poss_on_floor <- ifelse(df$poss_on_floor > 0, df$poss_on_floor / df$poss_on_floor * x_poss, NA_real_)
      return(df)
    }

    if (identical(mode, "Per X Minutes")) {
      if (!is.finite(x_min) || x_min <= 0) return(df)
      for (col in count_cols) {
        if (col %in% names(df)) df[[col]] <- ifelse(df$minutes > 0, df[[col]] / df$minutes * x_min, NA_real_)
      }
      if ("poss_on_floor" %in% names(df)) df$poss_on_floor <- ifelse(df$minutes > 0, df$poss_on_floor / df$minutes * x_min, NA_real_)
      return(df)
    }

    df
  }

  debounced_range <- reactive(input$ts_dates) %>% debounce(300)
  debounced_teams <- reactive(input$ts_teams) %>% debounce(300)
  debounced_ts_filters <- reactive(list(
    game_type = input$ts_game_type,
    opp_names = input$ts_opponents,
    home_away = input$ts_home_away,
    outcome = input$ts_outcome,
    rank_side = input$ts_opp_rank_side,
    rank_n = input$ts_opp_rank_n,
    metric = input$ts_opp_rank_metric,
    clutch_enabled = input$ts_clutch_enabled,
    clutch_margin = input$ts_clutch_margin,
    clutch_status = input$ts_clutch_status,
    clutch_minutes = input$ts_clutch_minutes,
    clutch_ot_margin = input$ts_clutch_ot_margin
  )) %>% debounce(300)

  gn_params <- reactive({
    min_gn <- if (!is.null(input$ts_gn_min) && nzchar(input$ts_gn_min)) as.integer(input$ts_gn_min) else NA_integer_
    max_gn <- if (!is.null(input$ts_gn_max) && nzchar(input$ts_gn_max)) as.integer(input$ts_gn_max) else NA_integer_
    last_n <- if (!is.null(input$ts_last_n) && nzchar(input$ts_last_n)) as.integer(input$ts_last_n) else NA_integer_
    if (!is.na(last_n)) {
      min_gn <- NA_integer_
      max_gn <- NA_integer_
    }
    if (!is.na(min_gn) || !is.na(max_gn)) {
      last_n <- NA_integer_
    }
    if (!is.na(min_gn) && !is.na(max_gn) && min_gn > max_gn) {
      tmp <- min_gn
      min_gn <- max_gn
      max_gn <- tmp
    }
    list(min_gn = min_gn, max_gn = max_gn, last_n = last_n)
  }) %>% debounce(150)

  selected_team_ids <- reactive({
    td <- ts_ref$teams
    teams_in <- debounced_teams()
    if (is.null(td) || !nrow(td) || is.null(teams_in) || !length(teams_in)) return(NULL)
    td %>% filter(team_name %in% teams_in) %>% pull(team_id)
  })

  selected_opp_ids <- reactive({
    td <- ts_ref$teams
    opp_names <- debounced_ts_filters()$opp_names
    if (is.null(td) || !nrow(td) || is.null(opp_names) || !length(opp_names)) return(NULL)
    td %>% filter(team_name %in% opp_names) %>% pull(team_id)
  })

  fallback_needed <- reactive({
    rng <- debounced_range()
    if (is.null(rng)) return(FALSE)

    start_d <- as.Date(rng[1])
    end_d <- as.Date(rng[2])
    if (is.na(start_d) || is.na(end_d)) return(FALSE)

    gy <- as.integer(input$ts_game_year)
    season_bounds <- shared$season_date_bounds(gy)
    date_changed <- (start_d != season_bounds$start) || (end_d != season_bounds$end)

    f <- debounced_ts_filters()
    extra_filters <- (!is.null(f$game_type) && any(nzchar(f$game_type))) ||
      (!is.null(f$opp_names) && length(f$opp_names) > 0) ||
      nzchar(f$home_away %||% "") ||
      nzchar(f$outcome %||% "") ||
      nzchar(f$rank_side %||% "") ||
      isTRUE(f$clutch_enabled)

    gp <- gn_params()
    gn_active <- !is.na(gp$min_gn) || !is.na(gp$max_gn) || !is.na(gp$last_n)
    gn_raw_active <- nzchar(input$ts_gn_min %||% "") ||
      nzchar(input$ts_gn_max %||% "") ||
      nzchar(input$ts_last_n %||% "")

    date_changed || extra_filters || gn_active || gn_raw_active
  })

  mv_result_df <- reactive({
    req(identical(input$main_tabs, "traditional_stats"))
    gy_int <- as.integer(input$ts_game_year)
    req(gy_int)

    out <- tryCatch(
      DBI::dbGetQuery(
        pg_pool,
        "SELECT player_id, team_id, team_name, player_name AS \"Player\",
                gp, poss_on_floor, minutes,
                pts, reb, ast, stl, blk, tov, fgm, fga, \"3pm\", \"3pa\", ftm, fta,
                fg_pct, tp_pct, ft_pct, efg, ts
         FROM basketball_test.player_traditional_stats_mv
         WHERE game_year = $1",
        params = list(gy_int)
      ),
      error = function(e) NULL
    )
    if (is.null(out)) return(NULL)

    sel_names <- debounced_teams()
    if (!is.null(sel_names) && length(sel_names) > 0) {
      out <- out %>% filter(team_name %in% sel_names)
    }

    out %>%
      clean_ts_rows() %>%
      arrange(desc(pts), desc(minutes), team_name, Player)
  }) %>% bindEvent(input$main_tabs, input$ts_game_year, debounced_teams())

  live_result_df <- reactive({
    req(identical(input$main_tabs, "traditional_stats"))

    gy_int <- as.integer(input$ts_game_year)
    req(gy_int)
    rng <- debounced_range()
    req(rng)
    req(!is.na(rng[1]), !is.na(rng[2]))

    f <- debounced_ts_filters()
    tids <- selected_team_ids()
    opp_ids <- selected_opp_ids()
    gp <- gn_params()
    clutch_enabled <- isTRUE(f$clutch_enabled)
    max_margin <- if (clutch_enabled) suppressWarnings(as.integer(f$clutch_margin)) else NA_integer_
    margin_status <- if (clutch_enabled) (f$clutch_status %||% "all") else NA_character_
    max_time_remaining <- if (clutch_enabled) suppressWarnings(as.integer(f$clutch_minutes)) * 60L else NA_integer_
    ot_margin_filter <- if (clutch_enabled) isTRUE(f$clutch_ot_margin) else FALSE

    sched <- DBI::dbGetQuery(
      pg_pool,
      "SELECT game_id, game_date, team_id, team_name, opp_team_id, opp_team_name,
              game_type, is_home, has_won, gn
       FROM basketball_test.final_schedule_mv
       WHERE game_year = $1",
      params = list(gy_int)
    )
    if (!nrow(sched)) return(NULL)

    start_d <- as.Date(rng[1])
    end_d <- as.Date(rng[2])
    sched <- sched %>% filter(game_date >= !!start_d, game_date <= !!end_d)

    if (!is.null(tids) && length(tids) > 0) {
      sched <- sched %>% filter(team_id %in% tids)
    }

    if (!is.null(f$game_type) && any(nzchar(f$game_type))) {
      gt_vals <- as.integer(f$game_type[nzchar(f$game_type)])
      sched <- sched %>% filter(game_type %in% gt_vals)
    }

    if (!is.null(opp_ids) && length(opp_ids) > 0) {
      sched <- sched %>% filter(opp_team_id %in% opp_ids)
    }

    ha <- f$home_away %||% ""
    if (nzchar(ha)) {
      sched <- if (identical(ha, "home")) sched %>% filter(is_home == TRUE) else sched %>% filter(is_home == FALSE)
    }

    outcome <- f$outcome %||% ""
    if (nzchar(outcome)) {
      sched <- if (identical(outcome, "win")) sched %>% filter(has_won == TRUE) else sched %>% filter(has_won == FALSE)
    }

    rank_side <- f$rank_side %||% ""
    rank_n <- suppressWarnings(as.integer(if (!nzchar(f$rank_n %||% "")) NA_character_ else f$rank_n))
    rank_metric <- f$metric %||% ""
    if (nzchar(rank_side) && !is.na(rank_n) && nzchar(rank_metric)) {
      team_ranks <- DBI::dbGetQuery(
        pg_pool,
        "SELECT team_id,
                rank() OVER (ORDER BY off_ppp DESC) AS rank_off,
                rank() OVER (ORDER BY def_ppp ASC) AS rank_def,
                rank() OVER (ORDER BY net_rtg DESC) AS rank_net
         FROM basketball_test.team_ppp_ratings_mv
         WHERE game_year = $1",
        params = list(gy_int)
      )
      if (nrow(team_ranks)) {
        rank_col <- if (rank_metric == "off") "rank_off" else if (rank_metric == "def") "rank_def" else "rank_net"
        opp_keep <- if (rank_side == "top") {
          team_ranks %>% filter(.data[[rank_col]] <= rank_n) %>% pull(team_id)
        } else {
          max_rank <- suppressWarnings(max(team_ranks[[rank_col]], na.rm = TRUE))
          team_ranks %>% filter(.data[[rank_col]] >= (max_rank - rank_n + 1)) %>% pull(team_id)
        }
        sched <- sched %>% filter(opp_team_id %in% opp_keep)
      }
    }

    if (!is.na(gp$min_gn)) sched <- sched %>% filter(gn >= gp$min_gn)
    if (!is.na(gp$max_gn)) sched <- sched %>% filter(gn <= gp$max_gn)
    if (!is.na(gp$last_n)) {
      sched <- sched %>%
        group_by(team_id) %>%
        arrange(desc(game_date), desc(game_id), .by_group = TRUE) %>%
        mutate(rn_recent = row_number()) %>%
        ungroup() %>%
        filter(rn_recent <= gp$last_n) %>%
        select(-rn_recent)
    }

    if (!nrow(sched)) return(NULL)

    sched_pairs <- sched %>% select(game_id, team_id) %>% distinct()
    if (!nrow(sched_pairs)) return(NULL)
    game_ids_csv <- paste(sched_pairs$game_id, collapse = ",")
    team_ids_csv <- paste(sched_pairs$team_id, collapse = ",")

    acts <- DBI::dbGetQuery(
      pg_pool,
      "WITH pairs AS (
         SELECT p.game_id, p.team_id
         FROM unnest(
           string_to_array($1, ',')::int8[],
           string_to_array($2, ',')::int4[]
         ) AS p(game_id, team_id)
       )
       SELECT d.id, d.game_id, d.team_id, d.lineup_hash, d.segment_id, d.end_game_seconds_remaining,
              d.type, d.parameters_type, d.parameters_made, d.parameters_points, d.player_id,
              d.type_lineup, d.final_end_poss, d.final_end_id, d.quarter, d.own_team_score, d.opp_team_score
       FROM basketball_test.df_pts_poss_lineups_longer_mv d
       JOIN pairs p
         ON p.game_id = d.game_id
        AND p.team_id = d.team_id",
      params = list(game_ids_csv, team_ids_csv)
    )
    if (clutch_enabled && nrow(acts)) {
      acts <- acts %>%
        mutate(
          quarter = suppressWarnings(as.integer(quarter)),
          margin_abs = abs(coalesce(own_team_score, 0) - coalesce(opp_team_score, 0)),
          score_diff = coalesce(own_team_score, 0) - coalesce(opp_team_score, 0)
        )

      if (!is.na(max_margin)) {
        acts <- acts %>%
          filter(margin_abs <= max_margin | (quarter > 4 & !ot_margin_filter))
      }

      if (!is.na(margin_status) && !identical(margin_status, "all")) {
        if (identical(margin_status, "leading")) {
          acts <- acts %>% filter(score_diff > 0 | (quarter > 4 & !ot_margin_filter))
        } else if (identical(margin_status, "trailing")) {
          acts <- acts %>% filter(score_diff < 0 | (quarter > 4 & !ot_margin_filter))
        } else if (identical(margin_status, "tied")) {
          acts <- acts %>% filter(score_diff == 0 | (quarter > 4 & !ot_margin_filter))
        }
      }

      if (!is.na(max_time_remaining)) {
        acts <- acts %>%
          filter(end_game_seconds_remaining <= max_time_remaining | quarter > 4)
      }
    }
    if (!nrow(acts)) return(NULL)

    lineup_map <- DBI::dbGetQuery(
      pg_pool,
      "WITH pairs AS (
         SELECT p.game_id, p.team_id
         FROM unnest(
           string_to_array($2, ',')::int8[],
           string_to_array($3, ',')::int4[]
         ) AS p(game_id, team_id)
       )
       SELECT DISTINCT ll.game_id, ll.team_id, ll.lineup_hash, ll.player_id
       FROM basketball_test.lineups_lookup ll
       JOIN pairs p
         ON p.game_id = ll.game_id
        AND p.team_id = ll.team_id
       WHERE ll.game_year = $1
         AND COALESCE(ll.is_on_verdict, 0)::int = 1",
      params = list(gy_int, game_ids_csv, team_ids_csv)
    )
    if (!nrow(lineup_map)) return(NULL)

    poss_end <- acts %>%
      filter(
        type_lineup == "offense",
        final_end_poss,
        !is.na(id),
        !is.na(lineup_hash)
      ) %>%
      distinct(game_id, team_id, lineup_hash, poss_end_id = id)

    player_usage <- poss_end %>%
      inner_join(
        lineup_map %>% rename(on_floor_player_id = player_id),
        by = c("game_id", "team_id", "lineup_hash"),
        relationship = "many-to-many"
      ) %>%
      group_by(player_id = on_floor_player_id, team_id) %>%
      summarise(
        gp = n_distinct(game_id),
        poss_on_floor = n_distinct(game_id, team_id, poss_end_id),
        .groups = "drop"
      )

    seg_times <- acts %>%
      filter(!is.na(lineup_hash), !is.na(segment_id), !is.na(end_game_seconds_remaining)) %>%
      group_by(game_id, team_id, lineup_hash, segment_id) %>%
      summarise(seg_seconds = max(end_game_seconds_remaining, na.rm = TRUE) - min(end_game_seconds_remaining, na.rm = TRUE), .groups = "drop")

    player_minutes <- seg_times %>%
      inner_join(
        lineup_map,
        by = c("game_id", "team_id", "lineup_hash"),
        relationship = "many-to-many"
      ) %>%
      group_by(player_id, team_id) %>%
      summarise(minutes = round(sum(seg_seconds, na.rm = TRUE) / 60, 1), .groups = "drop")

    stats <- acts %>%
      filter(!is.na(player_id), player_id > 0) %>%
      group_by(player_id, team_id) %>%
      summarise(
        pts = sum(ifelse(type == "shot" & parameters_made == "made" & type_lineup == "offense", coalesce(parameters_points, 0), 0), na.rm = TRUE) +
          sum(ifelse(type == "freeThrow" & parameters_made == "made" & type_lineup == "offense", 1, 0), na.rm = TRUE),
        reb = sum(ifelse(type == "rebound" & type_lineup == "offense", 1, 0), na.rm = TRUE),
        ast = sum(ifelse(type == "assist" & type_lineup == "offense", 1, 0), na.rm = TRUE),
        stl = sum(ifelse(type == "steal" & type_lineup == "offense", 1, 0), na.rm = TRUE),
        blk = sum(ifelse(type == "block" & type_lineup == "offense", 1, 0), na.rm = TRUE),
        tov = sum(ifelse(type == "turnover" & type_lineup == "offense", 1, 0), na.rm = TRUE),
        fgm = sum(ifelse(type == "shot" & parameters_made == "made" & type_lineup == "offense", 1, 0), na.rm = TRUE),
        fga = sum(ifelse(type == "shot" & type_lineup == "offense", 1, 0), na.rm = TRUE),
        `3pm` = sum(ifelse(type == "shot" & parameters_made == "made" & parameters_points == 3 & type_lineup == "offense", 1, 0), na.rm = TRUE),
        `3pa` = sum(ifelse(type == "shot" & parameters_points == 3 & type_lineup == "offense", 1, 0), na.rm = TRUE),
        ftm = sum(ifelse(type == "freeThrow" & parameters_made == "made" & type_lineup == "offense", 1, 0), na.rm = TRUE),
        fta = sum(ifelse(type == "freeThrow" & type_lineup == "offense", 1, 0), na.rm = TRUE),
        .groups = "drop"
      ) %>%
      mutate(
        fg_pct = ifelse(fga > 0, round(fgm / fga * 100, 1), NA_real_),
        tp_pct = ifelse(`3pa` > 0, round(`3pm` / `3pa` * 100, 1), NA_real_),
        ft_pct = ifelse(fta > 0, round(ftm / fta * 100, 1), NA_real_),
        efg = ifelse(fga > 0, round((fgm + 0.5 * `3pm`) / fga * 100, 1), NA_real_),
        ts = ifelse((fga + 0.44 * fta) > 0, round(pts / (2 * (fga + 0.44 * fta)) * 100, 1), NA_real_)
      )

    names_df <- DBI::dbGetQuery(
      pg_pool,
      "SELECT DISTINCT player_id, team_id, team_name, firstname, lastname
       FROM basketball_test.full_rosters
       WHERE game_year = $1",
      params = list(gy_int)
    ) %>%
      mutate(Player = paste(firstname, lastname))

    stats %>%
      left_join(player_minutes, by = c("player_id", "team_id")) %>%
      left_join(player_usage, by = c("player_id", "team_id")) %>%
      left_join(names_df %>% select(player_id, team_id, team_name, Player), by = c("player_id", "team_id")) %>%
      mutate(
        minutes = coalesce(minutes, 0),
        gp = coalesce(gp, 0L),
        poss_on_floor = coalesce(poss_on_floor, 0L)
      ) %>%
      clean_ts_rows() %>%
      arrange(desc(pts), desc(minutes), team_name, Player)
    }) %>% bindEvent(
    input$main_tabs,
    input$ts_game_year,
    debounced_range(),
    debounced_teams(),
    debounced_ts_filters(),
    gn_params()
  )

  result_df <- reactive({
    req(identical(input$main_tabs, "traditional_stats"))
    if (!isTRUE(fallback_needed())) {
      mv_df <- mv_result_df()
      if (!is.null(mv_df)) return(mv_df)
    }
    live_result_df()
  }) %>% bindEvent(
    input$main_tabs,
    input$ts_game_year,
    debounced_range(),
    debounced_teams(),
    debounced_ts_filters(),
    gn_params()
  )

  ts_mode_context <- reactive({
    base_df <- result_df()
    if (is.null(base_df) || !nrow(base_df)) {
      return(list(df = base_df, x_poss = NA_real_, x_min = NA_real_, rate_threshold = 0))
    }

    min_gp <- suppressWarnings(as.integer(TS_NORM_MIN_GP))
    if (!is.finite(min_gp) || min_gp < 1) min_gp <- 1L
    pct <- suppressWarnings(as.numeric(TS_NORM_PCT))
    if (!is.finite(pct)) pct <- 75
    pct <- max(70, min(90, pct))

    df0 <- base_df %>%
      mutate(
        poss_pg = ifelse(gp > 0, poss_on_floor / gp, NA_real_),
        min_pg = ifelse(gp > 0, minutes / gp, NA_real_)
      )

    eligible <- df0 %>%
      filter(gp >= min_gp, !is.na(poss_pg), !is.na(min_pg), poss_pg > 0, min_pg > 0)

    if (!nrow(eligible)) {
      eligible <- df0 %>%
        filter(gp > 0, !is.na(poss_pg), !is.na(min_pg), poss_pg > 0, min_pg > 0)
    }

    x_poss <- if (nrow(eligible)) as.numeric(stats::quantile(eligible$poss_pg, probs = pct / 100, na.rm = TRUE, type = 7)) else NA_real_
    x_min <- if (nrow(eligible)) as.numeric(stats::quantile(eligible$min_pg, probs = pct / 100, na.rm = TRUE, type = 7)) else NA_real_
    poss_vec <- base_df$poss_on_floor
    poss_vec <- poss_vec[is.finite(poss_vec) & poss_vec > 0]
    rate_threshold <- if (length(poss_vec)) {
      as.numeric(stats::quantile(poss_vec, probs = 1 - TS_RATE_KEEP_PCT, na.rm = TRUE, type = 7))
    } else 0

    list(
      df = base_df,
      x_poss = x_poss,
      x_min = x_min,
      rate_threshold = rate_threshold
    )
  }) %>% bindEvent(result_df())

  ts_display_context <- reactive({
    ctx <- ts_mode_context()
    df <- ctx$df
    mode <- input$ts_display_mode %||% "Totals"
    show_ineligible <- isTRUE(input$ts_show_ineligible)
    poss_threshold <- as.numeric(ctx$rate_threshold %||% 0)
    if (is.null(df) || !nrow(df)) {
      return(list(df = df, mode = mode, removed = 0L, ineligible = 0L, threshold = poss_threshold, show_ineligible = show_ineligible))
    }

    removed <- 0L
    ineligible <- 0L
    df$rate_eligible <- TRUE
    if (identical(mode, "Per 60 Possessions") || identical(mode, "Per 30 Minutes")) {
      keep <- !is.na(df$poss_on_floor) & df$poss_on_floor >= poss_threshold
      ineligible <- sum(!keep, na.rm = TRUE)
      df$rate_eligible <- keep
      if (!show_ineligible) {
        removed <- ineligible
        df <- df[keep, , drop = FALSE]
      }
    }

    df <- apply_ts_mode(df, mode, x_poss = ctx$x_poss, x_min = ctx$x_min)

    list(df = df, mode = mode, removed = removed, ineligible = ineligible, threshold = poss_threshold, show_ineligible = show_ineligible)
  }) %>% bindEvent(ts_mode_context(), input$ts_display_mode, input$ts_show_ineligible)

  output$ts_mode_warning <- renderUI({
    disp_ctx <- ts_display_context()
    req(!is.null(disp_ctx$mode))
    if (!(identical(disp_ctx$mode, "Per 60 Possessions") || identical(disp_ctx$mode, "Per 30 Minutes"))) return(NULL)
    if (isTRUE(disp_ctx$show_ineligible)) {
      if (!isTRUE(disp_ctx$ineligible > 0)) return(NULL)
      return(
        div(
          class = "alert alert-info py-2 mb-2",
          sprintf(
            "%d non-eligible players shown in gray in %s (below %s total possessions).",
            as.integer(disp_ctx$ineligible),
            disp_ctx$mode,
            format(as.integer(disp_ctx$threshold), big.mark = ",")
          )
        )
      )
    }
    if (!isTRUE(disp_ctx$removed > 0)) return(NULL)
    div(class = "alert alert-warning py-2 mb-2",
        sprintf("%d players hidden in %s (below %s total possessions).",
                as.integer(disp_ctx$removed), disp_ctx$mode, format(as.integer(disp_ctx$threshold), big.mark = ",")))
  }) %>% bindEvent(ts_display_context(), input$main_tabs)

  output$ts_table <- DT::renderDataTable({
    req(identical(input$main_tabs, "traditional_stats"))
    disp_ctx <- ts_display_context()
    df <- disp_ctx$df
    if (is.null(df) || nrow(df) == 0) return(NULL)
    mode <- disp_ctx$mode

    disp <- df %>%
      transmute(
        Team = team_name,
        Player,
        GP = gp,
        `Poss On Floor` = poss_on_floor,
        Min = minutes,
        PTS = pts,
        REB = reb,
        AST = ast,
        STL = stl,
        BLK = blk,
        TOV = tov,
        FGM = fgm,
        FGA = fga,
        `FG%` = fg_pct,
        `3PM` = `3pm`,
        `3PA` = `3pa`,
        `3P%` = tp_pct,
        FTM = ftm,
        FTA = fta,
        `FT%` = ft_pct,
        `eFG%` = efg,
        `TS%` = ts,
        `.eligible_rate` = coalesce(rate_eligible, TRUE)
      )

    order_col <- which(grepl("^PTS", names(disp)))
    if (!length(order_col)) order_col <- 6L
    round_cols <- setdiff(names(disp), c("Team", "Player", "GP", ".eligible_rate"))
    style_cols <- setdiff(names(disp), ".eligible_rate")

    DT::datatable(
      disp,
      rownames = FALSE,
      options = list(
        dom = "tip",
        pageLength = 50,
        deferRender = TRUE,
        scrollX = TRUE,
        scrollY = "70vh",
        scrollCollapse = TRUE,
        order = list(list(order_col - 1L, "desc")),
        columnDefs = list(
          list(className = "dt-center", targets = "_all"),
          list(visible = FALSE, targets = which(names(disp) == ".eligible_rate") - 1L)
        )
      )
    ) %>%
      DT::formatRound(intersect(round_cols, names(disp)), 1) %>%
      DT::formatStyle(
        columns = style_cols,
        valueColumns = ".eligible_rate",
        color = DT::styleEqual(c(TRUE, FALSE), c("inherit", "#9aa0a6")),
        backgroundColor = DT::styleEqual(c(TRUE, FALSE), c(NA, "#f8f9fb"))
      )
  }) %>% bindEvent(ts_display_context(), input$main_tabs)

  output$ts_download_csv <- downloadHandler(
    filename = function() sprintf("traditional_player_stats_%s.csv", Sys.Date()),
    content = function(file) {
      disp_ctx <- ts_display_context()
      df <- disp_ctx$df
      if (is.null(df) || !nrow(df)) {
        write.csv(data.frame(), file, row.names = FALSE)
        return()
      }
      out <- df %>%
        transmute(
          team = team_name,
          player = Player,
          gp = gp,
          poss_on_floor = poss_on_floor,
          minutes = minutes,
          pts = pts,
          reb = reb,
          ast = ast,
          stl = stl,
          blk = blk,
          tov = tov,
          fgm = fgm,
          fga = fga,
          x3pm = `3pm`,
          x3pa = `3pa`,
          ftm = ftm,
          fta = fta,
          fg_pct = fg_pct,
          tp_pct = tp_pct,
          ft_pct = ft_pct,
          efg = efg,
          ts = ts
        )
      write.csv(out, file, row.names = FALSE)
    }
  )
}

