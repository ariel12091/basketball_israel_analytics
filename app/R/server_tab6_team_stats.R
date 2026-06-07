# server_tab6_team_stats.R - Tab 6: Traditional Team Stats server logic

server_tab6_team_stats <- function(input, output, session, shared) {

  apply_tst_mode <- function(df, mode) {
    if (is.null(df) || !nrow(df)) return(df)
    count_cols <- c("pts", "reb", "ast", "stl", "blk", "tov", "fgm", "fga", "3pm", "3pa", "ftm", "fta")
    mode <- mode %||% "Per Game"

    if (identical(mode, "Per Game")) {
      for (col in count_cols) if (col %in% names(df)) df[[col]] <- ifelse(df$gp > 0, df[[col]] / df$gp, NA_real_)
      if ("poss_on_floor" %in% names(df)) df$poss_on_floor <- ifelse(df$gp > 0, df$poss_on_floor / df$gp, NA_real_)
      if ("minutes" %in% names(df)) df$minutes <- ifelse(df$gp > 0, df$minutes / df$gp, NA_real_)
      return(df)
    }

    if (identical(mode, "Per 100 Possessions")) {
      for (col in count_cols) if (col %in% names(df)) df[[col]] <- ifelse(df$poss_on_floor > 0, df[[col]] / df$poss_on_floor * 100, NA_real_)
      if ("minutes" %in% names(df)) df$minutes <- ifelse(df$poss_on_floor > 0, df$minutes / df$poss_on_floor * 100, NA_real_)
      if ("poss_on_floor" %in% names(df)) df$poss_on_floor <- ifelse(df$poss_on_floor > 0, 100, NA_real_)
      return(df)
    }

    if (identical(mode, "Per 40 Minutes")) {
      for (col in count_cols) if (col %in% names(df)) df[[col]] <- ifelse(df$minutes > 0, df[[col]] / df$minutes * 40, NA_real_)
      if ("poss_on_floor" %in% names(df)) df$poss_on_floor <- ifelse(df$minutes > 0, df$poss_on_floor / df$minutes * 40, NA_real_)
      return(df)
    }

    df
  }

  run_team_traditional_dynamic <- function(pool, game_year, start_d, end_d,
                                           team_ids_csv, game_type_csv, opp_ids_csv,
                                           home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric,
                                           max_margin, margin_status, max_time_remaining, ot_margin_filter,
                                           min_gn, max_gn, last_n_games) {
    db_get_query(
      pool,
      "WITH params AS (
         SELECT
           CASE WHEN $4::text IS NULL OR btrim($4::text) = '' THEN NULL::int4[]
                ELSE string_to_array(regexp_replace($4::text, '\\s+', '', 'g'), ',')::int4[] END AS team_ids,
           CASE WHEN $5::text IS NULL OR btrim($5::text) = '' THEN NULL::int4[]
                ELSE string_to_array(regexp_replace($5::text, '\\s+', '', 'g'), ',')::int4[] END AS game_types,
           CASE WHEN $6::text IS NULL OR btrim($6::text) = '' THEN NULL::int4[]
                ELSE string_to_array(regexp_replace($6::text, '\\s+', '', 'g'), ',')::int4[] END AS opp_ids
       ),
       games_base AS (
         SELECT
           fs.game_id,
           fs.team_id,
           fs.game_year,
           fs.opp_team_id,
           fs.game_date,
           ROW_NUMBER() OVER (
             PARTITION BY fs.team_id, fs.game_year
             ORDER BY fs.game_date DESC NULLS LAST, fs.game_id DESC
           ) AS rn_recent
         FROM basketball_test.final_schedule_mv fs
         CROSS JOIN params p0
         WHERE fs.game_year = $1::int4
           AND ($2::date IS NULL OR fs.game_date >= $2::date)
           AND ($3::date IS NULL OR fs.game_date <= $3::date)
           AND (p0.team_ids IS NULL OR fs.team_id = ANY(p0.team_ids))
           AND (p0.game_types IS NULL OR fs.game_type = ANY(p0.game_types))
           AND (p0.opp_ids IS NULL OR fs.opp_team_id = ANY(p0.opp_ids))
           AND ($7::text IS NULL OR $7::text = '' OR ($7::text = 'home' AND fs.is_home) OR ($7::text = 'away' AND NOT fs.is_home))
           AND ($8::text IS NULL OR $8::text = '' OR ($8::text = 'win' AND fs.has_won IS TRUE) OR ($8::text = 'loss' AND fs.has_won IS FALSE))
           AND ($16::int4 IS NULL OR fs.gn >= $16::int4)
           AND ($17::int4 IS NULL OR fs.gn <= $17::int4)
       ),
       games_last_n AS (
         SELECT *
         FROM games_base
         WHERE ($18::int4 IS NULL OR rn_recent <= $18::int4)
       ),
       games_ranked AS (
         SELECT
           gb.game_id,
           gb.team_id,
           gb.game_year,
           CASE
             WHEN $9::text IN ('top','bottom') THEN
               CASE COALESCE($11::text, 'net')
                 WHEN 'off' THEN r.rank_off_ppp
                 WHEN 'def' THEN r.rank_def_ppp
                 ELSE r.rank_net_rtg
               END
             ELSE NULL
           END AS opp_rank,
           CASE
             WHEN $9::text = 'bottom' THEN
               MAX(
                 CASE COALESCE($11::text, 'net')
                   WHEN 'off' THEN r.rank_off_ppp
                   WHEN 'def' THEN r.rank_def_ppp
                   ELSE r.rank_net_rtg
                 END
               ) OVER (PARTITION BY gb.game_year)
             ELSE NULL
           END AS max_rank
         FROM games_last_n gb
         LEFT JOIN basketball_test.team_ppp_ratings_mv r
           ON r.game_year::int4 = gb.game_year
          AND r.team_id::int4 = gb.opp_team_id
          AND $9::text IN ('top','bottom')
       ),
       games_filtered AS (
         SELECT gr.game_id, gr.team_id
         FROM games_ranked gr
         WHERE $9::text IS NULL OR $9::text = '' OR $10::int4 IS NULL
            OR ($9::text = 'top' AND gr.opp_rank <= $10::int4)
            OR ($9::text = 'bottom' AND gr.opp_rank >= (gr.max_rank - $10::int4 + 1))
       ),
       acts AS (
         SELECT
           d.id,
           d.game_id,
           d.team_id,
           d.lineup_hash,
           d.segment_id,
           d.end_game_seconds_remaining,
            d.type,
            d.event_owner_side,
            d.parameters_type,
            d.parameters_made,
            d.parameters_points,
            d.type_lineup,
           d.final_end_poss,
           d.quarter,
           d.own_team_score,
           d.opp_team_score
         FROM basketball_test.df_pts_poss_lineups_longer_mv d
         JOIN games_filtered gf
           ON gf.game_id = d.game_id
          AND gf.team_id = d.team_id
         WHERE (
           $12::int4 IS NULL
           OR ABS(COALESCE(d.own_team_score, 0) - COALESCE(d.opp_team_score, 0)) <= $12::int4
           OR (d.quarter > 4 AND NOT COALESCE($15::bool, FALSE))
         )
         AND (
           $13::text IS NULL OR $13::text = '' OR $13::text = 'all'
           OR ($13::text = 'leading' AND COALESCE(d.own_team_score, 0) > COALESCE(d.opp_team_score, 0))
           OR ($13::text = 'trailing' AND COALESCE(d.own_team_score, 0) < COALESCE(d.opp_team_score, 0))
           OR ($13::text = 'tied' AND COALESCE(d.own_team_score, 0) = COALESCE(d.opp_team_score, 0))
           OR (d.quarter > 4 AND NOT COALESCE($15::bool, FALSE))
         )
         AND (
           $14::int4 IS NULL
           OR d.end_game_seconds_remaining <= $14::int4
           OR d.quarter > 4
         )
       ),
       team_stats AS (
         SELECT
           a.team_id,
           (
              SUM(CASE WHEN a.type = 'shot' AND a.parameters_made = 'made' AND a.type_lineup = 'offense'
                       THEN COALESCE(a.parameters_points, 0) ELSE 0 END)
              + SUM(CASE WHEN a.type = 'freeThrow' AND a.parameters_made = 'made' AND a.type_lineup = 'offense'
                         THEN 1 ELSE 0 END)
            )::int AS pts,
            SUM(CASE WHEN a.type = 'rebound' AND (
                     (a.type_lineup = 'offense' AND a.parameters_type = 'offensive') OR
                     (a.type_lineup = 'defense' AND a.parameters_type = 'defensive')
                   ) THEN 1 ELSE 0 END)::int AS reb,
            SUM(CASE WHEN a.type = 'assist'  AND a.type_lineup = 'offense' THEN 1 ELSE 0 END)::int AS ast,
            SUM(CASE WHEN a.type = 'steal'   AND a.type_lineup = 'defense' THEN 1 ELSE 0 END)::int AS stl,
            SUM(CASE WHEN a.type = 'block'   AND a.type_lineup = 'defense' THEN 1 ELSE 0 END)::int AS blk,
            SUM(CASE WHEN a.type = 'turnover'AND a.type_lineup = 'offense' THEN 1 ELSE 0 END)::int AS tov,
            SUM(CASE WHEN a.type = 'shot' AND a.parameters_made = 'made' AND a.type_lineup = 'offense' THEN 1 ELSE 0 END)::int AS fgm,
            SUM(CASE WHEN a.type = 'shot' AND a.type_lineup = 'offense' THEN 1 ELSE 0 END)::int AS fga,
            SUM(CASE WHEN a.type = 'shot' AND a.parameters_made = 'made' AND a.parameters_points = 3 AND a.type_lineup = 'offense' THEN 1 ELSE 0 END)::int AS \"3pm\",
            SUM(CASE WHEN a.type = 'shot' AND a.parameters_points = 3 AND a.type_lineup = 'offense' THEN 1 ELSE 0 END)::int AS \"3pa\",
            SUM(CASE WHEN a.type = 'freeThrow' AND a.parameters_made = 'made' AND a.type_lineup = 'offense' THEN 1 ELSE 0 END)::int AS ftm,
            SUM(CASE WHEN a.type = 'freeThrow' AND a.type_lineup = 'offense' THEN 1 ELSE 0 END)::int AS fta
         FROM acts a
         GROUP BY a.team_id
       ),
       poss_end AS (
         SELECT DISTINCT
           a.game_id,
           a.team_id,
           a.id AS poss_end_id
         FROM acts a
         WHERE a.type_lineup = 'offense'
           AND a.final_end_poss
           AND a.id IS NOT NULL
       ),
       team_usage AS (
         SELECT
           pe.team_id,
           COUNT(DISTINCT pe.game_id)::int AS gp,
           COUNT(DISTINCT (pe.game_id, pe.team_id, pe.poss_end_id))::int AS poss_on_floor
         FROM poss_end pe
         GROUP BY pe.team_id
       ),
       seg_times AS (
         SELECT
           a.team_id,
           a.game_id,
           a.lineup_hash,
           a.segment_id,
           MAX(a.end_game_seconds_remaining) - MIN(a.end_game_seconds_remaining) AS seg_seconds
         FROM acts a
         WHERE a.lineup_hash IS NOT NULL
           AND a.segment_id IS NOT NULL
           AND a.end_game_seconds_remaining IS NOT NULL
         GROUP BY a.team_id, a.game_id, a.lineup_hash, a.segment_id
       ),
       team_minutes AS (
         SELECT
           st.team_id,
           ROUND(SUM(COALESCE(st.seg_seconds, 0))::numeric / 60.0, 1) AS minutes
         FROM seg_times st
         GROUP BY st.team_id
       ),
       team_names AS (
         SELECT
           fr.team_id,
           MIN(btrim(fr.team_name)) AS team_name
         FROM basketball_test.full_rosters fr
         WHERE fr.game_year = $1::int4
         GROUP BY fr.team_id
       )
       SELECT
         ts.team_id,
         tn.team_name,
         COALESCE(tu.gp, 0)::int AS gp,
         COALESCE(tu.poss_on_floor, 0)::int AS poss_on_floor,
         COALESCE(tm.minutes, 0)::numeric AS minutes,
         ts.pts,
         ts.reb,
         ts.ast,
         ts.stl,
         ts.blk,
         ts.tov,
         ts.fgm,
         ts.fga,
         ts.\"3pm\",
         ts.\"3pa\",
         ts.ftm,
         ts.fta,
         CASE WHEN ts.fga > 0 THEN ROUND((ts.fgm::numeric / ts.fga::numeric) * 100, 1) ELSE NULL END AS fg_pct,
         CASE WHEN ts.\"3pa\" > 0 THEN ROUND((ts.\"3pm\"::numeric / ts.\"3pa\"::numeric) * 100, 1) ELSE NULL END AS tp_pct,
         CASE WHEN ts.fta > 0 THEN ROUND((ts.ftm::numeric / ts.fta::numeric) * 100, 1) ELSE NULL END AS ft_pct,
         CASE WHEN ts.fga > 0 THEN ROUND(((ts.fgm::numeric + 0.5 * ts.\"3pm\"::numeric) / ts.fga::numeric) * 100, 1) ELSE NULL END AS efg,
         CASE WHEN (ts.fga + 0.44 * ts.fta) > 0 THEN ROUND((ts.pts::numeric / (2.0 * (ts.fga::numeric + 0.44 * ts.fta::numeric))) * 100, 1) ELSE NULL END AS ts
       FROM team_stats ts
       LEFT JOIN team_usage tu ON tu.team_id = ts.team_id
       LEFT JOIN team_minutes tm ON tm.team_id = ts.team_id
       LEFT JOIN team_names tn ON tn.team_id = ts.team_id
       WHERE tn.team_name IS NOT NULL
       ORDER BY ts.pts DESC, tm.minutes DESC, tn.team_name",
      params = list(
        as.integer(game_year),
        if (!is.na(start_d)) as.Date(start_d) else NA,
        if (!is.na(end_d)) as.Date(end_d) else NA,
        team_ids_csv,
        game_type_csv,
        opp_ids_csv,
        home_away,
        outcome,
        opp_rank_side,
        opp_rank_n,
        opp_rank_metric,
        max_margin,
        margin_status,
        max_time_remaining,
        isTRUE(ot_margin_filter),
        min_gn,
        max_gn,
        last_n_games
      )
    )
  }

  pr_vec <- function(x, invert = FALSE) {
    n <- sum(!is.na(x))
    if (n <= 1) return(rep(NA_real_, length(x)))
    r <- rank(x, na.last = "keep", ties.method = "average")
    p <- (r - 1) / (n - 1)
    if (invert) p <- 1 - p
    as.numeric(p)
  }

  rank_vec <- function(x, invert = FALSE) {
    if (invert) dplyr::dense_rank(x) else dplyr::dense_rank(dplyr::desc(x))
  }

  add_rank_and_percentiles <- function(df) {
    if (is.null(df) || !nrow(df)) return(df)
    metric_cfg <- list(
      pts = FALSE, reb = FALSE, ast = FALSE, stl = FALSE, blk = FALSE, tov = TRUE,
      fgm = FALSE, fga = FALSE, `3pm` = FALSE, `3pa` = FALSE, ftm = FALSE, fta = FALSE,
      fg_pct = FALSE, tp_pct = FALSE, ft_pct = FALSE, efg = FALSE, ts = FALSE
    )
    for (m in names(metric_cfg)) {
      if (!m %in% names(df)) next
      inv <- isTRUE(metric_cfg[[m]])
      df[[paste0("rank_", m)]] <- rank_vec(df[[m]], invert = inv)
      df[[paste0("pr_", m)]] <- pr_vec(df[[m]], invert = inv)
    }
    df
  }

  trend_symbol <- function(delta) {
    if (is.na(delta)) return("&#8212;")
    if (delta > 0) return("&#9650;")
    if (delta < 0) return("&#9660;")
    "&#9654;"
  }

  fmt_cell <- function(value, rank_now, rank_prev) {
    value_num <- suppressWarnings(as.numeric(value))
    rank_now_num <- suppressWarnings(as.numeric(rank_now))
    rank_prev_num <- suppressWarnings(as.numeric(rank_prev))

    val_txt <- ifelse(
      is.na(value_num),
      "NA",
      format(round(value_num, 1), nsmall = 1, trim = TRUE)
    )
    delta <- as.integer(rank_prev_num) - as.integer(rank_now_num)
    delta_txt <- ifelse(is.na(delta) | delta == 0, "", as.character(abs(delta)))
    trend_txt <- vapply(delta, trend_symbol, character(1))

    ranked_cell <- paste0(
      "<div style='display:flex; justify-content:space-between; gap:8px;'>",
      "<span>", val_txt, "</span>",
      "<span style='font-size:11px; color:#c9d1d9;'>#",
      as.integer(rank_now_num),
      " ",
      trend_txt,
      delta_txt,
      "</span></div>"
    )
    ifelse(!is.finite(rank_now_num), val_txt, ranked_cell)
  }

  tst_ref <- reactiveValues(teams = NULL)

  observeEvent(list(input$main_tabs, input$game_year), ignoreInit = TRUE, {
    if (!identical(input$main_tabs, "team_stats")) return(NULL)
    gy_int <- as.integer(input$game_year)
    req(gy_int)

    teams_df <- cached_ref_query(
      key = sprintf("tst_teams_%d", gy_int),
      query_fun = function() {
        db_get_query(
          pg_pool,
          "SELECT DISTINCT team_id, team_name
             FROM basketball_test.full_rosters
            WHERE game_year = $1::int4
            ORDER BY team_name",
          params = list(gy_int)
        )
      }
    )
    tst_ref$teams <- teams_df
    updateSelectizeInput(session, "tst_teams", choices = teams_df$team_name, selected = character(0), server = TRUE)
    updateSelectizeInput(session, "tst_opponents", choices = teams_df$team_name, selected = character(0), server = TRUE)

    gn_df <- cached_ref_query(
      key = sprintf("tst_gn_%d", gy_int),
      query_fun = function() {
        db_get_query(
          pg_pool,
          "SELECT DISTINCT gn
             FROM basketball_test.final_schedule_mv
            WHERE game_year = $1::int4
            ORDER BY gn",
          params = list(gy_int)
        )
      }
    )
    gn_vals <- if (nrow(gn_df)) as.integer(gn_df$gn) else integer(0)
    update_gn_last_n_choices(session, "tst", gn_vals)
  })

  observeEvent(input$game_year, {
    b <- shared$season_date_bounds(input$game_year)
    updateDateRangeInput(session, "tst_dates", start = b$start, end = b$end, min = b$start, max = b$end)
  }, ignoreInit = FALSE)

  setup_gn_last_n_sync(session, input, "tst")

  observeEvent(input$tst_reset, {
    b <- shared$season_date_bounds(input$game_year %||% DEFAULT_GAME_YEAR)
    updateDateRangeInput(session, "tst_dates", start = b$start, end = b$end, min = b$start, max = b$end)
    updateSelectizeInput(session, "tst_teams", selected = character(0))
    updateSelectizeInput(session, "tst_game_type", selected = "")
    updateSelectizeInput(session, "tst_opponents", selected = character(0))
    updateSelectInput(session, "tst_home_away", selected = "")
    updateSelectInput(session, "tst_outcome", selected = "")
    updateSelectInput(session, "tst_opp_rank_side", selected = "")
    updateSelectInput(session, "tst_opp_rank_n", selected = "")
    updateSelectInput(session, "tst_opp_rank_metric", selected = "")
    updateSelectInput(session, "tst_display_mode", selected = "Per Game")
    updateSelectInput(session, "tst_rank_change_basis", selected = "week")
    updateSliderInput(session, "tst_min_gp_slider", value = 1, min = 1, max = 40)
    updateNumericInput(session, "tst_min_gp", value = 1, min = 1, max = 40)
    updateCheckboxInput(session, "tst_clutch_enabled", value = FALSE)
    updateSliderInput(session, "tst_clutch_margin", value = 5)
    updateSelectInput(session, "tst_clutch_status", selected = "all")
    updateSliderInput(session, "tst_clutch_minutes", value = 5)
    updateCheckboxInput(session, "tst_clutch_ot_margin", value = FALSE)
    updateSelectizeInput(session, "tst_gn_min", selected = "")
    updateSelectizeInput(session, "tst_gn_max", selected = "")
    updateSelectizeInput(session, "tst_last_n", selected = "")
  })

  observeEvent(input$tst_min_gp_slider, ignoreInit = TRUE, {
    s <- suppressWarnings(as.integer(input$tst_min_gp_slider))
    n <- suppressWarnings(as.integer(input$tst_min_gp))
    if (is.na(s)) return(NULL)
    if (is.na(n) || s != n) updateNumericInput(session, "tst_min_gp", value = s)
  })

  observeEvent(input$tst_min_gp, ignoreInit = TRUE, {
    n <- suppressWarnings(as.integer(input$tst_min_gp))
    s <- suppressWarnings(as.integer(input$tst_min_gp_slider))
    if (is.na(n)) return(NULL)
    if (is.na(s) || n != s) updateSliderInput(session, "tst_min_gp_slider", value = n)
  })

  debounced_range <- reactive(input$tst_dates) %>% debounce(300)
  debounced_teams <- reactive(input$tst_teams) %>% debounce(300)
  debounced_tst_filters <- reactive(list(
    game_type = input$tst_game_type,
    opp_names = input$tst_opponents,
    home_away = input$tst_home_away,
    outcome = input$tst_outcome,
    rank_side = input$tst_opp_rank_side,
    rank_n = input$tst_opp_rank_n,
    metric = input$tst_opp_rank_metric,
    clutch_enabled = input$tst_clutch_enabled,
    clutch_margin = input$tst_clutch_margin,
    clutch_status = input$tst_clutch_status,
    clutch_minutes = input$tst_clutch_minutes,
    clutch_ot_margin = input$tst_clutch_ot_margin
  )) %>% debounce(300)

  gn_params <- reactive({
    resolve_gn_last_n_params(input, "tst")
  }) %>% debounce(150)

  selected_team_ids <- reactive({
    td <- tst_ref$teams
    teams_in <- debounced_teams()
    if (is.null(td) || !nrow(td) || is.null(teams_in) || !length(teams_in)) return(NULL)
    td %>% filter(team_name %in% teams_in) %>% pull(team_id)
  })

  selected_opp_ids <- reactive({
    td <- tst_ref$teams
    opp_names <- debounced_tst_filters()$opp_names
    if (is.null(td) || !nrow(td) || is.null(opp_names) || !length(opp_names)) return(NULL)
    td %>% filter(team_name %in% opp_names) %>% pull(team_id)
  })

  build_tst_db_args <- function() {
    f <- debounced_tst_filters()
    tids <- selected_team_ids()
    opp_ids <- selected_opp_ids()
    gp <- gn_params()

    clutch_enabled <- isTRUE(f$clutch_enabled)
    max_margin <- if (clutch_enabled) suppressWarnings(as.integer(f$clutch_margin)) else NA_integer_
    margin_status <- if (clutch_enabled) (f$clutch_status %||% "all") else NA_character_
    max_time_remaining <- if (clutch_enabled) suppressWarnings(as.integer(f$clutch_minutes)) * 60L else NA_integer_
    ot_margin_filter <- if (clutch_enabled) isTRUE(f$clutch_ot_margin) else FALSE

    list(
      team_ids_csv = if (!is.null(tids) && length(tids) > 0) paste(as.integer(tids), collapse = ",") else NA_character_,
      game_type_csv = if (!is.null(f$game_type) && any(nzchar(f$game_type))) paste(as.integer(f$game_type[nzchar(f$game_type)]), collapse = ",") else NA_character_,
      opp_ids_csv = if (!is.null(opp_ids) && length(opp_ids) > 0) paste(as.integer(opp_ids), collapse = ",") else NA_character_,
      opp_rank_side = if (nzchar(f$rank_side %||% "")) f$rank_side else NA_character_,
      opp_rank_n = suppressWarnings(as.integer(if (!nzchar(f$rank_n %||% "")) NA_character_ else f$rank_n)),
      opp_rank_metric = if (nzchar(f$metric %||% "")) f$metric else NA_character_,
      home_away = if (nzchar(f$home_away %||% "")) f$home_away else NA_character_,
      outcome = if (nzchar(f$outcome %||% "")) f$outcome else NA_character_,
      max_margin = max_margin,
      margin_status = margin_status,
      max_time_remaining = max_time_remaining,
      ot_margin_filter = ot_margin_filter,
      min_gn = gp$min_gn,
      max_gn = gp$max_gn,
      last_n_games = gp$last_n
    )
  }

  query_team_stats <- function(end_override = NA) {
    gy_int <- as.integer(input$game_year)
    rng <- debounced_range()
    req(gy_int, rng, !is.na(rng[1]), !is.na(rng[2]))

    db_args <- build_tst_db_args()

    start_d <- as.Date(rng[1])
    end_d <- if (is.na(end_override)) as.Date(rng[2]) else as.Date(end_override)
    if (is.na(end_d) || end_d < start_d) return(data.frame())

    run_team_traditional_dynamic(
      pg_pool,
      game_year = gy_int,
      start_d = start_d,
      end_d = end_d,
      team_ids_csv = db_args$team_ids_csv,
      game_type_csv = db_args$game_type_csv,
      opp_ids_csv = db_args$opp_ids_csv,
      home_away = db_args$home_away,
      outcome = db_args$outcome,
      opp_rank_side = db_args$opp_rank_side,
      opp_rank_n = db_args$opp_rank_n,
      opp_rank_metric = db_args$opp_rank_metric,
      max_margin = db_args$max_margin,
      margin_status = db_args$margin_status,
      max_time_remaining = db_args$max_time_remaining,
      ot_margin_filter = db_args$ot_margin_filter,
      min_gn = db_args$min_gn,
      max_gn = db_args$max_gn,
      last_n_games = db_args$last_n_games
    )
  }

  current_df <- reactive({
    req(identical(input$main_tabs, "team_stats"))
    out <- tryCatch(query_team_stats(), error = function(e) NULL)
    if (is.null(out) || !nrow(out)) return(NULL)
    out %>% add_rank_and_percentiles()
  }) %>% bindEvent(
    input$main_tabs, input$game_year, debounced_range(), debounced_teams(), debounced_tst_filters(), gn_params()
  )

  previous_df <- reactive({
    req(identical(input$main_tabs, "team_stats"))
    rng <- debounced_range()
    req(rng, !is.na(rng[2]))
    end_d <- as.Date(rng[2])
    basis <- input$tst_rank_change_basis %||% "week"

    prev_end <- if (identical(basis, "match")) {
      q <- tryCatch(db_get_query(
        pg_pool,
        "SELECT MAX(game_date)::date AS d
         FROM basketball_test.final_schedule_mv
         WHERE game_year = $1::int4
           AND game_date < $2::date",
        params = list(as.integer(input$game_year), end_d)
      ), error = function(e) NULL)
      if (is.null(q) || !nrow(q) || is.na(q$d[1])) as.Date(NA) else as.Date(q$d[1])
    } else {
      end_d - 7
    }
    if (is.na(prev_end)) return(NULL)

    out <- tryCatch(query_team_stats(end_override = prev_end), error = function(e) NULL)
    if (is.null(out) || !nrow(out)) return(NULL)
    out %>% add_rank_and_percentiles()
  }) %>% bindEvent(
    input$main_tabs, input$game_year, debounced_range(), debounced_teams(), debounced_tst_filters(), gn_params(), input$tst_rank_change_basis
  )

  tst_display_df <- reactive({
    df <- current_df()
    if (is.null(df) || !nrow(df)) return(df)
    min_gp <- suppressWarnings(as.integer(input$tst_min_gp))
    if (!is.finite(min_gp) || is.na(min_gp) || min_gp < 1L) min_gp <- 1L
    df <- df %>% filter(coalesce(gp, 0L) >= min_gp)
    if (is.null(df) || !nrow(df)) return(df)
    apply_tst_mode(df, input$tst_display_mode %||% "Per Game") %>% add_rank_and_percentiles()
  }) %>% bindEvent(current_df(), input$tst_display_mode, input$tst_min_gp, input$tst_min_gp_slider)

  observeEvent(tst_display_df(), ignoreInit = FALSE, {
    df <- tst_display_df()
    max_gp <- 1L
    if (!is.null(df) && nrow(df) && "gp" %in% names(df)) {
      max_gp <- suppressWarnings(as.integer(max(df$gp, na.rm = TRUE)))
      if (!is.finite(max_gp) || is.na(max_gp) || max_gp < 1L) max_gp <- 1L
    }
    cur_num <- suppressWarnings(as.integer(input$tst_min_gp))
    cur_sld <- suppressWarnings(as.integer(input$tst_min_gp_slider))
    target <- max(1L, min(max_gp, dplyr::coalesce(cur_num, cur_sld, 1L)))
    updateSliderInput(session, "tst_min_gp_slider", min = 1, max = max_gp, value = target)
    updateNumericInput(session, "tst_min_gp", min = 1, max = max_gp, value = target)
  })

  output$tst_table <- DT::renderDataTable({
    req(identical(input$main_tabs, "team_stats"))
    df <- tst_display_df()
    if (is.null(df) || !nrow(df)) return(NULL)
    prev <- previous_df()

    metric_cols <- c("pts", "reb", "ast", "stl", "blk", "tov", "fgm", "fga", "3pm", "3pa", "ftm", "fta", "fg_pct", "tp_pct", "ft_pct", "efg", "ts")
    prev_rank <- list()
    if (!is.null(prev) && nrow(prev)) {
      for (m in metric_cols) {
        rk <- paste0("rank_", m)
        if (rk %in% names(prev)) prev_rank[[m]] <- setNames(prev[[rk]], as.character(prev$team_id))
      }
    }

    team_key <- as.character(df$team_id)
    prev_rank_for <- function(metric_name) {
      if (is.null(prev_rank[[metric_name]])) return(rep(NA_integer_, nrow(df)))
      as.integer(prev_rank[[metric_name]][team_key])
    }
    make_metric_col <- function(values, ranks_now, metric_name) {
      fmt_cell(values, ranks_now, prev_rank_for(metric_name))
    }

    disp <- data.frame(
      team_id = df$team_id,
      Team = df$team_name,
      GP = df$gp,
      `Poss On Floor` = df$poss_on_floor,
      Min = df$minutes,
      stringsAsFactors = FALSE,
      check.names = FALSE
    )
    disp$PTS <- make_metric_col(df$pts, df$rank_pts, "pts")
    disp$REB <- make_metric_col(df$reb, df$rank_reb, "reb")
    disp$AST <- make_metric_col(df$ast, df$rank_ast, "ast")
    disp$STL <- make_metric_col(df$stl, df$rank_stl, "stl")
    disp$BLK <- make_metric_col(df$blk, df$rank_blk, "blk")
    disp$TOV <- make_metric_col(df$tov, df$rank_tov, "tov")
    disp$FGM <- make_metric_col(df$fgm, df$rank_fgm, "fgm")
    disp$FGA <- make_metric_col(df$fga, df$rank_fga, "fga")
    disp$`3PM` <- make_metric_col(df$`3pm`, df$`rank_3pm`, "3pm")
    disp$`3PA` <- make_metric_col(df$`3pa`, df$`rank_3pa`, "3pa")
    disp$FTM <- make_metric_col(df$ftm, df$rank_ftm, "ftm")
    disp$FTA <- make_metric_col(df$fta, df$rank_fta, "fta")
    disp$`FG%` <- make_metric_col(df$fg_pct, df$rank_fg_pct, "fg_pct")
    disp$`3P%` <- make_metric_col(df$tp_pct, df$rank_tp_pct, "tp_pct")
    disp$`FT%` <- make_metric_col(df$ft_pct, df$rank_ft_pct, "ft_pct")
    disp$`eFG%` <- make_metric_col(df$efg, df$rank_efg, "efg")
    disp$`TS%` <- make_metric_col(df$ts, df$rank_ts, "ts")
    disp$pr_pts <- df$pr_pts
    disp$pr_reb <- df$pr_reb
    disp$pr_ast <- df$pr_ast
    disp$pr_stl <- df$pr_stl
    disp$pr_blk <- df$pr_blk
    disp$pr_tov <- df$pr_tov
    disp$pr_fgm <- df$pr_fgm
    disp$pr_fga <- df$pr_fga
    disp$pr_3pm <- df$pr_3pm
    disp$pr_3pa <- df$pr_3pa
    disp$pr_ftm <- df$pr_ftm
    disp$pr_fta <- df$pr_fta
    disp$pr_fg_pct <- df$pr_fg_pct
    disp$pr_tp_pct <- df$pr_tp_pct
    disp$pr_ft_pct <- df$pr_ft_pct
    disp$pr_efg <- df$pr_efg
    disp$pr_ts <- df$pr_ts

    pr_map <- c(
      PTS = "pr_pts", REB = "pr_reb", AST = "pr_ast", STL = "pr_stl", BLK = "pr_blk",
      TOV = "pr_tov", FGM = "pr_fgm", FGA = "pr_fga", `3PM` = "pr_3pm", `3PA` = "pr_3pa",
      FTM = "pr_ftm", FTA = "pr_fta", `FG%` = "pr_fg_pct", `3P%` = "pr_tp_pct", `FT%` = "pr_ft_pct",
      `eFG%` = "pr_efg", `TS%` = "pr_ts"
    )
    pr_cols <- unname(pr_map)
    hide_cols <- c("team_id", pr_cols)

    dt <- DT::datatable(
      disp,
      rownames = FALSE,
      escape = FALSE,
      extensions = "Buttons",
      options = list(
        headerCallback = HEADER_TOOLTIP_JS,
        dom = "Btip",
        buttons = list(
          list(
            extend = "csv",
            text = "Download CSV",
            filename = sprintf("traditional_team_stats_%s", Sys.Date()),
            exportOptions = list(columns = ":visible", stripHtml = TRUE)
          )
        ),
        pageLength = 50,
        deferRender = TRUE,
        scrollX = TRUE,
        scrollY = "70vh",
        scrollCollapse = TRUE,
        order = list(list(which(names(disp) == "PTS") - 1L, "desc")),
        columnDefs = list(
          list(className = "dt-center", targets = "_all"),
          list(visible = FALSE, targets = (which(names(disp) %in% hide_cols) - 1L))
        )
      )
    ) %>%
      DT::formatRound(c("GP", "Poss On Floor", "Min"), 1)

    for (nm in names(pr_map)) {
      pr_col <- pr_map[[nm]]
      if (!(nm %in% names(disp)) || !(pr_col %in% names(disp))) next
      dt <- DT::formatStyle(dt, nm, backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = pr_col)
    }
    dt
  }, server = FALSE) %>% bindEvent(tst_display_df(), previous_df(), input$main_tabs)

  output$tst_filter_chips <- renderUI({
    build_filter_chips("tst", input, shared$season_date_bounds, reset_btn_id = "tst_reset")
  })
  setup_chip_clears("tst", session, input, shared,
    game_type_id = "tst_game_type", opponents_id = "tst_opponents",
    home_away_id = "tst_home_away", outcome_id = "tst_outcome",
    gn_min_id = "tst_gn_min", gn_max_id = "tst_gn_max", last_n_id = "tst_last_n",
    opp_rank_ids = c("tst_opp_rank_side", "tst_opp_rank_n", "tst_opp_rank_metric"),
    date_id = "tst_dates", gy_input_id = "game_year",
    teams_ids = "tst_teams",
    clutch_enabled_id = "tst_clutch_enabled")
}

