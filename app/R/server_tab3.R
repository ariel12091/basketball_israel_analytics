# server_tab3.R - Tab 3: Team Ratings server logic

TR_SUMMARY_FILTERABLE_COLS <- c(
  "GP" = "games_played",
  "Min" = "minutes",
  "W" = "wins",
  "L" = "losses",
  "Off PPP" = "off_ppp",
  "Def PPP" = "def_ppp",
  "Net Rtg" = "net_rtg",
  "Off Pace" = "off_pace",
  "Def Pace" = "def_pace",
  "Off Poss" = "off_poss",
  "Def Poss" = "def_poss"
)

TR_FF_FILTERABLE_COLS <- c(
  "Off PPP" = "off_ppp",
  "Off eFG%" = "off_efg",
  "Off OREB%" = "off_oreb",
  "Off TOV%" = "off_tov",
  "Off FTR" = "off_ftr",
  "Off Poss" = "off_poss",
  "Def PPP" = "def_ppp",
  "Def eFG%" = "def_efg",
  "Def OREB%" = "def_oreb",
  "Def TOV%" = "def_tov",
  "Def FTR" = "def_ftr",
  "Def Poss" = "def_poss",
  "Min" = "minutes",
  "Net" = "net_rtg"
)

TR_SP_FILTERABLE_COLS <- c(
  shot_profile_metric_cols("Off", "off"),
  "Off Poss" = "off_poss",
  shot_profile_metric_cols("Def", "def"),
  "Def Poss" = "def_poss",
  "Min" = "minutes"
)

TR_TRAD_FILTERABLE_COLS <- c(
  "GP" = "gp",
  "Poss On Floor" = "poss_on_floor",
  "Min" = "minutes",
  "PTS" = "pts",
  "REB" = "reb",
  "OREB" = "oreb",
  "DREB" = "dreb",
  "AST" = "ast",
  "STL" = "stl",
  "BLK" = "blk",
  "DFL" = "dfl",
  "TOV" = "tov",
  "FGM" = "fgm",
  "FGA" = "fga",
  "FG%" = "fg_pct",
  "2PM" = "2pm",
  "2PA" = "2pa",
  "2P%" = "two_pct",
  "3PM" = "3pm",
  "3PA" = "3pa",
  "3P%" = "tp_pct",
  "FTM" = "ftm",
  "FTA" = "fta",
  "FT%" = "ft_pct",
  "eFG%" = "efg",
  "TS%" = "ts"
)

server_tab3 <- function(input, output, session, shared) {
  tr_stat_filter_state <- make_stat_filter_state()
  tr_stat_filter_cols <- reactive({
    mode <- input$tr_view_mode %||% "Summary"
    if (identical(mode, "Traditional")) {
      TR_TRAD_FILTERABLE_COLS
    } else if (identical(mode, "Four Factors")) {
      TR_FF_FILTERABLE_COLS
    } else if (identical(mode, "Shot Profile")) {
      TR_SP_FILTERABLE_COLS
    } else {
      TR_SUMMARY_FILTERABLE_COLS
    }
  })

  setup_stat_filter_handlers("tr", input, session, tr_stat_filter_cols, tr_stat_filter_state)

  log_tab3 <- function(msg, level = "INFO") {
    app_log("tab3", msg, level = level, session = session)
  }
  log_tab3_error <- function(msg) log_tab3(msg, level = "ERROR")
  log_tab3_state <- function(tag = "state") {
    safe_chr <- function(x) {
      if (is.null(x) || is.environment(x)) return(NA_character_)
      paste(as.character(x), collapse = "|")
    }
    safe_date <- function(x) {
      if (is.null(x) || is.environment(x) || length(x) < 1) return(NA_character_)
      out <- tryCatch(as.Date(x[[1]]), error = function(e) as.Date(NA))
      if (is.na(out)) NA_character_ else as.character(out)
    }
    msg <- paste0(
      tag,
      " game_year=", safe_chr(input$game_year),
      " tr_dates_start=", safe_date(input$tr_dates),
      " tr_dates_end=", if (is.null(input$tr_dates) || is.environment(input$tr_dates) || length(input$tr_dates) < 2) NA_character_ else {
        d2 <- tryCatch(as.Date(input$tr_dates[[2]]), error = function(e) as.Date(NA))
        if (is.na(d2)) NA_character_ else as.character(d2)
      },
      " view=", safe_chr(input$tr_view_mode),
      " trad_mode=", safe_chr(input$tr_trad_display_mode),
      " trad_side=", if (isTRUE(input$tr_trad_defense_mode)) "defense" else "offense"
    )
    log_tab3(msg, level = "DEBUG")
  }
  safe_tr_date <- function(x) {
    if (is.null(x) || is.environment(x)) return(as.Date(NA))
    out <- tryCatch(as.Date(x), error = function(e) as.Date(NA))
    if (length(out) < 1) return(as.Date(NA))
    out[1]
  }
  tr_date_part <- function(x, idx) {
    if (is.null(x) || is.environment(x)) return(as.Date(NA))
    i <- suppressWarnings(as.integer(idx))
    if (is.na(i) || i < 1L) i <- 1L
    if (length(x) < i) return(as.Date(NA))
    safe_tr_date(x[[i]])
  }

  add_team_pace_cols <- function(df, minutes_map = NULL) {
    if (is.null(df) || !nrow(df)) return(df)
    gp_col <- if ("games_played" %in% names(df)) {
      "games_played"
    } else if ("gp" %in% names(df)) {
      "gp"
    } else {
      NA_character_
    }
    gp <- if (is.na(gp_col)) rep(NA_real_, nrow(df)) else suppressWarnings(as.numeric(df[[gp_col]]))
    gp[!is.finite(gp) | gp <= 0] <- NA_real_
    off_poss <- if ("off_poss" %in% names(df)) suppressWarnings(as.numeric(df$off_poss)) else rep(NA_real_, nrow(df))
    def_poss <- if ("def_poss" %in% names(df)) suppressWarnings(as.numeric(df$def_poss)) else rep(NA_real_, nrow(df))
    minutes_vec <- rep(NA_real_, nrow(df))
    if (!is.null(minutes_map) && "team_id" %in% names(df)) {
      mins <- suppressWarnings(as.numeric(minutes_map[as.character(df$team_id)]))
      mins[!is.finite(mins) | mins <= 0] <- NA_real_
      minutes_vec <- mins
    }
    miss <- is.na(minutes_vec) & !is.na(gp)
    if (any(miss)) minutes_vec[miss] <- gp[miss] * 40
    df$minutes <- minutes_vec
    df$off_pace <- ifelse(is.na(minutes_vec), NA_real_, (off_poss / minutes_vec) * 40)
    df$def_pace <- ifelse(is.na(minutes_vec), NA_real_, (def_poss / minutes_vec) * 40)
    df
  }

  fetch_team_game_minutes <- function(pool, p) {
    game_type_csv <- if (is.null(p$game_type_csv)) NA_character_ else p$game_type_csv
    opp_ids_csv <- if (is.null(p$opp_ids_csv)) NA_character_ else p$opp_ids_csv
    home_away <- if (is.null(p$home_away)) NA_character_ else p$home_away
    outcome <- if (is.null(p$outcome)) NA_character_ else p$outcome
    opp_rank_side <- if (is.null(p$rank_side)) NA_character_ else p$rank_side
    opp_rank_metric <- if (is.null(p$metric)) NA_character_ else p$metric

    poss_scope_active <- (!is.na(p$max_margin)) ||
      (!is.na(p$margin_status) && !identical(p$margin_status, "all")) ||
      (!is.na(p$max_time_remaining)) ||
      isTRUE(p$ot_margin_filter) ||
      any(!is.na(c(
        p$num_starters_off, p$num_starters_def,
        p$num_starters_off_min, p$num_starters_off_max,
        p$num_starters_def_min, p$num_starters_def_max
      )))

    if (!isTRUE(poss_scope_active)) {
      return(db_get_query(
        pool,
        "WITH params AS (
           SELECT
             CASE WHEN $4::text IS NULL OR btrim($4::text) = '' THEN NULL::int4[]
                  ELSE string_to_array(regexp_replace($4::text, '\\s+', '', 'g'), ',')::int4[] END AS game_types,
             CASE WHEN $5::text IS NULL OR btrim($5::text) = '' THEN NULL::int4[]
                  ELSE string_to_array(regexp_replace($5::text, '\\s+', '', 'g'), ',')::int4[] END AS opp_ids
         ),
         sched_base AS (
           SELECT
             fs.game_id, fs.team_id, fs.game_year, fs.opp_team_id, fs.game_date, fs.gn,
             fs.is_home, fs.has_won,
             ROW_NUMBER() OVER (
               PARTITION BY fs.team_id, fs.game_year
               ORDER BY fs.game_date DESC NULLS LAST, fs.game_id DESC
             ) AS rn_recent
           FROM basketball_test.final_schedule_mv fs
           CROSS JOIN params p0
           WHERE fs.game_year = $1::int4
             AND ($2::date IS NULL OR fs.game_date >= $2::date)
             AND ($3::date IS NULL OR fs.game_date <= $3::date)
             AND (p0.game_types IS NULL OR fs.game_type = ANY(p0.game_types))
             AND (p0.opp_ids IS NULL OR fs.opp_team_id = ANY(p0.opp_ids))
             AND ($6::text IS NULL OR $6::text = '' OR ($6::text = 'home' AND fs.is_home) OR ($6::text = 'away' AND NOT fs.is_home))
             AND ($7::text IS NULL OR $7::text = '' OR ($7::text = 'win' AND fs.has_won IS TRUE) OR ($7::text = 'loss' AND fs.has_won IS FALSE))
             AND ($11::int4 IS NULL OR fs.gn >= $11::int4)
             AND ($12::int4 IS NULL OR fs.gn <= $12::int4)
         ),
         sched_last_n AS (
           SELECT *
           FROM sched_base
           WHERE ($13::int4 IS NULL OR rn_recent <= $13::int4)
         ),
         sched_ranked AS (
           SELECT
             sb.*,
             CASE
               WHEN $8::text IN ('top','bottom') THEN
                 CASE COALESCE($10::text, 'net')
                   WHEN 'off' THEN r.rank_off_ppp
                   WHEN 'def' THEN r.rank_def_ppp
                   ELSE r.rank_net_rtg
                 END
               ELSE NULL
             END AS opp_rank,
             CASE
               WHEN $8::text = 'bottom' THEN
                 MAX(
                   CASE COALESCE($10::text, 'net')
                     WHEN 'off' THEN r.rank_off_ppp
                     WHEN 'def' THEN r.rank_def_ppp
                     ELSE r.rank_net_rtg
                   END
                 ) OVER (PARTITION BY sb.game_year)
               ELSE NULL
             END AS max_rank
           FROM sched_last_n sb
           LEFT JOIN basketball_test.team_ppp_ratings_mv r
             ON r.game_year::int4 = sb.game_year
            AND r.team_id::int4 = sb.opp_team_id
            AND $8::text IN ('top','bottom')
         ),
         sched_filtered AS (
           SELECT game_id, team_id
           FROM sched_ranked
           WHERE $8::text IS NULL OR $8::text = '' OR $9::int4 IS NULL
              OR ($8::text = 'top' AND opp_rank <= $9::int4)
              OR ($8::text = 'bottom' AND opp_rank >= (max_rank - $9::int4 + 1))
         ),
         game_quarters AS (
           SELECT
             sf.team_id,
             sf.game_id,
             GREATEST(MAX(COALESCE(d.quarter, 4)), 4) AS max_q
           FROM sched_filtered sf
           JOIN basketball_test.df_pts_poss_lineups_longer_mv d
             ON d.game_id = sf.game_id
            AND d.team_id = sf.team_id
           GROUP BY sf.team_id, sf.game_id
         )
         SELECT
           team_id,
           SUM(40 + 5 * GREATEST(max_q - 4, 0))::numeric AS game_minutes
         FROM game_quarters
         GROUP BY team_id",
        params = list(
          as.integer(p$game_year),
          if (!is.na(p$start_d)) as.Date(p$start_d) else NA,
          if (!is.na(p$end_d)) as.Date(p$end_d) else NA,
          game_type_csv,
          opp_ids_csv,
          home_away,
          outcome,
          opp_rank_side,
          p$rank_n,
          opp_rank_metric,
          p$min_gn,
          p$max_gn,
          p$last_n_games
        )
      ))
    }

    db_get_query(
      pool,
      "WITH params AS (
         SELECT
           CASE WHEN $4::text IS NULL OR btrim($4::text) = '' THEN NULL::int4[]
                ELSE string_to_array(regexp_replace($4::text, '\\s+', '', 'g'), ',')::int4[] END AS game_types,
           CASE WHEN $5::text IS NULL OR btrim($5::text) = '' THEN NULL::int4[]
                ELSE string_to_array(regexp_replace($5::text, '\\s+', '', 'g'), ',')::int4[] END AS opp_ids
       ),
       sched_base AS (
         SELECT
           fs.game_id, fs.team_id, fs.game_year, fs.opp_team_id, fs.game_date, fs.gn,
           fs.is_home, fs.has_won,
           ROW_NUMBER() OVER (
             PARTITION BY fs.team_id, fs.game_year
             ORDER BY fs.game_date DESC NULLS LAST, fs.game_id DESC
           ) AS rn_recent
         FROM basketball_test.final_schedule_mv fs
         CROSS JOIN params p0
         WHERE fs.game_year = $1::int4
           AND ($2::date IS NULL OR fs.game_date >= $2::date)
           AND ($3::date IS NULL OR fs.game_date <= $3::date)
           AND (p0.game_types IS NULL OR fs.game_type = ANY(p0.game_types))
           AND (p0.opp_ids IS NULL OR fs.opp_team_id = ANY(p0.opp_ids))
           AND ($6::text IS NULL OR $6::text = '' OR ($6::text = 'home' AND fs.is_home) OR ($6::text = 'away' AND NOT fs.is_home))
           AND ($7::text IS NULL OR $7::text = '' OR ($7::text = 'win' AND fs.has_won IS TRUE) OR ($7::text = 'loss' AND fs.has_won IS FALSE))
           AND ($11::int4 IS NULL OR fs.gn >= $11::int4)
           AND ($12::int4 IS NULL OR fs.gn <= $12::int4)
       ),
       sched_last_n AS (
         SELECT *
         FROM sched_base
         WHERE ($13::int4 IS NULL OR rn_recent <= $13::int4)
       ),
       sched_ranked AS (
         SELECT
           sb.*,
           CASE
             WHEN $8::text IN ('top','bottom') THEN
               CASE COALESCE($10::text, 'net')
                 WHEN 'off' THEN r.rank_off_ppp
                 WHEN 'def' THEN r.rank_def_ppp
                 ELSE r.rank_net_rtg
               END
             ELSE NULL
           END AS opp_rank,
           CASE
             WHEN $8::text = 'bottom' THEN
               MAX(
                 CASE COALESCE($10::text, 'net')
                   WHEN 'off' THEN r.rank_off_ppp
                   WHEN 'def' THEN r.rank_def_ppp
                   ELSE r.rank_net_rtg
                 END
               ) OVER (PARTITION BY sb.game_year)
             ELSE NULL
           END AS max_rank
         FROM sched_last_n sb
         LEFT JOIN basketball_test.team_ppp_ratings_mv r
           ON r.game_year::int4 = sb.game_year
          AND r.team_id::int4 = sb.opp_team_id
          AND $8::text IN ('top','bottom')
       ),
        sched_filtered AS (
          SELECT game_id, team_id
          FROM sched_ranked
          WHERE $8::text IS NULL OR $8::text = '' OR $9::int4 IS NULL
             OR ($8::text = 'top' AND opp_rank <= $9::int4)
             OR ($8::text = 'bottom' AND opp_rank >= (max_rank - $9::int4 + 1))
        ),
        filtered_rows AS (
          SELECT
            d.team_id,
            d.game_id,
            d.lineup_hash,
            d.segment_id,
            d.id,
            d.event_elapsed_seconds
          FROM basketball_test.df_pts_poss_lineups_longer_mv d
          JOIN sched_filtered sf
            ON sf.game_id = d.game_id
           AND sf.team_id = d.team_id
          WHERE (COALESCE($14::int4, NULL) IS NULL
                 OR ABS(CASE WHEN d.type_lineup = 'offense'
                             THEN (d.own_team_score - COALESCE(d.team_score, 0)) - d.opp_team_score
                             ELSE d.own_team_score - (d.opp_team_score - COALESCE(d.team_score, 0))
                        END) <= $14::int4
                 OR (d.quarter > 4 AND NOT COALESCE($17::bool, FALSE)))
            AND ($15::text IS NULL OR $15::text = '' OR $15::text = 'all'
                 OR ($15::text = 'leading'  AND
                     CASE WHEN d.type_lineup = 'offense'
                          THEN (d.own_team_score - COALESCE(d.team_score, 0)) > d.opp_team_score
                          ELSE d.own_team_score > (d.opp_team_score - COALESCE(d.team_score, 0))
                     END)
                 OR ($15::text = 'trailing' AND
                     CASE WHEN d.type_lineup = 'offense'
                          THEN (d.own_team_score - COALESCE(d.team_score, 0)) < d.opp_team_score
                          ELSE d.own_team_score < (d.opp_team_score - COALESCE(d.team_score, 0))
                     END)
                 OR ($15::text = 'tied' AND
                     CASE WHEN d.type_lineup = 'offense'
                          THEN (d.own_team_score - COALESCE(d.team_score, 0)) = d.opp_team_score
                          ELSE d.own_team_score = (d.opp_team_score - COALESCE(d.team_score, 0))
                     END)
                 OR (d.quarter > 4 AND NOT COALESCE($17::bool, FALSE)))
            AND ($16::int4 IS NULL OR d.end_game_seconds_remaining <= $16::int4 OR d.quarter > 4)
            AND (COALESCE($20::int4, $18::int4) IS NULL OR d.own_starters >= COALESCE($20::int4, $18::int4))
            AND (COALESCE($21::int4, $18::int4) IS NULL OR d.own_starters <= COALESCE($21::int4, $18::int4))
            AND (COALESCE($22::int4, $19::int4) IS NULL OR d.opp_starters >= COALESCE($22::int4, $19::int4))
            AND (COALESCE($23::int4, $19::int4) IS NULL OR d.opp_starters <= COALESCE($23::int4, $19::int4))
            AND d.lineup_hash IS NOT NULL
            AND d.segment_id IS NOT NULL
            AND d.event_elapsed_seconds IS NOT NULL
        ),
        filtered_segments AS (
          SELECT
            team_id,
            game_id,
            lineup_hash,
            segment_id,
            GREATEST(
              (array_agg(event_elapsed_seconds ORDER BY id DESC))[1] -
              (array_agg(event_elapsed_seconds ORDER BY id))[1],
              0
            )::numeric AS seg_seconds
          FROM filtered_rows
          GROUP BY team_id, game_id, lineup_hash, segment_id
        )
        SELECT
          team_id,
          ROUND(SUM(seg_seconds) / 60.0, 3)::numeric AS game_minutes
        FROM filtered_segments
        GROUP BY team_id",
      params = list(
        as.integer(p$game_year),
        if (!is.na(p$start_d)) as.Date(p$start_d) else NA,
        if (!is.na(p$end_d)) as.Date(p$end_d) else NA,
        game_type_csv,
        opp_ids_csv,
        home_away,
        outcome,
        opp_rank_side,
        p$rank_n,
        opp_rank_metric,
        p$min_gn,
        p$max_gn,
        p$last_n_games,
        p$max_margin,
        p$margin_status,
        p$max_time_remaining,
        isTRUE(p$ot_margin_filter),
        p$num_starters_off,
        p$num_starters_def,
        p$num_starters_off_min,
        p$num_starters_off_max,
        p$num_starters_def_min,
        p$num_starters_def_max
      )
    )
  }

  # -------------------------------------------------------------
  # Tab 3: Team Ratings (Fully Expanded Logic)
  # -------------------------------------------------------------
  observeEvent(input$tr_reset, {
    log_tab3_state("before_reset")
    tryCatch({
      b <- shared$season_date_bounds(input$game_year %||% DEFAULT_GAME_YEAR)
      do_upd <- function(step, expr) {
        log_tab3(paste0("reset_step_start: ", step), level = "DEBUG")
        force(expr)
        log_tab3(paste0("reset_step_ok: ", step), level = "DEBUG")
      }
      do_upd("tr_view_mode", updateRadioButtons(session, "tr_view_mode", selected = "Summary"))
      do_upd("tr_trad_defense_mode", updateCheckboxInput(session, "tr_trad_defense_mode", value = FALSE))
      do_upd("tr_trad_display_mode", updateSelectInput(session, "tr_trad_display_mode", selected = "Per Game"))
      do_upd("tr_dates", updateDateRangeInput(session, "tr_dates", start = b$start, end = b$end))
      do_upd("tr_game_type", updateSelectizeInput(session, "tr_game_type", selected = character(0)))
      do_upd("tr_opponents", updateSelectizeInput(session, "tr_opponents", selected = character(0)))
      do_upd("tr_home_away", updateSelectInput(session, "tr_home_away", selected = ""))
      do_upd("tr_outcome", updateSelectInput(session, "tr_outcome", selected = ""))
      do_upd("tr_opp_rank", reset_opp_rank_inputs(session, "tr"))
      do_upd("tr_starters", reset_starters_inputs(session, "tr"))
      do_upd("tr_clutch", reset_clutch_inputs(session, "tr"))
      do_upd("tr_gn_last_n", reset_gn_last_n_inputs(session, "tr"))
      do_upd("tr_stat_filters", reset_stat_filters(tr_stat_filter_state))
    }, error = function(e) {
      log_tab3_error(paste0("tr_reset error: ", conditionMessage(e)))
      showNotification(paste("Reset failed:", conditionMessage(e)), type = "error", duration = 8)
    })
  })

  observeEvent(input$tr_view_mode, {
    reset_stat_filters(tr_stat_filter_state)
  }, ignoreInit = TRUE)

  tr_teams_for_year <- reactive({
    gy_int <- as.integer(input$game_year)
    req(gy_int)
    fetch_teams_distinct(gy_int)
  })

  # Year change always re-syncs the date range to that season's bounds
  # (matches every other date-bearing tab). The combined observer below only
  # refreshes choice pools so it never clobbers user-picked dates on tab switch.
  observeEvent(input$game_year, {
    b <- shared$season_date_bounds(input$game_year %||% DEFAULT_GAME_YEAR)
    updateDateRangeInput(session, "tr_dates", start = b$start, end = b$end, min = b$start, max = b$end)
  }, ignoreInit = FALSE)

  observeEvent(list(input$game_year, input$main_tabs), ignoreInit = FALSE, {
    if (!identical(input$main_tabs, "team_ratings")) return(NULL)
    req(input$game_year)

    td <- tr_teams_for_year()
    opponent_choices <- stats::setNames(as.character(td$team_id), as.character(td$team_name))
    updateSelectizeInput(
      session, "tr_opponents",
      choices = opponent_choices,
      selected = restore_aware_selection(
        session, "tr_opponents", isolate(input$tr_opponents), opponent_choices
      )
    )

    gy_int <- as.integer(input$game_year)
    gn_df <- fetch_gn_values(gy_int)
    gn_vals <- if (nrow(gn_df)) as.integer(gn_df$gn) else integer(0)
    update_gn_last_n_choices(session, "tr", gn_vals)
  })

  run_team_ratings_dynamic <- function(pool, game_year, start_d, end_d, game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric, max_margin, margin_status, max_time_remaining, ot_margin_filter, min_gn = NA_integer_, max_gn = NA_integer_, last_n_games = NA_integer_, num_starters_off = NA_integer_, num_starters_def = NA_integer_, num_starters_off_min = NA_integer_, num_starters_off_max = NA_integer_, num_starters_def_min = NA_integer_, num_starters_def_max = NA_integer_) {
    allowed <- guard_heavy_request(
      session, key = "tab3_team_summary",
      start_d = start_d, end_d = end_d,
      min_gn = min_gn, max_gn = max_gn, last_n = last_n_games,
      max_calls = 35L, window_sec = 60L
    )
    if (!isTRUE(allowed)) return(data.frame())
    db_get_query(pool, paste0("SELECT * FROM basketball_test.get_team_ratings_dynamic(", "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,$7::text,$8::text,$9::int4,$10::text,$11::int4,$12::text,$13::int4,$14::bool,$15::int4,$16::int4,$17::int4,$18::int4,$19::int4,$20::int4,$21::int4,$22::int4,$23::int4", ")"), params = list(as.integer(game_year), if (!is.na(start_d)) as.Date(start_d) else NA, if (!is.na(end_d)) as.Date(end_d) else NA, game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric, max_margin, margin_status, max_time_remaining, ot_margin_filter, min_gn, max_gn, last_n_games, num_starters_off, num_starters_def, num_starters_off_min, num_starters_off_max, num_starters_def_min, num_starters_def_max))
  }

  run_team_ff_dynamic <- function(pool, game_year, start_d, end_d, game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric, max_margin, margin_status, max_time_remaining, ot_margin_filter, min_gn = NA_integer_, max_gn = NA_integer_, last_n_games = NA_integer_, num_starters_off = NA_integer_, num_starters_def = NA_integer_, num_starters_off_min = NA_integer_, num_starters_off_max = NA_integer_, num_starters_def_min = NA_integer_, num_starters_def_max = NA_integer_) {
    allowed <- guard_heavy_request(
      session, key = "tab3_team_ff",
      start_d = start_d, end_d = end_d,
      min_gn = min_gn, max_gn = max_gn, last_n = last_n_games,
      max_calls = 35L, window_sec = 60L
    )
    if (!isTRUE(allowed)) return(data.frame())
    db_get_query(pool, paste0("SELECT * FROM basketball_test.get_team_four_factors_dynamic(", "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,$7::text,$8::text,$9::int4,$10::text,$11::int4,$12::text,$13::int4,$14::bool,$15::int4,$16::int4,$17::int4,$18::int4,$19::int4,$20::int4,$21::int4,$22::int4,$23::int4", ")"), params = list(as.integer(game_year), if (!is.na(start_d)) as.Date(start_d) else NA, if (!is.na(end_d)) as.Date(end_d) else NA, game_type_csv, opp_ids_csv, home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric, max_margin, margin_status, max_time_remaining, ot_margin_filter, min_gn, max_gn, last_n_games, num_starters_off, num_starters_def, num_starters_off_min, num_starters_off_max, num_starters_def_min, num_starters_def_max))
  }

  run_team_traditional_dynamic <- function(pool, p, end_override = NA) {
    end_d <- if (is.na(end_override)) p$end_d else as.Date(end_override)
    start_d <- p$start_d
    if (!is.na(end_d) && !is.na(start_d) && end_d < start_d) return(data.frame())
    allowed <- guard_heavy_request(
      session, key = "tab3_team_traditional",
      start_d = start_d, end_d = end_d,
      min_gn = p$min_gn, max_gn = p$max_gn, last_n = p$last_n_games,
      max_calls = 35L, window_sec = 60L
    )
    if (!isTRUE(allowed)) return(data.frame())

    db_get_query(
      pool,
      "WITH params AS (
         SELECT
           CASE WHEN $4::text IS NULL OR btrim($4::text) = '' THEN NULL::int4[]
                ELSE string_to_array(regexp_replace($4::text, '\\s+', '', 'g'), ',')::int4[] END AS game_types,
           CASE WHEN $5::text IS NULL OR btrim($5::text) = '' THEN NULL::int4[]
                ELSE string_to_array(regexp_replace($5::text, '\\s+', '', 'g'), ',')::int4[] END AS opp_ids
       ),
       games_base AS (
         SELECT
           fs.game_id, fs.team_id, fs.game_year, fs.opp_team_id, fs.game_date,
           ROW_NUMBER() OVER (PARTITION BY fs.team_id, fs.game_year ORDER BY fs.game_date DESC NULLS LAST, fs.game_id DESC) AS rn_recent
         FROM basketball_test.final_schedule_mv fs
         CROSS JOIN params p0
         WHERE fs.game_year = $1::int4
           AND ($2::date IS NULL OR fs.game_date >= $2::date)
           AND ($3::date IS NULL OR fs.game_date <= $3::date)
           AND (p0.game_types IS NULL OR fs.game_type = ANY(p0.game_types))
           AND (p0.opp_ids IS NULL OR fs.opp_team_id = ANY(p0.opp_ids))
           AND ($6::text IS NULL OR $6::text = '' OR ($6::text = 'home' AND fs.is_home) OR ($6::text = 'away' AND NOT fs.is_home))
           AND ($7::text IS NULL OR $7::text = '' OR ($7::text = 'win' AND fs.has_won IS TRUE) OR ($7::text = 'loss' AND fs.has_won IS FALSE))
           AND ($15::int4 IS NULL OR fs.gn >= $15::int4)
           AND ($16::int4 IS NULL OR fs.gn <= $16::int4)
       ),
       games_last_n AS (
         SELECT *
         FROM games_base
         WHERE ($17::int4 IS NULL OR rn_recent <= $17::int4)
       ),
       games_ranked AS (
         SELECT
           gb.game_id, gb.team_id, gb.game_year,
           CASE
             WHEN $8::text IN ('top','bottom') THEN
               CASE COALESCE($10::text, 'net')
                 WHEN 'off' THEN r.rank_off_ppp
                 WHEN 'def' THEN r.rank_def_ppp
                 ELSE r.rank_net_rtg
               END
             ELSE NULL
           END AS opp_rank,
           CASE
             WHEN $8::text = 'bottom' THEN
               MAX(
                 CASE COALESCE($10::text, 'net')
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
          AND $8::text IN ('top','bottom')
       ),
       games_filtered AS (
         SELECT gr.game_id, gr.team_id
         FROM games_ranked gr
         WHERE $8::text IS NULL OR $8::text = '' OR $9::int4 IS NULL
            OR ($8::text = 'top' AND gr.opp_rank <= $9::int4)
            OR ($8::text = 'bottom' AND gr.opp_rank >= (gr.max_rank - $9::int4 + 1))
       ),
       acts AS (
          SELECT d.id, d.game_id, d.team_id, d.lineup_hash, d.segment_id, d.end_game_seconds_remaining, d.event_elapsed_seconds,
                 d.type, d.parameters_type, d.parameters_made, d.parameters_points, d.event_owner_side, d.type_lineup, d.final_end_poss, d.quarter,
                 d.own_team_score, d.opp_team_score
         FROM basketball_test.df_pts_poss_lineups_longer_mv d
         JOIN games_filtered gf ON gf.game_id = d.game_id AND gf.team_id = d.team_id
         WHERE (
           $11::int4 IS NULL
           OR ABS(COALESCE(d.own_team_score, 0) - COALESCE(d.opp_team_score, 0)) <= $11::int4
           OR (d.quarter > 4 AND NOT COALESCE($14::bool, FALSE))
         )
         AND (
           $12::text IS NULL OR $12::text = '' OR $12::text = 'all'
           OR ($12::text = 'leading' AND COALESCE(d.own_team_score, 0) > COALESCE(d.opp_team_score, 0))
           OR ($12::text = 'trailing' AND COALESCE(d.own_team_score, 0) < COALESCE(d.opp_team_score, 0))
           OR ($12::text = 'tied' AND COALESCE(d.own_team_score, 0) = COALESCE(d.opp_team_score, 0))
           OR (d.quarter > 4 AND NOT COALESCE($14::bool, FALSE))
         )
         AND ($13::int4 IS NULL OR d.end_game_seconds_remaining <= $13::int4 OR d.quarter > 4)
       ),
       team_stats AS (
         SELECT
           a.team_id,
           (
              SUM(CASE WHEN a.type = 'shot' AND a.parameters_made = 'made' AND (
                     ($18::text = 'offense' AND a.type_lineup = 'offense') OR
                     ($18::text = 'defense' AND a.type_lineup = 'defense')
                  ) THEN COALESCE(a.parameters_points, 0) ELSE 0 END)
              + SUM(CASE WHEN a.type = 'freeThrow' AND a.parameters_made = 'made' AND (
                     ($18::text = 'offense' AND a.type_lineup = 'offense') OR
                     ($18::text = 'defense' AND a.type_lineup = 'defense')
                  ) THEN 1 ELSE 0 END)
             )::int AS pts,
             SUM(CASE WHEN a.type = 'rebound' AND (
                     ($18::text = 'offense' AND a.parameters_type = 'offensive' AND a.type_lineup = 'offense') OR
                     ($18::text = 'defense' AND a.parameters_type = 'offensive' AND a.type_lineup = 'defense')
                   ) THEN 1 ELSE 0 END)::int AS oreb,
             SUM(CASE WHEN a.type = 'rebound' AND (
                     ($18::text = 'offense' AND a.parameters_type = 'defensive' AND a.type_lineup = 'defense') OR
                     ($18::text = 'defense' AND a.parameters_type = 'defensive' AND a.type_lineup = 'offense')
                   ) THEN 1 ELSE 0 END)::int AS dreb,
            SUM(CASE WHEN a.type = 'assist' AND (
                     ($18::text = 'offense' AND a.type_lineup = 'offense') OR
                     ($18::text = 'defense' AND a.type_lineup = 'defense')
                   ) THEN 1 ELSE 0 END)::int AS ast,
             SUM(CASE WHEN a.type = 'steal' AND (
                      ($18::text = 'offense' AND a.type_lineup = 'defense') OR
                      ($18::text = 'defense' AND a.type_lineup = 'offense')
                    ) THEN 1 ELSE 0 END)::int AS stl,
             SUM(CASE WHEN a.type = 'block' AND (
                      ($18::text = 'offense' AND a.type_lineup = 'defense') OR
                      ($18::text = 'defense' AND a.type_lineup = 'offense')
                    ) THEN 1 ELSE 0 END)::int AS blk,
             SUM(CASE WHEN a.type = 'deflection' AND (
                      ($18::text = 'offense' AND a.type_lineup = 'defense') OR
                      ($18::text = 'defense' AND a.type_lineup = 'offense')
                    ) THEN 1 ELSE 0 END)::int AS dfl,
            SUM(CASE WHEN a.type = 'turnover' AND (
                     ($18::text = 'offense' AND a.type_lineup = 'offense') OR
                     ($18::text = 'defense' AND a.type_lineup = 'defense')
                   ) THEN 1 ELSE 0 END)::int AS tov,
            SUM(CASE WHEN a.type = 'shot' AND a.parameters_made = 'made' AND (
                     ($18::text = 'offense' AND a.type_lineup = 'offense') OR
                     ($18::text = 'defense' AND a.type_lineup = 'defense')
                   ) THEN 1 ELSE 0 END)::int AS fgm,
            SUM(CASE WHEN a.type = 'shot' AND (
                     ($18::text = 'offense' AND a.type_lineup = 'offense') OR
                     ($18::text = 'defense' AND a.type_lineup = 'defense')
                   ) THEN 1 ELSE 0 END)::int AS fga,
            SUM(CASE WHEN a.type = 'shot' AND a.parameters_made = 'made' AND a.parameters_points = 3 AND (
                     ($18::text = 'offense' AND a.type_lineup = 'offense') OR
                     ($18::text = 'defense' AND a.type_lineup = 'defense')
                   ) THEN 1 ELSE 0 END)::int AS \"3pm\",
            SUM(CASE WHEN a.type = 'shot' AND a.parameters_points = 3 AND (
                     ($18::text = 'offense' AND a.type_lineup = 'offense') OR
                     ($18::text = 'defense' AND a.type_lineup = 'defense')
                   ) THEN 1 ELSE 0 END)::int AS \"3pa\",
            SUM(CASE WHEN a.type = 'freeThrow' AND a.parameters_made = 'made' AND (
                     ($18::text = 'offense' AND a.type_lineup = 'offense') OR
                     ($18::text = 'defense' AND a.type_lineup = 'defense')
                   ) THEN 1 ELSE 0 END)::int AS ftm,
            SUM(CASE WHEN a.type = 'freeThrow' AND (
                     ($18::text = 'offense' AND a.type_lineup = 'offense') OR
                     ($18::text = 'defense' AND a.type_lineup = 'defense')
                   ) THEN 1 ELSE 0 END)::int AS fta
         FROM acts a
         GROUP BY a.team_id
       ),
       poss_end AS (
         SELECT DISTINCT a.game_id, a.team_id, a.id AS poss_end_id
         FROM acts a
         WHERE a.type_lineup = $18::text AND a.final_end_poss AND a.id IS NOT NULL
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
           a.team_id, a.game_id, a.lineup_hash, a.segment_id,
           GREATEST(
             (array_agg(a.event_elapsed_seconds ORDER BY a.id DESC))[1] -
             (array_agg(a.event_elapsed_seconds ORDER BY a.id))[1],
             0
           )::numeric AS seg_seconds
         FROM acts a
         WHERE a.lineup_hash IS NOT NULL AND a.segment_id IS NOT NULL AND a.event_elapsed_seconds IS NOT NULL
         GROUP BY a.team_id, a.game_id, a.lineup_hash, a.segment_id
       ),
       team_minutes AS (
         SELECT st.team_id, ROUND(SUM(COALESCE(st.seg_seconds, 0))::numeric / 60.0, 1) AS minutes
         FROM seg_times st
         GROUP BY st.team_id
       ),
       team_names AS (
         SELECT fr.team_id, MIN(btrim(fr.team_name)) AS team_name
         FROM basketball_test.full_rosters fr
         WHERE fr.game_year = $1::int4
         GROUP BY fr.team_id
       )
       SELECT
         ts.team_id, tn.team_name,
         COALESCE(tu.gp, 0)::int AS gp,
         COALESCE(tu.poss_on_floor, 0)::int AS poss_on_floor,
         COALESCE(tm.minutes, 0)::numeric AS minutes,
         ts.pts, (ts.oreb + ts.dreb)::int AS reb, ts.oreb, ts.dreb, ts.ast, ts.stl, ts.blk, ts.dfl, ts.tov,
         ts.fgm, ts.fga, (ts.fgm - ts.\"3pm\")::int AS \"2pm\", (ts.fga - ts.\"3pa\")::int AS \"2pa\",
         ts.\"3pm\", ts.\"3pa\", ts.ftm, ts.fta,
         CASE WHEN ts.fga > 0 THEN ROUND((ts.fgm::numeric / ts.fga::numeric) * 100, 1) ELSE NULL END AS fg_pct,
         CASE WHEN (ts.fga - ts.\"3pa\") > 0 THEN ROUND(((ts.fgm - ts.\"3pm\")::numeric / (ts.fga - ts.\"3pa\")::numeric) * 100, 1) ELSE NULL END AS two_pct,
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
        p$game_year, p$start_d, end_d,
        p$game_type_csv, p$opp_ids_csv, p$home_away, p$outcome,
        p$rank_side, p$rank_n, p$metric,
        p$max_margin, p$margin_status, p$max_time_remaining, p$ot_margin_filter,
        p$min_gn, p$max_gn, p$last_n_games, p$trad_side
      )
    )
  }

  apply_tr_trad_mode <- function(df, mode) {
    if (is.null(df) || !nrow(df)) return(df)
    count_cols <- c("pts", "reb", "oreb", "dreb", "ast", "stl", "blk", "dfl", "tov", "fgm", "fga", "2pm", "2pa", "3pm", "3pa", "ftm", "fta")
    mode <- mode %||% "Per Game"
    if (identical(mode, "Per Game")) {
      for (col in count_cols) if (col %in% names(df)) df[[col]] <- ifelse(df$gp > 0, df[[col]] / df$gp, NA_real_)
      if ("poss_on_floor" %in% names(df)) df$poss_on_floor <- ifelse(df$gp > 0, df$poss_on_floor / df$gp, NA_real_)
      if ("minutes" %in% names(df)) df$minutes <- ifelse(df$gp > 0, df$minutes / df$gp, NA_real_)
      return(df)
    }
    if (identical(mode, "Per 75 Possessions")) {
      for (col in count_cols) if (col %in% names(df)) df[[col]] <- ifelse(df$poss_on_floor > 0, df[[col]] / df$poss_on_floor * 75, NA_real_)
      if ("minutes" %in% names(df)) df$minutes <- ifelse(df$poss_on_floor > 0, df$minutes / df$poss_on_floor * 75, NA_real_)
      if ("poss_on_floor" %in% names(df)) df$poss_on_floor <- ifelse(df$poss_on_floor > 0, 75, NA_real_)
      return(df)
    }
    if (identical(mode, "Per 40 Minutes")) {
      for (col in count_cols) if (col %in% names(df)) df[[col]] <- ifelse(df$minutes > 0, df[[col]] / df$minutes * 40, NA_real_)
      if ("poss_on_floor" %in% names(df)) df$poss_on_floor <- ifelse(df$minutes > 0, df$poss_on_floor / df$minutes * 40, NA_real_)
      return(df)
    }
    df
  }

  tr_params <- reactive({
    default_params <- function() {
      gy <- suppressWarnings(as.integer(input$game_year))
      if (!is.finite(gy) || is.na(gy)) gy <- suppressWarnings(as.integer(DEFAULT_GAME_YEAR))
      if (!is.finite(gy) || is.na(gy)) gy <- 2026L
      b <- shared$season_date_bounds(as.character(gy))
      list(
        game_year = gy, start_d = as.Date(b$start), end_d = as.Date(b$end),
        game_type_csv = NA_character_, opp_ids_csv = NA_character_,
        home_away = NA_character_, outcome = NA_character_, rank_side = NA_character_,
        rank_n = NA_integer_, metric = NA_character_, max_margin = NA_integer_,
        margin_status = NA_character_, max_time_remaining = NA_integer_, ot_margin_filter = FALSE,
        min_gn = NA_integer_, max_gn = NA_integer_, last_n_games = NA_integer_,
        num_starters_off = NA_integer_, num_starters_def = NA_integer_,
        num_starters_off_min = NA_integer_, num_starters_off_max = NA_integer_,
        num_starters_def_min = NA_integer_, num_starters_def_max = NA_integer_,
        trad_side = "offense"
      )
    }
    tryCatch({
      gy <- as.integer(input$game_year)
      req(gy)
      start_d <- tr_date_part(input$tr_dates, 1L)
      end_d <- tr_date_part(input$tr_dates, 2L)
      filters <- game_context_filter_values(input, "tr")
      context <- game_context_db_args(
        filters, resolve_gn_last_n_params(input, "tr"),
        integerize_opponents = TRUE
      )
      clutch <- resolve_clutch_params(
        enabled = input$tr_clutch_enabled,
        margin = input$tr_clutch_margin,
        status = input$tr_clutch_status,
        minutes = input$tr_clutch_minutes,
        ot_margin = input$tr_clutch_ot_margin
      )
      trad_side <- if (isTRUE(input$tr_trad_defense_mode)) "defense" else "offense"

      list(game_year = gy, start_d = start_d, end_d = end_d,
           game_type_csv = context$game_type_csv,
           opp_ids_csv = context$opp_ids_csv,
           home_away = context$home_away, outcome = context$outcome,
           rank_side = context$opp_rank_side, rank_n = context$opp_rank_n,
           metric = context$opp_rank_metric,
           max_margin = clutch$max_margin, margin_status = clutch$margin_status,
           max_time_remaining = clutch$max_time_remaining,
           ot_margin_filter = clutch$ot_margin_filter,
           min_gn = context$min_gn, max_gn = context$max_gn,
           last_n_games = context$last_n_games,
           num_starters_off = NA_integer_, num_starters_def = NA_integer_,
           num_starters_off_min = context$num_starters_off_min,
           num_starters_off_max = context$num_starters_off_max,
           num_starters_def_min = context$num_starters_def_min,
           num_starters_def_max = context$num_starters_def_max,
           trad_side = trad_side)
    }, error = function(e) {
      log_tab3_error(paste0("tr_params error: ", conditionMessage(e)))
      tb <- tryCatch(paste(capture.output(sys.calls()), collapse = " || "), error = function(err) "")
      if (nzchar(tb)) log_tab3_error(paste0("tr_params calls: ", tb))
      default_params()
    })
  }) %>% debounce(300)

  tr_delta_enabled <- reactive({
    p <- tr_params()
    bounds <- shared$season_date_bounds(as.character(p$game_year))
    has_custom_gn <- !is.na(p$min_gn) || !is.na(p$max_gn) || !is.na(p$last_n_games)
    if (has_custom_gn) return(FALSE)
    # Baseline-only:
    # - both dates empty (default), or
    # - exactly season bounds.
    if (is.na(p$start_d) && is.na(p$end_d)) return(TRUE)
    if (is.na(p$start_d) || is.na(p$end_d)) return(FALSE)
    identical(as.Date(p$start_d), as.Date(bounds$start)) && identical(as.Date(p$end_d), as.Date(bounds$end))
  })

  tr_csv_slug <- function(x) {
    x <- tolower(as.character(x %||% ""))
    x <- gsub("[^a-z0-9]+", "_", x)
    x <- gsub("^_+|_+$", "", x)
    if (!nzchar(x)) "table" else x
  }

  tr_csv_button <- function(mode) {
    mode <- mode %||% "Summary"
    mode_slug <- tr_csv_slug(mode)
    button_text <- "Download CSV"

    if (identical(mode, "Traditional")) {
      side_slug <- if (isTRUE(input$tr_trad_defense_mode)) "opponent" else "team"
      display_slug <- tr_csv_slug(input$tr_trad_display_mode %||% "Per Game")
      mode_slug <- paste("traditional", side_slug, display_slug, sep = "_")
      button_text <- if (identical(side_slug, "opponent")) "Download Opponent CSV" else "Download Team CSV"
    }

    list(
      list(
        extend = "csv",
        text = button_text,
        filename = sprintf("team_ratings_%s_%s", mode_slug, csv_export_stamp()),
        exportOptions = list(
          columns = ":visible",
          stripHtml = TRUE,
          stripNewlines = TRUE,
          trim = TRUE,
          modifier = list(search = "applied", order = "applied")
        )
      )
    )
  }

  tr_effective_anchor <- reactive({
    p <- tr_params()
    end_d <- if (is.na(p$end_d)) shared$season_date_bounds(as.character(p$game_year))$end else as.Date(p$end_d)
    q <- tryCatch(
      db_get_query(
        pg_pool,
        "WITH params AS (
           SELECT
             CASE WHEN $4::text IS NULL OR btrim($4::text) = '' THEN NULL::int4[]
                  ELSE string_to_array(regexp_replace($4::text, '\\s+', '', 'g'), ',')::int4[] END AS game_types,
             CASE WHEN $5::text IS NULL OR btrim($5::text) = '' THEN NULL::int4[]
                  ELSE string_to_array(regexp_replace($5::text, '\\s+', '', 'g'), ',')::int4[] END AS opp_ids
         )
         SELECT
           MAX(fs.game_date)::date AS max_game_date,
           MAX(fs.gn)::int AS max_gn
         FROM basketball_test.final_schedule_mv fs
         CROSS JOIN params p0
         WHERE fs.game_year = $1::int4
           AND ($2::date IS NULL OR fs.game_date >= $2::date)
           AND ($3::date IS NULL OR fs.game_date <= $3::date)
           AND (p0.game_types IS NULL OR fs.game_type = ANY(p0.game_types))
           AND (p0.opp_ids IS NULL OR fs.opp_team_id = ANY(p0.opp_ids))
           AND ($6::text IS NULL OR $6::text = '' OR ($6::text = 'home' AND fs.is_home) OR ($6::text = 'away' AND NOT fs.is_home))
           AND ($7::text IS NULL OR $7::text = '' OR ($7::text = 'win' AND fs.has_won IS TRUE) OR ($7::text = 'loss' AND fs.has_won IS FALSE))
           AND ($8::int4 IS NULL OR fs.gn >= $8::int4)
           AND ($9::int4 IS NULL OR fs.gn <= $9::int4)",
        params = list(
          p$game_year, p$start_d, end_d, p$game_type_csv, p$opp_ids_csv,
          p$home_away, p$outcome, p$min_gn, p$max_gn
        )
      ),
      error = function(e) NULL
    )
    if (is.null(q) || !nrow(q)) {
      return(list(end_date = end_d, max_gn = NA_integer_))
    }
    list(
      end_date = if (!is.na(q$max_game_date[1])) as.Date(q$max_game_date[1]) else end_d,
      max_gn = suppressWarnings(as.integer(q$max_gn[1]))
    )
  })

  tr_prev_match_end <- reactive({
    if (!isTRUE(tr_delta_enabled())) return(as.Date(NA))
    p <- tr_params()
    anchor <- tr_effective_anchor()
    end_d <- anchor$end_date
    q <- tryCatch(
      db_get_query(
        pg_pool,
        "SELECT MAX(game_date)::date AS d
         FROM basketball_test.final_schedule_mv
         WHERE game_year = $1::int4
           AND game_date < $2::date",
        params = list(p$game_year, end_d)
      ),
      error = function(e) NULL
    )
    if (is.null(q) || !nrow(q) || is.na(q$d[1])) as.Date(NA) else as.Date(q$d[1])
  })

  setup_gn_last_n_sync(session, input, "tr")

  tr_fallback_needed <- reactive({
    p <- tr_params()
    bounds <- shared$season_date_bounds(as.character(p$game_year))
    is_static_ui_default <- !is.na(p$start_d) && !is.na(p$end_d) &&
      identical(as.Date(p$start_d), as.Date(DEFAULT_START)) &&
      identical(as.Date(p$end_d), as.Date(DEFAULT_END))
    has_dates <- {
      if (is.na(p$start_d) && is.na(p$end_d)) {
        FALSE
      } else if (is_static_ui_default) {
        FALSE
      } else if (is.na(p$start_d) || is.na(p$end_d)) {
        TRUE
      } else {
        !(identical(as.Date(p$start_d), as.Date(bounds$start)) &&
            identical(as.Date(p$end_d), as.Date(bounds$end)))
      }
    }
    has_gt <- !is.na(p$game_type_csv)
    has_opp <- !is.na(p$opp_ids_csv)
    has_ha <- !is.na(p$home_away)
    has_out <- !is.na(p$outcome)
    has_rank <- !is.na(p$rank_side) || !is.na(p$rank_n)
    has_clutch <- !is.na(p$max_margin) || (!is.na(p$margin_status) && p$margin_status != "all") || !is.na(p$max_time_remaining)
    has_gn <- !is.na(p$min_gn) || !is.na(p$max_gn) || !is.na(p$last_n_games)
    has_starters <- !is.na(p$num_starters_off_min) || !is.na(p$num_starters_off_max) || !is.na(p$num_starters_def_min) || !is.na(p$num_starters_def_max)
    has_dates || has_gt || has_opp || has_ha || has_out || has_rank || has_clutch || has_gn || has_starters
  })

  tr_data <- reactive({
    p <- tr_params()
    if (tr_fallback_needed()) {
      run_team_ratings_dynamic(pg_pool, game_year = p$game_year, start_d = p$start_d, end_d = p$end_d, game_type_csv = p$game_type_csv, opp_ids_csv = p$opp_ids_csv, home_away = p$home_away, outcome = p$outcome, opp_rank_side = p$rank_side, opp_rank_n = p$rank_n, opp_rank_metric = p$metric, max_margin = p$max_margin, margin_status = p$margin_status, max_time_remaining = p$max_time_remaining, ot_margin_filter = p$ot_margin_filter, min_gn = p$min_gn, max_gn = p$max_gn, last_n_games = p$last_n_games, num_starters_off = p$num_starters_off, num_starters_def = p$num_starters_def, num_starters_off_min = p$num_starters_off_min, num_starters_off_max = p$num_starters_off_max, num_starters_def_min = p$num_starters_def_min, num_starters_def_max = p$num_starters_def_max)
    } else {
      db_get_query(pg_pool,
        "SELECT game_year, team_id, team_name, off_ppp, def_ppp, net_rtg,
                games_played, wins, losses, off_poss, def_poss,
                rank_net_rtg, rank_off_ppp, rank_def_ppp,
                off_fga, off_layup_att, off_dunk_att, off_fg3_att, off_c3_att, off_c3_known_att,
                def_fga, def_layup_att, def_dunk_att, def_fg3_att, def_c3_att, def_c3_known_att
           FROM basketball_test.team_ppp_ratings_mv
          WHERE game_year = $1::int4
          ORDER BY rank_net_rtg",
        params = list(as.integer(p$game_year)))
    }
  })

  tr_ff_data <- reactive({
    p <- tr_params()
    if (tr_fallback_needed()) {
      df <- run_team_ff_dynamic(pg_pool, game_year = p$game_year, start_d = p$start_d, end_d = p$end_d, game_type_csv = p$game_type_csv, opp_ids_csv = p$opp_ids_csv, home_away = p$home_away, outcome = p$outcome, opp_rank_side = p$rank_side, opp_rank_n = p$rank_n, opp_rank_metric = p$metric, max_margin = p$max_margin, margin_status = p$margin_status, max_time_remaining = p$max_time_remaining, ot_margin_filter = p$ot_margin_filter, min_gn = p$min_gn, max_gn = p$max_gn, last_n_games = p$last_n_games, num_starters_off = p$num_starters_off, num_starters_def = p$num_starters_def, num_starters_off_min = p$num_starters_off_min, num_starters_off_max = p$num_starters_off_max, num_starters_def_min = p$num_starters_def_min, num_starters_def_max = p$num_starters_def_max)
    } else {
      df <- db_get_query(pg_pool,
        "SELECT *
           FROM basketball_test.team_four_factors_mv
          WHERE game_year = $1::int4",
        params = list(as.integer(p$game_year)))
    }

    if (is.null(df) || nrow(df) == 0) return(df)

    # Compute percentile ranks - all teams qualify (>>100 poss).
    add_team_metric_ranks(df)
  })

  tr_prev_data <- reactive({
    prev_end <- tr_prev_match_end()
    if (is.na(prev_end)) return(NULL)
    p <- tr_params()
    tryCatch(
      run_team_ratings_dynamic(
        pg_pool, game_year = p$game_year, start_d = p$start_d, end_d = prev_end,
        game_type_csv = p$game_type_csv, opp_ids_csv = p$opp_ids_csv, home_away = p$home_away,
        outcome = p$outcome, opp_rank_side = p$rank_side, opp_rank_n = p$rank_n, opp_rank_metric = p$metric,
        max_margin = p$max_margin, margin_status = p$margin_status, max_time_remaining = p$max_time_remaining,
        ot_margin_filter = p$ot_margin_filter, min_gn = p$min_gn, max_gn = p$max_gn, last_n_games = p$last_n_games,
        num_starters_off = p$num_starters_off, num_starters_def = p$num_starters_def,
        num_starters_off_min = p$num_starters_off_min, num_starters_off_max = p$num_starters_off_max,
        num_starters_def_min = p$num_starters_def_min, num_starters_def_max = p$num_starters_def_max
      ),
      error = function(e) NULL
    )
  })

  tr_prev_summary_ranks_from_mv <- reactive({
    if (isTRUE(tr_fallback_needed())) return(NULL)
    if (!isTRUE(tr_delta_enabled())) return(NULL)
    p <- tr_params()
    bounds <- shared$season_date_bounds(as.character(p$game_year))
    start_d <- if (!is.na(p$start_d)) as.Date(p$start_d) else as.Date(bounds$start)
    end_d <- if (!is.na(p$end_d)) as.Date(p$end_d) else as.Date(bounds$end)
    roll <- tryCatch(
      db_get_query(
        pg_pool,
        "WITH scoped AS (
           SELECT tm.*
           FROM basketball_test.team_metrics_by_game_mv tm
           WHERE tm.game_year = $1::int4
             AND tm.game_date >= $2::date
             AND tm.game_date <= $3::date
         ),
         team_games AS (
           SELECT
             g.team_id, g.game_id, g.game_date,
             ROW_NUMBER() OVER (PARTITION BY g.team_id ORDER BY g.game_date DESC, g.game_id DESC) AS rn
           FROM (SELECT DISTINCT team_id, game_id, game_date FROM scoped) g
         ),
         cuts AS (
           SELECT
             tg.team_id,
             MAX(tg.game_date) FILTER (WHERE tg.rn = 2) AS prev_game_date,
             MAX(tg.game_id)   FILTER (WHERE tg.rn = 2) AS prev_game_id
           FROM team_games tg
           GROUP BY tg.team_id
           HAVING MAX(CASE WHEN tg.rn = 2 THEN 1 ELSE 0 END) = 1
         )
         SELECT
           s.team_id,
           COUNT(DISTINCT s.game_id)::int AS gp,
           COALESCE(SUM(s.off_poss), 0)::numeric AS poss_on_floor,
           COALESCE(SUM(s.off_minutes), 0)::numeric AS minutes,
           COALESCE(SUM(s.off_points_raw), 0)::numeric AS off_points_raw,
           COALESCE(SUM(s.def_points_raw), 0)::numeric AS def_points_raw,
           COALESCE(SUM(s.off_poss_raw), 0)::numeric AS off_poss_raw,
           COALESCE(SUM(s.def_poss_raw), 0)::numeric AS def_poss_raw,
           COALESCE(SUM(s.off_ts_poss_raw), 0)::numeric AS off_ts_poss_raw,
           COALESCE(SUM(s.def_ts_poss_raw), 0)::numeric AS def_ts_poss_raw,
           COALESCE(SUM(s.off_oreb_count_raw), 0)::numeric AS off_oreb_count_raw,
           COALESCE(SUM(s.def_oreb_count_raw), 0)::numeric AS def_oreb_count_raw,
           COALESCE(SUM(s.off_oreb_opp_raw), 0)::numeric AS off_oreb_opp_raw,
           COALESCE(SUM(s.def_oreb_opp_raw), 0)::numeric AS def_oreb_opp_raw,
           COALESCE(SUM(s.off_tov_raw), 0)::numeric AS off_tov_raw,
           COALESCE(SUM(s.def_tov_raw), 0)::numeric AS def_tov_raw,
           COALESCE(SUM(s.off_fta_raw), 0)::numeric AS off_fta_raw,
           COALESCE(SUM(s.def_fta_raw), 0)::numeric AS def_fta_raw,
           COALESCE(SUM(s.off_fga_raw), 0)::numeric AS off_fga_raw,
           COALESCE(SUM(s.def_fga_raw), 0)::numeric AS def_fga_raw,
           COALESCE(SUM(s.pts), 0)::numeric AS pts,
           COALESCE(SUM(s.reb), 0)::numeric AS reb,
           COALESCE(SUM(s.ast), 0)::numeric AS ast,
           COALESCE(SUM(s.stl), 0)::numeric AS stl,
           COALESCE(SUM(s.blk), 0)::numeric AS blk,
           COALESCE(SUM(s.tov), 0)::numeric AS tov,
           COALESCE(SUM(s.fgm), 0)::numeric AS fgm,
           COALESCE(SUM(s.fga), 0)::numeric AS fga,
           COALESCE(SUM(s.\"3pm\"), 0)::numeric AS \"3pm\",
           COALESCE(SUM(s.\"3pa\"), 0)::numeric AS \"3pa\",
           COALESCE(SUM(s.ftm), 0)::numeric AS ftm,
           COALESCE(SUM(s.fta), 0)::numeric AS fta
         FROM scoped s
         JOIN cuts c
           ON c.team_id = s.team_id
          AND (s.game_date < c.prev_game_date OR (s.game_date = c.prev_game_date AND s.game_id <= c.prev_game_id))
         GROUP BY s.team_id",
        params = list(as.integer(p$game_year), as.Date(start_d), as.Date(end_d))
      ),
      error = function(e) NULL
    )
    if (is.null(roll) || !nrow(roll)) return(NULL)
    roll$off_ppp <- ifelse(roll$off_poss_raw > 0, round((roll$off_points_raw / roll$off_poss_raw) * 100, 1), NA_real_)
    roll$def_ppp <- ifelse(roll$def_poss_raw > 0, round((roll$def_points_raw / roll$def_poss_raw) * 100, 1), NA_real_)
    roll$net_rtg <- round(roll$off_ppp - roll$def_ppp, 1)
    list(
      off = setNames(dplyr::min_rank(dplyr::desc(roll$off_ppp)), as.character(roll$team_id)),
      def = setNames(dplyr::min_rank(roll$def_ppp), as.character(roll$team_id)),
      net = setNames(dplyr::min_rank(dplyr::desc(roll$net_rtg)), as.character(roll$team_id))
    )
  })

  tr_prev_ff_ranks_from_mv <- reactive({
    if (isTRUE(tr_fallback_needed())) return(NULL)
    if (!isTRUE(tr_delta_enabled())) return(NULL)
    p <- tr_params()
    bounds <- shared$season_date_bounds(as.character(p$game_year))
    start_d <- if (!is.na(p$start_d)) as.Date(p$start_d) else as.Date(bounds$start)
    end_d <- if (!is.na(p$end_d)) as.Date(p$end_d) else as.Date(bounds$end)
    roll <- tryCatch(
      db_get_query(
        pg_pool,
        "WITH scoped AS (
           SELECT tm.*
           FROM basketball_test.team_metrics_by_game_mv tm
           WHERE tm.game_year = $1::int4
             AND tm.game_date >= $2::date
             AND tm.game_date <= $3::date
         ),
         team_games AS (
           SELECT
             g.team_id, g.game_id, g.game_date,
             ROW_NUMBER() OVER (PARTITION BY g.team_id ORDER BY g.game_date DESC, g.game_id DESC) AS rn
           FROM (SELECT DISTINCT team_id, game_id, game_date FROM scoped) g
         ),
         cuts AS (
           SELECT
             tg.team_id,
             MAX(tg.game_date) FILTER (WHERE tg.rn = 2) AS prev_game_date,
             MAX(tg.game_id)   FILTER (WHERE tg.rn = 2) AS prev_game_id
           FROM team_games tg
           GROUP BY tg.team_id
           HAVING MAX(CASE WHEN tg.rn = 2 THEN 1 ELSE 0 END) = 1
         )
         SELECT
           s.team_id,
           COALESCE(SUM(s.off_points_raw), 0)::numeric AS off_points_raw,
           COALESCE(SUM(s.def_points_raw), 0)::numeric AS def_points_raw,
           COALESCE(SUM(s.off_poss_raw), 0)::numeric AS off_poss_raw,
           COALESCE(SUM(s.def_poss_raw), 0)::numeric AS def_poss_raw,
           COALESCE(SUM(s.off_ts_poss_raw), 0)::numeric AS off_ts_poss_raw,
           COALESCE(SUM(s.def_ts_poss_raw), 0)::numeric AS def_ts_poss_raw,
           COALESCE(SUM(s.off_oreb_count_raw), 0)::numeric AS off_oreb_count_raw,
           COALESCE(SUM(s.def_oreb_count_raw), 0)::numeric AS def_oreb_count_raw,
           COALESCE(SUM(s.off_oreb_opp_raw), 0)::numeric AS off_oreb_opp_raw,
           COALESCE(SUM(s.def_oreb_opp_raw), 0)::numeric AS def_oreb_opp_raw,
           COALESCE(SUM(s.off_tov_raw), 0)::numeric AS off_tov_raw,
           COALESCE(SUM(s.def_tov_raw), 0)::numeric AS def_tov_raw,
           COALESCE(SUM(s.off_fta_raw), 0)::numeric AS off_fta_raw,
           COALESCE(SUM(s.def_fta_raw), 0)::numeric AS def_fta_raw,
           COALESCE(SUM(s.off_fga_raw), 0)::numeric AS off_fga_raw,
           COALESCE(SUM(s.def_fga_raw), 0)::numeric AS def_fga_raw,
           COALESCE(SUM(s.off_fgm_raw), 0)::numeric AS off_fgm_raw,
           COALESCE(SUM(s.def_fgm_raw), 0)::numeric AS def_fgm_raw,
           COALESCE(SUM(s.off_fg3m_raw), 0)::numeric AS off_fg3m_raw,
           COALESCE(SUM(s.def_fg3m_raw), 0)::numeric AS def_fg3m_raw
         FROM scoped s
         JOIN cuts c
           ON c.team_id = s.team_id
          AND (s.game_date < c.prev_game_date OR (s.game_date = c.prev_game_date AND s.game_id <= c.prev_game_id))
         GROUP BY s.team_id",
        params = list(as.integer(p$game_year), as.Date(start_d), as.Date(end_d))
      ),
      error = function(e) NULL
    )
    if (is.null(roll) || !nrow(roll)) return(NULL)
    ff <- data.frame(team_id = roll$team_id)
    ff$off_ppp  <- ifelse(roll$off_poss_raw > 0, round((roll$off_points_raw / roll$off_poss_raw) * 100, 1), NA_real_)
    ff$def_ppp  <- ifelse(roll$def_poss_raw > 0, round((roll$def_points_raw / roll$def_poss_raw) * 100, 1), NA_real_)
    ff$net_rtg  <- round(ff$off_ppp - ff$def_ppp, 1)
    ff$off_efg  <- ifelse(roll$off_fga_raw > 0, round((roll$off_fgm_raw + 0.5 * roll$off_fg3m_raw) / roll$off_fga_raw * 100, 1), NA_real_)
    ff$off_oreb <- ifelse(roll$off_oreb_opp_raw > 0, round((roll$off_oreb_count_raw / roll$off_oreb_opp_raw) * 100, 1), NA_real_)
    ff$off_tov  <- ifelse(roll$off_poss_raw > 0, round((roll$off_tov_raw / roll$off_poss_raw) * 100, 1), NA_real_)
    ff$off_ftr  <- ifelse(roll$off_fga_raw > 0, round((roll$off_fta_raw / roll$off_fga_raw) * 100, 1), NA_real_)
    ff$def_efg  <- ifelse(roll$def_fga_raw > 0, round((roll$def_fgm_raw + 0.5 * roll$def_fg3m_raw) / roll$def_fga_raw * 100, 1), NA_real_)
    ff$def_oreb <- ifelse(roll$def_oreb_opp_raw > 0, round((roll$def_oreb_count_raw / roll$def_oreb_opp_raw) * 100, 1), NA_real_)
    ff$def_tov  <- ifelse(roll$def_poss_raw > 0, round((roll$def_tov_raw / roll$def_poss_raw) * 100, 1), NA_real_)
    ff$def_ftr  <- ifelse(roll$def_fga_raw > 0, round((roll$def_fta_raw / roll$def_fga_raw) * 100, 1), NA_real_)

    list(
      off = setNames(dplyr::min_rank(dplyr::desc(ff$off_ppp)), as.character(ff$team_id)),
      def = setNames(dplyr::min_rank(ff$def_ppp), as.character(ff$team_id)),
      net = setNames(dplyr::min_rank(dplyr::desc(ff$net_rtg)), as.character(ff$team_id)),
      off_efg = setNames(dplyr::min_rank(dplyr::desc(ff$off_efg)), as.character(ff$team_id)),
      off_oreb = setNames(dplyr::min_rank(dplyr::desc(ff$off_oreb)), as.character(ff$team_id)),
      off_tov = setNames(dplyr::min_rank(ff$off_tov), as.character(ff$team_id)),
      off_ftr = setNames(dplyr::min_rank(dplyr::desc(ff$off_ftr)), as.character(ff$team_id)),
      def_efg = setNames(dplyr::min_rank(ff$def_efg), as.character(ff$team_id)),
      def_oreb = setNames(dplyr::min_rank(ff$def_oreb), as.character(ff$team_id)),
      def_tov = setNames(dplyr::min_rank(dplyr::desc(ff$def_tov)), as.character(ff$team_id)),
      def_ftr = setNames(dplyr::min_rank(ff$def_ftr), as.character(ff$team_id))
    )
  })

  tr_prev_traditional_ranks_from_mv <- reactive({
    p <- tr_params()
    if (identical(p$trad_side, "defense")) return(NULL)
    if (isTRUE(tr_fallback_needed())) return(NULL)
    if (!isTRUE(tr_delta_enabled())) return(NULL)
    bounds <- shared$season_date_bounds(as.character(p$game_year))
    start_d <- if (!is.na(p$start_d)) as.Date(p$start_d) else as.Date(bounds$start)
    end_d <- if (!is.na(p$end_d)) as.Date(p$end_d) else as.Date(bounds$end)
    mode <- input$tr_trad_display_mode %||% "Per Game"
    q <- tryCatch(
      db_get_query(
        pg_pool,
        "WITH scoped AS (
           SELECT tm.*
           FROM basketball_test.team_metrics_by_game_mv tm
           WHERE tm.game_year = $1::int4
             AND tm.game_date >= $2::date
             AND tm.game_date <= $3::date
         ),
         team_games AS (
           SELECT
             g.team_id, g.game_id, g.game_date,
             ROW_NUMBER() OVER (PARTITION BY g.team_id ORDER BY g.game_date DESC, g.game_id DESC) AS rn
           FROM (SELECT DISTINCT team_id, game_id, game_date FROM scoped) g
         ),
         cuts AS (
           SELECT
             tg.team_id,
             MAX(tg.game_date) FILTER (WHERE tg.rn = 2) AS prev_game_date,
             MAX(tg.game_id)   FILTER (WHERE tg.rn = 2) AS prev_game_id
           FROM team_games tg
           GROUP BY tg.team_id
           HAVING MAX(CASE WHEN tg.rn = 2 THEN 1 ELSE 0 END) = 1
         )
         SELECT
           s.team_id,
           COUNT(DISTINCT s.game_id)::int AS gp,
           COALESCE(SUM(s.off_poss), 0)::numeric AS poss_on_floor,
           COALESCE(SUM(s.off_minutes), 0)::numeric AS minutes,
           COALESCE(SUM(s.pts), 0)::numeric AS pts,
           COALESCE(SUM(s.reb), 0)::numeric AS reb,
           COALESCE(SUM(s.ast), 0)::numeric AS ast,
           COALESCE(SUM(s.stl), 0)::numeric AS stl,
           COALESCE(SUM(s.blk), 0)::numeric AS blk,
           COALESCE(SUM(s.dfl), 0)::numeric AS dfl,
           COALESCE(SUM(s.tov), 0)::numeric AS tov,
           COALESCE(SUM(s.fgm), 0)::numeric AS fgm,
           COALESCE(SUM(s.fga), 0)::numeric AS fga,
           COALESCE(SUM(s.\"3pm\"), 0)::numeric AS \"3pm\",
           COALESCE(SUM(s.\"3pa\"), 0)::numeric AS \"3pa\",
           COALESCE(SUM(s.ftm), 0)::numeric AS ftm,
           COALESCE(SUM(s.fta), 0)::numeric AS fta
         FROM scoped s
         JOIN cuts c
           ON c.team_id = s.team_id
          AND (s.game_date < c.prev_game_date OR (s.game_date = c.prev_game_date AND s.game_id <= c.prev_game_id))
         GROUP BY s.team_id",
        params = list(as.integer(p$game_year), as.Date(start_d), as.Date(end_d))
      ),
      error = function(e) NULL
    )
    if (is.null(q) || !nrow(q)) return(NULL)
    minutes_q <- tryCatch(
      db_get_query(
        pg_pool,
        "WITH acts AS (
           WITH scoped AS (
             SELECT tm.team_id, tm.game_id, tm.game_date
             FROM basketball_test.team_metrics_by_game_mv tm
             WHERE tm.game_year = $1::int4
               AND tm.game_date >= $2::date
               AND tm.game_date <= $3::date
           ),
           team_games AS (
             SELECT
               g.team_id, g.game_id, g.game_date,
               ROW_NUMBER() OVER (PARTITION BY g.team_id ORDER BY g.game_date DESC, g.game_id DESC) AS rn
             FROM (SELECT DISTINCT team_id, game_id, game_date FROM scoped) g
           ),
           cuts AS (
             SELECT
               tg.team_id,
               MAX(tg.game_date) FILTER (WHERE tg.rn = 2) AS prev_game_date,
               MAX(tg.game_id)   FILTER (WHERE tg.rn = 2) AS prev_game_id
             FROM team_games tg
             GROUP BY tg.team_id
             HAVING MAX(CASE WHEN tg.rn = 2 THEN 1 ELSE 0 END) = 1
           )
           SELECT d.team_id, d.game_id, d.lineup_hash, d.segment_id, d.segment_seconds
           FROM basketball_test.df_pts_poss_lineups_longer_mv d
           JOIN scoped s
             ON s.game_id = d.game_id
            AND s.team_id = d.team_id
           JOIN cuts c
             ON c.team_id = s.team_id
            AND (s.game_date < c.prev_game_date OR (s.game_date = c.prev_game_date AND s.game_id <= c.prev_game_id))
         ),
         seg_times AS (
           SELECT
             a.team_id, a.game_id, a.lineup_hash, a.segment_id,
             MAX(a.segment_seconds) AS seg_seconds
           FROM acts a
           WHERE a.lineup_hash IS NOT NULL
             AND a.segment_id IS NOT NULL
             AND a.segment_seconds IS NOT NULL
           GROUP BY a.team_id, a.game_id, a.lineup_hash, a.segment_id
         )
         SELECT
           st.team_id,
           ROUND(SUM(COALESCE(st.seg_seconds, 0))::numeric / 60.0, 1) AS minutes
         FROM seg_times st
         GROUP BY st.team_id",
        params = list(as.integer(p$game_year), as.Date(start_d), as.Date(end_d))
      ),
      error = function(e) NULL
    )
    if (!is.null(minutes_q) && nrow(minutes_q)) {
      min_map <- setNames(as.numeric(minutes_q$minutes), as.character(minutes_q$team_id))
      q$minutes <- as.numeric(min_map[as.character(q$team_id)])
      q$minutes[is.na(q$minutes)] <- 0
    }
    q$`2pm` <- q$fgm - q$`3pm`
    q$`2pa` <- q$fga - q$`3pa`
    q$fg_pct <- ifelse(q$fga > 0, round((q$fgm / q$fga) * 100, 1), NA_real_)
    q$two_pct <- ifelse(q$`2pa` > 0, round((q$`2pm` / q$`2pa`) * 100, 1), NA_real_)
    q$tp_pct <- ifelse(q$`3pa` > 0, round((q$`3pm` / q$`3pa`) * 100, 1), NA_real_)
    q$ft_pct <- ifelse(q$fta > 0, round((q$ftm / q$fta) * 100, 1), NA_real_)
    q$efg <- ifelse(q$fga > 0, round(((q$fgm + 0.5 * q$`3pm`) / q$fga) * 100, 1), NA_real_)
    q$ts <- ifelse((q$fga + 0.44 * q$fta) > 0, round((q$pts / (2 * (q$fga + 0.44 * q$fta))) * 100, 1), NA_real_)
    q <- apply_tr_trad_mode(q, mode)

    metric_cfg <- c(
      pts = FALSE, reb = FALSE, ast = FALSE, stl = FALSE, blk = FALSE, dfl = FALSE, tov = TRUE,
      fgm = FALSE, fga = FALSE, `2pm` = FALSE, `2pa` = FALSE, `3pm` = FALSE, `3pa` = FALSE, ftm = FALSE, fta = FALSE,
      fg_pct = FALSE, two_pct = FALSE, tp_pct = FALSE, ft_pct = FALSE, efg = FALSE, ts = FALSE
    )
    ranks <- list()
    for (m in names(metric_cfg)) {
      if (!m %in% names(q)) next
      inv <- isTRUE(metric_cfg[[m]])
      rk <- if (inv) dplyr::min_rank(q[[m]]) else dplyr::min_rank(dplyr::desc(q[[m]]))
      ranks[[m]] <- setNames(rk, as.character(q$team_id))
    }
    ranks
  })

  tr_prev_ff_data <- reactive({
    prev_end <- tr_prev_match_end()
    if (is.na(prev_end)) return(NULL)
    p <- tr_params()
    tryCatch(
      run_team_ff_dynamic(
        pg_pool, game_year = p$game_year, start_d = p$start_d, end_d = prev_end,
        game_type_csv = p$game_type_csv, opp_ids_csv = p$opp_ids_csv, home_away = p$home_away,
        outcome = p$outcome, opp_rank_side = p$rank_side, opp_rank_n = p$rank_n, opp_rank_metric = p$metric,
        max_margin = p$max_margin, margin_status = p$margin_status, max_time_remaining = p$max_time_remaining,
        ot_margin_filter = p$ot_margin_filter, min_gn = p$min_gn, max_gn = p$max_gn, last_n_games = p$last_n_games,
        num_starters_off = p$num_starters_off, num_starters_def = p$num_starters_def,
        num_starters_off_min = p$num_starters_off_min, num_starters_off_max = p$num_starters_off_max,
        num_starters_def_min = p$num_starters_def_min, num_starters_def_max = p$num_starters_def_max
      ),
      error = function(e) NULL
    )
  })

  tr_traditional_data <- reactive({
    p <- tr_params()
    df <- tryCatch(run_team_traditional_dynamic(pg_pool, p), error = function(e) NULL)
    if (is.null(df) || !nrow(df)) return(df)
    apply_tr_trad_mode(df, input$tr_trad_display_mode %||% "Per Game")
  })

  tr_prev_traditional_data <- reactive({
    prev_end <- tr_prev_match_end()
    if (is.na(prev_end)) return(NULL)
    p <- tr_params()
    df <- tryCatch(run_team_traditional_dynamic(pg_pool, p, end_override = prev_end), error = function(e) NULL)
    if (is.null(df) || !nrow(df)) return(df)
    apply_tr_trad_mode(df, input$tr_trad_display_mode %||% "Per Game")
  })

  tr_game_minutes <- reactive({
    p <- tr_params()
    fetch_team_game_minutes(pg_pool, p)
  })

  output$tr_table <- renderDT({
    input$tr_traditional_visible_col_order_restore
    mode <- input$tr_view_mode
    mins_map <- NULL
    if (!identical(mode, "Traditional")) {
      mins_df <- tryCatch(tr_game_minutes(), error = function(e) NULL)
      if (is.data.frame(mins_df) && nrow(mins_df) && all(c("team_id", "game_minutes") %in% names(mins_df))) {
        mins_map <- setNames(suppressWarnings(as.numeric(mins_df$game_minutes)), as.character(mins_df$team_id))
      }
    }
    show_delta <- isTRUE(tr_delta_enabled())
    empty_dt <- function(msg = "No data returned for current filters") {
      DT::datatable(
        data.frame(Info = msg, check.names = FALSE),
        rownames = FALSE,
        options = list(headerCallback = HEADER_TOOLTIP_JS, dom = "t")
      )
    }

    tryCatch({
    if (identical(mode, "Traditional")) {
      df <- tr_traditional_data()
      if (is.null(df) || !nrow(df)) return(empty_dt("Traditional: no data for current filters"))
      is_defense_trad <- identical((tr_params()$trad_side %||% "offense"), "defense")
      df <- apply_stat_filters(df, tr_stat_filter_state$filters())
      if (is.null(df) || !nrow(df)) return(empty_dt("Traditional: no rows match stat filters"))

      rank_vec_local <- function(x, invert = FALSE) {
        if (invert) dplyr::min_rank(x) else dplyr::min_rank(dplyr::desc(x))
      }
      metric_cfg <- if (!is_defense_trad) {
        c(
          pts = FALSE, reb = FALSE, oreb = FALSE, dreb = FALSE, ast = FALSE, stl = FALSE, blk = FALSE, dfl = FALSE, tov = TRUE,
          fgm = FALSE, fga = FALSE, `2pm` = FALSE, `2pa` = FALSE, `3pm` = FALSE, `3pa` = FALSE, ftm = FALSE, fta = FALSE,
          fg_pct = FALSE, two_pct = FALSE, tp_pct = FALSE, ft_pct = FALSE, efg = FALSE, ts = FALSE
        )
      } else {
        c(
          pts = TRUE, reb = TRUE, oreb = TRUE, dreb = TRUE, ast = TRUE, stl = TRUE, blk = TRUE, dfl = TRUE, tov = FALSE,
          fgm = TRUE, fga = TRUE, `2pm` = TRUE, `2pa` = TRUE, `3pm` = TRUE, `3pa` = TRUE, ftm = TRUE, fta = TRUE,
          fg_pct = TRUE, two_pct = TRUE, tp_pct = TRUE, ft_pct = TRUE, efg = TRUE, ts = TRUE
        )
      }
      for (m in names(metric_cfg)) {
        if (!m %in% names(df)) next
        inv <- isTRUE(metric_cfg[[m]])
        df[[paste0("rank_", m)]] <- rank_vec_local(df[[m]], invert = inv)
        df[[paste0("pr_", m)]] <- pr_vec(df[[m]], invert = inv)
      }

      prev_rank_map <- tr_prev_traditional_ranks_from_mv()
      if (is.null(prev_rank_map)) {
        prev <- if (isTRUE(tr_delta_enabled())) tr_prev_traditional_data() else NULL
        prev_rank_map <- list()
        if (!is.null(prev) && nrow(prev)) {
          for (m in names(metric_cfg)) {
            if (!m %in% names(prev)) next
            inv <- isTRUE(metric_cfg[[m]])
            rk <- rank_vec_local(prev[[m]], invert = inv)
            prev_rank_map[[m]] <- setNames(rk, as.character(prev$team_id))
          }
        }
      }
      make_cell <- function(vals, ranks_now, metric_name) {
        prev_r <- if (!is.null(prev_rank_map[[metric_name]])) as.integer(prev_rank_map[[metric_name]][as.character(df$team_id)]) else rep(NA_integer_, nrow(df))
        delta <- prev_r - as.integer(ranks_now)
        fmt_rank_cell(vals, ranks_now, delta, digits = 1, show_delta)
      }

      disp <- data.frame(
        Team = df$team_name,
        GP = df$gp,
        `Poss On Floor` = df$poss_on_floor,
        Min = df$minutes,
        PTS = make_cell(df$pts, df$rank_pts, "pts"),
        REB = make_cell(df$reb, df$rank_reb, "reb"),
        OREB = make_cell(df$oreb, df$rank_oreb, "oreb"),
        DREB = make_cell(df$dreb, df$rank_dreb, "dreb"),
        AST = make_cell(df$ast, df$rank_ast, "ast"),
        STL = make_cell(df$stl, df$rank_stl, "stl"),
        BLK = make_cell(df$blk, df$rank_blk, "blk"),
        DFL = make_cell(df$dfl, df$rank_dfl, "dfl"),
        TOV = make_cell(df$tov, df$rank_tov, "tov"),
        FGM = make_cell(df$fgm, df$rank_fgm, "fgm"),
        FGA = make_cell(df$fga, df$rank_fga, "fga"),
        `FG%` = make_cell(df$fg_pct, df$rank_fg_pct, "fg_pct"),
        `2PM` = make_cell(df$`2pm`, df$`rank_2pm`, "2pm"),
        `2PA` = make_cell(df$`2pa`, df$`rank_2pa`, "2pa"),
        `2P%` = make_cell(df$two_pct, df$rank_two_pct, "two_pct"),
        `3PM` = make_cell(df$`3pm`, df$`rank_3pm`, "3pm"),
        `3PA` = make_cell(df$`3pa`, df$`rank_3pa`, "3pa"),
        `3P%` = make_cell(df$tp_pct, df$rank_tp_pct, "tp_pct"),
        FTM = make_cell(df$ftm, df$rank_ftm, "ftm"),
        FTA = make_cell(df$fta, df$rank_fta, "fta"),
        `FT%` = make_cell(df$ft_pct, df$rank_ft_pct, "ft_pct"),
        `eFG%` = make_cell(df$efg, df$rank_efg, "efg"),
        `TS%` = make_cell(df$ts, df$rank_ts, "ts"),
        pr_pts = df$pr_pts, pr_reb = df$pr_reb, pr_oreb = df$pr_oreb, pr_dreb = df$pr_dreb, pr_ast = df$pr_ast, pr_stl = df$pr_stl, pr_blk = df$pr_blk, pr_dfl = df$pr_dfl, pr_tov = df$pr_tov,
        pr_fgm = df$pr_fgm, pr_fga = df$pr_fga, pr_2pm = df$pr_2pm, pr_2pa = df$pr_2pa, pr_3pm = df$pr_3pm, pr_3pa = df$pr_3pa, pr_ftm = df$pr_ftm, pr_fta = df$pr_fta,
        pr_fg_pct = df$pr_fg_pct, pr_two_pct = df$pr_two_pct, pr_tp_pct = df$pr_tp_pct, pr_ft_pct = df$pr_ft_pct, pr_efg = df$pr_efg, pr_ts = df$pr_ts,
        check.names = FALSE
      )
      sort_map <- c(
        PTS = "pts", REB = "reb", OREB = "oreb", DREB = "dreb", AST = "ast", STL = "stl", BLK = "blk", DFL = "dfl",
        TOV = "tov", FGM = "fgm", FGA = "fga", `FG%` = "fg_pct", `2PM` = "2pm", `2PA` = "2pa",
        `2P%` = "two_pct", `3PM` = "3pm", `3PA` = "3pa",
        `3P%` = "tp_pct", FTM = "ftm", FTA = "fta", `FT%` = "ft_pct",
        `eFG%` = "efg", `TS%` = "ts"
      )
      sort_dir_map <- if (!is_defense_trad) {
        c(
          PTS = "desc", REB = "desc", OREB = "desc", DREB = "desc", AST = "desc", STL = "desc", BLK = "desc", DFL = "desc",
          TOV = "asc", FGM = "desc", FGA = "desc", `FG%` = "desc", `2PM` = "desc", `2PA` = "desc",
          `2P%` = "desc", `3PM` = "desc", `3PA` = "desc",
          `3P%` = "desc", FTM = "desc", FTA = "desc", `FT%` = "desc",
          `eFG%` = "desc", `TS%` = "desc"
        )
      } else {
        c(
          PTS = "asc", REB = "asc", OREB = "asc", DREB = "asc", AST = "asc", STL = "asc", BLK = "asc", DFL = "asc",
          TOV = "desc", FGM = "asc", FGA = "asc", `FG%` = "asc", `2PM` = "asc", `2PA` = "asc",
          `2P%` = "asc", `3PM` = "asc", `3PA` = "asc",
          `3P%` = "asc", FTM = "asc", FTA = "asc", `FT%` = "asc",
          `eFG%` = "asc", `TS%` = "asc"
        )
      }
      for (nm in names(sort_map)) {
        sort_col <- paste0("sort__", make.names(nm))
        vals <- suppressWarnings(as.numeric(df[[sort_map[[nm]]]]))
        if (identical(sort_dir_map[[nm]], "desc")) {
          vals[is.na(vals)] <- -Inf
        } else {
          vals[is.na(vals)] <- Inf
        }
        disp[[sort_col]] <- vals
      }
      pr_map <- c(
        PTS = "pr_pts", REB = "pr_reb", OREB = "pr_oreb", DREB = "pr_dreb", AST = "pr_ast", STL = "pr_stl", BLK = "pr_blk", DFL = "pr_dfl",
        TOV = "pr_tov", FGM = "pr_fgm", FGA = "pr_fga", `FG%` = "pr_fg_pct", `2PM` = "pr_2pm", `2PA` = "pr_2pa",
        `2P%` = "pr_two_pct", `3PM` = "pr_3pm", `3PA` = "pr_3pa",
        `3P%` = "pr_tp_pct", FTM = "pr_ftm", FTA = "pr_fta", `FT%` = "pr_ft_pct",
        `eFG%` = "pr_efg", `TS%` = "pr_ts"
      )
      sort_col_names <- paste0("sort__", make.names(names(sort_map)))
      hidden_cols <- c(unname(pr_map), sort_col_names)
      disp <- apply_visible_col_order(disp, isolate(input$tr_traditional_visible_col_order), hidden_cols)

      sort_order_defs <- lapply(names(sort_map), function(nm) {
        dir_best <- sort_dir_map[[nm]]
        dir_seq <- if (identical(dir_best, "desc")) list("desc", "asc") else list("asc", "desc")
        list(
          targets = which(names(disp) == nm) - 1L,
          orderData = which(names(disp) == paste0("sort__", make.names(nm))) - 1L,
          orderSequence = dir_seq
        )
      })
      hidden_targets <- which(names(disp) %in% hidden_cols) - 1L

      dt <- datatable(
        disp, rownames = FALSE,
        escape = dt_escape_except(disp, names(sort_map)),
        extensions = c("Buttons", "ColReorder"),
        options = list(
          headerCallback = HEADER_TOOLTIP_JS,
          initComplete = dt_col_order_init_callback("tr_traditional_visible_col_order", "onoff.traditional_team.visible_col_order.v1"),
          colReorder = TRUE,
          dom = "Btip",
          buttons = tr_csv_button(mode),
          pageLength = 50, deferRender = TRUE, scrollX = TRUE, scrollY = "70vh", scrollCollapse = TRUE,
          columnDefs = c(list(
            list(className = "dt-center", targets = "_all"),
            list(visible = FALSE, targets = hidden_targets)
          ), sort_order_defs)
        )
      ) %>%
        formatRound(c("GP", "Poss On Floor", "Min"), 1)

      for (nm in names(pr_map)) {
        pr_col <- pr_map[[nm]]
        if (nm %in% names(disp) && pr_col %in% names(disp)) {
          dt <- DT::formatStyle(dt, nm, backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = pr_col)
        }
      }
      return(dt)

    } else if (identical(mode, "Four Factors")) {
      # ============================================================
      # FOUR FACTORS TEAM TABLE
      # ============================================================
      df <- tr_ff_data()
      if (is.null(df) || nrow(df) == 0) return(empty_dt("Four Factors: no data for current filters"))

      pr_cols <- c("pr_off_ppp", "pr_off_efg", "pr_off_oreb", "pr_off_tov", "pr_off_ftr",
                   "pr_def_ppp", "pr_def_efg", "pr_def_oreb", "pr_def_tov", "pr_def_ftr", "pr_net")

      keep_cols <- c("team_name",
                     "off_ppp", "off_efg", "off_oreb", "off_tov", "off_ftr",
                     "off_poss",
                     "def_ppp", "def_efg", "def_oreb", "def_tov", "def_ftr",
                     "def_poss",
                     "net_rtg")
      df <- add_team_pace_cols(df, minutes_map = mins_map)
      df <- apply_stat_filters(df, tr_stat_filter_state$filters())
      if (is.null(df) || !nrow(df)) return(empty_dt("Four Factors: no rows match stat filters"))

      # Rank deltas vs last matchday (baseline scope only), and rank labels in-cell
      rk_off_now <- dplyr::min_rank(dplyr::desc(df$off_ppp))
      rk_def_now <- dplyr::min_rank(df$def_ppp)
      rk_net_now <- dplyr::min_rank(dplyr::desc(df$net_rtg))
      rk_off_efg_now <- dplyr::min_rank(dplyr::desc(df$off_efg))
      rk_off_oreb_now <- dplyr::min_rank(dplyr::desc(df$off_oreb))
      rk_off_tov_now <- dplyr::min_rank(df$off_tov)
      rk_off_ftr_now <- dplyr::min_rank(dplyr::desc(df$off_ftr))
      rk_def_efg_now <- dplyr::min_rank(df$def_efg)
      rk_def_oreb_now <- dplyr::min_rank(df$def_oreb)
      rk_def_tov_now <- dplyr::min_rank(dplyr::desc(df$def_tov))
      rk_def_ftr_now <- dplyr::min_rank(df$def_ftr)

      d_off <- rep(NA_integer_, nrow(df))
      d_def <- rep(NA_integer_, nrow(df))
      d_net <- rep(NA_integer_, nrow(df))
      d_off_efg <- rep(NA_integer_, nrow(df))
      d_off_oreb <- rep(NA_integer_, nrow(df))
      d_off_tov <- rep(NA_integer_, nrow(df))
      d_off_ftr <- rep(NA_integer_, nrow(df))
      d_def_efg <- rep(NA_integer_, nrow(df))
      d_def_oreb <- rep(NA_integer_, nrow(df))
      d_def_tov <- rep(NA_integer_, nrow(df))
      d_def_ftr <- rep(NA_integer_, nrow(df))

      if (show_delta) {
        key <- as.character(df$team_id)
        mv_prev <- tr_prev_ff_ranks_from_mv()
        if (!is.null(mv_prev)) {
          d_off <- as.integer(mv_prev$off[key]) - as.integer(rk_off_now)
          d_def <- as.integer(mv_prev$def[key]) - as.integer(rk_def_now)
          d_net <- as.integer(mv_prev$net[key]) - as.integer(rk_net_now)
          d_off_efg <- as.integer(mv_prev$off_efg[key]) - as.integer(rk_off_efg_now)
          d_off_oreb <- as.integer(mv_prev$off_oreb[key]) - as.integer(rk_off_oreb_now)
          d_off_tov <- as.integer(mv_prev$off_tov[key]) - as.integer(rk_off_tov_now)
          d_off_ftr <- as.integer(mv_prev$off_ftr[key]) - as.integer(rk_off_ftr_now)
          d_def_efg <- as.integer(mv_prev$def_efg[key]) - as.integer(rk_def_efg_now)
          d_def_oreb <- as.integer(mv_prev$def_oreb[key]) - as.integer(rk_def_oreb_now)
          d_def_tov <- as.integer(mv_prev$def_tov[key]) - as.integer(rk_def_tov_now)
          d_def_ftr <- as.integer(mv_prev$def_ftr[key]) - as.integer(rk_def_ftr_now)
        } else {
          prev <- tr_prev_ff_data()
          if (!is.null(prev) && nrow(prev)) {
            rk_off_prev <- setNames(dplyr::min_rank(dplyr::desc(prev$off_ppp)), as.character(prev$team_id))
            rk_def_prev <- setNames(dplyr::min_rank(prev$def_ppp), as.character(prev$team_id))
            rk_net_prev <- setNames(dplyr::min_rank(dplyr::desc(prev$net_rtg)), as.character(prev$team_id))
            rk_off_efg_prev <- setNames(dplyr::min_rank(dplyr::desc(prev$off_efg)), as.character(prev$team_id))
            rk_off_oreb_prev <- setNames(dplyr::min_rank(dplyr::desc(prev$off_oreb)), as.character(prev$team_id))
            rk_off_tov_prev <- setNames(dplyr::min_rank(prev$off_tov), as.character(prev$team_id))
            rk_off_ftr_prev <- setNames(dplyr::min_rank(dplyr::desc(prev$off_ftr)), as.character(prev$team_id))
            rk_def_efg_prev <- setNames(dplyr::min_rank(prev$def_efg), as.character(prev$team_id))
            rk_def_oreb_prev <- setNames(dplyr::min_rank(prev$def_oreb), as.character(prev$team_id))
            rk_def_tov_prev <- setNames(dplyr::min_rank(dplyr::desc(prev$def_tov)), as.character(prev$team_id))
            rk_def_ftr_prev <- setNames(dplyr::min_rank(prev$def_ftr), as.character(prev$team_id))
            d_off <- as.integer(rk_off_prev[key]) - as.integer(rk_off_now)
            d_def <- as.integer(rk_def_prev[key]) - as.integer(rk_def_now)
            d_net <- as.integer(rk_net_prev[key]) - as.integer(rk_net_now)
            d_off_efg <- as.integer(rk_off_efg_prev[key]) - as.integer(rk_off_efg_now)
            d_off_oreb <- as.integer(rk_off_oreb_prev[key]) - as.integer(rk_off_oreb_now)
            d_off_tov <- as.integer(rk_off_tov_prev[key]) - as.integer(rk_off_tov_now)
            d_off_ftr <- as.integer(rk_off_ftr_prev[key]) - as.integer(rk_off_ftr_now)
            d_def_efg <- as.integer(rk_def_efg_prev[key]) - as.integer(rk_def_efg_now)
            d_def_oreb <- as.integer(rk_def_oreb_prev[key]) - as.integer(rk_def_oreb_now)
            d_def_tov <- as.integer(rk_def_tov_prev[key]) - as.integer(rk_def_tov_now)
            d_def_ftr <- as.integer(rk_def_ftr_prev[key]) - as.integer(rk_def_ftr_now)
          }
        }
      }

      df$off_ppp_lbl <- fmt_rank_cell(df$off_ppp, rk_off_now, d_off, 1, show_delta)
      df$off_efg_lbl <- fmt_rank_cell(df$off_efg, rk_off_efg_now, d_off_efg, 1, show_delta)
      df$off_oreb_lbl <- fmt_rank_cell(df$off_oreb, rk_off_oreb_now, d_off_oreb, 1, show_delta)
      df$off_tov_lbl <- fmt_rank_cell(df$off_tov, rk_off_tov_now, d_off_tov, 1, show_delta)
      df$off_ftr_lbl <- fmt_rank_cell(df$off_ftr, rk_off_ftr_now, d_off_ftr, 1, show_delta)
      df$def_ppp_lbl <- fmt_rank_cell(df$def_ppp, rk_def_now, d_def, 1, show_delta)
      df$def_efg_lbl <- fmt_rank_cell(df$def_efg, rk_def_efg_now, d_def_efg, 1, show_delta)
      df$def_oreb_lbl <- fmt_rank_cell(df$def_oreb, rk_def_oreb_now, d_def_oreb, 1, show_delta)
      df$def_tov_lbl <- fmt_rank_cell(df$def_tov, rk_def_tov_now, d_def_tov, 1, show_delta)
      df$def_ftr_lbl <- fmt_rank_cell(df$def_ftr, rk_def_ftr_now, d_def_ftr, 1, show_delta)
      df$net_rtg_lbl <- fmt_rank_cell(df$net_rtg, rk_net_now, d_net, 1, show_delta)

      df <- df %>% arrange(desc(net_rtg))
      disp_ff <- data.frame(
        team_name = df$team_name,
        minutes = df$minutes,
        off_ppp = df$off_ppp_lbl,
        off_efg = df$off_efg_lbl,
        off_oreb = df$off_oreb_lbl,
        off_tov = df$off_tov_lbl,
        off_ftr = df$off_ftr_lbl,
        off_poss = df$off_poss,
        def_ppp = df$def_ppp_lbl,
        def_efg = df$def_efg_lbl,
        def_oreb = df$def_oreb_lbl,
        def_tov = df$def_tov_lbl,
        def_ftr = df$def_ftr_lbl,
        def_poss = df$def_poss,
        net_rtg = df$net_rtg_lbl,
        pr_off_ppp = df$pr_off_ppp,
        pr_off_efg = df$pr_off_efg,
        pr_off_oreb = df$pr_off_oreb,
        pr_off_tov = df$pr_off_tov,
        pr_off_ftr = df$pr_off_ftr,
        pr_def_ppp = df$pr_def_ppp,
        pr_def_efg = df$pr_def_efg,
        pr_def_oreb = df$pr_def_oreb,
        pr_def_tov = df$pr_def_tov,
        pr_def_ftr = df$pr_def_ftr,
        pr_net = df$pr_net,
        check.names = FALSE
      )
      ff_sort_map <- c(
        off_ppp = "off_ppp", off_efg = "off_efg", off_oreb = "off_oreb", off_tov = "off_tov", off_ftr = "off_ftr",
        def_ppp = "def_ppp", def_efg = "def_efg", def_oreb = "def_oreb", def_tov = "def_tov", def_ftr = "def_ftr",
        net_rtg = "net_rtg"
      )
      ff_sort_dir_map <- c(
        off_ppp = "desc", off_efg = "desc", off_oreb = "desc", off_tov = "asc", off_ftr = "desc",
        def_ppp = "asc", def_efg = "asc", def_oreb = "asc", def_tov = "desc", def_ftr = "asc",
        net_rtg = "desc"
      )
      for (nm in names(ff_sort_map)) {
        vals <- suppressWarnings(as.numeric(df[[ff_sort_map[[nm]]]]))
        if (identical(ff_sort_dir_map[[nm]], "desc")) {
          vals[is.na(vals)] <- -Inf
        } else {
          vals[is.na(vals)] <- Inf
        }
        disp_ff[[paste0("sort__", nm)]] <- vals
      }
      ff_sort_order_defs <- lapply(names(ff_sort_map), function(nm) {
        dir_best <- ff_sort_dir_map[[nm]]
        dir_seq <- if (identical(dir_best, "desc")) list("desc", "asc") else list("asc", "desc")
        list(
          targets = which(names(disp_ff) == nm) - 1L,
          orderData = which(names(disp_ff) == paste0("sort__", nm)) - 1L,
          orderSequence = dir_seq
        )
      })

      sketch_ff <- htmltools::withTags(table(class = 'display', thead(
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
          th(class = "sub-head", title = OFF_OREB_TOOLTIP, "OREB%"), th(class = "sub-head", "TOV%"),
          th(class = "sub-head", "FTR"), th(class = "sub-head", "Poss"),
          th(class = "sub-head section-left-border", "PPP"), th(class = "sub-head", "eFG%"),
          th(class = "sub-head", title = DEF_OREB_TOOLTIP, "OREB%"), th(class = "sub-head", "TOV%"),
          th(class = "sub-head", "FTR"), th(class = "sub-head", "Poss"),
          th(class = "sub-head section-left-border", "Net")
        )
      )))

      hide_idx <- which(colnames(disp_ff) %in% pr_cols) - 1L
      off_ppp_idx <- which(names(disp_ff) == "off_ppp") - 1L
      def_ppp_idx <- which(names(disp_ff) == "def_ppp") - 1L
      net_idx     <- which(names(disp_ff) == "net_rtg") - 1L
      ff_sort_hide_idx <- which(grepl("^sort__", names(disp_ff))) - 1L

      col_defs <- list(
        list(targets = c(hide_idx, ff_sort_hide_idx), visible = FALSE),
        list(targets = "_all", className = "dt-center")
      )
      if (length(off_ppp_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = off_ppp_idx, className = "section-left-border dt-center")
      if (length(def_ppp_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = def_ppp_idx, className = "section-left-border dt-center")
      if (length(net_idx))     col_defs[[length(col_defs) + 1]] <- list(targets = net_idx, className = "section-left-border dt-center")
      col_defs <- c(col_defs, ff_sort_order_defs)

      dt <- DT::datatable(disp_ff, container = sketch_ff, rownames = FALSE,
                          escape = dt_escape_except(disp_ff, names(ff_sort_map)),
                          extensions = "Buttons",
                          options = list(
                            headerCallback = HEADER_TOOLTIP_JS,
                            dom = "Btip",
                            buttons = tr_csv_button(mode),
                            pageLength = 50,
                            deferRender = TRUE, scrollX = TRUE,
                            scrollY = "70vh", scrollCollapse = TRUE,
                            order = list(list(net_idx, "desc")),
                            columnDefs = col_defs
                          ))

      rate_cols <- intersect(c("off_efg", "off_oreb", "off_tov", "off_ftr", "def_efg", "def_oreb", "def_tov", "def_ftr"), names(disp_ff))
      ppp_cols  <- intersect(c("off_ppp", "def_ppp", "net_rtg"), names(disp_ff))
      poss_cols <- intersect(c("off_poss", "def_poss"), names(disp_ff))

      if (length(poss_cols)) dt <- DT::formatCurrency(dt, poss_cols, currency = "", interval = 3, mark = ",", digits = 0)
      if ("minutes" %in% names(disp_ff)) dt <- DT::formatRound(dt, "minutes", 1)

      # Color logic - same polarity as Tab 2 FF
      if ("pr_off_ppp"  %in% names(disp_ff)) dt <- DT::formatStyle(dt, "off_ppp",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_ppp")
      if ("pr_off_efg"  %in% names(disp_ff)) dt <- DT::formatStyle(dt, "off_efg",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_efg")
      if ("pr_off_oreb" %in% names(disp_ff)) dt <- DT::formatStyle(dt, "off_oreb", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_oreb")
      if ("pr_off_tov"  %in% names(disp_ff)) dt <- DT::formatStyle(dt, "off_tov",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_tov")
      if ("pr_off_ftr"  %in% names(disp_ff)) dt <- DT::formatStyle(dt, "off_ftr",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_ftr")
      if ("pr_def_ppp"  %in% names(disp_ff)) dt <- DT::formatStyle(dt, "def_ppp",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_def_ppp")
      if ("pr_def_efg"  %in% names(disp_ff)) dt <- DT::formatStyle(dt, "def_efg",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_def_efg")
      if ("pr_def_oreb" %in% names(disp_ff)) dt <- DT::formatStyle(dt, "def_oreb", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_def_oreb")
      if ("pr_def_tov"  %in% names(disp_ff)) dt <- DT::formatStyle(dt, "def_tov",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_def_tov")
      if ("pr_def_ftr"  %in% names(disp_ff)) dt <- DT::formatStyle(dt, "def_ftr",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_def_ftr")
      if ("pr_net"      %in% names(disp_ff)) dt <- DT::formatStyle(dt, "net_rtg",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_net")

      return(dt)

    } else if (identical(mode, "Shot Profile")) {
      # ============================================================
      # SHOT PROFILE TEAM TABLE (descriptive shot-diet shares)
      # ============================================================
      df <- tr_data()
      if (is.null(df) || nrow(df) == 0) return(empty_dt("Shot Profile: no data for current filters"))
      need <- c("off_fga", "off_layup_att", "off_c3_known_att", "def_fga")
      if (!all(need %in% names(df))) return(empty_dt("Shot Profile columns unavailable"))
      df <- add_team_pace_cols(df, minutes_map = mins_map)
      df <- add_shot_profile_metrics(df, list(
        off = c("off_layup_att", "off_dunk_att", "off_fga", "off_fg3_att", "off_c3_att", "off_c3_known_att"),
        def = c("def_layup_att", "def_dunk_att", "def_fga", "def_fg3_att", "def_c3_att", "def_c3_known_att")
      ))
      df <- apply_stat_filters(df, tr_stat_filter_state$filters())
      if (is.null(df) || !nrow(df)) return(empty_dt("Shot Profile: no rows match stat filters"))

      # eFG% context column (level — Tab 3 has no on/off split), joined from
      # the four-factors data (same filter params as tr_data()).
      df$off_efg <- NULL
      df$def_efg <- NULL
      ffd <- tr_ff_data()
      if (!is.null(ffd) && nrow(ffd) && all(c("team_id", "off_efg", "def_efg") %in% names(ffd))) {
        df <- dplyr::left_join(df, ffd[, c("team_id", "off_efg", "def_efg")], by = "team_id")
      } else {
        df$off_efg <- NA_real_
        df$def_efg <- NA_real_
      }

      # Rank = share magnitude order (descriptive: #1 = most of that shot
      # type). Exception: def_efg ranks ascending (#1 = stingiest defense).
      sp_cols <- c("off_efg", "def_efg",
                   as.vector(outer(c("off", "def"), SHOT_PROFILE_METRIC_SUFFIXES, paste0)))
      sp_disp <- gsub("_share$|_pct3$", "", sp_cols)  # off_efg, off_layup, off_c3, ...
      fmt_share_cell <- function(vals, ranks) {
        v <- suppressWarnings(as.numeric(vals))
        r <- suppressWarnings(as.integer(ranks))
        ifelse(is.na(v), "—",
               paste0(format(round(v, 1), nsmall = 1, trim = TRUE), "%<br>",
                      ifelse(is.na(r), "#NA", paste0("#", r))))
      }

      df <- df %>% arrange(desc(off_rim_share))
      disp_sp <- data.frame(team_name = df$team_name, minutes = df$minutes, check.names = FALSE)
      for (i in seq_along(sp_cols)) {
        v <- df[[sp_cols[i]]]
        rk <- if (identical(sp_cols[i], "def_efg")) dplyr::min_rank(v) else dplyr::min_rank(dplyr::desc(v))
        disp_sp[[sp_disp[i]]] <- fmt_share_cell(v, rk)
      }
      disp_sp$off_poss <- df$off_poss
      disp_sp$def_poss <- df$def_poss
      # column order: team, min, off block + poss, def block + poss
      off_block <- sp_disp[startsWith(sp_disp, "off_")]
      def_block <- sp_disp[startsWith(sp_disp, "def_")]
      disp_sp <- disp_sp[, c("team_name", "minutes", off_block, "off_poss", def_block, "def_poss")]

      for (i in seq_along(sp_cols)) {
        vals <- suppressWarnings(as.numeric(df[[sp_cols[i]]]))
        vals[is.na(vals)] <- -Inf
        disp_sp[[paste0("sort__", sp_disp[i])]] <- vals
      }

      # FF-style background coloring by share percentile. Value-hierarchy
      # polarity: eFG/interior/3PA/C3 green-high on offense, red-high on
      # defense; the 2PT Jumper column flips (like TOV% in Four Factors).
      pr_vec_sp <- function(x, invert = FALSE) {
        n <- sum(!is.na(x))
        if (n <= 1) return(rep(NA_real_, length(x)))
        r <- rank(x, na.last = "keep", ties.method = "average")
        p <- (r - 1) / (n - 1)
        if (invert) p <- 1 - p
        as.numeric(p)
      }
      for (i in seq_along(sp_cols)) {
        is_jumper <- grepl("_mid_share$", sp_cols[i])
        is_def <- startsWith(sp_cols[i], "def_")
        inv <- (is_def == !is_jumper)
        disp_sp[[paste0("pr_", sp_disp[i])]] <- pr_vec_sp(suppressWarnings(as.numeric(df[[sp_cols[i]]])), invert = inv)
      }
      sp_sort_defs <- lapply(sp_disp, function(nm) {
        list(
          targets = which(names(disp_sp) == nm) - 1L,
          orderData = which(names(disp_sp) == paste0("sort__", nm)) - 1L,
          orderSequence = list("desc", "asc")
        )
      })

      c3_title <- "Corner 3s as % of 3PA with known court location; — = location unknown"
      sketch_sp <- htmltools::withTags(table(class = "display", thead(
        tr(
          th(class = "group-head", colspan = 2, ""),
          th(class = "group-head section-left-border", colspan = 8, "Offense Shot Profile (eFG% + shares of FGA)"),
          th(class = "group-head section-left-border", colspan = 8, "Defense Shot Profile (eFG% + shares of FGA)")
        ),
        tr(
          th(class = "sub-head", "Team"), th(class = "sub-head", "Min"),
          th(class = "sub-head section-left-border", "eFG%"),
          th(class = "sub-head", "Lay-up"), th(class = "sub-head", "Dunk"),
          th(class = "sub-head", "Lay+Dunk"), th(class = "sub-head", "3PA"),
          th(class = "sub-head", title = c3_title, "Corner 3 Share"), th(class = "sub-head", "2PT Jumper"),
          th(class = "sub-head", "Poss"),
          th(class = "sub-head section-left-border", "eFG%"),
          th(class = "sub-head", "Lay-up"), th(class = "sub-head", "Dunk"),
          th(class = "sub-head", "Lay+Dunk"), th(class = "sub-head", "3PA"),
          th(class = "sub-head", title = c3_title, "Corner 3 Share"), th(class = "sub-head", "2PT Jumper"),
          th(class = "sub-head", "Poss")
        )
      )))

      sp_hide_idx <- which(grepl("^sort__|^pr_", names(disp_sp))) - 1L
      off_first_idx <- which(names(disp_sp) == "off_efg") - 1L
      def_first_idx <- which(names(disp_sp) == "def_efg") - 1L
      col_defs <- list(
        list(targets = sp_hide_idx, visible = FALSE),
        list(targets = "_all", className = "dt-center")
      )
      if (length(off_first_idx)) col_defs[[length(col_defs) + 1L]] <- list(targets = off_first_idx, className = "section-left-border dt-center")
      if (length(def_first_idx)) col_defs[[length(col_defs) + 1L]] <- list(targets = def_first_idx, className = "section-left-border dt-center")
      col_defs <- c(col_defs, sp_sort_defs)

      dt <- DT::datatable(disp_sp, container = sketch_sp, rownames = FALSE,
                          escape = dt_escape_except(disp_sp, sp_disp),
                          extensions = "Buttons",
                          options = list(
                            headerCallback = HEADER_TOOLTIP_JS,
                            dom = "Btip",
                            buttons = tr_csv_button(mode),
                            pageLength = 50, deferRender = TRUE, scrollX = TRUE,
                            scrollY = "70vh", scrollCollapse = TRUE,
                            order = list(list(which(names(disp_sp) == "off_rim") - 1L, "desc")),
                            columnDefs = col_defs
                          ))
      if ("minutes" %in% names(disp_sp)) dt <- DT::formatRound(dt, "minutes", 1)
      dt <- DT::formatCurrency(dt, intersect(c("off_poss", "def_poss"), names(disp_sp)),
                               currency = "", interval = 3, mark = ",", digits = 0)
      for (nm in sp_disp) {
        pr_col <- paste0("pr_", nm)
        if (nm %in% names(disp_sp) && pr_col %in% names(disp_sp)) {
          dt <- DT::formatStyle(dt, nm, backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = pr_col)
        }
      }
      return(dt)

    } else {
      # ============================================================
      # SUMMARY TEAM TABLE (existing behavior)
      # ============================================================
      df <- tr_data()
      if (is.null(df) || nrow(df) == 0) return(empty_dt("Summary: no data for current filters"))
      df <- add_team_pace_cols(df, minutes_map = mins_map)
      df <- apply_stat_filters(df, tr_stat_filter_state$filters())
      if (is.null(df) || !nrow(df)) return(empty_dt("Summary: no rows match stat filters"))
      rk_net_now <- dplyr::min_rank(dplyr::desc(df$net_rtg))
      rk_off_now <- dplyr::min_rank(dplyr::desc(df$off_ppp))
      rk_def_now <- dplyr::min_rank(df$def_ppp)
      d_net <- rep(NA_integer_, nrow(df))
      d_off <- rep(NA_integer_, nrow(df))
      d_def <- rep(NA_integer_, nrow(df))
      if (show_delta) {
        key <- as.character(df$team_id)
        mv_prev <- tr_prev_summary_ranks_from_mv()
        if (!is.null(mv_prev)) {
          d_net <- as.integer(mv_prev$net[key]) - as.integer(rk_net_now)
          d_off <- as.integer(mv_prev$off[key]) - as.integer(rk_off_now)
          d_def <- as.integer(mv_prev$def[key]) - as.integer(rk_def_now)
        } else {
          prev <- tr_prev_data()
          if (!is.null(prev) && nrow(prev)) {
            pkey <- if ("team_id" %in% names(prev)) as.character(prev$team_id) else as.character(prev$team_name)
            key <- if ("team_id" %in% names(df) && "team_id" %in% names(prev)) as.character(df$team_id) else as.character(df$team_name)
            prv_net <- setNames(dplyr::min_rank(dplyr::desc(prev$net_rtg)), pkey)
            prv_off <- setNames(dplyr::min_rank(dplyr::desc(prev$off_ppp)), pkey)
            prv_def <- setNames(dplyr::min_rank(prev$def_ppp), pkey)
            d_net <- as.integer(prv_net[key]) - as.integer(rk_net_now)
            d_off <- as.integer(prv_off[key]) - as.integer(rk_off_now)
            d_def <- as.integer(prv_def[key]) - as.integer(rk_def_now)
          }
        }
      }
      df$off_ppp_lbl <- fmt_rank_cell(df$off_ppp, rk_off_now, d_off, 1, show_delta)
      df$def_ppp_lbl <- fmt_rank_cell(df$def_ppp, rk_def_now, d_def, 1, show_delta)
      df$net_rtg_lbl <- fmt_rank_cell(df$net_rtg, rk_net_now, d_net, 1, show_delta)
      pretty_names <- c("Season", "Team", "GP", "Min", "W", "L", "Off PPP", "Def PPP", "Net Rtg", "Off Pace", "Def Pace", "Off Poss", "Def Poss")
      disp_df <- df %>% select(game_year, team_name, games_played, minutes, wins, losses, off_ppp_lbl, def_ppp_lbl, net_rtg_lbl, off_pace, def_pace, off_poss, def_poss, rank_net_rtg, rank_off_ppp, rank_def_ppp)
      names(disp_df)[names(disp_df) == "off_ppp_lbl"] <- "off_ppp"
      names(disp_df)[names(disp_df) == "def_ppp_lbl"] <- "def_ppp"
      names(disp_df)[names(disp_df) == "net_rtg_lbl"] <- "net_rtg"
      disp_df$rank_net_rtg <- rk_net_now
      disp_df$rank_off_ppp <- rk_off_now
      disp_df$rank_def_ppp <- rk_def_now
      disp_df$sort_off_ppp <- suppressWarnings(as.numeric(df$off_ppp))
      disp_df$sort_def_ppp <- suppressWarnings(as.numeric(df$def_ppp))
      disp_df$sort_net_rtg <- suppressWarnings(as.numeric(df$net_rtg))
      disp_df$sort_off_ppp[is.na(disp_df$sort_off_ppp)] <- -Inf
      disp_df$sort_def_ppp[is.na(disp_df$sort_def_ppp)] <- Inf
      disp_df$sort_net_rtg[is.na(disp_df$sort_net_rtg)] <- -Inf
      max_rank <- max(c(rk_net_now, rk_off_now, rk_def_now), na.rm = TRUE)
      if (max_rank < 2) max_rank <- 2
      cuts <- seq(1.5, max_rank - 0.5, 1)
      cols_rank <- colorRampPalette(c("#1a6b38", "#6b5a20", "#8b2020"))(length(cuts) + 1)

      summary_hidden <- which(names(disp_df) %in% c("rank_net_rtg", "rank_off_ppp", "rank_def_ppp", "sort_off_ppp", "sort_def_ppp", "sort_net_rtg")) - 1L
      summary_order_defs <- list(
        list(targets = which(names(disp_df) == "off_ppp") - 1L, orderData = which(names(disp_df) == "sort_off_ppp") - 1L, orderSequence = list("desc", "asc")),
        list(targets = which(names(disp_df) == "def_ppp") - 1L, orderData = which(names(disp_df) == "sort_def_ppp") - 1L, orderSequence = list("asc", "desc")),
        list(targets = which(names(disp_df) == "net_rtg") - 1L, orderData = which(names(disp_df) == "sort_net_rtg") - 1L, orderSequence = list("desc", "asc"))
      )
      dt <- datatable(
        disp_df,
        colnames = pretty_names,
        rownames = FALSE,
        escape = dt_escape_except(disp_df, c("off_ppp", "def_ppp", "net_rtg")),
        extensions = "Buttons",
        options = list(
          headerCallback = HEADER_TOOLTIP_JS,
          dom = "Btip",
          buttons = tr_csv_button(mode),
          pageLength = 50,
          scrollX = TRUE,
          scrollY = "70vh",
          scrollCollapse = TRUE,
          columnDefs = c(
            list(
              list(className = 'dt-center', targets = "_all"),
              list(visible = FALSE, targets = summary_hidden)
            ),
            summary_order_defs
          )
        )
      ) %>%
        formatRound(c("minutes", "off_pace", "def_pace"), 1) %>%
        formatCurrency(c("off_poss", "def_poss"), currency = "", interval = 3, mark = ",", digits = 0) %>%
        formatStyle(columns = c("net_rtg", "off_ppp", "def_ppp"), valueColumns = c("rank_net_rtg", "rank_off_ppp", "rank_def_ppp"), backgroundColor = styleInterval(cuts, cols_rank))
      return(dt)
    }
    }, error = function(e) {
      msg <- paste0("Team Ratings render error: ", conditionMessage(e))
      log_tab3_error(msg)
      tb <- tryCatch(paste(capture.output(sys.calls()), collapse = " || "), error = function(err) "")
      if (nzchar(tb)) log_tab3_error(paste0("tr_table calls: ", tb))
      showNotification(msg, type = "error", duration = 8)
      DT::datatable(
        data.frame(Error = msg, check.names = FALSE),
        rownames = FALSE,
        options = list(headerCallback = HEADER_TOOLTIP_JS, dom = "t")
      )
    })
  }, server = FALSE)

  # ---- Filter Chips ----
  output$tr_filter_chips <- renderUI({
    tryCatch(
      {
        td <- tr_teams_for_year()
        team_map <- if (!is.null(td) && nrow(td)) {
          stats::setNames(as.character(td$team_name), as.character(td$team_id))
        } else {
          NULL
        }
        build_filter_chips(
          "tr", input, shared$season_date_bounds,
          reset_btn_id = "tr_reset",
          opponent_label_map = team_map,
          extra_children = stat_filter_chips_ui("tr", tr_stat_filter_state, tr_stat_filter_cols)
        )
      },
      error = function(e) {
        log_tab3_error(paste0("tr_filter_chips error: ", conditionMessage(e)))
        NULL
      }
    )
  })
  setup_chip_clears("tr", session, input, shared,
    game_type_id = "tr_game_type", opponents_id = "tr_opponents",
    home_away_id = "tr_home_away", outcome_id = "tr_outcome",
    gn_min_id = "tr_gn_min", gn_max_id = "tr_gn_max", last_n_id = "tr_last_n",
    opp_rank_ids = c("tr_opp_rank_side", "tr_opp_rank_n", "tr_opp_rank_metric"),
    date_id = "tr_dates", gy_input_id = "game_year",
    starters_ids = c("tr_num_starters_off_mode", "tr_num_starters_off",
                     "tr_num_starters_def_mode", "tr_num_starters_def"),
    clutch_enabled_id = "tr_clutch_enabled")
}
