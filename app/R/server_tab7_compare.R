# server_tab7_compare.R - Tab 7: Compare server logic

CMP_FILTERABLE_COLS <- c(
  "A" = "metric_a",
  "Total Poss A" = "poss_a",
  "B" = "metric_b",
  "Total Poss B" = "poss_b",
  "Gap" = "gap"
)

server_tab7_compare <- function(input, output, session, shared) {

  cmp_ref <- reactiveValues(
    teams = NULL,
    players = NULL
  )
  selected_metric <- reactiveVal("net_rtg")
  cmp_auto_default_ids <- reactiveVal(integer(0))
  cmp_defaults_active <- reactiveVal(FALSE)
  cmp_syncing_sides <- reactiveVal(FALSE)
  cmp_auto_min_state <- reactiveValues(
    last_auto = NA_integer_,
    updating = FALSE
  )
  cmp_auto_enabled <- reactiveVal(TRUE)
  cmp_auto_min_bootstrapped <- reactiveVal(FALSE)
  cmp_ready <- reactiveVal(FALSE)
  cmp_init_token <- reactiveVal(0L)
  cmp_suppress_preset_echo <- reactiveVal(NULL)
  cmp_teams_ref_loaded_year <- reactiveVal(NA_integer_)
  cmp_refs_loaded_year <- reactiveVal(NA_integer_)
  cmp_player_refs_loaded_year <- reactiveVal(NA_integer_)
  selected_detail_entity <- reactiveVal(NULL)
  detail_view_active <- reactiveVal(FALSE)
  cmp_active_view <- reactiveVal("league")
  selected_team_players_team <- reactiveVal(NULL)
  selected_team_players_player <- reactiveVal(NULL)
  cmp_player_raw_cache <- reactiveVal(NULL)
  cmp_stat_filter_state <- make_stat_filter_state()
  setup_stat_filter_handlers("cmp", input, session, CMP_FILTERABLE_COLS, cmp_stat_filter_state)
  CMP_AUTO_TARGET_ROWS <- 50L
  CMP_FILTER_DEBOUNCE_MS <- 250L
  cmp_profile_enabled <- identical(Sys.getenv("CMP_PROFILE"), "1")
  cmp_profile_path <- file.path(
    normalizePath(getwd(), winslash = "/", mustWork = FALSE),
    ".codex_debug",
    "tab7_profile.log"
  )
  cmp_profile_run_id <- sprintf(
    "%s-%s",
    format(Sys.time(), "%Y%m%d-%H%M%S"),
    substr(session$token %||% "session", 1L, 8L)
  )

  cmp_profile_log <- function(label, elapsed_ms = NA_real_, extra = NULL) {
    if (!isTRUE(cmp_profile_enabled)) return(invisible(NULL))
    dir.create(dirname(cmp_profile_path), recursive = TRUE, showWarnings = FALSE)
    parts <- c(
      sprintf("ts=%s", format(Sys.time(), "%Y-%m-%d %H:%M:%OS3")),
      sprintf("run=%s", cmp_profile_run_id),
      sprintf("label=%s", label)
    )
    if (is.finite(elapsed_ms)) {
      parts <- c(parts, sprintf("ms=%.1f", elapsed_ms))
    }
    if (!is.null(extra) && nzchar(extra)) {
      parts <- c(parts, sprintf("extra=%s", gsub("[\r\n]+", " ", as.character(extra))))
    }
    cat(paste(parts, collapse = " | "), "\n", file = cmp_profile_path, append = TRUE)
    invisible(NULL)
  }

  cmp_profile_time <- function(label, expr, extra = NULL) {
    if (!isTRUE(cmp_profile_enabled)) return(force(expr))

    started <- proc.time()[["elapsed"]]
    extra_val <- NULL
    error_msg <- NULL

    on.exit({
      elapsed_ms <- (proc.time()[["elapsed"]] - started) * 1000
      if (!is.null(error_msg)) {
        cmp_profile_log(label, elapsed_ms, paste0("error=", error_msg))
      } else if (is.function(extra)) {
        cmp_profile_log(label, elapsed_ms, extra(extra_val))
      } else {
        cmp_profile_log(label, elapsed_ms, extra)
      }
    }, add = TRUE)

    tryCatch({
      extra_val <- force(expr)
      extra_val
    }, error = function(e) {
      error_msg <<- conditionMessage(e)
      stop(e)
    })
  }

  # -- Detail view constants --


  DETAIL_METRICS <- list(
    ratings = list(
      title = "Ratings",
      metrics = list(
        list(label = "Win%", col_ratings = "win_pct", col_ff = NULL, polarity = "higher", fmt = "pct"),
        list(label = "Net Rtg", col_ratings = "net_rtg", col_ff = NULL, polarity = "higher", fmt = "net"),
        list(label = "Off Rtg", col_ratings = "off_ppp", col_ff = NULL, polarity = "higher", fmt = "rtg"),
        list(label = "Def Rtg", col_ratings = "def_ppp", col_ff = NULL, polarity = "lower", fmt = "rtg")
      )
    ),
    off_ff = list(
      title = "Offensive Four Factors",
      metrics = list(
        list(label = "eFG%", col_ratings = NULL, col_ff = "off_efg", polarity = "higher", fmt = "pct", factor = "efg"),
        list(label = "TOV%", col_ratings = NULL, col_ff = "off_tov", polarity = "lower", fmt = "pct", factor = "tov"),
        list(label = "OREB%", col_ratings = NULL, col_ff = "off_oreb", polarity = "higher", fmt = "pct", factor = "oreb"),
        list(label = "FTR", col_ratings = NULL, col_ff = "off_ftr", polarity = "higher", fmt = "pct", factor = "ftr")
      )
    ),
    def_ff = list(
      title = "Defensive Four Factors",
      metrics = list(
        list(label = "Opp eFG%", col_ratings = NULL, col_ff = "def_efg", polarity = "lower", fmt = "pct", factor = "efg"),
        list(label = "Opp TOV%", col_ratings = NULL, col_ff = "def_tov", polarity = "higher", fmt = "pct", factor = "tov"),
        list(label = "Opp OREB%", col_ratings = NULL, col_ff = "def_oreb", polarity = "lower", fmt = "pct", factor = "oreb"),
        list(label = "Opp FTR", col_ratings = NULL, col_ff = "def_ftr", polarity = "lower", fmt = "pct", factor = "ftr")
      )
    ),
    shooting = list(
      title = "Offensive Shooting",
      metrics = list(
        list(label = "2PT Acc", col_ratings = NULL, col_ff = NULL, col_shooting = "off_fg2_acc", polarity = "higher", fmt = "pct"),
        list(label = "2PT Freq", col_ratings = NULL, col_ff = NULL, col_shooting = "off_fg2_freq", polarity = "neutral", fmt = "pct"),
        list(label = "3PT Acc", col_ratings = NULL, col_ff = NULL, col_shooting = "off_fg3_acc", polarity = "higher", fmt = "pct"),
        list(label = "3PT Freq", col_ratings = NULL, col_ff = NULL, col_shooting = "off_fg3_freq", polarity = "neutral", fmt = "pct")
      )
    ),
    # Teams-only section (gated in the detail renderer); defensive polarity:
    # lower opponent accuracy is better, frequency is neutral.
    def_shooting = list(
      title = "Defensive Shooting",
      metrics = list(
        list(label = "Opp 2PT Acc", col_ratings = NULL, col_ff = NULL, col_shooting = "def_fg2_acc", polarity = "lower", fmt = "pct"),
        list(label = "Opp 2PT Freq", col_ratings = NULL, col_ff = NULL, col_shooting = "def_fg2_freq", polarity = "neutral", fmt = "pct"),
        list(label = "Opp 3PT Acc", col_ratings = NULL, col_ff = NULL, col_shooting = "def_fg3_acc", polarity = "lower", fmt = "pct"),
        list(label = "Opp 3PT Freq", col_ratings = NULL, col_ff = NULL, col_shooting = "def_fg3_freq", polarity = "neutral", fmt = "pct")
      )
    ),
    # Teams-only descriptive shot-diet sections (gated in section_metrics).
    # polarity neutral: shares describe the mix, not quality — no winner, no est.±.
    off_shot_profile = list(
      title = "Offensive Shot Profile",
      metrics = list(
        list(label = "Lay-up%", col_ratings = "off_layup_share", col_ff = NULL, polarity = "neutral", fmt = "pct"),
        list(label = "Dunk%", col_ratings = "off_dunk_share", col_ff = NULL, polarity = "neutral", fmt = "pct"),
        list(label = "Lay+Dunk%", col_ratings = "off_rim_share", col_ff = NULL, polarity = "neutral", fmt = "pct"),
        list(label = "3PA%", col_ratings = "off_fg3_share", col_ff = NULL, polarity = "neutral", fmt = "pct"),
        list(label = "C3% of 3PA", col_ratings = "off_c3_pct3", col_ff = NULL, polarity = "neutral", fmt = "pct"),
        list(label = "2PT Jumper%", col_ratings = "off_mid_share", col_ff = NULL, polarity = "neutral", fmt = "pct")
      )
    ),
    def_shot_profile = list(
      title = "Defensive Shot Profile",
      metrics = list(
        list(label = "Opp Lay-up%", col_ratings = "def_layup_share", col_ff = NULL, polarity = "neutral", fmt = "pct"),
        list(label = "Opp Dunk%", col_ratings = "def_dunk_share", col_ff = NULL, polarity = "neutral", fmt = "pct"),
        list(label = "Opp Lay+Dunk%", col_ratings = "def_rim_share", col_ff = NULL, polarity = "neutral", fmt = "pct"),
        list(label = "Opp 3PA%", col_ratings = "def_fg3_share", col_ff = NULL, polarity = "neutral", fmt = "pct"),
        list(label = "Opp C3% of 3PA", col_ratings = "def_c3_pct3", col_ff = NULL, polarity = "neutral", fmt = "pct"),
        list(label = "Opp 2PT Jumper%", col_ratings = "def_mid_share", col_ff = NULL, polarity = "neutral", fmt = "pct")
      )
    )
  )

  # -- Helpers --

  parse_single_date <- function(x) {
    if (is.null(x) || length(x) != 1) return(as.Date(NA))
    d <- suppressWarnings(as.Date(x))
    if (length(d) != 1 || is.na(d)) return(as.Date(NA))
    d
  }

  cmp_player_compare_mode <- function() {
    mode <- input$cmp_player_compare_mode %||% "other"
    if (identical(mode, "self")) "self" else "other"
  }

  selected_player_value <- function(input_id) {
    val <- as.character(input[[input_id]] %||% character(0))
    val <- val[nzchar(val)]
    if (length(val)) val[1] else ""
  }

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

    # Players mode uses shared time filters for different-player comparisons,
    # and side-specific time filters for same-player comparisons.
    if (identical(input$cmp_mode, "Players")) {
      if (identical(cmp_player_compare_mode(), "self")) {
        dr <- input[[paste0("cmp_player_", side, "_dates")]]
        gn_min_input <- input[[paste0("cmp_player_", side, "_gn_min")]]
        gn_max_input <- input[[paste0("cmp_player_", side, "_gn_max")]]
      } else {
        dr <- input$cmp_players_dates
        gn_min_input <- input$cmp_players_gn_min
        gn_max_input <- input$cmp_players_gn_max
      }
      if (!is.null(dr) && length(dr) == 2) {
        d1 <- suppressWarnings(as.Date(dr[[1]]))
        d2 <- suppressWarnings(as.Date(dr[[2]]))
        if (!is.na(d1)) start_d <- d1
        if (!is.na(d2)) end_d <- d2
      }
      gn_min <- suppressWarnings(as.integer(gn_min_input %||% ""))
      gn_max <- suppressWarnings(as.integer(gn_max_input %||% ""))
      if (is.finite(gn_min)) min_gn <- gn_min
      if (is.finite(gn_max)) max_gn <- gn_max
    }
    if (!identical(input$cmp_mode, "Players")) {
      preset <- input$cmp_preset %||% ""
      if (identical(preset, "date_split")) {
        split_date <- parse_single_date(input$cmp_split_date)
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
    opp_st_mode <- get_input("opp_starters_mode") %||% ""
    opp_st_val <- get_input("opp_starters_val") %||% ""
    num_starters_off <- NA_integer_
    num_starters_off_min <- NA_integer_
    num_starters_off_max <- NA_integer_
    num_starters_def <- NA_integer_
    num_starters_def_min <- NA_integer_
    num_starters_def_max <- NA_integer_
    if (nzchar(st_mode) && nzchar(st_val)) {
      v <- as.integer(st_val)
      if (st_mode == "gte") { num_starters_off_min <- v; num_starters_off_max <- 5L }
      else if (st_mode == "lte") { num_starters_off_min <- 0L; num_starters_off_max <- v }
    }
    if (nzchar(opp_st_mode) && nzchar(opp_st_val)) {
      v <- as.integer(opp_st_val)
      if (opp_st_mode == "gte") { num_starters_def_min <- v; num_starters_def_max <- 5L }
      else if (opp_st_mode == "lte") { num_starters_def_min <- 0L; num_starters_def_max <- v }
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
    team_ids_csv <- csv_if_any(team_sel, integerize = TRUE)
    opp_ids_csv <- csv_if_any(opp_sel, integerize = TRUE)

    # Game type
    gt <- get_input("game_type") %||% character(0)
    game_type_csv <- csv_if_any(gt)

    # Home/away, outcome
    home_away <- blank_to_na_character(get_input("home_away"))
    outcome <- blank_to_na_character(get_input("outcome"))

    # Opp rank
    opp_rank_side <- blank_to_na_character(get_input("opp_rank_side"))
    opp_rank_n <- blank_to_na_integer(get_input("opp_rank_n"))
    opp_rank_metric <- blank_to_na_character(get_input("opp_rank_metric"))

    list(
      game_year = gy, start_d = start_d, end_d = end_d,
      game_type_csv = game_type_csv, team_ids_csv = team_ids_csv, opp_ids_csv = opp_ids_csv,
      home_away = home_away, outcome = outcome,
      opp_rank_side = opp_rank_side, opp_rank_n = opp_rank_n,
      opp_rank_metric = opp_rank_metric,
      max_margin = max_margin, margin_status = margin_status,
      max_time_remaining = max_time_remaining, ot_margin_filter = ot_margin_filter,
      min_gn = min_gn, max_gn = max_gn, last_n_games = last_n,
      num_starters_off = num_starters_off, num_starters_def = num_starters_def,
      num_starters_off_min = num_starters_off_min,
      num_starters_off_max = num_starters_off_max,
      num_starters_def_min = num_starters_def_min, num_starters_def_max = num_starters_def_max
    )
  }

  # Short label for DT column headers and detail grid headers
  side_label_short <- function(side) {
    # Players mode: presets not applicable
    if (identical(input$cmp_mode, "Players")) return(toupper(side))

    preset <- input$cmp_preset %||% ""
    if (!nzchar(preset)) return(toupper(side))

    switch(preset,
      starters_bench = if (side == "a") "Starters" else "Bench",
      opp_starters_bench = if (side == "a") "vs Starters" else "vs Bench",
      clutch         = if (side == "a") "Clutch" else "Non-Clutch",
      home_away      = if (side == "a") "Home" else "Away",
      win_loss       = if (side == "a") "Win" else "Loss",
      top_bottom_rank = {
        n <- input[[paste0("cmp_", side, "_opp_rank_n")]] %||% ""
        if (nzchar(n)) {
          if (side == "a") paste0("vs Top ", n) else paste0("vs Bottom ", n)
        } else {
          toupper(side)
        }
      },
      date_split = {
        split_date <- parse_single_date(input$cmp_split_date)
        if (!is.na(split_date)) {
          d <- format(split_date, "%b %d")
          if (side == "a") paste0("Before ", d) else paste0("From ", d)
        } else {
          toupper(side)
        }
      },
      gn_split = {
        gn <- suppressWarnings(as.integer(input$cmp_split_gn %||% ""))
        if (is.finite(gn)) {
          if (side == "a") paste0("GN 1\u2013", gn) else paste0("GN ", gn + 1L, "+")
        } else {
          toupper(side)
        }
      },
      toupper(side)  # unknown preset fallback
    )
  }

  # Build a short description of what a side's filters mean
  side_label_full <- function(side) {
    pfx <- paste0("cmp_", side, "_")
    get_input <- function(name) input[[paste0(pfx, name)]]
    parts <- character(0)

    st_mode <- get_input("starters_mode") %||% ""
    st_val <- get_input("starters_val") %||% ""
    if (nzchar(st_mode) && nzchar(st_val)) {
      op <- if (st_mode == "gte") "\u2265" else "\u2264"
      parts <- c(parts, paste0("Own starters ", op, st_val))
    }
    opp_st_mode <- get_input("opp_starters_mode") %||% ""
    opp_st_val <- get_input("opp_starters_val") %||% ""
    if (nzchar(opp_st_mode) && nzchar(opp_st_val)) {
      op <- if (opp_st_mode == "gte") "\u2265" else "\u2264"
      parts <- c(parts, paste0("Opp starters ", op, opp_st_val))
    }
    ha <- get_input("home_away") %||% ""
    if (nzchar(ha)) parts <- c(parts, tools::toTitleCase(ha))
    oc <- get_input("outcome") %||% ""
    if (nzchar(oc)) parts <- c(parts, tools::toTitleCase(oc))
    if (isTRUE(get_input("clutch"))) parts <- c(parts, "Clutch")

    # Opp rank
    opp_side <- get_input("opp_rank_side") %||% ""
    opp_n <- get_input("opp_rank_n") %||% ""
    opp_metric <- get_input("opp_rank_metric") %||% ""
    if (nzchar(opp_side) && nzchar(opp_n)) {
      metric_lbl <- switch(opp_metric, off = "Off", def = "Def", net = "Net", "")
      rank_lbl <- paste0("vs ", tools::toTitleCase(opp_side), " ", opp_n)
      if (nzchar(metric_lbl)) rank_lbl <- paste0(rank_lbl, " (", metric_lbl, ")")
      parts <- c(parts, rank_lbl)
    }

    team_label_map <- if (!is.null(cmp_ref$teams) && nrow(cmp_ref$teams)) {
      stats::setNames(as.character(cmp_ref$teams$team_name), as.character(cmp_ref$teams$team_id))
    } else {
      NULL
    }
    map_team_labels <- function(values) {
      values <- as.character(values)
      if (is.null(team_label_map)) return(values)
      labels <- unname(team_label_map[values])
      labels[is.na(labels) | !nzchar(labels)] <- values[is.na(labels) | !nzchar(labels)]
      labels
    }

    # Opponents
    opps <- get_input("opponents") %||% character(0)
    if (length(opps) && any(nzchar(opps))) {
      opp_names <- map_team_labels(opps[nzchar(opps)])
      if (length(opp_names) <= 2) {
        parts <- c(parts, paste0("vs ", paste(opp_names, collapse = ", ")))
      } else {
        parts <- c(parts, paste0("vs ", length(opp_names), " opps"))
      }
    }

    # Teams
    teams <- get_input("teams") %||% character(0)
    if (length(teams) && any(nzchar(teams))) {
      tm_names <- map_team_labels(teams[nzchar(teams)])
      if (length(tm_names) <= 2) {
        parts <- c(parts, paste(tm_names, collapse = ", "))
      } else {
        parts <- c(parts, paste0(length(tm_names), " teams"))
      }
    }

    # Game type
    gt <- get_input("game_type") %||% character(0)
    if (length(gt) && any(nzchar(gt))) {
      gt_labels <- c("5" = "Regular", "16" = "QF", "17" = "Finals",
                      "26" = "SF", "33" = "Play-in", "34" = "W.Cup", "35" = "S.Cup")
      gt_vals <- gt[nzchar(gt)]
      gt_names <- unname(gt_labels[gt_vals])
      gt_names <- gt_names[!is.na(gt_names)]
      if (length(gt_names)) parts <- c(parts, paste(gt_names, collapse = "+"))
    }

    if (!identical(input$cmp_mode, "Players")) {
      preset <- input$cmp_preset %||% ""
      if (identical(preset, "date_split")) {
        split_date <- parse_single_date(input$cmp_split_date)
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
    if (length(parts)) paste(parts, collapse = " \u00b7 ") else paste0("Side ", toupper(side))
  }

  cmp_gap_direction <- function() {
    switch(input$cmp_preset %||% "",
      date_split = "b_minus_a",
      gn_split = "b_minus_a",
      "a_minus_b"
    )
  }

  cmp_gap_after_minus_before <- function() {
    identical(cmp_gap_direction(), "b_minus_a")
  }

  cmp_gap_value <- function(val_a, val_b) {
    if (identical(cmp_gap_direction(), "b_minus_a")) val_b - val_a else val_a - val_b
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
    if (length(ids)) {
      return(df[0, , drop = FALSE])
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

  cmp_min_poss <- function() {
    val <- suppressWarnings(as.integer(input$cmp_min_poss %||% 10L))
    if (!is.finite(val) || val < 0L) return(10L)
    val
  }

  cmp_auto_minposs_from_df <- function(df, target_rows = CMP_AUTO_TARGET_ROWS, step = 10L) {
    if (is.null(df) || !NROW(df)) return(NA_integer_)
    if ("poss_a" %in% names(df) && "poss_b" %in% names(df)) {
      vals <- pmin(suppressWarnings(as.numeric(df$poss_a)), suppressWarnings(as.numeric(df$poss_b)))
    } else if ("poss_a" %in% names(df)) {
      vals <- suppressWarnings(as.numeric(df$poss_a))
    } else if ("poss_b" %in% names(df)) {
      vals <- suppressWarnings(as.numeric(df$poss_b))
    } else {
      return(NA_integer_)
    }
    vals <- vals[is.finite(vals)]
    if (!length(vals)) return(NA_integer_)
    vals <- sort(vals, decreasing = TRUE)
    if (length(vals) <= target_rows) return(0L)
    kth <- vals[target_rows]
    if (!is.finite(kth)) return(NA_integer_)
    as.integer(ceiling(kth / step) * step)
  }

  # -- SQL runners --

  run_compare_query <- function(key, p, sql, params) {
    allowed <- guard_heavy_request(
      session, key = key,
      start_d = p$start_d, end_d = p$end_d,
      min_gn = p$min_gn, max_gn = p$max_gn, last_n = p$last_n_games,
      max_calls = 50L, window_sec = 60L
    )
    if (!isTRUE(allowed)) return(data.frame())
    cmp_profile_time(
      paste0("run_compare_query:", key),
      db_get_query(pg_pool, sql, params = params),
      extra = function(res) sprintf("rows=%d", NROW(res))
    )
  }

  cmp_team_dynamic_params <- function(p) {
    list(
      p$game_year, as.Date(p$start_d), as.Date(p$end_d),
      p$game_type_csv, p$opp_ids_csv, p$home_away, p$outcome,
      p$opp_rank_side, p$opp_rank_n, p$opp_rank_metric,
      p$max_margin, p$margin_status, p$max_time_remaining, p$ot_margin_filter,
      p$min_gn, p$max_gn, p$last_n_games,
      p$num_starters_off, p$num_starters_def,
      p$num_starters_off_min, p$num_starters_off_max,
      p$num_starters_def_min, p$num_starters_def_max
    )
  }

  cmp_lineup_query_args <- function(p, min_poss) {
    lu <- cmp_lu_params()
    team_filter <- merge_lineup_team_csv(p$team_ids_csv, lu$team_csv)
    if (isTRUE(team_filter$conflict)) return(NULL)
    list(
      lu$num,
      team_filter$team_csv,
      lu$player_csv,
      lu$player_off_csv,
      lu$exact,
      as.Date(p$start_d), as.Date(p$end_d),
      min_poss,
      p$game_year,
      p$game_type_csv, p$opp_ids_csv, p$home_away, p$outcome,
      p$opp_rank_side, p$opp_rank_n, p$opp_rank_metric,
      p$max_margin, p$margin_status, p$max_time_remaining, p$ot_margin_filter,
      p$min_gn, p$max_gn, p$last_n_games,
      p$num_starters_off, p$num_starters_def,
      p$num_starters_off_min, p$num_starters_off_max,
      p$num_starters_def_min, p$num_starters_def_max
    )
  }

  cmp_player_query_params <- function(p, team_ids_csv) {
    list(
      p$game_year,
      if (!is.na(p$start_d)) as.Date(p$start_d) else NA,
      if (!is.na(p$end_d)) as.Date(p$end_d) else NA,
      team_ids_csv,
      p$game_type_csv, p$opp_ids_csv, p$home_away, p$outcome,
      p$opp_rank_side, p$opp_rank_n, p$opp_rank_metric,
      p$max_margin, p$margin_status, p$max_time_remaining, p$ot_margin_filter,
      p$min_gn, p$max_gn, p$last_n_games
    )
  }

  cmp_team_csv_or_na <- function(team_ids_csv) {
    if (is.null(team_ids_csv) || is.na(team_ids_csv) || !nzchar(team_ids_csv)) NA_character_ else team_ids_csv
  }

  run_team_ratings <- function(p) {
    run_compare_query(
      key = "cmp_team_ratings",
      p = p,
      sql = paste0(
        "SELECT * FROM basketball_test.get_team_ratings_dynamic(",
        "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,$7::text,$8::text,$9::int4,$10::text,",
        "$11::int4,$12::text,$13::int4,$14::bool,$15::int4,$16::int4,$17::int4,",
        "$18::int4,$19::int4,$20::int4,$21::int4,$22::int4,$23::int4",
        ")"
      ),
      params = cmp_team_dynamic_params(p)
    )
  }

  run_team_ff <- function(p) {
    run_compare_query(
      key = "cmp_team_ff",
      p = p,
      sql = paste0(
        "SELECT * FROM basketball_test.get_team_four_factors_dynamic(",
        "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,$7::text,$8::text,$9::int4,$10::text,",
        "$11::int4,$12::text,$13::int4,$14::bool,$15::int4,$16::int4,$17::int4,",
        "$18::int4,$19::int4,$20::int4,$21::int4,$22::int4,$23::int4",
        ")"
      ),
      params = cmp_team_dynamic_params(p)
    )
  }

  cmp_lu_state_raw <- reactive({
    num <- suppressWarnings(as.integer(input$cmp_lu_num %||% "5"))
    if (!is.finite(num) || num < 2L || num > 5L) num <- 5L
    team_val <- cmp_lu_filter$team()
    player_on_ids <- if (nzchar(team_val)) as.integer(cmp_lu_filter$players_on()) else integer(0)
    player_off_ids <- if (nzchar(team_val)) as.integer(cmp_lu_filter$players_off()) else integer(0)
    player_csv <- if (length(player_on_ids)) paste(player_on_ids, collapse = ",") else NA_character_
    player_off_csv <- if (length(player_off_ids)) paste(player_off_ids, collapse = ",") else NA_character_
    list(num = num, team_csv = if (nzchar(team_val)) team_val else NA_character_,
         player_csv = player_csv, player_off_csv = player_off_csv,
         exact = length(player_on_ids) > 0L)
  })

  cmp_lu_state <- reactive(cmp_lu_state_raw()) %>% debounce(CMP_FILTER_DEBOUNCE_MS)

  # Read lineup filter controls (shared across both sides)
  cmp_lu_params <- function() {
    cmp_lu_state()
  }

  merge_lineup_team_csv <- function(side_team_csv, lineup_team_csv) {
    side_team_csv <- if (is.null(side_team_csv) || is.na(side_team_csv) || !nzchar(side_team_csv)) NA_character_ else side_team_csv
    lineup_team_csv <- if (is.null(lineup_team_csv) || is.na(lineup_team_csv) || !nzchar(lineup_team_csv)) NA_character_ else lineup_team_csv

    if (is.na(side_team_csv)) return(list(team_csv = lineup_team_csv, conflict = FALSE))
    if (is.na(lineup_team_csv)) return(list(team_csv = side_team_csv, conflict = FALSE))

    side_ids <- suppressWarnings(as.integer(trimws(strsplit(side_team_csv, ",", fixed = TRUE)[[1]])))
    lineup_ids <- suppressWarnings(as.integer(trimws(strsplit(lineup_team_csv, ",", fixed = TRUE)[[1]])))
    keep_ids <- intersect(side_ids[is.finite(side_ids)], lineup_ids[is.finite(lineup_ids)])

    if (!length(keep_ids)) return(list(team_csv = NA_character_, conflict = TRUE))
    list(team_csv = paste(keep_ids, collapse = ","), conflict = FALSE)
  }

  run_lineups_summary <- function(p, min_poss = cmp_min_poss()) {
    params <- cmp_lineup_query_args(p, min_poss)
    if (is.null(params)) return(data.frame())
    run_compare_query(
      key = "cmp_lineups_summary",
      p = p,
      sql = paste0(
        "SELECT * FROM basketball_test.fetch_lineups_csv_v2(",
        "$1::int4,$2::text,$3::text,$4::text,$5::bool,$6::date,$7::date,$8::int4,$9::int4,",
        "$10::text,$11::text,$12::text,$13::text,$14::text,$15::int4,$16::text,$17::int4,$18::text,$19::int4,$20::bool,",
        "$21::int4,$22::int4,$23::int4,$24::int4,$25::int4,$26::int4,$27::int4,$28::int4,$29::int4",
        ")"
      ),
      params = params
    )
  }

  run_lineups_ff <- function(p, min_poss = cmp_min_poss()) {
    params <- cmp_lineup_query_args(p, min_poss)
    if (is.null(params)) return(data.frame())
    run_compare_query(
      key = "cmp_lineups_ff",
      p = p,
      sql = paste0(
        "SELECT * FROM basketball_test.fetch_lineups_four_factors_csv(",
        "$1::int4,$2::text,$3::text,$4::text,$5::bool,$6::date,$7::date,$8::int4,$9::int4,",
        "$10::text,$11::text,$12::text,$13::text,$14::text,$15::int4,$16::text,$17::int4,$18::text,$19::int4,$20::bool,",
        "$21::int4,$22::int4,$23::int4,$24::int4,$25::int4,$26::int4,$27::int4,$28::int4,$29::int4",
        ")"
      ),
      params = params
    )
  }

  cmp_team_shooting_params <- function(p, team_id) {
    list(
      5L,
      as.character(team_id),
      NA_character_,
      NA_character_,
      FALSE,
      as.Date(p$start_d), as.Date(p$end_d),
      0L,
      p$game_year,
      p$game_type_csv, p$opp_ids_csv, p$home_away, p$outcome,
      p$opp_rank_side, p$opp_rank_n, p$opp_rank_metric,
      p$max_margin, p$margin_status, p$max_time_remaining, p$ot_margin_filter,
      p$min_gn, p$max_gn, p$last_n_games,
      p$num_starters_off, p$num_starters_def,
      p$num_starters_off_min, p$num_starters_off_max,
      p$num_starters_def_min, p$num_starters_def_max
    )
  }

  add_shooting_rates <- function(row) {
    if (is.null(row) || !nrow(row)) return(NULL)
    pct <- function(num, den) {
      num <- suppressWarnings(as.numeric(num))
      den <- suppressWarnings(as.numeric(den))
      if (is.finite(num) && is.finite(den) && den > 0) num / den * 100 else NA_real_
    }
    val <- function(col) if (col %in% names(row)) sum(suppressWarnings(as.numeric(row[[col]])), na.rm = TRUE) else 0

    off_fg2_att <- val("off_fg2_att")
    off_fg3_att <- val("off_fg3_att")
    off_fga <- off_fg2_att + off_fg3_att

    def_fg2_att <- val("def_fg2_att")
    def_fg3_att <- val("def_fg3_att")
    def_fga <- def_fg2_att + def_fg3_att

    out <- row[1, , drop = FALSE]
    out$off_fg2_acc <- pct(val("off_fg2_made"), off_fg2_att)
    out$off_fg2_freq <- pct(off_fg2_att, off_fga)
    out$off_fg3_acc <- pct(val("off_fg3_made"), off_fg3_att)
    out$off_fg3_freq <- pct(off_fg3_att, off_fga)
    out$def_fg2_acc <- pct(val("def_fg2_made"), def_fg2_att)
    out$def_fg2_freq <- pct(def_fg2_att, def_fga)
    out$def_fg3_acc <- pct(val("def_fg3_made"), def_fg3_att)
    out$def_fg3_freq <- pct(def_fg3_att, def_fga)
    out
  }

  add_team_shot_profile_shares <- function(row) {
    if (is.null(row) || !nrow(row)) return(row)
    add_shot_profile_metrics(row, list(
      off = c("off_layup_att", "off_dunk_att", "off_fga", "off_fg3_att", "off_c3_att", "off_c3_known_att"),
      def = c("def_layup_att", "def_dunk_att", "def_fga", "def_fg3_att", "def_c3_att", "def_c3_known_att")
    ))
  }

  run_team_shooting <- function(p, team_id) {
    df <- run_compare_query(
      key = "cmp_team_shooting",
      p = p,
      sql = paste0(
        "SELECT * FROM basketball_test.fetch_lineups_csv_v2(",
        "$1::int4,$2::text,$3::text,$4::text,$5::bool,$6::date,$7::date,$8::int4,$9::int4,",
        "$10::text,$11::text,$12::text,$13::text,$14::text,$15::int4,$16::text,$17::int4,$18::text,$19::int4,$20::bool,",
        "$21::int4,$22::int4,$23::int4,$24::int4,$25::int4,$26::int4,$27::int4,$28::int4,$29::int4",
        ")"
      ),
      params = cmp_team_shooting_params(p, team_id)
    )
    if (is.null(df) || !nrow(df)) return(data.frame())
    if ("team_id" %in% names(df)) {
      df <- df[suppressWarnings(as.integer(df$team_id)) == as.integer(team_id), , drop = FALSE]
    }
    if (!nrow(df)) return(data.frame())

    sum_col <- function(col) if (col %in% names(df)) sum(suppressWarnings(as.numeric(df[[col]])), na.rm = TRUE) else 0
    team_name <- if ("team_name" %in% names(df)) as.character(df$team_name[1]) else NA_character_
    out <- data.frame(
      team_id = as.integer(team_id),
      team_name = team_name,
      off_fg2_made = sum_col("off_fg2_made"),
      off_fg2_att = sum_col("off_fg2_att"),
      off_fg3_made = sum_col("off_fg3_made"),
      off_fg3_att = sum_col("off_fg3_att"),
      def_fg2_made = sum_col("def_fg2_made"),
      def_fg2_att = sum_col("def_fg2_att"),
      def_fg3_made = sum_col("def_fg3_made"),
      def_fg3_att = sum_col("def_fg3_att"),
      stringsAsFactors = FALSE
    )
    add_shooting_rates(out)
  }

  add_cmp_player_usage_pct <- function(df) {
    if (is.null(df) || !nrow(df)) return(df)
    if (!("usg_pct" %in% names(df))) df$usg_pct <- NA_real_
    needed <- c("fga", "fta", "tov", "poss_on_floor")
    if (!all(needed %in% names(df))) return(df)

    as_num <- function(col) suppressWarnings(as.numeric(df[[col]]))
    zero_na <- function(x) {
      x[!is.finite(x)] <- 0
      x
    }

    fga <- zero_na(as_num("fga"))
    fta <- zero_na(as_num("fta"))
    tov <- zero_na(as_num("tov"))
    poss_on_floor <- as_num("poss_on_floor")
    pts <- if ("pts" %in% names(df)) as_num("pts") else rep(NA_real_, nrow(df))
    ts <- if ("ts" %in% names(df)) as_num("ts") else rep(NA_real_, nrow(df))

    shot_term <- fga + 0.44 * fta
    can_imply_ts_term <- is.finite(pts) & pts > 0 & is.finite(ts) & ts > 0
    shot_term[can_imply_ts_term] <- pts[can_imply_ts_term] / (2 * (ts[can_imply_ts_term] / 100))
    player_term <- shot_term + tov

    out <- suppressWarnings(as.numeric(df$usg_pct))
    team_key <- if ("team_id" %in% names(df)) as.character(df$team_id) else rep("all", nrow(df))
    team_key[is.na(team_key) | !nzchar(team_key)] <- "all"

    for (key in unique(team_key)) {
      idx <- which(team_key == key)
      team_term <- sum(player_term[idx], na.rm = TRUE)
      team_poss <- sum(poss_on_floor[idx], na.rm = TRUE) / 5
      ok <- !is.finite(out[idx]) &
        is.finite(player_term[idx]) & player_term[idx] >= 0 &
        is.finite(poss_on_floor[idx]) & poss_on_floor[idx] > 0 &
        is.finite(team_term) & team_term > 0 &
        is.finite(team_poss) & team_poss > 0
      if (any(ok)) {
        out[idx[ok]] <- 100 * player_term[idx][ok] * team_poss / (team_term * poss_on_floor[idx][ok])
      }
    }

    df$usg_pct <- round(out, 1)
    df
  }

  run_player_traditional <- function(p, team_ids_csv) {
    tryCatch({
      out <- run_compare_query(
        key = "cmp_player_traditional",
        p = p,
        sql = paste0(
          "SELECT * FROM basketball_test.get_player_traditional_dynamic(",
          "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,$7::text,$8::text,$9::text,$10::int4,$11::text,",
          "$12::int4,$13::text,$14::int4,$15::bool,$16::int4,$17::int4,$18::int4",
          ")"
        ),
        params = cmp_player_query_params(p, team_ids_csv)
      )
      add_cmp_player_usage_pct(out)
    }, error = function(e) {
      msg <- conditionMessage(e)
      if (grepl("statement timeout", msg, ignore.case = TRUE)) {
        showNotification("Player compare query timed out. Narrow filters or date range.", type = "warning", duration = 5)
      } else {
        app_log("tab7", sprintf("player_compare query failed: %s", msg), level = "ERROR", session = session)
        showNotification("Player compare query failed. Try narrowing filters or retry.", type = "error", duration = 6)
      }
      data.frame()
    })
  }

  run_four_factors <- function(p, team_ids_csv) {
    run_compare_query(
      key = "cmp_four_factors",
      p = p,
      sql = paste0(
        "SELECT * FROM basketball_test.four_factors_compute(",
        "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,",
        "$7::text,$8::text,$9::text,$10::int4,$11::text,",
        "$12::int4,$13::int4,$14::int4,$15::int4,$16::int4,$17::int4,$18::int4,$19::int4,$20::int4",
        ")"
      ),
      params = list(
        p$game_year,
        as.Date(p$start_d), as.Date(p$end_d),
        cmp_team_csv_or_na(team_ids_csv),
        p$game_type_csv, p$opp_ids_csv, p$home_away, p$outcome,
        p$opp_rank_side, p$opp_rank_n, p$opp_rank_metric,
        p$min_gn, p$max_gn, p$last_n_games,
        p$num_starters_off, p$num_starters_def,
        p$num_starters_off_min, p$num_starters_off_max,
        p$num_starters_def_min, p$num_starters_def_max
      )
    )
  }

  run_onoff_impact <- function(p, team_ids_csv) {
    run_compare_query(
      key = "cmp_onoff_impact",
      p = p,
      sql = paste0(
        "SELECT * FROM basketball_test.onoff_compute(",
        "$1::date,$2::date,$3::text,$4::int4,$5::int4,$6::numeric,$7::text,",
        "$8::text,$9::text,$10::text,$11::text,$12::text,$13::int4,$14::text,",
        "$15::int4,$16::int4,$17::int4,$18::int4,$19::int4,$20::int4,$21::int4,$22::int4,$23::int4",
        ")"
      ),
      params = list(
        as.Date(p$start_d), as.Date(p$end_d), team_ids_csv,
        0L, 0L, as.numeric(DEFAULT_MIN_NET), as.character(p$game_year),
        p$game_type_csv, p$opp_ids_csv, p$home_away, p$outcome,
        p$opp_rank_side, p$opp_rank_n, p$opp_rank_metric,
        p$min_gn, p$max_gn, p$last_n_games,
        p$num_starters_off, p$num_starters_def,
        p$num_starters_off_min, p$num_starters_off_max,
        p$num_starters_def_min, p$num_starters_def_max
      )
    )
  }

  # -- Metric chip definitions per mode --

  TEAM_METRICS <- c(
    "Net Rtg" = "net_rtg", "Offense" = "off_ppp", "Defense" = "def_ppp",
    "eFG%" = "off_efg", "TOV%" = "off_tov", "OREB%" = "off_oreb", "FTR" = "off_ftr"
  )

  PLAYER_METRICS <- c(
    "PPG" = "ppg", "RPG" = "rpg", "APG" = "apg", "SPG" = "spg",
    "FG%" = "fg_pct", "3P%" = "fg3_pct", "FT%" = "ft_pct", "TS%" = "ts_pct", "USG%" = "usg_pct"
  )

  TEAM_PLAYER_METRICS <- c(
    "PTS" = "pts", "REB" = "reb", "AST" = "ast", "STL" = "stl",
    "BLK" = "blk", "TOV" = "tov", "FG%" = "fg_pct", "3P%" = "tp_pct",
    "FT%" = "ft_pct", "eFG%" = "efg", "TS%" = "ts", "USG%" = "usg_pct"
  )

  TEAM_PLAYER_PCT_COLS <- c("fg_pct", "tp_pct", "ft_pct", "efg", "ts", "usg_pct")
  TEAM_PLAYER_LOWER_BETTER <- c("tov")

  PLAYER_VIEWS <- c(
    "Overall" = "overall",
    "Four Factors" = "ff_swing",
    "Shot Profile" = "shot_profile"
  )

  selected_player_view <- reactiveVal("overall")
  selected_team_player_metric <- reactiveVal("pts")

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

  load_cmp_teams_ref <- function(game_year) {
    cmp_profile_time(
      "load_cmp_teams_ref",
      normalize_teams_ref(fetch_teams_distinct(game_year)),
      extra = function(res) sprintf("game_year=%s;rows=%d", as.integer(game_year), NROW(res))
    )
  }

  load_cmp_players_ref <- function(game_year) {
    cmp_profile_time(
      "load_cmp_players_ref",
      normalize_players_ref(fetch_players_basic(game_year)),
      extra = function(res) sprintf("game_year=%s;rows=%d", as.integer(game_year), NROW(res))
    )
  }

  load_cmp_gn_ref <- function(game_year) {
    cmp_profile_time(
      "load_cmp_gn_ref",
      fetch_gn_values(game_year),
      extra = function(res) sprintf("game_year=%s;rows=%d", as.integer(game_year), NROW(res))
    )
  }

  ensure_cmp_teams_ref_loaded <- function(game_year) {
    gy_int <- as.integer(game_year)
    loaded <- identical(cmp_teams_ref_loaded_year(), gy_int) &&
      !is.null(cmp_ref$teams) && nrow(cmp_ref$teams)
    if (!isTRUE(loaded)) {
      cmp_ref$teams <- load_cmp_teams_ref(gy_int)
      cmp_teams_ref_loaded_year(gy_int)
    }
    invisible(NULL)
  }

  ensure_cmp_player_refs_loaded <- function(game_year,
                                            refresh_player_inputs = FALSE,
                                            apply_defaults = FALSE) {
    gy_int <- as.integer(game_year)
    ensure_cmp_teams_ref_loaded(gy_int)
    loaded <- identical(cmp_player_refs_loaded_year(), gy_int) &&
      !is.null(cmp_ref$players) && nrow(cmp_ref$players)
    if (!isTRUE(loaded)) {
      cmp_ref$players <- load_cmp_players_ref(gy_int)
      cmp_player_refs_loaded_year(gy_int)
    }
    if (isTRUE(refresh_player_inputs)) {
      refresh_player_choices("a")
      refresh_player_choices("b")
    }
    if (isTRUE(apply_defaults)) {
      apply_default_players()
    }
    invisible(NULL)
  }

  refresh_compare_ref_inputs <- function(game_year, include_players = FALSE) {
    cmp_profile_time(
      "refresh_compare_ref_inputs",
      {
        gy_int <- as.integer(game_year)
        base_loaded <- identical(cmp_refs_loaded_year(), gy_int) &&
          !is.null(cmp_ref$teams) && nrow(cmp_ref$teams)

        if (isTRUE(base_loaded)) {
          if (isTRUE(include_players)) {
            ensure_cmp_player_refs_loaded(
              gy_int,
              refresh_player_inputs = identical(input$cmp_mode, "Players"),
              apply_defaults = FALSE
            )
          }
          return(invisible(NULL))
        }

        teams_df <- load_cmp_teams_ref(gy_int)
        cmp_ref$teams <- teams_df
        cmp_teams_ref_loaded_year(gy_int)
        cmp_refs_loaded_year(gy_int)
        team_choices <- if (nrow(teams_df)) {
          stats::setNames(as.character(teams_df$team_id), as.character(teams_df$team_name))
        } else {
          character(0)
        }
        updateSelectizeInput(session, "cmp_a_teams", choices = team_choices, selected = character(0), server = FALSE)
        updateSelectizeInput(session, "cmp_b_teams", choices = team_choices, selected = character(0), server = FALSE)
        updateSelectizeInput(session, "cmp_a_opponents", choices = team_choices, selected = character(0), server = FALSE)
        updateSelectizeInput(session, "cmp_b_opponents", choices = team_choices, selected = character(0), server = FALSE)
        player_list_team_choices <- if (nrow(teams_df)) setNames(as.character(teams_df$team_id), teams_df$team_name) else character(0)
        updateSelectizeInput(session, "cmp_player_a_list_team_filter", choices = player_list_team_choices, selected = character(0), server = FALSE)
        updateSelectizeInput(session, "cmp_player_b_list_team_filter", choices = player_list_team_choices, selected = character(0), server = FALSE)
        lu_team_choices <- if (nrow(teams_df)) setNames(as.character(teams_df$team_id), teams_df$team_name) else character(0)
        cmp_lu_filter$reset_inputs(team_choices = c("All teams" = "", lu_team_choices), team_selected = "")

        gn_df <- load_cmp_gn_ref(gy_int)
        gn_choices <- if (nrow(gn_df)) as.character(gn_df$gn) else character(0)
        gn_choices_with_blank <- c("", gn_choices)
        updateSelectizeInput(session, "cmp_players_gn_min", choices = gn_choices_with_blank, selected = "", server = FALSE)
        updateSelectizeInput(session, "cmp_players_gn_max", choices = gn_choices_with_blank, selected = "", server = FALSE)
        updateSelectizeInput(session, "cmp_player_a_gn_min", choices = gn_choices_with_blank, selected = "", server = FALSE)
        updateSelectizeInput(session, "cmp_player_a_gn_max", choices = gn_choices_with_blank, selected = "", server = FALSE)
        updateSelectizeInput(session, "cmp_player_b_gn_min", choices = gn_choices_with_blank, selected = "", server = FALSE)
        updateSelectizeInput(session, "cmp_player_b_gn_max", choices = gn_choices_with_blank, selected = "", server = FALSE)
        updateSelectizeInput(session, "cmp_split_gn", choices = gn_choices_with_blank, selected = "", server = FALSE)

        b <- shared$season_date_bounds(as.character(gy_int))
        updateDateRangeInput(session, "cmp_players_dates", start = b$start, end = b$end, min = b$start, max = b$end)
        updateDateRangeInput(session, "cmp_player_a_dates", start = b$start, end = b$end, min = b$start, max = b$end)
        updateDateRangeInput(session, "cmp_player_b_dates", start = b$start, end = b$end, min = b$start, max = b$end)
        updateDateInput(session, "cmp_split_date", value = b$end, min = b$start, max = b$end)

        if (isTRUE(include_players)) {
          ensure_cmp_player_refs_loaded(
            gy_int,
            refresh_player_inputs = identical(input$cmp_mode, "Players"),
            apply_defaults = FALSE
          )
        }
      },
      extra = sprintf("game_year=%s", as.integer(game_year))
    )
  }

  cmp_lu_filter <- lineup_player_filter_server(
    "cmp_lu_filter",
    players_ref = reactive(normalize_players_ref(cmp_ref$players))
  )

  cmp_refs_state <- reactive({
    ensure_cmp_teams_ref_loaded(as.integer(input$game_year))
    if (identical(input$cmp_mode, "Players")) {
      ensure_cmp_player_refs_loaded(as.integer(input$game_year))
    }
    list(
      players = normalize_players_ref(cmp_ref$players),
      teams = normalize_teams_ref(cmp_ref$teams)
    )
  })

  cmp_side_state_raw <- reactive({
    list(
      a = collect_side_params("a"),
      b = collect_side_params("b")
    )
  })
  cmp_side_state <- reactive(cmp_side_state_raw()) %>% debounce(CMP_FILTER_DEBOUNCE_MS)

  cmp_player_selection_state_raw <- reactive({
    req(identical(input$cmp_mode, "Players"))

    refs <- cmp_refs_state()
    players_df <- refs$players
    teams_df <- refs$teams

    compare_mode <- cmp_player_compare_mode()
    player_a_id <- selected_player_value("cmp_player_a")
    player_b_id <- if (identical(compare_mode, "self")) player_a_id else selected_player_value("cmp_player_b")
    req(nzchar(player_a_id))
    req(nzchar(player_b_id))
    req(!is.null(players_df), all(c("team_id", "player_id", "name") %in% names(players_df)))
    req(!is.null(teams_df), all(c("team_id", "team_name") %in% names(teams_df)))
    req(nrow(players_df) > 0)

    team_ids_a <- unique(players_df$team_id[players_df$player_id == as.integer(player_a_id)])
    team_ids_b <- unique(players_df$team_id[players_df$player_id == as.integer(player_b_id)])
    team_sel_a <- suppressWarnings(as.integer(input$cmp_player_a_team %||% ""))
    team_sel_b <- if (identical(compare_mode, "self")) {
      team_sel_a
    } else {
      suppressWarnings(as.integer(input$cmp_player_b_team %||% ""))
    }
    if (is.finite(team_sel_a) && (team_sel_a %in% team_ids_a)) team_ids_a <- team_sel_a
    if (is.finite(team_sel_b) && (team_sel_b %in% team_ids_b)) team_ids_b <- team_sel_b
    if (!length(team_ids_a) || !length(team_ids_b)) return(NULL)

    name_a <- players_df$name[players_df$player_id == as.integer(player_a_id)][1]
    name_b <- players_df$name[players_df$player_id == as.integer(player_b_id)][1]
    team_name_a <- if (!is.null(teams_df)) teams_df$team_name[teams_df$team_id == team_ids_a[1]][1] else ""
    team_name_b <- if (!is.null(teams_df)) teams_df$team_name[teams_df$team_id == team_ids_b[1]][1] else ""

    list(
      player_a_id = player_a_id,
      player_b_id = player_b_id,
      player_a_id_int = as.integer(player_a_id),
      player_b_id_int = as.integer(player_b_id),
      players_df = players_df,
      team_ids_a = team_ids_a,
      team_ids_b = team_ids_b,
      team_csv_a = paste(team_ids_a, collapse = ","),
      team_csv_b = paste(team_ids_b, collapse = ","),
      name_a = name_a,
      name_b = name_b,
      team_name_a = team_name_a %||% "",
      team_name_b = team_name_b %||% "",
      compare_mode = compare_mode
    )
  })
  cmp_player_selection_state <- reactive(cmp_player_selection_state_raw()) %>% debounce(CMP_FILTER_DEBOUNCE_MS)

  observeEvent(list(input$main_tabs, input$cmp_mode, input$cmp_player_compare_mode, input$cmp_player_a, input$cmp_player_b), {
    missing_players <- if (identical(cmp_player_compare_mode(), "self")) {
      !nzchar(selected_player_value("cmp_player_a"))
    } else {
      !nzchar(selected_player_value("cmp_player_a")) || !nzchar(selected_player_value("cmp_player_b"))
    }
    if (!identical(input$main_tabs, "compare") ||
        !identical(input$cmp_mode, "Players") ||
        isTRUE(missing_players)) {
      cmp_player_raw_cache(NULL)
    }
    invisible(NULL)
  }, ignoreInit = FALSE)

  observeEvent(input$game_year, {
    cmp_player_raw_cache(NULL)
    invisible(NULL)
  }, ignoreInit = TRUE)

  render_metric_chips <- function(metrics, cur, input_id) {
    chips <- lapply(seq_along(metrics), function(i) {
      nm <- names(metrics)[i]
      val <- metrics[[i]]
      cls <- if (identical(val, cur)) "btn btn-sm btn-warning" else "btn btn-sm btn-outline-secondary"
      tags$button(
        type = "button",
        class = paste(cls, "js-shiny-event"),
        style = "border-radius: 20px; padding: 2px 12px; font-size: .76rem;",
        `data-input-id` = input_id,
        `data-shiny-value` = val,
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

  output$cmp_team_player_metric_chips_ui <- renderUI({
    req(identical(input$cmp_mode, "Teams"))
    req(!is.null(selected_team_players_team()))
    cur <- selected_team_player_metric()
    if (!(cur %in% TEAM_PLAYER_METRICS)) cur <- TEAM_PLAYER_METRICS[[1]]
    render_metric_chips(TEAM_PLAYER_METRICS, cur, "cmp_team_player_metric")
  })

  selected_team_detail <- function() {
    entity <- selected_detail_entity()
    if (is.null(entity) || !identical(entity$mode, "Teams")) return(NULL)
    team_id <- suppressWarnings(as.integer(entity$key))
    if (!is.finite(team_id)) return(NULL)
    list(team_id = team_id, team_name = as.character(entity$name %||% ""))
  }

  output$cmp_team_players_view_btn_ui <- renderUI({
    if (!identical(input$cmp_mode, "Teams")) return(NULL)
    team <- selected_team_detail()
    if (is.null(team)) return(NULL)
    cls <- if (identical(cmp_active_view(), "players")) "btn btn-sm btn-warning" else "btn btn-sm btn-outline-secondary"
    tags$button(id = "cmp_view_players_btn", type = "button",
      class = paste(cls, "cmp-view-toggle-btn js-shiny-event"),
      `data-input-id` = "cmp_detail_toggle",
      `data-shiny-value` = "players",
      "Players")
  })

  release_compare_ready_after_flush <- function(token) {
    if (inherits(session, "MockShinySession")) {
      cmp_ready(TRUE)
      return(invisible(NULL))
    }

    session$onFlushed(function() {
      later::later(function() {
        should_release <- isolate(
          identical(cmp_init_token(), token) &&
            identical(input$main_tabs, "compare")
        )
        if (!isTRUE(should_release)) return(invisible(NULL))
        isolate(cmp_ready(TRUE))
        invisible(NULL)
      }, delay = (CMP_FILTER_DEBOUNCE_MS + 150) / 1000)
    }, once = TRUE)
  }

  observeEvent(input$cmp_metric, {
    if (identical(input$cmp_mode, "Players")) return(NULL)
    metrics <- TEAM_METRICS
    m <- input$cmp_metric %||% ""
    if (m %in% unname(metrics)) selected_metric(m)
  }, ignoreInit = TRUE)

  observeEvent(input$cmp_detail_toggle, {
    view <- input$cmp_detail_toggle %||% "league"
    if (!(view %in% c("league", "detail", "players"))) view <- "league"
    if (identical(view, "players")) {
      team <- selected_team_detail()
      if (!identical(input$cmp_mode, "Teams") || is.null(team)) {
        view <- "detail"
      } else {
        selected_team_players_team(team)
        selected_team_players_player(NULL)
      }
    }
    cmp_active_view(view)
    is_detail <- identical(view, "detail")
    detail_view_active(is_detail)
    if (identical(view, "league")) {
      selected_detail_entity(NULL)
      selected_team_players_team(NULL)
      selected_team_players_player(NULL)
    }
    if (!identical(view, "players")) {
      selected_team_players_player(NULL)
    }
  }, ignoreInit = TRUE)

  observe({
    view <- cmp_active_view()
    if (identical(view, "players") && !identical(input$cmp_mode, "Teams")) view <- "league"
    if (identical(view, "players") && is.null(selected_team_detail())) view <- "detail"
    session$sendCustomMessage("toggle_cmp_view", list(view = view, detail = identical(view, "detail")))
  })

  observeEvent(input$cmp_player_view, {
    v <- input$cmp_player_view %||% ""
    if (v %in% unname(PLAYER_VIEWS)) selected_player_view(v)
  }, ignoreInit = TRUE)

  observeEvent(input$cmp_team_player_metric, {
    m <- input$cmp_team_player_metric %||% ""
    if (m %in% unname(TEAM_PLAYER_METRICS)) selected_team_player_metric(m)
  }, ignoreInit = TRUE)

  observeEvent(input$cmp_min_poss, {
    if (identical(input$cmp_mode, "Players")) return(invisible(NULL))
    if (!isTRUE(cmp_ready())) return(invisible(NULL))
    if (isTRUE(cmp_auto_min_state$updating)) return(invisible(NULL))
    cur_val <- suppressWarnings(as.integer(input$cmp_min_poss %||% 10L))
    last_auto <- suppressWarnings(as.integer(cmp_auto_min_state$last_auto))
    if (!is.na(cur_val) && !is.na(last_auto) && cur_val == last_auto) {
      return(invisible(NULL))
    }
    cmp_auto_enabled(FALSE)
  }, ignoreInit = TRUE)

  observeEvent(list(
    input$cmp_mode, input$cmp_preset, input$cmp_split_date, input$cmp_split_gn,
    input$cmp_a_starters_mode, input$cmp_a_starters_val, input$cmp_a_opp_starters_mode, input$cmp_a_opp_starters_val, input$cmp_a_teams,
    input$cmp_a_home_away, input$cmp_a_outcome, input$cmp_a_clutch,
    input$cmp_a_clutch_margin, input$cmp_a_clutch_minutes, input$cmp_a_opponents,
    input$cmp_a_game_type, input$cmp_a_opp_rank_side, input$cmp_a_opp_rank_n, input$cmp_a_opp_rank_metric,
    input$cmp_b_starters_mode, input$cmp_b_starters_val, input$cmp_b_opp_starters_mode, input$cmp_b_opp_starters_val, input$cmp_b_teams,
    input$cmp_b_home_away, input$cmp_b_outcome, input$cmp_b_clutch,
    input$cmp_b_clutch_margin, input$cmp_b_clutch_minutes, input$cmp_b_opponents,
    input$cmp_b_game_type, input$cmp_b_opp_rank_side, input$cmp_b_opp_rank_n, input$cmp_b_opp_rank_metric,
    input$cmp_lu_num, cmp_lu_filter$team(), cmp_lu_filter$players_on(), cmp_lu_filter$players_off(),
    input$game_year, input$main_tabs
  ), {
    if (!identical(input$main_tabs, "compare")) return(invisible(NULL))
    if (identical(input$cmp_mode, "Players")) return(invisible(NULL))
    if (!isTRUE(cmp_ready())) return(invisible(NULL))
    cmp_auto_enabled(TRUE)
  }, ignoreInit = TRUE)

  # -- Shared filter reset helper --

  reset_compare_side_filters <- function(side, reset_clutch_sliders = TRUE) {
    updateSelectInput(session, paste0("cmp_", side, "_starters_mode"), selected = "")
    updateSelectInput(session, paste0("cmp_", side, "_starters_val"), selected = "")
    updateSelectInput(session, paste0("cmp_", side, "_opp_starters_mode"), selected = "")
    updateSelectInput(session, paste0("cmp_", side, "_opp_starters_val"), selected = "")
    updateSelectInput(session, paste0("cmp_", side, "_home_away"), selected = "")
    updateSelectInput(session, paste0("cmp_", side, "_outcome"), selected = "")
    updateCheckboxInput(session, paste0("cmp_", side, "_clutch"), value = FALSE)
    if (isTRUE(reset_clutch_sliders)) {
      updateSliderInput(session, paste0("cmp_", side, "_clutch_margin"), value = 5)
      updateSliderInput(session, paste0("cmp_", side, "_clutch_minutes"), value = 5)
    }
    updateSelectizeInput(session, paste0("cmp_", side, "_teams"), selected = character(0))
    updateSelectizeInput(session, paste0("cmp_", side, "_opponents"), selected = character(0))
    updateSelectizeInput(session, paste0("cmp_", side, "_game_type"), selected = character(0))
    updateSelectInput(session, paste0("cmp_", side, "_opp_rank_side"), selected = "")
    updateSelectInput(session, paste0("cmp_", side, "_opp_rank_n"), selected = "")
    updateSelectInput(session, paste0("cmp_", side, "_opp_rank_metric"), selected = "")
  }

  apply_compare_preset <- function(preset) {
    if (identical(preset, "starters_bench")) {
      updateSelectInput(session, "cmp_a_starters_mode", selected = "gte")
      updateSelectInput(session, "cmp_a_starters_val", selected = "3")
      updateSelectInput(session, "cmp_b_starters_mode", selected = "lte")
      updateSelectInput(session, "cmp_b_starters_val", selected = "2")
    } else if (identical(preset, "opp_starters_bench")) {
      updateSelectInput(session, "cmp_a_opp_starters_mode", selected = "gte")
      updateSelectInput(session, "cmp_a_opp_starters_val", selected = "3")
      updateSelectInput(session, "cmp_b_opp_starters_mode", selected = "lte")
      updateSelectInput(session, "cmp_b_opp_starters_val", selected = "2")
    } else if (identical(preset, "clutch")) {
      updateCheckboxInput(session, "cmp_a_clutch", value = TRUE)
      updateSliderInput(session, "cmp_a_clutch_margin", value = 5)
      updateSliderInput(session, "cmp_a_clutch_minutes", value = 5)
    } else if (identical(preset, "home_away")) {
      updateSelectInput(session, "cmp_a_home_away", selected = "home")
      updateSelectInput(session, "cmp_b_home_away", selected = "away")
    } else if (identical(preset, "win_loss")) {
      updateSelectInput(session, "cmp_a_outcome", selected = "win")
      updateSelectInput(session, "cmp_b_outcome", selected = "loss")
    } else if (identical(preset, "top_bottom_rank")) {
      updateSelectInput(session, "cmp_a_opp_rank_side", selected = "top")
      updateSelectInput(session, "cmp_b_opp_rank_side", selected = "bottom")
      updateSelectInput(session, "cmp_a_opp_rank_n", selected = "4")
      updateSelectInput(session, "cmp_b_opp_rank_n", selected = "4")
      updateSelectInput(session, "cmp_a_opp_rank_metric", selected = "net")
      updateSelectInput(session, "cmp_b_opp_rank_metric", selected = "net")
    }
  }

  PRESET_MIRROR_BLOCKS <- list(
    home_away = c("home_away"),
    win_loss = c("outcome"),
    starters_bench = c("starters_mode", "starters_val"),
    opp_starters_bench = c("opp_starters_mode", "opp_starters_val"),
    clutch = c("clutch", "clutch_margin", "clutch_minutes"),
    top_bottom_rank = c("opp_rank_side")
  )

  SIDE_MULTI_SELECT_FIELDS <- c("teams", "opponents", "game_type")
  SIDE_CHECKBOX_FIELDS <- c("clutch")
  SIDE_SLIDER_FIELDS <- c("clutch_margin", "clutch_minutes")

  reset_compare_filters <- function() {
    updateSelectInput(session, "cmp_preset", selected = "")
    updateSliderInput(session, "cmp_min_poss", value = 10)
    cmp_auto_min_state$last_auto <- 10L
    cmp_auto_enabled(TRUE)
    updateDateInput(session, "cmp_split_date", value = DEFAULT_END)
    updateSelectizeInput(session, "cmp_split_gn", selected = character(0))
    b <- shared$season_date_bounds(input$game_year %||% DEFAULT_GAME_YEAR)
    updateDateRangeInput(session, "cmp_players_dates", start = b$start, end = b$end, min = b$start, max = b$end)
    updateDateRangeInput(session, "cmp_player_a_dates", start = b$start, end = b$end, min = b$start, max = b$end)
    updateDateRangeInput(session, "cmp_player_b_dates", start = b$start, end = b$end, min = b$start, max = b$end)
    updateSelectizeInput(session, "cmp_players_gn_min", selected = character(0))
    updateSelectizeInput(session, "cmp_players_gn_max", selected = character(0))
    updateSelectizeInput(session, "cmp_player_a_gn_min", selected = character(0))
    updateSelectizeInput(session, "cmp_player_a_gn_max", selected = character(0))
    updateSelectizeInput(session, "cmp_player_b_gn_min", selected = character(0))
    updateSelectizeInput(session, "cmp_player_b_gn_max", selected = character(0))
    for (s in c("a", "b")) {
      reset_compare_side_filters(s, reset_clutch_sliders = TRUE)
    }
    updateSelectizeInput(session, "cmp_player_a", selected = character(0))
    updateSelectizeInput(session, "cmp_player_b", selected = character(0))
    updateSelectizeInput(session, "cmp_player_a_list_team_filter", selected = character(0))
    updateSelectizeInput(session, "cmp_player_b_list_team_filter", selected = character(0))
    updateSelectInput(session, "cmp_player_a_team", selected = "")
    updateSelectInput(session, "cmp_player_b_team", selected = "")
    # Lineup controls
    updateRadioButtons(session, "cmp_lu_num", selected = "5")
    cmp_lu_filter$reset_inputs(team_selected = "")
    selected_detail_entity(NULL)
    selected_team_players_team(NULL)
    selected_team_players_player(NULL)
    reset_stat_filters(cmp_stat_filter_state)
  }

  refresh_player_choices <- function(side) {
    players_df <- normalize_players_ref(cmp_ref$players)
    if (is.null(players_df) || !nrow(players_df)) return(NULL)

    side <- match.arg(side, c("a", "b"))
    list_filter_id <- paste0("cmp_player_", side, "_list_team_filter")
    player_id <- paste0("cmp_player_", side)
    keep_val <- selected_player_value(player_id)

    team_sel <- as.character(input[[list_filter_id]] %||% character(0))
    team_sel <- team_sel[nzchar(team_sel)]
    filtered <- players_df
    if (length(team_sel)) {
      ids <- suppressWarnings(as.integer(team_sel))
      ids <- ids[is.finite(ids)]
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
    if (!(nzchar(keep_val) && keep_val %in% choice_values)) keep_val <- character(0)

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
    self_compare <- identical(cmp_player_compare_mode(), "self")
    if (nzchar(selected_player_value("cmp_player_a"))) return(invisible(NULL))
    if (!isTRUE(self_compare) && nzchar(selected_player_value("cmp_player_b"))) return(invisible(NULL))

    players_df <- normalize_players_ref(cmp_ref$players)
    if (is.null(players_df) || !nrow(players_df)) return(invisible(NULL))
    players_df <- players_df[order(players_df$name), c("player_id", "name"), drop = FALSE]
    players_df <- players_df[!duplicated(players_df$player_id), , drop = FALSE]

    choice_values <- as.character(players_df$player_id)
    player_choices <- stats::setNames(choice_values, players_df$name)
    available_ids <- unique(suppressWarnings(as.integer(players_df$player_id)))
    available_ids <- available_ids[is.finite(available_ids)]
    if (length(available_ids) < 1L) return(invisible(NULL))

    ids <- get_default_player_ids()
    ids <- ids[ids %in% available_ids]
    if (isTRUE(self_compare)) {
      id <- if (length(ids)) ids[[1]] else available_ids[[1]]
      session$onFlushed(function() {
        updateSelectizeInput(session, "cmp_player_a", choices = player_choices, selected = as.character(id), server = FALSE)
        updateSelectizeInput(session, "cmp_player_b", choices = player_choices, selected = as.character(id), server = FALSE)
      }, once = TRUE)
      cmp_auto_default_ids(as.integer(id))
      cmp_defaults_active(TRUE)
      return(invisible(NULL))
    }

    if (length(ids) < 2L) {
      # Fallback to first available two players.
      ids <- available_ids[seq_len(min(2L, length(available_ids)))]
    }
    if (length(ids) < 2L) return(invisible(NULL))

    session$onFlushed(function() {
      updateSelectizeInput(session, "cmp_player_a", choices = player_choices, selected = as.character(ids[[1]]), server = FALSE)
      updateSelectizeInput(session, "cmp_player_b", choices = player_choices, selected = as.character(ids[[2]]), server = FALSE)
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
    cmp_active_view("league")
    selected_team_players_team(NULL)
    selected_team_players_player(NULL)
    if (identical(input$main_tabs, "compare") && mode %in% c("Lineups", "Players")) {
      ensure_cmp_player_refs_loaded(
        as.integer(input$game_year),
        refresh_player_inputs = identical(mode, "Players"),
        apply_defaults = FALSE
      )
    }
  }, ignoreInit = TRUE)

  # -- Tab init: load ref data --

  observeEvent(list(input$main_tabs, input$game_year), ignoreInit = FALSE, {
    if (!identical(input$main_tabs, "compare")) return(NULL)
    gy_int <- as.integer(input$game_year)
    token <- cmp_init_token() + 1L

    cmp_init_token(token)
    cmp_ready(FALSE)
    cmp_auto_min_bootstrapped(FALSE)

    refresh_compare_ref_inputs(
      gy_int,
      include_players = (input$cmp_mode %in% c("Lineups", "Players"))
    )
    # Apply pending preset from home tab
    pending <- shared$pending_compare_preset()
    if (!is.null(pending) && nzchar(pending)) {
      shared$pending_compare_preset(NULL)
      reset_compare_side_filters("a", reset_clutch_sliders = FALSE)
      reset_compare_side_filters("b", reset_clutch_sliders = FALSE)
      apply_compare_preset(pending)
      cmp_suppress_preset_echo(pending)
      updateSelectInput(session, "cmp_preset", selected = pending)
    }

    release_compare_ready_after_flush(token)
  })

  # ignoreNULL = FALSE so clearing the last team (multi-select -> NULL) still fires
  # and restores the full player list, instead of leaving it filtered.
  observeEvent(input$cmp_player_a_list_team_filter, {
    refresh_player_choices("a")
  }, ignoreInit = TRUE, ignoreNULL = FALSE)

  observeEvent(input$cmp_player_b_list_team_filter, {
    refresh_player_choices("b")
  }, ignoreInit = TRUE, ignoreNULL = FALSE)

  observeEvent(list(input$cmp_player_a, input$cmp_player_b), {
    ids <- cmp_auto_default_ids()
    if (!length(ids)) return()
    cur_a <- suppressWarnings(as.integer(input$cmp_player_a %||% ""))
    cur_b <- suppressWarnings(as.integer(input$cmp_player_b %||% ""))
    if (identical(cmp_player_compare_mode(), "self")) {
      if (!(is.finite(cur_a) && cur_a %in% ids)) {
        cmp_defaults_active(FALSE)
      }
      return()
    }
    if (!((is.finite(cur_a) && cur_a %in% ids) && (is.finite(cur_b) && cur_b %in% ids))) {
      cmp_defaults_active(FALSE)
    }
  }, ignoreInit = TRUE)

  players_filters_pristine <- function() {
    b <- shared$season_date_bounds(input$game_year %||% DEFAULT_GAME_YEAR)
    same_date_input <- function(dr) {
      if (is.null(dr) || length(dr) < 2) return(TRUE)
      d_start <- suppressWarnings(as.Date(dr[[1]]))
      d_end <- suppressWarnings(as.Date(dr[[2]]))
      isTRUE(!is.na(d_start) && !is.na(d_end) && identical(d_start, as.Date(b$start)) && identical(d_end, as.Date(b$end)))
    }
    same_dates <- if (identical(cmp_player_compare_mode(), "self")) {
      same_date_input(input$cmp_player_a_dates) && same_date_input(input$cmp_player_b_dates)
    } else {
      same_date_input(input$cmp_players_dates)
    }

    empty_chr <- function(x) is.null(x) || !length(x) || !any(nzchar(as.character(x)))
    is_false <- function(x) isFALSE(isTRUE(x))

    same_dates &&
      (if (identical(cmp_player_compare_mode(), "self")) {
        empty_chr(input$cmp_player_a_gn_min) &&
          empty_chr(input$cmp_player_a_gn_max) &&
          empty_chr(input$cmp_player_b_gn_min) &&
          empty_chr(input$cmp_player_b_gn_max)
      } else {
        empty_chr(input$cmp_players_gn_min) &&
          empty_chr(input$cmp_players_gn_max)
      }) &&
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
    input$cmp_player_a_dates, input$cmp_player_a_gn_min, input$cmp_player_a_gn_max,
    input$cmp_player_b_dates, input$cmp_player_b_gn_min, input$cmp_player_b_gn_max,
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
    if (!length(ids)) return()
    cur_a <- suppressWarnings(as.integer(input$cmp_player_a %||% ""))
    cur_b <- suppressWarnings(as.integer(input$cmp_player_b %||% ""))
    if (identical(cmp_player_compare_mode(), "self")) {
      if (is.finite(cur_a) && cur_a %in% ids) {
        updateSelectizeInput(session, "cmp_player_a", selected = character(0), server = FALSE)
        updateSelectizeInput(session, "cmp_player_b", selected = character(0), server = FALSE)
      }
      cmp_defaults_active(FALSE)
      return()
    }
    if ((is.finite(cur_a) && cur_a %in% ids) && (is.finite(cur_b) && cur_b %in% ids)) {
      updateSelectizeInput(session, "cmp_player_a", selected = character(0), server = FALSE)
      updateSelectizeInput(session, "cmp_player_b", selected = character(0), server = FALSE)
    }
    cmp_defaults_active(FALSE)
  }, ignoreInit = TRUE)

  # -- Preset handler --

  observeEvent(input$cmp_preset, {
    preset <- input$cmp_preset
    suppress_preset <- cmp_suppress_preset_echo()
    if (!is.null(suppress_preset)) {
      cmp_suppress_preset_echo(NULL)
      if (identical(preset, suppress_preset)) {
        return()
      }
    }
    if (is.null(preset) || !nzchar(preset)) {
      return()
    }
    reset_compare_side_filters("a", reset_clutch_sliders = FALSE)
    reset_compare_side_filters("b", reset_clutch_sliders = FALSE)
    apply_compare_preset(preset)
  }, ignoreInit = TRUE)

  # Guide user when Date split preset is selected without a valid split date.
  observe({
    if (!identical(input$main_tabs, "compare")) return(invisible(NULL))
    if (!identical(input$cmp_mode, "Players") && identical(input$cmp_preset %||% "", "date_split")) {
      split_date <- parse_single_date(input$cmp_split_date)
      if (is.na(split_date)) {
        showNotification(
          "Date split preset needs a Split date. Please pick a date to continue.",
          type = "warning", duration = NULL, id = "cmp_split_date_missing"
        )
      } else {
        removeNotification("cmp_split_date_missing")
      }
    } else {
      removeNotification("cmp_split_date_missing")
    }
    invisible(NULL)
  })

  # -- Preset mode: mirror side filters A <-> B --
  # Keep preset-defining contrasts independent (e.g., home vs away).
  is_mirror_blocked <- function(preset, field) {
    field %in% (PRESET_MIRROR_BLOCKS[[preset]] %||% character(0))
  }

  normalize_side_field_value <- function(field, val) {
    if (field %in% SIDE_MULTI_SELECT_FIELDS) {
      v <- as.character(val %||% character(0))
      v <- v[nzchar(v)]
      return(sort(unique(v)))
    }
    if (field %in% SIDE_CHECKBOX_FIELDS) {
      return(isTRUE(val))
    }
    if (field %in% SIDE_SLIDER_FIELDS) {
      n <- suppressWarnings(as.numeric(val))
      if (!is.finite(n)) return(NA_real_)
      return(n)
    }
    as.character(val %||% "")
  }

  side_field_equal <- function(field, a, b) {
    va <- normalize_side_field_value(field, a)
    vb <- normalize_side_field_value(field, b)
    identical(va, vb)
  }

  mirror_side_field <- function(from_side, to_side, field) {
    from_id <- paste0("cmp_", from_side, "_", field)
    to_id <- paste0("cmp_", to_side, "_", field)
    val <- input[[from_id]]
    current_to <- input[[to_id]]

    if (side_field_equal(field, val, current_to)) return(invisible(NULL))

    if (field %in% SIDE_MULTI_SELECT_FIELDS) {
      updateSelectizeInput(session, to_id, selected = val %||% character(0))
    } else if (field %in% SIDE_CHECKBOX_FIELDS) {
      updateCheckboxInput(session, to_id, value = isTRUE(val))
    } else if (field %in% SIDE_SLIDER_FIELDS) {
      if (is.null(val) || !is.finite(as.numeric(val))) return(invisible(NULL))
      updateSliderInput(session, to_id, value = as.numeric(val))
    } else {
      updateSelectInput(session, to_id, selected = val %||% "")
    }
    invisible(NULL)
  }

  observe_mirror_side_field <- function(field, from_side, to_side) {
    observeEvent(input[[paste0("cmp_", from_side, "_", field)]], {
      if (isTRUE(cmp_syncing_sides())) return(invisible(NULL))
      if (identical(input$cmp_mode, "Players")) return(invisible(NULL))
      preset <- input$cmp_preset %||% ""
      if (!nzchar(preset) || is_mirror_blocked(preset, field)) return(invisible(NULL))
      if (side_field_equal(field, input[[paste0("cmp_", from_side, "_", field)]], input[[paste0("cmp_", to_side, "_", field)]])) {
        return(invisible(NULL))
      }
      cmp_syncing_sides(TRUE); on.exit(cmp_syncing_sides(FALSE), add = TRUE)
      mirror_side_field(from_side, to_side, field)
    }, ignoreInit = TRUE)
  }

  side_fields <- c(
    "starters_mode", "starters_val", "opp_starters_mode", "opp_starters_val", "teams",
    "home_away", "outcome",
    "clutch", "clutch_margin", "clutch_minutes",
    "opponents", "game_type",
    "opp_rank_side", "opp_rank_n", "opp_rank_metric"
  )

  for (fld in side_fields) {
    local({
      field <- fld
      observe_mirror_side_field(field, "a", "b")
      observe_mirror_side_field(field, "b", "a")
    })
  }

  # -- PvP Player Comparison (Players mode) --

  player_team_choices <- function(player_id_chr) {
    refs <- cmp_refs_state()
    players_df <- refs$players
    teams_df <- refs$teams
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
    list(label = "Deflections", col = "dfl", type = "count"),
    list(label = "Disruptions", col = "disruptions", type = "count"),
    list(label = "FG%",       col = "fg_pct", type = "pct"),
    list(label = "3P%",       col = "tp_pct", type = "pct"),
    list(label = "FT%",       col = "ft_pct", type = "pct"),
    list(label = "TS%",       col = "ts",     type = "pct"),
    list(label = "USG%",      col = "usg_pct", type = "pct")
  )

  cmp_player_raw_live <- reactive({
    req(identical(input$cmp_mode, "Players"))
    req(identical(input$main_tabs, "compare"))

    player_state <- cmp_player_selection_state()
    req(player_state)
    side_state <- cmp_side_state()
    pa <- side_state$a
    pb <- side_state$b

    res_a <- run_player_traditional(pa, player_state$team_csv_a)
    res_b <- run_player_traditional(pb, player_state$team_csv_b)
    if (!nrow(res_a) || !nrow(res_b)) return(NULL)

    row_a <- res_a[res_a$player_id == player_state$player_a_id_int, , drop = FALSE]
    row_b <- res_b[res_b$player_id == player_state$player_b_id_int, , drop = FALSE]
    if (!nrow(row_a) || !nrow(row_b)) return(NULL)

    list(
      row_a = row_a[1, ], row_b = row_b[1, ],
      name_a = player_state$name_a, name_b = player_state$name_b,
      team_a = player_state$team_name_a, team_b = player_state$team_name_b,
      team_ids_a = player_state$team_ids_a, team_ids_b = player_state$team_ids_b,
      player_a_id_int = player_state$player_a_id_int,
      player_b_id_int = player_state$player_b_id_int,
      pa = pa, pb = pb
    )
  })

  cmp_player_raw_state <- reactive({
    if (!identical(input$cmp_mode, "Players") || !identical(input$main_tabs, "compare")) {
      return(list(status = "idle", data = NULL))
    }

    live <- tryCatch(
      cmp_player_raw_live(),
      shiny.silent.error = function(e) structure(list(), class = "cmp_player_pending")
    )

    if (!inherits(live, "cmp_player_pending")) {
      return(list(status = "ready", data = live))
    }

    cached <- cmp_player_raw_cache()
    if (is.null(cached)) {
      return(list(status = "pending", data = NULL))
    }

    list(status = "stale", data = cached)
  })

  cmp_player_raw <- reactive(cmp_player_raw_state()$data)

  observe({
    live <- tryCatch(
      cmp_player_raw_live(),
      shiny.silent.error = function(e) structure(list(), class = "cmp_player_pending")
    )
    if (!inherits(live, "cmp_player_pending")) {
      cmp_player_raw_cache(live)
    }
    invisible(NULL)
  })

  cmp_player_ff_raw <- reactive({
    req(identical(input$cmp_mode, "Players"))
    req(identical(selected_player_view(), "ff_swing"))
    data <- cmp_player_raw()
    req(data)

    ff_a <- run_four_factors(data$pa, paste(data$team_ids_a, collapse = ","))
    ff_b <- run_four_factors(data$pb, paste(data$team_ids_b, collapse = ","))
    onoff_a <- run_onoff_impact(data$pa, paste(data$team_ids_a, collapse = ","))
    onoff_b <- run_onoff_impact(data$pb, paste(data$team_ids_b, collapse = ","))
    if (!nrow(ff_a) || !nrow(ff_b)) return(NULL)

    row_a <- ff_a[ff_a$player_id == data$player_a_id_int, , drop = FALSE]
    row_b <- ff_b[ff_b$player_id == data$player_b_id_int, , drop = FALSE]
    if (!nrow(row_a) || !nrow(row_b)) return(NULL)
    on_a <- onoff_a[onoff_a$player_id == data$player_a_id_int, , drop = FALSE]
    on_b <- onoff_b[onoff_b$player_id == data$player_b_id_int, , drop = FALSE]

    list(
      row_a = row_a[1, ], row_b = row_b[1, ],
      onoff_a = if (nrow(on_a)) on_a[1, ] else NULL,
      onoff_b = if (nrow(on_b)) on_b[1, ] else NULL,
      name_a = data$name_a, name_b = data$name_b,
      team_a = data$team_a, team_b = data$team_b
    )
  })

  cmp_player_shot_raw <- reactive({
    req(identical(input$cmp_mode, "Players"))
    req(identical(selected_player_view(), "shot_profile"))
    data <- cmp_player_raw()
    req(data)

    onoff_a <- run_onoff_impact(data$pa, paste(data$team_ids_a, collapse = ","))
    onoff_b <- run_onoff_impact(data$pb, paste(data$team_ids_b, collapse = ","))
    on_a <- onoff_a[onoff_a$player_id == data$player_a_id_int, , drop = FALSE]
    on_b <- onoff_b[onoff_b$player_id == data$player_b_id_int, , drop = FALSE]
    if (!nrow(on_a) || !nrow(on_b)) return(NULL)

    list(
      onoff_a = on_a[1, , drop = FALSE], onoff_b = on_b[1, , drop = FALSE],
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

  pvp_stat_row <- function(label, va, vb, fmt_fn, higher_is_better = TRUE,
                           sub_a = NULL, sub_b = NULL, neutral = FALSE) {
    diff <- if (!is.na(va) && !is.na(vb)) abs(va - vb) else NA_real_
    if (neutral) {
      a_better <- FALSE
      b_better <- FALSE
    } else if (higher_is_better) {
      a_better <- !is.na(va) && !is.na(vb) && va > vb
      b_better <- !is.na(va) && !is.na(vb) && vb > va
    } else {
      a_better <- !is.na(va) && !is.na(vb) && va < vb
      b_better <- !is.na(va) && !is.na(vb) && vb < va
    }
    diff_txt <- if (!neutral && !is.na(diff) && diff > 0.05) sprintf("+%.1f", diff) else NULL
    a_css <- if (neutral || a_better) val_win_css else val_lose_css
    b_css <- if (neutral || b_better) val_win_css else val_lose_css
    left_badge <- if (a_better && !is.null(diff_txt)) tags$span(style = badge_css, diff_txt) else NULL
    right_badge <- if (b_better && !is.null(diff_txt)) tags$span(style = badge_css, diff_txt) else NULL

    tags$div(
      style = "display: flex; align-items: center; padding: 10px 0; border-bottom: 1px solid rgba(255,255,255,.06);",
      tags$div(
        style = "flex: 1; display: flex; align-items: center; justify-content: flex-end; gap: 10px;",
        left_badge,
        tags$div(
          style = "text-align: right;",
          tags$span(style = a_css, fmt_fn(va)),
          sub_a
        )
      ),
      tags$div(
        style = "width: 130px; text-align: center; font-size: .85rem; font-weight: 600; color: #8b949e;",
        label
      ),
      tags$div(
        style = "flex: 1; display: flex; align-items: center; justify-content: flex-start; gap: 10px;",
        tags$div(
          style = "text-align: left;",
          tags$span(style = b_css, fmt_fn(vb)),
          sub_b
        ),
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

  cmp_player_state_card <- function(message) {
    tags$div(
      class = "card bg-dark border-secondary p-3",
      tags$div(class = "small text-muted", message)
    )
  }

  player_side_time_label <- function(side) {
    if (!identical(cmp_player_compare_mode(), "self")) return("")
    p <- collect_side_params(side)
    parts <- character(0)
    if (!is.na(p$start_d) && !is.na(p$end_d)) {
      parts <- c(parts, paste(format(as.Date(p$start_d), "%Y-%m-%d"), "to", format(as.Date(p$end_d), "%Y-%m-%d")))
    }
    if (is.finite(p$min_gn) || is.finite(p$max_gn)) {
      from <- if (is.finite(p$min_gn)) as.character(p$min_gn) else "Any"
      to <- if (is.finite(p$max_gn)) as.character(p$max_gn) else "Any"
      parts <- c(parts, paste0("GN ", from, "-", to))
    }
    paste(parts, collapse = " | ")
  }

  # -- FF Swing view --

  FF_SWING_STATS <- list(
    list(label = "Off Diff", col = "Off ON Diff", side = "off"),
    list(label = "eFG%",     col = "Off eFG% Diff", side = "off", factor = "efg"),
    list(label = "OREB%",    col = "Off OREB% Diff", side = "off", factor = "oreb"),
    list(label = "TOV%",     col = "Off TOV% Diff", side = "off", invert = TRUE, factor = "tov"),
    list(label = "FTR",      col = "Off FTR Diff", side = "off", factor = "ftr"),
    list(label = "Def Diff", col = "Def ON Diff", side = "def", invert = TRUE),
    list(label = "eFG%",     col = "Def eFG% Diff", side = "def", invert = TRUE, factor = "efg"),
    list(label = "OREB%",    col = "Def OREB% Diff", side = "def", invert = TRUE, factor = "oreb"),
    list(label = "TOV%",     col = "Def TOV% Diff", side = "def", factor = "tov"),
    list(label = "FTR",      col = "Def FTR Diff", side = "def", invert = TRUE, factor = "ftr"),
    list(label = "Disruptions/100", col = "Def Disruptions/100 Diff", side = "activity", requires_def_poss = TRUE)
  )

  render_ff_swing_ui <- function() {
    trad_state <- cmp_player_raw_state()
    if (identical(trad_state$status, "pending")) {
      return(cmp_player_state_card("Preparing player compare..."))
    }

    trad <- trad_state$data
    if (is.null(trad)) {
      return(cmp_player_state_card("No player data for current filters."))
    }

    data <- cmp_player_ff_raw()
    if (is.null(data)) {
      return(cmp_player_state_card("No player data for current filters."))
    }

    row_a <- data$row_a
    row_b <- data$row_b
    onoff_a <- data$onoff_a
    onoff_b <- data$onoff_b

    # GP / poss info from traditional data
    gp_a <- as.numeric(trad$row_a[["gp"]]); gp_b <- as.numeric(trad$row_b[["gp"]])
    poss_a <- if ("off_on_poss" %in% names(row_a)) as.numeric(row_a[["off_on_poss"]]) else NA_real_
    poss_b <- if ("off_on_poss" %in% names(row_b)) as.numeric(row_b[["off_on_poss"]]) else NA_real_
    info_line_ff <- function(gp, poss, side) {
      parts <- c()
      if (!is.na(gp)) parts <- c(parts, paste0(gp, " GP"))
      if (!is.na(poss)) parts <- c(parts, paste0(round(poss), " ON Poss"))
      time_label <- player_side_time_label(side)
      if (nzchar(time_label)) parts <- c(parts, time_label)
      paste(parts, collapse = " \u00b7 ")
    }

    fmt_swing <- function(v) {
      if (is.na(v)) return("\u2014")
      sprintf("%+.1f", v)
    }

    est_sub <- function(v, stat) {
      if (is.null(stat$factor) || is.na(v)) return(NULL)
      est <- ff_impact_pts(v, stat$factor)
      suffix <- if (identical(stat$side, "def")) " pts allowed" else " pts"
      tags$div(class = "ff-impact-est", title = FF_IMPACT_EST_TITLE,
               sprintf("est. %+.1f%s", est, suffix))
    }

    get_swing <- function(ff_row, onoff_row, stat) {
      source_row <- if (grepl("Diff$", stat$label)) onoff_row else ff_row
      if (is.null(source_row) || is.null(stat$col) || !(stat$col %in% names(source_row))) return(NA_real_)
      if (isTRUE(stat$requires_def_poss)) {
        poss_cols <- c("def_on_poss", "def_off_poss")
        if (!all(poss_cols %in% names(source_row))) return(NA_real_)
        poss <- suppressWarnings(as.numeric(unlist(source_row[1, poss_cols], use.names = FALSE)))
        if (any(!is.finite(poss)) || any(poss < 300)) return(NA_real_)
      }
      as.numeric(source_row[[stat$col]])
    }

    # Split stats by side
    off_stats <- FF_SWING_STATS[vapply(FF_SWING_STATS, function(s) s$side == "off", logical(1))]
    def_stats <- FF_SWING_STATS[vapply(FF_SWING_STATS, function(s) s$side == "def", logical(1))]
    activity_stats <- FF_SWING_STATS[vapply(FF_SWING_STATS, function(s) s$side == "activity", logical(1))]

    make_rows <- function(stats) {
      lapply(stats, function(stat) {
        va <- get_swing(row_a, onoff_a, stat)
        vb <- get_swing(row_b, onoff_b, stat)
        higher_is_better <- !isTRUE(stat$invert)
        pvp_stat_row(stat$label, va, vb, fmt_swing, higher_is_better,
                     sub_a = est_sub(va, stat), sub_b = est_sub(vb, stat))
      })
    }

    tagList(
      pvp_header(
        data$name_a, data$team_a, info_line_ff(gp_a, poss_a, "a"),
        data$name_b, data$team_b, info_line_ff(gp_b, poss_b, "b")
      ),
      tags$div(
        style = "max-width: 520px; margin: 0 auto;",
        tags$div(
          style = "text-align: center; font-size: .72rem; color: #6e7681; margin-bottom: 8px;",
          paste("Swing values use the same diffs as Tab 1 Four Factors (plus Off/Def Diff).", ff_impact_legend())
        ),
        pvp_section_header("Offensive Four Factors"),
        do.call(tagList, make_rows(off_stats)),
        pvp_section_header("Defensive Four Factors"),
        do.call(tagList, make_rows(def_stats)),
        pvp_section_header("Defensive Activity"),
        do.call(tagList, make_rows(activity_stats))
      )
    )
  }

  # -- Shot Profile view (descriptive shot-diet swing; no impact framing) --

  add_player_shot_profile_shares <- function(row) {
    if (is.null(row) || !nrow(row)) return(NULL)
    prefixes <- c("off_on", "off_off", "def_on", "def_off")
    need <- as.vector(outer(prefixes, c("_layup_att", "_dunk_att", "_fg2_att", "_fg3_att", "_c3_att", "_c3_known_att"), paste0))
    if (!all(need %in% names(row))) return(NULL)
    num0 <- function(col) {
      x <- suppressWarnings(as.numeric(row[[col]]))
      ifelse(is.na(x), 0, x)
    }
    for (p in prefixes) {
      row[[paste0(p, "_fga_in")]] <- num0(paste0(p, "_fg2_att")) + num0(paste0(p, "_fg3_att"))
    }
    add_shot_profile_metrics(row, stats::setNames(lapply(prefixes, function(p) {
      paste0(p, c("_layup_att", "_dunk_att", "_fga_in", "_fg3_att", "_c3_att", "_c3_known_att"))
    }), prefixes))
  }

  render_shot_profile_ui <- function() {
    trad_state <- cmp_player_raw_state()
    if (identical(trad_state$status, "pending")) {
      return(cmp_player_state_card("Preparing player compare..."))
    }
    data <- cmp_player_shot_raw()
    if (is.null(data)) {
      return(cmp_player_state_card("No player data for current filters."))
    }

    row_a <- add_player_shot_profile_shares(data$onoff_a)
    row_b <- add_player_shot_profile_shares(data$onoff_b)
    if (is.null(row_a) || is.null(row_b)) {
      return(cmp_player_state_card("Shot Profile columns unavailable for current filters."))
    }

    poss_a <- if ("ON Poss" %in% names(row_a)) as.numeric(row_a[["ON Poss"]]) else NA_real_
    poss_b <- if ("ON Poss" %in% names(row_b)) as.numeric(row_b[["ON Poss"]]) else NA_real_
    info_line <- function(poss, side) {
      parts <- character(0)
      if (is.finite(poss)) parts <- c(parts, paste0(round(poss), " ON Poss"))
      time_label <- player_side_time_label(side)
      if (nzchar(time_label)) parts <- c(parts, time_label)
      paste(parts, collapse = " · ")
    }

    sp_labels <- c("Lay-up%", "Dunk%", "Lay+Dunk%", "3PA%", "C3% of 3PA", "2PT Jumper%")
    sp_suffix <- c("layup_share", "dunk_share", "rim_share", "fg3_share", "c3_pct3", "mid_share")

    swing <- function(row, side, m) {
      on_v <- suppressWarnings(as.numeric(row[[paste0(side, "_on_", m)]]))
      off_v <- suppressWarnings(as.numeric(row[[paste0(side, "_off_", m)]]))
      if (!is.finite(on_v) || !is.finite(off_v)) return(list(d = NA_real_, on = on_v, off = off_v))
      list(d = round(on_v - off_v, 1), on = on_v, off = off_v)
    }
    fmt_swing <- function(v) if (is.na(v)) "—" else sprintf("%+.1f", v)
    onoff_sub <- function(s) {
      if (!is.finite(s$on) || !is.finite(s$off)) return(NULL)
      tags$div(style = "font-size:.72rem; color:#6e7681;",
               sprintf("on %.1f | off %.1f", s$on, s$off))
    }

    make_rows <- function(side) {
      lapply(seq_along(sp_suffix), function(i) {
        sa <- swing(row_a, side, sp_suffix[i])
        sb <- swing(row_b, side, sp_suffix[i])
        pvp_stat_row(sp_labels[i], sa$d, sb$d, fmt_swing,
                     sub_a = onoff_sub(sa), sub_b = onoff_sub(sb), neutral = TRUE)
      })
    }

    tagList(
      pvp_header(
        data$name_a, data$team_a, info_line(poss_a, "a"),
        data$name_b, data$team_b, info_line(poss_b, "b")
      ),
      tags$div(
        style = "max-width: 520px; margin: 0 auto;",
        tags$div(
          style = "text-align: center; font-size: .72rem; color: #6e7681; margin-bottom: 8px;",
          "Team shot-diet shift with the player ON vs OFF the floor (share of team FGA, percentage points). Descriptive — no point-impact estimate. C3% is of 3PA with known location; — = unknown."
        ),
        pvp_section_header("Offensive Shot Diet (ON − OFF)"),
        do.call(tagList, make_rows("off")),
        pvp_section_header("Defensive Shot Diet (ON − OFF)"),
        do.call(tagList, make_rows("def"))
      )
    )
  }

  # -- Overall PvP view --

  output$cmp_pvp_ui <- renderUI({
    if (identical(cmp_player_compare_mode(), "self")) {
      if (!nzchar(selected_player_value("cmp_player_a"))) {
        return(cmp_player_state_card("Select a player to run Players compare."))
      }
    } else if (!nzchar(selected_player_value("cmp_player_a")) || !nzchar(selected_player_value("cmp_player_b"))) {
      return(cmp_player_state_card("Select Player A and Player B to run Players compare."))
    }

    view <- selected_player_view()
    if (identical(view, "ff_swing")) {
      return(render_ff_swing_ui())
    }

    if (identical(view, "shot_profile")) {
      return(render_shot_profile_ui())
    }

    data_state <- cmp_player_raw_state()
    if (identical(data_state$status, "pending")) {
      return(cmp_player_state_card("Preparing player compare..."))
    }

    data <- data_state$data
    if (is.null(data)) {
      return(cmp_player_state_card("No player data for current filters."))
    }

    rate <- input$cmp_rate_mode %||% "Per Game"
    row_a <- data$row_a
    row_b <- data$row_b

    get_val <- function(row, stat) {
      col <- stat$col
      raw <- if (identical(col, "disruptions")) {
        if (!all(c("stl", "dfl") %in% names(row))) return(NA_real_)
        as.numeric(row[["stl"]]) + as.numeric(row[["dfl"]])
      } else {
        if (!(col %in% names(row))) return(NA_real_)
        as.numeric(row[[col]])
      }
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
    info_line <- function(gp, mpg, side) {
      parts <- c()
      if (!is.na(gp)) parts <- c(parts, paste0(gp, " GP"))
      if (!is.na(mpg)) parts <- c(parts, paste0(sprintf("%.1f", mpg), " MPG"))
      time_label <- player_side_time_label(side)
      if (nzchar(time_label)) parts <- c(parts, time_label)
      paste(parts, collapse = " \u00b7 ")
    }

    tagList(
      pvp_header(
        data$name_a, data$team_a, info_line(gp_a, min_a, "a"),
        data$name_b, data$team_b, info_line(gp_b, min_b, "b")
      ),
      tags$div(
        style = "max-width: 520px; margin: 0 auto;",
        do.call(tagList, stat_rows)
      )
    )
  })

  # -- Reactive comparison (auto-triggers on filter change) --

  cmp_joined_inner <- function(apply_min_poss = TRUE, limit_lineups = TRUE, sql_min_poss = cmp_min_poss()) {
    req(identical(input$main_tabs, "compare"))
    mode <- input$cmp_mode
    req(mode)

    # Players mode handled by cmp_pvp_ui - skip here
    if (identical(mode, "Players")) return(NULL)

    side_state <- cmp_side_state()
    pa <- side_state$a
    pb <- side_state$b
    metric <- selected_metric()
    if (mode == "Teams") {
      req(metric %in% TEAM_METRICS)
      is_ff <- metric %in% c("off_efg", "off_tov", "off_oreb", "off_ftr")
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
      if (!nrow(joined)) return(NULL)
      joined

    } else if (mode == "Lineups") {
      req(metric %in% TEAM_METRICS)
      is_ff <- metric %in% c("off_efg", "off_tov", "off_oreb", "off_ftr")
      if (is_ff) {
        res_a <- run_lineups_ff(pa, min_poss = sql_min_poss)
        res_b <- run_lineups_ff(pb, min_poss = sql_min_poss)
      } else {
        res_a <- run_lineups_summary(pa, min_poss = sql_min_poss)
        res_b <- run_lineups_summary(pb, min_poss = sql_min_poss)
      }
      res_a <- apply_side_team_filter(res_a, pa)
      res_b <- apply_side_team_filter(res_b, pb)
      if (!nrow(res_a) || !nrow(res_b)) return(NULL)

      pick_cols_lu <- function(df, suffix) {
        key <- "sub_lineup_hash"
        team_id_col <- if ("team_id" %in% names(df)) "team_id" else NULL
        name_col <- if ("player_names_str" %in% names(df)) "player_names_str" else NULL
        team_col <- if ("team_name" %in% names(df)) "team_name" else NULL
        total_poss_col <- if ("total_poss" %in% names(df)) "total_poss" else NULL
        off_poss_col <- if ("off_poss" %in% names(df)) "off_poss" else NULL
        def_poss_col <- if ("def_poss" %in% names(df)) "def_poss" else NULL
        metric_col <- if (metric %in% names(df)) metric else NULL
        cols <- c(key, team_id_col, name_col, team_col, metric_col, total_poss_col, off_poss_col, def_poss_col)
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
      extra <- intersect(c("player_names_str", "team_id", "team_name"), intersect(names(df_a), names(df_b)))
      joined <- dplyr::inner_join(df_a, df_b, by = c(join_by, extra), suffix = c("", ".b"))
      if (!("team_name" %in% names(joined)) && ("team_id" %in% names(joined))) {
        teams_df <- normalize_teams_ref(cmp_ref$teams)
        if (!is.null(teams_df) && nrow(teams_df)) {
          team_map <- setNames(teams_df$team_name, as.character(teams_df$team_id))
          joined$team_name <- unname(team_map[as.character(joined$team_id)])
        }
      }
      if (!nrow(joined)) return(NULL)
      joined

    } else if (mode == "Players") {
      # Ensure metric is valid for Players mode before querying
      req(metric %in% PLAYER_METRICS)

      player_state <- cmp_player_selection_state()
      req(player_state)

      res_a <- run_player_traditional(pa, player_state$team_csv_a)
      res_b <- run_player_traditional(pb, player_state$team_csv_b)
      if (!nrow(res_a) || !nrow(res_b)) return(NULL)

      res_a <- res_a[res_a$player_id == player_state$player_a_id_int, , drop = FALSE]
      res_b <- res_b[res_b$player_id == player_state$player_b_id_int, , drop = FALSE]
      if (!nrow(res_a) || !nrow(res_b)) return(NULL)

      rate_mode <- input$cmp_rate_mode %||% "Per Game"

      # SQL returns raw totals: pts, reb, ast, stl, gp, poss_on_floor, minutes
      # Percentages: fg_pct, tp_pct, ft_pct, ts
      get_player_metric <- function(row, m, rate) {
        count_map <- c("ppg" = "pts", "rpg" = "reb", "apg" = "ast", "spg" = "stl")
        pct_map <- c("fg_pct" = "fg_pct", "fg3_pct" = "tp_pct", "ft_pct" = "ft_pct", "ts_pct" = "ts", "usg_pct" = "usg_pct")

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

      val_a <- get_player_metric(res_a[1, ], metric, rate_mode)
      val_b <- get_player_metric(res_b[1, ], metric, rate_mode)
      poss_a <- if ("poss_on_floor" %in% names(res_a)) as.numeric(res_a$poss_on_floor[1]) else NA_real_
      poss_b <- if ("poss_on_floor" %in% names(res_b)) as.numeric(res_b$poss_on_floor[1]) else NA_real_

      data.frame(
        rank = 1L,
        entity_name = paste0(player_state$name_a, " vs ", player_state$name_b),
        metric_a = val_a, poss_a = poss_a,
        metric_b = val_b, poss_b = poss_b,
        gap = val_a - val_b, stringsAsFactors = FALSE
      )
    } else {
      NULL
    }
  }

  finalize_cmp_joined <- function(joined,
                                  mode,
                                  gap_after_minus_before = FALSE,
                                  apply_min_poss = TRUE,
                                  limit_lineups = TRUE,
                                  min_poss = cmp_min_poss()) {
    if (is.null(joined) || !nrow(joined)) return(NULL)

    out <- joined
    out$metric_a <- as.numeric(out$metric_a)
    out$metric_b <- as.numeric(out$metric_b)

    if (isTRUE(apply_min_poss)) out <- apply_min_poss_filter(out, min_poss = min_poss)
    if (is.null(out) || !nrow(out)) return(NULL)

    out$gap <- if (gap_after_minus_before) out$metric_b - out$metric_a else out$metric_a - out$metric_b
    out <- out[order(is.na(out$gap), -out$gap), , drop = FALSE]

    if (identical(mode, "Lineups") && isTRUE(limit_lineups)) {
      out <- utils::head(out, 50L)
    }

    out$rank <- seq_len(nrow(out))
    out$entity_name <- if (identical(mode, "Teams")) {
      out$team_name
    } else if ("player_names_str" %in% names(out)) {
      out$player_names_str
    } else {
      out$sub_lineup_hash
    }

    out
  }

  cmp_joined_base <- reactive({
    cmp_profile_time(
      "cmp_joined_base",
      {
        req(identical(input$main_tabs, "compare"))
        req(isTRUE(cmp_ready()))
        mode <- input$cmp_mode
        req(mode)
        if (identical(mode, "Players")) return(NULL)

        sql_min_poss <- if (identical(mode, "Lineups")) 0L else cmp_min_poss()
        cmp_joined_inner(apply_min_poss = FALSE, limit_lineups = FALSE, sql_min_poss = sql_min_poss)
      },
      extra = function(res) sprintf(
        "mode=%s;rows=%d;min_poss=%s",
        input$cmp_mode %||% "",
        NROW(res),
        cmp_min_poss()
      )
    )
  })

  observe({
    if (!identical(input$main_tabs, "compare")) return(invisible(NULL))
    if (identical(input$cmp_mode, "Players")) return(invisible(NULL))
    if (!isTRUE(cmp_ready())) return(invisible(NULL))
    if (!isTRUE(cmp_auto_enabled())) return(invisible(NULL))

    if (!isTRUE(cmp_auto_min_bootstrapped())) {
      cmp_auto_min_bootstrapped(TRUE)
      cmp_auto_min_state$last_auto <- suppressWarnings(as.integer(input$cmp_min_poss %||% 10L))
      cmp_profile_log("cmp_auto_min_bootstrap", extra = sprintf("value=%s", input$cmp_min_poss %||% ""))
      return(invisible(NULL))
    }

    df_base <- cmp_joined_base()
    min_needed <- cmp_auto_minposs_from_df(df_base)
    cur_val <- suppressWarnings(as.integer(input$cmp_min_poss %||% 10L))
    if (is.na(min_needed)) return(invisible(NULL))
    if (!is.na(cur_val) && cur_val == min_needed) return(invisible(NULL))

    cmp_auto_min_state$updating <- TRUE
    updateSliderInput(session, "cmp_min_poss", value = min_needed)
    cmp_auto_min_state$updating <- FALSE
    cmp_auto_min_state$last_auto <- min_needed
    cmp_profile_log("cmp_auto_min_update", extra = sprintf("from=%s;to=%s", cur_val, min_needed))
    invisible(NULL)
  })

  cmp_joined <- reactive({
    cmp_profile_time(
      "cmp_joined",
      {
        mode <- input$cmp_mode
        req(mode)
        if (identical(mode, "Players")) return(NULL)

        out <- finalize_cmp_joined(
          cmp_joined_base(),
          mode = mode,
          gap_after_minus_before = cmp_gap_after_minus_before(),
          apply_min_poss = TRUE,
          limit_lineups = TRUE,
          min_poss = cmp_min_poss()
        )
        out <- apply_stat_filters(out, cmp_stat_filter_state$filters())
        if (is.null(out) || !nrow(out)) return(NULL)
        out$rank <- seq_len(nrow(out))
        out
      },
      extra = function(res) sprintf(
        "mode=%s;rows=%d;min_poss=%s",
        input$cmp_mode %||% "",
        NROW(res),
        cmp_min_poss()
      )
    )
  })

  # -- Teams mode: player self-compare drilldown --

  TEAM_PLAYER_RAW_COLS <- c(
    "gp", "poss_on_floor", "minutes",
    "pts", "reb", "ast", "stl", "blk", "tov",
    "fgm", "fga", "3pm", "3pa", "ftm", "fta",
    "fg_pct", "tp_pct", "ft_pct", "efg", "ts", "usg_pct"
  )

  empty_team_player_side <- function(suffix) {
    out <- data.frame(team_id = integer(), player_id = integer(), check.names = FALSE)
    out[[paste0("team_name_", suffix)]] <- character()
    out[[paste0("player_name_", suffix)]] <- character()
    for (col in TEAM_PLAYER_RAW_COLS) out[[paste0(col, "_", suffix)]] <- numeric()
    out
  }

  normalize_team_player_rows <- function(df) {
    if (is.null(df) || !nrow(df) || is.null(names(df))) return(data.frame())
    if (!("player_name" %in% names(df)) && "Player" %in% names(df)) {
      df$player_name <- as.character(df$Player)
    }
    req_cols <- c("team_id", "player_id", "team_name", "player_name")
    if (!all(req_cols %in% names(df))) return(data.frame())

    out <- df[, unique(c(req_cols, intersect(TEAM_PLAYER_RAW_COLS, names(df)))), drop = FALSE]
    out$team_id <- suppressWarnings(as.integer(out$team_id))
    out$player_id <- suppressWarnings(as.integer(out$player_id))
    out$team_name <- as.character(out$team_name)
    out$player_name <- as.character(out$player_name)
    out <- out[
      is.finite(out$team_id) & is.finite(out$player_id) &
        nzchar(out$team_name) & nzchar(out$player_name),
      ,
      drop = FALSE
    ]
    if (!nrow(out)) return(data.frame())
    out
  }

  prep_team_player_side <- function(df, suffix) {
    df <- normalize_team_player_rows(df)
    if (is.null(df) || !nrow(df)) return(empty_team_player_side(suffix))
    id_cols <- df[, c("team_id", "player_id"), drop = FALSE]
    label_cols <- df[, c("team_name", "player_name"), drop = FALSE]
    names(label_cols) <- paste0(names(label_cols), "_", suffix)
    stat_cols <- df[, intersect(TEAM_PLAYER_RAW_COLS, names(df)), drop = FALSE]
    names(stat_cols) <- paste0(names(stat_cols), "_", suffix)
    out <- cbind(id_cols, label_cols, stat_cols)
    for (col in TEAM_PLAYER_RAW_COLS) {
      nm <- paste0(col, "_", suffix)
      if (!(nm %in% names(out))) out[[nm]] <- NA_real_
    }
    out
  }

  team_player_played_side <- function(df, suffix) {
    n <- NROW(df)
    val <- function(col) {
      nm <- paste0(col, "_", suffix)
      if (nm %in% names(df)) suppressWarnings(as.numeric(df[[nm]])) else rep(NA_real_, n)
    }
    gp <- val("gp")
    poss <- val("poss_on_floor")
    mins <- val("minutes")
    (is.finite(gp) & gp > 0) | (is.finite(poss) & poss > 0) | (is.finite(mins) & mins > 0)
  }

  calc_team_player_metric <- function(row_or_df, suffix, metric, rate_mode) {
    col <- paste0(metric, "_", suffix)
    if (!(col %in% names(row_or_df))) return(rep(NA_real_, NROW(row_or_df)))
    raw <- suppressWarnings(as.numeric(row_or_df[[col]]))
    if (metric %in% TEAM_PLAYER_PCT_COLS) return(raw)

    rate_mode <- rate_mode %||% "Per Game"
    if (identical(rate_mode, "Totals")) return(raw)
    if (identical(rate_mode, "Per 60 Possessions")) {
      poss <- suppressWarnings(as.numeric(row_or_df[[paste0("poss_on_floor_", suffix)]]))
      return(ifelse(is.finite(poss) & poss > 0, raw / poss * 60, NA_real_))
    }
    if (identical(rate_mode, "Per 30 Minutes")) {
      mins <- suppressWarnings(as.numeric(row_or_df[[paste0("minutes_", suffix)]]))
      return(ifelse(is.finite(mins) & mins > 0, raw / mins * 30, NA_real_))
    }

    gp <- suppressWarnings(as.numeric(row_or_df[[paste0("gp_", suffix)]]))
    ifelse(is.finite(gp) & gp > 0, raw / gp, NA_real_)
  }

  format_team_player_metric <- function(x, metric = selected_team_player_metric()) {
    if (!is.finite(x)) return("\u2014")
    if (metric %in% TEAM_PLAYER_PCT_COLS) sprintf("%.1f%%", x) else sprintf("%.1f", x)
  }

  format_team_player_metric_cell <- function(x, played, metric = selected_team_player_metric()) {
    txt <- format_team_player_metric(x, metric = metric)
    if (!isTRUE(played)) paste0(txt, " (didn't play)") else txt
  }

  format_team_player_gap <- function(x, metric = selected_team_player_metric()) {
    if (!is.finite(x)) return("\u2014")
    suffix <- if (metric %in% TEAM_PLAYER_PCT_COLS) "%" else ""
    if (abs(x) < 1e-9) return(paste0("+0.0", suffix))
    if (x > 0) sprintf("+%.1f%s", x, suffix) else sprintf("\u2212%.1f%s", abs(x), suffix)
  }

  cmp_team_players_joined <- reactive({
    req(identical(input$main_tabs, "compare"))
    req(identical(input$cmp_mode, "Teams"))
    req(identical(cmp_active_view(), "players"))
    req(isTRUE(cmp_ready()))
    team <- selected_team_players_team()
    req(team)
    team_id <- suppressWarnings(as.integer(team$team_id))
    req(is.finite(team_id))
    team_csv <- as.character(team_id)

    side_state <- cmp_side_state()
    pa <- side_state$a
    pb <- side_state$b

    res_a <- prep_team_player_side(run_player_traditional(pa, team_csv), "a")
    res_b <- prep_team_player_side(run_player_traditional(pb, team_csv), "b")
    if (!nrow(res_a) && !nrow(res_b)) return(NULL)

    joined <- dplyr::full_join(res_a, res_b, by = c("team_id", "player_id"))
    if (!nrow(joined)) return(NULL)

    joined$team_name <- dplyr::coalesce(joined$team_name_a, joined$team_name_b)
    joined$team_name <- dplyr::coalesce(joined$team_name, team$team_name)
    joined$player_name <- dplyr::coalesce(joined$player_name_a, joined$player_name_b)
    joined
  })

  cmp_team_players_scored <- reactive({
    df <- cmp_team_players_joined()
    if (is.null(df) || !nrow(df)) return(NULL)

    metric <- selected_team_player_metric()
    if (!(metric %in% TEAM_PLAYER_METRICS)) metric <- TEAM_PLAYER_METRICS[[1]]
    rate_mode <- input$cmp_team_player_rate_mode %||% "Per Game"

    df$metric_a <- calc_team_player_metric(df, "a", metric, rate_mode)
    df$metric_b <- calc_team_player_metric(df, "b", metric, rate_mode)
    df$played_a <- team_player_played_side(df, "a")
    df$played_b <- team_player_played_side(df, "b")
    df$metric_a <- ifelse(df$played_a, df$metric_a, 0)
    df$metric_b <- ifelse(df$played_b, df$metric_b, 0)
    df$poss_a <- dplyr::coalesce(suppressWarnings(as.numeric(df$poss_on_floor_a)), 0)
    df$poss_b <- dplyr::coalesce(suppressWarnings(as.numeric(df$poss_on_floor_b)), 0)
    min_poss <- cmp_min_poss()
    keep <- pmax(df$poss_a, df$poss_b, na.rm = TRUE) >= min_poss
    df <- df[keep, , drop = FALSE]
    if (is.null(df) || !nrow(df)) return(NULL)

    df$gap <- cmp_gap_value(df$metric_a, df$metric_b)
    df$gap_score <- if (metric %in% TEAM_PLAYER_LOWER_BETTER) -df$gap else df$gap
    df <- df[order(is.na(df$gap_score), -df$gap_score, df$team_name, df$player_name), , drop = FALSE]
    df$rank <- seq_len(nrow(df))
    df
  })

  cmp_team_players_for_selected_team <- reactive({
    team <- selected_team_players_team()
    df <- cmp_team_players_scored()
    if (is.null(df) || !nrow(df)) return(NULL)
    if (is.null(team)) return(df)
    out <- df[df$team_id == as.integer(team$team_id), , drop = FALSE]
    if (!nrow(out)) return(NULL)
    out$rank <- seq_len(nrow(out))
    out
  })

  observeEvent(input$cmp_team_players_player_click, {
    info <- input$cmp_team_players_player_click
    req(info$team_id, info$player_id)
    team_id <- suppressWarnings(as.integer(info$team_id))
    player_id <- suppressWarnings(as.integer(info$player_id))
    if (!is.finite(team_id) || !is.finite(player_id)) return(invisible(NULL))
    selected_team_players_player(list(
      team_id = team_id,
      player_id = player_id,
      team_name = as.character(info$team_name %||% ""),
      player_name = as.character(info$player_name %||% "")
    ))
    cmp_active_view("players")
  }, ignoreInit = TRUE)

  observeEvent(input$cmp_team_players_back_teams, {
    selected_team_players_player(NULL)
    cmp_active_view("detail")
    detail_view_active(TRUE)
  }, ignoreInit = TRUE)

  observeEvent(input$cmp_team_players_back_players, {
    selected_team_players_player(NULL)
  }, ignoreInit = TRUE)

  observeEvent(list(
    input$cmp_preset, input$cmp_split_date, input$cmp_split_gn,
    input$cmp_a_starters_mode, input$cmp_a_starters_val, input$cmp_a_opp_starters_mode, input$cmp_a_opp_starters_val, input$cmp_a_teams,
    input$cmp_a_home_away, input$cmp_a_outcome, input$cmp_a_clutch,
    input$cmp_a_clutch_margin, input$cmp_a_clutch_minutes, input$cmp_a_opponents,
    input$cmp_a_game_type, input$cmp_a_opp_rank_side, input$cmp_a_opp_rank_n, input$cmp_a_opp_rank_metric,
    input$cmp_b_starters_mode, input$cmp_b_starters_val, input$cmp_b_opp_starters_mode, input$cmp_b_opp_starters_val, input$cmp_b_teams,
    input$cmp_b_home_away, input$cmp_b_outcome, input$cmp_b_clutch,
    input$cmp_b_clutch_margin, input$cmp_b_clutch_minutes, input$cmp_b_opponents,
    input$cmp_b_game_type, input$cmp_b_opp_rank_side, input$cmp_b_opp_rank_n, input$cmp_b_opp_rank_metric,
    input$game_year
  ), {
    if (!identical(input$cmp_mode, "Teams")) return(invisible(NULL))
    selected_team_players_player(NULL)
  }, ignoreInit = TRUE)

  output$cmp_team_players_panel_ui <- renderUI({
    req(identical(input$main_tabs, "compare"))
    req(identical(input$cmp_mode, "Teams"))

    player <- selected_team_players_player()
    team <- selected_team_players_team()
    if (is.null(player) && is.null(team)) {
      return(tags$div(class = "detail-container",
        tags$div(class = "text-muted text-center mt-4",
          "Select a team in Detail view before opening Players.")
      ))
    }

    short_a <- side_label_short("a")
    short_b <- side_label_short("b")
    full_a <- side_label_full("a")
    full_b <- side_label_full("b")
    gy <- input$game_year

    if (!is.null(player)) {
      df <- cmp_team_players_for_selected_team()
      row <- if (!is.null(df) && nrow(df)) {
        df[df$team_id == as.integer(player$team_id) & df$player_id == as.integer(player$player_id), , drop = FALSE]
      } else {
        data.frame()
      }

      if (!nrow(row)) {
        return(tags$div(class = "detail-container",
          tags$button(class = "cmp-back-btn js-shiny-event",
            `data-input-id` = "cmp_team_players_back_players",
            "\u2190 Back to players"),
          tags$div(class = "text-muted text-center mt-4", "No player data for current filters.")
        ))
      }

      row <- row[1, , drop = FALSE]
      team_name <- row$team_name[1] %||% player$team_name
      player_name <- row$player_name[1] %||% player$player_name
      rate_mode <- input$cmp_team_player_rate_mode %||% "Per Game"
      col_a_text <- if (identical(short_a, "A")) "A" else paste0("A \u00b7 ", short_a)
      col_b_text <- if (identical(short_b, "B")) "B" else paste0("B \u00b7 ", short_b)

      detail_specs <- list(
        usage = list(
          title = "Usage",
          metrics = list(
            list(label = "GP", col = "gp", fmt = "num", polarity = "neutral", raw = TRUE),
            list(label = "Min", col = "minutes", fmt = "num", polarity = "neutral", raw = TRUE),
            list(label = "Poss", col = "poss_on_floor", fmt = "num", polarity = "neutral", raw = TRUE),
            list(label = "USG%", col = "usg_pct", fmt = "pct", polarity = "higher", pct = TRUE)
          )
        ),
        production = list(
          title = "Production",
          metrics = list(
            list(label = "PTS", col = "pts", fmt = "num", polarity = "higher"),
            list(label = "REB", col = "reb", fmt = "num", polarity = "higher"),
            list(label = "AST", col = "ast", fmt = "num", polarity = "higher"),
            list(label = "STL", col = "stl", fmt = "num", polarity = "higher"),
            list(label = "BLK", col = "blk", fmt = "num", polarity = "higher"),
            list(label = "TOV", col = "tov", fmt = "num", polarity = "lower")
          )
        ),
        shooting = list(
          title = "Shooting",
          metrics = list(
            list(label = "FG%", col = "fg_pct", fmt = "pct", polarity = "higher", pct = TRUE),
            list(label = "3P%", col = "tp_pct", fmt = "pct", polarity = "higher", pct = TRUE),
            list(label = "FT%", col = "ft_pct", fmt = "pct", polarity = "higher", pct = TRUE),
            list(label = "eFG%", col = "efg", fmt = "pct", polarity = "higher", pct = TRUE),
            list(label = "TS%", col = "ts", fmt = "pct", polarity = "higher", pct = TRUE)
          )
        )
      )

      get_detail_val <- function(spec, suffix) {
        played_col <- paste0("played_", suffix)
        if (played_col %in% names(row) && !isTRUE(row[[played_col]][1])) return(0)
        if (isTRUE(spec$raw) || isTRUE(spec$pct)) {
          return(suppressWarnings(as.numeric(row[[paste0(spec$col, "_", suffix)]])))
        }
        calc_team_player_metric(row, suffix, spec$col, rate_mode)
      }
      fmt_detail <- function(val, spec) {
        if (!is.finite(val)) return("\u2014")
        if (identical(spec$fmt, "pct")) return(sprintf("%.1f%%", val))
        if (isTRUE(spec$raw) && identical(spec$col, "gp")) return(sprintf("%.0f", val))
        sprintf("%.1f", val)
      }
      compute_detail_gap <- function(val_a, val_b, polarity) {
        if (!is.finite(val_a) || !is.finite(val_b)) {
          return(list(gap = NA_real_, a_wins = NA))
        }
        raw <- cmp_gap_value(val_a, val_b)
        if (identical(polarity, "neutral")) {
          return(list(gap = raw, a_wins = NA))
        }
        a_wins <- if (identical(polarity, "lower")) val_a < val_b else val_a > val_b
        list(gap = raw, a_wins = if (abs(raw) < 1e-9) NA else a_wins)
      }

      all_cells <- list(
        tags$div(class = "cmp-col-header cmp-col-a cmp-cell cmp-first-row", col_a_text),
        tags$div(class = "cmp-col-header cmp-col-gap cmp-cell cmp-first-row", "Gap"),
        tags$div(class = "cmp-col-header cmp-col-b cmp-cell cmp-first-row", col_b_text)
      )

      active_sections <- names(detail_specs)
      for (sec_key in active_sections) {
        sec <- detail_specs[[sec_key]]
        all_cells <- c(all_cells, list(tags$div(class = "cmp-section-title", sec$title)))

        computed <- lapply(sec$metrics, function(m) {
          va <- get_detail_val(m, "a")
          vb <- get_detail_val(m, "b")
          gi <- compute_detail_gap(va, vb, m$polarity)
          list(m = m, va = va, vb = vb, gi = gi)
        })
        max_abs_gap <- max(vapply(computed, function(x) {
          if (is.finite(x$gi$gap)) abs(x$gi$gap) else 0
        }, numeric(1)), na.rm = TRUE)
        if (max_abs_gap == 0) max_abs_gap <- 1

        for (j in seq_along(computed)) {
          x <- computed[[j]]
          m <- x$m
          gi <- x$gi
          a_cls <- if (is.na(gi$a_wins)) "winner" else if (gi$a_wins) "winner" else "loser"
          b_cls <- if (is.na(gi$a_wins)) "winner" else if (gi$a_wins) "loser" else "winner"
          is_last_row <- identical(sec_key, tail(active_sections, 1)) && j == length(computed)
          last_cls <- if (is_last_row) " cmp-last-row" else ""
          gap_text <- if (!is.finite(gi$gap)) "\u2014" else {
            suffix <- if (identical(m$fmt, "pct")) "%" else ""
            if (abs(gi$gap) < 1e-9) paste0("+0.0", suffix)
            else if (gi$gap > 0) sprintf("+%.1f%s", gi$gap, suffix)
            else sprintf("\u2212%.1f%s", abs(gi$gap), suffix)
          }
          winner_side <- if (is.na(gi$a_wins)) "none" else if (isTRUE(gi$a_wins)) "a" else "b"
          gap_color_cls <- if (winner_side == "a") "a-color" else if (winner_side == "b") "b-color" else ""
          bar_pct <- if (is.finite(gi$gap) && max_abs_gap > 0) round(abs(gi$gap) / max_abs_gap * 50, 1) else 0
          bar_cls <- if (identical(winner_side, "a")) "toward-a" else "toward-b"

          all_cells <- c(all_cells, list(
            tags$div(class = paste0("cmp-stat-row cmp-col-a cmp-cell", last_cls),
              tags$span(class = "cmp-stat-label", m$label),
              tags$span(class = paste("cmp-stat-value", a_cls), fmt_detail(x$va, m))),
            tags$div(class = paste0("cmp-gap-row cmp-col-gap cmp-cell", last_cls),
              tags$span(class = paste("cmp-gap-num", gap_color_cls), gap_text),
              tags$div(class = "cmp-bar-container",
                tags$div(class = "cmp-bar-center"),
                if (bar_pct > 0) tags$div(class = paste("cmp-bar", bar_cls),
                  style = sprintf("width: %.1f%%;", bar_pct)))),
            tags$div(class = paste0("cmp-stat-row cmp-col-b cmp-cell", last_cls),
              tags$span(class = "cmp-stat-label", m$label),
              tags$span(class = paste("cmp-stat-value", b_cls), fmt_detail(x$vb, m)))
          ))
        }
      }

      gp_a <- if (isTRUE(row$played_a[1])) suppressWarnings(as.numeric(row$gp_a[1])) else 0
      gp_b <- if (isTRUE(row$played_b[1])) suppressWarnings(as.numeric(row$gp_b[1])) else 0
      poss_a <- if (isTRUE(row$played_a[1])) suppressWarnings(as.numeric(row$poss_on_floor_a[1])) else 0
      poss_b <- if (isTRUE(row$played_b[1])) suppressWarnings(as.numeric(row$poss_on_floor_b[1])) else 0
      note_a <- if (isTRUE(row$played_a[1])) {
        sprintf(
          "<strong>%s</strong> GP \u00b7 <strong>%s</strong> Poss",
          if (is.finite(gp_a)) round(gp_a) else "\u2014",
          if (is.finite(poss_a)) format(round(poss_a), big.mark = ",") else "\u2014"
        )
      } else {
        "didn't play"
      }
      note_b <- if (isTRUE(row$played_b[1])) {
        sprintf(
          "<strong>%s</strong> GP \u00b7 <strong>%s</strong> Poss",
          if (is.finite(gp_b)) round(gp_b) else "\u2014",
          if (is.finite(poss_b)) format(round(poss_b), big.mark = ",") else "\u2014"
        )
      } else {
        "didn't play"
      }
      context_bar <- tags$div(class = "cmp-context-bar",
        tags$div(class = "cmp-context-side",
          tags$span(class = "cmp-context-badge a", "A"),
          tags$span(class = "cmp-context-info", HTML(note_a))),
        tags$div(class = "cmp-context-sep", "|"),
        tags$div(class = "cmp-context-side",
          tags$span(class = "cmp-context-badge b", "B"),
          tags$span(class = "cmp-context-info", HTML(note_b)))
      )

      return(tags$div(class = "detail-container",
        tags$button(class = "cmp-back-btn js-shiny-event",
          `data-input-id` = "cmp_team_players_back_players",
          "\u2190 Back to players"),
        tags$div(class = "cmp-team-header", player_name),
        tags$div(class = "cmp-team-subheader",
          paste0(team_name, " \u00b7 ", full_a, " vs ", full_b, " \u00b7 ", rate_mode, " \u00b7 ", gy, "-", as.integer(substr(gy, 3, 4)) + 1)),
        context_bar,
        tags$div(class = "cmp-compare-grid", do.call(tagList, all_cells))
      ))
    }

    if (!is.null(team)) {
      return(tags$div(
        tags$button(class = "cmp-back-btn js-shiny-event",
          `data-input-id` = "cmp_team_players_back_teams",
          "\u2190 Back to team detail"),
        tags$div(class = "cmp-team-header", team$team_name),
        tags$div(class = "cmp-team-subheader",
          paste0(full_a, " vs ", full_b, " \u00b7 ", input$cmp_team_player_rate_mode %||% "Per Game")),
        DT::dataTableOutput("cmp_team_players_table")
      ))
    }

    NULL
  })

  output$cmp_team_players_table <- DT::renderDataTable({
    team <- selected_team_players_team()
    req(team)
    df <- cmp_team_players_for_selected_team()
    req(df, nrow(df) > 0)

    metric <- selected_team_player_metric()
    show_df <- data.frame(
      team_id = df$team_id,
      player_id = df$player_id,
      `#` = df$rank,
      Player = df$player_name,
      `GP A` = ifelse(df$played_a, as.integer(df$gp_a), 0L),
      A = mapply(format_team_player_metric_cell, df$metric_a, df$played_a, MoreArgs = list(metric = metric), USE.NAMES = FALSE),
      `Total Poss A` = as.integer(df$poss_a),
      `GP B` = ifelse(df$played_b, as.integer(df$gp_b), 0L),
      B = mapply(format_team_player_metric_cell, df$metric_b, df$played_b, MoreArgs = list(metric = metric), USE.NAMES = FALSE),
      `Total Poss B` = as.integer(df$poss_b),
      Gap = vapply(df$gap, format_team_player_gap, character(1), metric = metric),
      gap_score = ifelse(is.finite(df$gap_score), df$gap_score, -Inf),
      check.names = FALSE,
      stringsAsFactors = FALSE
    )
    names(show_df)[6] <- side_label_short("a")
    names(show_df)[9] <- side_label_short("b")

    DT::datatable(
      show_df,
      rownames = FALSE,
      callback = DT::JS("
        table.on('click', 'tbody tr', function() {
          var data = table.row(this).data();
          if (!data || !window.Shiny) return;
          window.Shiny.setInputValue('cmp_team_players_player_click', {
            team_id: data[0],
            player_id: data[1],
            player_name: data[3],
            rand: Math.random()
          }, { priority: 'event' });
        });
      "),
      options = list(
        dom = "t",
        paging = FALSE,
        ordering = TRUE,
        order = list(list(11L, "desc")),
        columnDefs = list(
          list(visible = FALSE, targets = c(0L, 1L, 11L)),
          list(className = "dt-left", targets = 3L),
          list(className = "dt-right", targets = c(2L, 4L, 5L, 6L, 7L, 8L, 9L, 10L)),
          list(orderData = 11L, orderSequence = c("desc"), targets = 10L)
        ),
        rowCallback = DT::JS("function(row, data) { $(row).css('cursor', 'pointer'); }")
      )
    )
  })

  # -- Detail view: fetch all metrics for selected entity --

  cmp_detail_data <- reactive({
    entity <- selected_detail_entity()
    req(entity)
    req(identical(input$main_tabs, "compare"))
    req(isTRUE(cmp_ready()))

    side_state <- cmp_side_state()
    pa <- side_state$a
    pb <- side_state$b
    mode <- entity$mode

    if (mode == "Teams") {
      ratings_a <- run_team_ratings(pa)
      ratings_b <- run_team_ratings(pb)
      ff_a <- run_team_ff(pa)
      ff_b <- run_team_ff(pb)

      team_id <- entity$key
      shooting_a <- run_team_shooting(pa, team_id)
      shooting_b <- run_team_shooting(pb, team_id)
      ra <- ratings_a[ratings_a$team_id == team_id, , drop = FALSE]
      rb <- ratings_b[ratings_b$team_id == team_id, , drop = FALSE]
      fa <- ff_a[ff_a$team_id == team_id, , drop = FALSE]
      fb <- ff_b[ff_b$team_id == team_id, , drop = FALSE]
      sha <- if (nrow(shooting_a)) shooting_a[1, , drop = FALSE] else data.frame()
      shb <- if (nrow(shooting_b)) shooting_b[1, , drop = FALSE] else data.frame()

      if (!nrow(ra) && !nrow(fa) && !nrow(sha)) return(NULL)
      if (!nrow(rb) && !nrow(fb) && !nrow(shb)) return(NULL)

      list(
        mode = "Teams",
        entity_name = entity$name,
        ratings_a = if (nrow(ra)) add_team_shot_profile_shares(ra[1, , drop = FALSE])[1, ] else NULL,
        ratings_b = if (nrow(rb)) add_team_shot_profile_shares(rb[1, , drop = FALSE])[1, ] else NULL,
        ff_a = if (nrow(fa)) fa[1, ] else NULL,
        ff_b = if (nrow(fb)) fb[1, ] else NULL,
        shooting_a = if (nrow(sha)) sha[1, ] else NULL,
        shooting_b = if (nrow(shb)) shb[1, ] else NULL
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
        ff_b = if (nrow(fb)) fb[1, ] else NULL,
        shooting_a = if (nrow(sa)) add_shooting_rates(sa[1, , drop = FALSE]) else NULL,
        shooting_b = if (nrow(sb)) add_shooting_rates(sb[1, , drop = FALSE]) else NULL
      )
    } else {
      NULL
    }
  })

  # -- Detail view helpers --

  detail_get_value <- function(row, col) {
    if (is.null(row) || is.null(col) || !(col %in% names(row))) return(NA_real_)
    suppressWarnings(as.numeric(row[[col]]))
  }

  # All values are already Ã— 100 (SQL does it, Win% computed as w/gp*100 in detail_win_pct).
  detail_fmt <- function(val, fmt) {
    if (!is.finite(val)) return("\u2014")
    switch(fmt,
      pct = sprintf("%.1f%%", val),   # already Ã—100
      rtg = sprintf("%.1f", val),     # already Ã—100
      net = sprintf("%+.1f", val),    # already Ã—100
      sprintf("%.1f", val)
    )
  }

  # Returns Win% already Ã— 100 (matching SQL scale for other metrics)
  detail_win_pct <- function(row) {
    gp <- detail_get_value(row, "games_played")
    w <- detail_get_value(row, "wins")
    if (is.finite(gp) && gp > 0 && is.finite(w)) (w / gp) * 100 else NA_real_
  }

  detail_extract_value <- function(data_side_ratings, data_side_ff, data_side_shooting, metric_def) {
    if (!is.null(metric_def$col_ratings) && metric_def$col_ratings == "win_pct") {
      return(detail_win_pct(data_side_ratings))
    }
    if (!is.null(metric_def$col_ratings)) {
      return(detail_get_value(data_side_ratings, metric_def$col_ratings))
    }
    if (!is.null(metric_def$col_ff)) {
      return(detail_get_value(data_side_ff, metric_def$col_ff))
    }
    if (!is.null(metric_def$col_shooting)) {
      return(detail_get_value(data_side_shooting, metric_def$col_shooting))
    }
    NA_real_
  }

  detail_compute_gap <- function(val_a, val_b, polarity) {
    if (!is.finite(val_a) || !is.finite(val_b)) {
      return(list(gap = NA_real_, direction = "none", a_wins = NA))
    }
    raw_diff <- val_a - val_b
    abs_gap <- abs(raw_diff)
    direction <- if (raw_diff > 0) "a" else if (raw_diff < 0) "b" else "none"
    if (identical(polarity, "neutral")) {
      return(list(gap = raw_diff, direction = direction, a_wins = NA))
    }
    signed_gap <- if (polarity == "higher") abs_gap else -abs_gap
    if (abs_gap == 0) signed_gap <- 0
    a_wins <- if (abs_gap == 0) NA else {
      if (polarity == "higher") (val_a > val_b) else (val_a < val_b)
    }
    list(gap = signed_gap, direction = direction, a_wins = a_wins)
  }

  build_detail_context_bar <- function(ra, rb, mode) {
    build_side <- function(row, badge_cls) {
      parts <- character(0)
      if (mode == "Teams" && !is.null(row)) {
        gp <- detail_get_value(row, "games_played")
        w <- detail_get_value(row, "wins")
        l <- detail_get_value(row, "losses")
        if (is.finite(gp)) parts <- c(parts, paste0("<strong>", round(gp), "</strong> GP"))
        if (is.finite(w) && is.finite(l)) parts <- c(parts, paste0("<strong>", round(w), "</strong>-<strong>", round(l), "</strong>"))
      }
      if (!is.null(row)) {
        poss <- detail_get_value(row, "off_poss")
        if (!is.finite(poss)) poss <- detail_get_value(row, "total_poss")
        if (is.finite(poss)) parts <- c(parts, paste0("<strong>", format(round(poss), big.mark = ","), "</strong> Poss"))
      }
      if (length(parts) == 0) return(NULL)
      tags$div(class = "cmp-context-side",
        tags$span(class = paste("cmp-context-badge", badge_cls),
          if (badge_cls == "a") "A" else "B"),
        tags$span(class = "cmp-context-info", HTML(paste(parts, collapse = " \u00b7 ")))
      )
    }
    side_a <- build_side(ra, "a")
    side_b <- build_side(rb, "b")
    if (is.null(side_a) && is.null(side_b)) return(NULL)
    tags$div(class = "cmp-context-bar",
      side_a,
      tags$div(class = "cmp-context-sep", "|"),
      side_b
    )
  }

  # -- Detail view: back button --

  observeEvent(input$cmp_detail_back, {
    selected_detail_entity(NULL)
    cmp_active_view("league")
    detail_view_active(FALSE)
  }, ignoreInit = TRUE)

  # -- Detail view: main renderUI --

  output$cmp_detail_view_ui <- renderUI({
    data <- cmp_detail_data()
    entity <- selected_detail_entity()

    # No entity selected yet in detail mode â†’ show prompt
    if (is.null(entity) && isTRUE(detail_view_active())) {
      return(tags$div(class = "detail-container",
        tags$div(class = "text-muted text-center mt-4",
                 "Select a team or lineup from the dropdown to view detailed comparison.")))
    }

    # Entity selected but no data (e.g. filters exclude it)
    if (is.null(data) && !is.null(entity)) {
      return(tags$div(class = "detail-container",
        tags$div(class = "cmp-back-btn js-shiny-event",
          `data-input-id` = "cmp_detail_back",
          "\u2190 Back to league view"),
        tags$div(class = "text-muted text-center mt-4",
          "No data for this entity with current filters.")))
    }

    req(data)

    mode <- data$mode
    ra <- data$ratings_a; rb <- data$ratings_b
    fa <- data$ff_a; fb <- data$ff_b
    sha <- data$shooting_a; shb <- data$shooting_b
    short_a <- side_label_short("a"); short_b <- side_label_short("b")
    full_a <- side_label_full("a"); full_b <- side_label_full("b")
    gy <- input$game_year

    context_bar <- build_detail_context_bar(ra, rb, mode)

    # Build all grid cells â€” flat layout, each metric = 3 sibling cells sharing one grid row
    all_cells <- list()
    section_names <- names(DETAIL_METRICS)
    col_a_text <- if (identical(short_a, "A")) "A" else paste0("A \u00b7 ", short_a)
    col_b_text <- if (identical(short_b, "B")) "B" else paste0("B \u00b7 ", short_b)

    # Column headers (first row of grid)
    all_cells <- c(all_cells, list(
      tags$div(class = "cmp-col-header cmp-col-a cmp-cell cmp-first-row", col_a_text),
      tags$div(class = "cmp-col-header cmp-col-gap cmp-cell cmp-first-row js-cmp-detail-sort",
        "Gap ", tags$span(id = "cmp-sort-icon", "\u2195")),
      tags$div(class = "cmp-col-header cmp-col-b cmp-cell cmp-first-row", col_b_text)
    ))

    # Resolve the metrics each section renders, applying mode-specific gating
    # once (single source of truth): Defensive Shooting is Teams-only, and Win%
    # is hidden for Lineups (no GP/W/L data). Sections left with no metrics drop.
    section_metrics <- function(sk) {
      if (sk %in% c("def_shooting", "off_shot_profile", "def_shot_profile") && mode != "Teams") return(NULL)
      ml <- DETAIL_METRICS[[sk]]$metrics
      if (mode == "Lineups" && sk == "ratings") {
        ml <- Filter(function(m) m$label != "Win%", ml)
      }
      if (length(ml)) ml else NULL
    }
    section_metrics_map <- Filter(Negate(is.null),
      stats::setNames(lapply(section_names, section_metrics), section_names))
    active_sections <- names(section_metrics_map)

    for (sec_key in active_sections) {
      sec <- DETAIL_METRICS[[sec_key]]
      metrics_list <- section_metrics_map[[sec_key]]

      # Section title spans all 3 columns
      all_cells <- c(all_cells, list(
        tags$div(class = "cmp-section-title", sec$title)
      ))

      # Compute all gaps for max-gap bar scaling
      computed <- lapply(metrics_list, function(m) {
        va <- detail_extract_value(ra, fa, sha, m)
        vb <- detail_extract_value(rb, fb, shb, m)
        gap_info <- detail_compute_gap(va, vb, m$polarity)
        list(m = m, va = va, vb = vb, gap = gap_info)
      })

      max_abs_gap <- max(vapply(computed, function(x) {
        if (is.finite(x$gap$gap)) abs(x$gap$gap) else 0
      }, numeric(1)), na.rm = TRUE)
      if (max_abs_gap == 0) max_abs_gap <- 1

      n_metrics <- length(computed)
      is_last_section <- (sec_key == tail(active_sections, 1))

      for (j in seq_along(computed)) {
        x <- computed[[j]]
        m <- x$m; va <- x$va; vb <- x$vb; gi <- x$gap

        a_cls <- if (is.na(gi$a_wins)) "winner" else if (gi$a_wins) "winner" else "loser"
        b_cls <- if (is.na(gi$a_wins)) "winner" else if (gi$a_wins) "loser" else "winner"

        fmt_va <- detail_fmt(va, m$fmt)
        fmt_vb <- detail_fmt(vb, m$fmt)

        # Gap display text
        gap_text <- if (!is.finite(gi$gap)) "\u2014" else {
          g_display <- gi$gap
          pct_suffix <- if (m$fmt == "pct") "%" else ""
          if (abs(g_display) < 1e-9) {
            sprintf("\u00b10.0%s", pct_suffix)
          } else if (g_display > 0) {
            sprintf("+%.1f%s", g_display, pct_suffix)
          } else {
            sprintf("\u2212%.1f%s", abs(g_display), pct_suffix)
          }
        }
        winner_side <- if (is.na(gi$a_wins)) "none" else if (isTRUE(gi$a_wins)) "a" else "b"
        gap_color_cls <- if (winner_side == "a") "a-color" else if (winner_side == "b") "b-color" else ""
        bar_pct <- if (is.finite(gi$gap) && max_abs_gap > 0) round(abs(gi$gap) / max_abs_gap * 50, 1) else 0
        bar_cls <- if (winner_side == "a") {
          "toward-a"
        } else if (winner_side == "b") {
          "toward-b"
        } else if (identical(gi$direction, "a")) {
          "toward-a"
        } else {
          "toward-b"
        }

        # Last-row class for bottom border/radius
        is_last_row <- is_last_section && (j == n_metrics)
        last_cls <- if (is_last_row) " cmp-last-row" else ""

        # Estimated point size of this factor gap. Magnitude-only: direction
        # is already encoded by the gap bar/colors (spec: detail view).
        est_span <- NULL
        if (!is.null(m$factor) && is.finite(va) && is.finite(vb)) {
          est_span <- tags$span(
            class = "ff-impact-est",
            title = FF_IMPACT_EST_TITLE,
            sprintf("est. %.1f pts", abs(ff_impact_pts(va - vb, m$factor)))
          )
        }

        all_cells <- c(all_cells, list(
          tags$div(class = paste0("cmp-stat-row cmp-col-a cmp-cell", last_cls),
            `data-idx` = j - 1, `data-group` = sec_key,
            tags$span(class = "cmp-stat-label", m$label),
            tags$span(class = paste("cmp-stat-value", a_cls), fmt_va)),
          tags$div(class = paste0("cmp-gap-row cmp-col-gap cmp-cell", last_cls),
            `data-idx` = j - 1, `data-group` = sec_key,
            `data-default-idx` = j - 1,
            `data-gap` = if (is.finite(gi$gap)) round(gi$gap, 4) else 0,
            tags$span(class = paste("cmp-gap-num", gap_color_cls), gap_text),
            est_span,
            tags$div(class = "cmp-bar-container",
              tags$div(class = "cmp-bar-center"),
              if (bar_pct > 0) tags$div(class = paste("cmp-bar", bar_cls),
                style = sprintf("width: %.1f%%;", bar_pct)))),
          tags$div(class = paste0("cmp-stat-row cmp-col-b cmp-cell", last_cls),
            `data-idx` = j - 1, `data-group` = sec_key,
            tags$span(class = "cmp-stat-label", m$label),
            tags$span(class = paste("cmp-stat-value", b_cls), fmt_vb))
        ))
      }
    }

    tagList(
      tags$div(class = "detail-container",
        tags$div(class = "cmp-back-btn js-shiny-event",
          `data-input-id` = "cmp_detail_back",
          "\u2190 Back to league view"),
        tags$div(class = "cmp-team-header", data$entity_name),
        tags$div(class = "cmp-team-subheader",
          paste0(full_a, " vs ", full_b, " \u00b7 ", gy, "-", as.integer(substr(gy, 3, 4)) + 1)),
        context_bar,
        tags$div(class = "cmp-compare-grid",
          do.call(tagList, all_cells)),
        tags$div(
          style = "text-align: center; font-size: .72rem; color: #6e7681; margin-top: 8px;",
          ff_impact_legend()
        )
      )
    )
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

  output$cmp_summary_a_title <- renderText({ side_label_full("a") })
  output$cmp_summary_b_title <- renderText({ side_label_full("b") })

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
    cmp_active_view("detail")
    detail_view_active(TRUE)
  }, ignoreInit = TRUE)

  # -- Detail view: entity dropdown --

  output$cmp_detail_entity_dropdown_ui <- renderUI({
    if (!isTRUE(detail_view_active())) return(NULL)

    tags$div(
      style = "min-width: 200px;",
      selectizeInput("cmp_detail_entity_select", NULL,
        choices = NULL,
        selected = "",
        options = list(placeholder = "Select entity..."),
        width = "100%"
      )
    )
  })

  observe({
    if (!isTRUE(detail_view_active())) return()

    df <- cmp_joined()
    req(df, nrow(df) > 0)

    choices <- setNames(df$entity_name, df$entity_name)
    current <- selected_detail_entity()
    sel <- if (!is.null(current)) current$name else ""

    updateSelectizeInput(
      session, "cmp_detail_entity_select",
      choices = c("Select..." = "", choices),
      selected = sel,
      server = FALSE
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
    cmp_profile_time(
      "output_cmp_table",
      {
        df <- cmp_joined()
        req(df, nrow(df) > 0)

        mode <- input$cmp_mode
        entity_label <- if (mode == "Players") "Player" else if (mode == "Lineups") "Lineup" else "Team"

        side_a_label <- side_label_short("a")
        side_b_label <- side_label_short("b")

        # Sort by descending signed gap, keeping NA rows at the bottom
        gap_ord <- order(is.na(df$gap), -df$gap)
        df <- df[gap_ord, ]
        df$rank <- seq_len(nrow(df))

        if (identical(mode, "Lineups")) {
          show_df <- data.frame(
            `#` = df$rank,
            Entity = df$entity_name,
            Team = if ("team_name" %in% names(df)) df$team_name else "",
            A = ifelse(is.finite(df$metric_a), sprintf("%.1f", df$metric_a), "\u2014"),
            `Total Poss A` = as.integer(df$poss_a),
            B = ifelse(is.finite(df$metric_b), sprintf("%.1f", df$metric_b), "\u2014"),
            `Total Poss B` = as.integer(df$poss_b),
            Gap = ifelse(is.finite(df$gap), sprintf("%+.1f", df$gap), "\u2014"),
            gap_sort = ifelse(is.finite(df$gap), df$gap, -Inf),
            check.names = FALSE, stringsAsFactors = FALSE
          )
          names(show_df)[2] <- entity_label
          names(show_df)[4] <- side_a_label
          names(show_df)[6] <- side_b_label
          entity_col_idx <- 1L
          right_targets <- 3:7
          left_targets <- 1:2
          hidden_target <- 8L
          gap_target <- 7L
          a_header_idx <- 3L
          b_header_idx <- 5L
        } else {
          show_df <- data.frame(
            `#` = df$rank,
            Entity = df$entity_name,
            A = ifelse(is.finite(df$metric_a), sprintf("%.1f", df$metric_a), "\u2014"),
            `Total Poss A` = as.integer(df$poss_a),
            B = ifelse(is.finite(df$metric_b), sprintf("%.1f", df$metric_b), "\u2014"),
            `Total Poss B` = as.integer(df$poss_b),
            Gap = ifelse(is.finite(df$gap), sprintf("%+.1f", df$gap), "\u2014"),
            gap_sort = ifelse(is.finite(df$gap), df$gap, -Inf),
            check.names = FALSE, stringsAsFactors = FALSE
          )
          names(show_df)[2] <- entity_label
          names(show_df)[3] <- side_a_label
          names(show_df)[5] <- side_b_label
          entity_col_idx <- 1L
          right_targets <- 2:6
          left_targets <- 1L
          hidden_target <- 7L
          gap_target <- 6L
          a_header_idx <- 2L
          b_header_idx <- 4L
        }

        DT::datatable(
          show_df,
          callback = DT::JS(sprintf("
        table.on('click', 'tbody tr', function() {
          window.handleCompareTableRowClick(table, this, %d);
        });
      ", entity_col_idx)),
          options = list(
            dom = "t", paging = FALSE, ordering = TRUE,
            order = list(),
            columnDefs = list(
              list(className = "dt-right", targets = right_targets),
              list(className = "dt-left", targets = left_targets),
              list(visible = FALSE, targets = hidden_target),
              list(orderData = hidden_target, orderSequence = c("desc"), targets = gap_target)
            ),
            rowCallback = DT::JS(
              "function(row, data) { $(row).css('cursor', 'pointer'); }"
            ),
            headerCallback = DT::JS(sprintf("
          function(thead, data, start, end, display) {
            var cells = $(thead).find('th');
            var tips = %s;
            var badgeStyle = 'display:inline-block;border-radius:50%%;padding:2px 8px;font-size:.7rem;font-weight:600;';
            var aStyle = badgeStyle + 'background:rgba(123,140,222,.2);color:#7b8cde;border:1px solid rgba(123,140,222,.4);';
            var bStyle = badgeStyle + 'background:rgba(232,164,53,.15);color:#e8a435;border:1px solid rgba(232,164,53,.35);';
            cells.each(function() {
              var cell = $(this);
              var txt = cell.text().trim();
              if (tips[txt]) {
                cell.attr('title', tips[txt]);
                cell.css('cursor', 'help');
              } else {
                cell.removeAttr('title');
              }
            });
            var aText = $(cells[%d]).text().trim();
            var bText = $(cells[%d]).text().trim();
            if (aText === 'A') $(cells[%d]).html('<span style=\"' + aStyle + '\">A</span>');
            if (bText === 'B') $(cells[%d]).html('<span style=\"' + bStyle + '\">B</span>');
          }
        ", jsonlite::toJSON(as.list(COLUMN_TOOLTIPS), auto_unbox = TRUE), a_header_idx, b_header_idx, a_header_idx, b_header_idx))
          ),
          rownames = FALSE, selection = "none",
          class = "compact stripe nowrap"
        )
      },
      extra = function(res) sprintf("mode=%s;rows=%d", input$cmp_mode %||% "", NROW(cmp_joined()))
    )
  }, server = FALSE)

  # -- Reset --

  observeEvent(input$cmp_reset, {
    reset_compare_filters()
  })

  # -- Filter chips --

  output$cmp_filter_chips <- renderUI({
    tryCatch(
      build_filter_chips(
        "cmp", input, shared$season_date_bounds,
        reset_btn_id = "cmp_reset",
        extra_children = stat_filter_chips_ui("cmp", cmp_stat_filter_state, CMP_FILTERABLE_COLS)
      ),
      error = function(e) NULL
    )
  })
}
