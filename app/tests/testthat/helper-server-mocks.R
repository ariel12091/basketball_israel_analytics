# Shared helpers/mocks for tab server smoke tests
library(shiny)
library(dplyr)
library(DT)

`%||%` <- function(a, b) if (!is.null(a)) a else b

is_invalid_persisted_token <- function(x) {
  if (is.null(x)) return(logical(0))
  val <- trimws(tolower(as.character(x)))
  is.na(val) | val %in% c("undefined", "null", "nan", "na")
}

sanitize_persisted_choices <- function(x, max_len = 80L, numeric_only = FALSE) {
  if (is.null(x)) return(character(0))
  vals <- if (is.list(x)) unlist(x, recursive = FALSE, use.names = FALSE) else x
  vals <- trimws(as.character(vals))
  vals <- vals[!is.na(vals) & nzchar(vals)]
  vals <- vals[!is_invalid_persisted_token(vals)]
  if (isTRUE(numeric_only) && length(vals)) {
    nums <- suppressWarnings(as.integer(vals))
    vals <- vals[!is.na(nums)]
  }
  vals <- substr(vals, 1L, 200L)
  vals[seq_len(min(length(vals), max_len))]
}

sanitize_single_choice <- function(x, numeric_only = FALSE) {
  vals <- sanitize_persisted_choices(x, max_len = 1L, numeric_only = numeric_only)
  if (length(vals)) vals[[1]] else ""
}

DEFAULT_START <- as.Date("2024-10-01")
DEFAULT_END <- as.Date("2025-07-01")
DEFAULT_GAME_YEAR <- "2026"
DEFAULT_MIN_ALL <- 100L
DEFAULT_MIN_ON <- 300L
DEFAULT_MIN_NET <- -1e9
LD_DEFAULT_MIN_POSS <- 20L
LD_DEFAULT_NUM <- "5"
CUTS <- seq(0.05, 0.95, by = 0.05)
COLS_GRAD <- grDevices::colorRampPalette(c("#8b2020", "#6b5a20", "#1a6b38"))(20)
COLS_REV <- rev(COLS_GRAD)
GAME_TYPE_LABELS <- c(`1` = "League", `2` = "Cup")
HEADER_TOOLTIP_JS <- DT::JS("function(thead) {}")
OFF_OREB_TOOLTIP <- "Offensive rebound percentage"
DEF_OREB_TOOLTIP <- "Defensive rebound percentage"

dt_escape_except <- function(data, html_cols = character()) {
  data_cols <- names(data)
  html_cols <- intersect(as.character(html_cols), data_cols)
  if (!length(html_cols)) return(TRUE)
  which(!data_cols %in% html_cols)
}

pg_pool <- structure(list(), class = "mock_pool")
GL_DATA_CACHE <- cachem::cache_mem(max_size = 64 * 1024^2, max_age = 3600)

.mock_db_query_counts <- new.env(parent = emptyenv())

reset_mock_db_query_counts <- function() {
  rm(list = ls(envir = .mock_db_query_counts, all.names = TRUE), envir = .mock_db_query_counts)
  invisible(NULL)
}

increment_mock_db_query_count <- function(name) {
  current <- if (exists(name, envir = .mock_db_query_counts, inherits = FALSE)) {
    get(name, envir = .mock_db_query_counts, inherits = FALSE)
  } else {
    0L
  }
  assign(name, current + 1L, envir = .mock_db_query_counts)
  invisible(NULL)
}

mock_db_query_count <- function(name) {
  if (exists(name, envir = .mock_db_query_counts, inherits = FALSE)) {
    get(name, envir = .mock_db_query_counts, inherits = FALSE)
  } else {
    0L
  }
}

db_get_query <- function(pool, query, params = NULL) {
  q <- paste(query, collapse = " ")

  if (grepl("mv_lineup_totals_by_day", q, fixed = TRUE)) {
    increment_mock_db_query_count("gl_lineup_totals")
  }
  if (grepl("lineup_four_factors_by_game", q, fixed = TRUE)) {
    increment_mock_db_query_count("gl_lineup_ff")
  }

  if (grepl("onoff_default_mv", q, fixed = TRUE)) {
    return(data.frame(
      Team = c("Team A", "Team B"),
      Year = c(2026L, 2026L),
      `First Name` = c("Player", "Player"),
      `Last Name` = c("A", "B"),
      `Net RTG Diff` = c(12.5, 8.1),
      `Off ON Diff` = c(7.4, 3.2),
      `Def ON Diff` = c(-5.1, -4.9),
      `Off ON PPP` = c(116.2, 112.1),
      `Def ON PPP` = c(101.4, 104.0),
      `On Net RTG` = c(14.8, 8.1),
      `Off OFF PPP` = c(108.8, 108.9),
      `Def OFF PPP` = c(106.5, 108.9),
      `Off Net RTG` = c(2.3, 0.0),
      `ON Poss` = c(420L, 360L),
      `OFF Poss` = c(220L, 240L),
      minutes = c(180.5, 150.0),
      pr_net = c(1, 0),
      pr_off_on = c(1, 0),
      pr_off_off = c(0, 1),
      pr_def_on_inv = c(1, 0),
      pr_def_off_inv = c(1, 0),
      pr_off_on_d = c(1, 0),
      pr_def_on_d = c(1, 0),
      pr_def_on_d_inv = c(0, 1),
      pr_on_net = c(1, 0),
      pr_off_net = c(1, 0),
      off_on_fg2_made = c(90L, 80L), off_on_fg2_att = c(160L, 150L),
      off_on_fg3_made = c(38L, 32L), off_on_fg3_att = c(100L, 94L),
      off_off_fg2_made = c(42L, 45L), off_off_fg2_att = c(90L, 95L),
      off_off_fg3_made = c(15L, 18L), off_off_fg3_att = c(48L, 54L),
      def_on_fg2_made = c(72L, 76L), def_on_fg2_att = c(150L, 155L),
      def_on_fg3_made = c(30L, 35L), def_on_fg3_att = c(92L, 98L),
      def_off_fg2_made = c(48L, 50L), def_off_fg2_att = c(95L, 100L),
      def_off_fg3_made = c(18L, 20L), def_off_fg3_att = c(55L, 60L),
      player_id = c(11L, 21L),
      team_id = c(1L, 2L),
      check.names = FALSE
    ))
  }

  if (grepl("player_advanced_stats_mv", q, fixed = TRUE)) {
    return(data.frame(
      game_year = c(2026L, 2026L),
      player_id = c(11L, 21L),
      team_id = c(1L, 2L),
      team_name = c("Team A", "Team B"),
      `First Name` = c("Player", "Player"),
      `Last Name` = c("A", "B"),
      off_on_efg = c(0.56, 0.52),
      off_off_efg = c(0.51, 0.50),
      off_on_oreb = c(0.31, 0.28),
      off_off_oreb = c(0.25, 0.24),
      off_on_tov = c(0.12, 0.13),
      off_off_tov = c(0.15, 0.16),
      off_on_ftr = c(0.28, 0.25),
      off_off_ftr = c(0.24, 0.22),
      def_on_efg = c(0.50, 0.53),
      def_off_efg = c(0.54, 0.55),
      def_on_oreb = c(0.24, 0.27),
      def_off_oreb = c(0.29, 0.30),
      def_on_tov = c(0.16, 0.14),
      def_off_tov = c(0.12, 0.11),
      def_on_ftr = c(0.23, 0.26),
      def_off_ftr = c(0.27, 0.28),
      off_on_poss = c(420L, 360L),
      off_off_poss = c(220L, 240L),
      def_on_poss = c(420L, 360L),
      def_off_poss = c(220L, 240L),
      check.names = FALSE
    ))
  }

  if (grepl("get_team_ratings_dynamic", q, fixed = TRUE)) {
    return(data.frame(
      game_year = c(2026L, 2026L),
      team_id = c(1L, 2L),
      team_name = c("Team A", "Team B"),
      off_ppp = c(112.4, 108.8),
      def_ppp = c(101.7, 105.1),
      net_rtg = c(10.7, 3.7),
      off_poss = c(120L, 118L),
      def_poss = c(120L, 118L),
      total_poss = c(120L, 118L),
      games_played = c(4L, 4L),
      wins = c(3L, 2L),
      losses = c(1L, 2L),
      rank_net_rtg = c(1L, 2L),
      rank_off_ppp = c(1L, 2L),
      rank_def_ppp = c(1L, 2L)
    ))
  }

  if (grepl("get_team_four_factors_dynamic", q, fixed = TRUE)) {
    return(data.frame(
      team_id = c(1L, 2L),
      team_name = c("Team A", "Team B"),
      off_efg = c(54.8, 52.1),
      off_ts = c(56.1, 53.4),
      def_efg = c(50.7, 53.3),
      def_ts = c(52.4, 55.0),
      off_tov = c(12.8, 15.6),
      off_oreb = c(31.2, 27.5),
      off_ftr = c(28.4, 24.9),
      def_tov = c(16.1, 13.8),
      def_oreb = c(24.9, 28.3),
      def_ftr = c(22.7, 27.1),
      off_ppp = c(112.4, 108.8),
      def_ppp = c(101.7, 105.1),
      net_rtg = c(10.7, 3.7),
      off_poss = c(120L, 118L),
      def_poss = c(120L, 118L),
      total_poss = c(120L, 118L)
    ))
  }

  if (grepl("team_four_factors_mv", q, fixed = TRUE)) {
    return(data.frame(
      game_year = c(2026L, 2026L),
      team_id = c(1L, 2L),
      team_name = c("Team A", "Team B"),
      off_efg = c(54.8, 52.1),
      off_ts = c(56.1, 53.4),
      def_efg = c(50.7, 53.3),
      def_ts = c(52.4, 55.0),
      off_tov = c(12.8, 15.6),
      off_oreb = c(31.2, 27.5),
      off_ftr = c(28.4, 24.9),
      def_tov = c(16.1, 13.8),
      def_oreb = c(24.9, 28.3),
      def_ftr = c(22.7, 27.1),
      off_ppp = c(112.4, 108.8),
      def_ppp = c(101.7, 105.1),
      net_rtg = c(10.7, 3.7),
      off_poss = c(120L, 118L),
      def_poss = c(120L, 118L),
      total_poss = c(120L, 118L)
    ))
  }

  if (grepl("team_stats AS", q, fixed = TRUE) && grepl("poss_on_floor", q, fixed = TRUE)) {
    return(data.frame(
      team_id = c(1L, 2L),
      team_name = c("Team A", "Team B"),
      gp = c(4L, 4L),
      poss_on_floor = c(320, 300),
      minutes = c(160, 152),
      pts = c(360, 332),
      reb = c(150, 142),
      oreb = c(44, 38),
      dreb = c(106, 104),
      ast = c(82, 71),
      stl = c(28, 24),
      blk = c(18, 14),
      tov = c(45, 51),
      fgm = c(136, 124),
      fga = c(275, 268),
      `2pm` = c(102, 95),
      `2pa` = c(184, 180),
      `3pm` = c(34, 29),
      `3pa` = c(91, 88),
      ftm = c(54, 55),
      fta = c(70, 76),
      fg_pct = c(49.5, 46.3),
      two_pct = c(55.4, 52.8),
      tp_pct = c(37.4, 33.0),
      ft_pct = c(77.1, 72.4),
      efg = c(55.6, 51.7),
      ts = c(58.2, 54.8),
      check.names = FALSE
    ))
  }

  if (grepl("get_player_traditional_dynamic", q, fixed = TRUE)) {
    out <- data.frame(
      player_id = c(11L, 12L, 21L),
      team_id = c(1L, 1L, 2L),
      team_name = c("Team A", "Team A", "Team B"),
      player_name = c("Player A", "Player C", "Player B"),
      gp = c(5L, 4L, 5L),
      pts = c(100, 72, 90),
      reb = c(40, 24, 35),
      oreb = c(12, 7, 10),
      dreb = c(28, 17, 25),
      ast = c(30, 18, 20),
      stl = c(10, 7, 8),
      blk = c(6, 4, 5),
      tov = c(14, 11, 13),
      fgm = c(38, 28, 34),
      fga = c(70, 55, 66),
      `3pm` = c(11, 8, 9),
      `3pa` = c(29, 22, 26),
      ftm = c(13, 8, 13),
      fta = c(16, 10, 16),
      fg_pct = c(55.0, 51.0, 50.0),
      tp_pct = c(38.0, 36.0, 34.0),
      ft_pct = c(84.0, 80.0, 79.0),
      efg = c(62.8, 58.2, 56.8),
      ts = c(60.0, 58.0, 57.0),
      poss_on_floor = c(300, 220, 280),
      minutes = c(150, 120, 145),
      check.names = FALSE
    )
    team_csv <- if (!is.null(params) && length(params) >= 4L) params[[4]] else NA_character_
    if (!is.null(team_csv) && !is.na(team_csv) && nzchar(team_csv)) {
      team_ids <- suppressWarnings(as.integer(strsplit(team_csv, ",", fixed = TRUE)[[1]]))
      team_ids <- team_ids[is.finite(team_ids)]
      out <- out[out$team_id %in% team_ids, , drop = FALSE]
    }
    home_away <- if (!is.null(params) && length(params) >= 7L) params[[7]] else NA_character_
    if (!is.null(home_away) && !is.na(home_away) && identical(as.character(home_away), "home") &&
        nrow(out) && all(out$team_id == 1L)) {
      out <- out[out$player_id == 11L, , drop = FALSE]
    }
    if (!is.null(home_away) && !is.na(home_away) && identical(as.character(home_away), "away") &&
        nrow(out) && all(out$team_id == 1L)) {
      out <- out[out$player_id == 12L, , drop = FALSE]
    }
    return(out)
  }

  if (grepl("fetch_lineups_csv_v2", q, fixed = TRUE)) {
    return(data.frame(
      team_id = c(1L, 2L),
      sub_lineup_hash = c("lu1", "lu2"),
      player_names_str = c("A1, A2, A3, A4, A5", "B1, B2, B3, B4, B5"),
      team_name = c("Team A", "Team B"),
      off_ppp = c(118.2, 109.4),
      def_ppp = c(99.8, 104.3),
      net_rtg = c(18.4, 5.1),
      total_poss = c(42L, 37L),
      off_poss = c(42L, 37L),
      def_poss = c(42L, 37L),
      off_fg2_made = c(20L, 18L),
      off_fg2_att = c(35L, 34L),
      off_fg3_made = c(8L, 7L),
      off_fg3_att = c(24L, 23L),
      def_fg2_made = c(18L, 19L),
      def_fg2_att = c(33L, 34L),
      def_fg3_made = c(7L, 8L),
      def_fg3_att = c(22L, 21L)
    ))
  }

  if (grepl("fetch_lineups_four_factors_csv", q, fixed = TRUE)) {
    return(data.frame(
      team_id = c(1L, 2L),
      sub_lineup_hash = c("lu1", "lu2"),
      player_names_str = c("A1, A2, A3, A4, A5", "B1, B2, B3, B4, B5"),
      team_name = c("Team A", "Team B"),
      off_efg = c(57.1, 53.5),
      off_ts = c(58.7, 54.8),
      def_efg = c(51.4, 55.2),
      def_ts = c(53.1, 56.4),
      off_tov = c(11.2, 14.5),
      off_oreb = c(33.1, 28.6),
      off_ftr = c(29.3, 23.7),
      def_tov = c(15.8, 12.7),
      def_oreb = c(24.4, 29.1),
      def_ftr = c(21.2, 26.5),
      off_ppp = c(118.2, 109.4),
      def_ppp = c(99.8, 104.3),
      net_rtg = c(18.4, 5.1),
      minutes = c(20.5, 18.0),
      total_poss = c(42L, 37L),
      off_poss = c(42L, 37L),
      def_poss = c(42L, 37L),
      off_pts = c(50L, 41L),
      def_pts = c(42L, 39L),
      off_ts_poss = c(44L, 38L),
      off_oreb_cnt = c(10L, 8L),
      off_oreb_opps = c(30L, 28L),
      off_tov_cnt = c(5L, 6L),
      off_fta = c(12L, 9L),
      off_fga_cnt = c(35L, 31L),
      off_fgm_cnt = c(18L, 15L),
      off_fg3m_cnt = c(4L, 3L),
      def_ts_poss = c(40L, 37L),
      def_oreb_cnt = c(7L, 9L),
      def_oreb_opps = c(28L, 31L),
      def_tov_cnt = c(7L, 5L),
      def_fta = c(8L, 11L),
      def_fga_cnt = c(33L, 32L),
      def_fgm_cnt = c(15L, 17L),
      def_fg3m_cnt = c(3L, 4L)
    ))
  }

  if (grepl("final_schedule_mv", q, fixed = TRUE)) {
    return(data.frame(
      game_year = rep(2026L, 4),
      game_id = c(101L, 102L, 103L, 104L),
      team_id = rep(1L, 4),
      team_name = rep("Team A", 4),
      opp_team_id = c(2L, 3L, 4L, 5L),
      gn = 1:4,
      game_type = c(1L, 1L, 1L, 2L),
      game_date = as.Date(c("2025-10-10", "2025-10-17", "2025-10-24", "2025-10-31")),
      opp_team_name = c("Team B", "Team C", "Team D", "Team E"),
      team_score = c(90L, 100L, 110L, 120L),
      opp_score = c(120L, 110L, 100L, 90L),
      has_won = c(FALSE, TRUE, TRUE, TRUE),
      is_home = c(TRUE, FALSE, TRUE, FALSE)
    ))
  }

  if (grepl("mv_lineup_totals_by_day", q, fixed = TRUE)) {
    return(data.frame(
      team_id = rep(1L, 8),
      lineup_hash = paste0("lu", seq_len(8)),
      type_lineup = rep(c("offense", "defense"), 4),
      g_date = rep(as.Date(c("2025-10-10", "2025-10-17", "2025-10-24", "2025-10-31")), each = 2),
      game_id = rep(c(101L, 102L, 103L, 104L), each = 2),
      game_year = rep(2026L, 8),
      total_poss = rep(100, 8),
      total_pts = c(90, 120, 100, 110, 110, 100, 120, 90),
      fg2_made = c(20, 22, 21, 21, 23, 20, 24, 18),
      fg2_att = c(35, 36, 34, 35, 36, 33, 37, 32),
      fg3_made = c(8, 10, 9, 9, 10, 8, 11, 7),
      fg3_att = c(24, 25, 23, 24, 25, 22, 26, 21),
      num_starters = rep(5L, 8)
    ))
  }

  if (grepl("lineup_four_factors_by_game", q, fixed = TRUE)) {
    return(data.frame(
      lineup_hash = paste0("ff", seq_len(8)),
      team_id = rep(1L, 8),
      game_id = rep(c(101L, 102L, 103L, 104L), each = 2),
      game_year = rep(2026L, 8),
      type_lineup = rep(c("offense", "defense"), 4),
      total_points = c(90, 120, 100, 110, 110, 100, 120, 90),
      total_poss = rep(100, 8),
      ts_poss_count = c(90, 85, 88, 84, 86, 83, 84, 82),
      oreb_count = c(8, 14, 10, 12, 12, 10, 14, 8),
      oreb_opportunities = rep(40, 8),
      tov_count = c(18, 10, 15, 11, 12, 12, 9, 13),
      total_ft_attempts = c(12, 18, 14, 17, 16, 15, 18, 14),
      total_fga = c(70, 75, 72, 74, 74, 73, 76, 72),
      total_fgm = c(31, 36, 34, 35, 37, 33, 39, 31),
      total_fg3_made = c(8, 10, 9, 9, 10, 8, 11, 7),
      num_starters = rep(5L, 8)
    ))
  }

  if (grepl("full_rosters", q, fixed = TRUE)) {
    return(data.frame(
      team_id = c(1L, 1L, 2L),
      player_id = c(11L, 12L, 21L),
      name = c("Player A", "Player C", "Player B"),
      team_name = c("Team A", "Team A", "Team B")
    ))
  }

  data.frame()
}

cached_ref_query <- function(key, query_fun, ttl_sec = 300) {
  if (grepl("_gn_", key, fixed = TRUE)) return(data.frame(gn = 1:5))
  if (grepl("gl_teams_", key, fixed = TRUE)) {
    return(data.frame(team_id = 1:5, team_name = paste("Team", LETTERS[1:5])))
  }
  if (grepl("teams", key, fixed = TRUE) || grepl("_teams_", key, fixed = TRUE)) {
    return(data.frame(team_id = c(1L, 2L), team_name = c("Team A", "Team B")))
  }
  if (grepl("_players_", key, fixed = TRUE)) {
    return(data.frame(
      team_id = c(1L, 1L, 2L),
      player_id = c(11L, 12L, 21L),
      name = c("Player A", "Player C", "Player B")
    ))
  }
  if (grepl("cmp_default_scorers_", key, fixed = TRUE)) {
    return(data.frame(
      player_id = c(11L, 12L, 21L),
      gp = c(5L, 4L, 5L),
      pts = c(100, 72, 90)
    ))
  }
  data.frame()
}

guard_heavy_request <- function(...) TRUE
adaptive_baseline <- function(...) 0
empty_dt <- function(msg = "") data.frame(message = msg)
fmt_rank_cell <- function(value, rank_now, delta, digits = 1) as.character(round(as.numeric(value), digits = digits))

build_filter_chips <- function(...) shiny::tags$div(class = "filter-chips", "chips")
setup_chip_clears <- function(...) invisible(TRUE)
normalize_stat_filter_cols <- function(filterable_cols) {
  cols <- if (is.function(filterable_cols)) filterable_cols() else filterable_cols
  if (is.null(cols)) return(stats::setNames(character(0), character(0)))
  if (is.list(cols) && !is.atomic(cols)) cols <- unlist(cols, use.names = TRUE)
  labels <- names(cols)
  cols <- as.character(cols)
  if (is.null(labels)) labels <- rep("", length(cols))
  keep <- nzchar(labels) & nzchar(cols)
  stats::setNames(cols[keep], labels[keep])
}
make_stat_filter_state <- function() {
  list(filters = shiny::reactiveVal(list()), next_id = shiny::reactiveVal(1L))
}
reset_stat_filters <- function(state) {
  state$filters(list())
  state$next_id(1L)
  invisible(NULL)
}
apply_stat_filters <- function(df, filters) {
  if (is.null(df) || !nrow(df) || !length(filters)) return(df)
  for (f in filters) {
    if (!f$col %in% names(df)) next
    v <- suppressWarnings(as.numeric(df[[f$col]]))
    threshold <- suppressWarnings(as.numeric(f$value))
    if (length(threshold) != 1L || !is.finite(threshold)) next
    keep <- !is.na(v) & (if (identical(f$op, "le")) v <= threshold else v >= threshold)
    df <- df[keep, , drop = FALSE]
  }
  df
}
setup_stat_filter_handlers <- function(...) invisible(TRUE)
stat_filter_chips_ui <- function(...) list()
team_select_choices_with_all <- function(teams_df, all_label = "- All teams -") {
  if (is.null(teams_df) || !nrow(teams_df)) {
    out <- ""
    names(out) <- all_label
    return(out)
  }
  out <- c("", as.character(teams_df$team_id))
  names(out) <- c(all_label, teams_df$team_name)
  out
}
update_single_team_selectize <- function(session, select_id, teams_df, selected = "", all_label = "- All teams -") {
  updateSelectizeInput(
    session,
    select_id,
    choices = team_select_choices_with_all(teams_df, all_label = all_label),
    selected = selected,
    server = TRUE
  )
}
update_gn_last_n_choices <- function(session, prefix, gn_vals) {
  gn_vals <- suppressWarnings(as.integer(gn_vals))
  gn_vals <- gn_vals[is.finite(gn_vals)]
  gn_choices <- c("", as.character(gn_vals))
  last_choices <- if (length(gn_vals)) c("", as.character(seq_len(max(gn_vals, na.rm = TRUE)))) else ""
  updateSelectizeInput(session, paste0(prefix, "_gn_min"), choices = gn_choices, selected = "")
  updateSelectizeInput(session, paste0(prefix, "_gn_max"), choices = gn_choices, selected = "")
  updateSelectizeInput(session, paste0(prefix, "_last_n"), choices = last_choices, selected = "")
}
resolve_gn_last_n_params <- function(input, prefix) {
  min_gn <- input[[paste0(prefix, "_gn_min")]] %||% ""
  max_gn <- input[[paste0(prefix, "_gn_max")]] %||% ""
  last_n <- input[[paste0(prefix, "_last_n")]] %||% ""
  min_gn <- if (nzchar(min_gn)) as.integer(min_gn) else NA_integer_
  max_gn <- if (nzchar(max_gn)) as.integer(max_gn) else NA_integer_
  last_n <- if (nzchar(last_n)) as.integer(last_n) else NA_integer_
  if (!is.na(last_n)) { min_gn <- NA_integer_; max_gn <- NA_integer_ }
  if (!is.na(min_gn) || !is.na(max_gn)) last_n <- NA_integer_
  if (!is.na(min_gn) && !is.na(max_gn) && min_gn > max_gn) { tmp <- min_gn; min_gn <- max_gn; max_gn <- tmp }
  list(min_gn = min_gn, max_gn = max_gn, last_n = last_n)
}
setup_gn_last_n_sync <- function(session, input, prefix) invisible(TRUE)
reset_gn_last_n_inputs <- function(session, prefix) {
  updateSelectizeInput(session, paste0(prefix, "_gn_min"), selected = "")
  updateSelectizeInput(session, paste0(prefix, "_gn_max"), selected = "")
  updateSelectizeInput(session, paste0(prefix, "_last_n"), selected = "")
}
reset_opp_rank_inputs <- function(session, prefix) {
  updateSelectInput(session, paste0(prefix, "_opp_rank_side"), selected = "")
  updateSelectInput(session, paste0(prefix, "_opp_rank_n"), selected = "")
  updateSelectInput(session, paste0(prefix, "_opp_rank_metric"), selected = "")
}
reset_starters_inputs <- function(session, prefix, own_prefix = "num_starters_off", opp_prefix = "num_starters_def") {
  updateSelectInput(session, paste0(prefix, "_", own_prefix, "_mode"), selected = "")
  updateSelectInput(session, paste0(prefix, "_", own_prefix), selected = "")
  updateSelectInput(session, paste0(prefix, "_", opp_prefix, "_mode"), selected = "")
  updateSelectInput(session, paste0(prefix, "_", opp_prefix), selected = "")
}
reset_clutch_inputs <- function(session, prefix, status_default = "all", margin_default = 5, minutes_default = 5) {
  updateCheckboxInput(session, paste0(prefix, "_clutch_enabled"), value = FALSE)
  updateSliderInput(session, paste0(prefix, "_clutch_margin"), value = margin_default)
  updateSelectInput(session, paste0(prefix, "_clutch_status"), selected = status_default)
  updateSliderInput(session, paste0(prefix, "_clutch_minutes"), value = minutes_default)
  updateCheckboxInput(session, paste0(prefix, "_clutch_ot_margin"), value = FALSE)
}
blank_to_na_character <- function(value) {
  value <- value %||% ""
  if (!nzchar(value)) NA_character_ else value
}
blank_to_na_integer <- function(value) {
  value <- blank_to_na_character(value)
  suppressWarnings(as.integer(value))
}
csv_if_any <- function(values, integerize = FALSE) {
  if (is.null(values) || !length(values)) return(NA_character_)
  values <- values[nzchar(as.character(values))]
  if (!length(values)) return(NA_character_)
  if (isTRUE(integerize)) {
    values <- suppressWarnings(as.integer(values))
    values <- values[is.finite(values)]
    if (!length(values)) return(NA_character_)
  }
  paste(values, collapse = ",")
}
resolve_clutch_params <- function(enabled, margin, status, minutes, ot_margin) {
  clutch_enabled <- isTRUE(enabled)
  list(
    max_margin = if (clutch_enabled) suppressWarnings(as.integer(margin)) else NA_integer_,
    margin_status = if (clutch_enabled) blank_to_na_character(status) else NA_character_,
    max_time_remaining = if (clutch_enabled) suppressWarnings(as.integer(minutes)) * 60L else NA_integer_,
    ot_margin_filter = if (clutch_enabled) isTRUE(ot_margin) else FALSE
  )
}
resolve_starters_bounds <- function(off_mode, off_value, def_mode, def_value) {
  off_mode <- off_mode %||% ""
  def_mode <- def_mode %||% ""
  off_val <- if (nzchar(off_mode)) blank_to_na_integer(off_value) else NA_integer_
  def_val <- if (nzchar(def_mode)) blank_to_na_integer(def_value) else NA_integer_
  list(
    num_starters_off_min = if (identical(off_mode, "gte")) off_val else NA_integer_,
    num_starters_off_max = if (identical(off_mode, "lte")) off_val else NA_integer_,
    num_starters_def_min = if (identical(def_mode, "gte")) def_val else NA_integer_,
    num_starters_def_max = if (identical(def_mode, "lte")) def_val else NA_integer_
  )
}
apply_visible_col_order <- function(df, visible_order, hidden_cols = character()) {
  if (is.null(df) || !length(visible_order)) return(df)

  all_cols <- names(df)
  hidden_cols <- intersect(hidden_cols, all_cols)
  visible_cols <- setdiff(all_cols, hidden_cols)
  saved_visible <- intersect(as.character(visible_order), visible_cols)
  if (!length(saved_visible)) return(df)

  df[, c(saved_visible, setdiff(visible_cols, saved_visible), hidden_cols), drop = FALSE]
}
dt_col_order_init_callback <- function(...) {
  DT::JS("function(settings, json) {}")
}
csv_export_stamp <- function(now = Sys.time()) {
  format(now, "%Y%m%d_%H%M%S")
}

source(repo_file("R", "logger.R"), local = TRUE)
source(repo_file("R", "server_tab1.R"), local = TRUE)
source(repo_file("R", "mod_lineup_player_filter.R"), local = TRUE)
source(repo_file("R", "server_tab2.R"), local = TRUE)
source(repo_file("R", "server_tab3.R"), local = TRUE)
source(repo_file("R", "server_tab4.R"), local = TRUE)
source(repo_file("R", "server_tab5_traditional.R"), local = TRUE)
source(repo_file("R", "server_tab6_team_stats.R"), local = TRUE)
source(repo_file("R", "server_tab7_compare.R"), local = TRUE)

make_shared <- function(data_version = shiny::reactiveVal("test-etl-v1")) {
  pending_compare_preset <- shiny::reactiveVal(NULL)
  pending_ld_team <- shiny::reactiveVal(NULL)
  pending_gl_team <- shiny::reactiveVal(NULL)
  list(
    season_date_bounds = function(gy) {
      if (identical(as.character(gy), "2026")) {
        list(start = as.Date("2025-10-01"), end = as.Date("2026-07-01"))
      } else {
        list(start = DEFAULT_START, end = DEFAULT_END)
      }
    },
    selected_game_year = shiny::reactive({ DEFAULT_GAME_YEAR }),
    teams_for_year_df = shiny::reactive({ data.frame(team_id = c(1L, 2L), team_name = c("Team A", "Team B")) }),
    selected_opp_ids_on = shiny::reactive({ NULL }),
    selected_opp_ids_ld = shiny::reactive({ NULL }),
    data_version = data_version,
    pending_ld_team = pending_ld_team,
    pending_gl_team = pending_gl_team,
    pending_compare_preset = pending_compare_preset
  )
}
