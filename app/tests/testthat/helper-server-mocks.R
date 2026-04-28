# Shared helpers/mocks for tab server smoke tests
library(shiny)
library(dplyr)
library(DT)

`%||%` <- function(a, b) if (!is.null(a)) a else b

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

pg_pool <- structure(list(), class = "mock_pool")

db_get_query <- function(pool, query, params = NULL) {
  q <- paste(query, collapse = " ")

  if (grepl("get_team_ratings_dynamic", q, fixed = TRUE)) {
    return(data.frame(
      team_id = c(1L, 2L),
      team_name = c("Team A", "Team B"),
      off_ppp = c(112.4, 108.8),
      def_ppp = c(101.7, 105.1),
      net_rtg = c(10.7, 3.7),
      off_poss = c(120L, 118L),
      total_poss = c(120L, 118L),
      games_played = c(4L, 4L),
      wins = c(3L, 2L),
      losses = c(1L, 2L)
    ))
  }

  if (grepl("get_team_four_factors_dynamic", q, fixed = TRUE)) {
    return(data.frame(
      team_id = c(1L, 2L),
      team_name = c("Team A", "Team B"),
      off_ts = c(56.1, 53.4),
      off_tov = c(12.8, 15.6),
      off_oreb = c(31.2, 27.5),
      off_ftr = c(28.4, 24.9),
      off_poss = c(120L, 118L),
      total_poss = c(120L, 118L)
    ))
  }

  if (grepl("get_player_traditional_dynamic", q, fixed = TRUE)) {
    return(data.frame(
      player_id = c(11L, 12L, 21L),
      gp = c(5L, 4L, 5L),
      pts = c(100, 72, 90),
      reb = c(40, 24, 35),
      ast = c(30, 18, 20),
      stl = c(10, 7, 8),
      fg_pct = c(55.0, 51.0, 50.0),
      tp_pct = c(38.0, 36.0, 34.0),
      ft_pct = c(84.0, 80.0, 79.0),
      ts = c(60.0, 58.0, 57.0),
      poss_on_floor = c(300, 220, 280),
      minutes = c(150, 120, 145)
    ))
  }

  if (grepl("fetch_lineups_csv_v2", q, fixed = TRUE)) {
    return(data.frame(
      sub_lineup_hash = c("lu1", "lu2"),
      player_names_str = c("A1, A2, A3, A4, A5", "B1, B2, B3, B4, B5"),
      team_name = c("Team A", "Team B"),
      off_ppp = c(118.2, 109.4),
      def_ppp = c(99.8, 104.3),
      net_rtg = c(18.4, 5.1),
      total_poss = c(42L, 37L),
      off_poss = c(42L, 37L),
      def_poss = c(42L, 37L)
    ))
  }

  if (grepl("fetch_lineups_four_factors_csv", q, fixed = TRUE)) {
    return(data.frame(
      sub_lineup_hash = c("lu1", "lu2"),
      player_names_str = c("A1, A2, A3, A4, A5", "B1, B2, B3, B4, B5"),
      team_name = c("Team A", "Team B"),
      off_ts = c(58.7, 54.8),
      off_tov = c(11.2, 14.5),
      off_oreb = c(33.1, 28.6),
      off_ftr = c(29.3, 23.7),
      total_poss = c(42L, 37L),
      off_poss = c(42L, 37L),
      def_poss = c(42L, 37L)
    ))
  }

  if (grepl("final_schedule_mv", q, fixed = TRUE)) {
    return(data.frame(
      game_year = rep(2026L, 4),
      game_id = c(101L, 102L, 103L, 104L),
      team_id = rep(1L, 4),
      team_name = rep("Team A", 4),
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

source(repo_file("R", "server_tab1.R"), local = TRUE)
source(repo_file("R", "mod_lineup_player_filter.R"), local = TRUE)
source(repo_file("R", "server_tab2.R"), local = TRUE)
source(repo_file("R", "server_tab3.R"), local = TRUE)
source(repo_file("R", "server_tab4.R"), local = TRUE)
source(repo_file("R", "server_tab5_traditional.R"), local = TRUE)
source(repo_file("R", "server_tab7_compare.R"), local = TRUE)

make_shared <- function() {
  pending_compare_preset <- shiny::reactiveVal(NULL)
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
    pending_compare_preset = pending_compare_preset
  )
}
