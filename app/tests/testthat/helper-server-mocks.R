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

  if (grepl("full_rosters", q, fixed = TRUE)) {
    return(data.frame(
      team_id = c(1L, 2L),
      player_id = c(11L, 21L),
      name = c("Player A", "Player B"),
      team_name = c("Team A", "Team B")
    ))
  }

  data.frame()
}

cached_ref_query <- function(key, query_fun, ttl_sec = 300) {
  if (grepl("_gn_", key, fixed = TRUE)) return(data.frame(gn = 1:5))
  if (grepl("teams", key, fixed = TRUE) || grepl("_teams_", key, fixed = TRUE)) {
    return(data.frame(team_id = c(1L, 2L), team_name = c("Team A", "Team B")))
  }
  data.frame()
}

guard_heavy_request <- function(...) TRUE
adaptive_baseline <- function(...) 0
empty_dt <- function(msg = "") data.frame(message = msg)
fmt_rank_cell <- function(value, rank_now, delta, digits = 1) as.character(round(as.numeric(value), digits = digits))

build_filter_chips <- function(...) shiny::tags$div(class = "filter-chips", "chips")
setup_chip_clears <- function(...) invisible(TRUE)

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
