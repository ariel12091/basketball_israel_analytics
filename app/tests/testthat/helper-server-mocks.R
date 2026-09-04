# Shared helpers/mocks for tab server smoke tests
library(shiny)
library(dplyr)
library(DT)

# Real pure helpers — single source of truth shared with the app (R/helpers.R).
# Only the impure pieces (DB, caches, guards, chip builders) are stubbed below.
source(repo_file("R", "helpers.R"), local = TRUE)

DEFAULT_START <- as.Date("2024-10-01")
DEFAULT_END <- as.Date("2025-07-01")
DEFAULT_GAME_YEAR <- "2026"
DEFAULT_HOME_TEAM_ID <- "1"
DEFAULT_HOME_TEAM_NAME <- "Team A"
static_team_roster <- function(gy) {
  if (!identical(suppressWarnings(as.integer(gy)), 2026L)) return(NULL)
  data.frame(
    team_id = c(1L, 2L),
    team_name = c("Team A", "Team B")
  )
}
DEFAULT_MIN_ALL <- 100L
DEFAULT_MIN_ON <- 300L
DEFAULT_MIN_NET <- -1e9
LD_DEFAULT_MIN_POSS <- 20L
LD_DEFAULT_NUM <- "5"
CUTS <- seq(0.05, 0.95, by = 0.05)
# The colour ramp mirrors global.R. Read the real definition rather than
# copying it: a copy here silently kept the old palette when the ramp changed,
# so the mocks and the app disagreed about what a heat cell looks like.
.ramp_src <- readLines(repo_file("R", "global.R"), warn = FALSE)
eval(parse(text = .ramp_src[
  startsWith(.ramp_src, "RAMP_ANCHORS") |
  startsWith(.ramp_src, "COLS_GRAD") |
  startsWith(.ramp_src, "COLS_REV")
]))
GAME_TYPE_LABELS <- c(`1` = "League", `2` = "Cup")
onoff_tab_descriptor <- function(league = c("israel", "euroleague")) {
  league <- match.arg(league)
  if (identical(league, "israel")) {
    return(list(prefix = "on", game_type_id = "on_game_type", teams_id = "teams",
                show_impact = TRUE))
  }
  list(prefix = "euro", game_type_id = "euro_phase", teams_id = "euro_teams",
       show_impact = FALSE)
}
HEADER_TOOLTIP_JS <- DT::JS("function(thead) {}")
OFF_OREB_TOOLTIP <- "Offensive rebound percentage"
DEF_OREB_TOOLTIP <- "Defensive rebound percentage"

pg_pool <- structure(list(), class = "mock_pool")
GL_DATA_CACHE <- cachem::cache_mem(max_size = 64 * 1024^2, max_age = 3600)

# Passthrough in tests: no cross-test cache pollution, always hits the mock DB.
cached_season_df <- function(key_parts, query_fun) query_fun()

# Canonical per-season lookups (real versions live in global.R, which tests
# don't source, so they are stubbed with mock data here).
fetch_teams_distinct <- function(gy) {
  data.frame(team_id = c(1L, 2L), team_name = c("Team A", "Team B"))
}
fetch_teams_min <- function(gy) {
  data.frame(team_id = 1:5, team_name = paste("Team", LETTERS[1:5]))
}
fetch_gn_values <- function(gy) data.frame(gn = 1:5)
fetch_players_basic <- function(gy) {
  data.frame(
    team_id = c(1L, 1L, 2L),
    player_id = c(11L, 12L, 21L),
    name = c("Player A", "Player C", "Player B")
  )
}

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

mock_team_ratings_df <- function() {
  data.frame(
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
    off_fga = c(260L, 244L),
    off_layup_att = c(70L, 60L),
    off_dunk_att = c(12L, 9L),
    off_fg3_att = c(100L, 94L),
    off_c3_att = c(22L, 18L),
    off_c3_known_att = c(90L, 85L),
    def_fga = c(242L, 253L),
    def_layup_att = c(58L, 62L),
    def_dunk_att = c(7L, 8L),
    def_fg3_att = c(92L, 98L),
    def_c3_att = c(20L, 22L),
    def_c3_known_att = c(85L, 90L),
    rank_net_rtg = c(1L, 2L),
    rank_off_ppp = c(1L, 2L),
    rank_def_ppp = c(1L, 2L)
  )
}

# Production definitions live in mod_team_hub.R, which the compare server is
# sourced without in these server tests.
hub_fetch_team_ratings <- function(gy, ver) {
  db_get_query(
    pg_pool,
    "SELECT * FROM basketball_test.team_ppp_ratings_mv WHERE game_year = $1::int4",
    params = list(as.integer(gy))
  )
}

hub_fetch_team_ratings_presets <- function(gy, ver) {
  db_get_query(
    pg_pool,
    paste0(
      "SELECT preset_variant AS hub_variant, * ",
      "FROM basketball_test.team_ratings_preset_cache WHERE game_year = $1::int4"
    ),
    params = list(as.integer(gy))
  )
}

db_get_query <- function(pool, query, params = NULL) {
  q <- paste(query, collapse = " ")

  if (grepl("home_dashboard_combined", q, fixed = TRUE)) {
    increment_mock_db_query_count("home_dashboard_combined")
    ratings <- mock_team_ratings_df()
    variants <- c(
      "starters_hi", "starters_lo", "clutch",
      "last10", "top4", "bottom4"
    )
    storylines <- do.call(rbind, lapply(variants, function(variant) {
      out <- ratings
      out$hub_variant <- variant
      out[, c("hub_variant", setdiff(names(out), "hub_variant")), drop = FALSE]
    }))
    four_factors <- data.frame(
      game_year = c(2026L, 2026L),
      team_id = c(1L, 2L),
      team_name = c("Team A", "Team B"),
      off_efg = c(54.8, 52.1), off_tov = c(12.8, 15.6),
      off_oreb = c(31.2, 27.5), off_ftr = c(28.4, 24.9),
      def_efg = c(50.7, 53.3), def_tov = c(16.1, 13.8),
      def_oreb = c(24.9, 28.3), def_ftr = c(22.7, 27.1)
    )
    selected_team <- suppressWarnings(as.integer(params[[2]]))
    onoff <- data.frame(
      team_id = c(1L, 2L),
      `ON Poss` = c(420L, 360L),
      `Net RTG Diff` = c(12.5, 8.1),
      `First Name` = c("Player", "Player"),
      `Last Name` = c("A", "B"),
      check.names = FALSE
    )
    onoff <- onoff[onoff$team_id == selected_team, , drop = FALSE]
    traditional <- data.frame(
      team_id = c(1L, 2L), player_id = c(11L, 21L),
      player_name = c("Player A", "Player B"),
      gp = c(5L, 5L), pts = c(100, 90)
    )
    traditional <- traditional[
      traditional$team_id == selected_team,
      ,
      drop = FALSE
    ]
    lineups <- data.frame(
      player_names_str = c("A1, A2, A3, A4, A5", "A6, A7, A8, A9, A10"),
      net_rtg = c(18.4, -5.1),
      off_poss = c(42L, 55L),
      def_poss = c(42L, 55L)
    )
    encode <- function(x) jsonlite::toJSON(
      x,
      dataframe = "rows",
      auto_unbox = TRUE,
      na = "null",
      digits = NA
    )
    return(data.frame(
      data_version = "test-etl-v1",
      storylines_json = encode(storylines),
      ratings_json = encode(ratings),
      four_factors_json = encode(four_factors),
      onoff_json = encode(onoff),
      traditional_json = encode(traditional),
      lineups_json = encode(lineups),
      stringsAsFactors = FALSE
    ))
  }

  if (grepl("basketball_test.onoff_compute(", q, fixed = TRUE)) {
    increment_mock_db_query_count("onoff_compute")
  }

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
      off_on_layup_made = c(40L, 36L), off_on_layup_att = c(60L, 55L),
      off_on_dunk_made = c(9L, 7L), off_on_dunk_att = c(10L, 8L),
      off_on_c3_made = c(9L, 7L), off_on_c3_att = c(22L, 18L), off_on_c3_known_att = c(90L, 85L),
      off_off_layup_made = c(20L, 22L), off_off_layup_att = c(34L, 36L),
      off_off_dunk_made = c(3L, 4L), off_off_dunk_att = c(4L, 5L),
      off_off_c3_made = c(4L, 5L), off_off_c3_att = c(10L, 12L), off_off_c3_known_att = c(44L, 50L),
      def_on_layup_made = c(30L, 32L), def_on_layup_att = c(52L, 56L),
      def_on_dunk_made = c(5L, 6L), def_on_dunk_att = c(6L, 7L),
      def_on_c3_made = c(7L, 8L), def_on_c3_att = c(20L, 22L), def_on_c3_known_att = c(85L, 90L),
      def_off_layup_made = c(22L, 24L), def_off_layup_att = c(36L, 38L),
      def_off_dunk_made = c(2L, 3L), def_off_dunk_att = c(3L, 4L),
      def_off_c3_made = c(3L, 4L), def_off_c3_att = c(9L, 11L), def_off_c3_known_att = c(42L, 48L),
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
      def_on_disruptions = c(0.18, 0.15),
      def_off_disruptions = c(0.13, 0.12),
      `Def Disruptions/100 Diff` = c(5.0, 3.0),
      def_on_ftr = c(0.23, 0.26),
      def_off_ftr = c(0.27, 0.28),
      off_on_poss = c(420L, 360L),
      off_off_poss = c(220L, 240L),
      def_on_poss = c(420L, 360L),
      def_off_poss = c(220L, 240L),
      check.names = FALSE
    ))
  }

  if (grepl("team_ratings_preset_cache", q, fixed = TRUE)) {
    increment_mock_db_query_count("team_ratings_preset_cache")
    ratings <- mock_team_ratings_df()
    variants <- c(
      "starters_hi",
      "starters_lo",
      "clutch",
      "last10",
      "top4",
      "bottom4"
    )
    return(do.call(rbind, lapply(variants, function(variant) {
      out <- ratings
      out$hub_variant <- variant
      out[, c("hub_variant", setdiff(names(out), "hub_variant")), drop = FALSE]
    })))
  }

  # The Traditional box-score query (tabs 3 and 6) LEFT JOINs team_ppp_ratings_mv
  # for opponent ranks, so a bare grepl() on that name claims it and hands back
  # the ratings frame -- no pts/reb/dfl, so no rank_* columns get derived and the
  # display data.frame() dies on mismatched lengths. Let the team_stats branch
  # below answer it.
  if (grepl("team_ppp_ratings_mv", q, fixed = TRUE) &&
      !grepl("team_stats AS", q, fixed = TRUE)) {
    increment_mock_db_query_count("team_ppp_ratings_mv")
    return(mock_team_ratings_df())
  }

  if (grepl("get_team_ratings_dynamic", q, fixed = TRUE) ||
      grepl("get_team_ratings_direct", q, fixed = TRUE)) {
    increment_mock_db_query_count("team_ratings_dynamic")
    ratings <- mock_team_ratings_df()
    if (grepl("AS hub_variant", q, fixed = TRUE)) {
      increment_mock_db_query_count("hub_storylines_batch")
      variants <- c(
        "starters_hi",
        "starters_lo",
        "clutch",
        "last10",
        "top4",
        "bottom4"
      )
      return(do.call(rbind, lapply(variants, function(variant) {
        out <- ratings
        out$hub_variant <- variant
        out[, c("hub_variant", setdiff(names(out), "hub_variant")), drop = FALSE]
      })))
    }
    return(ratings)
  }

  if (grepl("get_team_four_factors_dynamic", q, fixed = TRUE) ||
      grepl("get_team_four_factors_direct", q, fixed = TRUE)) {
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
      dfl = c(42, 36),
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

  if (grepl("get_player_traditional_dynamic", q, fixed = TRUE) ||
      grepl("get_player_traditional_pergame", q, fixed = TRUE) ||
      grepl("get_player_traditional_from_games", q, fixed = TRUE) ||
      grepl("get_player_traditional_custom_clutch", q, fixed = TRUE)) {
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
      dfl = c(5, 3, 4),
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
      game_year = rep(2026L, 8),
      game_id = c(101L, 102L, 103L, 104L, 105L, 106L, 107L, 108L),
      team_id = c(rep(1L, 4), rep(2L, 4)),
      team_name = c(rep("Team A", 4), rep("Team B", 4)),
      opp_team_id = c(2L, 3L, 4L, 5L, 3L, 4L, 5L, 1L),
      gn = rep(1:4, 2),
      game_type = rep(c(1L, 1L, 1L, 2L), 2),
      game_date = rep(as.Date(c("2025-10-10", "2025-10-17", "2025-10-24", "2025-10-31")), 2),
      opp_team_name = c("Team B", "Team C", "Team D", "Team E",
                        "Team C", "Team D", "Team E", "Team A"),
      team_score = c(90L, 100L, 110L, 120L, 60L, 65L, 70L, 75L),
      opp_score = c(120L, 110L, 100L, 90L, 50L, 55L, 60L, 65L),
      has_won = c(FALSE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE, TRUE),
      is_home = c(TRUE, FALSE, TRUE, FALSE, TRUE, FALSE, TRUE, FALSE)
    ))
  }

  if (grepl("mv_lineup_totals_by_day", q, fixed = TRUE)) {
    return(data.frame(
      team_id = c(rep(1L, 8), rep(2L, 8)),
      lineup_hash = paste0("lu", seq_len(16)),
      type_lineup = rep(c("offense", "defense"), 8),
      g_date = rep(as.Date(c("2025-10-10", "2025-10-17", "2025-10-24", "2025-10-31")), each = 2, times = 2),
      game_id = rep(c(101L, 102L, 103L, 104L, 105L, 106L, 107L, 108L), each = 2),
      game_year = rep(2026L, 16),
      total_poss = rep(100, 16),
      total_pts = c(90, 120, 100, 110, 110, 100, 120, 90,
                    60, 50, 65, 55, 70, 60, 75, 65),
      fg2_made = c(20, 22, 21, 21, 23, 20, 24, 18, rep(15, 8)),
      fg2_att = c(35, 36, 34, 35, 36, 33, 37, 32, rep(40, 8)),
      fg3_made = c(8, 10, 9, 9, 10, 8, 11, 7, rep(5, 8)),
      fg3_att = c(24, 25, 23, 24, 25, 22, 26, 21, rep(20, 8)),
      num_starters = rep(5L, 16)
    ))
  }

  if (grepl("lineup_four_factors_by_game", q, fixed = TRUE)) {
    return(data.frame(
      lineup_hash = paste0("ff", seq_len(16)),
      team_id = c(rep(1L, 8), rep(2L, 8)),
      game_id = rep(c(101L, 102L, 103L, 104L, 105L, 106L, 107L, 108L), each = 2),
      game_year = rep(2026L, 16),
      type_lineup = rep(c("offense", "defense"), 8),
      total_points = c(90, 120, 100, 110, 110, 100, 120, 90,
                       60, 50, 65, 55, 70, 60, 75, 65),
      total_poss = rep(100, 16),
      ts_poss_count = c(90, 85, 88, 84, 86, 83, 84, 82, rep(80, 8)),
      oreb_count = c(8, 14, 10, 12, 12, 10, 14, 8,
                     2, 2, 3, 3, 4, 4, 5, 5),
      oreb_opportunities = rep(40, 16),
      tov_count = c(18, 10, 15, 11, 12, 12, 9, 13,
                    5, 5, 6, 6, 7, 7, 8, 8),
      total_ft_attempts = c(12, 18, 14, 17, 16, 15, 18, 14,
                            5, 5, 6, 6, 7, 7, 8, 8),
      total_fga = c(70, 75, 72, 74, 74, 73, 76, 72, rep(100, 8)),
      total_fgm = c(31, 36, 34, 35, 37, 33, 39, 31,
                    30, 25, 31, 26, 32, 27, 33, 28),
      total_fg3_made = c(8, 10, 9, 9, 10, 8, 11, 7, rep(0, 8)),
      num_starters = rep(5L, 16)
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
fmt_rank_cell <- function(value, rank_now, delta, digits = 1, show_delta = TRUE) as.character(round(as.numeric(value), digits = digits))

# Deliberate stubs over the real helpers.R versions (observer wiring and UI
# builders that testServer contexts don't need).
build_filter_chips <- function(...) shiny::tags$div(class = "filter-chips", "chips")
setup_chip_clears <- function(...) invisible(TRUE)
setup_stat_filter_handlers <- function(...) invisible(TRUE)
stat_filter_chips_ui <- function(...) list()
setup_gn_last_n_sync <- function(session, input, prefix) invisible(TRUE)
dt_col_order_init_callback <- function(...) {
  DT::JS("function(settings, json) {}")
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
  pending_nav <- shiny::reactiveVal(NULL)
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
    hub_storylines_ready_year = shiny::reactiveVal(NA_integer_),
    pending_ld_team = pending_ld_team,
    pending_gl_team = pending_gl_team,
    pending_compare_preset = pending_compare_preset,
    pending_nav = pending_nav
  )
}
