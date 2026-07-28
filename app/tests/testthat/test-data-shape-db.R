library(testthat)

skip_if_not(Sys.getenv("RUN_DB_TESTS", "0") == "1")
skip_if_not_installed("DBI")
skip_if_not_installed("RPostgres")

pg_env <- c("PG_HOST", "PG_PORT", "PG_DB", "PG_USER", "PG_PASS")
missing_env <- pg_env[!nzchar(Sys.getenv(pg_env))]
if (length(missing_env)) {
  skip(paste("Missing DB env:", paste(missing_env, collapse = ", ")))
}

con <- DBI::dbConnect(
  RPostgres::Postgres(),
  host = Sys.getenv("PG_HOST"),
  port = as.integer(Sys.getenv("PG_PORT")),
  dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE", unset = "require")
)
on.exit(DBI::dbDisconnect(con), add = TRUE)

columns_for <- function(table_name) {
  DBI::dbGetQuery(
    con,
    "SELECT a.attname AS column_name
       FROM pg_catalog.pg_class c
       JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
       JOIN pg_catalog.pg_attribute a ON a.attrelid = c.oid
      WHERE n.nspname = 'basketball_test'
        AND c.relname = $1
        AND c.relkind IN ('r', 'v', 'm', 'f', 'p')
        AND a.attnum > 0
        AND NOT a.attisdropped
      ORDER BY a.attnum",
    params = list(table_name)
  )$column_name
}

expect_has_columns <- function(table_name, expected) {
  cols <- columns_for(table_name)
  missing <- setdiff(expected, cols)
  expect_equal(missing, character(0), info = paste(table_name, "missing:", paste(missing, collapse = ", ")))
}

expect_lacks_columns <- function(table_name, forbidden) {
  cols <- columns_for(table_name)
  hits <- intersect(forbidden, cols)
  expect_equal(hits, character(0), info = paste(table_name, "unexpected:", paste(hits, collapse = ", ")))
}

expect_has_rows <- function(sql, params = list()) {
  out <- DBI::dbGetQuery(con, sql, params = params)
  expect_true(isTRUE(out$has_rows[[1]]), info = sql)
}

test_that("live DB has the app-required shape for default MVs", {
  expect_has_columns("df_pts_poss_lineups_longer_mv", c(
    "event_elapsed_seconds", "clock_regression_seconds",
    "segment_start_elapsed_seconds", "segment_end_elapsed_seconds",
    "segment_seconds"
  ))

  expect_has_columns("onoff_default_mv", c(
    "Team", "Year", "First Name", "Last Name", "Net RTG Diff",
    "Off ON Diff", "Def ON Diff", "Off ON PPP", "Def ON PPP",
    "On Net RTG", "Off OFF PPP", "Def OFF PPP", "Off Net RTG",
    "ON Poss", "OFF Poss", "minutes", "player_id", "team_id"
  ))

  expect_has_columns("player_four_factors_by_game", c(
    "player_id", "team_id", "game_id", "game_year", "is_on_key",
    "type_lineup", "total_points", "total_poss", "ts_poss_count",
    "tov_count", "steal_count", "deflection_count",
    "player_ts_poss_count", "player_tov_count",
    "minutes",
    "fg2_made", "fg2_att", "fg3_made", "fg3_att", "onoff_minutes"
  ))

  # usg_pct was dropped 2026-07-15: no consumer read it (Tab 5/7 USG% is
  # computed independently in player_traditional paths) and its stored
  # values had drifted from the repo formula.
  expect_lacks_columns("player_four_factors_by_game", "usg_pct")

  expect_has_columns("player_advanced_stats_mv", c(
    "def_on_disruptions", "def_off_disruptions", "Def Disruptions/100 Diff"
  ))

  expect_has_columns("team_ppp_ratings_mv", c(
    "game_year", "team_id", "team_name", "off_ppp", "def_ppp",
    "net_rtg", "games_played", "wins", "losses", "off_poss", "def_poss"
  ))

  expect_has_columns("team_ratings_preset_cache", c(
    "preset_variant", "game_year", "team_id", "team_name",
    "off_ppp", "def_ppp", "net_rtg", "games_played", "wins", "losses",
    "off_poss", "def_poss", "rank_net_rtg", "rank_off_ppp", "rank_def_ppp",
    "off_fga", "off_layup_att", "off_dunk_att", "off_fg3_att",
    "off_c3_att", "off_c3_known_att",
    "def_fga", "def_layup_att", "def_dunk_att", "def_fg3_att",
    "def_c3_att", "def_c3_known_att", "refreshed_at"
  ))

  expect_has_columns("team_four_factors_mv", c(
    "team_id", "game_year", "team_name",
    "off_ts", "off_efg", "off_oreb", "off_tov", "off_ftr", "off_ppp", "off_poss",
    "def_ts", "def_efg", "def_oreb", "def_tov", "def_ftr", "def_ppp", "def_poss",
    "net_rtg"
  ))

  expect_has_columns("mv_lineup_totals_by_day", c(
    "team_id", "lineup_hash", "type_lineup", "game_id", "game_year",
    "num_starters", "opp_starters", "minutes"
  ))

  expect_has_columns("lineup_four_factors_by_game", c(
    "team_id", "lineup_hash", "type_lineup", "game_id", "game_year",
    "num_starters", "opp_starters", "minutes"
  ))

  expect_has_columns("team_metrics_by_game_mv", c(
    "team_id", "game_id", "game_year", "game_date",
    "off_minutes", "def_minutes", "pts", "reb", "ast", "stl", "blk", "dfl",
    "tov", "fgm", "fga", "3pm", "3pa", "ftm", "fta"
  ))

  expect_has_columns("player_traditional_stats_mv", c(
    "player_id", "team_id", "team_name", "player_name", "gp",
    "poss_on_floor", "minutes", "pts", "reb", "oreb", "dreb", "ast", "stl", "blk",
    "dfl", "tov", "fgm", "fga", "3pm", "3pa", "ftm", "fta",
    "fg_pct", "tp_pct", "ft_pct", "efg", "ts", "usg_pct"
  ))
})

test_that("live DB does not expose removed AST% experiment columns in app data sources", {
  forbidden <- c("ast_pct", "parameters_kind", "shooting_foul_ft_trips")
  for (table_name in c("player_traditional_stats_mv", "team_metrics_by_game_mv")) {
    expect_lacks_columns(table_name, forbidden)
  }
})

test_that("live DB functions used by render paths return app-required columns", {
  player_trad <- DBI::dbGetQuery(con, "SELECT * FROM basketball_test.get_player_traditional_dynamic($1::int4) LIMIT 0", params = list(2026L))
  expect_true(all(c("player_id", "team_id", "team_name", "player_name", "gp", "poss_on_floor", "minutes", "pts", "reb", "oreb", "dreb", "ast", "stl", "blk", "dfl", "fg_pct", "efg", "ts", "usg_pct") %in% names(player_trad)))
  expect_false("ast_pct" %in% names(player_trad))

  team_ff <- DBI::dbGetQuery(con, "SELECT * FROM basketball_test.get_team_four_factors_dynamic($1::int4) LIMIT 0", params = list(2026L))
  expect_true(all(c("team_id", "game_year", "team_name", "off_efg", "off_tov", "off_ppp", "def_efg", "def_tov", "def_ppp", "net_rtg") %in% names(team_ff)))

  player_ff <- DBI::dbGetQuery(con, "SELECT * FROM basketball_test.four_factors_compute($1::int4) LIMIT 0", params = list(2026L))
  expect_true(all(c("def_on_disruptions", "def_off_disruptions", "Def Disruptions/100 Diff") %in% names(player_ff)))

  team_ratings <- DBI::dbGetQuery(con, "SELECT * FROM basketball_test.get_team_ratings_dynamic($1::int4) LIMIT 0", params = list(2026L))
  expect_true(all(c("game_year", "team_id", "team_name", "off_ppp", "def_ppp", "net_rtg", "games_played", "off_poss", "def_poss") %in% names(team_ratings)))
})

test_that("live DB has data behind the main app tabs for current season", {
  expect_has_rows('SELECT EXISTS(SELECT 1 FROM basketball_test.onoff_default_mv WHERE "Year" = $1::int4 LIMIT 1) AS has_rows', list(2026L))
  expect_has_rows("SELECT EXISTS(SELECT 1 FROM basketball_test.team_ppp_ratings_mv WHERE game_year = $1::int4 LIMIT 1) AS has_rows", list(2026L))
  expect_has_rows("SELECT EXISTS(SELECT 1 FROM basketball_test.team_ratings_preset_cache WHERE game_year = $1::int4 LIMIT 1) AS has_rows", list(2026L))
  expect_has_rows("SELECT EXISTS(SELECT 1 FROM basketball_test.team_four_factors_mv WHERE game_year = $1::int4 LIMIT 1) AS has_rows", list(2026L))
  expect_has_rows("SELECT EXISTS(SELECT 1 FROM basketball_test.player_traditional_stats_mv WHERE game_year = $1::int4 LIMIT 1) AS has_rows", list(2026L))
  expect_has_rows("SELECT EXISTS(SELECT 1 FROM basketball_test.team_metrics_by_game_mv WHERE game_year = $1::int4 LIMIT 1) AS has_rows", list(2026L))
  expect_has_rows("SELECT EXISTS(SELECT 1 FROM basketball_test.final_schedule_mv WHERE game_year = $1::int4 LIMIT 1) AS has_rows", list(2026L))
})
