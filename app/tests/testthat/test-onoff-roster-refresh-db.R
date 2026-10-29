source(repo_file("..", "etl", "onoff_roster_refresh.R"), local = TRUE)

test_that("a retry recovers old OFF games even after the roster was committed", {
  skip_if_not(Sys.getenv("RUN_DB_TESTS", "0") == "1")
  skip_if_not_installed("RPostgres")
  keys <- c("PG_HOST", "PG_DB", "PG_USER", "PG_PASS")
  skip_if_not(all(nzchar(Sys.getenv(keys))))
  con <- DBI::dbConnect(RPostgres::Postgres(), host = Sys.getenv("PG_HOST"),
    port = 5432L, dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
    password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE", "require"))
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  # Temp relations only: production data is never touched by this fixture.
  DBI::dbExecute(con, "CREATE TEMP TABLE lineups_lookup (game_year int, game_id int, team_id int, player_id int, lineup_hash text, is_on_verdict int)")
  DBI::dbExecute(con, "CREATE TEMP TABLE schedule (game_year int, game_id int)")
  DBI::dbExecute(con, "CREATE TEMP TABLE df_pts_poss_lineups_longer_mv (game_id int, team_id int, credit_lineup_hash text)")
  DBI::dbExecute(con, "CREATE TEMP TABLE player_four_factors_by_game (game_id int, team_id int, player_id int, is_on_key int)")
  DBI::dbExecute(con, "INSERT INTO schedule VALUES (2026,1),(2027,393),(2027,413),(2027,414)")
  DBI::dbExecute(con, "INSERT INTO lineups_lookup VALUES
    (2026,1,6,100,'old',1), (2027,393,6,100,'same',1),
    (2027,413,6,100,'same',1), (2027,413,6,1384,'same',0),
    (2027,414,9,200,'other',1), (2027,414,9,300,'other',0)")
  DBI::dbExecute(con, "INSERT INTO df_pts_poss_lineups_longer_mv VALUES
    (1,6,'old'),(393,6,'same'),(413,6,'same'),(414,9,'other')")
  DBI::dbExecute(con, "INSERT INTO player_four_factors_by_game VALUES
    (1,6,100,1),(393,6,100,1),(413,6,100,1),(413,6,1384,0),(414,9,200,1)")
  roster <- onoff_roster_snapshot(con, "pg_temp")
  # The before/after roster is identical on retry; coverage still repairs it.
  expect_identical(onoff_refresh_games(con, "pg_temp", 413L, roster), c(393L, 413L))
  # A second team / other season with no processed game is left alone.
  expect_identical(onoff_missing_roster_games(con, "pg_temp", 413L), c(393L, 413L))
  DBI::dbExecute(con, "INSERT INTO player_four_factors_by_game VALUES (393,6,1384,0)")
  expect_identical(onoff_refresh_games(con, "pg_temp", 413L, roster), 413L)
  # Missing rows in the new game are handled by the ordinary refresh, without
  # needlessly refreshing historical games when its roster is unchanged.
  DBI::dbExecute(con, "DELETE FROM player_four_factors_by_game WHERE game_id = 413")
  expect_identical(onoff_refresh_games(con, "pg_temp", 413L, roster), 413L)
})
