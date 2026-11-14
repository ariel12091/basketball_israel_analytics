source(repo_file("..", "etl", "onoff_roster_refresh.R"), local = TRUE)

test_that("a retry finds earlier games whose stored OFF rows the lookup no longer matches", {
  skip_if_not(Sys.getenv("RUN_DB_TESTS", "0") == "1")
  skip_if_not_installed("RPostgres")
  keys <- c("PG_HOST", "PG_DB", "PG_USER", "PG_PASS")
  skip_if_not(all(nzchar(Sys.getenv(keys))))
  con <- db_test_connect(RPostgres::Postgres(), host = Sys.getenv("PG_HOST"),
    port = 5432L, dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
    password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE", "require"))
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  # Temp relations only: production data is never touched by this fixture.
  DBI::dbExecute(con, "CREATE TEMP TABLE lineups_lookup (game_id int, team_id int, player_id int, lineup_hash text, is_on_verdict int)")
  DBI::dbExecute(con, "CREATE TEMP TABLE schedule (game_year int, game_id int)")
  DBI::dbExecute(con, "CREATE TEMP TABLE df_pts_poss_lineups_longer_mv (game_id int, team_id int, credit_lineup_hash text, final_end_poss boolean, team_score int)")
  DBI::dbExecute(con, "CREATE TEMP TABLE player_four_factors_by_game (game_id int, team_id int, player_id int, is_on_key int, total_poss bigint, total_points numeric)")
  DBI::dbExecute(con, "INSERT INTO schedule VALUES (2026,1),(2027,393),(2027,413),(2027,414)")
  # Team 6 played lineup A in 1 (last season), 393 and 413. Player 1384 is
  # paired with A only from game 413's lookup rows.
  DBI::dbExecute(con, "INSERT INTO lineups_lookup VALUES
    (1,6,100,'A',1), (393,6,100,'A',1), (413,6,100,'A',1), (413,6,1384,'A',0),
    (414,9,200,'C',1)")
  DBI::dbExecute(con, "INSERT INTO df_pts_poss_lineups_longer_mv VALUES
    (1,6,'A',TRUE,2), (393,6,'A',TRUE,2), (393,6,'A',TRUE,0), (413,6,'A',TRUE,3), (414,9,'C',TRUE,1)")
  # Game 393 was refreshed before 1384's pairing existed: no OFF row for him.
  DBI::dbExecute(con, "INSERT INTO player_four_factors_by_game VALUES
    (1,6,100,1,1,2), (393,6,100,1,2,2), (413,6,100,1,1,3), (413,6,1384,0,1,3), (414,9,200,1,1,1)")
  roster <- onoff_roster_snapshot(con, "pg_temp")
  # Same snapshot before and after (a retry): only the stored rows can tell.
  expect_identical(onoff_stale_games(con, "pg_temp", 413L), 393L)
  expect_identical(onoff_refresh_games(con, "pg_temp", 413L, roster), c(393L, 413L))
  # Other seasons and teams that played no processed game are left alone.
  expect_identical(onoff_stale_games(con, "pg_temp", 414L), integer(0))
  # A partial row (fewer possessions than the lineup played) is stale too.
  DBI::dbExecute(con, "INSERT INTO player_four_factors_by_game VALUES (393,6,1384,0,1,2)")
  expect_identical(onoff_stale_games(con, "pg_temp", 413L), 393L)
  # Once it matches, only the processed game is refreshed.
  DBI::dbExecute(con, "UPDATE player_four_factors_by_game SET total_poss = 2 WHERE game_id = 393 AND player_id = 1384")
  expect_identical(onoff_refresh_games(con, "pg_temp", 413L, roster), 413L)
})
