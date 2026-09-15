# The Israeli Player Stats readers, shared by Tab 5 and the On/Off tab's
# Player Stats filter chips (helpers.R).

test_that("the Israeli season reader normalises the Player Stats MV pull", {
  reset_mock_db_query_counts()
  out <- fetch_player_traditional_season_israel(pg_pool, "2026", "v1")
  expect_identical(mock_db_query_count("player_traditional_mv"), 1L)
  expect_true("Player" %in% names(out))
  expect_true(all(c("game_year", "team_id", "player_id", "gp", "pts") %in% names(out)))
})

test_that("the Israeli season reader fails to NULL, which cached_season_df never caches", {
  withr::local_options(ibpl.mock_player_traditional_error = TRUE)
  expect_null(fetch_player_traditional_season_israel(pg_pool, 2026L, "v1"))
})

test_that("the Israeli filtered reader issues one per-game read when no clutch window is set", {
  reset_mock_db_query_counts()
  out <- run_player_traditional_israel(
    pg_pool, session = NULL, guard_key = "test_reader",
    game_year = 2026L, start_d = as.Date("2025-11-01"), end_d = as.Date("2026-02-01"),
    team_ids_csv = NA_character_, game_type_csv = NA_character_, opp_ids_csv = NA_character_,
    home_away = NA_character_, outcome = NA_character_,
    opp_rank_side = NA_character_, opp_rank_n = NA_integer_, opp_rank_metric = NA_character_,
    max_margin = NA_integer_, margin_status = NA_character_,
    max_time_remaining = NA_integer_, ot_margin_filter = FALSE,
    min_gn = NA_integer_, max_gn = NA_integer_, last_n_games = NA_integer_
  )
  expect_identical(mock_db_query_count("player_traditional_reader"), 1L)
  expect_identical(nrow(out), 3L)
})

test_that("Tab 5 reads Israeli Player Stats through the shared readers", {
  txt <- read_repo_txt("R", "server_tab5_traditional.R")
  expect_match(txt, "fetch_player_traditional_season_israel(pg_pool, gy_int, ts_data_version())", fixed = TRUE)
  # Shared helpers take the pool as an argument (review remark 4).
  reader_src <- paste(deparse(fetch_player_traditional_season_israel), collapse = "\n")
  expect_false(grepl("pg_pool", reader_src, fixed = TRUE))
  expect_match(txt, 'guard_key = "tab5_player_traditional"', fixed = TRUE)
  expect_no_match(txt, "run_player_traditional_dynamic", fixed = TRUE)
  # The EuroLeague season pull keeps its competition-qualified key.
  expect_match(txt, 'list("euro_player_traditional_stats_mv", ts_competition(), gy_int, ts_data_version())', fixed = TRUE)
})
