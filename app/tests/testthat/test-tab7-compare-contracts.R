server_tab7_path <- repo_file("R", "server_tab7_compare.R")

server_tab7_txt <- function() {
  paste(readLines(server_tab7_path, warn = FALSE), collapse = "\n")
}

test_that("tab7 players compare guards against unloaded roster refs", {
  txt <- server_tab7_txt()
  expect_true(grepl("req\\(!is\\.null\\(players_df\\), nrow\\(players_df\\) > 0\\)", txt))
})

test_that("tab7 clutch minutes are converted to seconds like tab3", {
  txt <- server_tab7_txt()
  expect_true(grepl("max_time_remaining <- if \\(clutch_on\\) as\\.integer\\(get_input\\(\"clutch_minutes\"\\)\\) \\* 60L else NA_integer_", txt))
})

test_that("tab7 players compare uses a single row for side-specific summaries", {
  txt <- server_tab7_txt()
  expect_true(grepl("rank\\s*=\\s*1L", txt))
  expect_true(grepl("entity_name\\s*=\\s*paste0\\(player_a_name, \" vs \", player_b_name\\)", txt))
  expect_false(grepl("rank\\s*=\\s*1:2", txt))
  expect_false(grepl("metric_a\\s*=\\s*c\\(val_a, val_b\\)", txt))
})

test_that("tab7 player metric mapping uses actual SQL column names", {
  txt <- server_tab7_txt()
  # SQL returns pts/reb/ast/stl (totals) and tp_pct/ts (not fg3_pct/ts_pct)
  expect_true(grepl('"ppg"\\s*=\\s*"pts"', txt))
  expect_true(grepl('"fg3_pct"\\s*=\\s*"tp_pct"', txt))
  expect_true(grepl('"ts_pct"\\s*=\\s*"ts"', txt))
  expect_true(grepl("poss_on_floor", txt))
  # Must NOT reference non-existent columns
  expect_false(grepl("pts_per_game", txt))
  expect_false(grepl("total_pts", txt))
})
