server_tab7_path <- repo_file("R", "server_tab7_compare.R")

server_tab7_txt <- function() {
  paste(readLines(server_tab7_path, warn = FALSE), collapse = "\n")
}

test_that("tab7 players compare guards against unloaded roster refs", {
  txt <- server_tab7_txt()
  expect_true(grepl("cmp_player_selection_state <- reactive\\(", txt))
  expect_true(grepl("req\\(!is\\.null\\(players_df\\), nrow\\(players_df\\) > 0\\)", txt))
})

test_that("tab7 clutch minutes are converted to seconds like tab3", {
  txt <- server_tab7_txt()
  expect_true(grepl("max_time_remaining <- if \\(clutch_on\\) as\\.integer\\(get_input\\(\"clutch_minutes\"\\)\\) \\* 60L else NA_integer_", txt))
})

test_that("tab7 players compare uses a single row for side-specific summaries", {
  txt <- server_tab7_txt()
  expect_true(grepl("rank\\s*=\\s*1L", txt))
  expect_true(grepl("entity_name\\s*=\\s*paste0\\(player_state\\$name_a, \" vs \", player_state\\$name_b\\)", txt))
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

test_that("tab7 team four-factor chips map to actual four-factor columns", {
  txt <- server_tab7_txt()
  expect_true(grepl('"TS%"\\s*=\\s*"off_ts"', txt))
  expect_true(grepl('"TOV%"\\s*=\\s*"off_tov"', txt))
  expect_true(grepl('"OREB%"\\s*=\\s*"off_oreb"', txt))
  expect_true(grepl('metric %in% c\\("off_ts", "off_tov", "off_oreb", "off_ftr"\\)', txt))
  expect_false(grepl('"TS%"\\s*=\\s*"off_ts_pct"', txt))
  expect_false(grepl('"TOV%"\\s*=\\s*"off_tov_pct"', txt))
  expect_false(grepl('"OREB%"\\s*=\\s*"off_oreb_pct"', txt))
  expect_false(grepl('metric %in% c\\("off_ts_pct", "off_tov_pct", "off_oreb_pct", "off_ftr"\\)', txt))
})

test_that("tab7 compare table uses short side labels with custom badge header callback", {
  txt <- server_tab7_txt()
  expect_true(grepl('side_a_label\\s*<-\\s*side_label_short\\("a"\\)', txt))
  expect_true(grepl('side_b_label\\s*<-\\s*side_label_short\\("b"\\)', txt))
  expect_true(grepl("headerCallback = DT::JS", txt, fixed = TRUE))
  expect_true(grepl("if \\(aText === 'A'\\)", txt))
  expect_true(grepl("if \\(bText === 'B'\\)", txt))
})

test_that("tab7 detail view keeps full labels in subheader and short labels in columns", {
  txt <- server_tab7_txt()
  expect_true(grepl('short_a\\s*<-\\s*side_label_short\\("a"\\)', txt))
  expect_true(grepl('short_b\\s*<-\\s*side_label_short\\("b"\\)', txt))
  expect_true(grepl('full_a\\s*<-\\s*side_label_full\\("a"\\)', txt))
  expect_true(grepl('full_b\\s*<-\\s*side_label_full\\("b"\\)', txt))
  expect_true(grepl('col_a_text\\s*<-\\s*if \\(identical\\(short_a, "A"\\)\\) "A" else paste0\\("A \\\\u00b7 ", short_a\\)', txt))
  expect_true(grepl('col_b_text\\s*<-\\s*if \\(identical\\(short_b, "B"\\)\\) "B" else paste0\\("B \\\\u00b7 ", short_b\\)', txt))
  expect_true(grepl('paste0\\(full_a, " vs ", full_b, " \\\\u00b7 ", gy, "-", as.integer\\(substr\\(gy, 3, 4\\)\\) \\+ 1\\)', txt))
})
