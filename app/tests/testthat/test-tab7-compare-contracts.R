server_tab7_path <- repo_file("R", "server_tab7_compare.R")

server_tab7_txt <- function() {
  paste(readLines(server_tab7_path, warn = FALSE), collapse = "\n")
}

test_that("tab7 players compare guards against unloaded roster refs", {
  txt <- server_tab7_txt()
  expect_true(grepl("cmp_player_selection_state <- reactive\\(", txt))
  expect_true(grepl('req\\(!is\\.null\\(players_df\\), all\\(c\\("team_id", "player_id", "name"\\) %in% names\\(players_df\\)\\)\\)', txt))
  expect_true(grepl('req\\(!is\\.null\\(teams_df\\), all\\(c\\("team_id", "team_name"\\) %in% names\\(teams_df\\)\\)\\)', txt))
  expect_true(grepl("req\\(nrow\\(players_df\\) > 0\\)", txt))
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

test_that("tab7 player compare exposes defensive disruptions", {
  txt <- server_tab7_txt()
  expect_true(grepl('label = "Deflections", col = "dfl"', txt, fixed = TRUE))
  expect_true(grepl('label = "Disruptions", col = "disruptions"', txt, fixed = TRUE))
  expect_true(grepl('label = "Disruptions/100", col = "Def Disruptions/100 Diff"', txt, fixed = TRUE))
  expect_true(grepl('poss < 300', txt, fixed = TRUE))
})

test_that("tab7 team four-factor chips map to actual four-factor columns", {
  txt <- server_tab7_txt()
  expect_true(grepl('"eFG%"\\s*=\\s*"off_efg"', txt))
  expect_true(grepl('"TOV%"\\s*=\\s*"off_tov"', txt))
  expect_true(grepl('"OREB%"\\s*=\\s*"off_oreb"', txt))
  expect_true(grepl('metric %in% c\\("off_efg", "off_tov", "off_oreb", "off_ftr"\\)', txt))
  expect_false(grepl('"eFG%"\\s*=\\s*"off_efg_pct"', txt))
  expect_false(grepl('"TOV%"\\s*=\\s*"off_tov_pct"', txt))
  expect_false(grepl('"OREB%"\\s*=\\s*"off_oreb_pct"', txt))
  expect_false(grepl('metric %in% c\\("off_efg_pct", "off_tov_pct", "off_oreb_pct", "off_ftr"\\)', txt))
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

test_that("tab7 detail view registers Teams-only neutral shot-profile sections", {
  txt <- server_tab7_txt()
  expect_true(grepl("off_shot_profile = list", txt, fixed = TRUE))
  expect_true(grepl("def_shot_profile = list", txt, fixed = TRUE))
  # Teams gating includes both new sections
  expect_true(grepl('c("def_shooting", "off_shot_profile", "def_shot_profile")', txt, fixed = TRUE))
  # corner metric reads the known-denominator column, and no est/factor framing
  expect_true(grepl('col_ratings = "off_c3_pct3"', txt, fixed = TRUE))
  sp_block <- sub('.*off_shot_profile = list', "", txt)
  sp_block <- substr(sp_block, 1, regexpr("# --", sp_block, fixed = TRUE))
  expect_false(grepl("factor =", strsplit(sp_block, "PLAYER_VIEWS")[[1]][1], fixed = TRUE))
})

test_that("tab7 players mode registers the shot-profile view", {
  src <- paste(readLines(repo_file("R", "server_tab7_compare.R"), warn = FALSE), collapse = "\n")
  expect_true(grepl('"Shot Profile" = "shot_profile"', src, fixed = TRUE))
  expect_true(grepl("render_shot_profile_ui <- function", src, fixed = TRUE))
  expect_true(grepl('identical(view, "shot_profile")', src, fixed = TRUE))
  # neutral rows: the renderer must not use est/impact framing
  sp_fn <- sub(".*render_shot_profile_ui <- function", "", src)
  sp_fn <- strsplit(sp_fn, "# -- Overall PvP view --", fixed = TRUE)[[1]][1]
  expect_false(grepl("ff_impact", sp_fn, fixed = TRUE))
  expect_true(grepl("neutral = TRUE", sp_fn, fixed = TRUE))
})
