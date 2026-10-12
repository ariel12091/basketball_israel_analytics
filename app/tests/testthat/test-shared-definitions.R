library(testthat)

# docs/shared_definitions.md, rule R1: the database computes, R presents. A
# metric is not re-derived from the base action table inside an R SQL string;
# the stored function that owns it gets a parameter instead.
#
# This is a ratchet. The files below already query the base table and are
# listed in that document's deviation register (D11) as not yet audited. The
# list may only shrink. A new file here means a second implementation of
# something a stored function owns.

BASE_TABLE <- "df_pts_poss_lineups_longer_mv"
KNOWN_DIRECT_READERS <- c("global.R", "server_tab6_team_stats.R")

test_that("no new R file queries the base action table directly", {
  files <- list.files(repo_file("R"), pattern = "[.]R$")
  expect_gt(length(files), 20)
  reads <- files[vapply(files, function(f) grepl(BASE_TABLE, read_repo_txt("R", f), fixed = TRUE), logical(1))]
  expect_setequal(union(reads, KNOWN_DIRECT_READERS), KNOWN_DIRECT_READERS)
})

test_that("the ratchet list holds no file that has already been cleaned up", {
  for (f in KNOWN_DIRECT_READERS) {
    expect_true(grepl(BASE_TABLE, read_repo_txt("R", f), fixed = TRUE),
                info = paste(f, "no longer reads the base table: remove it from KNOWN_DIRECT_READERS and from D11"))
  }
})

test_that("the contract is where every instruction file says it is", {
  expect_true(file.exists(repo_file("..", "docs", "shared_definitions.md")))
  for (f in list(c("..", "CLAUDE.md"), c("..", "euroleague", "CLAUDE.md"))) {
    txt <- paste(readLines(do.call(repo_file, as.list(f)), warn = FALSE, encoding = "UTF-8"), collapse = "\n")
    expect_true(grepl("docs/shared_definitions.md", txt, fixed = TRUE), info = paste(f, collapse = "/"))
  }
})

test_that("Tab 3 takes pace minutes from the stored function, not from its own SQL", {
  src <- read_repo_txt("R", "server_tab3.R")
  expect_true(grepl("get_team_minutes_dynamic(", src, fixed = TRUE))
  expect_false(grepl("40 + 5 *", src, fixed = TRUE))
  expect_false(grepl("filtered_segments", src, fixed = TRUE))
})

test_that("Tab 3 rank arrows have one source: the stored readers with an end date", {
  src <- read_repo_txt("R", "server_tab3.R")
  # The removed blocks cut each team at its own previous game, from inline SQL.
  expect_false(grepl("_from_mv", src, fixed = TRUE))
  expect_false(grepl("team_metrics_by_game_mv", src, fixed = TRUE))
  expect_false(grepl("rn = 2", src, fixed = TRUE))
  for (fn in c("tr_prev_data", "tr_prev_ff_data", "tr_prev_traditional_data")) {
    expect_true(grepl(paste0(fn, "()"), src, fixed = TRUE), info = fn)
  }
})

test_that("player Traditional rows have one route: the shared reader chooser", {
  # get_player_traditional_dynamic measures minutes as an event span even with
  # no clutch window; Compare called it and showed other minutes than Player
  # Stats for 190 of 312 players. No app file may name it.
  files <- list.files(repo_file("R"), pattern = "[.]R$")
  named <- files[vapply(files, function(f) grepl("get_player_traditional_dynamic", read_repo_txt("R", f), fixed = TRUE), logical(1))]
  expect_identical(named, character(0))
  expect_true(grepl("fetch_player_traditional_filtered(", read_repo_txt("R", "server_tab7_compare.R"), fixed = TRUE))
})
