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
KNOWN_DIRECT_READERS <- c("global.R", "server_tab3.R", "server_tab6_team_stats.R")

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
