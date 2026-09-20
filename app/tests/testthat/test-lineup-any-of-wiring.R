# Wiring contracts for the two-box Players On filter.
#
# lineup_on_predicate() is unit-tested directly in test-lineup-local-filters.R,
# but a correct helper proves nothing about whether its callers reach it. These
# pin the call sites -- especially Tab 7, whose Compare player pool cannot be
# driven from a scripted browser session on this build (true on main too, so
# not a regression), leaving it without live coverage.

count_fixed <- function(txt, needle) {
  m <- gregexpr(needle, txt, fixed = TRUE)[[1]]
  if (length(m) == 1L && m[[1]] == -1L) 0L else length(m)
}

test_that("Tab 2 sends every on-player plus the required subset", {
  txt <- read_repo_txt("R", "server_tab2.R")

  expect_true(grepl("player_on_ids <- c(player_req_ids, player_any_ids)", txt, fixed = TRUE))
  expect_true(grepl("player_required_csv = if (length(player_req_ids))", txt, fixed = TRUE))
  # bindEvent() suppresses automatic dependencies, so the any-of box has to be
  # an explicit trigger: ld_params(), its bindEvent, both auto-min observers,
  # and the chip bar.
  expect_equal(count_fixed(txt, "ld_lineup_filter$players_on_any()"), 5L)
})

test_that("Tab 2 still fetches the ranked population with no player filter", {
  txt <- read_repo_txt("R", "server_tab2.R")

  # Ranks are computed on the full population; the boxes only narrow the
  # already-ranked frame in R.
  expect_equal(count_fixed(txt, "player_csv = NA_character_"), 2L)
})

test_that("Tab 10 passes both boxes to the shared local filter", {
  txt <- read_repo_txt("R", "server_tab10_euro_lineups.R")

  expect_true(grepl("player_csv     = csv_if_any(c(ld_filter$players_on(), ld_filter$players_on_any()))", txt, fixed = TRUE))
  expect_true(grepl("player_required_csv = paste(ld_filter$players_on(), collapse = \",\")", txt, fixed = TRUE))
  expect_equal(count_fixed(txt, "ld_filter$players_on_any()"), 4L)
})

test_that("Tab 7 withholds the on-filter from SQL when an any-of box is used", {
  txt <- read_repo_txt("R", "server_tab7_compare.R")

  # fetch_lineups_csv_v2 has no "at least one of" predicate.
  expect_true(grepl("on_any <- isTRUE(lu$has_any)", txt, fixed = TRUE))
  expect_true(grepl("if (on_any) NA_character_ else lu$player_csv", txt, fixed = TRUE))
  expect_true(grepl("if (on_any) FALSE else lu$exact", txt, fixed = TRUE))

  # Both readers wrapped, or one view would silently ignore the any-of box.
  expect_equal(count_fixed(txt, "cmp_apply_on_boxes(run_compare_query("), 2L)
  # ... re-applied through the shared helper, not a second implementation.
  expect_true(grepl("df <- apply_local_lineup_filters(df, list(", txt, fixed = TRUE))
})

test_that("the shared module exposes the any-of box and keeps the boxes disjoint", {
  txt <- read_repo_txt("R", "mod_lineup_player_filter.R")

  expect_true(grepl("players_on_any = reactive(current_player_values(\"players_on_any\"))", txt, fixed = TRUE))
  # One handler per box, driven off the vector, rather than the six pairwise
  # observers three boxes would otherwise need. It narrows the OTHER boxes'
  # option pools, so a player already claimed is never offered twice.
  expect_true(grepl("PLAYER_BOXES <- c(\"players_on\", \"players_on_any\", \"players_off\")", txt, fixed = TRUE))
  expect_true(grepl("observeEvent(input[[box_id]], refresh_other_box_pools(box_id),", txt, fixed = TRUE))
  # A cleared multi-select reports NULL, which the default would swallow --
  # and then its players would never return to the other pools.
  expect_true(grepl("ignoreInit = TRUE, ignoreNULL = FALSE", txt, fixed = TRUE))
  # Both the live handler and the initial population go through the same
  # pool rule, or a team pivot could offer a player two boxes already hold.
  expect_equal(count_fixed(txt, "lineup_box_pool(choices, mine, taken)"), 2L)
  expect_equal(count_fixed(txt, "observeEvent(input$players_o"), 0L)
})
