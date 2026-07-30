test_that("bookmark exclusion drops actions, heartbeats and DT internals", {
  ids <- c(
    "game_year", "main_tabs", "teams", "ld_minposs", "ld_lineup_filter-team",
    "go_onoff", "go_lineups", "go_team", "go_gamelogs", "go_playerstats", "go_compare",
    "open_glossary", "ld_reset", "cmp_reset",
    "idle_activity_ts", "hub_remembered_team", "ibpl_restore_state",
    "ld_lineup_click", "cmp_table_row_click",
    "ld_table_rows_current", "ld_table_rows_all", "ld_table_rows_selected",
    "ld_table_state", "ld_table_search", "ld_table_cell_clicked",
    "ld_table_row_last_clicked", "ld_table_columns_selected", "ld_table_cells_selected",
    "ld_table_search_columns"
  )

  excluded <- bookmark_excluded_ids(ids)

  # kept: real filter state
  expect_false("game_year" %in% excluded)
  expect_false("main_tabs" %in% excluded)
  expect_false("teams" %in% excluded)
  expect_false("ld_minposs" %in% excluded)
  expect_false("ld_lineup_filter-team" %in% excluded)

  # dropped: everything that is an action, a heartbeat, or DT bookkeeping
  expect_true(all(c(
    "go_onoff", "go_compare", "open_glossary", "ld_reset", "cmp_reset",
    "idle_activity_ts", "hub_remembered_team", "ibpl_restore_state",
    "ld_lineup_click", "cmp_table_row_click",
    "ld_table_rows_current", "ld_table_rows_all", "ld_table_rows_selected",
    "ld_table_state", "ld_table_search", "ld_table_cell_clicked",
    "ld_table_row_last_clicked", "ld_table_columns_selected",
    "ld_table_cells_selected", "ld_table_search_columns"
  ) %in% excluded))
})

test_that("bookmark exclusion handles empty and NULL input safely", {
  expect_identical(bookmark_excluded_ids(character(0)), character(0))
  expect_identical(bookmark_excluded_ids(NULL), character(0))
})
