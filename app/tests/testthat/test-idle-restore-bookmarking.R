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

fake_restore_session <- function(query_string) {
  ctx <- shiny:::RestoreContext$new(query_string)
  list(restoreContext = ctx)
}

test_that("restored_input_value reads saved values even after they were used", {
  s <- fake_restore_session('?_inputs_&teams=%5B%224%22%2C%227%22%5D&ld_minposs=120')

  expect_equal(as.character(restored_input_value(s, "teams")), c("4", "7"))
  # second read must still work: restoreInput() marks values used, so the
  # helper has to force the read
  expect_equal(as.character(restored_input_value(s, "teams")), c("4", "7"))
  expect_equal(as.character(restored_input_value(s, "ld_minposs")), "120")
})

test_that("restored_input_value falls back to the default", {
  s <- fake_restore_session('?_inputs_&teams=%5B%224%22%5D')
  expect_identical(restored_input_value(s, "missing_id"), character(0))
  expect_identical(restored_input_value(s, "missing_id", "fallback"), "fallback")

  no_ctx <- list(restoreContext = NULL)
  expect_identical(restored_input_value(no_ctx, "teams"), character(0))
})

test_that("restore_aware_selection prefers current, falls back to restored, filters to choices", {
  s <- fake_restore_session('?_inputs_&teams=%5B%224%22%2C%229%22%5D')
  choices <- c("Hapoel" = "4", "Maccabi" = "7")

  # no current selection -> use restored, dropping ids absent from choices
  expect_equal(restore_aware_selection(s, "teams", character(0), choices), "4")
  # current selection wins
  expect_equal(restore_aware_selection(s, "teams", "7", choices), "7")
  # current selection not in choices -> empty, not a stale restore
  expect_equal(restore_aware_selection(s, "teams", "99", choices), character(0))
  # no restore context and no current -> empty
  expect_equal(
    restore_aware_selection(list(restoreContext = NULL), "teams", character(0), choices),
    character(0)
  )
})

test_that("app enables url bookmarking and pushes urls without touching the address bar", {
  app_r_txt <- read_repo_txt("app.R")

  expect_match(app_r_txt, 'enableBookmarking(store = "url")', fixed = TRUE)
  expect_match(app_r_txt, "session$doBookmark()", fixed = TRUE)
  expect_match(app_r_txt, 'onBookmarked(function(url)', fixed = TRUE)
  expect_match(app_r_txt, '"ibpl_bookmark_url"', fixed = TRUE)
  expect_match(app_r_txt, "setBookmarkExclude(", fixed = TRUE)

  # the bookmark must never be written into the browser address bar
  expect_false(grepl("updateQueryString", app_r_txt, fixed = TRUE))
})

test_that("choice-populating observers preserve restored selections", {
  app_r_txt <- read_repo_txt("app.R")
  tab3_txt <- read_repo_txt("R", "server_tab3.R")
  tab4_txt <- read_repo_txt("R", "server_tab4.R")
  tab5_txt <- read_repo_txt("R", "server_tab5_traditional.R")
  tab7_txt <- read_repo_txt("R", "server_tab7_compare.R")
  mod_txt  <- read_repo_txt("R", "mod_lineup_player_filter.R")

  # the startup population path must not hard-reset selections any more
  expect_false(grepl(
    'updateSelectizeInput(session, "teams", choices = team_choices, selected = character(0)',
    app_r_txt, fixed = TRUE
  ))
  expect_match(app_r_txt, "restore_aware_selection(", fixed = TRUE)

  for (txt in list(tab3_txt, tab4_txt, tab5_txt, tab7_txt)) {
    expect_match(txt, "restore_aware_selection(", fixed = TRUE)
  }

  # the lineup module already intersects selections with real choices
  expect_match(mod_txt, "selected_in_choices(input$players_on, choices)", fixed = TRUE)
  expect_match(mod_txt, "restore_aware_selection(", fixed = TRUE)
})

test_that("browser stores bookmark urls and restores by navigation", {
  js <- read_repo_txt("www", "app.js")

  expect_match(js, '"ibpl_bookmark_url"', fixed = TRUE)
  expect_match(js, "window.location.replace(url)", fixed = TRUE)
  expect_match(js, "ibpl_v", fixed = TRUE)

  # the replay engine is gone
  for (dead in c(
    "persistIds", "sendRestoreState", "attemptRestoreSend", "applyRestoreValues",
    "reapplyDependentPlayerInputs", "requestRestoreFinish", "ibpl_restore_applied",
    "restoreMaxSendAttempts", "restoreTabQueryParam"
  )) {
    expect_false(grepl(dead, js, fixed = TRUE), info = dead)
  }
})

test_that("restore triggers on user return, never on expiry itself", {
  js <- read_repo_txt("www", "app.js")

  expect_match(js, "function restoreOnReturn()", fixed = TRUE)
  expect_match(js, "idleExpired = true;", fixed = TRUE)
  # expiry marks state and shows the pill; it must not navigate
  expect_match(js, "showPausedPill();", fixed = TRUE)
})

test_that("a visible disconnect stays paused until later user activity", {
  js <- read_repo_txt("www", "app.js")

  disconnect_start <- regexpr("function handleDisconnected()", js, fixed = TRUE)[[1]]
  handler_tail <- substring(js, disconnect_start)
  disconnect_end <- regexpr(
    "function registerMessageHandlers()", handler_tail, fixed = TRUE
  )[[1]]
  disconnect_handler <- substring(handler_tail, 1L, disconnect_end - 1L)

  expect_match(disconnect_handler, "showPausedPill();", fixed = TRUE)
  expect_false(grepl("restoreOnReturn();", disconnect_handler, fixed = TRUE))
  expect_match(
    js,
    "if (shouldRestoreFromPausedEvent(event)) restoreOnReturn();",
    fixed = TRUE
  )
})

test_that("paused controls stay reachable before deliberate resume", {
  js <- read_repo_txt("www", "app.js")

  expect_match(js, "function shouldRestoreFromPausedEvent(event)", fixed = TRUE)
  expect_match(js, 'event.type === "mousemove"', fixed = TRUE)
  expect_match(js, 'event.type === "keydown" && event.key === "Tab"', fixed = TRUE)
  expect_match(js, 'target.closest("#ibpl-idle-pill")', fixed = TRUE)
  expect_match(js, "shouldRestoreFromPausedEvent(event)", fixed = TRUE)
  expect_match(js, 'id="ibpl-idle-resume"', fixed = TRUE)
  expect_match(js, 'id="ibpl-idle-fresh"', fixed = TRUE)
})

test_that("the replay machinery is gone from R", {
  app_r_txt <- read_repo_txt("app.R")
  helpers_txt <- read_repo_txt("R", "helpers.R")

  for (dead in c(
    "restore_state_values", "ibpl_restore_state", "pending_ld_lineup_restore",
    "restore_selectize_ids", "restore_radio_ids", "restore_date_range_ids",
    "ibpl_restore_tab_from_query", "ibpl_restore_applied"
  )) {
    expect_false(grepl(dead, app_r_txt, fixed = TRUE), info = dead)
  }
  expect_false(grepl("ibpl_restore_tab_from_query", helpers_txt, fixed = TRUE))
})

test_that("home storylines stay gated when a bookmark restores another tab", {
  app_r_txt <- read_repo_txt("app.R")
  hub_txt <- read_repo_txt("R", "mod_team_hub.R")

  expect_match(app_r_txt, "startup_restore_pending", fixed = TRUE)
  expect_match(app_r_txt, 'restored_input_value(session, "main_tabs")', fixed = TRUE)
  expect_match(hub_txt, "suspendWhenHidden = TRUE", fixed = TRUE)
})
