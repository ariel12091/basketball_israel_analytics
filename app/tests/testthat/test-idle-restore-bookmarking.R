test_that("bookmark exclusion drops actions, heartbeats and DT internals", {
  ids <- c(
    "game_year", "main_tabs", "teams", "ld_minposs", "ld_lineup_filter-team",
    "go_onoff", "go_lineups", "go_team", "go_gamelogs", "go_playerstats", "go_compare",
    "open_glossary", "ld_reset", "cmp_reset",
    "idle_activity_ts", "hub_remembered_team", "home_set_default",
    "ibpl_restore_state",
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
    "idle_activity_ts", "hub_remembered_team", "home_set_default",
    "ibpl_restore_state",
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
  list(restoreContext = ctx, request = list(QUERY_STRING = query_string))
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

  no_ctx <- list(restoreContext = NULL, request = list(QUERY_STRING = ""))
  expect_identical(restored_input_value(no_ctx, "teams"), character(0))
})

test_that("restored_input_value survives a consumed or missing native context", {
  s <- list(
    restoreContext = NULL,
    request = list(
      QUERY_STRING = "_inputs_&main_tabs=%22euro%22&euro_teams=%5B%221%22%5D"
    )
  )

  expect_equal(as.character(restored_input_value(s, "euro_teams")), "1")
})

test_that("restored_input_value resolves module namespaces", {
  s <- fake_restore_session(paste0(
    "?_inputs_&ld_lineup_filter-team=4",
    "&ld_lineup_filter-players_on=%5B%22101%22%2C%22102%22%5D",
    "&ld_lineup_filter-players_off=%5B%22201%22%5D"
  ))
  s$ns <- function(id) paste0("ld_lineup_filter-", id)

  expect_equal(as.character(restored_input_value(s, "team")), "4")
  expect_equal(
    as.character(restored_input_value(s, "players_on")),
    c("101", "102")
  )
  expect_equal(as.character(restored_input_value(s, "players_off")), "201")
})

test_that("bookmark requests are distinguished from ordinary cached UI requests", {
  expect_true(is_bookmark_request(list(
    QUERY_STRING = "_inputs_&main_tabs=%22euro_team%22&league_select=%22E%22"
  )))
  expect_true(is_bookmark_request(list(QUERY_STRING = "?_inputs_&main_tabs=home")))
  expect_false(is_bookmark_request(list(QUERY_STRING = "")))
  expect_false(is_bookmark_request(list(QUERY_STRING = "foo=_inputs_")))
  expect_false(is_bookmark_request(list()))
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
  euro_global <- read_repo_txt("R", "global_euro.R")
  euro_lineups <- read_repo_txt("R", "server_tab10_euro_lineups.R")

  # the startup population path must not hard-reset selections any more
  expect_false(grepl(
    'updateSelectizeInput(session, "teams", choices = team_choices, selected = character(0)',
    app_r_txt, fixed = TRUE
  ))
  expect_match(app_r_txt, "update_restore_aware_selectize(", fixed = TRUE)

  for (txt in list(tab3_txt, tab4_txt, tab5_txt, tab7_txt)) {
    expect_match(txt, "restore_aware_selection(", fixed = TRUE)
  }

  # the lineup module carries native restore seeds through dependent choices
  expect_match(mod_txt, "restore_seed <- new.env(", fixed = TRUE)
  expect_match(mod_txt, 'restored_input_value(session, "team")', fixed = TRUE)
  expect_match(mod_txt, "selection_with_restore_seed(", fixed = TRUE)

  # EuroLeague choice rebuilds use the same restore bridges as Israel.
  expect_match(euro_global, "update_restore_aware_selectize(", fixed = TRUE)
  expect_match(
    euro_global,
    'restore_once_selection(\n      session, "euro_game_year"',
    fixed = TRUE
  )
  expect_match(euro_lineups, 'session, "euro_ld_opponents"', fixed = TRUE)
  expect_match(euro_lineups, 'session, "euro_ld_phase"', fixed = TRUE)

  expect_match(app_r_txt, "update_restore_aware_selectize(session, input, id, team_choices)", fixed = TRUE)
})

test_that("restored tab choice observers run initially and restore lineup players", {
  tab2_txt <- read_repo_txt("R", "server_tab2.R")
  tab3_txt <- read_repo_txt("R", "server_tab3.R")
  mod_txt <- read_repo_txt("R", "mod_lineup_player_filter.R")

  expect_match(
    tab2_txt,
    "observeEvent(list(input$main_tabs, input$game_year), ignoreInit = FALSE",
    fixed = TRUE
  )
  expect_match(
    tab3_txt,
    "observeEvent(list(input$game_year, input$main_tabs), ignoreInit = FALSE",
    fixed = TRUE
  )
  expect_match(mod_txt, "restore_seed$players_on", fixed = TRUE)
  expect_match(mod_txt, "restore_seed$players_off", fixed = TRUE)
  expect_match(
    tab2_txt,
    "refresh_player_choices(team_value = selected_team)",
    fixed = TRUE
  )
})

test_that("lineup restore survives the dependent-choice client round trip", {
  query <- paste0(
    "?_inputs_&ld_lineup_filter-team=4",
    "&ld_lineup_filter-players_on=%5B101%2C102%5D",
    "&ld_lineup_filter-players_off=%5B201%5D"
  )
  mock_session <- shiny:::MockShinySession$new()
  mock_session$restoreContext <- shiny:::RestoreContext$new(query)
  players_rx <- reactive(data.frame(
    team_id = c(4L, 4L, 4L),
    player_id = c(101L, 102L, 201L),
    name = c("Player A", "Player B", "Player C")
  ))

  testServer(
    lineup_player_filter_server,
    args = list(
      id = "ld_lineup_filter",
      players_ref = players_rx
    ),
    session = mock_session,
    {
      team_choices <- c("All teams" = "", "Team 4" = "4")
      selected_team <- update_team_choices(team_choices)

      # updateSelectizeInput() is asynchronous. The module must retain the
      # validated team long enough to populate its dependent roster.
      expect_equal(selected_team, "4")

      restored <- refresh_player_choices(team_value = selected_team)
      expect_equal(restored$team, "4")
      expect_equal(restored$players_on, c("101", "102"))
      expect_equal(restored$players_off, "201")

      # Once the browser confirms the restored values, later user clears must
      # not fall back to the old restore seed.
      session$setInputs(
        team = "4",
        players_on = c("101", "102"),
        players_off = "201"
      )
      session$setInputs(team = "", players_on = NULL, players_off = NULL)
      expect_equal(current_team_value(), "")
      expect_equal(current_player_values("players_on"), character(0))
      expect_equal(current_player_values("players_off"), character(0))
    }
  )
})

# updateSelectizeInput() validates its session, so capture has to happen on a
# real MockShinySession rather than a plain list.
fake_capture_session <- function(query_string) {
  sent <- new.env(parent = emptyenv())
  sent$msgs <- list()
  s <- shiny:::MockShinySession$new()
  s$restoreContext <- shiny:::RestoreContext$new(query_string)
  s$sendInputMessage <- function(inputId, message) {
    sent$msgs[[length(sent$msgs) + 1L]] <-
      paste0(inputId, "=", paste(message$value, collapse = ","))
    invisible(NULL)
  }
  list(sent = sent, session = s)
}

test_that("restore_once_selection applies a bookmarked value exactly once", {
  s <- fake_restore_session('?_inputs_&gl_team=%224%22')
  s$userData <- new.env(parent = emptyenv())
  s$ns <- function(id) id
  choices <- c("All" = "", "Hapoel" = "4", "Maccabi" = "7")

  expect_equal(restore_once_selection(s, "gl_team", NULL, choices), "4")
  # a later rebuild must respect the cleared input instead of resurrecting it
  expect_equal(restore_once_selection(s, "gl_team", NULL, choices), character(0))
  # an explicit current value always wins
  expect_equal(restore_once_selection(s, "gl_team", "7", choices), "7")
})

test_that("EuroLeague date ranges restore once before later season resets", {
  s <- fake_restore_session(paste0(
    '?_inputs_&euroteam_dates=',
    '%5B%222025-11-01%22%2C%222026-02-01%22%5D'
  ))
  s$userData <- new.env(parent = emptyenv())
  bounds <- list(start = as.Date("2025-09-01"), end = as.Date("2026-07-01"))

  expect_equal(
    restore_once_date_range(
      s, "euroteam_dates",
      list(start = as.Date("2024-09-01"), end = as.Date("2025-07-01"))
    ),
    as.Date(c("2024-09-01", "2025-07-01"))
  )

  expect_equal(
    restore_once_date_range(s, "euroteam_dates", bounds),
    as.Date(c("2025-11-01", "2026-02-01"))
  )
  expect_equal(
    restore_once_date_range(s, "euroteam_dates", bounds),
    as.Date(c("2025-09-01", "2026-07-01"))
  )
})

test_that("GN and last-N rebuilds seed from the bookmark once, then clear", {
  h <- fake_capture_session('?_inputs_&ld_gn_min=%223%22&ld_last_n=%225%22')

  update_gn_last_n_choices(h$session, "ld", 1:6)
  expect_true(all(c("ld_gn_min=3", "ld_gn_max=", "ld_last_n=5") %in% h$sent$msgs))

  # season change / tab re-entry keeps the historical clearing behaviour
  h$sent$msgs <- list()
  update_gn_last_n_choices(h$session, "ld", 1:6)
  expect_true(all(c("ld_gn_min=", "ld_gn_max=", "ld_last_n=") %in% h$sent$msgs))
})

test_that("tab observers that own restore bridges run on the initial flush", {
  # server_tab3.R writes the pair in the opposite order; either is fine.
  for (f in c("server_tab2.R", "server_tab3.R", "server_tab4.R",
              "server_tab7_compare.R")) {
    txt <- read_repo_txt("R", f)
    expect_match(
      txt,
      "observeEvent\\(list\\(input\\$(main_tabs, input\\$game_year|game_year, input\\$main_tabs)\\), ignoreInit = FALSE",
      info = f, all = FALSE
    )
  }

  # Player Stats is shared by both league sections, so its initial bridge also
  # listens to the league and the two league-specific season selectors.
  txt <- read_repo_txt("R", "server_tab5_traditional.R")
  expect_match(
    txt,
    "observeEvent(list(input$main_tabs, input$league_select, input$game_year,",
    fixed = TRUE
  )
  expect_match(txt, "input$euro_game_year), ignoreInit = FALSE", fixed = TRUE)
})

test_that("compare restores its player pair and lineup filter", {
  txt <- read_repo_txt("R", "server_tab7_compare.R")

  # cmp_player_a/b are populated by refresh_player_choices(side), which reads a
  # still-blank input during a restored startup.
  expect_match(
    txt, "restore_once_selection(session, player_id, keep_val, choice_values)",
    fixed = TRUE
  )

  # the lineup module must not be seeded through reset_inputs(): that clears the
  # restore seed before anything can read it
  expect_match(txt, "cmp_lu_filter$update_team_choices(", fixed = TRUE)
  expect_match(
    txt, "cmp_lu_filter$refresh_player_choices(team_value = selected_lu_team)",
    fixed = TRUE
  )
  init_block <- substring(
    txt, regexpr("lu_team_choices <- if (nrow(teams_df))", txt, fixed = TRUE)[[1]]
  )
  init_block <- substring(init_block, 1L, 600L)
  expect_false(grepl("cmp_lu_filter$reset_inputs(team_choices", init_block, fixed = TRUE))
})

test_that("bookmark params survive until shiny has created the session", {
  js <- read_repo_txt("www", "app.js")

  # The server restore context comes from .clientdata_url_search, which shiny
  # reads at init. Stripping the query string earlier disables restoration for
  # every server-populated choice.
  expect_match(js, "function scheduleBookmarkParamCleanup()", fixed = TRUE)
  expect_match(js, '"shiny:sessioninitialized"', fixed = TRUE)
  expect_match(js, "scheduleBookmarkParamCleanup();", fixed = TRUE)

  load_tail <- substring(js, regexpr("window.ibplDebugSavedSession", js, fixed = TRUE)[[1]])
  expect_false(grepl("\n  clearBookmarkParams();", js, fixed = TRUE))
  expect_false(grepl("clearBookmarkParams();", load_tail, fixed = TRUE))
})

test_that("no input is sent before shiny's init message", {
  js <- read_repo_txt("www", "app.js")

  # shiny:connected fires inside socket.onopen, *before* shiny sends init, and
  # an event-priority setInputValue() is flushed synchronously. That input then
  # becomes the first message the server sees, and shiny builds the session's
  # restore context from the first message's .clientdata_url_search -- absent on
  # an update -- so every bookmark restore silently dies.
  connected_at <- regexpr("function handleConnected()", js, fixed = TRUE)[[1]]
  expect_gt(connected_at, 0L)
  tail_txt <- substring(js, connected_at)
  body <- substring(
    tail_txt, 1L,
    regexpr("function handleSessionInitialized()", tail_txt, fixed = TRUE)[[1]] - 1L
  )
  expect_false(grepl("setInputValue", body, fixed = TRUE))
  expect_false(grepl("sendActivity", body, fixed = TRUE))

  # the sends live behind the session-initialized gate instead
  expect_match(js, "function handleSessionInitialized()", fixed = TRUE)
  expect_match(js, "sessionReady = true;", fixed = TRUE)
  expect_match(js, "if (!sessionReady) return;", fixed = TRUE)
  expect_match(
    js,
    'window.jQuery(document).one("shiny:sessioninitialized", handleSessionInitialized)',
    fixed = TRUE
  )
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

test_that("bookmark startup cannot overwrite the last valid restore URL", {
  js <- read_repo_txt("www", "app.js")
  app <- read_repo_txt("app.R")

  expect_match(
    js,
    'var loadedFromBookmark = location.search.indexOf("_inputs_") !== -1;',
    fixed = TRUE
  )
  expect_match(js, "var bookmarkCaptureArmed = !loadedFromBookmark;", fixed = TRUE)
  expect_match(js, "function armBookmarkCaptureFromUserEvent(event)", fixed = TRUE)
  expect_match(js, 'if (event.type === "mousemove") return;', fixed = TRUE)
  expect_match(
    js,
    "if (msg && msg.url && bookmarkCaptureArmed)",
    fixed = TRUE
  )
  expect_match(js, "armBookmarkCaptureFromUserEvent(event);", fixed = TRUE)
  expect_match(app, "IBPL_RESTORE_STATE_VERSION <- 13L", fixed = TRUE)
})

test_that("restore triggers on user return, never on expiry itself", {
  js <- read_repo_txt("www", "app.js")

  expect_match(js, "function restoreOnReturn()", fixed = TRUE)
  expect_match(js, "idleExpired = true;", fixed = TRUE)
  # expiry marks state and shows the pill; it must not navigate
  expect_match(js, "showPausedPill();", fixed = TRUE)
})

test_that("foreground return detects timeout before resetting activity", {
  js <- read_repo_txt("www", "app.js")

  handler_start <- regexpr(
    "function handleVisibilityChange()", js, fixed = TRUE
  )[[1]]
  handler_tail <- substring(js, handler_start)
  handler_end <- regexpr(
    "function bindActivity()", handler_tail, fixed = TRUE
  )[[1]]
  handler <- substring(handler_tail, 1L, handler_end - 1L)

  expect_gt(handler_start, 0L)
  expect_match(
    handler,
    "if ((Date.now() - lastActivity) >= timeoutMs) idleExpired = true;",
    fixed = TRUE
  )
  expect_match(
    handler,
    "if (!shinyReadyForRestore()) idleExpired = true;",
    fixed = TRUE
  )
  expect_match(handler, "restoreOnReturn();", fixed = TRUE)
  expect_lt(
    regexpr("idleExpired = true;", handler, fixed = TRUE)[[1]],
    regexpr("markActivity(true);", handler, fixed = TRUE)[[1]]
  )
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

test_that("bookmarks use the original per-request UI restore path", {
  app_r_txt <- read_repo_txt("app.R")

  # Both UI caches -- the tag tree and the rendered-HTML response -- sit
  # inside this one guard, so a bookmark request still reaches build_ui()
  # and its UI-time restoreInput() calls.
  expect_match(
    app_r_txt,
    "if (.UI_CACHE_ENABLED && !is_bookmark_request(request)) {",
    fixed = TRUE
  )
  expect_match(app_r_txt, "return(.UI_CACHED)", fixed = TRUE)
  expect_match(app_r_txt, "build_ui()", fixed = TRUE)
  expect_false(grepl("restore_tabset_after_flush", app_r_txt, fixed = TRUE))

  # The rendered-HTML cache must be reached only from inside that guard.
  guard_at <- regexpr(
    "if (.UI_CACHE_ENABLED && !is_bookmark_request(request)) {",
    app_r_txt, fixed = TRUE
  )
  ui_fn_at <- regexpr("ui <- function(request) {", app_r_txt, fixed = TRUE)
  resp_at <- regexpr("resp <- ui_response()", app_r_txt, fixed = TRUE)
  expect_gt(as.integer(resp_at), as.integer(guard_at))
  expect_gt(as.integer(guard_at), as.integer(ui_fn_at))
})

test_that("league switcher cannot redirect a restored EuroLeague tab to Home", {
  js <- read_repo_txt("www", "app.js")

  expect_match(
    js,
    'var bookmarkRestorePending = location.search.indexOf("_inputs_") !== -1;',
    fixed = TRUE
  )
  expect_match(
    js,
    "owner !== league && !opts.noRedirect && !bookmarkRestorePending",
    fixed = TRUE
  )
  expect_match(js, "applyValue(restoredValue, { noRedirect: true });", fixed = TRUE)
  expect_match(js, "bookmarkRestorePending = false;", fixed = TRUE)
})
