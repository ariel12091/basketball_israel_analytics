source(repo_file("R", "mod_team_hub.R"), local = TRUE)
shared_head_tags <- function() NULL
source(repo_file("R", "ui_tab0_home.R"), local = TRUE)

test_that("Home initially renders its named default team as selected", {
  html <- htmltools::renderTags(ui_tab0_home())$html

  expect_match(html, DEFAULT_HOME_TEAM_NAME, fixed = TRUE)
  expect_match(
    html,
    sprintf('option value="%s" selected', DEFAULT_HOME_TEAM_ID),
    fixed = TRUE
  )
  expect_match(html, 'option value="2"', fixed = TRUE)
  expect_match(html, "Team B", fixed = TRUE)
  expect_match(html, 'id="home_set_default"', fixed = TRUE)
  expect_match(html, "Set as default", fixed = TRUE)
  expect_false(grepl('id="home_set_default"[^>]* checked', html))
  expect_match(html, "home-team-controls", fixed = TRUE)
  expect_match(html, "ibplApplyInitialHubTeamDefault", fixed = TRUE)
})

test_that("team hub reserves an accessible loading card for storylines", {
  html <- htmltools::renderTags(team_hub_ui())$html

  expect_match(html, "hub-storylines-output", fixed = TRUE)
  expect_match(html, "hub-storylines-loading", fixed = TRUE)
  expect_match(html, "Analyzing team splits", fixed = TRUE)
  expect_match(html, 'role="status"', fixed = TRUE)
  expect_match(html, 'aria-live="polite"', fixed = TRUE)
})

test_that("team hub resolves its default before the selector round trip", {
  shared <- make_shared()
  reset_mock_db_query_counts()

  shiny::testServer(function(input, output, session) {
    server_team_hub(input, output, session, shared)
  }, {
    session$flushReact()

    expect_equal(shiny::isolate(shared$hub_storylines_ready_year()), 2026L)
    expect_equal(mock_db_query_count("home_dashboard_combined"), 1L)
    expect_equal(mock_db_query_count("team_ratings_preset_cache"), 0L)
    expect_equal(mock_db_query_count("hub_storylines_batch"), 0L)
  })
})

test_that("team hub keeps the resolved team in sync with manual selection", {
  shared <- make_shared()

  shiny::testServer(function(input, output, session) {
    server_team_hub(input, output, session, shared)
  }, {
    session$flushReact()
    identity_html <- paste(as.character(output$hub_identity), collapse = "")
    expect_match(
      identity_html,
      "Team A",
      fixed = TRUE
    )
    expect_match(identity_html, "Offense", fixed = TRUE)
    expect_match(identity_html, "Defense", fixed = TRUE)

    session$setInputs(home_team = "2")
    session$flushReact()
    expect_match(
      paste(as.character(output$hub_identity), collapse = ""),
      "Team B",
      fixed = TRUE
    )
  })
})

test_that("a saved valid team can replace the initial default", {
  shared <- make_shared()

  shiny::testServer(function(input, output, session) {
    server_team_hub(input, output, session, shared)
  }, {
    session$flushReact()
    session$setInputs(hub_remembered_team = "2")
    session$flushReact()

    expect_match(
      paste(as.character(output$hub_identity), collapse = ""),
      "Team B",
      fixed = TRUE
    )
    expect_false(isTRUE(input$home_set_default))
  })
})

test_that("switching teams does not replace the saved default", {
  shared <- make_shared()

  shiny::testServer(function(input, output, session) {
    test_state <- server_team_hub(input, output, session, shared)
  }, {
    session$flushReact()

    session$setInputs(home_team = "1", home_set_default = TRUE)
    session$flushReact()
    expect_equal(shiny::isolate(test_state$saved_default_team()), "1")

    session$setInputs(home_team = "2")
    session$flushReact()
    session$setInputs(home_set_default = FALSE)
    session$flushReact()

    expect_equal(shiny::isolate(test_state$saved_default_team()), "1")
  })
})

test_that("the default checkbox starts unchecked and preserves the saved team", {
  hub_r <- read_repo_txt("R", "mod_team_hub.R")
  app_js <- read_repo_txt("www", "app.js")

  home_observer_start <- regexpr(
    "observeEvent(input$home_team",
    hub_r,
    fixed = TRUE
  )[[1]]
  checkbox_observer_start <- regexpr(
    "observeEvent(input$home_set_default",
    hub_r,
    fixed = TRUE
  )[[1]]
  home_observer <- substring(
    hub_r,
    home_observer_start,
    checkbox_observer_start - 1L
  )

  expect_match(hub_r, 'hub_saved_default_team <- reactiveVal("")', fixed = TRUE)
  expect_match(
    hub_r,
    'hub_default_set_this_session <- reactiveVal(FALSE)',
    fixed = TRUE
  )
  expect_match(home_observer, "sync_home_default_checkbox", fixed = TRUE)
  expect_false(grepl("sendCustomMessage", home_observer, fixed = TRUE))
  expect_match(hub_r, "list(enabled = TRUE, teamId = tid)", fixed = TRUE)
  expect_match(hub_r, "identical(tid, saved_default)", fixed = TRUE)
  expect_match(
    app_js,
    "if (msg && msg.enabled && msg.teamId)",
    fixed = TRUE
  )
  expect_match(
    app_js,
    'var hubTeamDefaultKey = "ibplHubTeamDefaultEnabled";',
    fixed = TRUE
  )
  expect_match(
    app_js,
    'safeLocalGet(hubTeamDefaultKey) === "1"',
    fixed = TRUE
  )
  expect_match(
    app_js,
    "window.ibplApplyInitialHubTeamDefault = function()",
    fixed = TRUE
  )
  expect_match(app_js, "teamSelect.value = teamId;", fixed = TRUE)
  expect_false(grepl("defaultCheckbox.checked = true;", app_js, fixed = TRUE))
  expect_match(app_js, "safeLocalRemove(hubTeamKey)", fixed = TRUE)
})

test_that("team hub storyline fallback SQL batches all six variants", {
  sql <- hub_storyline_variants_sql()
  variants <- c(
    "starters_hi",
    "starters_lo",
    "clutch",
    "last10",
    "top4",
    "bottom4"
  )

  expect_equal(lengths(regmatches(sql, gregexpr("get_team_ratings_dynamic", sql))), 6L)
  expect_true(all(vapply(variants, grepl, logical(1), x = sql, fixed = TRUE)))
})

test_that("combined Home reader preserves the six existing source contracts", {
  sql <- hub_dashboard_query_sql()

  expect_match(sql, "home_dashboard_combined", fixed = TRUE)
  expect_match(sql, "team_ratings_preset_cache", fixed = TRUE)
  expect_match(sql, "team_ppp_ratings_mv", fixed = TRUE)
  expect_match(sql, "team_four_factors_mv", fixed = TRUE)
  expect_match(sql, "onoff_default_mv", fixed = TRUE)
  expect_match(sql, "player_traditional_stats_mv", fixed = TRUE)
  expect_match(sql, "sub_lineups_stats", fixed = TRUE)
  expect_false(grepl("fetch_lineups_csv_v2", sql, fixed = TRUE))
})

test_that("combined Home validation rejects incomplete required sections", {
  valid <- list(
    storylines = data.frame(hub_variant = "last10", team_id = 1L, net_rtg = 2),
    ratings = data.frame(
      team_id = 1L, team_name = "Team A", off_ppp = 110,
      def_ppp = 105, net_rtg = 5
    ),
    four_factors = data.frame(
      team_id = 1L,
      off_efg = 50, off_tov = 10, off_oreb = 20, off_ftr = 30,
      def_efg = 49, def_tov = 11, def_oreb = 19, def_ftr = 29
    ),
    onoff = data.frame(),
    traditional = data.frame(),
    lineups = data.frame()
  )

  expect_identical(hub_dashboard_validation_error(valid), "")

  invalid <- valid
  invalid$ratings <- data.frame()
  expect_match(
    hub_dashboard_validation_error(invalid),
    "required section ratings is empty",
    fixed = TRUE
  )

  invalid <- valid
  invalid$lineups <- data.frame(net_rtg = 1)
  expect_match(
    hub_dashboard_validation_error(invalid),
    "section lineups is missing columns",
    fixed = TRUE
  )
})
