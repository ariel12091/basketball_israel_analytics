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
    expect_equal(mock_db_query_count("team_ratings_preset_cache"), 1L)
    expect_equal(mock_db_query_count("hub_storylines_batch"), 0L)
  })
})

test_that("team hub keeps the resolved team in sync with manual selection", {
  shared <- make_shared()

  shiny::testServer(function(input, output, session) {
    server_team_hub(input, output, session, shared)
  }, {
    session$flushReact()
    expect_match(
      paste(as.character(output$hub_identity), collapse = ""),
      "Team A",
      fixed = TRUE
    )

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
  })
})

test_that("the default checkbox gates the existing local-storage team", {
  hub_r <- read_repo_txt("R", "mod_team_hub.R")
  app_js <- read_repo_txt("www", "app.js")

  expect_match(hub_r, "isTRUE(input$home_set_default)", fixed = TRUE)
  expect_match(hub_r, "list(enabled = TRUE, teamId = tid)", fixed = TRUE)
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
  expect_match(
    app_js,
    "defaultCheckbox.checked = true;",
    fixed = TRUE
  )
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
