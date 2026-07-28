source(repo_file("R", "mod_team_hub.R"), local = TRUE)

test_that("team hub reserves an accessible loading card for storylines", {
  html <- htmltools::renderTags(team_hub_ui())$html

  expect_match(html, "hub-storylines-output", fixed = TRUE)
  expect_match(html, "hub-storylines-loading", fixed = TRUE)
  expect_match(html, "Analyzing team splits", fixed = TRUE)
  expect_match(html, 'role="status"', fixed = TRUE)
  expect_match(html, 'aria-live="polite"', fixed = TRUE)
})

test_that("team hub marks storylines ready after rendering", {
  shared <- make_shared()
  reset_mock_db_query_counts()

  shiny::testServer(function(input, output, session) {
    server_team_hub(input, output, session, shared)
  }, {
    session$setInputs(home_team = "1")
    session$flushReact()

    expect_equal(shiny::isolate(shared$hub_storylines_ready_year()), 2026L)
    expect_equal(mock_db_query_count("hub_storylines_batch"), 1L)
  })
})

test_that("team hub storyline SQL batches all six variants", {
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
