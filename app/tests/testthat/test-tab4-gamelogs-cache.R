run_tab4_cache_session <- function(data_version, view_mode) {
  shiny::testServer(function(input, output, session) {
    session$userData$tab4 <- server_tab4(
      input,
      output,
      session,
      shared = make_shared(data_version = data_version)
    )
  }, {
    session$setInputs(
      main_tabs = "game_logs",
      game_year = "2026",
      gl_view_mode = view_mode,
      gl_team = "1"
    )
    session$flushReact()

    if (identical(view_mode, "Four Factors")) {
      expect_true(nrow(session$userData$tab4$gl_teams_ff()) > 0)
    } else {
      expect_true(nrow(session$userData$tab4$gl_teams_summary()) > 0)
    }
  })
}

test_that("tab4 season queries are reused across sessions", {
  GL_DATA_CACHE$reset()
  reset_mock_db_query_counts()
  data_version <- shiny::reactiveVal("etl-v1")

  run_tab4_cache_session(data_version, "Summary")
  run_tab4_cache_session(data_version, "Summary")
  expect_equal(mock_db_query_count("gl_lineup_totals"), 1L)
  expect_equal(mock_db_query_count("gl_lineup_ff"), 0L)

  run_tab4_cache_session(data_version, "Four Factors")
  run_tab4_cache_session(data_version, "Four Factors")
  expect_equal(mock_db_query_count("gl_lineup_totals"), 1L)
  expect_equal(mock_db_query_count("gl_lineup_ff"), 1L)
})

test_that("tab4 season cache invalidates when the ETL version changes", {
  GL_DATA_CACHE$reset()
  reset_mock_db_query_counts()
  data_version <- shiny::reactiveVal("etl-v1")

  shiny::testServer(function(input, output, session) {
    session$userData$tab4 <- server_tab4(
      input,
      output,
      session,
      shared = make_shared(data_version = data_version)
    )
  }, {
    session$setInputs(
      main_tabs = "game_logs",
      game_year = "2026",
      gl_view_mode = "Summary",
      gl_team = "1"
    )
    session$flushReact()
    expect_true(nrow(session$userData$tab4$gl_teams_summary()) > 0)
    expect_equal(mock_db_query_count("gl_lineup_totals"), 1L)

    data_version("etl-v2")
    session$flushReact()
    expect_true(nrow(session$userData$tab4$gl_teams_summary()) > 0)
    expect_equal(mock_db_query_count("gl_lineup_totals"), 2L)
  })
})
