test_that("tab4 summary percentiles stay stable when visible rows are filtered", {
  shiny::testServer(function(input, output, session) {
    session$userData$tab4 <- server_tab4(input, output, session, shared = make_shared())
  }, {
    tab4 <- session$userData$tab4
    session$setInputs(
      main_tabs = "game_logs",
      game_year = "2026",
      gl_view_mode = "Summary",
      gl_team = "1"
    )
    session$flushReact()

    full_df <- tab4$gl_teams_summary() %>% arrange(game_id)
    expect_equal(full_df$game_id, c(101L, 102L, 103L, 104L))
    expect_equal(full_df$pr_off_ppp, c(0, 1 / 3, 2 / 3, 1))
    expect_equal(full_df$pr_def_ppp, c(1, 2 / 3, 1 / 3, 0))

    session$setInputs(gl_home_away = "away")
    session$flushReact()

    filtered_df <- tab4$gl_teams_summary() %>% arrange(game_id)
    expect_equal(filtered_df$game_id, c(102L, 104L))
    expect_equal(filtered_df$pr_off_ppp, c(1 / 3, 1))
    expect_equal(filtered_df$pr_def_ppp, c(2 / 3, 0))
  })
})

test_that("tab4 four-factor percentiles stay stable when visible rows are filtered", {
  shiny::testServer(function(input, output, session) {
    session$userData$tab4 <- server_tab4(input, output, session, shared = make_shared())
  }, {
    tab4 <- session$userData$tab4
    session$setInputs(
      main_tabs = "game_logs",
      game_year = "2026",
      gl_view_mode = "Four Factors",
      gl_team = "1"
    )
    session$flushReact()

    full_df <- tab4$gl_teams_ff() %>% arrange(game_id)
    expect_equal(full_df$game_id, c(101L, 102L, 103L, 104L))
    expect_equal(full_df$pr_off_efg_pct, c(0, 1 / 3, 2 / 3, 1))
    expect_equal(full_df$pr_def_efg_pct, c(1, 2 / 3, 1 / 3, 0))
    expect_equal(full_df$pr_off_tov_pct, c(1, 2 / 3, 1 / 3, 0))
    expect_equal(full_df$pr_def_ppp, c(1, 2 / 3, 1 / 3, 0))

    session$setInputs(gl_home_away = "away")
    session$flushReact()

    filtered_df <- tab4$gl_teams_ff() %>% arrange(game_id)
    expect_equal(filtered_df$game_id, c(102L, 104L))
    expect_equal(filtered_df$pr_off_efg_pct, c(1 / 3, 1))
    expect_equal(filtered_df$pr_def_efg_pct, c(2 / 3, 0))
    expect_equal(filtered_df$pr_off_tov_pct, c(2 / 3, 0))
    expect_equal(filtered_df$pr_def_ppp, c(2 / 3, 0))
  })
})

test_that("tab4 opponent filter uses stable opponent team IDs", {
  shiny::testServer(function(input, output, session) {
    session$userData$tab4 <- server_tab4(input, output, session, shared = make_shared())
  }, {
    tab4 <- session$userData$tab4
    session$setInputs(
      main_tabs = "game_logs",
      game_year = "2026",
      gl_view_mode = "Summary",
      gl_team = "1",
      gl_opponents = "3"
    )
    session$flushReact()

    filtered_schedule <- tab4$gl_filtered_schedule()
    expect_equal(filtered_schedule$game_id, 102L)
    expect_equal(filtered_schedule$opp_team_id, 3L)
    expect_equal(filtered_schedule$opp_team_name, "Team C")
  })
})
