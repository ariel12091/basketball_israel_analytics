# Game log percentile colouring must be computed over the season's full game
# population, so a row's colour never depends on which rows are on screen.
# Every sidebar filter -- the team selector included -- narrows the DISPLAY
# only. Same contract as Tab 5 (test-tab5-percentile-population.R).
#
# The mock season holds two teams: Team A's four games sit strictly above
# Team B's on every metric, so Team A occupies ranks 5-8 of 8 and its
# percent_ranks are 4/7, 5/7, 6/7, 1 (reversed for metrics that fall across
# its schedule).

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
    expect_equal(full_df$pr_off_ppp, c(4 / 7, 5 / 7, 6 / 7, 1))
    expect_equal(full_df$pr_def_ppp, c(1, 6 / 7, 5 / 7, 4 / 7))

    session$setInputs(gl_home_away = "away")
    session$flushReact()

    filtered_df <- tab4$gl_teams_summary() %>% arrange(game_id)
    expect_equal(filtered_df$game_id, c(102L, 104L))
    expect_equal(filtered_df$pr_off_ppp, c(5 / 7, 1))
    expect_equal(filtered_df$pr_def_ppp, c(6 / 7, 4 / 7))
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
    expect_equal(full_df$net_rtg, round(full_df$off_ppp - full_df$def_ppp, 1))
    expect_equal(full_df$pr_off_efg_pct, c(4 / 7, 5 / 7, 6 / 7, 1))
    expect_equal(full_df$pr_def_efg_pct, c(1, 6 / 7, 5 / 7, 4 / 7))
    expect_equal(full_df$pr_off_tov_pct, c(1, 6 / 7, 5 / 7, 4 / 7))
    expect_equal(full_df$pr_def_ppp, c(1, 6 / 7, 5 / 7, 4 / 7))

    session$setInputs(gl_home_away = "away")
    session$flushReact()

    filtered_df <- tab4$gl_teams_ff() %>% arrange(game_id)
    expect_equal(filtered_df$game_id, c(102L, 104L))
    expect_equal(filtered_df$pr_off_efg_pct, c(5 / 7, 1))
    expect_equal(filtered_df$pr_def_efg_pct, c(6 / 7, 4 / 7))
    expect_equal(filtered_df$pr_off_tov_pct, c(6 / 7, 4 / 7))
    expect_equal(filtered_df$pr_def_ppp, c(6 / 7, 4 / 7))
  })
})

test_that("tab4 summary percentiles do not rescale when a team is selected", {
  shiny::testServer(function(input, output, session) {
    session$userData$tab4 <- server_tab4(input, output, session, shared = make_shared())
  }, {
    tab4 <- session$userData$tab4
    session$setInputs(
      main_tabs = "game_logs",
      game_year = "2026",
      gl_view_mode = "Summary",
      gl_team = ""
    )
    session$flushReact()

    league <- tab4$gl_teams_summary() %>%
      filter(team_id == 1L) %>%
      arrange(game_id)
    expect_equal(nrow(tab4$gl_teams_summary()), 8L)

    session$setInputs(gl_team = "1")
    session$flushReact()

    one_team <- tab4$gl_teams_summary() %>% arrange(game_id)
    expect_equal(one_team$game_id, league$game_id)
    expect_equal(one_team$pr_off_ppp, league$pr_off_ppp)
    expect_equal(one_team$pr_def_ppp, league$pr_def_ppp)

    # The regression: ranking Team A against only its own four games would
    # spread its percentiles across the whole 0-1 scale.
    expect_false(isTRUE(all.equal(one_team$pr_off_ppp, c(0, 1 / 3, 2 / 3, 1))))
  })
})

test_that("tab4 four-factor percentiles do not rescale when a team is selected", {
  shiny::testServer(function(input, output, session) {
    session$userData$tab4 <- server_tab4(input, output, session, shared = make_shared())
  }, {
    tab4 <- session$userData$tab4
    session$setInputs(
      main_tabs = "game_logs",
      game_year = "2026",
      gl_view_mode = "Four Factors",
      gl_team = ""
    )
    session$flushReact()

    league <- tab4$gl_teams_ff() %>%
      filter(team_id == 1L) %>%
      arrange(game_id)

    session$setInputs(gl_team = "1")
    session$flushReact()

    one_team <- tab4$gl_teams_ff() %>% arrange(game_id)
    expect_equal(one_team$game_id, league$game_id)
    for (pr_col in c("pr_off_ppp", "pr_def_ppp", "pr_off_efg_pct", "pr_def_efg_pct",
                     "pr_off_oreb_pct", "pr_def_oreb_pct", "pr_off_tov_pct",
                     "pr_def_tov_pct", "pr_off_ftr_pct", "pr_def_ftr_pct")) {
      expect_equal(one_team[[pr_col]], league[[pr_col]], info = pr_col)
    }
    expect_false(isTRUE(all.equal(one_team$pr_off_efg_pct, c(0, 1 / 3, 2 / 3, 1))))
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
