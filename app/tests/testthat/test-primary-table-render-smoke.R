table_output_text <- function(value) {
  paste(
    c(
      capture.output(print(value)),
      capture.output(str(value, max.level = 4))
    ),
    collapse = "\n"
  )
}

expect_primary_table_rendered <- function(value) {
  txt <- table_output_text(value)
  expect_true(nzchar(txt))
  expect_false(grepl("render error", txt, ignore.case = TRUE))
  expect_false(grepl("no data for current filters", txt, ignore.case = TRUE))
  expect_false(grepl("no rows match stat filters", txt, ignore.case = TRUE))
}

set_onoff_inputs <- function(session, mode = "Summary") {
  session$setInputs(
    main_tabs = "onoff",
    game_year = "2026",
    onoff_view_mode = mode,
    date_range = as.Date(c("2025-10-01", "2026-07-01")),
    teams = character(0),
    on_game_type = character(0),
    on_opponents = character(0),
    on_home_away = "",
    on_outcome = "",
    on_opp_rank_side = "",
    on_opp_rank_n = "",
    on_opp_rank_metric = "",
    on_num_starters_off_mode = "",
    on_num_starters_off = "",
    on_num_starters_def_mode = "",
    on_num_starters_def = "",
    on_gn_min = "",
    on_gn_max = "",
    on_last_n = "",
    min_all_poss = 0,
    min_on_poss = 0
  )
  session$elapse(500)
  session$flushReact()
}

set_lineup_inputs <- function(session, mode = "Summary") {
  session$setInputs(
    main_tabs = "lineup_data",
    game_year = "2026",
    ld_view_mode = mode,
    ld_num = "5",
    ld_dates = as.Date(c("2025-10-01", "2026-07-01")),
    ld_minposs = 0,
    ld_game_type = character(0),
    ld_opponents = character(0),
    ld_home_away = "",
    ld_outcome = "",
    ld_opp_rank_side = "",
    ld_opp_rank_n = "",
    ld_opp_rank_metric = "",
    ld_clutch_enabled = FALSE,
    ld_clutch_margin = 5,
    ld_clutch_status = "all",
    ld_clutch_minutes = 5,
    ld_clutch_ot_margin = FALSE,
    ld_num_starters_off_mode = "",
    ld_num_starters_off = "",
    ld_num_starters_def_mode = "",
    ld_num_starters_def = "",
    ld_gn_min = "",
    ld_gn_max = "",
    ld_last_n = ""
  )
  session$elapse(500)
  session$flushReact()
}

set_team_ratings_inputs <- function(session, mode) {
  session$setInputs(
    main_tabs = "team_ratings",
    game_year = "2026",
    tr_view_mode = mode,
    tr_dates = as.Date(c("2025-10-10", "2025-10-31")),
    tr_game_type = character(0),
    tr_opponents = character(0),
    tr_home_away = "",
    tr_outcome = "",
    tr_opp_rank_side = "",
    tr_opp_rank_n = "",
    tr_opp_rank_metric = "",
    tr_clutch_enabled = FALSE,
    tr_clutch_margin = 5,
    tr_clutch_status = "all",
    tr_clutch_minutes = 5,
    tr_clutch_ot_margin = FALSE,
    tr_gn_min = "",
    tr_gn_max = "",
    tr_last_n = "",
    tr_num_starters_off_mode = "",
    tr_num_starters_off = "",
    tr_num_starters_def_mode = "",
    tr_num_starters_def = "",
    tr_trad_display_mode = "Per Game",
    tr_trad_defense_mode = FALSE
  )
  session$elapse(500)
  session$flushReact()
}

set_gamelog_inputs <- function(session, mode = "Summary") {
  session$setInputs(
    main_tabs = "game_logs",
    game_year = "2026",
    gl_view_mode = mode,
    gl_team = "1",
    gl_dates = as.Date(c("2025-10-01", "2026-07-01")),
    gl_game_type = character(0),
    gl_home_away = "",
    gl_outcome = "",
    gl_opp_rank_side = "",
    gl_opp_rank_n = "",
    gl_opp_rank_metric = "",
    gl_num_starters_off_mode = "",
    gl_num_starters_off = "",
    gl_num_starters_def_mode = "",
    gl_num_starters_def = "",
    gl_gn_min = "",
    gl_gn_max = "",
    gl_last_n = ""
  )
  session$elapse(500)
  session$flushReact()
}

set_player_traditional_inputs <- function(session) {
  session$setInputs(
    main_tabs = "traditional_stats",
    game_year = "2026",
    ts_dates = as.Date(c("2025-10-10", "2025-10-31")),
    ts_teams = character(0),
    ts_game_type = character(0),
    ts_opponents = character(0),
    ts_home_away = "",
    ts_outcome = "",
    ts_opp_rank_side = "",
    ts_opp_rank_n = "",
    ts_opp_rank_metric = "",
    ts_clutch_enabled = FALSE,
    ts_clutch_margin = 5,
    ts_clutch_status = "all",
    ts_clutch_minutes = 5,
    ts_clutch_ot_margin = FALSE,
    ts_gn_min = "",
    ts_gn_max = "",
    ts_last_n = "",
    ts_display_mode = "Per Game",
    ts_show_ineligible = FALSE,
    ts_min_gp = 1,
    ts_min_gp_slider = 1
  )
  session$elapse(500)
  session$flushReact()
}

set_team_stats_inputs <- function(session) {
  session$setInputs(
    main_tabs = "team_stats",
    game_year = "2026",
    tst_dates = as.Date(c("2025-10-10", "2025-10-31")),
    tst_teams = character(0),
    tst_game_type = character(0),
    tst_opponents = character(0),
    tst_home_away = "",
    tst_outcome = "",
    tst_opp_rank_side = "",
    tst_opp_rank_n = "",
    tst_opp_rank_metric = "",
    tst_clutch_enabled = FALSE,
    tst_clutch_margin = 5,
    tst_clutch_status = "all",
    tst_clutch_minutes = 5,
    tst_clutch_ot_margin = FALSE,
    tst_gn_min = "",
    tst_gn_max = "",
    tst_last_n = "",
    tst_display_mode = "Per Game",
    tst_rank_change_basis = "week",
    tst_min_gp = 1,
    tst_min_gp_slider = 1
  )
  session$elapse(500)
  session$flushReact()
}

test_that("primary app tables render data-shaped output with mock data", {
  shiny::testServer(function(input, output, session) {
    server_tab1(input, output, session, shared = make_shared())
  }, {
    set_onoff_inputs(session, "Summary")
    expect_silent(rendered <- output$onoff_dt)
    expect_primary_table_rendered(rendered)

    set_onoff_inputs(session, "Four Factors")
    expect_silent(rendered <- output$onoff_dt)
    expect_primary_table_rendered(rendered)
  })

  shiny::testServer(function(input, output, session) {
    server_tab2(input, output, session, shared = make_shared())
  }, {
    set_lineup_inputs(session, "Summary")
    expect_silent(rendered <- output$ld_table)
    expect_primary_table_rendered(rendered)

    set_lineup_inputs(session, "Four Factors")
    expect_silent(rendered <- output$ld_table)
    expect_primary_table_rendered(rendered)
  })

  shiny::testServer(function(input, output, session) {
    server_tab3(input, output, session, shared = make_shared())
  }, {
    for (mode in c("Summary", "Four Factors", "Traditional")) {
      set_team_ratings_inputs(session, mode)
      expect_silent(rendered <- output$tr_table)
      expect_primary_table_rendered(rendered)
    }
  })

  shiny::testServer(function(input, output, session) {
    server_tab4(input, output, session, shared = make_shared())
  }, {
    set_gamelog_inputs(session, "Summary")
    expect_silent(rendered <- output$gl_table)
    expect_primary_table_rendered(rendered)

    set_gamelog_inputs(session, "Four Factors")
    expect_silent(rendered <- output$gl_table)
    expect_primary_table_rendered(rendered)
  })

  shiny::testServer(function(input, output, session) {
    server_tab5_traditional(input, output, session, shared = make_shared())
  }, {
    set_player_traditional_inputs(session)
    expect_silent(rendered <- output$ts_table)
    expect_primary_table_rendered(rendered)
  })

  shiny::testServer(function(input, output, session) {
    server_tab6_team_stats(input, output, session, shared = make_shared())
  }, {
    set_team_stats_inputs(session)
    expect_silent(rendered <- output$tst_table)
    expect_primary_table_rendered(rendered)
  })
})
