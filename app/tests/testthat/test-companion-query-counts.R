query_recorder <- function(server_env) {
  state <- new.env(parent = emptyenv())
  state$queries <- character(0)
  original <- get("db_get_query", envir = server_env, inherits = TRUE)

  assign("db_get_query", function(pool, query, params = NULL) {
    state$queries <- c(state$queries, paste(query, collapse = " "))
    original(pool, query, params = params)
  }, envir = server_env)

  withr::defer(assign("db_get_query", original, envir = server_env),
               envir = parent.frame())
  state
}

query_hits <- function(state, text) {
  sum(grepl(text, state$queries, fixed = TRUE))
}

set_companion_lineup_inputs <- function(session, mode = "Summary") {
  session$setInputs(
    main_tabs = "lineup_data", game_year = "2026", ld_view_mode = mode,
    ld_num = "5", ld_dates = as.Date(c("2025-11-01", "2026-02-01")),
    ld_minposs = 0, ld_game_type = character(0), ld_opponents = character(0),
    ld_home_away = "", ld_outcome = "", ld_opp_rank_side = "",
    ld_opp_rank_n = "", ld_opp_rank_metric = "", ld_clutch_enabled = FALSE,
    ld_clutch_margin = 5, ld_clutch_status = "all", ld_clutch_minutes = 5,
    ld_clutch_ot_margin = FALSE, ld_num_starters_off_mode = "",
    ld_num_starters_off = "", ld_num_starters_def_mode = "",
    ld_num_starters_def = "", ld_gn_min = "", ld_gn_max = "", ld_last_n = ""
  )
  session$elapse(500)
  session$flushReact()
}

set_companion_team_inputs <- function(session) {
  session$setInputs(
    main_tabs = "team_ratings", game_year = "2026", tr_view_mode = "Four Factors",
    tr_dates = as.Date(c("2025-11-01", "2026-02-01")),
    tr_game_type = character(0), tr_opponents = character(0),
    tr_home_away = "", tr_outcome = "", tr_opp_rank_side = "",
    tr_opp_rank_n = "", tr_opp_rank_metric = "",
    tr_clutch_enabled = TRUE, tr_clutch_margin = 5,
    tr_clutch_status = "all", tr_clutch_minutes = 5,
    tr_clutch_ot_margin = FALSE, tr_gn_min = "", tr_gn_max = "", tr_last_n = "",
    tr_num_starters_off_mode = "", tr_num_starters_off = "",
    tr_num_starters_def_mode = "", tr_num_starters_def = "",
    tr_trad_display_mode = "Per Game", tr_trad_defense_mode = FALSE
  )
  session$elapse(500)
  session$flushReact()
}

test_that("Israeli Lineups table and auto-min share one selected-view query", {
  recorder <- query_recorder(environment(server_tab2))

  shiny::testServer(function(input, output, session) {
    server_tab2(input, output, session, shared = make_shared())
  }, {
    set_companion_lineup_inputs(session, "Summary")
    expect_silent(output$ld_table)
    expect_identical(query_hits(recorder, "fetch_lineups_csv_v2"), 1L)
    expect_identical(query_hits(recorder, "fetch_lineups_four_factors_csv"), 0L)
  })
})

test_that("Israeli Team standard-clutch factors and minutes share one query", {
  recorder <- query_recorder(environment(server_tab3))

  shiny::testServer(function(input, output, session) {
    server_tab3(input, output, session, shared = make_shared())
  }, {
    set_companion_team_inputs(session)
    expect_silent(output$tr_table)
    expect_identical(
      query_hits(recorder, "get_team_four_factors_dashboard_dynamic"),
      1L
    )
    expect_identical(query_hits(recorder, "get_team_four_factors_dynamic"), 0L)
  })
})

test_that("Israeli Game Logs renders each view from one season fact", {
  recorder <- query_recorder(environment(server_tab4))

  shiny::testServer(function(input, output, session) {
    server_tab4(input, output, session, shared = make_shared())
  }, {
    session$setInputs(
      main_tabs = "game_logs", game_year = "2026", gl_view_mode = "Summary",
      gl_team = "1", gl_dates = as.Date(c("2025-10-01", "2026-07-01")),
      gl_game_type = character(0), gl_opponents = character(0),
      gl_home_away = "", gl_outcome = "", gl_num_starters_off_mode = "",
      gl_num_starters_off = "", gl_num_starters_def_mode = "",
      gl_num_starters_def = "", gl_gn_min = "", gl_gn_max = "", gl_last_n = ""
    )
    session$flushReact()
    expect_silent(output$gl_table)
    expect_identical(query_hits(recorder, "mv_lineup_totals_by_day"), 1L)
    expect_identical(query_hits(recorder, "lineup_four_factors_by_game"), 0L)
  })
})

make_euro_query_test_env <- function() {
  env <- new.env(parent = environment(server_tab4))
  env$EURO_COMPETITION_LABELS <- c(E = "EuroLeague", U = "EuroCup")
  env$euro_season_date_bounds <- function(season) {
    list(start = as.Date("2025-09-01"), end = as.Date("2026-07-01"))
  }
  env$euro_season_label <- function(season) as.character(season)
  env$euro_phase_label <- function(x) as.character(x)
  env$euro_data_version <- function() "euro-test-v1"
  env$setup_euro_section_filters <- function(...) invisible(TRUE)
  env$apply_season_date_bounds <- function(...) invisible(TRUE)

  env$query_state <- new.env(parent = emptyenv())
  env$query_state$queries <- character(0)
  env$db_get_query <- function(pool, query, params = NULL) {
    q <- paste(query, collapse = " ")
    env$query_state$queries <- c(env$query_state$queries, q)

    if (grepl("get_team_dashboard_dynamic", q, fixed = TRUE)) {
      return(data.frame(
        game_year = 2025L, team_id = c(1L, 2L),
        team_name = c("Team A", "Team B"), games_played = c(4L, 4L),
        wins = c(3L, 2L), losses = c(1L, 2L),
        off_ppp = c(112.4, 108.8), def_ppp = c(101.7, 105.1),
        net_rtg = c(10.7, 3.7), off_poss = c(120L, 118L),
        def_poss = c(120L, 118L), total_poss = c(240L, 236L),
        off_efg = c(54.8, 52.1), off_ts = c(56.1, 53.4),
        off_tov = c(12.8, 15.6), off_oreb = c(31.2, 27.5),
        off_ftr = c(28.4, 24.9), def_efg = c(50.7, 53.3),
        def_ts = c(52.4, 55.0), def_tov = c(16.1, 13.8),
        def_oreb = c(24.9, 28.3), def_ftr = c(22.7, 27.1),
        minutes = c(160, 160)
      ))
    }

    if (grepl("fetch_lineups_", q, fixed = TRUE) ||
        grepl("sub_lineups_stats_mv", q, fixed = TRUE)) {
      return(data.frame(
        team_id = c(1L, 2L), unit_key = c("u1", "u2"), unit_size = 5L,
        player_names_str = c("A1, A2, A3, A4, A5", "B1, B2, B3, B4, B5"),
        player_ids = c("{1,2,3,4,5}", "{6,7,8,9,10}"),
        off_poss = c(42L, 37L), off_pts = c(50L, 41L),
        off_fg2_made = c(20L, 18L), off_fg2_att = c(35L, 34L),
        off_fg3_made = c(8L, 7L), off_fg3_att = c(24L, 23L),
        off_ts_poss = c(44L, 38L), off_fgm = c(28L, 25L),
        off_fga = c(59L, 57L), off_fta = c(12L, 9L),
        off_oreb = c(10L, 8L), off_oreb_opp = c(30L, 28L),
        off_tov = c(5L, 6L), off_steals = c(7L, 5L),
        def_poss = c(42L, 37L), def_pts = c(42L, 39L),
        def_fg2_made = c(18L, 19L), def_fg2_att = c(33L, 34L),
        def_fg3_made = c(7L, 8L), def_fg3_att = c(22L, 21L),
        def_ts_poss = c(40L, 37L), def_fgm = c(25L, 27L),
        def_fga = c(55L, 55L), def_fta = c(8L, 11L),
        def_oreb = c(7L, 9L), def_oreb_opp = c(28L, 31L),
        def_tov = c(7L, 5L), def_steals = c(5L, 6L),
        minutes = c(20.5, 18.0), starters_poss_num = c(210L, 185L)
      ))
    }

    if (grepl("euroleague.team_four_factors_by_game", q, fixed = TRUE)) {
      return(data.frame(
        game_id = c(101L, 102L), team_id = c(1L, 1L),
        own_starters = c(5L, 5L), opp_starters = c(5L, 5L),
        off_pts = c(90L, 100L), off_poss = c(80L, 85L),
        off_ts_poss = c(78L, 82L), off_oreb = c(10L, 11L),
        off_oreb_opp = c(35L, 36L), off_tov = c(12L, 11L),
        off_fta = c(18L, 20L), off_fga = c(65L, 68L),
        off_fgm = c(32L, 34L), off_fg3m = c(9L, 10L),
        def_pts = c(85L, 95L), def_poss = c(80L, 85L),
        def_ts_poss = c(76L, 80L), def_oreb = c(9L, 10L),
        def_oreb_opp = c(34L, 35L), def_tov = c(13L, 12L),
        def_fta = c(16L, 17L), def_fga = c(64L, 66L),
        def_fgm = c(30L, 32L), def_fg3m = c(8L, 9L),
        round_number = c(1L, 2L), phase = c("RS", "RS"),
        game_date = as.Date(c("2025-10-01", "2025-10-08")),
        opp_team_id = c(2L, 3L), is_home = c(TRUE, FALSE),
        has_won = c(TRUE, TRUE), team_points = c(90L, 100L),
        opp_points = c(85L, 95L), team_name = c("Team A", "Team A"),
        opp_team_name = c("Team B", "Team C")
      ))
    }

    data.frame()
  }

  sys.source(repo_file("R", "server_tab9_euro_team.R"), envir = env)
  sys.source(repo_file("R", "server_tab10_euro_lineups.R"), envir = env)
  sys.source(repo_file("R", "server_tab11_euro_gamelogs.R"), envir = env)
  env
}

make_euro_query_shared <- function() {
  teams <- data.frame(team_id = c(1L, 2L), team_name = c("Team A", "Team B"))
  list(euro = list(
    competition = shiny::reactiveVal("E"), season = shiny::reactiveVal(2025L),
    teams_df = shiny::reactive({ teams }),
    players_df = shiny::reactive({
      data.frame(team_id = 1L, player_id = 11L, name = "Player A")
    }),
    phase_choices = shiny::reactive({ c("RS" = "RS") }),
    round_values = shiny::reactive({ 1:34 }),
    date_bounds = shiny::reactive({
      list(start = as.Date("2025-09-01"), end = as.Date("2026-07-01"))
    })
  ))
}

set_euro_common_inputs <- function(session, tab, prefix, mode) {
  values <- list(main_tabs = tab)
  values[[paste0(prefix, "_view_mode")]] <- mode
  values[[paste0(prefix, "_dates")]] <- as.Date(c("2025-11-01", "2026-02-01"))
  values[[paste0(prefix, "_teams")]] <- character(0)
  values[[paste0(prefix, "_phase")]] <- character(0)
  values[[paste0(prefix, "_opponents")]] <- character(0)
  values[[paste0(prefix, "_home_away")]] <- ""
  values[[paste0(prefix, "_outcome")]] <- ""
  values[[paste0(prefix, "_opp_rank_side")]] <- ""
  values[[paste0(prefix, "_opp_rank_n")]] <- ""
  values[[paste0(prefix, "_opp_rank_metric")]] <- ""
  values[[paste0(prefix, "_gn_min")]] <- ""
  values[[paste0(prefix, "_gn_max")]] <- ""
  values[[paste0(prefix, "_last_n")]] <- ""
  values[[paste0(prefix, "_num_starters_off_mode")]] <- ""
  values[[paste0(prefix, "_num_starters_off")]] <- ""
  values[[paste0(prefix, "_num_starters_def_mode")]] <- ""
  values[[paste0(prefix, "_num_starters_def")]] <- ""
  do.call(session$setInputs, values)
}

test_that("EuroLeague Team standard-clutch consumers share one query", {
  euro <- make_euro_query_test_env()
  shared <- make_euro_query_shared()

  shiny::testServer(function(input, output, session) {
    euro$server_tab9_euro_team(input, output, session, shared)
  }, {
    set_euro_common_inputs(session, "euro_team", "euroteam", "Four Factors")
    session$setInputs(
      euroteam_clutch_enabled = TRUE, euroteam_clutch_margin = 5,
      euroteam_clutch_status = "all", euroteam_clutch_minutes = 5,
      euroteam_clutch_ot_margin = FALSE
    )
    session$elapse(500)
    session$flushReact()
    expect_silent(output$euroteam_table)
    expect_identical(query_hits(euro$query_state, "get_team_dashboard_dynamic"), 1L)
  })
})

test_that("EuroLeague Lineups table and auto-min share one filtered query", {
  euro <- make_euro_query_test_env()
  shared <- make_euro_query_shared()

  shiny::testServer(function(input, output, session) {
    euro$server_tab10_euro_lineups(input, output, session, shared)
  }, {
    session$setInputs(
      main_tabs = "euro_lineups", euro_ld_view_mode = "Summary",
      euro_ld_group_size = "5", euro_ld_date_range = as.Date(c("2025-11-01", "2026-02-01")),
      euro_ld_minposs = 0, euro_ld_phase = character(0),
      euro_ld_opponents = character(0), euro_ld_home_away = "",
      euro_ld_outcome = "", euro_ld_opp_rank_side = "",
      euro_ld_opp_rank_n = "", euro_ld_opp_rank_metric = "",
      euro_ld_gn_min = "", euro_ld_gn_max = "", euro_ld_last_n = "",
      euro_ld_num_starters_off_mode = "", euro_ld_num_starters_off = "",
      euro_ld_num_starters_def_mode = "", euro_ld_num_starters_def = "",
      euro_ld_clutch_enabled = FALSE, euro_ld_clutch_margin = 5,
      euro_ld_clutch_status = "all", euro_ld_clutch_minutes = 5,
      euro_ld_clutch_ot_margin = FALSE
    )
    session$elapse(500)
    session$flushReact()
    expect_silent(output$euro_ld_dt)
    expect_identical(query_hits(euro$query_state, "fetch_lineups_pergame"), 1L)
  })
})

test_that("EuroLeague Game Logs reuses one season query across both views", {
  euro <- make_euro_query_test_env()
  shared <- make_euro_query_shared()

  shiny::testServer(function(input, output, session) {
    euro$server_tab11_euro_gamelogs(input, output, session, shared)
  }, {
    set_euro_common_inputs(session, "euro_game_logs", "eurogl", "Summary")
    session$setInputs(eurogl_dates = as.Date(c("2025-10-01", "2025-10-31")))
    session$flushReact()
    expect_silent(output$eurogl_table)
    session$setInputs(eurogl_view_mode = "Four Factors")
    session$flushReact()
    expect_silent(output$eurogl_table)
    expect_identical(
      query_hits(euro$query_state, "euroleague.team_four_factors_by_game"),
      1L
    )
  })
})
