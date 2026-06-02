test_that("tab7 teams compare uses four-factor query for four-factor chips", {
  shiny::testServer(function(input, output, session) {
    server_tab7_compare(input, output, session, shared = make_shared())
  }, {
    session$setInputs(
      main_tabs = "compare",
      game_year = "2026",
      cmp_mode = "Teams"
    )
    session$flushReact()
    session$setInputs(cmp_metric = "off_efg")
    session$flushReact()

    expect_equal(output$cmp_summary_a_label, "eFG%")
    expect_equal(output$cmp_summary_b_label, "eFG%")
    expect_equal(output$cmp_summary_a, "53.5")
    expect_equal(output$cmp_summary_b, "53.5")
    expect_equal(output$cmp_summary_gap, "0.0")
  })
})

test_that("tab7 lineups compare uses four-factor query for four-factor chips", {
  shiny::testServer(function(input, output, session) {
    server_tab7_compare(input, output, session, shared = make_shared())
  }, {
    session$setInputs(
      main_tabs = "compare",
      game_year = "2026",
      cmp_mode = "Lineups"
    )
    session$flushReact()
    session$setInputs(cmp_metric = "off_oreb")
    session$flushReact()

    expect_equal(output$cmp_summary_a_label, "OREB%")
    expect_equal(output$cmp_summary_b_label, "OREB%")
    expect_equal(output$cmp_summary_a, "31.0")
    expect_equal(output$cmp_summary_b, "31.0")
    expect_equal(output$cmp_summary_gap, "0.0")
  })
})

render_ui_text <- function(x) {
  paste(capture.output(print(x)), collapse = "\n")
}

test_that("tab7 team detail view shows shooting accuracy and frequency", {
  shiny::testServer(function(input, output, session) {
    server_tab7_compare(input, output, session, shared = make_shared())
  }, {
    session$setInputs(
      main_tabs = "compare",
      game_year = "2026",
      cmp_mode = "Teams"
    )
    session$flushReact()
    session$setInputs(cmp_table_row_click = list(entity_name = "Team A"))
    session$flushReact()

    detail_txt <- render_ui_text(output$cmp_detail_view_ui)
    expect_true(grepl("Shooting", detail_txt, fixed = TRUE))
    expect_true(grepl("2PT Acc", detail_txt, fixed = TRUE))
    expect_true(grepl("2PT Freq", detail_txt, fixed = TRUE))
    expect_true(grepl("3PT Acc", detail_txt, fixed = TRUE))
    expect_true(grepl("3PT Freq", detail_txt, fixed = TRUE))
    expect_true(grepl("57.1%", detail_txt, fixed = TRUE))
    expect_true(grepl("40.7%", detail_txt, fixed = TRUE))
  })
})

test_that("tab7 players compare keeps last successful view during debounce", {
  shiny::testServer(function(input, output, session) {
    server_tab7_compare(input, output, session, shared = make_shared())
  }, {
    session$setInputs(
      main_tabs = "compare",
      game_year = "2026",
      cmp_mode = "Players",
      cmp_player_a = "11",
      cmp_player_b = "21"
    )
    session$elapse(300)
    session$flushReact()

    before_txt <- render_ui_text(output$cmp_pvp_ui)
    expect_true(grepl("Player A", before_txt, fixed = TRUE))
    expect_true(grepl("Player B", before_txt, fixed = TRUE))
    expect_true(grepl("USG%", before_txt, fixed = TRUE))

    session$setInputs(cmp_player_a = "12")
    session$flushReact()

    during_txt <- render_ui_text(output$cmp_pvp_ui)
    expect_true(grepl("Player A", during_txt, fixed = TRUE))
    expect_false(grepl("Player C", during_txt, fixed = TRUE))
    expect_false(grepl("Preparing player compare", during_txt, fixed = TRUE))

    session$elapse(300)
    session$flushReact()

    after_txt <- render_ui_text(output$cmp_pvp_ui)
    expect_true(grepl("Player C", after_txt, fixed = TRUE))
    expect_true(grepl("Player B", after_txt, fixed = TRUE))
  })
})

test_that("tab7 teams player compare drills from team to player detail", {
  shiny::testServer(function(input, output, session) {
    server_tab7_compare(input, output, session, shared = make_shared())
  }, {
    session$setInputs(
      main_tabs = "compare",
      game_year = "2026",
      cmp_mode = "Teams",
      cmp_a_home_away = "home",
      cmp_b_home_away = "away"
    )
    session$elapse(300)
    session$flushReact()

    expect_false(grepl("Players", render_ui_text(output$cmp_team_players_view_btn_ui), fixed = TRUE))

    session$setInputs(cmp_table_row_click = list(entity_name = "Team A"))
    session$flushReact()

    expect_true(grepl("Players", render_ui_text(output$cmp_team_players_view_btn_ui), fixed = TRUE))

    session$setInputs(cmp_detail_toggle = "players")
    session$elapse(300)
    session$flushReact()

    players_txt <- render_ui_text(output$cmp_team_players_panel_ui)
    expect_true(grepl("Team A", players_txt, fixed = TRUE))
    expect_false(grepl("Player Compare", players_txt, fixed = TRUE))

    session$setInputs(cmp_team_players_player_click = list(team_id = 1L, player_id = 11L, player_name = "Player A"))
    session$flushReact()

    player_txt <- render_ui_text(output$cmp_team_players_panel_ui)
    expect_true(grepl("Player A", player_txt, fixed = TRUE))
    expect_true(grepl("didn't play", player_txt, fixed = TRUE))
    expect_true(grepl("0.0", player_txt, fixed = TRUE))
    expect_true(grepl("Production", player_txt, fixed = TRUE))
    expect_true(grepl("Shooting", player_txt, fixed = TRUE))
    expect_true(grepl("PTS", player_txt, fixed = TRUE))
    expect_true(grepl("TS%", player_txt, fixed = TRUE))
    expect_true(grepl("USG%", player_txt, fixed = TRUE))
  })
})

test_that("tab7 teams player compare follows preset gap direction", {
  cases <- list(
    list(preset = "", extra = list(), expected = "+20.0"),
    list(preset = "starters_bench", extra = list(), expected = "+20.0"),
    list(preset = "opp_starters_bench", extra = list(), expected = "+20.0"),
    list(preset = "clutch", extra = list(), expected = "+20.0"),
    list(preset = "home_away", extra = list(), expected = "+20.0"),
    list(preset = "win_loss", extra = list(), expected = "+20.0"),
    list(preset = "top_bottom_rank", extra = list(), expected = "+20.0"),
    list(preset = "date_split", extra = list(cmp_split_date = as.Date("2026-01-15")), expected = "\u221220.0"),
    list(preset = "gn_split", extra = list(cmp_split_gn = "10"), expected = "\u221220.0")
  )

  for (case in cases) {
    shiny::testServer(function(input, output, session) {
      server_tab7_compare(input, output, session, shared = make_shared())
    }, {
      inputs <- c(
        list(
          main_tabs = "compare",
          game_year = "2026",
          cmp_mode = "Teams",
          cmp_preset = case$preset
        ),
        case$extra
      )
      do.call(session$setInputs, inputs)
      session$flushReact()
      session$setInputs(cmp_a_home_away = "home", cmp_b_home_away = "away")
      session$elapse(300)
      session$flushReact()

      session$setInputs(cmp_table_row_click = list(entity_name = "Team A"))
      session$flushReact()
      session$setInputs(cmp_detail_toggle = "players")
      session$elapse(300)
      session$flushReact()
      session$setInputs(cmp_team_players_player_click = list(team_id = 1L, player_id = 11L, player_name = "Player A"))
      session$flushReact()

      player_txt <- render_ui_text(output$cmp_team_players_panel_ui)
      expect_true(
        grepl(case$expected, player_txt, fixed = TRUE),
        info = paste("preset", if (nzchar(case$preset)) case$preset else "<manual>")
      )
    })
  }
})

capture_input_messages <- function(session) {
  sent <- list()
  original <- session$sendInputMessage
  session$sendInputMessage <- function(inputId, message) {
    sent[[length(sent) + 1L]] <<- list(id = inputId, message = message)
    original(inputId, message)
  }
  list(
    all = function() sent,
    count = function(input_id) {
      sum(vapply(sent, function(x) identical(x$id, input_id), logical(1)))
    },
    last = function(input_id) {
      matches <- Filter(function(x) identical(x$id, input_id), sent)
      if (!length(matches)) return(NULL)
      matches[[length(matches)]]
    }
  )
}

test_that("tab7 players list team filter uses team ids and refreshes after cleared player", {
  shiny::testServer(function(input, output, session) {
    server_tab7_compare(input, output, session, shared = make_shared())
  }, {
    sent <- capture_input_messages(session)
    session$setInputs(
      main_tabs = "compare",
      game_year = "2026",
      cmp_mode = "Players"
    )
    session$flushReact()

    team_filter_msg <- sent$last("cmp_player_a_list_team_filter")$message
    expect_true(grepl('value="1">Team A', as.character(team_filter_msg$options), fixed = TRUE))
    expect_true(grepl('value="2">Team B', as.character(team_filter_msg$options), fixed = TRUE))

    session$setInputs(cmp_player_a = character(0))
    session$flushReact()
    before_refresh <- sent$count("cmp_player_a")

    expect_warning({
      session$setInputs(cmp_player_a_list_team_filter = "1")
      session$flushReact()
    }, NA)

    expect_gt(sent$count("cmp_player_a"), before_refresh)
    expect_equal(sent$last("cmp_player_a")$message$value, character(0))
  })
})

test_that("tab7 players mode loads filters without auto-selecting example players", {
  shiny::testServer(function(input, output, session) {
    server_tab7_compare(input, output, session, shared = make_shared())
  }, {
    sent <- capture_input_messages(session)
    session$setInputs(
      main_tabs = "compare",
      game_year = "2026",
      cmp_mode = "Players"
    )
    session$flushReact()

    team_filter_msg <- sent$last("cmp_player_a_list_team_filter")$message
    expect_true(grepl('value="1">Team A', as.character(team_filter_msg$options), fixed = TRUE))
    expect_equal(sent$last("cmp_player_a")$message$value, character(0))
    expect_equal(sent$last("cmp_player_b")$message$value, character(0))
    expect_true(grepl("Select Player A and Player B", render_ui_text(output$cmp_pvp_ui), fixed = TRUE))
  })
})

test_that("tab7 self player compare uses one player and side-specific time filters", {
  query_calls <- list()
  server_env <- environment(server_tab7_compare)
  original_db_get_query <- get("db_get_query", envir = server_env)
  assign("db_get_query", function(pool, query, params = NULL) {
    q <- paste(query, collapse = " ")
    if (grepl("get_player_traditional_dynamic", q, fixed = TRUE) &&
        !is.null(params) && length(params) >= 18L) {
      query_calls[[length(query_calls) + 1L]] <<- params
    }
    original_db_get_query(pool, query, params = params)
  }, envir = server_env)
  on.exit(assign("db_get_query", original_db_get_query, envir = server_env), add = TRUE)

  shiny::testServer(function(input, output, session) {
    server_tab7_compare(input, output, session, shared = make_shared())
  }, {
    session$setInputs(
      main_tabs = "compare",
      game_year = "2026",
      cmp_mode = "Players",
      cmp_player_compare_mode = "self",
      cmp_player_a = "11",
      cmp_player_a_dates = c(as.Date("2026-01-01"), as.Date("2026-01-15")),
      cmp_player_b_dates = c(as.Date("2026-02-01"), as.Date("2026-02-15")),
      cmp_player_a_gn_min = "1",
      cmp_player_a_gn_max = "3",
      cmp_player_b_gn_min = "4",
      cmp_player_b_gn_max = "5"
    )
    session$elapse(300)
    session$flushReact()

    pvp_txt <- render_ui_text(output$cmp_pvp_ui)
    expect_true(grepl("Player A", pvp_txt, fixed = TRUE))
    expect_true(grepl("2026-01-01 to 2026-01-15", pvp_txt, fixed = TRUE))
    expect_true(grepl("2026-02-01 to 2026-02-15", pvp_txt, fixed = TRUE))
    expect_true(grepl("GN 1-3", pvp_txt, fixed = TRUE))
    expect_true(grepl("GN 4-5", pvp_txt, fixed = TRUE))

    starts <- as.Date(vapply(query_calls, function(params) as.character(params[[2]]), character(1)))
    ends <- as.Date(vapply(query_calls, function(params) as.character(params[[3]]), character(1)))
    gn_min <- vapply(query_calls, function(params) as.integer(params[[16]]), integer(1))
    gn_max <- vapply(query_calls, function(params) as.integer(params[[17]]), integer(1))

    expect_true(as.Date("2026-01-01") %in% starts)
    expect_true(as.Date("2026-02-01") %in% starts)
    expect_true(as.Date("2026-01-15") %in% ends)
    expect_true(as.Date("2026-02-15") %in% ends)
    expect_true(1L %in% gn_min)
    expect_true(4L %in% gn_min)
    expect_true(3L %in% gn_max)
    expect_true(5L %in% gn_max)
  })
})

test_that("tab7 compare home away preset applies side values and keeps blocked field independent", {
  shiny::testServer(function(input, output, session) {
    server_tab7_compare(input, output, session, shared = make_shared())
  }, {
    sent <- capture_input_messages(session)
    session$setInputs(
      main_tabs = "compare",
      game_year = "2026",
      cmp_mode = "Teams"
    )
    session$flushReact()

    session$setInputs(cmp_preset = "home_away")
    session$flushReact()

    expect_equal(sent$last("cmp_a_home_away")$message$value, "home")
    expect_equal(sent$last("cmp_b_home_away")$message$value, "away")

    session$setInputs(cmp_a_outcome = "win")
    session$flushReact()
    expect_equal(sent$last("cmp_b_outcome")$message$value, "win")

    before_b_home_away <- sent$count("cmp_b_home_away")
    session$setInputs(cmp_a_home_away = "")
    session$flushReact()
    expect_equal(sent$count("cmp_b_home_away"), before_b_home_away)
  })
})

test_that("tab7 compare clutch preset blocks clutch mirroring but mirrors other side filters", {
  shiny::testServer(function(input, output, session) {
    server_tab7_compare(input, output, session, shared = make_shared())
  }, {
    sent <- capture_input_messages(session)
    session$setInputs(
      main_tabs = "compare",
      game_year = "2026",
      cmp_mode = "Teams"
    )
    session$flushReact()

    session$setInputs(cmp_preset = "clutch")
    session$flushReact()

    expect_true(isTRUE(sent$last("cmp_a_clutch")$message$value))
    expect_false(isTRUE(sent$last("cmp_b_clutch")$message$value))
    expect_equal(as.character(sent$last("cmp_a_clutch_minutes")$message$value), "5")

    before_b_clutch_minutes <- sent$count("cmp_b_clutch_minutes")
    session$setInputs(cmp_a_clutch_minutes = 3)
    session$flushReact()
    expect_equal(sent$count("cmp_b_clutch_minutes"), before_b_clutch_minutes)

    session$setInputs(cmp_a_game_type = "1")
    session$flushReact()
    expect_equal(sent$last("cmp_b_game_type")$message$value, "1")
  })
})
