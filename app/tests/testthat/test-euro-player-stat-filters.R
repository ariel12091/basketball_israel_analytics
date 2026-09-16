# Player Stats filter chips on the EuroLeague On/Off tab (Tab 8), mirroring
# test-onoff-player-stat-filters.R's server section for Tab 1. The pure
# classification/projection/join helpers (is_player_stat_filter,
# split_stat_filters, apply_player_stat_filters, retain_stat_filters_for_cols,
# ...) are league-neutral and already covered there; this file only exercises
# server_tab8_euro()'s own wiring.
#
# server_tab8_euro.R issues its own on/off and Four Factors queries
# (euroleague.onoff_compute / euroleague.four_factors_dashboard_compute /
# euroleague.player_onoff_default_mv / euroleague.player_advanced_stats_mv),
# none of which helper-server-mocks.R stubs (no test exercised this tab's
# server function before Task B). So this file sources server_tab8_euro.R
# into its own child environment with its own db_get_query for exactly those
# four queries, the same pattern test-companion-query-counts.R uses for tabs
# 9-11. The Player Stats reader queries themselves
# (euroleague.player_traditional_stats_mv / get_player_traditional_pergame)
# are NOT re-mocked here: fetch_player_stat_filter_frame() is called directly
# (never redefined in the child env), so its internal db_get_query lookup
# resolves through its OWN closure -- the shared helper-server-mocks.R
# environment -- which already has euro_player_traditional_mv /
# euro_player_traditional_reader counters wired up.

ps_filter <- function(id, label, col, op = "ge", value = 0) {
  list(id = id, label = label, col = col, op = op, value = value)
}

# One row per team (team 1/player 11, team 2/player 21), matching the
# traditional mock's player_id/team_id pairing (helper-server-mocks.R
# mock_player_traditional_df(): players 11 and 12 on team 1, player 21 on
# team 2). Two shapes, not one: onoff_four_factors_datatable() does an
# unconditional rename(`ON Poss` = off_on_poss, `OFF Poss` = off_off_poss),
# which errors on duplicate names if a frame already carries the Summary
# shape's own "ON Poss"/"OFF Poss" columns alongside the raw off_on_poss
# ones -- so the on/off shape and the Four Factors shape stay separate
# fixtures, sharing only the identity/rating columns both need.
euro8_identity_cols <- function() {
  data.frame(
    competition = c("E", "E"),
    game_year = c(2025L, 2025L),
    team_id = c(1L, 2L),
    player_id = c(11L, 21L),
    Team = c("Team A", "Team B"),
    `First Name` = c("Player", "Player"),
    `Last Name` = c("A", "B"),
    `Net RTG Diff` = c(12, -2),
    `Off ON Diff` = c(3, -1),
    `Def ON Diff` = c(-2, 1),
    minutes = c(180.5, 150.0),
    check.names = FALSE
  )
}

# player_onoff_default_mv / euroleague.onoff_compute shape (Summary).
euro8_onoff_rows <- function() {
  cbind(euro8_identity_cols(), data.frame(
    `Off ON PPP` = c(116.2, 108.0),
    `Def ON PPP` = c(101.4, 106.0),
    `On Net RTG` = c(14.8, 2.0),
    `Off OFF PPP` = c(108.8, 108.9),
    `Def OFF PPP` = c(106.5, 108.9),
    `Off Net RTG` = c(2.3, 0.0),
    `ON Poss` = c(420L, 360L),
    `OFF Poss` = c(220L, 240L),
    pr_net = c(1, 0),
    check.names = FALSE
  ))
}

# player_advanced_stats_mv / euroleague.four_factors_dashboard_compute shape
# (Four Factors). "Net RTG Diff" (from euro8_identity_cols()) is present here
# -- unlike the Israeli generic MV mock's advanced-stats shape -- so the Four
# Factors MV path never needs the competition+season join:
# ff_ranked_df()'s `df <- df_adv` branch fires directly.
euro8_ff_rows <- function() {
  cbind(euro8_identity_cols(), data.frame(
    off_on_efg = c(0.56, 0.52), off_off_efg = c(0.51, 0.50),
    off_on_oreb = c(0.31, 0.28), off_off_oreb = c(0.25, 0.24),
    off_on_tov = c(0.12, 0.13), off_off_tov = c(0.15, 0.16),
    off_on_ftr = c(0.28, 0.25), off_off_ftr = c(0.24, 0.22),
    def_on_efg = c(0.50, 0.53), def_off_efg = c(0.54, 0.55),
    def_on_oreb = c(0.24, 0.27), def_off_oreb = c(0.29, 0.30),
    def_on_tov = c(0.16, 0.14), def_off_tov = c(0.12, 0.11),
    def_on_ftr = c(0.23, 0.26), def_off_ftr = c(0.27, 0.28),
    off_on_poss = c(420L, 360L), off_off_poss = c(220L, 240L),
    def_on_poss = c(420L, 360L), def_off_poss = c(220L, 240L),
    off_on_ppp = c(116.2, 108.0), off_off_ppp = c(108.8, 108.9),
    def_on_ppp = c(101.4, 106.0), def_off_ppp = c(106.5, 108.9),
    check.names = FALSE
  ))
}

# Sources server_tab8_euro.R fresh into a child of the shared mocks
# environment, with its own db_get_query for the tab's own on/off and Four
# Factors queries (see file header). fetch_player_stat_filter_frame() itself
# is untouched, so it keeps hitting the real shared mocks.
make_euro8_test_env <- function() {
  env <- new.env(parent = environment(fetch_player_stat_filter_frame))
  env$EURO_DEFAULT_SEASON <- "2025"
  env$EURO_COMPETITION_LABELS <- c(E = "EuroLeague", U = "EuroCup")
  env$euro_season_date_bounds <- function(season) {
    list(start = as.Date("2025-09-01"), end = as.Date("2026-07-01"))
  }
  env$euro_season_label <- function(season) as.character(season)
  env$euro_phase_label <- function(x) as.character(x)
  env$euro_data_version <- function() "euro-test-v1"
  env$setup_euro_section_filters <- function(...) invisible(TRUE)

  env$db_get_query <- function(pool, query, params = NULL) {
    q <- paste(query, collapse = " ")
    if (grepl("euroleague.onoff_compute(", q, fixed = TRUE) ||
        grepl("euroleague.player_onoff_default_mv", q, fixed = TRUE)) {
      return(euro8_onoff_rows())
    }
    if (grepl("euroleague.four_factors_dashboard_compute(", q, fixed = TRUE) ||
        grepl("euroleague.player_advanced_stats_mv", q, fixed = TRUE)) {
      return(euro8_ff_rows())
    }
    data.frame()
  }

  sys.source(repo_file("R", "server_tab8_euro.R"), envir = env)
  env
}

make_euro8_shared <- function() {
  teams <- data.frame(team_id = c(1L, 2L), team_name = c("Team A", "Team B"))
  list(euro = list(
    competition = shiny::reactiveVal("E"),
    season = shiny::reactiveVal(2025L),
    teams_df = shiny::reactive({ teams }),
    seasons_df = shiny::reactive({ data.frame(game_year = 2025L) })
  ))
}

tab8_app <- function() {
  euro <- make_euro8_test_env()
  function(input, output, session) {
    session$userData$tab8 <- euro$server_tab8_euro(input, output, session, shared = make_euro8_shared())
  }
}

# Season-bounds dates (2025-09-01 .. 2026-07-01, matching euro_season_date_bounds
# above) keep the fast path, mirroring set_onoff_context() in
# test-onoff-player-stat-filters.R.
set_euro_context <- function(session, mode = "Summary", ...) {
  inputs <- list(
    main_tabs = "euro_onoff", league_select = "euroleague",
    euro_game_year = "2025", euro_view_mode = mode,
    euro_date_range = as.Date(c("2025-09-01", "2026-07-01")), euro_teams = character(0),
    euro_phase = character(0), euro_opponents = character(0),
    euro_home_away = "", euro_outcome = "",
    euro_opp_rank_side = "", euro_opp_rank_n = "", euro_opp_rank_metric = "",
    euro_num_starters_off_mode = "", euro_num_starters_off = "",
    euro_num_starters_def_mode = "", euro_num_starters_def = "",
    euro_gn_min = "", euro_gn_max = "", euro_last_n = "",
    euro_min_all_poss = 0, euro_min_on_poss = 0
  )
  do.call(session$setInputs, utils::modifyList(inputs, list(...)))
  session$elapse(500)
  session$flushReact()
}

set_stat_filters <- function(session, ...) {
  session$userData$tab8$stat_filter_state$filters(list(...))
  session$flushReact()
}

# The On/Off DataTable is server-side: row data never appears in the widget
# JSON, only the container. Use it for headers (the error table's "Info").
table_text <- function(value) paste(deparse(value), collapse = "")

test_that("Euro On/Off issues no Player Stats query without a Player Stats chip", {
  reset_mock_db_query_counts()
  shiny::testServer(tab8_app(), {
    set_euro_context(session)
    output$euro_dt
    set_stat_filters(session, ps_filter(1L, "Net", "Net RTG Diff", "ge", 10))
    output$euro_dt
    session$userData$tab8$filtered_result()
    expect_identical(mock_db_query_count("euro_player_traditional_mv"), 0L)
    expect_identical(mock_db_query_count("euro_player_traditional_reader"), 0L)
  })
})

test_that("a Player Stats chip reads the euro season MV once, filters by team and player, and reuses the frame", {
  reset_mock_db_query_counts()
  shiny::testServer(tab8_app(), {
    set_euro_context(session)
    tab8 <- session$userData$tab8
    unfiltered <- tab8$filtered_result()$df

    # Player 11 (team 1): pts 100 / gp 5 = 20 pts/g. Player 21 (team 2): 90/5 = 18.
    set_stat_filters(session, ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 19))
    res <- tab8$filtered_result()
    expect_false(res$error)
    expect_identical(as.integer(res$df$player_id), 11L)
    # Narrowing rows never recomputes the league-relative ranks.
    expect_identical(res$df$pr_net, unfiltered$pr_net[unfiltered$player_id == 11])
    expect_identical(mock_db_query_count("euro_player_traditional_mv"), 1L)

    # Threshold edits and ranges reuse the fetched frame.
    set_stat_filters(session, ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 17))
    expect_setequal(as.integer(tab8$filtered_result()$df$player_id), c(11L, 21L))
    set_stat_filters(session,
      ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 17),
      ps_filter(2L, "PTS/G", "ps_pts_pg", "le", 19))
    expect_identical(as.integer(tab8$filtered_result()$df$player_id), 21L)
    expect_identical(mock_db_query_count("euro_player_traditional_mv"), 1L)
    expect_identical(mock_db_query_count("euro_player_traditional_reader"), 0L)
  })
})

test_that("Player Stats and On/Off chips compose with AND in the table data", {
  # The renderer hands exactly these two inputs to onoff_summary_datatable().
  expect_match(read_repo_txt("R", "server_tab8_euro.R"),
               "onoff_summary_datatable(df, onoff_filters, pivot = pivot_targets)", fixed = TRUE)
  shiny::testServer(tab8_app(), {
    set_euro_context(session)
    tab8 <- session$userData$tab8
    table_players <- function() {
      onoff_filters <- split_stat_filters(tab8$stat_filter_state$filters())$onoff
      df <- onoff_clean_display_names(tab8$filtered_result()$df)
      onoff_summary_datatable(df, onoff_filters)$x$data$Player
    }
    set_stat_filters(session, ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 17))
    expect_setequal(table_players(), c("Player A", "Player B"))

    # Team A's Net RTG Diff is 12 (>= 10); Team B's is -2.
    set_stat_filters(session,
      ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 17),
      ps_filter(2L, "Net", "Net RTG Diff", "ge", 10))
    expect_identical(table_players(), "Player A")
  })
})

test_that("a failed Player Stats read shows an error, keeps the chips, and retries", {
  expect_match(read_repo_txt("R", "server_tab8_euro.R"),
               "data.frame(Info = PLAYER_STAT_FILTER_ERROR_TEXT", fixed = TRUE)
  withr::local_options(ibpl.mock_player_traditional_error = TRUE)
  shiny::testServer(tab8_app(), {
    set_euro_context(session)
    tab8 <- session$userData$tab8
    set_stat_filters(session, ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 0))
    res <- tab8$filtered_result()
    expect_true(res$error)
    expect_identical(nrow(res$df), 0L)
    # The table is the one-column Info error table, not the On/Off grid.
    expect_match(table_text(output$euro_dt), "<th>Info", fixed = TRUE)
    expect_length(tab8$stat_filter_state$filters(), 1L)

    options(ibpl.mock_player_traditional_error = FALSE)
    # A different value: reactiveVal ignores an identical set.
    set_stat_filters(session, ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 1))
    expect_false(tab8$filtered_result()$error)
  })
})

test_that("every Euro On/Off view renders with a Player Stats chip, including an empty result", {
  shiny::testServer(tab8_app(), {
    for (mode in c("Summary", "Four Factors")) {
      set_euro_context(session, mode)
      set_stat_filters(session, ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 19))
      expect_silent(txt <- table_text(output$euro_dt))
      expect_false(grepl("<th>Info", txt, fixed = TRUE))
      set_stat_filters(session, ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 1000))
      expect_silent(output$euro_dt)
    }
  })
})

test_that("switching views keeps Player Stats and sample chips and drops view-only ones", {
  shiny::testServer(tab8_app(), {
    set_euro_context(session, "Summary")
    tab8 <- session$userData$tab8
    set_stat_filters(session,
      ps_filter(1L, "Net", "Net RTG Diff", "ge", 0),
      ps_filter(2L, "PTS/G", "ps_pts_pg", "ge", 0),
      ps_filter(3L, "On Poss", "ON Poss", "ge", 0))
    session$setInputs(euro_view_mode = "Four Factors")
    session$flushReact()
    ids <- function() vapply(tab8$stat_filter_state$filters(), `[[`, integer(1), "id")
    expect_identical(ids(), c(2L, 3L))
  })
})

test_that("euro_reset_defaults clears Player Stats chips", {
  shiny::testServer(tab8_app(), {
    set_euro_context(session)
    tab8 <- session$userData$tab8
    set_stat_filters(session, ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 0))
    session$setInputs(euro_reset_defaults = 1)
    session$flushReact()
    expect_length(tab8$stat_filter_state$filters(), 0L)
  })
})

test_that("the Euro chips row groups the menu and discloses the starter-count scope", {
  txt <- read_repo_txt("R", "server_tab8_euro.R")
  expect_match(txt, "choice_groups = onoff_stat_filter_groups(euro_stat_filter_cols())", fixed = TRUE)
  expect_match(txt, "percent_hint = onoff_player_stats_note(starters_active)", fixed = TRUE)
  expect_match(txt, "PLAYER_STAT_STARTERS_CHIP_NOTE", fixed = TRUE)
  expect_no_match(txt, "observeEvent(input$euro_view_mode, {\n    reset_stat_filters", fixed = TRUE)
})

# One row per game-context control the ps_filter_context() reactive forwards
# to fetch_player_traditional_filtered() -> get_player_traditional_pergame().
# `param` is the SQL placeholder ($n) the value must arrive in.
euro_reader_forwarding_cases <- list(
  list(name = "dates", inputs = list(euro_date_range = as.Date(c("2025-11-01", "2026-02-01"))),
       param = 3L, expected = as.Date("2025-11-01")),
  list(name = "phase", inputs = list(euro_phase = "RS"), param = 6L, expected = "RS"),
  list(name = "opponents", inputs = list(euro_opponents = c("3", "4")),
       param = 7L, expected = "3,4"),
  list(name = "round min", inputs = list(euro_gn_min = "2"), param = 13L, expected = 2L)
)

test_that("every game-context control invalidates the frame and reaches the euro reader, forwarding competition and season", {
  reset_mock_db_query_counts()
  shiny::testServer(tab8_app(), {
    tab8 <- session$userData$tab8
    for (i in seq_along(euro_reader_forwarding_cases)) {
      case <- euro_reader_forwarding_cases[[i]]
      # Start from the default (fast-path) context, then change one control.
      set_euro_context(session)
      set_stat_filters(session, ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", i))
      tab8$filtered_result()
      before <- mock_db_query_count("euro_player_traditional_reader")

      do.call(set_euro_context, c(list(session), case$inputs))
      tab8$filtered_result()
      expect_identical(mock_db_query_count("euro_player_traditional_reader"), before + 1L,
                       label = paste(case$name, "reader calls"))

      params <- mock_db_last_params("euro_player_traditional_reader")
      expect_identical(params[[case$param]], case$expected, label = case$name)
      # Never forwarded: no team list.
      expect_identical(params[[1]], "E", label = paste(case$name, "$1 competition"))
      expect_identical(params[[2]], 2025L, label = paste(case$name, "$2 season"))
      expect_identical(params[[5]], NA_character_, label = paste(case$name, "$5 teams"))

      # A threshold edit in the same context reuses the frame.
      set_stat_filters(session, ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", i + 0.5))
      tab8$filtered_result()
      expect_identical(mock_db_query_count("euro_player_traditional_reader"), before + 1L,
                       label = paste(case$name, "reader calls after threshold edit"))
    }
  })
})
