# Player Stats filter chips on the On/Off Impact tab.
# Spec: docs/superpowers/specs/2026-09-15-onoff-player-stat-filters-design.md

ps_filter <- function(id, label, col, op = "ge", value = 0) {
  list(id = id, label = label, col = col, op = op, value = value)
}

trad_rows <- function() {
  data.frame(
    player_id = c(11L, 11L, 12L, 13L),
    team_id = c(1L, 2L, 1L, 1L),
    team_name = c("Team A", "Team B", "Team A", "Team A"),
    Player = c("Player A", "Player A", "Player C", "Player Z"),
    gp = c(5L, 2L, 4L, 0L),
    minutes = c(150, 40, 120, 12),
    pts = c(100, 30, 72, 4),
    reb = c(40, 8, 24, 1),
    ast = c(30, 2, 18, 0),
    stl = c(10, 1, 7, 0),
    blk = c(6, 0, 4, 0),
    tov = c(14, 3, 11, 1),
    fga = c(70, 25, 55, 3),
    fta = c(16, 4, 10, 0),
    tp_pct = c(38, 25, 36, NA),
    efg = c(62.8, 50, 58.2, 33.3),
    ts = c(60, 52, 58, 40),
    usg_pct = c(24.1, 18, 20.5, 10),
    poss_on_floor = c(300, 80, 220, 20),
    check.names = FALSE
  )
}

onoff_rows <- function() {
  data.frame(
    Team = c("Team A", "Team B", "Team A"),
    team_id = c(1, 2, 1),
    player_id = c(11, 11, 99),
    `Net RTG Diff` = c(5, -2, 7),
    pr_net = c(0.9, 0.2, 0.95),
    check.names = FALSE
  )
}

# ---- classification -------------------------------------------------------

test_that("Player Stats filters are classified by the ps_ column namespace, not the label", {
  expect_true(is_player_stat_filter(ps_filter(1L, "PTS/G", "ps_pts_pg")))
  expect_false(is_player_stat_filter(ps_filter(2L, "Net", "Net RTG Diff")))
  # A Player Stats-looking label does not make a filter one.
  expect_false(is_player_stat_filter(ps_filter(3L, "TS%", "ts")))
  expect_false(is_player_stat_filter(list(id = 4L, label = "no col")))
  expect_false(has_player_stat_filters(list()))
  expect_true(has_player_stat_filters(list(
    ps_filter(2L, "Net", "Net RTG Diff"), ps_filter(1L, "PTS/G", "ps_pts_pg")
  )))
})

test_that("split_stat_filters separates a mixed list without losing ids or order", {
  filters <- list(
    ps_filter(1L, "Net", "Net RTG Diff"), ps_filter(2L, "PTS/G", "ps_pts_pg"),
    ps_filter(3L, "On Poss", "ON Poss"), ps_filter(4L, "TS%", "ps_ts_pct")
  )
  out <- split_stat_filters(filters)
  expect_identical(vapply(out$player_stats, `[[`, integer(1), "id"), c(2L, 4L))
  expect_identical(vapply(out$onoff, `[[`, integer(1), "id"), c(1L, 3L))
  expect_identical(split_stat_filters(list()), list(player_stats = list(), onoff = list()))
})

test_that("the Player Stats menu adds unique labels to every On/Off view menu", {
  expect_identical(unname(PLAYER_STAT_FILTERABLE_COLS), names(PLAYER_STAT_FILTER_SOURCE))
  expect_true(all(startsWith(unname(PLAYER_STAT_FILTERABLE_COLS), "ps_")))
  for (menu in list(ONOFF_SUMMARY_FILTERABLE_COLS, ONOFF_FF_FILTERABLE_COLS, ON_SP_FILTERABLE_COLS)) {
    combined <- c(menu, PLAYER_STAT_FILTERABLE_COLS)
    expect_identical(anyDuplicated(names(combined)), 0L)
    expect_true(all(ONOFF_SAMPLE_FILTER_COLS %in% unname(menu)))
  }
})

# ---- projection ------------------------------------------------------------

test_that("the projection derives per-game values with the Player Stats tab's formulas", {
  out <- player_stat_filter_projection(trad_rows(), 2026)
  expect_named(out, c("game_year", "team_id", "player_id", names(PLAYER_STAT_FILTER_SOURCE)))
  a1 <- out[out$team_id == 1L & out$player_id == 11L, ]
  expect_identical(a1$game_year, 2026L)
  expect_equal(a1$ps_gp, 5)
  expect_equal(a1$ps_min_pg, 30)
  expect_equal(a1$ps_pts_pg, 20)
  expect_equal(a1$ps_reb_pg, 8)
  expect_equal(a1$ps_ast_pg, 6)
  expect_equal(a1$ps_stl_pg, 2)
  expect_equal(a1$ps_blk_pg, 1.2)
  # Percentages stay on the displayed 0-100 scale.
  expect_equal(a1$ps_3p_pct, 38)
  expect_equal(a1$ps_efg_pct, 62.8)
  expect_equal(a1$ps_ts_pct, 60)
  expect_equal(a1$ps_usg_pct, 24.1)
})

test_that("round_display_1 matches DT::formatRound(x, 1), i.e. JavaScript toFixed(1)", {
  # Expected strings were produced by node: (x).toFixed(1). Base round() gets
  # the first two wrong (10.2, 2.0) and 0.05 wrong (0.0).
  x <- c(41 / 4, 1.95, 0.05, 100.6 / 10, 10.05, 12.35, 1.45, 0.15, 43 / 4, -10.25, 38.1, 1.2)
  js <- c("10.3", "1.9", "0.1", "10.1", "10.1", "12.3", "1.4", "0.1", "10.8", "-10.3", "38.1", "1.2")
  expect_identical(sprintf("%.1f", round_display_1(x)), js)
  expect_identical(round_display_1(c(NA, NaN, Inf)), c(NA_real_, NA_real_, NA_real_))
  expect_identical(round_display_1(numeric(0)), numeric(0))
})

test_that("thresholds compare the displayed one-decimal value, not the raw average", {
  rows <- trad_rows()[1, ]
  rows$pts <- 41          # 41 / 4 = 10.25, displayed 10.3
  rows$gp <- 4L
  rows$minutes <- 40.24   # 40.24 / 4 = 10.06, displayed 10.1
  ps <- player_stat_filter_projection(rows, 2026)
  expect_equal(ps$ps_pts_pg, 10.3)
  expect_equal(ps$ps_min_pg, 10.1)
  onoff <- data.frame(team_id = 1, player_id = 11)
  keeps <- function(col, op, value) {
    nrow(apply_player_stat_filters(onoff, ps, list(ps_filter(1L, "x", col, op, value)), 2026)$df) == 1L
  }
  # The remark's example: a raw 10.06 would fail >= 10.1; the displayed 10.1 passes.
  expect_true(keeps("ps_min_pg", "ge", 10.1))
  expect_false(keeps("ps_min_pg", "ge", 10.2))
  expect_true(keeps("ps_pts_pg", "ge", 10.3))
  expect_false(keeps("ps_pts_pg", "ge", 10.4))
  expect_true(keeps("ps_pts_pg", "le", 10.3))
  expect_false(keeps("ps_pts_pg", "le", 10.2))
})

test_that("zero games played gives NA per-game values, never zero", {
  out <- player_stat_filter_projection(trad_rows(), 2026)
  z <- out[out$player_id == 13L, ]
  expect_equal(z$ps_gp, 0)
  per_game <- c("ps_min_pg", "ps_pts_pg", "ps_reb_pg", "ps_ast_pg", "ps_stl_pg", "ps_blk_pg")
  expect_true(all(is.na(unlist(z[per_game]))))
  expect_true(is.na(z$ps_3p_pct))
})

test_that("an empty Player Stats frame projects to an empty frame with every column", {
  out <- player_stat_filter_projection(trad_rows()[0, ], 2026)
  expect_identical(nrow(out), 0L)
  expect_named(out, c("game_year", "team_id", "player_id", names(PLAYER_STAT_FILTER_SOURCE)))
})

# ---- join and filter -------------------------------------------------------

test_that("the join is keyed on team and player, so a traded player is filtered per team", {
  ps <- player_stat_filter_projection(trad_rows(), 2026)
  joined <- join_player_stat_filter_frame(onoff_rows(), ps, 2026)
  expect_equal(joined$ps_pts_pg, c(20, 15, NA))
  expect_identical(names(joined)[1:5], names(onoff_rows()))
  # Ranks computed on the full population ride along untouched.
  expect_identical(joined$pr_net, onoff_rows()$pr_net)
})

test_that("the join ignores Player Stats rows from another season", {
  ps <- player_stat_filter_projection(trad_rows(), 2025)
  joined <- join_player_stat_filter_frame(onoff_rows(), ps, 2026)
  expect_true(all(is.na(joined$ps_pts_pg)))
})

test_that("the join tolerates a frame without id columns", {
  ps <- player_stat_filter_projection(trad_rows(), 2026)
  joined <- join_player_stat_filter_frame(data.frame(x = 1:2), ps, 2026)
  expect_true(all(is.na(joined$ps_gp)))
})

test_that("Player Stats filters fail closed on unmatched rows and form ranges", {
  ps <- player_stat_filter_projection(trad_rows(), 2026)
  ge10 <- ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 10)
  res <- apply_player_stat_filters(onoff_rows(), ps, list(ge10), 2026)
  expect_false(res$error)
  # Player 99 has no Player Stats row, so an active chip excludes it.
  expect_equal(res$df$ps_pts_pg, c(20, 15))
  range <- list(ge10, ps_filter(2L, "PTS/G", "ps_pts_pg", "le", 18))
  expect_equal(apply_player_stat_filters(onoff_rows(), ps, range, 2026)$df$team_id, 2)
})

test_that("with no Player Stats filters the frame passes through untouched", {
  res <- apply_player_stat_filters(onoff_rows(), NULL, list(), 2026)
  expect_identical(res, list(df = onoff_rows(), error = FALSE))
})

test_that("a missing Player Stats frame is an error, never an unfiltered table", {
  f <- list(ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 10))
  res <- apply_player_stat_filters(onoff_rows(), NULL, f, 2026)
  expect_true(res$error)
  expect_identical(nrow(res$df), 0L)
  expect_true(apply_player_stat_filters(onoff_rows(), data.frame(), f, 2026)$error)
})

# ---- menus and view switches -------------------------------------------------

test_that("switching views keeps Player Stats and sample-size chips and drops view-only ones", {
  filters <- list(
    ps_filter(1L, "Net", "Net RTG Diff"), ps_filter(2L, "PTS/G", "ps_pts_pg"),
    ps_filter(3L, "Min", "minutes"), ps_filter(4L, "On Off 2PT%", "on_off_fg2_pct")
  )
  out <- retain_stat_filters_for_cols(filters, c(ONOFF_FF_FILTERABLE_COLS, PLAYER_STAT_FILTERABLE_COLS))
  expect_identical(vapply(out$kept, `[[`, integer(1), "id"), c(2L, 3L))
  expect_identical(vapply(out$dropped, `[[`, integer(1), "id"), c(1L, 4L))
  expect_identical(
    retain_stat_filters_for_cols(list(), ONOFF_FF_FILTERABLE_COLS),
    list(kept = list(), dropped = list())
  )
})

test_that("the On/Off menu groups results, sample size and Player Stats", {
  cols <- c(ONOFF_FF_FILTERABLE_COLS, PLAYER_STAT_FILTERABLE_COLS)
  groups <- onoff_stat_filter_groups(cols)
  expect_named(groups, c("Impact and on/off results", "Sample size", "Player Stats"))
  expect_identical(groups[["Sample size"]], ONOFF_SAMPLE_FILTER_COLS)
  expect_identical(groups[["Player Stats"]], unname(PLAYER_STAT_FILTERABLE_COLS))
  expect_false(any(c(ONOFF_SAMPLE_FILTER_COLS, unname(PLAYER_STAT_FILTERABLE_COLS)) %in% groups[[1]]))

  choices <- stat_filter_choices(cols, groups)
  expect_identical(choices[["Choose..."]], "")
  expect_identical(names(choices[["Player Stats"]]), names(PLAYER_STAT_FILTERABLE_COLS))
  expect_identical(unlist(choices[["Sample size"]], use.names = FALSE), c("Min", "On Poss", "Off Poss"))
})

test_that("without groups the menu is the flat list it always was", {
  cols <- ONOFF_FF_FILTERABLE_COLS
  expect_identical(stat_filter_choices(cols), c("Choose..." = "", names(cols)))
})

test_that("the real stat-filter popover renders the groups as optgroups", {
  # helper-server-mocks.R stubs stat_filter_chips_ui(); load the real one.
  real <- new.env()
  sys.source(repo_file("R", "helpers.R"), envir = real)
  state <- list(filters = shiny::reactiveVal(list()), next_id = shiny::reactiveVal(1L))
  cols <- c(ONOFF_SUMMARY_FILTERABLE_COLS, PLAYER_STAT_FILTERABLE_COLS)
  ui <- shiny::isolate(real$stat_filter_chips_ui(
    "on", state, cols,
    percent_hint = onoff_player_stats_note(TRUE),
    choice_groups = onoff_stat_filter_groups(cols)
  ))
  html <- htmltools::renderTags(htmltools::tagList(ui))$html
  expect_match(html, '<optgroup label="Player Stats">', fixed = TRUE)
  expect_match(html, '<option value="PTS/G">PTS/G</option>', fixed = TRUE)
  expect_match(html, "not Player Stats.", fixed = TRUE)
})

# ---- starter-count disclosure --------------------------------------------------

test_that("the starter-count disclosure appears only when a starter restriction is active", {
  expect_identical(onoff_player_stats_note(FALSE),
                   "Player Stats use the selected games. Percentages use 0-100.")
  expect_match(onoff_player_stats_note(TRUE),
               "Starter-count filters affect On/Off possessions, not Player Stats.", fixed = TRUE)
  none <- list(num_starters_off_mode = "", num_starters_off = "",
               num_starters_def_mode = "", num_starters_def = "")
  expect_false(onoff_starter_restriction_active(none))
  expect_false(onoff_starter_restriction_active(list()))
  expect_false(onoff_starter_restriction_active(modifyList(none, list(num_starters_off_mode = "gte"))))
  expect_true(onoff_starter_restriction_active(
    modifyList(none, list(num_starters_def_mode = "lte", num_starters_def = "2"))
  ))
  dropped <- onoff_drop_starter_filters(
    modifyList(none, list(num_starters_off_mode = "gte", num_starters_off = "3", home_away = "home"))
  )
  expect_false(onoff_starter_restriction_active(dropped))
  expect_identical(dropped$home_away, "home")
})

# ---- frame fetch -----------------------------------------------------------------

ps_ctx <- function(...) {
  utils::modifyList(list(
    game_year = 2026L, fast = TRUE,
    start_d = as.Date("2025-10-01"), end_d = as.Date("2026-07-01"),
    game_type_csv = NA_character_, opp_ids_csv = NA_character_,
    home_away = NA_character_, outcome = NA_character_,
    opp_rank_side = NA_character_, opp_rank_n = NA_integer_, opp_rank_metric = NA_character_,
    min_gn = NA_integer_, max_gn = NA_integer_, last_n_games = NA_integer_,
    data_version = "v1"
  ), list(...))
}

test_that("the filter frame reads the season MV on the fast path and the per-game reader otherwise", {
  reset_mock_db_query_counts()
  fast <- fetch_player_stat_filter_frame(pg_pool, ps_ctx(), session = NULL)
  expect_identical(mock_db_query_count("player_traditional_mv"), 1L)
  expect_identical(mock_db_query_count("player_traditional_reader"), 0L)
  expect_equal(fast$ps_pts_pg[fast$team_id == 1L & fast$player_id == 11L], 20)

  slow <- fetch_player_stat_filter_frame(pg_pool, ps_ctx(fast = FALSE), session = NULL)
  expect_identical(mock_db_query_count("player_traditional_reader"), 1L)
  expect_equal(slow$ps_pts_pg[order(slow$player_id)], fast$ps_pts_pg[order(fast$player_id)])
})

test_that("a failed frame read returns NULL", {
  withr::local_options(ibpl.mock_player_traditional_error = TRUE)
  expect_null(fetch_player_stat_filter_frame(pg_pool, ps_ctx(), session = NULL))
  expect_null(fetch_player_stat_filter_frame(pg_pool, ps_ctx(fast = FALSE), session = NULL))
})

test_that("the shared frame fetcher uses the pool it is given (review remark 4)", {
  src <- paste(deparse(fetch_player_stat_filter_frame), collapse = "\n")
  expect_false(grepl("pg_pool", src, fixed = TRUE))
  sentinel <- structure(list(id = "sentinel"), class = "mock_pool")
  reset_mock_db_query_counts()
  fetch_player_stat_filter_frame(sentinel, ps_ctx(fast = TRUE), session = NULL)
  expect_identical(mock_db_last_params("player_traditional_mv_pool"), sentinel)
  fetch_player_stat_filter_frame(sentinel, ps_ctx(fast = FALSE), session = NULL)
  expect_identical(mock_db_last_params("player_traditional_reader_pool"), sentinel)
})

# ---- Tab 1 server --------------------------------------------------------------

tab1_app <- function(input, output, session) {
  session$userData$tab1 <- server_tab1(input, output, session, shared = make_shared())
}

# Season-bounds dates (2025-10-01 .. 2026-07-01 in make_shared) keep the fast path.
set_onoff_context <- function(session, mode = "Summary", ...) {
  inputs <- list(
    main_tabs = "onoff", game_year = "2026", onoff_view_mode = mode,
    date_range = as.Date(c("2025-10-01", "2026-07-01")), teams = character(0),
    on_game_type = character(0), on_opponents = character(0),
    on_home_away = "", on_outcome = "",
    on_opp_rank_side = "", on_opp_rank_n = "", on_opp_rank_metric = "",
    on_num_starters_off_mode = "", on_num_starters_off = "",
    on_num_starters_def_mode = "", on_num_starters_def = "",
    on_gn_min = "", on_gn_max = "", on_last_n = "",
    min_all_poss = 0, min_on_poss = 0
  )
  do.call(session$setInputs, utils::modifyList(inputs, list(...)))
  session$elapse(500)
  session$flushReact()
}

set_stat_filters <- function(session, ...) {
  session$userData$tab1$stat_filter_state$filters(list(...))
  session$flushReact()
}

# The On/Off DataTable is server-side: row data never appears in the widget
# JSON, only the container. Use it for headers (the error table's "Info").
table_text <- function(value) paste(deparse(value), collapse = "")

test_that("On/Off issues no Player Stats query without a Player Stats chip", {
  reset_mock_db_query_counts()
  shiny::testServer(tab1_app, {
    set_onoff_context(session)
    output$onoff_dt
    set_stat_filters(session, ps_filter(1L, "Net", "Net RTG Diff", "ge", 10))
    output$onoff_dt
    session$userData$tab1$filtered_result()
    expect_identical(mock_db_query_count("player_traditional_mv"), 0L)
    expect_identical(mock_db_query_count("player_traditional_reader"), 0L)
  })
})

test_that("a Player Stats chip reads once, filters by team and player, and keeps ranks", {
  reset_mock_db_query_counts()
  shiny::testServer(tab1_app, {
    set_onoff_context(session)
    tab1 <- session$userData$tab1
    unfiltered <- tab1$filtered_result()$df

    set_stat_filters(session, ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 19))
    res <- tab1$filtered_result()
    expect_false(res$error)
    expect_identical(as.integer(res$df$player_id), 11L)
    # Narrowing rows never recomputes the league-relative ranks.
    expect_identical(res$df$pr_net, unfiltered$pr_net[unfiltered$player_id == 11])
    expect_identical(mock_db_query_count("player_traditional_mv"), 1L)

    # Threshold edits and ranges reuse the fetched frame.
    set_stat_filters(session, ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 17))
    expect_setequal(as.integer(tab1$filtered_result()$df$player_id), c(11L, 21L))
    set_stat_filters(session,
      ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 17),
      ps_filter(2L, "PTS/G", "ps_pts_pg", "le", 19))
    expect_identical(as.integer(tab1$filtered_result()$df$player_id), 21L)
    expect_identical(mock_db_query_count("player_traditional_mv"), 1L)
    expect_identical(mock_db_query_count("player_traditional_reader"), 0L)
  })
})

test_that("Player Stats and On/Off chips compose with AND in the table data", {
  # The renderer hands exactly these two inputs to onoff_summary_datatable().
  expect_match(read_repo_txt("R", "server_tab1.R"),
               "onoff_summary_datatable(df, onoff_filters, pivot = pivot_targets)", fixed = TRUE)
  shiny::testServer(tab1_app, {
    set_onoff_context(session)
    tab1 <- session$userData$tab1
    table_players <- function() {
      onoff_filters <- split_stat_filters(tab1$stat_filter_state$filters())$onoff
      df <- onoff_clean_display_names(tab1$filtered_result()$df)
      onoff_summary_datatable(df, onoff_filters)$x$data$Player
    }
    set_stat_filters(session, ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 17))
    expect_setequal(table_players(), c("Player A", "Player B"))

    set_stat_filters(session,
      ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 17),
      ps_filter(2L, "Net", "Net RTG Diff", "ge", 10))
    expect_identical(table_players(), "Player A")
  })
})

test_that("starter counts and teams reuse the Player Stats frame", {
  reset_mock_db_query_counts()
  shiny::testServer(tab1_app, {
    set_onoff_context(session)
    tab1 <- session$userData$tab1
    set_stat_filters(session, ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 0))
    tab1$filtered_result()
    expect_identical(mock_db_query_count("player_traditional_mv"), 1L)

    # Starter counts restrict On/Off segments, not the games Player Stats read.
    set_onoff_context(session, on_num_starters_off_mode = "gte", on_num_starters_off = "3")
    tab1$filtered_result()
    # The reader's team filter only drops team-game rows, so a team change
    # cannot alter a team-player row; the frame is reused.
    set_onoff_context(session, on_num_starters_off_mode = "gte", on_num_starters_off = "3", teams = "1")
    tab1$filtered_result()
    expect_identical(mock_db_query_count("player_traditional_mv"), 1L)
    expect_identical(mock_db_query_count("player_traditional_reader"), 0L)
  })
})

test_that("a failed Player Stats read shows an error, keeps the chips, and retries", {
  expect_match(read_repo_txt("R", "server_tab1.R"),
               "data.frame(Info = PLAYER_STAT_FILTER_ERROR_TEXT", fixed = TRUE)
  withr::local_options(ibpl.mock_player_traditional_error = TRUE)
  shiny::testServer(tab1_app, {
    set_onoff_context(session)
    tab1 <- session$userData$tab1
    set_stat_filters(session, ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 0))
    res <- tab1$filtered_result()
    expect_true(res$error)
    expect_identical(nrow(res$df), 0L)
    # The table is the one-column Info error table, not the On/Off grid.
    expect_match(table_text(output$onoff_dt), "<th>Info", fixed = TRUE)
    expect_length(tab1$stat_filter_state$filters(), 1L)

    options(ibpl.mock_player_traditional_error = FALSE)
    # A different value: reactiveVal ignores an identical set.
    set_stat_filters(session, ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 1))
    expect_false(tab1$filtered_result()$error)
  })
})

test_that("every On/Off view renders with a Player Stats chip, including an empty result", {
  shiny::testServer(tab1_app, {
    for (mode in c("Summary", "Four Factors", "Shot Profile")) {
      set_onoff_context(session, mode)
      set_stat_filters(session, ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 19))
      expect_silent(txt <- table_text(output$onoff_dt))
      expect_false(grepl("<th>Info", txt, fixed = TRUE))
      set_stat_filters(session, ps_filter(1L, "PTS/G", "ps_pts_pg", "ge", 1000))
      expect_silent(output$onoff_dt)
    }
  })
})

test_that("auto min-possessions still reads the unfiltered On/Off sources", {
  txt <- read_repo_txt("R", "server_tab1.R")
  start <- regexpr("setup_onoff_auto_min(", txt, fixed = TRUE)
  end <- regexpr("setup_gn_last_n_sync(", txt, fixed = TRUE)
  expect_gt(start, 0)
  expect_false(grepl("ps_filter|player_stat|filtered_result", substr(txt, start, end)))
})
