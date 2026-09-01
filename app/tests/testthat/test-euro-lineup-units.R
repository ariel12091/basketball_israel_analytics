test_that("auto_minposs_from_df reproduces the Tab 2 behaviour it replaces", {
  # Fewer rows than the target: no threshold is needed.
  small <- data.frame(total_poss = c(500, 400, 300))
  expect_identical(auto_minposs_from_df(small, target_rows = 150L), 0L)

  # More rows than the target: the kth largest value, rounded up to the step.
  many <- data.frame(total_poss = seq(1000, 1, by = -1))
  expect_identical(
    auto_minposs_from_df(many, target_rows = 10L, step = 10L),
    as.integer(ceiling(991 / 10) * 10)
  )

  # Empty, NULL, and missing-column inputs are NA, never an error.
  expect_true(is.na(auto_minposs_from_df(NULL)))
  expect_true(is.na(auto_minposs_from_df(data.frame())))
  expect_true(is.na(auto_minposs_from_df(data.frame(other = 1:3))))

  # Non-finite values are dropped before ranking.
  mixed <- data.frame(total_poss = c(100, NA, Inf, 50, 25))
  expect_identical(auto_minposs_from_df(mixed, target_rows = 2L, step = 10L), 50L)

  # The Tab 2 default target must survive the move to helpers.R.
  expect_identical(formals(auto_minposs_from_df)$target_rows, 150L)
})

test_that("EuroLeague lineups use the shared blank-aware fast-path gate", {
  server <- paste(
    readLines(testthat::test_path("..", "..", "R", "server_tab10_euro_lineups.R"),
              warn = FALSE),
    collapse = "\n"
  )

  expect_match(server, "onoff_fallback_needed\\(")
  expect_false(grepl('euro_ld_home_away %\\|\\|% "all"', server))
  expect_false(grepl('euro_ld_outcome %\\|\\|% "all"', server))
})

test_that("EuroLeague auto min possessions follows the filtered Tab 2 population", {
  server <- paste(
    readLines(testthat::test_path("..", "..", "R", "server_tab10_euro_lineups.R"),
              warn = FALSE),
    collapse = "\n"
  )

  # The activation gate sits between the observer head and the population,
  # so this pins both invariants: Home must not pay for Tab 10 at startup,
  # and the threshold is still computed on the filtered population.
  expect_match(
    server,
    paste0(
      "observeEvent\\(euro_ld_auto_inputs\\(\\), \\{\\s+",
      "req\\(identical\\(input\\$main_tabs, .euro_lineups.\\)\\)\\s+",
      "df <- apply_local_unit_filters\\(euro_ld_full\\(\\)\\)"
    ),
    perl = TRUE
  )
  for (input_id in c(
    "euro_ld_opp_rank_side", "euro_ld_opp_rank_n", "euro_ld_opp_rank_metric",
    "euro_ld_view_mode", "euro_ld_num_starters_off_mode",
    "euro_ld_num_starters_off", "euro_ld_num_starters_def_mode",
    "euro_ld_num_starters_def", "euro_ld_clutch_enabled",
    "euro_ld_clutch_margin", "euro_ld_clutch_status",
    "euro_ld_clutch_minutes", "euro_ld_clutch_ot_margin"
  )) {
    expect_match(server, paste0("input\\$", input_id), fixed = FALSE)
  }

  enable_pos <- regexpr("# Register this before the calculation observer", server,
                        fixed = TRUE)[[1]]
  calculate_pos <- regexpr("observeEvent(euro_ld_auto_inputs()", server,
                           fixed = TRUE)[[1]]
  expect_gt(enable_pos, 0L)
  expect_gt(calculate_pos, enable_pos)
})

test_that("Tab 10 does not pull the season units on sessions that never open it", {
  server <- paste(
    readLines(testthat::test_path("..", "..", "R", "server_tab10_euro_lineups.R"),
              warn = FALSE),
    collapse = "\n"
  )

  # euro_ld_auto_inputs() is the trigger expression of an observeEvent, so it
  # is evaluated on EVERY session -- observers are never suspended by tab
  # visibility the way outputs are. Referencing euro_ld_full() here pulled the
  # whole EuroLeague season from sub_lineups_stats_mv (measured 2.6s) ahead of
  # the Home tab own query, on sessions that never opened Tab 10.
  start <- regexpr("euro_ld_auto_inputs <- reactive({", server, fixed = TRUE)[[1]]
  expect_gt(start, 0)
  rest <- substring(server, start)
  trigger <- substring(rest, 1, regexpr("})", rest, fixed = TRUE)[[1]])

  expect_false(grepl("euro_ld_full(", trigger, fixed = TRUE))
  expect_true(grepl("input$main_tabs", trigger, fixed = TRUE))
})
