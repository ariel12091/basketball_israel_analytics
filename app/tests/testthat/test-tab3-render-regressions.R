team_rating_inputs <- function(session, mode) {
  session$setInputs(
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

dt_output_text <- function(value) {
  paste(
    c(
      capture.output(print(value)),
      capture.output(str(value, max.level = 4))
    ),
    collapse = "\n"
  )
}

expect_team_rating_table <- function(output_text) {
  expect_true(nzchar(output_text))
  expect_false(grepl("Team Ratings render error", output_text, fixed = TRUE))
  expect_false(grepl("arguments imply differing number of rows", output_text, fixed = TRUE))
  expect_false(grepl("no data for current filters", output_text, fixed = TRUE))
}

test_that("team ratings four factors render without gp or minutes columns", {
  shiny::testServer(function(input, output, session) {
    server_tab3(input, output, session, shared = make_shared())
  }, {
    team_rating_inputs(session, "Four Factors")

    expect_silent(rendered <- output$tr_table)
    expect_team_rating_table(dt_output_text(rendered))
  })
})

test_that("team ratings minutes helper does not exit before adding minutes", {
  txt <- read_repo_txt("R", "helpers.R")
  fn <- regmatches(
    txt,
    regexpr("add_team_pace_cols <- function\\([\\s\\S]*?\\n\\}", txt, perl = TRUE)
  )

  expect_true(grepl("df\\$minutes\\s*<-", fn))
  expect_false(grepl("if \\(is\\.na\\(gp_col\\)\\) return\\(df\\)", fn))
})

test_that("team ratings traditional render does not require AST% experiment fields", {
  shiny::testServer(function(input, output, session) {
    server_tab3(input, output, session, shared = make_shared())
  }, {
    team_rating_inputs(session, "Traditional")

    expect_silent(rendered <- output$tr_table)
    output_text <- dt_output_text(rendered)
    expect_team_rating_table(output_text)
    expect_true(grepl("DFL", output_text, fixed = TRUE))
  })
})

test_that("AST% experiment fields stay out until the DB layer supports them", {
  scan_dirs <- c(
    repo_file("R"),
    repo_file("..", "sql", "functions"),
    repo_file("..", "sql", "materialized_views")
  )
  paths <- unlist(lapply(scan_dirs, list.files, pattern = "\\.(R|sql)$", full.names = TRUE))
  txt <- vapply(paths, function(path) paste(readLines(path, warn = FALSE), collapse = "\n"), character(1))
  hits <- grep("AST%|ast_pct|shooting_foul_ft_trips|parameters_kind", txt, perl = TRUE)

  expect_equal(length(hits), 0, info = paste(basename(paths[hits]), collapse = ", "))
})
