# Contract tests for AM_player_id_display_name_gaps: player id problems that
# leave a player without an identity-backed display name. One function,
# player_display_name_gaps() in etl/player_identity_dictionary.R, serves both
# the data quality report and the nightly ETL's Phase 6 warning.

dq_text <- function(...) {
  paste(readLines(repo_file("..", ...), warn = FALSE, encoding = "UTF-8"), collapse = "\n")
}

test_that("the report and the ETL both call the one shared check", {
  dq_r <- dq_text("etl", "run_data_quality_report.R")
  am <- sub(".*AM_player_id_display_name_gaps", "", dq_r)
  am <- sub("AL_reader_lineup_hash_unresolved_in_on_table.*", "", am)
  expect_true(grepl('severity = "warning"', am, fixed = TRUE))
  expect_true(grepl("player_display_name_gaps(con, schema = schema)", am, fixed = TRUE))

  etl <- dq_text("etl", "etl_full.R")
  expect_true(grepl("player_display_name_gaps(pg, SCHEMA, years)", etl, fixed = TRUE))
  # A warning, never a phase failure.
  block <- sub(".*name_gaps <- tryCatch", "", etl)
  block <- sub("if \\(warn_count == 0\\).*", "", block)
  expect_false(grepl("mark_phase_failed", block, fixed = TRUE))
})

test_that("the check reads the display-name owner and counts Basket's games", {
  fn <- dq_text("etl", "player_identity_dictionary.R")
  fn <- sub(".*player_display_name_gaps <- function", "", fn)
  expect_true(grepl("player_display_names_v", fn, fixed = TRUE))
  expect_true(grepl("basket_player_season", fn, fixed = TRUE))
  # Basket's season pages count regular-season games only.
  expect_true(grepl("s.game_type = 5", fn, fixed = TRUE))
  for (p in c("reused_provider_id", "no_identity_roster_spelling",
              "played_without_basket_link", "basket_registration_unlinked")) {
    expect_true(grepl(p, fn, fixed = TRUE), info = p)
  }
})

test_that("the findings page describes and tiers every problem", {
  env <- new.env()
  sys.source(repo_file("..", "etl", "dq_findings_html.R"), envir = env)
  entry <- env$DQ_FINDING_CATALOG$AM_player_id_display_name_gaps
  expect_false(is.null(entry))
  ctx <- list(teams = data.frame(team_id = 33L, team_name = "MACCABI ASHDOD"))
  row <- function(problem, player_id = 2306L, detail = "ELIYA NAKAR x6 | WYAL NAKAR x1") {
    data.frame(game_year = 2027L, team_id = 33L, player_id = player_id,
               display_name = "ELIYA NAKAR", problem = problem, detail = detail)
  }
  expect_identical(entry$tier_fn(row("no_identity_roster_spelling")), "medium")
  expect_identical(entry$tier_fn(row("reused_provider_id")), "medium")
  expect_identical(entry$tier_fn(row("played_without_basket_link")), "low")
  expect_identical(entry$tier_fn(row("basket_registration_unlinked", NA_integer_)), "low")
  text <- entry$describe(row("no_identity_roster_spelling"), ctx)
  expect_match(text, "ELIYA NAKAR (MACCABI ASHDOD, 2027)", fixed = TRUE)
  expect_match(text, "WYAL NAKAR x1", fixed = TRUE)
  expect_match(entry$describe(row("played_without_basket_link", detail = "3 regular-season game(s) played, last game 429"), ctx),
               "no basket.co.il registration is linked", fixed = TRUE)
})
