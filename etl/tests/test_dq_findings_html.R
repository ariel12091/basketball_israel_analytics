# Unit tests for etl/dq_findings_html.R (no DB).
# Run: Rscript -e "testthat::test_file('etl/tests/test_dq_findings_html.R')"
library(testthat)
Sys.setenv(DQ_NO_AUTORUN = "true")
source("../run_data_quality_report.R")
source("../dq_findings_html.R")

# build_checks() only quotes identifiers and strings, so a connection that
# can do just that lists every check without a database.
setClass("DqQuotingCon", contains = "DBIConnection")
setMethod("dbQuoteIdentifier", c("DqQuotingCon", "ANY"), function(conn, x, ...) {
  DBI::SQL(paste0('"', paste(x@name, collapse = '"."'), '"'))
})
setMethod("dbQuoteString", c("DqQuotingCon", "character"), function(conn, x, ...) {
  DBI::SQL(paste0("'", x, "'"))
})

test_that("every check in the report has a findings catalog entry", {
  ids <- vapply(build_checks(new("DqQuotingCon"), "basketball_test"), `[[`, character(1), "id")
  expect_length(setdiff(ids, names(DQ_FINDING_CATALOG)), 0L)
  expect_true(all(vapply(DQ_FINDING_CATALOG, function(e) e$tier %in% DQ_TIERS, logical(1))))
})

fixture <- function() {
  summary_df <- data.frame(
    check_id = c("V_team_game_score_reconciliation", "AB_clock_order_jitter",
                 "C_active_correction_residue_game_scoped_tables", "B_same_roster_name_multiple_player_ids",
                 "G_cleaned_action_duplicate_ids"),
    severity = c("error", "warning", "error", "warning", "error"),
    status = c("fail", "warning", "fail", "warning", "pass"),
    row_count = c(1, 1, 1, 1, 0), issue_count = c(1, 1, 1, 1, 0),
    title = c("Scores", "Jitter", "Residue", "Names", "Duplicates"),
    detail_file = "", error_message = NA_character_, stringsAsFactors = FALSE
  )
  details <- list(
    V_team_game_score_reconciliation = data.frame(
      game_id = 139L, team_id = 6L, team_name = "Bnei Herzliya", team_score = 89,
      traditional_points = 88, lineup_offense_points = 88, opp_score = 79, lineup_defense_points = 79
    ),
    AB_clock_order_jitter = data.frame(game_id = 500L, reversal_rows = 1, max_reversal_seconds = 9,
                                       action_transitions = "5000001->5000002"),
    C_active_correction_residue_game_scoped_tables = data.frame(
      source_table = "full_rosters", team_id = 9L, alias_player_id = 2052L, canonical_player_id = 1110L,
      rows = 13, games = 13
    ),
    B_same_roster_name_multiple_player_ids = data.frame(
      team_id = 9L, player_name = "NOAM AVIVI", distinct_player_ids = 2, id_games = "1110 [365] | 2052 [166]"
    )
  )
  ctx <- list(
    games = data.frame(game_id = c(139L, 139L), game_date = as.Date("2025-11-02"), team_id = c(6L, 13L),
                       team_name = c("Bnei Herzliya", "M. Raanana"), team_score = c(89, 79), is_home = c(TRUE, FALSE)),
    teams = data.frame(team_id = c(6L, 9L, 13L), team_name = c("Bnei Herzliya", "Ness Ziona", "M. Raanana")),
    players = data.frame(team_id = 9L, player_id = 1110L, firstname = "Noam", lastname = "Avivi")
  )
  list(summary = summary_df, details = details, ctx = ctx)
}

test_that("findings are ordered worst first and passing checks are left out", {
  f <- fixture()
  findings <- dq_build_findings(f$summary, f$details, f$ctx)
  expect_identical(unique(findings$tier), c("critical", "medium", "low"))
  expect_false("G_cleaned_action_duplicate_ids" %in% findings$check_id)
})

test_that("one person gets one card whichever id or name a check carries", {
  f <- fixture()
  findings <- dq_build_findings(f$summary, f$details, f$ctx)
  players <- findings[findings$entity == "player", , drop = FALSE]
  expect_identical(unique(players$player_key), "9:NOAM AVIVI")
})

test_that("the page leads with the games that need a fix and shows both score sides", {
  f <- fixture()
  path <- tempfile(fileext = ".html")
  write_dq_findings_html(f$summary, f$details, f$ctx, path,
                         meta = list(run_time = "2026-09-14 12:00", schema = "basketball_test", status = "FAIL"))
  html <- paste(readLines(path, warn = FALSE, encoding = "UTF-8"), collapse = "\n")
  expect_match(html, "1 game needs a data fix: 1 with critical problems, 0 with high. 1 player identity to repair.", fixed = TRUE)
  expect_match(html, "Another 1 game carries only medium or low findings", fixed = TRUE)
  expect_match(html, "Bnei Herzliya 89&ndash;79 M. Raanana", fixed = TRUE)
  expect_match(html, "The opponent scored 79 officially; lineup defense counts 79.", fixed = TRUE)
  expect_lt(regexpr('id="game-139"', html), regexpr('id="game-500"', html))
})
