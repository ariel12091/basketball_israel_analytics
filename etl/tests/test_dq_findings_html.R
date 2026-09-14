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
    games = data.frame(game_id = c(139L, 139L, 500L), game_year = c(2026L, 2026L, 2025L),
                       game_date = as.Date(c("2025-11-02", "2025-11-02", "2024-12-01")), team_id = c(6L, 13L, 4L),
                       team_name = c("Bnei Herzliya", "M. Raanana", "Hapoel Jerusalem"), team_score = c(89, 79, 80),
                       is_home = c(TRUE, FALSE, TRUE)),
    teams = data.frame(team_id = c(6L, 9L, 13L), team_name = c("Bnei Herzliya", "Ness Ziona", "M. Raanana")),
    players = data.frame(team_id = 9L, player_id = 1110L, firstname = "Noam", lastname = "Avivi")
  )
  list(summary = summary_df, details = details, ctx = ctx)
}

test_that("a finding that only involves substitutions or timeouts drops two tiers", {
  f <- fixture()
  f$summary <- rbind(f$summary, data.frame(
    check_id = "AK_misplaced_clock_runs", severity = "warning", status = "warning", row_count = 2,
    issue_count = 2, title = "Clock runs", detail_file = "", error_message = NA_character_
  ))
  f$details$AK_misplaced_clock_runs <- data.frame(
    game_id = c(398L, 399L), period = c("Q2", "Q3"), likely_misplaced_side = "before_jump",
    misplaced_events = c(37, 11), misplaced_gameplay_events = c(19, 0), misplaced_scoring_plays = c(2, 0),
    before_first_id = c(1, 3), before_last_id = c(2, 4), before_clock_left = c("0:24 to 0:00", "0:16 to 0:00"),
    after_first_id = c(5, 7), after_last_id = c(6, 8), after_clock_left = c("8:00 to 0:00", "9:51 to 0:04"),
    review_status = c("verified", "likely"), diagnosis = ""
  )
  findings <- dq_build_findings(f$summary, f$details, f$ctx)
  ak <- findings[findings$check_id == "AK_misplaced_clock_runs", , drop = FALSE]
  expect_identical(ak$tier[ak$game_id == 398L], "high")
  expect_identical(ak$tier[ak$game_id == 399L], "low")
  expect_match(ak$text[ak$game_id == 399L], "Only substitutions or timeouts are involved.", fixed = TRUE)
  expect_match(ak$effect[ak$game_id == 399L], "only lineup changes are misplaced", fixed = TRUE)
})

test_that("findings carry their season: games from the schedule, players from the check", {
  f <- fixture()
  f$details$C_active_correction_residue_game_scoped_tables$game_year <- 2026L
  findings <- dq_build_findings(f$summary, f$details, f$ctx)
  expect_identical(findings$season[findings$game_id %in% 139L], 2026L)
  expect_identical(findings$season[findings$game_id %in% 500L], 2025L)
  expect_identical(unique(findings$season[findings$check_id == "C_active_correction_residue_game_scoped_tables"]), 2026L)
  expect_identical(dq_season_label(2027L), "2026-27")
})

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
  # One summary view for all seasons plus one per season, and the controls.
  expect_match(html, '<div class="season-view" data-season="all">', fixed = TRUE)
  expect_match(html, '<div class="season-view" data-season="2025" hidden>', fixed = TRUE)
  expect_match(html, '<option value="2026">2025-26</option>', fixed = TRUE)
  expect_match(html, 'id="game-500" data-season="2025"', fixed = TRUE)
})
