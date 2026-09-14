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

test_that("a clock run that pushes the timeline forward is critical, even for substitutions", {
  f <- fixture()
  f$summary <- rbind(f$summary, data.frame(
    check_id = "AK_misplaced_clock_runs", severity = "warning", status = "warning", row_count = 4,
    issue_count = 4, title = "Clock runs", detail_file = "", error_message = NA_character_
  ))
  f$details$AK_misplaced_clock_runs <- data.frame(
    game_id = c(398L, 399L, 62572L, 62537L), period = c("Q2", "Q3", "Q1", "Q1"),
    jump_seconds = c(480, 591, 28, 597),
    likely_misplaced_side = c("before_jump", "before_jump", "before_jump", "after_jump"),
    misplaced_events = c(37, 11, 8, 1), misplaced_gameplay_events = c(19, 0, 5, 0), misplaced_scoring_plays = c(2, 0, 1, 0),
    before_first_id = 1, before_last_id = 2, before_clock_left = "0:24 to 0:00",
    after_first_id = 3, after_last_id = 4, after_clock_left = "8:00 to 0:00",
    review_status = "unreviewed", diagnosis = ""
  )
  findings <- dq_build_findings(f$summary, f$details, f$ctx)
  ak <- findings[findings$check_id == "AK_misplaced_clock_runs", , drop = FALSE]
  tier <- function(g) ak$tier[ak$game_id == g]
  # Game 399's run is substitutions only, yet it moved a quarter of minutes.
  expect_identical(c(tier(398L), tier(399L)), c("critical", "critical"))
  expect_match(ak$effect[ak$game_id == 399L], "pushes the period's clock forward by about 591s", fixed = TRUE)
  # A 28-second push stays under 3% of player minutes.
  expect_identical(tier(62572L), "low")
  # A single non-play after the jump moves nothing.
  expect_identical(tier(62537L), "low")
  expect_match(ak$text[ak$game_id == 62537L], "Only substitutions or timeouts are involved.", fixed = TRUE)
  # Each summary line counts only its own severity's games.
  html <- dq_summary_html(findings, f$summary)
  expect_match(html, 'Critical</span><span class="line">Events stamped with the wrong game clock</span><span class="reach mono">2 games', fixed = TRUE)
})

test_that("incomplete lineups rank by what was played under them, with the impact in the text", {
  f <- fixture()
  f$summary <- rbind(f$summary, data.frame(
    check_id = "R_invalid_lineup_player_counts", severity = "error", status = "fail", row_count = 2,
    issue_count = 15, title = "Lineups", detail_file = "", error_message = NA_character_
  ))
  f$details$R_invalid_lineup_player_counts <- data.frame(
    game_id = c(399L, 178L), team_id = c(5L, 4L), total_states = c(25, 34), invalid_states = c(4, 11),
    invalid_gameplay_states = c(0, 0), min_reported_n_on = c(0, 6), max_reported_n_on = c(4, 6),
    invalid_lineup_seconds = c(4, 782), invalid_lineup_points_scored = c(0, 33), invalid_lineup_points_allowed = c(0, 19),
    invalid_lineup_off_possessions = c(0, 23), invalid_lineup_def_possessions = c(0, 25)
  )
  lineups <- dq_build_findings(f$summary, f$details, f$ctx)
  lineups <- lineups[lineups$check_id == "R_invalid_lineup_player_counts", , drop = FALSE]
  expect_identical(lineups$tier[lineups$game_id == 399L], "low")
  expect_identical(lineups$tier[lineups$game_id == 178L], "critical")
  expect_match(lineups$text[lineups$game_id == 399L], "on court for 4 s, with 0 points scored and 0 allowed and 0 possession endings", fixed = TRUE)
  expect_match(lineups$text[lineups$game_id == 178L], "on court for 13:02 min, with 33 points scored and 19 allowed and 48 possession endings", fixed = TRUE)
})

test_that("the summary carries the measured impact and the Actions digest repeats it", {
  f <- fixture()
  f$summary <- rbind(f$summary, data.frame(
    check_id = "AK_misplaced_clock_runs", severity = "warning", status = "warning", row_count = 1,
    issue_count = 1, title = "Clock runs", detail_file = "", error_message = NA_character_
  ))
  f$details$AK_misplaced_clock_runs <- data.frame(
    game_id = 398L, period = "Q2", jump_seconds = 480, likely_misplaced_side = "before_jump",
    misplaced_events = 37, misplaced_gameplay_events = 19, misplaced_scoring_plays = 2,
    before_first_id = 1, before_last_id = 2, before_clock_left = "0:24 to 0:00",
    after_first_id = 3, after_last_id = 4, after_clock_left = "8:00 to 0:00",
    review_status = "verified", diagnosis = "", minutes_moved = 51,
    minutes_moved_by_team = "Bnei Herzliya 21.7; Hapoel Eilat 29.4",
    largest_player_change = "ZACK BRYANT (Bnei Herzliya) credited 9.1 min too few",
    share_of_team_player_minutes = 0.1469, impact_note = "Minutes estimated by spreading the run evenly between 10:00 and 8:00 left."
  )
  path <- tempfile(fileext = ".html")
  out <- write_dq_findings_html(f$summary, f$details, f$ctx, path,
                                meta = list(run_time = "2026-09-14 12:00", schema = "basketball_test", status = "FAIL"))
  html <- paste(readLines(path, warn = FALSE, encoding = "UTF-8"), collapse = "
")
  expect_match(html, "51.0 player-minutes credited to the wrong players by misclocked events (1 game)", fixed = TRUE)
  expect_match(html, "largest: ZACK BRYANT (Bnei Herzliya) credited 9.1 min too few.", fixed = TRUE)
  digest <- readLines(out$digest_path, warn = FALSE, encoding = "UTF-8")
  expect_true(any(grepl("**Measured impact:** 51.0 player-minutes", digest, fixed = TRUE)))
  expect_true(any(grepl("| Critical | Events stamped with the wrong game clock | 1 game |", digest, fixed = TRUE)))
})

test_that("a player-minute gap under 3% of expected minutes ranks low", {
  f <- fixture()
  f$summary <- rbind(f$summary, data.frame(
    check_id = "X_player_minute_conservation", severity = "error", status = "fail", row_count = 2,
    issue_count = 2, title = "Minutes", detail_file = "", error_message = NA_character_
  ))
  f$details$X_player_minute_conservation <- data.frame(
    game_id = c(399L, 178L), team_id = c(5L, 11L), team_name = c("Hapoel Holon", "Beer Sheva/Dimona"),
    actual_player_minutes = c(199.37, 216.5), expected_player_minutes = c(199.5, 199), minute_difference = c(-0.13, 17.5)
  )
  x <- dq_build_findings(f$summary, f$details, f$ctx)
  x <- x[x$check_id == "X_player_minute_conservation", , drop = FALSE]
  expect_identical(x$tier[x$game_id == 399L], "low")
  expect_match(x$text[x$game_id == 399L], "under 3% of player minutes (0.1%)", fixed = TRUE)
  expect_identical(x$tier[x$game_id == 178L], "critical")
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
