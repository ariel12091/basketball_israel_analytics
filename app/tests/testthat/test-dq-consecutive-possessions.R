# AN_consecutive_same_team_possessions: a team ends two possessions in a row in
# one period, with no opponent possession between. One builder,
# etl/dq_consecutive_possessions.R, serves both leagues' data quality reports.

dq_text <- function(...) {
  paste(readLines(repo_file("..", ...), warn = FALSE, encoding = "UTF-8"), collapse = "\n")
}

test_that("both league reports call the one shared check as a warning", {
  blocks <- list(
    israeli = sub(".*AN_consecutive_same_team_possessions", "", dq_text("etl", "run_data_quality_report.R")),
    euroleague = sub(".*AN_consecutive_same_team_possessions", "",
                     dq_text("euroleague", "scripts", "run_euro_data_quality_report.R"))
  )
  for (league in names(blocks)) {
    block <- substr(blocks[[league]], 1L, 1500L)
    expect_true(grepl('severity = "warning"', block, fixed = TRUE), info = league)
    expect_true(grepl("dq_consecutive_possessions.R", block, fixed = TRUE), info = league)
    expect_true(grepl(sprintf('consecutive_possession_details(con, "%s", schema = schema)', league), block, fixed = TRUE), info = league)
  }
})

test_that("the findings page names the game, the team and both time stamps", {
  env <- new.env()
  sys.source(repo_file("..", "etl", "dq_findings_html.R"), envir = env)
  entry <- env$DQ_FINDING_CATALOG$AN_consecutive_same_team_possessions
  expect_identical(entry$tier, "medium")
  expect_identical(entry$entity, "game")
  ctx <- list(teams = data.frame(team_id = 33L, team_name = "MACCABI ASHDOD"))
  row <- data.frame(
    game_id = 432L, team_id = 33L, team_name = "MACCABI ASHDOD", period = 1L,
    first_end_clock = "7:41", second_end_clock = "7:12",
    first_end_event_id = 4321L, second_end_event_id = 4330L,
    first_end = "shot 2pt made", second_end = "turnover"
  )
  text <- entry$describe(row, ctx)
  for (part in c("game 432", "MACCABI ASHDOD", "Q1", "7:41", "7:12", "shot 2pt made", "turnover")) {
    expect_match(text, part, fixed = TRUE)
  }
})

skip_if_not(Sys.getenv("RUN_DB_TESTS", "0") == "1")
skip_if_not_installed("DBI")
skip_if_not_installed("RPostgres")

source(repo_file("..", "etl", "dq_consecutive_possessions.R"), local = TRUE)
con <- DBI::dbConnect(
  RPostgres::Postgres(),
  host = Sys.getenv("PG_HOST"), port = as.integer(Sys.getenv("PG_PORT")),
  dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"), password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE", "require"), bigint = "numeric"
)
withr::defer(DBI::dbDisconnect(con), teardown_env())

test_that("the rule reports a repeat, and only inside one period of one game", {
  ends <- "SELECT * FROM (VALUES
      -- Game 1: A, B, A, A (a repeat), then B
      (1, 1, 10, 100, 'made',     '9:50'),
      (1, 1, 11, 200, 'made',     '9:30'),
      (1, 1, 12, 100, 'miss',     '9:10'),
      (1, 1, 13, 100, 'turnover', '8:55'),
      (1, 1, 14, 200, 'made',     '8:30'),
      -- A period break is not a repeat: B ends Q1 and starts Q2
      (1, 2, 15, 200, 'made',     '9:45'),
      -- A game break is not a repeat either: A ended game 1's Q1 sequence, then game 2
      (2, 1, 16, 200, 'made',     '9:55'),
      -- Order follows the event id, not the order the rows arrive in
      (3, 1, 22, 100, 'made',     '9:00'),
      (3, 1, 20, 100, 'made',     '9:40'),
      (3, 1, 21, 200, 'made',     '9:20')
    ) AS t(game_id, period, event_id, team_id, end_type, clock)"
  got <- DBI::dbGetQuery(con, consecutive_possession_pairs_sql(ends))
  expect_equal(nrow(got), 1L)
  expect_equal(as.numeric(got$game_id), 1)
  expect_equal(as.numeric(got$team_id), 100)
  expect_equal(as.numeric(got$first_end_event_id), 12)
  expect_equal(as.numeric(got$second_end_event_id), 13)
  expect_identical(got$first_end_clock, "9:10")
  expect_identical(got$second_end_clock, "8:55")
  expect_identical(got$first_end, "miss")
  expect_identical(got$second_end, "turnover")
})

test_that("every reported Israeli repeat is real, against an independent read of the ends", {
  details <- consecutive_possession_details(con, "israeli")
  expect_gt(nrow(details), 0L)
  for (col in c("game_id", "game_date", "team_name", "period", "first_end_clock", "second_end_clock")) {
    expect_false(anyNA(details[[col]]), info = col)
  }
  game <- details$game_id[[1]]
  ends <- DBI::dbGetQuery(con, sprintf(
    "SELECT quarter, id, team_id FROM basketball_test.df_pts_poss_lineups_longer_mv
      WHERE game_id = %d AND final_end_poss AND type_lineup = 'offense' ORDER BY id", as.integer(game)))
  n <- nrow(ends)
  repeats <- which(ends$team_id[-1] == ends$team_id[-n] & ends$quarter[-1] == ends$quarter[-n])
  expect_equal(sort(as.numeric(ends$id[repeats + 1L])),
               sort(as.numeric(details$second_end_event_id[details$game_id == game])))
})

test_that("every reported EuroLeague repeat is real, against an independent read of the ends", {
  # The EuroLeague report runs as the ETL role; app_readonly cannot read euroleague.actions.
  skip_if_not(DBI::dbGetQuery(con, "SELECT has_table_privilege('euroleague.actions', 'SELECT') AS ok")$ok,
              "this role cannot read euroleague.actions")
  details <- consecutive_possession_details(con, "euroleague")
  expect_gt(nrow(details), 0L)
  for (col in c("game_id", "competition", "season", "gamecode", "team_name", "period",
                "first_end_clock", "second_end_clock")) {
    expect_false(anyNA(details[[col]]), info = col)
  }
  game <- details$game_id[[1]]
  ends <- DBI::dbGetQuery(con, sprintf(
    "SELECT period, source_event_order AS id, possession_offense_team_id AS team_id FROM euroleague.actions
      WHERE game_id = %d AND end_possession ORDER BY source_event_order", as.integer(game)))
  n <- nrow(ends)
  repeats <- which(ends$team_id[-1] == ends$team_id[-n] & ends$period[-1] == ends$period[-n])
  # A raw overtime period can hold a repeat the effective period splits, so the
  # report may list fewer than this read, never more.
  expect_true(all(details$second_end_event_id[details$game_id == game] %in% ends$id[repeats + 1L]))
})
