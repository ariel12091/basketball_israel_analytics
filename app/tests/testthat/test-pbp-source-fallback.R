library(testthat)

pbp_fallback_env <- function() {
  env <- new.env(parent = globalenv())
  sys.source(file.path('..', '..', '..', 'etl', 'pbp_source_fallback.R'), envir = env)
  env
}

pbp_source_fixture <- function(game_id = 414L) {
  list(result = list(success = TRUE, gameInfo = list(
    gameId = as.integer(game_id), gameFinished = TRUE,
    homeTeam = list(id = 14L, players = data.frame(id = 1:5)),
    awayTeam = list(id = 17L, players = data.frame(id = 6:10))),
    actions = data.frame(id = 1:5, quarter = c(1L, 2L, 3L, 4L, 4L),
                         team_id = c(14L, 14L, 14L, 14L, 17L),
                         team_score = c(80, 0, 0, 0, 89))))
}

pbp_expected_fixture <- function() {
  data.frame(game_year = 2027L, team1 = 2122L, team2 = 2124L,
             pbp_team1 = 14L, pbp_team2 = 17L, score_team1 = 80L, score_team2 = 89L)
}

pbp_mock_clean <- function(raw) {
  actions <- raw$result$actions
  if (anyDuplicated(actions$id)) stop('Duplicate PBP action IDs remain after known fixes')
  actions
}

ARCHIVE_414 <- 'https://basket.co.il/livestats/data/game-414.json'

test_that('a valid basket.co.il archive is used without requesting Segev', {
  env <- pbp_fallback_env(); urls <- character()
  read <- function(url) { urls <<- c(urls, url); list(actions = pbp_source_fixture()$result) }
  result <- env$fetch_pbp_from_sources(414, 'segev', pbp_mock_clean,
    expected = pbp_expected_fixture(), read_fn = read, log_msg = function(...) NULL)
  expect_identical(urls, ARCHIVE_414)
  expect_identical(attr(result, 'pbp_source_url'), ARCHIVE_414)
})

test_that('archive fetch failures fall back to Segev for any game ID', {
  env <- pbp_fallback_env(); urls <- character(); logs <- character()
  read <- function(url) {
    urls <<- c(urls, url)
    if (grepl('basket.co.il', url, fixed = TRUE)) stop('HTTP 404')
    pbp_source_fixture(415)
  }
  result <- env$fetch_pbp_from_sources(415, 'segev', pbp_mock_clean,
    expected = pbp_expected_fixture(), read_fn = read, log_msg = function(x) logs <<- c(logs, x))
  expect_identical(urls, c('https://basket.co.il/livestats/data/game-415.json', 'segev'))
  expect_identical(result$result$gameInfo$gameId, 415L)
  expect_match(paste(logs, collapse = ' '), 'HTTP 404')
  expect_match(tail(logs, 1), 'fallback accepted')
})

test_that('duplicate action IDs in the archive fall back to Segev before database writes', {
  env <- pbp_fallback_env(); archive <- pbp_source_fixture()
  archive$result$actions$id[2] <- archive$result$actions$id[1]
  read <- function(url) if (url == ARCHIVE_414) list(actions = archive$result) else pbp_source_fixture()
  result <- env$fetch_pbp_from_sources(414, 'segev', pbp_mock_clean,
    expected = pbp_expected_fixture(), read_fn = read, log_msg = function(...) NULL)
  expect_identical(attr(result, 'pbp_source_url'), 'segev')
  expect_identical(anyDuplicated(result$result$actions$id), 0L)
})

test_that('incorrect archive scoring falls back to Segev even without duplicate IDs', {
  env <- pbp_fallback_env(); archive <- pbp_source_fixture()
  archive$result$actions$team_score[c(1, 5)] <- c(75, 92)
  read <- function(url) if (url == ARCHIVE_414) archive else pbp_source_fixture()
  result <- env$fetch_pbp_from_sources(414, 'segev', pbp_mock_clean,
    expected = pbp_expected_fixture(), read_fn = read, log_msg = function(...) NULL)
  expect_identical(attr(result, 'pbp_source_url'), 'segev')
})

test_that('bad Segev fallbacks are rejected and both source errors remain visible', {
  env <- pbp_fallback_env()
  for (defect in c('identity', 'unfinished', 'roster', 'quarter', 'score', 'duplicates', 'empty', 'teams')) {
    segev <- pbp_source_fixture()
    if (defect == 'identity') segev$result$gameInfo$gameId <- 999L
    if (defect == 'unfinished') segev$result$gameInfo$gameFinished <- FALSE
    if (defect == 'roster') segev$result$gameInfo$awayTeam$players <- data.frame(id = 1:4)
    if (defect == 'quarter') segev$result$actions$quarter[3] <- 2L
    if (defect == 'score') segev$result$actions$team_score[1] <- 79
    if (defect == 'duplicates') segev$result$actions$id[2] <- 1L
    if (defect == 'empty') segev$result$actions <- segev$result$actions[FALSE, ]
    if (defect == 'teams') segev$result$gameInfo$awayTeam$id <- 18L
    read <- function(url) { if (url == ARCHIVE_414) stop('archive unavailable'); segev }
    expect_error(env$fetch_pbp_from_sources(414, 'segev', pbp_mock_clean,
      expected = pbp_expected_fixture(), read_fn = read, log_msg = function(...) NULL),
      'No valid PBP source for game 414.*game-414.json: archive unavailable.*segev', info = defect)
  }
})

test_that('an explicit per-game override remains supported without a hardcoded ID', {
  env <- pbp_fallback_env()
  withr::local_envvar(PBP_GAME_402_FALLBACK_URL = 'reviewed-archive')
  urls <- character()
  read <- function(url) { urls <<- c(urls, url); pbp_source_fixture(402) }
  result <- env$fetch_pbp_from_sources(402, 'primary', pbp_mock_clean,
    read_fn = read, log_msg = function(...) NULL)
  expect_identical(urls, 'reviewed-archive')
  expect_identical(attr(result, 'pbp_source_url'), 'reviewed-archive')
})

test_that('the nightly caller passes its selected schedule row to source validation', {
  text <- paste(readLines(file.path('..', '..', '..', 'etl', 'etl_full.R'), warn = FALSE), collapse = '\n')
  expect_match(text, 'expected = fetchable_sched[i, , drop = FALSE]', fixed = TRUE)
  text <- paste(readLines(file.path('..', '..', '..', 'etl', 'etl_onoff.R'), warn = FALSE), collapse = '\n')
  expect_match(text, 'source("etl/pbp_source_fallback.R")', fixed = TRUE)
})

test_that('archive selection retains its matching embedded box score', {
  env <- pbp_fallback_env()
  box <- list(gameInfo = list(gameId = '414', homeTeamId = '14', awayTeamId = '17'),
              homeTeam = list(players = data.frame(playerId = 1:5)),
              awayTeam = list(players = data.frame(playerId = 6:10)))
  raw <- env$normalize_pbp_payload(list(actions = pbp_source_fixture()$result, boxscore = box))
  expect_identical(env$embedded_pbp_box(raw, 414)$result$boxscore, box)
  raw$result$boxscore$gameInfo$gameId <- '415'
  expect_error(env$embedded_pbp_box(raw, 414), 'requested game')
  raw$result$boxscore$gameInfo$gameId <- '414'
  raw$result$boxscore$gameInfo$homeTeamId <- '18'
  expect_error(env$embedded_pbp_box(raw, 414), 'do not match')
  expect_null(env$embedded_pbp_box(pbp_source_fixture(), 414))
})

test_that('real schedule IDs are mapped by season before validating game 414', {
  env <- pbp_fallback_env()
  schedule <- pbp_expected_fixture()[, c('game_year', 'team1', 'team2', 'score_team1', 'score_team2')]
  dictionary <- data.frame(game_year = c(2026L, 2027L, 2027L),
                           team_id_schedule = c(2122L, 2122L, 2124L),
                           team_id_rosters = c(99L, 14L, 17L))
  mapped <- env$map_pbp_schedule_teams(schedule, dictionary)
  expect_identical(mapped$pbp_team1, 14L)
  expect_identical(mapped$pbp_team2, 17L)
  expect_identical(mapped$team1, 2122L)
  expect_identical(mapped$team2, 2124L)
  expect_no_error(env$validate_pbp_source(pbp_source_fixture(), 414L, pbp_mock_clean, mapped))
  expect_error(env$map_pbp_schedule_teams(schedule, dictionary[-3, ]), 'Missing schedule-to-PBP')
  expect_error(env$map_pbp_schedule_teams(schedule, rbind(dictionary, dictionary[2, ])), 'duplicate season/team')
  text <- paste(readLines(file.path('..', '..', '..', 'etl', 'etl_full.R'), warn = FALSE), collapse = '\n')
  expect_match(text, 'map_pbp_schedule_teams(sched_subset, pbp_team_dictionary)', fixed = TRUE)
})
