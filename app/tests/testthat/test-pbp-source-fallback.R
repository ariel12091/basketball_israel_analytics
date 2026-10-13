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
  data.frame(team1 = 14L, team2 = 17L, score_team1 = 80L, score_team2 = 89L)
}

pbp_mock_clean <- function(raw) {
  actions <- raw$result$actions
  if (anyDuplicated(actions$id)) stop('Duplicate PBP action IDs remain after known fixes')
  actions
}

test_that('a valid primary feed is used without requesting the archive', {
  env <- pbp_fallback_env(); urls <- character()
  read <- function(url) { urls <<- c(urls, url); pbp_source_fixture() }
  result <- env$fetch_pbp_from_sources(414, 'primary', pbp_mock_clean,
    expected = pbp_expected_fixture(), read_fn = read, log_msg = function(...) NULL)
  expect_identical(urls, 'primary')
  expect_identical(attr(result, 'pbp_source_url'), 'primary')
})

test_that('fetch failures dynamically use the archive for any game ID', {
  env <- pbp_fallback_env(); urls <- character(); logs <- character()
  read <- function(url) {
    urls <<- c(urls, url)
    if (url == 'primary') stop('HTTP 503')
    list(actions = pbp_source_fixture(415)$result)
  }
  result <- env$fetch_pbp_from_sources(415, 'primary', pbp_mock_clean,
    expected = pbp_expected_fixture(), read_fn = read, log_msg = function(x) logs <<- c(logs, x))
  expect_identical(urls, c('primary', 'https://basket.co.il/livestats/data/game-415.json'))
  expect_identical(result$result$gameInfo$gameId, 415L)
  expect_match(paste(logs, collapse = ' '), 'HTTP 503')
  expect_match(tail(logs, 1), 'fallback accepted')
})

test_that('game 414 duplicate IDs trigger fallback before database writes', {
  env <- pbp_fallback_env(); primary <- pbp_source_fixture()
  primary$result$actions$id[2] <- primary$result$actions$id[1]
  read <- function(url) if (url == 'primary') primary else list(actions = pbp_source_fixture()$result)
  result <- env$fetch_pbp_from_sources(414, 'primary', pbp_mock_clean,
    expected = pbp_expected_fixture(), read_fn = read, log_msg = function(...) NULL)
  expect_identical(attr(result, 'pbp_source_url'), 'https://basket.co.il/livestats/data/game-414.json')
  expect_identical(anyDuplicated(result$result$actions$id), 0L)
})

test_that('incorrect primary scoring triggers fallback even without duplicate IDs', {
  env <- pbp_fallback_env(); primary <- pbp_source_fixture()
  primary$result$actions$team_score[c(1, 5)] <- c(75, 92)
  read <- function(url) if (url == 'primary') primary else pbp_source_fixture()
  result <- env$fetch_pbp_from_sources(414, 'primary', pbp_mock_clean,
    expected = pbp_expected_fixture(), read_fn = read, log_msg = function(...) NULL)
  expect_identical(attr(result, 'pbp_source_url'), 'https://basket.co.il/livestats/data/game-414.json')
})

test_that('bad archives are rejected and both source errors remain visible', {
  env <- pbp_fallback_env()
  for (defect in c('identity', 'unfinished', 'roster', 'quarter', 'score', 'duplicates', 'empty', 'teams')) {
    archive <- pbp_source_fixture()
    if (defect == 'identity') archive$result$gameInfo$gameId <- 999L
    if (defect == 'unfinished') archive$result$gameInfo$gameFinished <- FALSE
    if (defect == 'roster') archive$result$gameInfo$awayTeam$players <- data.frame(id = 1:4)
    if (defect == 'quarter') archive$result$actions$quarter[3] <- 2L
    if (defect == 'score') archive$result$actions$team_score[1] <- 79
    if (defect == 'duplicates') archive$result$actions$id[2] <- 1L
    if (defect == 'empty') archive$result$actions <- archive$result$actions[FALSE, ]
    if (defect == 'teams') archive$result$gameInfo$awayTeam$id <- 18L
    read <- function(url) { if (url == 'primary') stop('primary unavailable'); archive }
    expect_error(env$fetch_pbp_from_sources(414, 'primary', pbp_mock_clean,
      expected = pbp_expected_fixture(), read_fn = read, log_msg = function(...) NULL),
      'No valid PBP source for game 414.*primary unavailable.*game-414.json', info = defect)
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
