coverage_functions <- function() {
  env <- new.env(parent = globalenv())
  sys.source(repo_file("..", "etl", "lineup_coverage.R"), envir = env)
  env
}

coverage_example <- function() {
  players <- c(1:5, 6:10)
  lineups <- tibble::tibble(
    game_id = 1L, team_id = rep(c(8L, 12L), each = 5L),
    player_id = players, is_on_verdict = 1, n_on = 5L,
    lineup_hash = rep(vapply(list(1:5, 6:10), function(x)
      digest::digest(paste(x, collapse = "_"), algo = "md5", serialize = FALSE),
      character(1)), each = 5L)
  )
  actions <- tibble::tibble(game_id = 1L, id = c(100L, 101L),
    team_id = c(8L, 12L), quarter = 3L, quarter_time = c("09:59", "09:37"),
    type = c("shot", "freeThrow"))
  pws <- dplyr::mutate(actions, segment_id = 1L, team_id_defense = c(12L, 8L),
    lineup_hash_offense = lineups$lineup_hash[c(1L, 6L)],
    lineup_hash_defense = lineups$lineup_hash[c(6L, 1L)])
  list(actions = actions, pws = pws, lineups = lineups)
}

test_that("complete gameplay has exactly one valid five-player lineup per side", {
  env <- coverage_functions()
  x <- coverage_example()
  expect_invisible(env$assert_gameplay_lineup_coverage(x$actions, x$pws, x$lineups))
  # The full game's final action remains eligible; OT is checked too.
  x$actions$quarter <- x$pws$quarter <- 5L
  expect_invisible(env$assert_gameplay_lineup_coverage(x$actions, x$pws, x$lineups))
})

test_that("game 417's published Q2/Q3 gaps fail the gameplay gate", {
  env <- coverage_functions()
  x <- jsonlite::fromJSON(repo_file("tests", "fixtures", "game417-lineup-coverage.json"))
  failures <- env$gameplay_lineup_coverage_failures(x$actions, x$pws, x$lineups)
  expect_equal(nrow(failures), 16L)
  expect_true(all(c(4170559L, 4170569L, 4170276L) %in% failures$id))
  expect_false(any(c(4170667L, 4170669L) %in% failures$id))
  expect_error(env$assert_gameplay_lineup_coverage(x$actions, x$pws, x$lineups),
               "game=417 Q2 team=8 id=4170276", class = "lineup_coverage_error")
  covered <- x$actions$id %in% c(4170667L, 4170669L)
  expect_invisible(env$assert_gameplay_lineup_coverage(
    x$actions[covered, ], x$pws[covered, ], x$lineups))
})

test_that("missing source actions and duplicated mappings both fail", {
  env <- coverage_functions()
  x <- coverage_example()
  missing <- env$gameplay_lineup_coverage_failures(x$actions, x$pws[-1L, ], x$lineups)
  expect_equal(missing$id, 100L)
  expect_match(missing$reason, "missing staged action")
  duplicates <- env$gameplay_lineup_coverage_failures(
    x$actions, dplyr::bind_rows(x$pws, x$pws[1L, ]), x$lineups)
  expect_equal(duplicates$id, 100L)
  expect_match(duplicates$reason, "duplicate stint mapping")
  source_duplicate <- env$gameplay_lineup_coverage_failures(
    dplyr::bind_rows(x$actions, x$actions[1L, ]), x$pws, x$lineups)
  expect_match(source_duplicate$reason, "duplicate source action")
})

test_that("both sides need actual five-player states, not merely hashes or n_on", {
  env <- coverage_functions()
  x <- coverage_example()
  for (bad in list(x$lineups[-10L, ],
                   dplyr::mutate(x$lineups, n_on = 6L),
                   dplyr::mutate(x$lineups, player_id = 1L))) {
    expect_error(env$assert_gameplay_lineup_coverage(x$actions, x$pws, bad),
                 "invalid .* lineup")
  }
  x$pws$lineup_hash_offense[1L] <- "unknown"
  expect_error(env$assert_gameplay_lineup_coverage(x$actions, x$pws, x$lineups),
               "invalid offense lineup")
})

test_that("a segment, opposing team and unchanged source period/type are required", {
  env <- coverage_functions()
  x <- coverage_example()
  bad <- x$pws
  bad$segment_id[1L] <- NA_integer_
  expect_error(env$assert_gameplay_lineup_coverage(x$actions, bad, x$lineups),
               "missing segment")
  bad <- x$pws
  bad$team_id_defense[1L] <- bad$team_id[1L]
  expect_error(env$assert_gameplay_lineup_coverage(x$actions, bad, x$lineups),
               "invalid opposing team")
  bad <- x$pws
  bad$quarter[1L] <- 2L
  expect_error(env$assert_gameplay_lineup_coverage(x$actions, bad, x$lineups),
               "staged period/type differs from source")
  bad <- x$pws
  bad$type[1L] <- "substitution"
  expect_error(env$assert_gameplay_lineup_coverage(x$actions, bad, x$lineups),
               "staged period/type differs from source")
})

test_that("boundary/reset and administrative rows do not require lineups", {
  env <- coverage_functions()
  x <- coverage_example()
  administrative <- x$actions[rep(1L, 5L), ]
  administrative$id <- 102:106
  administrative$team_id <- 0L
  administrative$type <- c("substitution", "quarter", "clock", "timeout", "officialReview")
  expect_invisible(env$assert_gameplay_lineup_coverage(
    dplyr::bind_rows(x$actions, administrative), x$pws, x$lineups))
})

test_that("the rejection records all affected action IDs in the artifact directory", {
  env <- coverage_functions()
  x <- coverage_example()
  x$pws$lineup_hash_defense <- NA_character_
  diagnostic_dir <- tempfile("coverage_")
  messages <- character(0)
  error <- tryCatch(env$assert_gameplay_lineup_coverage(
    x$actions, x$pws, x$lineups,
    log_msg = function(msg, level) messages <<- c(messages, msg),
    diagnostic_dir = diagnostic_dir), lineup_coverage_error = identity)
  expect_s3_class(error, "lineup_coverage_error")
  expect_equal(error$failures$id, x$actions$id)
  report <- read.csv(file.path(diagnostic_dir, "lineup_coverage_game_1.csv"))
  expect_equal(report$id, x$actions$id)
  expect_true(any(grepl("Q3 clock=09:59 team=8 id=100", messages, fixed = TRUE)))
})

test_that("the full ETL guard runs after OT recovery and before the pws insert/commit", {
  full <- read_repo_txt("..", "etl", "etl_full.R")
  pos <- function(text) regexpr(text, full, fixed = TRUE)[[1L]]
  guard <- pos("assert_gameplay_lineup_coverage(")
  expect_gt(guard, pos("df_lineups_df <- recovery$lineups"))
  expect_lt(guard, pos('upsert_by_like(pg, SCHEMA, "pws", pws_stage'))
  expect_lt(guard, pos("DBI::dbCommit(pg)"))
  expect_match(full, "try(DBI::dbRollback(pg), silent = TRUE)", fixed = TRUE)
  expect_match(full, "successful_ids <- c(successful_ids, gid)", fixed = TRUE)
  legacy <- read_repo_txt("..", "etl", "etl_onoff.R")
  expect_match(legacy, 'source("etl/lineup_coverage.R")', fixed = TRUE)
  expect_lt(regexpr("assert_gameplay_lineup_coverage(", legacy, fixed = TRUE)[[1L]],
            regexpr('upsert_by_like(pg, SCHEMA, "pws", pws_stage', legacy, fixed = TRUE)[[1L]])
})
