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

# Game 462 (2026-11-10): one stray foul-drawn with no segment rolled back the
# whole game. Non-scoring actions are now dropped from pws and reported.
with_stray <- function(x, type = "foul-drawn", ids = 102L, segment = NA_integer_) {
  extra <- tibble::tibble(game_id = 1L, id = ids, team_id = 8L, quarter = 2L,
                          quarter_time = "02:21", type = type)
  x$actions <- dplyr::bind_rows(x$actions, extra)
  x$pws <- dplyr::bind_rows(x$pws, dplyr::mutate(extra, segment_id = segment,
    team_id_defense = NA_integer_, lineup_hash_offense = NA_character_,
    lineup_hash_defense = NA_character_))
  x
}

test_that("an isolated non-scoring action without a lineup is dropped, not fatal", {
  env <- coverage_functions()
  x <- with_stray(coverage_example())
  messages <- character(0)
  kept <- env$assert_gameplay_lineup_coverage(x$actions, x$pws, x$lineups,
    log_msg = function(msg, level) messages <<- c(messages, paste(level, msg)))
  expect_equal(sort(kept$id), c(100L, 101L))
  dropped <- attr(kept, "lineup_coverage_dropped")
  expect_equal(dropped$id, 102L)
  expect_match(dropped$reason, "missing segment")
  expect_true(any(grepl("^WARN .*dropped from pws.*id=102 type=foul-drawn", messages)))
  # Clean games carry an empty attribute and an unchanged pws.
  clean <- coverage_example()
  kept <- env$assert_gameplay_lineup_coverage(clean$actions, clean$pws, clean$lineups)
  expect_equal(nrow(attr(kept, "lineup_coverage_dropped")), 0L)
  expect_equal(kept$id, clean$pws$id)
})

test_that("fouls, scoring/possession actions, drift and structural faults stay fatal", {
  env <- coverage_functions()
  # A foul anchors a free-throw trip's credit lineup.
  for (type in c("foul", "shot", "assist", "turnover", "rebound")) {
    x <- with_stray(coverage_example(), type = type)
    expect_error(env$assert_gameplay_lineup_coverage(x$actions, x$pws, x$lineups),
                 "id=102", class = "lineup_coverage_error")
  }
  # More than LINEUP_COVERAGE_MAX_TOLERATED in one game is a structural fault.
  x <- with_stray(coverage_example(), ids = 102:105)
  expect_error(env$assert_gameplay_lineup_coverage(x$actions, x$pws, x$lineups),
               "4 offending action", class = "lineup_coverage_error")
  x <- with_stray(coverage_example(), ids = 102:104)
  expect_equal(nrow(attr(env$assert_gameplay_lineup_coverage(
    x$actions, x$pws, x$lineups), "lineup_coverage_dropped")), 3L)
  # One fatal row fails the game and reports the tolerable rows with it.
  x <- with_stray(coverage_example())
  x$pws$segment_id[1L] <- NA_integer_
  error <- tryCatch(env$assert_gameplay_lineup_coverage(x$actions, x$pws, x$lineups),
                    lineup_coverage_error = identity)
  expect_setequal(error$failures$id, c(100L, 102L))
  # A tolerated type with a pipeline fault (period drift, duplicate) is fatal.
  x <- with_stray(coverage_example())
  x$pws$quarter[x$pws$id == 102L] <- 3L
  expect_error(env$assert_gameplay_lineup_coverage(x$actions, x$pws, x$lineups),
               "staged period/type differs", class = "lineup_coverage_error")
})

test_that("dropped rows are recorded to one per-run file, appended per game", {
  env <- coverage_functions()
  dir <- tempfile("coverage_")
  x <- with_stray(coverage_example())
  kept <- env$assert_gameplay_lineup_coverage(x$actions, x$pws, x$lineups)
  expect_false(env$record_lineup_coverage_dropped(
    attr(env$assert_gameplay_lineup_coverage(coverage_example()$actions,
      coverage_example()$pws, coverage_example()$lineups), "lineup_coverage_dropped"), dir))
  expect_true(env$record_lineup_coverage_dropped(attr(kept, "lineup_coverage_dropped"), dir))
  expect_true(env$record_lineup_coverage_dropped(attr(kept, "lineup_coverage_dropped"), dir))
  report <- read.csv(file.path(dir, env$LINEUP_COVERAGE_DROPPED_FILE))
  expect_equal(report$id, c(102L, 102L))
  expect_equal(report$type, c("foul-drawn", "foul-drawn"))
})

test_that("dropped rows are recorded only after commit, and the DQ check reads them", {
  full <- read_repo_txt("..", "etl", "etl_full.R")
  pos <- function(text) regexpr(text, full, fixed = TRUE)[[1L]]
  expect_gt(pos("pws_stage <- assert_gameplay_lineup_coverage("), 0L)
  expect_gt(pos("record_lineup_coverage_dropped("), pos("DBI::dbCommit(pg)"))
  expect_lt(pos("unlink(file.path(\"etl\", \"logs\", \"data_quality\", \"details\", LINEUP_COVERAGE_DROPPED_FILE))"),
            pos("pws_stage <- assert_gameplay_lineup_coverage("))
  legacy <- read_repo_txt("..", "etl", "etl_onoff.R")
  expect_gt(regexpr("record_lineup_coverage_dropped(", legacy, fixed = TRUE)[[1L]],
            regexpr('DBI::dbExecute(pg, "COMMIT;")', legacy, fixed = TRUE)[[1L]])
  dq <- read_repo_txt("..", "etl", "run_data_quality_report.R")
  expect_match(dq, 'id = "AO_lineup_coverage_dropped_actions"', fixed = TRUE)
  expect_match(dq, '"lineup_coverage_dropped.csv"', fixed = TRUE)
  html <- read_repo_txt("..", "etl", "dq_findings_html.R")
  expect_match(html, "AO_lineup_coverage_dropped_actions = list(", fixed = TRUE)
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
