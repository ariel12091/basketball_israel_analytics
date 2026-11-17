# Register of accepted defects (euroleague/scripts/dq_accepted_defects.R): the
# EuroLeague report's N1 fails only on blocked checks the register does not
# cover, N1b lists the covered ones, N1c flags register rows that went stale.

defects_env <- function() {
  env <- new.env()
  sys.source(repo_file("..", "euroleague", "scripts", "dq_accepted_defects.R"), envir = env)
  env
}

write_register <- function(rows) {
  path <- tempfile(fileext = ".csv")
  header <- "competition,season,gamecode,gate,accepted_on,accepted_by,reason,reference"
  writeLines(c(header, rows), path, useBytes = TRUE)
  path
}

test_that("the shipped register is valid", {
  env <- defects_env()
  reg <- env$read_accepted_defects(repo_file("..", "euroleague", "data_quality", "accepted_defects.csv"))
  expect_gt(nrow(reg), 0L)
  expect_true(all(reg$gate %in% names(env$ACCEPTED_DEFECT_GATES)))
  expect_type(reg$season, "integer")
})

test_that("a malformed register row stops the check instead of being skipped", {
  env <- defects_env()
  good <- 'U,2026,100,score_progression,2026-11-17,user,"why",D37'
  expect_silent(env$read_accepted_defects(write_register(good)))
  bad <- list(
    `unknown gate` = 'U,2026,100,score,2026-11-17,user,"why",D37',
    `blank reason` = "U,2026,100,score_progression,2026-11-17,user,,D37",
    `blank accepted_by` = 'U,2026,100,score_progression,2026-11-17,,"why",D37',
    `whole number` = 'U,2026,10a,score_progression,2026-11-17,user,"why",D37',
    `YYYY-MM-DD` = 'U,2026,100,score_progression,17/11/2026,user,"why",D37',
    duplicate = c(good, good)
  )
  for (problem in names(bad)) {
    expect_error(env$read_accepted_defects(write_register(bad[[problem]])), problem, fixed = TRUE, info = problem)
  }
  no_reason_col <- tempfile(fileext = ".csv")
  writeLines(c("competition,season,gamecode,gate,accepted_on,accepted_by,reference",
               "U,2026,100,score_progression,2026-11-17,user,D37"), no_reason_col)
  expect_error(env$read_accepted_defects(no_reason_col), "missing column", fixed = TRUE)
})

test_that("blocked checks split into open, accepted and stale", {
  env <- defects_env()
  blocked <- data.frame(
    competition = c("U", "U", "E", "E"),
    season = c(2026L, 2026L, 2025L, 2025L),
    gamecode = c(100L, 5L, 265L, 265L),
    gate = c("score_progression", "possession_structure", "possession_structure", "boxscore_metrics"),
    stringsAsFactors = FALSE
  )
  register <- env$read_accepted_defects(write_register(c(
    'U,2026,100,score_progression,2026-11-17,user,"accepted",D37',
    'E,2025,265,boxscore_metrics,2026-11-17,user,"one foul",',
    'U,2026,18,lineup_structure,2026-11-10,user,"fixed since",D37'
  )))
  split <- env$classify_blocked_gates(blocked, register)

  # Each failed check needs its own row: accepting one of game 265's two
  # failures leaves the other open.
  expect_equal(
    sort(paste(split$open$competition, split$open$gamecode, split$open$gate)),
    c("E 265 possession_structure", "U 5 possession_structure")
  )
  expect_equal(sort(split$accepted$gamecode), c(100L, 265L))
  expect_true(all(c("reason", "accepted_by") %in% names(split$accepted)))
  expect_equal(sum(split$accepted$open), 0)
  expect_equal(split$stale$gamecode, 18L)
})

test_that("a blocked game failing none of the known checks is never accepted", {
  env <- defects_env()
  sql <- env$blocked_gates_sql("SELECT 1", '"euroleague"."schedule"')
  expect_true(grepl("'unexplained'::text AS gate", sql, fixed = TRUE))
  expect_false("unexplained" %in% names(env$ACCEPTED_DEFECT_GATES))
  expect_error(
    env$read_accepted_defects(write_register('U,2026,1,unexplained,2026-11-17,user,"x",')),
    "unknown gate", fixed = TRUE
  )
})
