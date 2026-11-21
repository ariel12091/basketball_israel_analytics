# The general clock rule (etl/etl_onoff.R, misplaced_dead_ball_runs): a run of
# dead-ball events stamped later than the plays after it takes the clock of
# the last event before it. Rows are copied from the provider feeds.
clock_run_fn <- function() {
  env <- new.env(parent = globalenv())
  wanted <- c("CLOCK_RUN_JUMP_SECONDS", "CLOCK_RUN_DEAD_BALL_TYPES",
              "misplaced_dead_ball_runs", "restamp_misplaced_dead_ball_runs")
  assignments <- Filter(function(e) is.call(e) && identical(e[[1]], as.name("<-")) &&
    as.character(e[[2]]) %in% wanted,
    parse(repo_file("..", "etl", "etl_onoff.R"), keep.source = FALSE))
  expect_length(assignments, length(wanted))
  invisible(lapply(assignments, eval, envir = env))
  env$restamp_misplaced_dead_ball_runs
}

feed_rows <- function(id, quarter, quarter_time, type) {
  data.frame(id = as.character(id), quarter = quarter, quarter_time = quarter_time,
             type = type, source_row = seq_along(id))
}

test_that("game 459: Q3 opening substitutions stamped 00:43-00:28 go to 10:00", {
  restamp <- clock_run_fn()
  rows <- feed_rows(
    c(4590369, 4590370, 4590371, 4590376, 4590381, 4590390, 4590391, 4590392,
      4590395, 4590396, 4590398, 4590399, 4590400),
    3L,
    c("10:00", "00:43", "00:43", "00:40", "00:33", "00:28", "00:27", "09:59",
      "09:43", "09:43", "09:41", "09:39", "09:36"),
    c("quarter", "clock", "substitution", "substitution", "substitution", "substitution",
      "clock", "clock", "shot", "rebound", "turnover", "steal", "shot")
  )
  fixed <- suppressMessages(restamp(rows, 459L))
  subs <- fixed$type == "substitution"
  expect_identical(fixed$quarter_time[subs], rep("10:00", 4))
  # Clock rows (dropped later) and every play keep their stamps.
  expect_identical(fixed$quarter_time[!subs], rows$quarter_time[!subs])
})

test_that("game 62506: substitutions in a 5:07 timeout stamped 4:21 go to 5:07", {
  restamp <- clock_run_fn()
  rows <- feed_rows(
    c(625060509, 625060510, 625060511, 625060518, 625060519, 625060521, 625060522,
      625060523, 625060524, 625060526, 625060529),
    3L,
    c("05:11", "05:09", "05:07", "04:21", "04:21", "04:53", "04:53", "04:53", "04:53", "04:41", "04:31"),
    c("shot", "assist", "timeout", "substitution", "substitution", "foul", "foul-drawn",
      "freeThrow", "freeThrow", "foul", "shot")
  )
  fixed <- suppressMessages(restamp(rows, 62506L))
  expect_identical(fixed$quarter_time, replace(rows$quarter_time, 4:5, "05:07"))
})

test_that("a run holding a live play, or an event stamped early, is left alone", {
  restamp <- clock_run_fn()
  # A shot inside the late-stamped run: by hand (KNOWN_CLOCK_STAMP_CORRECTIONS).
  live <- feed_rows(1:6, 2L,
    c("10:00", "00:28", "00:20", "00:00", "09:40", "09:30"),
    c("quarter", "substitution", "shot", "substitution", "rebound", "shot"))
  expect_identical(restamp(live, 1L)$quarter_time, live$quarter_time)
  # One play stamped EARLIER than its neighbours (game 658's steal): the
  # running maximum already ignores it.
  early <- feed_rows(1:5, 2L,
    c("00:10", "00:04", "00:03", "10:00", "00:00"),
    c("shot", "substitution", "shot", "steal", "quarter"))
  expect_identical(restamp(early, 1L)$quarter_time, early$quarter_time)
})
