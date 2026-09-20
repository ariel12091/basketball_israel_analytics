game406_correction_env <- function() {
  exprs <- parse(repo_file("..", "etl", "etl_onoff.R"), keep.source = FALSE)
  wanted <- c(
    "KNOWN_CLOCK_STAMP_CORRECTIONS",
    "KNOWN_PERIOD_LABEL_CORRECTIONS",
    "apply_known_period_label_corrections",
    "apply_known_clock_corrections",
    "apply_known_game406_wall_clock_correction"
  )
  assignments <- Filter(function(expr) {
    is.call(expr) && identical(expr[[1]], as.name("<-")) &&
      as.character(expr[[2]]) %in% wanted
  }, exprs)
  expect_equal(vapply(assignments, function(x) as.character(x[[2]]), character(1)), wanted)

  env <- new.env(parent = globalenv())
  invisible(lapply(assignments, eval, envir = env))
  env
}

test_that("game 406 period labels map provider Q3 to Q2 and early Q4 to Q3", {
  env <- game406_correction_env()
  rows <- data.frame(
    id = c(4060239L, 4060417L, 4060419L, 4060590L, 4060592L),
    quarter = c(3L, 3L, 4L, 4L, 4L),
    parameters_current_quarter = c(3L, NA, 4L, NA, 4L),
    parameters_quarter = c(3L, NA, 4L, NA, 4L)
  )

  corrected <- env$apply_known_period_label_corrections(rows, 406L)

  expect_identical(corrected$quarter, c(2L, 2L, 3L, 3L, 4L))
  expect_identical(corrected$parameters_current_quarter, c(2L, NA, 3L, NA, 4L))
  expect_identical(corrected$parameters_quarter, c(2L, NA, 3L, NA, 4L))
})

test_that("game 406 Q2 reset OUT rows move to the opening boundary", {
  env <- game406_correction_env()
  rows <- data.frame(
    id = c(4060230L, 4060231L, 4060232L, 4060233L, 4060234L),
    quarter_time = c("00:01", "00:01", "00:01", "00:00", "00:00")
  )

  corrected <- env$apply_known_clock_corrections(rows, 406L)

  expect_identical(corrected$quarter_time, rep("10:00", 5L))
})

test_that("game 406 Q4 wall-time correction is monotone and holds the reset at 10:00", {
  env <- game406_correction_env()
  rows <- data.frame(
    id = c(4060592L, 4060611L, 4060670L, 4060747L, 4060748L, 4060749L),
    quarter = 4L,
    quarter_time = "00:00",
    user_time = c("16:16:09", "16:16:27", "16:28:50", "16:41:32", "16:55:09", "16:55:13"),
    type = c("substitution", "substitution", "shot", "rebound", "quarter", "game")
  )

  corrected <- env$apply_known_game406_wall_clock_correction(rows, 406L)

  expect_identical(
    corrected$quarter_time,
    c("10:00", "10:00", "05:00", "00:00", "00:00", "00:00")
  )
  seconds <- vapply(strsplit(corrected$quarter_time, ":", fixed = TRUE), function(x) {
    as.integer(x[[1]]) * 60L + as.integer(x[[2]])
  }, integer(1))
  expect_true(all(diff(seconds) <= 0))
})

test_that("game 406 wall-time correction refuses a provider-repaired clock", {
  env <- game406_correction_env()
  rows <- data.frame(
    id = c(4060592L, 4060747L),
    quarter = 4L,
    quarter_time = c("10:00", "00:00"),
    user_time = c("16:16:09", "16:41:32"),
    type = c("substitution", "rebound")
  )

  expect_warning(
    corrected <- env$apply_known_game406_wall_clock_correction(rows, 406L),
    "provider no longer"
  )
  expect_identical(corrected$quarter_time, rows$quarter_time)
})

test_that("clean_actions wires the game 406 correction and drops false markers", {
  src <- readLines(repo_file("..", "etl", "etl_onoff.R"), warn = FALSE)
  expect_true(any(grepl(
    "a <- apply_known_game406_wall_clock_correction(a, game_id_val)",
    src,
    fixed = TRUE
  )))
  expect_true(any(grepl("id %in% c(4060238L, 4060239L)", src, fixed = TRUE)))
})
