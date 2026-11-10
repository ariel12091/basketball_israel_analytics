# Game 462 (2026-11-10): a foul-drawn (id 4620637) labelled Q2 02:21 whose id
# sits between Q3's 02:22 shot and 02:21 substitutions, with no matching foul
# in Q2. It failed the lineup coverage gate in Q2; the correction moves it, and
# only it, to Q3. Rows are copied from the provider feed (basket.co.il and
# Segev agree).
game462_correction_fn <- function() {
  env <- new.env(parent = globalenv())
  wanted <- c("KNOWN_PERIOD_LABEL_CORRECTIONS", "apply_known_period_label_corrections")
  assignments <- Filter(function(e) is.call(e) && identical(e[[1]], as.name("<-")) &&
    as.character(e[[2]]) %in% wanted,
    parse(repo_file("..", "etl", "etl_onoff.R"), keep.source = FALSE))
  expect_length(assignments, length(wanted))
  invisible(lapply(assignments, eval, envir = env))
  env$apply_known_period_label_corrections
}

test_that("game 462 moves only the stray foul-drawn from Q2 to Q3", {
  correct <- game462_correction_fn()
  rows <- data.frame(
    id = c(4620377L, 4620637L, 4620378L, 4620636L, 4620638L),
    quarter = c(2L, 2L, 2L, 3L, 3L),
    quarter_time = c("02:23", "02:21", "02:16", "02:22", "02:21"),
    type = c("assist", "foul-drawn", "clock", "shot", "substitution"),
    parameters_current_quarter = c(2L, 2L, 2L, 3L, 3L)
  )
  fixed <- correct(rows, 462L)
  expect_identical(fixed$quarter, c(2L, 3L, 2L, 3L, 3L))
  expect_identical(fixed$parameters_current_quarter, c(2L, 3L, 2L, 3L, 3L))
  expect_identical(fixed[, c("id", "quarter_time", "type")],
                   rows[, c("id", "quarter_time", "type")])
  # Idempotent, and scoped to game 462.
  expect_identical(correct(fixed, 462L), fixed)
  expect_identical(correct(rows, 461L), rows)
})
