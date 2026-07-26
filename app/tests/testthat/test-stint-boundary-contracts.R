test_that("the final action is inside the half-open stint boundary", {
  etl_r <- read_repo_txt("..", "etl", "etl_onoff.R")

  expect_true(grepl(
    "add_terminal_stint_join_end <- function(stints_df)",
    etl_r,
    fixed = TRUE
  ))
  expect_true(grepl("final_end_id + 1L", etl_r, fixed = TRUE))
  expect_true(grepl('bounds = "[)"', etl_r, fixed = TRUE))
  expect_true(grepl("select(-.join_end_id)", etl_r, fixed = TRUE))

  final_action_id <- 624610872L
  exclusive_end_id <- final_action_id + 1L
  expect_true(final_action_id < exclusive_end_id)
})

test_that("only the terminal join boundary advances", {
  exprs <- parse(file = repo_file("..", "etl", "etl_onoff.R"))
  assignment <- Filter(function(expr) {
    is.call(expr) && identical(expr[[1]], as.name("<-")) &&
      identical(expr[[2]], as.name("add_terminal_stint_join_end"))
  }, exprs)
  expect_length(assignment, 1L)
  env <- new.env(parent = globalenv())
  eval(assignment[[1]], envir = env)

  stints <- tibble::tibble(
    game_id = c(1L, 1L),
    team_id = c(8L, 8L),
    q_bucket = c(0L, 0L),
    final_end_id = c(20L, 30L)
  )
  joined <- env$add_terminal_stint_join_end(stints)

  expect_equal(joined$final_end_id, c(20L, 30L))
  expect_equal(joined$.join_end_id, c(20L, 31L))
})
