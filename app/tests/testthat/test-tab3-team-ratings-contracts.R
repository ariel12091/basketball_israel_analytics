test_that("starter-bounds helper call sites use valid argument names", {
  helpers_path <- testthat::test_path("..", "..", "R", "helpers.R")

  extract_call_block <- function(txt) {
    m <- regexpr("resolve_starters_bounds\\([\\s\\S]*?\\n\\s*\\)", txt, perl = TRUE)
    regmatches(txt, m)
  }

  helpers <- paste(readLines(helpers_path, warn = FALSE), collapse = "\n")
  call_txt <- extract_call_block(helpers)
  expect_length(call_txt, 1L)
  expect_false(grepl("off_value\\s*=", call_txt))
  expect_false(grepl("def_value\\s*=", call_txt))

  expect_true(grepl("off_val\\s*=\\s*filters\\$num_starters_off", helpers))
  expect_true(grepl("def_val\\s*=\\s*filters\\$num_starters_def", helpers))

  server_calls <- c(
    server_tab1.R = "onoff_db_args",
    server_tab2.R = "game_context_db_args",
    server_tab3.R = "game_context_db_args",
    server_tab8_euro.R = "onoff_db_args",
    server_tab9_euro_team.R = "game_context_db_args",
    server_tab10_euro_lineups.R = "game_context_db_args"
  )
  for (file in names(server_calls)) {
    txt <- paste(readLines(testthat::test_path("..", "..", "R", file), warn = FALSE),
                 collapse = "\n")
    expect_true(grepl(paste0(server_calls[[file]], "\\("), txt), info = file)
    expect_false(grepl("resolve_starters_bounds\\(", txt), info = file)
  }
})
