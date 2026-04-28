test_that("starter-bounds helper call sites use valid argument names", {
  files <- c(
    "server_tab1.R" = testthat::test_path("..", "..", "R", "server_tab1.R"),
    "server_tab2.R" = testthat::test_path("..", "..", "R", "server_tab2.R"),
    "server_tab3.R" = testthat::test_path("..", "..", "R", "server_tab3.R")
  )

  extract_call_block <- function(txt) {
    m <- regexpr("resolve_starters_bounds\\([\\s\\S]*?\\n\\s*\\)", txt, perl = TRUE)
    regmatches(txt, m)
  }

  for (nm in names(files)) {
    txt <- paste(readLines(files[[nm]], warn = FALSE), collapse = "\n")
    call_txt <- extract_call_block(txt)
    expect_false(grepl("off_value\\s*=", call_txt), info = nm)
    expect_false(grepl("def_value\\s*=", call_txt), info = nm)
  }

  txt1 <- paste(readLines(files[["server_tab1.R"]], warn = FALSE), collapse = "\n")
  txt2 <- paste(readLines(files[["server_tab2.R"]], warn = FALSE), collapse = "\n")
  txt3 <- paste(readLines(files[["server_tab3.R"]], warn = FALSE), collapse = "\n")

  expect_true(grepl("off_val\\s*=\\s*f\\$num_starters_off", txt1))
  expect_true(grepl("def_val\\s*=\\s*f\\$num_starters_def", txt1))
  expect_true(grepl("off_val\\s*=\\s*input\\$ld_num_starters_off", txt2))
  expect_true(grepl("def_val\\s*=\\s*input\\$ld_num_starters_def", txt2))
  expect_true(grepl("off_val\\s*=\\s*input\\$tr_num_starters_off", txt3))
  expect_true(grepl("def_val\\s*=\\s*input\\$tr_num_starters_def", txt3))
})
