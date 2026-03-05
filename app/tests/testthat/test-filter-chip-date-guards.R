test_that("filter chip date handling guards against environment values", {
  txt <- read_repo_txt("R", "global.R")
  expect_true(grepl("safe_date_token\\s*<-\\s*function", txt))
  expect_true(grepl("is\\.environment\\(date_input\\)", txt))
  expect_true(grepl("is\\.environment\\(x\\)", txt))
  expect_true(grepl("is\\.environment\\(val\\)", txt))
})
