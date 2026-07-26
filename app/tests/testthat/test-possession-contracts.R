test_that("free-throw progress repairs impossible provider denominators", {
  etl_r <- read_repo_txt("..", "etl", "etl_onoff.R")

  expect_true(grepl(".effective_ft_awarded = case_when(", etl_r, fixed = TRUE))
  expect_true(grepl(
    "parameters_free_throw_number > parameters_free_throws_awarded",
    etl_r,
    fixed = TRUE
  ))
  expect_true(grepl(
    "parameters_free_throw_number / NULLIF(.effective_ft_awarded, 0)",
    etl_r,
    fixed = TRUE
  ))
  expect_true(grepl("-.effective_ft_awarded", etl_r, fixed = TRUE))
})

test_that("data-quality report enforces the pct_ft domain", {
  dq_r <- read_repo_txt("..", "etl", "run_data_quality_report.R")

  expect_true(grepl("AJ_free_throw_progress_domain", dq_r, fixed = TRUE))
  expect_true(grepl("WHERE pct_ft < 0 OR pct_ft > 1", dq_r, fixed = TRUE))
})
