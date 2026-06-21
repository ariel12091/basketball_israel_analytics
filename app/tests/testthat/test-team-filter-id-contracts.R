test_that("team and opponent selectors use IDs rather than display names", {
  files <- c(
    repo_file("app.R"),
    repo_file("R", "server_tab1.R"),
    repo_file("R", "server_tab3.R"),
    repo_file("R", "server_tab4.R"),
    repo_file("R", "server_tab5_traditional.R"),
    repo_file("R", "server_tab6_team_stats.R"),
    repo_file("R", "server_tab7_compare.R")
  )
  code <- paste(unlist(lapply(files, readLines, warn = FALSE)), collapse = "\n")

  expect_false(grepl("filter\\(team_name\\s*%in%", code))
  expect_false(grepl("filter\\(Team\\s*%in%", code))
  expect_false(grepl("filter\\(opp_team_name\\s*%in%", code))
  expect_false(grepl("choices\\s*=\\s*td\\$team_name", code))
  expect_false(grepl("choices\\s*=\\s*teams_df\\$team_name", code))
})
