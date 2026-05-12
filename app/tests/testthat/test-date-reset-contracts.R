DATE_RESET_SPECS <- list(
  list(name = "tab1", server = repo_file("R", "server_tab1.R"), date_id = "date_range", reset_id = "reset_defaults"),
  list(name = "tab2", server = repo_file("R", "server_tab2.R"), date_id = "ld_dates", reset_id = "ld_reset"),
  list(name = "tab3", server = repo_file("R", "server_tab3.R"), date_id = "tr_dates", reset_id = "tr_reset"),
  list(name = "tab4", server = repo_file("R", "server_tab4.R"), date_id = "gl_dates", reset_id = "gl_reset"),
  list(name = "tab5", server = repo_file("R", "server_tab5_traditional.R"), date_id = "ts_dates", reset_id = "ts_reset")
)

test_that("date reset uses concrete season bounds for every tab", {
  for (s in DATE_RESET_SPECS) {
    txt <- paste(readLines(s$server, warn = FALSE), collapse = "\n")
    expect_true(grepl(sprintf("observeEvent\\(input\\$%s", s$reset_id), txt), info = s$name)
    expect_true(grepl(sprintf("updateDateRangeInput\\(session,\\s*\\\"%s\\\"", s$date_id), txt), info = s$name)
    expect_false(
      grepl(sprintf("updateDateRangeInput\\(session,\\s*\\\"%s\\\"[^\\n]*start\\s*=\\s*NA", s$date_id), txt, perl = TRUE),
      info = paste(s$name, "has NA date reset")
    )
    expect_true(grepl("season_date_bounds", txt), info = paste(s$name, "should use season bounds"))
  }
})

test_that("tab2 season change updates lineup date range", {
  txt <- paste(readLines(repo_file("R", "server_tab2.R"), warn = FALSE), collapse = "\n")
  expect_true(
    grepl(
      "observeEvent\\(list\\(input\\$main_tabs, input\\$game_year\\)[\\s\\S]*?updateDateRangeInput\\(session,\\s*\"ld_dates\"",
      txt,
      perl = TRUE
    )
  )
})
