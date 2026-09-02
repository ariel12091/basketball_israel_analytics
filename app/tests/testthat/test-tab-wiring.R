TAB_SPECS <- list(
  list(name = "tab1", ui = repo_file("R", "ui_tab1_onoff.R"), server = repo_file("R", "server_tab1.R"), reset_id = "reset_defaults", chips_id = "on_filter_chips", prefix = "on"),
  list(name = "tab2", ui = repo_file("R", "ui_tab2_lineup.R"), server = repo_file("R", "server_tab2.R"), reset_id = "ld_reset", chips_id = "ld_filter_chips", prefix = "ld"),
  list(name = "tab3", ui = repo_file("R", "ui_tab3_team.R"), server = repo_file("R", "server_tab3.R"), reset_id = "tr_reset", chips_id = "tr_filter_chips", prefix = "tr"),
  list(name = "tab4", ui = repo_file("R", "ui_tab4_gamelogs.R"), server = repo_file("R", "server_tab4.R"), reset_id = "gl_reset", chips_id = "gl_filter_chips", prefix = "gl"),
  list(name = "tab5", ui = repo_file("R", "ui_tab5_traditional.R"), server = repo_file("R", "server_tab5_traditional.R"), reset_id = "ts_reset", chips_id = "ts_filter_chips", prefix = "ts"),
  list(name = "tab7", ui = repo_file("R", "ui_tab7_compare.R"), server = repo_file("R", "server_tab7_compare.R"), reset_id = "cmp_reset", chips_id = "cmp_filter_chips", prefix = "cmp")
)

test_that("each tab UI contains reset button and chips output", {
  for (s in TAB_SPECS) {
    ui_txt <- paste(readLines(s$ui, warn = FALSE), collapse = "\n")
    expect_true(grepl(sprintf("actionButton\\(\\\"%s\\\"", s$reset_id), ui_txt), info = s$name)
    # The chips output reaches the page either directly or through
    # filter_chips_row(), which pairs it with the min-possession controls.
    expect_true(grepl(sprintf("(uiOutput|filter_chips_row)\\(\\s*\\\"%s\\\"", s$chips_id), ui_txt), info = s$name)
  }
})

test_that("each tab server wires reset observer and chips builder", {
  for (s in TAB_SPECS) {
    server_txt <- paste(readLines(s$server, warn = FALSE), collapse = "\n")
    expect_true(grepl(sprintf("observeEvent\\(input\\$%s", s$reset_id), server_txt), info = s$name)
    expect_true(grepl(sprintf("output\\$%s\\s*<-\\s*renderUI", s$chips_id), server_txt), info = s$name)
    expect_true(grepl("build_filter_chips\\(", server_txt), info = s$name)
    expect_true(grepl(sprintf("\\\"%s\\\"", s$prefix), server_txt), info = s$name)
    expect_true(grepl(sprintf("reset_btn_id\\s*=\\s*\\\"%s\\\"", s$reset_id), server_txt), info = s$name)
  }
})
