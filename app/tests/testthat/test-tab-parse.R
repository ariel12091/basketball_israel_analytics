TAB_FILES <- c(
  repo_file("R", "ui_tab1_onoff.R"),
  repo_file("R", "ui_tab2_lineup.R"),
  repo_file("R", "ui_tab3_team.R"),
  repo_file("R", "ui_tab4_gamelogs.R"),
  repo_file("R", "ui_tab5_traditional.R"),
  repo_file("R", "server_tab1.R"),
  repo_file("R", "server_tab2.R"),
  repo_file("R", "server_tab3.R"),
  repo_file("R", "server_tab4.R"),
  repo_file("R", "server_tab5_traditional.R")
)

test_that("all tab UI/server files parse", {
  for (f in TAB_FILES) {
    expect_error(parse(file = f), NA)
  }
})
