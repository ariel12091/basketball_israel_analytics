CHIPS_TABS <- list(
  list(file = "ui_tab1_onoff.R",         output = "on_filter_chips"),
  list(file = "ui_tab2_lineup.R",        output = "ld_filter_chips"),
  list(file = "ui_tab3_team.R",          output = "tr_filter_chips"),
  list(file = "ui_tab4_gamelogs.R",      output = "gl_filter_chips"),
  list(file = "ui_tab5_traditional.R",   output = "ts_filter_chips"),
  list(file = "ui_tab8_euro.R",          output = "euro_filter_chips"),
  list(file = "ui_tab9_euro_team.R",     output = "euroteam_filter_chips"),
  list(file = "ui_tab10_euro_lineups.R", output = "euro_ld_filter_chips"),
  list(file = "ui_tab11_euro_gamelogs.R", output = "eurogl_filter_chips")
)

test_that("every data tab reaches its chips through the shared row", {
  for (tab in CHIPS_TABS) {
    txt <- read_repo_txt("R", tab$file)
    expect_true(
      grepl(sprintf('filter_chips_row(\n          "%s"', tab$output), txt, fixed = TRUE) ||
        grepl(sprintf('filter_chips_row("%s"', tab$output), txt, fixed = TRUE) ||
        grepl(sprintf('filter_chips_row(\n        "%s"', tab$output), txt, fixed = TRUE),
      info = paste(tab$file, "does not wrap", tab$output, "in filter_chips_row()")
    )
    expect_false(
      grepl(sprintf('uiOutput("%s")', tab$output), txt, fixed = TRUE),
      info = paste(tab$file, "still renders", tab$output, "bare")
    )
  }
})
