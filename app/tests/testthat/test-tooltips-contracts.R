test_that("tooltip infrastructure is defined centrally", {
  global_txt <- read_repo_txt("R", "global.R")
  app_js_txt <- read_repo_txt("www", "app.js")

  expect_true(grepl("COLUMN_TOOLTIPS\\s*<-\\s*c\\(", global_txt))
  expect_true(grepl("FILTER_TOOLTIPS\\s*<-\\s*c\\(", global_txt))
  expect_true(grepl("\"quick_preset\"\\s*=\\s*\"Apply a prebuilt compare split", global_txt))
  expect_true(grepl("tt\\s*<-\\s*function\\(label, key\\)", global_txt))
  expect_true(grepl("HEADER_TOOLTIP_JS\\s*<-\\s*DT::JS", global_txt))
  expect_true(grepl("window\\.applyViewModeTooltips\\s*=\\s*function\\(", app_js_txt))
  expect_true(grepl("shiny:connected shiny:value", app_js_txt))
})

test_that("tooltip wiring is present in compare and tooltip-heavy tabs", {
  compare_txt <- read_repo_txt("R", "server_tab7_compare.R")
  lineup_filter_txt <- read_repo_txt("R", "mod_lineup_player_filter.R")
  tab1_ui_txt <- read_repo_txt("R", "ui_tab1_onoff.R")
  tab2_ui_txt <- read_repo_txt("R", "ui_tab2_lineup.R")
  tab7_ui_txt <- read_repo_txt("R", "ui_tab7_compare.R")

  expect_true(grepl("headerCallback\\s*=\\s*DT::JS\\(sprintf\\(", compare_txt))
  expect_true(grepl("jsonlite::toJSON\\(as\\.list\\(COLUMN_TOOLTIPS\\)", compare_txt))
  expect_true(grepl("tt\\(\"Min possessions per side \\(eligibility\\):\", \"min_poss_side\"\\)", tab1_ui_txt))
  expect_true(grepl("lineup_player_filter_ui\\(", tab2_ui_txt))
  expect_true(grepl("tt\\(\"Players On \\(exact/contains\\)\", \"players_on\"\\)", lineup_filter_txt))
  expect_true(grepl("tt\\(\"Quick preset\", \"quick_preset\"\\)", tab7_ui_txt))
})
