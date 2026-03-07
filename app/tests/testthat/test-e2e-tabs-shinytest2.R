library(testthat)

skip_if_not_installed("shinytest2")
skip_if_not(Sys.getenv("RUN_E2E", "0") == "1")

new_app <- function(name) {
  shinytest2::AppDriver$new(
    app_dir = ".",
    name = name,
    variant = shinytest2::platform_variant(),
    load_timeout = 30000,
    seed = 101
  )
}

test_that("e2e tab1 reset flow", {
  app <- new_app("tab1_reset")
  on.exit(app$stop(), add = TRUE)
  app$set_inputs(main_tabs = "onoff")
  app$set_inputs(on_game_type = "5")
  app$set_inputs(reset_defaults = 1)
  app$wait_for_idle()
  gt <- app$get_value(input = "on_game_type")
  expect_true(is.null(gt) || length(gt) == 0 || identical(gt, ""))
})

test_that("e2e tab2 reset flow", {
  app <- new_app("tab2_reset")
  on.exit(app$stop(), add = TRUE)
  app$set_inputs(main_tabs = "lineup_data")
  app$set_inputs(ld_game_type = "5")
  app$set_inputs(ld_reset = 1)
  app$wait_for_idle()
  gt <- app$get_value(input = "ld_game_type")
  expect_true(is.null(gt) || length(gt) == 0 || identical(gt, ""))
})

test_that("e2e tab3 reset flow", {
  app <- new_app("tab3_reset")
  on.exit(app$stop(), add = TRUE)
  app$set_inputs(main_tabs = "team_ratings")
  app$set_inputs(tr_game_type = "5")
  app$set_inputs(tr_reset = 1)
  app$wait_for_idle()
  gt <- app$get_value(input = "tr_game_type")
  expect_true(is.null(gt) || length(gt) == 0 || identical(gt, ""))
})

test_that("e2e tab4 reset flow", {
  app <- new_app("tab4_reset")
  on.exit(app$stop(), add = TRUE)
  app$set_inputs(main_tabs = "game_logs")
  app$set_inputs(gl_game_type = "5")
  app$set_inputs(gl_reset = 1)
  app$wait_for_idle()
  gt <- app$get_value(input = "gl_game_type")
  expect_true(is.null(gt) || length(gt) == 0 || identical(gt, ""))
})

test_that("e2e tab5 reset flow", {
  app <- new_app("tab5_reset")
  on.exit(app$stop(), add = TRUE)
  app$set_inputs(main_tabs = "traditional_stats")
  app$set_inputs(ts_game_type = "5")
  app$set_inputs(ts_reset = 1)
  app$wait_for_idle()
  gt <- app$get_value(input = "ts_game_type")
  expect_true(is.null(gt) || length(gt) == 0 || identical(gt, ""))
})
