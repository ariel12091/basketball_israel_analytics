library(testthat)

skip_if_not_installed("shinytest2")
skip_if_not(Sys.getenv("RUN_E2E", "0") == "1")

new_app <- function(name) {
  app_root <- normalizePath(file.path("..", ".."), winslash = "/", mustWork = TRUE)
  shinytest2::AppDriver$new(
    app_dir = app_root,
    name = name,
    variant = shinytest2::platform_variant(),
    load_timeout = 30000,
    seed = 101
  )
}

is_cleared <- function(v) {
  is.null(v) || length(v) == 0 || identical(v, "")
}

wait_for_game_type_cleared <- function(app, input_id, timeout_sec = 15, poll_sec = 0.5) {
  deadline <- Sys.time() + timeout_sec
  repeat {
    v <- app$get_value(input = input_id)
    if (is_cleared(v)) return(invisible(TRUE))
    if (Sys.time() >= deadline) break
    Sys.sleep(poll_sec)
  }
  fail(paste0("Input '", input_id, "' did not clear within ", timeout_sec, "s"))
}

test_that("e2e tab1 reset flow", {
  app <- new_app("tab1_reset")
  on.exit(app$stop(), add = TRUE)
  app$set_inputs(main_tabs = "onoff")
  app$set_inputs(on_game_type = "5")
  app$set_inputs(reset_defaults = 1)
  wait_for_game_type_cleared(app, "on_game_type")
})

test_that("e2e tab2 reset flow", {
  app <- new_app("tab2_reset")
  on.exit(app$stop(), add = TRUE)
  app$set_inputs(main_tabs = "lineup_data")
  app$set_inputs(ld_game_type = "5")
  app$set_inputs(ld_reset = 1)
  wait_for_game_type_cleared(app, "ld_game_type")
})

test_that("e2e tab3 reset flow", {
  app <- new_app("tab3_reset")
  on.exit(app$stop(), add = TRUE)
  app$set_inputs(main_tabs = "team_ratings")
  app$set_inputs(tr_game_type = "5")
  app$set_inputs(tr_reset = 1)
  wait_for_game_type_cleared(app, "tr_game_type")
})

test_that("e2e tab4 reset flow", {
  app <- new_app("tab4_reset")
  on.exit(app$stop(), add = TRUE)
  app$set_inputs(main_tabs = "game_logs")
  app$set_inputs(gl_game_type = "5")
  app$set_inputs(gl_reset = 1)
  wait_for_game_type_cleared(app, "gl_game_type")
})

test_that("e2e tab5 reset flow", {
  app <- new_app("tab5_reset")
  on.exit(app$stop(), add = TRUE)
  app$set_inputs(main_tabs = "traditional_stats")
  app$set_inputs(ts_game_type = "5")
  app$set_inputs(ts_reset = 1)
  wait_for_game_type_cleared(app, "ts_game_type")
})
