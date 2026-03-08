library(testthat)

skip_if_not_installed("shinytest2")
skip_if_not(Sys.getenv("RUN_E2E", "0") == "1")

new_app <- function(name) {
  app_root <- normalizePath(file.path("..", ".."), winslash = "/", mustWork = TRUE)
  shinytest2::AppDriver$new(
    app_dir = app_root,
    name = name,
    variant = shinytest2::platform_variant(),
    timeout = 60000,
    load_timeout = 30000,
    seed = 101
  )
}

is_cleared <- function(v) {
  is.null(v) || length(v) == 0 || identical(v, "")
}

set_input <- function(app, input_id, value) {
  do.call(app$set_inputs, setNames(list(value), input_id))
}

wait_for_game_type_cleared <- function(app, input_id, reset_id, timeout_sec = 15, poll_sec = 0.5) {
  start_time <- Sys.time()
  deadline <- Sys.time() + timeout_sec
  retriggered <- FALSE
  repeat {
    v <- app$get_value(input = input_id)
    if (is_cleared(v)) return(invisible(TRUE))
    if (!retriggered && as.numeric(difftime(Sys.time(), start_time, units = "secs")) >= (timeout_sec / 2)) {
      with_retry(set_input(app, reset_id, 2L))
      retriggered <- TRUE
    }
    if (Sys.time() >= deadline) break
    Sys.sleep(poll_sec)
  }
  fail(paste0("Input '", input_id, "' did not clear within ", timeout_sec, "s"))
}

with_retry <- function(expr, tries = 3L, sleep_sec = 1) {
  last_err <- NULL
  for (i in seq_len(tries)) {
    out <- tryCatch(list(ok = TRUE, value = force(expr)), error = function(e) list(ok = FALSE, err = e))
    if (isTRUE(out$ok)) return(out$value)
    last_err <- out$err
    if (i < tries) Sys.sleep(sleep_sec)
  }
  stop(last_err)
}

test_that("e2e tab1 reset flow", {
  app <- new_app("tab1_reset")
  on.exit(app$stop(), add = TRUE)
  with_retry(set_input(app, "main_tabs", "onoff"))
  with_retry(set_input(app, "on_game_type", "5"))
  with_retry(set_input(app, "reset_defaults", 1L))
  wait_for_game_type_cleared(app, "on_game_type", "reset_defaults", timeout_sec = 60)
})

test_that("e2e tab2 reset flow", {
  app <- new_app("tab2_reset")
  on.exit(app$stop(), add = TRUE)
  with_retry(set_input(app, "main_tabs", "lineup_data"))
  with_retry(set_input(app, "ld_game_type", "5"))
  with_retry(set_input(app, "ld_reset", 1L))
  wait_for_game_type_cleared(app, "ld_game_type", "ld_reset", timeout_sec = 60)
})

test_that("e2e tab3 reset flow", {
  app <- new_app("tab3_reset")
  on.exit(app$stop(), add = TRUE)
  with_retry(set_input(app, "main_tabs", "team_ratings"))
  with_retry(set_input(app, "tr_game_type", "5"))
  with_retry(set_input(app, "tr_reset", 1L))
  wait_for_game_type_cleared(app, "tr_game_type", "tr_reset", timeout_sec = 60)
})

test_that("e2e tab4 reset flow", {
  app <- new_app("tab4_reset")
  on.exit(app$stop(), add = TRUE)
  with_retry(set_input(app, "main_tabs", "game_logs"))
  with_retry(set_input(app, "gl_game_type", "5"))
  with_retry(set_input(app, "gl_reset", 1L))
  wait_for_game_type_cleared(app, "gl_game_type", "gl_reset", timeout_sec = 60)
})

test_that("e2e tab5 reset flow", {
  app <- new_app("tab5_reset")
  on.exit(app$stop(), add = TRUE)
  with_retry(set_input(app, "main_tabs", "traditional_stats"))
  with_retry(set_input(app, "ts_game_type", "5"))
  with_retry(set_input(app, "ts_reset", 1L))
  wait_for_game_type_cleared(app, "ts_game_type", "ts_reset", timeout_sec = 60)
})
