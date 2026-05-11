logger_env <- new.env(parent = globalenv())
sys.source(repo_file("R", "logger.R"), envir = logger_env)
app_log <- logger_env$app_log
session_log_id <- logger_env$session_log_id

with_env <- function(vars, expr) {
  old <- Sys.getenv(names(vars), names = TRUE, unset = NA)
  on.exit({
    for (nm in names(old)) {
      if (is.na(old[[nm]])) Sys.unsetenv(nm) else Sys.setenv(.named = setNames(list(old[[nm]]), nm))
    }
  })
  do.call(Sys.setenv, as.list(vars))
  force(expr)
}

test_that("app_log formats line with timestamp, level, component", {
  msgs <- testthat::capture_messages(app_log("tab1", "hello"))
  expect_length(msgs, 1L)
  expect_match(msgs[[1]], "\\[INFO\\] \\[tab1\\] hello")
  expect_match(msgs[[1]], "^[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2} ")
})

test_that("app_log attaches sid when session is given", {
  fake_session <- list(token = "abcdef1234567890")
  msgs <- testthat::capture_messages(app_log("tab2", "hi", session = fake_session))
  expect_match(msgs[[1]], "sid=abcdef12")
})

test_that("app_log omits sid when session is NULL or token missing", {
  msgs <- testthat::capture_messages(app_log("tab2", "hi"))
  expect_false(grepl("sid=", msgs[[1]]))
  msgs2 <- testthat::capture_messages(app_log("tab2", "hi", session = list()))
  expect_false(grepl("sid=", msgs2[[1]]))
})

test_that("APP_LOG_LEVEL gates lower-priority levels", {
  with_env(c(APP_LOG_LEVEL = "WARN"), {
    msgs <- testthat::capture_messages(app_log("tab1", "ignored", level = "INFO"))
    expect_length(msgs, 0L)
    msgs2 <- testthat::capture_messages(app_log("tab1", "shown", level = "ERROR"))
    expect_length(msgs2, 1L)
    expect_match(msgs2[[1]], "\\[ERROR\\]")
  })
})

test_that("unknown level falls back to INFO", {
  msgs <- testthat::capture_messages(app_log("tab1", "x", level = "VERBOSE"))
  expect_match(msgs[[1]], "\\[INFO\\]")
})

test_that("app_log writes to file when APP_LOG_FILE is set", {
  tf <- tempfile(fileext = ".log")
  on.exit(unlink(tf), add = TRUE)
  with_env(c(APP_LOG_FILE = tf), {
    suppressMessages(app_log("tabX", "to-disk"))
  })
  expect_true(file.exists(tf))
  lines <- readLines(tf, warn = FALSE)
  expect_length(lines, 1L)
  expect_match(lines[[1]], "\\[INFO\\] \\[tabX\\] to-disk")
})

test_that("app_log does not crash on invalid log file path", {
  with_env(c(APP_LOG_FILE = "/no/such/dir/that/does/not/exist/xyz.log"), {
    expect_error(suppressMessages(app_log("tabX", "should-not-crash")), NA)
  })
})

test_that("session_log_id returns first 8 chars of token", {
  expect_identical(session_log_id(list(token = "1234567890abcdef")), "12345678")
  expect_identical(session_log_id(NULL), "")
  expect_identical(session_log_id(list()), "")
  expect_identical(session_log_id(list(token = "")), "")
})
