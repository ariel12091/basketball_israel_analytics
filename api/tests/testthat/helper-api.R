# Shared by the API tests. Every test runs on recorded query answers
# (golden_replay_conn()); none touches a live database.

api_test_root <- local({
  dir <- normalizePath(getwd(), winslash = "/")
  repeat {
    if (file.exists(file.path(dir, "app", "app.R")) && dir.exists(file.path(dir, "api"))) break
    parent <- dirname(dir)
    if (identical(parent, dir)) stop("API tests: cannot find the repo root above ", getwd())
    dir <- parent
  }
  dir
})

source(file.path(api_test_root, "app", "tests", "goldens", "harness.R"), local = TRUE)
source(file.path(api_test_root, "app", "tests", "goldens", "presets", "game_logs.R"), local = TRUE)
source(file.path(api_test_root, "api", "coverage.R"), local = TRUE)
source(file.path(api_test_root, "api", "R", "bootstrap.R"), local = TRUE)

API_GL_FIXTURES <- file.path(api_test_root, "app", "tests", "goldens", "fixtures", "game_logs")

# A fresh API environment on a replay connection.
api_test_env <- function(dir = API_GL_FIXTURES) {
  env <- new.env(parent = globalenv())
  suppressMessages(api_bootstrap(golden_replay_conn(dir), envir = env, root = api_test_root))
  env
}

# Plumber hands a handler its request with the raw query string, and a
# PlumberResponse; these stand in for both.
api_req <- function(qs = "") list(QUERY_STRING = qs)
api_res <- function() plumber:::PlumberResponse$new()
api_body <- function(res) jsonlite::fromJSON(res$body, simplifyVector = TRUE)

# The api_error a call signals (fails the test when it signals none).
api_error_of <- function(expr) {
  err <- tryCatch({
    force(expr)
    NULL
  }, api_error = function(e) e)
  if (is.null(err)) testthat::fail("expected an api_error, none was signalled")
  err
}

# A copy of a fixture directory in a fresh temp dir, for tests that edit
# recorded answers.
api_copy_fixtures <- function(from = API_GL_FIXTURES) {
  to <- tempfile("api_fixtures_")
  dir.create(to)
  file.copy(list.files(from, full.names = TRUE), to)
  to
}

# The fixture key of the query `call(env)` runs, read off the replay miss it
# raises in an empty fixture directory, so a hand-made answer can be stored
# for it without retyping the query.
api_fixture_key_of <- function(call) {
  empty <- tempfile("api_empty_")
  dir.create(empty)
  env <- api_test_env(empty)
  msg <- tryCatch({
    call(env)
    ""
  }, error = function(e) conditionMessage(e))
  key <- regmatches(msg, regexpr("key=[0-9a-f]{40}", msg))
  if (!length(key)) stop("api_fixture_key_of: the call ran no unrecorded query: ", msg)
  sub("^key=", "", key)
}

# A GET through a real router built by api_router() -- Plumber's filters,
# route matching and error handlers included -- in-process, as httpuv would
# hand it a request. Returns list(status, headers, body).
api_http <- function(router, path, qs = "") {
  qs <- sub("^[?]", "", qs)
  req <- new.env()
  req$REQUEST_METHOD <- "GET"
  req$PATH_INFO <- path
  req$QUERY_STRING <- if (nzchar(qs)) paste0("?", qs) else ""
  req$SCRIPT_NAME <- ""
  req$SERVER_NAME <- "127.0.0.1"
  req$SERVER_PORT <- "3010"
  req$HTTP_HOST <- "127.0.0.1:3010"
  req$rook.input <- list(read = function(...) raw(0), rewind = function() invisible(0L))
  out <- router$call(req)
  out$body <- if (is.raw(out$body)) rawToChar(out$body) else as.character(out$body)
  out$status <- as.integer(out$status)
  out
}
