# api/run.R itself, end to end: the real script in its own R process, serving
# recorded answers (IBPL_API_REPLAY_DIR) over real HTTP on a free local port.
# Every other API test calls the router in-process; this one proves the
# launcher boots, binds and answers.

# One GET over a raw socket (no HTTP client package needed); returns the
# status code, or NA when nothing is listening yet.
api_run_get <- function(port, path) {
  con <- tryCatch(
    suppressWarnings(socketConnection("127.0.0.1", port, blocking = TRUE, open = "r+b",
                                      timeout = 60)),
    error = function(e) NULL
  )
  if (is.null(con)) return(NA_integer_)
  on.exit(close(con), add = TRUE)
  writeLines(c(sprintf("GET %s HTTP/1.0", path), "Host: 127.0.0.1", ""), con, sep = "\r\n")
  status <- readLines(con, n = 1L, warn = FALSE)
  as.integer(sub("^HTTP/[0-9.]+ ([0-9]{3}).*$", "\\1", status))
}

test_that("api/run.R serves a 200 and a 400 over HTTP", {
  skip_if_not_installed("callr")
  port <- tryCatch(httpuv::randomPort(host = "127.0.0.1"), error = function(e) NULL)
  if (is.null(port)) skip("no free local port could be bound")

  log <- tempfile("api_run_", fileext = ".log")
  proc <- withr::with_envvar(
    c(IBPL_API_REPLAY_DIR = API_GL_FIXTURES, API_HOST = "127.0.0.1", API_PORT = as.character(port)),
    callr::r_bg(function(root) {
      setwd(root)
      source(file.path("api", "run.R"))
    }, args = list(root = api_test_root), stdout = log, stderr = "2>&1")
  )
  on.exit(proc$kill(), add = TRUE)

  # The app sources in the child before the port opens.
  deadline <- Sys.time() + 120
  status <- NA_integer_
  while (is.na(status) && proc$is_alive() && Sys.time() < deadline) {
    Sys.sleep(0.5)
    status <- api_run_get(port, "/v1/il/game-logs?season=2026")
  }
  if (is.na(status)) {
    proc$kill()
    fail(paste0("api/run.R never answered on port ", port, ":\n",
                paste(utils::tail(readLines(log, warn = FALSE), 40), collapse = "\n")))
    return(invisible())
  }
  expect_identical(status, 200L)
  expect_identical(api_run_get(port, "/v1/il/teams?season=x"), 400L)
})
