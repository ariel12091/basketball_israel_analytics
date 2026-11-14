# Live-database tests must connect through db_test_connect()
# (helper-db-connect.R), which sets a statement timeout. A direct
# DBI::dbConnect() has none, and that is how a 55 s full-scan query ran
# nightly for days without failing a run.
test_that("live-database tests connect only through db_test_connect()", {
  pattern <- "dbConnect\\("
  # The pattern must find the one sanctioned call, or the check below
  # would pass on anything.
  helper <- readLines(test_path("helper-db-connect.R"), warn = FALSE)
  expect_true(any(grepl(pattern, helper)))

  files <- list.files(test_path(), pattern = "^test-.*\\.R$", full.names = TRUE)
  files <- files[basename(files) != "test-db-test-connect.R"]
  hits <- unlist(lapply(files, function(f) {
    lines <- readLines(f, warn = FALSE)
    at <- grep(pattern, lines)
    if (length(at)) sprintf("%s:%d", basename(f), at) else character(0)
  }))
  expect_identical(hits, character(0),
                   info = "use db_test_connect() instead of DBI::dbConnect()")
})
