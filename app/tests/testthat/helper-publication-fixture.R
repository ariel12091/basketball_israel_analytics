publication_repo_root <- function() {
  path <- normalizePath(getwd(), winslash = "/")
  while (!file.exists(file.path(path, "etl", "publication.R"))) {
    parent <- dirname(path)
    if (identical(parent, path)) stop("Cannot locate publication helpers")
    path <- parent
  }
  path
}

publication_fixture <- function() {
  testthat::skip_if_not(identical(Sys.getenv("RUN_DB_TESTS", "0"), "1"))
  root <- publication_repo_root()
  if (file.exists(file.path(root, "etl/.Renviron"))) readRenviron(file.path(root, "etl/.Renviron"))
  source(file.path(root, "etl/publication.R"), local = .GlobalEnv)
  owner <- publication_connect()
  schema <- paste0("pubtest_", Sys.getpid(), "_", sample.int(1e8, 1))
  qs <- as.character(DBI::dbQuoteIdentifier(owner, schema))
  snapshot <- function() {
    exists <- DBI::dbGetQuery(owner, "SELECT to_regclass('basketball_test.publication') IS NOT NULL AS ok")$ok[[1]]
    if (exists) DBI::dbGetQuery(owner, "SELECT * FROM basketball_test.publication ORDER BY league") else NULL
  }
  before <- snapshot()
  DBI::dbExecute(owner, paste("CREATE SCHEMA", qs))
  sql <- paste(readLines(file.path(root, "sql/publication/001_publication.sql"), warn = FALSE), collapse = "\n")
  DBI::dbExecute(owner, gsub("__SCHEMA__", qs, sql, fixed = TRUE), immediate = TRUE)
  DBI::dbExecute(owner, paste0("CREATE TABLE ", qs, ".fixture (n int)"))
  list(con = owner, schema = schema, qs = qs, status = function() {
    DBI::dbGetQuery(owner, paste0("SELECT * FROM ", qs, ".publication_status('il')"))
  }, cleanup = function() {
    # Scratch-only writes; verify the live publication relations remain unchanged.
    testthat::expect_identical(snapshot(), before)
    DBI::dbExecute(owner, paste("DROP SCHEMA", qs, "CASCADE"))
    DBI::dbDisconnect(owner)
  })
}
