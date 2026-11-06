publication_repo_root <- function() {
  path <- normalizePath(getwd(), winslash = "/")
  while (!file.exists(file.path(path, "etl", "publication.R"))) {
    parent <- dirname(path)
    if (identical(parent, path)) stop("Cannot locate publication helpers")
    path <- parent
  }
  path
}

# A scratch pubtest_<pid>_<rand> schema holding the publication SQL, deployed
# as the object owner (review R3). Call it inside test_that(): it skips unless
# RUN_DB_TESTS=1 and owner credentials exist, and it puts those credentials in
# the environment only for the calling test (etl/publication.R reads them from
# there), so later test files still see their own role.
publication_fixture <- function(envir = parent.frame()) {
  creds <- db_role_require("owner")
  withr::local_envvar(unlist(creds), .local_envir = envir)
  root <- publication_repo_root()
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
  list(con = owner, schema = schema, qs = qs, status = function(league = "il") {
    DBI::dbGetQuery(owner, paste0("SELECT * FROM ", qs, ".publication_status($1)"),
                    params = list(league))
  }, cleanup = function() {
    # Scratch-only writes; verify the live publication relations remain unchanged.
    testthat::expect_identical(snapshot(), before)
    DBI::dbExecute(owner, paste("DROP SCHEMA", qs, "CASCADE"))
    DBI::dbDisconnect(owner)
  })
}
