# Credentials for the live-DB tests that must run as one specific role.
#
# The identity and app_api proofs must run as the existing app_readonly login
# (execution amendment, 2026-10-20); the publication tests deploy into a
# scratch pubtest_* schema and need the object owner (the ETL role). A run
# whose environment holds the other role -- the nightly CI job runs with the
# ETL secrets, a local run from app/ picks up app/.Renviron -- must skip, not
# error, and must not leave another role's credentials in the environment for
# the test files that run after it. So credentials are read into a list:
# the environment when it already holds the wanted role, otherwise that role's
# .Renviron file, read without changing the environment.

DB_ROLE_KEYS <- c("PG_HOST", "PG_DB", "PG_USER", "PG_PASS", "PG_SSLMODE")

db_role_repo_root <- function() {
  path <- normalizePath(getwd(), winslash = "/")
  while (!file.exists(file.path(path, "etl", "publication.R"))) {
    parent <- dirname(path)
    if (identical(parent, path)) return(NULL)
    path <- parent
  }
  path
}

# Values of DB_ROLE_KEYS as one .Renviron file sets them; the environment is
# restored on exit.
db_role_read_renviron <- function(path) {
  before <- Sys.getenv(DB_ROLE_KEYS, unset = NA_character_, names = TRUE)
  on.exit({
    set <- before[!is.na(before)]
    if (length(set)) do.call(Sys.setenv, as.list(set))
    unset <- names(before)[is.na(before)]
    if (length(unset)) Sys.unsetenv(unset)
  }, add = TRUE)
  Sys.unsetenv(DB_ROLE_KEYS)
  readRenviron(path)
  as.list(Sys.getenv(DB_ROLE_KEYS))
}

db_role_is_readonly <- function(user) grepl("^app_readonly([.]|$)", user)

# role: "readonly" (app_readonly) or "owner" (anything else, i.e. the ETL
# role). Returns NULL when no matching credentials are available.
db_role_creds <- function(role = c("readonly", "owner")) {
  role <- match.arg(role)
  wanted <- function(creds) {
    !is.null(creds) &&
      all(nzchar(unlist(creds[c("PG_HOST", "PG_DB", "PG_USER", "PG_PASS")]))) &&
      identical(db_role_is_readonly(creds$PG_USER), role == "readonly")
  }
  env <- as.list(Sys.getenv(DB_ROLE_KEYS))
  if (wanted(env)) return(env)
  root <- db_role_repo_root()
  file <- if (is.null(root)) "" else
    file.path(root, if (role == "readonly") "app" else "etl", ".Renviron")
  if (nzchar(file) && file.exists(file)) {
    creds <- db_role_read_renviron(file)
    if (wanted(creds)) return(creds)
  }
  NULL
}

# Skips the calling test file/block unless live-DB tests are on and the role's
# credentials exist; returns the credentials otherwise.
db_role_require <- function(role = c("readonly", "owner")) {
  role <- match.arg(role)
  testthat::skip_if_not(identical(Sys.getenv("RUN_DB_TESTS", "0"), "1"),
                        "RUN_DB_TESTS is not 1")
  testthat::skip_if_not_installed("DBI")
  testthat::skip_if_not_installed("RPostgres")
  creds <- db_role_creds(role)
  if (is.null(creds)) testthat::skip(paste("No", role, "database credentials"))
  creds
}

# Always the session port: these tests take advisory locks or compare
# snapshots, which the transaction pooler cannot carry.
db_role_connect <- function(creds, ...) {
  DBI::dbConnect(RPostgres::Postgres(), host = creds$PG_HOST, port = 5432L,
    dbname = creds$PG_DB, user = creds$PG_USER, password = creds$PG_PASS,
    sslmode = if (nzchar(creds$PG_SSLMODE)) creds$PG_SSLMODE else "require",
    connect_timeout = 15L, ...)
}

# TRUE once the production web foundation is applied: app_api exists and the
# publication markers it reads exist.
db_web_foundation_applied <- function(con) {
  isTRUE(DBI::dbGetQuery(con,
    "SELECT to_regnamespace('app_api') IS NOT NULL
        AND to_regclass('basketball_test.publication') IS NOT NULL AS ok")$ok[[1]])
}
