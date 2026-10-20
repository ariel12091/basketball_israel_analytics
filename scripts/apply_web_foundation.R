# Additive web API deployment. Uses the existing app_readonly login unchanged.
# Dry run executes DDL and audit in a transaction, then rolls back.
# CONFIRM_WEB_FOUNDATION_APPLY=1 commits. No role/password/role-default changes.
main <- function() {
  confirm <- identical(Sys.getenv("CONFIRM_WEB_FOUNDATION_APPLY", "0"), "1")
  if (file.exists("etl/.Renviron")) readRenviron("etl/.Renviron")
  for (key in c("PG_HOST", "PG_DB", "PG_USER", "PG_PASS")) {
    if (!nzchar(Sys.getenv(key))) stop("Missing environment variable: ", key)
  }
  paths <- sort(list.files("sql/app_api", "\\.sql$", full.names = TRUE))
  if (!length(paths)) stop("Run from repository root: sql/app_api is missing")
  suppressPackageStartupMessages({ library(DBI); library(RPostgres) })
  source("scripts/apply_db_security.R", local = TRUE)
  con <- dbConnect(Postgres(), host = Sys.getenv("PG_HOST"),
                   port = as.integer(Sys.getenv("PG_DDL_PORT", "5432")),
                   dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
                   password = Sys.getenv("PG_PASS"), sslmode = "require",
                   connect_timeout = 15)
  on.exit(dbDisconnect(con), add = TRUE)
  dbBegin(con)
  open <- TRUE
  on.exit(if (open) try(dbRollback(con), silent = TRUE), add = TRUE)
  dbExecute(con, "SET LOCAL lock_timeout = '5s'")
  dbExecute(con, "SET LOCAL statement_timeout = '30s'")
  for (path in paths) {
    message("Deploying: ", path)
    dbExecute(con, paste(readLines(path, warn = FALSE), collapse = "\n"), immediate = TRUE)
  }
  run_db_security_hardening(con)
  if (confirm) dbCommit(con) else dbRollback(con)
  open <- FALSE
  message(if (confirm) "Web API committed; existing roles unchanged."
          else "Web API validated and rolled back; existing roles unchanged.")
}
if (sys.nframe() == 0L) main()
