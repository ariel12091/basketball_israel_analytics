# Security re-apply: the hardening (sql/security/enable_readonly_rls.sql)
# followed by the audit (sql/security/audit_app_access.sql) in one
# transaction. Dry run by default; CONFIRM_DB_SECURITY_APPLY=1 commits.
#
# Run as a script: Rscript scripts/apply_db_security.R (from the repo root).
# Sourced by scripts/apply_web_foundation.R, which calls
# run_db_security_hardening() inside its own transaction so the web role,
# the app_api schema and their grants are audited before anything commits.
# Sourcing defines the function only; the block at the bottom runs only
# when this file is the script being executed.

# Executes the hardening and the audit on an open transaction and stops on any
# violation, leaving the commit/rollback decision to the caller.
run_db_security_hardening <- function(con, root = ".") {
  hardening_path <- file.path(root, "sql", "security", "enable_readonly_rls.sql")
  audit_path <- file.path(root, "sql", "security", "audit_app_access.sql")
  if (!file.exists(hardening_path) || !file.exists(audit_path)) {
    stop("Run this script from the repository root")
  }

  hardening_sql <- paste(readLines(hardening_path, warn = FALSE), collapse = "\n")
  audit_sql <- paste(readLines(audit_path, warn = FALSE), collapse = "\n")

  invisible(DBI::dbExecute(con, "SET LOCAL client_min_messages = warning"))
  invisible(DBI::dbExecute(con, hardening_sql))
  violations <- DBI::dbGetQuery(con, audit_sql)

  if (nrow(violations)) {
    print(violations)
    stop("Database security audit failed; transaction will roll back")
  }
  invisible(violations)
}

if (sys.nframe() == 0L) {
  confirm_apply <- identical(Sys.getenv("CONFIRM_DB_SECURITY_APPLY", "0"), "1")

  if (file.exists("etl/.Renviron")) {
    readRenviron("etl/.Renviron")
  }

  required_env <- c("PG_HOST", "PG_DB", "PG_USER", "PG_PASS")
  missing_env <- required_env[!nzchar(Sys.getenv(required_env))]
  if (length(missing_env)) {
    stop(
      "Missing required database environment variables: ",
      paste(missing_env, collapse = ", ")
    )
  }

  suppressPackageStartupMessages({
    library(DBI)
    library(RPostgres)
  })

  ddl_port <- suppressWarnings(as.integer(Sys.getenv("PG_DDL_PORT", "5432")))
  if (!is.finite(ddl_port) || ddl_port <= 0L) {
    stop("PG_DDL_PORT must be a valid PostgreSQL port")
  }

  con <- dbConnect(
    Postgres(),
    host = Sys.getenv("PG_HOST"),
    port = ddl_port,
    dbname = Sys.getenv("PG_DB"),
    user = Sys.getenv("PG_USER"),
    password = Sys.getenv("PG_PASS"),
    sslmode = Sys.getenv("PG_SSLMODE", "require"),
    connect_timeout = 10
  )
  on.exit(dbDisconnect(con), add = TRUE)

  role <- dbGetQuery(con, "SELECT current_user AS role")$role[[1]]
  message("Database role: ", role)
  message(
    if (confirm_apply) {
      "Mode: APPLY (transaction will commit)"
    } else {
      "Mode: DRY RUN (transaction will roll back)"
    }
  )

  dbBegin(con)
  transaction_open <- TRUE
  on.exit({
    if (transaction_open) {
      try(dbRollback(con), silent = TRUE)
    }
  }, add = TRUE)

  run_db_security_hardening(con)

  if (confirm_apply) {
    dbCommit(con)
    transaction_open <- FALSE
    message("Database security hardening committed.")
  } else {
    dbRollback(con)
    transaction_open <- FALSE
    message("Database security hardening validated and rolled back.")
  }
}
