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

hardening_path <- file.path("sql", "security", "enable_readonly_rls.sql")
audit_path <- file.path("sql", "security", "audit_app_access.sql")
if (!file.exists(hardening_path) || !file.exists(audit_path)) {
  stop("Run this script from the repository root")
}

hardening_sql <- paste(readLines(hardening_path, warn = FALSE), collapse = "\n")
audit_sql <- paste(readLines(audit_path, warn = FALSE), collapse = "\n")

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

invisible(dbExecute(con, "SET LOCAL client_min_messages = warning"))
invisible(dbExecute(con, hardening_sql))
violations <- dbGetQuery(con, audit_sql)

if (nrow(violations)) {
  print(violations)
  stop("Database security audit failed; transaction will roll back")
}

if (confirm_apply) {
  dbCommit(con)
  transaction_open <- FALSE
  message("Database security hardening committed.")
} else {
  dbRollback(con)
  transaction_open <- FALSE
  message("Database security hardening validated and rolled back.")
}
