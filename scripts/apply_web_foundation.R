# Web foundation deploy (React migration Phase 0, Task 1 onwards):
#   1. the app_web login role       sql/security/web_role.sql
#   2. its password                 from env APP_WEB_PASSWORD, set server-side
#   3. the app_api schema and API   sql/app_api/*.sql, in file-name order
#   4. the security re-apply        scripts/apply_db_security.R's
#                                   run_db_security_hardening(): hardening + audit
# all in ONE transaction on the DDL port, so nothing commits unless the audit
# reports zero violations.
#
# Dry run by default: prints every statement (password redacted), executes
# them and rolls back. CONFIRM_WEB_FOUNDATION_APPLY=1 commits.
#
# Env:
#   APP_WEB_PASSWORD          required to apply; never printed or written to a
#                             file. A dry run without it skips step 2.
#   APP_WEB_CONNECTION_LIMIT  default 10.
#   PG_* from etl/.Renviron (object owner), PG_DDL_PORT (default 5432).
#
# Idempotent: a re-run brings app_web back to its attributes, re-sets the
# password (this is also the rotation step), and re-applies every grant.
#
# Run from the repository root:
#   Rscript scripts/apply_web_foundation.R
#   CONFIRM_WEB_FOUNDATION_APPLY=1 Rscript scripts/apply_web_foundation.R

confirm_apply <- identical(Sys.getenv("CONFIRM_WEB_FOUNDATION_APPLY", "0"), "1")

web_role_path <- file.path("sql", "security", "web_role.sql")
app_api_dir <- file.path("sql", "app_api")
security_script <- file.path("scripts", "apply_db_security.R")
if (!file.exists(web_role_path) || !dir.exists(app_api_dir) ||
    !file.exists(security_script)) {
  stop("Run this script from the repository root")
}

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

# The password is checked before any connection. Applying without one would
# leave a login role nobody can use -- or, on a re-run, keep a credential the
# operator meant to rotate -- so APPLY fails closed.
app_web_password <- Sys.getenv("APP_WEB_PASSWORD", "")
min_password_chars <- 16L
if (nzchar(app_web_password) && nchar(app_web_password) < min_password_chars) {
  stop("APP_WEB_PASSWORD must be at least ", min_password_chars, " characters")
}
if (confirm_apply && !nzchar(app_web_password)) {
  stop("APP_WEB_PASSWORD is not set; refusing to apply without it")
}

connection_limit_raw <- Sys.getenv("APP_WEB_CONNECTION_LIMIT", "10")
if (!grepl("^[0-9]+$", connection_limit_raw)) {
  stop("APP_WEB_CONNECTION_LIMIT must be a positive integer")
}
connection_limit <- as.integer(connection_limit_raw)
if (is.na(connection_limit) || connection_limit < 1L || connection_limit > 100L) {
  stop("APP_WEB_CONNECTION_LIMIT must be between 1 and 100")
}

ddl_port <- suppressWarnings(as.integer(Sys.getenv("PG_DDL_PORT", "5432")))
if (!is.finite(ddl_port) || ddl_port <= 0L) {
  stop("PG_DDL_PORT must be a valid PostgreSQL port")
}

read_sql <- function(path) {
  paste(readLines(path, warn = FALSE), collapse = "\n")
}

web_role_sql <- gsub(
  "__APP_WEB_CONNECTION_LIMIT__",
  as.character(connection_limit),
  read_sql(web_role_path),
  fixed = TRUE
)
if (grepl("__[A-Z_]+__", web_role_sql)) {
  stop("Unsubstituted placeholder left in ", web_role_path)
}

app_api_paths <- sort(list.files(app_api_dir, pattern = "\\.sql$", full.names = TRUE))
if (!length(app_api_paths)) {
  stop("No SQL files found in ", app_api_dir)
}

# ALTER ROLE takes no bind parameters. The password travels only as the bound
# parameter of set_config(); format('%L') quotes it server-side inside the DO
# block, so it is never interpolated on the client and never appears in a
# statement's text. The transaction-local setting is cleared straight after.
password_bind_sql <- paste0(
  "SELECT 1 AS ok FROM ",
  "(SELECT set_config('app_web_setup.password', $1, true)) AS s"
)
password_set_sql <- paste(
  "DO $$",
  "BEGIN",
  "  EXECUTE format('ALTER ROLE app_web PASSWORD %L',",
  "                 current_setting('app_web_setup.password'));",
  "  PERFORM set_config('app_web_setup.password', '', true);",
  "END",
  "$$",
  sep = "\n"
)

# Each step is a label plus a function of the connection; the dry-run print
# and the execution walk the same list.
steps <- c(
  list(list(
    label = web_role_path,
    sql = web_role_sql,
    run = function(con) DBI::dbExecute(con, web_role_sql, immediate = TRUE)
  )),
  if (nzchar(app_web_password)) list(list(
    label = "app_web password (APP_WEB_PASSWORD)",
    sql = paste0(
      sub("$1", "'<redacted>'", password_bind_sql, fixed = TRUE),
      ";\n", password_set_sql, ";"
    ),
    run = function(con) {
      DBI::dbGetQuery(con, password_bind_sql, params = list(app_web_password))
      DBI::dbExecute(con, password_set_sql)
    }
  )),
  lapply(app_api_paths, function(path) {
    sql <- read_sql(path)
    list(
      label = path,
      sql = sql,
      run = function(con) DBI::dbExecute(con, sql, immediate = TRUE)
    )
  }),
  list(list(
    label = "security re-apply (scripts/apply_db_security.R)",
    sql = paste(
      "-- sql/security/enable_readonly_rls.sql, then",
      "-- sql/security/audit_app_access.sql (must return zero rows)",
      sep = "\n"
    ),
    run = function(con) run_db_security_hardening(con)
  ))
)

suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
})
source(security_script, local = TRUE)

message(
  if (confirm_apply) {
    "Mode: APPLY (transaction will commit)"
  } else {
    "Mode: DRY RUN (statements execute, then the transaction rolls back)"
  }
)
message("app_web connection limit: ", connection_limit)
if (!nzchar(app_web_password)) {
  message("APP_WEB_PASSWORD unset: the password step is skipped in this dry run.")
}

for (step in steps) {
  cat(sprintf("\n-- ==== %s ====\n%s\n", step$label, step$sql))
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
message("\nDatabase role: ", role)

dbBegin(con)
transaction_open <- TRUE
on.exit({
  if (transaction_open) {
    try(dbRollback(con), silent = TRUE)
  }
}, add = TRUE)

for (step in steps) {
  message("Running: ", step$label)
  step$run(con)
}

# What the transaction now holds, read back from the catalog.
state <- dbGetQuery(con, "
  SELECT
    r.rolname,
    r.rolcanlogin,
    r.rolconnlimit,
    pg_has_role(r.oid, 'app_readonly', 'USAGE') AS inherits_app_readonly,
    (SELECT array_to_string(s.setconfig, ', ')
       FROM pg_db_role_setting s
      WHERE s.setrole = r.oid AND s.setdatabase = 0) AS role_settings,
    has_schema_privilege('app_readonly', 'app_api', 'USAGE') AS app_api_usage,
    has_schema_privilege('app_readonly', 'app_api', 'CREATE') AS app_api_create
  FROM pg_roles r
  WHERE r.rolname = 'app_web'
")
print(state, row.names = FALSE)

if (confirm_apply) {
  dbCommit(con)
  transaction_open <- FALSE
  message("Web foundation committed.")
} else {
  dbRollback(con)
  transaction_open <- FALSE
  message("Web foundation validated and rolled back.")
}
