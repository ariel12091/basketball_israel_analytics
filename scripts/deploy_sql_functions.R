# scripts/deploy_sql_functions.R
# Deploys sql/functions/*.sql files passed as args (whole-file, multi-statement)
# over the DDL port in ONE transaction. RPostgres dbExecute(immediate = TRUE)
# uses the simple protocol, so DROP + CREATE in one file body is fine.
# NOTE: files that DROP FUNCTION wipe app_readonly EXECUTE grants — always
# re-run scripts/apply_db_security.R (CONFIRM_DB_SECURITY_APPLY=1) after this.
args <- commandArgs(trailingOnly = TRUE)
if (!length(args)) stop("Usage: Rscript scripts/deploy_sql_functions.R <file.sql> [...]")
stopifnot(all(file.exists(args)))

if (file.exists("etl/.Renviron")) readRenviron("etl/.Renviron")
suppressPackageStartupMessages({ library(DBI); library(RPostgres) })

con <- dbConnect(Postgres(),
  host = Sys.getenv("PG_HOST"), port = as.integer(Sys.getenv("PG_DDL_PORT", "5432")),
  dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE", "require"),
  connect_timeout = 15L)
on.exit(dbDisconnect(con), add = TRUE)

dbBegin(con)
ok <- TRUE
for (f in args) {
  sql <- paste(readLines(f, warn = FALSE), collapse = "\n")
  res <- tryCatch({ dbExecute(con, sql, immediate = TRUE); TRUE },
                  error = function(e) { message(sprintf("FAILED %s: %s", f, conditionMessage(e))); FALSE })
  if (!res) { ok <- FALSE; break }
  message("deployed: ", f)
}
if (ok) { dbCommit(con); message("All functions deployed (committed).") } else {
  dbRollback(con); stop("Deploy rolled back; nothing changed.")
}
