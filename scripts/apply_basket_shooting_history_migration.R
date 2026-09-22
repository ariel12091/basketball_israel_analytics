# Apply the Basket shooting-history DDL. By default it validates the migration
# inside a transaction and rolls it back; pass --commit to persist it.
suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
})

args <- commandArgs(trailingOnly = TRUE)
commit <- "--commit" %in% args
file_arg <- args[startsWith(args, "--file=")]
migration <- if (length(file_arg)) {
  substring(file_arg[1], nchar("--file=") + 1L)
} else "sql/migrations/2026-09-22_basket_shooting_history.sql"

if (!file.exists(migration)) stop("Run from the repository root; missing: ", migration)
readRenviron("etl/.Renviron")

ddl_port <- Sys.getenv("PG_DDL_PORT", unset = "5432")
con <- dbConnect(
  RPostgres::Postgres(),
  host = Sys.getenv("PG_HOST"),
  port = as.integer(ddl_port),
  dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE"),
  connect_timeout = 15L,
  bigint = "numeric"
)
on.exit(dbDisconnect(con), add = TRUE)

sql <- paste(readLines(migration, warn = FALSE, encoding = "UTF-8"), collapse = "\n")
dbBegin(con)
ok <- FALSE
on.exit(if (!ok) try(dbRollback(con), silent = TRUE), add = TRUE)
dbExecute(con, "SET LOCAL lock_timeout = '10s'")
dbExecute(con, "SET LOCAL statement_timeout = '120s'")
dbExecute(con, sql, immediate = TRUE)

if (commit) {
  dbCommit(con)
  ok <- TRUE
  cat("Applied", migration, "\n")
} else {
  dbRollback(con)
  ok <- TRUE
  cat("Validated and rolled back", migration, "(pass --commit to apply)\n")
}
