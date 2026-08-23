suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
})
file_arg <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
script_path <- if (length(file_arg)) sub("^--file=", "", file_arg[[1]]) else "scripts/x"
repo_root <- normalizePath(file.path(dirname(script_path), ".."), winslash = "/", mustWork = TRUE)
readRenviron(file.path(repo_root, "etl", ".Renviron"))
con <- dbConnect(Postgres(), host = Sys.getenv("PG_HOST"), port = 5432L,
                 dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
                 password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE", "require"),
                 bigint = "numeric", connect_timeout = 15L)
on.exit(dbDisconnect(con), add = TRUE)
dbExecute(con, "SET statement_timeout = '10s'")
terminate_arg <- grep("^--terminate-pid=", commandArgs(trailingOnly = TRUE), value = TRUE)
if (length(terminate_arg)) {
  target_pid <- as.integer(sub("^--terminate-pid=", "", terminate_arg[[1]]))
  stopped <- dbGetQuery(con, "SELECT pg_terminate_backend($1::int) AS stopped", params = list(target_pid))
  cat(sprintf("terminated_pid=%d stopped=%s\n", target_pid, stopped$stopped[[1]]))
}
print(dbGetQuery(con, "
SELECT pid, usename, application_name, state, wait_event_type, wait_event,
       age(clock_timestamp(), xact_start) AS xact_age,
       age(clock_timestamp(), query_start) AS query_age,
       left(query, 180) AS query
FROM pg_stat_activity
WHERE datname = current_database() AND pid <> pg_backend_pid()
  AND (state = 'idle in transaction' OR query ILIKE '%player_traditional%')
ORDER BY xact_start NULLS LAST"), row.names = FALSE)
