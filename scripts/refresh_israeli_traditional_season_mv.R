suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
})

file_arg <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
script_path <- if (length(file_arg)) sub("^--file=", "", file_arg[[1]]) else "scripts/x"
repo_root <- normalizePath(file.path(dirname(script_path), ".."), winslash = "/", mustWork = TRUE)
readRenviron(file.path(repo_root, "etl", ".Renviron"))

con <- dbConnect(
  Postgres(), host = Sys.getenv("PG_HOST"), port = 5432L,
  dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE", "require"),
  bigint = "numeric", connect_timeout = 15L
)
on.exit(dbDisconnect(con), add = TRUE)
dbExecute(con, "SET statement_timeout = '180s'")

started <- proc.time()[["elapsed"]]
dbExecute(con, "REFRESH MATERIALIZED VIEW CONCURRENTLY basketball_test.player_traditional_stats_mv")
rows <- dbGetQuery(con, "SELECT count(*) AS n FROM basketball_test.player_traditional_stats_mv")$n[[1]]
cat(sprintf("refreshed rows=%s seconds=%.2f\n", rows, proc.time()[["elapsed"]] - started))
