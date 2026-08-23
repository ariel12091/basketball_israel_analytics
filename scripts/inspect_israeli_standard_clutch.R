suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
})

file_arg <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
script_path <- if (length(file_arg)) sub("^--file=", "", file_arg[[1]]) else "scripts/x"
repo_root <- normalizePath(file.path(dirname(script_path), ".."), winslash = "/", mustWork = TRUE)
readRenviron(file.path(repo_root, "app", ".Renviron"))

con <- dbConnect(Postgres(), host = Sys.getenv("PG_HOST"), port = 6543L,
                 dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
                 password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE", "require"),
                 bigint = "numeric", connect_timeout = 15L)
on.exit(dbDisconnect(con), add = TRUE)
dbExecute(con, "SET statement_timeout = '20s'")

print(dbGetQuery(con, "
SELECT n.nspname AS schema_name, c.relname, c.relkind,
       pg_total_relation_size(c.oid) AS bytes,
       obj_description(c.oid, 'pg_class') AS comment
FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
WHERE c.relname = 'default_clutch_player_totals_by_game'
ORDER BY n.nspname"), row.names = FALSE)

print(dbGetQuery(con, "
SELECT routine_schema, routine_name, routine_type
FROM information_schema.routines
WHERE routine_name ILIKE '%player%traditional%clutch%'
ORDER BY routine_schema, routine_name"), row.names = FALSE)
