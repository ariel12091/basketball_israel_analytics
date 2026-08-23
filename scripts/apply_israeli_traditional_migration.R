# Applies the Israeli Tab 5 per-game fact migration on the direct DDL path.

suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
})

file_arg <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
script_path <- if (length(file_arg)) sub("^--file=", "", file_arg[[1]]) else "scripts/x"
repo_root <- normalizePath(file.path(dirname(script_path), ".."), winslash = "/", mustWork = TRUE)
readRenviron(file.path(repo_root, "etl", ".Renviron"))

read_sql <- function(path) paste(readLines(file.path(repo_root, path), warn = FALSE), collapse = "\n")
execute_simple_file <- function(con, path) {
  statements <- trimws(strsplit(read_sql(path), ";", fixed = TRUE)[[1]])
  statements <- statements[nzchar(statements)]
  invisible(lapply(statements, function(sql) dbExecute(con, sql)))
}
timed <- function(label, expr) {
  started <- proc.time()[["elapsed"]]
  value <- force(expr)
  cat(sprintf("%-30s %.2fs\n", label, proc.time()[["elapsed"]] - started))
  flush.console()
  value
}

con <- dbConnect(
  Postgres(), host = Sys.getenv("PG_HOST"), port = 5432L,
  dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE", "require"),
  bigint = "numeric", connect_timeout = 15L
)
on.exit(if (dbIsValid(con)) dbDisconnect(con), add = TRUE)

exists <- dbGetQuery(con, "SELECT to_regclass('basketball_test.player_traditional_by_game') IS NOT NULL AS present")$present[[1]]
if (isTRUE(exists) && !identical(Sys.getenv("ALLOW_TRADITIONAL_FACT_REBUILD"), "1")) {
  stop("player_traditional_by_game already exists; set ALLOW_TRADITIONAL_FACT_REBUILD=1 for an intentional rebuild")
}

dbBegin(con)
committed <- FALSE
on.exit(if (!committed) try(dbRollback(con), silent = TRUE), add = TRUE)
dbExecute(con, "SET LOCAL lock_timeout = '5s'")
dbExecute(con, "SET LOCAL statement_timeout = '180s'")
dbExecute(con, "SET LOCAL search_path TO basketball_test, public")

timed("install compute function", dbExecute(con, read_sql("sql/functions/compute_player_traditional_by_game.sql")))
timed("build per-game table", execute_simple_file(con, "sql/materialized_views/player_traditional_by_game.sql"))
timed("install refresh function", dbExecute(con, read_sql("sql/functions/refresh_player_traditional_by_game_for_games.sql")))

reader_sql <- read_sql("sql/functions/get_player_traditional_from_games.sql")
reader_create_at <- regexpr("CREATE OR REPLACE FUNCTION", reader_sql, fixed = TRUE)[[1]]
dbExecute(con, paste0(
  "DROP FUNCTION IF EXISTS basketball_test.get_player_traditional_from_games(",
  "INT, DATE, DATE, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, INT, TEXT, ",
  "INT, TEXT, INT, BOOLEAN, INT, INT, INT)"
))
timed("install reader function", dbExecute(con, substring(reader_sql, reader_create_at)))

dbExecute(con, "REVOKE ALL ON FUNCTION basketball_test.compute_player_traditional_by_game(int4[]) FROM PUBLIC")
dbExecute(con, "REVOKE ALL ON FUNCTION basketball_test.refresh_player_traditional_by_game_for_games(int4[]) FROM PUBLIC")
dbExecute(con, paste0(
  "REVOKE ALL ON FUNCTION basketball_test.get_player_traditional_from_games(",
  "int4,date,date,text,text,text,text,text,text,int4,text,int4,text,int4,bool,int4,int4,int4) FROM PUBLIC"
))
dbExecute(con, "GRANT SELECT ON basketball_test.player_traditional_by_game TO app_readonly")
dbExecute(con, paste0(
  "GRANT EXECUTE ON FUNCTION basketball_test.get_player_traditional_from_games(",
  "int4,date,date,text,text,text,text,text,text,int4,text,int4,text,int4,bool,int4,int4,int4) TO app_readonly"
))
dbExecute(con, "ALTER TABLE basketball_test.player_traditional_by_game ENABLE ROW LEVEL SECURITY")
dbExecute(con, "DROP POLICY IF EXISTS app_readonly_select_all ON basketball_test.player_traditional_by_game")
dbExecute(con, "CREATE POLICY app_readonly_select_all ON basketball_test.player_traditional_by_game FOR SELECT TO app_readonly USING (true)")

profile <- dbGetQuery(con, "
SELECT count(*) AS rows, count(DISTINCT game_id) AS games,
       pg_total_relation_size('basketball_test.player_traditional_by_game') AS bytes
FROM basketball_test.player_traditional_by_game")
stopifnot(profile$rows[[1]] > 0, profile$games[[1]] > 0)

dbCommit(con)
committed <- TRUE
cat(sprintf("committed rows=%s games=%s bytes=%s\n",
            profile$rows[[1]], profile$games[[1]], profile$bytes[[1]]))
