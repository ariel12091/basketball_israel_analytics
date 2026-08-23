# Transactional smoke test for the Israeli Tab 5 per-game migration.
# Every DDL/data change is rolled back before disconnect.

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

dbBegin(con)
on.exit(try(dbRollback(con), silent = TRUE), add = TRUE)
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

profile <- dbGetQuery(con, "
SELECT count(*) AS rows, count(DISTINCT game_id) AS games,
       pg_total_relation_size('basketball_test.player_traditional_by_game') AS bytes
FROM basketball_test.player_traditional_by_game")
print(profile, row.names = FALSE)

bounds <- dbGetQuery(con, "
SELECT min(game_date) AS mn, max(game_date) AS mx
FROM basketball_test.final_schedule_mv WHERE game_year = 2026")
season_mid <- as.Date(bounds$mn[[1]]) + as.integer((as.Date(bounds$mx[[1]]) - as.Date(bounds$mn[[1]])) * 0.5)

full <- timed("reader full season", dbGetQuery(con,
  "SELECT * FROM basketball_test.get_player_traditional_from_games($1)",
  params = list(2026L)))
half <- timed("reader second half", dbGetQuery(con,
  "SELECT * FROM basketball_test.get_player_traditional_from_games($1,$2,$3)",
  params = list(2026L, season_mid, as.Date(bounds$mx[[1]]))))
cat(sprintf("reader_rows full=%d second_half=%d\n", nrow(full), nrow(half)))

mv <- dbGetQuery(con, "
SELECT player_id, team_id, gp, poss_on_floor, minutes, pts, reb, oreb, dreb,
       ast, stl, blk, dfl, tov, fgm, fga, \"3pm\", \"3pa\", ftm, fta,
       fg_pct, tp_pct, ft_pct, efg, ts, usg_pct
FROM basketball_test.player_traditional_stats_mv WHERE game_year = 2026")
cols <- intersect(names(full), names(mv))
full_cmp <- full[order(full$team_id, full$player_id), cols, drop = FALSE]
mv_cmp <- mv[order(mv$team_id, mv$player_id), cols, drop = FALSE]
full_keys <- paste(full_cmp$team_id, full_cmp$player_id, sep = ":")
mv_keys <- paste(mv_cmp$team_id, mv_cmp$player_id, sep = ":")
same_keys <- identical(full_keys, mv_keys)
different_cells <- if (same_keys) sum(mapply(function(x, y) {
  !(is.na(x) & is.na(y)) & (is.na(x) | is.na(y) | x != y)
}, full_cmp, mv_cmp)) else NA_integer_
cat(sprintf("mv_parity same_keys=%s differing_cells=%s\n", same_keys, different_cells))

sample_game <- dbGetQuery(con, "SELECT max(game_id)::int AS game_id FROM basketball_test.player_traditional_by_game")$game_id[[1]]
before_rows <- dbGetQuery(con, "SELECT count(*) AS n FROM basketball_test.player_traditional_by_game")$n[[1]]
touched <- timed("single-game refresh", dbGetQuery(con,
  "SELECT basketball_test.refresh_player_traditional_by_game_for_games(ARRAY[$1]::int4[]) AS n",
  params = list(as.integer(sample_game))))$n[[1]]
after_rows <- dbGetQuery(con, "SELECT count(*) AS n FROM basketball_test.player_traditional_by_game")$n[[1]]
cat(sprintf("incremental game_id=%d touched=%s total_before=%s total_after=%s\n",
            sample_game, touched, before_rows, after_rows))

dbRollback(con)
cat("rollback=complete persistent_changes=false\n")
