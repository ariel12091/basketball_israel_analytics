# Validates and optionally applies the Israeli Tab 5 standard-clutch cache.
# Default behavior is a full transactional smoke test followed by rollback.

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
execute_function_file <- function(con, path) {
  sql <- read_sql(path)
  starts <- gregexpr("CREATE OR REPLACE FUNCTION", sql, fixed = TRUE)[[1]]
  starts <- starts[starts > 0]
  if (length(starts) <= 1L) return(invisible(dbExecute(con, sql)))
  ends <- c(starts[-1L] - 1L, nchar(sql))
  invisible(mapply(
    function(from, to) dbExecute(con, trimws(substr(sql, from, to))),
    starts, ends,
    SIMPLIFY = FALSE
  ))
}
timed <- function(label, expr) {
  started <- proc.time()[["elapsed"]]
  value <- force(expr)
  cat(sprintf("%-38s %.2fs\n", label, proc.time()[["elapsed"]] - started))
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

confirm_apply <- identical(Sys.getenv("CONFIRM_ISRAELI_STANDARD_CLUTCH_APPLY", "0"), "1")
exists <- dbGetQuery(
  con,
  "SELECT to_regclass('basketball_test.default_clutch_player_totals_by_game') IS NOT NULL AS present"
)$present[[1]]
if (isTRUE(exists) && !identical(Sys.getenv("ALLOW_STANDARD_CLUTCH_REBUILD", "0"), "1")) {
  stop("default_clutch_player_totals_by_game already exists; refusing an accidental rebuild")
}

dbBegin(con)
finished <- FALSE
on.exit(if (!finished) try(dbRollback(con), silent = TRUE), add = TRUE)
dbExecute(con, "SET LOCAL lock_timeout = '5s'")
dbExecute(con, "SET LOCAL statement_timeout = '180s'")
dbExecute(con, "SET LOCAL search_path TO basketball_test, public")

timed(
  "install standard-aware compute",
  execute_function_file(con, "sql/functions/compute_player_traditional_by_game.sql")
)
timed(
  "build standard-clutch fact",
  execute_simple_file(con, "sql/materialized_views/default_clutch_player_totals_by_game.sql")
)
timed(
  "install incremental refresh",
  dbExecute(con, read_sql("sql/functions/refresh_default_clutch_player_totals_for_games.sql"))
)

reader_sql <- read_sql("sql/functions/get_player_traditional_from_games.sql")
reader_create_at <- regexpr("CREATE OR REPLACE FUNCTION", reader_sql, fixed = TRUE)[[1]]
dbExecute(con, paste0(
  "DROP FUNCTION IF EXISTS basketball_test.get_player_traditional_from_games(",
  "INT, DATE, DATE, TEXT, TEXT, TEXT, TEXT, TEXT, TEXT, INT, TEXT, ",
  "INT, TEXT, INT, BOOLEAN, INT, INT, INT)"
))
timed("install cached reader", dbExecute(con, substring(reader_sql, reader_create_at)))

dbExecute(con, "REVOKE ALL ON FUNCTION basketball_test.compute_player_traditional_by_game(int4[],boolean) FROM PUBLIC")
dbExecute(con, "REVOKE ALL ON FUNCTION basketball_test.compute_player_traditional_by_game(int4[]) FROM PUBLIC")
dbExecute(con, "REVOKE ALL ON FUNCTION basketball_test.refresh_default_clutch_player_totals_for_games(int4[]) FROM PUBLIC")
reader_sig <- paste0(
  "basketball_test.get_player_traditional_from_games(",
  "int4,date,date,text,text,text,text,text,text,int4,text,int4,text,int4,bool,int4,int4,int4)"
)
dbExecute(con, sprintf("REVOKE ALL ON FUNCTION %s FROM PUBLIC", reader_sig))
dbExecute(con, "GRANT SELECT ON basketball_test.default_clutch_player_totals_by_game TO app_readonly")
dbExecute(con, sprintf("GRANT EXECUTE ON FUNCTION %s TO app_readonly", reader_sig))
dbExecute(con, "ALTER TABLE basketball_test.default_clutch_player_totals_by_game ENABLE ROW LEVEL SECURITY")
dbExecute(con, "DROP POLICY IF EXISTS app_readonly_select_all ON basketball_test.default_clutch_player_totals_by_game")
dbExecute(con, paste(
  "CREATE POLICY app_readonly_select_all",
  "ON basketball_test.default_clutch_player_totals_by_game",
  "FOR SELECT TO app_readonly USING (true)"
))

profile <- dbGetQuery(con, "
SELECT count(*) AS rows, count(DISTINCT game_id) AS games,
       pg_total_relation_size('basketball_test.default_clutch_player_totals_by_game') AS bytes
FROM basketball_test.default_clutch_player_totals_by_game")
stopifnot(profile$rows[[1]] > 0, profile$games[[1]] > 0)
print(profile, row.names = FALSE)

cached_sql <- paste0(
  "SELECT * FROM basketball_test.get_player_traditional_from_games(",
  "p_game_year=>$1::int4,p_team_ids_csv=>$2::text,p_max_margin=>5,",
  "p_margin_status=>'all',p_max_time_remaining=>300,p_ot_margin_filter=>false,",
  "p_last_n_games=>$3::int4)"
)
dynamic_sql <- paste0(
  "SELECT * FROM basketball_test.get_player_traditional_dynamic(",
  "p_game_year=>$1::int4,p_team_ids_csv=>$2::text,p_max_margin=>5,",
  "p_margin_status=>'all',p_max_time_remaining=>300,p_ot_margin_filter=>false,",
  "p_last_n_games=>$3::int4)"
)
compare_scope <- function(label, team_csv = NA_character_, last_n = NA_integer_) {
  params <- list(2026L, team_csv, last_n)
  cached <- timed(paste(label, "cached"), dbGetQuery(con, cached_sql, params = params))
  dynamic <- timed(paste(label, "dynamic"), dbGetQuery(con, dynamic_sql, params = params))
  key <- function(x) x[order(x$team_id, x$player_id), , drop = FALSE]
  cached <- key(cached)
  dynamic <- key(dynamic)
  rownames(cached) <- NULL
  rownames(dynamic) <- NULL
  same <- isTRUE(all.equal(cached, dynamic, check.attributes = FALSE, tolerance = 0))
  cat(sprintf("%-38s rows=%d exact=%s\n", paste(label, "parity"), nrow(cached), same))
  if (!same) {
    print(all.equal(cached, dynamic, check.attributes = FALSE, tolerance = 0))
    stop("standard-clutch cache parity failed for ", label)
  }
}

team_id <- dbGetQuery(con, "
SELECT team_id::int FROM basketball_test.final_schedule_mv
WHERE game_year = 2026 GROUP BY team_id ORDER BY count(*) DESC, team_id LIMIT 1")$team_id[[1]]
compare_scope("one-team season", as.character(team_id), NA_integer_)
compare_scope("league last-10", NA_character_, 10L)
if (identical(Sys.getenv("RUN_FULL_STANDARD_CLUTCH_PARITY", "0"), "1")) {
  compare_scope("full season", NA_character_, NA_integer_)
}

sample_game <- dbGetQuery(
  con,
  "SELECT max(game_id)::int AS game_id FROM basketball_test.default_clutch_player_totals_by_game"
)$game_id[[1]]
touched <- timed(
  "single-game incremental refresh",
  dbGetQuery(
    con,
    "SELECT basketball_test.refresh_default_clutch_player_totals_for_games(ARRAY[$1]::int4[]) AS n",
    params = list(as.integer(sample_game))
  )
)$n[[1]]
cat(sprintf("incremental game_id=%d touched=%s\n", sample_game, touched))

if (confirm_apply) {
  dbCommit(con)
  cat("migration=committed\n")
} else {
  dbRollback(con)
  cat("migration=rolled_back persistent_changes=false\n")
}
finished <- TRUE
