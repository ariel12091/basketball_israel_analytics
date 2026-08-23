# App-role verification for the live Israeli standard-clutch cache.

suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
})

file_arg <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
script_path <- if (length(file_arg)) sub("^--file=", "", file_arg[[1]]) else "scripts/x"
repo_root <- normalizePath(file.path(dirname(script_path), ".."), winslash = "/", mustWork = TRUE)
readRenviron(file.path(repo_root, "app", ".Renviron"))

con <- dbConnect(
  Postgres(), host = Sys.getenv("PG_HOST"), port = as.integer(Sys.getenv("PG_PORT", "6543")),
  dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE", "require"),
  bigint = "numeric", connect_timeout = 15L
)
on.exit(if (dbIsValid(con)) dbDisconnect(con), add = TRUE)
dbExecute(con, "SET statement_timeout = '20s'")

profile <- dbGetQuery(con, "
SELECT count(*) AS rows, count(DISTINCT game_id) AS games,
       pg_total_relation_size('basketball_test.default_clutch_player_totals_by_game') AS bytes
FROM basketball_test.default_clutch_player_totals_by_game")
print(profile, row.names = FALSE)

sql <- paste0(
  "SELECT * FROM basketball_test.get_player_traditional_from_games(",
  "p_game_year=>2026,p_max_margin=>5,p_margin_status=>'all',",
  "p_max_time_remaining=>300,p_ot_margin_filter=>false)"
)
for (run in 1:2) {
  started <- proc.time()[["elapsed"]]
  result <- dbGetQuery(con, sql)
  elapsed <- proc.time()[["elapsed"]] - started
  cat(sprintf("standard_clutch run=%d seconds=%.2f rows=%d\n", run, elapsed, nrow(result)))
}

expect_denied <- function(label, sql) {
  denied <- tryCatch({
    dbGetQuery(con, sql)
    FALSE
  }, error = function(e) grepl("permission denied", conditionMessage(e), fixed = TRUE))
  cat(sprintf("%s_denied=%s\n", label, denied))
  stopifnot(denied)
}
expect_denied(
  "compute",
  "SELECT count(*) FROM basketball_test.compute_player_traditional_by_game(ARRAY[64942]::int4[], TRUE)"
)
expect_denied(
  "refresh",
  "SELECT basketball_test.refresh_default_clutch_player_totals_for_games(ARRAY[64942]::int4[])"
)
