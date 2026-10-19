#!/usr/bin/env Rscript

# Read-only behavioral and planning check for Workstream C's ribbon warning.

suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
})

readRenviron("app/.Renviron")

exprs <- parse("app/R/global.R", keep.source = FALSE)
is_sql_assignment <- vapply(exprs, function(expr) {
  is.call(expr) && identical(as.character(expr[[1]]), "<-") &&
    identical(as.character(expr[[2]]), "RIBBON_SQL_ISRAEL")
}, logical(1))
stopifnot(sum(is_sql_assignment) == 1L)
sql <- eval(exprs[[which(is_sql_assignment)]][[3]])

pg <- dbConnect(
  Postgres(),
  host = Sys.getenv("PG_HOST"),
  port = as.integer(Sys.getenv("PG_PORT", "6543")),
  dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE", "require"),
  connect_timeout = 15L
)
on.exit(dbDisconnect(pg), add = TRUE)

cases <- data.frame(
  game_id = c(100L, 62452L, 406L, 62534L),
  team_id = c(3L, 11L, 14L, 4L),
  expected_segments = c(0L, 1L, 2L, 2L),
  expected_seconds = c(0, 72, 736, 6)
)

rows <- lapply(seq_len(nrow(cases)), function(i) {
  started <- proc.time()[["elapsed"]]
  row <- dbGetQuery(pg, sql, params = list(cases$game_id[[i]], cases$team_id[[i]]))
  data.frame(
    game_id = cases$game_id[[i]],
    false_straddle_segments = as.integer(row$false_straddle_segments[[1]]),
    false_straddle_seconds = as.numeric(row$false_straddle_seconds[[1]]),
    elapsed_ms = round((proc.time()[["elapsed"]] - started) * 1000, 1)
  )
})
observed <- do.call(rbind, rows)
print(observed, row.names = FALSE)

stopifnot(identical(observed$false_straddle_segments, cases$expected_segments))
stopifnot(isTRUE(all.equal(observed$false_straddle_seconds, cases$expected_seconds)))

plan <- dbGetQuery(
  pg,
  paste("EXPLAIN (ANALYZE, BUFFERS, SETTINGS)", sql),
  params = list(62452L, 11L)
)[[1]]
cat("\n", paste(tail(plan, 8L), collapse = "\n"), "\n", sep = "")
