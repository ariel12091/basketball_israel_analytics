# Read-only benchmark for Israeli Player Stats (Tab 5) query paths.
#
# Uses the app_readonly credentials and pooler, isolates every measured call on
# a fresh connection, and reports timeouts separately from completed timings.

suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
})

args <- commandArgs(trailingOnly = TRUE)
runs_arg <- grep("^--runs=", args, value = TRUE)
runs <- if (length(runs_arg)) as.integer(sub("^--runs=", "", runs_arg[[1]])) else 2L
diagnostic_only <- "--diagnostic-only" %in% args
diagnostic <- diagnostic_only || "--diagnostic" %in% args
if (!is.finite(runs) || runs < 1L) stop("--runs must be positive")

file_arg <- grep("^--file=", commandArgs(trailingOnly = FALSE), value = TRUE)
script_path <- if (length(file_arg)) sub("^--file=", "", file_arg[[1]]) else "scripts/x"
repo_root <- normalizePath(file.path(dirname(script_path), ".."),
                           winslash = "/", mustWork = TRUE)
readRenviron(file.path(repo_root, "app", ".Renviron"))

open_connection <- function(timeout_ms) {
  con <- dbConnect(
    Postgres(), host = Sys.getenv("PG_HOST"), port = 6543L,
    dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
    password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE", "require"),
    bigint = "numeric", connect_timeout = 15L
  )
  dbExecute(con, "SET default_transaction_read_only = on")
  dbExecute(con, sprintf("SET statement_timeout = %d", as.integer(timeout_ms)))
  con
}

probe_con <- open_connection(20000L)
on.exit(if (dbIsValid(probe_con)) dbDisconnect(probe_con), add = TRUE)
target <- dbGetQuery(probe_con, "SELECT current_database() database, current_user username")
bounds <- dbGetQuery(
  probe_con,
  "SELECT min(game_date) mn, max(game_date) mx
     FROM basketball_test.final_schedule_mv WHERE game_year=$1::int4",
  params = list(2026L)
)
dbDisconnect(probe_con)
cat(sprintf("target database=%s user=%s client_path=pooler:6543 season=2026\n",
            target$database[[1]], target$username[[1]]))

season_start <- as.Date(bounds$mn[[1]])
season_end <- as.Date(bounds$mx[[1]])
season_mid <- season_start + as.integer((season_end - season_start) * 0.5)

dynamic_sql <- paste0(
  "SELECT * FROM basketball_test.get_player_traditional_dynamic(",
  "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,$7::text,$8::text,",
  "$9::text,$10::int4,$11::text,$12::int4,$13::text,$14::int4,$15::bool,",
  "$16::int4,$17::int4,$18::int4)"
)

params <- function(start, max_margin = NA_integer_, margin_status = "all",
                   max_time = NA_integer_, ot_margin = FALSE) {
  list(
    2026L, as.Date(start), season_end,
    NA_character_, NA_character_, NA_character_, "all", "all", "all",
    NA_integer_, "net", max_margin, margin_status, max_time, ot_margin,
    NA_integer_, NA_integer_, NA_integer_
  )
}

run_case <- function(name, sql, query_params, timeout_ms, repetitions = runs) {
  elapsed <- numeric()
  timeouts <- 0L
  rows <- NA_integer_
  for (k in seq_len(repetitions)) {
    con <- open_connection(timeout_ms)
    started <- proc.time()[["elapsed"]]
    result <- tryCatch(
      dbGetQuery(con, sql, params = query_params),
      error = function(e) structure(list(message = conditionMessage(e)), class = "bench_error")
    )
    seconds <- proc.time()[["elapsed"]] - started
    dbDisconnect(con)
    if (inherits(result, "bench_error")) {
      if (!grepl("statement timeout|canceling statement", result$message, ignore.case = TRUE)) {
        stop(sprintf("%s failed: %s", name, result$message))
      }
      timeouts <- timeouts + 1L
    } else {
      elapsed <- c(elapsed, seconds)
      rows <- nrow(result)
    }
  }
  summary <- if (length(elapsed)) {
    sprintf("min=%.2fs median=%.2fs max=%.2fs", min(elapsed), median(elapsed), max(elapsed))
  } else {
    "no completed calls"
  }
  cat(sprintf("%-30s %-39s rows=%s timeouts=%d/%d limit=%ds\n",
              name, summary, ifelse(is.na(rows), "-", rows), timeouts,
              repetitions, timeout_ms / 1000L))
  flush.console()
}

if (!diagnostic_only) {
  run_case(
    "season_mv",
    "SELECT * FROM basketball_test.player_traditional_stats_mv WHERE game_year=$1::int4",
    list(2026L), 20000L
  )
  run_case("nonclutch_second_half", dynamic_sql, params(season_mid), 20000L)
  run_case("standard_5_all_5m", dynamic_sql,
           params(season_start, 5L, "all", 300L, FALSE), 20000L)
  run_case("custom_3_all_4m", dynamic_sql,
           params(season_start, 3L, "all", 240L, FALSE), 20000L)
}

if (diagnostic) {
  run_case("nonclutch_second_half_diag", dynamic_sql, params(season_mid),
           120000L, repetitions = 1L)
  run_case("standard_5_all_5m_diag", dynamic_sql,
           params(season_start, 5L, "all", 300L, FALSE),
           120000L, repetitions = 1L)
}
