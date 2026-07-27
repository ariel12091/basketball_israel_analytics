# Captures output and timings for the Tab 2 starters fast-path cases.
# Usage: Rscript scripts/starters_baseline.R <outdir> <label>
args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 2) {
  stop("Usage: Rscript scripts/starters_baseline.R <outdir> <label>")
}

outdir <- args[[1]]
label <- args[[2]]
dir.create(outdir, recursive = TRUE, showWarnings = FALSE)

if (file.exists("etl/.Renviron")) {
  readRenviron("etl/.Renviron")
}
suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
})

con <- dbConnect(
  Postgres(),
  host = Sys.getenv("PG_HOST"),
  port = as.integer(Sys.getenv("PG_PORT", "6543")),
  dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE", "require"),
  bigint = "numeric",
  connect_timeout = 15L
)
on.exit(dbDisconnect(con), add = TRUE)
invisible(dbExecute(con, "SET statement_timeout = '120s'"))

cases <- list(
  st_sum_5v5 = "SELECT * FROM basketball_test.fetch_lineups_all(2::smallint, p_game_year := 2026, p_min_poss := 20, p_num_starters_off := 5, p_num_starters_def := 5)",
  st_sum_own5 = "SELECT * FROM basketball_test.fetch_lineups_all(2::smallint, p_game_year := 2026, p_min_poss := 20, p_num_starters_off := 5)",
  st_sum_range = "SELECT * FROM basketball_test.fetch_lineups_all(2::smallint, p_game_year := 2026, p_min_poss := 20, p_num_starters_off_min := 4, p_num_starters_def_min := 4)",
  st_sum_bench = "SELECT * FROM basketball_test.fetch_lineups_all(2::smallint, p_game_year := 2026, p_min_poss := 20, p_num_starters_off_max := 1)",
  st_ff_5v5 = "SELECT * FROM basketball_test.fetch_lineups_four_factors(2::smallint, p_game_year := 2026, p_min_poss := 20, p_num_starters_off := 5, p_num_starters_def := 5)",
  st_ff_own5 = "SELECT * FROM basketball_test.fetch_lineups_four_factors(2::smallint, p_game_year := 2026, p_min_poss := 20, p_num_starters_off := 5)",
  st_ff_range = "SELECT * FROM basketball_test.fetch_lineups_four_factors(2::smallint, p_game_year := 2026, p_min_poss := 20, p_num_starters_off_min := 4, p_num_starters_def_min := 4)",
  st_home_5v5 = "SELECT * FROM basketball_test.fetch_lineups_all(2::smallint, p_game_year := 2026, p_home_away := 'home', p_min_poss := 20, p_num_starters_off := 5, p_num_starters_def := 5)",
  st_clutch_5v5 = "SELECT * FROM basketball_test.fetch_lineups_all(2::smallint, p_game_year := 2026, p_min_poss := 5, p_max_margin := 5, p_num_starters_off := 5, p_num_starters_def := 5)"
)

timings <- data.frame(case = character(), median_s = numeric())
for (nm in names(cases)) {
  runs <- numeric(3)
  df <- NULL
  for (i in seq_len(3)) {
    t0 <- Sys.time()
    df <- dbGetQuery(con, cases[[nm]])
    runs[[i]] <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  }

  # Canonical row order so before/after CSVs compare cleanly.
  ord <- do.call(order, df)
  write.csv(
    df[ord, , drop = FALSE],
    file.path(outdir, sprintf("%s_%s.csv", label, nm)),
    row.names = FALSE
  )
  timings <- rbind(
    timings,
    data.frame(case = nm, median_s = round(median(runs), 3))
  )
  cat(sprintf("%-20s rows=%6d median=%.3fs\n", nm, nrow(df), median(runs)))
}

write.csv(
  timings,
  file.path(outdir, sprintf("%s_timings.csv", label)),
  row.names = FALSE
)
