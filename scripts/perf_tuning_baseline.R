# scripts/perf_tuning_baseline.R
# Captures function outputs + timings + diagnostics for the perf-tuning refactor
# (docs/superpowers/plans/2026-07-27-sql-function-perf-tuning.md).
# Usage: Rscript scripts/perf_tuning_baseline.R <outdir> <label>
#   label "baseline" also runs the D1/D3/D4 diagnostics.
args <- commandArgs(trailingOnly = TRUE)
if (length(args) < 2) stop("Usage: Rscript scripts/perf_tuning_baseline.R <outdir> <label>")
outdir <- args[[1]]; label <- args[[2]]
dir.create(outdir, recursive = TRUE, showWarnings = FALSE)

if (file.exists("etl/.Renviron")) readRenviron("etl/.Renviron")
suppressPackageStartupMessages({ library(DBI); library(RPostgres) })

con <- dbConnect(Postgres(),
  host = Sys.getenv("PG_HOST"), port = as.integer(Sys.getenv("PG_PORT", "6543")),
  dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE", "require"),
  bigint = "numeric", connect_timeout = 15L)
on.exit(dbDisconnect(con), add = TRUE)
invisible(dbExecute(con, "SET statement_timeout = '120s'"))

cases <- list(
  onoff_full = list(sql = "SELECT * FROM basketball_test.onoff_compute($1::date,$2::date,NULL,0,0,-999,'2026')",
                    params = list("2025-10-01", "2026-07-01")),
  onoff_lastn = list(sql = "SELECT * FROM basketball_test.onoff_compute($1::date,$2::date,NULL,0,0,-999,'2026',NULL,NULL,'all','all',NULL,NULL,NULL,NULL,NULL,5)",
                     params = list("2025-10-01", "2026-07-01")),
  ff_full  = list(sql = "SELECT * FROM basketball_test.four_factors_compute(2026)", params = list()),
  ff_lastn = list(sql = "SELECT * FROM basketball_test.four_factors_compute(2026,NULL,NULL,NULL,NULL,NULL,'all','all','all',NULL,'net',NULL,NULL,5)", params = list()),
  # p_num_starters_off_min := NULL pins the current 29-arg overload (stale 23/25-arg
  # overloads still live in the DB and make sparse named calls ambiguous)
  lineups_home = list(sql = "SELECT * FROM basketball_test.fetch_lineups_all(2::smallint, p_game_year := 2026, p_home_away := 'home', p_min_poss := 20, p_num_starters_off_min := NULL::integer)", params = list()),
  lineups_clutch = list(sql = "SELECT * FROM basketball_test.fetch_lineups_all(2::smallint, p_game_year := 2026, p_min_poss := 5, p_max_margin := 5, p_max_time_remaining := 300, p_num_starters_off_min := NULL::integer)", params = list()),
  lineups_ff_home = list(sql = "SELECT * FROM basketball_test.fetch_lineups_four_factors(2::smallint, p_game_year := 2026, p_home_away := 'home', p_min_poss := 20)", params = list()),
  lineups_ff_clutch = list(sql = "SELECT * FROM basketball_test.fetch_lineups_four_factors(2::smallint, p_game_year := 2026, p_min_poss := 5, p_max_margin := 5, p_max_time_remaining := 300)", params = list()),
  team_rt_filtered = list(sql = "SELECT * FROM basketball_test.get_team_ratings_dynamic(2026, p_home_away := 'home')", params = list()),
  team_rt_clutch = list(sql = "SELECT * FROM basketball_test.get_team_ratings_dynamic(2026, p_max_margin := 5, p_max_time_remaining := 300)", params = list()),
  team_rt_lastn = list(sql = "SELECT * FROM basketball_test.get_team_ratings_dynamic(2026, p_last_n_games := 5)", params = list()),
  team_ff_filtered = list(sql = "SELECT * FROM basketball_test.get_team_four_factors_dynamic(2026, p_home_away := 'home')", params = list()),
  team_ff_clutch = list(sql = "SELECT * FROM basketball_test.get_team_four_factors_dynamic(2026, p_max_margin := 5, p_max_time_remaining := 300)", params = list()),
  trad_full = list(sql = "SELECT * FROM basketball_test.get_player_traditional_dynamic(2026)", params = list()),
  trad_clutch = list(sql = "SELECT * FROM basketball_test.get_player_traditional_dynamic(2026, p_max_margin := 5, p_max_time_remaining := 300)", params = list())
)

timings <- data.frame(case = character(), median_s = numeric())
for (nm in names(cases)) {
  cs <- cases[[nm]]
  runs <- numeric(3)
  df <- NULL
  for (i in 1:3) {
    t0 <- Sys.time()
    df <- if (length(cs$params)) dbGetQuery(con, cs$sql, params = cs$params) else dbGetQuery(con, cs$sql)
    runs[i] <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  }
  # canonical row order so before/after CSVs diff cleanly regardless of tie order
  ord <- do.call(order, df)
  write.csv(df[ord, , drop = FALSE], file.path(outdir, sprintf("%s_%s.csv", label, nm)), row.names = FALSE)
  timings <- rbind(timings, data.frame(case = nm, median_s = round(median(runs), 3)))
  cat(sprintf("%-20s rows=%6d median=%.3fs\n", nm, nrow(df), median(runs)))
}
write.csv(timings, file.path(outdir, sprintf("%s_timings.csv", label)), row.names = FALSE)

if (label == "baseline") {
  d1 <- dbGetQuery(con, "
    SELECT COUNT(*) AS n_rows, COUNT(DISTINCT (game_id, team_id)) AS n_keys
    FROM basketball_test.final_schedule_mv")
  d3 <- dbGetQuery(con, "
    SELECT player_id, team_id, COUNT(*) AS n
    FROM basketball_test.four_factors_compute(2026)
    GROUP BY 1,2 HAVING COUNT(*) > 1")
  d4 <- dbGetQuery(con, "
    SELECT f.player_id, f.team_id
    FROM basketball_test.four_factors_compute(2026) f
    WHERE NOT EXISTS (
      SELECT 1 FROM basketball_test.full_rosters fr
      WHERE fr.player_id = f.player_id AND fr.team_id = f.team_id AND fr.game_year = 2026
    )")
  sink(file.path(outdir, "diagnostics_baseline.txt"))
  cat("D1 final_schedule_mv uniqueness (n_rows must equal n_keys):\n"); print(d1)
  cat("\nD3 four_factors_compute duplicate (player,team) rows (must be empty for Task 5 Step 2):\n"); print(d3)
  cat("\nD4 output players missing a same-season roster row (must be empty for Task 5 Step 2):\n"); print(d4)
  sink()
  cat("\nD1:\n"); print(d1); cat("D3:\n"); print(d3); cat("D4:\n"); print(d4)
}
