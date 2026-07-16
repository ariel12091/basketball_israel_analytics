# Which processed games are missing from cold parquet? (read-only diagnostic)
# Run from repo root. Uses app (readonly) credentials.
suppressMessages({library(DBI); library(RPostgres); library(arrow)})
source("etl/cold_reconcile.R")

readRenviron("app/.Renviron")
con <- dbConnect(RPostgres::Postgres(),
  host=Sys.getenv("PG_HOST"), port=as.integer(Sys.getenv("PG_PORT")),
  dbname=Sys.getenv("PG_DB"), user=Sys.getenv("PG_USER"),
  password=Sys.getenv("PG_PASS"), sslmode=Sys.getenv("PG_SSLMODE"),
  connect_timeout=15L, bigint="numeric")
processed <- dbGetQuery(con, "
  SELECT eg.game_id, eg.game_year, eg.processed_at, s.gn
  FROM basketball_test.etl_processed_games eg
  LEFT JOIN basketball_test.schedule s ON s.game_id = eg.game_id
  ORDER BY eg.game_id")
dbDisconnect(con)

for (tbl in c("actions_clean", "possessions", "pws", "stints", "subs")) {
  pq <- file.path("exports/cold", paste0(tbl, ".parquet"))
  pq_ids <- if (file.exists(pq)) {
    unique(read_parquet(pq, col_select = "game_id")$game_id)
  } else integer(0)
  gaps <- cold_coverage_gaps(processed$game_id, pq_ids)
  cat(sprintf("%-14s parquet games: %4d  processed: %4d  MISSING: %d\n",
              tbl, length(pq_ids), nrow(processed), length(gaps)))
  if (length(gaps)) print(processed[processed$game_id %in% gaps, ])
}
