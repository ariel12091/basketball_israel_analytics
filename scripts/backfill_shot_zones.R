# One-time backfill of basketball_test.shot_zones from cold parquet.
# Idempotent: upserts by (game_id, id). Run from repo root, etl credentials.
# Corner rule: 3PT shot AND parameters_coord_y <= 285 (raw units; = 2.85
# court units, the committed corner3_height in fiba_court_zones.R).
suppressMessages({library(DBI); library(RPostgres); library(arrow); library(dplyr)})

readRenviron("etl/.Renviron")
con <- dbConnect(RPostgres::Postgres(),
  host=Sys.getenv("PG_HOST"), port=as.integer(Sys.getenv("PG_PORT")),
  dbname=Sys.getenv("PG_DB"), user=Sys.getenv("PG_USER"),
  password=Sys.getenv("PG_PASS"), sslmode=Sys.getenv("PG_SSLMODE"),
  connect_timeout=15L, bigint="numeric")

z <- read_parquet("exports/cold/actions_clean.parquet",
  col_select = c("game_id", "id", "type", "parameters_points",
                 "parameters_coord_y")) |>
  distinct(game_id, id, .keep_all = TRUE) |>
  filter(type == "shot", parameters_points == 3,
         !is.na(parameters_coord_y)) |>
  transmute(game_id = as.integer(game_id), id = as.integer(id),
            is_corner3 = parameters_coord_y <= 285)

cat("3PT shots with coords in parquet:", nrow(z),
    sprintf(" corner share: %.1f%%\n", 100 * mean(z$is_corner3)))

dbExecute(con, "CREATE TEMP TABLE shot_zones_stage
                (game_id int, id int, is_corner3 bool)")
dbAppendTable(con, "shot_zones_stage", z)
n <- dbExecute(con, "
  INSERT INTO basketball_test.shot_zones (game_id, id, is_corner3)
  SELECT game_id, id, is_corner3 FROM shot_zones_stage
  ON CONFLICT (game_id, id) DO UPDATE SET is_corner3 = EXCLUDED.is_corner3")
cat("upserted rows:", n, "\n")

chk <- dbGetQuery(con, "
  SELECT count(*) AS rows, round(100.0 * avg(is_corner3::int), 1) AS corner_pct
  FROM basketball_test.shot_zones")
print(chk)
dbDisconnect(con)
