#!/usr/bin/env Rscript
# scripts/restore_cold_storage.R — Restore cold-stored tables from Parquet
#
# Usage:
#   Sys.setenv(APP_ENV = "test")
#   source("scripts/restore_cold_storage.R")
#
# Or from command line:
#   Rscript scripts/restore_cold_storage.R
#
# Prerequisites:
#   - Parquet files in exports/cold/ (download first if needed):
#     gh release download cold-storage/latest -D exports/cold/
#   - ETL .Renviron for DB credentials

env_file <- "etl/.Renviron"
if (file.exists(env_file)) readRenviron(env_file)

library(DBI)
library(RPostgres)
library(arrow)

schema <- if (identical(Sys.getenv("APP_ENV"), "prod")) "basketball" else "basketball_test"

pg <- DBI::dbConnect(
  RPostgres::Postgres(),
  host     = Sys.getenv("PG_HOST"),
  port     = as.integer(Sys.getenv("PG_PORT", "6543")),
  dbname   = Sys.getenv("PG_DB", "postgres"),
  user     = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"),
  sslmode  = Sys.getenv("PG_SSLMODE", "require")
)
on.exit(DBI::dbDisconnect(pg), add = TRUE)

source("etl/cold_storage.R")

cold_dir <- "exports/cold"
message(sprintf("Restoring from %s into schema %s ...", cold_dir, schema))

for (tbl in COLD_TABLES) {
  tryCatch({
    n <- restore_cold_table(pg, schema, tbl, cold_dir)
    message(sprintf("  %s: restored %s rows", tbl, format(n, big.mark = ",")))
  }, error = function(e) {
    message(sprintf("  %s: FAILED — %s", tbl, conditionMessage(e)))
  })
}

message("Restore complete. Run rebuild_all_mvs() next if needed.")
