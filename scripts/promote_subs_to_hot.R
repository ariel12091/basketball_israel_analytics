# One-time migration: promote basketball_test.subs out of cold storage.
#
# Why: the Shiny stint ribbon needs substitution evidence at REQUEST time, and a
# cold table is empty between ETL runs. subs costs ~9 MB against a 3.2 GB
# database, and its GRANT, RLS policy and app_readonly SELECT already exist.
#
# The code half of this change is etl/cold_storage.R (subs dropped from
# COLD_TABLES, subs_actions_clean_fk added to HOT_FKS_INTO_COLD). This script is
# the database half and runs ONCE.
#
# Order matters. subs_actions_clean_fk is currently VALID and actions_clean is
# empty between runs, so restoring rows BEFORE relaxing the constraint would be
# rejected row by row.
#
#   1. DROP subs_actions_clean_fk
#   2. Restore subs from its historical parquet
#   3. Re-ADD the constraint NOT VALID (same columns, same ON DELETE CASCADE)
#
# Dry run by default. To apply:
#   CONFIRM_SUBS_PROMOTION=1 Rscript scripts/promote_subs_to_hot.R
#
# Needs WRITE credentials (etl/.Renviron), not the app's readonly role, and the
# DDL port (5432) rather than the pooler.

confirm_apply <- identical(Sys.getenv("CONFIRM_SUBS_PROMOTION", "0"), "1")

readRenviron("etl/.Renviron")
suppressMessages({
  library(DBI); library(RPostgres); library(arrow)
})
source("etl/cold_storage.R")

schema <- Sys.getenv("PG_SCHEMA", "basketball_test")
cold_dir <- Sys.getenv("COLD_DIR", "exports/cold")
ddl_port <- suppressWarnings(as.integer(Sys.getenv("PG_DDL_PORT", "5432")))

stopifnot(!"subs" %in% COLD_TABLES, "subs" %in% names(HOT_FKS_INTO_COLD))

pg <- dbConnect(
  Postgres(),
  host = Sys.getenv("PG_HOST"), port = ddl_port,
  dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE", "require"),
  bigint = "numeric", connect_timeout = 15L
)
on.exit(try(dbDisconnect(pg), silent = TRUE), add = TRUE)

say <- function(...) cat(sprintf(...), "\n")

hot_now <- dbGetQuery(pg, sprintf('SELECT COUNT(*) n FROM "%s"."subs"', schema))$n[[1]]
fk_now <- dbGetQuery(pg, sprintf(
  "SELECT convalidated FROM pg_constraint
    WHERE conname = 'subs_actions_clean_fk'
      AND conrelid = '\"%s\".\"subs\"'::regclass", schema))

parquet_path <- file.path(cold_dir, "subs.parquet")
if (!file.exists(parquet_path)) stop("No parquet at ", parquet_path, call. = FALSE)
pq <- read_parquet(parquet_path, col_select = "game_id", mmap = FALSE)
pq_games <- length(unique(pq$game_id))

say("schema             : %s", schema)
say("subs rows now      : %d", hot_now)
say("subs FK validated  : %s", if (nrow(fk_now)) fk_now$convalidated[[1]] else "ABSENT")
say("parquet            : %s (%d rows, %d games)", parquet_path, nrow(pq), pq_games)

db_games <- dbGetQuery(pg, sprintf(
  'SELECT COUNT(DISTINCT game_id) n FROM "%s"."etl_processed_games"', schema))$n[[1]]
say("games ETL-processed: %d", db_games)
if (pq_games < db_games) {
  say("NOTE: parquet is %d game(s) short; those games get subs on their next ETL run.",
      db_games - pq_games)
}
# subs stopped being truncated when it was promoted, so Phase 2 has been
# writing rows for every game processed since. restore_cold_table() appends
# unconditionally and subs has PRIMARY KEY (game_id, id), so a blanket restore
# now violates it: on 2026-09-20 the table held 418 rows for games 401/404/405,
# 240 of them already in the parquet. Load only the keys the table lacks.
# `pq` above is a game_id-only projection, so the full frame is re-read here.
pq_full <- read_parquet(parquet_path, mmap = FALSE)
existing_keys <- dbGetQuery(pg, sprintf('SELECT game_id, id FROM "%s"."subs"', schema))
have_key <- paste(existing_keys$game_id, existing_keys$id, sep = "|")
to_add <- pq_full[!(paste(pq_full$game_id, pq_full$id, sep = "|") %in% have_key), , drop = FALSE]
say("rows to insert     : %d of %d (%d already present)",
    nrow(to_add), nrow(pq_full), nrow(pq_full) - nrow(to_add))

drop_sql <- cold_fk_drop_sql(schema)[["subs"]]
readd_sql <- cold_fk_readd_sql(schema)[["subs"]]
say("\n-- step 1 --\n%s", drop_sql)
say("\n-- step 2 --\ninsert %d row(s) from %s", nrow(to_add), parquet_path)
say("\n-- step 3 --\n%s", readd_sql)

if (!confirm_apply) {
  say("\nDRY RUN. Set CONFIRM_SUBS_PROMOTION=1 to apply.")
  quit(save = "no", status = 0)
}
if (!nrow(to_add)) {
  say("nothing to insert; subs already holds every archived row.")
  quit(save = "no", status = 0)
}

dbExecute(pg, "BEGIN;")
ok <- FALSE
on.exit({
  if (!ok) try(dbExecute(pg, "ROLLBACK;"), silent = TRUE)
  try(dbDisconnect(pg), silent = TRUE)
}, add = FALSE)

dbExecute(pg, drop_sql);                                   say("dropped subs_actions_clean_fk")
DBI::dbWriteTable(pg, DBI::Id(schema = schema, table = "subs"),
                  as.data.frame(to_add), append = TRUE, row.names = FALSE)
say("inserted %d rows", nrow(to_add))
dbExecute(pg, readd_sql);                                  say("re-added subs_actions_clean_fk NOT VALID")
dbExecute(pg, "COMMIT;")
ok <- TRUE

final <- dbGetQuery(pg, sprintf(
  'SELECT COUNT(*) rows, COUNT(DISTINCT game_id) games FROM "%s"."subs"', schema))
say("\nDONE. subs now holds %d rows across %d games.", final$rows[[1]], final$games[[1]])
say("Phase 7 will no longer truncate it. Verify after the next ETL run.")
