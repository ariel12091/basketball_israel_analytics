# etl/cold_storage.R — Cold storage export/restore helpers
#
# Exports ETL-only intermediate tables to cumulative Parquet files,
# then TRUNCATEs them to reclaim DB space. Used by etl_full.R Phase 7.

COLD_TABLES <- c("actions_clean", "possessions", "pws", "stints", "subs")

COLD_TABLE_KEYS <- list(
  actions_clean = c("game_id", "id"),
  possessions   = c("game_id", "id"),
  pws           = c("game_id", "id", "team_id"),
  stints        = c("game_id", "team_id", "final_start_id", "final_end_id"),
  subs          = c("game_id", "id")
)

#' Export a single table to cumulative Parquet (no truncation — see run_cold_storage_purge).
#'
#' @param pg DBI connection
#' @param schema DB schema name
#' @param table_name One of COLD_TABLES
#' @param cold_dir Local directory for Parquet files
#' @param log_msg Logging function
#' @param game_ids Optional game IDs published by the current ETL run. When
#'   supplied, only those hot rows are merged into cold storage.
#' @return TRUE if export succeeded, FALSE if failed
export_cold_table <- function(
  pg,
  schema,
  table_name,
  cold_dir,
  log_msg,
  game_ids = NULL
) {
  stopifnot(table_name %in% COLD_TABLES)

  # 1. Read only rows published by this run when scoped IDs are supplied.
  # Failed runs can leave older, already-marked games in hot tables; exporting
  # every processed marker would overwrite their valid cold snapshots.
  scoped_ids <- sort(unique(as.integer(game_ids)))
  scoped_ids <- scoped_ids[!is.na(scoped_ids)]
  if (length(scoped_ids)) {
    new_rows <- DBI::dbGetQuery(
      pg,
      sprintf(
        'SELECT t.* FROM "%s"."%s" t
         INNER JOIN "%s"."etl_processed_games" eg ON eg.game_id = t.game_id
         WHERE t.game_id IN (%s)',
        schema,
        table_name,
        schema,
        paste(scoped_ids, collapse = ",")
      )
    )
  } else {
    new_rows <- DBI::dbGetQuery(
      pg,
      sprintf(
        'SELECT t.* FROM "%s"."%s" t
         INNER JOIN "%s"."etl_processed_games" eg ON eg.game_id = t.game_id',
        schema, table_name, schema
      )
    )
  }
  if (nrow(new_rows) == 0) {
    log_msg(sprintf("  [COLD] %s: 0 rows, skipping export", table_name))
    return(TRUE)
  }

  # 2. Merge with existing Parquet if present
  dir.create(cold_dir, recursive = TRUE, showWarnings = FALSE)
  parquet_path <- file.path(cold_dir, paste0(table_name, ".parquet"))
  key_cols <- COLD_TABLE_KEYS[[table_name]]

  if (file.exists(parquet_path)) {
    # Windows cannot overwrite a file while Arrow keeps a memory-mapped
    # section open from a prior read of the same path.
    existing <- arrow::read_parquet(parquet_path, mmap = FALSE)
    # Current DB rows must win when a game is reprocessed and cold storage
    # already contains older rows with the same keys.
    merged <- rbind(new_rows, existing) |>
      dplyr::distinct(dplyr::across(dplyr::all_of(key_cols)), .keep_all = TRUE)
    log_msg(sprintf(
      "  [COLD] %s: merged %d existing + %d new -> %d unique rows",
      table_name, nrow(existing), nrow(new_rows), nrow(merged)
    ))
  } else {
    merged <- new_rows
    log_msg(sprintf("  [COLD] %s: %d new rows (first export)", table_name, nrow(merged)))
  }

  # 3. Write merged Parquet
  arrow::write_parquet(merged, parquet_path)

  # 4. Read-back verification
  verify <- arrow::read_parquet(parquet_path, mmap = FALSE)
  if (nrow(verify) != nrow(merged)) {
    log_msg(sprintf(
      "  [COLD] %s: VERIFICATION FAILED (expected %d rows, got %d)",
      table_name, nrow(merged), nrow(verify)
    ), "ERROR")
    return(FALSE)
  }

  log_msg(sprintf("  [COLD] %s: exported %d rows (%.1f MB parquet)",
                  table_name, nrow(merged), file.size(parquet_path) / 1e6))
  TRUE
}

#' Run Phase 7: export all cold tables to Parquet, then TRUNCATE all at once.
#'
#' Single TRUNCATE handles FK dependencies between cold tables.
#' lineups_lookup FK to actions_clean is dropped/re-added around the TRUNCATE.
#'
#' @param pg DBI connection
#' @param schema DB schema name
#' @param cold_dir Local Parquet directory (default: "exports/cold")
#' @param log_msg Logging function
#' @param game_ids Optional game IDs published by the current ETL run.
#' @return Named logical vector (TRUE = exported, FALSE = skipped/failed)
run_cold_storage_purge <- function(
  pg,
  schema,
  cold_dir = "exports/cold",
  log_msg,
  game_ids = NULL
) {
  # Phase A: export each table to Parquet (no truncation yet)
  results <- vapply(COLD_TABLES, function(tbl) {
    tryCatch(
      export_cold_table(
        pg,
        schema,
        tbl,
        cold_dir,
        log_msg,
        game_ids = game_ids
      ),
      error = function(e) {
        log_msg(sprintf("  [COLD] %s: EXPORT FAILED — %s", tbl, conditionMessage(e)), "ERROR")
        FALSE
      }
    )
  }, logical(1))

  if (!all(results)) {
    log_msg("Phase 7: some exports failed, skipping TRUNCATE", "WARN")
    return(results)
  }

  # Phase B: drop lineups_lookup FK, TRUNCATE all 5, re-add FK
  tryCatch({
    log_msg("  [COLD] Dropping lineups_lookup FK for TRUNCATE...")
    DBI::dbExecute(pg, sprintf(
      'ALTER TABLE "%s"."lineups_lookup" DROP CONSTRAINT IF EXISTS "lineups_lookup_actions_clean_fk"',
      schema))

    tbl_list <- paste(sprintf('"%s"."%s"', schema, COLD_TABLES), collapse = ", ")
    DBI::dbExecute(pg, paste("TRUNCATE", tbl_list))
    log_msg("  [COLD] All 5 tables truncated")

    DBI::dbExecute(pg, sprintf(
      'ALTER TABLE "%s"."lineups_lookup" ADD CONSTRAINT "lineups_lookup_actions_clean_fk"
       FOREIGN KEY (game_id, id) REFERENCES "%s"."actions_clean" (game_id, id) NOT VALID',
      schema, schema))
    log_msg("  [COLD] Re-added lineups_lookup FK (NOT VALID)")
  }, error = function(e) {
    log_msg(sprintf("  [COLD] TRUNCATE FAILED — %s", conditionMessage(e)), "ERROR")
  })

  purged <- sum(results)
  log_msg(sprintf("Phase 7 complete: %d/%d tables exported & purged", purged, length(COLD_TABLES)))
  results
}

#' Restore a single table from Parquet cold storage.
#'
#' @param pg DBI connection
#' @param schema DB schema name
#' @param table_name One of COLD_TABLES
#' @param cold_dir Local Parquet directory
#' @return Number of rows restored
restore_cold_table <- function(pg, schema, table_name, cold_dir = "exports/cold") {
  stopifnot(table_name %in% COLD_TABLES)
  parquet_path <- file.path(cold_dir, paste0(table_name, ".parquet"))
  if (!file.exists(parquet_path)) stop(sprintf("No Parquet found: %s", parquet_path))

  df <- arrow::read_parquet(parquet_path, mmap = FALSE)
  DBI::dbWriteTable(
    pg,
    DBI::Id(schema = schema, table = table_name),
    df,
    append = TRUE, row.names = FALSE
  )
  nrow(df)
}
