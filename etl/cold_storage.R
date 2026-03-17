# etl/cold_storage.R — Cold storage export/restore helpers
#
# Exports ETL-only intermediate tables to cumulative Parquet files,
# then TRUNCATEs them to reclaim DB space. Used by etl_full.R Phase 7.

COLD_TABLES <- c("actions_clean", "possessions", "pws", "stints", "subs")

COLD_TABLE_KEYS <- list(
  actions_clean = c("game_id", "id"),
  possessions   = c("game_id", "id"),
  pws           = c("game_id", "id", "team_id"),
  stints        = c("game_id", "segment_id", "team_id"),
  subs          = c("game_id", "id")
)

#' Export a single table to cumulative Parquet, then TRUNCATE.
#'
#' @param pg DBI connection
#' @param schema DB schema name
#' @param table_name One of COLD_TABLES
#' @param cold_dir Local directory for Parquet files
#' @param log_msg Logging function
#' @return TRUE if purge succeeded, FALSE if skipped
export_and_purge_table <- function(pg, schema, table_name, cold_dir, log_msg) {
  stopifnot(table_name %in% COLD_TABLES)

  # 1. Read current rows from DB
  new_rows <- DBI::dbGetQuery(
    pg, sprintf('SELECT * FROM "%s"."%s"', schema, table_name)
  )
  if (nrow(new_rows) == 0) {
    log_msg(sprintf("  [COLD] %s: 0 rows, skipping", table_name))
    return(TRUE)
  }

  # 2. Merge with existing Parquet if present
  dir.create(cold_dir, recursive = TRUE, showWarnings = FALSE)
  parquet_path <- file.path(cold_dir, paste0(table_name, ".parquet"))
  key_cols <- COLD_TABLE_KEYS[[table_name]]

  if (file.exists(parquet_path)) {
    existing <- arrow::read_parquet(parquet_path)
    merged <- rbind(existing, new_rows) |>
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
  verify <- arrow::read_parquet(parquet_path)
  if (nrow(verify) != nrow(merged)) {
    log_msg(sprintf(
      "  [COLD] %s: VERIFICATION FAILED (expected %d rows, got %d) — skipping TRUNCATE",
      table_name, nrow(merged), nrow(verify)
    ), "ERROR")
    return(FALSE)
  }

  # 5. TRUNCATE
  DBI::dbExecute(pg, sprintf('TRUNCATE "%s"."%s"', schema, table_name))
  log_msg(sprintf("  [COLD] %s: truncated, exported %d rows (%.1f MB parquet)",
                  table_name, nrow(merged), file.size(parquet_path) / 1e6))
  TRUE
}

#' Run Phase 7: export and purge all cold tables.
#'
#' @param pg DBI connection
#' @param schema DB schema name
#' @param cold_dir Local Parquet directory (default: "exports/cold")
#' @param log_msg Logging function
#' @return Named logical vector (TRUE = purged, FALSE = skipped)
run_cold_storage_purge <- function(pg, schema, cold_dir = "exports/cold", log_msg) {
  results <- vapply(COLD_TABLES, function(tbl) {
    tryCatch(
      export_and_purge_table(pg, schema, tbl, cold_dir, log_msg),
      error = function(e) {
        log_msg(sprintf("  [COLD] %s: FAILED — %s", tbl, conditionMessage(e)), "ERROR")
        FALSE
      }
    )
  }, logical(1))

  purged <- sum(results)
  log_msg(sprintf("Phase 7 complete: %d/%d tables purged", purged, length(COLD_TABLES)))
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

  df <- arrow::read_parquet(parquet_path)
  DBI::dbWriteTable(
    pg,
    DBI::Id(schema = schema, table = table_name),
    df,
    append = TRUE, row.names = FALSE
  )
  nrow(df)
}
