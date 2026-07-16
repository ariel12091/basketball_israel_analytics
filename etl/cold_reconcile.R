# etl/cold_reconcile.R — pure helpers for cold-storage export reconciliation.
#
# Why: exports used to be scoped to the CURRENT run's game_ids only. After a
# failed Phase 7 (exports failed -> TRUNCATE skipped), the next successful run
# exported only its own new games, then TRUNCATEd all hot rows — permanently
# losing the leftover games (this is how game_ids ~365-388 vanished).

#' Game ids export_cold_table must export: the run's own games plus any hot
#' rows whose game is absent from the cumulative parquet (rescue path).
#' Games already in parquet are NOT re-exported unless in run_ids — a game
#' with partial hot rows must not clobber its valid cold snapshot.
#' @param parquet_ids NULL means "no parquet file yet" -> everything is missing.
cold_export_scope <- function(run_ids, hot_ids, parquet_ids) {
  run_ids <- as.integer(run_ids); run_ids <- run_ids[!is.na(run_ids)]
  hot_ids <- as.integer(hot_ids); hot_ids <- hot_ids[!is.na(hot_ids)]
  if (is.null(parquet_ids)) parquet_ids <- integer(0)
  parquet_ids <- as.integer(parquet_ids)
  sort(unique(c(run_ids, setdiff(hot_ids, parquet_ids))))
}

#' Hot game_ids not covered by parquet AFTER export. Non-empty means the
#' export missed rows and TRUNCATE would destroy them — caller must abort.
cold_coverage_gaps <- function(hot_ids, parquet_ids) {
  hot_ids <- as.integer(hot_ids); hot_ids <- hot_ids[!is.na(hot_ids)]
  sort(unique(setdiff(hot_ids, as.integer(parquet_ids))))
}
