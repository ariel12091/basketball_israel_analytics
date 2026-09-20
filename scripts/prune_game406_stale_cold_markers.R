#!/usr/bin/env Rscript

# The cumulative cold merge is key-upsert based, so rows deliberately removed
# by a reprocess can survive in the older Parquet snapshot. Game 406 removed
# these two false quarter markers from clean_actions; prune those exact,
# validated rows after the accepted scoped reprocess. Back up the removed rows
# separately so this operation remains reversible.

suppressPackageStartupMessages(library(arrow))

tables <- c("actions_clean", "possessions", "pws")
stale_ids <- c(4060238L, 4060239L)
backup_dir <- "exports/cold/backup-2026-09-20-game406-stale-markers"
dir.create(backup_dir, recursive = TRUE, showWarnings = FALSE)

for (table in tables) {
  path <- file.path("exports/cold", paste0(table, ".parquet"))
  rows <- as.data.frame(read_parquet(path, mmap = FALSE))
  stale <- rows$game_id == 406L & rows$id %in% stale_ids
  removed <- rows[stale, , drop = FALSE]

  if (
    nrow(removed) != 2L ||
    !setequal(as.integer(removed$id), stale_ids) ||
    !all(as.character(removed$type) == "quarter")
  ) {
    stop(
      sprintf(
        "%s: expected exactly the two known game-406 quarter markers; found %d",
        table, nrow(removed)
      ),
      call. = FALSE
    )
  }

  backup <- file.path(backup_dir, paste0(table, ".parquet"))
  write_parquet(removed, backup)

  kept <- rows[!stale, , drop = FALSE]
  temp <- paste0(path, ".game406-prune.tmp")
  write_parquet(kept, temp)
  verify <- as.data.frame(read_parquet(temp, col_select = c("game_id", "id"), mmap = FALSE))
  if (nrow(verify) != nrow(kept) || any(verify$game_id == 406L & verify$id %in% stale_ids)) {
    stop(sprintf("%s: temporary Parquet verification failed", table), call. = FALSE)
  }
  if (!file.copy(temp, path, overwrite = TRUE)) {
    stop(sprintf("%s: could not replace %s", table, path), call. = FALSE)
  }
  unlink(temp)
  message(sprintf("%s: removed 2 stale game-406 markers; %d rows remain", table, nrow(kept)))
}
