# DB Cold Storage — Post-ETL Purge with Parquet Export

**Date:** 2026-03-17
**Status:** Draft
**Goal:** Reduce Supabase DB from ~602 MB to ~462 MB (under 500 MB free tier limit)

## Problem

The Supabase free tier allows 500 MB of database storage. The current DB is ~602 MB. Five intermediate tables used only during ETL account for ~140 MB but sit idle between ETL runs. These tables are never queried by the Shiny app, Plumber API, or any app-facing SQL function.

## Approach

Add a new Phase 7 to the ETL pipeline that exports intermediate tables to cumulative Parquet files, then truncates them. A lightweight tracking table replaces the current new-game detection query that scans one of the purged tables.

## Tables Purged

| Table | Size (MB) | ETL Role | App Role |
|-------|-----------|----------|----------|
| `pws` | 57.6 | Feeds `refresh_df_pts_poss_lineups_longer_for_games()` | None |
| `possessions` | 35.6 | Feeds same refresh function (`cum_scores` CTE) | None |
| `actions_clean` | 32.0 | Source for possessions computation (R-side) | None |
| `subs` | 8.6 | No downstream references found | None |
| `stints` | 5.8 | Source for pws computation (R-side) | None |
| **Total** | **140.2** | | |

**Dependency chain within ETL:**
- **Phase 2 (internal chaining):** `actions_clean` → `possessions` (via `compute_possessions()`) → `stints` (via `compute_stints()` which reads `possessions` from DB) → `pws` (joins possessions + stints in R). Each table is written to DB and then read back by subsequent Phase 2 steps.
- **Phase 4 (MV refresh):** `refresh_df_pts_poss_lineups_longer_for_games()` reads `pws` and `possessions` from DB for the new game_ids.
- **After Phase 4:** All 5 tables are idle until the next ETL run.

The data is always reproducible from the external API or from the Parquet backups.

## Design

### 1. ETL Phase 7 — Purge Logic

Runs after Phase 6 validation, only when `pipeline_ok == TRUE` and `dry_run == FALSE`.

For each of the 5 tables, independently:

1. Read current rows from DB into R data frame
2. If a local Parquet file exists for this table, read it, `rbind` with new rows, deduplicate
3. Write merged Parquet file (overwrite)
4. `TRUNCATE` the table in DB

If the Parquet write fails for a table, skip the TRUNCATE for that table and log a warning. After writing, read-back and verify `nrow()` matches expected count before proceeding to TRUNCATE. Other tables proceed independently.

On GitHub Actions: after all exports, upload Parquet files to a fixed GH release (`cold-storage/latest`) via `gh release upload --clobber`.

### 2. `etl_processed_games` Tracking Table

Replaces `SELECT DISTINCT game_id FROM actions_clean` for new-game detection (ETL Phase 2, line ~207).

```sql
CREATE TABLE basketball_test.etl_processed_games (
  game_id      int PRIMARY KEY,
  game_year    int NOT NULL,
  processed_at timestamptz DEFAULT now()
);
```

Changes:
- **Phase 2:** Query `etl_processed_games` instead of `actions_clean` for existing game_ids
- **End of Phase 2:** INSERT newly processed game_ids into `etl_processed_games` (immediately after `pws` upsert, mirroring the location of the query it replaces)
- **One-time backfill:** Populate from current `actions_clean` before first purge

Size: ~1 row per game, a few KB total.

### 3. Parquet Export Implementation

**Dependency:** `arrow` R package (binary from CRAN, fast install).

**File structure:**
```
exports/cold/           # gitignored
  actions_clean.parquet
  possessions.parquet
  pws.parquet
  stints.parquet
  subs.parquet
```

Five cumulative files, each containing full historical data across all seasons. Deduplication uses explicit key columns per table to avoid floating-point/timestamp precision mismatches:

| Table | Dedup Key |
|-------|-----------|
| `actions_clean` | `game_id, id` |
| `possessions` | `game_id, id` |
| `pws` | `game_id, id, team_id` |
| `stints` | `game_id, segment_id, team_id` |
| `subs` | `game_id, id` |

Uses `dplyr::distinct(.data, <key_cols>, .keep_all = TRUE)` instead of bare `distinct()`.

**Memory:** Largest table is `pws` at 57.6 MB on disk. With existing Parquet + new rows in memory, peak usage is ~200 MB — well within local Windows and GH Actions runner limits (7 GB RAM).

**Export function pattern:**
```r
export_cold_table <- function(pg, schema, table_name, cold_dir) {
  new_rows <- DBI::dbGetQuery(pg, sprintf('SELECT * FROM "%s"."%s"', schema, table_name))
  if (nrow(new_rows) == 0) return(TRUE)

  parquet_path <- file.path(cold_dir, paste0(table_name, ".parquet"))
  if (file.exists(parquet_path)) {
    existing <- arrow::read_parquet(parquet_path)
    key_cols <- COLD_TABLE_KEYS[[table_name]]  # e.g., c("game_id", "id")
    merged <- rbind(existing, new_rows) |> dplyr::distinct(dplyr::across(dplyr::all_of(key_cols)), .keep_all = TRUE)
  } else {
    merged <- new_rows
  }

  arrow::write_parquet(merged, parquet_path)
  TRUE
}
```

### 4. GitHub Actions Integration

**Workflow changes to `.github/workflows/etl-full.yml`:**

1. Add `arrow` to R package installation step
2. After Phase 7 exports, upload Parquets to GH release:
   ```bash
   gh release upload cold-storage/latest exports/cold/*.parquet --clobber
   ```

**First-time setup (manual, once):**
```bash
gh release create cold-storage/latest --title "Cold Storage Exports" --notes "Cumulative Parquet backups of ETL intermediate tables"
```

**Environment detection in R:**
```r
is_ci <- nzchar(Sys.getenv("GITHUB_ACTIONS"))
```
Used to conditionally trigger the `gh release upload` step.

### 5. Full Rebuild Restore Flow

When a full MV rebuild is needed (rare — e.g., fixing a metric formula in `df_pts_poss_lineups_longer_mv`):

1. Download Parquets:
   ```bash
   gh release download cold-storage/latest -D exports/cold/
   ```
2. Run restore script (`scripts/restore_cold_storage.R`) that loads each Parquet back into its table:
   ```r
   restore_cold_table <- function(pg, schema, table_name, cold_dir) {
     df <- arrow::read_parquet(file.path(cold_dir, paste0(table_name, ".parquet")))
     DBI::dbWriteTable(pg, DBI::Id(schema = schema, table = table_name), df, append = TRUE, row.names = FALSE)
   }
   ```
3. Run `rebuild_all_mvs()` from L1
4. Run Phase 7 purge again

**Note:** `etl_processed_games` does NOT need to be cleared for a rebuild — it correctly reflects which games have been processed. The restore puts historical data back into the intermediate tables so MVs can be rebuilt, but the tracking table remains valid. If you want to force re-processing of specific games (re-fetch from API), delete those game_ids from `etl_processed_games` before running ETL.

**Note on manual re-runs:** `etl_full(game_ids = c(12345))` for a game that was already purged will re-fetch from the API and re-insert into the intermediate tables. Phase 7 will merge+dedup with existing Parquet data. This is safe and expected.

## ETL Code Changes Required

**`etl/etl_full.R`:**
1. **Phase 2 new-game detection (~line 207):** Replace `SELECT DISTINCT game_id FROM actions_clean` with `SELECT game_id FROM etl_processed_games`
2. **End of Phase 2 (~line 434):** Add `INSERT INTO etl_processed_games` for newly processed game_ids
3. **Post-Phase-2 guardrail (~lines 450-458):** Remove `actions_clean` and `pws` from the total-row-count check (these tables now only contain current-batch rows, not cumulative). Keep `schedule` and `lineups_lookup` checks.
4. **New Phase 7:** Add cold storage export + TRUNCATE logic after Phase 6
5. **Phase 6 validation (~lines 872-878):** No change needed — per-game row checks for the current batch still work since the tables have current-batch rows until Phase 7 purges them

**`.github/workflows/etl-full.yml`:**
1. Add `arrow` to R package installation
2. Add GH release upload step after ETL completes
3. Add `gh release view || gh release create` guard for first-time setup

**New files:**
- `etl/cold_storage.R` — export/restore helper functions
- `scripts/restore_cold_storage.R` — standalone restore script for full rebuild

## Normal ETL Flow (After Implementation)

```
Phase 2: Upsert new game rows into the 5 tables (start empty from last purge)
         Insert new game_ids into etl_processed_games
Phase 3: Sub-lineup generation
Phase 4: MV refresh (reads the 5 tables for new game_ids only)
Phase 5: Sub-lineup stats refresh
Phase 6: Validation (per-game row checks on actions_clean, possessions, pws — still valid since Phase 7 hasn't run yet)
Phase 7: Export tables to Parquet → TRUNCATE → upload to GH release (CI only)
```

Between ETL runs, the 5 tables are empty. The app queries MVs and app-facing tables only.

## Risks and Mitigations

| Risk | Mitigation |
|------|------------|
| Parquet write fails, data lost before TRUNCATE | Export must succeed before TRUNCATE; failure skips that table |
| `arrow` package unavailable on GH Actions | Binary available on CRAN; add explicit install step |
| Full rebuild needed but Parquets corrupted | Data is reproducible from external API via full ETL re-ingest |
| Memory pressure from large Parquet + DB read | Peak ~200 MB, well within 7 GB runner limit |
| `etl_processed_games` out of sync | Backfilled once from `actions_clean`; then append-only alongside Phase 6 validation |

## Out of Scope

- Rewriting `onoff_compute()` / `four_factors_compute()` to eliminate `player_onoff_by_game` (87 MB) and `player_four_factors_by_game` (107 MB) — revisit if DB grows past buffer
- Trimming `lineups_lookup` (72.8 MB) — needs careful audit, not urgent
- Partitioning or compression — marginal gains vs. complexity
- Migration to a different hosting provider
