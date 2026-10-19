# DB Cold Storage Implementation Plan

> **For agentic workers:** REQUIRED: Use superpowers:subagent-driven-development (if subagents available) or superpowers:executing-plans to implement this plan. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Reduce Supabase DB from ~602 MB to ~462 MB by purging 5 ETL-only intermediate tables after each pipeline run, with Parquet cold storage backups.

**Architecture:** Add Phase 7 to `etl_full.R` that exports `actions_clean`, `possessions`, `pws`, `stints`, `subs` to cumulative Parquet files then TRUNCATEs them. A new `etl_processed_games` tracking table replaces the `actions_clean` scan for new-game detection. GH Actions uploads Parquets to a fixed GH release.

**Tech Stack:** R, `arrow` (Parquet I/O), PostgreSQL/Supabase, GitHub Actions + `gh` CLI

**Spec:** `docs/superpowers/specs/2026-03-17-db-cold-storage-design.md`

---

## File Structure

| Action | File | Responsibility |
|--------|------|----------------|
| Create | `etl/cold_storage.R` | Export, restore, and purge helper functions |
| Create | `scripts/restore_cold_storage.R` | Standalone restore script for full MV rebuilds |
| Modify | `etl/etl_full.R` | Replace `actions_clean` query, update guardrail, add Phase 7 |
| Modify | `.github/workflows/etl-full.yml` | Add `arrow` dep, add GH release upload step |
| Modify | `.gitignore` | Add `exports/cold/` |

---

## Chunk 1: Core Cold Storage Logic

### Task 1: Create `etl_processed_games` table and backfill

**Files:**
- Modify: `etl/etl_full.R:111-131` (add `ensure_etl_processed_games` near existing `ensure_app_meta`)

- [ ] **Step 1: Add `ensure_etl_processed_games` function to `etl_full.R`**

Add after the `set_last_success` function (line 131):

```r
ensure_etl_processed_games <- function(pg, schema) {
  DBI::dbExecute(
    pg,
    sprintf(
      'CREATE TABLE IF NOT EXISTS "%s"."etl_processed_games" (
         game_id      int PRIMARY KEY,
         game_year    int NOT NULL,
         processed_at timestamptz DEFAULT now()
       )',
      schema
    )
  )
}

backfill_etl_processed_games <- function(pg, schema, log_msg) {
  cnt <- DBI::dbGetQuery(
    pg,
    sprintf('SELECT count(*) AS n FROM "%s"."etl_processed_games"', schema)
  )$n
  if (cnt > 0) return(invisible(NULL))

  n <- DBI::dbExecute(
    pg,
    sprintf(
      'INSERT INTO "%s"."etl_processed_games" (game_id, game_year)
       SELECT DISTINCT ac.game_id, s.game_year
       FROM "%s"."actions_clean" ac
       JOIN "%s"."schedule" s USING (game_id)
       ON CONFLICT DO NOTHING',
      schema, schema, schema
    )
  )
  log_msg(sprintf("Backfilled etl_processed_games with %d games from actions_clean", n))
}
```

- [ ] **Step 2: Call backfill at pipeline start**

In `etl_full()`, after `log_msg(sprintf("Schema: %s ..."))` (line 179), add:

```r
ensure_etl_processed_games(pg, SCHEMA)
backfill_etl_processed_games(pg, SCHEMA, log_msg)
```

- [ ] **Step 3: Replace `actions_clean` scan with `etl_processed_games` query**

Replace lines 206-208 in `etl_full.R`:

```r
# OLD:
existing <- DBI::dbGetQuery(
  pg, sprintf('SELECT DISTINCT game_id FROM "%s"."actions_clean"', SCHEMA)
)
```

With:

```r
# NEW:
existing <- DBI::dbGetQuery(
  pg, sprintf('SELECT game_id FROM "%s"."etl_processed_games"', SCHEMA)
)
```

- [ ] **Step 4: Insert processed game_ids into tracking table at end of Phase 2**

After the `pws` upsert and ANALYZE (line 436), before `elapsed <- (proc.time() - t0)["elapsed"]` (line 438), add:

```r
# Record processed games in tracking table (only games that were actually fetched)
track_ids <- intersect(as.integer(ids), fetchable_sched$game_id)
if (length(track_ids)) {
  track_csv <- paste(track_ids, collapse = ",")
  track_years <- sched_subset$game_year[match(track_ids, sched_subset$game_id)]
  # Build VALUES clause for ON CONFLICT DO NOTHING (safe for re-runs)
  vals <- paste(
    sprintf("(%d, %d, now())", as.integer(track_ids), as.integer(track_years)),
    collapse = ", "
  )
  DBI::dbExecute(
    pg,
    sprintf(
      'INSERT INTO "%s"."etl_processed_games" (game_id, game_year, processed_at)
       VALUES %s
       ON CONFLICT (game_id) DO NOTHING',
      SCHEMA, vals
    )
  )
  log_msg(sprintf("  etl_processed_games: %d game(s) recorded", length(track_ids)))
}
```

- [ ] **Step 5: Update post-Phase-2 guardrail**

Replace line 450:

```r
# OLD:
for (tbl_name in c("schedule", "actions_clean", "lineups_lookup", "pws")) {
```

With:

```r
# NEW: actions_clean and pws are purged after each run — only check non-purged tables
for (tbl_name in c("schedule", "lineups_lookup")) {
```

- [ ] **Step 6: Test locally with dry_run**

Run:
```bash
RSCRIPT="/c/Program Files/R/R-4.4.2/bin/Rscript.exe"
"$RSCRIPT" -e "Sys.setenv(APP_ENV='test'); source('etl/etl_full.R'); etl_full(dry_run = TRUE)"
```

Expected: Pipeline completes, logs show `etl_processed_games` backfill message, no errors.

- [ ] **Step 7: Commit**

```bash
git add etl/etl_full.R
git commit -m "feat(etl): add etl_processed_games tracking table, replace actions_clean scan"
```

---

### Task 2: Create cold storage helper functions

**Files:**
- Create: `etl/cold_storage.R`

- [ ] **Step 1: Create `etl/cold_storage.R`**

```r
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
```

- [ ] **Step 2: Commit**

```bash
git add etl/cold_storage.R
git commit -m "feat(etl): add cold storage export/restore helpers"
```

---

### Task 3: Wire Phase 7 into `etl_full.R`

**Files:**
- Modify: `etl/etl_full.R:169-178` (source cold_storage.R)
- Modify: `etl/etl_full.R:1037-1064` (add Phase 7 before summary)

- [ ] **Step 1: Source `cold_storage.R` in the definitions block**

After `eval(parse(text = etl_lines), envir = .GlobalEnv)` (line 178), add:

```r
source("etl/cold_storage.R")
log_msg("Sourced etl/cold_storage.R")
```

- [ ] **Step 2: Add Phase 7 block**

Insert after the closing brace of the `if (!length(processed_ids))` / else block (line 1037), before the Summary section (line 1039):

```r
  # =========================================================================
  # Phase 7: Cold Storage Purge
  # =========================================================================

  if (!dry_run && isTRUE(pipeline_ok)) {
    log_msg("--- Phase 7: Cold Storage Purge ---")
    tryCatch({
      t0 <- proc.time()
      cold_dir <- "exports/cold"

      purge_results <- run_cold_storage_purge(pg, SCHEMA, cold_dir, log_msg)

      # Upload to GH release on CI
      if (nzchar(Sys.getenv("GITHUB_ACTIONS"))) {
        log_msg("  Uploading Parquet files to GH release cold-storage/latest ...")
        parquet_files <- list.files(cold_dir, pattern = "\\.parquet$", full.names = TRUE)
        if (length(parquet_files)) {
          upload_cmd <- sprintf(
            'gh release upload cold-storage/latest %s --clobber',
            paste(shQuote(parquet_files), collapse = " ")
          )
          upload_exit <- system(upload_cmd)
          if (upload_exit == 0) {
            log_msg(sprintf("  Uploaded %d Parquet file(s) to cold-storage/latest", length(parquet_files)))
          } else {
            log_msg("  GH release upload failed (non-zero exit) — Parquets saved locally", "WARN")
          }
        }
      }

      elapsed <- (proc.time() - t0)["elapsed"]
      log_msg(sprintf("Phase 7 complete in %.1fs", elapsed))
    }, error = function(e) {
      log_msg(sprintf("Phase 7 FAILED: %s", conditionMessage(e)), "ERROR")
      mark_phase_failed("Phase 7", conditionMessage(e))
    })
  } else if (!dry_run) {
    log_msg("Skipping Phase 7 (cold storage purge) due to pipeline failures")
  }
```

- [ ] **Step 3: Commit**

```bash
git add etl/etl_full.R
git commit -m "feat(etl): add Phase 7 cold storage purge to pipeline"
```

---

### Task 4: Create standalone restore script

**Files:**
- Create: `scripts/restore_cold_storage.R`

- [ ] **Step 1: Create `scripts/restore_cold_storage.R`**

```r
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
```

- [ ] **Step 2: Commit**

```bash
git add scripts/restore_cold_storage.R
git commit -m "feat(scripts): add cold storage restore script for full MV rebuilds"
```

---

## Chunk 2: CI Integration and Cleanup

### Task 5: Update `.gitignore`

**Files:**
- Modify: `.gitignore`

- [ ] **Step 1: Add `exports/cold/` to `.gitignore`**

Append to `.gitignore`:

```
exports/cold/
```

- [ ] **Step 2: Commit**

```bash
git add .gitignore
git commit -m "chore: gitignore cold storage Parquet exports"
```

---

### Task 6: Update GH Actions workflow

**Files:**
- Modify: `.github/workflows/etl-full.yml`

- [ ] **Step 1: Add `arrow` to R package list**

In the `Install ETL deps (cached)` step (line 35-53), add to the `packages` list:

```yaml
            any::arrow
```

- [ ] **Step 2: Add GH release ensure + upload step**

After the "Run ETL wrapper" step (line 55-118) and before "Upload ETL logs" (line 120), add:

```yaml
      - name: Ensure cold-storage release exists
        if: success()
        shell: bash
        env:
          GH_TOKEN: ${{ github.token }}
        run: |
          gh release view cold-storage/latest > /dev/null 2>&1 || \
            gh release create cold-storage/latest \
              --title "Cold Storage Exports" \
              --notes "Cumulative Parquet backups of ETL intermediate tables"
```

Note: The actual Parquet upload happens inside the R pipeline (Phase 7) using `system("gh release upload ...")`. The `GH_TOKEN` env var must also be added to the "Run ETL wrapper" step:

```yaml
          GH_TOKEN: ${{ github.token }}
```

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/etl-full.yml
git commit -m "ci: add arrow dep and cold-storage release setup to ETL workflow"
```

**2026-04-01 follow-up: scheduled workflow failure diagnosis and fix**

- Observed failure: scheduled `ETL Full` GitHub Actions runs were marked failed even when the ETL itself completed successfully with `No new games to process`.
- Root cause: the workflow-level step `Ensure cold-storage release exists` still ran on no-op ETL runs, and `github.token` only had read access to repository contents. That caused `gh release create cold-storage/latest` to fail with `HTTP 403: Resource not accessible by integration`.
- Fix shipped in `.github/workflows/etl-full.yml`:
  - scope `contents: write` permission to the `etl` job
  - run the release-ensure step only when `hashFiles('exports/cold/*.parquet') != ''`
- Result: no-op scheduled runs no longer fail on release setup, and runs that actually produce cold-storage parquet files still have the permissions needed to create/upload the release assets.

---

### Task 7: End-to-end local test

- [ ] **Step 1: Verify `arrow` is installed locally**

```bash
RSCRIPT="/c/Program Files/R/R-4.4.2/bin/Rscript.exe"
"$RSCRIPT" -e "if (!requireNamespace('arrow', quietly=TRUE)) install.packages('arrow'); cat('arrow', packageVersion('arrow'), '\n')"
```

Expected: Prints arrow version.

- [ ] **Step 2: Run full ETL (non-dry-run) with no new games**

```bash
"$RSCRIPT" -e "Sys.setenv(APP_ENV='test'); source('etl/etl_full.R'); etl_full()"
```

Expected: "No new games to process" → skips Phases 3-7. Logs show backfill of `etl_processed_games`. No errors.

- [ ] **Step 3: Run full ETL for a specific game to exercise Phase 7**

Pick a recent game_id from the schedule. Run:

```bash
"$RSCRIPT" -e "Sys.setenv(APP_ENV='test'); source('etl/etl_full.R'); etl_full(game_ids = c(<GAME_ID>))"
```

Expected:
- Phases 2-6 complete normally
- Phase 7 logs show `[COLD]` export messages for each table
- Parquet files appear in `exports/cold/`
- DB tables are truncated (verify with a quick query)

- [ ] **Step 4: Verify Parquet files are valid**

```bash
"$RSCRIPT" -e "for (f in list.files('exports/cold', full.names=TRUE)) { df <- arrow::read_parquet(f); cat(basename(f), nrow(df), 'rows\n') }"
```

Expected: Each file prints a positive row count.

- [ ] **Step 5: Test restore flow**

```bash
"$RSCRIPT" -e "Sys.setenv(APP_ENV='test'); source('scripts/restore_cold_storage.R')"
```

Expected: Each table shows "restored N rows". Then verify tables have data:

```bash
"$RSCRIPT" -e "
  readRenviron('etl/.Renviron')
  pg <- DBI::dbConnect(RPostgres::Postgres(), host=Sys.getenv('PG_HOST'), port=as.integer(Sys.getenv('PG_PORT')), dbname=Sys.getenv('PG_DB'), user=Sys.getenv('PG_USER'), password=Sys.getenv('PG_PASS'), sslmode=Sys.getenv('PG_SSLMODE'))
  for (t in c('actions_clean','possessions','pws','stints','subs')) cat(t, DBI::dbGetQuery(pg, sprintf('SELECT count(*) AS n FROM basketball_test.%s', t))\$n, '\n')
  DBI::dbDisconnect(pg)
"
```

- [ ] **Step 6: Re-run ETL to verify purge works on restored data**

```bash
"$RSCRIPT" -e "Sys.setenv(APP_ENV='test'); source('etl/etl_full.R'); etl_full(game_ids = c(<SAME_GAME_ID>))"
```

Expected: Phase 7 merges + deduplicates, Parquet file sizes stay stable, tables truncated again.

- [ ] **Step 7: Final commit — all working**

```bash
git add etl/ scripts/ .github/ .gitignore
git commit -m "test: verify cold storage export/restore/purge cycle"
```
