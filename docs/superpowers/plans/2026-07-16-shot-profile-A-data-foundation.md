# Shot Profile — Plan A: Data Foundation (cold-export gap fix + shot_zones)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Fix the cold-storage export gap (games missing from parquet forever after a failed run), recover the ~8 lost games, and create the persistent `shot_zones` table (corner-3 flags) with backfill + incremental ETL population — the data layer Plans B (SQL) and C (Shiny) build on.

**Architecture:** The parquet export in `etl/cold_storage.R` is scoped to the current run's game_ids; after a failed Phase 7, the next successful run exports only its own games and then TRUNCATEs everything — leftover games' rows are lost. Fix = widen the export scope to include hot rows missing from parquet + a coverage gate before TRUNCATE. Corner-3 flags come from `actions_clean.parameters_coord_y` (raw units; committed v2 rule: 3PT and `y ≤ 285` ⇔ court-units y ≤ 2.85 = `corner3_height` in `scripts/analysis/fiba_court_zones.R`), persisted in a tiny `shot_zones` table that is never cold-purged, populated incrementally at the end of ETL Phase 2 and backfilled once from parquet.

**Tech Stack:** R 4.4.2 (`"/c/Program Files/R/R-4.4.2/bin/Rscript.exe"`, or PowerShell: `& "C:\Program Files\R\R-4.4.2\bin\Rscript.exe"`), DBI/RPostgres, arrow (parquet), PostgreSQL (Supabase), testthat for unit tests.

## Global Constraints

- Schema is `basketball_test` everywhere.
- Parameterized SQL (`$1, $2`) for any user/variable values; `sprintf` only for identifiers/id-lists as the existing ETL code does.
- Credentials: `readRenviron("etl/.Renviron")` (write access). DDL runs on **port 5432** (same pooler host); DML on 6543.
- 2-space indent, snake_case, base-R style (no purrr).
- **R scripts must run from the repo root** (relative paths); on Windows prefer PowerShell + absolute script path (Git Bash showed spurious segfaults with a wrong cwd).
- Edit SQL/R files with exact strings only — never multi-line regex; check `git diff --stat` plausibility after any scripted edit.
- Commits end with: `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`
- Branches: Tasks 1–4 on `etl/cold-export-gap`; Tasks 5–7 on `sql/shot-zones` (created from main after Task 4 merges).

---

### Task 1: Pure reconciliation helpers (TDD)

**Files:**
- Create: `etl/cold_reconcile.R`
- Test: `etl/tests/test_cold_reconcile.R`

**Interfaces:**
- Produces: `cold_export_scope(run_ids, hot_ids, parquet_ids)` → integer vector of game_ids to export (run's games ∪ hot games absent from parquet). `cold_coverage_gaps(hot_ids, parquet_ids)` → integer vector of hot game_ids NOT covered by parquet (must be empty before TRUNCATE). Both pure, no DB.

- [ ] **Step 1: Create branch**

```bash
git checkout main && git checkout -b etl/cold-export-gap
```

- [ ] **Step 2: Write the failing tests**

Create `etl/tests/test_cold_reconcile.R`:

```r
# Unit tests for etl/cold_reconcile.R (pure helpers, no DB).
# Run: Rscript -e "testthat::test_file('etl/tests/test_cold_reconcile.R')"
library(testthat)
source("etl/cold_reconcile.R")

test_that("cold_export_scope = run ids plus hot ids missing from parquet", {
  # run processed 300; 250+251 are leftovers from a failed prior run:
  # 250 already exported (in parquet), 251 never exported -> must be included
  expect_equal(
    cold_export_scope(run_ids = 300L, hot_ids = c(250L, 251L, 300L),
                      parquet_ids = c(100L, 250L)),
    c(251L, 300L)
  )
})

test_that("cold_export_scope with empty run still rescues unexported hot rows", {
  expect_equal(
    cold_export_scope(run_ids = integer(0), hot_ids = c(9L, 7L),
                      parquet_ids = 7L),
    9L
  )
})

test_that("cold_export_scope drops NA and duplicates, sorts ascending", {
  expect_equal(
    cold_export_scope(run_ids = c(5L, NA, 5L), hot_ids = c(5L, 3L),
                      parquet_ids = integer(0)),
    c(3L, 5L)
  )
})

test_that("cold_export_scope with no parquet file (NULL) exports all hot rows", {
  expect_equal(
    cold_export_scope(run_ids = 1L, hot_ids = c(1L, 2L), parquet_ids = NULL),
    c(1L, 2L)
  )
})

test_that("cold_coverage_gaps flags hot ids not in parquet", {
  expect_equal(cold_coverage_gaps(hot_ids = c(1L, 2L, 3L), parquet_ids = c(1L, 3L)), 2L)
  expect_equal(cold_coverage_gaps(hot_ids = c(1L, 3L), parquet_ids = c(1L, 2L, 3L)),
               integer(0))
  expect_equal(cold_coverage_gaps(hot_ids = integer(0), parquet_ids = integer(0)),
               integer(0))
})
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('etl/tests/test_cold_reconcile.R')"` (from repo root)
Expected: FAIL — `cannot open file 'etl/cold_reconcile.R'`

- [ ] **Step 4: Write the implementation**

Create `etl/cold_reconcile.R`:

```r
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
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('etl/tests/test_cold_reconcile.R')"`
Expected: `[ FAIL 0 | WARN 0 | SKIP 0 | PASS 8 ]`

- [ ] **Step 6: Commit**

```bash
git add etl/cold_reconcile.R etl/tests/test_cold_reconcile.R
git commit -m "feat(etl): pure helpers for cold-export scope + coverage reconciliation

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 2: Diagnose the actual gap (read-only)

**Files:**
- Create: `scripts/analysis/cold_gap_diagnostic.R` (rerunnable; writes findings to stdout/scratch)

**Interfaces:**
- Consumes: `etl/cold_reconcile.R::cold_coverage_gaps`
- Produces: the authoritative list of missing game_ids (needed by Task 4). No code interface — findings recorded in the Task 4 recovery run.

- [ ] **Step 1: Write the diagnostic script**

Create `scripts/analysis/cold_gap_diagnostic.R`:

```r
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
```

- [ ] **Step 2: Run it and record the missing ids**

Run (PowerShell, repo root): `& "C:\Program Files\R\R-4.4.2\bin\Rscript.exe" scripts/analysis/cold_gap_diagnostic.R`
Expected: ~8 missing game_ids per table (doc says ids ~365–388, 2026 games), identical across the 5 tables. **Write the exact id list into the Task 4 recovery step before executing it.** If MISSING = 0 everywhere, the gap has self-healed — skip Task 4, keep Tasks 1/3.

- [ ] **Step 3: Commit**

```bash
git add scripts/analysis/cold_gap_diagnostic.R
git commit -m "chore(etl): cold-storage gap diagnostic script

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: Wire scope fix + coverage gate into cold_storage.R

**Files:**
- Modify: `etl/cold_storage.R:26-107` (`export_cold_table`) and `:120-195` (`run_cold_storage_purge`)
- Modify: `etl/etl_full.R` (one `source()` line — see Step 1)

**Interfaces:**
- Consumes: `cold_export_scope`, `cold_coverage_gaps` from `etl/cold_reconcile.R`
- Produces: `export_cold_table` (same signature) now rescues unexported hot games; `run_cold_storage_purge` (same signature) aborts TRUNCATE if any hot game is missing from `actions_clean.parquet` after export.

- [ ] **Step 1: Source the helpers**

In `etl/etl_full.R`, find the line that sources cold storage (search `source` + `cold_storage`; it reads like `source("etl/cold_storage.R")` or a `file.path` equivalent near the other Phase-7 setup) and add directly above it, matching its exact `source()` style:

```r
source("etl/cold_reconcile.R")
```

Also add the same `source("etl/cold_reconcile.R")` to `scripts/restore_cold_storage.R` ONLY if it sources `etl/cold_storage.R` (check first; if it doesn't, skip).

- [ ] **Step 2: Replace the scoping block in `export_cold_table`**

In `etl/cold_storage.R`, replace this exact block (currently lines 36–63):

```r
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
```

with:

```r
  # 1. Scope = this run's games PLUS any hot games absent from the parquet.
  # Failed runs leave already-marked games in hot tables; a game already in
  # parquet must not be re-exported off-run (partial hot rows would clobber
  # its valid cold snapshot), but a game entirely MISSING from parquet must
  # be rescued or the closing TRUNCATE destroys it (the games-365-388 bug).
  parquet_path <- file.path(cold_dir, paste0(table_name, ".parquet"))
  parquet_ids <- if (file.exists(parquet_path)) {
    unique(arrow::read_parquet(parquet_path, col_select = "game_id",
                               mmap = FALSE)$game_id)
  } else NULL
  hot_ids <- DBI::dbGetQuery(
    pg,
    sprintf(
      'SELECT DISTINCT t.game_id FROM "%s"."%s" t
       INNER JOIN "%s"."etl_processed_games" eg ON eg.game_id = t.game_id',
      schema, table_name, schema
    )
  )$game_id
  scoped_ids <- cold_export_scope(game_ids, hot_ids, parquet_ids)
  rescued <- setdiff(scoped_ids, as.integer(game_ids))
  if (length(rescued)) {
    log_msg(sprintf(
      "  [COLD] %s: rescuing %d hot game(s) missing from parquet: %s",
      table_name, length(rescued), paste(rescued, collapse = ",")
    ), "WARN")
  }
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
    new_rows <- data.frame()
  }
```

Note the else-branch change: with the new scope logic an empty `scoped_ids`
means "nothing hot needs exporting" (previously NULL meant "export
everything"; that full-export path is now expressed as hot-minus-parquet and
happens automatically). The later `parquet_path` local at old line 71 becomes
a duplicate assignment — delete the old `parquet_path <- ...` line inside
step "2. Merge with existing Parquet" and keep the one added above.

- [ ] **Step 3: Add the coverage gate before TRUNCATE in `run_cold_storage_purge`**

In `run_cold_storage_purge`, directly after the `if (!all(results)) {...}` block (line ~148) and before the `# Phase B` comment, insert:

```r
  # Coverage gate: TRUNCATE is destructive — verify every hot game now has
  # parquet coverage (actions_clean is the canonical table for this check).
  hot_now <- DBI::dbGetQuery(pg, sprintf(
    'SELECT DISTINCT t.game_id FROM "%s"."actions_clean" t
     INNER JOIN "%s"."etl_processed_games" eg ON eg.game_id = t.game_id',
    schema, schema))$game_id
  pq_path <- file.path(cold_dir, "actions_clean.parquet")
  pq_now <- if (file.exists(pq_path)) {
    unique(arrow::read_parquet(pq_path, col_select = "game_id",
                               mmap = FALSE)$game_id)
  } else integer(0)
  gaps <- cold_coverage_gaps(hot_now, pq_now)
  if (length(gaps)) {
    log_msg(sprintf(
      "Phase 7: COVERAGE GATE FAILED — %d hot game(s) not in parquet (%s); skipping TRUNCATE",
      length(gaps), paste(gaps, collapse = ",")
    ), "ERROR")
    return(setNames(rep(FALSE, length(COLD_TABLES)), COLD_TABLES))
  }
```

- [ ] **Step 4: Syntax-check both files**

Run: `"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "invisible(lapply(c('etl/cold_reconcile.R','etl/cold_storage.R'), function(f) {parse(f); cat(f, 'OK\n')}))"`
Expected: both `OK`. Then `git diff --stat` — only the two R files plus the one-line etl_full.R change; no unexplained line-count blowups.

- [ ] **Step 5: Re-run unit tests (regression)**

Run: `"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('etl/tests/test_cold_reconcile.R')"`
Expected: `PASS 8`

- [ ] **Step 6: Commit**

```bash
git add etl/cold_storage.R etl/etl_full.R scripts/restore_cold_storage.R
git commit -m "fix(etl): rescue unexported hot games + coverage gate before cold TRUNCATE

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: Recover the missing games (data operation, not code)

**Files:** none (operational task; uses existing ETL)

**Interfaces:**
- Consumes: missing game_id list from Task 2; the fixed export path from Task 3.
- Produces: parquet coverage for the gap games — precondition for Task 6's backfill to be complete.

- [ ] **Step 1: Un-mark the missing games so ETL re-fetches them**

New-game detection uses `etl_processed_games`; deleting a game's marker makes the next run re-fetch its raw JSON and reprocess it (all base-table writes are per-game upserts, so this is idempotent). Using etl (write) credentials — fill in the ids from Task 2:

```r
# Rscript -e:  (replace <IDS> with the Task-2 list, e.g. 365,366,...,388)
readRenviron('etl/.Renviron')
con <- DBI::dbConnect(RPostgres::Postgres(),
  host=Sys.getenv('PG_HOST'), port=as.integer(Sys.getenv('PG_PORT')),
  dbname=Sys.getenv('PG_DB'), user=Sys.getenv('PG_USER'),
  password=Sys.getenv('PG_PASS'), sslmode=Sys.getenv('PG_SSLMODE'))
n <- DBI::dbExecute(con, 'DELETE FROM basketball_test.etl_processed_games WHERE game_id IN (<IDS>)')
cat('unmarked', n, 'games\n')
DBI::dbDisconnect(con)
```

Write this to a temp .R file and run it (long `-e` segfaults; known pitfall).

- [ ] **Step 2: Run the full ETL**

```bash
"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "Sys.setenv(APP_ENV='test'); source('etl/etl_full.R'); etl_full()"
```

Expected in the log (`etl/logs/`): the unmarked games processed in Phase 2; Phase 7 exports them (possibly via the new `rescuing ... missing from parquet` path); coverage gate passes; TRUNCATE runs.
**Contingency:** if the provider no longer serves those games' JSON (Phase 2 failures for those ids), re-mark them as processed (INSERT back into `etl_processed_games` with their original `game_year`) and accept NULL corner-3 for those games — the spec's fail-open display covers this. Record the outcome in the PR description.

- [ ] **Step 3: Verify coverage is closed**

Run: `& "C:\Program Files\R\R-4.4.2\bin\Rscript.exe" scripts/analysis/cold_gap_diagnostic.R`
Expected: `MISSING: 0` for all 5 tables.

- [ ] **Step 4: Merge the branch**

```bash
git checkout main && git merge etl/cold-export-gap && git push origin main
git branch -d etl/cold-export-gap
```

---

### Task 5: `shot_zones` table migration

**Files:**
- Create: `sql/migrations/2026-07-16_shot_zones.sql`

**Interfaces:**
- Produces: table `basketball_test.shot_zones(game_id int, id int, is_corner3 bool, PRIMARY KEY (game_id, id))` — one row per 3PT shot **with coordinates**; absent row ⇒ corner status unknown (downstream renders "—"). Plans B/C join it on `(game_id, id)`.

- [ ] **Step 1: Create branch**

```bash
git checkout main && git checkout -b sql/shot-zones
```

- [ ] **Step 2: Write the migration**

Create `sql/migrations/2026-07-16_shot_zones.sql`:

```sql
-- shot_zones: persistent per-shot zone flags derived from actions_clean
-- coordinates BEFORE cold-storage purge. actions_clean is truncated after
-- each ETL run, so this table is the only live home for coordinate-derived
-- facts. Rows: 3PT shots with coordinates only (~23k for 2025+2026, ~1-2 MB).
-- Corner rule = committed v2 cut (scripts/analysis/fiba_court_zones.R):
-- corner3_height 2.85 court units = 285 raw provider units.
-- NEVER add this table to etl/cold_storage.R COLD_TABLES.

CREATE TABLE IF NOT EXISTS basketball_test.shot_zones (
  game_id    int  NOT NULL,
  id         int  NOT NULL,
  is_corner3 bool NOT NULL,
  PRIMARY KEY (game_id, id)
);

COMMENT ON TABLE basketball_test.shot_zones IS
  '3PT corner flags from actions_clean coords (y<=285 raw). Populated in ETL Phase 2 + one-time backfill (scripts/backfill_shot_zones.R). Persistent — not cold storage.';
```

- [ ] **Step 3: Apply it (DDL on port 5432)**

Write to a temp .R file and run:

```r
readRenviron('etl/.Renviron')
con <- DBI::dbConnect(RPostgres::Postgres(),
  host=Sys.getenv('PG_HOST'), port=5432L,
  dbname=Sys.getenv('PG_DB'), user=Sys.getenv('PG_USER'),
  password=Sys.getenv('PG_PASS'), sslmode=Sys.getenv('PG_SSLMODE'))
sql <- paste(readLines('sql/migrations/2026-07-16_shot_zones.sql'), collapse = "\n")
DBI::dbExecute(con, sql)
print(DBI::dbGetQuery(con, "SELECT to_regclass('basketball_test.shot_zones') AS t"))
DBI::dbDisconnect(con)
```

Expected: `t = basketball_test.shot_zones`. (Multiple statements in one dbExecute can fail — if it errors, split the CREATE and COMMENT into two `dbExecute` calls.)

- [ ] **Step 4: Apply security (RLS + readonly grant)**

```bash
"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" scripts/apply_db_security.R           # dry-run: review planned statements mention shot_zones
"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" scripts/apply_db_security.R --apply   # check the script's actual apply flag in its header before running
```

(Open the script first — CLAUDE.md says dry-run by default; use whatever apply switch it defines.) Then verify as readonly:

```r
# temp .R with app/.Renviron creds:
print(DBI::dbGetQuery(con, "SELECT count(*) FROM basketball_test.shot_zones"))
```

Expected: `0` (readable, empty). Also run the security contract suite:
`"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('app/tests/testthat/test-db-security-contracts.R')"` — expected PASS (grep the test first; if it enumerates an explicit table allowlist, add `shot_zones` to it in the same commit).

- [ ] **Step 5: Commit**

```bash
git add sql/migrations/2026-07-16_shot_zones.sql
git commit -m "feat(sql): shot_zones persistent corner-3 flag table

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: One-time backfill from parquet

**Files:**
- Create: `scripts/backfill_shot_zones.R`

**Interfaces:**
- Consumes: `exports/cold/actions_clean.parquet` (complete after Task 4), table from Task 5.
- Produces: fully populated `shot_zones` for all historical games. Idempotent (ON CONFLICT DO UPDATE).

- [ ] **Step 1: Write the backfill script**

Create `scripts/backfill_shot_zones.R`:

```r
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
```

- [ ] **Step 2: Run it**

Run (PowerShell, repo root): `& "C:\Program Files\R\R-4.4.2\bin\Rscript.exe" scripts/backfill_shot_zones.R`
Expected: ~23,000+ rows (2025+2026 3PAs, more after Task 4's recovered games), `corner_pct` ≈ **9.3** (the documented v2 corner share of 3s). If corner_pct is ~40%+, the y-units assumption is wrong (probably already ÷100) — STOP and check `SELECT max(parameters_coord_y) FROM` the parquet: raw units max ≈ 1400–1500, court units max ≈ 14–15; fix the 285 threshold accordingly (2.85 for court units) and re-run.

- [ ] **Step 3: Cross-check against the analysis numbers**

```r
# temp .R, readonly creds — corner attempts by season should reconcile with
# the shotgen analysis (2,139 corners total at the pre-recovery snapshot;
# slightly MORE now if Task 4 added games).
dbGetQuery(con, "
  SELECT s.game_year, count(*) FILTER (WHERE z.is_corner3) AS c3_att, count(*) AS fg3_att
  FROM basketball_test.shot_zones z
  JOIN basketball_test.schedule s USING (game_id)
  GROUP BY 1 ORDER BY 1")
```

Expected: totals ≥ the analysis-era counts, corner ≈ 9–10% of 3s each season.

- [ ] **Step 4: Commit**

```bash
git add scripts/backfill_shot_zones.R
git commit -m "feat(sql): one-time shot_zones backfill from cold parquet

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 7: Incremental population in ETL Phase 2

**Files:**
- Modify: `etl/etl_full.R` (after the player-identity block that follows Phase 2, i.e. after line ~822 `})` of the identity `tryCatch`, inside the same `if (length(processed_ids))` region — see Step 1 for the exact anchor)

**Interfaces:**
- Consumes: `processed_ids` (this run's committed game ids), populated `actions_clean` (hot at this point; purge is Phase 7), table from Task 5.
- Produces: `shot_zones` rows for every new game, available before Phase 4 MV refresh (Plan B's MVs join this table).

- [ ] **Step 1: Add the upsert block**

In `etl/etl_full.R`, locate this exact existing block (the player-identity refresh that runs right after Phase 2):

```r
  if (length(processed_ids)) {
    tryCatch({
      identity_counts <- sync_player_identity_dictionary(pg, SCHEMA)
```

and insert AFTER that whole `tryCatch({...}, error = ...)` closes (after the line `  }` that ends this `if` — i.e. as a new sibling `if` block directly beneath it, before the `# Guardrail: verify key tables have rows` comment):

```r
  # Shot zones: persist coordinate-derived corner-3 flags for this run's
  # games BEFORE Phase 7 purges actions_clean. Corner rule = v2 cut
  # (y <= 285 raw units = 2.85 court units). 3PT shots with coords only.
  if (length(processed_ids)) {
    tryCatch({
      ids_csv <- paste(sort(unique(as.integer(processed_ids))), collapse = ",")
      zn <- DBI::dbExecute(pg, sprintf(
        'INSERT INTO "%s"."shot_zones" (game_id, id, is_corner3)
         SELECT game_id, id, (parameters_coord_y::numeric <= 285)
         FROM "%s"."actions_clean"
         WHERE type = \'shot\' AND parameters_points = 3
           AND parameters_coord_y IS NOT NULL
           AND game_id IN (%s)
         ON CONFLICT (game_id, id) DO UPDATE SET is_corner3 = EXCLUDED.is_corner3',
        SCHEMA, SCHEMA, ids_csv
      ))
      log_msg(sprintf("  shot_zones: %d 3PT rows upserted for %d game(s)",
                      zn, length(processed_ids)))
    }, error = function(e) {
      log_msg(sprintf("shot_zones upsert FAILED: %s", conditionMessage(e)), "ERROR")
      mark_phase_failed("Shot zones", conditionMessage(e))
    })
  }
```

Before writing, check the actual column type of `actions_clean.parameters_points` (`\d` or a quick query) — if it's text, use `parameters_points = '3'`; mirror however `parameters_points` is compared elsewhere in the codebase's SQL (see `onoff_mv.sql`, which uses numeric `= 3`).

- [ ] **Step 2: Syntax-check**

Run: `"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "parse('etl/etl_full.R'); cat('OK\n')"`
Expected: `OK`. `git diff --stat etl/etl_full.R` shows only ~+25 lines.

- [ ] **Step 3: Live verification (one already-processed game, no ETL run needed)**

`actions_clean` is empty between runs, so verify the SQL shape against the backfilled table instead: pick one game_id from Task 2's recovered list, run the SELECT part of the upsert with `actions_clean` swapped for a parquet-loaded temp table? Simpler: trust the backfill (identical predicate, Task 6 Step 2/3 verified it) and verify the hook on the **next scheduled ETL run**: after it fires, check the log for `shot_zones: N 3PT rows upserted` and run:

```sql
-- readonly: every processed game with 3PAs since the hook merged has zone rows
SELECT count(*) FROM basketball_test.etl_processed_games eg
WHERE eg.processed_at > '<merge date>'
  AND NOT EXISTS (SELECT 1 FROM basketball_test.shot_zones z WHERE z.game_id = eg.game_id);
```

Expected: 0 (off-season note: if no new games arrive, this stays unverified until the season starts — the backfill keeps the table complete meanwhile).

- [ ] **Step 4: Update docs + merge**

Add to `CLAUDE.md` under Cold Storage ("Tracking" paragraph area): one line —

```
**shot_zones:** persistent per-shot corner-3 flags (3PT + coords only), written at end of Phase 2 from actions_clean, backfilled from parquet (`scripts/backfill_shot_zones.R`). NOT a cold table — never truncate.
```

```bash
git add etl/etl_full.R CLAUDE.md
git commit -m "feat(etl): incremental shot_zones upsert at end of Phase 2

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git checkout main && git merge sql/shot-zones && git push origin main
git branch -d sql/shot-zones
```

---

## Completion criteria (Plan A)

1. `cold_gap_diagnostic.R` reports MISSING: 0 for all five tables.
2. `shot_zones` populated (~23k+ rows, corner ≈ 9.3% of 3s), readable by `app_readonly`, security contract tests pass.
3. Unit tests green; ETL run completes end-to-end with the coverage gate passing.
4. Plans B (MV/function columns) and C (Shiny UI) unblocked — they join `shot_zones (game_id, id)`.
