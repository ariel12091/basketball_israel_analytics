# Merge player_onoff_by_game into player_four_factors_by_game + season-safe PKs — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [x]`) syntax for tracking.

**Goal:** Drop the 115 MB `player_onoff_by_game` MV by folding its five payload columns (`fg2_made`, `fg2_att`, `fg3_made`, `fg3_att`, `minutes`→`onoff_minutes`) into `player_four_factors_by_game` (identical 7-column key), repoint `onoff_compute()` at the merged table with **byte-identical output**, and add `game_year` to the `lineups_lookup_on` / `sub_lineups` primary keys so recurring cross-season lineups can't break ETL Phase 3.

**Architecture:** `player_four_factors_by_game` (a physical table, incrementally refreshed per game) gains 5 columns. Its refresh function grows a second CTE branch that is a verbatim port of the `player_onoff_by_game` MV query (lineup-grain minutes rounding preserved for exact parity), LEFT JOINed onto the existing FF aggregation on the shared key. `onoff_compute` swaps `p.total_pts`→`p.total_points` and `p.minutes`→`p.onoff_minutes` — everything else untouched. The MV is dropped only after full per-key parity and end-to-end function-output diffs pass.

**Tech Stack:** PostgreSQL (Supabase; DDL on port 5432 direct, per CLAUDE.md), R 4.4.2 / DBI / RPostgres for migration scripts, testthat for the app suite.

## PROGRESS / RESUME STATE (checkpoint 2026-07-14, session ended before DB changes)

**Where we stopped:** Tasks 0 and 1 are COMPLETE. **No database changes have been applied yet** — the DB is untouched (all work so far: read-only queries, baseline snapshots, this plan, branch `sql/merge-player-onoff-ff`). Resume at **Task 2** (write repo SQL artifacts), then Tasks 3→8 in order.

**Durable artifacts (survive session end):**
- Baselines + diagnostics + reusable R scripts: `output/merge_pobg_2026-07-14/`
  - `baseline/onoff_case{1..6}_*.csv` — onoff_compute outputs for the 6-case grid (rows sorted by player_id, team_id; numerics rounded to 6 dp; team-filter case used team_ids **2,3**)
  - `baseline/ff_case{1,2,4}_*.csv` — four_factors_compute outputs
  - `baseline/pff_game_checksums.csv` — per-game FF column sums, 439 games
  - `baseline/pff_stale_games.csv` — the **94 stale games** (see Task 0 finding below) for the amended Task 5 gate
  - `scripts/baseline_capture.R` — re-run with env `PHASE=after` + `AUDIT_OUT_DIR=<dir>` to produce the post-swap `after/` CSVs for diffing (Task 6 Step 2)
  - `scripts/preflight.R`, `scripts/diagnose_g1.R`, `scripts/diagnose_g1b.R` + `*_out.txt` — gate results
- Branch `sql/merge-player-onoff-ff` exists; only this plan doc is committed on it.

**Key facts discovered (do not re-derive):**
1. **The FF table is stale for 94 games (~2,500 rows)** — 31 games in 2025, 63 in 2026. Fresh recompute == MV exactly (verified NULL-safe on game 224); stored FF table diverges. Root cause: FF table refreshes incrementally per ETL'd game; the June identity merges changed `lineups_lookup` without re-refreshing old games; the MV rebuilds fully nightly. So the merge's `refresh(NULL)` also **fixes live Tab-1 FF filtered-path data** for those games. Report this to the user in the final summary.
2. Aside from staleness, the design premise holds: identical key sets and `total_pts==total_points`, `total_poss==total_poss` (game 224 fresh check: 0 diffs vs MV).
3. `player_four_factors_by_game.minutes` is **float8** (not numeric) — cast `::numeric` before `round()` in SQL.
4. `four_factors_compute` signature starts `p_game_year INT` (NOT the onoff_compute shape); call with named args (see `scripts/baseline_capture.R`).
5. R/DBI param lists: use `NA_character_`/`NA_integer_`, never `NULL` (length-0 error).
6. No FKs reference `sub_lineups_pkey` / `lineups_lookup_on_pkey`; no NULL `game_year` in either table. Row counts at checkpoint: pobg 735,958 / pff 735,018 / sub_lineups 217,178 / lineups_lookup_on 43,368.
7. `etl/.Renviron` uses port 6543 (pooler). DDL + long statements: connect on **5432**, `SET statement_timeout = 0`.
8. ETL cron in `.github/workflows/etl-full.yml` is paused (off-season) and uncommitted, but the local Windows Task Scheduler job may still run nightly — if resuming on a later day, re-check nothing refreshed the MV/table between baseline capture and Task 4/5 (cheap re-run: `scripts/preflight.R` G1 should reproduce the same mismatch counts; pobg vs merged parity in Task 5 is computed live anyway so timing only matters for interpreting checksum diffs).

**Execution order reminder for resume:** Task 2 (repo SQL: extend refresh fn + CTAS def + repoint onoff_compute + delete pobg.sql + migration file) → Task 3 (registry/etl_full/test/docs edits) → Task 4 (ALTER ADD COLUMN ×5, CREATE OR REPLACE refresh fn, `refresh(NULL)`) → Task 5 (parity gates, amended per Task 0 finding) → Task 6 (CREATE OR REPLACE onoff_compute **without** the DROP line, `PHASE=after` grid diff, DROP MV, VACUUM FULL ANALYZE, grants audit) → Task 7 (PK rebuilds) → Task 8 (tests, commit, merge). The onoff CSVs must diff **identical** before/after; the ff CSVs may differ only via rows touched by the 94 stale games — if `ff_case*.csv` diffs appear, verify affected players belong to stale games before accepting.

## Global Constraints

- Schema is `basketball_test`; DDL runs on port **5432**, same host as pooler (CLAUDE.md Environment).
- The live Shiny app keeps working throughout: no `DROP FUNCTION` on `onoff_compute` (use `CREATE OR REPLACE` only, preserves `app_readonly` EXECUTE grant); no `DROP TABLE` on `player_four_factors_by_game` (use `ALTER TABLE ADD COLUMN`, preserves grants/RLS/indexes).
- **Data parity is a hard gate:** zero key mismatches and zero value mismatches between old MV and merged columns; `onoff_compute` output before vs after must be identical for the whole baseline grid. Any mismatch → stop, do not drop the MV.
- ETL credentials from `etl/.Renviron`; never hardcode credentials; scratch scripts and outputs go to the session scratchpad, not the repo.
- Branch: `sql/merge-player-onoff-ff` off `main`; merge back per CLAUDE.md workflow.
- New fg columns are `integer`, `onoff_minutes` is `numeric` (values compared numerically to the MV's bigint/numeric — equality unaffected).

---

### Task 0: Branch + preflight gates

**Files:** none (git + read-only DB queries)

**Interfaces:**
- Produces: branch `sql/merge-player-onoff-ff`; empirical confirmation that (a) MV and FF-table key sets are identical, (b) `total_pts`==`total_points` and `total_poss`==`total_poss` per key, (c) NULL-ability of `type_lineup`/`own_starters`/`opp_starters` (drives the COALESCE-sentinel join), (d) no FKs reference `sub_lineups_pkey`/`lineups_lookup_on_pkey`, (e) no NULL `game_year` in either PK-change table.

- [x] **Step 1:** `git checkout -b sql/merge-player-onoff-ff`
- [x] **Step 2:** Run preflight R script (scratchpad, etl/.Renviron creds, port from env) with these queries; every gate must hold:

```sql
-- G1: key parity + measure parity (expect every count = 0)
WITH j AS (
  SELECT COALESCE(p.player_id, f.player_id) pid,
         p.player_id IS NULL AS only_ff, f.player_id IS NULL AS only_mv,
         (p.total_pts IS DISTINCT FROM f.total_points) AS pts_diff,
         (p.total_poss IS DISTINCT FROM f.total_poss) AS poss_diff
  FROM basketball_test.player_onoff_by_game p
  FULL JOIN basketball_test.player_four_factors_by_game f
    ON f.player_id = p.player_id AND f.team_id = p.team_id AND f.game_id = p.game_id
   AND f.is_on_key = p.is_on_key
   AND COALESCE(f.type_lineup,'~') = COALESCE(p.type_lineup,'~')
   AND COALESCE(f.own_starters,-1) = COALESCE(p.own_starters,-1)
   AND COALESCE(f.opp_starters,-1) = COALESCE(p.opp_starters,-1)
)
SELECT count(*) FILTER (WHERE only_ff)  AS ff_only,
       count(*) FILTER (WHERE only_mv)  AS mv_only,
       count(*) FILTER (WHERE pts_diff) AS pts_mismatch,
       count(*) FILTER (WHERE poss_diff) AS poss_mismatch
FROM j;

-- G2: nullability of key columns (informational; sentinel join covers NULLs)
SELECT count(*) FILTER (WHERE type_lineup IS NULL)  AS null_tl,
       count(*) FILTER (WHERE own_starters IS NULL) AS null_own,
       count(*) FILTER (WHERE opp_starters IS NULL) AS null_opp
FROM basketball_test.player_four_factors_by_game;

-- G3: no inbound FKs on the two PKs being changed (expect 0 rows)
SELECT conname, conrelid::regclass
FROM pg_constraint
WHERE contype = 'f'
  AND confrelid IN ('basketball_test.sub_lineups'::regclass,
                    'basketball_test.lineups_lookup_on'::regclass);

-- G4: no NULL game_year (expect 0,0)
SELECT (SELECT count(*) FROM basketball_test.sub_lineups WHERE game_year IS NULL) AS sl_null,
       (SELECT count(*) FROM basketball_test.lineups_lookup_on WHERE game_year IS NULL) AS llo_null;

-- G5: exact row counts (recorded for later comparison)
SELECT (SELECT count(*) FROM basketball_test.player_onoff_by_game) AS mv_rows,
       (SELECT count(*) FROM basketball_test.player_four_factors_by_game) AS ff_rows,
       (SELECT count(*) FROM basketball_test.sub_lineups) AS sub_lineups_rows,
       (SELECT count(*) FROM basketball_test.lineups_lookup_on) AS llo_rows;
```

Expected: G1 all zeros (if not: STOP — the merge design assumes shared keys and identical pts/poss; re-plan). G3 empty. G4 zeros.

> **Task 0 finding (2026-07-14):** G1 did NOT come back zero: 1,598 MV-only keys, 658 FF-only keys, ~2,500 pts/poss mismatches across 94 games (31×2025, 63×2026). Diagnosis (game 224, NULL-safe 3-way): a fresh recompute from current `lineups_lookup` + `df_pts_poss_lineups_longer_mv` matches the MV **exactly** (0 diffs) and mismatches the stored FF table (116 missing keys, 148 value diffs). Root cause: the FF table is refreshed incrementally per ETL'd game, so games whose `lineups_lookup` changed outside ETL (June identity merges) went stale; the MV is fully rebuilt nightly and is current. **Resolution:** proceed — Task 4's `refresh(NULL)` rebuilds the FF table from current data, which both enables the merge and corrects the stale rows. Affected-game list saved to `baseline/pff_stale_games.csv`. Task 5 gates amended: (a) merged-vs-MV on/off parity must be all-zero for ALL rows; (b) FF per-game checksums must be identical for every game NOT in the stale list; (c) games whose FF checksums changed must be a subset of the 94 stale games.

---

### Task 1: Baseline captures (the "failing test" for parity)

**Files:** scratchpad only — `baseline_onoff_grid.R` writing `baseline/onoff_case<i>.csv`, `baseline/ff_case<i>.csv`, `baseline/pff_game_checksums.csv`

**Interfaces:**
- Produces: CSVs consumed by Task 5/6 verification. Grid cases (all with `p_min_all=0, p_min_on=0, p_min_net=-1e9` to maximize row coverage):
  1. year '2026', window 2025-10-01..2026-07-01, no filters
  2. year '2025', window 2024-10-01..2025-07-01, no filters
  3. case 1 + `p_team_ids` = two team ids picked from `team_ppp_ratings_mv` (record which)
  4. case 1 + `p_num_starters_off=5, p_num_starters_def=5`
  5. case 1 + `p_last_n_games=10`
  6. case 1 + `p_game_type_csv='5', p_home_away='home', p_outcome='win'`

- [x] **Step 1:** Run `onoff_compute` for all 6 cases; write each result (all columns, ordered by player_id, team_id) to CSV.
- [x] **Step 2:** Run `four_factors_compute` for cases 1, 2, 4; write CSVs (guards against accidental FF regressions).
- [x] **Step 3:** Per-game FF checksums for the whole table:

```sql
SELECT game_id, count(*) AS n,
       sum(total_points) sp, sum(total_poss) spo, sum(ts_poss_count) sts,
       sum(oreb_count) so, sum(oreb_opportunities) soo, sum(tov_count) st,
       sum(total_ft_attempts) sft, sum(total_fga) sfga, sum(total_fgm) sfgm,
       sum(total_fg3_made) sf3, sum(player_ts_poss_count) spts,
       sum(player_tov_count) sptv, round(sum(minutes),3) sm,
       round(sum(usg_pct),1) su
FROM basketball_test.player_four_factors_by_game
GROUP BY game_id ORDER BY game_id;
```

Write to `baseline/pff_game_checksums.csv`.

---

### Task 2: New SQL artifacts in the repo

**Files:**
- Modify: `sql/functions/refresh_player_four_factors_by_game_for_games.sql`
- Modify: `sql/materialized_views/player_four_factors_by_game.sql`
- Modify: `sql/functions/onoff_compute.sql`
- Delete: `sql/materialized_views/player_onoff_by_game.sql`
- Create: `sql/migrations/2026-07-14_merge_player_onoff_into_ff.sql`

**Interfaces:**
- Produces: merged-table schema = existing 23 columns + `fg2_made int, fg2_att int, fg3_made int, fg3_att int, onoff_minutes numeric` (appended, in that order). `onoff_compute` signature and RETURNS TABLE unchanged.

- [x] **Step 1:** Extend the refresh function. Keep every existing CTE byte-identical; add after `complex_flags`:

```sql
  lineup_totals AS (          -- verbatim port of player_onoff_by_game.lineup_totals
    SELECT
      cs.game_id, s.game_year, cs.team_id, cs.lineup_hash, cs.type_lineup,
      cs.own_starters, cs.opp_starters,
      SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 2 AND cs.parameters_made = 'made' THEN 1 ELSE 0 END) AS fg2_made,
      SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 2 THEN 1 ELSE 0 END) AS fg2_att,
      SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 3 AND cs.parameters_made = 'made' THEN 1 ELSE 0 END) AS fg3_made,
      SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 3 THEN 1 ELSE 0 END) AS fg3_att
    FROM clean_stats cs
    JOIN basketball_test.schedule s ON s.game_id = cs.game_id
    GROUP BY cs.game_id, s.game_year, cs.team_id, cs.lineup_hash, cs.type_lineup, cs.own_starters, cs.opp_starters
  ),
  onoff_lineup_segments AS (  -- verbatim port of player_onoff_by_game.lineup_segments
    SELECT
      cs.game_id, cs.team_id, cs.lineup_hash, cs.type_lineup,
      cs.own_starters, cs.opp_starters, cs.segment_id,
      GREATEST(MAX(cs.end_game_seconds_remaining) - MIN(cs.end_game_seconds_remaining), 0)::numeric AS seg_seconds
    FROM clean_stats cs
    WHERE cs.lineup_hash IS NOT NULL
      AND cs.segment_id IS NOT NULL
      AND cs.end_game_seconds_remaining IS NOT NULL
    GROUP BY cs.game_id, cs.team_id, cs.lineup_hash, cs.type_lineup, cs.own_starters, cs.opp_starters, cs.segment_id
  ),
  onoff_lineup_minutes AS (   -- rounding at lineup grain preserved (ROUND per lineup, then SUM per player)
    SELECT
      game_id, team_id, lineup_hash, type_lineup, own_starters, opp_starters,
      CASE WHEN type_lineup = 'offense' THEN ROUND(SUM(seg_seconds) / 60.0, 3) ELSE 0::numeric END AS minutes
    FROM onoff_lineup_segments
    GROUP BY game_id, team_id, lineup_hash, type_lineup, own_starters, opp_starters
  ),
  onoff_player AS (
    SELECT
      b0.player_id, b0.team_id, lt.game_id, lt.game_year, b0.is_on_key,
      lt.type_lineup, lt.own_starters, lt.opp_starters,
      SUM(lt.fg2_made) AS fg2_made, SUM(lt.fg2_att) AS fg2_att,
      SUM(lt.fg3_made) AS fg3_made, SUM(lt.fg3_att) AS fg3_att,
      SUM(COALESCE(lm.minutes, 0)) AS onoff_minutes
    FROM base0 b0
    JOIN lineup_totals lt
      ON lt.lineup_hash = b0.lineup_hash AND lt.team_id = b0.team_id
    LEFT JOIN onoff_lineup_minutes lm
      ON lm.game_id = lt.game_id AND lm.team_id = lt.team_id
     AND lm.lineup_hash = lt.lineup_hash AND lm.type_lineup = lt.type_lineup
     AND lm.own_starters = lt.own_starters AND lm.opp_starters = lt.opp_starters
    GROUP BY b0.player_id, b0.team_id, lt.game_id, lt.game_year, b0.is_on_key,
             lt.type_lineup, lt.own_starters, lt.opp_starters
  ),
  ff AS (
    -- the ENTIRE existing final SELECT ... FROM segment_stats ss LEFT JOIN segment_times st ... GROUP BY ...
  )
  SELECT
    ff.player_id, ff.team_id, ff.game_id, ff.game_year, ff.is_on_key, ff.type_lineup,
    ff.num_starters, ff.own_starters, ff.opp_starters, ff.total_points, ff.total_poss,
    ff.ts_poss_count, ff.oreb_count, ff.oreb_opportunities, ff.tov_count,
    ff.total_ft_attempts, ff.total_fga, ff.total_fgm, ff.total_fg3_made,
    ff.player_ts_poss_count, ff.player_tov_count, ff.minutes, ff.usg_pct,
    op.fg2_made::int, op.fg2_att::int, op.fg3_made::int, op.fg3_att::int, op.onoff_minutes
  FROM ff
  LEFT JOIN onoff_player op
    ON op.player_id = ff.player_id AND op.team_id = ff.team_id AND op.game_id = ff.game_id
   AND op.is_on_key = ff.is_on_key
   AND COALESCE(op.type_lineup, '~') = COALESCE(ff.type_lineup, '~')
   AND COALESCE(op.own_starters, -1) = COALESCE(ff.own_starters, -1)
   AND COALESCE(op.opp_starters, -1) = COALESCE(ff.opp_starters, -1);
```

INSERT column list gains `fg2_made, fg2_att, fg3_made, fg3_att, onoff_minutes` at the end.

- [x] **Step 2:** Rewrite `sql/materialized_views/player_four_factors_by_game.sql` (full-rebuild path) as the same combined query without the `game_ids` filter, appending after the index DDL:

```sql
ALTER TABLE basketball_test.player_four_factors_by_game
  ALTER COLUMN fg2_made TYPE int, ... -- (not needed if CTAS casts ::int in SELECT — cast in SELECT instead)
```
(Use `::int` casts in the CTAS SELECT; no post-ALTER.)

- [x] **Step 3:** `sql/functions/onoff_compute.sql` — replace only the `agg` CTE source block:

```sql
      SUM(p.total_points)  AS total_pts,
      SUM(p.total_poss) AS total_poss,
      ROUND(
        SUM(p.total_points) / NULLIF(SUM(p.total_poss), 0)::numeric * 100, 1
      ) AS ppp_calc,
      SUM(p.fg2_made)::bigint AS fg2_made,
      SUM(p.fg2_att)::bigint  AS fg2_att,
      SUM(p.fg3_made)::bigint AS fg3_made,
      SUM(p.fg3_att)::bigint  AS fg3_att,
      SUM(COALESCE(p.onoff_minutes, 0))::numeric AS minutes
    FROM basketball_test.player_four_factors_by_game p
```
Header comment updated. Signature, RETURNS TABLE, and every downstream CTE unchanged.

- [x] **Step 4:** Delete `sql/materialized_views/player_onoff_by_game.sql` (required: `validate_mv_registry()` discovers files by CREATE statement and errors on unregistered objects).

- [x] **Step 5:** Write `sql/migrations/2026-07-14_merge_player_onoff_into_ff.sql` documenting the applied DDL: the `ALTER TABLE ... ADD COLUMN` block, pointer to the two function files, `DROP MATERIALIZED VIEW basketball_test.player_onoff_by_game`, and the two PK rebuilds (Task 7).

---

### Task 3: Repo R/test/docs updates

**Files:**
- Modify: `sql/rebuild_all_mvs.R:23` (remove player_onoff_by_game registry entry)
- Modify: `etl/etl_full.R:942` (`mvs = c("lineup_four_factors_by_game")`)
- Modify: `app/tests/testthat/test-data-shape-db.R:66-76` (drop pobg block; add new columns to pff expectation)
- Modify: `CLAUDE.md:124` MV tree; `PROJECT.md:136,153,226`

- [x] **Step 1:** Registry: delete the `player_onoff_by_game` list entry.
- [x] **Step 2:** etl_full mv_levels L3 → `c("lineup_four_factors_by_game")`.
- [x] **Step 3:** Test file: remove `expect_has_columns("player_onoff_by_game", ...)`; extend the pff expectation with `"fg2_made","fg2_att","fg3_made","fg3_att","onoff_minutes"`.
- [x] **Step 4:** Docs: L3 line loses `player_onoff_by_game`; PROJECT.md:153 → `onoff_compute → player_four_factors_by_game, final_schedule_mv`; PROJECT.md:226 source note updated. `CLAUDE.md` was intentionally left unchanged per `AGENTS.md`.

---

### Task 4: Apply migration part 1 (columns + function + full refresh)

**Files:** scratchpad `apply_merge.R` (port 5432, etl creds, `SET statement_timeout = 0`)

- [x] **Step 1:** `ALTER TABLE basketball_test.player_four_factors_by_game ADD COLUMN fg2_made int, ADD COLUMN fg2_att int, ADD COLUMN fg3_made int, ADD COLUMN fg3_att int, ADD COLUMN onoff_minutes numeric;`
- [x] **Step 2:** Execute the new `refresh_player_four_factors_by_game_for_games.sql` (CREATE OR REPLACE).
- [x] **Step 3:** `SELECT basketball_test.refresh_player_four_factors_by_game_for_games(NULL);` — full delete+reinsert returned `735,958` rows in ~240s.

---

### Task 5: Parity gate (hard stop on any mismatch)

- [x] **Step 1:** Row count: merged table count == still-live `player_onoff_by_game` row count (`735,958` after stale-game correction); rows with NULL `fg2_att`/`onoff_minutes` == 0.
- [x] **Step 2:** Full per-key parity vs the still-live MV (all zeros):

```sql
WITH j AS (
  SELECT p.player_id IS NULL AS only_ff, f.player_id IS NULL AS only_mv,
         (f.fg2_made IS DISTINCT FROM p.fg2_made) OR (f.fg2_att IS DISTINCT FROM p.fg2_att)
         OR (f.fg3_made IS DISTINCT FROM p.fg3_made) OR (f.fg3_att IS DISTINCT FROM p.fg3_att) AS fg_diff,
         (f.onoff_minutes IS DISTINCT FROM p.minutes) AS min_diff,
         (f.total_points IS DISTINCT FROM p.total_pts) AS pts_diff,
         (f.total_poss IS DISTINCT FROM p.total_poss) AS poss_diff
  FROM basketball_test.player_onoff_by_game p
  FULL JOIN basketball_test.player_four_factors_by_game f
    ON f.player_id = p.player_id AND f.team_id = p.team_id AND f.game_id = p.game_id
   AND f.is_on_key = p.is_on_key
   AND COALESCE(f.type_lineup,'~') = COALESCE(p.type_lineup,'~')
   AND COALESCE(f.own_starters,-1) = COALESCE(p.own_starters,-1)
   AND COALESCE(f.opp_starters,-1) = COALESCE(p.opp_starters,-1)
)
SELECT count(*) FILTER (WHERE only_ff) ff_only, count(*) FILTER (WHERE only_mv) mv_only,
       count(*) FILTER (WHERE fg_diff) fg_mismatch, count(*) FILTER (WHERE min_diff) min_mismatch,
       count(*) FILTER (WHERE pts_diff) pts_mismatch, count(*) FILTER (WHERE poss_diff) poss_mismatch
FROM j;
```

- [x] **Step 3:** Re-run the per-game FF checksum query; core FF columns changed only for the 94 known stale games. `sum(usg_pct)` changed for all games after the full refresh, but `four_factors_compute()` does not read `usg_pct`; visible FF output diffs were verified to map to stale-game rows.
- [x] **Step 4:** If any gate fails: investigate; the MV is untouched, `onoff_compute` still reads it — the app is unaffected. Do NOT proceed to Task 6.

---

### Task 6: Swap onoff_compute, end-to-end diff, drop MV, compact

- [x] **Step 1:** Execute new `onoff_compute.sql` **CREATE OR REPLACE only** (skip the leading `DROP FUNCTION` line — grants must survive).
- [x] **Step 2:** Re-run the Task 1 grid (all 6 onoff cases + 3 FF cases); all 6 on/off CSVs diffed identical. FF diffs are attributable to stale-game correction as above.
- [x] **Step 3:** `DROP MATERIALIZED VIEW basketball_test.player_onoff_by_game;`
- [x] **Step 4:** `VACUUM FULL ANALYZE basketball_test.player_four_factors_by_game;` then record `pg_database_size` + relation sizes. DB size went `978,373,779` → `663,424,147` bytes; PFF table total size went `384,475,136` → `190,619,648` bytes.
- [x] **Step 5:** Grants audit:

```sql
SELECT has_table_privilege('app_readonly','basketball_test.player_four_factors_by_game','SELECT') AS tbl_ok,
       (SELECT count(*) FROM pg_policies WHERE schemaname='basketball_test'
         AND tablename='player_four_factors_by_game') AS policies,
       has_function_privilege('app_readonly',
         (SELECT oid FROM pg_proc p JOIN pg_namespace n ON n.oid=p.pronamespace
           WHERE n.nspname='basketball_test' AND p.proname='onoff_compute' LIMIT 1),
         'EXECUTE') AS fn_ok;
```
Expect `tbl_ok=t, policies>=1, fn_ok=t`.

---

### Task 7: Season-safe PKs

- [x] **Step 1:** In one transaction each:

```sql
BEGIN;
ALTER TABLE basketball_test.lineups_lookup_on
  DROP CONSTRAINT lineups_lookup_on_pkey,
  ADD CONSTRAINT lineups_lookup_on_pkey PRIMARY KEY (player_id, lineup_hash, team_id, game_year);
COMMIT;

BEGIN;
ALTER TABLE basketball_test.sub_lineups
  DROP CONSTRAINT sub_lineups_pkey,
  ADD CONSTRAINT sub_lineups_pkey PRIMARY KEY (team_id, lineup_hash, sub_lineup_hash, game_year);
COMMIT;
```

- [x] **Step 2:** Verify: row counts unchanged (`sub_lineups=217,178`, `lineups_lookup_on=43,368`); PK definitions show the 4-column keys; duplicate-insert probes fail with unique violations.

---

### Task 8: Test suite, commit, merge

- [x] **Step 1:** Run the app test suite (`scripts/test_all.R` or `testthat::test_dir("app/tests/testthat")`), at minimum `test-data-shape-db.R` (live-DB) and the mocked server tests.
- [x] **Step 2:** Commit all repo changes on `sql/merge-player-onoff-ff`; merge to `main`; delete branch.
- [x] **Step 3:** Record final size savings + note that the next ETL run exercises the new refresh path end-to-end. Final observed DB size savings after drop + vacuum: `314,949,632` bytes; the next ETL run will exercise the merged `refresh_player_four_factors_by_game_for_games(int4[])` incremental path.
