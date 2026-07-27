# Starters Fast Path (Tab 2) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make starters-vs-starters queries on Tab 2 (Summary + Four Factors) run on pre-aggregated MVs (~0.3–0.4s) instead of the raw action scan (0.75–2.4s), by adding `opp_starters` as a key to the two lineup MVs and rewiring the function gates.

**Architecture:** `own_starters` is already each MV's `num_starters` column (functionally determined by the lineup), so only `opp_starters` is added as a grouping key (measured row growth 1.64×, ~+10–15MB). Minutes use the **gap-exclusive** model (user decision 2026-07-27): per contiguous opponent-window inside a segment (gaps-and-islands), summed per `opp_starters`; poss/pts/shooting counts are exact per action row and unaffected by windowing. Function gates split "clutch" (margins/time → raw path, unchanged) from "starters-only" (→ pre-agg path with uniform own/opp filters). The old per-type-lineup starters mapping in the non-clutch branches is wrong (it maps def params to defense-row `num_starters`, which is the OWN count) and is deleted.

**Tech Stack:** PostgreSQL 15+ (Supabase), plpgsql, R harness from `scripts/perf_tuning_baseline.R` pattern.

## Global Constraints

- SQL edits: exact-string Edit operations only; `git diff --stat` plausibility check after each file.
- **Output contract:** poss/pts/shooting columns must be byte-identical to the current raw path for starters queries, and ALL non-starters outputs (15-case harness) must stay byte-identical. **Minutes on starters-filtered queries are the accepted exception** (gap-exclusive model; exact-match filters differ only when an opponent count recurs non-contiguously in one segment). Minutes on NON-starters queries must remain identical.
- MV changes require DROP+CREATE (REFRESH re-runs the stored definition). `lineup_four_factors_by_game` is L3 with L4 dependents — rebuild via `sql/rebuild_all_mvs.R` order. `mv_lineup_totals_by_day` is L2.
- `df_pts_poss_lineups_longer_mv` (source, 250MB) is persistent — no cold-storage restore needed. `schedule` is live.
- Functions: signatures must NOT change. After DROP+CREATE of functions re-run `scripts/apply_db_security.R` (CONFIRM=1).
- Semantics rule (validated against the clutch path): `p_num_starters_off*` filters the reporting lineup's OWN starter count on ALL rows (both type_lineups); `p_num_starters_def*` filters OPP starter count on ALL rows. Uniform — never per-type mapping.
- NULL `opp_starters` rows: raw path excludes them whenever an opp filter is active (SQL NULL comparison) — the MV path must preserve that (keep NULL, never COALESCE to a sentinel; a sentinel like -1 would wrongly match `<=` max filters).
- Branch: `sql/starters-fast-path`. Commit per task.
- Out of scope: Tab 3 (measured fine: 0.5–0.6s with starters), clutch+starters combinations (stay on raw path), `sub_lineups_stats` fast path (stays starters-free), the 851MB DB-size drift.

---

### Task 1: Branch, recon checks, extended baseline

**Files:**
- Create: `scripts/starters_baseline.R`
- Read-only recon: `sql/functions/refresh_player_four_factors_by_game_for_games.sql`, `sql/materialized_views/team_four_factors_mv.sql`, `etl/` phase-4 refresh code

**Interfaces:**
- Produces: `starters_baseline.R` runnable as `Rscript scripts/starters_baseline.R <outdir> <label>` — captures the starters cases (below) AS WELL AS re-using the 15 perf cases via `perf_tuning_baseline.R`. Task 7 diffs against it.

- [ ] **Step 1: Branch**

```bash
git checkout -b sql/starters-fast-path
```

- [ ] **Step 2: Recon A — pff minutes method.** Read `refresh_player_four_factors_by_game_for_games.sql` and `sql/materialized_views/player_four_factors_by_game.sql`; note how `onoff_minutes` is attributed across `(own_starters, opp_starters)` keys. If pff already implements a window/island method, mirror its expressions verbatim in Tasks 2–3 for cross-tab consistency; if pff duplicates segment minutes across starter splits (approximation), do NOT copy that — this plan's island method is the standard going forward, and record the pff discrepancy as a finding.

- [ ] **Step 3: Recon B — L4 dependency.** Confirm `team_four_factors_mv` reads `lineup_four_factors_by_game` and that its GROUP BY is team/year-level (so the added `opp_starters` key only adds rows that re-sum to identical totals). Run: `grep -n "lineup_four_factors_by_game\|GROUP BY" sql/materialized_views/team_four_factors_mv.sql`. Expected: source confirmed, GROUP BY has no `num_starters`. If it DOES key by starters, STOP and reassess.

- [ ] **Step 4: Recon C — ETL refresh mechanism.** `grep -rn "mv_lineup_totals_by_day\|lineup_four_factors_by_game" etl/ sql/rebuild_all_mvs.R` — confirm both are refreshed via plain `REFRESH MATERIALIZED VIEW` (with or without CONCURRENTLY) and note whether CONCURRENTLY is used (if yes, the new unique indexes in Tasks 2–3 must be created NULLS NOT DISTINCT or must include COALESCE-free NULL handling that still guarantees uniqueness — decide per findings; PG15 supports `UNIQUE NULLS NOT DISTINCT`).

- [ ] **Step 5: Write `scripts/starters_baseline.R`** — same harness skeleton as `scripts/perf_tuning_baseline.R` (connection, 3 runs, canonical row order, CSV per case) with these cases:

```r
cases <- list(
  st_sum_5v5   = "SELECT * FROM basketball_test.fetch_lineups_all(2::smallint, p_game_year := 2026, p_min_poss := 20, p_num_starters_off := 5, p_num_starters_def := 5)",
  st_sum_own5  = "SELECT * FROM basketball_test.fetch_lineups_all(2::smallint, p_game_year := 2026, p_min_poss := 20, p_num_starters_off := 5)",
  st_sum_range = "SELECT * FROM basketball_test.fetch_lineups_all(2::smallint, p_game_year := 2026, p_min_poss := 20, p_num_starters_off_min := 4, p_num_starters_def_min := 4)",
  st_sum_bench = "SELECT * FROM basketball_test.fetch_lineups_all(2::smallint, p_game_year := 2026, p_min_poss := 20, p_num_starters_off_max := 1)",
  st_ff_5v5    = "SELECT * FROM basketball_test.fetch_lineups_four_factors(2::smallint, p_game_year := 2026, p_min_poss := 20, p_num_starters_off := 5, p_num_starters_def := 5)",
  st_ff_own5   = "SELECT * FROM basketball_test.fetch_lineups_four_factors(2::smallint, p_game_year := 2026, p_min_poss := 20, p_num_starters_off := 5)",
  st_ff_range  = "SELECT * FROM basketball_test.fetch_lineups_four_factors(2::smallint, p_game_year := 2026, p_min_poss := 20, p_num_starters_off_min := 4, p_num_starters_def_min := 4)",
  st_home_5v5  = "SELECT * FROM basketball_test.fetch_lineups_all(2::smallint, p_game_year := 2026, p_home_away := 'home', p_min_poss := 20, p_num_starters_off := 5, p_num_starters_def := 5)",
  st_clutch_5v5 = "SELECT * FROM basketball_test.fetch_lineups_all(2::smallint, p_game_year := 2026, p_min_poss := 5, p_max_margin := 5, p_num_starters_off := 5, p_num_starters_def := 5)"
)
```

(`st_clutch_5v5` must stay byte-identical end-to-end — it remains on the raw path. Note: `p_num_starters_off := 5` named calls are unambiguous since the 2026-07-27 overload cleanup.)

- [ ] **Step 6: Run baselines**

```bash
"$RSCRIPT" scripts/starters_baseline.R "<SCRATCHPAD>/starters_baseline" baseline
"$RSCRIPT" scripts/perf_tuning_baseline.R "<SCRATCHPAD>/starters_baseline" base15
```

- [ ] **Step 7: Commit**

```bash
git add scripts/starters_baseline.R
git commit -m "test(sql): starters-path baseline harness"
```

---

### Task 2: mv_lineup_totals_by_day + opp_starters key + island minutes

**Files:**
- Modify: `sql/materialized_views/sub_lineups_by_day.sql` (full rewrite of the definition)

**Interfaces:**
- Produces: MV with all existing columns PLUS `opp_starters int` key column; `minutes` becomes per-(lineup, type, game, opp_starters) gap-exclusive window minutes. Unique index gains `opp_starters`.

- [ ] **Step 1: Replace the definition** with:

```sql
-- basketball_test.mv_lineup_totals_by_day source
-- Keyed by opp_starters since 2026-07 (starters fast path). own starter count
-- is num_starters (a property of the lineup itself). Minutes are GAP-EXCLUSIVE:
-- summed per contiguous opponent-window (gaps-and-islands on opp_starters
-- within a segment), computed across ALL rows (no type_lineup filter) to
-- capture full floor time, then attached to offense rows only (as before).

CREATE MATERIALIZED VIEW basketball_test.mv_lineup_totals_by_day
TABLESPACE pg_default
AS
WITH
base AS (
    SELECT
        d.team_id,
        d.lineup_hash,
        d.type_lineup,
        d.game_id,
        d.segment_id,
        d.opp_starters,
        d.num_starters,
        d.id,
        d.event_elapsed_seconds,
        d.final_end_poss,
        d.team_score,
        d.type,
        d.parameters_points,
        d.parameters_made,
        (ROW_NUMBER() OVER (PARTITION BY d.team_id, d.lineup_hash, d.game_id, d.segment_id ORDER BY d.id)
       - ROW_NUMBER() OVER (PARTITION BY d.team_id, d.lineup_hash, d.game_id, d.segment_id, d.opp_starters ORDER BY d.id)
        ) AS opp_island
    FROM df_pts_poss_lineups_longer_mv d
),
-- Gap-exclusive stint time: one span per contiguous opp_starters window
window_times AS (
    SELECT
        b.team_id,
        b.lineup_hash,
        b.game_id,
        s.game_date AS g_date,
        s.game_year,
        b.segment_id,
        b.opp_starters,
        GREATEST(MAX(b.event_elapsed_seconds) - MIN(b.event_elapsed_seconds), 0) AS window_seconds
    FROM base b
    JOIN schedule s USING (game_id)
    WHERE b.event_elapsed_seconds IS NOT NULL
    GROUP BY b.team_id, b.lineup_hash, b.game_id, s.game_date, s.game_year, b.segment_id, b.opp_starters, b.opp_island
),
window_minutes AS (
    SELECT
        wt.team_id, wt.lineup_hash, wt.game_id, wt.g_date, wt.game_year, wt.opp_starters,
        SUM(wt.window_seconds) / 60.0 AS minutes
    FROM window_times wt
    GROUP BY wt.team_id, wt.lineup_hash, wt.game_id, wt.g_date, wt.game_year, wt.opp_starters
),
day_stats AS (
    SELECT
        b.team_id,
        b.lineup_hash,
        b.type_lineup,
        s.game_date AS g_date,
        b.game_id,
        s.game_year,
        b.opp_starters,
        MAX(b.num_starters) AS num_starters,
        SUM(CASE WHEN COALESCE(b.final_end_poss, false) THEN 1 ELSE 0 END) AS total_poss,
        COALESCE(SUM(b.team_score), 0) AS total_pts,
        SUM(CASE WHEN b.type = 'shot' AND b.parameters_points = 2 AND b.parameters_made = 'made' THEN 1 ELSE 0 END) AS fg2_made,
        SUM(CASE WHEN b.type = 'shot' AND b.parameters_points = 2 THEN 1 ELSE 0 END) AS fg2_att,
        SUM(CASE WHEN b.type = 'shot' AND b.parameters_points = 3 AND b.parameters_made = 'made' THEN 1 ELSE 0 END) AS fg3_made,
        SUM(CASE WHEN b.type = 'shot' AND b.parameters_points = 3 THEN 1 ELSE 0 END) AS fg3_att
    FROM base b
    JOIN schedule s USING (game_id)
    GROUP BY b.team_id, b.lineup_hash, b.type_lineup, s.game_date, b.game_id, s.game_year, b.opp_starters
)
SELECT
    ds.team_id,
    ds.lineup_hash,
    ds.type_lineup,
    ds.g_date,
    ds.game_id,
    ds.game_year,
    ds.opp_starters,
    ds.total_poss,
    ds.total_pts,
    ds.fg2_made,
    ds.fg2_att,
    ds.fg3_made,
    ds.fg3_att,
    ds.num_starters,
    CASE WHEN ds.type_lineup = 'offense' THEN wm.minutes END AS minutes
FROM day_stats ds
LEFT JOIN window_minutes wm
  ON wm.team_id = ds.team_id
 AND wm.lineup_hash = ds.lineup_hash
 AND wm.game_id = ds.game_id
 AND wm.opp_starters IS NOT DISTINCT FROM ds.opp_starters
WITH DATA;

-- View indexes:
CREATE INDEX idx_mv_ltotals_day_date ON basketball_test.mv_lineup_totals_by_day USING btree (g_date, lineup_hash, type_lineup);
CREATE UNIQUE INDEX idx_mv_ltotals_day_pk ON basketball_test.mv_lineup_totals_by_day
  USING btree (lineup_hash, type_lineup, g_date, num_starters, opp_starters) NULLS NOT DISTINCT;
```

Notes locked in: the day-level SUM of window minutes replaces the old per-segment MAX-span (gap-exclusive by design); `IS NOT DISTINCT FROM` handles NULL `opp_starters` join; adjust the `NULLS NOT DISTINCT` clause per Recon C findings (drop CONCURRENTLY expectations if ETL doesn't use it). **Aggregation invariant:** summing all `opp_starters` rows per (lineup, type, g_date) must reproduce the old row's poss/pts/shot counts exactly.

- [ ] **Step 2: Commit** `git add sql/materialized_views/sub_lineups_by_day.sql && git commit -m "feat(sql): opp_starters key + gap-exclusive minutes in mv_lineup_totals_by_day"`

---

### Task 3: lineup_four_factors_by_game + opp_starters key

**Files:**
- Modify: `sql/materialized_views/lineup_four_factors_by_game.sql`

- [ ] **Step 1:** Read the current 148-line definition; apply the same transformation as Task 2: add `d.opp_starters` to every grouping level and the output; recompute its minutes column (if present) with the identical `base`/`window_times`/`window_minutes` island CTEs; extend the unique index `idx_lff_pk` to `(lineup_hash, team_id, game_id, type_lineup, num_starters, opp_starters)` with the same NULLS NOT DISTINCT decision. Keep every existing FF counter expression verbatim — only the grouping keys change. Same aggregation invariant as Task 2.

- [ ] **Step 2: Commit.**

---

### Task 4: Rebuild MVs in dependency order + invariant checks

- [ ] **Step 1: Rebuild.** `mv_lineup_totals_by_day` is L2 (no dependents per the tree); `lineup_four_factors_by_game` is L3 with L4 `team_four_factors_mv` dependent. Use the repo's rebuild entry point:

```bash
"$RSCRIPT" -e "source('sql/rebuild_all_mvs.R'); rebuild_all_mvs(from_level = 2)"
```

(If a targeted rebuild path exists for single MVs, prefer it — check `sql/rebuild_all_mvs.R` first; full L2+ rebuild is the safe fallback. DDL runs on port 5432.)

- [ ] **Step 2: Invariant queries** (run via temp .R file):

```sql
-- (a) by-day MV: collapsed totals must match a raw recomputation
SELECT COUNT(*) FROM (
  SELECT lineup_hash, type_lineup, game_id, SUM(total_poss) p, SUM(total_pts) t
  FROM basketball_test.mv_lineup_totals_by_day GROUP BY 1,2,3
  EXCEPT
  SELECT lineup_hash, type_lineup, game_id,
         SUM(CASE WHEN COALESCE(final_end_poss,false) THEN 1 ELSE 0 END),
         COALESCE(SUM(team_score),0)
  FROM basketball_test.df_pts_poss_lineups_longer_mv GROUP BY 1,2,3
) x;  -- expect 0
-- (b) team_four_factors_mv totals unchanged vs pre-rebuild snapshot (capture
--     SELECT * ORDER BY team_id, game_year before Task 4; diff after)
```

- [ ] **Step 3:** Fix and re-run until invariants hold. Commit any DDL corrections.

---

### Task 5: fetch_lineups_all gate rewire

**Files:**
- Modify: `sql/functions/fetch_lineups_all.sql`

- [ ] **Step 1: Split the gate.** Replace the `v_clutch_active` assignment with:

```sql
  v_clutch_active   := (p_max_margin IS NOT NULL OR v_margin_status <> 'all' OR p_max_time_remaining IS NOT NULL);
  v_starters_active := (p_num_starters_off IS NOT NULL OR p_num_starters_def IS NOT NULL
                        OR p_num_starters_off_min IS NOT NULL OR p_num_starters_off_max IS NOT NULL
                        OR p_num_starters_def_min IS NOT NULL OR p_num_starters_def_max IS NOT NULL);
```

(declare `v_starters_active boolean;`). Fast-path check: keep requiring `NOT v_clutch_active AND NOT v_starters_active` (sub_lineups_stats has no starter keys). Clutch path condition stays `IF v_clutch_active` — starters params inside it keep working as today (raw predicates unchanged).

- [ ] **Step 2: Fix the non-clutch path's starter filtering.** In its `lineup_totals` CTE, DELETE the old per-type mapping block:

```sql
      AND (
        (lt.type_lineup = 'offense'
          AND (COALESCE(p_num_starters_off_min, p_num_starters_off) IS NULL OR lt.num_starters >= COALESCE(p_num_starters_off_min, p_num_starters_off))
          AND (COALESCE(p_num_starters_off_max, p_num_starters_off) IS NULL OR lt.num_starters <= COALESCE(p_num_starters_off_max, p_num_starters_off)))
        OR
        (lt.type_lineup = 'defense'
          AND (COALESCE(p_num_starters_def_min, p_num_starters_def) IS NULL OR lt.num_starters >= COALESCE(p_num_starters_def_min, p_num_starters_def))
          AND (COALESCE(p_num_starters_def_max, p_num_starters_def) IS NULL OR lt.num_starters <= COALESCE(p_num_starters_def_max, p_num_starters_def)))
      )
```

and replace with the uniform (clutch-path-equivalent) semantics on the new keys:

```sql
      AND (COALESCE(p_num_starters_off_min, p_num_starters_off) IS NULL OR lt.num_starters >= COALESCE(p_num_starters_off_min, p_num_starters_off))
      AND (COALESCE(p_num_starters_off_max, p_num_starters_off) IS NULL OR lt.num_starters <= COALESCE(p_num_starters_off_max, p_num_starters_off))
      AND (COALESCE(p_num_starters_def_min, p_num_starters_def) IS NULL OR lt.opp_starters >= COALESCE(p_num_starters_def_min, p_num_starters_def))
      AND (COALESCE(p_num_starters_def_max, p_num_starters_def) IS NULL OR lt.opp_starters <= COALESCE(p_num_starters_def_max, p_num_starters_def))
```

(`lineup_totals`'s GROUP BY stays on `(team_id, game_year, lineup_hash, type_lineup, num_starters)` — it SUMs across the opp_starters rows that survive the filter; num_starters weighting in the final SELECT is unchanged.)

- [ ] **Step 3: Commit.**

---

### Task 6: fetch_lineups_four_factors gate rewire

**Files:**
- Modify: `sql/functions/fetch_lineups_four_factors.sql`

- [ ] **Step 1:** Same gate split as Task 5 Step 1 (this file computes `v_use_fast_path` too — it must require `NOT v_starters_active` as well, since its fast path reads `lineup_four_factors_by_game` UNFILTERED by games; actually verify: its fast path CAN now accept starters filters because lff has the keys — if `v_full_window`-style conditions allow, add the four uniform starter conditions to the fast path's `lineup_ff` CTE instead of demoting to the filtered path. Prefer that: starters-only + no other filters = fast path + starter predicates).

- [ ] **Step 2:** In the NON-clutch filtered path's `lineup_ff`-equivalent CTE, replace the per-type mapping block (same shape as Task 5 Step 2, aliases `lf.`) with the uniform four conditions on `lf.num_starters` / `lf.opp_starters`.

- [ ] **Step 3:** Keep the clutch path untouched. Commit.

---

### Task 7: Deploy functions + full verification

- [ ] **Step 1: Deploy + grants**

```bash
"$RSCRIPT" scripts/deploy_sql_functions.R sql/functions/fetch_lineups_all.sql sql/functions/fetch_lineups_four_factors.sql
CONFIRM_DB_SECURITY_APPLY=1 "$RSCRIPT" scripts/apply_db_security.R
```

- [ ] **Step 2: Verify.** Run both harnesses as `after`:
  - 15 perf cases: byte-identical, all of them.
  - `st_clutch_5v5`: byte-identical (still raw path).
  - `st_*` starters cases: **all columns except `minutes` byte-identical** (compare with minutes column dropped); minutes eyeballed — must be ≤ baseline minutes per row (gap-exclusive can only shrink) and equal wherever a lineup had a single contiguous opponent window.
  - Timings: `st_ff_5v5` expected ~2.4s → ≤0.5s; `st_sum_5v5` ~0.6–0.75s → ≤0.4s.

- [ ] **Step 3: Test suite** `"$RSCRIPT" scripts/test_all.R` + the RUN_DB_TESTS pair from `app/` — all pass.

- [ ] **Step 4: Commit any fixups.**

---

### Task 8: Docs + merge

- [ ] **Step 1:** `docs/session_updates.md` entry: the opp_starters keys, gap-exclusive minutes decision + when it differs, gate split, dead-branch deletion, timings table.
- [ ] **Step 2:** CLAUDE.md: update the `mv_lineup_totals_by_day` bullet (uses `opp_starters` key; gap-exclusive window minutes) and the Clutch Time Filter section note that starters filters no longer force the clutch path.
- [ ] **Step 3:** Merge `sql/starters-fast-path` → main, push, delete branch (finishing-a-development-branch options first).

## Self-Review Notes

- Spec coverage: MV keys (Tasks 2–3), rebuild (4), gates + dead-branch fix (5–6), verification incl. clutch-path regression guard (7), docs (8). Gap-exclusive minutes = user decision, encoded in Task 2 DDL and the Task 7 minutes tolerance.
- Type consistency: `opp_starters int` matches `df_pts_poss_lineups_longer_mv.opp_starters`; filters reference `lt.`/`lf.` per file's aliases; uniform-semantics rule stated once in Global Constraints and applied in both function tasks.
- Known risks called out where they live: NULLS NOT DISTINCT vs ETL CONCURRENTLY (Recon C), team_four_factors_mv re-sum invariant (Recon B + Task 4), pff minutes-method consistency (Recon A).
