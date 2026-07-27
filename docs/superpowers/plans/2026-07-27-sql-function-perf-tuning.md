# SQL Function Performance Tuning Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Apply six approved performance refactors to the app-facing SQL functions without changing their output contracts, deploy them to Supabase, and prove equivalence with before/after output diffs and timings.

**Architecture:** All changes are behavior-preserving rewrites inside the seven `sql/functions/*.sql` files (plpgsql, schema `basketball_test`). A baseline R script captures current outputs + timings from the live DB before any deploy; after deploying all edited functions in one batch, the same script re-runs and diffs. Grants are restored via the existing security-apply script because the files DROP+CREATE.

**Tech Stack:** PostgreSQL 15+ (Supabase), plpgsql, R 4.4.2 + DBI/RPostgres for deploy/verify harness.

## Global Constraints

- Schema is `basketball_test`; DDL connects on port 5432 (same pooler host), queries on 6543.
- SQL file edits use exact-string Edit operations only — never regex/scripted rewrites (repo rule after a past 170-line truncation). After each file edit run `git diff --stat` and sanity-check the line delta.
- `fetch_lineups_all.sql` and `fetch_lineups_four_factors.sql` must stay structurally in sync (CLAUDE.md rule).
- Function signatures must NOT change (React/Plumber + Shiny call them positionally).
- `app_readonly` EXECUTE grants are dropped by DROP FUNCTION — after deploy, re-run `scripts/apply_db_security.R` with `CONFIRM_DB_SECURITY_APPLY=1`.
- **Output contract (hard gate, user requirement): after-deploy outputs must be byte-identical to baselines for ALL 15 cases.** No exceptions. The four_factors_compute roster edit (Task 5 Step 2) only ships if diagnostics D3+D4 prove it cannot change output; otherwise skip it. Any post-deploy diff ⇒ roll back the causing file and report.
- Branch: `sql/perf-function-tuning`. Commit after each task.
- The five approved items: (1) dedupe double raw-MV scan in team ratings, (2) shot_zones EXISTS→LEFT JOIN, (3) last-N-games `schedule_ranked` pattern everywhere, (4) year-scoped roster join in four_factors_compute, (5) `plan_cache_mode = force_custom_plan` on all seven functions.
- Out of scope (explicitly deferred): MIN/MAX segment-seconds swap (user: the min/max approach was buggy — keep `array_agg` first/last-by-id verbatim), switching onoff_compute's opponent-strength source to `team_ppp_ratings_mv` (semantic decision pending), casting `team_ppp_ratings_mv` columns to int inside the MV (requires MV rebuild), dropping `DISTINCT` from onoff sched CTEs.

---

### Task 1: Branch + baseline capture harness

**Files:**
- Create: `scripts/perf_tuning_baseline.R`
- Baselines written to: `<SCRATCHPAD>/perf_baseline/` (session scratchpad dir, NOT the repo)

**Interfaces:**
- Produces: `perf_tuning_baseline.R` runnable as `Rscript scripts/perf_tuning_baseline.R <outdir> <label>` writing `<label>_<case>.csv` per case + `<label>_timings.csv` + `diagnostics_<label>.txt`. Task 8 re-runs it with label `after` and diffs.

- [ ] **Step 1: Create branch**

```bash
git checkout -b sql/perf-function-tuning
```

- [ ] **Step 2: Write the harness script**

Assumptions declared up front (repo rule: declare query assumptions in prose):
- `final_schedule_mv` is assumed unique per `(game_id, team_id)` — diagnostic D1 verifies; the schedule_ranked join pattern (already live in `fetch_lineups_all`) relies on it.
- Timings are median of 3 warm runs per case on the pooler; EXPLAIN inside plpgsql is not visible, so wall-clock is the evidence.

```r
# scripts/perf_tuning_baseline.R
# Captures function outputs + timings + diagnostics for the perf-tuning refactor.
# Usage: Rscript scripts/perf_tuning_baseline.R <outdir> <label>
args <- commandArgs(trailingOnly = TRUE)
outdir <- args[[1]]; label <- args[[2]]
dir.create(outdir, recursive = TRUE, showWarnings = FALSE)

if (file.exists("etl/.Renviron")) readRenviron("etl/.Renviron")
suppressPackageStartupMessages({ library(DBI); library(RPostgres) })

con <- dbConnect(Postgres(),
  host = Sys.getenv("PG_HOST"), port = as.integer(Sys.getenv("PG_PORT", "6543")),
  dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE", "require"),
  bigint = "numeric", connect_timeout = 15L)
on.exit(dbDisconnect(con), add = TRUE)
dbExecute(con, "SET statement_timeout = '120s'")

cases <- list(
  onoff_full = list(sql = "SELECT * FROM basketball_test.onoff_compute($1::date,$2::date,NULL,0,0,-999,'2026')",
                    params = list("2025-10-01", "2026-07-01")),
  onoff_lastn = list(sql = "SELECT * FROM basketball_test.onoff_compute($1::date,$2::date,NULL,0,0,-999,'2026',NULL,NULL,'all','all',NULL,NULL,NULL,NULL,NULL,5)",
                     params = list("2025-10-01", "2026-07-01")),
  ff_full  = list(sql = "SELECT * FROM basketball_test.four_factors_compute(2026)", params = list()),
  ff_lastn = list(sql = "SELECT * FROM basketball_test.four_factors_compute(2026,NULL,NULL,NULL,NULL,NULL,'all','all','all',NULL,'net',NULL,NULL,5)", params = list()),
  lineups_home = list(sql = "SELECT * FROM basketball_test.fetch_lineups_all(2::smallint, p_game_year := 2026, p_home_away := 'home', p_min_poss := 20)", params = list()),
  lineups_clutch = list(sql = "SELECT * FROM basketball_test.fetch_lineups_all(2::smallint, p_game_year := 2026, p_min_poss := 5, p_max_margin := 5, p_max_time_remaining := 300)", params = list()),
  lineups_ff_home = list(sql = "SELECT * FROM basketball_test.fetch_lineups_four_factors(2::smallint, p_game_year := 2026, p_home_away := 'home', p_min_poss := 20)", params = list()),
  lineups_ff_clutch = list(sql = "SELECT * FROM basketball_test.fetch_lineups_four_factors(2::smallint, p_game_year := 2026, p_min_poss := 5, p_max_margin := 5, p_max_time_remaining := 300)", params = list()),
  team_rt_filtered = list(sql = "SELECT * FROM basketball_test.get_team_ratings_dynamic(2026, p_home_away := 'home')", params = list()),
  team_rt_clutch = list(sql = "SELECT * FROM basketball_test.get_team_ratings_dynamic(2026, p_max_margin := 5, p_max_time_remaining := 300)", params = list()),
  team_rt_lastn = list(sql = "SELECT * FROM basketball_test.get_team_ratings_dynamic(2026, p_last_n_games := 5)", params = list()),
  team_ff_filtered = list(sql = "SELECT * FROM basketball_test.get_team_four_factors_dynamic(2026, p_home_away := 'home')", params = list()),
  team_ff_clutch = list(sql = "SELECT * FROM basketball_test.get_team_four_factors_dynamic(2026, p_max_margin := 5, p_max_time_remaining := 300)", params = list()),
  trad_full = list(sql = "SELECT * FROM basketball_test.get_player_traditional_dynamic(2026)", params = list()),
  trad_clutch = list(sql = "SELECT * FROM basketball_test.get_player_traditional_dynamic(2026, p_max_margin := 5, p_max_time_remaining := 300)", params = list())
)

timings <- data.frame(case = character(), median_s = numeric())
for (nm in names(cases)) {
  cs <- cases[[nm]]
  runs <- numeric(3)
  df <- NULL
  for (i in 1:3) {
    t0 <- Sys.time()
    df <- if (length(cs$params)) dbGetQuery(con, cs$sql, params = cs$params) else dbGetQuery(con, cs$sql)
    runs[i] <- as.numeric(difftime(Sys.time(), t0, units = "secs"))
  }
  ord <- do.call(order, df)
  write.csv(df[ord, , drop = FALSE], file.path(outdir, sprintf("%s_%s.csv", label, nm)), row.names = FALSE)
  timings <- rbind(timings, data.frame(case = nm, median_s = round(median(runs), 3)))
  cat(sprintf("%-20s rows=%6d median=%.3fs\n", nm, nrow(df), median(runs)))
}
write.csv(timings, file.path(outdir, sprintf("%s_timings.csv", label)), row.names = FALSE)

# Diagnostics (baseline label only)
if (label == "baseline") {
  d1 <- dbGetQuery(con, "
    SELECT COUNT(*) AS n_rows, COUNT(DISTINCT (game_id, team_id)) AS n_keys
    FROM basketball_test.final_schedule_mv")
  d3 <- dbGetQuery(con, "
    SELECT player_id, team_id, COUNT(*) AS n
    FROM basketball_test.four_factors_compute(2026)
    GROUP BY 1,2 HAVING COUNT(*) > 1")
  d4 <- dbGetQuery(con, "
    SELECT f.player_id, f.team_id
    FROM basketball_test.four_factors_compute(2026) f
    WHERE NOT EXISTS (
      SELECT 1 FROM basketball_test.full_rosters fr
      WHERE fr.player_id = f.player_id AND fr.team_id = f.team_id AND fr.game_year = 2026
    )")
  sink(file.path(outdir, "diagnostics_baseline.txt"))
  cat("D1 final_schedule_mv uniqueness (n_rows must equal n_keys):\n"); print(d1)
  cat("\nD3 four_factors_compute duplicate (player,team) rows (must be empty for Task 5 Step 2):\n"); print(d3)
  cat("\nD4 output players missing a same-season roster row (must be empty for Task 5 Step 2):\n"); print(d4)
  sink()
  cat("\nD1:\n"); print(d1); cat("D3:\n"); print(d3); cat("D4:\n"); print(d4)
}
```

Note: `ord <- do.call(order, df)` sorts rows canonically before writing so before/after CSVs diff cleanly regardless of within-tie ordering differences.

- [ ] **Step 3: Run baseline**

```bash
"$RSCRIPT" scripts/perf_tuning_baseline.R "<SCRATCHPAD>/perf_baseline" baseline
```

Expected: 15 CSVs + timings CSV + diagnostics file. Record D1 (must show equal counts — if not, STOP: the schedule_ranked pattern assumption fails), D3 and D4 (both empty ⇒ Task 5 Step 2 is GO; otherwise Task 5 Step 2 is SKIPPED to preserve identical output, and the D3/D4 rows are reported to the user as a data-quality finding).

- [ ] **Step 4: Commit the harness**

```bash
git add scripts/perf_tuning_baseline.R
git commit -m "test(sql): add perf-tuning baseline/verify harness"
```

---

### Task 2: get_team_ratings_dynamic — single clutch scan, shot_zones LEFT JOIN, schedule_ranked, team_names

**Files:**
- Modify: `sql/functions/get_team_ratings_dynamic.sql`

**Interfaces:**
- Produces: same signature and RETURNS TABLE as today. New CTE names: `schedule_ranked`, `clutch_rows`, `team_names`. `qualifying_games`/`win_loss`/`pivoted` semantics unchanged.

Four edits to the single RETURN QUERY:

- [ ] **Step 1: Replace correlated last-N subquery with schedule_ranked CTE**

Old (lines 85-114 region) — replace:

```sql
  RETURN QUERY
  WITH 
  -- CTE 1: Games Base (Filter Schedule)
  games_base AS (
    SELECT fs.game_id, fs.team_id, fs.game_year, fs.opp_team_id, fs.has_won
    FROM basketball_test.final_schedule_mv fs
    WHERE fs.game_year = p_game_year
```

with:

```sql
  RETURN QUERY
  WITH 
  schedule_ranked AS (
    SELECT
      fsr.game_id,
      fsr.team_id,
      fsr.game_year,
      ROW_NUMBER() OVER (
        PARTITION BY fsr.team_id, fsr.game_year
        ORDER BY fsr.game_date DESC NULLS LAST, fsr.game_id DESC
      ) AS rn_recent
    FROM basketball_test.final_schedule_mv fsr
    WHERE fsr.game_year = p_game_year
  ),
  -- CTE 1: Games Base (Filter Schedule)
  games_base AS (
    SELECT fs.game_id, fs.team_id, fs.game_year, fs.opp_team_id, fs.has_won
    FROM basketball_test.final_schedule_mv fs
    JOIN schedule_ranked sr
      ON sr.game_id = fs.game_id
     AND sr.team_id = fs.team_id
     AND sr.game_year = fs.game_year
    WHERE fs.game_year = p_game_year
```

Then replace the whole correlated block:

```sql
      AND (p_last_n_games IS NULL
           OR COALESCE((
                SELECT fsr.rn_recent
                FROM (
                  SELECT fs2.game_id,
                         ROW_NUMBER() OVER (
                           PARTITION BY fs2.team_id, fs2.game_year
                           ORDER BY fs2.game_date DESC NULLS LAST, fs2.game_id DESC
                         ) AS rn_recent
                  FROM basketball_test.final_schedule_mv fs2
                  WHERE fs2.team_id = fs.team_id
                    AND fs2.game_year = fs.game_year
                ) fsr
                WHERE fsr.game_id = fs.game_id
              ), 2147483647) <= p_last_n_games)
```

with:

```sql
      AND (p_last_n_games IS NULL OR sr.rn_recent <= p_last_n_games)
```

- [ ] **Step 2: Replace qualifying_games + base_agg with clutch_rows single scan**

Replace everything from `-- CTE 4: Qualifying Games` through the end of `base_agg` (the block ending `GROUP BY qg.game_year, qg.team_id, dppllm.type_lineup\n  ),`) with:

```sql
  -- CTE 4: single clutch-filtered scan of the raw MV (materialized once,
  -- consumed by qualifying_games and base_agg — replaces the former double scan)
  -- NOTE: Use pre-shot margin (subtract points scored from current score)
  clutch_rows AS (
      SELECT
        gf.game_year,
        gf.team_id,
        gf.game_id,
        gf.has_won,
        dppllm.type_lineup,
        dppllm.team_score,
        CASE WHEN dppllm.final_end_poss IS TRUE THEN 1 ELSE 0 END AS final_end_flag,
        dppllm.type,
        dppllm.parameters_points,
        dppllm.parameters_type,
        z.is_corner3
      FROM basketball_test.df_pts_poss_lineups_longer_mv dppllm
      JOIN games_filtered gf ON gf.game_id = dppllm.game_id AND gf.team_id = dppllm.team_id
      LEFT JOIN basketball_test.shot_zones z
        ON z.game_id = dppllm.game_id
       AND z.id = dppllm.id
      WHERE (p_max_margin IS NULL
             OR ABS(CASE WHEN dppllm.type_lineup = 'offense'
                         THEN (dppllm.own_team_score - COALESCE(dppllm.team_score, 0)) - dppllm.opp_team_score
                         ELSE dppllm.own_team_score - (dppllm.opp_team_score - COALESCE(dppllm.team_score, 0))
                    END) <= p_max_margin
             OR (dppllm.quarter > 4 AND NOT COALESCE(p_ot_margin_filter, FALSE)))
        AND (v_margin_status = 'all'
             OR (v_margin_status = 'leading'  AND
                 CASE WHEN dppllm.type_lineup = 'offense'
                      THEN (dppllm.own_team_score - COALESCE(dppllm.team_score, 0)) > dppllm.opp_team_score
                      ELSE dppllm.own_team_score > (dppllm.opp_team_score - COALESCE(dppllm.team_score, 0))
                 END)
             OR (v_margin_status = 'trailing' AND
                 CASE WHEN dppllm.type_lineup = 'offense'
                      THEN (dppllm.own_team_score - COALESCE(dppllm.team_score, 0)) < dppllm.opp_team_score
                      ELSE dppllm.own_team_score < (dppllm.opp_team_score - COALESCE(dppllm.team_score, 0))
                 END)
             OR (v_margin_status = 'tied'     AND
                 CASE WHEN dppllm.type_lineup = 'offense'
                      THEN (dppllm.own_team_score - COALESCE(dppllm.team_score, 0)) = dppllm.opp_team_score
                      ELSE dppllm.own_team_score = (dppllm.opp_team_score - COALESCE(dppllm.team_score, 0))
                 END)
             OR (dppllm.quarter > 4 AND NOT COALESCE(p_ot_margin_filter, FALSE)))
        AND (p_max_time_remaining IS NULL OR dppllm.end_game_seconds_remaining <= p_max_time_remaining OR dppllm.quarter > 4)
        AND (COALESCE(p_num_starters_off_min, p_num_starters_off) IS NULL OR dppllm.own_starters >= COALESCE(p_num_starters_off_min, p_num_starters_off))
        AND (COALESCE(p_num_starters_off_max, p_num_starters_off) IS NULL OR dppllm.own_starters <= COALESCE(p_num_starters_off_max, p_num_starters_off))
        AND (COALESCE(p_num_starters_def_min, p_num_starters_def) IS NULL OR dppllm.opp_starters >= COALESCE(p_num_starters_def_min, p_num_starters_def))
        AND (COALESCE(p_num_starters_def_max, p_num_starters_def) IS NULL OR dppllm.opp_starters <= COALESCE(p_num_starters_def_max, p_num_starters_def))
  ),

  -- CTE 4a: distinct games with at least one qualifying row
  qualifying_games AS (
      SELECT DISTINCT cr.game_year, cr.team_id, cr.game_id, cr.has_won
      FROM clutch_rows cr
  ),

  -- CTE 4b: Win/Loss counts (from qualifying games only)
  win_loss AS (
    SELECT qg.game_year,
           qg.team_id,
           COUNT(*) FILTER (WHERE qg.has_won = TRUE) AS wins,
           COUNT(*) FILTER (WHERE qg.has_won = FALSE) AS losses
    FROM qualifying_games qg
    GROUP BY qg.game_year, qg.team_id
  ),

  -- CTE 5: Base Aggregation over the pre-filtered rows
  base_agg AS (
      SELECT
        cr.game_year,
        cr.team_id,
        cr.type_lineup,
        sum(cr.team_score) / NULLIF(sum(cr.final_end_flag), 0)::numeric AS ppp,
        sum(cr.final_end_flag) AS total_poss,
        COUNT(DISTINCT cr.game_id) AS games_count,
        SUM(CASE WHEN cr.type = 'shot' THEN 1 ELSE 0 END) AS fga,
        SUM(CASE WHEN cr.type = 'shot' AND cr.parameters_points = 2 AND cr.parameters_type = 'lay-up' THEN 1 ELSE 0 END) AS layup_att,
        SUM(CASE WHEN cr.type = 'shot' AND cr.parameters_points = 2 AND cr.parameters_type IN ('dunk', 'allyhoop') THEN 1 ELSE 0 END) AS dunk_att,
        SUM(CASE WHEN cr.type = 'shot' AND cr.parameters_points = 3 THEN 1 ELSE 0 END) AS fg3_att,
        SUM(CASE WHEN cr.type = 'shot' AND cr.parameters_points = 3 AND cr.is_corner3 IS TRUE THEN 1 ELSE 0 END) AS c3_att,
        SUM(CASE WHEN cr.type = 'shot' AND cr.parameters_points = 3 AND cr.is_corner3 IS NOT NULL THEN 1 ELSE 0 END) AS c3_known_att
      FROM clutch_rows cr
      GROUP BY cr.game_year, cr.team_id, cr.type_lineup
  ),
```

Equivalence argument (recorded for review): the old `base_agg` joined `qualifying_games` back to the raw MV and re-applied the identical clutch predicate — the surviving rows are by construction exactly the rows of `clutch_rows`. `shot_zones` PK is `(game_id, id)` so the LEFT JOIN cannot fan out; `is_corner3` is NOT NULL in the table, so `z.is_corner3 IS NOT NULL` ⇔ "row exists" ⇔ the old EXISTS with `is_corner3 IS NOT NULL`, and `IS TRUE` matches the old corner EXISTS.

- [ ] **Step 3: Replace the full_rosters dedupe-by-GROUP-BY join with a team_names CTE**

Old `final_calc` header:

```sql
  final_calc AS (
      SELECT
        p.game_year,
        p.team_id,
        fr.team_name,
```

New — insert `team_names` before `final_calc` and join it instead of `full_rosters` (delete the old `JOIN basketball_test.full_rosters fr ...` and the entire `GROUP BY p.game_year, ...` tail of final_calc, since rows are already unique):

```sql
  team_names AS (
      SELECT fr.team_id, MIN(fr.team_name) AS team_name
      FROM basketball_test.full_rosters fr
      WHERE fr.game_year = p_game_year
      GROUP BY fr.team_id
  ),

  final_calc AS (
      SELECT
        p.game_year,
        p.team_id,
        tn.team_name,
```

and the old FROM/JOIN/GROUP BY tail of final_calc (`FROM pivoted p JOIN basketball_test.full_rosters fr ON fr.game_year = p.game_year AND fr.team_id = p.team_id GROUP BY ...` through the end of the GROUP BY list) becomes just (team_names is a CTE — no schema qualifier):

```sql
      FROM pivoted p
      JOIN team_names tn ON tn.team_id = p.team_id
  )
```

- [ ] **Step 4: Add plan_cache_mode to the function header**

```sql
LANGUAGE plpgsql
STABLE
AS $$
```
→
```sql
LANGUAGE plpgsql
STABLE
SET plan_cache_mode = force_custom_plan
AS $$
```

- [ ] **Step 5: Sanity + commit**

```bash
git diff --stat sql/functions/get_team_ratings_dynamic.sql
git commit -am "perf(sql): single clutch scan + shot_zones join in get_team_ratings_dynamic"
```

Expected stat: roughly −60/+90 lines; eyeball full diff for accidental truncation.

---

### Task 3: get_team_four_factors_dynamic — schedule_ranked (both branches), team_names, plan_cache_mode

**Files:**
- Modify: `sql/functions/get_team_four_factors_dynamic.sql`

Both the clutch and non-clutch branches contain the identical `games_base` correlated last-N block and the identical `final_calc` roster join.

- [ ] **Step 1: For EACH branch (2×), insert schedule_ranked and join it**

After each `RETURN QUERY\n  WITH` (one under `-- CLUTCH PATH`, one under `-- NON-CLUTCH PATH`), insert:

```sql
  schedule_ranked AS (
    SELECT
      fsr.game_id,
      fsr.team_id,
      fsr.game_year,
      ROW_NUMBER() OVER (
        PARTITION BY fsr.team_id, fsr.game_year
        ORDER BY fsr.game_date DESC NULLS LAST, fsr.game_id DESC
      ) AS rn_recent
    FROM basketball_test.final_schedule_mv fsr
    WHERE fsr.game_year = p_game_year
  ),
```

In each `games_base`, add after `FROM basketball_test.final_schedule_mv fs`:

```sql
    JOIN schedule_ranked sr
      ON sr.game_id = fs.game_id
     AND sr.team_id = fs.team_id
     AND sr.game_year = fs.game_year
```

and replace each correlated block (identical text in both branches — use replace_all):

```sql
      AND (p_last_n_games IS NULL
           OR COALESCE((
                SELECT fsr.rn_recent
                FROM (
                  SELECT fs2.game_id,
                         ROW_NUMBER() OVER (
                           PARTITION BY fs2.team_id, fs2.game_year
                           ORDER BY fs2.game_date DESC NULLS LAST, fs2.game_id DESC
                         ) AS rn_recent
                  FROM basketball_test.final_schedule_mv fs2
                  WHERE fs2.team_id = fs.team_id
                    AND fs2.game_year = fs.game_year
                ) fsr
                WHERE fsr.game_id = fs.game_id
              ), 2147483647) <= p_last_n_games)
```
→
```sql
      AND (p_last_n_games IS NULL OR sr.rn_recent <= p_last_n_games)
```

- [ ] **Step 2: For EACH branch (2×), replace final_calc roster join with team_names**

Insert before each `final_calc AS (`:

```sql
  team_names AS (
    SELECT fr.team_id, MIN(fr.team_name) AS team_name
    FROM basketball_test.full_rosters fr
    WHERE fr.game_year = p_game_year
    GROUP BY fr.team_id
  ),
```

In each `final_calc`: `fr.team_name` → `tn.team_name`; replace

```sql
    FROM pivoted p
    JOIN basketball_test.full_rosters fr
      ON fr.game_year = p.game_year AND fr.team_id = p.team_id
    GROUP BY p.team_id, p.game_year, fr.team_name,
```
and the rest of the GROUP BY list, with:

```sql
    FROM pivoted p
    JOIN team_names tn ON tn.team_id = p.team_id
  )
```

(The dedupe GROUP BY is dead once the join is 1:1.)

- [ ] **Step 3: plan_cache_mode header (same fragment as Task 2 Step 4). Commit.**

```bash
git diff --stat sql/functions/get_team_four_factors_dynamic.sql
git commit -am "perf(sql): schedule_ranked + team_names in get_team_four_factors_dynamic"
```

---

### Task 4: onoff_compute — schedule_ranked + plan_cache_mode

**Files:**
- Modify: `sql/functions/onoff_compute.sql`

- [ ] **Step 1: Insert schedule_ranked as first CTE** (after `RETURN QUERY\n  WITH`, before the `/* ... Opponent strength ranking ... */` comment):

```sql
  schedule_ranked AS (
    SELECT
      fsr.game_id,
      fsr.team_id,
      fsr.game_year,
      ROW_NUMBER() OVER (
        PARTITION BY fsr.team_id, fsr.game_year
        ORDER BY fsr.game_date DESC NULLS LAST, fsr.game_id DESC
      ) AS rn_recent
    FROM basketball_test.final_schedule_mv fsr
    WHERE (v_game_year IS NULL OR fsr.game_year::text = v_game_year)
  ),
```

Note the TEXT year comparison — this function's year param is text; the filter matches `sched`'s own year filter so rank partitions are identical to the old correlated form for every surviving row.

- [ ] **Step 2: Join it in `sched` and swap the last-N predicate**

In `sched`, after `FROM basketball_test.final_schedule_mv fs`, add:

```sql
    JOIN schedule_ranked sr
      ON sr.game_id = fs.game_id
     AND sr.team_id = fs.team_id
     AND sr.game_year = fs.game_year
```

Replace the correlated block (same 15-line text as Task 3) with:

```sql
      AND (p_last_n_games IS NULL OR sr.rn_recent <= p_last_n_games)
```

- [ ] **Step 3: plan_cache_mode header. Commit.**

```bash
git commit -am "perf(sql): schedule_ranked + custom plans in onoff_compute"
```

---

### Task 5: four_factors_compute — schedule_ranked, year-scoped roster join, plan_cache_mode

**Files:**
- Modify: `sql/functions/four_factors_compute.sql`

- [ ] **Step 1: schedule_ranked** — insert after `RETURN QUERY\n  WITH`, scoped `WHERE fsr.game_year = p_game_year` (int compare); join in `games_base` after its FROM line; swap the correlated last-N block for `AND (p_last_n_games IS NULL OR sr.rn_recent <= p_last_n_games)`. Same fragments as Task 3 Step 1.

- [ ] **Step 2 (GATED): Year-scope the roster join in final_rows**

**GO only if baseline diagnostics D3 AND D4 are both empty.** Rationale: D3 empty means no (player_id, team_id) has conflicting name/team_name spellings anywhere in full_rosters (duplicates would otherwise appear in the output), so a year-scoped MIN() returns the same unique values; D4 empty means no output player would be dropped by the year filter. Together they guarantee byte-identical output. If either has rows: SKIP this step, leave the join untouched, and report the rows to the user.

Old:

```sql
    FROM pivoted p
    JOIN (
      SELECT DISTINCT
        full_rosters.player_id,
        full_rosters.team_id,
        full_rosters.firstname,
        full_rosters.lastname,
        full_rosters.team_name
      FROM basketball_test.full_rosters
    ) r ON p.player_id = r.player_id AND p.team_id = r.team_id
```

New:

```sql
    FROM pivoted p
    JOIN (
      SELECT
        fr2.player_id,
        fr2.team_id,
        MIN(fr2.firstname) AS firstname,
        MIN(fr2.lastname)  AS lastname,
        MIN(fr2.team_name) AS team_name
      FROM basketball_test.full_rosters fr2
      WHERE fr2.game_year = p_game_year
      GROUP BY fr2.player_id, fr2.team_id
    ) r ON p.player_id = r.player_id AND p.team_id = r.team_id
```

- [ ] **Step 3: plan_cache_mode header. Commit.**

```bash
git commit -am "perf(sql): year-scoped rosters + schedule_ranked in four_factors_compute"
```

---

### Task 6: get_player_traditional_dynamic — schedule_ranked + plan_cache_mode

**Files:**
- Modify: `sql/functions/get_player_traditional_dynamic.sql`

- [ ] **Step 1: schedule_ranked** (scoped `WHERE fsr.game_year = p_game_year`) inserted after `RETURN QUERY\n  WITH`, becoming the first CTE before `games_base AS (`; join `sr` in `games_base` after its FROM line; replace this file's (whitespace-differing) correlated block:

```sql
      AND (
        p_last_n_games IS NULL OR COALESCE((
          SELECT fsr.rn_recent
          FROM (
            SELECT
              fs2.game_id,
              ROW_NUMBER() OVER (
                PARTITION BY fs2.team_id, fs2.game_year
                ORDER BY fs2.game_date DESC NULLS LAST, fs2.game_id DESC
              ) AS rn_recent
            FROM basketball_test.final_schedule_mv fs2
            WHERE fs2.team_id = fs.team_id
              AND fs2.game_year = fs.game_year
          ) fsr
          WHERE fsr.game_id = fs.game_id
        ), 2147483647) <= p_last_n_games
      )
```
→
```sql
      AND (p_last_n_games IS NULL OR sr.rn_recent <= p_last_n_games)
```

- [ ] **Step 2: plan_cache_mode header. Commit.**

```bash
git commit -am "perf(sql): schedule_ranked + custom plans in get_player_traditional_dynamic"
```

---

### Task 7: fetch_lineups_four_factors sync + fetch_lineups_all plan_cache_mode

**Files:**
- Modify: `sql/functions/fetch_lineups_four_factors.sql`
- Modify: `sql/functions/fetch_lineups_all.sql`

- [ ] **Step 1: In fetch_lineups_four_factors, mirror fetch_lineups_all's schedule_ranked pattern in BOTH branches.** First `grep -n "p_last_n_games IS NULL" sql/functions/fetch_lineups_four_factors.sql` — expect 2 correlated blocks (clutch + non-clutch). For each branch insert after `RETURN QUERY\n  WITH`:

```sql
  schedule_ranked AS (
    SELECT
      fsr.game_id,
      fsr.team_id,
      fsr.game_year,
      ROW_NUMBER() OVER (
        PARTITION BY fsr.team_id, fsr.game_year
        ORDER BY fsr.game_date DESC NULLS LAST, fsr.game_id DESC
      ) AS rn_recent
    FROM basketball_test.final_schedule_mv fsr
    WHERE (p_game_year IS NULL OR fsr.game_year = p_game_year)
  ),
```

(nullable-year form — matches fetch_lineups_all lines 113-124 exactly). In each `games_base`, add after `FROM basketball_test.final_schedule_mv fs`:

```sql
    JOIN schedule_ranked sr
      ON sr.game_id = fs.game_id
     AND sr.team_id = fs.team_id
     AND sr.game_year = fs.game_year
```

and replace each correlated block with:

```sql
      AND (p_last_n_games IS NULL OR sr.rn_recent <= p_last_n_games)
```

- [ ] **Step 2: plan_cache_mode in both files.** These two use ` LANGUAGE plpgsql\n STABLE\nAS $function$` — insert ` SET plan_cache_mode = force_custom_plan` between STABLE and AS.

- [ ] **Step 3: Verify sync + commit**

```bash
grep -c "schedule_ranked" sql/functions/fetch_lineups_all.sql          # expect 4
grep -c "schedule_ranked" sql/functions/fetch_lineups_four_factors.sql # expect 8 (2 branches x def+join refs)
git commit -am "perf(sql): restore lineups-pair last-N sync; custom plans"
```

(Counts: fetch_lineups_all has 1 branch pair ×(1 def + 3 refs) = 4 today; four_factors gets 2 branches × 4 = 8. If actual ref-counts differ, eyeball the diff instead of trusting the number.)

---

### Task 8: Deploy + verify

**Files:**
- Create: `scripts/deploy_sql_functions.R`

- [ ] **Step 1: Write the deploy script**

```r
# scripts/deploy_sql_functions.R
# Deploys sql/functions/*.sql files passed as args (whole-file, multi-statement)
# over the DDL port in ONE transaction. RPostgres dbExecute(immediate) uses the
# simple protocol, so DROP + CREATE in one file body is fine.
args <- commandArgs(trailingOnly = TRUE)
if (!length(args)) stop("Usage: Rscript scripts/deploy_sql_functions.R <file.sql> [...]")
stopifnot(all(file.exists(args)))

if (file.exists("etl/.Renviron")) readRenviron("etl/.Renviron")
suppressPackageStartupMessages({ library(DBI); library(RPostgres) })

con <- dbConnect(Postgres(),
  host = Sys.getenv("PG_HOST"), port = as.integer(Sys.getenv("PG_DDL_PORT", "5432")),
  dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE", "require"),
  connect_timeout = 15L)
on.exit(dbDisconnect(con), add = TRUE)

dbBegin(con)
ok <- TRUE
for (f in args) {
  sql <- paste(readLines(f, warn = FALSE), collapse = "\n")
  res <- tryCatch({ dbExecute(con, sql, immediate = TRUE); TRUE },
                  error = function(e) { message(sprintf("FAILED %s: %s", f, conditionMessage(e))); FALSE })
  if (!res) { ok <- FALSE; break }
  message("deployed: ", f)
}
if (ok) { dbCommit(con); message("All functions deployed (committed).") } else {
  dbRollback(con); stop("Deploy rolled back; nothing changed.")
}
```

- [ ] **Step 2: Deploy all seven files**

```bash
"$RSCRIPT" scripts/deploy_sql_functions.R \
  sql/functions/get_team_ratings_dynamic.sql \
  sql/functions/get_team_four_factors_dynamic.sql \
  sql/functions/onoff_compute.sql \
  sql/functions/four_factors_compute.sql \
  sql/functions/get_player_traditional_dynamic.sql \
  sql/functions/fetch_lineups_all.sql \
  sql/functions/fetch_lineups_four_factors.sql
```

Expected: `deployed:` line per file, then committed. On any failure the transaction rolls back — DB unchanged.

- [ ] **Step 3: Restore grants (DROP+CREATE wiped app_readonly EXECUTE)**

```bash
CONFIRM_DB_SECURITY_APPLY=1 "$RSCRIPT" scripts/apply_db_security.R
```

Expected: "Database security hardening committed." and empty violations. If audit fails it self-rolls-back — investigate before proceeding.

- [ ] **Step 4: Re-run harness as `after` + diff**

```bash
"$RSCRIPT" scripts/perf_tuning_baseline.R "<SCRATCHPAD>/perf_baseline" after
cd "<SCRATCHPAD>/perf_baseline"
for f in baseline_*.csv; do a="after_${f#baseline_}"; diff -q "$f" "$a" || echo "DIFF: $f"; done
```

Expected: **zero diffs across all 15 cases** (Task 5 Step 2's gate guarantees ff_* equality too). ANY diff ⇒ stop, investigate, and if not immediately explainable roll back by deploying the pre-branch versions: `git stash && git checkout main -- sql/functions && Rscript scripts/deploy_sql_functions.R <the seven files> && CONFIRM_DB_SECURITY_APPLY=1 Rscript scripts/apply_db_security.R && git checkout sql/perf-function-tuning -- sql/functions && git stash pop`.

- [ ] **Step 5: Compare timings**

Print `baseline_timings.csv` vs `after_timings.csv` side by side. Expect team_rt_clutch and team_rt_filtered to drop materially; others flat-to-better. Timings worse by >20% on any case ⇒ investigate before merging.

- [ ] **Step 6: Run the R test suite**

```bash
"$RSCRIPT" scripts/test_all.R
```

Expected: all pass (server tests use mocks; db-security contract tests hit the live DB and must pass post-grant-restore).

- [ ] **Step 7: Commit harness outputs summary + deploy script**

```bash
git add scripts/deploy_sql_functions.R
git commit -m "feat(scripts): transactional SQL function deploy helper"
```

---

### Task 9: Documentation + merge

- [ ] **Step 1:** Append an entry to `docs/session_updates.md` (date 2026-07-27): the five changes, D1/D3/D4 diagnostic results, before/after timings table, confirmation that all 15 output cases diffed identical, and the note that `plan_cache_mode = force_custom_plan` is now set on all seven app functions.

- [ ] **Step 2:** Update CLAUDE.md Pitfalls (PostgreSQL section) with one line: "Correlated last-N subqueries: use the `schedule_ranked` CTE pattern (all seven app functions now do); DROP+CREATE of functions requires re-running `scripts/apply_db_security.R` to restore app_readonly EXECUTE."

- [ ] **Step 3:** Merge per repo convention:

```bash
git checkout main
git merge sql/perf-function-tuning
git push origin main
git branch -d sql/perf-function-tuning
```

## Self-Review Notes

- Spec coverage: item 1 → Task 2; item 2 → Task 2 Step 2; item 3 → Tasks 2-7; item 4 → Task 5 (gated); item 5 → Tasks 2-7. The MIN/MAX segment-time item was removed at user request (buggy approach) — array_agg stays. Verification promise (EXPLAIN/timings) → Tasks 1 & 8 (wall-clock medians; EXPLAIN not visible inside plpgsql — documented substitution).
- Type consistency: `schedule_ranked` year filter is `::text` in onoff_compute (text param), int elsewhere, nullable form in the lineups pair — matches each function's existing year semantics.
- Output equivalence is a hard gate: zero diffs required on all 15 cases; the only edit that could have changed output (Task 5 Step 2) is gated by D3+D4 proofs.
