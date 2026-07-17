# Shot Profile — Plan B: SQL layer (MV columns + filtered-path functions) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add shot-diet count columns (lay-up / dunk / corner-3, with a corner-known denominator) to both Tab-1 fast+filtered paths and both Tab-3 fast+filtered paths, so Plan C (Shiny UI) can render Shot Profile from either path.

**Architecture:** Fast paths get new pre-computed columns: `onoff_default_mv` (a CTAS **table** with incremental refresh fn) and `team_ppp_ratings_mv` (a real MV). Filtered paths: `onoff_compute` SUMs per-game columns that must first be added to `player_four_factors_by_game` (+ its incremental refresh fn); `get_team_ratings_dynamic` aggregates action rows directly and takes the flags inline. Corner-3 comes from `LEFT JOIN basketball_test.shot_zones z ON (z.game_id, z.id) = (game_id, id)` (Plan A table, complete for all 439 games). Two branches, independently mergeable: `sql/shot-profile-mv` (Tasks 1–6), then `sql/shot-profile-fns` (Tasks 7–9).

**Tech Stack:** PostgreSQL (Supabase), plpgsql, R 4.4.2 (`"/c/Program Files/R/R-4.4.2/bin/Rscript.exe"`; PowerShell: `& "C:\Program Files\R\R-4.4.2\bin\Rscript.exe"`), DBI/RPostgres.

## Spec deviations (deliberate, discovered while planning)

1. **`onoff_compute` reads `player_four_factors_by_game`, not `df_pts_poss_lineups_longer_mv`** (spec §4 predates the 2026-07-14 pobg merge). Per the architecture rule "SQL functions only SUM pre-computed columns from MVs", the per-game counts are added to `player_four_factors_by_game` and `onoff_compute` just SUMs them.
2. **+`c3_known_att` columns** (spec's 24 onoff columns → **28**; 10 team columns → **12**). `c3_known_att` = 3PA whose corner flag is known (row exists in `shot_zones`). This implements the spec's fail-open rule at aggregate level: corner share = `c3_att / c3_known_att`, render "—" when `c3_known_att` is 0; `ab3 = c3_known_att − c3_att` (unknowns excluded from the split, never misclassified).
3. **`onoff_default_mv` and `player_four_factors_by_game` are tables with incremental refresh functions** (`refresh_onoff_default_for_games`, `refresh_player_four_factors_by_game_for_games`) that duplicate the CTAS query — they MUST be updated in lockstep or the next ETL silently writes NULLs into the new columns.
4. **PROJECT.md React-drift note deferred to Plan C** (PROJECT.md currently holds uncommitted user WIP; don't touch it).
5. **Rim tags are constrained to 2PA.** Live validation found eight mirrored rows tagged `lay-up` with `parameters_points = 3`. Because Plan C derives `mid = fga - rim - fg3` and requires `rim <= fg2`, lay-up/dunk predicates also require `parameters_points = 2`; malformed 3-point lay-up tags remain classified only as 3PA.

## Global Constraints

- Schema `basketball_test` everywhere. MV/CTAS files use unqualified table names (rebuild sets `search_path`); function files use `basketball_test.`-qualified names — match each file's existing style.
- Edit SQL files with **exact strings only, never regex**; after every scripted edit run `git diff --stat` and sanity-check the line delta printed in the task.
- DDL (rebuilds, CREATE FUNCTION) on **port 5432**, same pooler host; read checks can use 6543. Credentials `readRenviron("etl/.Renviron")` for DDL/write, `app/.Renviron` for read-only verification.
- Long `Rscript -e` segfaults — write temp .R files to the scratchpad and run them (never repo root).
- New column vocabulary (used identically everywhere):
  - `layup_att/made`: `type = 'shot' AND parameters_points = 2 AND parameters_type = 'lay-up'` (+ `parameters_made = 'made'`)
  - `dunk_att/made`: `type = 'shot' AND parameters_points = 2 AND parameters_type IN ('dunk', 'allyhoop')`
  - `c3_att/made`: `type = 'shot' AND parameters_points = 3 AND z.is_corner3 IS TRUE`
  - `c3_known_att`: `type = 'shot' AND parameters_points = 3 AND z.is_corner3 IS NOT NULL`
  - team-side extra: `fga`: `type = 'shot'`
  - Per-cell metric order everywhere: `layup_made, layup_att, dunk_made, dunk_att, c3_made, c3_att, c3_known_att`. Cell order: `off_on, off_off, def_on, def_off`.
- Commits end with:
  `Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>`
- Expected league invariants (from Plan A verification): corner 3s by season 2025 = 1,035 / 2026 = 1,150; 3PA with known flag 2025 = 11,497 / 2026 = 12,091; corner share ≈ 9.0–9.5%.

---

### Task 1: Baseline snapshot (read-only, BEFORE any change)

**Files:**
- Create: `<scratchpad>/planb_baseline.R` (temp, not committed)

**Interfaces:**
- Produces: `<scratchpad>/planb_baseline_out.txt` — row counts + timings that Tasks 6 and 9 compare against.

- [ ] **Step 1: Create branch**

```bash
git checkout main && git pull && git checkout -b sql/shot-profile-mv
```

- [ ] **Step 2: Write and run the baseline script**

Write `<scratchpad>/planb_baseline.R`:

```r
# Baseline before Plan B: row counts + filtered-path timings + dependency check.
suppressMessages({library(DBI); library(RPostgres)})
readRenviron("app/.Renviron")
con <- dbConnect(RPostgres::Postgres(),
  host=Sys.getenv("PG_HOST"), port=as.integer(Sys.getenv("PG_PORT")),
  dbname=Sys.getenv("PG_DB"), user=Sys.getenv("PG_USER"),
  password=Sys.getenv("PG_PASS"), sslmode=Sys.getenv("PG_SSLMODE"),
  connect_timeout=15L, bigint="numeric")

sink("<scratchpad>/planb_baseline_out.txt", split = TRUE)  # substitute the real scratchpad path

cat("== row counts ==\n")
print(dbGetQuery(con, "
  SELECT 'onoff_default_mv' AS t, \"Year\" AS yr, count(*) FROM basketball_test.onoff_default_mv GROUP BY 2
  UNION ALL SELECT 'player_four_factors_by_game', game_year, count(*) FROM basketball_test.player_four_factors_by_game GROUP BY 2
  UNION ALL SELECT 'team_ppp_ratings_mv', game_year, count(*) FROM basketball_test.team_ppp_ratings_mv GROUP BY 2
  ORDER BY 1, 2"))

cat("\n== MV-on-MV dependency check (expect: no L3/L4 MV reads onoff_default_mv or team_ppp_ratings_mv) ==\n")
print(dbGetQuery(con, "
  SELECT DISTINCT dependent.relname AS dependent_mv, source.relname AS source_rel
  FROM pg_depend d
  JOIN pg_rewrite r ON r.oid = d.objid
  JOIN pg_class dependent ON dependent.oid = r.ev_class
  JOIN pg_class source ON source.oid = d.refobjid
  JOIN pg_namespace n ON n.oid = source.relnamespace
  WHERE n.nspname = 'basketball_test'
    AND source.relname IN ('onoff_default_mv', 'team_ppp_ratings_mv')
    AND dependent.relname <> source.relname"))

cat("\n== filtered-path timings (3 runs each, record the medians) ==\n")
for (i in 1:3) print(system.time(dbGetQuery(con, "
  SELECT * FROM basketball_test.onoff_compute(
    make_date(2025,10,1), make_date(2026,7,1), NULL, 0, 0, -1000, '2026',
    NULL, NULL, 'all', 'all', NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")))
for (i in 1:3) print(system.time(dbGetQuery(con, "
  SELECT * FROM basketball_test.get_team_ratings_dynamic(2026)")))
sink()
dbDisconnect(con)
```

Run (PowerShell, repo root): `& "C:\Program Files\R\R-4.4.2\bin\Rscript.exe" <scratchpad>/planb_baseline.R`
Expected: dependency query returns **0 rows** (both objects are leaves — hard requirement before the DROP+CREATE in Task 6); onoff_compute ≈ 2–8 s, get_team_ratings_dynamic ≈ 1–5 s. **Record the numbers.** If the dependency query returns rows, STOP — the rebuild order assumption is wrong; re-plan Task 6 before proceeding.

---

### Task 2: `onoff_default_mv` CTAS — 28 shot-profile columns

**Files:**
- Modify: `sql/materialized_views/onoff_mv.sql`

**Interfaces:**
- Produces: table columns `{off,def}_{on,off}_{layup_made,layup_att,dunk_made,dunk_att,c3_made,c3_att,c3_known_att}` (28, bigint) on `basketball_test.onoff_default_mv`. Consumed by Plan C Tab 1 fast path (`SELECT *` in `app/R/server_tab1.R` — no R change needed in Plan B) and mirrored by Task 3.

- [ ] **Step 1: Add action flags + shot_zones join to the `base` CTE**

In `sql/materialized_views/onoff_mv.sql`, replace this exact block:

```sql
            CASE WHEN d.type = 'shot' AND d.parameters_points = 3 AND d.parameters_made = 'made' THEN 1 ELSE 0 END AS fg3_made_flag,
            CASE WHEN d.type = 'shot' AND d.parameters_points = 3 THEN 1 ELSE 0 END AS fg3_att_flag
           FROM base0 b0
             JOIN df_pts_poss_lineups_longer_mv d USING (lineup_hash)
             JOIN sched s USING (game_id)
```

with:

```sql
            CASE WHEN d.type = 'shot' AND d.parameters_points = 3 AND d.parameters_made = 'made' THEN 1 ELSE 0 END AS fg3_made_flag,
            CASE WHEN d.type = 'shot' AND d.parameters_points = 3 THEN 1 ELSE 0 END AS fg3_att_flag,
            CASE WHEN d.type = 'shot' AND d.parameters_points = 2 AND d.parameters_type = 'lay-up' AND d.parameters_made = 'made' THEN 1 ELSE 0 END AS layup_made_flag,
            CASE WHEN d.type = 'shot' AND d.parameters_points = 2 AND d.parameters_type = 'lay-up' THEN 1 ELSE 0 END AS layup_att_flag,
            CASE WHEN d.type = 'shot' AND d.parameters_points = 2 AND d.parameters_type IN ('dunk', 'allyhoop') AND d.parameters_made = 'made' THEN 1 ELSE 0 END AS dunk_made_flag,
            CASE WHEN d.type = 'shot' AND d.parameters_points = 2 AND d.parameters_type IN ('dunk', 'allyhoop') THEN 1 ELSE 0 END AS dunk_att_flag,
            CASE WHEN d.type = 'shot' AND d.parameters_points = 3 AND z.is_corner3 IS TRUE AND d.parameters_made = 'made' THEN 1 ELSE 0 END AS c3_made_flag,
            CASE WHEN d.type = 'shot' AND d.parameters_points = 3 AND z.is_corner3 IS TRUE THEN 1 ELSE 0 END AS c3_att_flag,
            CASE WHEN d.type = 'shot' AND d.parameters_points = 3 AND z.is_corner3 IS NOT NULL THEN 1 ELSE 0 END AS c3_known_att_flag
           FROM base0 b0
             JOIN df_pts_poss_lineups_longer_mv d USING (lineup_hash)
             JOIN sched s USING (game_id)
             LEFT JOIN shot_zones z ON z.game_id = d.game_id AND z.id = d.id
```

- [ ] **Step 2: Sum the flags in the `shot_agg` CTE**

Replace:

```sql
            sum(base.fg2_made_flag) AS fg2_made,
            sum(base.fg2_att_flag) AS fg2_att,
            sum(base.fg3_made_flag) AS fg3_made,
            sum(base.fg3_att_flag) AS fg3_att
           FROM base
```

with:

```sql
            sum(base.fg2_made_flag) AS fg2_made,
            sum(base.fg2_att_flag) AS fg2_att,
            sum(base.fg3_made_flag) AS fg3_made,
            sum(base.fg3_att_flag) AS fg3_att,
            sum(base.layup_made_flag) AS layup_made,
            sum(base.layup_att_flag) AS layup_att,
            sum(base.dunk_made_flag) AS dunk_made,
            sum(base.dunk_att_flag) AS dunk_att,
            sum(base.c3_made_flag) AS c3_made,
            sum(base.c3_att_flag) AS c3_att,
            sum(base.c3_known_att_flag) AS c3_known_att
           FROM base
```

- [ ] **Step 3: Pivot in `final_rows`**

Replace:

```sql
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 0 THEN sa.fg3_made END) AS def_off_fg3_made,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 0 THEN sa.fg3_att END)  AS def_off_fg3_att
           FROM step2_joined s2j
```

with:

```sql
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 0 THEN sa.fg3_made END) AS def_off_fg3_made,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 0 THEN sa.fg3_att END)  AS def_off_fg3_att,
            -- Shot Profile counts (28 columns; c3_known_att = 3PA with known corner flag)
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 1 THEN sa.layup_made END) AS off_on_layup_made,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 1 THEN sa.layup_att END)  AS off_on_layup_att,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 1 THEN sa.dunk_made END) AS off_on_dunk_made,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 1 THEN sa.dunk_att END)  AS off_on_dunk_att,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 1 THEN sa.c3_made END) AS off_on_c3_made,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 1 THEN sa.c3_att END)  AS off_on_c3_att,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 1 THEN sa.c3_known_att END) AS off_on_c3_known_att,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 0 THEN sa.layup_made END) AS off_off_layup_made,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 0 THEN sa.layup_att END)  AS off_off_layup_att,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 0 THEN sa.dunk_made END) AS off_off_dunk_made,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 0 THEN sa.dunk_att END)  AS off_off_dunk_att,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 0 THEN sa.c3_made END) AS off_off_c3_made,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 0 THEN sa.c3_att END)  AS off_off_c3_att,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 0 THEN sa.c3_known_att END) AS off_off_c3_known_att,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 1 THEN sa.layup_made END) AS def_on_layup_made,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 1 THEN sa.layup_att END)  AS def_on_layup_att,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 1 THEN sa.dunk_made END) AS def_on_dunk_made,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 1 THEN sa.dunk_att END)  AS def_on_dunk_att,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 1 THEN sa.c3_made END) AS def_on_c3_made,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 1 THEN sa.c3_att END)  AS def_on_c3_att,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 1 THEN sa.c3_known_att END) AS def_on_c3_known_att,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 0 THEN sa.layup_made END) AS def_off_layup_made,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 0 THEN sa.layup_att END)  AS def_off_layup_att,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 0 THEN sa.dunk_made END) AS def_off_dunk_made,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 0 THEN sa.dunk_att END)  AS def_off_dunk_att,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 0 THEN sa.c3_made END) AS def_off_c3_made,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 0 THEN sa.c3_att END)  AS def_off_c3_att,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 0 THEN sa.c3_known_att END) AS def_off_c3_known_att
           FROM step2_joined s2j
```

- [ ] **Step 4: Carry through `final_scored`**

Replace:

```sql
            fr.def_off_fg2_made, fr.def_off_fg2_att, fr.def_off_fg3_made, fr.def_off_fg3_att,
            fr.offense_on_ppp - fr.defense_on_ppp AS on_net_rtg,
```

with:

```sql
            fr.def_off_fg2_made, fr.def_off_fg2_att, fr.def_off_fg3_made, fr.def_off_fg3_att,
            fr.off_on_layup_made, fr.off_on_layup_att, fr.off_on_dunk_made, fr.off_on_dunk_att,
            fr.off_on_c3_made, fr.off_on_c3_att, fr.off_on_c3_known_att,
            fr.off_off_layup_made, fr.off_off_layup_att, fr.off_off_dunk_made, fr.off_off_dunk_att,
            fr.off_off_c3_made, fr.off_off_c3_att, fr.off_off_c3_known_att,
            fr.def_on_layup_made, fr.def_on_layup_att, fr.def_on_dunk_made, fr.def_on_dunk_att,
            fr.def_on_c3_made, fr.def_on_c3_att, fr.def_on_c3_known_att,
            fr.def_off_layup_made, fr.def_off_layup_att, fr.def_off_dunk_made, fr.def_off_dunk_att,
            fr.def_off_c3_made, fr.def_off_c3_att, fr.def_off_c3_known_att,
            fr.offense_on_ppp - fr.defense_on_ppp AS on_net_rtg,
```

- [ ] **Step 5: Add to the outer SELECT (defines table column order)**

Replace:

```sql
    def_off_fg2_made, def_off_fg2_att, def_off_fg3_made, def_off_fg3_att,
   player_id,
   team_id
```

with:

```sql
    def_off_fg2_made, def_off_fg2_att, def_off_fg3_made, def_off_fg3_att,
    off_on_layup_made, off_on_layup_att, off_on_dunk_made, off_on_dunk_att, off_on_c3_made, off_on_c3_att, off_on_c3_known_att,
    off_off_layup_made, off_off_layup_att, off_off_dunk_made, off_off_dunk_att, off_off_c3_made, off_off_c3_att, off_off_c3_known_att,
    def_on_layup_made, def_on_layup_att, def_on_dunk_made, def_on_dunk_att, def_on_c3_made, def_on_c3_att, def_on_c3_known_att,
    def_off_layup_made, def_off_layup_att, def_off_dunk_made, def_off_dunk_att, def_off_c3_made, def_off_c3_att, def_off_c3_known_att,
   player_id,
   team_id
```

- [ ] **Step 6: Verify diff plausibility and commit**

Run: `git diff --stat sql/materialized_views/onoff_mv.sql`
Expected: 1 file, roughly **+80/−4** lines. Anything wildly different (e.g. hundreds of deletions) means an edit clobbered the file — `git checkout -- <file>` and redo.

```bash
git add sql/materialized_views/onoff_mv.sql
git commit -m "feat(sql): shot-profile columns in onoff_default_mv CTAS (28 cols)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 3: `refresh_onoff_default_for_games` — mirror the 28 columns

**Files:**
- Modify: `sql/functions/refresh_onoff_default_for_games.sql`

**Interfaces:**
- Consumes: the column names/order defined in Task 2 (INSERT list must stay aligned with its inner SELECT).
- Produces: incremental refresh that fills all 28 new columns (used by ETL Phase 4 `[INC] onoff_default_mv`).

- [ ] **Step 1: Extend the INSERT column list**

Replace:

```sql
    def_off_fg2_made,def_off_fg2_att,def_off_fg3_made,def_off_fg3_att,
    player_id,team_id
  )
```

with:

```sql
    def_off_fg2_made,def_off_fg2_att,def_off_fg3_made,def_off_fg3_att,
    off_on_layup_made,off_on_layup_att,off_on_dunk_made,off_on_dunk_att,off_on_c3_made,off_on_c3_att,off_on_c3_known_att,
    off_off_layup_made,off_off_layup_att,off_off_dunk_made,off_off_dunk_att,off_off_c3_made,off_off_c3_att,off_off_c3_known_att,
    def_on_layup_made,def_on_layup_att,def_on_dunk_made,def_on_dunk_att,def_on_c3_made,def_on_c3_att,def_on_c3_known_att,
    def_off_layup_made,def_off_layup_att,def_off_dunk_made,def_off_dunk_att,def_off_c3_made,def_off_c3_att,def_off_c3_known_att,
    player_id,team_id
  )
```

- [ ] **Step 2: Add action flags + shot_zones join to the `base` CTE**

Replace:

```sql
            CASE WHEN d.type = 'shot' AND d.parameters_points = 3 AND d.parameters_made = 'made' THEN 1 ELSE 0 END AS fg3_made_flag,
            CASE WHEN d.type = 'shot' AND d.parameters_points = 3 THEN 1 ELSE 0 END AS fg3_att_flag
           FROM base0 b0
             JOIN basketball_test.df_pts_poss_lineups_longer_mv d USING (lineup_hash)
             JOIN sched s USING (game_id)
```

with:

```sql
            CASE WHEN d.type = 'shot' AND d.parameters_points = 3 AND d.parameters_made = 'made' THEN 1 ELSE 0 END AS fg3_made_flag,
            CASE WHEN d.type = 'shot' AND d.parameters_points = 3 THEN 1 ELSE 0 END AS fg3_att_flag,
            CASE WHEN d.type = 'shot' AND d.parameters_points = 2 AND d.parameters_type = 'lay-up' AND d.parameters_made = 'made' THEN 1 ELSE 0 END AS layup_made_flag,
            CASE WHEN d.type = 'shot' AND d.parameters_points = 2 AND d.parameters_type = 'lay-up' THEN 1 ELSE 0 END AS layup_att_flag,
            CASE WHEN d.type = 'shot' AND d.parameters_points = 2 AND d.parameters_type IN ('dunk', 'allyhoop') AND d.parameters_made = 'made' THEN 1 ELSE 0 END AS dunk_made_flag,
            CASE WHEN d.type = 'shot' AND d.parameters_points = 2 AND d.parameters_type IN ('dunk', 'allyhoop') THEN 1 ELSE 0 END AS dunk_att_flag,
            CASE WHEN d.type = 'shot' AND d.parameters_points = 3 AND z.is_corner3 IS TRUE AND d.parameters_made = 'made' THEN 1 ELSE 0 END AS c3_made_flag,
            CASE WHEN d.type = 'shot' AND d.parameters_points = 3 AND z.is_corner3 IS TRUE THEN 1 ELSE 0 END AS c3_att_flag,
            CASE WHEN d.type = 'shot' AND d.parameters_points = 3 AND z.is_corner3 IS NOT NULL THEN 1 ELSE 0 END AS c3_known_att_flag
           FROM base0 b0
             JOIN basketball_test.df_pts_poss_lineups_longer_mv d USING (lineup_hash)
             JOIN sched s USING (game_id)
             LEFT JOIN basketball_test.shot_zones z ON z.game_id = d.game_id AND z.id = d.id
```

- [ ] **Step 3: Sum in `shot_agg`**

Apply exactly the same replacement as Task 2 Step 2 (the block text is identical in this file):

Replace:

```sql
            sum(base.fg2_made_flag) AS fg2_made,
            sum(base.fg2_att_flag) AS fg2_att,
            sum(base.fg3_made_flag) AS fg3_made,
            sum(base.fg3_att_flag) AS fg3_att
           FROM base
```

with:

```sql
            sum(base.fg2_made_flag) AS fg2_made,
            sum(base.fg2_att_flag) AS fg2_att,
            sum(base.fg3_made_flag) AS fg3_made,
            sum(base.fg3_att_flag) AS fg3_att,
            sum(base.layup_made_flag) AS layup_made,
            sum(base.layup_att_flag) AS layup_att,
            sum(base.dunk_made_flag) AS dunk_made,
            sum(base.dunk_att_flag) AS dunk_att,
            sum(base.c3_made_flag) AS c3_made,
            sum(base.c3_att_flag) AS c3_att,
            sum(base.c3_known_att_flag) AS c3_known_att
           FROM base
```

- [ ] **Step 4: Pivot in `final_rows`**

Apply exactly the same replacement as Task 2 Step 3 — the anchor block (`MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 0 THEN sa.fg3_made END) AS def_off_fg3_made,` … `FROM step2_joined s2j`) is identical in this file; insert the same 28 `MAX(...)` lines (with the `-- Shot Profile counts` comment) before `FROM step2_joined s2j`.

- [ ] **Step 5: Carry through `final_scored`**

Apply exactly the same replacement as Task 2 Step 4 (identical anchor text in this file).

- [ ] **Step 6: Add to the inner final SELECT (must match INSERT list order)**

Replace:

```sql
    def_off_fg2_made, def_off_fg2_att, def_off_fg3_made, def_off_fg3_att,
    player_id,
    team_id
   FROM final_scored
```

with:

```sql
    def_off_fg2_made, def_off_fg2_att, def_off_fg3_made, def_off_fg3_att,
    off_on_layup_made, off_on_layup_att, off_on_dunk_made, off_on_dunk_att, off_on_c3_made, off_on_c3_att, off_on_c3_known_att,
    off_off_layup_made, off_off_layup_att, off_off_dunk_made, off_off_dunk_att, off_off_c3_made, off_off_c3_att, off_off_c3_known_att,
    def_on_layup_made, def_on_layup_att, def_on_dunk_made, def_on_dunk_att, def_on_c3_made, def_on_c3_att, def_on_c3_known_att,
    def_off_layup_made, def_off_layup_att, def_off_dunk_made, def_off_dunk_att, def_off_c3_made, def_off_c3_att, def_off_c3_known_att,
    player_id,
    team_id
   FROM final_scored
```

- [ ] **Step 7: Verify diff and commit**

Run: `git diff --stat sql/functions/refresh_onoff_default_for_games.sql`
Expected: roughly **+88/−6**. Count-check: the INSERT list and the inner SELECT must both have exactly 28 new names in the same order (`grep -c "layup_made\|layup_att\|dunk_made\|dunk_att\|c3_made\|c3_att\|c3_known_att" sql/functions/refresh_onoff_default_for_games.sql` should print a number consistent with flags+sums+pivot+carry+insert+select).

```bash
git add sql/functions/refresh_onoff_default_for_games.sql
git commit -m "feat(sql): mirror shot-profile columns in refresh_onoff_default_for_games

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 4: `player_four_factors_by_game` + its refresh fn — 7 per-game columns

**Files:**
- Modify: `sql/materialized_views/player_four_factors_by_game.sql`
- Modify: `sql/functions/refresh_player_four_factors_by_game_for_games.sql`

**Interfaces:**
- Produces: per-game int columns `layup_made, layup_att, dunk_made, dunk_att, c3_made, c3_att, c3_known_att` on `basketball_test.player_four_factors_by_game` (NULL when the row has no `onoff_player` match, same as existing `fg2_*`). Task 7's `onoff_compute` SUMs these.

- [ ] **Step 1: CTAS — extend `lineup_totals` (flags + shot_zones join)**

In `sql/materialized_views/player_four_factors_by_game.sql`, replace:

```sql
    SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 3 AND cs.parameters_made = 'made' THEN 1 ELSE 0 END) AS fg3_made,
    SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 3 THEN 1 ELSE 0 END) AS fg3_att
  FROM clean_stats cs
  JOIN basketball_test.schedule s ON s.game_id = cs.game_id
  GROUP BY cs.game_id, s.game_year, cs.team_id, cs.lineup_hash, cs.type_lineup, cs.own_starters, cs.opp_starters
```

with:

```sql
    SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 3 AND cs.parameters_made = 'made' THEN 1 ELSE 0 END) AS fg3_made,
    SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 3 THEN 1 ELSE 0 END) AS fg3_att,
    SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 2 AND cs.parameters_type = 'lay-up' AND cs.parameters_made = 'made' THEN 1 ELSE 0 END) AS layup_made,
    SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 2 AND cs.parameters_type = 'lay-up' THEN 1 ELSE 0 END) AS layup_att,
    SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 2 AND cs.parameters_type IN ('dunk', 'allyhoop') AND cs.parameters_made = 'made' THEN 1 ELSE 0 END) AS dunk_made,
    SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 2 AND cs.parameters_type IN ('dunk', 'allyhoop') THEN 1 ELSE 0 END) AS dunk_att,
    SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 3 AND z.is_corner3 IS TRUE AND cs.parameters_made = 'made' THEN 1 ELSE 0 END) AS c3_made,
    SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 3 AND z.is_corner3 IS TRUE THEN 1 ELSE 0 END) AS c3_att,
    SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 3 AND z.is_corner3 IS NOT NULL THEN 1 ELSE 0 END) AS c3_known_att
  FROM clean_stats cs
  JOIN basketball_test.schedule s ON s.game_id = cs.game_id
  LEFT JOIN basketball_test.shot_zones z ON z.game_id = cs.game_id AND z.id = cs.id
  GROUP BY cs.game_id, s.game_year, cs.team_id, cs.lineup_hash, cs.type_lineup, cs.own_starters, cs.opp_starters
```

- [ ] **Step 2: CTAS — sum in `onoff_player`**

Replace:

```sql
    SUM(lt.fg2_made) AS fg2_made,
    SUM(lt.fg2_att) AS fg2_att,
    SUM(lt.fg3_made) AS fg3_made,
    SUM(lt.fg3_att) AS fg3_att,
    SUM(COALESCE(lm.minutes, 0)) AS onoff_minutes
```

with:

```sql
    SUM(lt.fg2_made) AS fg2_made,
    SUM(lt.fg2_att) AS fg2_att,
    SUM(lt.fg3_made) AS fg3_made,
    SUM(lt.fg3_att) AS fg3_att,
    SUM(lt.layup_made) AS layup_made,
    SUM(lt.layup_att) AS layup_att,
    SUM(lt.dunk_made) AS dunk_made,
    SUM(lt.dunk_att) AS dunk_att,
    SUM(lt.c3_made) AS c3_made,
    SUM(lt.c3_att) AS c3_att,
    SUM(lt.c3_known_att) AS c3_known_att,
    SUM(COALESCE(lm.minutes, 0)) AS onoff_minutes
```

- [ ] **Step 3: CTAS — outer SELECT**

Replace:

```sql
  op.fg2_made::int AS fg2_made,
  op.fg2_att::int AS fg2_att,
  op.fg3_made::int AS fg3_made,
  op.fg3_att::int AS fg3_att,
  op.onoff_minutes
FROM ff
```

with:

```sql
  op.fg2_made::int AS fg2_made,
  op.fg2_att::int AS fg2_att,
  op.fg3_made::int AS fg3_made,
  op.fg3_att::int AS fg3_att,
  op.layup_made::int AS layup_made,
  op.layup_att::int AS layup_att,
  op.dunk_made::int AS dunk_made,
  op.dunk_att::int AS dunk_att,
  op.c3_made::int AS c3_made,
  op.c3_att::int AS c3_att,
  op.c3_known_att::int AS c3_known_att,
  op.onoff_minutes
FROM ff
```

- [ ] **Step 4: Refresh fn — INSERT column list**

In `sql/functions/refresh_player_four_factors_by_game_for_games.sql`, replace:

```sql
    fg2_made, fg2_att, fg3_made, fg3_att, onoff_minutes
  )
```

with:

```sql
    fg2_made, fg2_att, fg3_made, fg3_att,
    layup_made, layup_att, dunk_made, dunk_att, c3_made, c3_att, c3_known_att,
    onoff_minutes
  )
```

- [ ] **Step 5: Refresh fn — `lineup_totals` flags + join**

Replace:

```sql
      SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 3 AND cs.parameters_made = 'made' THEN 1 ELSE 0 END) AS fg3_made,
      SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 3 THEN 1 ELSE 0 END) AS fg3_att
    FROM clean_stats cs
    JOIN basketball_test.schedule s ON s.game_id = cs.game_id
    GROUP BY cs.game_id, s.game_year, cs.team_id, cs.lineup_hash, cs.type_lineup, cs.own_starters, cs.opp_starters
```

with:

```sql
      SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 3 AND cs.parameters_made = 'made' THEN 1 ELSE 0 END) AS fg3_made,
      SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 3 THEN 1 ELSE 0 END) AS fg3_att,
      SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 2 AND cs.parameters_type = 'lay-up' AND cs.parameters_made = 'made' THEN 1 ELSE 0 END) AS layup_made,
      SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 2 AND cs.parameters_type = 'lay-up' THEN 1 ELSE 0 END) AS layup_att,
      SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 2 AND cs.parameters_type IN ('dunk', 'allyhoop') AND cs.parameters_made = 'made' THEN 1 ELSE 0 END) AS dunk_made,
      SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 2 AND cs.parameters_type IN ('dunk', 'allyhoop') THEN 1 ELSE 0 END) AS dunk_att,
      SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 3 AND z.is_corner3 IS TRUE AND cs.parameters_made = 'made' THEN 1 ELSE 0 END) AS c3_made,
      SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 3 AND z.is_corner3 IS TRUE THEN 1 ELSE 0 END) AS c3_att,
      SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 3 AND z.is_corner3 IS NOT NULL THEN 1 ELSE 0 END) AS c3_known_att
    FROM clean_stats cs
    JOIN basketball_test.schedule s ON s.game_id = cs.game_id
    LEFT JOIN basketball_test.shot_zones z ON z.game_id = cs.game_id AND z.id = cs.id
    GROUP BY cs.game_id, s.game_year, cs.team_id, cs.lineup_hash, cs.type_lineup, cs.own_starters, cs.opp_starters
```

- [ ] **Step 6: Refresh fn — `onoff_player` sums**

Replace:

```sql
      SUM(lt.fg2_made) AS fg2_made,
      SUM(lt.fg2_att) AS fg2_att,
      SUM(lt.fg3_made) AS fg3_made,
      SUM(lt.fg3_att) AS fg3_att,
      SUM(COALESCE(lm.minutes, 0)) AS onoff_minutes
```

with:

```sql
      SUM(lt.fg2_made) AS fg2_made,
      SUM(lt.fg2_att) AS fg2_att,
      SUM(lt.fg3_made) AS fg3_made,
      SUM(lt.fg3_att) AS fg3_att,
      SUM(lt.layup_made) AS layup_made,
      SUM(lt.layup_att) AS layup_att,
      SUM(lt.dunk_made) AS dunk_made,
      SUM(lt.dunk_att) AS dunk_att,
      SUM(lt.c3_made) AS c3_made,
      SUM(lt.c3_att) AS c3_att,
      SUM(lt.c3_known_att) AS c3_known_att,
      SUM(COALESCE(lm.minutes, 0)) AS onoff_minutes
```

- [ ] **Step 7: Refresh fn — final SELECT (aligned with INSERT list)**

Replace:

```sql
    op.fg2_made::int,
    op.fg2_att::int,
    op.fg3_made::int,
    op.fg3_att::int,
    op.onoff_minutes
  FROM ff
```

with:

```sql
    op.fg2_made::int,
    op.fg2_att::int,
    op.fg3_made::int,
    op.fg3_att::int,
    op.layup_made::int,
    op.layup_att::int,
    op.dunk_made::int,
    op.dunk_att::int,
    op.c3_made::int,
    op.c3_att::int,
    op.c3_known_att::int,
    op.onoff_minutes
  FROM ff
```

- [ ] **Step 8: Verify diff and commit**

Run: `git diff --stat`
Expected: 2 files, each roughly **+30/−3**.

```bash
git add sql/materialized_views/player_four_factors_by_game.sql sql/functions/refresh_player_four_factors_by_game_for_games.sql
git commit -m "feat(sql): per-game shot-profile counts in player_four_factors_by_game

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 5: `team_ppp_ratings_mv` — 12 team shot-diet columns

**Files:**
- Modify: `sql/materialized_views/team_ppp_ratings_mv.sql` (full-file rewrite — it is small)

**Interfaces:**
- Produces: MV columns `{off,def}_{fga,layup_att,dunk_att,fg3_att,c3_att,c3_known_att}` (12, bigint) on `basketball_test.team_ppp_ratings_mv`. Consumed by Plan C Tab 3 fast path and compared against Task 8's function output.

- [ ] **Step 1: Replace the entire file content with:**

```sql
-- basketball_test.team_ppp_ratings_mv source

CREATE MATERIALIZED VIEW basketball_test.team_ppp_ratings_mv
TABLESPACE pg_default
AS WITH base AS (
         SELECT s.game_year,
            dppllm.team_id,
            dppllm.type_lineup,
            sum(dppllm.team_score) / NULLIF(sum(dppllm.final_end_poss::integer), 0)::numeric AS ppp,
            sum(dppllm.final_end_poss::integer) AS total_poss,
            COUNT(DISTINCT dppllm.game_id) AS games_count,
            SUM(CASE WHEN dppllm.type = 'shot' THEN 1 ELSE 0 END) AS fga,
            SUM(CASE WHEN dppllm.type = 'shot' AND dppllm.parameters_points = 2 AND dppllm.parameters_type = 'lay-up' THEN 1 ELSE 0 END) AS layup_att,
            SUM(CASE WHEN dppllm.type = 'shot' AND dppllm.parameters_points = 2 AND dppllm.parameters_type IN ('dunk', 'allyhoop') THEN 1 ELSE 0 END) AS dunk_att,
            SUM(CASE WHEN dppllm.type = 'shot' AND dppllm.parameters_points = 3 THEN 1 ELSE 0 END) AS fg3_att,
            SUM(CASE WHEN dppllm.type = 'shot' AND dppllm.parameters_points = 3 AND z.is_corner3 IS TRUE THEN 1 ELSE 0 END) AS c3_att,
            SUM(CASE WHEN dppllm.type = 'shot' AND dppllm.parameters_points = 3 AND z.is_corner3 IS NOT NULL THEN 1 ELSE 0 END) AS c3_known_att
           FROM df_pts_poss_lineups_longer_mv dppllm
             JOIN schedule s USING (game_id)
             LEFT JOIN shot_zones z ON z.game_id = dppllm.game_id AND z.id = dppllm.id
          GROUP BY s.game_year, dppllm.team_id, dppllm.type_lineup
        ), win_loss AS (
         SELECT fs.game_year,
            fs.team_id,
            COUNT(*) FILTER (WHERE fs.has_won = TRUE) AS wins,
            COUNT(*) FILTER (WHERE fs.has_won = FALSE) AS losses
           FROM final_schedule_mv fs
          GROUP BY fs.game_year, fs.team_id
        ), pivoted AS (
         SELECT base.game_year,
            base.team_id,
            max(base.ppp) FILTER (WHERE base.type_lineup = 'offense'::text) AS off_ppp_raw,
            max(base.ppp) FILTER (WHERE base.type_lineup = 'defense'::text) AS def_ppp_raw,
            max(base.games_count) AS games_played,
            max(base.total_poss) FILTER (WHERE base.type_lineup = 'offense'::text) AS off_poss,
            max(base.total_poss) FILTER (WHERE base.type_lineup = 'defense'::text) AS def_poss,
            max(base.fga) FILTER (WHERE base.type_lineup = 'offense'::text) AS off_fga,
            max(base.layup_att) FILTER (WHERE base.type_lineup = 'offense'::text) AS off_layup_att,
            max(base.dunk_att) FILTER (WHERE base.type_lineup = 'offense'::text) AS off_dunk_att,
            max(base.fg3_att) FILTER (WHERE base.type_lineup = 'offense'::text) AS off_fg3_att,
            max(base.c3_att) FILTER (WHERE base.type_lineup = 'offense'::text) AS off_c3_att,
            max(base.c3_known_att) FILTER (WHERE base.type_lineup = 'offense'::text) AS off_c3_known_att,
            max(base.fga) FILTER (WHERE base.type_lineup = 'defense'::text) AS def_fga,
            max(base.layup_att) FILTER (WHERE base.type_lineup = 'defense'::text) AS def_layup_att,
            max(base.dunk_att) FILTER (WHERE base.type_lineup = 'defense'::text) AS def_dunk_att,
            max(base.fg3_att) FILTER (WHERE base.type_lineup = 'defense'::text) AS def_fg3_att,
            max(base.c3_att) FILTER (WHERE base.type_lineup = 'defense'::text) AS def_c3_att,
            max(base.c3_known_att) FILTER (WHERE base.type_lineup = 'defense'::text) AS def_c3_known_att
           FROM base
          GROUP BY base.game_year, base.team_id
        ), teams AS (
         SELECT DISTINCT full_rosters.game_year,
            full_rosters.team_id,
            full_rosters.team_name
           FROM full_rosters
        ), final AS (
         SELECT p.game_year,
            t.team_id,
            t.team_name,
            round(p.off_ppp_raw, 3) * 100::numeric AS off_ppp,
            round(p.def_ppp_raw, 3) * 100::numeric AS def_ppp,
            round(p.off_ppp_raw - p.def_ppp_raw, 3) * 100::numeric AS net_rtg,
            p.games_played,
            wl.wins,
            wl.losses,
            p.off_poss,
            p.def_poss,
            p.off_fga,
            p.off_layup_att,
            p.off_dunk_att,
            p.off_fg3_att,
            p.off_c3_att,
            p.off_c3_known_att,
            p.def_fga,
            p.def_layup_att,
            p.def_dunk_att,
            p.def_fg3_att,
            p.def_c3_att,
            p.def_c3_known_att
           FROM pivoted p
             JOIN teams t ON t.game_year = p.game_year AND t.team_id = p.team_id
             LEFT JOIN win_loss wl ON wl.game_year = p.game_year AND wl.team_id = p.team_id
        )
 SELECT game_year,
    team_id,
    team_name,
    off_ppp,
    def_ppp,
    net_rtg,
    games_played,
    wins,
    losses,
    off_poss,
    def_poss,
    dense_rank() OVER (PARTITION BY game_year ORDER BY net_rtg DESC NULLS LAST) AS rank_net_rtg,
    dense_rank() OVER (PARTITION BY game_year ORDER BY off_ppp DESC NULLS LAST) AS rank_off_ppp,
    dense_rank() OVER (PARTITION BY game_year ORDER BY def_ppp) AS rank_def_ppp,
    off_fga,
    off_layup_att,
    off_dunk_att,
    off_fg3_att,
    off_c3_att,
    off_c3_known_att,
    def_fga,
    def_layup_att,
    def_dunk_att,
    def_fg3_att,
    def_c3_att,
    def_c3_known_att
   FROM final
WITH DATA;

-- View indexes:
CREATE INDEX team_ppp_ratings_mv_join_idx ON basketball_test.team_ppp_ratings_mv USING btree (game_year, team_id);
```

- [ ] **Step 2: Verify diff and commit**

Run: `git diff --stat sql/materialized_views/team_ppp_ratings_mv.sql`
Expected: roughly **+50/−2** (the untouched middle survives byte-identical — if the whole file shows as rewritten, check line endings with `git diff --ignore-cr-at-eol`).

```bash
git add sql/materialized_views/team_ppp_ratings_mv.sql
git commit -m "feat(sql): team shot-diet counts in team_ppp_ratings_mv (12 cols)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 6: Deploy branch 1 (rebuild L2–L4 + refresh fns) + invariants + merge

**Files:**
- Create: `<scratchpad>/planb_deploy_mv.R`, `<scratchpad>/planb_invariants.R` (temp, not committed)

**Interfaces:**
- Consumes: Tasks 2–5 SQL files; `sql/rebuild_all_mvs.R` (`rebuild_all_mvs(from_level = 2)`).
- Produces: live DB with the new fast-path columns; `sql/shot-profile-mv` merged to main. Tasks 7–9 build on this.

**Pre-flight notes:** Only L1 (`df_pts_poss_longer.sql`) reads cold tables, so `from_level = 2` is safe with cold storage purged (verified 2026-07-17). The two refresh functions MUST be replaced in the same session as the rebuild — ETL Phase 4 calls them and the old versions would leave the new columns NULL.

- [ ] **Step 1: Rebuild L2–L4**

Write `<scratchpad>/planb_deploy_mv.R`:

```r
# Rebuild L2-L4 objects with new shot-profile columns, then replace the two
# incremental refresh functions. DDL on port 5432.
suppressMessages({library(DBI); library(RPostgres)})
readRenviron("etl/.Renviron")
source("sql/rebuild_all_mvs.R")   # uses PG_* env; connects itself on 5432
rebuild_all_mvs(from_level = 2)

deploy_fn <- function(pg, path) {
  txt <- paste(readLines(path, warn = FALSE), collapse = "\n")
  # Execute DROP statement(s) before CREATE separately; CREATE..$$..$$ as one.
  create_at <- regexpr("CREATE OR REPLACE FUNCTION", txt, fixed = TRUE)
  stopifnot(create_at > 0)
  head_part <- substr(txt, 1, create_at - 1)
  body_part <- substr(txt, create_at, nchar(txt))
  for (stmt in strsplit(head_part, ";", fixed = TRUE)[[1]]) {
    stmt <- trimws(stmt)
    if (nzchar(stmt)) DBI::dbExecute(pg, paste0(stmt, ";"))
  }
  DBI::dbExecute(pg, body_part)
  cat("deployed:", path, "\n")
}

pg <- dbConnect(RPostgres::Postgres(),
  host=Sys.getenv("PG_HOST"), port=5432L,
  dbname=Sys.getenv("PG_DB"), user=Sys.getenv("PG_USER"),
  password=Sys.getenv("PG_PASS"), sslmode="require")
deploy_fn(pg, "sql/functions/refresh_onoff_default_for_games.sql")
deploy_fn(pg, "sql/functions/refresh_player_four_factors_by_game_for_games.sql")
dbDisconnect(pg)
cat("DONE\n")
```

Run: `& "C:\Program Files\R\R-4.4.2\bin\Rscript.exe" <scratchpad>/planb_deploy_mv.R`
Expected: every registry object prints `CREATE ... OK` (no `ERROR:` lines — an error in `onoff_default_mv` or `team_ppp_ratings_mv` means a SQL typo in Tasks 2/5; fix and rerun), then `deployed:` twice. Full run ≈ 3–6 min.

- [ ] **Step 2: Run invariants**

Write `<scratchpad>/planb_invariants.R`:

```r
suppressMessages({library(DBI); library(RPostgres)})
readRenviron("app/.Renviron")
con <- dbConnect(RPostgres::Postgres(),
  host=Sys.getenv("PG_HOST"), port=as.integer(Sys.getenv("PG_PORT")),
  dbname=Sys.getenv("PG_DB"), user=Sys.getenv("PG_USER"),
  password=Sys.getenv("PG_PASS"), sslmode=Sys.getenv("PG_SSLMODE"),
  connect_timeout=15L, bigint="numeric")

cat("== A. row counts per year (compare to Task 1 baseline — must match) ==\n")
print(dbGetQuery(con, "
  SELECT 'onoff_default_mv' AS t, \"Year\" AS yr, count(*) FROM basketball_test.onoff_default_mv GROUP BY 2
  UNION ALL SELECT 'player_four_factors_by_game', game_year, count(*) FROM basketball_test.player_four_factors_by_game GROUP BY 2
  UNION ALL SELECT 'team_ppp_ratings_mv', game_year, count(*) FROM basketball_test.team_ppp_ratings_mv GROUP BY 2
  ORDER BY 1, 2"))

cat("\n== B. onoff cell invariants (expect all zeros) ==\n")
print(dbGetQuery(con, "
  SELECT
    count(*) FILTER (WHERE COALESCE(off_on_layup_att,0) + COALESCE(off_on_dunk_att,0) > COALESCE(off_on_fg2_att,0)
                     OR COALESCE(off_off_layup_att,0) + COALESCE(off_off_dunk_att,0) > COALESCE(off_off_fg2_att,0)
                     OR COALESCE(def_on_layup_att,0) + COALESCE(def_on_dunk_att,0) > COALESCE(def_on_fg2_att,0)
                     OR COALESCE(def_off_layup_att,0) + COALESCE(def_off_dunk_att,0) > COALESCE(def_off_fg2_att,0)) AS rim_gt_fg2,
    count(*) FILTER (WHERE COALESCE(off_on_c3_att,0) > COALESCE(off_on_c3_known_att,0)
                     OR COALESCE(off_on_c3_known_att,0) > COALESCE(off_on_fg3_att,0)
                     OR COALESCE(def_on_c3_att,0) > COALESCE(def_on_c3_known_att,0)
                     OR COALESCE(def_on_c3_known_att,0) > COALESCE(def_on_fg3_att,0)) AS c3_order,
    count(*) FILTER (WHERE COALESCE(off_on_c3_made,0) > COALESCE(off_on_c3_att,0)) AS c3_made_gt_att
  FROM basketball_test.onoff_default_mv"))

cat("\n== C. pff cell invariants (expect all zeros) ==\n")
print(dbGetQuery(con, "
  SELECT
    count(*) FILTER (WHERE COALESCE(layup_att,0) + COALESCE(dunk_att,0) > COALESCE(fg2_att,0)) AS rim_gt_fg2,
    count(*) FILTER (WHERE COALESCE(c3_att,0) > COALESCE(c3_known_att,0) OR COALESCE(c3_known_att,0) > COALESCE(fg3_att,0)) AS c3_order
  FROM basketball_test.player_four_factors_by_game"))

cat("\n== D. team MV league totals (expect 2025: c3 1035 / known 11497; 2026: c3 1150 / known 12091; off = def) ==\n")
print(dbGetQuery(con, "
  SELECT game_year,
         SUM(off_c3_att) AS off_c3, SUM(def_c3_att) AS def_c3,
         SUM(off_c3_known_att) AS off_c3_known, SUM(def_c3_known_att) AS def_c3_known,
         SUM(off_fg3_att) AS off_fg3, SUM(off_fga) AS off_fga
  FROM basketball_test.team_ppp_ratings_mv GROUP BY 1 ORDER BY 1"))

cat("\n== E. incremental refresh smoke test (game 388; counts must be unchanged after) ==\n")
readRenviron("etl/.Renviron")
w <- dbConnect(RPostgres::Postgres(),
  host=Sys.getenv("PG_HOST"), port=as.integer(Sys.getenv("PG_PORT")),
  dbname=Sys.getenv("PG_DB"), user=Sys.getenv("PG_USER"),
  password=Sys.getenv("PG_PASS"), sslmode=Sys.getenv("PG_SSLMODE"))
print(dbGetQuery(w, "SELECT basketball_test.refresh_player_four_factors_by_game_for_games(ARRAY[388]) AS pff_rows"))
print(dbGetQuery(w, "SELECT basketball_test.refresh_onoff_default_for_games(ARRAY[388]) AS onoff_rows"))
dbDisconnect(w)
cat("\n== F. post-refresh: new columns still populated for game 388 players (expect no all-NULL year) ==\n")
print(dbGetQuery(con, "
  SELECT \"Year\", count(*) AS rows, count(off_on_c3_known_att) AS c3k_not_null
  FROM basketball_test.onoff_default_mv GROUP BY 1 ORDER BY 1"))
print(dbGetQuery(con, "
  SELECT game_year, count(*) AS rows, count(c3_known_att) AS c3k_not_null
  FROM basketball_test.player_four_factors_by_game WHERE game_id = 388 GROUP BY 1"))
dbDisconnect(con)
```

Run: `& "C:\Program Files\R\R-4.4.2\bin\Rscript.exe" <scratchpad>/planb_invariants.R`
Expected: A matches Task 1 exactly; B/C all zeros; D: `off_c3` = `def_c3` and `off_c3_known` = `def_c3_known` **exactly** per year, and both within ~1% of the Plan-A parquet numbers (c3 1,035/1,150; known 11,497/12,091 — small shortfalls are legitimate: `df_pts_poss_lineups_longer_mv` omits actions without lineup attribution, while the parquet backfill counted every `actions_clean` row); E returns positive row counts; F shows `c3k_not_null > 0` (NULLs are allowed only on rows that also have NULL `fg2_att` — the no-onoff-match rows). **If D's `off_c3` is far off (e.g. ~2× the expected count or half of it):** the shot_zones join or perspective attribution is wrong — check `z.id = d.id` and the offense/defense duplication gotcha before touching anything else.

- [ ] **Step 3: Merge branch 1**

```bash
git checkout main && git merge sql/shot-profile-mv && git push origin main
git branch -d sql/shot-profile-mv
```

---

### Task 7: `onoff_compute` — 28 output columns (branch 2)

**Files:**
- Modify: `sql/functions/onoff_compute.sql`

**Interfaces:**
- Consumes: `player_four_factors_by_game.{layup_made,layup_att,dunk_made,dunk_att,c3_made,c3_att,c3_known_att}` (Task 4, deployed in Task 6).
- Produces: 28 new bigint output columns named exactly like the `onoff_default_mv` columns (`off_on_layup_made` … `def_off_c3_known_att`), appended after `def_off_fg3_att` in RETURNS TABLE and the final SELECT. Plan C's Shiny code reads both paths by these names.

- [ ] **Step 1: Create branch**

```bash
git checkout main && git checkout -b sql/shot-profile-fns
```

- [ ] **Step 2: Extend RETURNS TABLE**

Replace:

```sql
    def_on_fg2_made bigint, def_on_fg2_att bigint, def_on_fg3_made bigint, def_on_fg3_att bigint,
    def_off_fg2_made bigint, def_off_fg2_att bigint, def_off_fg3_made bigint, def_off_fg3_att bigint
)
```

with:

```sql
    def_on_fg2_made bigint, def_on_fg2_att bigint, def_on_fg3_made bigint, def_on_fg3_att bigint,
    def_off_fg2_made bigint, def_off_fg2_att bigint, def_off_fg3_made bigint, def_off_fg3_att bigint,
    off_on_layup_made bigint, off_on_layup_att bigint, off_on_dunk_made bigint, off_on_dunk_att bigint,
    off_on_c3_made bigint, off_on_c3_att bigint, off_on_c3_known_att bigint,
    off_off_layup_made bigint, off_off_layup_att bigint, off_off_dunk_made bigint, off_off_dunk_att bigint,
    off_off_c3_made bigint, off_off_c3_att bigint, off_off_c3_known_att bigint,
    def_on_layup_made bigint, def_on_layup_att bigint, def_on_dunk_made bigint, def_on_dunk_att bigint,
    def_on_c3_made bigint, def_on_c3_att bigint, def_on_c3_known_att bigint,
    def_off_layup_made bigint, def_off_layup_att bigint, def_off_dunk_made bigint, def_off_dunk_att bigint,
    def_off_c3_made bigint, def_off_c3_att bigint, def_off_c3_known_att bigint
)
```

- [ ] **Step 3: Sum in `agg`**

Replace:

```sql
      SUM(p.fg2_made)::bigint AS fg2_made,
      SUM(p.fg2_att)::bigint  AS fg2_att,
      SUM(p.fg3_made)::bigint AS fg3_made,
      SUM(p.fg3_att)::bigint  AS fg3_att,
      SUM(COALESCE(p.onoff_minutes, 0))::numeric AS minutes
```

with:

```sql
      SUM(p.fg2_made)::bigint AS fg2_made,
      SUM(p.fg2_att)::bigint  AS fg2_att,
      SUM(p.fg3_made)::bigint AS fg3_made,
      SUM(p.fg3_att)::bigint  AS fg3_att,
      SUM(p.layup_made)::bigint AS layup_made,
      SUM(p.layup_att)::bigint  AS layup_att,
      SUM(p.dunk_made)::bigint AS dunk_made,
      SUM(p.dunk_att)::bigint  AS dunk_att,
      SUM(p.c3_made)::bigint AS c3_made,
      SUM(p.c3_att)::bigint  AS c3_att,
      SUM(p.c3_known_att)::bigint AS c3_known_att,
      SUM(COALESCE(p.onoff_minutes, 0))::numeric AS minutes
```

- [ ] **Step 4: Carry through `with_names`**

Replace:

```sql
      a.fg2_made,
      a.fg2_att,
      a.fg3_made,
      a.fg3_att,
      a.minutes,
      r.firstname,
```

with:

```sql
      a.fg2_made,
      a.fg2_att,
      a.fg3_made,
      a.fg3_att,
      a.layup_made,
      a.layup_att,
      a.dunk_made,
      a.dunk_att,
      a.c3_made,
      a.c3_att,
      a.c3_known_att,
      a.minutes,
      r.firstname,
```

- [ ] **Step 5: Carry through `filtered`**

Replace:

```sql
      wn.fg2_made,
      wn.fg2_att,
      wn.fg3_made,
      wn.fg3_att,
      wn.minutes,
      wn.firstname,
```

with:

```sql
      wn.fg2_made,
      wn.fg2_att,
      wn.fg3_made,
      wn.fg3_att,
      wn.layup_made,
      wn.layup_att,
      wn.dunk_made,
      wn.dunk_att,
      wn.c3_made,
      wn.c3_att,
      wn.c3_known_att,
      wn.minutes,
      wn.firstname,
```

- [ ] **Step 6: Pivot ON/OFF in `type_level`**

Replace:

```sql
      MAX(CASE WHEN f.is_on_key = 0 THEN f.fg2_made END) AS fg2_off_made,
      MAX(CASE WHEN f.is_on_key = 0 THEN f.fg2_att END)  AS fg2_off_att,
      MAX(CASE WHEN f.is_on_key = 0 THEN f.fg3_made END) AS fg3_off_made,
      MAX(CASE WHEN f.is_on_key = 0 THEN f.fg3_att END)  AS fg3_off_att
    FROM filtered f
```

with:

```sql
      MAX(CASE WHEN f.is_on_key = 0 THEN f.fg2_made END) AS fg2_off_made,
      MAX(CASE WHEN f.is_on_key = 0 THEN f.fg2_att END)  AS fg2_off_att,
      MAX(CASE WHEN f.is_on_key = 0 THEN f.fg3_made END) AS fg3_off_made,
      MAX(CASE WHEN f.is_on_key = 0 THEN f.fg3_att END)  AS fg3_off_att,
      MAX(CASE WHEN f.is_on_key = 1 THEN f.layup_made END) AS layup_on_made,
      MAX(CASE WHEN f.is_on_key = 1 THEN f.layup_att END)  AS layup_on_att,
      MAX(CASE WHEN f.is_on_key = 1 THEN f.dunk_made END) AS dunk_on_made,
      MAX(CASE WHEN f.is_on_key = 1 THEN f.dunk_att END)  AS dunk_on_att,
      MAX(CASE WHEN f.is_on_key = 1 THEN f.c3_made END) AS c3_on_made,
      MAX(CASE WHEN f.is_on_key = 1 THEN f.c3_att END)  AS c3_on_att,
      MAX(CASE WHEN f.is_on_key = 1 THEN f.c3_known_att END) AS c3_known_on_att,
      MAX(CASE WHEN f.is_on_key = 0 THEN f.layup_made END) AS layup_off_made,
      MAX(CASE WHEN f.is_on_key = 0 THEN f.layup_att END)  AS layup_off_att,
      MAX(CASE WHEN f.is_on_key = 0 THEN f.dunk_made END) AS dunk_off_made,
      MAX(CASE WHEN f.is_on_key = 0 THEN f.dunk_att END)  AS dunk_off_att,
      MAX(CASE WHEN f.is_on_key = 0 THEN f.c3_made END) AS c3_off_made,
      MAX(CASE WHEN f.is_on_key = 0 THEN f.c3_att END)  AS c3_off_att,
      MAX(CASE WHEN f.is_on_key = 0 THEN f.c3_known_att END) AS c3_known_off_att
    FROM filtered f
```

- [ ] **Step 7: Pivot offense/defense in `final_rows`**

Replace:

```sql
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.fg2_off_made END) AS def_off_fg2_made,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.fg2_off_att END)  AS def_off_fg2_att,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.fg3_off_made END) AS def_off_fg3_made,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.fg3_off_att END)  AS def_off_fg3_att
    FROM type_ranked tr
```

with:

```sql
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.fg2_off_made END) AS def_off_fg2_made,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.fg2_off_att END)  AS def_off_fg2_att,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.fg3_off_made END) AS def_off_fg3_made,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.fg3_off_att END)  AS def_off_fg3_att,
      MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.layup_on_made END) AS off_on_layup_made,
      MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.layup_on_att END)  AS off_on_layup_att,
      MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.dunk_on_made END) AS off_on_dunk_made,
      MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.dunk_on_att END)  AS off_on_dunk_att,
      MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.c3_on_made END) AS off_on_c3_made,
      MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.c3_on_att END)  AS off_on_c3_att,
      MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.c3_known_on_att END) AS off_on_c3_known_att,
      MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.layup_off_made END) AS off_off_layup_made,
      MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.layup_off_att END)  AS off_off_layup_att,
      MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.dunk_off_made END) AS off_off_dunk_made,
      MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.dunk_off_att END)  AS off_off_dunk_att,
      MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.c3_off_made END) AS off_off_c3_made,
      MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.c3_off_att END)  AS off_off_c3_att,
      MAX(CASE WHEN tr.type_lineup = 'offense' THEN tr.c3_known_off_att END) AS off_off_c3_known_att,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.layup_on_made END) AS def_on_layup_made,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.layup_on_att END)  AS def_on_layup_att,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.dunk_on_made END) AS def_on_dunk_made,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.dunk_on_att END)  AS def_on_dunk_att,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.c3_on_made END) AS def_on_c3_made,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.c3_on_att END)  AS def_on_c3_att,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.c3_known_on_att END) AS def_on_c3_known_att,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.layup_off_made END) AS def_off_layup_made,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.layup_off_att END)  AS def_off_layup_att,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.dunk_off_made END) AS def_off_dunk_made,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.dunk_off_att END)  AS def_off_dunk_att,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.c3_off_made END) AS def_off_c3_made,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.c3_off_att END)  AS def_off_c3_att,
      MAX(CASE WHEN tr.type_lineup = 'defense' THEN tr.c3_known_off_att END) AS def_off_c3_known_att
    FROM type_ranked tr
```

- [ ] **Step 8: Carry through `final_scored`**

Replace:

```sql
      fr.def_off_fg2_made, fr.def_off_fg2_att, fr.def_off_fg3_made, fr.def_off_fg3_att,
      fr.offense_on_ppp  - fr.defense_on_ppp  AS on_net_rtg,
```

with:

```sql
      fr.def_off_fg2_made, fr.def_off_fg2_att, fr.def_off_fg3_made, fr.def_off_fg3_att,
      fr.off_on_layup_made, fr.off_on_layup_att, fr.off_on_dunk_made, fr.off_on_dunk_att,
      fr.off_on_c3_made, fr.off_on_c3_att, fr.off_on_c3_known_att,
      fr.off_off_layup_made, fr.off_off_layup_att, fr.off_off_dunk_made, fr.off_off_dunk_att,
      fr.off_off_c3_made, fr.off_off_c3_att, fr.off_off_c3_known_att,
      fr.def_on_layup_made, fr.def_on_layup_att, fr.def_on_dunk_made, fr.def_on_dunk_att,
      fr.def_on_c3_made, fr.def_on_c3_att, fr.def_on_c3_known_att,
      fr.def_off_layup_made, fr.def_off_layup_att, fr.def_off_dunk_made, fr.def_off_dunk_att,
      fr.def_off_c3_made, fr.def_off_c3_att, fr.def_off_c3_known_att,
      fr.offense_on_ppp  - fr.defense_on_ppp  AS on_net_rtg,
```

- [ ] **Step 9: Final SELECT (order must match RETURNS TABLE)**

Replace:

```sql
    fs.def_on_fg2_made, fs.def_on_fg2_att, fs.def_on_fg3_made, fs.def_on_fg3_att,
    fs.def_off_fg2_made, fs.def_off_fg2_att, fs.def_off_fg3_made, fs.def_off_fg3_att
  FROM final_scored fs
```

with:

```sql
    fs.def_on_fg2_made, fs.def_on_fg2_att, fs.def_on_fg3_made, fs.def_on_fg3_att,
    fs.def_off_fg2_made, fs.def_off_fg2_att, fs.def_off_fg3_made, fs.def_off_fg3_att,
    fs.off_on_layup_made, fs.off_on_layup_att, fs.off_on_dunk_made, fs.off_on_dunk_att,
    fs.off_on_c3_made, fs.off_on_c3_att, fs.off_on_c3_known_att,
    fs.off_off_layup_made, fs.off_off_layup_att, fs.off_off_dunk_made, fs.off_off_dunk_att,
    fs.off_off_c3_made, fs.off_off_c3_att, fs.off_off_c3_known_att,
    fs.def_on_layup_made, fs.def_on_layup_att, fs.def_on_dunk_made, fs.def_on_dunk_att,
    fs.def_on_c3_made, fs.def_on_c3_att, fs.def_on_c3_known_att,
    fs.def_off_layup_made, fs.def_off_layup_att, fs.def_off_dunk_made, fs.def_off_dunk_att,
    fs.def_off_c3_made, fs.def_off_c3_att, fs.def_off_c3_known_att
  FROM final_scored fs
```

- [ ] **Step 10: Verify diff and commit**

Run: `git diff --stat sql/functions/onoff_compute.sql`
Expected: roughly **+120/−7**. The existing `DROP FUNCTION IF EXISTS basketball_test.onoff_compute(date, date, ...)` header line stays unchanged — the parameter signature did not change (return-type changes require the DROP, which is already there).

```bash
git add sql/functions/onoff_compute.sql
git commit -m "feat(sql): shot-profile output columns in onoff_compute (28 cols)

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 8: `get_team_ratings_dynamic` — 12 output columns

**Files:**
- Modify: `sql/functions/get_team_ratings_dynamic.sql`

**Interfaces:**
- Consumes: `df_pts_poss_lineups_longer_mv` action rows + `shot_zones` (inline; this function does not use pre-aggregated shot counts because of the clutch filters).
- Produces: 12 new INT output columns named exactly like the `team_ppp_ratings_mv` columns (`off_fga` … `def_c3_known_att`), appended after `rank_def_ppp`.

- [ ] **Step 1: Extend RETURNS TABLE**

Replace:

```sql
    rank_net_rtg   BIGINT,
    rank_off_ppp   BIGINT,
    rank_def_ppp   BIGINT
) 
```

with:

```sql
    rank_net_rtg   BIGINT,
    rank_off_ppp   BIGINT,
    rank_def_ppp   BIGINT,
    off_fga        INT,
    off_layup_att  INT,
    off_dunk_att   INT,
    off_fg3_att    INT,
    off_c3_att     INT,
    off_c3_known_att INT,
    def_fga        INT,
    def_layup_att  INT,
    def_dunk_att   INT,
    def_fg3_att    INT,
    def_c3_att     INT,
    def_c3_known_att INT
) 
```

(Note: keep the trailing space after `)` if present in the original — copy the anchor exactly as it appears.)

- [ ] **Step 2: Add flags + join in `base_agg`**

Replace:

```sql
        sum(dppllm.team_score) / NULLIF(sum(dppllm.final_end_poss::integer), 0)::numeric AS ppp,
        sum(dppllm.final_end_poss::integer) AS total_poss,
        COUNT(DISTINCT dppllm.game_id) AS games_count
      FROM basketball_test.df_pts_poss_lineups_longer_mv dppllm
      JOIN qualifying_games qg ON qg.game_id = dppllm.game_id AND qg.team_id = dppllm.team_id
```

with:

```sql
        sum(dppllm.team_score) / NULLIF(sum(dppllm.final_end_poss::integer), 0)::numeric AS ppp,
        sum(dppllm.final_end_poss::integer) AS total_poss,
        COUNT(DISTINCT dppllm.game_id) AS games_count,
        SUM(CASE WHEN dppllm.type = 'shot' THEN 1 ELSE 0 END) AS fga,
        SUM(CASE WHEN dppllm.type = 'shot' AND dppllm.parameters_points = 2 AND dppllm.parameters_type = 'lay-up' THEN 1 ELSE 0 END) AS layup_att,
        SUM(CASE WHEN dppllm.type = 'shot' AND dppllm.parameters_points = 2 AND dppllm.parameters_type IN ('dunk', 'allyhoop') THEN 1 ELSE 0 END) AS dunk_att,
        SUM(CASE WHEN dppllm.type = 'shot' AND dppllm.parameters_points = 3 THEN 1 ELSE 0 END) AS fg3_att,
        SUM(CASE WHEN dppllm.type = 'shot' AND dppllm.parameters_points = 3 AND z.is_corner3 IS TRUE THEN 1 ELSE 0 END) AS c3_att,
        SUM(CASE WHEN dppllm.type = 'shot' AND dppllm.parameters_points = 3 AND z.is_corner3 IS NOT NULL THEN 1 ELSE 0 END) AS c3_known_att
      FROM basketball_test.df_pts_poss_lineups_longer_mv dppllm
      JOIN qualifying_games qg ON qg.game_id = dppllm.game_id AND qg.team_id = dppllm.team_id
      LEFT JOIN basketball_test.shot_zones z ON z.game_id = dppllm.game_id AND z.id = dppllm.id
```

- [ ] **Step 3: Pivot in `pivoted`**

Replace:

```sql
        max(base_agg.total_poss) FILTER (WHERE base_agg.type_lineup = 'offense'::text) AS off_poss,
        max(base_agg.total_poss) FILTER (WHERE base_agg.type_lineup = 'defense'::text) AS def_poss
      FROM base_agg
```

with:

```sql
        max(base_agg.total_poss) FILTER (WHERE base_agg.type_lineup = 'offense'::text) AS off_poss,
        max(base_agg.total_poss) FILTER (WHERE base_agg.type_lineup = 'defense'::text) AS def_poss,
        max(base_agg.fga) FILTER (WHERE base_agg.type_lineup = 'offense'::text) AS off_fga,
        max(base_agg.layup_att) FILTER (WHERE base_agg.type_lineup = 'offense'::text) AS off_layup_att,
        max(base_agg.dunk_att) FILTER (WHERE base_agg.type_lineup = 'offense'::text) AS off_dunk_att,
        max(base_agg.fg3_att) FILTER (WHERE base_agg.type_lineup = 'offense'::text) AS off_fg3_att,
        max(base_agg.c3_att) FILTER (WHERE base_agg.type_lineup = 'offense'::text) AS off_c3_att,
        max(base_agg.c3_known_att) FILTER (WHERE base_agg.type_lineup = 'offense'::text) AS off_c3_known_att,
        max(base_agg.fga) FILTER (WHERE base_agg.type_lineup = 'defense'::text) AS def_fga,
        max(base_agg.layup_att) FILTER (WHERE base_agg.type_lineup = 'defense'::text) AS def_layup_att,
        max(base_agg.dunk_att) FILTER (WHERE base_agg.type_lineup = 'defense'::text) AS def_dunk_att,
        max(base_agg.fg3_att) FILTER (WHERE base_agg.type_lineup = 'defense'::text) AS def_fg3_att,
        max(base_agg.c3_att) FILTER (WHERE base_agg.type_lineup = 'defense'::text) AS def_c3_att,
        max(base_agg.c3_known_att) FILTER (WHERE base_agg.type_lineup = 'defense'::text) AS def_c3_known_att
      FROM base_agg
```

- [ ] **Step 4: Carry through `final_calc` (SELECT list AND GROUP BY)**

Replace:

```sql
        p.off_poss,
        p.def_poss
      FROM pivoted p
      JOIN basketball_test.full_rosters fr
        ON fr.game_year = p.game_year AND fr.team_id = p.team_id
      GROUP BY p.game_year, p.team_id, fr.team_name, p.off_ppp_raw, p.def_ppp_raw, p.games_played, p.wins, p.losses, p.off_poss, p.def_poss
```

with:

```sql
        p.off_poss,
        p.def_poss,
        p.off_fga,
        p.off_layup_att,
        p.off_dunk_att,
        p.off_fg3_att,
        p.off_c3_att,
        p.off_c3_known_att,
        p.def_fga,
        p.def_layup_att,
        p.def_dunk_att,
        p.def_fg3_att,
        p.def_c3_att,
        p.def_c3_known_att
      FROM pivoted p
      JOIN basketball_test.full_rosters fr
        ON fr.game_year = p.game_year AND fr.team_id = p.team_id
      GROUP BY p.game_year, p.team_id, fr.team_name, p.off_ppp_raw, p.def_ppp_raw, p.games_played, p.wins, p.losses, p.off_poss, p.def_poss,
        p.off_fga, p.off_layup_att, p.off_dunk_att, p.off_fg3_att, p.off_c3_att, p.off_c3_known_att,
        p.def_fga, p.def_layup_att, p.def_dunk_att, p.def_fg3_att, p.def_c3_att, p.def_c3_known_att
```

- [ ] **Step 5: Final SELECT (order must match RETURNS TABLE)**

Replace:

```sql
    dense_rank() OVER (PARTITION BY fc.game_year ORDER BY fc.net_rtg DESC NULLS LAST) AS rank_net_rtg,
    dense_rank() OVER (PARTITION BY fc.game_year ORDER BY fc.off_ppp DESC NULLS LAST) AS rank_off_ppp,
    dense_rank() OVER (PARTITION BY fc.game_year ORDER BY fc.def_ppp ASC NULLS LAST)  AS rank_def_ppp
  FROM final_calc fc;
```

with:

```sql
    dense_rank() OVER (PARTITION BY fc.game_year ORDER BY fc.net_rtg DESC NULLS LAST) AS rank_net_rtg,
    dense_rank() OVER (PARTITION BY fc.game_year ORDER BY fc.off_ppp DESC NULLS LAST) AS rank_off_ppp,
    dense_rank() OVER (PARTITION BY fc.game_year ORDER BY fc.def_ppp ASC NULLS LAST)  AS rank_def_ppp,
    fc.off_fga::int,
    fc.off_layup_att::int,
    fc.off_dunk_att::int,
    fc.off_fg3_att::int,
    fc.off_c3_att::int,
    fc.off_c3_known_att::int,
    fc.def_fga::int,
    fc.def_layup_att::int,
    fc.def_dunk_att::int,
    fc.def_fg3_att::int,
    fc.def_c3_att::int,
    fc.def_c3_known_att::int
  FROM final_calc fc;
```

- [ ] **Step 6: Verify diff and commit**

Run: `git diff --stat sql/functions/get_team_ratings_dynamic.sql`
Expected: roughly **+60/−5**. The file's line 1 bare `DROP FUNCTION IF EXISTS basketball_test.get_team_ratings_dynamic;` stays — return-type change requires it and it's already there.

```bash
git add sql/functions/get_team_ratings_dynamic.sql
git commit -m "feat(sql): team shot-diet output columns in get_team_ratings_dynamic

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
```

---

### Task 9: Deploy functions + security + parity/timing verification + docs + merge

**Files:**
- Create: `<scratchpad>/planb_deploy_fns.R`, `<scratchpad>/planb_parity.R` (temp, not committed)
- Modify: `CLAUDE.md` (two lines, see Step 5)

**Interfaces:**
- Consumes: Tasks 7–8 SQL; `scripts/apply_db_security.R`; `app/tests/testthat/test-db-security-contracts.R`; Task 1 baseline timings.
- Produces: live filtered paths with shot-profile columns; Plan C unblocked on both paths.

- [ ] **Step 1: Deploy both functions**

Write `<scratchpad>/planb_deploy_fns.R` (same `deploy_fn` helper as Task 6 Step 1 — copy it verbatim):

```r
suppressMessages({library(DBI); library(RPostgres)})
readRenviron("etl/.Renviron")
deploy_fn <- function(pg, path) {
  txt <- paste(readLines(path, warn = FALSE), collapse = "\n")
  create_at <- regexpr("CREATE OR REPLACE FUNCTION", txt, fixed = TRUE)
  stopifnot(create_at > 0)
  head_part <- substr(txt, 1, create_at - 1)
  body_part <- substr(txt, create_at, nchar(txt))
  for (stmt in strsplit(head_part, ";", fixed = TRUE)[[1]]) {
    stmt <- trimws(stmt)
    if (nzchar(stmt)) DBI::dbExecute(pg, paste0(stmt, ";"))
  }
  DBI::dbExecute(pg, body_part)
  cat("deployed:", path, "\n")
}
pg <- dbConnect(RPostgres::Postgres(),
  host=Sys.getenv("PG_HOST"), port=5432L,
  dbname=Sys.getenv("PG_DB"), user=Sys.getenv("PG_USER"),
  password=Sys.getenv("PG_PASS"), sslmode="require")
deploy_fn(pg, "sql/functions/onoff_compute.sql")
deploy_fn(pg, "sql/functions/get_team_ratings_dynamic.sql")
dbDisconnect(pg)
cat("DONE\n")
```

Run: `& "C:\Program Files\R\R-4.4.2\bin\Rscript.exe" <scratchpad>/planb_deploy_fns.R`
Expected: `deployed:` twice, no errors.

- [ ] **Step 2: Re-apply security (DROP FUNCTION removed the app_readonly EXECUTE grants)**

Open `scripts/apply_db_security.R` and check its header for the apply flag name (CLAUDE.md: dry-run by default). Then:

```bash
"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" scripts/apply_db_security.R            # dry-run: statements must mention onoff_compute + get_team_ratings_dynamic
"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" scripts/apply_db_security.R --apply    # use the script's actual flag
"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('app/tests/testthat/test-db-security-contracts.R')"
```

Expected: contract tests PASS (they verify app_readonly can EXECUTE allowlisted functions and nothing else).

- [ ] **Step 3: Fast/filtered parity check**

Write `<scratchpad>/planb_parity.R`:

```r
suppressMessages({library(DBI); library(RPostgres)})
readRenviron("app/.Renviron")
con <- dbConnect(RPostgres::Postgres(),
  host=Sys.getenv("PG_HOST"), port=as.integer(Sys.getenv("PG_PORT")),
  dbname=Sys.getenv("PG_DB"), user=Sys.getenv("PG_USER"),
  password=Sys.getenv("PG_PASS"), sslmode=Sys.getenv("PG_SSLMODE"),
  connect_timeout=15L, bigint="numeric")

cat("== team parity: get_team_ratings_dynamic(2026) vs team_ppp_ratings_mv (expect 0 mismatches) ==\n")
print(dbGetQuery(con, "
  WITH f AS (SELECT * FROM basketball_test.get_team_ratings_dynamic(2026)),
       m AS (SELECT * FROM basketball_test.team_ppp_ratings_mv WHERE game_year = 2026)
  SELECT count(*) AS mismatches
  FROM f JOIN m ON m.team_id = f.team_id
  WHERE f.off_fga IS DISTINCT FROM m.off_fga::int
     OR f.off_layup_att IS DISTINCT FROM m.off_layup_att::int
     OR f.off_dunk_att IS DISTINCT FROM m.off_dunk_att::int
     OR f.off_fg3_att IS DISTINCT FROM m.off_fg3_att::int
     OR f.off_c3_att IS DISTINCT FROM m.off_c3_att::int
     OR f.off_c3_known_att IS DISTINCT FROM m.off_c3_known_att::int
     OR f.def_fga IS DISTINCT FROM m.def_fga::int
     OR f.def_layup_att IS DISTINCT FROM m.def_layup_att::int
     OR f.def_dunk_att IS DISTINCT FROM m.def_dunk_att::int
     OR f.def_fg3_att IS DISTINCT FROM m.def_fg3_att::int
     OR f.def_c3_att IS DISTINCT FROM m.def_c3_att::int
     OR f.def_c3_known_att IS DISTINCT FROM m.def_c3_known_att::int"))

cat("\n== onoff parity: onoff_compute(full 2026) vs onoff_default_mv ==\n")
cat("(control = existing fg3 column; new-column mismatches must not exceed control mismatches; expect both 0)\n")
print(dbGetQuery(con, "
  WITH f AS (SELECT * FROM basketball_test.onoff_compute(
               make_date(2025,10,1), make_date(2026,7,1), NULL, 0, 0, -1000, '2026',
               NULL, NULL, 'all', 'all', NULL, NULL, NULL,
               NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)),
       m AS (SELECT * FROM basketball_test.onoff_default_mv WHERE \"Year\" = 2026)
  SELECT
    count(*) FILTER (WHERE f.off_on_fg3_att IS DISTINCT FROM m.off_on_fg3_att) AS control_fg3_mism,
    count(*) FILTER (WHERE f.off_on_layup_att IS DISTINCT FROM m.off_on_layup_att
                     OR f.off_on_dunk_att IS DISTINCT FROM m.off_on_dunk_att
                     OR f.off_on_c3_att IS DISTINCT FROM m.off_on_c3_att
                     OR f.off_on_c3_known_att IS DISTINCT FROM m.off_on_c3_known_att
                     OR f.def_on_layup_att IS DISTINCT FROM m.def_on_layup_att
                     OR f.def_on_c3_att IS DISTINCT FROM m.def_on_c3_att) AS new_col_mism,
    count(*) AS joined_rows
  FROM f JOIN m ON m.player_id = f.player_id AND m.team_id = f.team_id"))

cat("\n== timing regression vs Task 1 baseline (3 runs each; median must be < 1.5x baseline and << 20s) ==\n")
for (i in 1:3) print(system.time(dbGetQuery(con, "
  SELECT * FROM basketball_test.onoff_compute(
    make_date(2025,10,1), make_date(2026,7,1), NULL, 0, 0, -1000, '2026',
    NULL, NULL, 'all', 'all', NULL, NULL, NULL,
    NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL, NULL)")))
for (i in 1:3) print(system.time(dbGetQuery(con, "
  SELECT * FROM basketball_test.get_team_ratings_dynamic(2026)")))
dbDisconnect(con)
```

Run: `& "C:\Program Files\R\R-4.4.2\bin\Rscript.exe" <scratchpad>/planb_parity.R`
Expected: team mismatches = 0; onoff `new_col_mism` = 0 (and `control_fg3_mism` = 0 — if control > 0, the discrepancy predates this work; new_col_mism must be ≤ control); timings within 1.5× the Task 1 baseline. **If the shot_zones LEFT JOIN regresses `get_team_ratings_dynamic` badly**, check `EXPLAIN` for a seq scan on shot_zones — its PK (game_id, id) should give an index path; only add an index after plan evidence.

- [ ] **Step 4: Run the full app test suite (regression)**

```bash
"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" scripts/test_all.R
```

Expected: same pass/fail profile as main (no new failures — Plan B adds columns; nothing in the Shiny tests reads them yet).

- [ ] **Step 5: CLAUDE.md doc lines**

In `CLAUDE.md` under **Key MV designs**, replace:

```
- `onoff_default_mv`: No WHERE pre-filter — stores ALL players. Min-poss filtered in R. Includes 16 shooting split columns via `shot_agg` CTE LEFT JOIN.
```

with:

```
- `onoff_default_mv`: No WHERE pre-filter — stores ALL players. Min-poss filtered in R. Includes 16 shooting split columns via `shot_agg` CTE LEFT JOIN + 28 shot-profile columns (`{off,def}_{on,off}_{layup,dunk,c3}_{made,att}` + `c3_known_att`; corner-3 via `shot_zones` join). `player_four_factors_by_game` carries the same 7 counts per game for `onoff_compute`; `team_ppp_ratings_mv` / `get_team_ratings_dynamic` carry 12 team columns (`{off,def}_{fga,layup_att,dunk_att,fg3_att,c3_att,c3_known_att}`). Corner share = `c3_att / c3_known_att` (render "—" when known = 0); never divide by `fg3_att`.
```

- [ ] **Step 6: Commit docs + merge**

```bash
git add CLAUDE.md
git commit -m "docs: shot-profile SQL columns in CLAUDE.md

Co-Authored-By: Claude Fable 5 <noreply@anthropic.com>"
git checkout main && git merge sql/shot-profile-fns && git push origin main
git branch -d sql/shot-profile-fns
```

---

## Completion criteria (Plan B)

1. Both fast paths (`onoff_default_mv`, `team_ppp_ratings_mv`) and both filtered paths (`onoff_compute`, `get_team_ratings_dynamic`) expose the shot-profile columns with matching names.
2. Invariants hold: rim ≤ fg2, c3 ≤ c3_known ≤ fg3; league corner totals reconcile with Plan A (1,035 / 1,150 corners; 11,497 / 12,091 known 3PA); fast = filtered on full-season parity.
3. Incremental refresh functions fill the new columns (smoke-tested on game 388); ETL Phase 4 needs no changes.
4. Security contract tests pass; filtered-path timings within 1.5× baseline.
5. Plan C (Shiny Tabs 1/3/7 UI) unblocked — reads the new columns by the exact names above from either path.
