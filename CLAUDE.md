# CLAUDE.md

## Token Efficiency

**Trust this context** — avoid re-reading files or spawning exploration agents when the answer is documented here. Use `replace_all=true` for repetitive edits, read files in large chunks (limit=300+), and batch similar operations.

## Project Overview

Basketball Israel Analytics — R/Shiny dashboard + React frontend for player on/off impact, lineup combos, team ratings. Data: play-by-play JSON → ETL → PostgreSQL (Supabase) → Shiny app / Plumber API.

**Live app:** https://ibpl-stats.shinyapps.io/onoff-shiny/

**Tech:** R 4.4.2, Shiny (bslib/BS5), DBI/RPostgres (no dbplyr), PostgreSQL on Supabase (port 6543), schema `basketball_test`. React 19 + TypeScript + Vite in `frontend-v2/` with Plumber/R API (Phase 2 — Tabs 1-2 complete, Tabs 3-4 stubs, Tab 5 Shiny only).

## Git Branching

**Only `main` is a permanent branch.** All others are short-lived and created on-demand.

Branch naming convention — prefix signals the area of change:
- `shiny/<name>` — Shiny UI/server (`app/`)
- `react/<name>` — React frontend (`frontend-v2/`)
- `sql/<name>` — Materialized views / SQL functions (high-risk — verify MV rebuild order before merging)
- `etl/<name>` — ETL pipeline
- `infra/<name>` — CI workflows, scripts, deploy config

Workflow:
```bash
git checkout -b shiny/fix-filter-reset   # create branch
# ... work, commit ...
# open PR on GitHub → merge → delete branch
git branch -d shiny/fix-filter-reset
```

`main` is protected by a local git hook — direct pushes are rejected. Tags (`backup/...`) are used for snapshots instead of long-lived backup branches.

**One-time setup** (required after cloning):
```bash
git config core.hooksPath scripts/hooks
```

## Commands

```bash
RSCRIPT="/c/Program Files/R/R-4.4.2/bin/Rscript.exe"
"$RSCRIPT" -e "shiny::runApp('app')"                    # Run Shiny app locally
"$RSCRIPT" -e "rsconnect::deployApp('app')"             # Deploy Shiny
"$RSCRIPT" -e "Sys.setenv(APP_ENV='test'); source('etl/etl_full.R'); etl_full()"  # Full ETL
cd frontend-v2 && npm run dev                            # Vite dev (port 5173, proxies /api → 3002)
cd frontend-v2/server && "$RSCRIPT" run.R                # Plumber API (port 3002)
```

## Architecture

### Shiny App (`app/`)

```
app.R                  Entry point — sources R/*.R, assembles ui/server
R/global.R             Libraries, constants, DB pool, CSS, helpers
R/ui_tab{1-5}*.R       Tab UI definitions
R/server_tab{1-5}*.R   Tab server logic (receive shared list)
```

**Modular pattern:** `app.R` calls `server_tab*(input, output, session, shared)`. Shared list contains: `season_date_bounds`, `selected_game_year`, `teams_for_year_df`, `selected_opp_ids_on`, `selected_opp_ids_ld`.

**Global season selector:** Single `input$game_year` in navbar header. All tabs read from this — no per-tab season inputs.

**Direct SQL (no dbplyr):** All DB access uses `DBI::dbGetQuery(pg_pool, ...)` with `$1, $2` params. Pool pre-warmed with `SELECT 1` at source time. `bigint = "numeric"` in `dbPool()`.

**UI theme:** Dark editorial (bslib BS5), DM Sans + JetBrains Mono, amber accent `#e8a435`. Filter chips bar, loading skeletons, tab icons with active amber underline.

### Shiny Tabs — Fast/Filtered Paths

All tabs: sidebar 3-col / main 9-col, FixedHeader, mobile collapse behind "Show Filters". Summary/Four Factors toggle per tab.

| Tab | Fast Path (MV) | Filtered Path (SQL) |
|-----|----------------|---------------------|
| 1: On/Off Impact | `onoff_default_mv` / `player_advanced_stats_mv` | `onoff_compute()` / `four_factors_compute()` |
| 2: Lineup Data | — (always SQL) | `fetch_lineups_csv_v2()` / `fetch_lineups_four_factors_csv()` |
| 3: Team Ratings | `team_ppp_ratings_mv` / `team_four_factors_mv` | `get_team_ratings_dynamic()` / `get_team_four_factors_dynamic()` |
| 4: Game Logs | `mv_lineup_totals_by_day` + `final_schedule_mv` | — (direct MV queries) |
| 5: Player Stats | `player_traditional_stats_mv` | SQL pushdown with pair-key filtering |

### React Frontend (`frontend-v2/`)

**Status:** Tab 1 + Tab 2 complete with live data. Tabs 3-4 placeholder stubs. Tab 5 Shiny only.

**Stack:** React 19 + TypeScript + Vite + TanStack Query + react-select. Plumber/R API on port 3002 mirrors Shiny server logic exactly.

**Key components:** `OnOffPage.tsx` (Tab 1), `LineupsPage.tsx` (Tab 2), `DataTable.tsx` (sortable + CSV export), `HeatCell`/`ShotCell`/`FFCell` (visualization cells), `FilterDrawer.tsx` + `FilterChips.tsx` + `store.ts` (filter state via useReducer), `LineupModal.tsx` (lineup game log).

**Design reference:** `prototype.html` — single-file HTML/CSS/JS mock for all 4 tabs.

**Plumber API endpoints:** `/api/onoff/{summary,four-factors}`, `/api/lineups/{summary,four-factors,game-log}`, `/api/meta/{teams,players,game-numbers,last-updated}`. Hardening via env vars: CORS allowlist, optional API key, IP rate limiting, response cache (60s TTL).

**Rename functions:** `rename_onoff()`, `rename_lineup_summary()`, `rename_lineup_ff()` map SQL snake_case → TS camelCase. Single source of truth for the SQL↔React column contract.

## Key Tables & MVs

**Base tables:** `schedule`, `actions_clean`, `full_rosters`, `possessions`, `pws`, `lineups_lookup`, `stints`, `sub_lineups`

**MV dependency tree** (refresh in this order):
```
L1: final_schedule_mv, df_pts_poss_lineups_longer_mv
L2: mv_lineup_totals_by_day, team_ppp_ratings_mv, onoff_default_mv
L3: player_onoff_by_game, player_four_factors_by_game, lineup_four_factors_by_game, player_advanced_stats_mv
L4: team_four_factors_mv
```

**CASCADE warning:** `DROP ... CASCADE` on L1 drops all L2-L4. Use `sql/rebuild_all_mvs.R`:
```r
rebuild_all_mvs()                        # all L1-L4
rebuild_all_mvs(from_level = 2)          # skip L1
```

**Key MV designs:**
- `onoff_default_mv`: No WHERE pre-filter — stores ALL players. Min-poss filtered in R. Includes 16 shooting split columns via `shot_agg` CTE LEFT JOIN.
- `sub_lineups_stats`: Pre-computed lineup stats with 8 shooting columns. Key: `(team_id, sub_lineup_hash, game_year)`.
- `mv_lineup_totals_by_day`: Uses `g_date` (not `game_date`), `total_poss`/`total_pts` (not `poss`/`pts`).
- `sched_long`: VIEW (not MV) — reads live from `schedule`. Only `final_schedule_mv` needs REFRESH.

**Column naming gotchas:**
- `parameters_points` = 2 or 3 (for 2pt/3pt split). `parameters_type` = shot type string ("lay-up", etc.) — NOT "2pt"/"3pt"
- `parameters_made` = "made", "missed", or "blocked"
- `lineups_lookup` has individual rows per player (`player_id` is single int, NOT array)

## SQL Functions

| Function | Params | Purpose |
|----------|--------|---------|
| `onoff_compute` | 14 | Player on/off PPP with percentile ranks |
| `four_factors_compute` | 11 | Player TS%, OREB%, TOV%, FTR on/off splits |
| `fetch_lineups_csv_v2` | 20 | Lineup combos (Summary) + clutch + shooting splits |
| `fetch_lineups_four_factors_csv` | 20 | Lineup combos (Four Factors) + clutch |
| `get_team_ratings_dynamic` | 14 | Team PPP ratings + wins/losses + clutch |
| `get_team_four_factors_dynamic` | 14 | Team four-factor rates + clutch |

**Fast-path gate rule:** SQL functions checking `p_start_date IS NULL` must also accept explicit full-season window (`make_date(year-1,10,1)` to `make_date(year,7,1)`) since React always sends dates.

**Architecture note:** SQL functions only `SUM` pre-computed columns from MVs — they don't recompute raw counts. Metric formula fixes go in the base MVs (`player_four_factors_by_game`, `lineup_four_factors_by_game`, `player_advanced_stats_mv`).

## Four Factors Metrics

Computed in MVs, aggregated by SQL functions. All rates × 100 in final output.

| Metric | Formula | Notes |
|--------|---------|-------|
| **TS%** | `pts / (2 × (FGA + distinct personal-foul FT trips))` | and-1 = 1 trip |
| **TOV%** | `turnovers / total_poss` | |
| **OREB%** | `OREBs / (missed+blocked shots + missed last personal-foul FTs)` | |
| **FTR** | `all FTs / all FGA` | Includes tech/flagrant FTs intentionally |

**Color polarity:** Offense green-high (except TOV% red-high). Defense red-high (except TOV% green-high). Unranked (<100 poss) = gray/null.

## Shooting Splits (2PT/3PT)

Available in Tabs 1, 2, 4 Summary views. Not in FF views or Tab 3.

- **Tab 1:** 16 columns (off/def × on/off × fg2/fg3 × made/att)
- **Tab 2:** 8 columns (off/def × fg2/fg3 × made/att) — no on/off split (lineup-level)
- **Tab 4:** Same as Tab 2 per game

**ShotCell rendering:** Continuous RGB gradient (`accColor`), not discrete classes. Defense sign flip. Min 50 FGA → gray muting. Weighted league averages computed dynamically from dataset.

## Clutch Time Filter

Available in Tabs 2 and 3 only. 4 SQL params: `p_max_margin`, `p_margin_status`, `p_max_time_remaining`, `p_ot_margin_filter`.

**Key design:** Uses pre-shot margin (subtracts `team_score` from cumulative). OT bypasses margin/status by default (`p_ot_margin_filter = FALSE`). Time filter always bypasses OT. Non-clutch path uses pre-aggregated MVs; clutch path queries raw `df_pts_poss_lineups_longer_mv` (scores are action-level).

## Auto Min Possessions

- **Tab 1:** Top 35% by ON Poss (client-side in React via `autoMinPoss()`, server-side in Shiny). Only lowers threshold.
- **Tab 2:** 150-row target cap on `totalPoss` (server-side `auto_minposs_target_r()`). Raises AND lowers. Computed on team/player-filtered data BEFORE min_poss filter.
- **Auto/Manual:** Manual slider → `autoEnabled = false`. Filter change → `autoEnabled = true`. `autoUpdating` ref prevents auto-triggered changes from being treated as manual.

## Tab 2: Lineup Details

- **Server-side ranking:** Plumber fetches ALL lineups (min_poss=0), computes PR ranks on full population, caches in `RANKED_CACHE` (game-level key), applies local filters (team/player/minPoss) via `apply_lineup_local_filters()`. Two-layer cache: `RANKED_CACHE` + `RESP_CACHE`.
- **TOTAL row:** Sum raw counts → derive rates (client-side). Pinned at top, not clickable, PR fields null.
- **Local state:** groupSize, teamId, playersOn/Off, minPoss, clutch params are `useState` in LineupsPage (NOT in shared FilterContext). Drawer Teams filter is for data; local Team dropdown is only for Players On/Off option pool.
- **Lineup click → modal:** JS onclick → `Shiny.setInputValue`. Resolves `sub_lineup_hash` → `lineup_hash(es)` via `sub_lineups`. Summary/FF branch for modal content.
- **PG array parsing:** `player_ids` comes as `{1,2,3}` text; vectorized `parse_pg_int_array_json()` → list column for proper JSON serialization.

## ETL

**Use `etl_full.R`** — base tables → sub-lineups → MV refresh → validation. Logs to `etl/logs/`.

**Key points:**
- `fetch_israel_schedule()`: JSON fields are mixed-case (`GN`, `ExternalID`) — must explicitly map to lowercase DB columns. `upsert_by_like()` is case-sensitive.
- ETL needs write access (`etl/.Renviron` with postgres user), app uses readonly (`app/.Renviron`).
- Starters lineage: `extract_starters()` → `lineups_lookup` → `pws` → MVs.
- Incremental refresh: `refresh_sub_lineups_stats_for_games(int4[])`.

## Environment

Two `.Renviron` files (gitignored): `app/.Renviron` (readonly), `etl/.Renviron` (write access). Format: `PG_HOST`, `PG_PORT=6543`, `PG_DB=postgres`, `PG_USER`, `PG_PASS`, `PG_SSLMODE=require`, `POOL_MAX=3`.

- Port 6543 = pooler (app/ETL), Port 5432 = direct (DDL)
- DDL uses same pooler host on port 5432 (not `db.<ref>.supabase.co`)
- `SET search_path` needs `SET LOCAL` in transaction on pooler

## ETL Scheduler

Daily via Windows Task Scheduler → `scripts/run_etl_full.ps1`. Writes marker to `etl/logs/last_success.txt` + `app_meta` DB table. Per-run log files. `StartWhenAvailable=True`, `WakeToRun=True`. Currently `Interactive only` mode.

## Security

1. Never hardcode credentials — `.Renviron` + `Sys.getenv()` only
2. All Shiny client inputs are untrusted — hidden columns/controls are not a security boundary
3. Parameterized SQL only (`$1, $2` placeholders) — never `sprintf()`/`paste0()` for user values
4. Fail-closed on unexpected input

## Code Conventions

2-space indent, snake_case, parameterized SQL. Schema `basketball_test`.

## Pitfalls & Lessons Learned

### PostgreSQL / Supabase
- `REFRESH MATERIALIZED VIEW` re-runs stored definition — must DROP+CREATE to change query
- `DROP ... CASCADE` propagates — rebuild dependents in L2→L3→L4 order
- `ANALYZE;` without table fails on Supabase — scope to specific tables
- `score` column from raw JSON is unreliable — use `own_team_score`/`opp_team_score`
- `segment_id` repeats across games — always include `game_id` in GROUP BY
- Floor time: compute `MAX - MIN` across ALL rows per segment (no `type_lineup` filter), then SUM with offense filter to avoid double-counting
- Clutch CTEs: propagate `team_id` through all CTEs + always use table aliases (avoid PL/pgSQL variable ambiguity)
- `fetch_lineups_all.sql` and `fetch_lineups_four_factors.sql` have near-identical clutch structures — keep them in sync

### R / Shiny / DT
- `bigint = "numeric"` in `dbPool()` — integer64 breaks dplyr `coalesce()`, `+`, many tidyverse ops. `SUM(integer)` → bigint
- `dateRangeInput` NA pitfall: `updateDateRangeInput()` with `start` outside `min` → NA. Guard with `is.na()` checks
- DT JS render `row` guard: `if (type !== 'display' || !row) return data;` — prevents TypeError crash from `filter = "top"` init
- `server = TRUE` in `updateSelectizeInput`: only use when setting `choices`, not just `selected`. Clear tags first on multi-select team switch
- `formatRound()` clobbers JS `columnDefs` render — do all formatting in JS
- `uiOutput`/`renderUI` causes NULL on startup — use static inputs + `update*Input()`
- `tags(...)` is invalid — use `tags$p(...)`, `tags$div(...)`. `htmltools::dataURI` not exported — use `base64enc::dataURI`
- `bindEvent()` must include GN reactive in triggers or GN changes won't re-run computation

### Deploy Scripts
- `DROP FUNCTION` signature must be exact (param count must match) — verify against actual CREATE signature
- Long `Rscript -e` segfaults — write to temp .R file
- MV DDL: `readLines()` + `paste(collapse="\n")`, strip comment header, execute as single string
- `$function$` boundary: find end with `grep("^\\$function\\$;$")`

### React / Plumber
- PR column naming: `prOffOn` (PPP rank) ≠ `prOffOnD` (Diff rank) — use correct one per column
- HeatCell format variants: `"diff"` (+0.12), `"ppp"` (0.9), `"net"` (+1.2) — don't mix
- FF unranked = null, not 0 or 50. Guard: `pr === null ? null : expr`
- CSV export: pass explicit `columnKeys`/`columnHeaders` to exclude internal PR fields
- Filter reducer: season change resets teams/opponents, GN↔lastN mutual exclusion
- Plumber rename functions are the single source of truth for SQL↔React column contract

### Debugging
- **Data first, code second.** Diagnostic query on actual data before analyzing code
- **MVs bake in constraints.** Read MV SQL before adding UI controls that interact with MV data
- **Trace the type chain.** SQL type → R type → dplyr → JS at each boundary
- **Test incrementally.** Deploy/test one layer at a time (SQL → R → UI)

## Backlogs

**Security/Resilience:**
1. DB `statement_timeout` guardrail (8s) in `global.R`
2. Short TTL cache for Tab 4 MV queries (30-60s)
3. Click burst guard for lineup modal (~300ms)

**Performance (React+Plumber):**
1. Profile SQL functions with `EXPLAIN (ANALYZE, BUFFERS)` for filtered cases
2. Audit indexes only after query-plan evidence
