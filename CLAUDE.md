# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Token Efficiency

This file is updated after every session. **Trust this context** — avoid re-reading files or spawning exploration agents when the answer is documented here. Use `replace_all=true` for repetitive edits, read files in large chunks (limit=300+), and batch similar operations.

## Project Overview

Basketball Israel Analytics — R/Shiny dashboard for player on/off impact, lineup combos, and team ratings. Data from play-by-play JSON (basket.co.il / stats.segevstats.com) → ETL → PostgreSQL (Supabase) → Shiny app.

**Live app:** https://ibpl-stats.shinyapps.io/onoff-shiny/

**Tech:** R 4.4.2, Shiny (bslib/BS5), PostgreSQL on Supabase (port 6543), schema `basketball_test`, deployed to shinyapps.io

## Commands

```bash
RSCRIPT="/c/Program Files/R/R-4.4.2/bin/Rscript.exe"
"$RSCRIPT" -e "shiny::runApp('app')"                    # Run app locally
"$RSCRIPT" -e "rsconnect::deployApp('app')"             # Deploy
"$RSCRIPT" -e "Sys.setenv(APP_ENV='test'); source('etl/etl_full.R'); etl_full()"  # Full ETL
```

## Architecture

```
app/
├── app.R                  Entry point - sources modules, assembles UI/server
├── R/
│   ├── global.R           Libraries, constants, DB pool, CSS, helpers
│   ├── ui_tab1_onoff.R    Tab 1 UI (On/Off Impact)
│   ├── ui_tab2_lineup.R   Tab 2 UI (Lineup Data)
│   ├── ui_tab3_team.R     Tab 3 UI (Team Ratings)
│   ├── server_tab1.R      Tab 1 server logic (~400 lines)
│   ├── server_tab2.R      Tab 2 server logic (~500 lines)
│   └── server_tab3.R      Tab 3 server logic (~250 lines)
├── app_backup.R           Original monolithic file (backup)
sql/functions/             PL/pgSQL for filtered queries
sql/materialized_views/    Pre-computed fast-path views
etl/                       etl_full.R (orchestrator), etl_onoff.R, etl_lineups.R
```

### Modular App Pattern

**Entry point (`app.R`):** Sources all `R/*.R` files, defines `ui` (assembles tab panels), defines `server` (calls tab server functions with shared context).

**Shared state:** Tab servers receive a `shared` list containing common reactives:
```r
shared <- list(
  season_date_bounds = season_date_bounds,
  selected_game_year = selected_game_year,
  teams_for_year_df = teams_for_year_df,
  selected_opp_ids_on = selected_opp_ids_on,
  selected_opp_ids_ld = selected_opp_ids_ld
)
server_tab1(input, output, session, shared)
```

**Deferred table initialization:** Database tables are initialized on first server request (not at source time) to avoid connection issues with Supabase pooler:
```r
# In global.R
init_tables <- function() {
  full_rosters <<- get_tbl("full_rosters")
  # ...
}
# In app.R server function
init_tables()
```

### Shiny Tabs

All tabs: sidebar 3-col / main 9-col, FixedHeader extension, mobile collapse behind "Show Filters"

| Tab | Filters | Fast Path (MV) | Filtered Path (SQL) |
|-----|---------|----------------|---------------------|
| 1: On/Off Impact | season, dates, team, game filters, min poss | `onoff_default_mv` / `player_advanced_stats_mv` | `onoff_compute()` / `four_factors_compute()` |
| 2: Lineup Data | + players on/off, group size 2-5, clutch time | — (always SQL) | `fetch_lineups_csv_v2()` / `fetch_lineups_four_factors_csv()` |
| 3: Team Ratings | season, dates, game filters, clutch time | `team_ppp_ratings_mv` / `team_four_factors_mv` | `get_team_ratings_dynamic()` / `get_team_four_factors_dynamic()` |

### Key Tables & MVs

**Base tables:** `schedule`, `actions_clean`, `full_rosters`, `possessions`, `pws`, `lineups_lookup`, `stints`, `sub_lineups`

**`lineups_lookup` schema:** Individual rows per player (not array). Columns: `id`, `game_id`, `player_id` (single int), `team_id`, `quarter`, `quarter_time`, `end_game_seconds_remaining`, `end_quarter_seconds_remaining`, `is_on_verdict`, `lineup_id`, `n_on`, `lineup_hash`, `game_year`. To get player names for a lineup hash, join to `full_rosters` and aggregate, or use `sub_lineups_stats.player_names_str`.

**Key column inventory:**

| Table | From JSON PBP | Computed in ETL |
|-------|--------------|-----------------|
| `actions_clean` | `quarter`, `parameters_*` (team, player, type, quarter, player_in/out, current_quarter, current_quarter_time, coord_x/y, points, fast_break, second_chance_points, points_from_turnover, made, kind, fouled_on, free_throws, free_throws_awarded, free_throw_number, is_coach_foul, is_bench_foul), `id`, `parent_action_id`, `user_time`, `quarter_time`, `type`, `player_id`, `team_id`, `score` (raw JSON — unreliable), `total_player_points`, `game_id`, `row_num` | `end_quarter_seconds_remaining`, `end_game_seconds_remaining`, `team_score` (points on made shots, else NA) |

**Shot column naming (important):**
- `parameters_points` = point value (2 or 3) — used for 2pt/3pt split in CASE expressions
- `parameters_type` = shot type string ("lay-up", "jump-shot", "dunk") — NOT "2pt"/"3pt"
- `parameters_made` = outcome string ("made", "missed", "blocked")
- `type` = action type ("shot", "freeThrow", "rebound", "turnover", "foul", etc.)
| `possessions` | All `actions_clean` columns | + `pct_ft`, `q_bucket`, `end_poss`, `sum_poss_poss`, `sum_block`, `sum_tech`, `final_end_poss` |
| `pws` | All `possessions` columns | + stint fields: `lineup_hash_offense`, `lineup_hash_defense`, `team_id_defense`, `segment_id`, `final_start_seg`, `final_end_seg`, `final_start_id`, `final_end_id` |
| `df_pts_poss_lineups_longer_mv` | Most `pws` columns (team_id flipped per branch) | + `own_team_score`, `opp_team_score` (cumulative), `type_lineup` ('offense'/'defense'), `lineup_hash` |

**`own_team_score` / `opp_team_score`:** Cumulative game scores computed via `cum_scores` CTE on `possessions`. Uses `total_cum - team_cum` pattern (no schedule join needed). Offense branch: own = acting team's cum score. Defense branch: own = total minus acting team's (i.e., defending team's cum score). Scores mirror each other — same pattern as `sched_long`.

**MV dependency tree** (refresh in this order):
```
L1: final_schedule_mv, df_pts_poss_lineups_longer_mv (depends on: possessions, pws)
L2: mv_lineup_totals_by_day, team_ppp_ratings_mv, onoff_default_mv
L3: player_onoff_by_game, player_four_factors_by_game, lineup_four_factors_by_game, player_advanced_stats_mv
L4: team_four_factors_mv
```

**CASCADE warning:** `DROP MATERIALIZED VIEW df_pts_poss_lineups_longer_mv CASCADE` drops all 8 dependent MVs (L2–L4). Use `sql/rebuild_all_mvs.R` to rebuild:
```r
source("sql/rebuild_all_mvs.R")
rebuild_all_mvs()                        # rebuild all L1-L4
rebuild_all_mvs(from_level = 2)          # skip L1, rebuild L2-L4 only
rebuild_all_mvs(skip = "final_schedule_mv")  # skip specific MVs
```

**`onoff_default_mv` design:** No `WHERE` pre-filter on possessions — stores ALL players (559 rows for 2026 season). Min-poss filtering is done locally in R (`filter(ON Poss >= input$min_on_poss)`). Includes 16 shooting split columns (off/def × on/off × fg2/fg3 × made/att) via a `shot_agg` CTE that LEFT JOINs in `final_rows` to avoid passing columns through 7 intermediate CTEs. See "Shooting Splits" section for details.

**`sub_lineups_stats` design:** Pre-computed table for fast-path lineup queries. Populated by `refresh_sub_lineups_stats()` (called in ETL). Includes 8 shooting split columns (off/def × fg2/fg3 × made/att). Unique key: `(team_id, sub_lineup_hash, game_year)`.

**Function → MV mapping:**
- `onoff_compute` → `player_onoff_by_game`, `final_schedule_mv`
- `four_factors_compute` → `lineup_four_factors_by_game`, `final_schedule_mv`
- `fetch_lineups_*` → `mv_lineup_totals_by_day`, `final_schedule_mv`
- `get_team_*_dynamic` → `lineup_four_factors_by_game`, `final_schedule_mv`

**`team_ppp_ratings_mv` columns:** `game_year`, `team_id`, `team_name`, `off_ppp`, `def_ppp`, `net_rtg`, `games_played`, `wins`, `losses`, `off_poss`, `def_poss`, `rank_net_rtg`, `rank_off_ppp`, `rank_def_ppp`

**Wins/Losses in Team Ratings:** `get_team_ratings_dynamic()` returns `wins` and `losses`. When clutch filter is active, wins/losses only count games that have qualifying clutch possessions (not all filtered games). Uses `qualifying_games` CTE which applies clutch WHERE clause to identify games, then counts wins/losses from that subset.

**Minutes Calculation (Tab 2):** Computed from `end_game_seconds_remaining` using segment-level aggregation:
1. `segment_times`: `MAX(end_game_seconds_remaining) - MIN(end_game_seconds_remaining)` per segment across ALL rows (no type_lineup filter - captures full floor time including defense-to-offense transitions)
2. `segment_stats`: poss/pts per segment per type_lineup (offense vs defense stats differ)
3. Join and sum: `SUM(stint_seconds) FILTER (WHERE type_lineup = 'offense')` to avoid double-counting (each segment counted once)
Sources: `mv_lineup_totals_by_day.minutes`, `lineup_four_factors_by_game.minutes`, `sub_lineups_stats.minutes` (via `refresh_sub_lineups_stats()`)

### SQL Functions (params)

| Function | Params | Purpose |
|----------|--------|---------|
| `onoff_compute` | 14 | Player on/off PPP with percentile ranks |
| `four_factors_compute` | 11 | Player TS%, OREB%, TOV%, FTR on/off splits |
| `fetch_lineups_csv_v2` | 20 | Lineup combos (Summary) + clutch filters + minutes + shooting splits |
| `fetch_lineups_four_factors_csv` | 20 | Lineup combos (Four Factors) + clutch filters + minutes |
| `get_team_ratings_dynamic` | 14 | Team PPP ratings + wins/losses + clutch filters |
| `get_team_four_factors_dynamic` | 14 | Team four-factor rates + clutch filters |

### ETL

**Use `etl_full.R`** — runs: base tables → sub-lineups → MV refresh → validation. Logs to `etl/logs/`.

Key helpers: `upsert_by_like()` (schema-driven upsert), `fetch_israel_schedule()`, `compute_possessions()`, `compute_lineups_lookup()`

**ETL needs write access** — uses `etl/.Renviron` (postgres user with write access), separate from `app/.Renviron` (readonly).

## Environment

**Credentials setup:** Create two `.Renviron` files (gitignored, not tracked):

1. `app/.Renviron` — readonly user for Shiny app
2. `etl/.Renviron` — postgres user with write access for ETL

Both files use the same format:
```
PG_HOST=<supabase-pooler>
PG_PORT=6543
PG_DB=postgres
PG_USER=<user>
PG_PASS=<pass>
PG_SSLMODE=require
POOL_MAX=3
```

- Port 6543 = pooler (app/ETL), Port 5432 = direct (DDL)
- DDL uses **same pooler host** on port 5432 (not `db.<ref>.supabase.co` — that doesn't resolve)
- `SET search_path` doesn't persist on pooler — use `SET LOCAL` in transaction
- On port 5432 direct, `SET search_path` persists normally for the session

## Four Factors View

Now in main `app/app.R` (not app_test.R). Toggle between Summary/Four Factors in each tab. Four Factors shows TS%, OREB%, TOV%, FTR on/off splits with visual range bars.

**Ranking:** Players with <100 poss appear unranked/gray. Ranks computed in R via `percent_rank()`.

**Color polarity:** Offense metrics green-high (except TOV% red-high). Defense metrics red-high (except TOV% green-high).

## Shooting Splits (2PT/3PT)

Available in Tab 1 (On/Off Impact) Summary and Tab 2 (Lineup Data) Summary. Not in Four Factors views or Tab 3.

**Tab 1:** 16 columns (off/def × on/off × fg2/fg3 × made/att). Source: `onoff_default_mv` via `shot_agg` CTE, or `onoff_compute()` via `player_onoff_by_game`.

**Tab 2:** 8 columns (off/def × fg2/fg3 × made/att) — no on/off split since Tab 2 shows lineup-level stats. Columns: `off_fg2_made`, `off_fg2_att`, `off_fg3_made`, `off_fg3_att`, `def_fg2_made`, `def_fg2_att`, `def_fg3_made`, `def_fg3_att`.
- **Fast path:** reads from `sub_lineups_stats` (8 shooting columns populated by `refresh_sub_lineups_stats()`)
- **Non-clutch filtered path:** SUMs `fg2_made/att`, `fg3_made/att` from `mv_lineup_totals_by_day` (which already has these columns), then FILTER by offense/defense
- **Clutch path:** CASE expressions in `clutch_actions` CTE compute shot flags from raw `df_pts_poss_lineups_longer_mv`, propagated through `segment_stats` → `lineup_totals`, FILTER in final SELECT
- **R rendering:** "Off Shot" / "Def Shot" display columns after +/-, JS stacked bar visualization (same `make_shot_render` pattern as Tab 1), dynamic weighted averages for color thresholds (min 50 FGA)
- **Deploy:** `deploy_shooting_tab2.R` — ALTER TABLE + deploy functions + refresh

## Clutch Time Filter

Available in Tab 2 (Lineup Data) and Tab 3 (Team Ratings). Not in Tab 1 (On/Off Impact).

**UI Controls:**
- Enable checkbox → conditionalPanel with:
  - Max margin slider (0–10, default 5)
  - Score status dropdown (All/Leading/Trailing/Tied)
  - Max minutes remaining slider (1–5, default 5)
  - "Exclude OT if margin exceeded" checkbox

**SQL Parameters (4 new params per function):**
```sql
p_max_margin         INT     DEFAULT NULL   -- ABS(own - opp) <= this
p_margin_status      TEXT    DEFAULT 'all'  -- 'all'|'leading'|'trailing'|'tied'
p_max_time_remaining INT     DEFAULT NULL   -- seconds; R converts minutes*60
p_ot_margin_filter   BOOLEAN DEFAULT FALSE  -- TRUE = apply margin to OT
```

**WHERE clause pattern (used in all clutch-enabled functions):**

Uses **pre-shot margin** to correctly include possessions that started in clutch time. The `own_team_score` includes points just scored, so we subtract `team_score` to get the score before the basket:
- Offense: `(own_team_score - team_score) - opp_team_score`
- Defense: `own_team_score - (opp_team_score - team_score)`

```sql
AND (p_max_margin IS NULL
     OR ABS(CASE WHEN type_lineup = 'offense'
                 THEN (own_team_score - COALESCE(team_score, 0)) - opp_team_score
                 ELSE own_team_score - (opp_team_score - COALESCE(team_score, 0))
            END) <= p_max_margin
     OR (quarter > 4 AND NOT COALESCE(p_ot_margin_filter, FALSE)))
AND (v_margin_status = 'all'
     OR (v_margin_status = 'leading' AND pre_shot_own > pre_shot_opp)
     OR (v_margin_status = 'trailing' AND pre_shot_own < pre_shot_opp)
     OR (v_margin_status = 'tied' AND pre_shot_own = pre_shot_opp)
     OR (quarter > 4 AND NOT COALESCE(p_ot_margin_filter, FALSE)))
AND (p_max_time_remaining IS NULL
     OR end_game_seconds_remaining <= p_max_time_remaining
     OR quarter > 4)
```

**Overtime handling:**
- By default (`p_ot_margin_filter = FALSE`): OT always qualifies as clutch (bypasses margin AND status filters)
- When checked (`p_ot_margin_filter = TRUE`): OT must also satisfy margin/status filters
- Time filter always bypasses OT (no "time remaining" concept in OT)

**R wrapper extraction pattern:**
```r
clutch_enabled <- isTRUE(input$ld_clutch_enabled)
max_margin <- if (clutch_enabled) as.integer(input$ld_clutch_margin) else NA_integer_
margin_status <- if (clutch_enabled) input$ld_clutch_status else NA_character_
max_time_remaining <- if (clutch_enabled) as.integer(input$ld_clutch_minutes) * 60L else NA_integer_
ot_margin_filter <- if (clutch_enabled) isTRUE(input$ld_clutch_ot_margin) else FALSE
```

**Deploy script:** `deploy_clutch.R` deploys all 5 clutch-enabled functions to Supabase (port 5432).

### Possession Computation (`etl_onoff.R` → `compute_possessions()`)

A possession ends (`end_poss = TRUE`) when:
1. Made shot
2. Next action is a defensive rebound (miss → DREB)
3. Made last free throw (`pct_ft == 1`, where `pct_ft = ft_number / ft_awarded`)
4. Turnover

Post-processing (`final_end_poss`):
- **Blocked shots:** possession end shifts to the next row (`lead(end_poss)`)
- **Technical fouls:** suppressed (don't end possessions)
- **Double possession flags** (`sum_poss_poss >= 2`): suppressed when `id == parent_action_id`
- **End-of-quarter override** (`eoq_targets`): forces `final_end_poss = TRUE` for missed shots, OREBs, or blocks immediately before an `end-of-quarter` action

### Four Factors Metric Formulas

Computed in MVs `player_four_factors_by_game` / `lineup_four_factors_by_game`, then aggregated by dynamic SQL functions. All rates × 100 in final output.

| Metric | Formula | Numerator | Denominator |
|--------|---------|-----------|-------------|
| **TS%** | `total_points / (2 × ts_poss_count)` | Points scored | 2 × (FGA + distinct personal-foul FT trips) |
| **TOV%** | `tov_count / total_poss` | All turnovers | Possession count (`sum(final_end_poss)`) |
| **OREB%** | `oreb_count / oreb_opportunities` | Offensive rebounds | Missed + blocked shots + missed last FTs (personal fouls only) |
| **FTR** | `total_ft_attempts / total_fga` | All FTs (including technical/flagrant) | All FGA |

**FTR note:** Intentionally includes all FT types — measures overall FT-to-FGA ratio, not just personal-foul FTs.

### Raw Count Definitions (MV CASE logic)

Source: `player_four_factors_by_game` / `lineup_four_factors_by_game` SELECT clause. Both use identical CASE expressions. `player_advanced_stats_mv` has its own inline copy.

| Column | SQL CASE logic | Notes |
|--------|---------------|-------|
| `total_points` | `sum(team_score)` | `team_score` = `parameters_points` when made, else NULL |
| `total_poss` | `sum(final_end_flag)` | `1` when `final_end_poss IS TRUE`, else `0` |
| `ts_poss_count` | `count(type='shot') + count(DISTINCT parent_action_id WHERE type='freeThrow' AND parent is personal foul)` | FGA + distinct personal-foul FT trips (and-1 = 1 trip) |
| `oreb_count` | `count(type='rebound' AND parameters_type='offensive')` | All OREBs including team rebounds |
| `oreb_opportunities` | `count(type='shot' AND parameters_made IN ('missed','blocked'))` + `count(type='freeThrow' AND missed AND pct_ft=1 AND personal foul)` | Missed/blocked shots + missed last personal-foul FTs |
| `tov_count` | `count(type='turnover')` | All turnovers |
| `total_ft_attempts` | `count(type='freeThrow')` | All FTs, no foul-type filter |
| `total_fga` | `count(type='shot')` | All shots including blocked |

**`complex_flags` CTE:** LEFT JOINs each action to its parent foul via `parent_action_id` to get `parent_type` and `parent_param`. Only matches `type='foul'` parents. Actions without a foul parent get NULL → excluded from personal-foul filters (affects `ts_poss_count` and `oreb_opportunities` FT conditions).

**`pct_ft`:** `parameters_free_throw_number / parameters_free_throws_awarded` (computed in ETL `compute_possessions()`). `pct_ft = 1` means last FT in the sequence.

**Architecture note:** SQL functions (`four_factors_compute`, `fetch_lineups_four_factors`, `get_team_four_factors_dynamic`) only aggregate (`SUM`) pre-computed columns from MVs — they don't recompute raw counts. Fixes to metric formulas only need to touch the base MVs (`player_four_factors_by_game`, `lineup_four_factors_by_game`, `player_advanced_stats_mv`). `team_four_factors_mv` aggregates from `lineup_four_factors_by_game` so it picks up fixes automatically on refresh.

**Deploy note:** Use `etl/.Renviron` (postgres user) for DDL. Port 5432 on same pooler host. Set `search_path` for MVs that reference unqualified tables (`player_advanced_stats_mv`, `onoff_default_mv`, `team_ppp_ratings_mv`, `df_pts_poss_lineups_longer_mv`). SQL files contain multiple statements (CREATE + indexes) — split on `;` and execute individually.

## Code Conventions

- 2-space indent, snake_case, parameterized SQL queries
- Schema `basketball_test` via `in_schema()`; ETL uses `SCHEMA` variable

## Lessons Learned

### PostgreSQL / Supabase
- `LANGUAGE plpgsql` requires exact return types; `PERCENT_RANK()` returns `double precision`
- `REFRESH MATERIALIZED VIEW` re-runs stored definition — must DROP+CREATE to change query
- `DROP MATERIALIZED VIEW ... CASCADE` propagates — always rebuild dependents in L2→L3→L4 order
- `SET search_path` needs `SET LOCAL` in transaction on pooler
- `ANALYZE;` without table fails on Supabase — scope to specific tables
- `score` column from raw JSON is unreliable — use `own_team_score`/`opp_team_score` (cumulative) instead
- Clutch filtering uses IF/ELSE branching in PL/pgSQL: non-clutch path uses pre-aggregated MVs, clutch path queries raw `df_pts_poss_lineups_longer_mv` with inline aggregation (can't use pre-agg MVs because score/time are action-level)
- **segment_id repeats across games** — always include `game_id` in GROUP BY when aggregating by segment_id, otherwise `MAX - MIN` time calculations will be wrong
- **Floor time vs offense-only time** — to get accurate floor time (stint duration), compute `MAX - MIN` of `end_game_seconds_remaining` across ALL rows per segment (no `type_lineup` filter). Within a segment, offense and defense actions interleave, so filtering to offense-only misses defensive possessions' time contribution. Use offense filter only at final SUM to avoid double-counting

### R / Shiny / DT
- **`bigint = "numeric"` in `dbPool()`** — RPostgres returns PostgreSQL `bigint` as R `integer64` by default, which is incompatible with dplyr `coalesce()`, `+`, and many tidyverse operations. Fix: add `bigint = "numeric"` to the pool connection. Safe for basketball stats (precision loss only for values > 2^53). `SUM()` on integer in PostgreSQL returns `bigint`, so even flag columns (CASE 0/1) produce bigint sums
- **dateRangeInput NA pitfall** — `updateDateRangeInput()` with `start` outside the input's `min` produces `NA`. The "reset to defaults" button must use season-appropriate dates (from `season_date_bounds()`), not global `DEFAULT_START`/`DEFAULT_END`. Also guard `fallback_needed()` and `live_result_df()` against NA dates: `if (is.na(start_d) || is.na(end_d)) return(FALSE)` and `req(!is.na(rng[1]), !is.na(rng[2]))`
- **MV fast path via `dbGetQuery`** — replaced `tbl() %>% collect()` for `onoff_default_mv` with direct `dbGetQuery(pg_pool, sprintf(...))`, eliminating 2.2s metadata query. Requires `bigint = "numeric"` in pool
- `formatRound()` clobbers JS `columnDefs` render — do all formatting in JS if using custom render
- `uiOutput`/`renderUI` causes NULL window on startup — use static inputs + `update*Input()`
- Hoist `colorRampPalette()`, `seq()` to global constants
- `FixedColumns` takes too much space on mobile — use `FixedHeader` only
- Mobile sidebar: wrap in `collapse d-md-block`, button with `d-md-none`; keep view mode toggles outside collapse
- **Deferred table init:** `tbl(pool, in_schema(...))` queries metadata at source time, which fails on Supabase pooler. Wrap in `init_tables()` called from server function instead
- **Modular refactor:** Use `source("R/file.R", local = TRUE)` pattern. Tab servers are functions receiving `(input, output, session, shared)`. Shared reactives passed via list to avoid duplication
- **DT JS render `row` guard:** When `filter = "top"` is used, DT calls render functions during filter init with no `row` arg. Always guard: `if (type !== 'display' || !row) return data;`. A TypeError here crashes the entire page and blocks ALL Shiny client-side processing (selectize, inputs, etc.)
- **`server = TRUE` in `updateSelectizeInput`:** Only use when setting `choices`. Omit when only updating `selected` — it re-registers the server callback without choices, causing stale dropdowns. To clear old tags on team switch (multi-select + server mode), send empty-choices update first, then set new choices

### Deploy Scripts
- `$$`-quoted SQL: don't regex-split — use `DROP FUNCTION` then single CREATE string
- Long `Rscript -e` segfaults — write to temp .R file instead
- For MV DDL: read SQL files with `readLines()` + `paste(collapse="\n")`, strip comment header, execute as single string
- When rebuilding all MVs after CASCADE: use a helper `run_sql(label, sql)` with tryCatch for progress logging
- Always test DB logic on a single `game_id` first before deploying MV changes
- **DROP FUNCTION signature must be exact** — when changing RETURNS TABLE, the old function must be dropped first with its exact parameter signature. The `-- DROP FUNCTION` comment in SQL files may have stale signatures from before clutch params were added. Always verify against the actual CREATE OR REPLACE parameter list (count of params must match)
- **Function boundary detection:** For `$function$`-delimited SQL, find end with `grep("^\\$function\\$;$")` — don't use `LANGUAGE plpgsql` which precedes the body

### Clutch Path CTEs
- **Propagate `team_id` through all CTEs** — `segment_times`, `segment_stats`, and `lineup_totals`/`lineup_ff` must include `team_id` in SELECT, GROUP BY, and JOIN conditions. Different teams can share the same `lineup_hash`, causing "column team_id is ambiguous" errors if omitted
- **Always use table aliases in PL/pgSQL CTEs** — Unqualified column names like `SELECT team_id FROM clutch_actions` cause "ambiguous" errors because PostgreSQL can't distinguish between column references and PL/pgSQL variables. Always use aliases: `SELECT ca.team_id FROM clutch_actions ca`
- **Parallel file consistency** — `fetch_lineups_all.sql` and `fetch_lineups_four_factors.sql` have near-identical clutch path structures. When fixing one, verify the other matches. Reference `fetch_lineups_all.sql` as the canonical pattern

### Debugging Process
- **Check data before code.** When a UI element "doesn't work," first verify what the data pipeline actually returns (`SELECT MIN/MAX/COUNT` on the MV, check column types with `class()`). Don't analyze rendering code or reactive chains until you've confirmed the data is correct. A simple diagnostic query is worth more than 10 minutes of static analysis.
- **MVs bake in parameters — always check what's fixed.** When an MV is the "fast path" for a parameterized function, the MV has equivalent built-in filters (WHERE clauses). If the function takes `min_on` as a param, the MV has a `WHERE on_poss >= X`. Before adding UI controls that interact with MV data, read the MV SQL to understand its constraints. This applies to any pre-computed view.
- **Trace the full type chain.** When adding new columns that flow through SQL → R → dplyr → DT/JS, trace the types at each stage. PostgreSQL `SUM(integer)` → `bigint` → R `integer64` → incompatible with dplyr. Catching this requires thinking about the pipeline, not just the code at each layer.
- **Test incrementally, not all at once.** Multi-file changes (SQL MVs + SQL functions + R rendering + CSS + UI) should be deployed and tested one layer at a time. Deploy SQL, verify with a query. Add R code, test the app. Don't stack 7 file changes and deploy everything, then debug a cascade of interacting failures.
- **Use your own documentation during debugging.** CLAUDE.md documents how the MV/function architecture works. Consulting it during debugging — not just during implementation — would immediately point to the right layer (e.g., "the function takes min_on → the MV must pre-filter → that's why the slider has no effect below 300").
