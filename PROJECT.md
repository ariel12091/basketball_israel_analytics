# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Token Efficiency

This file is updated after every session. **Trust this context** — avoid re-reading files or spawning exploration agents when the answer is documented here. Use `replace_all=true` for repetitive edits, read files in large chunks (limit=300+), and batch similar operations.

## Project Overview

Basketball Israel Analytics — R/Shiny dashboard for player on/off impact, lineup combos, and team ratings. Data from play-by-play JSON (basket.co.il / stats.segevstats.com) → ETL → PostgreSQL (Supabase) → Shiny app.

**Live app:** https://ibpl-stats.shinyapps.io/onoff-shiny/

**Tech:** R 4.4.2, Shiny (bslib/BS5), DBI/RPostgres (no dbplyr), PostgreSQL on Supabase (port 6543), schema `basketball_test`, deployed to shinyapps.io

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
│   ├── ui_tab4_gamelogs.R Tab 4 UI (Game Logs)
│   ├── server_tab1.R      Tab 1 server logic (~400 lines)
│   ├── server_tab2.R      Tab 2 server logic (~600 lines)
│   ├── server_tab3.R      Tab 3 server logic (~250 lines)
│   └── server_tab4.R      Tab 4 server logic (~300 lines)
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

**Direct SQL queries (no dbplyr lazy tables):** All DB access uses `DBI::dbGetQuery(pg_pool, ...)` with parameterized SQL. No `tbl()`/`in_schema()` calls — eliminates metadata round trips. Pool is pre-warmed at source time with `SELECT 1` to force the SSL handshake before any user session.

### Shiny Tabs

All tabs: sidebar 3-col / main 9-col, FixedHeader extension, mobile collapse behind "Show Filters"

| Tab | Filters | Fast Path (MV) | Filtered Path (SQL) |
|-----|---------|----------------|---------------------|
| 1: On/Off Impact | season, dates, team, game filters, min poss | `onoff_default_mv` / `player_advanced_stats_mv` | `onoff_compute()` / `four_factors_compute()` |
| 2: Lineup Data | + players on/off, group size 2-5, clutch time | — (always SQL) | `fetch_lineups_csv_v2()` / `fetch_lineups_four_factors_csv()` |
| 3: Team Ratings | season, dates, game filters, clutch time | `team_ppp_ratings_mv` / `team_four_factors_mv` | `get_team_ratings_dynamic()` / `get_team_four_factors_dynamic()` |
| 4: Game Logs | season, team (optional), dates, game filters | `mv_lineup_totals_by_day` + `final_schedule_mv` / `lineup_four_factors_by_game` | — (direct MV queries) |

**GN filters note:** If a reactive uses bindEvent(), include the GN reactive (or raw GN inputs) in the bindEvent triggers. Otherwise GN changes will not re-run the computation even if SQL supports GN filtering (this was the root cause in Tab 1 before adding gn_params() to the bindEvent list).

## Session Bug Fixes (GN)
- GN filter changes did not update Tab 1 output because `gn_params()` was not included in the `bindEvent` triggers for `result_df()`. Fix: add `gn_params()` to the `bindEvent` list in `app/R/server_tab1.R`.
- `attempt to use zero-length variable name` error on app load came from named selectize choices like `c("" = "")`. Fix: use unnamed choices (e.g., `c("", as.character(gn_vals))`) for GN selectize inputs in server tabs.
- GN selectize inputs appeared to do nothing because fallback stayed on MV when GN changes didn�t trigger recompute. Fix is the `bindEvent` update above (and ensuring GN raw inputs are non-empty when selected).
## Session Summary (Plan)
1. Read the GN implementation plan and existing server logic
   - Verified intended GN behavior and noted that dynamic SQL path must be triggered when GN filters are set.
2. Convert GN inputs to selectize and enforce mutual exclusivity
   - Implemented GN range (`min/max`) and last-N as selectize inputs, with observers that clear the other mode when one is set to prevent conflicting filters.
3. Wire GN filters into SQL compute calls
   - Passed `min_gn`, `max_gn`, and `last_n` into `onoff_compute`, `four_factors_compute`, `fetch_lineups_*`, and `get_team_*` so GN filters actually affect results instead of the MV.
4. Fix GN selectize crash on load
   - Replaced named empty choices (`c("" = "")`) with unnamed empty choices (`c("", ...)`) to avoid the R error `attempt to use zero-length variable name`.
5. Fix GN reactivity in Tab 1
   - Added `gn_params()` to the `bindEvent(...)` triggers for `result_df()` in `app/R/server_tab1.R` so GN changes re-run the reactive and bypass the MV when needed.
6. Validate behavior with manual SQL checks
   - Compared GN range and last-N results against manual aggregation for multiple metrics across all tabs to confirm outputs matched.
7. Document the failure mode and fixes
   - Added GN bindEvent note and session bug fixes in `PROJECT.md` for future reference.
## Ops Notes (ETL Scheduler)
- Task Scheduler inline command strings are brittle (quoting errors caused failures); use scripts/run_etl_full.ps1 wrapper instead.
- Daily ETL is run via Windows Task Scheduler task `onoff_etl_full_daily` calling `scripts/run_etl_full.ps1` (wrapper avoids quoting issues).
- Wrapper runs `etl_full(dry_run=TRUE)` then `etl_full(dry_run=FALSE)`, appends output to `logs/etl_full.log`, and deletes logs older than 2 days.
- Successful runs write a marker timestamp to `etl/logs/last_success.txt`; the app reads this to show "Last updated" in the top-right.
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

**`fetch_israel_schedule()`:** Fetches from `basket.co.il/pbp/json/games_all.json`, flattens game objects via `as.data.frame()`. JSON field names are mixed-case (e.g. `GN`, `ExternalID`) — must explicitly map to lowercase DB columns in `mutate()` (e.g. `gn = as.integer(GN)`, `game_id = as.integer(ExternalID)`). The `upsert_by_like()` helper matches columns by **exact name** (case-sensitive), so unmapped uppercase JSON fields get dropped and the DB column gets `NA`.

**`sched_long`:** A VIEW (not MV) — reads live from `schedule`. Changes to `schedule` are visible immediately; only `final_schedule_mv` needs an explicit `REFRESH`.

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

Available in Tab 1 (On/Off Impact) Summary, Tab 2 (Lineup Data) Summary, and Tab 4 (Game Logs) Summary. Not in Four Factors views or Tab 3. Tabs 1, 2, and 4 show the shot splits legend box (conditionally visible in Summary mode only).

**Tab 1:** 16 columns (off/def × on/off × fg2/fg3 × made/att). Source: `onoff_default_mv` via `shot_agg` CTE, or `onoff_compute()` via `player_onoff_by_game`.

**Tab 2:** 8 columns (off/def × fg2/fg3 × made/att) — no on/off split since Tab 2 shows lineup-level stats. Columns: `off_fg2_made`, `off_fg2_att`, `off_fg3_made`, `off_fg3_att`, `def_fg2_made`, `def_fg2_att`, `def_fg3_made`, `def_fg3_att`.
- **Fast path:** reads from `sub_lineups_stats` (8 shooting columns populated by `refresh_sub_lineups_stats()`)
- **Non-clutch filtered path:** SUMs `fg2_made/att`, `fg3_made/att` from `mv_lineup_totals_by_day` (which already has these columns), then FILTER by offense/defense
- **Clutch path:** CASE expressions in `clutch_actions` CTE compute shot flags from raw `df_pts_poss_lineups_longer_mv`, propagated through `segment_stats` → `lineup_totals`, FILTER in final SELECT
- **R rendering:** "Off Shot" / "Def Shot" display columns after +/-, JS stacked bar visualization (same `make_shot_render` pattern as Tab 1), dynamic weighted averages for color thresholds (min 50 FGA)
- **Deploy:** `deploy_shooting_tab2.R` — ALTER TABLE + deploy functions + refresh

## Tab 4: Game Logs

Teams-only per-game log with Summary and Four Factors views. Displays all games immediately on tab click (no team selection required). Team dropdown defaults to "All teams" and is optional.

**Data sources:** `mv_lineup_totals_by_day` (Summary) / `lineup_four_factors_by_game` (FF), joined with `final_schedule_mv` for schedule info.

**Ordering:** `gn` (game number from schedule), `game_id`, `game_date`, `team_name`. DT sorts by GN ascending (column 0).

**Columns:** GN, Date, Team, Opponent, W/L, Score, Off/Def PPP, Net, Off/Def Shot splits, Off/Def Poss, Min (Summary) or Four Factors rates.

**Multi-team pattern:** Uses `sched_pairs` (game_id + team_id) `inner_join` to support all-teams display — no single `team_id` filter required.

## Tab 2: Clickable Lineup → Modal Game Log

In Tab 2 (Lineup Data), player name columns are clickable links (both Summary and Four Factors views). TOTAL row is not clickable (guarded by `row[0] === 0` / `is_total` check).

**Click handler:** JS `onclick` → `Shiny.setInputValue('ld_lineup_click', {hash, team_id, ts}, {priority: 'event'})`. Hidden columns `team_id` and `sub_lineup_hash` are appended to the DT data and hidden via `columnDefs`.

**Modal handler (`observeEvent(input$ld_lineup_click)`):**
1. Resolves `sub_lineup_hash` → `lineup_hash(es)` via `sub_lineups` table (5-man case: hash used directly)
2. Branches on `input$ld_view_mode`:
   - **Summary:** Queries `mv_lineup_totals_by_day` → PPP + shot splits modal
   - **Four Factors:** Queries `lineup_four_factors_by_game` → TS%, OREB%, TOV%, FTR modal (same pattern as Tab 4 FF)
3. Pivots offense/defense → one row per game
4. Joins `final_schedule_mv` for GN, date, opponent, score, result
5. Gets lineup name from `sub_lineups_stats.player_names_str`
6. Renders DT in `modalDialog(size = "xl")` with W/L coloring, ordered by GN

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
- **All DB access uses `dbGetQuery()`** — no `tbl()`/`in_schema()` anywhere in active code. Eliminates metadata round trips (~200-400ms each to Supabase). Pool pre-warmed with `SELECT 1` at source time. Requires `bigint = "numeric"` in pool
- `formatRound()` clobbers JS `columnDefs` render — do all formatting in JS if using custom render
- `uiOutput`/`renderUI` causes NULL window on startup — use static inputs + `update*Input()`
- Hoist `colorRampPalette()`, `seq()` to global constants
- `FixedColumns` takes too much space on mobile — use `FixedHeader` only
- Mobile sidebar: wrap in `collapse d-md-block`, button with `d-md-none`; keep view mode toggles outside collapse
- **Modular refactor:** Use `source("R/file.R", local = TRUE)` pattern. Tab servers are functions receiving `(input, output, session, shared)`. Shared reactives passed via list to avoid duplication
- **DT JS render `row` guard:** When `filter = "top"` is used, DT calls render functions during filter init with no `row` arg. Always guard: `if (type !== 'display' || !row) return data;`. A TypeError here crashes the entire page and blocks ALL Shiny client-side processing (selectize, inputs, etc.)
- **`server = TRUE` in `updateSelectizeInput`:** Only use when setting `choices`. Omit when only updating `selected` — it re-registers the server callback without choices, causing stale dropdowns. To clear old tags on team switch (multi-select + server mode), send empty-choices update first, then set new choices

### ETL / upsert_by_like
- **JSON→DB column name case mismatch** — `upsert_by_like()` matches columns by exact name (case-sensitive). JSON fields like `GN`, `ExternalID` don't match lowercase DB columns `gn`, `game_id`. Unmapped fields are dropped and the DB column gets `NA`. Fix: explicitly map in `mutate()` (e.g. `gn = as.integer(GN)`). This caused `gn` to be NULL for 87/113 games in 2026.

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







## Session Notes (Auto-Threshold Min Possessions)
- **Goal:** Auto-adjust min possessions so filtered results always include at least the top 35% by usage, while preserving manual overrides.
- **Tab 1 (On/Off):** Implemented dynamic `min_on_poss` and `min_all_poss` using top 35% by `ON Poss` (`off_on_poss` in fallback/FF). Auto uses min filters = 0 during threshold computation to avoid empty results on sparse date ranges.
- **Tab 2 (Lineups):** Implemented dynamic `ld_minposs` using top 35% by `total_poss` on the locally filtered dataset.
- **Auto/Manual behavior (simple model):**
  - Manual slider change sets `auto_enabled(FALSE)`.
  - Filter changes set `auto_enabled(TRUE)`.
  - Reset sets defaults and `auto_enabled(FALSE)`; auto resumes on next filter change.
- **Why:** Prevents "no rows" on narrow filters while keeping user control intact.

## Security Best Practices

1. **Never hardcode credentials in any file.**
   - Do not put DB passwords, API keys, or tokens in .R scripts, SQL files, docs, or logs.
   - Use app/.Renviron and etl/.Renviron (gitignored) plus Sys.getenv(...) only.

2. **Secret exposure policy (pragmatic).**
   - Treat a secret as compromised if it was committed/pushed, shared externally, or exposed in deployed logs/artifacts.
   - If it only existed in untracked local files and never left the machine, rotation is recommended but not mandatory.

3. **Treat all Shiny client inputs as untrusted.**
   - input$... values and Shiny.setInputValue(...) payloads can be tampered with in browser DevTools.
   - Hidden columns / UI controls are not a security boundary.

4. **Parameterize SQL values; never interpolate user-controlled values into SQL text.**
   - Use DBI::dbGetQuery(..., params = list(...)) with placeholders ($1, $2, ...).
   - Do not build SQL with sprintf() / paste0() for dynamic user values (especially WHERE, IN, ANY filters).

5. **Prefer fail-closed behavior on unexpected input.**
   - If lookup/validation fails, do not fall back to raw client input for SQL filtering.
   - Validate type/shape first, and return no data or an explicit error if invalid.

## Backlog - Security/Resilience

1. **DB statement timeout guardrail**
   - File: pp/R/global.R
   - Add a pooled connection statement_timeout (for example 8s) via DBI::dbExecute(...) wrapped in 	ryCatch.

2. **Short TTL cache for Tab 4 season-heavy queries**
   - File: pp/R/server_tab4.R
   - Cache gl_lineup_totals and gl_lineup_ff by game_year for 30-60 seconds.

3. **Click burst guard for lineup modal**
   - File: pp/R/server_tab2.R
   - Ignore duplicate ld_lineup_click events within ~300ms to reduce accidental query bursts.

## Session Lessons (2026-02-12 Security + Ops)

1. Scheduler reliability depends on task settings, not just script correctness.
   - The nightly ETL missed its run at 23:30 because Task Scheduler terminated the task (`0xC000013A`).
   - Hardened settings that mattered: `StartWhenAvailable=True`, `WakeToRun=True`, and battery-stop flags disabled.

2. "Last updated" in the app is source-priority driven.
   - The app first reads `basketball_test.app_meta.etl_full_last_success`.
   - If DB metadata is stale/unavailable, it falls back to `etl/logs/last_success.txt`.
   - Operational implication: refresh/update DB metadata in ETL to keep UI status accurate.

3. Log strategy should be append-safe and forensic-friendly.
   - Replaced single rolling wrapper log with per-run files: `etl_full_wrapper_YYYYMMDD_HHMMSS.log`.
   - Keep retention cleanup, but avoid amending a single file across runs.
   - This makes failed runs easier to isolate and compare.

4. Interactive-only scheduled tasks have an execution caveat.
   - Current task mode is `Interactive only`, so it may not run when no user session is active.
   - If we need true unattended nightly runs, switch task principal/logon mode accordingly.




## Session Lessons (2026-02-12 Startup Performance + Validation)

1. Tab 3 heavy compute is now activation-gated and should be validated by network evidence, not assumption.
   - `server_tab3.R` already gates the expensive path with `observeEvent(list(input$tr_game_year, input$main_tabs), ignoreInit = TRUE)` and `if (!identical(input$main_tabs, "team_ratings")) return(NULL)`.
   - Live Playwright check confirmed no `dataobj/tr_table` call on initial Tab 1 load; it appears only after clicking Team Ratings.

2. `server = TRUE` on low-cardinality selectize inputs can create unnecessary startup network chatter.
   - Root cause of residual Tab 3 preload was `updateSelectizeInput(..., server = TRUE)` for `tr_opponents`.
   - Switching Tab 3 opponents update to client-side selectize removed initial `dataobj/tr_opponents` calls while preserving behavior after tab activation.

3. Benchmark scripts can fail due to stale function signatures even when app code is correct.
   - `get_team_ratings_dynamic` and `get_team_four_factors_dynamic` now use 17 args (with GN filters); old 13-arg benchmark calls produce false errors.
   - Keep benchmark SQL signatures aligned with deployed function signatures before using results for performance conclusions.

4. Supabase function deploy reliability depends on explicit overload handling.
   - A mixed state occurred when `fetch_lineups_four_factors*` had old/new overloads co-existing.
   - Safe pattern: explicitly drop known overload signatures, parse `$function$` boundaries, then execute each `CREATE OR REPLACE FUNCTION` separately and verify with `pg_get_function_identity_arguments`.

5. Practical validation flow for startup bottlenecks.
   - Validate in this order: code guard condition -> local run network trace -> live network trace after deploy.
   - This catches cases where local fixes are correct but production still serves old behavior.

## Session Lessons (2026-02-13 UX Explainers + Example Snippets)

1. For first-time users, inline explainers work better than modal walkthroughs.
   - We removed the startup popup and moved guidance into per-tab collapsible explainers that are hidden by default.
   - This reduced interruption while still giving context exactly where users need it.

2. Summary and Four Factors need different educational content.
   - Each tab now has mode-specific explainer text and a mode-specific example snippet.
   - Reusing one explanation across both modes created confusion because columns/logic differ.

3. On shinyapps.io, local image paths can fail; embed snippets as data URIs.
   - `htmltools::dataURI` is not exported; use `base64enc::dataURI` via a helper (`app_image_src()`) in `app/R/global.R`.
   - Rendering snippets with `tags$img(src = app_image_src(...))` made images reliable in deployment.

4. Keep explainers example-driven and numerically aligned with visible table data.
   - We replaced placeholder player text/stats with real rows and matching numbers from actual outputs.
   - The explainer paragraph now maps directly to every data point shown in the snippet.

5. Common Shiny pitfalls in UI refactors.
   - `tags(...)` is invalid; use `tags$p(...)`, `tags$div(...)`, etc.
   - Reactive values cannot be read outside reactive context; session-level one-time flags should use `session$userData` (or equivalent non-reactive state).

6. Snippet readability matters as much as content.
   - Small inline screenshots were unreadable; switching to full-width embedded snippets improved usability.
   - Four Factors snippet crops were adjusted to remove unnecessary blank space that expanded the viewport.

7. Validation flow that worked for this UX cycle.
   - Capture real snippets (Playwright), wire into UI, run parse checks, then verify in deployed shinyapps.
   - This prevented local-only assumptions and caught deployment-specific rendering issues early.

## Session Lessons (2026-02-13 React Frontend UX + Parity Alignment)

1. React migration status is foundation-first, not full replacement yet.
   - The current React app improves UI control and iteration speed, but Shiny still has broader production-complete feature coverage.
   - Treat React as parity work in progress until all tabs/metrics are fully matched and benchmarked.

2. Tab 1 Four Factors parity requires cell-level rendering, not a generic percentile widget.
   - The correct behavior is per-metric diff cells with: diff value, ON/OFF values, shared range track, and ON/OFF dots.
   - Ranking/no-ranking state must be handled per cell (unranked display fallback when rank context is missing).
   - Polarity must follow the Shiny model:
     - Offense TS/OREB/FTR: good-high
     - Offense TOV: good-low
     - Defense TS/OREB/FTR: good-low
     - Defense TOV: good-high

3. Legends must match mode and visual language of the table they explain.
   - On/Off Summary: show shot-split legend only.
   - On/Off Four Factors: show four-factors legend only.
   - Four-factors legend should use the same scale primitives as FF cells (track + ON/OFF dots), with explicit mapping text:
     - white dot = Off-Court
     - black dot = On-Court
   - Avoid extra legend noise (duplicate marker blocks / unnecessary badges).

4. Explainers should be fully hidden at startup.
   - "What this tab answers" and example content are both collapsed by default behind a single toggle.
   - This keeps first paint focused on filters and table content.

5. React examples should reuse real snippet assets, not placeholders.
   - Snippets were copied to `frontend/public/snippets/` and wired to mode-specific explainers:
     - `onoff-row-snippet.png`
     - `onoff-ff-row-snippet.png`
     - `lineup-row-snippet.png`
     - `lineup-ff-row-snippet.png`
   - Keep snippet and paragraph values aligned with real table outputs.

6. Filter panel structure decisions that improved clarity.
   - Removed duplicate reset action: keep a single `Clear filters` button with full default-reset behavior.
   - Renamed `Core Filters` to `Time Filters`.
   - GN wording standardized to `Game Number`.
   - `Teams` for On/Off moved into `Game Filters`.
   - `Eligibility` made non-accordion with mobile-friendly sliders.
   - For Lineups tab:
     - `Lineup Specific` moved to first position and open by default.
     - `Group Size` moved out of sidebar into page-level inline controls.
     - `Clutch Filter` made non-accordion and placed last.

7. Navbar and sidebar compaction improved usability on dense screens.
   - Reduced vertical bloat in header/sidebar spacing.
   - Sidebar made sticky + internally scrollable on desktop, normal flow on mobile.
   - Multi-select behavior retained with menu staying open for fast repeated selection.

### Frontend-v2 API hardening (2026-02-16)

1. Added explicit CORS allowlist in `frontend-v2/server/plumber.R`.
   - `FRONTEND_ALLOWED_ORIGINS` (comma-separated origins).
   - Requests with `Origin` outside this list return `403`.

2. Added optional API key filter (no-registration friendly by default).
   - `FRONTEND_API_KEY`:
     - Empty/unset: anonymous access (current behavior).
     - Set: requires `X-API-Key` (or `Authorization: Bearer <key>`), else `401`.

3. Added basic IP rate limiting in Plumber.
   - `FRONTEND_RATE_WINDOW_SEC` (default `60`)
   - `FRONTEND_RATE_MAX_REQUESTS` (default `180`)
   - Exceeded limit returns `429`.
   - Note: in-memory/per-process; use infra-level limits for production-grade enforcement.

4. Tightened default bind host in `frontend-v2/server/run.R`.
   - Defaults now:
     - `PLUMBER_HOST=127.0.0.1`
     - `PLUMBER_PORT=3002`
   - Override with env vars when intentionally exposing behind a proxy.

## Session Lessons (2026-02-16 Lineups Modal + Filter Ownership)

1. Tab 2 lineup modal failure root cause was backend JSON boxing, not click wiring.
   - `LineupModal` expected numeric scalars (`offPpp`, `defPpp`, `minutes`, etc.) and called `toFixed()`.
   - `/api/lineups/game-log` was returning one-element arrays for these fields under the default serializer, causing runtime `TypeError` and blank modal.
   - Fix: switched endpoint serializer to `unboxedJSON` in `frontend-v2/server/plumber.R` for `/api/lineups/game-log`.

2. Click flow parity with Shiny is structurally correct.
   - Both Shiny and Plumber resolve `sub_lineup_hash -> lineup_hash(es)`, join schedule context, aggregate offense/defense by game, then sort by GN.
   - Main difference is transport: Shiny renders modal table directly server-side, React consumes JSON from Plumber.

3. Tab 2 drawer filter impact was partially misunderstood and had to be audited field-by-field.
   - Drawer filters that affect Tab 2 query results: season, date range, game type, opponents, home/away, outcome, GN range, last N, opponent strength.
   - Drawer filters that do not affect Tab 2 query results: drawer teams, drawer eligibility sliders.
   - Tab 2-specific controls (group size, clutch, local team/players/min poss) are handled by tab-local state and/or local post-fetch filtering.

4. Tab ownership of filters was clarified and adjusted.
   - Moved Tab 1-exclusive controls (`Teams`, `Min ON Poss`, `Min All Poss`) out of global drawer and into `OnOffPage` in the same inline control style used by Tab 2.
   - Removed those controls from `FilterDrawer` to avoid cross-tab confusion and false expectations.

5. Local debugging can be invalidated by port collisions across multiple Vite apps.
   - `frontend` and `frontend-v2` were both bound to `5173`; Playwright initially opened the wrong app.
   - Always verify owning process for the dev port before UI diagnosis.
## Session Lessons (2026-02-16 Tab 2 Clear Filters Visibility + Reset Scope)

1. Restored original conditional visibility behavior for clear controls.
   - Removed extra always-visible Tab 2 clear buttons.
   - Kept global clear controls hidden until at least one active filter exists.

2. Included Tab 2 local lineup player selections in active-filter detection.
   - Added `lineupPlayersActive` to shared filter state.
   - Set true when `Players On` or `Players Off` has selections.
   - Reset to false on tab unmount.
   - Result: existing clear controls appear when lineup local selections are active.

3. Extended global clear action to reset Tab 2 local state.
   - Added `resetSeq` counter in shared state, incremented on `RESET`.
   - `LineupsPage` listens to `resetSeq` and clears local controls.
   - Reset now includes: `teamId`, `playersOn`, and `playersOff`.

4. Outcome aligns with intended UX contract.
   - No persistent extra clear button clutter.
   - Same conditional visibility as before.
   - Clear action now fully clears Tab 2 lineup selectors, including Team.

## Session Lessons (2026-02-17 React Tab 1/2 Filter Semantics + Stability)

1. Keep Tab 2 fast-path filtering client-side unless there is a proven scaling issue.
   - We tested adding backend params for local lineup filters, then reverted.
   - Current preferred model: API returns the base lineup set, UI applies local team/player/min-poss filters.

2. "Min All Poss" semantics must be explicit and aligned with user intent.
   - Correct behavior in Tab 1 is NOT on + off >= threshold.
   - Correct behavior is BOTH sides pass: on >= min_all AND off >= min_all (plus min_on as a separate gate).

3. Low-threshold rows (especially min=0) require defensive rendering.
   - Sparse rows can contain NA/null numerics and unstable keys.
   - Hardening applied:
     - /api/onoff/summary normalizes NA in all numeric columns before response.
     - HeatCell is null/NaN-safe and renders - instead of crashing on 	oFixed.
     - Tab 1 row keys use a composite key to avoid reconciliation issues that looked like "sorting not working".

4. "Clear all" visibility should not depend only on visible chips.
   - Local-only Tab 2 state (team / players on / players off) can be active even when there are no shared chips.
   - FilterChips must still render the bar when hasActiveFilters is true, so users can clear local state.

5. Final team-filter ownership model for React Tab 1/2:
   - Drawer Teams filters Tab 1 and Tab 2 result tables.
   - Tab 2 local Team is only for "Lineup Player Selection" (populating Players On/Off options).
   - Local team and drawer teams are mutually exclusive:
     - selecting local team clears drawer teams,
     - selecting drawer teams clears local team.
   - Local team placeholder uses Select team (not All teams) to avoid implying table filtering.

6. Game Type in drawer should be multi-select with menu left open.
   - Replaced single-select control with multi-select eact-select.
   - Kept closeMenuOnSelect={false} for faster batch selection.
## Session Notes (2026-02-17 Parity Audit: Shiny vs React+Plumber)

1. Fixed in this session:
   - Tab 1 FF filtered join now uses Shiny-equivalent key in plumber:
     - `player_id + team_id` (fallback to `player_id` only if `team_id` is missing unexpectedly).
   - Tab 1 FF fast-path MV join now also includes `team_id`:
     - `ff.player_id = o.player_id AND ff.team_id = o.team_id AND ff.game_year = o."Year"`.

2. Remaining discrepancies to track:
   - Ranking edge case (`n <= 1`) differs:
     - React `percentileRank()` returns `0.5` for single qualified row.
     - Shiny `pr_vec()` returns `NA` (unranked).
   - Adaptive baseline quantile implementation differs slightly:
     - React uses index-based p75 approximation.
     - Shiny uses `quantile(..., 0.75)` interpolation.
   - API default `end_date` differs from Shiny season bound convention:
     - Plumber defaults use `YYYY-06-30` on several endpoints.
     - Shiny season bounds use `YYYY-07-01`.
## Session Notes (2026-02-17 FF Filtered Bottleneck Benchmark)

1. Change tested:
   - Replaced filtered Tab 1 FF path in plumber from:
     - `four_factors_compute` + `onoff_compute` + R `merge`
   - To:
     - single DB-side join query (`run_ff_with_diffs_compute`) keyed by `player_id + team_id`.

2. Benchmark result (same filter params, 12 timed runs after warmup):
   - Old path median: `0.990s`
   - New path median: `0.780s`
   - Speedup: `1.27x` (about `210ms` median saved)
   - Row-count parity: both returned `266` rows.

3. Conclusion:
   - Improvement is valid but moderate.
   - This confirms the R merge layer was not the main latency driver.
   - Primary remaining bottleneck is inside compute SQL/function runtime.

## Backlog - Performance (Tab 1/2 React+Plumber)

1. Profile SQL functions directly with `EXPLAIN (ANALYZE, BUFFERS)`:
   - `basketball_test.four_factors_compute`
   - `basketball_test.onoff_compute`
   - Use representative filtered cases (GN, opponent strength, game type, clutch).

2. Add request-level timing instrumentation in plumber for high-cost endpoints:
   - `/api/onoff/four-factors` (filtered)
   - `/api/lineups/summary`
   - `/api/lineups/four-factors`
   - Log DB time vs serialization time separately.

3. Add short TTL cache for repeated identical filtered requests (API layer).

4. Audit indexes only after query-plan evidence (avoid speculative indexing).
## Session Notes (2026-02-17 Player IDs Parsing Optimization)

1. Implemented fix:
   - Replaced row-by-row `player_ids` parsing in `frontend-v2/server/plumber.R` with vectorized parsing via a single batched JSON decode helper (`parse_pg_int_array_json`).
   - Applied in both `rename_lineup_summary()` and `rename_lineup_ff()`.

2. Benchmark (1988 lineup rows; 3 methods):
   - Old loop parse median: `0.4700s`
   - Vectorized parse median: `0.1900s`
   - SQL JSON-return parse median: `0.1600s`
   - Speedup vs old:
     - Vectorized: `2.47x`
     - SQL JSON-return: `2.94x`

3. Conclusion:
   - Vectorized parser is now the active implementation and removes per-row loop overhead.
   - SQL JSON-return remains a possible incremental follow-up, but vectorized parsing already captures most of the gain.

4. Engineering rule:
   - Do not use loops for data transformations unless absolutely necessary.
   - Prefer vectorized/batched operations first; only use loops when correctness requires sequential logic or no vectorized alternative exists.
## Session Notes (2026-02-17 Lineup Modal Game-Log Vectorization + Real Benchmark)

1. Implemented easy vectorized win in plumber Tab 2 modal backend.
   - Endpoint: `frontend-v2/server/plumber.R` `/api/lineups/game-log`.
   - Replaced row-by-row `lapply` game assembly and per-row schedule lookup:
     - `sched[sched$game_id == r$game_id, ]`
   - New path uses:
     - one keyed `merge(..., by = "game_id")` with schedule,
     - vectorized column calculations,
     - one `data.frame(...)` build,
     - vectorized GN sort.
   - Applied to both view modes: `summary` and `ff`.

2. Real endpoint benchmark (live API path, not synthetic) on `http://127.0.0.1:3002`.
   - Request:
     - `/api/lineups/game-log?sub_hash=d372f7ce005e03780a3adeead1feae25&team_id=14&game_year=2026`
   - 30 requests per mode:
     - `summary`: `p50 1124.1ms`, `p90 1143.1ms`, `mean 1130.4ms`, `min/max 1108.4/1228.3ms`
     - `ff`: `p50 1120.6ms`, `p90 1134.5ms`, `mean 1132.4ms`, `min/max 1113.3/1428.7ms`

3. Takeaway.
   - Vectorization is still the correct implementation and removes avoidable R loop overhead.
   - On real requests, total latency is now dominated by DB/query + network path, not R row assembly.
   - Next optimization focus should remain SQL/function runtime and query planning.

4. Benchmark execution reliability note.
   - `Rscript -e` via PowerShell can corrupt inline R when `$` symbols are expanded and can hit encoding/BOM issues.
   - Stable method: write benchmark to a temporary ASCII `.R` file and run `Rscript <file>`.

## Session Notes (2026-02-17 onoff_compute Bottleneck Refactor)

1. Added endpoint-level timing instrumentation in `frontend-v2/server/plumber.R` (opt-in).
   - Controlled by `FRONTEND_PROFILE_TIMING=1`.
   - Logs `total_ms`, `db_ms`, `transform_ms`, and row count for:
     - `/api/onoff/summary`
     - `/api/onoff/four-factors`
     - `/api/lineups/summary`
     - `/api/lineups/four-factors`
     - `/api/lineups/game-log`

2. Root-cause profiling (EXPLAIN ANALYZE) findings.
   - `onoff_compute` was the main DB hotspot (~1.23s execution in sampled run).
   - `fetch_lineups_four_factors_csv` was the next major hotspot (~675ms).

3. SQL optimizations applied to `sql/functions/onoff_compute.sql`.
   - Replaced broad name join (`full_rosters` x `final_schedule_mv` + DISTINCT) with schedule-scoped `roster_names` CTE.
   - Reworked analytic pipeline to avoid repeated row-level window passes:
     - replaced `step1/step1_on_rank/step1_joined/step2` row-flow with pivoted `type_level` and ranked `type_ranked`.
   - Removed extra rejoin to `agg` for shooting splits by carrying split columns through pipeline.
   - Kept output contract intact (42 columns) and ranking semantics for `pr_net` via `final_net_rank` on non-null `total_net_rtg`.

4. Benchmarks (same harness, same parameters).
   - Baseline: `onoff_compute` median ~`800ms`.
   - After scoped names fix: median ~`770ms`.
   - After analytic refactor: median ~`390ms`.
   - Net gain vs baseline: ~`51%` faster.

5. Validation.
   - Output shape remained stable: `rows=228`, `cols=42` in benchmark query.
   - `pr_net` remained populated for eligible rows in tested run.

6. Lesson.
   - Larger wins came from reducing repeated window/sort passes and redundant joins, not micro-optimizing R-side transformations.

## Session Notes (2026-02-17 fetch_lineups_four_factors Fast-Path Activation)

1. Bottleneck identified.
   - `fetch_lineups_four_factors_csv` median latency was ~`665ms` for full-season default calls.
   - Root cause: `fetch_lineups_four_factors` fast path required `p_start_date IS NULL AND p_end_date IS NULL`.
   - React/plumber sends explicit season dates by default, so fast path was skipped even for full-season queries.

2. SQL fix in `sql/functions/fetch_lineups_four_factors.sql`.
   - Added season-window detection:
     - `v_season_start = make_date(p_game_year - 1, 10, 1)`
     - `v_season_end   = make_date(p_game_year, 7, 1)`
     - `v_full_window` true when explicit dates cover that full window.
   - Updated fast-path gate to allow either:
     - null dates, or
     - explicit full-season window (`v_full_window`).

3. Benchmark result (same harness, 8 runs).
   - Before: median `665ms`, p90 `699ms`.
   - After: median `435ms`, p90 `452ms`.
   - Improvement: ~`34.6%` faster.

4. Validation.
   - Full-season explicit dates and null-date calls now produce identical outputs in tested case:
     - `rows base=2511`, `rows nullwin=2511`, diff rows on key metrics = `0`.
   - Narrower date range still changes rows as expected (`rows shifted=2490`).

5. Lesson.
   - Fast-path predicates should align with API calling conventions (explicit default dates vs NULL dates), otherwise planned DB optimizations stay inactive.

## Session Notes (2026-02-17 fetch_lineups_four_factors Clutch Path Optimization)

1. Clutch bottleneck profile (pre-fix).
   - `non_clutch` median: `390ms`.
   - Clutch scenarios were much slower:
     - `margin<=5, all, <=300s`: `1090ms` median.
     - `margin<=8, tied, <=180s`: `985ms` median.
     - `margin<=10, leading, <=240s, OT`: `975ms` median.

2. Root-cause target.
   - In clutch path, `complex_flags` read from full `df_pts_poss_lineups_longer_mv` as base (`d`) and then joined parent foul rows.
   - This forced broader scans than needed because the expensive parent lookup should only run for already filtered clutch rows.

3. SQL fix in `sql/functions/fetch_lineups_four_factors.sql`.
   - Rewrote `complex_flags` CTE to start from `clean_stats` (already date/team/filter constrained) instead of full raw MV.
   - Kept parent foul join semantics the same (`t2.id = parent_action_id`, same game, `t2.type='foul'`).

4. Benchmark result (same harness after deploy).
   - `non_clutch`: `410ms` median (within expected variance).
   - Clutch improved substantially:
     - `margin<=5, all, <=300s`: `450ms` median (from `1090ms`, ~`58.7%` faster).
     - `margin<=8, tied, <=180s`: `390ms` median (from `985ms`, ~`60.4%` faster).
     - `margin<=10, leading, <=240s, OT`: `440ms` median (from `975ms`, ~`54.9%` faster).

5. Validation.
   - Clutch query returned valid rows and plausible values after change.
   - Sample checks confirmed non-empty output and normal metric ranges.

6. Lesson.
   - In clutch mode, anchoring secondary joins to already-filtered clutch rows is a high-leverage optimization.

## Session Notes (2026-02-17 fetch_lineups_csv_v2 Fast-Path Activation)

1. Bottleneck identified.
   - `fetch_lineups_csv_v2` non-clutch default calls were slower than expected (~`550ms` median).
   - Root cause: `fetch_lineups_all` fast path required `p_start_date IS NULL AND p_end_date IS NULL`.
   - API sends explicit default season dates, so non-clutch defaults missed fast path.

2. SQL fix in `sql/functions/fetch_lineups_all.sql`.
   - Added season-window detection:
     - `v_season_start = make_date(p_game_year - 1, 10, 1)`
     - `v_season_end   = make_date(p_game_year, 7, 1)`
     - `v_full_window` when explicit dates cover full window.
   - Updated `v_use_fast_path` to allow:
     - null dates, or
     - explicit full-season date window.

3. Benchmark result (same harness, 6 runs).
   - Before:
     - `summary_non_clutch`: median `550ms`, p90 `560ms`.
     - `summary_clutch_5_all_300`: median `425ms`.
     - `summary_clutch_8_tied_180`: median `390ms`.
   - After:
     - `summary_non_clutch`: median `280ms`, p90 `280ms`.
     - `summary_clutch_5_all_300`: median `420ms`.
     - `summary_clutch_8_tied_180`: median `390ms`.
   - Improvement: non-clutch default summary ~`49%` faster.

4. Validation.
   - Full-season explicit dates and null-date calls are equivalent in tested case:
     - `rows base=2514`, `rows null=2514`, `diff=0` on key columns.

5. Lesson.
   - Fast-path enablement must follow frontend/API default parameter behavior, otherwise optimized SQL branches stay inactive.

## Ops Incident (2026-02-17 ETL)
- **Symptom:** Scheduled task `onoff_etl_full_daily` showed running/stuck behavior (`0x41301`) and produced no new wrapper log for 2026-02-17; app "Last updated" stayed stale.
- **Root cause:** `scripts/run_etl_full.ps1` used `System.Diagnostics.Process` with synchronous `StandardOutput.ReadToEnd()` / `StandardError.ReadToEnd()` before a reliable completion path. Under output pressure this can deadlock and leave wrapper/task hung.
- **Fix applied:** Rewrote wrapper process execution to `Start-Process ... -RedirectStandardOutput ... -RedirectStandardError ... -Wait` and capture `ExitCode` directly; added single-instance lock file (`logs/etl_full_wrapper.lock`) to prevent overlap.
- **Verification:**
  - Manual wrapper execution completed with `exit_code=0` and full ETL output captured in `logs/etl_full_wrapper_*.log`.
  - `etl/logs/last_success.txt` updated to `2026-02-18 00:30:37`.
  - Triggered Windows task manually; final status became `Ready` with `LastTaskResult = 0`.
  - New scheduler-path wrapper log created: `logs/etl_full_wrapper_20260218_003142.log`.
- **Operational takeaway:** Prefer script wrappers + file redirection over inline process stream reads for long ETL jobs. Keep locking to avoid concurrent scheduler overlaps.

## Storage Optimization (2026-02-18): Slim `df_pts_poss_lineups_longer_mv`
- **Goal:** Bring DB storage below free-tier threshold while preserving app behavior.
- **Change:** Reduced `basketball_test.df_pts_poss_lineups_longer_mv` to only columns used by downstream MVs/functions (Tab 1/2/3/4 paths, clutch logic, traditional stats), and replaced index set with query-relevant indexes only.
- **File updated:** `sql/materialized_views/df_pts_poss_longer.sql`.
- **Rebuild required:** Ran full MV dependency rebuild via `source('sql/rebuild_all_mvs.R'); rebuild_all_mvs(from_level = 1)`.
- **Measured impact:**
  - DB total after change: **449 MB** (`pg_database_size` = `470,690,963` bytes).
  - `df_pts_poss_lineups_longer_mv` after change: **123 MB** (`129,269,760` bytes).
  - This moved the project below the `<500 MB` target.
- **Validation:** Key runtime outputs still return expected row counts:
  - `onoff_default_mv` (561), `player_traditional_stats_mv` (4710),
  - `fetch_lineups_csv_v2` (450), `fetch_lineups_four_factors_csv` (450),
  - `get_team_ratings_dynamic` (14), `get_team_four_factors_dynamic` (14).
- **Takeaway:** For storage-constrained environments, keep heavyweight �long� MVs column-minimal and index-minimal, then rebuild dependent MVs in order.

## Egress Optimization (2026-02-18): Tab 1/2 API payload reduction
- **Objective:** Reduce network egress from React + plumber app without changing user-visible functionality.
- **Backend changes (`frontend-v2/server/plumber.R`):**
  - Added in-memory response cache (`RESP_CACHE`) with TTL (`FRONTEND_CACHE_TTL_SEC`, default 60s) for heavy endpoints:
    - `/api/onoff/summary`
    - `/api/onoff/four-factors`
    - `/api/lineups/summary`
    - `/api/lineups/four-factors`
  - Added `min_poss` query param to lineup endpoints (default `20`) and wired it into SQL function calls (instead of hardcoded `min_poss = 0`).
- **Frontend changes:**
  - `frontend-v2/src/pages/LineupsPage.tsx`: pass tab-local `min_poss` slider value in API params.
  - `frontend-v2/src/hooks/useApi.ts`: added URL-level client cache (meta endpoints TTL 10m, other endpoints TTL 60s) to avoid duplicate fetches and rapid repeat calls.
- **Measured payload impact (game_year=2026, group=5):**
  - Tab 2 Summary:
    - `min_poss=0`: 2530 rows, ~1435.9 KB JSON, ~156.7 KB gzip
    - `min_poss=20`: 450 rows, ~258.2 KB JSON, ~35.6 KB gzip
  - Tab 2 Four Factors:
    - `min_poss=0`: 2527 rows, ~1848.4 KB JSON, ~221.2 KB gzip
    - `min_poss=20`: 450 rows, ~335.4 KB JSON, ~52.5 KB gzip
- **Result:** Default lineup payload now drops by ~77-82% compressed size on initial load (and more on raw JSON), with additional egress savings from server/client response caching.

## Frontend Data Layer Update (2026-02-18): TanStack Query Adoption
- **What changed:** React frontend moved from effect-based manual fetch/cache handling to TanStack Query-backed `useApi`.
- **Files updated:**
  - `frontend-v2/package.json` (added `@tanstack/react-query`)
  - `frontend-v2/src/main.tsx` (added `QueryClientProvider`)
  - `frontend-v2/src/hooks/useApi.ts` (rewritten to `useQuery` while keeping existing hook signature)
  - `frontend-v2/src/features/filters/FilterDrawer.tsx` (moved metadata calls to shared `useApi`)
- **Behavioral impact:**
  - Automatic in-flight dedupe for identical query keys.
  - Standardized cache/stale lifecycle (`staleTime`, `gcTime`) with request cancellation via query signal.
  - Reduced duplicate metadata fetches across components.
  - Existing backend cache in `frontend-v2/server/plumber.R` remains active and complementary.
- **Tradeoffs:**
  - Pros: cleaner async state management, less custom cache code, lower duplicate-request egress risk.
  - Cons: additional dependency + query-key discipline required; stale cache windows must be tuned to data freshness expectations.
- **Validation:** frontend build succeeds after migration (`npm run build`).
## Git History Identity Rewrite + Local Realignment (2026-02-18)
- Rewrote full repo commit history author/committer identity to Ariel Taieb <ariel12091@gmail.com> and force-pushed to origin/main.
- Local repo was then realigned to rewritten origin/main while preserving uncommitted work via stash/restore flow.
- Safety branch created before alignment: ackup/pre_rewrite_20260218_030105.
- Important: commit SHAs changed due to history rewrite; uncommitted local edits were preserved and not included in the rewrite.

## Tab 5 Bottleneck Work + Benchmarks (2026-02-18)
- Scope: addressed selected bottlenecks for Traditional Stats (Tab 5) and measured each change.

### Rebuild
- Ran full MV rebuild (ebuild_all_mvs(from_level = 1)) with ETL credentials.
- Rebuild completed successfully; key post-rebuild counts:
  - df_pts_poss_lineups_longer_mv: 398,700
  - player_traditional_stats_mv: 4,710
  - mv_lineup_totals_by_day: 23,741
  - lineup_four_factors_by_game: 23,741

### #2 Prefilter lineups_lookup in SQL (Tab 5 live path)
- File: pp/R/server_tab5_traditional.R
- Change: replaced season-wide lineups_lookup fetch + R-side reduction with SQL-side filtering to the filtered game/team set.
- Benchmark (5 runs):
  - Old avg:  .582s
  - New avg:  .472s
  - Improvement: 18.9%
  - Output parity: same rows.

### #3 Remove string-based distinct key counting
- Files:
  - sql/materialized_views/player_traditional_stats_mv.sql
  - pp/R/server_tab5_traditional.R
- Change:
  - SQL: COUNT(DISTINCT concat_ws(...)) -> COUNT(DISTINCT (game_id, team_id, poss_end_id))
  - R: 
_distinct(paste(...)) -> 
_distinct(game_id, team_id, poss_end_id)
- Benchmark (5 runs, SQL expression comparison):
  - Old avg:  .598s
  - New avg:  .596s
  - Improvement:  .3% (marginal)
  - Output parity: identical rows and values.

### #6 Index consistency / activation
- Files:
  - sql/performance/traditional_tab_indexes.sql
  - sql/materialized_views/df_pts_poss_longer.sql
- Change:
  - Removed broader duplicate lineup index definition from performance script; retained targeted partial index.
  - Added two Tab 5-focused indexes to df_pts_poss_lineups_longer_mv definition.
- Applied performance indexes in DB using ETL credentials.
- Benchmark (representative filtered actions query):
  - Before:  .326s avg
  - After:  .292s avg
  - Improvement: 10.4%

### #1 Push recalculation filters to SQL for live path (excluding team-only mode)
- File: pp/R/server_tab5_traditional.R
- Change:
  - cts and lineup_map queries now filter directly in SQL by filtered (game_id, team_id) pairs.
  - Used pair-key filtering (game_id_team_id) to preserve exact pair semantics.
  - Team-only filtering remains on MV path (no forced recalculation).
- Benchmark (recalculation scenario with date/game-type/home filters):
  - Old avg: 3.136s
  - New avg: 1.364s
  - Improvement: 56.5%
  - Parity: same_rows=TRUE, same_keys=TRUE.

### Takeaways
- Biggest win came from SQL pushdown on live-path row selection (#1), then SQL prefilter of lineups_lookup (#2).
- Distinct-count string removal (#3) improves clarity and robustness, but perf impact is small in this workload.
- Index adjustments (#6) produce measurable gains and should remain in rebuild/index scripts to keep environments consistent.

## Session Notes (2026-02-18 Tab 5 Index Necessity + Rscript Reliability)
- `idx_df_longer_game_team_core` was tested as **marginal** for the Tab 5 live `acts` query.
- A/B benchmark on the same filter window (`rows=47458`) showed only a small median difference (~2.6%), with no meaningful EXPLAIN runtime gain.
- Decision: drop and remove the index definition from SQL sources to prevent re-creation on rebuild.
- Files updated:
  - `sql/materialized_views/df_pts_poss_longer.sql`
  - `sql/performance/traditional_tab_indexes.sql`

### Rscript execution reliability (PowerShell)
- Common failure mode: inline `Rscript -e` is brittle in PowerShell because of quoting, `$` interpolation, and occasional BOM/encoding issues.
- Recommended method for repeatable benchmarks/scripts:
  1. Write code to a temporary ASCII/UTF-8 (no BOM) `.R` file.
  2. Run `Rscript <temp_file.R>`.
  3. Delete the temp file.
- Operational note: if DDL is included (e.g., `DROP INDEX`), run on a fresh DB connection afterward before timing queries to avoid prepared-statement/session artifacts.

## Session Notes (2026-02-18 Tab 2 Shiny vs Plumber Diff Snapshot)
- Scope reviewed: `app/R/server_tab2.R` vs `frontend-v2/server/plumber.R` (lineup summary, lineup four-factors, lineup game-log).

### High-level conclusion
- Core computation is mostly in Supabase/Postgres (SQL functions + MVs), but behavior still differs by **where filters/post-processing are applied**.
- Shiny is a single reactive pipeline; plumber is split across endpoint params, SQL calls, and frontend-local filtering.

### Endpoint-by-endpoint parity status
1. `/api/lineups/summary`
   - SQL source parity: yes (`fetch_lineups_csv_v2`).
   - Filter parity:
     - Game-level filters (date, game type, opponents, home/away, outcome, opp strength, GN, clutch): aligned in SQL params.
     - Team + players-on/off + local list behavior: split across layers in plumber (client/local) vs centralized in Shiny local post-filter path.
     - `min_poss`: placement differs (plumber currently sends to SQL; Shiny pattern applies locally post-base).

2. `/api/lineups/four-factors`
   - SQL source parity: yes (`fetch_lineups_four_factors_csv`).
   - Same filter placement caveats as summary endpoint.
   - Percentile/ordering drift risk remains if rank/order semantics are computed in different layers/order than Shiny.

3. `/api/lineups/game-log`
   - Logic is close to Shiny modal click path:
     - resolves `sub_lineup_hash -> lineup_hash(es)` from `sub_lineups`,
     - reads `sub_lineups_stats` for display name,
     - joins `final_schedule_mv` metadata,
     - aggregates from `mv_lineup_totals_by_day` (summary) or `lineup_four_factors_by_game` (FF).
   - Main risk is duplicated logic maintenance (two codepaths, one in Shiny and one in plumber), not source-table mismatch.

### Documented gaps to track
- Gap A: `min_poss` filtering stage mismatch (SQL vs local).
- Gap B: local team / players-on / players-off behavior split between frontend and API, not single-source.
- Gap C: percentile/rank semantics may drift if not kept rank-first then local-filter-after (Shiny behavior).
- Gap D: duplicated game-log assembly code increases drift risk over time.

### Operational takeaway
- Even with shared Supabase SQL sources, parity depends on filter ownership/order and post-processing placement.
- For strict parity, define one canonical ownership map per filter and keep it identical across Shiny and plumber.

## Session Notes (2026-02-18 State Cup Ingestion + UI Support)
- Added explicit-ID fallback in etl/etl_full.R:
  - When requested game_ids are missing from etch_israel_schedule() feed, ETL now falls back to rows already present in asketball_test.schedule.
  - Synthesizes pbp_url/ox_url from game_id for fallback rows.
- Upserted State Cup schedule rows into asketball_test.schedule using upsert_by_like and ran ETL validation sequence.

### ETL execution results (test env)
1. Single-game live test (game_id=291) succeeded end-to-end:
   - ctions_clean +580, ull_rosters +34, possessions +580, lineups_lookup +731, pws +580.
   - All validation checks passed.
2. Remaining 2026 games (292,293,294,303,304) succeeded end-to-end:
   - ctions_clean +3052, ull_rosters +160, possessions +3052, lineups_lookup +3633, pws +3052.
   - All validation checks passed.

### Competition type mapping
- Reserved game_type=35 for **State Cup** (distinct from existing 34 Winner Cup).

### UI support added
- Added State Cup (35) option to game-type filters in Shiny tabs 1-5 and React filter UIs.
- Note: Existing rows with game_type=35 are included under "All" even without selection; this change enables explicit selection/isolation.

## Session Notes (2026-02-19 Full Index Benchmark Sweep)
- Scope: run A/B performance checks for all indexes in `basketball_test`.
- Method:
  - Benchmarked all **non-PK / non-UNIQUE** indexes by timing representative queries with index present vs dropped, then recreated.
  - Listed **PK/UNIQUE** indexes as skipped for safety.
- Artifacts:
  - `logs/index_benchmark_ab_safe.csv` (34 safe indexes benchmarked)
  - `logs/index_benchmark_skipped_unique_pk.csv` (22 skipped PK/UNIQUE indexes)

### Results summary
- Most indexes showed small/noise-level deltas.
- Strongly protective indexes (do not drop):
  - `lineups_lookup_lineup_hash_idx` (~+62% slower without)
  - `lineups_lookup_team_player_lineup_on_idx` (~+36.6% slower without)
  - `idx_sub_lineups_gin_players` (~+106% slower without)
  - `idx_sub_lineups_lineup_hash` (~+42.4% slower without)
- Low-risk drop candidates identified:
  - `dfppl_parent_game_idx` (5864 kB, idx_scan=0, ~noise impact)
  - `idx_lff_lineup_hash` (576 kB, idx_scan=0, ~noise impact)
  - `player_traditional_stats_mv_year_team_name_idx` (56 kB, idx_scan=0, ~noise impact)
- Optional tiny cleanup only (negligible storage):
  - `final_sched_mv_opp`, `idx_tffmv_gy`, `team_ppp_ratings_mv_join_idx` (16 kB each)

### Decision taken
- No further index drops were executed in this step (not worth operational churn now).
- Keep benchmark outputs as baseline for future storage-pressure events.

## Session Notes (2026-02-20 State Cup Final 309 JSON + Team-ID Mapping)
- Context: `etl/run_state_cup_final_etl.ps1` correctly detected final `game_id=309`, but ETL live path failed when `games_all.json` did not yet include `ExternalID=309`.
- Verified source links:
  - Final page row existed on `more-games.asp?cYear=2026&other_list_id=4` with stats link `game_id=309`.
  - PBP endpoint for 309 was reachable (`get_team_action.php?game_id=309`).
- Root mismatch clarified:
  - Stats/roster IDs (`team_id_rosters`) for final were `2` (Maccabi Tel-Aviv) and `6` (Bnei Herzliya).
  - Schedule uses `team_id_schedule`; mapping from `basketball_test.schedule_team_dict` (2026):
    - `team_id_rosters=2 -> team_id_schedule=1109`
    - `team_id_rosters=6 -> team_id_schedule=1118`
- Canonical fallback JSON (schedule-format IDs) for final game:
```json
{
  "ExternalID": "309",
  "game_type": 35,
  "team1": 1109,
  "team2": 1118,
  "team_name_1": "???? ?\"?",
  "team_name_2": "??? ??????",
  "team_name_eng_1": "Maccabi Tel-Aviv",
  "team_name_eng_2": "Bnei Herzliya",
  "game_date_txt": "19/02/2026",
  "game_year": 2026,
  "score_team1": 109,
  "score_team2": 90,
  "pbp_link": "https://stats.segevstats.com/realtimestat_heb/gameStats.php?game_id=309&lang=he"
}
```
- Also saved to `docs/state_cup_final_309.json.md`.
- Note (2026-02-20): after upserting `game_id=309` into `basketball_test.schedule` via `upsert_by_like`, the ETL fallback path in `etl_full.R` can include this game in `sched_subset` even when `games_all.json` does not yet contain `ExternalID=309`.

## Session Notes (2026-02-20 ETL 309 Recovery + Phase 5 Incremental Fix)
- Trigger: State Cup final `game_id=309` was detected by `etl/run_state_cup_final_etl.ps1`, but live ETL initially failed in Phase 2.
- Root cause chain:
  - `games_all.json` did not yet include `ExternalID=309`.
  - ETL fallback in `etl_full.R` relies on `%s.schedule` containing the missing game.
  - Before seeding schedule, fallback could not resolve 309 and live path failed.
- Data normalization resolved during session:
  - Stats-side IDs (`team_id_rosters`) were `2` (Maccabi Tel-Aviv) and `6` (Bnei Herzliya).
  - Schedule requires `team_id_schedule`; mapping from `schedule_team_dict` (2026):
    - `2 -> 1109`
    - `6 -> 1118`
- Applied fix for ingestion:
  - Upserted `game_id=309` into `basketball_test.schedule` using `upsert_by_like` with schedule-format IDs (`team1=1109`, `team2=1118`) and final score/date.
  - After this, ETL dry-run showed expected fallback message:
    - `Added 1 game(s) from basketball_test.schedule fallback (missing from feed)`.
- Live ETL outcome for 309 (post-upsert):
  - Phase 2 succeeded: schedule/actions/rosters/possessions/lineups/pws all loaded.
  - Phase 6 validation passed for game 309.
  - Phase 5 (`refresh_sub_lineups_stats`) timed out under default statement timeout.
- Performance fix added in this session:
  - Added incremental SQL function:
    - `basketball_test.refresh_sub_lineups_stats_for_games(int4[])`
    - file: `sql/functions/refresh_sub_lineups_incremental.sql`
  - Updated `etl/etl_full.R` Phase 5:
    - Prefer incremental refresh for `processed_ids` when function exists.
    - Fallback to full refresh only when incremental function is unavailable.
  - Deployment verification:
    - Direct call `refresh_sub_lineups_stats_for_games(array[309])` succeeded (`touched=380`).
- Operational guidance going forward:
  - For newly published cup/final games that are in stats endpoint but missing from `games_all.json`, seed one `schedule` row first (with mapped `team_id_schedule`) and then run single-game ETL.
  - For single-game ETL runs, use incremental sub-lineups refresh path to avoid full-table Phase 5 timeouts.

## Session Notes (2026-02-20 Tab 4 Minutes Removal + ETL Integrity Threshold)
- Tab 4 (Game Logs) minutes column was removed from both Summary and Four Factors tables in `app/R/server_tab4.R`.
- Tab 4 data fetches no longer pull `minutes` from `mv_lineup_totals_by_day` / `lineup_four_factors_by_game`.
- Added ETL validation warning in Phase 6 (`etl/etl_full.R`) based on deduped team timeline minutes from `df_pts_poss_lineups_longer_mv` (offense rows):
  - warn when `minutes < 39.0`
  - warn when `minutes > 40.0` and `max_quarter <= 4` (no OT flag)
- Noise comparison (2026):
  - `<40.0`: 217 team-rows across 123 games
  - `<39.0`: 4 team-rows across 3 games
  - `<38.5`: 2 team-rows across 1 game
- Final threshold set to `39.0` to reduce noisy warnings while still catching meaningful under-coverage.
