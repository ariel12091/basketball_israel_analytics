# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Token Efficiency

This file is updated after every session. **Trust this context** — avoid re-reading files or spawning exploration agents when the answer is documented here. Use `replace_all=true` for repetitive edits, read files in large chunks (limit=300+), and batch similar operations.

## Project Overview

Basketball Israel Analytics — R/Shiny dashboard for player on/off impact, lineup combos, and team ratings. Data from play-by-play JSON (basket.co.il / stats.segevstats.com) → ETL → PostgreSQL (Supabase) → Shiny app.

**Live app:** https://ibpl-stats.shinyapps.io/onoff-shiny/

**Tech:** R 4.4.2, Shiny (bslib/BS5), DBI/RPostgres (no dbplyr), PostgreSQL on Supabase (port 6543), schema `basketball_test`, deployed to shinyapps.io. React 18 + TypeScript + Vite frontend in `frontend-v2/` with Plumber/R API backend (Phase 2 migration in progress).

## Commands

```bash
RSCRIPT="/c/Program Files/R/R-4.4.2/bin/Rscript.exe"
"$RSCRIPT" -e "shiny::runApp('app')"                    # Run Shiny app locally
"$RSCRIPT" -e "rsconnect::deployApp('app')"             # Deploy Shiny
"$RSCRIPT" -e "Sys.setenv(APP_ENV='test'); source('etl/etl_full.R'); etl_full()"  # Full ETL

# React frontend (from frontend-v2/)
cd frontend-v2 && npm run dev                            # Vite dev server (port 5173)
cd frontend-v2/server && "$RSCRIPT" -e "plumber::plumb('plumber.R')\$run(port=8787)"  # Plumber API
cd frontend-v2 && npx tsc --noEmit                       # Type-check only
cd frontend-v2 && npm run build                          # Production build
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

**GN filters note:** If a reactive uses `bindEvent()`, include the GN reactive (or raw GN inputs) in the `bindEvent` triggers. Otherwise GN changes will not re-run the computation even if SQL supports GN filtering. This was the root cause in Tab 1 before adding `gn_params()` to the `bindEvent` list.

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

## Auto-Threshold Min Possessions

Auto-adjusts min possessions so filtered results always include at least the top 35% by usage, while preserving manual overrides.
- **Tab 1 (On/Off):** Dynamic `min_on_poss` and `min_all_poss` using top 35% by `ON Poss` (`off_on_poss` in fallback/FF). Auto uses min filters = 0 during threshold computation to avoid empty results on sparse date ranges.
- **Tab 2 (Lineups):** Dynamic `ld_minposs` using top 35% by `total_poss` on the locally filtered dataset.
- **Auto/Manual behavior:** Manual slider change sets `auto_enabled(FALSE)`. Filter changes set `auto_enabled(TRUE)`. Reset sets defaults and `auto_enabled(FALSE)`; auto resumes on next filter change.

## Ops Notes (ETL Scheduler)

- Daily ETL is run via Windows Task Scheduler task `onoff_etl_full_daily` calling `scripts/run_etl_full.ps1` (wrapper avoids quoting issues).
- Wrapper runs `etl_full(dry_run=TRUE)` then `etl_full(dry_run=FALSE)`, appends output to `logs/etl_full.log`, and deletes logs older than 2 days.
- Successful runs write a marker timestamp to `etl/logs/last_success.txt`; the app reads this to show "Last updated" in the top-right.
- Task Scheduler inline command strings are brittle (quoting errors caused failures); use `scripts/run_etl_full.ps1` wrapper instead.
- Hardened settings: `StartWhenAvailable=True`, `WakeToRun=True`, battery-stop flags disabled.
- Current task mode is `Interactive only` — may not run when no user session is active. Switch logon mode for true unattended nightly runs.
- **"Last updated" source-priority:** App reads `basketball_test.app_meta.etl_full_last_success` first; falls back to `etl/logs/last_success.txt` if DB metadata is stale/unavailable.
- **Log strategy:** Per-run files (`etl_full_wrapper_YYYYMMDD_HHMMSS.log`) instead of single rolling log — easier to isolate and compare failed runs.

## Security Best Practices

1. **Never hardcode credentials.** Use `app/.Renviron` and `etl/.Renviron` (gitignored) + `Sys.getenv(...)` only.
2. **Secret exposure policy:** Treat as compromised if committed/pushed, shared externally, or exposed in deployed logs. Local-only exposure: rotation recommended but not mandatory.
3. **Treat all Shiny client inputs as untrusted.** `input$...` values and `Shiny.setInputValue(...)` payloads can be tampered with in DevTools. Hidden columns / UI controls are not a security boundary.
4. **Parameterize SQL values.** Use `DBI::dbGetQuery(..., params = list(...))` with `$1, $2, ...` placeholders. Never `sprintf()`/`paste0()` for dynamic user values.
5. **Fail-closed on unexpected input.** If lookup/validation fails, return no data or explicit error — don't fall back to raw client input.

## Backlog - Security/Resilience

1. **DB statement timeout guardrail** — `app/R/global.R`: Add pooled connection `statement_timeout` (e.g. 8s) via `DBI::dbExecute(...)` in `tryCatch`.
2. **Short TTL cache for Tab 4** — `app/R/server_tab4.R`: Cache `gl_lineup_totals` and `gl_lineup_ff` by `game_year` for 30-60 seconds.
3. **Click burst guard for lineup modal** — `app/R/server_tab2.R`: Ignore duplicate `ld_lineup_click` events within ~300ms.

## Frontend Redesign

**Design principles:**
1. **Plumber must replicate every Shiny logic** — same SQL functions, same rename mappings, same fast-path/filtered-path branching, same percentile rank columns. The Plumber API is a direct translation of the Shiny server, not a reinterpretation.
2. **React UI must cover every Shiny feature** — even when using a better alternative (e.g., slide-out drawer instead of sidebar, filter chips instead of persistent controls). No feature should be "missing" in the React app.

### `frontend/` — Abandoned React Suggestion
React 18 + Vite + TypeScript. Partial Tab 1 only. **Not maintained — do NOT reference.**

### `frontend-v2/` — React + Vite + Plumber (Active)

**Phase 2 in progress.** Tab 1 (On/Off Impact) and Tab 2 (Lineup Data) complete with live data. Tabs 3-4 are placeholder stubs.

**Tech stack:** React 18 + TypeScript + Vite (frontend), Plumber/R (API), PostgreSQL on Supabase.

**Design system:** Dark editorial aesthetic (navy/charcoal `#080c14` → `#1a2436`). Fonts: DM Sans (body) + JetBrains Mono (data). Accent: amber `#e8a435`. Semantic colors: positive `#34d399`, negative `#f87171`, neutral `#60a5fa`. Shot splits: 2PT `#5b8abd`, 3PT `#d4843e`.

**Design reference:** `prototype.html` (renamed from original `index.html`) — single-file HTML/CSS/JS with mock data for all 4 tabs. Open in browser directly.

```
frontend-v2/
├── prototype.html              Design reference (all 4 tabs, mock data)
├── index.html                  Vite entry point
├── vite.config.ts              Proxy /api → Plumber (localhost:8787)
├── server/
│   └── plumber.R               R/Plumber API (mirrors Shiny server logic)
├── src/
│   ├── main.tsx                React entry
│   ├── App.tsx                 Routes to AppShell
│   ├── types/index.ts          TypeScript interfaces (OnOffPlayer, OnOffFourFactors, LineupSummary, LineupFourFactors, LineupGameLog, Player, FilterState, Team)
│   ├── app/layout/
│   │   └── AppShell.tsx        Topbar + MainTabs + FilterChips + content + FilterDrawer
│   ├── features/
│   │   ├── filters/
│   │   │   ├── store.ts        FilterContext + useReducer + buildApiParams + needsFilteredPath
│   │   │   ├── FilterDrawer.tsx  Slide-out right panel (Time, Game, Opponent Strength, Eligibility)
│   │   │   └── FilterChips.tsx   Active filter chips with × remove + "Clear all"
│   │   ├── tables/
│   │   │   ├── DataTable.tsx   Sortable table with grouped headers, sticky header, CSV export
│   │   │   ├── HeatCell.tsx    Heat-map cell (9 stops) with format variants (diff/ppp/net)
│   │   │   ├── ShotCell.tsx    Stacked 2PT/3PT bar + continuous RGB accuracy gradient
│   │   │   ├── FFCell.tsx      Range track (on/off dots + bar) with heat background + unranked
│   │   │   ├── FFValueCell.tsx Simple colored metric value
│   │   │   └── LineupModal.tsx Lineup game log modal (Summary + FF views)
│   │   └── navigation/
│   │       ├── MainTabs.tsx    4 tab buttons
│   │       └── GlossaryModal.tsx  Glossary modal (9 basketball terms)
│   ├── pages/
│   │   ├── OnOffPage.tsx       Tab 1 — Summary/FF toggle, tables, legends, auto min-poss
│   │   └── LineupsPage.tsx     Tab 2 — Lineup combos, group size, team/player filters, clutch, modal
│   ├── utils/
│   │   └── ranking.ts          Shared: autoMinPoss, adaptiveBaseline, percentileRank, computeShotAvgs
│   ├── hooks/
│   │   ├── useApi.ts           Fetch with loading/error, AbortController, debounce
│   │   └── useSorting.ts       Column sort state (key + direction)
│   └── styles/
│       ├── tokens.css          CSS custom properties
│       ├── layout.css          Topbar, tabs, drawer, responsive
│       ├── table.css           Data tables, heat classes, shot cells, FF cells
│       └── index.css           Imports all above
```

### Plumber API Layer (`server/plumber.R`)

Mirrors Shiny's server logic exactly. Uses same `.Renviron` credentials as the Shiny app (readonly user).

```bash
# Run Plumber API (from frontend-v2/server/)
"$RSCRIPT" -e "plumber::plumb('plumber.R')$run(port=8787)"

# Run Vite dev server (from frontend-v2/)
npm run dev
```

| Endpoint | Shiny Equivalent | Fast Path | Filtered Path |
|----------|------------------|-----------|---------------|
| `GET /api/onoff/summary` | `server_tab1.R` reactives | `onoff_default_mv` | `onoff_compute()` |
| `GET /api/onoff/four-factors` | `server_tab1.R` FF reactives | `player_advanced_stats_mv` | `four_factors_compute()` |
| `GET /api/lineups/summary` | `server_tab2.R` summary | — (always SQL) | `fetch_lineups_csv_v2()` |
| `GET /api/lineups/four-factors` | `server_tab2.R` FF | — (always SQL) | `fetch_lineups_four_factors_csv()` |
| `GET /api/lineups/game-log` | `server_tab2.R` modal | `mv_lineup_totals_by_day` / `lineup_four_factors_by_game` | — |
| `GET /api/meta/teams` | `teams_for_year_df` | Direct query | — |
| `GET /api/meta/players` | `full_rosters` | Direct query | — |
| `GET /api/meta/game-numbers` | `gn_choices` reactive | Direct query | — |
| `GET /api/meta/last-updated` | `last_updated` output | `app_meta` table + file fallback | — |

**Fast-path logic:** `needsFilteredPath()` checks if any filter beyond `game_year` is active (team_ids, opponents, gameType, homeAway, outcome, gnMin/gnMax, lastN, oppRank*, non-default dates). If FALSE → reads from MV. If TRUE → calls SQL function. This mirrors Shiny's `fallback_needed()`.

**Column rename:** `rename_onoff()` maps snake_case SQL columns to camelCase TypeScript fields. Critical PR columns:
- `pr_off_on` / `pr_def_on_inv` / `pr_net` — PPP-level percentile ranks (for PPP column heat backgrounds)
- `pr_off_on_d` / `pr_def_on_d` — Diff-specific percentile ranks (for Diff column heat backgrounds)
- `pr_on_net` / `pr_off_net` — Net RTG percentile ranks (for On/Off Net RTG heat backgrounds)

**⚠ PR column confusion pitfall:** `prOffOn` (PPP rank) ≠ `prOffOnD` (Diff rank). Summary Diff columns must use `prOffOnD`/`prDefOnD`, NOT the PPP ranks. This was a real bug — mixing them up makes Impact columns show wrong heat colors.

### React Component Architecture

**LineupsPage.tsx** — Tab 2 main component:
- Mode toggle (Summary / Four Factors), Group size pills (2-5)
- Tab-specific local state: groupSize, teamId, playersOn/Off, minPoss, clutch params (NOT in shared FilterContext)
- Two-tier ranking: API returns ALL lineups (no team/player/minPoss filter), ranks computed client-side on full population, then local filters applied
- TOTAL row: sum raw counts → derive rates (PPP = pts/poss*100; FF = raw counts → rates). Pinned at top during sort.
- Explainer accordion, shot/FF legends, auto min-poss (top 35% by totalPoss)
- Players On/Off: MultiSelect with mutual exclusion; client-side containment filter on `playerIds` array
- Clutch time: collapsible accordion (margin, status, minutes, OT checkbox) → re-fetches with clutch params
- Lineup click → `LineupModal` with game log (TOTAL row not clickable)

**LineupModal.tsx** — Lineup game log modal:
- Fetches `/api/lineups/game-log` with sub_hash, team_id, game_year, view_mode
- Summary mode: GN, Date, Opponent, W/L, Score, Off PPP, Def PPP, Net, Off Shot, Def Shot, Off Poss, Def Poss, Min
- FF mode: grouped headers (Offense/Defense), PPP, TS%, OREB%, TOV%, FTR, Poss, Min
- Escape-to-close, overlay-click-to-close

**OnOffPage.tsx** — Tab 1 main component:
- Mode toggle (Summary / Four Factors)
- Explainer accordion ("How to read this table")
- Shot legend (Summary) / FF legend (FF) — conditionally visible
- Auto min-poss threshold (top 35% by ON Poss, disabled on manual slider change)
- Error display + empty data message
- CSV export with explicit column key/header arrays (excludes internal PR fields)

**HeatCell.tsx** — Heat-map colored cell with 3 format modes:
- `format="diff"` (default) — 2dp with +/- sign (e.g., "+0.12")
- `format="ppp"` — 1dp plain (e.g., "0.9")
- `format="net"` — 1dp with +/- sign (e.g., "+1.2")
- Uses `heatClass(pr)` → `.heat-1` through `.heat-9` CSS classes
- Exported `heatClass()` function also used by FFCell for backgrounds

**ShotCell.tsx** — Stacked 2PT/3PT frequency bar + accuracy text:
- Continuous RGB gradient via `accColor(pct, avg, sign)` matching Shiny's `make_shot_render`:
  ```
  d = sign * (pct - avg) / avg, clamped to [-1,1] after 3x amplification
  d < 0 → red channel=200, green fades; d > 0 → green channel=170, red fades
  ```
- `isDefense` prop: flips sign (lower opponent FG% = green)
- `minFga` muting: below 50 total FGA → gray `#bbb` text + 30% opacity bar
- Weighted league averages computed dynamically from dataset (same as Shiny's `weighted.mean`)

**FFCell.tsx** — Range track with on/off dots:
- `heatPr` prop for background heat color (imports `heatClass`)
- `onRank` / `offRank` accept `number | null` — null = unranked
- Unranked treatment: gray diff text, hidden range bar, dimmed sub-text (opacity 0.5)
- Polarity-aware: `invertDiff` flag for reversed metrics

**FF percentile ranking (client-side):**
- Summary PR comes from Plumber (server-side `percent_rank()` in SQL)
- FF PR computed client-side in OnOffPage from full dataset before min-poss filtering
- Adaptive baseline: players with < top-65%-tile ON Poss get null ranks (unranked/gray)
- Correct polarity per metric:
  - COLS_GRAD (higher=green): Off TS%, Off OREB%, Off FTR, Def TOV%
  - COLS_REV (higher=red): Def TS%, Def OREB%, Def FTR, Off TOV%
- 8 heat-background PRs computed: `_hOffTs`, `_hOffOreb`, `_hOffTov` (inv), `_hOffFtr`, `_hDefTs` (inv), `_hDefOreb` (inv), `_hDefTov`, `_hDefFtr` (inv)

### Filter System

**FilterContext + useReducer** in `store.ts`:
- `FilterState` interface with 15+ fields (gameYear, dates, teamIds, opponents, gameType, homeAway, outcome, gnMin/gnMax, lastN, oppRank*, minOnPoss, minAllPoss)
- `SET_FIELD` action with side effects:
  - `gameYear` change → resets dates to season bounds + clears teamIds/opponents (IDs are season-specific)
  - `lastN` set → clears gnMin/gnMax (mutual exclusion)
  - `gnMin`/`gnMax` set → clears lastN (mutual exclusion)
- `SET_MULTIPLE` for batch updates
- `RESET` preserves gameYear but resets everything else to defaults
- `needsFilteredPath()` determines MV vs SQL function path
- `buildApiParams()` converts state to API query params

**FilterDrawer.tsx** — Collapsible sections:
- Time Filters (Season, Date Range) — defaultOpen
- Game Filters (Game Type, Teams (react-select multi), Opponents (react-select multi), Home/Away, Outcome, GN range, Last N)
- Opponent Strength (Top/Bottom, Rank N, Metric)
- Eligibility (Min ON Poss slider 0-3000, Min All Poss slider 0-2000) — defaultOpen
- Teams/Opponents use `react-select` with `isMulti` + `closeMenuOnSelect={false}` + dark-theme `StylesConfig`

**FilterChips.tsx** — Dynamic chips showing active filters:
- Season chip (always visible, not removable)
- Game type chip with label mapping (5→Regular, 16→PO QF, etc.)
- Team/opponent count chips
- Min poss chip (if non-default)
- Date range chip (if non-default for season)
- Home/Away, Outcome, GN range, Last N, Opponent Strength chips
- "Clear all" button when `hasActiveFilters`

**AppShell.tsx** — Topbar with season display, "Last updated" timestamp (fetched from `/api/meta/last-updated`), Glossary button (opens `GlossaryModal`), filter toggle with badge count

**GlossaryModal.tsx** — Modal with 9 basketball terms (PPP, Net Rating, TS%, OREB%, TOV%, FTR, Possessions, GN, Clutch). Escape to close. Uses existing `.modal-overlay` / `.modal-card` CSS.

### Auto Min-Poss Threshold

React implementation mirrors Shiny's `auto_enabled` reactive:
- Computes threshold as top-35%-tile ON Poss value from unfiltered data
- `autoEnabled` ref: starts `true`, set `false` on manual slider change, set `true` on filter change
- `autoUpdating` ref: guards against treating auto-dispatched slider changes as manual
- `prevMinOnPoss` ref: detects whether slider change was manual vs auto-triggered

### Tab 1 Parity Status (vs Shiny) — COMPLETE

All Shiny Tab 1 features are implemented:
- Summary table with all columns, heat-map backgrounds on all 14 value columns with correct PR columns
- Shot split cells (continuous RGB gradient, defense sign flip, min 50 FGA muting)
- Four Factors with range tracks, heat backgrounds, unranked treatment, correct polarity
- All filters including multi-select teams/opponents (react-select), game type, opponent strength
- Auto min-poss with manual override detection
- Column sorting, CSV export (excludes internal fields), error/empty display
- Filter chips, explainer accordion, glossary modal, "Last updated" indicator
- Row keys use playerId (not array index)
- DataTable accepts `rowKey` prop for stable React reconciliation

### Tab 2 Parity Status (vs Shiny) — COMPLETE (pending live testing)

All Shiny Tab 2 features are implemented:
- Summary table: Players (clickable → modal), Off PPP, Def PPP, Net RTG (heat-colored), Off/Def Shot splits, Off Poss, Min, Poss, +/-
- Four Factors table: Players, Off/Def (PPP, TS%, OREB%, TOV%, FTR, Poss), Min, Total Poss, Net — all heat-colored with correct polarity
- Group size pills (2-5) → re-fetches from API
- Team filter (single select) + Players On/Off (multi-select with mutual exclusion)
- Clutch time accordion (margin, status, minutes, OT checkbox) → re-fetches with clutch params
- Two-tier percentile ranking: computed on full dataset (adaptive baseline), local filters applied after
- TOTAL row: sum raw counts then derive rates, pinned at top, bold, not clickable
- Auto min-poss (top 35% by totalPoss), manual slider override detection
- Lineup click → LineupModal with per-game log (Summary: shot splits; FF: rate columns)
- CSV export with clean column headers (no internal fields)
- Shared filters (season, dates, opponents, game type, etc.) trigger re-fetch

**Plumber endpoints:** `/api/lineups/summary` (calls `fetch_lineups_csv_v2` with 23 params), `/api/lineups/four-factors` (calls `fetch_lineups_four_factors_csv`), `/api/lineups/game-log` (resolves hash → MV query → pivot → schedule join), `/api/meta/players` (roster lookup)

**Rename functions:** `rename_lineup_summary()` maps 19 SQL columns → camelCase, parses `player_ids` from PG array `{1,2,3}` → JSON array. `rename_lineup_ff()` maps 31 columns including raw counts for TOTAL row.

**Shared utilities:** `src/utils/ranking.ts` — extracted from OnOffPage: `autoMinPoss()`, `adaptiveBaseline()`, `percentileRank()`, `computeShotAvgs()`. Both Tab 1 and Tab 2 import from here.

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
- **Named empty selectize choices crash:** `c("" = "")` causes `attempt to use zero-length variable name`. Use unnamed choices (e.g., `c("", as.character(vals))`) for selectize inputs.
- **`server = TRUE` on low-cardinality selectize inputs** creates unnecessary startup network chatter. Root cause of Tab 3 preload was `updateSelectizeInput(..., server = TRUE)` for `tr_opponents`. Switching to client-side removed initial `dataobj/tr_opponents` calls.
- **Tab 3 activation gating:** `server_tab3.R` gates expensive compute with `observeEvent(..., ignoreInit = TRUE)` + `if (!identical(input$main_tabs, "team_ratings")) return(NULL)`. Validated via Playwright network trace.
- **`tags(...)` is invalid Shiny HTML** — use `tags$p(...)`, `tags$div(...)`, etc. Reactive values cannot be read outside reactive context; use `session$userData` for session-level one-time flags.
- **Inline explainers > modal walkthroughs** for first-time users. Per-tab collapsible explainers hidden by default, mode-specific content + examples. Snippets embedded as data URIs via `base64enc::dataURI` helper (`app_image_src()` in `global.R`). `htmltools::dataURI` is not exported — use `base64enc::dataURI`.

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
- **Supabase function overload handling:** Mixed states can occur when old/new overloads co-exist. Safe pattern: explicitly drop known overload signatures, parse `$function$` boundaries, execute each `CREATE OR REPLACE FUNCTION` separately, and verify with `pg_get_function_identity_arguments`.
- **Benchmark script signatures:** Keep benchmark SQL signatures aligned with deployed function signatures. Old arg counts produce false errors (e.g. `get_team_ratings_dynamic` moved from 13 to 17 args with GN filters).

### Clutch Path CTEs
- **Propagate `team_id` through all CTEs** — `segment_times`, `segment_stats`, and `lineup_totals`/`lineup_ff` must include `team_id` in SELECT, GROUP BY, and JOIN conditions. Different teams can share the same `lineup_hash`, causing "column team_id is ambiguous" errors if omitted
- **Always use table aliases in PL/pgSQL CTEs** — Unqualified column names like `SELECT team_id FROM clutch_actions` cause "ambiguous" errors because PostgreSQL can't distinguish between column references and PL/pgSQL variables. Always use aliases: `SELECT ca.team_id FROM clutch_actions ca`
- **Parallel file consistency** — `fetch_lineups_all.sql` and `fetch_lineups_four_factors.sql` have near-identical clutch path structures. When fixing one, verify the other matches. Reference `fetch_lineups_all.sql` as the canonical pattern

### React / Plumber Migration
- **PR column naming matters:** SQL `onoff_compute()` returns ~10 `pr_*` columns with distinct meanings. `pr_off_on` = PPP-level rank (for Off ON PPP column background). `pr_off_on_d` = Diff rank (for Off ON Diff column background). Using the wrong one makes heat colors meaningless. Always verify which PR column maps to which display column.
- **HeatCell format variants:** Don't use a single `formatDiff()` for all heat-colored cells. PPP values (0.95) should NOT show +/- signs. Net RTG values should show +/- at 1dp. Diff values show +/- at 2dp. The `format` prop (`"diff"` | `"ppp"` | `"net"`) handles this.
- **ShotCell continuous gradient:** Shiny's `accColor()` uses continuous RGB mapping, not discrete CSS classes. The formula: normalize `(pct - avg) / avg`, amplify 3x, clamp [-1,1], map to `rgb(r,g,60)`. Defense columns need `sign=-1` (lower opponent FG% = green). Below 50 total FGA → gray + faded.
- **FF unranked = null, not 0 or 50:** Players below the adaptive baseline should have `null` ranks, not `?? 50` (which makes them appear average). Null ranks → gray diff text, hidden range bar, dimmed sub-values. The `heatPr` background also gets null → no heat color.
- **Null-safe rank arithmetic:** When computing `pr * 100` or `1 - pr`, always guard for null: `pr === null ? null : (inv ? (1 - pr) : pr) * 100`. Otherwise `null * 100` → `0` which is wrong (makes unranked players look bottom-tier instead of unranked).
- **CSV export column control:** Pass explicit `columnKeys` and `columnHeaders` arrays to DataTable's export function. Without this, internal fields like `prNet`, `prOffOn`, `playerId`, `teamId` leak into the CSV.
- **Filter reducer side effects:** Season change must reset teamIds + opponents (IDs are season-specific). GN and lastN are mutually exclusive — setting one clears the other. These are easy to miss and cause stale filter state.
- **Auto min-poss manual detection:** Use a `prevMinOnPoss` ref to detect manual slider changes. An `autoUpdating` ref flag set before dispatch prevents the auto-triggered change from being interpreted as manual. Without this guard, every auto-update immediately disables auto mode.
- **Plumber rename functions:** Create a dedicated `rename_*()` function for each endpoint that maps every SQL column to its TypeScript camelCase name. This is the single source of truth for the SQL↔React column contract. When SQL adds columns, update both the rename function AND the TypeScript interface.

### Debugging Process
- **Check data before code.** When a UI element "doesn't work," first verify what the data pipeline actually returns (`SELECT MIN/MAX/COUNT` on the MV, check column types with `class()`). Don't analyze rendering code or reactive chains until you've confirmed the data is correct. A simple diagnostic query is worth more than 10 minutes of static analysis.
- **MVs bake in parameters — always check what's fixed.** When an MV is the "fast path" for a parameterized function, the MV has equivalent built-in filters (WHERE clauses). If the function takes `min_on` as a param, the MV has a `WHERE on_poss >= X`. Before adding UI controls that interact with MV data, read the MV SQL to understand its constraints. This applies to any pre-computed view.
- **Trace the full type chain.** When adding new columns that flow through SQL → R → dplyr → DT/JS, trace the types at each stage. PostgreSQL `SUM(integer)` → `bigint` → R `integer64` → incompatible with dplyr. Catching this requires thinking about the pipeline, not just the code at each layer.
- **Test incrementally, not all at once.** Multi-file changes (SQL MVs + SQL functions + R rendering + CSS + UI) should be deployed and tested one layer at a time. Deploy SQL, verify with a query. Add R code, test the app. Don't stack 7 file changes and deploy everything, then debug a cascade of interacting failures.
- **Use your own documentation during debugging.** CLAUDE.md documents how the MV/function architecture works. Consulting it during debugging — not just during implementation — would immediately point to the right layer (e.g., "the function takes min_on → the MV must pre-filter → that's why the slider has no effect below 300").
- **Validate startup bottlenecks in order:** code guard condition → local run network trace → live network trace after deploy. This catches cases where local fixes are correct but production still serves old behavior.

## Session Lessons (2026-02-17 React Tab 1/2)

1. Tab 2 local lineup filters remain client-side by design.
   - Backend optional params for local team/player/min-poss were tested and reverted.

2. Tab 1 min-poss semantics in React:
   - min_on_poss: ON side threshold.
   - min_all_poss: BOTH ON and OFF must each meet threshold (not ON+OFF sum).

3. Tab 1 low-possession stability hardening:
   - /api/onoff/summary: replace NA in all numeric columns.
   - HeatCell: null/NaN-safe render (- fallback), no 	oFixed crash.
   - Use stable composite row keys in Tab 1 to avoid apparent non-sorting from key collisions.

4. Tab 2 filter ownership:
   - Drawer Teams filters Tab 2 table rows.
   - Local Tab 2 Team is only for Players On/Off option pool.
   - Local team and drawer teams are mutually exclusive in UI.

5. Clear-all visibility rule:
   - Show clear control when hasActiveFilters is true even if no standard chips render (covers local Tab 2 state).

6. Drawer Game Type is multi-select with closeMenuOnSelect={false}.
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