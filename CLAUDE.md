# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Basketball Israel Analytics — an R/Shiny dashboard for analyzing Israeli basketball player on/off court impact, lineup combinations, and team efficiency ratings. Data is sourced from play-by-play JSON feeds (basket.co.il / stats.segevstats.com), processed through an ETL pipeline, stored in PostgreSQL (Supabase), and displayed via an interactive Shiny web app.

**Live app:** https://ibpl-stats.shinyapps.io/onoff-shiny/

## Tech Stack

- **R 4.4.2** with renv for dependency management
- **Shiny** (Bootstrap 5 via bslib) for the web UI
- **PostgreSQL** on Supabase (pooled connection, port 6543)
- **Schemas:** `basketball` (prod), `basketball_test` (test/dev)
- **Deployment:** shinyapps.io (account: ibpl-stats)

## Commands

R is not on PATH. Use the full path to Rscript:

```bash
RSCRIPT="/c/Program Files/R/R-4.4.2/bin/Rscript.exe"

# Restore R dependencies
"$RSCRIPT" -e "renv::restore()"

# Run the Shiny app locally
"$RSCRIPT" -e "shiny::runApp('app')"

# Run ETL (test environment)
"$RSCRIPT" -e "Sys.setenv(APP_ENV='test'); source('etl/etl_onoff.R'); etl_update()"

# Deploy to shinyapps.io
"$RSCRIPT" -e "rsconnect::deployApp('app')"

# Snapshot new R dependencies
"$RSCRIPT" -e "renv::snapshot()"

# Run full ETL pipeline (base tables + sub-lineups + MV refresh + validation)
"$RSCRIPT" -e "Sys.setenv(APP_ENV='test'); source('etl/etl_full.R'); etl_full()"

# Run full ETL dry run (preview without writes)
"$RSCRIPT" -e "Sys.setenv(APP_ENV='test'); source('etl/etl_full.R'); etl_full(dry_run=TRUE)"

# Run a deploy/test script
"$RSCRIPT" deploy_team_ff.R
```

## Architecture

```
app/app.R          Shiny UI + server (3 tabs: On/Off Impact, Lineup Data, Team Ratings)
    │
    ▼ (DBI/pool — connection-pooled PostgreSQL queries)
    │
sql/functions/     PL/pgSQL stored functions for dynamic filtered queries
    │              - onoff_compute.sql: player on/off impact (14 params)
    │              - four_factors_compute.sql: player four-factors on/off splits (11 params)
    │              - fetch_lineups_all.sql: lineup queries with rank filtering
    │              - fetch_lineups_csv.sql: CSV-param wrapper for lineups
    │              - get_team_ratings_dynamic.sql: team efficiency ratings
    │              - get_team_four_factors_dynamic.sql: team four-factor rates (10 params)
    │              - refresh_sub_lineups.sql: refresh sub-lineup stats
    │
sql/materialized_views/   Pre-computed views for fast unfiltered queries
    │              - onoff_mv.sql: default on/off metrics
    │              - team_ppp_ratings_mv.sql: team PPP ratings with ranks
    │              - df_pts_poss_longer.sql: df_pts_poss_lineups_longer_mv (pivoted points/possessions)
    │              - sub_lineups_by_day.sql: sub-lineups grouped by day
    │              - final_schedule_mv.sql: schedule with margin/win/home flags
    │              - player_advanced_stats_mv.sql: player four-factors on/off splits
    │              - player_onoff_by_game.sql: pre-aggregated player on/off stats per game
    │              - lineup_four_factors_by_game.sql: lineup four-factor counts per game
    │              - team_four_factors_mv.sql: team four-factor rates (TS%, OREB%, TOV%, FTR, PPP)
    │
etl/               ETL pipeline (R scripts)
    ├── etl_onoff.R      Main orchestrator: fetch schedule → PBP → clean → possessions → lineups → upsert
    └── etl_lineups.R    Generates C(5,k) sub-lineup combinations (k=2,3,4), MD5-hashed
```

### Shiny app diagram

```
navbarPage ("Player Analytics")
│
├── Tab 1: On/Off Impact
│   Filters: season, date range, team, game type, opponents,
│            home/away, outcome, opponent strength (rank side/n/metric),
│            min possessions (all sides + ON)
│   Output:  DTOutput("onoff_dt") — player on/off PPP table with percentile coloring
│   Data:
│     No filters → onoff_default_mv (materialized view via dbplyr)
│     Filters    → onoff_compute() (PL/pgSQL, 14 params)
│
├── Tab 2: Lineup Data
│   Filters: season, team, players on/off, group size (2-5), date range,
│            min possessions, game type, opponents, home/away, outcome,
│            opponent strength, view mode (Summary / Four Factors)
│   Output:  DTOutput("ld_table") — lineup combos with TOTAL row pinned at top
│   Data:
│     Summary view      → fetch_lineups_csv_v2() (PL/pgSQL, 16 params)
│     Four Factors view → fetch_lineups_four_factors_csv() (PL/pgSQL, 16 params)
│
└── Tab 3: Team Ratings
    Filters: season, date range, game type, opponents, home/away,
             outcome, opponent strength, view mode (Summary / Four Factors)
    Output:  DTOutput("tr_table") — team off/def/net PPP with rank coloring
    Data:
      Summary view:
        No filters → team_ppp_ratings_mv (materialized view via dbplyr)
        Filters    → get_team_ratings_dynamic() (PL/pgSQL, 10 params)
      Four Factors view:
        No filters → team_four_factors_mv (materialized view via dbplyr)
        Filters    → get_team_four_factors_dynamic() (PL/pgSQL, 10 params)
```

### Dual-path query strategy

The app uses materialized views for default/unfiltered queries (fast path) and calls PL/pgSQL stored functions when the user applies filters (dynamic path). This pattern appears in all three tabs.

### Key database tables

| Table | Purpose |
|-------|---------|
| `schedule` | Game metadata (dates, teams, game type) |
| `actions_clean` | Play-by-play events |
| `full_rosters` | Player-team-game mappings |
| `possessions` | Processed possession data |
| `pws` | Possessions joined to stints (on/off segments) |
| `lineups_lookup` | Player on/off status per stint |
| `stints` | Game segments for lineup tracking |
| `sub_lineups` | 2-3-4 man sub-combinations with MD5 hashes |

### Key materialized views

| View | Key Columns | Purpose |
|------|-------------|---------|
| `df_pts_poss_lineups_longer_mv` | lineup_hash, type_lineup (offense/defense), game_id, team_id, team_score, final_end_poss | Core view — UNIONs pws offense + defense rows with lineup hashes; base for all other MVs |
| `onoff_default_mv` | player_id, team_id, game_year, Off/Def ON/OFF PPP, net_rtg diffs, percentile ranks, on_poss, off_poss | Pre-computed player on/off impact with percentile rankings; used as fast path for On/Off tab |
| `team_ppp_ratings_mv` | game_year, team_id, team_name, off_ppp, def_ppp, net_rtg, rank_net_rtg, rank_off_ppp, rank_def_ppp | Team-level efficiency ratings with dense_rank per season |
| `mv_lineup_totals_by_day` | team_id, lineup_hash, type_lineup, g_date, game_id, game_year, total_poss, total_pts | Daily aggregated lineup stats (points/possessions per lineup per game) |
| `final_schedule_mv` | game_id, game_year, game_date, game_type, team_id, team_name, opp_team_id, opp_team_name, team_score, opp_score, margin, has_won, is_home | Materialized from `sched_long` with computed margin, win flag, and home flag; indexed on date, team, opponent, and filter columns |
| `player_advanced_stats_mv` | player_id, team_id, game_year, off/def on/off TS%, OREB%, TOV%, FTR, poss counts, diff columns, percentile ranks | Player four-factors on/off splits (TS%, OREB%, TOV%, FT rate) with on-minus-off diffs and percentile rankings |
| `lineup_four_factors_by_game` | lineup_hash, team_id, game_id, game_year, type_lineup, total_points, total_poss, ts_poss_count, oreb_count, oreb_opportunities, tov_count, total_ft_attempts, total_fga | Pre-aggregated four-factor counts per lineup_hash per game; same logic as player_four_factors_by_game but without player-level grouping |
| `player_onoff_by_game` | player_id, team_id, game_id, game_year, is_on_key, type_lineup, total_pts, total_poss | Pre-aggregated player on/off stats per game; joins lineups_lookup × mv_lineup_totals_by_day; used by `onoff_compute()` for ~24x speedup; refreshed during ETL |
| `team_four_factors_mv` | team_id, game_year, team_name, off/def TS%, OREB%, TOV%, FTR, PPP, poss, raw counts, net_rtg | Team-level four-factor rates aggregated from lineup_four_factors_by_game; used as fast path for Tab 3 Four Factors view |

### SQL function signatures

**`onoff_compute`** (14 params, LANGUAGE plpgsql) — player on/off impact
Called from Tab 1 via `run_onoff_compute_14()` (app.R:498). Only when filters are active; otherwise `onoff_default_mv` is used.
```sql
onoff_compute(
  p_start_date DATE, p_end_date DATE, p_team_ids TEXT,
  p_min_all INT, p_min_on INT, p_min_net NUMERIC, p_game_year TEXT,
  p_game_type_csv TEXT, p_opp_ids_csv TEXT, p_home_away TEXT, p_outcome TEXT,
  p_opp_rank_side TEXT, p_opp_rank_n INT, p_opp_rank_metric TEXT
)
RETURNS TABLE (
  "Team", "First Name", "Last Name",
  "Net RTG Diff", "Off ON Diff", "Def ON Diff",
  "Off ON PPP", "Def ON PPP", "On Net RTG",
  "Off OFF PPP", "Def OFF PPP", "Off Net RTG",
  "ON Poss", "OFF Poss",
  pr_net, pr_off_on, pr_off_off, pr_def_on_inv, pr_def_off_inv,
  pr_off_on_d, pr_def_on_d, pr_def_on_d_inv, pr_on_net, pr_off_net,
  player_id, team_id
)
```

**`fetch_lineups_csv_v2`** (16 params, LANGUAGE plpgsql) — lineup combos
Called from Tab 2 via `run_fetch_lineups_16()` (app.R:858). Always used — no materialized view fast path for this tab.
```sql
fetch_lineups_csv_v2(
  p_num_lineup INT, p_team_ids_csv TEXT, p_player_ids_csv TEXT,
  p_player_off_csv TEXT, p_exact BOOL, p_start_date DATE, p_end_date DATE,
  p_min_poss INT, p_game_year INT,
  p_game_type_csv TEXT, p_opp_team_ids_csv TEXT, p_home_away TEXT, p_outcome TEXT,
  p_opp_rank_side TEXT, p_opp_rank_n INT, p_opp_rank_metric TEXT
)
RETURNS TABLE (
  team_id, sub_lineup_hash, num_lineup, player_ids INT[],
  player_names TEXT[], player_names_str,
  off_poss, off_pts, off_ppp, def_poss, def_pts, def_ppp, net_rtg, game_year
)
```

**`fetch_lineups_all`** (16 params, LANGUAGE plpgsql) — same as csv_v2 but accepts arrays instead of CSV strings
Not called from the app. Array-based variant for direct SQL queries or other tooling.
```sql
fetch_lineups_all(
  p_num_lineup SMALLINT, p_team_ids INT[], p_player_ids INT[],
  p_player_off_ids INT[], p_exact BOOL, p_start_date DATE, p_end_date DATE,
  p_min_poss INT, p_game_year INT,
  p_game_type_csv TEXT, p_opp_team_ids_csv TEXT, p_home_away TEXT, p_outcome TEXT,
  p_opp_rank_side TEXT, p_opp_rank_n INT, p_opp_rank_metric TEXT
)
RETURNS TABLE (same as fetch_lineups_csv_v2)
```

**`four_factors_compute`** (11 params, LANGUAGE plpgsql) — player four-factors on/off splits
Called from Tab 1 (Four Factors view) via `run_four_factors_compute()` (app_test.R). Only when game filters are active; otherwise `player_advanced_stats_mv` is used.
```sql
four_factors_compute(
  p_game_year INT, p_start_date DATE, p_end_date DATE,
  p_team_ids_csv TEXT, p_game_type_csv TEXT, p_opp_ids_csv TEXT,
  p_home_away TEXT, p_outcome TEXT,
  p_opp_rank_side TEXT, p_opp_rank_n INT, p_opp_rank_metric TEXT
)
RETURNS TABLE (
  player_id, team_id, firstname, lastname, team_name, game_year,
  off_on_ts, off_off_ts, def_on_ts, def_off_ts,
  off_on_oreb, off_off_oreb, def_on_oreb, def_off_oreb,
  off_on_tov, off_off_tov, def_on_tov, def_off_tov,
  off_on_ftr, off_off_ftr, def_on_ftr, def_off_ftr,
  off_on_poss, off_off_poss, def_on_poss, def_off_poss,
  "Off TS% Diff", "Off OREB% Diff", "Off TOV% Diff", "Off FTR Diff",
  "Def TS% Diff", "Def OREB% Diff", "Def TOV% Diff", "Def FTR Diff"
)
```

**`fetch_lineups_four_factors_csv`** (16 params, LANGUAGE plpgsql) — lineup four-factors
Called from Tab 2 (Four Factors view) via `run_fetch_lineups_ff_16()` (app_test.R). CSV wrapper delegates to `fetch_lineups_four_factors()`.
```sql
fetch_lineups_four_factors_csv(
  p_num_lineup INT, p_team_ids_csv TEXT, p_player_ids_csv TEXT,
  p_player_off_csv TEXT, p_exact BOOL, p_start_date DATE, p_end_date DATE,
  p_min_poss INT, p_game_year INT,
  p_game_type_csv TEXT, p_opp_team_ids_csv TEXT, p_home_away TEXT, p_outcome TEXT,
  p_opp_rank_side TEXT, p_opp_rank_n INT, p_opp_rank_metric TEXT
)
RETURNS TABLE (
  team_id, sub_lineup_hash, num_lineup, player_ids INT[], player_names TEXT[], player_names_str,
  off_ts, off_oreb, off_tov, off_ftr, off_poss, off_pts, off_ppp,
  def_ts, def_oreb, def_tov, def_ftr, def_poss, def_pts, def_ppp,
  net_rtg, game_year
)
```

**`get_team_ratings_dynamic`** (10 params, LANGUAGE plpgsql) — team efficiency
Called from Tab 3 via `run_team_ratings_dynamic()` (app.R:1205). Only when filters are active; otherwise `team_ppp_ratings_mv` is used.
```sql
get_team_ratings_dynamic(
  p_game_year INT, p_start_date DATE, p_end_date DATE,
  p_game_type_csv TEXT, p_opp_team_ids_csv TEXT, p_home_away TEXT, p_outcome TEXT,
  p_opp_rank_side TEXT, p_opp_rank_n INT, p_opp_rank_metric TEXT
)
RETURNS TABLE (
  game_year, team_id, team_name, off_ppp, def_ppp, net_rtg,
  rank_net_rtg, rank_off_ppp, rank_def_ppp
)
```

**`get_team_four_factors_dynamic`** (10 params, LANGUAGE plpgsql) — team four-factor rates
Called from Tab 3 (Four Factors view) via `run_team_ff_dynamic()` (app_test.R). Only when filters are active; otherwise `team_four_factors_mv` is used.
```sql
get_team_four_factors_dynamic(
  p_game_year INT, p_start_date DATE, p_end_date DATE,
  p_game_type_csv TEXT, p_opp_team_ids_csv TEXT, p_home_away TEXT, p_outcome TEXT,
  p_opp_rank_side TEXT, p_opp_rank_n INT, p_opp_rank_metric TEXT
)
RETURNS TABLE (
  team_id, game_year, team_name,
  off_ts, off_oreb, off_tov, off_ftr, off_ppp, off_poss,
  off_pts, off_ts_poss, off_oreb_cnt, off_oreb_opps, off_tov_cnt, off_fta, off_fga_cnt,
  def_ts, def_oreb, def_tov, def_ftr, def_ppp, def_poss,
  def_pts, def_ts_poss, def_oreb_cnt, def_oreb_opps, def_tov_cnt, def_fta, def_fga_cnt,
  net_rtg
)
```

**`refresh_sub_lineups_stats`** (no params, LANGUAGE sql) — recomputes `sub_lineups_stats` from `mv_lineup_totals_by_day`; returns void
Not called from the app. Invoked during ETL after data loads.

### ETL data flow

The ETL consists of two scripts that run independently:

#### `etl_onoff.R` — Main pipeline (`etl_update()`)

Entry point: `etl_update(game_ids = NULL, season = NULL, competition = NULL)`

Runs inside a single transaction with ROLLBACK on error. Schema is selected by `APP_ENV` (`"prod"` → `basketball`, `"test"` → `basketball_test`). Test schema is auto-cloned from prod on startup if tables are missing.

| Step | Function | Upserts to | Description |
|------|----------|-----------|-------------|
| 1 | `fetch_israel_schedule()` | `schedule` | Fetch JSON from `basket.co.il/pbp/json/games_all.json`, filter to games with score > 0 |
| 2 | — | — | Determine new games: game_ids not yet in `actions_clean` |
| 3 | `fetch_game_pbp()` | — | Fetch PBP JSON from `stats.segevstats.com` per game |
| 4 | `clean_actions()` | `actions_clean` | Unnest actions, compute end-of-quarter seconds, normalize IDs, deduplicate |
| 5 | — | `subs` | Filter actions to type="substitution" |
| 6 | `extract_roster()` | `full_rosters` | Extract home/away rosters from PBP, set game_year |
| 7 | `compute_possessions()` | `possessions` | 3-phase possession tracking: base end-of-poss detection → end-of-quarter override → final flag |
| 8 | `compute_lineups_lookup()` | `lineups_lookup` | Track player on/off status via substitutions, build lineup_hash (MD5 of sorted player IDs) |
| 9 | `compute_stints()` | `stints` | Offensive/defensive segments: cross-join offense × defense lineups within aligned quarters |
| 10 | — | `pws` | Left-join possessions to stints (possessions-within-stints) |
| 11 | `ANALYZE` | — | Update query planner statistics |

**Key helper: `upsert_by_like(pg, schema, table, df)`** — Schema-driven upsert that auto-creates missing test tables by cloning from prod, stages data in a temp table, and uses `INSERT ... ON CONFLICT DO UPDATE`.

**MV refreshes are NOT called by `etl_onoff.R`** — they must be run manually or via a separate script after the ETL completes.

#### `etl_lineups.R` — Sub-lineup generation (manual, not called by `etl_onoff.R`)

Generates C(5,k) sub-lineup combinations (k=2,3,4) from full 5-man lineups already in `lineups_lookup`. Run manually after `etl_onoff.R` for specific game_ids.

| Step | Description | Table |
|------|-------------|-------|
| 1 | Pull ON lineups from `lineups_lookup` (where `is_on_verdict = 1`) | reads `lineups_lookup` |
| 2 | Anti-join against existing `lineups_lookup_on` to find new rows | reads `lineups_lookup_on` |
| 3 | Insert new ON-lineup rows | writes `lineups_lookup_on` |
| 4 | `build_sub_lineups_all()` — for each 5-man lineup, generate all C(5,2) + C(5,3) + C(5,4) = 25 sub-combos | — |
| 5 | Each sub-lineup gets: underscore-separated player IDs (`sub_lineup_id`), MD5 hash (`sub_lineup_hash`), size (`num_lineup`) | — |
| 6 | Insert into `sub_lineups` table (`player_ids` column is GENERATED in Postgres from `lineup_id`) | writes `sub_lineups` |

#### Full ETL + refresh sequence

```
1. Run etl_update()          → upserts 8 base tables
2. Run etl_lineups.R         → generates sub_lineups (manual, as needed)
3. Refresh MVs in order      → see MV refresh order below
```

### MV refresh order

MVs must be refreshed in dependency order. The `search_path` must be set to `basketball_test` (or `basketball` for prod) before refreshing, since most MV definitions use unqualified table names.

```
Level 1 — depend only on base tables:
  1. REFRESH MATERIALIZED VIEW final_schedule_mv;
  2. REFRESH MATERIALIZED VIEW df_pts_poss_lineups_longer_mv;

Level 2 — depend on Level 1 MVs + base tables:
  3. REFRESH MATERIALIZED VIEW mv_lineup_totals_by_day;
  4. REFRESH MATERIALIZED VIEW team_ppp_ratings_mv;
  5. REFRESH MATERIALIZED VIEW onoff_default_mv;

Level 3 — depend on Level 2 MVs + base tables:
  6. REFRESH MATERIALIZED VIEW player_onoff_by_game;
  7. REFRESH MATERIALIZED VIEW player_four_factors_by_game;
  8. REFRESH MATERIALIZED VIEW lineup_four_factors_by_game;
  9. REFRESH MATERIALIZED VIEW player_advanced_stats_mv;

Level 4 — depend on Level 3 MVs:
 10. REFRESH MATERIALIZED VIEW team_four_factors_mv;
```

### MV dependency graph

```
base tables (pws, schedule, lineups_lookup, full_rosters)
│
├─► df_pts_poss_lineups_longer_mv
│   ├─► mv_lineup_totals_by_day
│   │   └─► player_onoff_by_game  (+ lineups_lookup)
│   ├─► onoff_default_mv          (+ lineups_lookup, schedule)
│   ├─► team_ppp_ratings_mv       (+ schedule, full_rosters)
│   ├─► player_four_factors_by_game (+ lineups_lookup, schedule)
│   ├─► player_advanced_stats_mv  (+ lineups_lookup, schedule, full_rosters)
│   └─► lineup_four_factors_by_game (+ schedule)
│       └─► team_four_factors_mv
│
└─► final_schedule_mv (via sched_long VIEW)
```

### Which functions use which MVs

| Function | Reads from MV | Purpose |
|----------|--------------|---------|
| `onoff_compute` | `player_onoff_by_game`, `mv_lineup_totals_by_day`, `final_schedule_mv` | Player on/off impact (Tab 1) |
| `four_factors_compute` | `lineup_four_factors_by_game`, `final_schedule_mv` | Player four-factors (Tab 1 FF) |
| `fetch_lineups_csv_v2` | `mv_lineup_totals_by_day`, `final_schedule_mv` | Lineup combos (Tab 2) |
| `fetch_lineups_four_factors_csv` | `lineup_four_factors_by_game`, `final_schedule_mv` | Lineup four-factors (Tab 2 FF) |
| `get_team_ratings_dynamic` | `df_pts_poss_lineups_longer_mv`, `team_ppp_ratings_mv`, `final_schedule_mv` | Team ratings (Tab 3) |
| `get_team_four_factors_dynamic` | `lineup_four_factors_by_game`, `final_schedule_mv` | Team four-factors (Tab 3 FF) |

## Environment Variables

Database credentials are read from `.Renviron` (git-ignored). Required variables:

```
PG_HOST=<supabase-pooler-host>
PG_DB=postgres
PG_USER=<user>
PG_PASS=<password>
PG_PORT=6543
PG_SSLMODE=require
POOL_MAX=3
```

The ETL uses `APP_ENV` (`"prod"` or `"test"`) to select the database schema.

## In-Development: Four Factors View (app/app_test.R)

`app/app_test.R` is the development version of the app. It adds a **"Four Factors" view mode** to Tab 1 (On/Off Impact). This feature is working but buggy.

### What it adds

Tab 1 gets a radio button toggle (`onoff_view_mode`) switching between "Summary" (existing behavior) and "Four Factors". The Four Factors view shows on/off splits for four efficiency metrics: **TS% (true shooting), OREB% (offensive rebound rate), TOV% (turnover rate), FTR (free throw rate)** — for both offense and defense.

### Data flow (Four Factors mode)

**MV path (no game filters active):**
1. Reads from `player_advanced_stats_mv` via lazy table `advanced_stats_mv`
2. Joins with `onoff_default_mv` to get Net RTG Diff, Off ON Diff, Def ON Diff (these columns don't exist in the advanced stats MV)
3. Filters locally by team (`selected_team_ids()`) and min ON possessions
4. Computes percentile ranks in R (not SQL) using `percent_rank()` with a `RANKING_BASELINE` of 100 possessions — players below this threshold get `NA` ranks and appear unranked/gray

**Dynamic SQL path (game filters active — `fallback_needed()` is TRUE):**
1. Calls `four_factors_compute()` via `live_ff_result_df()` — computes four-factor splits for the filtered game set
2. Calls `onoff_compute()` via `live_result_df()` — gets Net RTG Diff, Off ON Diff, Def ON Diff for the same filtered game set
3. Joins the two results by (player_id, team_id)
4. Filters locally by team and min ON possessions; computes percentile ranks in R (same as MV path)

### Fallback logic difference from production

In `app_test.R`, the `fallback_needed()` logic is changed: team selection and min-possession changes do **not** trigger the dynamic SQL path. Instead, the MV data is filtered locally in R. Only date range changes and game filter changes (game type, opponents, home/away, outcome, opponent strength) trigger `onoff_compute()`.

### Visual rendering (Four Factors columns)

Each four-factor column renders a custom visual via JS `columnDefs.render`:
- **Diff value** at top (bold number, gray if unranked)
- **Range bar** (90px wide): shows on-court and off-court percentile positions as dots connected by a line
  - Filled black dot (`dot-on`) = on-court rank position
  - Hollow dot (`dot-off`) = off-court rank position
  - Gray connector line between them
- **Sub-text** below: on-value (bold) | off-value

### Color logic

- Offense factors (TS%, OREB%, FTR): higher diff = better → green-high gradient
- Offense TOV%: higher diff = worse → reversed gradient (red-high)
- Defense factors (TS%, OREB%, FTR): higher diff = worse → reversed gradient (red-high)
- Defense TOV%: higher diff = better → green-high gradient
- Net Diff, Off Rtg Diff: green-high; Def Rtg Diff: reversed (red-high)

### Table layout (Four Factors mode)

```
Header row 1:  [empty x2] | Total | Offense Impact (On-Off) x5 | Defense Impact (On-Off) x5
Header row 2:  Team | Player | Diff | RTG | TS% | OREB% | TOV% | FTR | RTG | TS% | OREB% | TOV% | FTR
```

### Key differences from app.R (production)

- Adds `htmltools` library import
- Adds `RANKING_BASELINE <- 100` constant
- Adds lazy table: `advanced_stats_mv <- tbl(pg_pool, in_schema("basketball_test", "player_advanced_stats_mv"))`
- Adds `onoff_view_mode` radio button in Tab 1 sidebar
- Adds `conditionalPanel` legend (visible only in Four Factors mode)
- `result_df()` reactive branches on `input$onoff_view_mode`
- `renderDT` branches on mode: Summary uses the existing sketch/coloring; Four Factors uses custom JS render with `metric_map` loop
- Custom CSS for `diff-val`, `rank-bar-container`, `dot-on`, `dot-off`, `range-connect`, `sub-text`, `view-mode-container`, `legend-box`
- Uses Google Fonts (Inter) via external stylesheet link

## Code Conventions

- R code uses 2-space indentation (per .Rproj settings)
- Database queries use parameterized `params = list(...)` — never paste user input into SQL
- The app references schema `basketball_test` directly via `in_schema()`; ETL uses a configurable `SCHEMA` variable
- Column/table naming is snake_case throughout

## Lessons Learned

### Deploy script patterns

- **Deploying `$$`-quoted functions**: Do NOT regex-split the SQL file — the `$$` body confuses the splitter. Instead, use `DROP FUNCTION IF EXISTS schema.func_name;` (no signature = drops all overloads) as a separate `dbExecute`, then extract the CREATE statement with `grep("^CREATE OR REPLACE", sql_lines)` and send it as one string. See `deploy_team_ff.R` for the reference pattern.
- **Deploying MVs**: MV SQL files have no dollar-quoting, so splitting on `\n(?=CREATE )` works fine to separate the CREATE MATERIALIZED VIEW from the CREATE INDEX statements.
- **Long inline R scripts**: `Rscript -e '...'` segfaults when the script is too long. Write to a `.R` file and run that instead.

### PostgreSQL: `LANGUAGE plpgsql` vs `LANGUAGE sql`

- `LANGUAGE sql` auto-casts return types (e.g., `double precision` → `numeric`). `LANGUAGE plpgsql` is **strict** — return types must match exactly or the function errors at call time, not at creation time.
- `PERCENT_RANK()` and other window ranking functions return `double precision`, not `numeric`. Declare return columns accordingly.

### Materialized view gotchas

- `REFRESH MATERIALIZED VIEW` re-runs the **stored query definition** — it does NOT update the definition to match the SQL file. To change the query (e.g., add a WHERE clause), you must `DROP` and `CREATE` the MV.
- MVs using **unqualified table names** (e.g., `schedule` instead of `basketball_test.schedule`) depend on `search_path` at creation time. On Supabase (PgBouncer in transaction mode), `SET search_path` does not persist across statements. Use `dbBegin` + `SET LOCAL search_path TO basketball_test, public` + `dbCommit` to keep it within a single transaction.

### R / RPostgres type handling

- PostgreSQL `bigint` (`int8`) maps to R `numeric` (double), not `integer`. `sprintf("%d", ...)` fails — use `sprintf("%.0f", ...)`, `format(n, big.mark = ",")`, or cast to `::int` in the SQL query (safe when values are small).

### Optimization pattern: pre-aggregated MVs

- All fast dynamic functions (`four_factors_compute`, `fetch_lineups_*`, `get_team_*_dynamic`) avoid scanning raw tables by reading from pre-aggregated MVs (`lineup_four_factors_by_game`, `mv_lineup_totals_by_day`). The `onoff_compute` function was the only one that still joined raw `lineups_lookup` × `df_pts_poss_lineups_longer_mv` at query time. Adding `player_onoff_by_game` brought it in line with the same pattern — **24x speedup** (22s → 0.8s).
