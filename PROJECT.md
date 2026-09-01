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

**Direct SQL queries (no dbplyr lazy tables):** All DB access uses `DBI::dbGetQuery(pg_pool, ...)` with parameterized SQL. No `tbl()`/`in_schema()` calls — eliminates metadata round trips. The pool is created at source time, but its first connection is opened lazily by the first database query.

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
- GN selectize inputs appeared to do nothing because fallback stayed on MV when GN changes didn’t trigger recompute. Fix is the `bindEvent` update above (and ensuring GN raw inputs are non-empty when selected).
## Session Update (Tab 3 Sorting)
- Team Ratings sort behavior now enforces best-first order on rated metric columns so top rank remains first when sorting (#1 at top for the selected metric).
- Sorting uses hidden numeric columns mapped via DataTables orderData, so displayed HTML rank/value/delta cells still sort by raw numeric metric.
- One-direction orderSequence is set per metric (for example: desc for off_ppp, asc for def_ppp, asc for off_tov) to prevent accidental reversed ranking order.
- NA sort values are pushed to the bottom using direction-aware sentinels (-Inf for descending metrics, Inf for ascending metrics).
- Applied in all Team Ratings modes in app/R/server_tab3.R: Summary, Four Factors, and Traditional.

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
- Successful runs write `etl/logs/last_success.txt` for local operations and
  `app_meta.etl_full_last_success` in PostgreSQL; the deployed app reads the DB
  key to show "Last updated" in the top-right.
### Key Tables & MVs

**Canonical database and query context:** `docs/database_context.md`. Read this
before adding or reconstructing SQL; it records relation grains, existing query
surfaces, every live column, filter semantics, refresh order, diagnostics, and
current anomalies. For observed inputs, exact low-cardinality value counts, and
larger-domain ranges for every column, also read
`docs/database_value_profiles.md`.

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
| `df_pts_poss_lineups_longer_mv` | Most `pws` columns (team_id flipped per branch) | + `own_team_score`, `opp_team_score` (cumulative), `type_lineup` ('offense'/'defense'), `lineup_hash`, and canonical timing fields: `event_elapsed_seconds`, `clock_regression_seconds`, `segment_start_elapsed_seconds`, `segment_end_elapsed_seconds`, `segment_seconds` |

**`own_team_score` / `opp_team_score`:** Cumulative game scores computed via `cum_scores` CTE on `possessions`. Uses `total_cum - team_cum` pattern (no schedule join needed). Offense branch: own = acting team's cum score. Defense branch: own = total minus acting team's (i.e., defending team's cum score). Scores mirror each other — same pattern as `sched_long`.

**MV dependency tree** (refresh in this order):
```
L1: final_schedule_mv, df_pts_poss_lineups_longer_mv (depends on: possessions, pws)
L2: mv_lineup_totals_by_day, team_ppp_ratings_mv, onoff_default_mv
L3: player_four_factors_by_game, lineup_four_factors_by_game, player_advanced_stats_mv
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

**Starters-keyed lineup MVs:** `mv_lineup_totals_by_day` and
`lineup_four_factors_by_game` are keyed by own starters (`num_starters`) plus
`opp_starters`. Their unique indexes use `NULLS NOT DISTINCT`, and separate
starter-leading indexes serve season/own/opp predicates. Minutes preserve the
canonical `segment_seconds` budget, split it across contiguous opponent-count
windows, and attach only windows containing offense rows. After rebuilding
`lineup_four_factors_by_game`, also rebuild `team_metrics_by_game_mv` and
`team_metrics_rolling_mv` so their canonical-minute snapshots stay aligned.

**Function → MV mapping:**
- `onoff_compute` → `player_four_factors_by_game`, `final_schedule_mv`
- `four_factors_compute` → `lineup_four_factors_by_game`, `final_schedule_mv`
- `fetch_lineups_*` → `mv_lineup_totals_by_day`, `final_schedule_mv`
- `get_team_*_dynamic` → `lineup_four_factors_by_game`, `final_schedule_mv`

**`team_ppp_ratings_mv` columns:** `game_year`, `team_id`, `team_name`, `off_ppp`, `def_ppp`, `net_rtg`, `games_played`, `wins`, `losses`, `off_poss`, `def_poss`, `rank_net_rtg`, `rank_off_ppp`, `rank_def_ppp`

**Wins/Losses in Team Ratings:** `get_team_ratings_dynamic()` returns `wins` and `losses`. When clutch filter is active, wins/losses only count games that have qualifying clutch possessions (not all filtered games). Uses `qualifying_games` CTE which applies clutch WHERE clause to identify games, then counts wins/losses from that subset.

**Canonical minutes calculation:** Raw provider clocks remain untouched for auditing. Runtime minutes use canonical elapsed time and consecutive lineup-segment boundaries, so delayed or out-of-order actions cannot inflate a stint. Consumers deduplicate `segment_seconds` at `(game_id, team_id, lineup_hash, segment_id)` and count each duration once; possession and point statistics remain split by `type_lineup`.

`lineup_four_factors_by_game.minutes` stores that duration once on the offense
row. In `team_metrics_by_game_mv`, `off_minutes` and `def_minutes` intentionally
mirror the same canonical team floor duration; never independently sum the
empty defense-row minute payload.

The normal incremental ETL path calls `refresh_segment_clock_fields_for_games()` from `refresh_df_pts_poss_lineups_longer_for_games()`. See `docs/canonical_clock_minutes.md` for the formula, affected cases, integration points, and deployment constraints.

### SQL Functions (params)

| Function | Params | Purpose |
|----------|--------|---------|
| `onoff_compute` | 14 | Player on/off PPP with percentile ranks |
| `four_factors_compute` | 11 | Player TS%, OREB%, TOV%, FTR on/off splits |
| `fetch_lineups_csv_v2` | 20 | Lineup combos (Summary) + clutch filters + minutes + shooting splits |
| `fetch_lineups_four_factors_csv` | 20 | Lineup combos (Four Factors) + clutch filters + minutes |
| `get_team_ratings_dynamic` | 14 | Team PPP ratings + wins/losses + clutch filters |
| `get_team_four_factors_dynamic` | 14 | Team four-factor rates + clutch filters |

### Shared-scan analytical reader rules (required)

These rules govern Israeli analytical readers. The EuroLeague sub-project
inherits them and may add provider-specific constraints in
`euroleague/PROJECT.md`.

1. **One expensive filtered fact per user request.** Resolve the selected games
   and action/fact eligibility once. When multiple outputs use identical
   eligibility, derive them from one narrowly projected materialized CTE or one
   pre-aggregated fact. Do not independently rescan action grain for metrics,
   duration, ranks, observers, or charts within the same request.
2. **Choose the lowest sufficient source by route.** Default-season requests
   use indexed materialized views; non-clutch filters use per-game additive
   facts; an exact standard-clutch preset uses an incrementally maintained fact
   where available; only arbitrary custom clutch reaches action grain. A route
   may not accept and ignore a filter.
3. **Combine consumers, not unrelated features.** Share a filtered set only
   when consumers have identical selected-game, clutch, starter, and row-
   eligibility semantics. Keep each public reader narrow enough for the view
   actually rendered. Summary must not pay for Four Factors, Traditional,
   lineup expansion, or unused shot-profile work merely to create a universal
   dashboard function.
4. **Give each heavy result one application owner.** One Shiny reactive or API
   query owns the expensive result. All same-semantics consumers project from
   that cached result. Add a query-count test for the render/filter boundary;
   the normal acceptance limit is one action-grain database call per rendered
   view and filter change.
5. **Do not mistake a helper for shared execution.** Separate calls to a view,
   SQL function, or CTE-returning helper still evaluate separately. Use one
   combined consumer call or a persisted/incremental fact. Do not create
   request-local temporary tables in the app path.
6. **Materialize narrowly and calculate rates late.** Carry only the identity,
   additive metric, and canonical-duration columns needed downstream.
   Aggregate counts and seconds at their correct grains before calculating
   PPP, ratings, percentages, pace, or ranks. Count segment duration once at
   segment grain; a lineup change never creates a possession.
7. **Prefer durable pre-aggregation for common routes.** If a common filter
   repeatedly needs action grain, extend the appropriate per-game or standard-
   clutch additive fact and its incremental refresh lifecycle. Do not copy a
   second action table or repeatedly reconstruct the same rows.
8. **Require structural and behavioral proof.** Static tests must assert the
   intended primary-fact scan count, one materialization boundary when reused,
   filter-before-expansion order, late ratios, and absence of nested companion
   scans. Behavioral gates compare complete, non-vacuous results including
   additive counts, duration, ranks, nulls, and provider extensions.
9. **Measure warm and backend-first performance separately.** Before routing,
   run at least 15 alternating complete-fetch warm samples and report median
   and p90. Also run repeated candidate-first and legacy-first samples on
   distinct fresh backends and inspect `EXPLAIN (ANALYZE, BUFFERS, SETTINGS)`.
   A warm win alone is insufficient on PostgreSQL 17; reject material cold
   regressions for the actual consumer.
10. **Measure the real UI composition.** Time the calls made by one rendered
    view, not a sum of mutually exclusive views. Record query count and full
    fetch latency. Treat buffer reduction, round-trip reduction, and elapsed-
    time reduction as separate evidence.
11. **Apply additively and preserve rollback.** Retain existing readers as
    compatibility surfaces during cutover, revoke PUBLIC execution, update the
    app-role grant and independent audit allowlists together, run security and
    reachability audits, and keep app deployment separate from DDL approval.
12. **Document justified duplication.** If readers remain separate because
    grains differ or a combined body regresses cold latency, record the measured
    reason. Historical duplication alone is not justification.

Current Israeli Team result from the 2026-09-01 measurement: Summary and its
Minutes path remain unchanged. The additive migration 051 reader is limited to
standard-clutch **Four Factors + Minutes**, omits Ratings ranks/records and
Israeli shot-profile work, and is routed locally through one shared reactive.
Exact parity, 15-sample warm, repeated fresh-backend, security, and query-count
gates passed. The app change has not been deployed. See
`euroleague/docs/plans/2026-09-01-israeli-standard-clutch-dashboard-measurement.md`.

### ETL

**Use `etl_full.R`** — runs: base tables → sub-lineups → MV refresh → validation. Logs to `etl/logs/`.

Key helpers: `upsert_by_like()` (schema-driven upsert), `fetch_israel_schedule()`, `compute_possessions()`, `compute_lineups_lookup()`

Before a game is written, `complete_roster_from_action_players()` recovers PBP
participants missing from that game roster using same-season/team roster
history. `assert_action_players_in_roster()` then fails that game before its
transaction begins if any nonzero PBP `(game_id, team_id, player_id)` still has
no roster row. This prevents a roster omission from silently generating a
four-player lineup.

Stint action ranges are half-open `[final_start_id, final_end_id)`. Persisted
`final_end_id` remains a real action ID because it is foreign-keyed. During the
PWS join only, `add_terminal_stint_join_end()` advances the final interval's
temporary upper bound by one, keeping the maximum action attributable without
overlapping adjacent stints or weakening the FK.

**Lineup anomaly status (2026-07-22):** Game 62461/team 8 was repaired after
the source game roster omitted Cody Demps (player 2543) despite 26 PBP actions.
The reprocess removed 18 invalid lineup states and restored 430 unmatched
event-team rows; the full-history action-player/roster audit now has zero gaps.
The remaining 52 invalid states are mixed: games 178 and 62452 are confirmed
material source substitution failures, game 62479 is a small likely source
failure, and 28 states across games 157, 168, 190, 205, 209, 211, 357, 381,
62447, and 62534 are transient bulk-reset declarations rather than sustained
gameplay failures. Do not globally suppress `n_on = 0`; game 62452 shows that
it can begin a genuine sustained defect. Detailed evidence and follow-up:
`docs/lineup_anomalies_etl_memory_2026-07-22.md`.

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

**Tab 1:** 16 columns (off/def × on/off × fg2/fg3 × made/att). Source: `onoff_default_mv` via `shot_agg` CTE, or `onoff_compute()` via `player_four_factors_by_game`.

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

Starter filters are not clutch filters. In the Tab 2 functions,
`v_clutch_active` covers margin/status/time only; starters-only requests use
the pre-aggregated lineup MVs with uniform semantics on every row:
`num_starters` is the reporting lineup's own count and `opp_starters` is the
opponent count. Summary's `sub_lineups_stats` fast path still requires no
starter filters, while the Four Factors fast path supports starter predicates
directly.

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
3. Made last free throw (`pct_ft == 1`, where `pct_ft = ft_number / effective_ft_awarded`)
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

**`pct_ft`:** computed in ETL `compute_possessions()` as
`parameters_free_throw_number / effective_ft_awarded`. Normally the effective
denominator is `parameters_free_throws_awarded`; if the provider reports an
impossible smaller total than the current attempt number, the attempt number is
used as the hard lower bound. Raw provider fields remain unchanged. `pct_ft = 1`
means the last FT in the sequence, and DQ check `AJ` enforces the published 0-1
domain.

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
- **segment_id repeats across games and teams** — always identify a duration by `(game_id, team_id, lineup_hash, segment_id)`.
- **Do not derive minutes with raw clock extrema** — `MAX(end_game_seconds_remaining) - MIN(...)` is vulnerable to delayed provider actions. Use persisted canonical `segment_seconds`, deduplicated at segment grain, and count each segment once.

### R / Shiny / DT
- **`bigint = "numeric"` in `dbPool()`** — RPostgres returns PostgreSQL `bigint` as R `integer64` by default, which is incompatible with dplyr `coalesce()`, `+`, and many tidyverse operations. Fix: add `bigint = "numeric"` to the pool connection. Safe for basketball stats (precision loss only for values > 2^53). `SUM()` on integer in PostgreSQL returns `bigint`, so even flag columns (CASE 0/1) produce bigint sums
- **dateRangeInput NA pitfall** — `updateDateRangeInput()` with `start` outside the input's `min` produces `NA`. The "reset to defaults" button must use season-appropriate dates (from `season_date_bounds()`), not global `DEFAULT_START`/`DEFAULT_END`. Also guard `fallback_needed()` and `live_result_df()` against NA dates: `if (is.na(start_d) || is.na(end_d)) return(FALSE)` and `req(!is.na(rng[1]), !is.na(rng[2]))`
- **All DB access uses `dbGetQuery()`** — no `tbl()`/`in_schema()` anywhere in active code. Eliminates metadata round trips (~200-400ms each to Supabase). Pool connections are opened lazily on the first query. Requires `bigint = "numeric"` in pool
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
- **Avoid long reader-blocking migration transactions** — commit metadata-only DDL quickly, build supporting indexes or replacement MVs concurrently where possible, update large tables in bounded game batches, and keep cutover transactions brief.
- **A killed client may leave a server transaction and lock behind** — after a timeout or disconnected DDL client, inspect `pg_stat_activity` and `pg_locks`; terminate a confirmed orphan before retrying.
- **Canonical-clock refresh planner guard** — `refresh_segment_clock_fields_for_games()` locally disables nested loops because PostgreSQL severely under-estimates its large CTE update join. Keep the guard scoped to the function and batch broad backfills.

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
   - File: app/R/global.R
   - Add a pooled connection statement_timeout (for example 8s) via DBI::dbExecute(...) wrapped in tryCatch.

2. **Short TTL cache for Tab 4 season-heavy queries**
   - File: app/R/server_tab4.R
   - Cache gl_lineup_totals and gl_lineup_ff by game_year for 30-60 seconds.

3. **Click burst guard for lineup modal**
   - File: app/R/server_tab2.R
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
     - HeatCell is null/NaN-safe and renders - instead of crashing on toFixed.
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
   - Replaced single-select control with multi-select react-select.
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

5. Add an app-level fast path for Israeli Tab 2 Lineups after measuring the
   deployed tab end to end:
   - Cache the full-season, full-league result by season, lineup size, view, and
     Israeli ETL/data version. Load lineup sizes on demand rather than caching
     every 2-5 player population eagerly.
   - Apply team, players ON/OFF, minimum possessions, and table/stat filters
     locally, preserving full-population percentile ranks.
   - Keep filters that change the game set on the dynamic SQL path initially:
     dates, game type, opponents, home/away, Win/Loss, opponent strength, GN or
     Last N, clutch, and starter context.
   - Summary can use `sub_lineups_stats`; Four Factors can cache the full-season
     result aggregated from `lineup_four_factors_by_game`. Preserve raw additive
     counts and calculate rates only after aggregation.
   - Recorded warm baselines are about `280ms` for Summary's database-internal
     full-season fast path, `435ms` for full-season Four Factors, and `390-550ms`
     for non-clutch filtered calls. Prioritize this work if deployed latency or
     concurrency shows that avoiding repeated default queries is material.

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
- **Takeaway:** For storage-constrained environments, keep heavyweight “long” MVs column-minimal and index-minimal, then rebuild dependent MVs in order.

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
- Safety branch created before alignment: backup/pre_rewrite_20260218_030105.
- Important: commit SHAs changed due to history rewrite; uncommitted local edits were preserved and not included in the rewrite.

## Tab 5 Bottleneck Work + Benchmarks (2026-02-18)
- Scope: addressed selected bottlenecks for Traditional Stats (Tab 5) and measured each change.

### Rebuild
- Ran full MV rebuild (rebuild_all_mvs(from_level = 1)) with ETL credentials.
- Rebuild completed successfully; key post-rebuild counts:
  - df_pts_poss_lineups_longer_mv: 398,700
  - player_traditional_stats_mv: 4,710
  - mv_lineup_totals_by_day: 23,741
  - lineup_four_factors_by_game: 23,741

### #2 Prefilter lineups_lookup in SQL (Tab 5 live path)
- File: app/R/server_tab5_traditional.R
- Change: replaced season-wide lineups_lookup fetch + R-side reduction with SQL-side filtering to the filtered game/team set.
- Benchmark (5 runs):
  - Old avg: 0.582s
  - New avg: 0.472s
  - Improvement: 18.9%
  - Output parity: same rows.

### #3 Remove string-based distinct key counting
- Files:
  - sql/materialized_views/player_traditional_stats_mv.sql
  - app/R/server_tab5_traditional.R
- Change:
  - SQL: COUNT(DISTINCT concat_ws(...)) -> COUNT(DISTINCT (game_id, team_id, poss_end_id))
  - R: n_distinct(paste(...)) -> n_distinct(game_id, team_id, poss_end_id)
- Benchmark (5 runs, SQL expression comparison):
  - Old avg: 0.598s
  - New avg: 0.596s
  - Improvement: 0.3% (marginal)
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
  - Before: 0.326s avg
  - After: 0.292s avg
  - Improvement: 10.4%

### #1 Push recalculation filters to SQL for live path (excluding team-only mode)
- File: app/R/server_tab5_traditional.R
- Change:
  - acts and lineup_map queries now filter directly in SQL by filtered (game_id, team_id) pairs.
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
  - When requested game_ids are missing from fetch_israel_schedule() feed, ETL now falls back to rows already present in basketball_test.schedule.
  - Synthesizes pbp_url/box_url from game_id for fallback rows.
- Upserted State Cup schedule rows into basketball_test.schedule using upsert_by_like and ran ETL validation sequence.

### ETL execution results (test env)
1. Single-game live test (game_id=291) succeeded end-to-end:
   - actions_clean +580, full_rosters +34, possessions +580, lineups_lookup +731, pws +580.
   - All validation checks passed.
2. Remaining 2026 games (292,293,294,303,304) succeeded end-to-end:
   - actions_clean +3052, full_rosters +160, possessions +3052, lineups_lookup +3633, pws +3052.
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
- ETL implementation detail:
  - Phase 6 variable `minute_floor_warn <- 39.0` controls the under-minute threshold.
  - Minute check is game/team-level and runs only for `processed_ids`.
  - Purpose is data-integrity alerting (warnings only); ETL continues and does not fail on these checks.

## Session Notes (2026-02-20): Starters + Lineup Lineage

- Starter source: in ETL, starter flags are extracted from the boxscore payload (`box_url`, `get_team_score.php`) inside `extract_roster()`.
- `lineups_lookup` now carries `num_starters` per `(game_id, team_id, lineup_hash)`.
- `pws` is the source of truth for downstream lineup MV starter counts:
  - `pws.num_starters_offense` joined from lineup hash on offense side.
  - `pws.num_starters_defense` joined from lineup hash on defense side.
- `df_pts_poss_lineups_longer_mv.num_starters` now comes directly from `pws`:
  - offense rows use `num_starters_offense`
  - defense rows use `num_starters_defense`
- Integrity guard added: preserve roster names before `full_rosters` upsert via `enrich_roster_names_from_existing()` to avoid null-name overwrite from partial backfill payloads.
- Root-cause lesson: always join roster identity with `game_year` plus team/player keys where relevant; avoid rebuilding lookup grain when a direct join to existing lookup output is available.

## Session Notes (2026-02-20): `extract_roster` Compatibility Fix + Postmortem

- Required rule: keep legacy roster identity extraction logic unchanged; add only starter enrichment.
- Why this mattered: switching roster extraction to boxscore changed/nullified identity fields (names/team labels) and caused downstream `NA NA` display regressions.

### Old / required `extract_roster` logic (legacy-compatible)
```r
extract_roster <- function(pbp) {
  gi <- pbp$result$gameInfo
  if (is.null(gi$homeTeam) || is.null(gi$awayTeam)) return(tibble::tibble())
  home <- tibble::as_tibble(gi$homeTeam$players) |>
    mutate(team_id = gi$homeTeam$id, game_id = gi$gameId,
           team_name = gi$homeTeam$name, team_name_local = gi$homeTeam$nameLocal)
  away <- tibble::as_tibble(gi$awayTeam$players) |>
    mutate(team_id = gi$awayTeam$id, game_id = gi$gameId,
           team_name = gi$awayTeam$name, team_name_local = gi$awayTeam$nameLocal)
  dplyr::bind_rows(away, home) |>
    rename(player_id = id) |>
    mutate(across(c(game_id, team_id, player_id), as.integer)) |>
    distinct(game_id, team_id, player_id, .keep_all = TRUE)
}
```

### Added function for starter only (do not replace roster extraction)
```r
extract_starters <- function(box) {
  bs <- box$result$boxscore
  gi <- bs$gameInfo
  if (is.null(bs$homeTeam) || is.null(bs$awayTeam)) return(tibble::tibble())

  home <- tibble::as_tibble(bs$homeTeam$players) |>
    rename(player_id = playerId) |>
    mutate(team_id = as.integer(gi$homeTeamId), game_id = as.integer(gi$gameId))
  away <- tibble::as_tibble(bs$awayTeam$players) |>
    rename(player_id = playerId) |>
    mutate(team_id = as.integer(gi$awayTeamId), game_id = as.integer(gi$gameId))

  out <- dplyr::bind_rows(away, home)
  if (!"starter" %in% names(out) && "isStarter" %in% names(out)) out$starter <- out$isStarter
  if (!"starter" %in% names(out) && "starterSign" %in% names(out)) out$starter <- out$starterSign
  if (!"starter" %in% names(out)) out$starter <- FALSE

  out |>
    mutate(
      across(c(game_id, team_id, player_id), as.integer),
      starter = dplyr::coalesce(as.logical(starter), FALSE)
    ) |>
    distinct(game_id, team_id, player_id, .keep_all = TRUE) |>
    dplyr::select(game_id, team_id, player_id, starter)
}
```

### ETL flow rule (current)
- Build `roster_df` from PBP: `map(pbps, extract_roster)`.
- Build `starters_df` from boxscore: `map(boxes, extract_starters)`.
- `left_join` on `(game_id, team_id, player_id)`, default `starter=FALSE` if missing.

### Postmortem: mistakes in this session and why they were harmful
- Mistake: replaced `extract_roster` source from PBP to boxscore.
- Harm: broke compatibility with legacy identity/name fields and propagated `NA NA` names in app outputs.
- Mistake: changed function signature/behavior without preserving existing ETL contracts.
- Harm: increased regression risk in downstream joins and made debugging harder.
- Mistake: rebuilt lookup-related paths instead of using direct joins where already available.
- Harm: created duplicate-variant risk and unnecessary complexity.
- Rule going forward: when extending ETL, prefer additive enrichment to existing trusted functions; do not change source semantics unless explicitly requested.


## Session Notes (2026-02-21): full_rosters Backfill + lineups_lookup Rebuild
- Updated ETL roster year assignment to use `schedule.game_year` by `game_id` (instead of `Sys.Date()` / hardcoded year):
  - `etl/etl_full.R`
  - `etl/etl_onoff.R`
- Kept roster identity source on PBP (`extract_roster`) and starter source on boxscore (`extract_starters`), joined on `(game_id, team_id, player_id)`.
- Added `extract_starters` guard for one-sided/empty boxscore player arrays so ingestion does not fail when one side is blank.
- Ran full `full_rosters` backfill across scored schedule games in `basketball_test` using constructed Segev URLs:
  - `pbp_url = get_team_action.php?game_id=<id>`
  - `box_url = get_team_score.php?game_id=<id>`
- Known exceptions (left as-is for now): `62522`, `62527`, `62541`.
- Rebuilt only `lineups_lookup` from current `actions_clean` + `full_rosters` for all available game IDs.
- Compatibility audit after rebuild:
  - `lineups_lookup` vs `full_rosters` key mismatch rows: `0`
  - `game_year` mismatch rows: `0`
  - null `game_year` in both tables: `0`
- `pws` integrity check against rebuilt `lineups_lookup`:
  - offense/defense lineup-hash missing in `lineups_lookup`: `0`
  - offense/defense `num_starters` mismatch: `0`
  - only out-of-sync game_ids are the known exceptions: `62522`, `62527`, `62541`
- Operational takeaway:
  - No full `pws` rebuild required right now for normal games.
  - Revisit the 3 known exception games later with a dedicated workaround.

## Session Notes (2026-02-22): Starters Filter Semantics + Lineup Data num_starters
- Updated starters filter labels across tabs to match own/opponent semantics and expose explicit reset choice:
  - `ALL` option now shown in starters mode selects on Tabs 1/2/3/4.
  - Labels use `Own lineup starters` / `Opponent lineup starters` and `Own value` / `Opp value`.
- Lineup Data (Summary) now shows `# Starters` instead of `Size`:
  - UI table switched to `num_starters` display column.
  - Safe fallback kept in app server: if SQL response lacks `num_starters`, use `num_lineup` temporarily.
- SQL function output updated to return `num_starters` natively:
  - `basketball_test.fetch_lineups_all(...)`
  - `basketball_test.fetch_lineups_csv_v2(...)`
- `num_starters` computation details:
  - fast/default branch (`sub_lineups_stats` path): `num_starters = num_lineup::numeric`
  - dynamic branches (clutch and non-clutch): possession-weighted average using
    `SUM(num_starters * total_poss) / SUM(total_poss)`.
- After function deploy and MV rebuild, row-count consistency check passed:
  - `df_pts_poss_lineups_longer_mv = 411,602`
  - `player_onoff_by_game = player_four_factors_by_game = 414,784`
  - `mv_lineup_totals_by_day = lineup_four_factors_by_game = 24,507`

### Clarification: Offense/Defense vs Own/Opp Starters Intent
- Previous interpretation (`offense starters` / `defense starters`) could imply filtering only by possession side.
- Current intended business meaning is lineup-context based, not possession-side based:
  - `Own starters` = number of starters in the player's/team lineup context.
  - `Opp starters` = number of starters in the opposing lineup context.
- This is evaluated for the possessions where the player/lineup is on court, using own-vs-opponent lineup composition semantics rather than “offensive possession only” or “defensive possession only” semantics.
- Practical implication:
  - `Own starters` filter asks: “When we were in this lineup-starter state, how did we perform?”
  - `Opp starters` filter asks: “Against opponent lineups in this starter state, how did we perform?”

## Session Notes (2026-02-22): Read-Only RLS Rollout (Supabase)
- Added SQL migration: `sql/security/enable_readonly_rls.sql`.
- Migration behavior:
  - Enables RLS on base tables (`relkind in ('r','p')`) in schemas `basketball_test` and `basketball`.
  - Creates idempotent read policy `rls_read_all` with `USING (true)` for roles:
    - `anon`
    - `authenticated`
    - `service_role`
  - Grants schema/table read access and default read privileges for those roles.
- Deployment/verification:
  - Applied on Supabase direct connection (port `5432`) using ETL credentials.
  - In `basketball_test`, RLS is enabled on `19` tables.
  - Strict before/after validation used row-count + content digest checks over key MVs and function outputs:
    - `final_schedule_mv`
    - `df_pts_poss_lineups_longer_mv`
    - `mv_lineup_totals_by_day`
    - `player_onoff_by_game`
    - `player_four_factors_by_game`
    - `lineup_four_factors_by_game`
    - `onoff_default_mv`
    - `team_ppp_ratings_mv`
    - `onoff_compute(...)`
    - `four_factors_compute(...)`
    - `fetch_lineups_csv_v2(...)`
    - `fetch_lineups_four_factors_csv(...)`
    - `get_team_ratings_dynamic(...)`
    - `get_team_four_factors_dynamic(...)`
  - Outcome: all checks identical (`OVERALL_IDENTICAL=TRUE`).
- ETL permission check after RLS:
  - ETL user (`postgres.jfmxhveitknfwqpjoamn`) retains write capability (rollback-safe INSERT/UPDATE smoke tests passed).
  - Role has `rolbypassrls = true`, so ETL pipelines are not blocked by RLS.

## Session Notes (2026-02-23): App Compatibility + SQL Function Version Verification
- App code compatibility check:
  - Parsed successfully: `app/app.R` and all files under `app/R/*.R`.
  - Runtime source smoke test passed (`SOURCE_OK`) without launch-time object errors.
  - Filter-chip wiring is complete across tabs (UI `uiOutput(...)` + server `renderUI(...)` + shared `build_filter_chips(...)` in `global.R`).
- DB compatibility check with app credentials (`app/.Renviron`):
  - Successful calls (signature-compatible and readable):
    - `onoff_compute(...)`
    - `four_factors_compute(...)`
    - `get_team_ratings_dynamic(...)`
    - `get_team_four_factors_dynamic(...)`
    - `fetch_lineups_csv_v2(...)`
    - `fetch_lineups_four_factors_csv(...)`
    - `get_player_traditional_dynamic(...)`
    - `player_traditional_stats_mv`, `full_rosters`
- SQL version verification (active DB definitions):
  - Database contains both legacy and extended overloaded signatures for several functions.
  - App call-sites use the extended/latest arities (including starters-range params), so PostgreSQL resolves to the latest overloads at runtime.
  - Confirmed for:
    - `onoff_compute`
    - `four_factors_compute`
    - `get_team_ratings_dynamic`
    - `get_team_four_factors_dynamic`
    - `fetch_lineups_csv_v2`
    - `fetch_lineups_four_factors_csv`



## Session Update (2026-02-26)
- Fixed Game Logs filter-chip crash in app/R/server_tab4.R by guarding team_label_map creation (avoid setNames() on invalid/empty team data).
- Set Player Stats default display mode to Per Game in app/R/ui_tab5_traditional.R and aligned server fallbacks/resets in app/R/server_tab5_traditional.R.
- Commit reference: bcc6583.
## Session Notes (2026-02-27): Incremental Analytics Tables + Name Canonicalization
- Phase 4 now uses mixed refresh mode:
  - Refresh MVs by dependency level.
  - Refresh incremental tables by `game_id` set:
    - `player_four_factors_by_game` via `refresh_player_four_factors_by_game_for_games(int4[])`
    - `team_metrics_by_game_mv` via `refresh_team_metrics_by_game_for_games(int4[])`
    - `onoff_default_mv` via `refresh_onoff_default_for_games(int4[])`
    - `player_advanced_stats_mv` via `refresh_player_advanced_stats_for_games(int4[])`
- `onoff_default_mv` and `player_advanced_stats_mv` are now physical tables (not materialized views) and are refreshed by affected `game_year` derived from input `game_ids` because ranking fields are season-scoped.
- Name/root-cause fix for duplicate player rows:
  - Canonicalized roster-name aggregation in SQL by `(player_id, team_id, game_year)`.
  - Normalized ETL name fields (trim/collapse whitespace, normalize dotted initials) and mapped `team_name` from schedule-side team names.
  - Explicit canonical mapping retained for `player_id=29543` (`???? ??????` -> `YARON GOLDMAN`).
- Added unique indexes:
  - `onoff_default_mv`: `("Year", team_id, player_id)`
  - `player_advanced_stats_mv`: `(game_year, team_id, player_id)`
- Added Phase 6 integrity assertions in `etl/etl_full.R`:
  - duplicate-key check for `onoff_default_mv` on `("Year", team_id, player_id)`
  - duplicate-key check for `player_advanced_stats_mv` on `(game_year, team_id, player_id)`
  - on violation, Phase 6 is marked failed.
- Verification snapshot after rebuild + ETL smoke on game_ids `161,162,164`:
  - duplicate groups in `onoff_default_mv`: `0`
  - duplicate groups in `player_advanced_stats_mv`: `0`

## Session Notes (2026-02-28): Incremental `df_pts_poss_lineups_longer` + Safety/Parity
- Added targeted pre-change backup artifacts under:
  - `backups/pre_dfppllm_incremental_20260227/`
  - includes metadata snapshots (`row_counts.csv`, `signatures.csv`, `index_defs.csv`, `object_defs.csv`) and targeted pg_dump (`basketball_test_targeted_pre_dfppllm.dump`).
- Converted `basketball_test.df_pts_poss_lineups_longer_mv` from materialized view to incremental table-maintained object.
  - Build SQL updated: `sql/materialized_views/df_pts_poss_longer.sql`.
  - New incremental function:
    - `basketball_test.refresh_df_pts_poss_lineups_longer_for_games(int4[])`
    - file: `sql/functions/refresh_df_pts_poss_lineups_longer_for_games.sql`
- ETL Phase 4 updated in `etl/etl_full.R`:
  - L1 now refreshes `final_schedule_mv` as MV.
  - `df_pts_poss_lineups_longer_mv` refreshed incrementally by `processed_ids` via new function.
  - Phase 4 summary now reflects `7 MVs + 5 incremental tables`.
- Rebuild registry updated for mixed object type:
  - `df_pts_poss_lineups_longer_mv` marked as `type = "table"` in `sql/rebuild_all_mvs.R`.
- Post-conversion parity checks:
  - Pre signature: `eac162dac28930f20412a6ad0b44a23f`
  - Post signature: `eac162dac28930f20412a6ad0b44a23f` (exact match)
  - Row count unchanged: `415,298`.
- Performance snapshot:
  - Full refresh via function (`NULL`): ~`44.84s`
  - Incremental refresh for 3 recent games (`164,162,161`): ~`1.04s`, touched `3,696` rows.
- End-to-end ETL smoke (`etl_full(game_ids=c(161,162,164))`) succeeded:
  - Phase 4 logged `[INC] df_pts_poss_lineups_longer_mv ... touched 3,696 rows`.
  - Downstream incremental tables and Phase 6 validations passed.
- Index persistence fix:
  - Re-added and persisted unique key indexes in table-build SQL for:
    - `onoff_default_mv` -> `idx_onoff_default_pk ("Year", team_id, player_id)`
    - `player_advanced_stats_mv` -> `idx_pas_pk (game_year, team_id, player_id)`
  - Duplicate checks remain clean (`0` groups) for both tables.

## Session Notes (2026-02-28): Storage Cleanup (Orphan Tables + Index Audit)
- Performed targeted cleanup of obsolete helper/orphan tables in `basketball_test` (no internal dependencies found):
  - dropped: `onoff_default`, `lineup_dim`, `lineup_players`, `two_man`, `schedule_stage_load`.
- Code/reference audit (app/etl/sql/frontend) did not find active usage of those dropped objects.
- Ran index-usage audit for high-volume upstream tables:
  - `actions_clean`
  - `pws`
  - `lineups_lookup`
- Audit result:
  - no non-unique zero-scan indexes eligible for safe drop.
  - all remaining indexes on those tables show usage (`idx_scan > 0`) or are PK/unique.
- Size impact from orphan-table drop was minimal (`~73 KB`).
- Earlier `VACUUM FULL` on top 20 `basketball_test` relations reduced DB size by ~`114.3 MB`.

## Session Update (Traditional Mirror + Rebounds)
- df_pts_poss_lineups_longer_mv now follows dynamic type_lineup semantics from the duplication flow (mirrored offense/defense context rows).
- Traditional rebound rule aligned across team/player traditional pipelines:
  - OREB: type='rebound' AND type_lineup='offense' AND parameters_type='offensive'
  - DREB: type='rebound' AND type_lineup='defense' AND parameters_type='defensive'
  - REB = OREB + DREB
- Updated SQL objects:
  - sql/functions/get_player_traditional_dynamic.sql
  - sql/functions/refresh_team_metrics_by_game_for_games.sql
  - sql/materialized_views/player_traditional_stats_mv.sql
  - sql/materialized_views/team_metrics_by_game_mv.sql
- Team Ratings traditional mode uses mirrored opponent logic (defense-mode stats mirror offense-mode stats from the opponent perspective).
- ETL Phase 4 (incremental) now refreshes player_traditional_stats_mv when the materialized view exists (guarded in etl/etl_full.R).

## Session Update (Tab 4 Date Bounds)
- Game Logs tab defaulted to static `DEFAULT_START/DEFAULT_END` while season default is `2026`, causing empty results and a perceived load failure.
- Fix in `app/R/server_tab4.R`: on `input$game_year` change, `gl_dates` is now reset to `shared$season_date_bounds(...)`.
- Reset flow now also restores season bounds (with min/max limits) instead of clearing dates to `NA`.

## Session Update (2026-03-03)
- Fixed Game Logs tab date-range mismatch in `app/R/server_tab4.R`.
- Root cause: the tab used static defaults (`DEFAULT_START`/`DEFAULT_END`) while the active default season is `2026`.
- Change: `gl_dates` now syncs to `shared$season_date_bounds(input$game_year)` on season change.
- Change: reset now restores season bounds (with min/max) instead of setting dates to `NA`.
- Impact: Game Logs loads correctly on default season without manual date adjustment.

- Fixed Traditional rank-delta rendering in `app/R/server_tab3.R`.
- Change: `show_delta` now follows `tr_delta_enabled()` instead of being hard-disabled.
- Impact: delta arrows are shown in Traditional mode (team/opponent) when baseline rules allow deltas.

## Session Update (2026-03-05): Team Ratings Reset Crash Fix
- Fixed Team Ratings reset crash (`Error in unclass(x) : cannot unclass an environment`) triggered after clicking "Reset to defaults".
- Root cause was in Shiny output flush/JSON serialization for reset update payloads (not SQL/data logic).
- Stabilization changes:
  - `app/R/server_tab3.R`
    - reset now uses canonical scalar/vector payloads
    - `tr_trad_defense_mode` reset switched from `bslib::update_switch(...)` to `updateCheckboxInput(...)`
    - selectize resets normalized to `selected = character(0)`
    - date reset uses season bounds (`start/end`) without extra reset payload fields
  - `app/app.R`
    - removed temporary global debug error-hook overrides to avoid handler side effects during runtime errors
- Result: Team Ratings reset no longer crashes; app stays responsive.

## Session Update (2026-03-05): Stability + Test Infrastructure Hardening
- Fixed Team Ratings reset crash path (`unclass(x)` during Shiny JSON flush) by stabilizing reset payloads and removing brittle runtime debug hook behavior.
  - Updated: `app/R/server_tab3.R`, `app/R/global.R`, `app/app.R`
- Fixed Lineup tab reset date behavior to always restore season bounds instead of `NA` date payloads.
  - Updated: `app/R/server_tab2.R`
- Fixed Tab 1 fallback filter behavior where applying game filters could incorrectly return empty results unless `min_all_poss = 0`.
  - Root cause: fallback SQL path enforced possession thresholds differently from MV path.
  - Fix: fetch fallback rows with `min_all=0`/`min_on=0`, then apply local min-possession filtering consistently with MV behavior.
  - Updated: `app/R/server_tab1.R`

### Testing System Added
- Added a project test harness under `app/tests/testthat` and one-command runner:
  - `scripts/test_all.R`
- Added contract tests covering all primary tabs (1-5):
  - UI/server wiring for reset + filter chips
  - date-reset season-bounds contract (no `NA` reset dates)
  - parse smoke tests for tab UI/server files
  - filter-chip date/environment guard checks
- Added `shiny::testServer` smoke tests for tabs 1-5 reset flows.
- Added optional `shinytest2` E2E tab reset smoke tests (guarded by `RUN_E2E=1`).
- Added optional DB parity tests for fixed game IDs (`159,160,161,162,163`) validating team four-factor rebound fields against direct lineup FF aggregates (guarded by `RUN_DB_TESTS=1` + DB env vars).

### CI / Release Gate
- Added/updated GitHub workflows:
  - `.github/workflows/r-tests.yml` to run automated tests on push/PR.
  - `.github/workflows/deploy-gated.yml` to enforce a test gate before deploy job execution.
- Default CI mode keeps optional E2E/DB tests off unless explicitly enabled via env flags.

## Session Update (2026-03-05): CI/Test Gate + ETL GitHub Workflow
- Added multi-layer testing and release gating:
  - `app/tests/testthat` suite for tab wiring, date reset contracts, parse checks, and filter/date guards.
  - `shiny::testServer` smoke tests for tabs 1-5 reset flows.
  - optional `shinytest2` E2E smoke tests (`RUN_E2E=1`).
  - optional DB parity tests on fixed game IDs (`RUN_DB_TESTS=1`) for key four-factor rebound fields.
  - one-command runner: `scripts/test_all.R`.
- CI policy implemented in GitHub Actions:
  - `.github/workflows/r-tests.yml`
    - fast suite on every push/PR to `main`.
    - full suite (E2E + DB parity) on nightly schedule.
  - `.github/workflows/deploy-gated.yml`
    - full pre-deploy test gate, deploy job runs only after gate passes.

### ETL Automation Added (GitHub Actions)
- Added ETL workflow:
  - `.github/workflows/etl-full.yml` (name: `ETL Full`)
  - triggers:
    - manual (`workflow_dispatch`)
    - nightly schedule
  - runs on `windows-latest` and uploads ETL logs as artifacts.
- Added portable ETL wrapper:
  - `scripts/run_etl_full.ps1`
  - no hardcoded machine paths; auto-resolves repo root and `Rscript` from `RSCRIPT_PATH` or `PATH`.
  - supports flags:
    - `-DryRunOnly`
    - `-SkipDryRun`

### ETL Workflow Inputs / Secrets
- Manual inputs (`Run workflow`):
  - `app_env` (default `test`)
  - `dry_run_only` (boolean)
  - `skip_dry_run` (boolean)
- Required GitHub Actions secrets for DB access:
  - `PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASS`, `PG_SSLMODE`
- Dry-run only in GitHub Actions:
  - set `dry_run_only = true`, `skip_dry_run = false`.

## Session Update (2026-03-08): E2E CI Stabilization (shinytest2)
- Full R workflow now follows shinytest2 CI pattern for the full run:
  - uses `r-lib/actions/setup-pandoc@v2`
  - uses `rstudio/shinytest2/actions/test-app@actions/v1` with `app-dir: app`
  - fast suite remains custom `Rscript scripts/test_all.R`

### Root Causes Found
- `actionButton` reset events were triggered in tests via `set_inputs(...)` instead of real clicks.
  - In shinytest2 this can fail to reproduce real button behavior.
  - Fix: use `app$click("<reset_id>")` for all tab reset actions.
- Multi-select `*_game_type` resets used `selected = ""` in app server code.
  - For `multiple = TRUE` selectize inputs, canonical clear is `character(0)`.
  - Fix applied across tab reset handlers and shared chip-clear helper.
- E2E tests were reported as `empty test` when passing.
  - Cause: helper-only checks without explicit `expect_*`.
  - Fix: add explicit `expect_true(...)` per test.

### E2E Test Method (Recommended)
- Use `AppDriver` with explicit timeout (`timeout = 60000`).
- For button actions, prefer `app$click(...)` over `set_inputs(...)`.
- Use retries around flaky operations (`set_inputs`, `click`) and poll target input state.
- Assert final state explicitly with `expect_true(...)`.

### Debugging Method (Used in this session)
- Validate UI behavior independently with Playwright on live app:
  - set game type -> click reset -> verify it clears in UI.
- Measure cold-run timing separately from CI:
  - load was ~6.8s in repeated cold runs
  - reset clear was ~5s in repeated cold runs
- If CI still fails while UI is correct, prioritize test-harness semantics (event triggering, assertion style) before increasing timeouts further.

## Session Update (2026-03-08): Project Skill Library (Credential-Safe)
- Added reusable project skills under docs/skills/ (all generated/validated with skill-creator tooling and kept credential-free):
  - shiny-tab-contracts
  - basketball-sql-semantics
  - shiny-ci-e2e
  - etl-wrapper-ops
  - deploy-and-runtime-hygiene
  - performance-safe-refactors
  - data-integrity-guards

### Skill Purpose Summary
- shiny-tab-contracts: keep cross-tab reset/filter/UI contracts consistent.
- basketball-sql-semantics: preserve metric definitions and clutch/type_lineup semantics.
- shiny-ci-e2e: stabilize shinytest2 and GitHub Actions test behavior.
- etl-wrapper-ops: harden ETL wrapper execution, logging, and local/CI parity.
- deploy-and-runtime-hygiene: keep deploy/runtime setup robust and non-local-path dependent.
- performance-safe-refactors: improve hot-path performance while preserving outputs.
- data-integrity-guards: run structural and metric integrity checks before/after ETL or SQL changes.

## Git Branch Workflow

Use short-lived feature branches (do not commit new work directly to `main`).

Solo workflow (no formal PR needed):

1. Create/switch branch from updated main:
   - `git checkout main`
   - `git pull origin main`
   - `git checkout -b feature/<short-topic>`
2. Commit and push branch:
   - `git add <files>`
   - `git commit -m "<message>"`
   - `git push -u origin feature/<short-topic>`
3. Merge locally into `main`:
   - `git checkout main`
   - `git merge --no-ff feature/<short-topic>`
   - `git push origin main`
4. Delete branch after merge:
   - Local: `git branch -d feature/<short-topic>`
   - Remote: `git push origin --delete feature/<short-topic>`

If an urgent hotfix is committed to `main`, return to branch workflow for the next change.
## Session Update (2026-03-19): Idle Timeout + shinyapps Deploy Fix
- App idle session timeout is active and configurable:
  - APP_IDLE_TIMEOUT_MIN (preferred; minutes)
  - APP_IDLE_TIMEOUT_SEC (fallback; seconds)
  - APP_IDLE_CHECK_SEC (check interval; seconds)
- App server now reads timeout values from global.R constants (no hardcoded timeout in app.R).
- Browser activity heartbeat is throttled (15s) to avoid unnecessary high-frequency Shiny.setInputValue(...) events.
- Tab 7 player-compare non-timeout DB errors now log detailed message server-side and show a generic user notification.

### Deploy Failure Root Cause (shinyapps.io)
- Error observed:
  - Error in client$setEnvVars(application$guid, deployment$envVars) : attempt to apply non-function
- Cause:
  - Local rsconnect client object for shinyapps.io did not expose setEnvVars, but deployment metadata requested env var update.
  - app/rsconnect/shinyapps.io/ibpl-stats/onoff-shiny.dcf had envVars: ...
- Fix:
  - Removed envVars: from that DCF so deployApp() does not attempt setEnvVars(...) on redeploy.
## Session Update (2026-03-26): ETL Automation Reliability (Cold Storage Follow-up)
- Root cause in automation path:
  - `scripts/run_etl_full.ps1` could fail immediately when `RSCRIPT_PATH` was unset and `Rscript` was not on PATH.
  - Phase 7 cold-storage purge ran even when no new games were processed, adding unnecessary parquet/DDL steps to routine no-op runs.
- Fixes applied:
  - `scripts/run_etl_full.ps1`: added fallback to `C:\Program Files\R\R-4.4.2\bin\Rscript.exe` when env/PATH lookup fails.
  - `etl/etl_full.R`: Phase 7 now runs only when `length(processed_ids) > 0` and pipeline is healthy; otherwise logs skip reason.
- Verification:
  - Wrapper run completed with `exit_code=0` and explicit log line:
    - `Skipping Phase 7 (cold storage purge): no new games processed`
  - Log reference: `logs/etl_full_wrapper_20260324_163129.log`
## Session Update (2026-03-27): Compare Tab Four-Factor Fixes + Labeling
- Compare tab root cause for `TS%`, `TOV%`, and `OREB%` chip crashes:
  - `TEAM_METRICS` in `app/R/server_tab7_compare.R` used nonexistent names (`off_ts_pct`, `off_tov_pct`, `off_oreb_pct`) while compare SQL returns `off_ts`, `off_tov`, `off_oreb`.
  - `cmp_joined()` also still used the old `_pct` names in the `is_ff` branch check, so even after fixing the chip map it could still route through ratings instead of four factors.
  - Failure mode was the Shiny/data-frame error `replacement has 0 rows, data has N` because `pick_cols()` could not create `metric_a` / `metric_b`.
- Fixes applied:
  - Updated tab 7 team metric mappings and the `is_ff` checks to use actual four-factor column names.
  - Added `app/tests/testthat/test-tab7-compare-server.R` with real `shiny::testServer` coverage for tab 7 four-factor chip flows (Teams and Lineups).
  - Extended `app/tests/testthat/helper-server-mocks.R` with tab 7 mock query responses so compare tests run in CI without a live DB.
- Testing lesson:
  - Existing tab 7 coverage was mostly text/contract based and did not execute the chip-selection reactive path.
  - For Shiny regressions like this, prefer at least one behavioral `testServer()` path that sets inputs in the same sequence as the UI (mode change first, chip click second).
- Compare side-label work:
  - Added `side_label_short()` / `side_label_full()` split in tab 7 usage.
  - Compare DT headers now use short labels (`Home`, `Away`, `Starters`, `Bench`, etc.); custom mode keeps `A`/`B` and renders colored badges via `headerCallback`.
  - Detail view column headers now use short labels while summary cards and detail subheader keep full verbose labels.
- Verification:
  - `Rscript scripts/test_all.R` passed locally after the compare fixes and side-label changes.

## Session Update (2026-04-06): Tab 5 Navbar Mode Sync + Per-30 Minutes Fix
- Root cause for Tab 5 mode switching appearing broken in the app/deploy:
  - The visible navbar hover menu in app/app.R was driving a hidden selectInput("ts_display_mode", ...) in app/R/ui_tab5_traditional.R.
  - The JS path only mutated the DOM select value and triggered a browser change event, which was brittle and could fail to propagate the new value to Shiny consistently.
  - Result: input in app/R/server_tab5_traditional.R could stay stale, so Totals, Per 60 Possessions, and Per 30 Minutes appeared not to apply even though the navbar UI suggested they had changed.
- Fix applied:
  - Hardened the shared navbar mode-menu handler in app/app.R to call Shiny.setInputValue(...) explicitly for both select-backed and radio-backed tab mode controls, in addition to updating the underlying DOM control.
  - This makes the navbar mode picker more robust across tabs, with Tab 5 as the original failure case.
- Separate Tab 5 calculation bug:
  - In Per 30 Minutes, count stats and possessions were normalized, but minutes itself was not.
  - Result: the Min column still showed the source minutes instead of 30, making the row internally inconsistent.
- Fix applied:
  - app/R/server_tab5_traditional.R: in apply_ts_mode(), minutes is now explicitly normalized as minutes / minutes * 30, so valid rows show Min = 30 in Per 30 Minutes.
- Related Tab 5 stat-chip work:
  - The stat-filter chips feature filters the current display-mode values (not raw base totals), while percentage columns remain effectively stable because they are not transformed by the mode conversion.

## Session Update (2026-04-08): Tab 5 Total Poss In Rate Modes
- Requirement clarified:
  - Keep "Poss On Floor" as the mode-adjusted display column in all modes.
  - Add a separate "Total Poss" column only in non-Totals modes.
  - In Totals mode, do not show both because "Poss On Floor" already represents the raw total possession count.
- Implementation:
  - app/R/server_tab5_traditional.R copies the pre-conversion poss_on_floor value into an in-memory total_poss column before apply_ts_mode() mutates the mode-specific display values.
  - total_poss is then inserted next to "Poss On Floor" only for non-Totals table render and CSV export paths.
  - The stat-filter menu exposes "Total Poss" only outside Totals mode.
- Performance note:
  - This is not a new DB query or persistent cache.
  - It is a single vectorized in-memory column copy on the already-loaded Tab 5 data frame, so the cost is negligible compared with the query and DT render.

## Architecture Decision (2026-07-22): Web Stack Direction

- The deployed R/Shiny application is the maintained product and the source of
  truth for behavior. Continue normal feature work and maintenance there.
- `frontend-v2` and its R/Plumber API are stale prototypes, not parity-complete
  application layers. Do not treat them as current contracts or update them
  incidentally during Shiny work.
- There is no active Python rewrite. FastAPI async behavior alone is not a
  sufficient reason to migrate: it can improve concurrent I/O handling, but it
  does not make PostgreSQL queries faster, and Python Shiny retains stateful,
  serial reactive execution unless long work is explicitly moved to extended
  tasks.
- If a future full web migration is approved, prefer a stateless
  React/TypeScript frontend plus FastAPI backend over Python Shiny plus FastAPI.
  Treat the existing React code as a prototype: reuse worthwhile presentation
  components, but rebuild API/data contracts from the active Shiny behavior.
- Any future migration must be incremental and parity-tested by vertical slice.
  Keep the PostgreSQL functions/materialized views and the working R ETL in
  place initially, and keep R/Shiny live until the replacement covers the
  required behavior.
- If the goal is only near-term maintainability, refactor the active R/Shiny
  modules into smaller query, transformation, and rendering units rather than
  changing languages.

### Known React drift

Among other drift, Shiny Tabs 1/3/7 have a "Shot Profile" display mode
(shot-diet shares including corner-3-of-known-3PA) that `frontend-v2` does not
implement. See `docs/superpowers/specs/2026-07-16-shot-profile-design.md` for the
current feature specification.

## Session Update (2026-08-15): Real `# Starters` on Both Lineup Tabs

### What the column was

Tab 2's `# Starters` was two different things depending on which branch of
`fetch_lineups_all` answered the request:

| Branch | Expression |
|---|---|
| Fast path (`sub_lineups_stats`) | `s.num_lineup::numeric` — the group size, a constant |
| Filtered path A | `ROUND(SUM(lt.num_starters * lt.total_poss) / NULLIF(SUM(lt.total_poss),0), 2)` |
| Filtered path B | same weighted expression |

The possession-weighted average already existed on both filtered paths. Only
the fast path returned a placeholder, so the column silently changed meaning
whenever a filter forced a dynamic path — visible as a starter filter showing
5-player units with values below 5. That is what prompted the work.

### Purpose

Make the column mean one thing everywhere, in both leagues: the
possession-weighted mean of own starters on court,
`SUM(own_starters * possessions) / (off_poss + def_poss)`. Weighted by
offensive **and** defensive possessions — not offence alone — because that is
what the two filtered branches already did, and conforming the fast path to
live code beat changing two expressions that were working.

### What changed

Israeli (`3880747`):

- `ALTER TABLE basketball_test.sub_lineups_stats ADD COLUMN starters_poss_num numeric`.
- `refresh_sub_lineups_stats()` and `refresh_sub_lineups_stats_for_games()`
  compute it, summed at the **possession grain**
  (`SUM(CASE WHEN final_end_poss THEN num_starters ELSE 0 END)`) rather than by
  adding `num_starters` to the GROUP BY the way `fetch_lineups_all` does. Same
  value, but it cannot fan out the join to `segment_times` if `num_starters`
  ever varies inside a segment.
- `fetch_lineups_all` fast path divides the stored numerator by
  `off_poss + def_poss`, same expression and same 2-decimal rounding as the
  filtered branches.
- `fetch_lineups_csv_v2` needed nothing — it is a pure wrapper
  (`RETURN QUERY SELECT * FROM fetch_lineups_all(...)`).

EuroLeague (`94ba256`, migration `euroleague/sql/039_lineup_starters_numerator.sql`):

- `sub_lineups_stats_mv` and all three readers (`fetch_lineups_dynamic`,
  `fetch_lineups_direct`, `fetch_lineups_pergame`) gain `starters_poss_num`.
  Every reader already FILTERED on `own_starters`; none RETURNED it.
- The read layer returns the **numerator**, never the ratio — Tab 10 derives
  every rate in R from summed counts and the schema stores no ratios by design.
- `fetch_lineups_pergame` added to both security declarations.

Display (`750a4dd`): `# Starters` renders as a whole number in the shared
renderer, so both tabs move together. Display only — filter and sort still use
the exact value.

### Pitfalls hit, in the order they bit

1. **The spec's description of the column was simply wrong**, and so was an
   earlier version of this note. "Tab 2's `# Starters` is a constant equal to
   the group size" is true of one branch out of three. It was corrected only
   because the behaviour was questioned against the live app. Read every branch
   of a multi-path function before describing what it returns.
2. **`num_starters` and `own_starters` are the same value under two names.**
   `df_pts_poss_longer.sql` aliases both to `pws.num_starters_offense` on
   offense rows and `pws.num_starters_defense` on defense rows. The filter uses
   one name and the display the other; they agree. Do not "fix" this.
3. **The GRANT risk was the opposite of what the spec claimed.**
   `basketball_test.sub_lineups_stats` is a TABLE, so `ADD COLUMN` preserves
   its ACL, and all three functions kept their signatures under
   `CREATE OR REPLACE`, so EXECUTE survived. The hazard was entirely on the
   EuroLeague side, where `sub_lineups_stats_mv` is a MATERIALIZED VIEW whose
   query cannot be altered.
4. **`CREATE FUNCTION` grants EXECUTE to PUBLIC by default.** Recreating the
   three EuroLeague readers silently widened them to every role in the
   database; the pre-migration ACL had no PUBLIC entry. Caught by diffing
   `pg_class.relacl` / `pg_proc.proacl` against values captured *before* the
   migration. Always capture the ACL first, and always REVOKE FROM PUBLIC
   before re-granting.
5. **`information_schema.role_table_grants` lies** — it filters by the current
   user's role membership and reported *no* grants on a relation that plainly
   had them. Query `pg_class.relacl` / `pg_proc.proacl` instead.
6. **Integer division, silently.** `lineup_totals_by_game.possessions` is
   `integer`, so `sum(smallint * integer)` is `bigint`, and `bigint / bigint`
   truncates. The MV returned `2` where the readers returned `2.9895`. The
   readers were correct only because they cast `::numeric` in their final
   SELECT. Fixed by casting in the MV. This shifted the season averages
   upward — 5-man units read 2.22 before and 2.34 after.
7. **The fast-vs-filtered invariant is what caught it.** Comparing the two
   paths over the full-season window is cheap and is a direct test of the
   defect being fixed. It reported 1,377 of 5,864 units differing; component
   comparison said identical. The contradiction was the signal — chasing it
   found the truncation. Both leagues now agree to 0.0000 across every unit.
8. **The Edit tool converted three LF-only SQL files to CRLF**, turning an
   85-line change into a 2,000-line diff. Caught on the `git diff --stat`
   plausibility check, stripped, amended. Check the stat before every commit
   touching SQL.

### Still open

- **`refresh_sub_lineups_stats()` is not actually full.** 301 rows in the 2025
  season were never regenerated and so have a NULL numerator, despite having
  possessions. Their `off_poss`, `off_pts` and the rest are equally stale
  leftovers; the new column merely exposed it. Those rows render `# Starters`
  blank.
Both tabs have now been checked in a running browser — the Israeli side during
the parity work, the EuroLeague side by the maintainer after the whole-number
rounding landed.
