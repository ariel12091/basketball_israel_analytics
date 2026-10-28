# CLAUDE.md

## Web app (Next.js, `web/`) -- LIVE, deploys from main

The Next.js/React app replacing Shiny is live at https://ibpl-game-logs.vercel.app
(Vercel, Hobby plan). **A push to `main` that changes `web/` deploys Vercel
Production**; a push to `main` also redeploys Shiny on Connect Cloud, which
stays live as the fallback. Read `web/CLAUDE.md` first: rules for the app,
how to deploy (feature branch -> Preview + live checks -> merge), load tests
and rollback.

- Work on `react/<name>` branches off `main`; merge only after the Preview
  live checks (`.github/workflows/web-preview-live.yml`) passed on the head.
- `sql/web-foundation` holds the migration history, the golden harness and
  unapplied SQL/loader edits. **Never merge it whole.**
- Parity fixtures (`web/test/goldens/`, ~750 MB) are gitignored and generated
  from the R goldens by `scripts/export_goldens_json.R` (on
  `sql/web-foundation`); without them the parity tests fail by design.
- Budget: Fast Origin Transfer is 10 GB/month; tab pages are CDN-cached for
  an hour, and each Preview load test costs ~0.35 GB.

## Token Efficiency

**Trust this context** — avoid re-reading files or spawning exploration agents when the answer is documented here. Use `replace_all=true` for repetitive edits, read files in large chunks (limit=300+), and batch similar operations.

## Shared definitions -- the contract (read before changing a metric, filter or label)

**`docs/shared_definitions.md` is normative for every surface: SQL, R/Shiny, Plumber/React, R and Python ETL, both leagues, every tab.** It holds the canonical definition of each number and each control with its single owner, the ten rules derived from the 2026-10-12 drift audit, and the register of known deviations. Where code disagrees with it, the code is wrong unless the difference is in that register.

The rules in one line each (the document has the reasons and the checks):

1. **One definition, one implementation.** The database computes; R, Plumber and React present. Never re-derive a metric in an R SQL string or a second SQL file -- add a parameter to the owner.
2. **Catalogues live in tables.** Never type a label or a team name into code.
3. **One control, one meaning** on every tab, on the fast and filtered paths, in both leagues.
4. **No twin without a pin.** Two implementations that must agree get a test that calls both; an edit to one is not done until the other is edited or shown not to need it.
5. **Counts cross boundaries; rates are derived last.** Never rebuild a count from a rounded rate; round once, from the exact total (seconds, for time).
6. **Events follow credit; time follows the floor.** Aggregate each on its own key and stack them; never join events to time.
7. **EuroLeague reuses the Israeli implementation.** A difference in what a number means is never a league difference.
8. **Every layer that is not SQL is a consumer** -- ETL writes facts, Plumber renames, React displays.
9. **A check must be able to fail.**
10. **The fast path is an optimisation, not a second definition.**

Before changing anything: find the term in the document (add it first if missing), change its owner, list and update the twins, run the pins, and record any difference that must remain in the deviation register the same day.

## Project Overview

Basketball Israel Analytics — R/Shiny dashboard + React frontend for player on/off impact, lineup combos, team ratings. Data: play-by-play JSON → ETL → PostgreSQL (Supabase) → Shiny app / Plumber API.

**Live app:** https://arieltaieb-basketball-israel-analytics.share.connect.posit.cloud/ (Posit Connect Cloud since 2026-09. The old https://ibpl-stats.shinyapps.io/onoff-shiny/ is now a 308 redirect into Connect Cloud, so there is no shinyapps.io deployment left to compare against.)

**Tech:** R 4.4.2, Shiny (bslib/BS5), DBI/RPostgres (no dbplyr), PostgreSQL on Supabase (port 6543), schema `basketball_test`. React 19 + TypeScript + Vite in `frontend-v2/` with a Plumber/R API in `frontend-v2/server` (**deprecated**; the new API is `api/`, see Service layer and API). Phase 2 there: Tabs 1-2 complete, Tabs 3-4 stubs, Tab 5 Shiny only.

## Git Branching

**Only `main` is a permanent branch.** All others are short-lived and created on-demand.

Branch naming convention — prefix signals the area of change:
- `shiny/<name>` — Shiny UI/server (`app/`)
- `react/<name>` — Next.js app (`web/`); merging to `main` deploys Vercel Production
- `sql/<name>` — Materialized views / SQL functions (high-risk — verify MV rebuild order before merging)
- `etl/<name>` — ETL pipeline
- `infra/<name>` — CI workflows, scripts, deploy config

Workflow:
```bash
git checkout -b shiny/fix-filter-reset   # create branch
# ... work, commit ...
git checkout main
git merge shiny/fix-filter-reset
git push origin main
git branch -d shiny/fix-filter-reset
```

Tags (`backup/...`) are used for snapshots instead of long-lived backup branches.

**One-time setup** (required after cloning):
```bash
git config core.hooksPath scripts/hooks
```

## Commands

```bash
RSCRIPT="/c/Program Files/R/R-4.4.2/bin/Rscript.exe"
"$RSCRIPT" -e "shiny::runApp('app')"                    # Run Shiny app locally
# Deploy: push to main. Connect Cloud builds from GitHub via app/manifest.json.
"$RSCRIPT" -e 'rsconnect::showLogs(appPath="app", account="ibpl-stats", server="shinyapps.io", entries=1500)'  # live server log
"$RSCRIPT" -e "Sys.setenv(APP_ENV='test'); source('etl/etl_full.R'); etl_full()"  # Full ETL
cd frontend-v2 && npm run dev                            # Vite dev (port 5173, proxies /api → 3002)
cd frontend-v2/server && "$RSCRIPT" run.R                # Plumber API (port 3002)
```

## Architecture

### Shiny App (`app/`)

```
app.R                  Entry point — sources R/*.R, assembles ui/server
R/helpers.R            Pure helpers shared with the test suite (no side effects at source time)
R/global.R             Libraries, constants, DB pool, caches/guards, UI builders, canonical lookups
www/mobile.css         Viewport-scoped mobile presentation styles
www/mobile.js          Viewport mode and mobile interaction layer
R/logger.R             app_log() structured logging
R/mod_lineup_player_filter.R  Team/players-on/off filter module (Tabs 2, 7)
R/ui_tab{0-5,7}*.R     Tab UI definitions (0=Home, 7=Compare)
R/server_tab{1-5,7}*.R Tab server logic (receive shared list)
```

**Note:** `ui/server_tab6_team_stats.R` exist on disk but are NOT sourced in `app.R` (unwired; team stats live in Tab 5's display modes).

**Modular pattern:** `app.R` calls `server_tab*(input, output, session, shared)`. Shared list contains: `season_date_bounds`, `selected_game_year`, `teams_for_year_df`, `selected_opp_ids_on`, `selected_opp_ids_ld`, `data_version`, `pending_ld_team`, `pending_gl_team`, `pending_compare_preset`.

**Cross-session caching:** season-level MV pulls (Tabs 1, 4, 5) are shared across sessions via `GL_DATA_CACHE` — `cached_season_df()` helper or `bindCache()`, keyed on `game_year` + `shared_data_version(shared)` (ETL timestamp), so caches invalidate after each ETL run. Reference dropdowns go through four canonical lookups in `global.R` — `fetch_teams_distinct()`, `fetch_teams_min()`, `fetch_gn_values()`, `fetch_players_basic()` — one `cached_ref_query()` key per dataset per season, shared by all tabs and the prewarm (never write per-tab cache keys for the same data). `docs/shinyapps_worker_tuning.md` is shinyapps.io-era and no longer applies: Connect Cloud runs **more than one R process per container** (two `Shiny application starting` lines on one container is normal), so every in-process cache — `GL_DATA_CACHE`, `RANKED_CACHE`, `cached_ref_query`, `.UI_RESPONSE` — pays its own cold cost. Do not reason from "the worker".

**Test mocks:** `tests/testthat/helper-server-mocks.R` sources `R/helpers.R` (real implementations) and stubs only the impure pieces (`db_get_query`, `cached_ref_query`, `fetch_*`, guards, chip builders). Never copy a helper implementation into the mocks — put it in `helpers.R`.

**Session guardrails:** `guard_heavy_request()` (per-session rate limit + query-window caps), `statement_timeout` (`PG_STATEMENT_TIMEOUT_MS`, default 20s), idle-session timeout with client-side state restore (`APP_IDLE_CLOSE_SESSION`, **off by default since the Connect Cloud move**; 10-min timeout). Connect Cloud stops the container itself 5-8 min after last use — before that timer fires — and the resume reload then lands on a cold start, so the timer-driven pause is off and only the disconnect-driven one remains. `www/app.js` reads the mode from `IBPL_IDLE_CONFIG.closeSession`. `.Renviron` is credentials only, and on Connect Cloud it is **not deployed at all** — it is gitignored and Connect builds from GitHub, so every env var lives in the Connect Cloud dashboard (see § Posit Connect Cloud). `app.R` logs the resolved idle config at startup.

**DB security:** app connects as `app_readonly` (SELECT-only, EXECUTE on an explicit function allowlist, RLS enabled). Apply/audit via `scripts/apply_db_security.R` (dry-run by default) + `sql/security/*.sql`; contract tests in `test-db-security-contracts.R`.

**Global season selector:** Single `input$game_year` in navbar header. All tabs read from this — no per-tab season inputs. The EuroLeague section has its own navbar select (`input$euro_game_year`, provider season = starting year); its default is derived from `DEFAULT_GAME_YEAR`, so one bump rolls both leagues.

**Direct SQL (no dbplyr):** All DB access uses `DBI::dbGetQuery(pg_pool, ...)` with `$1, $2` params. `bigint = "numeric"` in `dbPool()`. The pool is `minSize = 0`. Measured 2026-08-18: `minSize` makes no difference to *steady-state* query latency (0.250s after a 22s idle gap either way) — don't "fix" it for that. But a *first* checkout on a booted worker costs ~1,700-2,200ms (TCP + TLS + auth + the `onCreate` SET), and with `minSize = 0` that always lands on a user request. Measured 2026-09-01: `minSize = 1` moves it to boot but costs +2.7s there against -1.7s on the request, so it loses whenever the worker is booted by the request it must then serve. `global.R` instead schedules a `later::later()` checkout right after the pool is created, connecting once R goes idle: boot time is unchanged and the connection is ready before the first session queries. Set `POOL_PREWARM=false` to disable.

**UI is built once per worker.** `enableBookmarking()` needs a function UI, so Shiny rebuilt all twelve tabs on every page load — ~3s for ~1MB of byte-identical HTML. `app.R` now builds it once (`build_ui()` → `.UI_CACHED`); `ui()` returns the cached value. Cold start 7.7s → 3.3s. **That cached the tag tree, not its HTML** — Shiny still re-serialised ~1MB on every request. Measured 2026-09-01: `renderTags` on the cached tree cost 6.59s on the first call (originally attributed to Sass compilation — that was wrong, see below) and ~1.2s on every call after, so a cold `GET /` took 10.5s and a warm one ~1.1s — the largest single item in a 22s cold Home load. `shiny:::uiHttpHandler` returns an `httpResponse` verbatim, so `app.R` now also caches the rendered page (`.UI_RESPONSE` / `ui_response()`) and renders it synchronously before `startServer` (it used to use `later::later(delay = 0)`, which runs ahead of the already-queued first request). Steady-state `GET /` 1.24s → 3.5ms (~300x, repeatedly measured — trust this one). **Re-measured 2026-09-02 (n=4/arm, fresh worker per run): cold Home cards ~8.4s, warm worker ~4.0s.** The original "22.2s → 9.6s" was taken while `C:` was 100% full — do not quote it. The first-render cost is `bs_theme_dependencies()` at 2.3-3.5s on its first call per process (~87% native file I/O — WALL 2,190ms vs Rprof SAMPLED 282ms), **not** Sass: the sass cache is verified *hit* (key count 205 → 205), and `sass` 0.4.9 already caches outside `tempdir()`, so a persistent sass cache is not an available win. Note the pre-render only helps a worker that has been idle long enough to finish it: start the app and load it immediately and the first request races it and loses (reproduced at ~41s launch-to-usable). **On Connect Cloud that gap never exists** — the process is started *because* a request is waiting, so both `later()` warmups always lost that race. Both now run synchronously before `startServer` instead, which moved 5.6-5.8s off the first request; see § Posit Connect Cloud. See `docs/home_cold_start_handoff_2026-09-01.md` § Corrections and § Session 2026-09-02. Output is byte-identical (verified: two *uncached* workers differ from each other in exactly the same per-worker random ids). `IBPL_CACHE_UI_HTML=false` drops just this layer. Safe here only because restore is server-side (`session$restoreContext` / `restored_input_value()`), never Shiny's UI-level `restoreInput()`. **Set `IBPL_CACHE_UI=false` whenever you drive the local app in a browser** -- with it on, selectize's assets 404 and every player/team dropdown looks broken for reasons unrelated to your change (see the lead in section Posit Connect Cloud). Likewise **Set `IBPL_CACHE_UI=false` while editing `www/app.css`, `www/app.js`, `www/mobile.css`, or `www/mobile.js`** — they're read by `includeCSS()`/`includeScript()` at build time, so otherwise an edit needs an app restart, not a browser reload. **Launch with Run App / `runApp()`, never select-all + Ctrl+Enter** — the latter builds the UI with no app context, so Shiny emits a BS3-style navbar (no `nav-link`/`nav-item`, so `app.js` never builds the tab hover menus) with an incomplete theme dependency (`bootstrap-5.3.1/font.css` 404s, so the fonts never load), and that broken build is then cached for the life of the process. Health check: the served page should contain a non-zero number of `nav-link` and `nav-item` occurrences (24 and 19 as of 2026-09-24 -- the count grows with tabs, so never test a fixed number) and every local JS/CSS asset it references should return 200; the `run-shiny-local` project skill (`.claude/skills/`) runs this check. Startup timing is instrumented client-side and lands in the app log as `[startup] ... client timing: nav->dom Xms | dom->connected Yms`.

**UI theme:** Dark editorial (bslib BS5), DM Sans + JetBrains Mono, amber accent `#e8a435`. Filter chips bar, loading skeletons, tab icons with active amber underline.

### Service layer and API (Game Logs is the first slice)

- `app/R/svc_*.R` is the shared service layer: pure functions that Shiny reactives and the API both call. `svc_game_logs.R` owns Game Logs for both leagues (`svc_game_logs()`, params via `svc_game_logs_params()`; league differences in the `GL_LEAGUE` descriptor); `svc_params.R` holds `api_error()` and the generic parsers. Tab 4 and Tab 11 are consumers. `etl_last_success_db()` (`global.R`) is the one data-version read.
- `api/` is the new Plumber API (`plumber.R`, `R/handlers_*.R`, `README.md`). Run: `PG_PORT=5432 ... "$RSCRIPT" api/run.R` (port 3010, 127.0.0.1; never 6543). Tests: `api/tests/testthat`. Feature ids are tracked by `api/coverage.R` (`API_DONE_PREFIXES`, now `GL`) against `docs/superpowers/specs/2026-10-19-api-mcp-feature-inventory.md`; a done section's ids must each be named in a `covers()` call.
- `options(ibpl.pg_pool = <pool>)` injects the pool into `global.R` (skips `dbPool`, `onStop`, prewarm); the API and tests use it.
- **Goldens:** `app/tests/goldens/` (`harness.R`, `presets/`, recorded `fixtures/`, `out/`) hold what the real Shiny server rendered per preset; `scripts/capture_goldens.R --tab game_logs` replays offline, `--record` re-records from the live DB (port 5432, nothing else querying). Replay is strict only for the queries Game Logs depends on (`GOLDEN_STRICT_READERS` in `harness.R`: the season pulls, scoreless games, both data versions, and the reference lookups for the preset seasons, named by the app function that issues them), so **re-record (`--record`) only when one of those queries or lookups changes**; a strict miss says so and names the command. Every golden also replays the whole session start-up (Home, Tab 3, the EuroLeague section), but a miss there is answered with zero rows and printed as a note, and Home's default team is pinned (`GOLDEN_HOME_TEAM_ID`), so a `DEFAULT_GAME_YEAR` bump, a catalogue-sync bot commit or an edit to an unrelated start-up query needs no re-record. A Game Logs dependency wrongly left off that list would get an empty answer and change a golden, so the check still fails closed (`test-golden-harness.R` replays every preset with the start-up fixtures deleted). Tests: `test-golden-*.R`, `test-svc-game-logs.R`; nightly `test-api-parity-live-db.R`. Known Game Logs deviations: `docs/shared_definitions.md` D24, D25.

### Shiny Tabs — Fast/Filtered Paths

All tabs: sidebar 3-col / main 9-col, FixedHeader, mobile collapse behind "Show Filters". Summary/Four Factors toggle per tab.

| Tab | Fast Path (MV) | Filtered Path (SQL) |
|-----|----------------|---------------------|
| 0: Home | — (navigation cards → tabs) | — |
| 1: On/Off Impact | `onoff_default_mv` / `player_advanced_stats_mv` | `onoff_compute()` / `four_factors_compute()` |
| 2: Lineup Data | — (always SQL) | `fetch_lineups_csv_v2()` / `fetch_lineups_four_factors_csv()` |
| 3: Team Ratings | `team_ppp_ratings_mv` / `team_four_factors_mv` | `get_team_ratings_dynamic()` / `get_team_four_factors_dynamic()` |
| 4: Game Logs | `mv_lineup_totals_by_day` + `final_schedule_mv` | — (direct MV queries) |
| 5: Player Stats | `player_traditional_stats_mv` | `fetch_player_traditional_filtered()` -> `get_player_traditional_from_games()`, or `get_player_traditional_custom_clutch()` inside a clutch window (Compare's player modes use the same helper) |
| 7: Compare | reuses tab 1/2/3/5 paths per compare mode | same, two-sided (A/B splits) |

### React Frontend (`frontend-v2/`) -- deprecated, superseded by `web/`

**Status:** Tab 1 + Tab 2 complete with live data. Tabs 3-4 placeholder stubs. Tab 5 Shiny only.

**Stack:** React 19 + TypeScript + Vite + TanStack Query + react-select. The `frontend-v2/server` Plumber API (port 3002) is **deprecated** and mirrors Shiny logic by copy; the new `api/` calls the shared service layer instead.

**Key components:** `OnOffPage.tsx` (Tab 1), `LineupsPage.tsx` (Tab 2), `DataTable.tsx` (sortable + CSV export), `HeatCell`/`ShotCell`/`FFCell` (visualization cells), `FilterDrawer.tsx` + `FilterChips.tsx` + `store.ts` (filter state via useReducer), `LineupModal.tsx` (lineup game log).

**Design reference:** `prototype.html` — single-file HTML/CSS/JS mock for all 4 tabs.

**Plumber API endpoints:** `/api/onoff/{summary,four-factors}`, `/api/lineups/{summary,four-factors,game-log}`, `/api/meta/{teams,players,game-numbers,last-updated}`. Hardening via env vars: CORS allowlist, optional API key, IP rate limiting, response cache (60s TTL).

**Rename functions:** `rename_onoff()`, `rename_lineup_summary()`, `rename_lineup_ff()` map SQL snake_case → TS camelCase. Single source of truth for the SQL↔React column contract.

## Key Tables & MVs

**Base tables:** `schedule`, `actions_clean`*, `full_rosters`, `possessions`*, `pws`*, `lineups_lookup`, `stints`*, `sub_lineups`, `subs`

(*) **Cold storage tables** — truncated after each ETL run. See Cold Storage section below.

**Catalogue tables (since 2026-10-12): `game_types` and `season_teams`.** They are the source of truth for game-type labels and for the team names the dropdowns show. The app does not read them at runtime: its UI is built before any database connection, so it sources a generated copy, `app/R/catalogues_generated.R`, which defines `STATIC_TEAM_ROSTERS`, `GAME_TYPE_LABELS` and `GAME_TYPE_CHOICES_UI` (`app.R` sources it ahead of `global.R`; tests reach it through `global_defs()`). **Never edit that file or re-type those constants: change the table row.**

- `game_types`: `game_type` PK, `label` (dropdown), `short_label` (chips, Game Logs), `sort_order`. Codes sharing a label are one UI choice (Winner Cup = 10 and 34) and must share a `short_label`.
- `season_teams`: PK `(game_year, team_id)` on the PBP/roster id, with the display name. Team names otherwise have no single home: `full_rosters`, `schedule_team_dict` and `final_schedule_mv` each spell them differently (2027: five teams differ, e.g. HAPOEL EILAT vs roster IRONI EILAT), and `schedule_team_dict` uses its two name columns the opposite way round in 2026 and 2027.
- `scripts/sync_catalogues.R` keeps everything in step: it inserts teams that `full_rosters` has and `season_teams` lacks (insert-only, roster spelling, needs `CONFIRM_CATALOGUE_SYNC=1`), regenerates the copy, and fails if `schedule` holds a `game_type` with no row in `game_types`. `.github/workflows/catalogue-sync.yml` runs it after every successful `ETL Full` run and commits the generated file when it changed -- a bot push to `main`, which deploys but does not trigger `r-tests`.
- So a new season's teams appear by themselves the night its rosters first load. What stays manual: bumping `DEFAULT_GAME_YEAR`, correcting a display name (update the row; the next sync carries it), and labelling a new provider game type (insert the row; until then the sync workflow fails and the code shows as a bare number).
- `test-game-types-db.R` and `test-season-teams-db.R` (DB-backed, nightly) fail when the generated copy and the tables differ, i.e. when the sync stopped landing.

The tables themselves were created by `scripts/apply_catalogue_migrations.R` (idempotent, dry-run by default, `CONFIRM_CATALOGUE_MIGRATION=1`), followed by a DB security re-apply.

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
- **Credit attribution (2026-10-12).** `df_pts_poss_lineups_longer_mv` has five `credit_*` columns (`credit_lineup_hash`, `credit_segment_id`, `credit_num_starters`, `credit_own_starters`, `credit_opp_starters`) next to the physical ones. A free throw is credited to the lineup on its parent action's row (the foul, or the made shot of an and-one), from the same team's same perspective; every other row to its own lineup. **Events (points, possession ends, counts, FT trips, starters filters) group on the credit columns; floor time, games played and Gameflow use the physical ones.** Filled by `refresh_ft_credit_fields_for_games(int4[])`, which `refresh_df_pts_poss_lineups_longer_for_games` calls, so the nightly ETL needs no change. `player_stats_actions_by_game` carries `credit_lineup_hash` too. Design, measurements and gates: `docs/superpowers/plans/2026-10-12-free-throw-foul-lineup-attribution.md`; `scripts/apply_ft_credit_migration.R`, `scripts/verify_ft_credit.R`; nightly test `test-ft-credit-db.R`.

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
| `get_team_traditional_dynamic` | 18 | Team box-score totals for Tab 3 Traditional, counted from raw actions; `p_side` = offense (own) or defense (opponents'). Canonical segment minutes outside a clutch window, event span inside one. Same pre-shot clutch margin as `get_team_ratings_dynamic` (FGA and possessions match it exactly under a clutch filter). `gp` counts a game when any action survives the filters, the same as that function's `games_played`. **Two paths since 2026-10-12:** outside a clutch window it sums per-game totals (`team_metrics_by_game_mv` for the box and possessions, the opponent's row for the defence side, `lineup_four_factors_by_game` for minutes -- the view's own `off_minutes` is rounded per game and must not be summed); inside one it counts action rows. `p_use_game_totals := FALSE` (19th parameter, testing only) forces the action path; `test-team-reader-paths-db.R` requires the two to return the same table. Full 2025-26 season: 4.4 s -> 0.2 s |
| `get_team_minutes_dynamic` | 23 | Team floor time under the shared game, clutch and starters filters; the single owner of the minutes behind Tab 3's pace. Measured segment time outside a clutch window, event span inside one, physical starters. Tab 3 used inline R SQL for this until 2026-10-12, which outside a clutch or starters filter returned the nominal game length (40 + 5 per overtime): pace was 73.89 for team 3 against 76.04 on measured time, because three 2026 games have truncated play-by-play. Pinned nightly by `test-team-minutes-db.R` |

**Fast-path gate rule:** SQL functions checking `p_start_date IS NULL` must also accept explicit full-season window (`make_date(year-1,10,1)` to `make_date(year,7,1)`) since React always sends dates.

**Architecture note:** SQL functions only `SUM` pre-computed columns from MVs — they don't recompute raw counts. Metric formula fixes go in the base MVs (`player_four_factors_by_game`, `lineup_four_factors_by_game`, `player_advanced_stats_mv`).

## Four Factors Metrics

Computed in MVs, aggregated by SQL functions. All rates × 100 in final output.

| Metric | Formula | Notes |
|--------|---------|-------|
| **TS%** | `pts / (2 × (FGA + personal-foul FT trips))` | Each trip counted once, in the lineup that drew the foul. An and-one FT hangs off the made shot, so it is not a trip. Same definition in every Israeli view since 2026-10-12 (the Traditional views used `FGA + 0.44 × FTA` before) |
| **TOV%** | `turnovers / total_poss` | |
| **OREB%** | `OREBs / (missed+blocked shots + missed last personal-foul FTs)` | |
| **FTR** | `all FTs / all FGA` | Includes tech/flagrant FTs intentionally |

**Color polarity:** Offense green-high (except TOV% red-high). Defense red-high (except TOV% green-high). Unranked (<100 poss) = gray/null.

**Estimated point impact:** FF deltas in Tabs 1/7 carry an italic `est. ±X pts` annotation = delta × `FF_IMPACT_WEIGHTS` (helpers.R: efg 1.45, tov −1.36, oreb 0.63, ftr 0.13 pts/100 per pp; league-fit, refit via `scripts/fit_ff_impact_weights.R`). Defense wording: "pts allowed". No summed impact column — it would duplicate the rating diff.

**On/Off Net RTG Diff tags (Tabs 1 and 8):** full history and the data behind every threshold in PROJECT.md § "Session Update (2026-10-05/06)".
- `3PT luck` -- `onoff_3pt_luck()` recounts every 3PA both teams took at the season league 3P% (`onoff_league_3p()`); tagged when removing that luck flips the sign. The user's model: 3P% is luck, 2P% is not.
- `small sample` -- padded diff = raw x `onoff_pad_shrink()` (`ONOFF_PAD_POSS = 4000` per side; the team term cancels, so shrink depends on possessions only). `onoff_sample_flag()` tags raw top/bottom 20% with shrink < 0.6x the table median. **Do not go back to a rank-crossing rule** ("top 10% raw, not padded"): padding barely reorders a table of regulars (Spearman 0.99), so it tagged 90th -> 85th percentile drift.
- The hover is not a `title`: the renderer writes `data-*` on `.onoff-net-tip` and `www/app.js` ("On/Off Net cell tooltip") draws `.onoff-tip`. That renderer is a `sprintf(paste0(...))` template -- a literal `%` must be `%%`, and a `//` comment kills the whole function (one pasted line; a test forbids it).
- Refit `ONOFF_PAD_POSS` once 2026-27 completes; the fit script was never committed. EuroLeague uses the Israeli X.

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

- **Tabs 1/8 (Shiny):** top 35% by ON Poss, `setup_onoff_auto_min()` in `helpers.R`. Lowers the default/hand-set bar; raises only to undo its own lowering, capped at the value it lowered from (`state$<slot>_base`). React `autoMinPoss()` is still lower-only.
- **Tab 2:** 150-row target on `total_poss` -- `auto_minposs_from_df()` in Shiny (shared with Tab 10), `auto_minposs_target_r()` in Plumber. Raises AND lowers. Computed on team/player-filtered data BEFORE the min_poss filter.
- **Auto/Manual:** Manual slider -> auto off. Filter change -> auto back on. An `updating` flag / `last_auto` value tells an auto-written slider value from a hand-set one.
- **Season switch must re-arm and recalibrate -- in both directions.** Fixed ea2feaa (2026-10-06). `DEFAULT_GAME_YEAR` is the current season, which is near-empty for its first months, so EVERY first visit lowers the bar (Tab 1: 300 -> ~70). Tabs 1/8 used to be lower-only ("never overrule a deliberately loose value"), which also stopped auto undoing *its own* lowering: switching to a full season kept the near-empty season's bar. A lower-only/raise-only guard must tell "auto wrote this" (`cur_val == last_auto`) from "user set this", and a raise must be capped at the pre-auto baseline so it never exceeds what a direct load shows. Reset clears the baseline. Test: `test-league-shared-helpers.R` "climb back when the season switches".
- **Re-arm on the season/competition reactive itself, not on the dates it moves.** EuroLeague date bounds depend on season only, so a date-driven re-arm missed EuroLeague <-> EuroCup switches (Tab 10, fixed ea2feaa). Tabs 2 and 7 re-arm on `input$game_year`.
- **Season switch runs auto twice.** The observer fires on `game_year` before `updateDateRangeInput()` reaches the browser, so the first pass queries new year + OLD dates. `fetch_lineups_all` filters on both, returns nothing, auto no-ops, and the date echo recalibrates. Correct, but one wasted lineup query per switch (Tab 2; not fixed).
- **`testServer()` does not apply `updateSliderInput()`.** An auto-min test must echo each auto value back with `session$setInputs()`, or the "is this auto's own value" check sees the stale input.

## Tab 2: Lineup Details

**Tabs 2 and 10 filter players with a chip roster (2026-09-22).**
`lineup_player_filter_ui(layout = "chips")` shows the team's players as surname
chips (ordered by season minutes, top 12 plus "+N more") under an
**On / Any of / Off** switch that sets what a tap does; tapping a chip already
in that mode clears it. The summary line spells the filter out and carries two
clickable tokens -- "with [at least|exactly] [k] of A, B, C" -- the quantifier
from 2 any-of players up, the count from 3 up (capped at n-1: all n is just
On). One selection can therefore say `A AND (exactly 2 of B, C, D) AND NOT E`.
**Marking a player "Any of" ADDS a clause, it does not relax one.** Tab 7
Compare keeps the three dropdowns (`layout = "inline"`) and always means "at
least one of".

**The three selectizes are still the model.** In the chip layout they stay in
the DOM, hidden (`.lineup-chips-model`), and the chips (`www/app.js`, "Lineup
player chips") read and write them, so bookmark restore, row pivots, the
filter-chip bar's clears and every server-side reader are unchanged. The roster
reaches the widget as the `lineup-chips-roster` custom message. The count and
quantifier are the only inputs the widget sets itself --
`<ns>players_on_any_min` and `<ns>players_on_any_exact`, parsed as untrusted by
`parse_any_min()` / `parse_any_exact()` and applied by
`lineup_on_predicate(any_min =, any_exact =)` through `p$player_any_min` /
`p$player_any_exact`. `lineup_player_filter_server(chips = TRUE)` turns off box
pooling (`lineup_box_pool()`), which only serves visible dropdowns and would
race a player moving between boxes. A new trigger or reader of the player
filter must list `players_on_any_min()` and `players_on_any_exact()` as well;
`test-lineup-player-chips.R` pins the call-site counts.

**Where the lineup player filter actually runs.** Get this wrong and a UI tweak
gets scoped as a `sql/` branch with a grant re-apply, which is what happened
once. The SQL params exist, but Tab 2 passes `NA` for both:

| Surface | Player filter runs in |
|---|---|
| Tab 2, Tab 10 | **R** -- `apply_local_lineup_filters()` (`helpers.R`) |
| Tab 7 Compare | **SQL** -- the only surface that sends player ids |
| React/Plumber | its OWN copy `apply_lineup_local_filters()` (`plumber.R:205`), a different function that `helpers.R` cannot reach -- and it has no count / "exactly" support |

Tabs 2/10 fetch every lineup at `min_poss = 0`, rank the full population, then
narrow in R. `fetch_lineups_csv_v2` has no "at least one of" predicate, so when
the any-of box is non-empty Tab 7 withholds the on-filter from SQL and
re-applies the shared helper to the result -- sound because `p_min_poss` filters
`total_poss`, which does not depend on which lineups survive.

**Adding a fourth box to `mod_lineup_player_filter.R`:** add it to the
`PLAYER_BOXES` vector (which drives clearing, the dropdown layouts' pooling and
the exclusion observers -- do not add pairwise observers), and add
`<name>_label = NULL` to `ui_tab7_compare.R`, whose inline layout supplies its
own labels; a box whose label argument defaults to non-NULL renders a stray
label and wraps that row. The observers need `ignoreNULL = FALSE` or clearing a
box never returns its players to the other pools. In the chip layout it also
needs a mode button, an entry in the JS `BOX` / `STATES` maps, and a chip-state
style in `app.css`.

- **Server-side ranking:** Plumber fetches ALL lineups (min_poss=0), computes PR ranks on full population, caches in `RANKED_CACHE` (game-level key), applies local filters (team/player/minPoss) via `apply_lineup_local_filters()`. Two-layer cache: `RANKED_CACHE` + `RESP_CACHE`.
- **TOTAL row:** Sum raw counts → derive rates (client-side). Pinned at top, not clickable, PR fields null.
- **Local state:** groupSize, teamId, playersOn/Off, minPoss, clutch params are `useState` in LineupsPage (NOT in shared FilterContext). Drawer Teams filter is for data; local Team dropdown is only for Players On/Off option pool.
- **Lineup click → modal:** JS onclick → `Shiny.setInputValue`. Resolves `sub_lineup_hash` → `lineup_hash(es)` via `sub_lineups`. Summary/FF branch for modal content.
- **PG array parsing:** `player_ids` comes as `{1,2,3}` text; vectorized `parse_pg_int_array_json()` → list column for proper JSON serialization.

## ETL

**Use `etl_full.R`** — base tables → sub-lineups → MV refresh → validation → cold storage purge. Logs to `etl/logs/`.

**Phases:** 1 (schedule) → 2 (actions/possessions/pws) → 3 (sub-lineups) → 4 (MV refresh) → 5 (validation) → 6 (meta) → **7 (cold storage purge)**

**Key points:**
- `fetch_israel_schedule()`: JSON fields are mixed-case (`GN`, `ExternalID`) — must explicitly map to lowercase DB columns. `upsert_by_like()` is case-sensitive.
- ETL needs write access (`etl/.Renviron` with postgres user), app uses readonly (`app/.Renviron`).
- Starters lineage: `extract_starters()` → `lineups_lookup` → `pws` → MVs.
- Incremental refresh: `refresh_sub_lineups_stats_for_games(int4[])`.
- New-game detection uses `etl_processed_games` table (not `actions_clean` which is truncated).

## Cold Storage

**Purpose (original):** Keep Supabase DB under 500MB free tier by exporting ETL-only intermediate tables to Parquet and TRUNCATing them after each run.

**That premise is stale.** Measured 2026-09-19: `pg_database_size()` is **3237 MB**. The 500 MB free tier has not bound for a long time, so "is this table worth 9 MB?" is no longer the right question to ask about cold storage. The mechanism still earns its keep on backup/restore cost, not on the free tier.

**Cold tables** (written in Phase 2, read in Phase 4, purged in Phase 7):
- `actions_clean` (~32MB), `possessions` (~36MB), `pws` (~58MB), `stints` (~6MB) — **~131MB total**

**`subs` is HOT since 2026-09-19** — promoted out of cold storage so the Shiny stint ribbon can read substitution evidence at *request* time (a cold table is empty between ETL runs, so the reader could never use it). ~9MB, ~79k rows. Its GRANT, RLS policy and `app_readonly` SELECT already existed. Migration: `scripts/promote_subs_to_hot.R` (dry-run by default, `CONFIRM_SUBS_PROMOTION=1` to apply).

**FKs into cold `actions_clean`** are registered in `HOT_FKS_INTO_COLD` (`etl/cold_storage.R`), and `cold_fk_drop_sql()` / `cold_fk_readd_sql()` build the statements Phase 7 runs around the TRUNCATE. A hot table referencing `actions_clean` blocks TRUNCATE, so its constraint is dropped first and re-added **NOT VALID** after: the referenced rows are gone by design, while rows a later ETL run inserts still validate, because Phase 2 writes them while `actions_clean` holds that game. Column order differs per constraint and is load-bearing — `lineups_lookup` references `(game_id, id)`, `subs` references `(id, game_id)`. **Add any new hot table with such an FK to that list or Phase 7 fails.**

**Files:**
- `etl/cold_storage.R` — `export_cold_table()`, `run_cold_storage_purge()`, `restore_cold_table()`
- `scripts/restore_cold_storage.R` — standalone restore script for full MV rebuilds
- `exports/cold/*.parquet` — cumulative Parquet files (gitignored, one per table with key-based dedup)

**How it works:**
1. Export each table to cumulative Parquet (merge with existing via key dedup + read-back verification)
2. Drop `lineups_lookup_actions_clean_fk` (lineups_lookup FKs to actions_clean)
3. TRUNCATE all 5 tables in one statement (handles inter-table FKs)
4. Re-add FK as `NOT VALID`

**FK constraint:** `lineups_lookup` → `actions_clean` via `lineups_lookup_actions_clean_fk`. Must be dropped before TRUNCATE and re-added after. Other FKs are all between cold tables (handled by joint TRUNCATE).

**Tracking:** `etl_processed_games` table (game_id PK, game_year, processed_at) tracks which games have been ETL'd — replaces the old `SELECT DISTINCT game_id FROM actions_clean` for incremental detection. Auto-backfilled on first run.

**shot_zones:** persistent per-shot corner-3 flags (3PT + coords only), written at end of Phase 2 from actions_clean, backfilled from parquet (`scripts/backfill_shot_zones.R`). NOT a cold table — never truncate.

**Restore (for MV rebuilds):**
```bash
"$RSCRIPT" scripts/restore_cold_storage.R    # loads all 5 tables from Parquet
```

**GH Actions:** Parquet files uploaded to `cold-storage/latest` release on CI. Workflow creates the release if needed.

## Environment

Two `.Renviron` files (gitignored): `app/.Renviron` (readonly), `etl/.Renviron` (write access). Format: `PG_HOST`, `PG_PORT=6543`, `PG_DB=postgres`, `PG_USER`, `PG_PASS`, `PG_SSLMODE=require`, `POOL_MAX=3`.

- Port 6543 = pooler (app/ETL), Port 5432 = direct (DDL)
- DDL uses same pooler host on port 5432 (not `db.<ref>.supabase.co`)
- `SET search_path` needs `SET LOCAL` in transaction on pooler

## Posit Connect Cloud

The app moved off shinyapps.io in 2026-09. Deploys are **git-backed**: Connect
Cloud builds from GitHub using `app/manifest.json`, so anything gitignored
(`app/.Renviron`, `app/renv/`, `app/renv.lock`) is simply absent at runtime.

**Read the live server log** — this is the highest-value diagnostic here, and it
still goes through the old shinyapps.io API:

```bash
"$RSCRIPT" -e 'rsconnect::showLogs(appPath="app", account="ibpl-stats", server="shinyapps.io", entries=1500)'
```

`entries` caps at 1500 (3000 returns HTTP 400). You get container start/stop
events, `Listening on`, and every `app_log()` line including the client
`nav->dom` timings.

**Every env var must be set in the Connect Cloud dashboard.** There is no
deployed `.Renviron` any more, so anything not set silently falls back to the
committed default. Required: `PG_HOST`, `PG_DB`, `PG_USER`, `PG_PASS`,
`PG_PORT=6543`, `PG_SSLMODE=require`, `POOL_MAX`. Optional, all with working
defaults: `PG_STATEMENT_TIMEOUT_MS`, `POOL_PREWARM`, `IBPL_CACHE_UI`,
`IBPL_CACHE_UI_HTML`, `IBPL_MOBILE` (defaults to true; false skips both mobile includes), `APP_IDLE_CLOSE_SESSION`, `APP_IDLE_TIMEOUT_SEC`,
`APP_IDLE_TIMEOUT_MIN`, `APP_IDLE_WARNING_SEC`, `APP_IDLE_CHECK_SEC`,
`APP_IDLE_STATE_TTL_HOURS`, `GL_DATA_CACHE_MAX_AGE_SEC`, `GL_DATA_CACHE_MAX_MB`,
`REF_CACHE_TTL_SEC`, `APP_LOG_LEVEL`, `APP_LOG_FILE`.

**Measured 2026-09-07, from the production log (n=8 restarts, 23 sessions):**

| | |
|---|---|
| container start -> `Listening on` | **2.0-3.9s** — boot is not the problem |
| client `nav->dom`, warm process | 1.6-4.0s |
| client `nav->dom`, fresh process | **10-20s** |

**Nearly every visit is the cold arm**, because the Connect Cloud runtime setting
"Idle timeout" is 5s — the platform minimum of a 5-60s range. Five seconds
after the last connection closes the worker is killed, taking `.UI_RESPONSE`, the
`bs_theme_dependencies()` warm-up, `GL_DATA_CACHE`, `RANKED_CACHE`,
`cached_ref_query` and the pooled connection with it. Raising that setting is the
cheapest available win and needs no code.

The cold penalty was **not** the page size. The ~1.1 MB HTML document is
identical in both arms, so it cannot explain a 1.6-4.0s vs 10-20s split; an
earlier version of this section claimed it was the real cost, which was asserted
without measurement and is wrong. What differed was server-side warm-up blocking
the first request: measured "Listening on" to first `GET /` answered was
**5.6-5.8s** (n = 3) while both warmups ran from `later::later(delay = 0)`, and
**0.20-0.31s** (n = 3) once they were moved ahead of `startServer`. Warm `GET /`
is 3-30ms either way. A `delay = 0` callback scheduled during sourcing runs on
the first event-loop pass — ahead of the request already queued — so "off the
boot critical path" was never true on a request-started worker.

On the `NS_ERROR_CORRUPTED_CONTENT` console errors, separate the established from
the guessed. **Established:** they happen only on a cold load; all five affected
files (jquery, 3x selectize, bslib-component-css) return 200 with the right
`Content-Type` on a warm worker; the failing responses carried *no*
`Content-Type` at all, which is an aborted request, not a 404 (a real Shiny 404
carries `text/html`); and it is one cascade, not many failures — whichever file
is aborted takes everything downstream with it, jquery first.
**Also established, do not re-derive it:** "blocked R starves the static assets"
is false. R blocked 15s: `GET /` took 14.5s while every dependency asset still
returned in ~1ms, because httpuv serves registered static paths on background
threads. **Not established:** *why* the requests abort. Connect Cloud's
interstitial does carry `setTimeout(function(){window.location.reload();}, 7000)`,
but it is reset by server heartbeats and explicitly `clearTimeout`-ed immediately
before the intentional reload, so it is not demonstrated that it ever fires during
the app page load. Treat it as a hypothesis, not the cause.

**Untested lead, measured locally 2026-09-20.** With `IBPL_CACHE_UI` ON (the
default) a local `runApp()` returns **404** for `selectize-0.15.2/selectize.min.js`,
`selectize.css`, `selectize-plugin-a11y.min.js` and
`bslib-component-css-0.9.0/bslib-component-css.min.css`; selectize never
initialises and every `selectizeInput` renders as a bare empty `<select>`. With
`IBPL_CACHE_UI=false` those same URLs return 200. Reproduced on `main`, so it is
not branch-specific, and consistent with the pre-render design: the page renders
before `startServer()`, which snapshots the static paths. It is **the same file
set** as the cascade above, but the symptoms differ (clean 404 here, aborted
response with no `Content-Type` in production), so treat it as a lead, not a
proof. Testing it on Connect Cloud is one dashboard env var.


**Do not quote local `runApp('app')` timings as production numbers.** A local
launch measured 13.7s against production's ~3s: a cold OS file cache plus, at
one point, a stray `rsconnect::writeManifest()` at the top of `app.R` costing
4.5s per boot and inflating `manifest.json` from 61 to 72 packages. Never leave
`writeManifest()` in `app.R`.

**Shiny's native bookmark restore is inactive here.** Across 23 logged sessions,
all 10 that carried a real bookmark URL (2.2-6.9 KB of `_inputs_`) logged
`restore context active=FALSE values=0 ... has_inputs=TRUE`. The app runs on its
own fallback, `request_restore_context()` in `helpers.R`, which rebuilds a
`RestoreContext` from the query string — that is why `tab=` is populated while
`values=0`. That fallback predates the migration, so this is **not** proven to
be a Connect Cloud regression, and it can no longer be compared.

The disconnect nodes the client hides (`#ss-connect-dialog`, `#ss-overlay`,
`.ss-gray-out`) are `shiny-server-client`'s and are served by Connect Cloud too
— verified against the deployed page 2026-09-07.

## ETL Scheduler

Production ETL is scheduled nightly in GitHub Actions via `.github/workflows/etl-full.yml` at `19:15 UTC` (22:15 Israel daylight time, 21:15 standard time). GitHub delays the scheduled trigger by ~2.5h, so runs actually start around 00:45 IDT. The former Windows Task Scheduler task was absent in the 2026-09-11 investigation, so do not rely on it as a fallback. Successful ETL writes `app_meta.etl_full_last_success`, which also versions Shiny's season caches; after a manual database aggregate refresh, advance that marker to invalidate cached results. See `docs/etl_clock_incident_handoff_2026-09-13.md`.

`workflow_dispatch` accepts an optional `game_ids` input (comma-separated, e.g. `398,399`) to force specific games through the pipeline regardless of `etl_processed_games`, bypassing the normal new-games-only diff -- e.g. `gh workflow run etl-full.yml -f game_ids=398,399`. Dormant for the `schedule` trigger (inputs are only read on `workflow_dispatch`), so nightly runs are unaffected. Threaded through `scripts/run_etl_full.ps1`'s `-GameIds` param into `etl_full(game_ids=c(...))`.

## Security

1. Never hardcode credentials — `.Renviron` + `Sys.getenv()` only
2. All Shiny client inputs are untrusted — hidden columns/controls are not a security boundary
3. Parameterized SQL only (`$1, $2` placeholders) — never `sprintf()`/`paste0()` for user values
4. Fail-closed on unexpected input

## Code Conventions

2-space indent, snake_case, parameterized SQL. Schema `basketball_test`.

### EuroLeague tabs REUSE the Israeli implementation — non-negotiable

Every EuroLeague tab has an Israeli companion: **8↔1** (on/off), **9↔3** (team
ratings), **10↔2** (lineups). Unless the functionality is *vastly* different,
**use the existing helper/function/module and adapt it to the EuroLeague
schema.** Never write a parallel `euro_` implementation of logic that already
exists — it doubles the surface that must stay in step, and it demonstrably
drifts (three EL tabs had already diverged three ways on identical dropdown
wiring).

- Before writing anything `euro_`-prefixed, find the Israeli function that does
  it and ask what actually differs. Usually only the schema name, the season
  convention, or an input prefix — all parameterisable.
- If shared logic needs a small generalisation to serve both, **generalise the
  existing function** (add a prefix / bounds / flag argument); don't clone it.
  The result goes in `helpers.R` or `global.R`, never `global_euro.R`.
- Name it neutrally — `apply_season_date_bounds(session, id, bounds)`, not
  `euro_apply_season_dates(session, id, season)`. The league belongs in the
  argument, not the name.
- A `euro_` prefix is justified only for the league dimension itself:
  schema-qualified queries, provider season convention, the competition
  dimension, phase-vs-`game_type`, round-vs-GN.

#### Direction: extract shared logic, do NOT merge the tab files

Decided 2026-08-10. The two leagues' tabs duplicate each other and the
duplication has caused real bugs, but the fix is **extraction into a league
descriptor plus helpers/modules — not one merged file per pair.**

Measured overlap against the Israeli counterpart, after normalising the
`euro_` prefix and the schema name:

| EuroLeague file | lines | shared with companion |
|---|---|---|
| `server_tab8_euro.R` (on/off) | 1306 | **81%** |
| `ui_tab8_euro.R` | 190 | 68% |
| `server_tab9_euro_team.R` | 536 | 34% |
| `server_tab10_euro_lineups.R` | 702 | 23% |

Tabs 9 and 10 score lower mostly because the EuroLeague versions are *smaller*
(no clutch, no shot profile, no FF impact annotations), not because the logic
differs in kind. Tab 8 is a near-clone of tab 1 and is the largest remaining
target.

Why not merge per pair: tabs 1/2/3 are live and working, `server_tab3.R` alone
is ~2,100 lines, and the `if (league)` branches would land inside the very file
the merge is meant to simplify. Zero user-visible gain for a large refactor of
working code.

The league differences are enumerable, so they belong in one descriptor: schema
name, SQL function names and signatures, season convention (Israeli
`game_year` is the season-ending year, EuroLeague `season` is the provider
season — a `+1` offset), GN meaning, phase text versus integer `game_type`, and
feature flags for clutch, shot profile, shot splits and FF impact weights.

Each extraction is **one byte-identical move plus a test run** — never a
rewrite bundled with a move. Verify a move by reversing the transform and
diffing against `HEAD`, not by writing new tests for moved code. Unifying
*code* never means mixing *data*: no ranked table mixes leagues, and cache keys
keep their league dimension.

#### What is already shared (do not re-clone these)

| Concern | Shared function | Where |
|---|---|---|
| Filter chip bar | `build_filter_chips()` | `global.R` |
| Chip clear observers | `setup_chip_clears()` | `global.R` |
| Team-ratings value/rank/delta cell | `fmt_rank_cell()` | `helpers.R` |
| Season date bounds on an input | `apply_season_date_bounds()` | `helpers.R` |
| Lineup team/players-on/off filter | `lineup_player_filter_server()` | `mod_lineup_player_filter.R` |
| Percentile rank vector | `pr_vec()` | `helpers.R` |
| Auto min-possessions | `auto_minposs_from_df()`, `setup_onoff_auto_min()` | `helpers.R` |
| Local lineup filtering | `apply_local_lineup_filters()` | `helpers.R` |
| Players-on set semantics | `lineup_on_predicate()`, `parse_required_ids()` | `helpers.R` |
| Per-box option pool | `lineup_box_pool()` | `helpers.R` |
| On/off DataTables | `onoff_summary_datatable()`, `onoff_four_factors_datatable()` | `helpers.R` |

`build_filter_chips()` takes the league dimension as arguments, every one
defaulting to the Israeli behaviour so no Israeli call site passes any of them:

- `season_value` / `season_label` — the season fed to the bounds function and
  the chip text. EuroLeague passes its own selector value and a
  competition-qualified label (`"EuroLeague 25-26"`); Israeli reads the global
  `input$game_year`.
- `date_input_id` — Israeli tabs use `<prefix>_dates` (Tab 1 is `date_range`);
  EuroLeague ids do not follow that pattern.
- `dates_show_when_set` — whether a resolved date range earns a chip on its own
  or only when it differs from the season bounds.
- `game_type_input_id` / `game_type_labeller` — EuroLeague's filter is
  `<prefix>_phase` holding provider text, labelled by `euro_phase_label()`;
  Israeli is `<prefix>_game_type` labelled by `GAME_TYPE_LABELS`.
- `gn_label` — `"GN"` (Israeli schedule game number) vs `"Rd"` (EuroLeague
  round). This is the round-vs-GN league dimension, not a style choice.

`setup_chip_clears()` takes `bounds_fn` for the same reason: it resolves the
date-clear target from `gy_input_id`, and passing a EuroLeague season to the
Israeli `shared$season_date_bounds` silently produced the wrong window.

Two conventions the shared builder assumes — align a new tab to them rather
than adding a special case:

- `""` is the blank sentinel for every single-select filter. Tab 10 used
  `"all"` and had to be changed; the SQL coerces `''` to `'all'` itself, so
  there was never a reason to differ.
- The clear-chip id is always `<prefix>_clear_<thing>`, and the prefix is the
  same one passed to `build_filter_chips()` and `setup_chip_clears()`.

## Pitfalls & Lessons Learned

### PostgreSQL / Supabase
- `REFRESH MATERIALIZED VIEW` re-runs stored definition — must DROP+CREATE to change query
- `DROP ... CASCADE` propagates — rebuild dependents in L2→L3→L4 order
- **Port 6543 breaks RPostgres when two clients query at once.** RPostgres sends every `dbGetQuery()` as prepare then execute, even with no params, and the transaction pooler can put another client's query between the two. Root cause of `bind message supplies N parameters, but prepared statement "" requires M`, `unnamed prepared statement does not exist` and `Multiple queries must use the same column names`. Measured 2026-10-12 with four clients as `app_readonly` (`scripts/pooler_concurrency_repro.R`): different statements on 6543 were 80-95% bad, about a quarter returning ANOTHER client's columns with no error; the same statement with different params was 983/1000 ok with no crossed values; port 5432 was clean in both; one client alone was clean in 720 queries (the single exception, 7 errors in 120 right after the transaction-wrapper experiment, is unexplained). `immediate = TRUE` avoids it but takes no params. `db_get_query()` is exposed. **The live-DB tests are not flaky: never run them while a local app, another DB script or a second test run is querying as `app_readonly`.** Not fixed -- see Backlogs.
- **A new aggregation over `df_pts_poss_lineups_longer_mv` must group EVENTS on `credit_lineup_hash` / `credit_segment_id` (and the `credit_*` starters), not the physical columns.** A segment changes whenever either team substitutes, and about a third of free-throw trips have a substitution between the free throws; grouping events physically counts those trips twice and splits a trip's points from its possession. Before 2026-10-12 this put the stored Four Factors TS% about 1.8 points low (2026: 55.24 vs 57.01). Time is the opposite: `segment_seconds` and event-span minutes must stay on the physical columns. When one CTE feeds both, carry both sets of keys.
- **Games played must not be derived from a credited join.** A player who came on only for the later free throws of a trip has floor presence but no credited possession; `poss_end` in the player traditional sources therefore carries `floor_lineup_hash` (for `gp`) and the credited `lineup_hash` (for `poss_on_floor`).
- **A bare column name inside a plpgsql `RETURNS TABLE` function is ambiguous with the output column of the same name** (`SELECT player_id ... USING (player_id, team_id)` failed at run time, not at CREATE). Qualify it and join with `ON`. After deploying a function, call it once before moving on.
- **`player_four_factors_by_game` carries partial rows for players who were not on a game's roster.** Its refresh function joins `lineups_lookup` without `game_id`, so a lineup that recurs in another game or season gives OFF rows to that game's non-roster players (2,897 of ~18,000 player-games on 2026-10-12). Their ON+OFF sums never reach the team's, so compare this table to team totals through the best-covered player, and expect a few such rows to appear or vanish whenever an old game is re-refreshed. Not fixed.
- **A CTE that feeds several per-team aggregates can be re-run once per team.** `get_team_traditional_dynamic` collected a season's actions in one CTE (260k rows) and joined five aggregates of it. The planner estimated that CTE at 1 row, joined the aggregates with nested loops, and re-executed four of them 14 times each, sorting on disk every time (server `work_mem` is 3.5 MB): 3.6 s of a 4.4 s call. `AS MATERIALIZED` on each aggregate CTE halved it; raising `work_mem` added nothing. Look for `loops=14` on an aggregate node in `EXPLAIN ANALYZE`. A LANGUAGE sql function shows only a Function Scan, so explain its body with the parameters written in as literals. A test on parameters alone (`WHERE p_max_margin IS NOT NULL OR ...`, no column) becomes a one-time filter, which is how one sql function can hold two paths and run only one.
- **Tab 3's rank arrows compare against the last league game day before the latest one**, by calling the same stored reader again with that end date -- in every mode, with or without filters, as EuroLeague's team tab does. Until 2026-10-12 the default state used inline R SQL that rolled each team back to before its own latest game (a table that never existed), so adding a Home filter or switching Traditional to Defence silently changed what the arrow meant; the two definitions disagreed for 10 of 14 teams on 2026-27 points per game. Those blocks also gave OREB and DREB no arrow.
- **After changing a materialized view the nightly ETL refreshes, time the definition under the ETL's limit.** The ETL role's `statement_timeout` is 2 minutes; the 2026-10-12 credit migration built its views under a 30-minute one and never checked. `mv_lineup_totals_by_day` then stopped finishing at all (over 10 minutes): its "slices without offense" step joined two CTEs and ran a NOT EXISTS over a third, the planner estimated the join at 2 rows and chose a nested loop over the whole CTE (about 170k x 85k). The first ETL run with a game to process failed in Phase 4. Fixed by reading both outputs off one join with a window count (6 s, row-identical); `scripts/restage_matview.R` swaps such a rebuilt view in after proving it equals the live one. A CTE has no index, so a nested loop over one is always quadratic: look for `Nested Loop` above a `CTE Scan` in `EXPLAIN`. A rolled-back call of each refresh function does not cover this -- `REFRESH MATERIALIZED VIEW` has to be timed on its own.
- `ANALYZE;` without table fails on Supabase — scope to specific tables
- `score` column from raw JSON is unreliable — use `own_team_score`/`opp_team_score`
- `segment_id` repeats across games — always include `game_id` in GROUP BY
- Floor time: collapse the perspective IN the segment GROUP BY -- key on `(game_id, team_id, lineup_hash, segment_id)` with `type_lineup` absent, take `MAX(segment_seconds)`, then SUM with **no** offense filter. Attach the result to the offense output row once (`CASE WHEN type_lineup = 'offense' THEN ... END`); that is where the single-count guard belongs. `player_traditional_stats_mv.segment_times` is the reference implementation. **Do not filter the sum on offense being present** -- it looks like double-count protection but a segment with no offensive possession then contributes zero, which cost 0.586 min/team-game across 92% of team-games until 2026-09-05. See `docs/unattributed_floor_time_2026-09-05.md`.
- Canonical segment boundaries use a running maximum of raw event elapsed time ordered by action ID within each game/team. Keep raw elapsed and regression fields for auditing; do not count a backward clock jump again when it catches up. Keep `sql/functions/refresh_segment_clock_fields_for_games.sql`, `sql/materialized_views/df_pts_poss_longer.sql`, and the 2026-09-12 monotonic-clock migration aligned. Games 398 and 399 exposed the bug; see `docs/canonical_clock_minutes.md`.
- Games 398/399 also had a SEPARATE, since-fixed defect: a provider dead-ball substitution flurry right after a period start was stamped with the clock from near the periods END, not its start (unrelated to the monotonic-clock fix above). Corrected via a per-id lookup table, `KNOWN_CLOCK_STAMP_CORRECTIONS` in `etl/etl_onoff.R`, applied in `clean_actions()` before quarter-clock fields are derived -- generalizes the earlier one-off game-381 filter into a reusable, guarded pattern (a row is a no-op with a warning if the feed no longer shows the expected wrong stamp). See `docs/game_398_399_misclocked_clock_fix_plan_2026-09-14.md`.
- Clutch CTEs: propagate `team_id` through all CTEs + always use table aliases (avoid PL/pgSQL variable ambiguity)
- `fetch_lineups_all.sql` and `fetch_lineups_four_factors.sql` have near-identical clutch structures — keep them in sync
- **The two lineup readers must return the same possessions, points and minutes for the same arguments**; `test-ft-credit-db.R` pins it nightly. They had drifted four ways by 2026-10-12, each one a copy of an expression edited in one file only: (1) Four Factors wrote the clutch starters filter per side (`type_lineup = 'offense' AND ... OR type_lineup = 'defense' AND ...`), which kept only offense and defense rows even with no filter set, so substitution and timeout rows (no `type_lineup`) fell out of the stint span -- 611 clutch minutes against Summary's 704; (2) that form also made "5 starters" keep every defensive row; (3) both counted a clutch segment's time only if it held an offensive action (22.6 of 722.6 minutes); (4) `ROUND(SUM(per-game minutes), 1)` lands a hair under a half-tenth boundary and shows 0.1 less than `ROUND(SUM(seconds) / 60, 1)` -- always round the second total. In the clutch path events (credited lineup) and time (physical lineup) are aggregated separately and stacked with `UNION ALL`, never joined, and the starters filter is applied to each on its own columns (`event_ok` / `time_ok`).
- Last-N-games filters: use the `schedule_ranked` windowed CTE pattern (all seven app functions do since 2026-07-27) — never a correlated per-row subquery
- DROP FUNCTION wipes `app_readonly` EXECUTE grants — after deploying functions (`scripts/deploy_sql_functions.R`), always re-run `scripts/apply_db_security.R` with `CONFIRM_DB_SECURITY_APPLY=1`
- App functions carry `SET plan_cache_mode = force_custom_plan` — the plpgsql generic-plan cliff was reproduced live (5s query → >120s timeout); keep the setting on new heavy functions

- `TRUNCATE` on a parent table fails even if child is empty — FK existence alone blocks it. Must drop FK, truncate, re-add, or truncate all tables in one statement
- Cold storage tables (`actions_clean`, `possessions`, `pws`, `stints`, `subs`) are empty between ETL runs — don't query them in the app
- Supabase dashboard reports ~30-40 MB more than `pg_database_size()` — account for this overhead when sizing against the 500 MB free tier
- Dropped redundant indexes (2026-03-18): `sub_lineups_team_id_lineup_hash_lineup_id_key` (12 MB, uniqueness covered by PK via `sub_lineup_hash = md5(lineup_id)`), `idx_sub_lineups_lineup_hash` (1.5 MB), `idx_sub_lineups_gin_players` (872 KB), `lineups_lookup_lineup_hash_idx` (2.2 MB). Do NOT recreate these.

### R / Shiny / DT
- `bigint = "numeric"` in `dbPool()` — integer64 breaks dplyr `coalesce()`, `+`, many tidyverse ops. `SUM(integer)` → bigint
- `dateRangeInput` NA pitfall: `updateDateRangeInput()` with `start` outside `min` → NA. Guard with `is.na()` checks
- DT JS render `row` guard: `if (type !== 'display' || !row) return data;` — prevents TypeError crash from `filter = "top"` init
- `server = TRUE` in `updateSelectizeInput`: only use when setting `choices`, not just `selected`. Clear tags first on multi-select team switch
- `formatRound()` clobbers JS `columnDefs` render — do all formatting in JS
- `uiOutput`/`renderUI` causes NULL on startup — use static inputs + `update*Input()`
- `tags(...)` is invalid — use `tags$p(...)`, `tags$div(...)`. `htmltools::dataURI` not exported — use `base64enc::dataURI`
- `bindEvent()` must include GN reactive in triggers or GN changes won't re-run computation
- **Never put a data reactive in an `observeEvent()` trigger expression.** The trigger is evaluated on **every session**, and observers — unlike outputs — are never suspended by tab visibility. Tab 10 had `euro_ld_full()` in its auto-min-poss trigger and so pulled a whole EuroLeague season (2,630ms) on every *Home* visit, ahead of Home's own query. Keep only plain inputs in the trigger and gate the handler with `req(identical(input$main_tabs, "<tab>"))`, as Israeli Tab 2 does. Fixed 4487c2f.
- **A choices-populating observer that reads a lazily-loaded ref has a race.**
  `lineup_player_filter_server()` fills its player boxes from
  `observeEvent(input$team)`, which reads `players_ref` at that instant and has
  no dependency on it changing. Tab 7 loads the Lineups-mode roster from the
  `cmp_mode` observer (`server_tab7_compare.R:1747`, `ignoreInit = TRUE`) with
  `refresh_player_inputs = FALSE`, so a team selected before that load lands
  leaves all three boxes **permanently empty** -- nothing re-refreshes them.
  The real click order (Compare -> Lineups -> team) avoids it, so it is latent,
  not user-visible. **Unfixed as of 2026-09-21.** It also makes Compare
  impossible to drive from a script unless you wait for the roster first.
- **A custom message outruns the input updates sent in the same flush.**
  Observed 2026-09-22: the chip widget's `lineup-chips-roster` message
  (`session$sendCustomMessage()`) reached the browser before the restored
  selections that `updateSelectizeInput()` had queued in the same call, so the
  first render saw an empty group -- and clamping stored client state on that
  render silently turned a restored "at least 2" into 1. Keep what was asked
  for and clamp only what is shown or sent; never derive persistent client
  state from inputs updated in the same flush as the message.


### Deploy Scripts
- `DROP FUNCTION` signature must be exact (param count must match) — verify against actual CREATE signature
- Long `Rscript -e` segfaults — write to temp .R file
- MV DDL: `readLines()` + `paste(collapse="\n")`, strip comment header, execute as single string
- `$function$` boundary: find end with `grep("^\\$function\\$;$")`
- Editing a file (`Edit` or similar tools) can silently normalize its WHOLE line-ending convention (mixed CRLF/LF files in this repo -- `app.R`, `global.R`, `etl_onoff.R`, `run_etl_full.ps1`, `CLAUDE.md` all mix them), turning a small real change into a 100+ line spurious diff. `cat -A` in Git Bash is not a reliable detector -- it can display no `^M` even where `xxd`/`tr -cd '\r'|wc -c` prove CRLF is actually present (Git Bash's own `cat` silently translates on display). Fix: extract the pristine committed blob (`git show HEAD:<file>`), splice the change in via `perl -0777` on raw bytes (`\Q...\E` literal match), and verify with `tr -cd '\r'|wc -c` arithmetic (orig CR + added-block CR - removed-block CR = spliced CR) before installing and `git diff --stat`.
- Two more line-ending traps (2026-10-06): (1) `awk '/\r$/'` in Git Bash also reports CRLF lines as LF -- `patch` then fails hunks with "different line endings". Splice with a `\r?\n`-tolerant perl pattern that reuses the matched EOL. (2) To commit ONE hunk of a file that also holds the user's uncommitted edits, build the blob from `git show HEAD:<file>` + the splice and stage it with `git hash-object -w --no-filters` + `git update-index --cacheinfo`. Without `--no-filters`, autocrlf rewrites every line (a 1-line change became 1,268). A dirty file also blocks `git checkout main`; when the merge is a fast-forward, `git branch -f main HEAD && git checkout main` moves the ref without touching the working tree.

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

## Shot Clock Derivation

**No native shot clock in PBP data.** Can be derived from `end_game_seconds_remaining` in `df_pts_poss_lineups_longer_mv`.

**Approach:** Possession start = previous possession's `final_end_poss` action time (`prev_end`). Shot clock = 24 - (poss_start - poss_end). OREB resets to 14s; defensive foul when clock < 14 resets to 14s (FIBA rule). Existing turnover type `24-seconds-violation` in `parameters_type` validates the derivation.

**`type_lineup = 'offense'` action ownership:** Actions in the offense perspective are not always *by* the offensive team. Defensive actions (`deflection`, `steal`, `block`, `foul`) with `type_lineup = 'offense'` represent actions done *to* the offensive team by the defense. Only these are genuine offensive-team actions: `shot`, `turnover`, `freeThrow`, `assist`, `foul-drawn`, `rebound (offensive)`.

**Limitations:**
- PBP timestamps are whole-second precision — expect ±2-3 sec noise (dead-ball/inbound time between possessions)
- PBP doesn't log passing/dribbling, so many possessions only record the ending action — using `first_action` as start massively underestimates duration
- `prev_end` baseline is the most reliable proxy despite slightly overestimating (includes dead-ball time)
- ~6-8% of possessions show negative clock values — mostly dead-ball imprecision, not real violations

**Working SQL query:** `tmp_shotclock.R` (not committed) — iterates all possessions for a game with ID-based boundaries, defensive action filtering, and OREB/foul reset logic.

## Backlogs

**Security/Resilience:**
1. Click burst guard for lineup modal (~300ms)
2. Vendor Google Fonts + bootstrap-icons into `www/` (or add SRI) — currently loaded from CDNs without integrity hashes
3. **Cold-storage backup is not cumulative -- the only full archive is one local disk.** The `cold-storage/latest` release holds just the last CI run's increment: verified 2026-09-19 as 3 games (401, 404, 406) at 14-90 KB per table, against 445 games and 6.3 MB in local `exports/cold/`. The "cumulative Parquet with key-based dedup" above describes that local folder, which is gitignored; CI runs in a fresh workspace and overwrites the release instead of merging into it. Lose that disk and ~140 MB of truncated ETL intermediates (`actions_clean`, `possessions`, `pws`, `stints`, `subs`) is recoverable only by re-fetching every game's PBP. Fix: have `.github/workflows/etl-full.yml` download the existing release assets and merge before upload.
   Four games are in neither source: `393`, `397`, `399`, `400` (DB has 451, local parquet 445, release adds 401/404/406). 393 postdates the local snapshot; 397/399/400 predate it and should have been exported, so an export was skipped or rolled back -- note 398 IS present while 397 and 399 are not, which lines up with the failed-run window in `docs/etl_clock_incident_handoff_2026-09-13.md`, but that link is unproven.

(Done: `statement_timeout` guardrail — 20s via `PG_STATEMENT_TIMEOUT_MS`; Tab 4 MV cache — `GL_DATA_CACHE`; per-session rate limit — `guard_heavy_request()`.)

**Scalability (Shiny):**
1. ~~Apply shinyapps.io worker settings~~ — obsolete, the app is on Connect Cloud
2. If concurrency still hurts: `ExtendedTask` + `promises`/`mirai` for slowest filtered-path queries

**Data:**
1. **2025 State Cup has no play-by-play.** `game_type = 35` is the State Cup: 7 games a season (4 QF, 2 SF, 1 final), and the only games with a NULL `gn` — correct, a cup bracket has no league game number. The 2026 seven (ids 291-309, final = 309) were processed; the 2025 seven (ids **737922-738725**) have **zero rows in `etl_processed_games`** and so no segments, actions or possessions. They sit in `schedule` and `final_schedule_mv` with final scores, flow into the schedule-derived team MVs, and are invisible in every on/off, lineup and minutes view. `pbp_link` is empty for all 14 cup games including the processed ones, so that column is not the fetch route — see `etl/run_state_cup_final_etl.ps1` and `docs/state_cup_final_309.json.md` for how 2026 was done. Seven games of playoff-calibre data.
   Note the 2025 cup ids come from a different provider space (normal range is 23-64942). Harmless for display — both game-log tabs sort on date first and use `game_id` only as a same-date tiebreak — but anything keying or ordering on `game_id` alone will place them after every other game.
   They are also the 14 team-games that `verify_minutes_migration()` skips as "absent from both sides"; if they are ever backfilled that exemption should stop finding them.

**Performance (React+Plumber):**
1. Profile SQL functions with `EXPLAIN (ANALYZE, BUFFERS)` for filtered cases
2. Audit indexes only after query-plan evidence

**Shiny (added 2026-09-22):**
1. **Tab 7 Compare lineup filter: add the count and "exactly".** Tabs 2/10 have
   "at least / exactly [k] of" (see the Tab 2 section); Compare still shows three
   dropdowns and always means "at least one of". The filter half is cheap: when
   the any-of box is non-empty Compare already withholds the on-filter from SQL
   and re-applies `apply_local_lineup_filters()`, so it only has to pass
   `player_any_min` / `player_any_exact` through. The open question is the UI --
   the inline layout has no summary line to put the tokens in.
2. **EuroLeague phase-list query errors at startup.** A local run on 2026-09-22
   logged `[WARN] [db_perf] ... SELECT DISTINCT phase FROM
   euroleague.final_schedule_mv WHERE competition = $1::text AND game_year = ...
   status=error` (`sql_ms=NA`) during session start. Not investigated: unknown
   whether it is a bad parameter (e.g. a season not yet resolved), a grant, or a
   timeout, and whether production logs it too. The query is
   `euro_fetch_phases()` (`global_euro.R:119`), cached per competition+season.

**Shiny (added 2026-10-11):**
1. **EuroLeague Lineups takes ~3.5s from tab click to first row** (local, warm
   caches, n=3: 3.5 / 3.5 / 5.1s). Not broken down. The tab pulls every unit
   for the season at no possession minimum (1,965 units for 26-27), ranks the
   full set in R, then filters and renders. Profile the SQL, the ranking and
   the DT render separately before changing anything, and read production's
   log for the same tab -- local timings are not production numbers.
2. **No loading indicator on EuroLeague Lineups.** The table area is blank
   while it works. Other tabs have skeletons.
3. **Clicks in the first seconds of a session wait behind startup.** One local
   session logged `server modules initialized (9.360s)` and `prewarm complete
   for season 2027 (10.420s)`; a tab clicked 1.5s after connecting waited for
   both (that is where a "13-15s" first visit came from -- not the query).
   The same process's first session initialised in 2.4s, and local runs have
   the UI cache off, so measure this in production before treating it as real.
   Note the prewarm is the ISRAELI season's even when the visitor is on
   EuroLeague.
4. **Gameflow row buttons do not show in the touch quarter cards.** The pinned
   gutter copy has no `data-clip`; tapping a row still selects.
5. **Navbar is two rows at 1280px** for every update text, including none.
   Menus work (stacking fix); it is only tall.
6. **Top scorer qualifier is a fixed 3 games.** The user chose to label it
   ("Top scorer (min 3 games)") rather than scale it. If it comes up again:
   scale `HUB_TOP_SCORER_MIN_GP` with the team's games played (e.g. half).
7. **Team Insights** is unfinished, on the local branch `shiny/team-insights`
   only. It fails six tests as-is (literal colours; `team_insights_entry_ui`
   missing from the Home UI test). Do not merge until the user says so.

**Reliability / scaling (added 2026-10-12) -- do these in order, after the schema/code drift tasks:**
1. **Move the app and the DB-backed tests off port 6543.** See the port-6543 pitfall above. Plan: read Pool Size in the Supabase dashboard (session mode on 5432 caps clients at Pool Size per role; default believed 15, never read; Postgres `max_connections` is 60), then switch `PG_PORT` to 5432 for the tests and a local app first, then in the Connect Cloud dashboard and the GitHub secret. Consider `POOL_MAX=1`: an R process runs one query at a time. A BEGIN/COMMIT wrapper around each query did NOT cleanly fix it in one (confounded) run -- do not assume it does. Client-side parameter interpolation is the fallback and conflicts with the parameterised-SQL rule.
2. **Re-run the 20-user stress test** (`docs/launch_stress_test_2026-09-10.md`) on the new port. It ran through 6543; whether its four stuck-busy sessions were this defect is unknown.
3. **Extend the stress test to 50 users** and only then decide on architecture. The 20-user run showed query latency flat (median 5.4-5.6s) and startup degrading (nav p90 9-11s at 5 users, 35-42s at 20). Capacity comes from the number of R processes, which Connect Cloud does not document; the already-listed `ExtendedTask` step is the incremental answer if one slow query blocking a process is the limiter.

**Free-throw credit attribution (added 2026-10-12):**
1. **Clean up after one nightly ETL has run on it.** Run `scripts/verify_ft_credit.R --check-base` (all four G1 checks must be 0), then `scripts/apply_ft_credit_migration.R --cleanup --apply` and `scripts/verify_ft_credit.R --drop`. Until then about 400 MB of `__ftc_old` views and the `ft_credit_migration` baseline schema stay in the database as the rollback copy.
2. **EuroLeague still credits a trip with its last free throw.** No double count there, but not the Israeli rule (lineup that drew the foul). Aligning it is an open decision.
3. **Gameflow disagrees with on/off on re-credited free throws by design** (about 3.5% of points): the ribbon shows who was on the floor when each point was scored.
4. ~~Clutch lineup readers dropped events and time~~ -- fixed 2026-10-12, see the lineup-reader pitfall. Left over: `sub_lineups_stats` holds 4 five-man rows for 2026 with zero possessions and zero minutes whose lineup no longer exists in the base table (its refresh upserts and never deletes). They show only in Tab 2 Summary at a possession minimum of 0. Not deleted.
7. **SHELVED 2026-10-12: player readers do not return their true-shooting attempt count.** The multi-team TOTAL row (Tab 5) and the usage fallback recombine rows by attempts. The app side is done: `ts_attempts()` (`helpers.R`) sums a `ts_att` column when a reader returns one and otherwise falls back to `ts_implied_attempts()`, which recovers attempts from the row's own TS% and so loses a scoreless row's missed free-throw trips (a 0-point row with ten missed trips plus a 10-point row on ten attempts shows 50%, not 25%). No reader returns `ts_att` yet, so today's behaviour is unchanged. Remaining, Israeli: append `ts_att` (= the `ts_poss_count` each already computes) to `player_traditional_stats_mv` (a staged rebuild) and to `get_player_traditional_from_games`, `_dynamic`, `_custom_clutch` (DROP + CREATE, then the security re-apply). Remaining, EuroLeague: the same column on `euroleague.player_traditional_stats_mv` and its three readers (`_pergame`, `_standard_clutch`, `_custom_clutch`), as a numbered gated migration plus `euroleague_function_contract.py`. Tests waiting in `test-ts-trips.R`.
5. **No data-quality-report finding for NULL credit columns.** The nightly `test-ft-credit-db.R` covers it; a finding in `etl/run_data_quality_report.R` would surface it in the report as well.
6. **The non-roster rows in `player_four_factors_by_game`** (see Pitfalls) predate this work and are a separate fix: join `lineups_lookup` on `game_id` in its refresh function, as `compute_player_traditional_by_game` already does.

**Architecture (accepted, trigger-gated):** when a tab goes React+Plumber-only, retire its mega-signature SQL function(s) and let the route build the SQL — see `docs/adr_api_owns_query_construction.md` for trigger, migration order, and the DB-role tightening that follows. Until then the stored functions are the shared source of truth for both frontends; do NOT duplicate their logic into R query builders.

## Session Update (2026-09-21): Period Anchors, Game 406, and Gameflow

### Anchor/ribbon conclusions

- The original anchor reprocessing population was selected by period-opening
  gaps, not by the strict substitution-straddle condition. Do not infer that
  Workstream A is complete from its original checklist alone.
- The strict-inside audit (`sub.elapsed < segment_end`) showed game 100 was an
  endpoint false positive. Game 62452 is the sole substantive omitted case,
  and is not anchor-repairable: its Q4 feed sends five players OUT at 10:00
  without declaring the incoming five.
- Game 404 is the clean anchor success. Game 406 was a separate provider
  defect--mislabelled periods plus an unusable Q4 clock--and required a guarded
  game-specific correction. See the 2026-09-19 correctness plan and the
  ribbon Workstream A-C/D handoffs for the complete classifications.
- `subs` is now hot. The strict-inside ribbon health signal remains for genuine
  lineup uncertainty, but game 406 overrides its technical 50-second message
  with the more relevant Q4 timing disclosure.

### Game 406 correction and database state

- The provider's Q3 is the real Q2; its Q4 contains the real Q3 followed by
  Q4. Real-Q4 actions are frozen at 00:00/00:01, so exact Q4 time cannot be
  recovered from the feed.
- Commit `2c23b94` relabels Q3 -> Q2, splits provider Q4 at the verified action
  boundary, repairs the Q2 opening reset stamps, and estimates Q4 positions
  linearly from wall-clock timestamps. The estimate preserves order, but clean
  controls showed normal errors around 10-20 seconds and occasional larger
  dead-ball drift.
- A scoped `etl_full(game_ids = 406)` write completed successfully. This was a
  database change. Final/quarter scores reconcile to 99-79 and
  28-12 / 26-24 / 19-18 / 26-25; all four periods appear once; both ribbon
  perspectives have zero excluded gameplay segments.
- Cold Parquet initially retained deleted marker ids 4060238/4060239 because
  its merge is key-upsert based. The rows were validated, backed up under
  `exports/cold/backup-2026-09-20-game406-stale-markers/`, and pruned from
  cold `actions_clean`, `possessions`, and `pws`.
- Evidence and reproduction are in
  `docs/game406_wall_clock_reconstruction_report_2026-09-20.md`,
  `scripts/report_game406_wall_clock_fix.R`, and
  `scripts/prune_game406_stale_cold_markers.R`.
- A genuine team-14 Q2 opening-reset gap of about 50 seconds remains. It was
  not filled and is unrelated to Q4. The global DQ report still fails for
  unrelated historical residue; game 406 has zero unmatched gameplay.

### Gameflow and release state

- Alternating Q2/Q4 shading was removed on desktop and mobile. Quarter labels
  and boundaries remain; hover is now the only shaded region.
- The ambiguous `approx` title badge was removed. Game 406 instead explains in
  the alert area that only Q4 timing is approximate and wall-clock-derived.
  Other games keep their ordinary lineup-health behavior.
- Commit `479c591` contains the UI, warning, report, and reproduction scripts
  and reached `main`/`origin/main`. Focused ribbon/mobile tests, R parsing, and
  JavaScript syntax checks passed (existing locale warnings only).
- These Gameflow UI changes were not deployed in this session. Deploy from a
  clean `main` checkout and verify game 406 on desktop and mobile; do not deploy
  the dirty shared workspace wholesale.

## Session Update (2026-09-22): R Tests CI Repair and Test Cleanup

This supersedes the earlier note that the current `main` failures were merely
pre-existing. GitHub Actions run `35699466280` (fast) and scheduled run
`35579247046` (DB-backed) were inspected and their failures were addressed in
the working tree. Nothing from this session was committed or pushed, so Actions
has not yet rerun these changes.

### Failures fixed

- `test-companion-query-counts.R`: the EuroLeague Game Logs mock had fallen
  behind the reader contract and omitted `minutes`; the fixture now includes
  it while retaining the season-rank and one-query assertions.
- `test-period-opening-anchors.R`: both CI jobs used `dbplyr` but the workflow
  did not install it. `any::dbplyr` is now listed in both dependency sets.
- `test-source-encoding.R`: the test caught a real Connect-safety regression.
  The visible ellipsis is preserved with `intToUtf8(8230L)`, avoiding raw
  non-ASCII source and parse-time `\u` escapes.
- `test-idle-restore-bookmarking.R`: removed the redundant exact-call-text
  assertion. The same test still checks use of the restore-aware helper, and
  the suite separately exercises restored behavior.
- `test-server-tabs-smoke.R`: replaced five literal `expect_true(TRUE)` calls
  with explicit assertions that the reset reactive flush does not error. No
  literal no-op `expect_true(TRUE)` / `expect_false(FALSE)` assertions remain
  under `app/tests` or `etl/tests`.
- Live ribbon tests now interpolate their trusted numeric fixture values before
  execution. This keeps the real production SQL and reconciliation assertions,
  while avoiding intermittent unnamed-prepared-statement failures through the
  Supabase transaction pooler.

### Nightly data-parity repair

- The useful minute-parity test found game `62506` stale in
  `team_metrics_by_game_mv`: both team rows published `40.0` minutes while the
  rebuilt canonical lineup fact held `39.9`.
- The existing scoped refresh function was run for game `62506`; exactly two
  derived rows were replaced and the parity query then returned zero rows.
  No base play-by-play data changed.
- `etl_full.R` now compares published team minutes with the fully rebuilt
  `lineup_four_factors_by_game` fact and unions only stale historical game IDs
  into the normal incremental team-metrics refresh. This prevents offline
  historical corrections from leaving downstream rows stale without forcing a
  full team-metrics rebuild every night.

### Verification

- Full fast suite: passed.
- DB parity and shape checks: passed after the scoped repair.
- PostgreSQL period-anchor tests: passed once `dbplyr` was available.
- Corrected live ribbon reader file: `1,065` assertions passed across both
  leagues.
- Changed files pass `git diff --check`; local Windows runs still emit the
  pre-existing `LC_ALL=C.UTF-8` warning flood.

## Shipped Features Not Covered Above

One line each; details and code pointers in PROJECT.md § "Shipped Work From
Merged Branches". Home Team Hub (`mod_team_hub.R`) with an opt-in saved team
(`localStorage` -> `hub_default_team()`); CSV export with timestamped names and
persisted column order (`csv_export_button()`, `dt_col_order_init_callback()`);
lineup filters re-applied after reconnect once choices load; DataTables escape
all but named HTML columns (`dt_escape_except()` -- use it, never
`escape = FALSE`); shared filter helpers (`game_context_filters_ui()`,
`resolve_gn_last_n_params()`, `resolve_clutch_params()`,
`update_single_team_selectize()`); Compare Defensive Shooting and opponent-rank
filter; Tab 5 multi-team TOTAL row (`is_multi_team_total`); `COLUMN_TOOLTIPS`;
ETL player identity dictionary, scoped cold export (`cold_export_scope()`),
leading-OT lineup recovery, roster/action integrity checks.

## Shot Chart Coordinates vs Court Drawing (2026-10-01)

Full findings: `docs/shot_chart_court_dimensions_2026-10-01.md` (buggy-shot list: `docs/shot_chart_buggy_shots_2027.csv`).

- Shot `coord_x`/`coord_y` are a **tracker grid, not cm**: x 0-1500 in steps of 15 (centre 750), y = depth from baseline 0-1400 in steps of 28. Rim at y ~112-140.
- **The 2/3 label is derived from position** (3 of 29,763 shots in 2026 disagree within a grid cell), so the 2/3 boundary IS the tracker's three-point line: straight sides at |x-750| ~495-510 up to depth ~800, apex at y ~1071-1078. Best fit: ellipse around rim (126, 750), 945 deep x 680 wide, corner lines at 495 -- 13 wrong-side shots in 2026 vs 178 for basket.co.il's line, 668 for `fiba_court_zones.R`, 3,168 for FIBA.
- basket.co.il's chart (`courtLines()` in its `game.js`) hand-fitted only the 3pt line (ellipse 900x600, width stretch 1.12); **paint, restricted area and circles are FIBA-true and never adjusted**, so lay-ups/dunks plot outside them. Our script adjusted the 3pt line and RA (145) but not the paint.
- Interior zones have **no ground truth**: box-score `Points_in_the_paint` is not reproducible from coordinates or shot type (131/438 team-games exact). Don't retry it.
- 2026-27 games 393/397/399/400 have no in-house shot coordinates (cold-storage gap); 621/626/648/651 exist only in the GitHub release parquet.

## Clickable Cues, Navbar Width, Home Hub (2026-10-11)

Full history and measurements: PROJECT.md § "Session Update (2026-10-11)".
Spec and plan: `docs/superpowers/specs/2026-10-11-clickable-cues-design.md`,
`docs/superpowers/plans/2026-10-11-clickable-cues.md`.

### The cue vocabulary -- one meaning each, visible at rest

All in one "Clickable cues" block at the end of `www/app.css`. A new
interactive element takes one of these; do not invent a sixth.

| Cue | Means | Where |
|---|---|---|
| Amber underlined text + `›` | opens a modal, detail view or tab | `.ld-lineup-link`, `.euro-ld-unit`, `.ribbon-link`, Compare rows (`td:nth-child(2)`, the first cell is a rank), `.hub-story-line`, `.hub-card-go` |
| Small triangle caret | opens a **menu**, nothing else | navbar tabs with a view-mode menu, on/off team and player cells (`td[data-pivot-trigger]`) |
| Pencil (`.chip-edit`) | goes to the control that owns the value | `make_chip()` when `focus_id` is set |
| Dotted underline | explains on hover -- pointer devices only | `th[title]`, `.onoff-net-tip` |
| `table_hint(...)` line | says in words what the table responds to | above interactive tables, both leagues |

- Sorting keeps its sort arrows and the lineup tokens keep their button look.
  A caret on anything that is not a menu breaks the vocabulary.
- **Table-header explanations live on `th[title]`** (written by
  `HEADER_TOOLTIP_JS`). `data-tooltip` is `tt()`'s label mechanism and never
  appears on a DT header -- the first version of the header cue targeted
  `th[data-tooltip]` and would have matched nothing.
- **Touch is `body.ibpl-touch-tips`** (set in `mobile.js`; phones and touch
  tablets at any width), never a viewport width or a bare media query. On
  touch the existing `.ibpl-m-th-info` button is the Explain cue; adding an
  underline there runs under the button.
- `table_hint()` (`helpers.R`) holds a pointer sentence and a touch sentence
  per interaction, written out in full, because the action differs and not
  only the verb: a tap on a header sorts, so its definition is reached through
  the info button. `NA` for touch = pointer-only (column drag).
- Glyphs go in `::after`, so they never reach CSV exports, sort values or
  copied text. The on/off renderer (`sprintf(paste0(...))`) was not edited;
  every cue on its cells is CSS.
- The font's `▾` renders as a faint dash in DM Sans. The carets are CSS
  triangles (`border-top: 5px solid currentColor`).

### The navbar has almost no spare width

Measured at 1440px (Israeli league, scrollbar present). The right cluster
(`#navbar_right_cluster`) is `position: fixed`; `app.js` measures it into
`--navbar-cluster-w` and `#main_tabs` reserves that as padding.

- The tab row fits with **~38px** left for the update text beside the status
  dot. "Oct 10" is 38.2px; "May 28" is 43px. Anything wider wraps the row.
- So: the tab caret is absolutely positioned in the 16px gap between tabs and
  takes **no width**; the active tab's mode label shows only from 1600px; the
  update date is `last_updated_short()` ("Oct 10", full stamp in `title`)
  with a stacked "UPDATED" caption below 1600px, where the status dot
  (`.nav-updated-dot`) is hidden to buy 10px. Widest date leaves 8px.
- At 1366px the row wraps but nothing is covered; at 1280px it wraps for
  every text, including none.
- **A wrapped row used to make menu items unclickable.** Tab links are stacked
  above closed menus on purpose (`.navbar .navbar-nav .nav-link`, z 10001, so
  a closed menu never covers a neighbour), which also put second-row tabs on
  top of an OPEN first-row menu. `.nav-item.tab-has-dropdown:hover` is now
  z 10002, and the menu opens after 140ms (`ibpl-tab-menu-in`) so crossing a
  tab on the way to one below does not pop a menu over the destination.
  Keep both halves: the raise without the delay brings back the pointer trap.
- Adding anything to the navbar means re-measuring row count at 1280, 1366,
  1440 and 1600, **after** "Last updated" has loaded -- before it loads the
  cluster is 220px narrower and everything appears to fit.
- The view-mode radios are `display: none !important`
  (`.view-mode-container`); the navbar menu is the only way to change mode on
  desktop. `ts_display_mode` is a selectize: the menu must go through
  `sel.selectize.setValue()` / `getValue()`, since a selectize `<select>` only
  holds the option it is showing and `.value =` leaves it blank.

### Gameflow

- **`data-clip` is the contract.** `app.js` keys clicks, hover and the
  `is-pair-selected` class on every element that carries it. The player-row
  button (`.ibpl-ribbon-row-btn`, a `<rect>` drawn first in the gutter row)
  needed no JS for that reason. Anything you add to the SVG with a
  `data-clip` becomes clickable and selectable.
- Underlined names read as decoration, not as "selectable" (user, 2026-10-11).
  Rows are buttons; bars are outlined with a drop shadow.
- **A third surface can open Gameflow with three lines:**
  `ribbon_inline_ui("<prefix>")` in the UI,
  `ribbon_modal_server(input, output, session, "<prefix>", league, ver_fn,
  svg_id_prefix = "<prefix>")` in the server, and the prefix in
  `INLINE_PREFIXES` in `mobile.js`. Links come from `ribbon_link_cell(...,
  input_id = "<prefix>_ribbon_click")`. Home uses `"hub"`. Desktop gets the
  modal, touch gets the inline quarter cards in that surface's own panel.
- Not done: the touch quarter cards pin a copy of the gutter that has no
  `data-clip`, so the row button does not show there. Tapping a row still
  selects.

### Home hub

- Layout, from 1200px: `.home-hub-layout` is a 12-column grid and
  `#team_hub_section` is `display: contents`, so the question links
  (`.home-nav-group`, "Investigate further") sit in the hub's own row. The
  hub breaks out of Home's 760px column with margins, not a transform.
  Below 1200px nothing applies and the links are the full question cards.
- The grid rule is scoped `body.league-il .home-hub-layout`; unscoped it
  would undo `.league-only-il { display: none }`.
- **Never `display: none` an empty Shiny output.** Shiny treats it as hidden
  and stops sending it values, so it would stay empty for good. A hub box
  with nothing to show (no 100-possession lineup early in a season) collapses
  to zero width instead: `flex: 0 0 0; padding: 0`.
- **A link inside a `js-shiny-event` card fires both.** The team card is one
  big button; the "Last game ... View Gameflow" line is a row of its own for
  that reason.
- `hub_fetch_last_games(gy, ver)`: one cached season-level query, one row per
  team, with `has_gameflow` (the game has rows in `mv_lineup_totals_by_day`).
  The link is offered only when that is TRUE.

### Seasons

- `EURO_DEFAULT_SEASON` is **derived** from `DEFAULT_GAME_YEAR`
  (`euro_default_season()`: season-ending year minus one). It was a literal
  `"2025"` that nobody bumped when 26-27 loaded, so EuroLeague/EuroCup were
  served on 25-26 and switched only after a database round trip. Rolling to a
  new season is one bump of `DEFAULT_GAME_YEAR`; a test fails on a literal.
- The static default is not cosmetic. Until `euro_init_season_inputs()` hears
  from the database, the navbar select, every EuroLeague date input and every
  EuroLeague query run on it.

### Traps hit this session

- **`printf` and heredocs in the Bash tool collapse backslashes.** A CSS
  escape `"\25BE"` arrived as byte 0x15 + `BE`, and `"\00B7"` as a NUL + `B7`.
  The NUL made git treat `app.css` as binary, which also switched off
  line-ending conversion: a 24-line append showed as a 3,982-line diff.
  Write anything containing a backslash with the Edit/Write tools, or from a
  script file passed to `python -I`.
- **Verify the live site in a browser, not with curl.** Connect Cloud answers
  curl with its JS interstitial (200, ~5 KB), so a grep for a marker never
  matches however long you poll.
- **A deploy lands in well under a minute**; the manifest's `files` list is
  not enforced (`www/mobile.js` and `www/mobile.css` are live and absent from
  it), so a new `R/*.R` file deploys without touching `manifest.json`.
- **Playwright CDP emulation outlives the script.** Touch / `hover: none`
  set through `newCDPSession` kept applying to the page after a fresh session
  tried to clear it. Open a new page for emulation and close it.
- Chromium touch emulation is classified `tablet`, never `phone`, by
  `ibplDeviceKind()`: the phone-only floating row name cannot be exercised
  that way.
- The league choice persists in `localStorage` (`ibpl_league_select`). A
  profile that last used the Israeli league never runs the EuroLeague season
  observer, so "works on my machine" says nothing about a first-time visitor.
- Committing a dirty working tree "under the branch" carries everything in
  it. Team Insights reached production that way for an hour. It is unfinished
  and lives only on the local branch `shiny/team-insights`; keep it off
  `main` until the user says it is ready (it also fails six tests as-is).

### Open

- EuroLeague Lineups showed "No units match" after a season switch, with a
  date range that had not followed the season. Seen once, BEFORE the default
  season was derived; not reproducible after (re-tested 2026-10-11: 26-27 ->
  25-26 -> 26-27 -> 25-26, dates and rows follow each time). Probably a
  symptom of the date inputs being built on 25-26 bounds; unproven. First
  arrival on that tab took ~13-15s locally with a blank table meanwhile.
- At 1280px the navbar still wraps to two rows (menus work, it is just tall).
