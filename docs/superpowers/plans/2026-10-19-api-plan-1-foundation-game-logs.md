# API Plan 1: parity foundation + Game Logs end to end -- Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the parity machinery (recorded-DB goldens of today's Shiny output, a coverage gate over the inventory IDs) and carry one tab -- Game Logs, both leagues -- all the way through: Shiny logic moved into a shared service function, a Plumber API that serves it, and tests proving the API equals Shiny.

**Architecture:** Shiny's Game Logs closures move, unchanged in behaviour, into `app/R/svc_game_logs.R`. Shiny's reactives and the new `api/` Plumber router both call it. Goldens are captured from the *real* app server under `shiny::testServer`, with database reads served from recorded fixtures, so they are deterministic and run in the fast suite. A nightly DB-backed test runs Shiny and the API side by side on the live database.

**Tech Stack:** R 4.4.2, Shiny `testServer`, testthat, DBI S4 methods (replay connection), plumber, callr + httr2 (API smoke test), digest.

**Spec:** `docs/superpowers/specs/2026-10-19-api-mcp-design.md`; feature IDs from `docs/superpowers/specs/2026-10-19-api-mcp-feature-inventory.md`.

## Sequencing (my call, overrulable)

The design lists Phase 1 (service layer for every tab) before Phases 2-3 (API). This plan instead takes **one tab through every phase first**, so the golden format, the service-function shape, the response contract and the parity test are proven on the smallest tab before they are repeated twelve times. Later plans, one per tab family, reuse all of it:

| Plan | Content |
|---|---|
| 1 (this) | Foundation + Game Logs (GL-01..08, X-04, X-05 for GL, X-08, X-09 for GL, X-14 CSV) |
| 2 | Player Stats (PS-*) |
| 3 | Team Ratings (TR-*) incl. the rank-arrow inline SQL |
| 4 | On/Off (ON-*) incl. stateless auto min-poss and the On/Off CSV the Shiny button never delivered |
| 5 | Lineups (LD-*) incl. lineup game-log modal inline SQL |
| 6 | Gameflow + Home (GF-*, HOME-*) |
| 7 | Compare (CMP-*) incl. the starters-preset detail timeout |
| 8 | MCP server over the API |
| 9 | Hosting + access control (after decision #5 and the Connect Cloud check) |

## Global Constraints

- Shiny output must not change during extraction; the goldens are the proof (design § Constraints).
- One implementation per behaviour: the API calls `svc_*`; it never re-derives a rule (`docs/shared_definitions.md` R1, R4, R8). EuroLeague reuses the Israeli implementation (R7): the league is a parameter.
- Parameterised SQL only; every query parameter untrusted; unknown/unsupported API parameters -> HTTP 400 naming the parameter; statement timeout -> 504 (design § Plumber API).
- DB-backed tests and scripts connect on **port 5432**, never 6543 (pooler crosses concurrent prepared statements). Never run them beside a local app or another DB test run.
- Never touch the user's local working tree or branch (`shiny/ft-credit-ts`, dirty). Work on short-lived branches from `origin/main`: `infra/api-parity-harness` (Tasks 1-4), `shiny/svc-game-logs` (Tasks 5-7), `api/game-logs` (Tasks 8-11). No pushes to `main`; the user merges.
- Files containing backslashes are written with the Write/Edit tools, never heredocs (they collapse `\\`). `app.R`, `global.R`, `helpers.R` have mixed line endings: splice per CLAUDE.md § Deploy Scripts and verify `git diff --stat` shows only the intended lines.
- 2-space indent, snake_case, base `lapply`/`vapply`, no purrr. A move is verified by reversing it and diffing against `HEAD`, not by new tests for moved code.
- API league ids: `il`, `euroleague`, `eurocup` (map to `"E"`, `"U"`). Paths under `/v1/`. JSON by default, `?format=csv` returns the same rows.

## Review Focus

1. **A replay miss must fail, not return empty.** A golden run whose query (or parameters) was never recorded must stop with the query label; returning a 0-row frame would make every golden "pass" on nothing. Test in Task 2.
2. **Empty result vs. no data.** Filters that match no games: Shiny shows nothing / "No games match"; the API must return `rows: []` with 200 and the resolved filters in `meta`, not 404 or 500. Test in Task 9.
3. **Malformed filter values.** `team=abc`, `last_n=-3`, `gn_min=20&gn_max=5`, `last_n` together with `gn_min`, `date_from` after `date_to`, a season outside the data: Shiny's inputs can't produce these, a caller can. Each is a 400 naming the parameter (GN and last N are mutually exclusive, X-05). Test in Task 8.
4. **League id spaces.** An Israeli team id sent to `/v1/euroleague/game-logs` must filter to nothing or 400, never match an EuroLeague team that happens to share the number (X-01). Test in Task 9.
5. **Cache crossing data versions.** After `app_meta.etl_full_last_success` advances, the API must not serve the previous season pull (X-08). Test in Task 8.

---

### Task 1: Pool override hook in `global.R`

The API and the golden harness must load `global.R`'s definitions without its side effects (pool creation, `onStop`, prewarm checkout).

**Files:**
- Modify: `app/R/global.R` (the `# ---------------- PostgreSQL pool ----------------` block through the prewarm `tryCatch`)
- Test: `app/tests/testthat/test-pool-override.R`

**Interfaces:**
- Produces: option `ibpl.pg_pool`. When `getOption("ibpl.pg_pool")` is non-NULL at source time, `global.R` sets `pg_pool <- getOption("ibpl.pg_pool")` and skips `dbPool()`, `onStop()` and the prewarm. Otherwise behaviour is unchanged.

- [ ] **Step 1: Write the failing test** `test-pool-override.R`:
  - "global.R uses an injected pool and has no side effects": `options(ibpl.pg_pool = structure(list(), class = "fake_pool"))`, then `source(repo_file("R","catalogues_generated.R"), local = env)` and `source(repo_file("R","global.R"), local = env)` with `env <- new.env()`; mock `pool::dbPool` and `shiny::onStop` via `testthat::local_mocked_bindings(.package = ...)` to `stop("called")`. Assert `inherits(env$pg_pool, "fake_pool")` and no error.
  - "global.R parse has exactly one dbPool call, inside the override branch": parse the file, assert the `dbPool` call is nested under an `if` whose condition reads `getOption("ibpl.pg_pool")`.
- [ ] **Step 2: Run** `cd app && "$RSCRIPT" -e "testthat::test_file('tests/testthat/test-pool-override.R')"` -- expect FAIL (`called`).
- [ ] **Step 3: Implement** the `if (is.null(getOption("ibpl.pg_pool"))) { <existing block unchanged> } else { pg_pool <- getOption("ibpl.pg_pool") }`, existing block indented only. Splice per Global Constraints; `git diff --stat app/R/global.R` must show only the wrapped block.
- [ ] **Step 4: Run** the new test and the full fast suite (`"$RSCRIPT" scripts/test_all.R` with `RUN_DB_TESTS=0`) -- PASS.
- [ ] **Step 5: Commit** `infra: let global.R take an injected pool (API, goldens)`.

### Task 2: Record/replay database for goldens

**Files:**
- Create: `app/tests/goldens/harness.R` (sourced by golden tests and the capture script; not a `helper-*` file, so mock-based tests never load it)
- Test: `app/tests/testthat/test-golden-harness.R`

**Interfaces:**
- Produces:
  - `golden_replay_conn(fixture_dir)` -> an S4 object of class `GoldenReplayConnection` (extends `DBIConnection`) with `dbGetQuery(conn, statement, params = NULL, ...)` returning the recorded frame for `golden_query_key(statement, params)`; on a miss it stops with `"golden replay miss: <db_query_label(statement)> key=<key>"`. Also methods for `dbExecute` (returns 0L) and `dbIsValid` (TRUE).
  - `golden_record_conn(real_con, fixture_dir)` -> class `GoldenRecordConnection`; forwards `dbGetQuery` to `real_con` and writes `<fixture_dir>/<key>.rds` (`saveRDS(..., compress = "xz")`).
  - `golden_query_key(statement, params) -> character(1)`: `digest::digest(list(gsub("[[:space:]]+", " ", trimws(statement)), unname(lapply(params, as.character))), algo = "sha1")`.
  - `golden_app_env(conn)` -> environment holding the real app: sets `options(ibpl.pg_pool = conn)` and `Sys.setenv(IBPL_CACHE_UI_HTML = "false")`, sources the same files in the same order as `app.R` lines 6-34 plus `svc_*.R` files that exist, into one environment, and returns it. It must reset `GL_DATA_CACHE`, `RANKED_CACHE` and the `cached_ref_query` store so runs are independent.
  - `golden_run_server(env, inputs, capture)` -> runs `shiny::testServer(env$server, {...})` where `env$server` is the server function built by sourcing `app.R`'s server body (implementer: source `app/app.R` with `shiny::shinyApp` mocked to return its `server` argument), applies `inputs` (a named list) with `session$setInputs`, flushes, and returns `capture(session, output)`.
  - `golden_capture_dt(slug_prefix)` -> a capture function: before the run it `trace()`s `DT::datatable` to push `list(data = data, options = options)` into a sink; it returns the last sink entry whose `options$buttons[[1]]$filename` starts with `slug_prefix`, as `list(data = as.data.frame(data), order = options$order, hidden = <names of columns whose columnDefs have visible = FALSE>)`, then `untrace()`s. Errors if none matched ("no datatable rendered for <slug>").
- [ ] **Step 1: Write the failing tests** `test-golden-harness.R`:
  - "replay returns a recorded frame": write one fixture for `SELECT 1 AS x WHERE $1 = 1` with params `list(1L)`; `DBI::dbGetQuery(golden_replay_conn(dir), "SELECT 1 AS x  WHERE $1 = 1", params = list(1L))` equals `data.frame(x = 1L)` (whitespace-normalised key).
  - "replay miss is an error naming the query" (Review Focus 1): different params -> `expect_error(..., "golden replay miss")`.
  - "db_get_query goes through the replay connection": with `env <- golden_app_env(golden_replay_conn(dir))`, `env$db_get_query(env$pg_pool, <recorded statement>, params = list(1L))` returns the fixture.
- [ ] **Step 2: Run** the file -- FAIL (functions undefined).
- [ ] **Step 3: Implement** `harness.R` to the interfaces above (`setClass("GoldenReplayConnection", contains = "DBIConnection", slots = c(dir = "character"))`, `setMethod("dbGetQuery", ...)`).
- [ ] **Step 4: Run** -- PASS.
- [ ] **Step 5: Commit** `test: record/replay DB connection and app loader for goldens`.

### Task 3: Coverage gate over the inventory

**Files:**
- Create: `app/tests/testthat/test-api-coverage-gate.R`, `api/coverage.R`
- Modify: `docs/superpowers/specs/2026-10-19-api-mcp-feature-inventory.md` (no content change; the gate reads it)

**Interfaces:**
- Produces:
  - `inventory_ids(path) -> character()`: every `| <ID> |` first-column cell matching `^[A-Z]+-[0-9]{2}$`.
  - `covered_ids(test_dirs) -> character()`: IDs named in `covers("GL-01", "GL-04", ...)` calls found by parsing (not grepping) every `test-*.R` under `test_dirs`.
  - `covers(...)`: a no-op marker function defined in `api/coverage.R`, called inside `test_that()` blocks.
  - `API_DONE_PREFIXES`: character vector in `api/coverage.R`; empty in this task, `"GL"` after Task 11.
- [ ] **Step 1: Write the failing tests:**
  - "inventory has 109 unique ids": `length(ids) == 109L`, `!anyDuplicated(ids)`.
  - "every id under a done prefix is covered": `setdiff(ids[sub("-.*", "", ids) %in% API_DONE_PREFIXES], covered)` is empty, printing the missing ones.
  - "the gate can fail" (R9): with a temp inventory listing `ZZ-01` and `API_DONE_PREFIXES` locally set to `"ZZ"`, the uncovered set is `"ZZ-01"`.
  - "strict mode lists everything uncovered": when `Sys.getenv("IBPL_COVERAGE_STRICT") == "1"`, expect no uncovered ids at all (skipped otherwise; documents the end state).
- [ ] **Step 2: Run** -- FAIL (functions undefined).
- [ ] **Step 3: Implement** `api/coverage.R`.
- [ ] **Step 4: Run** -- PASS (done set empty).
- [ ] **Step 5: Commit** `test: coverage gate over the API feature inventory`.

### Task 4: Game Logs goldens, captured from today's Shiny

**Files:**
- Create: `app/tests/goldens/presets/game_logs.R`, `scripts/capture_goldens.R`, `app/tests/goldens/fixtures/game_logs/*.rds`, `app/tests/goldens/out/game_logs/*.rds`
- Test: `app/tests/testthat/test-golden-game-logs.R`

**Interfaces:**
- Consumes: Task 2's `golden_app_env`, `golden_run_server`, `golden_capture_dt`, `golden_record_conn`, `golden_replay_conn`.
- Produces:
  - `GAME_LOGS_PRESETS`: named list, each `list(league = "il"|"euroleague", covers = c(<ids>), inputs = list(<Shiny input ids> = <values>), slug = <csv slug>)`. Every preset sets `main_tabs` and the season explicitly (`game_year = "2026"`; EuroLeague `euro_competition = "E"`, `euro_game_year = "2025"` -- implementer: read the real EuroLeague season input ids from `make_euro_shared_context`).
  - Golden file per preset: `out/game_logs/<preset>.rds` = the `golden_capture_dt()` result.
  - `scripts/capture_goldens.R --tab game_logs [--record]`: with `--record` connects on port 5432 (`etl/.Renviron`-style vars from `app/.Renviron`, port forced to 5432) through `golden_record_conn`, and rewrites fixtures; without it replays and rewrites only `out/`. Prints row counts per preset.
- Presets (season 2026 Israeli, complete; 2025 EuroLeague). Team and opponent ids are resolved once at first capture from `fetch_teams_min(2026)` (first two by team_id) and written into the preset file as literals:

| Preset | League | View | Inputs beyond defaults | Covers |
|---|---|---|---|---|
| il_summary_default | il | Summary | -- | GL-01, GL-03, GL-04, GL-05, GL-06, GL-07 |
| il_ff_default | il | Four Factors | -- | GL-01, GL-04, GL-05 |
| il_summary_team | il | Summary | `gl_team` | GL-01, GL-02 |
| il_summary_last5_home | il | Summary | `gl_last_n = "5"`, `gl_home_away = "home"` | GL-02 |
| il_ff_gn_win | il | Four Factors | `gl_gn_min = "5"`, `gl_gn_max = "15"`, `gl_outcome = "win"` | GL-02 |
| il_summary_dates_type | il | Summary | `gl_dates` = 2025-12-01..2026-01-31, `gl_game_type` = League code | GL-02 |
| il_ff_starters | il | Four Factors | off `gte` 4, def `lte` 2 | GL-02 |
| il_summary_opps_statfilter | il | Summary | `gl_opponents` = 2 ids; stat filter Net >= 0 via `gl_stat_filter_col = "Net"`, `gl_stat_filter_op = "ge"`, `gl_stat_filter_value = 0`, then `gl_add_stat_filter = 1` | GL-02, X-09 |
| il_ff_away_loss_last10 | il | Four Factors | away, loss, last 10 | GL-02 |
| il_summary_empty | il | Summary | team + GN 30-31 (no such games) | GL-02 |
| el_summary_default | euroleague | Summary | -- | GL-01, GL-03, GL-04, GL-05, GL-07 |
| el_ff_default | euroleague | Four Factors | -- | GL-01, GL-04 |
| el_ff_teams_phase | euroleague | Four Factors | 2 teams, phase `"RS"` | GL-02 |
| el_summary_round_starters | euroleague | Summary | round 10-20, own starters `gte` 4 | GL-02 |
| el_summary_last5_home | euroleague | Summary | last 5, home | GL-02 |

- [ ] **Step 1: Write the test** `test-golden-game-logs.R`: for each preset, replay -> capture -> `expect_identical(captured, readRDS(golden))`; each `test_that` calls `covers(preset$covers)`. Plus "golden files exist for every preset" (fails until captured).
- [ ] **Step 2: Run** -- FAIL (no fixtures).
- [ ] **Step 3: Capture** on `origin/main` code (no extraction yet): `"$RSCRIPT" scripts/capture_goldens.R --tab game_logs --record`. Expected: 15 presets, `il_summary_empty` 0 rows (capture records "no datatable rendered" as `NULL` golden for that preset only -- the Shiny renderer returns `NULL`), every other preset > 0 rows. Check total fixture size (`du -sh app/tests/goldens/fixtures`); if over 15 MB, stop and report the size to the user before committing (do not trim recorded columns: the replay must return exactly what the DB returned).
- [ ] **Step 4: Run** the test twice in fresh R sessions -- PASS both times (determinism).
- [ ] **Step 5: Commit** fixtures, goldens, presets, script: `test: Game Logs goldens from current Shiny (15 presets, both leagues)`.

### Task 5: Move the Israeli Game Logs logic into `svc_game_logs.R`

**Files:**
- Create: `app/R/svc_game_logs.R`
- Modify: `app/R/server_tab4.R`, `app/app.R` (source line before `server_tab4.R`), `app/tests/testthat/helper-server-mocks.R` (source the svc file after `helpers.R`), `scripts/capture_goldens.R`/harness load order if needed
- Test: existing `test-tab4-*.R`, `test-golden-game-logs.R`

**Interfaces:**
- Produces (all pure, no Shiny):
  - Moved verbatim from `server_tab4.R`: `GL_SUMMARY_FILTERABLE_COLS`, `GL_FF_FILTERABLE_COLS`, `gl_pr_col_name`, `gl_filter_starters`, `gl_join_schedule_info`, `gl_build_summary_metrics`, `gl_build_ff_metrics`, `gl_attach_percentiles`. (`gl_result_cell_renderer`, `gl_date_cell_renderer`, `gl_apply_heat_styles` are presentation and stay.)
  - `gl_filter_schedule(sched, f) -> data.frame`: body of `gl_filtered_schedule` from "Last N" to the GN filter, with `input$gl_*` replaced by `f$team` (character or `""`), `f$dates` (length-2 Date or NULL), `f$game_type`, `f$opponents`, `f$home_away`, `f$outcome`, `f$gn` (the `resolve_gn_last_n_params()` list).
  - `gl_starters_bounds(off_mode, off_val, def_mode, def_val) -> list(off_min, off_max, def_min, def_max)`: body of `gl_starters_filter`.
  - `gl_summary_display(df, stat_filters, scoreless) -> data.frame | NULL` and `gl_ff_display(df, stat_filters, scoreless)`: the renderer's frame steps up to and including `attach_has_scores` (shot totals + `add_shot_split_metrics`, `apply_stat_filters`, `attach_has_scores`). The `add_ribbon_link_column` and `select()` stay in Shiny.
  - `gl_shot_averages(disp) -> list(off = c(fg2 = int, fg3 = int), def = ...)`: the `a2`/`a3` computation from the renderer, defaults 53/34. Shiny's renderer calls it.
- [ ] **Step 1:** Move the eight top-level objects (cut/paste only). Verify: `git show HEAD:app/R/server_tab4.R` minus the moved lines equals the new `server_tab4.R` minus nothing else; concatenating the moved block reproduces the removed text byte for byte (`diff`).
- [ ] **Step 2:** Extract `gl_filter_schedule`, `gl_starters_bounds`, `gl_summary_display`, `gl_ff_display`, `gl_shot_averages`; the reactives become one-line calls. Verify by reversing (substitute `f$x` back to `input$gl_x`, inline) and diffing against `HEAD`: only the substitutions differ.
- [ ] **Step 3: Run** `test-golden-game-logs.R` -- all 10 IL presets PASS unchanged; then the full fast suite -- PASS.
- [ ] **Step 4: Commit** `shiny: Game Logs logic moves to svc_game_logs.R (goldens unchanged)`.

### Task 6: Move the EuroLeague Game Logs logic, inline SQL into a reader

**Files:**
- Modify: `app/R/svc_game_logs.R`, `app/R/server_tab11_euro_gamelogs.R`, `app/R/global_euro.R`
- Test: `test-golden-game-logs.R`, `test-tab11-euro-gamelog-minutes.R`, `test-companion-query-counts.R`

**Interfaces:**
- Produces:
  - `euro_fetch_game_log_rows(competition, season) -> data.frame` in `global_euro.R`: the inline `season_rows` SQL, same statement text and params, through `db_get_query`; the `cached_season_df` wrapper stays at the call site.
  - `gl_build_euro_games(df, starters_bounds = NULL, apply_starters = TRUE)`: the `build_games` closure, verbatim.
  - `gl_filter_euro_rows(df, f) -> data.frame`: the body of `filtered_games` up to (not including) `build_games`, with inputs replaced by `f$teams`, `f$dates`, `f$phase`, `f$opponents`, `f$home_away`, `f$outcome`, `f$gn`.
  - `GL_EURO_PERCENTILE_METRICS`: the `percentile_metrics` vector.
- [ ] **Step 1:** Move each piece; verify each by reverse-and-diff as in Task 5. The reader statement must be byte-identical so the recorded fixture key still matches -- a replay miss here means the SQL text changed.
- [ ] **Step 2: Run** goldens (all 15) and the two EL test files -- PASS.
- [ ] **Step 3: Commit** `shiny: EuroLeague Game Logs logic and season reader move to the shared layer`.

### Task 7: One `svc_game_logs()` for both leagues

**Files:**
- Modify: `app/R/svc_game_logs.R`, `app/R/server_tab4.R`, `app/R/server_tab11_euro_gamelogs.R`
- Test: `app/tests/testthat/test-svc-game-logs.R`, goldens

**Interfaces:**
- Consumes: Tasks 5-6 functions.
- Produces:
  - `svc_game_logs(p, readers = gl_default_readers()) -> list(rows = data.frame, meta = list(...))` where `p` is the validated list from Task 8's `svc_game_logs_params()` (`league`, `competition`, `season`, `view` = `"summary"|"four_factors"`, `team`/`teams`, `dates`, `game_type` (IL codes) / `phase` (EL text), `opponents`, `home_away`, `outcome`, `starters` (the four bounds), `gn`, `stat_filters` (list of `list(col, op, value)`), `data_version`).
  - `rows`: the display frame **before** the `select()`/ribbon link, i.e. `gl_summary_display()` / `gl_ff_display()` output (IL) and `gl_attach_percentiles(gl_build_euro_games(...))` with `attach_has_scores(df, NULL)` (EL).
  - `meta`: `path = "fast"` (Game Logs always reads season pulls), `filters` (echo of `p`), `default_sort = list(list(column = "game_date", direction = "desc"), list(column = <"gn" | "round_number">, direction = "desc"))`, `shot_averages = gl_shot_averages(rows)` (IL Summary only, else NULL), `percentile_population = "season"`.
  - `gl_default_readers()`: `list(schedule = function(gy) <the final_schedule_mv query>, lineup_totals = ..., lineup_ff = ..., euro_rows = euro_fetch_game_log_rows, scoreless = fetch_scoreless_games)`, each the exact query the reactives run today, wrapped in `cached_season_df()` keyed `list("gl_<name>", league, season, data_version)`.
  - Shiny's `gl_teams_summary` / `gl_teams_ff` / EL `filtered_games` become thin reactives that build `p` from inputs and call the same building blocks (Shiny keeps its own `bindCache`/`bindEvent` wrappers; the service function owns logic, not reactivity).
  - League differences live in one descriptor `GL_LEAGUE <- list(il = list(round_col = "gn", type_col = "game_type", team_multi = FALSE, has_shots = TRUE, has_stat_filters = TRUE), euroleague = list(round_col = "round_number", type_col = "phase", team_multi = TRUE, has_shots = FALSE, has_stat_filters = FALSE))`. **Unify the two filter functions only where the goldens prove it** (rule R7, design § Constraints): if a unified `gl_filter_games(df, f, league)` changes any golden, keep both functions and record the difference in the deviation register of `docs/shared_definitions.md` the same day.
- [ ] **Step 1: Write failing tests** `test-svc-game-logs.R` (replay fixtures): for each preset, `svc_game_logs(<p built from the preset's inputs>)$rows` equals the golden `data` after dropping `gameflow` and applying `select` column set membership (rows identical on every column the golden has, minus `gameflow`); `meta$default_sort` as above; `covers(...)` as in the preset.
- [ ] **Step 2: Run** -- FAIL.
- [ ] **Step 3: Implement** `svc_game_logs`, `gl_default_readers`, `GL_LEAGUE`; switch the Shiny reactives.
- [ ] **Step 4: Run** `test-svc-game-logs.R`, goldens, full fast suite -- PASS.
- [ ] **Step 5: Commit** `shiny: one svc_game_logs() serves both leagues`.

### Task 8: API skeleton -- bootstrap, parameters, errors, cache, reference endpoints

**Files:**
- Create: `api/plumber.R`, `api/run.R`, `api/R/bootstrap.R`, `api/R/params.R`, `api/R/respond.R`, `api/README.md`
- Test: `api/tests/testthat/test-api-params.R`, `api/tests/testthat/test-api-reference.R`, `api/tests/testthat.R`; `scripts/test_all.R` runs `api/tests` after the app suite

**Interfaces:**
- Produces:
  - `api_bootstrap(conn = NULL)`: builds a pool on `PG_PORT` from env (default **5432**) unless `conn` is given (tests pass `golden_replay_conn`), sets `options(ibpl.pg_pool = ...)`, sources `catalogues_generated.R`, `helpers.R`, `global.R`, `logger.R`, `global_euro.R`, `svc_*.R` from `app/R` into the global env. No UI building.
  - `svc_game_logs_params(query, league) -> list` in `app/R/svc_game_logs.R` (owned by the service, used by the API; Shiny builds the same list from inputs): fail-closed. Errors are `api_error(status = 400L, param = "<name>", message)` conditions. Accepted query params: `season`, `view` (`summary`|`four_factors`), `team` (IL, one id) / `teams` (EL, comma list), `date_from`, `date_to`, `game_type` (IL codes, comma list, Winner Cup 10/34 accepted as either) / `phase` (EL), `opponents`, `home_away` (`home`|`away`), `outcome` (`win`|`loss`), `own_starters`, `opp_starters` (`gte:4` / `lte:2`), `gn_min`, `gn_max` (IL) / `round_min`, `round_max` (EL), `last_n`, `stat` (repeatable `col:op:value`, `op` in `ge`|`le`, `col` a value of `GL_*_FILTERABLE_COLS`), `sort`, `format`. Any other name -> 400 naming it.
  - `api_error(status, param, message)`; `api_respond(res, payload, format)`: JSON (`jsonlite::toJSON(dataframe = "rows", na = "null", auto_unbox = TRUE, digits = NA)`) or CSV (`format=csv`, rows only, `Content-Disposition` with `csv_export_stamp()`); a DB error whose message contains `statement timeout` -> 504; any other error -> 500 without the SQL text.
  - Reference endpoints: `GET /v1/meta/data-version` -> `{il, euroleague}` from the same functions Shiny uses (`shared_data_version` source / `euro_data_version()`); `GET /v1/{league}/seasons`; `GET /v1/{league}/teams?season=`, `/players`, `/game-numbers` (IL), `/phases`, `/rounds` (EL), `/game-types` (IL, from `GAME_TYPE_CHOICES_UI`) -- each calling the canonical lookup Shiny calls (`fetch_teams_min`, `fetch_players_basic`, `fetch_gn_values`, `euro_fetch_*`).
- [ ] **Step 1: Write failing tests** `test-api-params.R` (Review Focus 3): each of `team=abc`, `last_n=-3`, `last_n=0`, `gn_min=20&gn_max=5`, `last_n=5&gn_min=3`, `date_from=2026-02-01&date_to=2026-01-01`, `home_away=both`, `stat=net_rtg:gt:0`, `foo=1`, `season=1999` -> `api_error` with status 400 and the right `param`. A valid full query -> the `p` list with typed fields equal to what Shiny builds for preset `il_summary_opps_statfilter`. `test-api-reference.R` (replay): `/v1/il/teams?season=2026` equals `fetch_teams_min(2026)`; `/v1/meta/data-version` changes when the recorded `app_meta` value changes (Review Focus 5: two fixture dirs with different versions give different cache keys and both answers are served correctly).
- [ ] **Step 2: Run** `"$RSCRIPT" -e "testthat::test_dir('api/tests/testthat')"` -- FAIL.
- [ ] **Step 3: Implement** the files above. Plumber routes call handler functions (`handle_teams(req, res, league, season)`), so tests call handlers directly.
- [ ] **Step 4: Run** -- PASS. Then a smoke run: `callr::r_bg` starts `api/run.R` on port 3010 with replay fixtures (`IBPL_API_REPLAY_DIR`), `httr2` GETs `/v1/il/teams?season=2026` -> 200 JSON, `/v1/il/teams?season=x` -> 400 JSON `{error, param}`.
- [ ] **Step 5: Commit** `api: Plumber skeleton, parameter validation, reference endpoints`.

### Task 9: `GET /v1/{league}/game-logs`

**Files:**
- Modify: `api/plumber.R`; create `api/R/handlers_game_logs.R`, `api/R/columns.R`
- Test: `api/tests/testthat/test-api-game-logs.R`

**Interfaces:**
- Consumes: `svc_game_logs_params`, `svc_game_logs`, `api_respond`.
- Produces:
  - `handle_game_logs(req, res, league)` -> response contract (design § Response contract): `rows` (snake_case keys: `Off Shot` -> `off_shot_fga`, `Def Shot` -> `def_shot_fga`; `has_scores` boolean instead of the HTML link; `pr_*` kept), `totals = null`, `meta` from `svc_game_logs` plus `league`, `competition`, `season`, `view`, `data_version`, `columns`, `warnings = []`.
  - `api_columns(view_cols, polarity) -> list` in `columns.R`: per column `key`, `label` (from the GL header builders' labels -- implementer: read `gamelog_summary_header` / `gamelog_ff_header`), `definition` (from `COLUMN_TOOLTIPS` where a key matches, else null), `format`, `polarity` (`"higher_better"` / `"lower_better"` from the renderers' `heat_reverse` maps -- move those maps into `svc_game_logs.R` as `GL_HEAT_REVERSE_SUMMARY`, `GL_HEAT_REVERSE_FF` and have the renderers read them), `percentile_key` (`pr_<metric>` when present), `hidden` (the golden's `hidden` set).
  - `sort=<col>:<asc|desc>[,...]`; default = `meta$default_sort`; NAs last.
- [ ] **Step 1: Write failing tests** (replay): for every `GAME_LOGS_PRESETS` entry, translate `inputs` to the query string (a helper `preset_to_query(preset)` in the test file -- the only place the Shiny-input -> API-parameter mapping is written down, so a mismatch fails here), call `handle_game_logs`, and assert that on every golden column except `gameflow` (after the renames) the API rows equal the golden `data` value for value, in the same order, and that `has_scores` is present; `meta$columns` polarity of `def_ppp` is `lower_better` and `off_tov_pct` `lower_better`; `covers(preset$covers, "X-04", "X-05", "X-12", "X-13")`. Also: `il_summary_empty` -> 200, `rows` length 0, `meta$filters` echoed (Review Focus 2); `/v1/euroleague/game-logs?teams=<an IL-only id like 99999>` -> 200 with 0 rows (Review Focus 4); `format=csv` -> header row equals the visible column keys in golden order, row count equal (X-14, GL-08).
- [ ] **Step 2: Run** -- FAIL.
- [ ] **Step 3: Implement.**
- [ ] **Step 4: Run** -- PASS.
- [ ] **Step 5: Commit** `api: Game Logs endpoint, both leagues, parity with Shiny goldens`.

### Task 10: Live side-by-side parity (nightly)

**Files:**
- Create: `app/tests/testthat/test-api-parity-live-db.R`

**Interfaces:**
- Consumes: `golden_app_env`, `golden_run_server`, `golden_capture_dt`, `api_bootstrap`, `handle_game_logs`, `GAME_LOGS_PRESETS`, `preset_to_query` (move it from Task 9's test into `api/tests/testthat/helper-presets.R` and source it from here).
- [ ] **Step 1: Write the test:** `skip_if_not(Sys.getenv("RUN_DB_TESTS") == "1")`; one real connection on port **5432** (skip with a message if `PG_PORT` is 6543 -- the test sets 5432 itself); for each preset, Shiny capture and API response computed in the same run against the live DB; assert equal. Presets use the latest season too: add `game_year = DEFAULT_GAME_YEAR` copies of `il_summary_default` and `el_summary_default` at run time.
- [ ] **Step 2: Run locally** with `RUN_DB_TESTS=1` and nothing else querying the DB -- PASS. Report the run time.
- [ ] **Step 3: Commit** `test: nightly live parity, Shiny vs API, Game Logs`.

### Task 11: Close the slice -- gate, docs

**Files:**
- Modify: `api/coverage.R` (`API_DONE_PREFIXES <- "GL"`), `docs/shared_definitions.md` (owners for the Game Logs functions now in `svc_game_logs.R`; twins: Shiny reactive and API handler; the pin: `test-svc-game-logs.R` + `test-api-game-logs.R`), `CLAUDE.md` (Architecture: `app/R/svc_*.R` service layer, `api/` API with run command, goldens and `scripts/capture_goldens.R`; the `ibpl.pg_pool` option), `docs/superpowers/specs/2026-10-19-api-mcp-feature-inventory.md` (GL rows: "Where it runs" -> `svc_game_logs.R`)
- [ ] **Step 1: Run** `test-api-coverage-gate.R` -- PASS with GL done; temporarily remove one `covers("GL-06")` and confirm it FAILS naming GL-06; restore.
- [ ] **Step 2: Run** the full fast suite and `api/tests` -- PASS.
- [ ] **Step 3: Commit** `docs: Game Logs served by the shared layer and the API`. Hand the three branches to the user for review and merge (Shiny-affecting branch `shiny/svc-game-logs` deploys on merge; ask before merging).
