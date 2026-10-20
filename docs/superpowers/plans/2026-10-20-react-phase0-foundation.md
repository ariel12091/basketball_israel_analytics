# React migration Phase 0: foundation, Game Logs end to end, load and consistency proofs -- Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Stand up the Next.js app, its API and its database surface; serve Game Logs (both leagues) identical to Shiny; and prove the riskiest assumptions (publication consistency, connection behaviour, load on the expensive queries) before any other tab is migrated.

**Architecture:** Three parts, each ending in working, tested software. **A** (database): an `app_web` login role, an `app_api` schema of read functions, and publication markers with single-writer ownership wired into both loaders. **B** (web): a Next.js 16 app in `web/` whose route handlers are the only front door, reading Postgres through `pg` with per-request deadline budgets, CDN caching keyed by the publication version, and Game Logs ported from `app/R/svc_game_logs.R` to a shared TypeScript module. **C** (proof): concurrency, freshness and load tests that close the review gaps G1-G12 named in the spec.

**Tech Stack:** Next.js 16 (App Router), React, TypeScript, `pg` (node-postgres), `zod`, `nuqs`, TanStack Table + Virtual, Vitest, Playwright, k6 (load); PostgreSQL 15 on Supabase; R 4.4.2 (ETL, goldens); Python (EuroLeague loader); Vercel Hobby.

**Spec:** `docs/superpowers/specs/2026-10-20-react-migration-design.md` (approved 2026-10-20). Feature IDs: `docs/superpowers/specs/2026-10-19-api-mcp-feature-inventory.md`. Contract: `docs/shared_definitions.md`.

## Global Constraints

- Shiny keeps working unchanged (decision 16). New SQL is additive in `app_api`; no function or view Shiny calls is altered.
- Every number equals Shiny's for the same filters, value for value, against the goldens in `app/tests/goldens/` (completed seasons: Israeli `2026`, EuroLeague `2025`). A known defect is ported as-is (D24, D25) and stays registered.
- One owner per rule (decision 12): Postgres computes anything needing raw play-by-play; one shared TypeScript module (`web/src/rules/`) derives from returned rows; each TS rule names its R source; rounding goes through one function reproducing R's `round()` (halves to even).
- Data access only from server code, as login role `app_web` (member of `app_readonly`), with `pg`. Never `postgres.js`, never Supabase's Data API, never a credential in the browser or the repo.
- Request deadline 5 s: about 0.5 s local queue, 0.5 s connection/first exchange (including pooler wait), at most 3 s SQL (`SET LOCAL statement_timeout` from the remaining budget), remainder for processing and transfer. `app_web` role defaults: `statement_timeout = '3s'`, `idle_in_transaction_session_timeout = '5s'`.
- Cache rule: long `s-maxage` only when the snapshot says `published` and its version equals the URL's `v`; about 60 s otherwise; a stale `v` redirects to the current one, uncached. Paged downloads only in `published`.
- Every writer of served data claims its league with an advisory lock held on a session connection (direct or port 5432), fences every write transaction on ownership, and finalizes through `finish_publication`.
- Request caps from the 2026-10-20 security audit carry over (raw query ≤ 4096 bytes; ≤ 50 ids per list; ≤ 5 sort keys; stat filters ≤ 2 x filterable columns of the view); CSV cells defused per `docs/shared_definitions.md` § 3.
- One responsive design; every UI test runs at phone (390 px) and desktop (1440 px) widths.
- Live DB access from scripts/tests: port 5432, read-only unless a task says otherwise. Any production DB write (role, schema, functions, markers) is applied by a dry-run-by-default script that needs an explicit `CONFIRM_*=1`, run by the user or with the user's go-ahead.
- npm/npx run through PowerShell, not the Bash tool (path mangling). Files with backslashes are written with Write/Edit, never heredocs.
- Every commit message ends with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`; never push or merge without the user.
- No paged downloads are needed in Phase 0 if every season download fits 4.5 MB (Task 5/10 measure it); if one does not, paging follows the spec's rule (only in `published`, every page carries `(state, version)`, the client restarts on mismatch) and Task 14's paged-download test applies.
- Branches (short-lived, user merges): `sql/web-foundation` (Tasks 1-5), `react/foundation` (6-12), `infra/phase0-proofs` (13-16).

## Review Focus

1. **Empty or partial season** (a league or season with no games yet, or a game present in the schedule without lineup rows): the API must return 200 with zero or partial rows and correct totals, never 500 or a percentile computed over NaN. Pinned in Task 9 and Task 10.
2. **A request arriving exactly while the ETL commits**: the response must carry the snapshot's own `(state, version)` and therefore the short cache; never a long-cached mix. Pinned in Task 14.
3. **Phone user on a slow connection** opening the largest season download: the page must show a loading state and stay usable; the response must stay under 4.5 MB. Pinned in Task 11 and Task 16.
4. **Two leagues' ids colliding** (an Israeli team id used on `/api/v1/euroleague/...`): 200 with no rows or 400, never another league's team. Pinned in Task 10.
5. **A dropped connection mid-request** (pooler restarts, network blip): the request fails within its deadline with 503 and the client connection is destroyed, not returned to the pool. Pinned in Task 7.

---

## Part A -- Database foundation (`sql/web-foundation`)

### Task 1: `app_web` role and `app_api` schema under the security scripts

**Files:**
- Create: `sql/security/web_role.sql`, `sql/app_api/000_schema.sql`, `scripts/apply_web_foundation.R`
- Modify: `sql/security/enable_readonly_rls.sql`, `sql/security/audit_app_access.sql`, `scripts/apply_db_security.R`
- Test: `app/tests/testthat/test-web-role-db.R`, `app/tests/testthat/test-db-security-contracts.R`

**Interfaces:**
- Produces: login role `app_web` (`LOGIN`, `IN ROLE app_readonly`, `CONNECTION LIMIT` from env `APP_WEB_CONNECTION_LIMIT`, default 10; `ALTER ROLE app_web SET statement_timeout = '3s'`, `SET idle_in_transaction_session_timeout = '5s'`); schema `app_api` with `USAGE` and `EXECUTE ON ALL FUNCTIONS` granted to `app_readonly` only, `REVOKE ALL ... FROM PUBLIC, anon, authenticated`. Password never in SQL: the apply script reads `APP_WEB_PASSWORD` from env and issues `ALTER ROLE ... PASSWORD` through a bound parameter-free `format('%L')` call executed server-side.
- `scripts/apply_web_foundation.R`: dry run prints the statements; `CONFIRM_WEB_FOUNDATION_APPLY=1` applies; idempotent; ends by running the security re-apply.

- [ ] **Step 1: Write the failing contract tests** in `test-db-security-contracts.R` (static, no DB): `web_role.sql` creates `app_web` with `IN ROLE app_readonly` and both role-default timeouts; the audit SQL lists `app_api` among checked schemas; no SQL file grants anything on `app_api` to `anon`, `authenticated` or `PUBLIC`.
- [ ] **Step 2: Write the failing DB test** `test-web-role-db.R` (`RUN_DB_TESTS=1`, connects as `app_web` with `APP_WEB_PASSWORD`, port from `PG_PORT` forced to 5432): `SHOW statement_timeout` is `3s`; `SELECT` on `basketball_test.final_schedule_mv` succeeds; `INSERT INTO basketball_test.app_meta` fails with permission denied; calling a function in `basketball_test` that is not on the allowlist fails; `SET ROLE anon` / `SET ROLE service_role` fail.
- [ ] **Step 3: Run both** -- FAIL (files and role missing).
- [ ] **Step 4: Implement** the SQL files and apply script; extend `apply_db_security.R`, the hardening SQL and the audit to cover `app_api` -- its functions are granted through the same enumerated allowlist mechanism as `basketball_test` (review R9), so a re-apply keeps them.
- [ ] **Step 5: Apply** -- dry run, show the user, then `CONFIRM_WEB_FOUNDATION_APPLY=1` with the user's go-ahead. Run both tests -- PASS. (This is the identity proof, gap G3.)
- [ ] **Step 6: Commit** `sql: app_web login role and app_api schema under the security scripts`.

### Task 2: Publication markers and writer ownership in SQL

**Files:**
- Create: `sql/publication/001_publication.sql`, `scripts/apply_publication.R`
- Test: `app/tests/testthat/test-publication-db.R`

**Interfaces:**
- Produces, in schema `basketball_test` (owner functions; `app_api` wrappers in Task 5):
  - table `publication(league text PRIMARY KEY CHECK (league IN ('il','euroleague','eurocup')), state text NOT NULL CHECK (state IN ('publishing','published','failed')), version bigint NOT NULL, owner_run uuid, started_at timestamptz, finished_at timestamptz)`; seeded `published`, version 1 for all three leagues.
  - `publication_lock_key(league text) RETURNS bigint` -- stable key for `pg_advisory_lock`.
  - `begin_publication(league text, run_id uuid) RETURNS void` -- requires the caller's session to hold `pg_advisory_lock(publication_lock_key(league))` (checked via `pg_locks` for `pg_backend_pid()`), else raises; if the row is `publishing` under another owner (necessarily stale, because that owner no longer holds the lock), marks it `failed` with version + 1 first; then sets `state='publishing', owner_run=run_id, started_at=now()`.
  - `fence_publication(league text, run_id uuid) RETURNS void` -- `SELECT ... FROM publication WHERE league = $1 AND owner_run = $2 AND state = 'publishing' FOR SHARE`; raises `fenced_out` if no row. Called first in every write transaction.
  - `finish_publication(league text, run_id uuid, ok boolean) RETURNS bigint` -- one `UPDATE ... WHERE league = $1 AND owner_run = $2 AND state = 'publishing'` setting `state = CASE WHEN ok THEN 'published' ELSE 'failed' END, version = version + 1, finished_at = now()`; raises if 0 rows; returns the new version.
  - `publication_status(league text) RETURNS TABLE(state text, version bigint)` -- reports `stale` when `state = 'publishing'` and no session holds the league's advisory lock.
- Grants (review R9): the security hardening revokes every function grant in `basketball_test`
  and restores only its enumerated allowlist (`sql/security/enable_readonly_rls.sql`), so a grant
  made by this migration alone would be removed by the next re-apply. `publication_status` and
  every read-only helper it calls as invoker (`publication_lock_key`) are therefore added to the
  `app_readonly` allowlist **and** to the independent audit allowlist
  (`sql/security/audit_app_access.sql`). `begin_publication`, `fence_publication` and
  `finish_publication` stay off both allowlists -- EXECUTE only for the ETL/loader role(s).
- **Test isolation (review R3):** the SQL file is written with a `__SCHEMA__` placeholder;
  `scripts/apply_publication.R` deploys it to `basketball_test` (live) and, for tests, to a
  schema `pubtest_<random>` created and dropped by the test (`DROP SCHEMA ... CASCADE` in
  teardown). The same function definitions run against isolated relations; the live CHECK
  constraint is never widened. Tests assert afterwards that no row in
  `basketball_test.publication` changed (compare before/after).

- [ ] **Step 1: Write failing DB tests** (`test-publication-db.R`, two separate connections on 5432 as the ETL role, against a `pubtest_<random>` schema deployed from the same SQL file): (a) `begin` without the advisory lock raises; (b) run A holds lock + begins, run B cannot acquire the lock (`pg_try_advisory_lock` false); (c) A's write transaction calls `fence` then commits; (d) A's connection closes -> `publication_status` reports `stale`; B acquires lock, `begin` marks A's attempt `failed` (version + 1), B publishes; (e) A's later `fence` raises `fenced_out` and `finish(A, ...)` raises; (f) an in-flight A write transaction holding the `FOR SHARE` lock delays B's `begin` until A commits; after B's `begin`, no A write can commit; (g) `finish(..., ok = false)` gives `failed` and version + 1; (h) a run that never begins changes nothing.
- [ ] **Step 2: Run** -- FAIL.
- [ ] **Step 3: Implement** the SQL and the dry-run apply script (`CONFIRM_PUBLICATION_APPLY=1`).
- [ ] **Step 4: Apply with the user's go-ahead; run** -- PASS.
- [ ] **Step 5: Commit** `sql: publication markers with advisory-lock liveness and fenced writes`.

### Task 3: Israeli ETL and repair scripts publish through the markers

**Files:**
- Create: `etl/publication.R`
- Modify: `etl/etl_full.R`, `etl/etl_onoff.R` (only where write transactions begin), `scripts/restage_matview.R`, `scripts/refresh_israeli_traditional_season_mv.R`, `scripts/finish_canonical_segment_minutes_refresh.R`, `.github/workflows/etl-full.yml`
- Test: `app/tests/testthat/test-etl-publication.R` (static + mock), `app/tests/testthat/test-etl-publication-db.R`

**Interfaces:**
- Consumes: Task 2 functions.
- Produces (`etl/publication.R`): `publication_open(league = "il", port = Sys.getenv("PUBLICATION_LOCK_PORT", "5432"), schema = "basketball_test") -> list(con, run_id, league, schema)` opens a dedicated session connection, takes `pg_advisory_lock`, calls `begin_publication`; `publication_fence(pub, con)` runs `fence_publication` on the given write connection (must be inside its open transaction); `publication_fenced_tx(pub, con, expr)` wraps `expr` in `dbBegin` + fence + `dbCommit` (rollback on error); `publication_close(pub, ok)` calls `finish_publication` and disconnects; `with_publication(league, fn)` for scripts.
- **Write inventory first (review R1):** list every statement in `etl/etl_full.R`, `etl/etl_onoff.R` and the helpers they call that writes a relation the app serves -- explicit transactions, autocommit `dbExecute`/`dbWriteTable` (e.g. Phase 3's sub-lineup `dbWriteTable` calls, the shot-zone writes), and refresh paths that run with no new games (e.g. `force_full_sub_lineup_stats`). Record it in `docs/etl_publication_writes.md`. Every autocommit write of served data is wrapped in a fenced transaction (`publication_fenced_tx(pub, con, expr)`).
- `etl_full()`: opens the publication **whenever any inventoried write is about to run** -- including a no-new-game refresh -- and not on dry runs or runs that make no relevant change; every write transaction calls `publication_fence` first; ends with `publication_close(pub, ok = run_success)` after Phase 6 validation and before Phase 7, where `run_success <- isTRUE(pipeline_ok) && length(unique(failed_base_ids)) == 0` -- the same condition the run already uses for `etl_full_last_success` (review R8); every downstream validation failure that sets `pipeline_ok` therefore also prevents `published`. A crash leaves the lock released by connection loss (stale).
- Workflow: `concurrency: { group: etl-full, cancel-in-progress: false }`.

- [ ] **Step 1: Write failing static tests** (`test-etl-publication.R`): every write in `docs/etl_publication_writes.md` sits inside `publication_fenced_tx(` or a block whose `dbBegin` is followed by `publication_fence(`; no autocommit write of a served relation remains; `publication_close(` appears after the Phase 6 block and before Phase 7; the workflow has the concurrency group; each listed repair script wraps its write in `with_publication(`. Static checks are supporting evidence only (review R1).
- [ ] **Step 2: Write the failing DB tests** (`test-etl-publication-db.R`, `pubtest_<random>` schema): `with_publication` where `fn` commits a fenced write -> `published`, version + 1; `fn` errors after a fenced commit -> `failed`, version + 1; closing the lock connection -> `stale`; **an autocommit-style write converted to `publication_fenced_tx` is rejected after takeover**; **a mixed run -- one game committed, one base-game load failed and rolled back -- advances the version and ends `failed`, never `published`** (review R8); an all-success run ends `published`; a run with no relevant write leaves state and version untouched; a downstream (Phase 6) validation failure ends `failed`; **the no-new-game refresh path opens and finalizes a publication** (run `etl_full`'s refresh branch against the test schema with `force_full_sub_lineup_stats = TRUE` on a fixture, or the extracted refresh function); **removing the fence from one wrapper makes a test fail** (mutation check, reverted).
- [ ] **Step 3: Run** -- FAIL.
- [ ] **Step 4: Implement**; for `etl_full.R`, splice on raw bytes (mixed line endings) and confirm `git diff --stat` shows only the intended lines.
- [ ] **Step 5: Run both tests and the fast suite** -- PASS. Run one `etl_full(dry_run = TRUE)` locally: no publication opened (logged).
- [ ] **Step 6: Commit** `etl: Israeli writers publish through fenced publication markers`.

### Task 4: EuroLeague loader publishes through the markers

**Files:**
- Create: `euroleague/src/euroleague_possessions/publication.py`
- Modify: the write path behind `euroleague/scripts/load_new_games.py` (`postgres_backend.py` / `transaction_writer.py` -- wherever a transaction that writes served `euroleague.*` data begins), `.github/workflows/euroleague-load.yml` (keep its existing concurrency group)
- Test: `euroleague/tests/test_publication.py`

**Interfaces:**
- Produces: `Publication.open(conn_factory, league)` (league = `"euroleague"` for competition `E`, `"eurocup"` for `U`), `.fence(cur)`, `.close(ok)`, and context manager `publishing(conn_factory, league)`; same semantics as Task 3 (session connection on port 5432 for the lock; fence first in every write transaction; finish after the loader's verification step).

- [ ] **Step 1: Write failing tests** (pytest, live DB gated on `RUN_DB_TESTS=1`, scratch league): same cases as Task 3 Step 2, plus a static test that every write transaction in the loader calls `fence` first.
- [ ] **Step 2: Run** -- FAIL. **Step 3: Implement.** **Step 4: Run + the loader's existing test suite** -- PASS.
- [ ] **Step 5: Commit** `euroleague: loader publishes through fenced publication markers`.

### Task 5: `app_api` read functions for Game Logs and the version

**Files:**
- Create: `sql/app_api/010_version.sql`, `sql/app_api/020_game_logs.sql`
- Modify: `scripts/apply_web_foundation.R` (deploys `sql/app_api/*.sql` in order, then security re-apply)
- Test: `app/tests/testthat/test-app-api-game-logs-db.R`

**Interfaces:**
- Produces:
  - `app_api.version() RETURNS TABLE(league text, state text, version bigint)` -- one row per league, from `publication_status`.
  - `app_api.game_logs_il(p_season int) RETURNS TABLE(... schedule columns ..., type_lineup text, num_starters int, <summed counts>, row_count bigint, ctl_poss numeric, state text, version bigint)` -- one statement: the season's `final_schedule_mv` rows joined to `mv_lineup_totals_by_day` and `lineup_four_factors_by_game` aggregated to `(game_id, team_id, type_lineup, num_starters)` (sums of every count column `gl_build_summary_metrics` / `gl_build_ff_metrics` sum, minutes as `numeric`), with `count(*) OVER ()` and `sum(total_poss) OVER ()` as completeness controls and `(state, version)` from `publication_status('il')` in the same statement.
  - `app_api.game_logs_euro(p_competition text, p_season int)` -- **a native EuroLeague row contract, not the Israeli one (review R10):** exactly the rows and columns `euro_fetch_game_log_rows()` (`app/R/global_euro.R`) returns -- one row per `team_four_factors_by_game` record with both `own_starters` and `opp_starters`, the off/def count columns side by side, schedule columns and `minutes` -- with no aggregation and no offense/defense expansion, because EuroLeague's Last-N (D24) counts these native rows. Rows are returned in the order that query produces; since that query has no `ORDER BY`, the tie order inside a game is unspecified in SQL -- the function adds `ORDER BY` on the physical row order Shiny's query yields today (verified against the goldens), and any remaining ambiguity is registered next to D24. League `euroleague`/`eurocup` by competition.
  - `app_api.scoreless_games_il(p_season int)` -- the query behind `fetch_scoreless_games()`.
  All `STABLE`, `SET plan_cache_mode = force_custom_plan`, `SET search_path = ''` with qualified names; the `(state, version)` join makes each call one snapshot.
- **Envelope for empty results (review R6):** each reader returns **one JSON value** -- `jsonb_build_object('state', ..., 'version', ..., 'row_count', ..., 'ctl_poss', ..., 'rows', coalesce(jsonb_agg(...), '[]'))` -- built in the same statement from one CTE over the filtered fact, so metadata and controls exist even with zero rows and no second scan is added. Column lists above describe the objects inside `rows`.

- [ ] **Step 1: Write failing DB tests** (`RUN_DB_TESTS=1`, as `app_web`): a season with no games returns `rows = []`, `row_count = 0` and a `(state, version)`; for season 2026, `sum` of each count column per `(game_id, team_id, type_lineup)` from `game_logs_il` equals the same sums over the raw `mv_lineup_totals_by_day` / `lineup_four_factors_by_game` season pull (the Shiny inputs); `row_count` equals `nrow`; the starters split loses nothing (sum over `num_starters` equals the unsplit sum); for EuroLeague 2025, the returned native rows equal the replayed Shiny input frame row for row, in order (review R10); `version()` returns three rows; calls as `app_web` succeed, as `anon` fail.
- [ ] **Step 2: Run** -- FAIL. **Step 3: Implement and deploy with the user's go-ahead**, then run the security re-apply (`CONFIRM_DB_SECURITY_APPLY=1`). **Step 4: Run** -- PASS; then **run the hardening once more and re-run** (review R9): `app_api.version()` and a Game Logs reader still succeed as `app_web`, and `begin_publication` / `fence_publication` / `finish_publication` still fail as `app_web`. Record the payload size of each season result serialized as JSON (`length(jsonlite::toJSON(...))`) in the test output.
- [ ] **Step 5: Commit** `sql: app_api Game Logs readers and version with same-snapshot completeness`.

## Part B -- Web foundation and Game Logs (`react/foundation`)

### Task 6: Next.js project and CI

**Files:**
- Create: `web/` (Next.js 16 App Router, TypeScript strict, ESLint, Vitest, Playwright), `web/README.md`, `.github/workflows/web-tests.yml`
- Test: `web/src/app/page.test.tsx`, `web/e2e/smoke.spec.ts`

**Interfaces:**
- Produces: `npm run lint`, `npm run typecheck`, `npm test` (Vitest), `npm run e2e` (Playwright, projects `phone` 390x844 and `desktop` 1440x900), `npm run build`; dependencies pinned to exact versions in `package.json`.

- [ ] **Step 1: Scaffold** with `create-next-app` (PowerShell), add Vitest, Playwright, `pg`, `zod`, `nuqs`, `@tanstack/react-table`, `@tanstack/react-virtual`, `server-only`.
- [ ] **Step 2: Write tests**: the home page renders the title "IBPL Analytics" (unit); `/` loads at both widths with no console errors (e2e).
- [ ] **Step 3: Run all four commands** -- PASS. The CI workflow runs lint, typecheck, unit tests and build on PRs touching `web/`.
- [ ] **Step 4: Commit** `web: Next.js 16 scaffold with lint, types, unit and e2e tests`.

### Task 7: Database client with deadline budgets and snapshots

**Files:**
- Create: `web/src/server/db.ts`, `web/src/server/deadline.ts`
- Test: `web/src/server/db.test.ts` (unit, fake server), `web/src/server/db.int.test.ts` (live, `RUN_DB_TESTS=1`)

**Interfaces:**
- Produces (all `server-only`):
  - `class Deadline { constructor(totalMs = 5000); remaining(): number; expired(): boolean }` and budgets `QUEUE_MS = 500`, `CONNECT_MS = 500`, `SQL_MAX_MS = 3000`.
  - `withSnapshot<T>(deadline: Deadline, fn: (q: Query) => Promise<T>): Promise<T>` -- waits in a per-instance FIFO queue (max length `DB_QUEUE_MAX`, default 20; rejects with `BusyError` when full or when `QUEUE_MS` elapses; a queued entry whose deadline has passed is removed and never started); checks out a client from a `pg.Pool` (`max = DB_POOL_MAX`, default 2; `connectionString` from server env `DATABASE_URL_WEB`); runs `BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY` under a timer of `CONNECT_MS` covering the whole first exchange; then runs `fn` and `COMMIT`. **One overall timer (review R2)** covers checkout, setup, `fn` and commit: when the request deadline passes, the client is destroyed (`release(true)`, which closes the socket even if the server has stopped responding) and the caller gets 504/503; nothing for that request runs afterwards. Any error destroys the client.
  - `type Query = <R>(sql: string, params: unknown[]) => Promise<R[]>` -- parameterised only. **Before each query** the wrapper sets `SET LOCAL statement_timeout` to `min(SQL_MAX_MS - sqlTimeUsed, deadline.remaining() - 500)` and refuses to issue the query (throws `DeadlineError`) when that is ≤ 0, so several fast statements cannot together exceed either the request or the database-work budget.
  - `BusyError` maps to HTTP 503 with `Retry-After: 2`; `statement_timeout` errors map to 504.

- [ ] **Step 1: Write failing unit tests** with a fake `pg` server (a TCP server speaking just enough protocol, or a mocked `Pool`/`Client`): queue overflow -> `BusyError`; queued past `QUEUE_MS` -> dropped, `fn` never called; first exchange slower than `CONNECT_MS` -> client destroyed (`release(true)`), `BusyError`; per-query `SET LOCAL` value shrinks as SQL time is used and as the deadline approaches; **four statements of 1 s each against a 3 s SQL budget -> the fourth is never issued (`DeadlineError`)**; **a connection that silently stops responding mid-`fn` (no FIN) -> the overall timer destroys the client and the request ends by its deadline**; a clean disconnect mid-`fn` -> client destroyed, error within the deadline (Review Focus 5).
- [ ] **Step 2: Write failing integration tests** (live, port 5432 and 6543 both, as `app_web`): `withSnapshot` returns rows from `app_api.version()`; `SHOW statement_timeout` inside the snapshot equals the computed value; two queries in one snapshot see the same `txid_current_snapshot()`.
- [ ] **Step 3: Run** -- FAIL. **Step 4: Implement.** **Step 5: Run** -- PASS.
- [ ] **Step 6: Commit** `web: database client with deadline budgets, bounded queue and read-only snapshots`.

### Task 8: API conventions -- validation, versioned caching, version endpoint, CSV

**Files:**
- Create: `web/src/server/api.ts`, `web/src/server/params.ts`, `web/src/server/csv.ts`, `web/src/app/api/v1/version/route.ts`
- Test: `web/src/server/api.test.ts`, `web/src/server/params.test.ts`, `web/src/server/csv.test.ts`

**Interfaces:**
- Produces:
  - `cacheHeaders(snapshot: {state: string, version: number}, requestedV: number | null): Headers` -- `Vercel-CDN-Cache-Control: s-maxage=31536000` and `Cache-Control: public, max-age=0` when `state === 'published' && version === requestedV`; else `s-maxage=60`.
  - `versionRedirect(url: URL, current: number): Response | null` -- 307 to the same URL with `v=current`, `Cache-Control: no-store`, when `v` is missing or differs.
  - `apiError(status: 400|404|503|504, param: string | null, message: string): Response` -- JSON `{error, param}`, never SQL or stack.
  - `rawQueryGuard(url: URL)` -- rejects > 4096 bytes before decoding (400 `query`); rejects undecodable or non-UTF-8 names/values, empty names, repeated names (400 naming the param).
  - `defuseCsvCell(s: string): string` -- leading `'` when the first character is one of `= + - @ \t \r`, text cells only; `toCsv(rows, columns)`.
  - `GET /api/v1/version` -> `{ leagues: [{league, state, version}] }`, `s-maxage=60`.
- Port the rules of `app/R/svc_params.R` and `svc_game_logs_params()` (`app/R/svc_game_logs.R`) to zod schemas in `params.ts` with the same caps and messages; their R tests (`api/tests/testthat/test-api-params.R`, `test-api-limits.R`) are the case list.

- [ ] **Step 1: Write failing tests**: every case of `test-api-params.R` and `test-api-limits.R` translated (same params, same 400 `param` names); cache headers for `(published, v)`, `(publishing, v)`, `(published, v-1)`; redirect for a missing and a stale `v`; CSV defusing for `=1+1`, `+x`, `-x`, `@x`, tab, CR, and a numeric `-3.2` left alone; `defuseCsvCell` matches the R `csv_defuse_formula()` on the same 18 inputs used by `test-csv-formula-defuse.R`.
- [ ] **Step 2: Run** -- FAIL. **Step 3: Implement.** **Step 4: Run** -- PASS.
- [ ] **Step 5: Commit** `web: API conventions -- validation caps, versioned caching, version endpoint, CSV defusing`.

### Task 9: Shared rules module -- rounding and Game Logs

**Files:**
- Create: `web/src/rules/round.ts`, `web/src/rules/gameLogs.ts`, `web/src/rules/statFilters.ts`, `scripts/export_goldens_json.R`, `web/test/goldens/game_logs/*.json`
- Test: `web/src/rules/round.test.ts`, `web/src/rules/gameLogs.test.ts`

**Interfaces:**
- Consumes: Task 5's **two contracts** -- Israeli `IlGameLogRow` (one row per `(game_id, team_id, type_lineup, num_starters)` plus schedule columns) and EuroLeague/EuroCup `EuroNativeRow` (one row per native record with `own_starters`, `opp_starters`, paired off/def counts) -- as distinct TypeScript types (review R10).
- Produces (pure, no I/O), each with a doc comment naming its R source in `app/R/svc_game_logs.R` / `app/R/helpers.R`:
  - `rRound(x: number, digits: number): number` -- R's `round()`.
  - `percentRank(values: (number|null)[]): (number|null)[]` -- dplyr `percent_rank` (min rank, `(rank-1)/(n-1)`, NA kept).
  - `filterGames(rows, f: GameLogFilters, league: 'il'|'euroleague'|'eurocup')` -- `gl_filter_games` incl. the EuroLeague Last-N row-slicing defect (D24) and blank-date-end behaviour (D25). For EuroLeague/EuroCup it runs on `EuroNativeRow[]` **before** any expansion or aggregation.
  - `buildEuroGames(native: EuroNativeRow[], startersBounds, applyStarters)` -- `gl_build_euro_games`: converts the retained native rows into offense/defense metric rows (own starters on offense, opponent starters on defense) and builds the per-game metrics. The EuroLeague sequence is exactly Shiny's: filter native rows -> `buildEuroGames` -> attach percentiles against `buildEuroGames(fullSeasonNative, apply_starters = false)`.
  - `buildSummaryMetrics`, `buildFfMetrics`, `attachPercentiles(display, baseline, metrics)`, `joinScheduleInfo`, `summaryDisplay(df, statFilters, scoreless)`, `ffDisplay`, `shotAverages(disp)` -- `gl_build_summary_metrics`, `gl_build_ff_metrics`, `gl_attach_percentiles`, `gl_join_schedule_info`, `gl_summary_display`, `gl_ff_display`, `gl_shot_averages`; starters bounds applied on the `num_starters` column.
  - `applyStatFilters(rows, filters)` -- `apply_stat_filters`.
  - `gameLogs(rows, scoreless, p: GameLogParams): {rows, meta}` -- composes them as `svc_game_logs()` does.
- `scripts/export_goldens_json.R`: converts each `app/tests/goldens/out/game_logs/<preset>.rds` and its replayed input frames (EuroLeague presets: the native `euro_fetch_game_log_rows()` frame, in its row order) into `web/test/goldens/game_logs/<preset>.json` (`{preset, params, input: {rows, scoreless}, expected: {data, order, hidden}}`); numbers written with `digits = NA` so no precision is lost.

- [ ] **Step 1: Write failing tests**: `rRound` -- `0.5 -> 0`, `1.5 -> 2`, `2.5 -> 2`, `-0.5 -> 0`, `0.15` and `2.675` to 2 digits equal R's results (taken from R); `percentRank` equals dplyr on `[3,1,NA,3,2]`; for each of the 15 golden presets, `gameLogs(input, params)` rows equal `expected.data` on every golden column except `gameflow`, in order (exact `===` per value); an empty input (Review Focus 1) gives zero rows and `null` shot averages without NaN; **a EuroLeague fixture** with several own/opponent starter combinations for one game and a Last-N cutoff falling inside that game's rows -- retained native rows and final values equal Shiny's, alone and with home/away, for EuroLeague and EuroCup; the test fails if expansion or starter aggregation is moved before Last-N (mutation check, reverted) (review R10).
- [ ] **Step 2: Export the goldens** (`Rscript scripts/export_goldens_json.R`) and **run** -- FAIL.
- [ ] **Step 3: Implement** (behaviour ported, typed functions over row arrays -- no dplyr-style chains).
- [ ] **Step 4: Run** -- PASS on all 15 presets. Any value that cannot match: stop and report the preset, column and both values; do not adjust the golden.
- [ ] **Step 5: Update `docs/shared_definitions.md`**: each Game Logs rule now owned by `web/src/rules/gameLogs.ts` for the new app (twin: the R function, kept by Shiny under decision 16; pin: `gameLogs.test.ts` against the goldens).
- [ ] **Step 6: Commit** `web: shared rules -- R rounding and Game Logs, exact against the Shiny goldens`.

### Task 10: Game Logs routes

**Files:**
- Create: `web/src/app/api/v1/[league]/game-logs/route.ts`, `web/src/server/gameLogsRepo.ts`
- Test: `web/src/app/api/v1/[league]/game-logs/route.int.test.ts`

**Interfaces:**
- Consumes: `withSnapshot` (Task 7), `cacheHeaders`, `versionRedirect`, `rawQueryGuard`, params schema (Task 8), `app_api.game_logs_*` (Task 5).
- Produces: `GET /api/v1/{il|euroleague|eurocup}/game-logs?season=&v=` -> `{rows, scoreless, meta: {league, season, state, version, row_count, columns}}` -- the **season download** the browser filters (decision 14); `format=csv` not here (CSV is of the displayed table, Task 11). `gameLogsRepo.season(q, league, season)` checks `row_count` and `ctl_poss` from the same execution against the returned rows and throws (500, logged) on mismatch (gap G7).

- [ ] **Step 1: Write failing integration tests** (live, `RUN_DB_TESTS=1`, the route handler invoked in-process): for `il` 2026 and `euroleague` 2025, response rows passed through `gameLogs()` reproduce each preset's golden; `v` missing -> 307; stale `v` -> 307 no-store; `published` + matching `v` -> long `s-maxage`; an Israeli-only team id on `/euroleague/` yields no rows (Review Focus 4); a season with no games -> 200, empty rows, `meta.state`/`meta.version` present, and (review R6) with a current `v` the published long cache, with a stale `v` a 307, with a non-published state the 60 s cache (Review Focus 1); each response's JSON size is logged and below 4.5 MB.
- [ ] **Step 2: Run** -- FAIL. **Step 3: Implement.** **Step 4: Run** -- PASS.
- [ ] **Step 5: Nightly live parity (spec, Proof 5):** a Node script `web/test/parity/game-logs-live.ts` fetches the current season (Israeli `DEFAULT_GAME_YEAR`, EuroLeague `euro_default_season()`) through `gameLogsRepo` and `gameLogs()` for the two default presets; `app/tests/testthat/test-web-parity-live-db.R` captures Shiny for the same presets with the existing harness and compares with the script's JSON output. Runs in the nightly `r-tests` job (`RUN_DB_TESTS=1`).
- [ ] **Step 6: Commit** `web: Game Logs season routes, versioned and completeness-checked; nightly parity`.

### Task 11: Game Logs page

**Files:**
- Create: `web/src/app/[league]/game-logs/page.tsx`, `web/src/components/table/DataTable.tsx`, `web/src/components/filters/*`, `web/src/client/useVersion.ts`, `web/src/client/useSeasonData.ts`
- Test: `web/e2e/game-logs.spec.ts`, component tests next to the components

**Interfaces:**
- Consumes: Task 9 rules, Task 10 route, `defuseCsvCell` (Task 8).
- Produces: the Game Logs screen with today's controls and columns (inventory GL-01..GL-08, X-05 subset, X-09, X-12, X-13, X-14): views Summary / Four Factors; filters as in `app/R/ui_tab4_gamelogs.R` / `ui_tab11_euro_gamelogs.R`; stat filter chips; heat colours from `pr_*` with the polarity in `GL_HEAT_REVERSE_*`; default sort date desc then GN/round desc; CSV of the visible table with defused cells; filter state in the URL via `nuqs`; season data fetched once per `(league, season, v)` and all filtering done in the browser with the shared rules; `useVersion` polls `/api/v1/version` every 60 s and on focus and refetches on change; loading skeleton while the season downloads. The Gameflow link renders as a disabled cue (Gameflow is Phase 6). **GL-06 is explicitly deferred (review R5):** its test is tagged `[GL-06][deferred:phase6]` and does not count as coverage; GL-06 stays incomplete until Gameflow ships, and the switch still requires it.
- `frontend-v2/src/features/tables/*` and filter components may be adapted; every reused piece is verified by the tests below.

- [ ] **Step 1: Write failing e2e tests** at both widths: for 6 representative presets (both leagues, both views, starters, last-N + home, stat filter, empty), set the URL filters and compare every visible cell with the golden (formatted as Shiny formats it: one decimal where `GL_ROUND_1` says, thousands separators on possessions); column order and default sort; sort toggles by best direction; CSV download content equals the visible rows with formula cells defused; reloading the URL restores the filters; slow network (Playwright throttling) shows the skeleton and the page stays interactive (Review Focus 3); a simulated version change refetches.
- [ ] **Step 2: Run** -- FAIL. **Step 3: Implement.** **Step 4: Run** -- PASS at both widths.
- [ ] **Step 5: Commit** `web: Game Logs page, browser-side filtering, versioned refresh`.

### Task 12: Preview deployment

**Files:**
- Create: `web/vercel.json` (or `next.config.ts` headers), `docs/web_deployment.md`
- Modify: `CLAUDE.md` (a short "Web app (`web/`)" section: run, test, deploy, env vars, the rules of this plan's Global Constraints)

**Interfaces:**
- Produces: a Vercel project on the Hobby plan, root `web/`, preview deployments per branch; server env `DATABASE_URL_WEB` (app_web, chosen port), `DB_POOL_MAX`, `DB_QUEUE_MAX`; nothing `NEXT_PUBLIC_` that is secret. `docs/web_deployment.md` documents credential issuance and rotation (gap G3), env vars and how to roll back.

- [ ] **Step 1: The user creates/links the Vercel project** (account action) and sets the env vars; Claude prepares the config and the doc.
- [ ] **Step 2: Deploy a preview; verify in a browser** at both widths: Game Logs renders for both leagues; `/api/v1/version` answers; response headers show `x-vercel-cache: HIT` on the second identical request.
- [ ] **Step 3: Commit** `web: preview deployment config and runbook`.

## Part C -- Proofs (`infra/phase0-proofs`)

### Task 13: Connection behaviour under concurrency (G6, G8, G10)

**Files:**
- Create: `web/test/load/concurrency.int.test.ts`, `docs/web_connection_budget.md`

**Interfaces:**
- Consumes: Task 7 client against the preview database settings.

- [ ] **Step 1: Write the tests** (live, quiet hours, user informed). **Correctness and overload are separate proofs (review R7):** (a) row ownership of admitted requests -- concurrency kept within capacity (`DB_POOL_MAX` x instances, queue not overflowing), each request running a query whose result embeds its own request id (`SELECT $1::text AS rid, ...`), including two queries inside one snapshot; every response is a 200 carrying only its own id and the complete expected rows; run against port 6543 and 5432; (a2) overload -- 200 concurrent requests against the same limits: expected outcomes are 200s with correct rows **or prompt 503s** (within `QUEUE_MS` + margin); a rejected request never executes later (`pg_stat_activity` and a server-side request log show no work for its id); accepted work drains within its bound. These 503s are expected here; in Task 15's acceptance load test 503s remain failures; (b) full pool -- hold all `app_web` backend slots with sleeping sessions, send new requests: each ends by its deadline with 503/504, and after release `pg_stat_activity` shows no leftover `app_web` work; (c) superseded queued requests never start after their deadline; (d) multi-instance burst -- 4 Node processes x 20 concurrent cache-miss requests, observing client and backend connection counts (`pg_stat_activity` grouped by `usename`).
- [ ] **Step 2: Run; write `docs/web_connection_budget.md`**: measured results per port; the chosen port (the one that passes (a)); the budget -- `app_web` `CONNECTION LIMIT`, pooler pool size, ETL reserve, Shiny share -- checked against Supabase's formula; resulting `DB_POOL_MAX` / `DB_QUEUE_MAX`.
- [ ] **Step 3: Commit** `infra: concurrency, pool and connection-budget proofs`.

### Task 14: Publication consistency end to end (G2, G9, G11, G12)

**Files:**
- Create: `app/tests/testthat/test-publication-boundaries-db.R`, `web/test/load/freshness.int.test.ts`

- [ ] **Step 1: Write the tests** against an isolated `pubtest_<random>` schema (review R3) that holds the publication functions **and** copies of the served relations the Game Logs readers use, with the `app_api` readers deployed against it from the same SQL files (schema placeholder). The **real** writer wrappers (`etl/publication.R`, the EuroLeague `publication.py`) drive the writes, and the **real** route handlers serve the reads (pointed at the test schema by a server env var used only in tests). Verify afterwards that only `pubtest_*` relations changed. Cases: a request between a Phase-4-like and Phase-6-like commit returns `publishing` and short cache headers; a run failing after a commit ends `failed`, never `published`; a killed writer reads `stale`; paused run A, takeover and publication by B, then A's fresh write and in-flight write -- neither commits after B publishes; a paged download across a commit is discarded and restarted by the client logic; two frames read in separate snapshots across a publication are rejected and re-read in one (G11); an open browser across a version change refetches (Review Focus 2); **mutation checks**: removing one fence from a wrapper, or setting long cache on a `publishing` answer, makes a test fail (reverted after).
- [ ] **Step 2: Run** -- PASS required. **Step 3: Commit** `infra: publication consistency proofs`.

### Task 15: Load test including the expensive database paths (G1)

**Files:**
- Create: `sql/app_api/090_bench.sql` (thin `app_api` wrappers for `onoff_compute`, the starters path of `get_team_four_factors_dynamic`, and the custom-clutch readers, both leagues -- read-only, used by the bench only), `web/src/app/api/v1/bench/[name]/route.ts` (a routable segment -- Next.js treats `_`-prefixed folders as private and does not route them, review R4; returns 404 unless `ENABLE_BENCH=1`, never set in production), `web/test/load/k6/*.js`, `docs/phase0_load_report.md`

- [ ] **Step 1: Write the method first** in `docs/phase0_load_report.md`: duration (10 min ramp + 20 min steady), 150 virtual users, think time 3-8 s, cache-hit mix (60% default views, 40% distinct random filter combinations), simultaneous identical requests (bursts of 20), cold start (deploy, then first request), latency measured at the client as time to last byte; errors, timeouts and 503s count as failures; pass = criterion 3 (95% <= 2 s, none > 5 s).
- [ ] **Step 2: Smoke-test the bench routes on the built and deployed preview** (review R4): 404 with `ENABLE_BENCH` unset; real results (non-empty rows, timing) with it set.
- [ ] **Step 3: Run** against the preview at a quiet hour announced to the user; Game Logs and the bench routes.
- [ ] **Step 4: Record** results, the slowest functions with `EXPLAIN (ANALYZE, BUFFERS)`, and either "criterion 3 met" or the measured capacity limits and the changes needed (function work, compute size) before Phases 2-7.
- [ ] **Step 5: Commit** `infra: Phase 0 load report`.

### Task 16: Payload budget and coverage from test results (G4, G5 groundwork)

**Files:**
- Create: `web/test/coverage/gate.ts`, `web/test/coverage/gate.test.ts`, `docs/phase0_payloads.md`

**Interfaces:**
- Produces: `npm run coverage-gate` -- reads Vitest and Playwright JSON results, maps test titles tagged `[GL-04]` etc. to inventory IDs, and fails when an ID in the phase's scope has no executed, passing, non-skipped test for each applicable league and both widths. The scope is an explicit file `web/test/coverage/scope.json` -- Phase 0: `GL-01..GL-08` required **except GL-06, listed as `deferred` with reason "Gameflow, Phase 6"**; a test tagged `deferred:` never counts toward completion; the switch scope lists all 109 IDs with no deferrals. The R source scanner stays as a traceability aid.

- [ ] **Step 1: Write failing tests** for the gate: a skipped test does not count; a failed one blocks; an ID covered only at desktop width blocks; a `deferred:` test does not complete its ID; the switch scope fails while GL-06 is deferred.
- [ ] **Step 2: Implement; run** with Phase 0's results -- GL-01..GL-05, GL-07, GL-08 covered; GL-06 reported as deferred.
- [ ] **Step 3: Measure and record payloads** (largest season per league, gzip and raw) in `docs/phase0_payloads.md` against 4.5 MB.
- [ ] **Step 4: Commit** `infra: coverage gate from test results; payload budget`. Hand all three branches to the user with the Phase 0 report.

## Review record (2026-10-20)

An implementation review raised R1-R7 (unfenced autocommit and no-new-game ETL writes; whole-request
deadlines; isolated DB tests; routable bench path; GL-06 deferred, not covered; metadata for
empty results; overload vs correctness tests). Each is folded into the affected task, marked
"review Rn".

A second review raised R8 (finalize with the run's full success condition, including failed
base games) and R9 (publication-reader grants survive the security re-apply; write functions
stay denied to the web role). Both are folded into Tasks 1, 2, 3 and 5, marked "review Rn".

A third review raised R10 (EuroLeague keeps its native starter-split rows until Last-N runs,
as D24 requires). Folded into Tasks 5 and 9, marked "review R10".
