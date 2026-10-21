# React migration -- HANDOFF (updated 2026-10-21)

## START HERE (current state, read this first)

**Restart checkpoint (user, 2026-10-21):** implementation and verification work
is saved in `d361610`. Restart notes are also in root `MEMORY.md`, `CLAUDE.md`,
`PROJECT.md` and `web/CLAUDE.md`. After restarting, recheck free disk space and
whether port 3100 is listening; do not assume a previous dev process survived.
Start only one server (`cd web; npm.cmd run dev -- --port 3100`) if needed.
No application debugging or new feature work was done in the restart-notes turn.

**Rules for this work are in `web/CLAUDE.md`** (user, 2026-10-21): features
first in the spirit of Shiny, no redesign or pixel polish (a redesign comes
later); one layout per viewport width, mobile first; idiomatic React (TanStack
Table, JSX content, server first render), with Shiny's CSS only for the look.

**State (2026-10-21, current execution; branch `sql/web-foundation`, not pushed,
not merged; nothing applied to production):**
- **Game Logs: done**, both leagues (`784face`, `7009ad0`). Unit 37/37,
  browser 40/40 (390/1440 px, 15 stored Shiny presets), live smoke 6/6.
  GL-06 Gameflow deferred. The latest preview below includes `7009ad0`.
- **On/Off: page built, both leagues** (`07795cd` API, `9f7075b` page, plus
  `ac76683`). Summary / Four Factors / Shot Profile (Israeli only),
  sidebar incl. Opponent Strength, min-poss sliders with auto-min, Net cell
  3PT luck / small-sample tags + tooltip, pivot menu, FF detail toggle, luck
  popover, stat filters, **Player Stats (ps_*) chips** via
  `GET /api/v1/{league}/player-stat-filters` (`server/playerStatFilterRepo.ts`,
  `rules/playerStats.ts`). Row pipeline is the pure `onoffTableRows()` in
  `rules/onoff.ts` (page and parity tests share it).
  **Checked by hand against production Shiny:** 2026-27 defaults in all three
  views (row counts, auto-min 180/60, first-row cells identical) and
  2025-26 "PTS/G >= 15" (27 rows both). EuroLeague FF filtered path ~9.6 s
  (same SQL as Shiny; Phase 8).
- **On/Off verification added:** 45 stored Shiny presets across IL Summary /
  Four Factors / Shot Profile and EuroLeague Summary / Four Factors. Every
  view covers default, home, teams on the fast path, teams + outcome on the
  filtered path, last N, starters, opponent rank, an On/Off stat filter and a
  Player Stats chip. The harness chooses the strict readers by tab and captures
  tables without CSV slugs by column name. All 45 goldens replay identically
  offline. All 82 web unit tests pass, including comparison of the separately
  captured live API rows against every stored numeric/boolean field, ranks,
  luck and sample flags. Fixed testServer possession bars are 100 / 300.
- **UI continuation:** Four Factors detail visibility is now React state
  passed to the cells, replacing the imperative body-class effect. Sliders
  cancel an interrupted pointer drag without committing its draft value.
  TanStack Table and the shared tab kit remain in place; no new layout or
  user-agent rules. CSV repair registered as D26 in
  `docs/shared_definitions.md` (Israeli sidebar only; Euro has no such button).
- **Browser acceptance is OPEN:** last complete run passed 97/102 tests;
  five cases failed (four slider-keyboard cases under last-N presets and one
  stale-row interaction). A narrower rerun passed 10/14; slider failures remain
  intermittent despite waiting for auto values and URL commits. Do not mark
  On/Off done or assume this is only a test issue. Investigate the keyboard
  event / controlled-value / nuqs path before Lineups. Six live
  On/Off server-render smoke checks already pass (IL / EuroLeague / EuroCup,
  390 and 1440 px). Phone and desktop screenshots were inspected in
  `output/playwright/onoff-*` and `live-onoff-*`.

**Latest Vercel Preview:**
https://ibpl-game-logs-n69n42tt5-ariel12091-8759.vercel.app/il/onoff?season=2026

Deployment `dpl_BgYVJwgQb2L7H4r4ho1x2ogB77WR` is READY, Preview
(`target: null`, `productionUrl: null`), protected by Vercel login. Euro paths
use season 2025. All 16 authenticated endpoint checks pass on their first
attempt: Game Logs, all supported On/Off views in all three competitions,
filtered IL Summary / EuroLeague FF, the IL Player Stats frame, and version.
Typecheck, lint (zero errors; two existing warnings) and production builds pass.

**Next, in order:**
1. Resolve the On/Off slider/browser failures before declaring that family
   accepted. User reports disk pressure. `npm.cmd cache clean --force` completed;
   the remaining disposable `_npx` cache (about 522 MB) was also removed.
   C: last measured 0.71 GB free after cleanup (was 0.14 GB after npm's own clean).
   Recheck free space and dev-server health before rerunning tests. Last debug
   run: `tmp_onoff_key_debug.log` (5/10 passed); keydown targets the correct
   slider, but some increments never appear. `ONOFF_DEBUG=1` enables event logs
   in the test. A later isolated attempt (`tmp_onoff_slider_debug.log`) produced
   no usable output and is not counted as a passing check. Temporary application
   console logging was removed before the checkpoint. Port 3100 is shared.
2. Lineups (Tab 2 / Tab 10) the same way; the pivot menu already links to
   `/{league}/lineups?team=&players_on=`.
3. Then Team Ratings, Player Stats, Gameflow + Home, Compare. Phase 0 proofs
   stay parked until the features are across.

### Reproduce the On/Off checks

`web/README.md` has the commands. Export stored R frames with
`scripts/export_onoff_goldens_json.R`; with the shared server on 3100, explicitly
capture read-only route inputs using `web/scripts/capture-onoff-answers.mjs`.
Those generated JSON files are ignored; the stored RDS oracle and database-query
fixtures are versioned. Run `npm.cmd test` and the On/Off Playwright tests from
`web/`. The live smoke suite is opt-in with `LIVE_DB_SMOKE=1`.

Do not re-record R goldens just to regenerate JSON. A fresh live recording uses
`scripts/capture_goldens.R --tab onoff --record`, alone as `app_readonly` on
5432. `--resume` preserves completed outputs and the fixture index after an
interruption; use it only for the same capture, not after changing presets or SQL.
The script now asserts the existing login and sets session read-only mode.

### Hiccups and lessons for the next executor

- The earlier Game Logs shortcuts (plain selectors, ID chips, missing grouped
  headers) were implementation mistakes. Numeric goldens did not prove control
  behaviour. The user identified them; the later corrections and
  `web/CLAUDE.md` are the authority. Compare controls and interactions as well
  as numbers; do not treat backend scope reductions as UI redesign permission.
- EuroLeague last 5 has zero rows at fixed 100 / 300 possession limits. The
  first recorder rejected that legitimate result. Presets now explicitly allow
  those two empty frames; they are not silently skipped.
- The capture script was edited while R was still executing it. All 45 outputs
  and the fixture index were written and the connection closed, but the process
  then reported a trailing parse error. The saved scripts parse, and all 45
  outputs were verified by a separate full offline replay. Freeze the executable
  files before the next long capture; restart rather than edit a running script.
- Browser setup initially raced auto-min updates and nuqs URL commits. It also
  clicked rows while the table was being replaced after Reset. Tests now wait
  for the initial auto values, each URL update, the reset answer and stable
  scrolling, but remaining keyboard failures are unresolved. Assertions still
  compare against the original fixed-bar R frames. The cache cleanup was
  requested because the disk lacks space, not as a proposed fix for React.
- One attempted duplicate dev server hit EADDRINUSE. Reuse the existing 3100
  server; do not start another Next process in this directory.
- Windows R emits C.UTF-8 / package-build warnings on stderr, which PowerShell
  may present as NativeCommandError even after successful assertions. Capture
  the log and propagate `$LASTEXITCODE`; do not classify locale warnings as
  numeric failures. Use npm/npx `.cmd` and preserve UTF-8 when editing.
- Sandbox registry / PostgreSQL access initially returned EACCES; approved
  escalated read-only commands worked. This was not a credential or role issue.
- Disk pressure became explicit during the last debug attempt. Cache cleanup
  completed, but free space remains low. Do not conflate npm-cache cleanup with
  a verified fix for the slider failures. No data directories, installed project
  dependencies, source files or golden fixtures were deleted for this cleanup.
- React Doctor's changed-file scan reports no diagnostics (five files; displayed
  score 53/100). The broader branch scan still reports existing complexity and
  accessibility work. Its subscription-cleanup error in `PivotMenu` was checked:
  the timeout and every listener have matching cleanup. The real slider
  cancellation finding was fixed. Do not describe the whole migration as clean
  based on this scan.
- The older Vercel first-deployment production classification and optional
  GitHub/MCP setup gaps remain in the historical record below. The new deployment
  is explicitly verified as Preview. No production promotion is authorized.

**Integration constraint:** do not merge this entire branch. The older
`87c3de2` publication/loader edits call functions absent from production. Keep
them unapplied; integrate the web work separately. Existing roles/passwords,
Shiny, schemas and ETL remain unchanged.

Everything below is the earlier record.

# (Earlier) Phase 0 execution status -- HANDOFF (2026-10-20)

## START HERE: decision for the next session (user, 2026-10-20, end of session)

**Lean thin slice first. No database changes.**

- Build the **Game Logs page in Next.js** (`web/`, both leagues) and get it on a **Vercel
  preview**. That is the whole next goal.
- **Database:** connect as the existing `app_readonly` login (from `app/.Renviron`, server-side
  env only, never in the browser or the repo) and run **Shiny's existing queries** against the
  existing views -- `gl_query_*` in `app/R/svc_game_logs.R`, `euro_fetch_game_log_rows()` in
  `app/R/global_euro.R`, `fetch_scoreless_games()` in `app/R/global.R`. No new user, no new
  schema, no new functions, **no production apply**.
- **Cache version:** the existing `app_meta.etl_full_last_success` (Israeli) and
  `euro_data_version()`'s query (EuroLeague), with a short CDN cache (about 60 s).
- The reviewed `app_api` schema and publication markers on this branch stay **unapplied**;
  they return only when caching precision or load requires them. Do NOT merge the ETL /
  EuroLeague loader publication edits (from `87c3de2`) -- they call functions that do not
  exist in production and would break the nightly runs.
- **Process (budget):** build directly in one session (no per-task subagent reviews), mid-tier
  model for routine work, ONE review at the end. No new design/plan documents unless a real
  decision forces one.
- Reuse what exists in `web/` (from `87c3de2`): config, `db.ts`/`deadline.ts`, `api.ts`,
  `csv.ts`, rules in progress. Known issues: 13/15 Game Logs goldens fail on minutes (summation
  order / R rounding); game-type labels are hardcoded (must come from the catalogue); code is
  minified one-liners (reformat). Goldens JSON (~280 MB) is generated by
  `scripts/export_goldens_json.R` into the gitignored `web/test/goldens/`.
- Vercel project linking, authentication and preview env configuration are now done;
  see the execution update below. Loading the newly configured MCP tools requires
  a client refresh/reload.
- Done = the preview shows Game Logs for both leagues matching the Shiny goldens.

## Lean slice execution update (2026-10-21)

**Numeric/API baseline verified; UI acceptance reopened by user feedback.**
Implemented on `sql/web-foundation`; not pushed or merged. The previous "slice
complete" statement overstated UI parity: numeric golden tests did not verify
searchable selectors, readable team/opponent chips, or grouped Four Factors headers.
The broader original Phase 0/Part C remains outside this narrowed slice.

### UI correction review (2026-10-21)

The spec's "No redesign. Same screens, same controls, same behaviour" and plan
Task 11 remain applicable under the lean backend amendment. Checked directly against
`ui_tab4_gamelogs.R`, `ui_tab11_euro_gamelogs.R`, `gamelog_ff_header()` in `helpers.R`,
and inventory GL-01/GL-02. Israeli Team is intentionally single-select; Euro teams
and opponents in all competitions support multiple selections. The missing type-to-
search interaction, ID-based chips and absent Offense/Defense/Usage header groups
were implementation omissions, not authorized UI changes.

Reported gaps corrected: searchable keyboard-operable pickers with removable named
selections, human-readable active chips, and the original 9/5/5/2 Four Factors
header spans and PPP/eFG%/OREB%/TOV%/FTR subheaders (Gameflow remains deferred).
Added six browser cases across all three competitions at both widths: typing,
keyboard and pointer selection, multiple teams/opponents where supported, named
chips, removal, URL persistence, Escape, no matches, and overflow checks. All 22
browser tests (these six plus the previous 16 regressions) pass. Typecheck and
local/Vercel builds pass; lint has the existing TanStack compatibility warning.
React Doctor reports 69/100 with the earlier warnings and two additional bounded
selection-list lookup advisories. Phone and desktop screenshots were inspected.
This is a targeted correction, not a claim that every
remaining Shiny presentation detail or the original full Phase 0 is complete.

**Verified preview (READY, Preview environment):**
https://ibpl-game-logs-dpj400xsb-ariel12091-8759.vercel.app/il/game-logs?season=2026

Use season 2025 on `/euroleague/game-logs` and `/eurocup/game-logs` for the tested
Euro seasons. Deployment ID: `dpl_AFA5N4ngspSHco5ddByNnEk8twCE`. Next.js 16.4.2;
updated Vercel build completed in 11 seconds. Deployment protection uses Vercel login.

- Added the Next.js Game Logs screen, Summary/Four Factors, URL filters, starter
  bounds, game context, Israeli stat filters, sorting, heat colours, CSV, loading,
  empty and retry states. Includes Israeli League, EuroLeague and EuroCup.
- Added read-only API routes using the existing Shiny SQL and existing version
  queries. Raw lineup facts stay on the server: compute the table there and return
  only its rows and filter metadata, avoiding the 12.85 MB browser payload. Warm
  cache is bounded to four seasons/views with a 60-second TTL; CDN TTL is 60 seconds.
  This is a deliberate lean-slice change from the original browser-filtering plan.
- Fixed compensated minute sums and R ties-to-even decimal rounding. Tests now
  compare against the original stored R table goldens, not a freshly rerun R service.
  All 15 stored presets pass against the original expected tables. The complete
  unit suite passes 35 tests, including request-validation cases.
- Exported game-type and phase labels from the existing R catalogue. Reformatted
  web sources. Updated Next/React/Vitest dependencies; initial typecheck, lint
  (one TanStack/React Compiler compatibility warning) and production build passed.
- Live local API checks succeeded as `app_readonly`: Israeli 2026 = 442 rows,
  EuroLeague 2025 = 802, EuroCup 2025 = 384. No database objects or roles changed.
  Connection startup measured 853 ms, so the web-only connect allowance is 1500 ms
  rather than 500 ms; SQL budget remains 3000 ms and database-work deadline 5000 ms.
  Server-side table computation adds time beyond that database deadline; the lean
  slice does not claim the original plan's end-to-end performance acceptance.
  SSL follows the existing Shiny `require` setting; optional `PG_SSLROOTCERT` plus
  `PG_SSLMODE=verify-full` supports project CA verification.
- Vercel CLI 64.7.0 installed globally; authenticated as `ariel12091-8759`.
  Official Vercel Codex plugin installed in user scope (`vercel@openai-curated`).
  Shared MCP endpoint `https://mcp.vercel.com` added to
  `C:/Users/ariel/.codex/config.toml`; OAuth succeeded and CLI reports enabled/OAuth.
  This running session has not loaded its tools; authenticated `list_teams` and
  `search_vercel_documentation` checks still need a tool refresh/reload.
- Created/linked Vercel project `ibpl-game-logs` in `ariel12091-8759`.
  Existing PG connection settings copied into encrypted **preview-only** env vars
  with `web/scripts/configure-preview.mjs` (values go through stdin, never logs).
  `.vercelignore` excludes credentials, dependencies and generated golden payloads;
  a dry-run verified deployment inputs. GitHub linking needs an additional login
  connection and did not complete; CLI preview deployment remains available.
- Final verification: 16 golden/browser cases passed at phone (390px) and desktop
  (1440px) widths, covering seven presets, visible cells, URL restoration, CSV,
  best-direction defensive sorting, empty state, retry and version refresh.
  Six additional live browser smoke checks passed across all three competitions
  at both widths. Phone and desktop screenshots were visually reviewed in
  `output/playwright/`. Local typecheck, lint and production build passed; final
  Vercel build also passed. Runtime dependency audit reports zero vulnerabilities.
- Final review fixed defensive Four Factors sort direction, invalid stat-filter
  URL rendering, export availability after an error, and stable empty table data.
  React Doctor improved from 66 to 69 after its actionable table/button findings
  were fixed. Nine warnings remain: component size/complexity, effect-based fetch,
  stateless filter-button index keys, small bounded array lookups, an asserted map
  invariant, and serial queries on one snapshot connection. These were reviewed;
  they do not establish a correctness failure. Broader component cleanup is deferred.
- `web/scripts/verify-preview.mjs <deployment-url>` verifies Summary/Four Factors
  and version APIs through authenticated Vercel CLI calls. It invokes Node directly
  to preserve query-string ampersands on Windows. `web/README.md` documents local
  execution, golden generation, browser checks and deployment.
  The final deployed verifier passed all six endpoints on the first attempt:
  Israeli Summary/FF 442 rows each, EuroLeague Summary/FF 802 each, EuroCup
  Summary 384, and the version endpoint reported all three leagues.
- Vercel classified the new project's first deployment as production despite the
  explicit Preview option. That seed deployment has no PG credentials (credentials
  are Preview-only). Subsequent deployments correctly use Preview. Shiny and the
  production database were not changed. No production promotion is authorized.
- Only follow-up for agent setup: refresh/reload the client to load the already
  authenticated MCP server, then run `search_vercel_documentation` and `list_teams`.
  These two MCP calls could not be performed in this session's fixed tool inventory.
  GitHub auto-deploy linking is optional and still needs its login connection.
- Do not merge this whole historical branch: it contains the old unapplied ETL /
  publication work. The new Game Logs commit is suitable for a separate cherry-pick
  when integrating the lean slice. No SQL, ETL or loader edits were added this run.

The rest of this file is the historical record. Its production-apply steps are
superseded by the lean-slice decision above and must not be executed.


Plan: `docs/superpowers/plans/2026-10-20-react-phase0-foundation.md` (read its "Execution
amendment (user)" first). Spec: `docs/superpowers/specs/2026-10-20-react-migration-design.md`.
Branch: `sql/web-foundation` (not pushed, not merged). `main` is at `ae865b0` locally /
`7cd39a2` on origin (D24 fix deployed).

## Authorization and constraints (user, 2026-10-20)

- The web app uses the existing `app_readonly` login. No `app_web` role, no password, role
  default, membership or connection-limit changes. Add only grants the new objects need.
- Every production database change goes through a dry-run-by-default apply script and is
  applied only after the user's explicit go-ahead. Nothing has been applied yet.
- Keep Shiny working unchanged; no production game load to test publication code.
- PostgreSQL is 17.4 (verified live), not the plan's "15" label.

## History

1. A first executor (Claude) committed Task 1 in its original `app_web` form (`a352e98`,
   superseded by the amendment) and a comment fix (`db0f5b8`).
2. A second executor started at 06:58 UTC under the amendment, worked uncommitted across
   Tasks 2-9 and 16, and stopped at its limit without a handoff.
3. Takeover: production verified untouched (no `app_api`, no publication objects, no
   `app_web`). An inventory of the second executor's work is in
   `.superpowers/sdd/2026-10-20-react-phase0-foundation/inventory-of-other-session.md`
   (git-ignored workspace; ledger `progress.md` in the same folder). Its work was saved
   as one unreviewed commit, `87c3de2`.
4. Work unit U1 (database: Tasks 1-amended, 2, 5) fixed and reviewed:
   `0e09866` app_web leftovers removed, identity proof as app_readonly;
   `b4b4cbd` publication liveness scoped to this database;
   `a657ded` Game Logs readers fixed (old version filtered a non-existent column) and proven
   equal to Shiny's own season pulls;
   `7768bf2` safe apply order + deploy-and-call catalogue test in a scratch schema.
   Review verdict: publication semantics, reader parity and grants sound; two Important
   findings sent back (fix round 1, below).

## Last step completed before handoff

**U1 fix round 1 committed:** `aca4bc0` (identity proof asserts SQLSTATE 42501 in ordinary
rolled-back transactions, plus privilege-function checks) and `14c1a2c` (smoke calls before
the hardening in both apply scripts -- hardening locks now held 1.75 s / 1.62 s in the dry
runs; begin records owner pid + backend_start and refuses shared/xact locks; status reports
`stale` unless that exact owner holds the lock -- `publication_status` is now SECURITY
DEFINER with an empty search_path because app_readonly cannot read other roles'
`backend_start`; finish requires the live owner; poll for `stale`; header notes).
Scratch-schema DB tests, catalogue test and security contracts pass; both dry runs pass.

**Next:** a scoped re-review of `7768bf2..14c1a2c` (the seven findings, plus the new
SECURITY DEFINER function), then the production apply below.

## Then, in order

1. **Ask the user for the production apply** (off-peak, not during the nightly ETL window):
   `scripts/apply_publication.R` first, then `scripts/apply_web_foundation.R`, each with its
   CONFIRM flag; then the post-apply DB tests (`test-web-role-db.R`,
   `test-app-api-game-logs-db.R`) and a security re-apply + re-test (review R9).
2. **U2 -- Tasks 3 and 4 (ETL and EuroLeague loader).** Known defects in the WIP:
   - merging them before step 1 breaks the nightly ETL and loader (they call publication
     functions); the merge must follow the apply;
   - `etl_full.R` edit breaks `test-player-identity-dictionary-contracts.R:48` (CI red);
   - the Israeli ETL bumps the version every night even with no relevant writes (plan: it
     must not); last-success marker and Phase 7 run after the publication closes, unfenced;
   - EuroLeague loader: raises on the first failed group (skipping later groups), skips the
     manifest on partial runs, verifies twice -- restore the original behaviour;
   - Task 3 leftovers: write-inventory doc, static test, mutation check, R8 mixed-run test;
   - "Closing open result set" warnings in `test-etl-publication-db.R`.
3. **U3 -- Task 9 (shared Game Logs rules).** 13/15 goldens fail on minutes (Israeli
   Summary off by 0.1; FF/EuroLeague in the last decimals) -- likely summation order /
   rounding vs R. Golden JSON (~280 MB) is gitignored under `web/test/goldens/`. The TS test
   must compare against the stored goldens (it re-ran R), game-type labels must come from
   the catalogue (hardcoded now), and the web code is minified one-liners -- reformat.
4. **U4 -- Tasks 6-8, 10-12** (scaffold leftovers, db client tests, API conventions, routes,
   page, preview). EuroLeague season payload is 12.85 MB from the DB and the reader takes
   ~2.8 s: Task 10 must fit 4.5 MB and the 3 s SQL budget.
5. **U5 -- Part C** (Tasks 13-16). Task 14 must prove crash release through the session
   pooler with a real kill.

`web/.env.example` (no secrets) is untracked because the credential hook blocks `.env*` names.
