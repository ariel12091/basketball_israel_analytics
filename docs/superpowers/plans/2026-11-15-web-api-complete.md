# Web API complete — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Any caller of `/api/v1/[league]/*` (the page, the Ask chat, a future MCP server, a customer) can get the finished table a page shows — filters, auto bars, rate view, Hebrew team names — as JSON or CSV, from one implementation.

**Architecture:** Lineups already does this: `lineupPageOptions()` (`web/src/lib/lineupPageState.ts`) turns the page query into table options and `lineupsPageAnswer()` (`web/src/server/lineupsApi.ts`) applies `lineupPage()` on the server. Give Player Stats, Team Ratings, On/Off and Compare the same pair (an options function in `web/src/lib/pageState/`, a `*PageAnswer` in `web/src/server/`), reached through `shape=table` on the existing route; the page clients call the same options function, and the chat calls the page answers, so `web/src/ask/pageRows.ts` and the composition half of `web/src/ask/compareTool.ts` are deleted. Hebrew team names move from the chat layer into the answer functions. CSV is one more response format over the table shape.

**Tech Stack:** Next.js 16 route handlers, TypeScript, Vitest, Playwright (existing e2e), `pg` via `server/db.ts`.

**Spec:** this conversation's findings (2026-11-15), recorded here because there is no separate spec: the registry tags 88 of 109 inventory features (`docs/superpowers/specs/2026-10-19-api-mcp-feature-inventory.md`); `lib/tabQuery.ts` strips `stats` from the API paths; `ask/pageRows.ts` ("Keep the two in step") and `ask/compareTool.ts` ("Keep this in step with CompareClient") copy the pages' composition; CSV exists only in the browser (`downloadCsv`, `components/shiny/DataTable.tsx:246`); Hebrew team names exist only in `ask/run.ts`; R `api/` serves Game Logs only and nothing calls it.

## Global Constraints

- `docs/shared_definitions.md` rules 1, 4 and 10: one implementation per behaviour; the table shape is the page's own function, never a second copy.
- Default responses do not change: a request without `shape`/`format` returns exactly what it returns today (the pages depend on it). `shape` accepts `table` only; `format` accepts `json` (default) and `csv`; anything else is a 400 `ParamError`.
- `shape=table` means "what a freshly opened link with this query shows": auto min-possessions on unless the query hand-sets the bar (`min_poss`, `min_on`, `min_all`), exactly as `ask/pageRows.ts` `autoBar()` does today.
- Caching goes through `answerJson()` (`server/api.ts`): cacheable only with a pinned `v`. CSV gets the same header logic. Fast Origin Transfer is 10 GB/month; no new prefetching.
- Hebrew fields are Israeli only: `team_name_he`, `opp_team_name_he`, `name_he`, `player_names_he_str`, `player_he`. No page renders them.
- Branches `react/<short>` off `main`, one PR per Part; merge only after Web Preview live checks pass on the head, and only on the user's "deploy". Run npm/npx via PowerShell (`npm.cmd`, `npx.cmd`). Check `C:` free space before builds (was ~0.2 GB on 2026-11-15).

## Review Focus

1. **A link that hand-sets a bar** (`min_on=50` on On/Off, `min_poss=0` on Lineups) must keep it in `shape=table`, not be overruled by auto. Pinned in Task 2 and Task 3.
2. **EuroLeague and EuroCup** requests with `shape=table` must work and carry no Hebrew fields (and no `dfl`). Pinned in Task 2 and Task 6.
3. **An unknown `shape` or `format`** (`shape=full`, `format=xlsx`) is a 400, not a silent default. Pinned in Task 1.
4. **CSV cells that start with `=`, `+`, `-`, `@`** are defused exactly as the page's export (`defuseCsvCell`). Pinned in Task 7.
5. **An empty population** (a new season's first days) returns `rows: []` with its meta, and a header-only CSV. Pinned in Task 2 and Task 7.

---

## Part A — the table shape (branch `react/api-tables`)

### Task 1: `shape` parameter plumbing

**Files:**
- Modify: `web/src/server/api.ts` (add `answerShape()`, `answerFormat()`)
- Test: `web/src/server/api.test.ts`

**Interfaces:**
- Produces: `answerShape(search: URLSearchParams): "population" | "table"` — reads and deletes `shape` from `search`; throws `ParamError("shape", "shape is table")` for any other value. `answerFormat(search): "json" | "csv"` — same for `format`; `csv` implies `table`.

- [ ] **Step 1:** Write `api.test.ts` tests `answerShape`: absent → `"population"`, `table` → `"table"` and the key is removed, `full` → throws ParamError on `shape`; `answerFormat`: absent → `"json"`, `csv` → `"csv"`, `xlsx` → throws.
- [ ] **Step 2:** Run `npx.cmd vitest run src/server/api.test.ts` — FAIL (not defined).
- [ ] **Step 3:** Implement both in `server/api.ts`.
- [ ] **Step 4:** Run the test — PASS.
- [ ] **Step 5:** Commit `feat(api): shape and format parameters`.

### Task 2: Player Stats and Team Ratings page answers

**Files:**
- Create: `web/src/lib/pageState/playerStats.ts`, `web/src/lib/pageState/teamRatings.ts`
- Modify: `web/src/server/playerStatsRepo.ts` (`playerStatsAnswer`), `web/src/server/teamRatingsRepo.ts` (`teamRatingsAnswer`), both routes, `web/src/components/PlayerStatsClient.tsx:55-80`, `web/src/components/TeamRatingsClient.tsx:70-85`
- Test: `web/src/lib/pageState/pageState.test.ts`

**Interfaces:**
- Consumes: `answerShape` (Task 1); rules `playerDisplay()`, `teamTableRows()` unchanged.
- Produces:
  - `playerStatsPageOptions(query: Query, rows: Row[]): { mode: PlayerMode; players: string[]; minGp: number; showIneligible: boolean; filters: StatFilter[] }` — the body is today's composition in `ask/pageRows.ts` (player_stats branch); `minGp` needs `rows` for its cap.
  - `teamRatingsPageOptions(query: Query): { view: TeamView; filters: StatFilter[]; defense: boolean }`.
  - `playerStatsPageAnswer(league, search)` / `teamRatingsPageAnswer(league, search)`: the population answer with `rows` replaced by the finished table; `meta` unchanged plus `shape: "table"`.
  - `Query` is `Record<string,string>` (`lib/tabQuery.ts`); server callers pass `Object.fromEntries(search)`.

- [ ] **Step 1:** Write `pageState.test.ts`: (a) `playerStatsPageOptions({view:"Totals", min_gp:"99"}, rows with max gp 8)` gives `mode "Totals"`, `minGp 8`; unknown view → `"Per Game"`; (b) `teamRatingsPageOptions({side:"defense", stats:'[{"col":"net_rtg","op":"ge","value":0}]'})` gives `defense true` and one filter; (c) for the stored fixture rows the existing `ask/pageRows.test.ts` uses, `playerDisplay(rows, ids, playerStatsPageOptions(q, rows)).rows` equals `pageRows("player_stats", ...)` (temporary parity pin, deleted in Task 5); (d) empty `rows` → `minGp 1`, table `[]`.
- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3:** Implement the two options functions; make both clients call them inside their existing `useMemo` (no behaviour change); add the two page answers; in each route, `answerShape(search) === "table"` calls the page answer, otherwise the population answer as today.
- [ ] **Step 4:** Run `npx.cmd vitest run` (full suite) — PASS; `npx.cmd tsc --noEmit -p .` clean.
- [ ] **Step 5:** Add to `web/e2e/live-smoke.spec.ts` (LIVE_DB_SMOKE only): `/api/v1/il/player-stats?season=<default>&shape=table&view=Totals` returns rows whose count equals the page's rendered row count for the same query, and `/api/v1/euroleague/team-ratings?shape=table` returns rows with no `team_name_he` key.
- [ ] **Step 6:** Commit `feat(api): finished Player Stats and Team Ratings tables (shape=table)`.

### Task 3: On/Off page answer

**Files:**
- Create: `web/src/lib/pageState/onoff.ts`
- Modify: `web/src/server/onoffApi.ts` (`onoffAnswer`), its route, `web/src/components/OnOffClient.tsx:200-230`
- Test: `web/src/lib/pageState/pageState.test.ts`

**Interfaces:**
- Consumes: `autoBars`, `autoShare`, `onoffTableRows`, `applyPlayerStatFilters` (rules, unchanged); `autoBar()` moves from `ask/pageRows.ts` to `lib/pageState/onoff.ts`, same signature.
- Produces: `onoffPageOptions(query: Query, payload: OnOffPayload, league: League): { view: View; teams: number[]; minAll: number; minOn: number; filters: StatFilter[]; psFilters: StatFilter[] }`; `onoffPageAnswer(league, search)` — when `psFilters` is non-empty it reads the player-stat-filters frame in-process (as `ask/run.ts:178` does) before filtering.

- [ ] **Step 1:** Tests: no `min_on` → auto bar from `autoBars`; `min_on=50` → 50 even when auto would differ (Review Focus 1); a `ps_` stat filter is split into `psFilters`.
- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3:** Implement. `OnOffClient` keeps `useAutoMin` (it writes the bars into the URL); the table `useMemo` calls `onoffTableRows(..., onoffPageOptions(query, data, league))`, so after auto writes, client and server agree.
- [ ] **Step 4:** Full suite and tsc — PASS.
- [ ] **Step 5:** Commit `feat(api): finished On/Off table (shape=table)`.

### Task 4: Compare page answer

**Files:**
- Create: `web/src/rules/compareTable.ts`, `web/src/server/comparePage.ts`
- Modify: `web/src/ask/compareTool.ts` (keep only chat concerns: `gap_size`, param errors, `pageQuery`), `web/src/components/CompareClient.tsx` (where it joins/finalizes rows), compare route
- Test: `web/src/rules/compareTable.test.ts`; existing `ask/compareTool.test.ts` must stay green

**Interfaces:**
- Produces (pure, no I/O): `compareLeagueTable(q: Query, a: Row[], b: Row[]): Row[]`, `compareDetailTable(q: Query, reads: { ra; rb; fa; fb; sa; sb }): Row[]`, `comparePlayersTable(q: Query, a: Row[], b: Row[]): Row[]` — moved verbatim from `compareTool.ts` (`league`, `detail`, `players` bodies minus the reads).
- Produces (server): `comparePageAnswer(league, search)` — resolves the page query (`resolveCompareQuery`), reads both sides in-process through `compareAnswer` with the same `pairReader` version check, returns `{ rows, meta: { version, view, sides } }`.
- The move is byte-identical where possible: verify by diffing the moved bodies against `git show HEAD:web/src/ask/compareTool.ts` (CLAUDE.md "verify a move with a diff").

- [ ] **Step 1:** `compareTable.test.ts`: for the fixture reads in `compareTool.test.ts`, each table function returns the rows the old `compareTool` returned (copy the expectations).
- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3:** Move the bodies; `compareTool.ts` calls `comparePageAnswer` then adds `gap_size`; `CompareClient` calls the three table functions after its own async reads (its progress UI is unchanged); route `shape=table` → `comparePageAnswer`.
- [ ] **Step 4:** Full suite, tsc, and `npx.cmd playwright test e2e/compare-controls.spec.ts --workers=1` (only when the user is not using the 3100 server) — PASS.
- [ ] **Step 5:** Commit `refactor(compare): one table builder for the page, the API and the chat`.

### Task 5: The chat reads the page answers; delete the copies

**Files:**
- Modify: `web/src/ask/registry.ts` (answer functions: `lineupsPageAnswer`, `playerStatsPageAnswer`, `teamRatingsPageAnswer`, `onoffPageAnswer`, `comparePageAnswer`), `web/src/ask/flatten.ts` (drop the `pageRows` call; `withTeamNetRank` moves into `flatten.ts`), `web/src/ask/run.ts` (drop the second player-stats read at :178)
- Delete: `web/src/ask/pageRows.ts`, `web/src/ask/pageRows.test.ts` (its cases move to `pageState.test.ts` where not already covered), the temporary parity pin from Task 2
- Test: `ask/run.test.ts`, `ask/flatten.test.ts`, `ask/registry.test.ts`

- [ ] **Step 1:** Change `registry.test.ts` to assert each table tool's `answer` is the page answer (by function name, as it checks names today) — FAIL.
- [ ] **Step 2:** Make the switch; the chat passes `shape=table` semantics by calling the page answers directly.
- [ ] **Step 3:** Full suite and tsc — PASS. `git grep -n "pageRows\|Keep the two in step\|Keep this in step" web/src` returns nothing.
- [ ] **Step 4:** Push; on the Preview (with the branch-scoped Ask env vars, as on `react/he-names`) ask the questions in `web/test/ask/cases.json` whose `expect` names `compare`, `onoff`, `lineups`, `player_stats` or `team_ratings`; the answers' numbers match the linked pages.
- [ ] **Step 5:** Commit `refactor(ask): read the pages' own table answers`; open PR "API: finished tables (shape=table)".

## Part B — Hebrew team names in the API (branch `react/api-he`)

### Task 6: Hebrew names in the answer functions

**Files:**
- Modify: `web/src/server/teamNames.ts` (`readTeamNamesHe(q: Query, league, season)` takes the request's snapshot, like `readPlayerNames`), the answers `teamRatingsAnswer`, `playerStatsAnswer`, `onoffAnswer`, `lineupsAnswer`, `gameLogsAnswer`, `gameflowAnswer` (meta `ownTeamHe`/`oppTeamHe`, and the summary rows), `homeAnswer` (every frame), `compareAnswer`; `web/src/ask/run.ts` (keep `applyTeamNamesHe` for `list_teams` only), `web/src/ask/flatten.ts` (Gameflow rows take `team_name_he` from meta), `docs/shared_definitions.md` (owner: `server/teamNames.ts`, applied in the answers)
- Test: `web/src/server/teamNames.test.ts`, one assertion per answer in its existing test file

- [ ] **Step 1:** Tests: each Israeli answer's rows carry `team_name_he` for a fixture team; Game Logs rows carry `opp_team_name_he`; a EuroLeague answer carries neither (Review Focus 2).
- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3:** Implement with one helper call per answer, inside its existing `withSnapshot`.
- [ ] **Step 4:** Full suite, tsc; the Preview chat still names teams in Hebrew (one Compare and one Gameflow question).
- [ ] **Step 5:** Commit; PR "API: Hebrew team names in Israeli answers".

## Part C — CSV (branch `react/api-csv`)

### Task 7: `format=csv`

**Files:**
- Modify: `web/src/server/csv.ts` (already holds `defuseCsvCell`; add `csvText(cols: CsvColumn[], rows: Row[]): string`, moved from `downloadCsv`, which keeps only the Blob/anchor part and calls it), `web/src/server/csvColumns.ts` (`csvColumns(tool, league, view, rows): CsvColumn[]` — the page tables' column lists: `summaryColumns`/`ffColumns`/`spColumns` (On/Off), `columnsFor` (Game Logs), and the Team Ratings, Player Stats and Lineups column builders exported from their table components)
- Modify: `web/src/server/api.ts` (`answerCsv(answer, url, name)` — same cache headers as `answerJson`, `Content-Type: text/csv; charset=utf-8`, `Content-Disposition: attachment; filename="<tab>_<league>_<season>.csv"`), the five table routes and Game Logs
- Test: `web/src/server/csv.test.ts`, `web/src/server/csvColumns.test.ts`

**Interfaces:**
- `CsvColumn = { label: string; text: (row: Row) => string }` — derived from DataTable's `Column` (`label`, `render(row).text` or the raw value), so the file equals the page's own export.

- [ ] **Step 1:** Tests: `csvText` quotes every field, doubles quotes, joins with `\r\n`, and defuses `=SUM(A1)` exactly as `defuseCsvCell` (Review Focus 4); empty rows → header line only (Review Focus 5); for one stored On/Off and one Team Ratings fixture, the server CSV equals the string the page's `downloadCsv` would build for the same rows and default columns.
- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3:** Implement; columns are the page's default visible set in default order (column reorder is per-browser and not part of the API).
- [ ] **Step 4:** Full suite, tsc; live smoke: `/api/v1/il/team-ratings?shape=table&format=csv` returns 200 with `text/csv` and the page's header row.
- [ ] **Step 5:** Commit; PR "API: CSV for every table".

## Part D — coverage and the R API (branch `react/api-cover`)

### Task 8: Route-level inventory coverage

**Files:**
- Create: `web/src/server/apiCoverage.ts` (`API_COVERAGE: Record<RouteName, string[]>` inventory ids per route, and `CLIENT_ONLY: Record<string, string>` id → reason), `web/src/server/apiCoverage.test.ts`

- [ ] **Step 1:** Test: every id in the inventory doc (parse `| <ID> |` rows, as `ask/registry.test.ts` does) is in exactly one of `API_COVERAGE` (any route) or `CLIENT_ONLY`; every listed id exists in the doc.
- [ ] **Step 2:** Run — FAIL.
- [ ] **Step 3:** Fill the map. Routes: all 88 ids the registry tags, plus X-01, X-02, X-03, X-06, X-07, X-08, X-21 (reachable today), X-09 (Part A), X-14, TR-11, GL-08 (Part C), X-20 (route guards: `rawQueryGuard`, deadlines, the snapshot queue). Client-only, with reasons: X-10 chips bar, X-12 heat colours, X-13 default sort, X-15 column reorder, X-17 row pivots (links between pages), X-18 URL state, X-19 last-updated stamp, HOME-08, HOME-09 (navigation cards).
- [ ] **Step 4:** Run — PASS.
- [ ] **Step 5:** Commit `test(api): every inventory feature is served or named client-only`.

### Task 9: Retire the R API

**Files:**
- Delete: `api/` (whole directory), `app/tests/testthat/test-api-parity-live-db.R`
- Modify: `CLAUDE.md` (Service layer section: drop the `api/` bullets; the service layer `app/R/svc_*.R` stays for Shiny), the Shiny retirement checklist (add nothing: `api/` is already gone), `docs/superpowers/specs/2026-10-19-api-mcp-design.md` (a "Superseded 2026-11-15" note at the top: the MCP server, when built, sits on `/api/v1`)

- [ ] **Step 1:** `git grep -n "api/run.R\|api/coverage\|api/R/\|test-api-parity" -- . ':!docs/superpowers'` — list every reference; each is updated or deleted in this task.
- [ ] **Step 2:** Delete and edit; `git grep` from Step 1 returns nothing outside history docs.
- [ ] **Step 3:** The essential R tests still pass: run the `r-tests.yml` fast set locally or push and read the run.
- [ ] **Step 4:** Commit `chore: retire the R api/ (superseded by /api/v1)`; PR "API: coverage map, retire R api/".
