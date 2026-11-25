# Gameflow Win Probability Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Gameflow shows each team's win chance at every moment (score readout) and ranks runs, and picks the defining stretch, by win-chance swing.

**Architecture:** The reader payload (`RawRibbon`) gains `win_prob`, the rows of `basketball_test.game_win_probability(league, game)`; `ribbonModel()` turns them into a page-team series on the ribbon's clock; `detectRuns()` ranks by swing from that series; the chart's score tooltip and the run texts read it. Fixtures carry the same key, so unit tests and e2e `loadFixture()` see real data.

**Tech Stack:** Next.js / TypeScript (`web/`), vitest (`npm test`), Playwright e2e, R fixture script, Postgres.

**Spec:** `docs/superpowers/specs/2026-11-24-gameflow-win-probability-design.md`

## Global Constraints

- Work in a scratch copy on branch `react/winprob`; never switch the main checkout's branch (another session uses it). Commit via a temp index or in the scratch clone.
- Rule 1: the probabilities come from `game_win_probability()` only; web code maps and selects, never recomputes the model.
- League argument: `'il'` for Israeli, `'euro'` for EuroLeague and EuroCup.
- Function rows are home-side; everything on the page is page-team side (`own` = the `team` query param).
- Clock: `elapsed = (period <= 4 ? 2400 : 2400 + 300 * (period - 4)) - seconds_left` (regulation `seconds_left` counts to the end of regulation, overtime to the end of that overtime; final row has `seconds_left = 0` and `is_final`).
- Display: whole percent; before the final row clamp to "<1%" / ">99%"; the final row shows 100% / 0%.
- No WPA on players, lineups or stints (spec non-goal).
- Merge to `main` only after Preview live checks pass on the head AND the user says "deploy".

## Review Focus

1. A game with no `win_prob` rows (no active fit, or a game the states miss): Gameflow must render as today, runs ranked by net with today's defining rule, no "Win" row. Test in Task 3 and Task 2.
2. Page team is the AWAY team: chances flipped (own = 1 - home). Fixture 440 or 1147 must be an away-team fixture; test in Task 2.
3. Overtime game: elapsed mapping for period 5+. Test in Task 2 with a synthetic OT row.
4. A run whose first point comes in the same second a possession starts: `wpFrom` must be the pre-run state. Pinned by the margin cross-check in Task 2 (row score margin before = run.fromMargin, after = run.toMargin) over every candidate run of the three fixtures.
5. EuroCup game (competition U): league 'euro' must still return rows. Live check in Task 5.

---

### Task 1: Fixtures carry `win_prob`

**Files:**
- Modify: `scripts/save_gameflow_fixture.R`
- Modify: `web/src/rules/__fixtures__/gameflow-{673,440,1147}.json` (regenerated)
- Modify: `web/src/rules/gameflow.ts` (type `RawRibbon` only)

**Interfaces:**
- Produces: `RawRibbon.win_prob?: RawWinProb[]` with
  `RawWinProb = { possession_no: number; is_final: boolean; period: number; seconds_left: number; home_team_id: number; away_team_id: number; home_score: number; away_score: number; home_win_probability: number }`.

- [ ] Step 1: In the script, after the reader row, run `SELECT possession_no, is_final, period, seconds_left, home_team_id, away_team_id, home_score, away_score, home_win_probability FROM basketball_test.game_win_probability($1, $2)` with `c(if (league == "israel") "il" else "euro", game_id)` and add it to `out` as `win_prob` (a list of row objects, `jsonlite` `dataframe = "rows"`).
- [ ] Step 2: Regenerate the three fixtures with the same args they were made with (read each file's game/team from its existing content and the script usage; 1147 is EuroLeague). Run as `app_readonly` with nothing else querying (pooler note in root CLAUDE.md).
- [ ] Step 3: Verify: each fixture has `win_prob`, its last row `is_final: true` with probability 0 or 1, and `git diff --stat` shows only the added key (no other fixture field changed).
- [ ] Step 4: Add `RawWinProb` and the optional field to `RawRibbon`. `npm test` still passes.
- [ ] Step 5: Commit "gameflow: fixtures carry win probability".

### Task 2: Win-probability series and swing (`rules/gameflowWinProb.ts`)

**Files:**
- Create: `web/src/rules/gameflowWinProb.ts`, `web/src/rules/gameflowWinProb.test.ts`
- Modify: `web/src/rules/gameflow.ts` (`ribbonModel` adds `winProb`)

**Interfaces:**
- Consumes: `RawWinProb` (Task 1).
- Produces:
  - `type WinPoint = { elapsed: number; own: number; ownScore: number; oppScore: number; final: boolean }`
  - `winSeries(rows: RawWinProb[] | undefined, team: number): WinPoint[]` (sorted by possession_no; `[]` when none; `own` = home prob if team is home, else 1 - it)
  - `winChanceAt(series: WinPoint[], t: number): WinPoint | null` (last point with elapsed <= t; first point when t precedes all; null on empty)
  - `runSwing(series: WinPoint[], run: { side: Side; start: number; end: number }): { from: number; to: number } | null` (from the RUN TEAM's side: `from` = last point with elapsed < start, else the first point; `to` = `winChanceAt(end)`; null on empty)
  - `winPct(p: WinPoint): string` per Global Constraints
  - `RibbonModel.winProb: WinPoint[]`

- [ ] Step 1: Failing tests:
  - `winSeries` on fixture 673 is non-empty, ends `final: true`, `own` in [0,1]; for the away fixture, `own` = 1 - `home_win_probability` row by row.
  - Elapsed mapping: period 2, seconds_left 1500 -> 900; period 5, seconds_left 120 -> 2580.
  - `winChanceAt(series, 0)` is the first point; `winChanceAt(series, 1e9)` is the final point.
  - Margin cross-check, all three fixtures, every run of `detectRuns(model.steps)` (today's candidates, before Task 3): the score margin (own - opp, from the run side) of the point used for `from` equals `run.fromMargin` and of the point used for `to` equals `run.toMargin` (sign as in `runExplanation`). If this fails because of same-second possession starts, match by score instead of time and keep the test.
  - `winPct`: 0.004 -> "<1%", 0.996 -> ">99%", 0.71 -> "71%", final 1 -> "100%".
  - `runSwing([], run)` is null.
- [ ] Step 2: `npm test -- gameflowWinProb` fails (module missing).
- [ ] Step 3: Implement; wire `winProb: winSeries(raw.win_prob, team)` into `ribbonModel`.
- [ ] Step 4: `npm test` passes.
- [ ] Step 5: Commit "gameflow: win-probability series".

### Task 3: Runs ranked and defined by swing

**Files:**
- Modify: `web/src/rules/gameflowRuns.ts`, `web/src/rules/gameflowSummary.ts`
- Test: `web/src/rules/gameflowRuns.test.ts`, `web/src/rules/gameflowSummary.test.ts`

**Interfaces:**
- Consumes: `runSwing`, `winPct`, `WinPoint` (Task 2).
- Produces: `detectRuns(steps: Score[], series: WinPoint[] = []): Run[]`; `Run` gains `wpFrom: number | null; wpTo: number | null` (run team's side). `runExplanation`'s first line gains ` · win chance 41% → 78%` when both are non-null.

- [ ] Step 1: Failing tests:
  - With a series: the taken runs are the non-overlapping candidates in order of swing (`wpTo - wpFrom`) descending; ties by net, then shorter, then earlier. Build a synthetic steps + series case where the biggest-net run has the smaller swing and assert it ranks second.
  - Defining = the winner's candidate with the largest swing; never the loser's; none when the final margin is 0.
  - Without a series (`[]`): output equals today's (snapshot the three fixtures' runs from `origin/main` behaviour before editing: side, start, end, defining).
  - `runExplanation` first line ends with "· win chance 41% → 78%" for a run with those values, unchanged when null.
- [ ] Step 2: Run; new tests fail.
- [ ] Step 3: Implement; `gameflowSummary` passes `model.winProb`. Keep `RUN_MIN_NET`, `RUN_MAX_OPP`, `RUN_MAX` unchanged.
- [ ] Step 4: `npm test` passes; update any existing run test whose expectation changed, and say which in the commit message.
- [ ] Step 5: Commit "gameflow: runs ranked by win-probability swing".

### Task 4: Read in production code, show on the page, Ask

**Files:**
- Modify: `web/src/server/gameflowRepo.ts`, `web/src/components/shiny/GameflowChart.tsx` (score tooltip ~line 759, run chip label), `web/src/components/shiny/GameflowPanel.tsx` (phone run rows `.gf-m-runrow`, score readout), `web/src/ask/flatten.ts`, `web/src/app/web.css` (only if the new row needs a style)
- Test: `web/src/ask/flattenGameflow.test.ts`, `web/e2e/gameflow.spec.ts`

**Interfaces:**
- Consumes: `RibbonModel.winProb`, `winChanceAt`, `winPct`, `Run.wpFrom/wpTo`.

- [ ] Step 1: Repo: inside `read()`'s snapshot, before `ribbonModel`, set `raw.win_prob` from `game_win_probability($1::text, $2::int4)` with `[euro ? "euro" : "il", game]`. A failure of this query must not fail the page: catch, log with `logEvent` (code only, per web/CLAUDE.md privacy rule), continue with none.
- [ ] Step 2: Score tooltip: a row under the teams, "Win 71%" under own and "Win 29%" under opp (`winPct` of the point and of 1 - own), in both the ratings grid and the no-ratings text form; nothing when the series is empty. Phone score readout the same.
- [ ] Step 3: Run texts: the chip's aria-label and the phone run row append ", win chance 41% → 78%" / " · 41% → 78%". Chip visible text unchanged ("37-5 ›").
- [ ] Step 4: Ask: `gameflowRows` run rows gain `win_chance: "41% → 78%"` (or omitted when null); extend `flattenGameflow.test.ts`.
- [ ] Step 5: e2e (`gameflow.spec.ts`, fixtures): hovering the margin band of 673 shows a tooltip containing "Win" and a percent; grep `e2e/` for "defining stretch", "37-5" and run labels and update any pin whose run changed (Task 3's fixture output tells you). Run the fixture e2e locally only if the user is not running the dev server (memory: dev server + e2e load).
- [ ] Step 6: `npm test`, `npx tsc --noEmit`, `npm run lint` pass (run npm/npx via PowerShell; npx is broken under the Bash tool).
- [ ] Step 7: Commit "gameflow: win chance in the score readout and runs".

### Task 5: Contract, push, Preview

**Files:**
- Modify: `docs/shared_definitions.md` ("Gameflow run" row; WPA row), `web/CLAUDE.md` (Gameflow section, one bullet)

- [ ] Step 1: Contract: "Gameflow run" row -> the three shown are ranked by win-probability swing (ties net, shorter, earlier); defining = winner's largest-swing run; with no probabilities, the 2026-11-17 rules. WPA row: "shown in Gameflow runs (since <merge date>)"; keep "never credited to players".
- [ ] Step 2: web/CLAUDE.md Gameflow bullet: where the series comes from, the fallback, and the score-readout row.
- [ ] Step 3: Commit, push `react/winprob`; open a PR (template; branch type react).
- [ ] Step 4: Watch the Preview live checks in the background (no long polling); when green, check one Israeli, one EuroLeague and one EuroCup game on the Preview in a browser (desktop and phone width): readout shows Win %, runs show the swing. Send the user the Preview link.
- [ ] Step 5: Stop. Merge only when the user says "deploy".
