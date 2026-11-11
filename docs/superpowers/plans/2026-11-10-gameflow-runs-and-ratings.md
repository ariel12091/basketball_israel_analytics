# Gameflow Runs and Ratings Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Mark scoring runs on the web Gameflow with a tooltip that explains them (lineup first, then the results that stand out against the rest of the game), and add Net/ORtg/DRtg to the player card and Net to the bar tooltip, in both leagues.

**Architecture:** Both Gameflow SQL readers gain one `events` JSON column: every possession end, shot, free throw, turnover and offensive rebound, from both teams' perspectives, each carrying the five player ids of its **credited** lineup (On/Off convention). EuroLeague exposes those rows through a new curated view (migration 061). Web rules in `web/src/rules/` only sum those rows into ratings, detect runs from the existing score steps, and pick the standout numbers; the React Gameflow components display them.

**Tech Stack:** PostgreSQL (Supabase), R (`app/R/global.R` holds the readers; `scripts/gameflow_r_sources.R` exports them to JSON), Python apply scripts for EuroLeague migrations, Next.js 16 / React 19 / TypeScript, vitest, Playwright.

**Spec:** `docs/superpowers/specs/2026-11-10-gameflow-runs-and-ratings-design.md`

## Global Constraints

- Branch `react/gf-runs` (already created off `main`, spec committed as 92ab1b4). Merge to `main` only after the Preview live checks pass on the head commit; a push to `main` deploys Production.
- `docs/shared_definitions.md` is the contract: Task 1 lands before any code.
- Ratings follow the On/Off convention (credited lineup). +/-, points for/against, bars and floor time stay physical.
- Ratings are shown with their possession count: `Net +31.4 on 14 poss`. Rates always with counts: `6/10`, never a bare `60%`.
- Run rule: net ≥ 10 for the run team while the other team scores ≤ 6; starts at the run team's first point, ends at its last; greedy by net (shorter first on tie), no overlap, at most 3; the first is the defining stretch.
- A three-point standout is labelled shooting variance, never credited to the lineup.
- Use `rRound` for displayed decimals and `ratingLabel()` for ratings (one decimal, dash when no possession).
- Run npm through PowerShell (`npm.cmd`); write files containing backslashes with the Write/Edit tools; DB scripts as temp `.R` files, never long `Rscript -e`.
- Live-DB tests on port 5432 only, never while anything else queries as `app_readonly`.
- After the EuroLeague migration: re-run `scripts/apply_db_security.R` with `CONFIRM_DB_SECURITY_APPLY=1`.
- `app/R/global.R` has mixed line endings: after editing it, check `git diff --stat` shows only the intended lines (see root `CLAUDE.md`, Deploy Scripts).

## Review Focus

1. **A game with no runs** (game 440 has none under the rule): no band, no chip, no empty tooltip, and the chart is unchanged. Test in Task 6.
2. **A player whose credited lineup has no possession in a window** (a 0:20 stint): card shows `–` ratings and `0 poss`, never `NaN` or `Infinity`. Test in Task 4.
3. **EuroLeague game with a credit lineup missing from `lineup_totals_by_game`**: its events drop out of the view; the pin in Task 3 must fail on it instead of silently passing. Test in Task 3.
4. **Two runs adjacent in time** (Hapoel 30-4 ends Q3 4:18, Maccabi 37-5 starts Q3 2:25): bands and chips must not overlap at 390 px. Test in Task 7.
5. **A run crossing a period break**: band spans the quarter line and the tooltip's window reads across it (`Q3 2:25 – Q4 2:31`). Test in Task 6 (game 673's defining run does this).

---

### Task 1: Contract entries

**Files:**
- Modify: `docs/shared_definitions.md` (section 2 R6; section 3 Rates table; section 5 register)

- [ ] **Step 1: Amend R6** — replace "Floor time, games played and Gameflow use the physical lineup columns." with: floor time, games played and Gameflow's bars, +/-, points for/against and margin use the physical columns; Gameflow stint ratings are events and follow credit.
- [ ] **Step 2: Add three rows to Rates**:
  - *Gameflow stint rating*: ORtg = `100 × pts / poss` over the side's offensive event rows whose credited lineup contains every selected player (and, for a stint, whose elapsed is within the stint, both ends inclusive); DRtg the same over defensive rows; Net = ORtg − DRtg. Owner of the rows: the `events` CTE of `RIBBON_SQL_ISRAEL` / `RIBBON_SQL_EURO`; rates: `stintRatings()` (`web/src/rules/gameflowRatings.ts`).
  - *Gameflow run*: the rule from Global Constraints. Owner: `detectRuns()` (`web/src/rules/gameflowRuns.ts`).
  - *Gameflow run standout*: the rule from Task 6 Step 3, compared with the rest of the same game only (no season fallback). Owner: `runStandouts()`, same file.
- [ ] **Step 3: Update the "Gameflow ORtg" row**: drop "Israeli league only"; EuroLeague possession ends come from `euroleague.ribbon_events_v` (migration 061).
- [ ] **Step 4: Register entry**: next free D number, "EuroLeague Gameflow stint ratings credit a free-throw trip to the lineup on the floor for its last free throw (inherits D1); Israeli credits the lineup that drew the foul. Follow-up: align D1." Dated 2026-11-10.
- [ ] **Step 5: Commit** — `git add docs/shared_definitions.md && git commit -m "docs: Gameflow stint ratings and runs in the contract"`

### Task 2: Israeli reader returns credited events

**Files:**
- Modify: `app/R/global.R:271-383` (`RIBBON_SQL_ISRAEL`)
- Regenerate: `web/src/data/gameflowSql.json`
- Create: `app/tests/testthat/test-gameflow-events-db.R`

**Interfaces:**
- Produces: column `events` (jsonb array, ordered by `elapsed, order_key`) of rows
  `{team_id:int, elapsed:num, order_key:int, off:bool, players:int[5], pts:int, kind:"fg2"|"fg3"|"ft"|"tov"|"oreb"|null, made:bool|null, poss_end:bool}`.
  Both perspectives: each real event appears once per team, `off` true on the offense's row.

- [ ] **Step 1: Write the failing DB test** `test-gameflow-events-db.R`, gated `skip_if_not(identical(Sys.getenv("RUN_DB_TESTS"), "1"))`, port 5432. For games 673 and 390: run `RIBBON_SQL_ISRAEL` (extract it with the same regex `test-stint-ribbon-readers.R` uses), parse `events`, and for every `(team_id, player_id)` in `basketball_test.player_four_factors_by_game` with the player ON (`is_on_key` = its on value; read the column's values first), assert:
  - sum of `poss_end` over rows with `team_id` = team, `off` = TRUE, player ∈ `players` equals the FF table's `SUM(total_poss)` for `type_lineup = 'offense'`; same for `pts` vs `SUM(total_points)`; same pair for `off` = FALSE vs `type_lineup = 'defense'`.
  - Also per team: `SUM(poss_end)` with `off` = TRUE equals `team_metrics_by_game_mv`'s possessions for that team-game.
- [ ] **Step 2: Run it, expect FAIL** (`events` column missing): `RUN_DB_TESTS=1 "$RSCRIPT" -e "testthat::test_file('app/tests/testthat/test-gameflow-events-db.R')"`
- [ ] **Step 3: Add an `events` CTE** to `RIBBON_SQL_ISRAEL` and `(SELECT jsonb_agg(to_jsonb(events) ORDER BY elapsed, order_key) FROM events) AS events` to the final SELECT. Source rows: `df_pts_poss_lineups_longer_mv` with `game_id = $1` (both teams), `type_lineup IN ('offense','defense')`, kept when `final_end_poss` or `team_score > 0` or `kind` is not null. `players` from the existing `lineup_players` CTE joined on `credit_lineup_hash`. `kind`: shot with `parameters_points` 2 → `fg2`, 3 → `fg3`; `freeThrow` → `ft`; `turnover` → `tov`; `rebound` with `parameters_type = 'offensive'` → `oreb`. `made` = `parameters_made = 'made'` for shots and free throws, else NULL. `pts` = `COALESCE(team_score, 0)`.
- [ ] **Step 4: Regenerate the JSON**: `"$RSCRIPT" -e "source('scripts/gameflow_r_sources.R'); gameflow_export_sources()"`; `git diff --stat web/src/data/` shows only `gameflowSql.json` (and no layout change).
- [ ] **Step 5: Run the DB test and the existing ribbon tests, expect PASS**: the new file, then `test-stint-ribbon-readers.R`. If a pin fails, fix the CTE, not the test.
- [ ] **Step 6: Measure the payload**: byte size of the `events` value for game 673 raw and gzipped; record both in the commit message.
- [ ] **Step 7: Commit** `app/R/global.R web/src/data/gameflowSql.json app/tests/testthat/test-gameflow-events-db.R` — `"sql: Gameflow reader returns credited events (Israeli)"`

### Task 3: EuroLeague events view and reader

**Files:**
- Create: `euroleague/sql/061_ribbon_events_read_layer.sql`, `euroleague/scripts/apply_061_ribbon_events_read_layer.py` (model both on 053/054 and `apply_054_ribbon_margin_own_score.py`)
- Modify: `sql/security/enable_readonly_rls.sql:116-117`, `sql/security/audit_app_access.sql:127-128` (add `ribbon_events_v`)
- Modify: `app/R/global.R:385-426` (`RIBBON_SQL_EURO`), regenerate `web/src/data/gameflowSql.json`
- Modify: `app/tests/testthat/test-gameflow-events-db.R`

**Interfaces:**
- Produces: `euroleague.ribbon_events_v(game_id, team_id, period, elapsed_seconds, source_event_order, type_lineup, player_ids int[], points, fg2_att, fg2_made, fg3_att, fg3_made, ft_attempts, turnovers, orebounds, final_end_poss, own_team_score, opp_team_score)`, from `euroleague.action_team_context_actions` joined to `(SELECT DISTINCT game_id, team_id, own_lineup, player_ids FROM euroleague.lineup_totals_by_game)` on `(game_id, team_id, own_lineup)` exactly as `ribbon_segments_v` does (never pair `own_lineup` and `player_ids` positionally), rows with `type_lineup IN ('offense','defense')` that end a possession or have any of the counted fields > 0. `GRANT SELECT ... TO app_readonly`.
- Produces: `RIBBON_SQL_EURO` returns `events` in the Task 2 shape (`kind` from the count columns; `ft` rows' `made` = `points > 0`) and a real `poss` column (`team_id = $2`, `final_end_poss`, with `own`/`opp` from the view's scores) replacing `NULL::jsonb AS poss`.

- [ ] **Step 1: Extend the DB test** with one EuroLeague and one EuroCup game (pick the latest published of each from `euroleague.final_schedule_mv`): the same four ON pins against `euroleague.player_four_factors_by_game`, plus: number of distinct `(game_id, team_id, own_lineup)` in `action_team_context_actions` for that game with no match in `lineup_totals_by_game` is 0 (Review Focus 3).
- [ ] **Step 2: Run it, expect FAIL** (view missing).
- [ ] **Step 3: Write 061 and its apply script** (dry run by default, applies with a confirm env var as 054 does). Apply.
- [ ] **Step 4: Time the view on a full season**: `EXPLAIN (ANALYZE, BUFFERS)` of `SELECT * FROM euroleague.ribbon_events_v WHERE game_id = <id>` for a 2025 game and a 2026 game; no `Nested Loop` over a CTE scan; under 1 s. Record times in the commit message.
- [ ] **Step 5: Security**: add `ribbon_events_v` to both security files, then `CONFIRM_DB_SECURITY_APPLY=1 "$RSCRIPT" scripts/apply_db_security.R`; confirm `app_readonly` can `SELECT` from the view.
- [ ] **Step 6: Update `RIBBON_SQL_EURO`**, regenerate the JSON, run the DB test and `test-stint-ribbon-readers.R` and `test-db-security-contracts.R`, expect PASS.
- [ ] **Step 7: Commit** — `"sql: EuroLeague ribbon_events_v (061) and Gameflow events/possessions"`

### Task 4: Events in the web model and `stintRatings()`

**Files:**
- Modify: `web/src/rules/gameflow.ts` (`RawRibbon`, `ribbonModel`)
- Create: `web/src/rules/gameflowRatings.ts`, `web/src/rules/gameflowRatings.test.ts`
- Create: `scripts/save_gameflow_fixture.R` (runs `RIBBON_SQL_ISRAEL` for a game/team on port 5432 and writes the row as `RawRibbon` JSON: `lanes`, `margin`, `poss`, `events`, `n_periods`, the three warning counts)
- Create: `web/src/rules/__fixtures__/gameflow-673.json` and `gameflow-440.json` (team 2), written by that script

**Interfaces:**
- Consumes: Task 2/3 `events` shape.
- Produces:
  - `export type GameflowEvent = { side: Side; elapsed: number; order_key: number; off: boolean; players: string[]; pts: number; kind: "fg2" | "fg3" | "ft" | "tov" | "oreb" | null; made: boolean | null; poss_end: boolean }` — `side` own when `team_id === team`; `players` as `String(player_id)` so they compare to `Lane.player_key`.
  - `RawRibbon.events?: RawEvent[] | null`; `ribbonModel(...).events: GameflowEvent[]` (empty array when the reader returned none).
  - `export type StintRatings = { offPoss: number; offPts: number; defPoss: number; defPts: number; ortg: number | null; drtg: number | null; net: number | null }`
  - `export function stintRatings(events: GameflowEvent[], side: Side, players: string[], window?: { start: number; end: number }): StintRatings`

- [ ] **Step 0: Save the fixtures**: `"$RSCRIPT" scripts/save_gameflow_fixture.R 673 2` and `... 440 2`.
- [ ] **Step 1: Write failing tests** in `gameflowRatings.test.ts`:
  - hand-built events: two offensive possession ends with 3 and 0 pts, one defensive end with 2 pts, all crediting `["1","2","3","4","5"]` → for `["1"]`: `offPoss 2, offPts 3, ortg 150, defPoss 1, drtg 200, net -50`.
  - membership: an event crediting a lineup without `"2"` is excluded for `["1","2"]` and included for `["1"]`.
  - window: an event at `elapsed === window.end` is included; at `end + 1` excluded.
  - empty: no possession → `ortg`, `drtg`, `net` all `null`, counts `0` (Review Focus 2).
  - fixture 673: for every Maccabi player, `stintRatings(model.events, "own", [key])` equals the per-player ON totals asserted in Task 2's DB test (copy the expected numbers for three players — DiBartolomeo, Wallace, Brissett — into the test from that test's output).
- [ ] **Step 2: Run, expect FAIL**: `npm.cmd test -- gameflowRatings`
- [ ] **Step 3: Implement** the type, the model change and `stintRatings()`; ratings are `100 × pts / poss`, unrounded (display rounds).
- [ ] **Step 4: Run, expect PASS**; also `npm.cmd test -- gameflow` (existing tests unchanged) and `npm.cmd run typecheck`.
- [ ] **Step 5: Commit** — `"web: Gameflow events in the model and stintRatings()"`

### Task 5: Ratings on the card and the bar tooltip

**Files:**
- Modify: `web/src/components/shiny/GameflowPanel.tsx` (`CardStats`, `StintStats`, `GameflowDetails` card builders ~lines 555-700, hover tip ~lines 528-534)
- Modify: `web/e2e/gameflow.spec.ts`

**Interfaces:**
- Consumes: `stintRatings()`, `ribbonModel(...).events`, `ratingLabel()`.
- Produces: `CardStats.ratings?: StintRatings`.

- [ ] **Step 1: Write failing e2e checks** (390 and 1440 px, game 673 team 2 with client data from the fixture): clicking DiBartolomeo's row shows `Net`, `ORtg`, `DRtg` entries with the values from Task 4's fixture test and `on N poss`; hovering one of his bars shows `· Net ` after the `+/-`; selecting two Maccabi players shows ratings on "Together"; selecting one player from each team shows no ratings on "Head to head"; a EuroLeague game shows ratings too.
- [ ] **Step 2: Run, expect FAIL**: `npx.cmd playwright test e2e/gameflow.spec.ts`
- [ ] **Step 3: Implement**: whole-game card → `stintRatings(events, side, [key])`; stint card and hover tip → same with the bar's `{start, end}`; Together → all selected keys, no window; Head to head → none. In `StintStats` add three `dt/dd` pairs after Points: `Net` (`+`-signed), `ORtg`, `DRtg`, each `ratingLabel(x)` plus `on {offPoss} poss` (DRtg uses `defPoss`). Hover tip line: `… · +/- {pm} · Net {net} on {offPoss + defPoss} poss`. Info text under the card: "Ratings count free throws for the lineup that drew the foul, as On/Off does, so they can differ slightly from the +/- beside them."
- [ ] **Step 4: Run, expect PASS**, plus `npm.cmd run lint`, `typecheck`.
- [ ] **Step 5: Commit** — `"web: Net/ORtg/DRtg on the Gameflow card and Net in the bar tooltip"`

### Task 6: Run detection, lineup changes and standouts

**Files:**
- Create: `web/src/rules/gameflowRuns.ts`, `web/src/rules/gameflowRuns.test.ts`

**Interfaces:**
- Consumes: `ribbonModel` output (`steps`, `lanes`/`bars`, `events`), `clock()`.
- Produces:
  - `export type Run = { side: Side; start: number; end: number; pts: number; oppPts: number; net: number; defining: boolean; fromMargin: number; toMargin: number }` (`start`/`end` = elapsed of the run team's first and last scoring step; margins from the own team's perspective just before `start` and at `end`).
  - `export function detectRuns(steps: Score[]): Run[]` — sorted by `start`.
  - `export type RunLineups = { side: Side; onSince: number; cameIn: string[]; wentOut: string[]; changesDuring: number; stayedOn: boolean }[]` (both sides; player labels).
  - `export function runLineups(model: RibbonModel, run: Run): RunLineups`
  - `export type Standout = { side: Side; metric: "fg2" | "fg3" | "ft" | "tov" | "oreb" | "rtg"; inRun: { made: number; att: number }; rest: { made: number; att: number }; variance: boolean }`
  - `export function runStandouts(events: GameflowEvent[], run: Run): Standout[]` — at most 2.

- [ ] **Step 1: Write failing tests**:
  - `detectRuns` on fixture 673: exactly three runs; defining = own, `pts 37, oppPts 5`, `clock(start)` `"Q3 2:25"`, `clock(end)` `"Q4 2:31"`; then opp `30-4` `Q2 2:50`–`Q3 4:18`; then own `16-6` `Q1 3:45`–`Q1 0:48` (values from the R prototype in the spec). Hand-built: 10-6 qualifies, 9-0 does not, 16-7 does not; a tie on net keeps the shorter window.
  - `detectRuns` on a fixture for game 440 (save it like 673): `[]` (Review Focus 1).
  - `runLineups(673 defining)`: own `onSince` = elapsed of Q3 3:54 (1566), `cameIn` contains DiBartolomeo, `wentOut` contains Brissett; opp side lists Blakeney and Timor in, Bryant and Carrington out.
  - `runStandouts(673 defining)`: includes own `fg3` with `variance: true` and opp `tov`; every standout has `inRun.att ≥ 6` for shooting, `≥ 8` possessions for `tov`/`rtg`.
- [ ] **Step 2: Run, expect FAIL**: `npm.cmd test -- gameflowRuns`
- [ ] **Step 3: Implement.** Scoring events from consecutive `steps` (`own` and `margin` give each side's increment). Candidate windows: every (i, j) of the same side's scoring steps with that side's points − other side's ≥ 10 and other side's ≤ 6; greedy selection as in Global Constraints. `runLineups`: for each side, the bar set covering the run's `start`; `onSince` = latest start among that five's current bars (the moment the last of them came on); `cameIn`/`wentOut` = difference against the five just before `onSince`. Standout rule: per side and metric, compare the run's rate to the rest of the game's (shooting: made/att; `tov`: per possession; `oreb`: per own missed shot; `rtg`: pts per possession); qualifies when the run has the minimum attempts above, the rest of the game has at least as many, and the absolute difference is at least 20 percentage points (shooting, `tov`, `oreb`) or 40 points per 100 (`rtg`); rank by difference ÷ that threshold; keep 2. `variance` = `metric === "fg3"`. No season-level fallback: when the rest of the game is too short, the metric is not shown (my call; the spec's fallback is dropped to keep the payload single-game). Task 1's standout row says so.
- [ ] **Step 4: Run, expect PASS**, plus typecheck.
- [ ] **Step 5: Commit** — `"web: Gameflow run detection, lineup changes and standouts"`

### Task 7: Run bands, chips and tooltip

**Files:**
- Modify: `web/src/components/shiny/GameflowChart.tsx` (band `<rect>`s under the margin path and lanes; chip row above), `web/src/components/shiny/GameflowPanel.tsx` (tooltip; tap on touch via React state)
- Modify: `web/src/app/web.css` (run tokens; check existing class names before naming — see memory `feedback-web-css-traps`)
- Modify: `web/e2e/gameflow.spec.ts`

**Interfaces:**
- Consumes: `detectRuns`, `runLineups`, `runStandouts`, `clock()`, `data.meta.ownTeam/oppTeam`.

- [ ] **Step 1: Write failing e2e checks** (390 and 1440): game 673 shows three chips; the defining chip reads `37-5 ›` in the run team's colour and is filled, with `aria-label` naming the team in full (`Maccabi Tel-Aviv 37-5 run, defining stretch`) — no abbreviation, since both teams in a derby share words; the band spans the Q3/Q4 line; hovering (1440) or tapping (390) the chip opens a tooltip whose lines, in order, are the window and swing, the lineup line containing `since Q3 3:54` and `DiBartolomeo`, a line with `from three` and `(… the rest of the game)`, and a line starting `Estimated cause`; the three-point line says it is shooting variance; at 390 the Hapoel and Maccabi chips' boxes do not intersect (Review Focus 4); game 440 shows no chips and its chart markup has no band elements.
- [ ] **Step 2: Run, expect FAIL.**
- [ ] **Step 3: Implement.** Band colour from the run team's side tokens; defining band stronger opacity, filled chip; other runs lighter band, outlined chip. Chip row: if two chips would overlap, offset the later one onto a second row. Tooltip copy (exact):
  - (`{TEAM}` is the full team name from `data.meta`.) `{TEAM} {pts}-{oppPts} · {clock(start)} – {clock(end)} · from {down|up} {abs(fromMargin)} to {down|up|level} {abs(toMargin)}` (own team's perspective).
  - Lineup, per side, run team first: `{TEAM}: this five had been on since {clock(onSince)} ({cameIn} in for {wentOut})` and, if `changesDuring > 0`, `; {n} change(s) during the run`, else `; stayed on throughout`. When both sides' `onSince` is earlier than the other team's last score before `start` and `changesDuring` is 0 for both, append ` — same fives as before the run`.
  - Each standout: `{TEAM} {made}/{att} from three ({rest.made}/{rest.att} the rest of the game)`; `tov`: `{TEAM} {made} turnovers in {att} possessions ({rrate}% otherwise)`; analogous for 2PT, FT, OREB, rating. A `variance` line ends with ` — shooting variance`.
  - `Estimated cause: the lineup change at {clock(onSince)} lines up with the run; this is a timing match, not proof.` When the only standouts are `variance`, use `Estimated cause: hot three-point shooting; no lineup effect is claimed.`
- [ ] **Step 4: Run, expect PASS**, plus lint, typecheck, full `npm.cmd test`. Run the `react-doctor` skill on the changed components.
- [ ] **Step 5: Commit** — `"web: Gameflow run bands, chips and explanation tooltip"`

### Task 8: Live checks and Preview

**Files:**
- Modify: `web/e2e/gameflow-live.spec.ts`

- [ ] **Step 1: Add live checks** (`LIVE_DB_SMOKE=1`): Israeli game 673 team 2 and one EuroLeague game return `events` with length > 0 and a non-null `poss`; the 673 page shows the `37-5 ›` chip and a `Net` on DiBartolomeo's card.
- [ ] **Step 2: Run locally against the production-mode server on 3100 (PG_PORT=5432)**, expect PASS.
- [ ] **Step 3: Push `react/gf-runs`**; wait for `web-preview-live.yml` on the head commit; expect green.
- [ ] **Step 4: Report** the Preview URL for game 673 to the user. Do not merge to `main` until the user says to deploy (memory: production deploy needs explicit words).
