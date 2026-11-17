# Player Stats stat views Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Split the ~29-column Player Stats table into `Main | Shooting | All` view tabs, move the display mode to a pill switch, and soften heat on counting columns.

**Architecture:** Column sets are a pure rule in `web/src/rules/playerStatsTable.ts` (`playerViewColumns`), unit-tested. `PlayerStatsTable` filters its columns through it; `PlayerStatsClient` owns the new `cols` URL key, renders `ViewTabs` for the column view and a new shared `PillSwitch` for the mode. No API, SQL or number changes.

**Tech Stack:** Next.js / React 19, TypeScript, TanStack Table (`DataTable`), vitest, Playwright.

**Spec:** `docs/superpowers/specs/2026-11-17-player-stats-views-design.md`

## Global Constraints

- Branch `react/ps-views` off `origin/main` (4dba705 or later). The main checkout is on `react/openapi` with uncommitted edits (some in `PlayerStatsClient.tsx`): never touch them. Work in a scratch copy (memory `feedback-work-beside-another-session`: `git archive origin/main` into the scratchpad, commit via a temp index / `commit-tree`, or a plain clone there); no extra worktrees in the repo folder.
- Run npm/npx via PowerShell (`npm.cmd`, `npx.cmd`). Files with backslashes: Write/Edit tools only.
- URL: `view` = mode (unchanged values `Totals`, `Per Game`, `Per 60 Possessions`, `Per 30 Minutes`, default `Per Game`); new `cols` = `main` (default, omitted) | `shooting` | `all`. `cols` must never reach `/api/v1/.../player-stats`.
- Main: `team_name, Player, gp, minutes, pts, reb, ast, stl, blk, tov, fg_pct, tp_pct, ft_pct, ts, usg_pct`
- Shooting: `team_name, Player, gp, minutes, fgm, fga, fg_pct, 2pm, 2pa, two_pct, 3pm, 3pa, tp_pct, ftm, fta, ft_pct, efg, ts`
- All: today's columns and order (DFL Israeli only, as today).
- Per 60 Possessions adds `base_poss_pg` ("Poss/G") right after `minutes` in Main and Shooting; Per 30 Minutes adds `base_min_pg` ("Min/G") there. In All, positions unchanged.
- Pill labels: `Totals | Per game | Per 60 poss | Per 30 min` (visible text); each button's `title` is the full mode name.
- Soft heat on counts: `pts reb oreb dreb ast stl blk dfl tov fgm fga 2pm 2pa 3pm 3pa ftm fta`. Full heat: `fg_pct two_pct tp_pct ft_pct efg ts usg_pct`.
- Check light AND dark theme, 390 and 1440 px, all three competitions (il, euroleague, eurocup).

## Review Focus

1. Saved column order/hidden list from before (localStorage `onoff.ts.visible_col_order.v1`, `.hidden`) must still apply and must not resurrect columns outside the current view -- test in Task 2.
2. A user who hid every non-locked column of Main, then opens Shooting, still sees Shooting's columns they didn't hide -- covered by "hidden applies per id" test in Task 2.
3. An old link `?view=Per%2030%20Minutes` (no `cols`) opens Main in Per 30 with Min/G -- e2e in Task 3.
4. A junk `cols=foo` falls back to Main, no error -- unit test in Task 1 (`parsePlayerColumnView`).
5. Changing `cols` must not refetch data (no `cols` in the API path) -- unit test in Task 1 on `playerStatsPath`.

---

### Task 1: Column-view rule + URL key

**Files:**
- Modify: `web/src/rules/playerStatsTable.ts`
- Modify: `web/src/lib/tabQuery.ts` (`PLAYER_QUERY_DEFAULTS`, `playerStatsPath`)
- Test: `web/src/rules/playerStatsTable.test.ts`, new `web/src/lib/tabQuery.test.ts` (or add to an existing test that imports `tabQuery` if one exists)

**Interfaces:**
- Produces:
  - `export const PLAYER_COLUMN_VIEWS = ["Main", "Shooting", "All"] as const; export type PlayerColumnView = (typeof PLAYER_COLUMN_VIEWS)[number];`
  - `export function parsePlayerColumnView(cols: string | null | undefined): PlayerColumnView` (`"shooting"`→Shooting, `"all"`→All, else Main)
  - `export function playerColumnViewParam(v: PlayerColumnView): string` (`"main" | "shooting" | "all"`)
  - `export function playerViewColumns(view: PlayerColumnView, mode: PlayerMode, league: League): string[]` -- ordered column ids incl. `team_name`, `Player`, and the base column for the mode.
  - `export const PLAYER_SOFT_HEAT: ReadonlySet<string>` (the counts list above)
  - `PLAYER_QUERY_DEFAULTS.cols = "main"`

- [ ] **Step 1: Write failing tests** in `playerStatsTable.test.ts`:
  - `playerViewColumns("Main", "Per Game", "il")` equals the Main list exactly.
  - `playerViewColumns("Shooting", "Per Game", "euroleague")` equals the Shooting list.
  - `playerViewColumns("Main", "Per 60 Possessions", "il")` has `base_poss_pg` at index 4; `"Per 30 Minutes"` has `base_min_pg` at index 4; Totals has neither.
  - `playerViewColumns("All", "Per Game", "il")` contains `dfl`; with `"euroleague"` it doesn't; length il = 29 (team, player + 27 labels).
  - `parsePlayerColumnView("foo")`, `(null)` → `"Main"`; `"all"` → `"All"`; round-trips with `playerColumnViewParam`.
  - In the tabQuery test: `playerStatsPath("il", {...PLAYER_QUERY_DEFAULTS, season: "2026", cols: "shooting"})` does not contain `cols=`.
- [ ] **Step 2: Run** `npx.cmd vitest run src/rules/playerStatsTable.test.ts src/lib/tabQuery.test.ts` -- expect FAIL (exports missing).
- [ ] **Step 3: Implement.** `All` = `["team_name","Player", ...]` built from `PLAYER_LABELS` order with the same DFL/base rules `PlayerStatsTable` uses today (move that ordering logic here so the table and the rule share one list). Add `"cols"` to `playerStatsPath`'s delete list.
- [ ] **Step 4: Run** the same command -- PASS. Also `npx.cmd tsc --noEmit`.
- [ ] **Step 5: Commit** `feat(player-stats): column views rule and cols URL key`.

### Task 2: Table renders the chosen view, soft heat on counts

**Files:**
- Modify: `web/src/components/shiny/PlayerStatsTable.tsx`
- Modify: `web/src/app/web.css` (append to the modern block at the end)

**Interfaces:**
- Consumes: `playerViewColumns`, `PLAYER_SOFT_HEAT`, `PlayerColumnView` (Task 1).
- Produces: `PlayerStatsTable` prop `columnView: PlayerColumnView`.

- [ ] **Step 1: Build columns from `playerViewColumns("All", mode, league)`**, then: saved order applied as today over the full list; `visible = ordered.filter(c => viewIds.includes(c.id))`; `DataTable` gets `visible.filter(not hidden)`; `ColumnsMenu` gets `visible` (so it lists only this view's columns), the same `hidden` list and `move`. `move` reorders within the full stored list (moving A before B in a view keeps other views' columns where they are relative to their neighbours).
- [ ] **Step 2: Soft heat.** For ids in `PLAYER_SOFT_HEAT`, return `className: "ibpl-heat-soft"` on the cell alongside `bg`. In `web.css`, make `.ibpl-heat-soft` render the heat at about half strength in both themes (e.g. `background-color: color-mix(in srgb, var(--ibpl-heat) 50%, transparent) !important`; check it wins over the inline style and over light.css's `.ibpl-heat` tint). Rate columns unchanged.
- [ ] **Step 3: Verify** `npx.cmd tsc --noEmit` and `npx.cmd vitest run src/rules` pass. Rendering is checked in Task 3.
- [ ] **Step 4: Commit** `feat(player-stats): table shows one column view; softer heat on counts`.

### Task 3: View tabs + mode pill switch in the page, e2e

**Files:**
- Create: `web/src/components/shiny/PillSwitch.tsx`
- Modify: `web/src/components/PlayerStatsClient.tsx`, `web/src/app/web.css`
- Test: `web/e2e/player-stats.spec.ts`

**Interfaces:**
- Consumes: Task 1 exports; Task 2 `columnView` prop.
- Produces: `export default function PillSwitch({ label, options, value, onChange }: { label: string; options: { value: string; label: string; title?: string }[]; value: string; onChange: (v: string) => void })` -- a `role="group"` with `aria-label={label}` of `aria-pressed` buttons, class `ibpl-pill-switch` (same look as `.ibpl-compare-metrics`: pills, selected = `btn-warning`, others `btn-outline-secondary`, `btn-sm`). Compare is not migrated in this plan.

- [ ] **Step 1: Update e2e first** (`player-stats.spec.ts`):
  - Golden preset test: navigate with `cols=all` added to `g.query` so the existing cell-by-cell check still covers every column.
  - New test per league: default load → `th` ids equal `playerViewColumns("Main","Per Game",league)`; click view button "Shooting" → URL has `cols=shooting`, headers equal the Shooting list; no new request to `/api/v1/*/player-stats` after the click (count route hits).
  - Old link `?view=Per%2030%20Minutes` → Main headers with `base_min_pg` at index 4; pill "Per 30 min" has `aria-pressed="true"`.
  - Interactions test: replace the click on button "Per 30 Minutes" with the pill `getByRole("group", { name: "Mode" }).getByRole("button", { name: "Per 30 min" })`.
  - Columns menu: hide `PTS` in Main, switch to All → `PTS` still hidden; Shooting → its columns all shown.
  - The existing no-horizontal-page-scroll assertion stays and runs at both widths.
- [ ] **Step 2: Run** `npx.cmd playwright test e2e/player-stats.spec.ts --workers=1` against the dev server on 3100 (check it is up and is this branch; don't run while the user is testing) -- expect FAIL.
- [ ] **Step 3: Implement.** `columnView = parsePlayerColumnView(query.cols)`; `ViewTabs views={[...PLAYER_COLUMN_VIEWS]} view={columnView} onView={(v) => set({ cols: playerColumnViewParam(v) })}` (Main must clear the key -- set the default so the URL omits it); `PillSwitch label="Mode"` with the four modes (visible labels per Global Constraints, `title` = mode) calling the existing `onMode`. Wrap both in `<div className="ps-views-row">`: flex, `flex-wrap: wrap`, `align-items: end`, gap 8px; at <768 px the switch goes on its own line under the tabs. Pass `columnView` to `PlayerStatsTable`; do NOT add it to the table's `key` (no remount needed). `TabFrame` keeps `views={[...PLAYER_MODES]}`.
- [ ] **Step 4: Run** the spec again -- PASS. Then `npx.cmd vitest run` and `npx.cmd tsc --noEmit` and `npx.cmd eslint src` -- clean.
- [ ] **Step 5: Visual check.** Screenshots of il and euroleague Player Stats at 390 and 1440, light and dark (each view once), and one at 1024. Look for: switch wrapping cleanly, tabs fade not clipping, soft heat visibly lighter than rate heat in both themes. Repeat view/mode switching several times on both widths. Send the screenshots to the user.
- [ ] **Step 6: Commit** `feat(player-stats): Main/Shooting/All view tabs, mode as a pill switch` (include the spec and this plan).

### Task 4: Ship

- [ ] **Step 1:** Push `react/ps-views`; open a PR to `main` (body ends with the Claude Code line). Wait for the Vercel Preview and `web-preview-live.yml` on the head commit.
- [ ] **Step 2:** Open the Preview at 390 and 1440 and check one view switch per league.
- [ ] **Step 3:** Stop. Merging deploys production -- only on the user's explicit "deploy".
