# Stats-tab layout, part 1: shared kit + On/Off — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the shared stats-tab pattern (toolbar, right-side filters drawer, on-page view tabs, one-line key, muted green/red palette, clear "more info" marker, data columns first) in the web tab kit and apply it to On/Off for all three leagues.

**Architecture:** Every new piece lives in the shared kit (`TabKit.tsx` and small new files beside it) behind an opt-in: `TabFrame layout="drawer"` and `heat(..., "muted")` / `shotCell(..., palette: "muted")`. On/Off opts in; every other tab keeps today's look until its own plan. No data, route or SQL change.

**Tech Stack:** Next.js / React 19 / TypeScript, TanStack Table (`DataTable.tsx`), Vitest (pure functions only; there is no component-test library), Playwright at 390 and 1440 px.

**Spec:** `docs/superpowers/specs/2026-10-28-stats-tab-layout-design.md`

## Global Constraints

- Do not edit `web/src/styles/shiny/app.css` or `mobile.css` (byte copies of Shiny, pinned by `src/styles/shiny.test.ts`). New CSS goes in `web/src/app/web.css`.
- New web-only text goes in a new `web/src/data/layoutUi.json` (not written by the R export). Do not edit `shinyUi.json` by hand.
- Nothing new in the navbar.
- One layout per viewport width; width breakpoints from `layoutClasses.ts` / existing body classes (`ibpl-collapsed-nav` = phone menu width).
- No number, filter meaning, URL parameter or row order changes. Shiny golden browser tests (`e2e/onoff.spec.ts` presets) must keep passing.
- Every tab other than On/Off must look and behave exactly as before (opt-in only).
- Run npm/npx via PowerShell (`npm.cmd`, `npx.cmd`). One dev server on port 3100; start it with `ASK_ENABLED=true` for the Ask specs.
- Commit after each task to `sql/web-foundation`, staging only the task's files (the working tree holds unrelated uncommitted work).

## Review Focus

1. **Chip edit (pencil) with the drawer closed** — expected: the drawer opens, the owning section expands, that control is focused. Test added in Task 3.
2. **Auto min-poss after a stepper change** — a − / + tap turns auto off; a filter change turns it back on and re-calibrates, exactly as with the slider. Test added in Task 5.
3. **Typed stepper values out of range or off-step** (e.g. "-5", "3005", "123", "abc") — expected: clamped to [min, max] and rounded to the step; non-numbers revert. Unit test in Task 2.
4. **Touch users reaching every tooltip** (header definitions, Net cell 3PT luck / small sample, shot cells, "About this view") at 390 px — expected: one tap opens it, a second tap or outside tap closes it, sorting still works. Test added in Task 7.
5. **Saved column order from before the change** — a user's stored order must not put Usage back at the end. Test added in Task 6.

---

### Task 1: Muted palette for heat and shot cells

**Files:**
- Modify: `web/src/components/shiny/DataTable.tsx` (`heat`, `shotCell` at ~lines 72-175)
- Create: `web/src/components/shiny/palette.ts`
- Test: `web/src/components/shiny/palette.test.ts`
- Modify: `web/src/app/web.css` (shot bar colours under a `.ibpl-muted` scope)

**Interfaces:**
- Produces: `export type HeatPalette = "shiny" | "muted"`; `heat(pr: unknown, reverse: boolean, palette: HeatPalette = "shiny"): string | undefined`; `shotCell(m, { ...opts, palette: "gamelogs" | "onoff" | "muted" })`; `MUTED_HEAT: string[]` (20 entries, index-aligned with `ui.heat.cuts` + 1) and `mutedAccuracy(d: number): string` in `palette.ts`.

- [ ] **Step 1: Write the failing tests** in `palette.test.ts`:
  - `heat(0.0, false, "muted") === MUTED_HEAT[0]` (reddest) and `heat(1.0, false, "muted") === MUTED_HEAT[19]` (greenest); `heat(1.0, true, "muted") === MUTED_HEAT[0]`.
  - For every cut `c` in `ui.heat.cuts`, `heat(c, false, "muted")` and `heat(c, false)` select the same band index (ranking unchanged).
  - `heat(null, false, "muted") === undefined`; `heat("x", false, "muted") === undefined`.
  - `heat(0.5, false)` still equals today's `ui.heat.grad[9]` (shiny default untouched).
  - `MUTED_HEAT[9]` and `MUTED_HEAT[10]` have alpha ≤ 0.06 (middle reads neutral).
  - `mutedAccuracy(1)` is a green, `mutedAccuracy(-1)` a red, `mutedAccuracy(0)` the neutral text colour `"#cfc8bb"`.
- [ ] **Step 2: Run** `npx.cmd vitest run src/components/shiny/palette.test.ts` — expected FAIL (module missing).
- [ ] **Step 3: Implement.** `MUTED_HEAT`: bands 0-9 interpolate `rgba(210,120,105,.30)` → `rgba(210,120,105,.04)`, bands 10-19 `rgba(110,180,130,.04)` → `rgba(110,180,130,.38)`. `mutedAccuracy(d)` interpolates neutral `#cfc8bb` toward `#7fc79a` (d>0) or `#d98b7e` (d<0). In `shotCell`, `palette: "muted"` uses `mutedAccuracy` and adds class `shot-bar-muted` on the bar container; in `web.css`: `.shot-bar-muted .shot-bar-2pt { background: #4a453d; color: #e6e2da }` and `.shot-bar-muted .shot-bar-3pt { background: rgba(232,164,53,.6); color: #14110d }`. Frequency percentages stay inside the bar (the spec drops only the "2s/3s" word labels).
- [ ] **Step 4: Run** the test file — PASS; run `npx.cmd vitest run` — all pass.
- [ ] **Step 5: Commit** `web: muted heat and shot-cell palette (opt-in)`.

### Task 2: Shared stepper, view tabs and more-info marker (kit components)

**Files:**
- Create: `web/src/components/shiny/PossStepper.tsx`, `web/src/components/shiny/ViewTabs.tsx`, `web/src/components/shiny/MoreInfo.tsx`, `web/src/lib/stepValue.ts`
- Test: `web/src/lib/stepValue.test.ts`
- Modify: `web/src/app/web.css`

**Interfaces:**
- Produces: `stepValue(raw: string | number, min: number, max: number, step: number): number | null` (null = not a number); `PossStepper({ id, label, min, max, step, value, onChange }: { id: string; label: ReactNode; min: number; max: number; step: number; value: number; onChange: (v: number) => void })` — renders `−`, a text input `id={id}` with `inputMode="numeric"`, `+`; buttons labelled `Decrease ${plain label}` / `Increase …`; commits typed text on Enter or blur via `stepValue`, reverts on null. `ViewTabs({ views, view, onView, tips }: { views: string[]; view: string; onView: (v: string) => void; tips?: Record<string, string> })` — `role="tablist"`, each `role="tab"` with `aria-selected`. `MoreInfo({ label, children }: { label: ReactNode; children: ReactNode })` — renders `label` as a button with class `ibpl-more` (dashed underline); hover (pointer) or tap (touch) opens a popover with `children`; outside tap / Escape closes.

- [ ] **Step 1: Write failing tests** for `stepValue`: `("123",0,2000,10)→120`, `("125",0,2000,10)→130`, `(-5,0,2000,10)→0`, `("3005",0,3000,10)→3000`, `("abc",0,2000,10)→null`, `("",0,2000,10)→null`, `(" 40 ",0,2000,10)→40`.
- [ ] **Step 2: Run** `npx.cmd vitest run src/lib/stepValue.test.ts` — FAIL.
- [ ] **Step 3: Implement** `stepValue` (round to nearest step, then clamp) and the two components; CSS: stepper is one 32px-tall pill with 32px tap targets; tabs underline the active one in `var(--ibpl-accent)`.
- [ ] **Step 4: Run** the test — PASS; `npx.cmd tsc --noEmit -p .` — clean.
- [ ] **Step 5: Commit** `web: PossStepper, ViewTabs and MoreInfo kit components`.

### Task 3: Filters drawer in TabFrame (opt-in)

**Files:**
- Create: `web/src/components/shiny/FilterDrawer.tsx`
- Modify: `web/src/components/shiny/TabKit.tsx` (`TabFrame`, `Frame` context, `reveal`, `ChipsRow`, `GameFiltersAccordion`)
- Modify: `web/src/app/web.css`
- Create: `web/src/data/layoutUi.json`
- Test: `web/e2e/stats-layout.spec.ts`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces:
  - `TabFrame` new props: `layout?: "sidebar" | "drawer"` (default `"sidebar"`), `drawerSections?: DrawerSection[]`, `drawerFooter?: ReactNode`, `resultLabel?: string` (phone close button text, e.g. "Show 142 players").
  - `export type DrawerSection = { id: string; title: string; body: ReactNode; defaultOpen?: boolean }`.
  - `Frame` gains `layout`, `drawerOpen: boolean`, `setDrawerOpen(v: boolean)`, `openSection(id: string)`.
  - `GameFiltersAccordion` new prop `bare?: boolean` — renders its body without the accordion wrapper (the drawer section is the wrapper).
  - `ChipsRow` new props `activeCount?: number`; in drawer layout its Filters button toggles the drawer and reads "Filters · N" when `activeCount > 0`.
- Behaviour (from the spec, frontend-v2 `FilterDrawer.tsx` / `layout.css` "FILTER DRAWER"): fixed on the right below the navbar, 340px; from 1200px the page content gets `margin-right: 340px` while open (pushed, not covered); 768-1199px it overlays with a shadow; at phone width it is full screen with a sticky bottom button showing `resultLabel` that closes it. Header "FILTERS" + square × close; Escape closes and returns focus to the Filters button. Red "Clear all filters" at the top only when `activeCount > 0`, calling the tab's reset. Sections collapsible, first open. `reveal(id)` in drawer layout: open the drawer, open the section containing `id` (sections wrap their body in `data-section={id}`), then the existing scroll/highlight/focus.

- [ ] **Step 1: Write failing browser tests** in `stats-layout.spec.ts` against `/il/onoff?season=2026` with stubbed `/api/v1/**` (copy the stub pattern from `e2e/onoff.spec.ts`) — these fail until Task 5 wires On/Off, which is fine; they are the acceptance tests for Tasks 3-5:
  - `drawer opens, pushes content at 1440, closes on Escape`: click `Filters`; `getByRole("complementary", { name: "Filters" })` visible; at 1440 the `[role="main"]` box's right edge ≤ viewport − 340; Escape → drawer hidden and the Filters button focused.
  - `phone drawer is full screen with result button`: at 390, drawer width = 390; button `/^Show \d+ players$/` closes it.
  - `Clear all filters appears only when a filter is set`: absent initially; after choosing a team it appears; clicking it removes the team chip.
  - `chip edit opens the drawer at its control`: set a team, close drawer, click the team chip's edit pencil → drawer open, the Teams input focused.
- [ ] **Step 2: Implement** `FilterDrawer` and the TabFrame/ChipsRow/accordion changes; `layoutUi.json` holds `{"drawer": {"title": "Filters", "clearAll": "Clear all filters", "close": "Close filters"}}`.
- [ ] **Step 3: Verify** the default layout is untouched: `npx.cmd playwright test e2e/team-controls.spec.ts e2e/lineups.spec.ts e2e/game-logs.spec.ts` — all pass.
- [ ] **Step 4: Commit** `web: opt-in filters drawer in TabFrame`.

### Task 4: One-line table key and "About this view"

**Files:**
- Create: `web/src/components/shiny/TableKey.tsx`
- Modify: `web/src/components/shiny/Blocks.tsx` (export `Explainer`/`Example` bodies for reuse, no behaviour change)
- Modify: `web/src/data/layoutUi.json`, `web/src/app/web.css`

**Interfaces:**
- Produces: `TableKey({ shading, shots, about, extra }: { shading: boolean; shots: boolean; about?: Block[]; extra?: ReactNode })` — one line; "Shading = league rank: red bottom … green top" (with a 5-swatch strip from `MUTED_HEAT`) when `shading`; "Shot cells: top = accuracy 2P% | 3P% (green good, red poor) · bar = shot frequency ▬ 2PT ▬ 3PT" (swatches in the Task 1 bar colours) when `shots`; `extra` (e.g. the Four Factors impact text) after; `about` renders an "About this view" `MoreInfo` (Task 2) whose popover holds the explainer + example blocks. Copy lives in `layoutUi.json` under `"key"`.

- [ ] **Step 1: Add failing browser checks** to `stats-layout.spec.ts`: Summary view shows both key parts; Four Factors shows only the shading part plus the impact text; "About this view" opens a popover containing the Summary explainer's first sentence (read it from `shinyUi.json` in the test, never retype).
- [ ] **Step 2: Implement** `TableKey`.
- [ ] **Step 3: Commit** `web: one-line TableKey with About this view`.

### Task 5: On/Off adopts the pattern

**Files:**
- Modify: `web/src/components/OnOffClient.tsx` (~lines 380-600)
- Modify: `web/src/components/shiny/OnOffTables.tsx` (pass `"muted"` to every `heat(...)` and `palette: "muted"` to `shotCell`; add class `ibpl-muted` to the table)
- Modify: `web/e2e/onoff.spec.ts`, `web/e2e/onoff-errors.spec.ts`

**Interfaces:**
- Consumes: Tasks 1-4 (`TabFrame layout="drawer"`, `DrawerSection`, `PossStepper`, `ViewTabs`, `TableKey`, `heat(…, "muted")`).

Layout of `main`, top to bottom: `ChipsRow` (Filters · N, chips, Stat filter, then in `controls`: the two `PossStepper`s replacing the two `Slider`s with the same ids `min_all_poss`/`min_on_poss` (euro: `euro_` prefix), min/max/step and the same `onChange` bodies so auto min-poss is unchanged; the Four Factors "Show on/off detail" toggle; Download CSV (moved from the sidebar, same handler)); `ViewTabs`; `TableKey` (`shading` always; `shots` only in Summary; `extra` = the view's `impact` block; `about` = the view's `explainer` + `example` blocks); `TableHint`; `OutputArea`. Removed from the page: `ViewContent` blocks, `ViewRadios` (`sidebarTop`), the "What do '3PT luck' and 'small sample' mean?" button (its `luck.body` text moves into the Net tooltip in Task 7). Drawer sections, in today's order: `dates` "Dates and teams" (DateRangeInput + Teams), `starters` "Lineup starters" (StarterRows), `game` "Game filters" (`GameFiltersAccordion bare`); `drawerFooter` = the "If rows disappear…" help text; `resultLabel` = `Show ${rows.length} players`.

- [ ] **Step 1: Update `onoff.spec.ts`/`onoff-errors.spec.ts`** to the new structure without weakening them: "Show Filters" → open the drawer via `Filters`; slider assertions (`fixedBar`, `.irs--shiny` drag) → stepper input value and a `Increase` tap moving the value by 10 and switching auto off; Download CSV found in the toolbar; Reset → "Clear all filters".
- [ ] **Step 2: Implement** the On/Off changes.
- [ ] **Step 3: Run** `npx.cmd playwright test e2e/onoff.spec.ts e2e/onoff-errors.spec.ts e2e/stats-layout.spec.ts` — all pass at both widths, all three leagues (the golden presets prove numbers and order unchanged).
- [ ] **Step 4: Add the Review Focus #2 test** to `stats-layout.spec.ts`: after load (auto value shown), tap `Increase Min ON Poss` → value +10 and the auto indicator off; then pick a team in the drawer → value returns to an auto-computed value (assert it differs from the tapped value or equals the request's `min_on` echoed by the stub).
- [ ] **Step 5: Commit** `web: On/Off uses the drawer, steppers, view tabs, key and muted palette`.

### Task 6: Usage columns right after the names

**Files:**
- Modify: `web/src/components/shiny/OnOffTables.tsx` (`summaryColumns` ~513, FF ~657, SP ~707 and `SUMMARY_GROUPS`/`FF_GROUPS`/`SP_GROUPS`)
- Modify: `web/src/components/shiny/DataTable.tsx` only if the saved-order storage key needs a version argument (add `orderVersion?: string` appended to the key)
- Test: `web/e2e/stats-layout.spec.ts`

**Interfaces:**
- Produces: `DataTable` prop `orderVersion?: string` (default `""`, key unchanged for other tabs); On/Off passes `"v2"`.

Move `...usage()` to directly after `...nameCols(ctx)` in all three column lists, and the `{ label: "Usage", span: 3, cls: SEP }` group entry to directly after the name-columns group. Phone keeps today's floating name label (the names are not pinned twice); Usage becomes the first scrolled columns. *(Deviation from the spec's "pinned" wording, recorded for the user: pinning three more columns would take ~150 of 390 px; the plan makes them the first columns on screen instead.)*

- [ ] **Step 1: Failing test:** at 1440, in every view, the `On Poss` and `Off Poss` header cells are fully inside the visible scroll box without scrolling; at 390 they are visible on load.
- [ ] **Step 2: Failing test (Review Focus #5):** seed `localStorage` with today's saved order for the On/Off table (Usage last), load → Usage still right after Player.
- [ ] **Step 3: Implement**; update golden-column expectations in `onoff.spec.ts` only where they depend on position (they key by `data-column`, so most do not).
- [ ] **Step 4: Run** the On/Off specs — pass.
- [ ] **Step 5: Commit** `web: On/Off usage columns follow the names`.

### Task 7: One "more info" marker, tap-to-open on touch

**Files:**
- Modify: `web/src/components/shiny/DataTable.tsx` (header tooltips), `OnOffTables.tsx` (`NetCell` tags, `NetTooltip`), `web/src/app/web.css`

**Interfaces:**
- Consumes: `MoreInfo` (Task 2).
- Produces: opt-in DataTable prop `moreInfo?: boolean` switches header tooltips from `title` to `MoreInfo`; on touch the sort is its own tap target (the sort arrows), and the old `.ibpl-m-th-info` button is not rendered when `moreInfo` is on.

`NetCell` tags become amber-outlined pills (`.onoff-luck-tag` restyled in `web.css` under `.ibpl-muted`), each a `MoreInfo` trigger whose popover shows that section of today's `NetTooltip` plus the matching paragraph of `luck.body` (from `shinyUi.json`). The shot-cell `title`s become `MoreInfo` on the accuracy and bar when `palette: "muted"`.

- [ ] **Step 1: Failing tests (Review Focus #4)** at 390 with touch emulation (open a fresh page for emulation, per root `CLAUDE.md`): tap the `Net` header text → definition popover; tap again → closed; tap the header's sort arrow → rows re-sort; tap a "small sample" tag → popover with the sample section and the explanation text; tap a shot cell accuracy → popover with "2PT accuracy: …".
- [ ] **Step 2: Implement.**
- [ ] **Step 3: Run** all On/Off and stats-layout specs plus `e2e/touch-tips*` if present — pass.
- [ ] **Step 4: Commit** `web: shared MoreInfo marker; On/Off tooltips reachable by tap`.

### Task 8: Sign-off pass

- [ ] **Step 1:** `npx.cmd vitest run`, `npx.cmd tsc --noEmit -p .`, `npm.cmd run lint` (no new findings).
- [ ] **Step 2:** Run every browser spec that touches TabKit or DataTable: `onoff`, `onoff-errors`, `stats-layout`, `lineups`, `team-controls`, `game-logs`, `player-stats`, `team-ratings`, `heat`, `missing-sort`, `home`, `league-navigation`. All pass (a failure that passes alone is re-run once and reported as flaky).
- [ ] **Step 3:** Screenshots of `/il/onoff`, `/euroleague/onoff` at 390 and 1440 (drawer closed and open, Summary and Four Factors) shown to the user for sign-off before any other tab is planned.
- [ ] **Step 4:** Update `web/CLAUDE.md` resume point and `MEMORY.md` with the pattern's location and the opt-in switches; commit and push.
