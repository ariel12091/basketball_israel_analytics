# Frontend v2 Progress

## Latest Updates (Session 6) — Server-Side Ranking + Parity Fixes

### Tab 2 Parity Audit Fixes
- Fixed critical two-tier ranking bug: API was sending `min_poss: minPoss` to SQL, pre-filtering data before ranking. Now server fetches with `min_poss=0` for full-population ranking.
- Fixed auto min-poss algorithm: Tab 2 uses 150-row target cap (`autoMinPossTarget`, raises AND lowers) — NOT Tab 1's top-35% algorithm.
- Added missing Summary columns: Def Poss, Off Pts, Def Pts (USAGE group span 4→7).

### Server-Side Ranking (egress optimization)
Moved all ranking + local filtering to Plumber server to reduce client data transfer:

**Plumber changes (`server/plumber.R`):**
- Added ranking helpers: `adaptive_baseline_r()`, `pr_rank()`, `auto_minposs_target_r()`
- Added two-layer caching: `RANKED_CACHE` (game-level key, excludes local filters) + `RESP_CACHE` (full query string)
- Added `apply_lineup_local_filters()` — filters by team/player containment/min_poss, computes autoMinPoss
- `/api/lineups/summary` and `/api/lineups/four-factors` endpoints:
  - Fetch full dataset with `min_poss=0`, compute percentile ranks on full population
  - Cache ranked dataset per game-level filter key (60s TTL)
  - Apply local filters (team, players on/off, min_poss) on cached data
  - Return `{rows: [...], meta: {autoMinPoss: N}}`
  - Summary: 3 PR columns (prNet, prOffPpp, prDefPppInv)
  - FF: 11 PR columns with correct inversions (def_ppp, def_ts, def_oreb, def_ftr, off_tov inverted)
- Fixed `playerIds` serialization: kept as list column (not JSON string) for proper array serialization via `@serializer unboxedJSON`

**Frontend changes:**
- Added `LineupApiResponse<T>` wrapper type (`{rows, meta}`)
- PR fields changed from optional to required-but-nullable (`prNet: number | null`)
- `LineupsPage.tsx` simplified:
  - Removed client-side ranking (~70 lines: `summaryRanked`, `ffRanked` useMemos)
  - Removed client-side filtering (~10 lines: `summaryFiltered`, `ffFiltered` useMemos)
  - Sends local filters to API: `filter_team_ids`, `players_on`, `players_off`, `min_poss`
  - Uses server-provided `meta.autoMinPoss` instead of client computation
  - `datasetKey` memo excludes minPoss to avoid auto re-enable on slider drag
  - Removed imports: `adaptiveBaseline`, `percentileRank`, `autoMinPossTarget`

**Architecture:**
```
Client                          Server (Plumber)
├─ apiParams (all filters)  →   ├─ RANKED_CACHE hit?
│                               │   ├─ miss: SQL(min_poss=0) → rank → cache
│                               │   └─ hit: use cached ranked data
│                               ├─ apply_lineup_local_filters()
│                               │   ├─ filter by team/players
│                               │   ├─ compute autoMinPoss
│                               │   └─ filter by min_poss
├─ {rows, meta} ←              └─ return {rows, meta: {autoMinPoss}}
├─ TOTAL row computation
├─ sorting
└─ render
```

**Current state:** Tab 1 + Tab 2 complete with server-side ranking. Build passes clean.

---

## Previous Updates (Session 5) — Backend & Infrastructure Catch-up

### Summary of 20+ commits between Sessions 4 and 5

**Infrastructure:**
- Plumber port changed from 8787 to **3002** with new `server/run.R` launcher
- React upgraded to **19.2**, added **TanStack Query** (`@tanstack/react-query`)
- `useApi.ts` rewritten from manual fetch to `useQuery` (staleTime, gcTime, debounce, dedup)
- `main.tsx` wraps app in `QueryClientProvider`
- API hardening: CORS allowlist, optional API key, IP rate limiting (env var driven)
- Vite proxy target updated to `localhost:3002`

**SQL Optimizations (benchmarked):**
- `onoff_compute`: 51% faster (800ms to 390ms) — scoped roster names, reduced analytic passes
- `fetch_lineups_four_factors_csv`: non-clutch 35% faster, clutch 55-60% faster — fast-path gate for explicit dates, filtered `complex_flags` CTE
- `fetch_lineups_csv_v2`: non-clutch 49% faster (550ms to 280ms) — same fast-path gate
- Fast-path rule: SQL must accept both NULL dates and explicit full-season window

**Storage & Egress:**
- `df_pts_poss_lineups_longer_mv` slimmed (123 MB), total DB <500 MB
- `min_poss=20` default to SQL reduces lineup payload 77-82% gzip
- Plumber in-memory response cache (60s TTL) + TanStack client-side cache

**Plumber Fixes:**
- Lineup modal: switched to `unboxedJSON` serializer (fixed scalar-as-array bug)
- Game-log assembly vectorized (merge + vectorized columns, no lapply)
- Player IDs parsing vectorized via `parse_pg_int_array_json()` (2.5x faster)
- Filter ownership clarified: drawer teams filter table rows, local team for player dropdown only
- Tab 1 controls (Teams, Min ON/All Poss) moved from global drawer into OnOffPage inline

**Shiny-only additions:**
- Tab 5 (Player Stats / Traditional) with `player_traditional_stats_mv` + live recalculation
- State Cup (game_type=35) added to all tab filters
- Tab 4 minutes column removed (integrity concerns)
- Last-N recency logic fixed; Tab 2 default lineup cap ~150 rows

**ETL enhancements:**
- Starters lineage: `extract_starters()` from boxscore, `num_starters` in `lineups_lookup` / `pws` / MVs
- Incremental sub_lineups refresh: `refresh_sub_lineups_stats_for_games(int4[])`
- Roster `game_year` derived from `schedule.game_year` (not `Sys.Date()`)
- ETL wrapper deadlock fix (Start-Process + lock file), Phase 6 minute integrity warnings

**Current state:** Tab 1 + Tab 2 complete. Tabs 3-4 placeholder. Tab 5 Shiny-only. Next: Tab 3 (Team Ratings) or Tab 4 (Game Logs) React migration.

---

## Previous Updates (Session 4)

### Tab 2 (Lineup Data) — React Migration COMPLETE

**All Shiny Tab 2 features implemented with full parity.** Build passes.

**Files created:**
- `src/pages/LineupsPage.tsx` (~1131 lines) — Main Tab 2 component
- `src/features/tables/LineupModal.tsx` — Game log modal for clicked lineups
- `src/utils/ranking.ts` — Shared utilities extracted from OnOffPage

**Files modified:**
- `src/types/index.ts` — Added LineupSummary, LineupFourFactors, LineupGameLog, Player interfaces
- `server/plumber.R` — Added 4 endpoints + rename helpers
- `src/pages/OnOffPage.tsx` — Refactored to import from shared `utils/ranking.ts`
- `src/app/layout/AppShell.tsx` — Wired LineupsPage (replaced placeholder)
- `src/styles/layout.css` — Added ~200 lines for Tab 2 controls, modals, multi-select

---

## Previous Updates (Session 3)

### PHASE 1 COMPLETE — All 9 Features Done!

---

### Mobile Optimizations - COMPLETE ✅

**Feature:** Filter sections in the drawer start collapsed on mobile/tablet screens for better UX.

**Implementation:**
Added CSS media query at 900px breakpoint:
```css
@media (max-width: 900px) {
  /* Collapse all filter sections by default on mobile */
  .filter-section.open .filter-section-body { display: none; }
  .filter-section.open .filter-section-chevron { transform: rotate(0deg); }
}
```

**Behavior:**
- Desktop (>900px): Time Filters and Eligibility start open
- Mobile/tablet (≤900px): All sections start collapsed
- Users can click headers to expand/collapse as needed
- Saves vertical space on small screens

**How to Test:**
1. Open `frontend-v2/index.html` on mobile or resize browser to <900px width
2. Click filter toggle to open drawer
3. All sections (Time, Game, Eligibility, Opponent Strength) start collapsed
4. Click any header → section expands
5. Click again → section collapses

---

### Filter Drawer Interaction - COMPLETE ✅

**Feature:** Team and opponent selectors in the filter drawer now filter tables across Tab 1, Tab 2, and Tab 3.

**Implementation:**
1. Updated state:
   - `currentFilters.drawerTeam` — team filter for Tab 1/2/3
   - `currentFilters.drawerOpponents` — opponent filter for Tab 1/2/3

2. Populated dropdowns:
   - `populateDrawerFilters()` fills Teams and Opponents dropdowns with all 12 teams
   - Called on page load initialization

3. Added event listeners:
   - Teams dropdown → updates `drawerTeam`, re-renders all Tab 1/2/3 tables
   - Opponents dropdown → updates `drawerOpponents`, re-renders all Tab 1/2/3 tables

4. Updated all render functions:
   - `renderSummaryTable()` / `renderFFTable()` — filter by team
   - `renderLineupsTable()` / `renderLineupsFFTable()` — filter by team
   - `renderTeamsTable()` / `renderTeamsFFTable()` — filter by team

5. Chip sync integration:
   - "Team: X" chip appears when drawer team selected
   - "vs X" chip appears when opponent selected
   - Click × to clear filter
   - Badge count includes drawer filters

**How to Test:**
1. Open `frontend-v2/index.html` in browser
2. Click filter toggle to open drawer
3. Select a team (e.g., "Maccabi Tel Aviv") in Teams dropdown
4. Go to Tab 1 → only Maccabi players shown
5. Go to Tab 2 → only Maccabi lineups shown
6. Go to Tab 3 → only Maccabi team shown
7. "Team: Maccabi Tel Aviv" chip appears at top
8. Click × on chip → filter clears, all data shows again

---

### Chip Sync - COMPLETE ✅

**Feature:** Active filter chips update dynamically when filters change, with badge count in topbar.

**Implementation:**
1. `updateFilterChips()` function:
   - Generates chips based on current filter state
   - Always shows "2025-26" season (non-removable)
   - Shows "Min X ON Poss" when > 0
   - Shows "Min X Side Poss" when > 0
   - Shows "Team: X" when team filter is active
   - Each removable chip has × button with onRemove handler

2. `handleChipRemove(index)` function:
   - Calls appropriate removal handler
   - Resets slider value and state
   - Re-renders affected tables
   - Updates chips again

3. `updateChipCount()` function:
   - Counts active filters
   - Updates badge number in topbar filter toggle
   - Hides badge when count = 0

4. Integration:
   - Called after `updateMinPoss()` (slider changes)
   - Called after team filter changes (Tab 4)
   - Called on page load initialization
   - `filterGameLogs()` helper for chip removal

**How to Test:**
1. Open `frontend-v2/index.html` in browser
2. Open filter drawer, drag "Min ON Possessions" slider → chip appears
3. Drag to 0 → chip disappears
4. Go to Tab 4, select a team → "Team: X" chip appears
5. Click × on any chip → filter clears, table updates
6. Topbar badge shows count (e.g., "2" when 2 filters active)

---

### Min Possessions Slider - COMPLETE ✅

**Feature:** Live filtering by minimum possessions in Tab 1 (On/Off Impact) and Tab 2 (Lineup Data).

**Implementation:**
1. Added IDs and `oninput` handlers to existing sliders in filter drawer:
   - "Min ON Possessions" (0-3000, default 300) — Tab 1
   - "Min Poss per Side" (0-2000, default 100) — Tab 2

2. Updated state: `currentFilters.minOnPoss = 300` and `currentFilters.minSidePoss = 100`

3. `updateMinPoss(type, value)` function:
   - Updates state when slider changes
   - Updates value label in real-time
   - Re-renders appropriate tables (Tab 1 or Tab 2)

4. Updated all render functions to filter data:
   - `renderSummaryTable()` / `renderFFTable()`: filter by `onPoss >= minOnPoss`
   - `renderLineupsTable()` / `renderLineupsFFTable()`: filter by `offPoss >= minSidePoss && defPoss >= minSidePoss`
   - Table-info labels update to show filtered count

**How to Test:**
1. Open `frontend-v2/index.html` in browser
2. Click filter toggle to open drawer
3. Tab 1: Drag "Min ON Possessions" slider → players filter out in real-time
4. Tab 2: Drag "Min Poss per Side" slider → lineups filter out in real-time
5. Value label updates as you drag
6. Works across both Summary and Four Factors views

---

### Group Size Pills - COMPLETE ✅

**Feature:** Tab 2 lineup data now switches between 2-man, 3-man, 4-man, and 5-man lineup combinations.

**Implementation:**
1. Created mock datasets for all group sizes:
   - `MOCK_LINEUPS_2` / `MOCK_LINEUPS_2_FF` (2-man combos)
   - `MOCK_LINEUPS_3` / `MOCK_LINEUPS_3_FF` (3-man combos)
   - `MOCK_LINEUPS_4` / `MOCK_LINEUPS_4_FF` (4-man combos)
   - Existing 5-man data unchanged

2. Added state: `currentGroupSize = 5`

3. Helper function: `getLineupsData(groupSize, isFourFactors)` — returns correct dataset

4. `setGroupSize(button, size)` function:
   - Updates active pill styling
   - Re-renders both Summary and Four Factors tables
   - Updates table-info text (e.g., "Showing 10 lineups · 3-man · sorted by Net")

5. **Bug fix:** Removed duplicate stub function that was preventing the feature from working

**How to Test:**
1. Open `frontend-v2/index.html` in browser
2. Go to Tab 2 (Lineup Data)
3. Click "2", "3", "4", or "5" pills below the mode toggle
4. Table instantly switches to show that group's lineups
5. Works in both Summary and Four Factors views

---

## Previous Updates (Session 2)

### Column Sorting - FIXED ✅
**Issue:** Sorting didn't work because column index didn't match object property order.

**Solution:** Created proper column-to-key mappings for all 8 table types:
- `onoff-summary` → maps "Net" to `net`, "Player" to `player`, etc.
- `onoff-ff` → maps "Diff" to `netDiff`, Four Factors columns
- `lineups`, `lineups-ff`, `teams`, `teams-ff`, `gamelogs`, `gamelogs-ff`

**How it works:**
1. Click any header (Net, PPP, TS%, Team, etc.)
2. Table sorts by that column's data property
3. Arrow indicator (▲ ▼) shows sort direction
4. Click again to reverse sort
5. Works across all tabs and both Summary/Four Factors modes

### Phase 1 Features Complete

✅ **Column Sorting** (fixed with proper key mapping)
- Handles all column types: numeric, text, nested objects
- Visual indicators update dynamically
- Sort state persists until new column clicked

✅ **CSV Export**
- All CSV buttons functional across all tabs
- Proper filenames: `onoff_summary.csv`, `teams_four_factors.csv`, etc.
- Exports currently displayed data (respects filters)

✅ **Game Logs Team Filter**
- Tab 4 dropdown filters by selected team
- Updates both Summary and Four Factors views
- "All Teams" shows everything

✅ **Keyboard Shortcuts**
- **Escape** closes modal or filter drawer
- Smooth navigation without mouse

### How to Test

1. Open `frontend-v2/index.html` in a browser
2. **Sorting:** Click "Net", "PPP", "Team" — watch data re-sort with arrow indicator
3. **CSV Export:** Click any "CSV" button — downloads current table
4. **Game Filter:** Tab 4 → select team → table filters
5. **Keyboard:** Open drawer/modal → press Escape → closes

### Phase 1 Progress: 9/9 COMPLETE! ✅

✅ Column sorting
✅ CSV export
✅ Game logs team filter
✅ Keyboard shortcuts
✅ Group size pills
✅ Min possessions slider
✅ Chip sync
✅ Filter drawer → table interaction
✅ Mobile optimizations

**Phase 1 is 100% complete!** The prototype is fully interactive with all planned features.

### Code Architecture

**Key Functions:**
- `sortTable(key, dataArray, renderFn, headerEl)` — sorts by data property key
- `makeHeadersSortable()` — sets up click handlers with column→key mappings
- `exportTableCSV(btn)` — detects active tab/mode and exports correct data
- Keyboard listener — handles Escape globally

**Column Mapping Pattern:**
```js
const columnMaps = {
  'onoff-summary': { 'Net': 'net', 'Player': 'player', ... },
  'teams-ff': { 'TS%': 'offTs', 'OREB%': 'offOreb', ... }
};
```

### Next Steps

**Phase 1 is COMPLETE!** 🎉

The static prototype now has:
- Full interactivity across all 4 tabs
- Live filtering by team, possessions, group size
- Dynamic filter chips with removal
- Sortable columns
- CSV export
- Keyboard shortcuts
- Mobile-optimized UI

**Ready for Phase 2: React Migration** (see PLAN.md for roadmap)

---

**Files Modified:**
- `index.html` — added sorting, CSV export, keyboard shortcuts, team filter
- `PLAN.md` — updated checklist
- `PROGRESS.md` — this file

---

## Latest Updates (2026-02-17)

### Tab 1 Stability + Filter Semantics

- Fixed low-possession blank-table behavior when Min ON Poss = 0:
  - Backend /api/onoff/summary now normalizes NA in numeric columns before response.
  - HeatCell now handles null/NaN safely and renders - fallback.
- Fixed apparent non-sorting in Tab 1 under sparse rows by using stable composite row keys.
- Wired Min All Poss to client filtering with correct semantics:
  - requires BOTH sides (on and off) to meet threshold, plus minOnPoss.

### Tab 1/2 Drawer + Local Filter Ownership

- Drawer Game Type converted to multi-select (eact-select, menu stays open).
- Drawer Teams now applies to Tab 1 and Tab 2 table filtering.
- Tab 2 local Team remains local and is used only for Players On/Off option pool.
- Added mutual exclusivity between Tab 2 local Team and drawer Teams:
  - selecting one clears the other.
- Tab 2 local team label changed from All teams to Select team to reduce confusion.
- Added section label Lineup Player Selection next to local team selector.

### Clear-All Behavior

- Clear control now appears even when only Tab 2 local state is active (no standard chips shown), by honoring hasActiveFilters visibility.

### Commits in this cycle

- 9ae8cf0 Fix Tab 1 low-possession rendering and min-poss filters
- b83a1f Refine Tab 1/2 filters, stability, and team-selection behavior
- e26b175 Clarify Tab 2 local team selector label