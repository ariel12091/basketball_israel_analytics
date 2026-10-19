# Shiny Tooltips — Progress

**Date:** 2026-03-31
**Status:** Implementation complete, needs smoke test

## What was done

### Task 1: Tooltip infrastructure in global.R (`be15699`)
- Added `COLUMN_TOOLTIPS` — 40+ named entries mapping DT column headers to descriptions (PPP, TS%, OREB%, TOV%, FTR, traditional stats, etc.)
- Added `FILTER_TOOLTIPS` — 15 entries for sidebar filter labels
- Added `tt(label, key)` helper — wraps label in `tags$span(data-tooltip=...)` for CSS tooltips
- Added `HEADER_TOOLTIP_JS` — DT `headerCallback` that sets native `title` on `th` elements
- Added CSS for `[data-tooltip]` pseudo-element tooltips (dark theme: `#1c2333` bg, `#e6edf3` text)

### Task 2: DT headerCallback on all tables (`e24cb1f`)
- Added `headerCallback = HEADER_TOOLTIP_JS` to 13 `datatable()` calls across:
  - `server_tab1.R` (2), `server_tab2.R` (4), `server_tab3.R` (5), `server_tab4.R` (2), `server_tab5_traditional.R` (1), `server_tab6_team_stats.R` (1)
- Skipped `server_tab7_compare.R` (already has custom headerCallback)

### Task 3: Sidebar filter tooltips (`71981cc`)
- Replaced 44 plain-string labels with `tt()` calls across 7 UI files (ui_tab1-7)
- Tooltipped labels: Min possessions, Own/Opp lineup starters, From/To GN, Last N, Opponent Strength, Clutch, Group size, Players On/Off

### Task 4: View-mode radio tooltips (`1dbe8ca`)
- Added jQuery snippet in `shared_head_tags()` that sets `data-tooltip` on radio labels inside `.view-mode-container`
- Covers: "Summary" → "PPP ratings and shooting splits", "Four Factors" → "TS%, OREB%, TOV%, FTR breakdown", "Traditional" → "Box-score counting stats"

### Bug fixes
- **accordion_panel value error** (`bd540ee`): `bslib::accordion_panel()` requires `value` to be a string; `tt()` returns `tags$span`. Fixed by adding explicit `value = "Opponent Strength"` to all 5 affected accordion panels.
- **Tooltip rendering** (`5b79108`): Added `display: inline-block` to `[data-tooltip]` spans (required for CSS pseudo-element positioning). Switched DT headers from CSS `::after` to native `title` attribute (CSS tooltips get clipped by DT's `overflow: hidden` scroll container).

## Still needed

- [ ] **Smoke test**: Run the app and verify tooltips appear on:
  - DT column headers (all tabs, Summary + FF views) — native browser tooltips
  - Sidebar filter labels (all tabs) — styled dark tooltips
  - View-mode radio buttons (Tabs 1-4) — styled dark tooltips
- [ ] **Verify Tab 7 (Compare)**: has its own headerCallback — confirm it still works and that column tooltips don't conflict
- [ ] **Optional**: Consider adding tooltips to compare tab metric chips if useful

## Design decisions
- **CSS tooltips for sidebar/radios**: dark themed, matches app aesthetic
- **Native `title` for DT headers**: pragmatic — CSS `::after` gets clipped by DT scroll container
- **Central definitions**: one `COLUMN_TOOLTIPS` list + one `FILTER_TOOLTIPS` list in `global.R`, reused everywhere
