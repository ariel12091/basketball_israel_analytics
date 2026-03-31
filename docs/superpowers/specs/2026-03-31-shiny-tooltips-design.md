# Shiny App Tooltips Design

**Date:** 2026-03-31
**Scope:** CSS-only tooltips for DT column headers, sidebar filters, and view toggles

## Approach

- CSS-only tooltips via `data-tooltip` attribute + `::after` pseudo-element
- Central definitions in `global.R`: `COLUMN_TOOLTIPS` and `FILTER_TOOLTIPS` named lists
- Helper function `tt(label, key)` wraps text in `span(data-tooltip=...)`
- DT sketch headers get `data-tooltip` attributes directly on `th` elements
- Dark theme styling matching existing `#0d1117` / `#e6edf3` palette

## CSS Spec

~20 lines in `shared_css` (inline in `global.R`). Dark background (`#1c2333`), light text (`#e6edf3`), `0.4s` delay, positioned above element, arrow, `max-width: 260px`, `font-size: 0.75rem`, `z-index: 9999`.

## Area 1: DT Column Header Tooltips

| Header | Tooltip |
|--------|---------|
| PPP / Off PPP / Def PPP | Points per 100 possessions |
| Net / Net Rtg / Net RTG | Offensive PPP minus Defensive PPP |
| Off (Net Impact group) | Offensive PPP difference (On minus Off court) |
| Def (Net Impact group) | Defensive PPP difference (On minus Off court) |
| Diff (FF total) | Net PPP impact (On minus Off court) |
| TS% | True Shooting: pts / (2 x (FGA + FT trips)) |
| OREB% | Offensive rebound rate: OREBs / available misses |
| TOV% | Turnover rate: turnovers / possessions |
| FTR | Free throw rate: FTA / FGA |
| Off Shot / Def Shot | 2PT/3PT frequency and accuracy split |
| On Poss / Off Poss / Poss | Number of possessions |
| Min | Minutes played |
| GP | Games played |
| Poss On Floor | Total possessions while player was on court |
| eFG% | Effective FG%: (FGM + 0.5 x 3PM) / FGA |
| FG% | Field goal percentage |
| 3P% | Three-point percentage |
| FT% | Free throw percentage |
| W-L | Win-Loss record |
| GN | Game number within the team's season |

Applied in sketch headers (`htmltools::withTags`) across server_tab1.R, server_tab2.R, server_tab3.R, server_tab4_gamelogs.R.

## Area 2: Sidebar Filter Tooltips

| Filter Label | Tooltip |
|--------------|---------|
| Min possessions per side (eligibility) | Minimum OFF + DEF possessions to appear in table |
| Minimum ON possessions (for ranking) | Minimum ON-court possessions for percentile ranking |
| Own lineup starters | Filter by number of starters in the team's lineup |
| Opponent lineup starters | Filter by number of starters in the opposing lineup |
| From/To Game Number (GN) | Team's sequential game number this season |
| Last N Team Games | Only include the team's most recent N games |
| Opponent Strength (accordion) | Filter games by opponent's league ranking |
| Clutch | Close-game situations: margin, time remaining, and score status |
| Group size | Number of players in each lineup combination (2-5) |
| Players On (exact/contains) | Lineups must include all selected players |
| Players Off (exclude any) | Lineups must exclude all selected players |
| Min possessions (Off + Def) | Minimum total possessions for lineup to appear |

Applied via `tt()` helper in ui_tab1-7.

## Area 3: View Toggle Tooltips

| Element | Tooltip |
|---------|---------|
| Summary (radio) | PPP ratings and shooting splits |
| Four Factors (radio) | TS%, OREB%, TOV%, FTR breakdown |
| Traditional (radio) | Box-score counting stats |

Applied via `data-tooltip` on the radio label or a wrapping span.

## Implementation

1. Add CSS for `[data-tooltip]` to `shared_css` in `global.R`
2. Define `COLUMN_TOOLTIPS` and `FILTER_TOOLTIPS` named lists in `global.R`
3. Create `tt(label, key)` helper in `global.R`
4. Update DT sketch headers in server_tab1.R, server_tab2.R, server_tab3.R, server_tab4_gamelogs.R
5. Update sidebar filter labels in ui_tab1-7
6. Update view-mode radio buttons in ui_tab1-4
