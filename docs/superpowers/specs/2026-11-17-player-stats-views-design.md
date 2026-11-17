# Player Stats: stat views (design, approved 2026-11-17)

## Why

The Player Stats table shows ~29 columns at once and reads as crowded. The
user's main uses are scanning league leaders and comparing a team's rotation.
Looking up one player is a priority too, but belongs on a future player
profile page, not here.

## Approaches

- **Stat views as tabs -- picked.** `Main | Shooting | All`, each a narrow,
  readable table; matches the view tabs every other tab already has.
- Merged "5-12 · 41.7%" cells -- rejected: two numbers per cell (against
  "numbers once") and an ambiguous sort key.
- Fewer columns visible by default via the Columns menu -- rejected: shooting
  detail becomes hard to discover and saved layouts hide the change.
- Placement: the user picked "stat groups take the tabs, the mode becomes a
  compact pill switch" over "mode keeps the tabs, groups become a small
  switch" (groups would be overlooked) and two tab rows (height, two nav
  levels).

## Design

- Tabs **Main | Shooting | All** replace the mode tabs.
  - Main: Team, Player, GP, Min, PTS, REB, AST, STL, BLK, TOV, FG%, 3P%,
    FT%, TS%, USG%
  - Shooting: Team, Player, GP, Min, FGM, FGA, FG%, 2PM, 2PA, 2P%, 3PM, 3PA,
    3P%, FTM, FTA, FT%, eFG%, TS%
  - All: today's table, unchanged
- The mode becomes a pill switch right of the tabs:
  `Totals | Per game | Per 60 poss | Per 30 min`; it wraps under the tabs on
  phones. Same numbers as today.
- (Claude's call) In Per 60 / Per 30 modes, Main and Shooting also show
  Poss/G or Min/G after Min, as today.
- (Claude's call) URL: `view` keeps meaning the mode, so existing links work;
  new `cols=shooting|all`, Main when absent. `cols` never reaches the API.
- (Claude's call) The Columns menu lists only the current view's columns;
  hiding and order are stored once and apply in every view that has the
  column.
- (Claude's call) DFL only in All.
- (Claude's call) Heat: counting columns get a lighter tint; rate columns
  (FG%, 2P%, 3P%, FT%, eFG%, TS%, USG%) keep full strength.
- Unchanged: CSV (visible columns, as now), stat filters (any column),
  sorting, TOTAL rows, rate eligibility, default mode (Per Game).

## Non-goals

Player profile page; new stats; any SQL or API change.

## Done when

Both leagues at 390 and 1440 px, light and dark theme; Player Stats e2e
specs updated; shipped on a `react/` branch through Preview checks.
