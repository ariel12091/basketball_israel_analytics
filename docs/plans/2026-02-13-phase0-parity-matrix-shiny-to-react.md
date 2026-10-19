# Phase 0 Parity Matrix - Shiny to React Web

Date: 2026-02-13  
Scope: Desktop-first React web migration with strict functional parity to current Shiny app, plus safe UX upgrades.

## Migration Constraints

1. Preserve existing metric definitions and SQL logic.
2. Preserve current filter semantics and defaults.
3. Preserve current mode behavior (Summary vs Four Factors).
4. Preserve current explanation pattern (hidden by default, mode-specific).
5. Improve UX only where behavior remains equivalent or clearer.

## Tech Targets

1. Frontend: React + TypeScript.
2. Data layer: TanStack Query.
3. Table layer: AG Grid (recommended for analytics parity).
4. API: thin service over current SQL functions/materialized views.

## Global App Features

| Area | Current Shiny Behavior | React Parity Requirement | React Upgrade (Optional) | Priority |
|---|---|---|---|---|
| Main navigation | 4 tabs (`On/Off Impact`, `Lineup Data`, `Team Ratings`, `Game Logs`) | Same 4 destinations, same naming | URL routes + deep linking | P0 |
| Last updated | Timestamp in top-right | Same timestamp source and format | Tooltip for data source freshness | P1 |
| Filter layout | Sidebar (desktop), collapsible on mobile | Same desktop/ mobile behavior | Resizable desktop sidebar | P1 |
| Reset | Per-tab reset to defaults | Exact default restoration | Confirm/reset profile presets | P1 |
| Explainers | Hidden by default, mode-specific content | Same default closed state and per-mode text | Remember open/closed per user | P1 |
| Empty states | Guidance text when no rows | Same guidance and recovery actions | One-click "widen filters" helper | P2 |

## Shared Filter Semantics (All Tabs)

| Feature | Current Shiny Behavior | React Parity Requirement | Priority |
|---|---|---|---|
| Season filter | Per-tab season selector | Same values and defaults | P0 |
| Date range | Per-tab range bounded by season | Same min/max and reset behavior | P0 |
| Opponents filter | Multi-select | Same options and semantics | P0 |
| Game type | Multi-select including All | Same mapping of display -> value | P0 |
| Home/Away | All/Home/Away | Same values | P0 |
| Outcome | All/Win/Loss | Same values | P0 |
| GN min/max | Numeric game number range | Same inclusive range behavior | P0 |
| Last N | Team last N games | Same behavior | P0 |
| GN vs Last N | Mutually exclusive | Preserve mutual-clearing rules | P0 |
| Opponent strength block | Top/Bottom + N + Metric | Same conditional query params | P0 |

## Tab 1: On/Off Impact

### Controls and Behavior

| Feature | Current Shiny Behavior | React Parity Requirement | React Upgrade | Priority |
|---|---|---|---|---|
| View mode | `Summary` / `Four Factors` radio | Same two modes and default | Segmented control with keyboard shortcuts | P0 |
| Teams | Multi-select teams | Same multi-select behavior | Search + pinned recent teams | P1 |
| Min possessions (all) | `min_all_poss` slider | Same min/max/step/default | Quick presets (Low/Medium/High) | P1 |
| Min ON possessions | `min_on_poss` slider | Same min/max/step/default | Add percentile preset chips | P2 |
| Download CSV | Exports current table | Same exported columns per mode | Include filter metadata in file header | P2 |

### Output and Presentation

| Feature | Current Shiny Behavior | React Parity Requirement | React Upgrade | Priority |
|---|---|---|---|---|
| Summary table | Net/Off/Def impact + on/off court stats + shot splits | Same columns/order/format rules | Column pinning and saved column sets | P0 |
| Four Factors table | TS/OREB/TOV/FTR on/off and diff/rank views | Same metrics/polarity and formatting | Factor comparison mini-bars | P1 |
| Legends | Summary shot legend; FF rank legend | Same mode-conditional display | Sticky legend on scroll | P2 |
| Explainer + example | Hidden by default; summary and FF variants | Same behavior and current copy intent | Inline glossary anchors | P1 |
| Color logic | Green/red semantics + defensive polarity | Preserve exact polarity behavior | User-toggle colorblind palette | P2 |

## Tab 2: Lineup Data

### Controls and Behavior

| Feature | Current Shiny Behavior | React Parity Requirement | React Upgrade | Priority |
|---|---|---|---|---|
| Group size | 2/3/4/5 selector | Same options and default | Shortcut chips for quick switching | P1 |
| Team selector | Required to enable player filters | Same dependency | Clear disabled states/messages | P0 |
| Players On | Multi-select include filter | Same inclusion semantics | Typeahead with role badges | P1 |
| Players Off | Multi-select exclusion filter | Same exclusion semantics | Quick clear all chips | P1 |
| On/Off overlap | Cannot select same player in both | Preserve mutual exclusion | Inline validation badge | P0 |
| Clutch filters | Enable + margin/status/minutes/OT options | Same parameter semantics | Saved clutch presets | P1 |
| Min possessions | Slider threshold | Same filtering semantics | Inline expected-row counter | P2 |

### Output and Presentation

| Feature | Current Shiny Behavior | React Parity Requirement | React Upgrade | Priority |
|---|---|---|---|---|
| Summary table | Lineup stats incl. Off/Def PPP, Net, minutes, poss, shot splits | Same metrics and order | Advanced multi-sort templates | P0 |
| FF table | Lineup four factors + workload | Same metric definitions | Compare-selected lineups panel | P2 |
| Clickable lineup row | Opens modal game log for lineup | Same interaction and data path | Side panel drilldown (faster context) | P1 |
| Modal no-data state | "No game data" modal message | Same guard behavior | Non-blocking inline state | P2 |
| Explainer + snippet | Hidden by default, mode-specific | Same behavior | Add "how to interpret sample size" chip | P1 |

## Tab 3: Team Ratings

### Controls and Behavior

| Feature | Current Shiny Behavior | React Parity Requirement | React Upgrade | Priority |
|---|---|---|---|---|
| View mode | Summary/Four Factors | Same options and default | Mode hotkeys | P2 |
| Opponents | Multi-select team names | Same behavior | Fast-select top rivals | P2 |
| Clutch filters | Same clutch model as Tab 2 | Same parameter mapping | Shared clutch component with presets | P1 |
| Startup gating | Heavy compute only when tab active | Preserve lazy-load behavior | Preload metadata only, not full data | P0 |

### Output and Presentation

| Feature | Current Shiny Behavior | React Parity Requirement | React Upgrade | Priority |
|---|---|---|---|---|
| Summary table | Team Off PPP, Def PPP, Net, Games, W-L | Same metrics and sort logic | Add rank badges and sparkline trend | P1 |
| FF table | Team four-factor profiles | Same columns and polarity | Compare-2-team quick mode | P2 |
| Explainer + snippet | Hidden by default, mode-specific | Same behavior | Context links to glossary | P2 |

## Tab 4: Game Logs

### Controls and Behavior

| Feature | Current Shiny Behavior | React Parity Requirement | React Upgrade | Priority |
|---|---|---|---|---|
| Team filter | Optional; default all teams | Preserve optional behavior | Team quick-filter chips | P1 |
| View mode | Summary/Four Factors | Same modes/default | Persistent last-used mode | P2 |
| Game filters | Same base filters + GN/Last N | Same semantics | Reusable filter presets | P2 |

### Output and Presentation

| Feature | Current Shiny Behavior | React Parity Requirement | React Upgrade | Priority |
|---|---|---|---|---|
| Summary table | GN, team/opp, score, Off/Def PPP, Net, Min | Same columns/default sort | Sticky GN + score columns | P1 |
| FF table | Single-game factor-level metrics | Same metric semantics | Expand row for possession context | P2 |
| Summary legend | Shot split legend only in summary | Same mode visibility | Compact legend toggle | P2 |
| Explainer + snippet | Hidden by default, mode-specific | Same behavior | Optional "walkthrough" panel | P2 |

## Data/API Contract Matrix

| Domain | Existing Source | API Endpoint Target | Notes | Priority |
|---|---|---|---|---|
| On/Off Summary | `onoff_default_mv` / `onoff_compute()` | `GET /api/onoff/summary` | Keep fast-path vs filtered-path behavior | P0 |
| On/Off FF | `player_advanced_stats_mv` / `four_factors_compute()` | `GET /api/onoff/four-factors` | Preserve rank baseline logic | P0 |
| Lineups Summary | `fetch_lineups_csv_v2()` | `GET /api/lineups/summary` | Preserve clutch + GN params | P0 |
| Lineups FF | `fetch_lineups_four_factors_csv()` | `GET /api/lineups/four-factors` | Preserve same filters | P0 |
| Lineup modal games | `mv_lineup_totals_by_day` / `lineup_four_factors_by_game` + schedule join | `GET /api/lineups/{hash}/games` | Must support both modes | P0 |
| Team Summary | `team_ppp_ratings_mv` / `get_team_ratings_dynamic()` | `GET /api/teams/summary` | Preserve wins/losses logic under clutch | P0 |
| Team FF | `team_four_factors_mv` / `get_team_four_factors_dynamic()` | `GET /api/teams/four-factors` | Keep possession context | P0 |
| Game Logs Summary | `mv_lineup_totals_by_day` + `final_schedule_mv` | `GET /api/gamelogs/summary` | Preserve all-teams default | P0 |
| Game Logs FF | `lineup_four_factors_by_game` + schedule join | `GET /api/gamelogs/four-factors` | Preserve GN ordering | P0 |

## Table Interaction Parity Matrix (Desktop)

| Interaction | Current State | React Required | Priority |
|---|---|---|---|
| Sort columns | DT sort | Multi-sort | P0 |
| Scroll large tables | DT scrolling | Virtualized rows | P0 |
| Header persistence | FixedHeader | Sticky header | P0 |
| Export | CSV buttons | CSV export (visible or full columns configurable) | P1 |
| Conditional formatting | JS/CSS renderers | Equivalent cell renderer rules | P0 |
| Hidden technical columns | Used for click payloads | Hidden data fields in row model | P0 |
| Clickable links in rows | JS `Shiny.setInputValue` | React row/cell action handlers | P0 |

## Performance Parity Requirements

1. No heavy Team Ratings query on first app load unless tab active.
2. No unnecessary query bursts on filter initialization.
3. Debounce high-churn inputs (date ranges/select search) before fetch.
4. Preserve fast-path behavior where current app uses MVs.

## Accessibility and UX Requirements

1. Keyboard operable tab and mode toggles.
2. Keyboard navigable data grid and actionable cells.
3. Sufficient contrast in heatmap colors; offer colorblind-safe palette.
4. Explaners must remain hidden by default and discoverable.

## Acceptance Criteria (Parity Gate)

1. Every current control has a mapped React equivalent.
2. Every current table has same metric outputs for agreed test fixtures.
3. Every cross-control rule (GN vs Last N, players on/off mutual exclusion, mode-specific UI) matches current behavior.
4. Lineup row drilldown works for Summary and Four Factors.
5. Startup and tab-switch performance meets or beats current app baseline.

## Execution Backlog (Phase 0 to Phase 1 Handoff)

1. Finalize this matrix with owner signoff.
2. Convert each `P0` row into implementation tickets.
3. Produce API schema documents and sample payloads for each endpoint.
4. Create golden parity test fixtures (5-10 scenarios per tab).
5. Lock grid and chart library decisions.

## Out of Scope for Initial Cut

1. Native mobile app packaging.
2. Metric-definition changes.
3. Large data model redesign.
4. New advanced analytics not present in current Shiny app.
