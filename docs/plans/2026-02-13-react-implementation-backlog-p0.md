# React Web Implementation Backlog - P0

Date: 2026-02-13  
Source: `docs/plans/2026-02-13-phase0-parity-matrix-shiny-to-react.md`

## Usage

1. Each ticket is implementation-ready.
2. `Must preserve` defines strict parity with existing Shiny behavior.
3. `Acceptance` is the pass/fail gate.

## EPIC A: App Shell and Navigation

### P0-A1: Create desktop-first app shell with 4-tab routing

Scope:
1. Build app shell with routes for `onoff`, `lineups`, `teams`, `gamelogs`.
2. Keep labels aligned with current UI naming.

Must preserve:
1. Same top-level tab model and discoverability.

Acceptance:
1. All 4 destinations are reachable from top nav.
2. Direct route URL loads correct tab view.

### P0-A2: Implement responsive filter panel behavior

Scope:
1. Desktop: pinned sidebar.
2. Mobile: collapsible filter drawer with explicit show/hide action.

Must preserve:
1. Current "Show Filters" affordance on small screens.

Acceptance:
1. Desktop keeps filters visible by default.
2. Mobile hides filters by default and can open/close without losing state.

## EPIC B: Shared Filter Semantics

### P0-B1: Shared filter model and defaults

Scope:
1. Create typed filter schema used by all tabs.
2. Include season, date range, game type, opponents, home/away, outcome.

Must preserve:
1. Existing default values and value mappings.

Acceptance:
1. Reset restores exact defaults per tab.
2. API payload matches expected parameter names/types.

### P0-B2: GN range and Last N mutual exclusion rule

Scope:
1. Implement shared rule engine for `gn_min`, `gn_max`, `last_n`.

Must preserve:
1. Setting `last_n` clears GN range.
2. Setting GN range clears `last_n`.

Acceptance:
1. UI state updates immediately and deterministically for both directions.
2. API never receives both active GN range and active Last N simultaneously.

### P0-B3: Opponent strength filter parameterization

Scope:
1. Implement `opp_rank_side`, `opp_rank_n`, `opp_rank_metric` in shared model.

Must preserve:
1. Same values and conditional usage as current app.

Acceptance:
1. Parameters flow end-to-end to API for all tabs that support this block.

## EPIC C: Tab 1 (On/Off Impact) Parity

### P0-C1: Implement Tab 1 mode switch and query integration

Scope:
1. Build `Summary` / `Four Factors` mode control.
2. Wire mode into query keys and API endpoints.

Must preserve:
1. Default mode is `Summary`.
2. Mode changes column set and legend behavior.

Acceptance:
1. Summary and FF render distinct correct datasets.

### P0-C2: Implement Tab 1 filters with parity defaults

Scope:
1. Team multi-select, date range, game filters, GN/Last N, opponent strength.
2. Sliders: `min_all_poss`, `min_on_poss`.

Must preserve:
1. Existing slider bounds/step/default semantics.

Acceptance:
1. Filter changes update table results correctly.
2. Reset restores exact Tab 1 defaults.

### P0-C3: Reproduce Tab 1 output table parity

Scope:
1. Summary columns parity (impact + on/off court stats + shot splits).
2. FF columns parity (factor metrics and rank context).
3. Conditional formatting parity rules.

Must preserve:
1. Current polarity logic for offense/defense and TOV interpretation.

Acceptance:
1. Numeric parity against Shiny on agreed fixtures.
2. Color/format parity for core columns.

### P0-C4: Preserve mode-conditional legends and hidden explainer pattern

Scope:
1. Show summary shot legend only in Summary.
2. Show FF legend only in FF.
3. Keep explainer hidden by default and mode-specific.

Acceptance:
1. Legend visibility exactly follows mode.
2. Explainer remains collapsed on first load.

## EPIC D: Tab 2 (Lineup Data) Parity

### P0-D1: Team-dependent player filter model

Scope:
1. Keep players selectors disabled until team selected.
2. Load players for selected team.

Must preserve:
1. Current dependency behavior.

Acceptance:
1. No players selectable before team selection.
2. Team change refreshes player options.

### P0-D2: Players On/Off mutual exclusion

Scope:
1. Prevent same player in both `players_on` and `players_off`.

Must preserve:
1. Current conflict resolution behavior.

Acceptance:
1. Selecting overlap auto-resolves overlap immediately.

### P0-D3: Implement clutch filters with full parameter parity

Scope:
1. Enable clutch toggle and conditional controls (margin, status, minutes, OT margin option).

Must preserve:
1. Same enable/disable semantics and parameter values.

Acceptance:
1. Clutch off sends no clutch constraints.
2. Clutch on sends full clutch payload.

### P0-D4: Reproduce Tab 2 Summary/FF tables

Scope:
1. Summary columns parity.
2. FF columns parity.
3. Min possessions threshold behavior parity.

Acceptance:
1. Numeric parity on golden fixtures.

### P0-D5: Rebuild clickable lineup row -> game detail flow

Scope:
1. Keep row action on lineup identity cell.
2. Load game-level details for selected lineup in current mode.
3. Handle no-data state.

Must preserve:
1. Works in both Summary and FF modes.

Acceptance:
1. Clicking lineup opens detail view with expected games.
2. No-data path shows explicit message.

## EPIC E: Tab 3 (Team Ratings) Parity

### P0-E1: Preserve startup lazy-loading behavior

Scope:
1. Do not fetch heavy Team Ratings dataset until tab is active.

Must preserve:
1. No initial-load Team query when user opens app on another tab.

Acceptance:
1. Network trace shows no Team data request before Team tab activation.

### P0-E2: Implement Team tab filters with parity semantics

Scope:
1. Season/date/game filters + GN/Last N + opponent strength + clutch.

Acceptance:
1. Same filter behavior and reset behavior as current app.

### P0-E3: Reproduce Team Summary/FF output tables

Scope:
1. Summary columns include Off/Def/Net and Games/W-L.
2. FF columns parity and formatting.

Acceptance:
1. Numeric parity against Shiny fixtures.

## EPIC F: Tab 4 (Game Logs) Parity

### P0-F1: Implement optional team filter with all-teams default

Scope:
1. Team selector optional, default to all teams.

Must preserve:
1. Results populate without selecting a specific team.

Acceptance:
1. Default load returns all-team game logs.

### P0-F2: Implement Tab 4 filters and mode behavior

Scope:
1. Full filter set and mode switch with parity semantics.

Acceptance:
1. Summary and FF mode datasets render correctly with same filter behavior.

### P0-F3: Reproduce Game Logs output parity

Scope:
1. Summary table parity (GN + score + PPP/Net/min).
2. FF table parity.
3. Preserve GN-based default ordering.

Acceptance:
1. Numeric and ordering parity against Shiny fixtures.

## EPIC G: API Layer (P0)

### P0-G1: Build thin API over existing SQL functions/MVs

Scope:
1. Endpoints:
   - `/api/onoff/summary`
   - `/api/onoff/four-factors`
   - `/api/lineups/summary`
   - `/api/lineups/four-factors`
   - `/api/lineups/{hash}/games`
   - `/api/teams/summary`
   - `/api/teams/four-factors`
   - `/api/gamelogs/summary`
   - `/api/gamelogs/four-factors`

Must preserve:
1. Existing SQL metric logic and result definitions.

Acceptance:
1. Endpoint responses match schema contracts.
2. Endpoint outputs match Shiny for fixture scenarios.

### P0-G2: Input validation and strict parameter contracts

Scope:
1. Validate enums, numeric ranges, dates, and optional fields.
2. Return consistent error format.

Acceptance:
1. Invalid payloads fail with clear 4xx responses.
2. Valid payloads pass and map correctly to SQL params.

## EPIC H: Grid and Interaction Parity

### P0-H1: Implement core desktop grid interactions

Scope:
1. Multi-sort.
2. Virtualized rows for large datasets.
3. Sticky headers.
4. Hidden technical fields for row action payloads.

Must preserve:
1. Existing clickable-row workflows and render semantics.

Acceptance:
1. All tab tables support required interactions without regressions.

### P0-H2: Conditional cell rendering parity

Scope:
1. Port heatmap/metric coloring and number formatting rules.

Acceptance:
1. Side-by-side review with Shiny shows equivalent interpretation cues.

## EPIC I: Performance and Quality Gates

### P0-I1: Query-trigger discipline and debounce

Scope:
1. Debounce high-churn filters.
2. Avoid duplicate fetches during initialization.

Acceptance:
1. No bursty duplicate requests from a single logical user action.

### P0-I2: Golden parity test suite

Scope:
1. Create 5-10 fixture scenarios per tab.
2. Compare React/API output to Shiny output.

Acceptance:
1. P0 release blocked unless all fixtures pass.

## Definition of Done (P0 Release)

1. All P0 tickets completed and accepted.
2. Parity fixtures pass for all 4 tabs.
3. Startup behavior preserves Team tab lazy-load rule.
4. Critical workflows verified:
   - Tab 1 mode/filter parity
   - Tab 2 lineup drilldown
   - Tab 3 filtered team comparison
   - Tab 4 all-teams default logs
5. No known blocker-level regression versus current Shiny functionality.
