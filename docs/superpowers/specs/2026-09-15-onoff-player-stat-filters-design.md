# On/Off Impact — Player-Stat Filter Chips

**Date:** 2026-09-15  
**Status:** Proposed specification  
**Initial scope:** Israeli-league Shiny On/Off Impact tab  
**Follow-up scope:** EuroLeague parity after the Israeli implementation is validated

## Summary

Extend the existing `+ Filter` action chip in the On/Off Impact tab so users can narrow the table by traditional player statistics as well as on/off metrics. A user should be able to construct a query such as:

`Net >= 5` · `On Poss >= 200` · `PTS/G >= 10` · `TS% >= 55`

All active chips compose with AND. Player-stat values are calculated over the same selected games as the On/Off result wherever the current traditional-stat reader supports the filter. Player-stat columns are used only for filtering in this version; they are not added to the visible On/Off table.

The existing action chip, popover, filter state, and removable-chip behavior remain the interaction model. This is an extension of the current On/Off filtering system, not a new sidebar section or a cross-tab transfer workflow.

## Problem

The On/Off table can currently be filtered by impact, on-court and off-court rates, shooting results, minutes, and possessions. It cannot answer common cohort questions such as:

- Which rotation players averaging at least 15 minutes per game have positive on/off impact?
- Which high-usage scorers have negative defensive impact?
- Which efficient shooters have enough on-court possessions to make their on/off result worth examining?

Users can answer the player-production part in Player Stats and the impact part in On/Off, but cannot combine the criteria in one result set.

## Goals

1. Let users filter On/Off rows by a focused set of traditional player statistics.
2. Reuse the current `+ Filter` chip and removable-chip interaction.
3. Make every rate denominator explicit in the label.
4. Match the On/Off tab's selected game population as closely as the current Player Stats query surface allows.
5. Avoid any additional Player Stats query when no Player Stats chip is active.
6. Preserve current percentile ranks and heat colors; narrowing rows must not recompute league-relative ranks.
7. Keep the first implementation small enough to test and validate before EuroLeague parity work.

## Non-goals

- Displaying traditional Player Stats as visible columns in the On/Off table.
- Moving filters from the Player Stats tab or synchronizing live state between tabs.
- OR logic between chips.
- Arbitrary formulas or user-defined denominators.
- Segment-level traditional statistics under own/opponent starter-count restrictions.
- Adding or changing materialized views in the first implementation unless performance validation demonstrates a need.
- Changing the On/Off table's column order; sample-size prominence is a separate UI change.

## User Experience

### Entry point

Continue to use the existing `+ Filter` chip rendered above the On/Off table. No new button or sidebar control is added.

### Filter menu organization

The column selector should distinguish three kinds of values. If native optgroups can be introduced without complicating the shared helper, use them. Otherwise prefix each label as shown below.

**Impact and on/off results**

- `Impact — Net`
- `Impact — Off`
- `Impact — Def`
- Existing on-court, off-court, Four Factors, and Shot Profile choices appropriate to the selected view

**Sample size**

- `Sample — Min`
- `Sample — On Poss`
- `Sample — Off Poss`

**Player Stats**

- `Player Stats — GP`
- `Player Stats — MIN/G`
- `Player Stats — PTS/G`
- `Player Stats — REB/G`
- `Player Stats — AST/G`
- `Player Stats — STL/G`
- `Player Stats — BLK/G`
- `Player Stats — 3P%`
- `Player Stats — eFG%`
- `Player Stats — TS%`
- `Player Stats — USG%`

The initial list is intentionally smaller than the full Player Stats table. Additional fields can be added after observing actual use.

### Operators and values

- Supported operators remain `>=` and `<=`.
- Multiple filters compose with AND.
- Multiple filters on the same field form a range.
- Percentage inputs use the displayed 0–100 scale; `55` means 55%.
- Per-game inputs use normal displayed values; `15` in `PTS/G` means 15 points per game.
- Rows with `NA` for an active field are excluded.

### Chip labels

After submission, use compact removable labels:

- `PTS/G >= 10 ×`
- `TS% >= 55 ×`
- `Net >= 5 ×`

The `Player Stats —` menu prefix is not repeated on the active chip because the stat label is already unambiguous.

### Explanatory copy

Add a short line to the popover when a Player Stats field is selected, or one persistent compact note if conditional copy is awkward:

> Player Stats use the selected games. Percentages use 0–100. Starter-count filters affect On/Off possessions, not Player Stats.

The last sentence is required whenever either starter-count filter is active. It may be hidden when neither restriction is active.

### View switching

Player Stats filters are common to Summary, Four Factors, and Shot Profile and should remain active when the user switches views. Filters belonging only to the previous On/Off view should be removed if the new view cannot evaluate them.

This replaces the current behavior that clears every stat chip on any view change. Removal of incompatible chips must be deterministic and must not remove Player Stats or sample-size filters.

### Reset

`Reset to defaults` clears Player Stats chips along with every existing On/Off filter.

## Metric Definitions

Player Stats must use the same definitions as the Player Stats tab. Do not create a second set of basketball formulas in the On/Off server.

| Filter | Definition |
|---|---|
| GP | Distinct games represented in the selected Player Stats population |
| MIN/G | Total player minutes / GP |
| PTS/G | Total points / GP |
| REB/G | Total rebounds / GP |
| AST/G | Total assists / GP |
| STL/G | Total steals / GP |
| BLK/G | Total blocks / GP |
| 3P% | Made threes / attempted threes × 100 |
| eFG% | Existing canonical Player Stats eFG% |
| TS% | Existing canonical Player Stats true-shooting percentage |
| USG% | Existing canonical Player Stats usage percentage |

Division by zero produces `NA`, not zero. Filtering on that metric then excludes the row.

## Filter Context Semantics

### Filters that must match On/Off

When a Player Stats chip is active, calculate the Player Stats frame using the active On/Off values for:

- season
- date range
- selected teams
- game type
- opponent teams
- home/away
- outcome
- opponent-rank side, count, and metric
- game-number range
- last N games

These inputs map to the existing `basketball_test.get_player_traditional_from_games(...)` reader and its default-season materialized-view path.

### Starter-count restrictions

The On/Off tab's own-lineup and opponent-lineup starter-count controls restrict lineup segments. The existing traditional Player Stats reader aggregates selected games and does not accept these segment restrictions.

Version 1 semantics are therefore:

- On/Off metrics respect starter-count restrictions.
- Player Stats filters use the matching selected games but not starter-count restrictions.
- The UI discloses this difference.

Silently pretending that Player Stats use the starter-restricted possessions is not acceptable. Supporting that behavior later requires a separate possession/segment-level player-stat design.

## Data Grain and Identity

Join Player Stats onto On/Off data using:

`game_year + team_id + player_id`

Never join by display name.

The filter is evaluated at the existing On/Off row grain: one player-team-season row. A player who represented two teams can qualify for one team and fail for another. Do not use a multi-team `TOTAL` row for filtering On/Off team rows.

Rows without a matching Player Stats record receive `NA` for every `ps_*` field and are excluded whenever a Player Stats filter is active.

## Proposed Internal Representation

### Filter columns

Use namespaced hidden columns to avoid collisions with On/Off values:

```r
PLAYER_STAT_FILTERABLE_COLS <- c(
  "Player Stats — GP"    = "ps_gp",
  "Player Stats — MIN/G" = "ps_min_pg",
  "Player Stats — PTS/G" = "ps_pts_pg",
  "Player Stats — REB/G" = "ps_reb_pg",
  "Player Stats — AST/G" = "ps_ast_pg",
  "Player Stats — STL/G" = "ps_stl_pg",
  "Player Stats — BLK/G" = "ps_blk_pg",
  "Player Stats — 3P%"   = "ps_3p_pct",
  "Player Stats — eFG%"  = "ps_efg_pct",
  "Player Stats — TS%"   = "ps_ts_pct",
  "Player Stats — USG%"  = "ps_usg_pct"
)
```

The actual constant may use plain ASCII labels internally if Unicode punctuation creates test or encoding problems; displayed labels should remain readable.

### Filter classification

Add small pure helpers:

```r
is_player_stat_filter <- function(filter) {
  startsWith(filter$col %||% "", "ps_")
}

has_player_stat_filters <- function(filters) {
  any(vapply(filters, is_player_stat_filter, logical(1)))
}
```

Classification must use the internal column namespace, not label text.

### Shared Player Stats projection

Extract or introduce a shared helper that takes the canonical normalized Player Stats result and returns only:

```text
game_year, team_id, player_id,
ps_gp, ps_min_pg, ps_pts_pg, ps_reb_pg, ps_ast_pg,
ps_stl_pg, ps_blk_pg, ps_3p_pct, ps_efg_pct, ps_ts_pct, ps_usg_pct
```

Reuse the existing Player Stats normalization and formula helpers. The On/Off server must not depend on the render-only data frame or input state inside `server_tab5_traditional()`.

## Query and Reactive Design

### Lazy activation

Create a Player Stats filter-frame reactive in `server_tab1.R` with this contract:

1. If no active filter has an internal column beginning with `ps_`, return `NULL` immediately.
2. Otherwise resolve the current On/Off game-filter context.
3. Use the default-season Player Stats materialized view when the context qualifies for the existing fast path.
4. Use `get_player_traditional_from_games(...)` for supported non-default contexts.
5. Normalize once and project to the hidden `ps_*` columns.
6. Cache by data version plus the complete filter-context key.

Normal On/Off browsing must not issue this query.

### Avoiding duplicated reader logic

The Player Stats reader selection and normalization currently live primarily in `server_tab5_traditional.R`. Extract the minimum reusable data-access and projection logic into a neutral helper location rather than copying the SQL call into Tab 1.

The shared code should accept explicit arguments and return data; it must not read `input` directly. Tab 5 and Tab 1 remain responsible for translating their own inputs into those arguments.

### Join timing

Join the projected Player Stats frame to the fully calculated On/Off population before local stat-chip filtering and after league-relative On/Off percentile ranks are calculated.

This ordering guarantees:

- Player Stats filtering does not change On/Off percentile ranks.
- Every On/Off view receives the same qualifying player population.
- Hidden `ps_*` values are available to `apply_stat_filters()`.

The renderer's visible-column selection currently drops unknown columns. Either:

1. apply Player Stats filters upstream and pass only qualifying rows to the renderer, or
2. explicitly retain and hide every `ps_*` column in each renderer.

Option 1 is preferred. It keeps Player Stats filtering out of the table-column/container positional contract and avoids modifying the grouped-header column counts.

### Recommended filter pipeline

```text
raw On/Off population
  -> calculate league-relative ranks
  -> apply team and possession minimums
  -> if ps_* chips exist:
       fetch/project Player Stats
       join on season + team + player
       apply ps_* filters
  -> renderer applies the remaining view-specific On/Off filters
  -> visible table
```

Split the active filter list into `player_stat_filters` and `onoff_filters`. Do not apply the same filter twice.

## Empty and Error States

- If valid Player Stats filtering produces zero rows, show the normal empty table state with copy such as `No players match the active filters.`
- If the Player Stats query fails, do not silently show unfiltered On/Off results. Show a compact table-level error stating that Player Stats filters could not be evaluated, and keep the active chips visible for retry/removal.
- If a filter field is unavailable for the active league or query path, reject it before adding the chip or remove it during a league/view transition with a visible notification.

## Performance Requirements

1. Zero Player Stats queries when no `ps_*` chip is active.
2. At most one Player Stats query per distinct supported On/Off filter context per reactive flush.
3. Adding or removing multiple local threshold chips must reuse the already-fetched Player Stats frame when the game context has not changed.
4. Default-season results should reuse the existing shared season cache and data-version invalidation behavior.
5. Player Stats filtering remains local after the aggregate frame has been fetched; thresholds are not added as SQL parameters in version 1.
6. Existing On/Off auto-minimum calculations must remain based on the full On/Off population, not the Player Stats-filtered subset.

## State and Bookmarking

Player Stats chips should use the same state mechanism as existing On/Off stat chips. If stat-chip state is not currently bookmarkable, bookmarking it is not required for version 1. The change must not break restoration of season, dates, teams, or game-context filters.

League changes must clear unsupported Player Stats chips so Israeli player IDs or metric availability never leak into the EuroLeague context.

## Files Expected to Change

- `app/R/helpers.R`
  - Player Stats filter-column definition or shared filter definitions
  - filter classification/splitting helpers
  - shared Player Stats filter-frame projection
  - optional compatibility helper for preserving common chips across view changes
- `app/R/server_tab1.R`
  - extend filter choices
  - lazy Player Stats frame reactive
  - identity join and upstream `ps_*` filtering
  - revised view-switch behavior
  - query-error handling
- `app/R/server_tab5_traditional.R`
  - extract/reuse canonical reader or projection helpers as needed; behavior should remain unchanged
- `app/R/ui_tab1_onoff.R`
  - explanatory copy only, if the note cannot live entirely in the filter popover
- `app/tests/testthat/`
  - pure-helper, server-reactive, rendering, and regression tests

No SQL change is expected for the initial Israeli implementation because the current traditional-stat function already supports the required game-level filter inputs.

## Test Plan

### Pure helper tests

- Correct classification of `ps_*` and non-`ps_*` filters.
- Correct splitting of mixed filter lists without losing IDs or order.
- Correct per-game derivation for every counting statistic.
- `GP = 0` produces `NA` per-game values.
- Percentage values remain on the 0–100 display scale.
- Identity join is team- and player-specific.
- Missing Player Stats rows fail closed under an active Player Stats filter.

### Server tests

- No Player Stats query without a Player Stats chip.
- First Player Stats chip triggers exactly one aggregate read.
- Changing only a threshold reuses the aggregate frame.
- Changing dates, teams, opponent, game type, GN, or last N invalidates the frame.
- A single `PTS/G >= N` chip returns the expected rows.
- Mixed `Net >= N` and `PTS/G >= N` chips compose with AND.
- Two chips on one metric form a range.
- Removing one chip leaves the others active.
- Reset clears all Player Stats chips.
- Player Stats chips survive Summary/Four Factors/Shot Profile switches.
- Incompatible view-specific chips are removed without clearing common chips.
- Starter-count restrictions do not alter Player Stats values and trigger the disclosure copy.
- Query failure does not fall back to unfiltered results.

### Cross-tab validation

For at least three contexts—default season, restricted dates/teams, and opponent/game-number filters—compare the hidden filter values against the Player Stats tab in Per Game mode. Values for the same `game_year + team_id + player_id` must agree within the displayed rounding tolerance.

### Regression tests

- Existing On/Off Summary, Four Factors, and Shot Profile render tests still pass.
- Grouped table headers remain aligned.
- On/Off percentile colors do not change when a Player Stats threshold is added.
- Auto possession minimums do not change when a Player Stats threshold is added.
- Existing Player Stats tab output and filters remain unchanged after helper extraction.

### Manual visual checks

- The expanded filter menu is readable on desktop and mobile.
- The popover remains above the DataTable header and horizontal scroller.
- Long Player Stats labels do not overflow the popover.
- Active chips wrap cleanly.
- The starter-filter scope note is visible but not visually dominant.
- Dark-theme styling remains consistent.

## Acceptance Criteria

The feature is complete when:

1. A user can add any initial-scope Player Stats filter from the On/Off `+ Filter` chip.
2. Player Stats and On/Off filters combine using AND and produce correct rows.
3. Player Stats values match the Player Stats tab for every supported shared context.
4. All rate labels and percentage conventions are explicit.
5. Player Stats chips work in all three Israeli On/Off views and persist across view switches.
6. No Player Stats query runs until a Player Stats chip is active.
7. Starter-count scope is disclosed and never misrepresented.
8. Query failure cannot silently produce an unfiltered result.
9. Existing table headers, ranks, heat colors, auto-minimums, and Player Stats behavior remain unchanged.
10. Automated tests and a deployed visual check pass.

## Rollout

1. Implement and test the Israeli-league path.
2. Validate query latency and cache reuse against default and filtered contexts.
3. Deploy and observe real use before expanding the metric list.
4. Add EuroLeague parity using its canonical traditional-stat reader and league-specific availability rules.

