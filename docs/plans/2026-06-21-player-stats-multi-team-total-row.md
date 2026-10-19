# Player Stats Multi-Team Total Row Plan

Date: 2026-06-21

## Goal

Add one `TOTAL` row in the Player Stats tab for each player represented on two or more teams under the current filters. The row must combine the player's team rows without changing the existing team-specific rows.

## Requirement Assumption

Interpret “more than 2 teams” as “more than one team” because the request also says “both teams.” Confirm this before implementation. If the literal requirement is three or more teams, only the eligibility threshold changes.

## Approach

Implement the aggregation in `app/R/server_tab5_traditional.R`, after raw totals are loaded and before display-mode conversion. Do not change `get_player_traditional_dynamic()` or `player_traditional_stats_mv`.

Use the stable `identity_id` dictionary to match a player across teams. Do not group by player name alone. Load a season-level lookup from `resolved_player_identity_v` that maps each `(game_year, team_id, player_id)` result row to one `identity_id`; fail safely by leaving ambiguous or unresolved rows uncombined.

## Calculation Rules

- Sum: GP, possessions, minutes, PTS, rebounds, assists, steals, blocks, turnovers, and made/attempted shooting counts.
- Recalculate from summed counts: FG%, 2P%, 3P%, FT%, eFG%, and TS%.
- Calculate TOTAL USG% as a possession-weighted average of the component team USG% values; do not sum percentages.
- Set `team_name` to `TOTAL` and retain the canonical player display name.
- Build totals before Per Game / Per 60 Possessions / Per 30 Minutes conversion so every display mode uses the combined raw totals correctly.
- A total reflects the teams included by the current database filters. A single selected team produces no total row.

## Implementation Tasks

### 1. Add and validate the identity lookup

File: `app/R/server_tab5_traditional.R`

- Add a cached query for the selected season using `resolved_player_identity_v`.
- Reduce it to one unambiguous `identity_id` per `(game_year, team_id, canonical_player_id)`.
- Join the lookup to both MV and dynamic-query results.
- Verify known multi-team players resolve to the same identity across teams.

### 2. Add a raw-total aggregation helper

File: `app/R/server_tab5_traditional.R`

- Add a helper that detects identities with at least two distinct teams.
- Generate one aggregate row per eligible identity.
- Recalculate all percentage columns from raw totals.
- Add an internal flag such as `is_multi_team_total` for styling and filtering.
- Append the generated rows while preserving the original team rows.

### 3. Integrate with filtering and display modes

File: `app/R/server_tab5_traditional.R`

- Run aggregation after raw result normalization and before `ts_mode_context()`.
- Ensure minimum-GP and stat filters apply to TOTAL rows using combined values.
- Update player filtering so selecting any team entry for a multi-team player can include its TOTAL row, while team filtering continues to limit which teams contribute.
- Ensure eligibility thresholds and rate calculations do not double-count appended TOTAL rows.

### 4. Render and style the row

Files:

- `app/R/server_tab5_traditional.R`
- `app/www/app.css`, only if existing DT styling is insufficient

- Display `TOTAL` in the Team column.
- Give TOTAL rows a subtle bold/background treatment.
- Keep sorting, column visibility, tooltips, and exports working.
- Exclude internal identity and row-type columns from the visible table and export.

### 5. Verification

- Parse the modified R file with the configured R 4.4.2 `Rscript`.
- Test a season with a player on exactly two teams.
- Confirm one TOTAL row appears and all counting totals equal the component rows.
- Confirm shooting percentages are recalculated from combined makes/attempts.
- Check Totals, Per Game, Per 60 Possessions, and Per 30 Minutes modes.
- Check no total appears for a one-team player or when only one team remains after filtering.
- Check date, opponent, game type, GN, clutch, player, minimum-GP, and stat filters.
- Verify the MV fast path and dynamic SQL path return equivalent TOTAL values for equivalent filters.
- Confirm unresolved/ambiguous identities do not create false combined rows.

## Expected Scope

Small-to-medium change concentrated in the Shiny server layer. No SQL function signature, materialized-view schema, ETL, or deployment change should be required.
