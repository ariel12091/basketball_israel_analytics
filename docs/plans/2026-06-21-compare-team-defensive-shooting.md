# Compare Teams Detail: Defensive Shooting Plan

## Goal

Add a Defensive Shooting section to the Compare tab's Teams detail view, following the existing offensive Shooting section and applying defensive metric polarity.

## Scope

- Teams detail view only.
- Reuse the existing shooting query and defensive shooting columns.
- No database, DDL, ETL, or materialized-view changes.
- Leave Lineups and Players detail views unchanged.

## Implementation

1. Extend `add_shooting_rates()` in `app/R/server_tab7_compare.R` to calculate:
   - Opp 2PT accuracy: `def_fg2_made / def_fg2_att`
   - Opp 2PT frequency: `def_fg2_att / (def_fg2_att + def_fg3_att)`
   - Opp 3PT accuracy: `def_fg3_made / def_fg3_att`
   - Opp 3PT frequency: `def_fg3_att / (def_fg2_att + def_fg3_att)`
   - Return `NA` when the relevant denominator is zero.

2. Extend `run_team_shooting()` to aggregate the existing columns:
   - `def_fg2_made`
   - `def_fg2_att`
   - `def_fg3_made`
   - `def_fg3_att`

3. Update `DETAIL_METRICS`:
   - Rename the current `Shooting` section to `Offensive Shooting`.
   - Add `Defensive Shooting` with:
     - Opp 2PT Acc — lower is better
     - Opp 2PT Freq — neutral
     - Opp 3PT Acc — lower is better
     - Opp 3PT Freq — neutral

4. Make the new section Teams-only when constructing active detail sections and metric rows. Do not show it for Lineups.

## Tests

Extend `app/tests/testthat/test-tab7-compare-server.R` to verify:

- Teams detail contains `Offensive Shooting` and `Defensive Shooting`.
- All four opponent shooting labels render.
- Defensive accuracy and frequency values are calculated correctly from mock data.
- Lower opponent accuracy uses defensive winner polarity.
- Frequency remains neutral.
- Lineups detail does not contain `Defensive Shooting`.

## Verification

1. Run the focused Tab 7 tests:

   ```powershell
   & 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' -e "testthat::test_file('app/tests/testthat/test-tab7-compare-server.R')"
   ```

2. Run the complete test suite:

   ```powershell
   & 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' scripts/test_all.R
   ```

3. Manually verify Teams detail with different A/B filters:
   - Defensive values update with the filters.
   - Gap signs and winner colors follow lower-is-better accuracy.
   - Gap sorting includes the new section.

## Baseline

Before implementation, the focused Tab 7 server test file passes all 77 assertions.
