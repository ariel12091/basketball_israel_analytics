# Free-throw foul-lineup attribution review

Date: 2026-10-12

Plan: [Free Throws Credited to the Lineup That Drew the Foul](2026-10-12-free-throw-foul-lineup-attribution.md)

Scope: the implementation and accepted deviations recorded in the plan, with emphasis on the seven review areas requested.

## Severity definitions

| Severity | Meaning |
| --- | --- |
| P1 — High | A structural correctness problem that should be fixed before considering the migration fully validated. |
| P2 — Medium | Incorrect edge-case results or verification gaps that require correction. |
| P3 — Low | A robustness weakness without a demonstrated failure in the current definition files. |

## Findings

### 1. P1 — High: rewrite the clutch stats-to-time joins

Both readers inner-join credited event segments to physical time segments. That drops qualifying free throws when the parent segment has no rows inside the clutch window, and drops physical time when its offense events were re-credited elsewhere.

The reported one point and 2.4 minutes are evidence of this structural problem, not a bounded approximation. Aggregate events and physical time independently, then combine them while preserving unmatched rows. A LEFT JOIN alone only fixes the event loss.

References:

- [fetch_lineups_all.sql:304](../../../sql/functions/fetch_lineups_all.sql#L304)
- [fetch_lineups_four_factors.sql:406](../../../sql/functions/fetch_lineups_four_factors.sql#L406)

### 2. P2 — Medium: G6 does not fail on changed values

check_fn() prints changed columns but never calls report() for them. An offline reproduction changed pts from 10 to 999 in five cases; the failure count remained zero. Missing snapshots are silently skipped, and intersecting column names hides missing columns.

Define permitted changes per reader, and fail on unexpected changes, missing cases, or schema differences.

Reference: [verify_ft_credit.R:295](../../../scripts/verify_ft_credit.R#L295).

### 3. P2 — Medium: G3 can pass with missing team-games or no coverage

G3's inner join checks only keys present in both actual and expected results. Removing a whole team-game from the derived object can therefore evade G3. Use a full comparison, require expected coverage, and reject an empty comparison.

The NULL-key fix in compare_sums() is sound, but that function also intentionally treats NULL sums as zero and cannot distinguish an absent all-zero group.

Reference: [verify_ft_credit.R:221](../../../scripts/verify_ft_credit.R#L221).

### 4. P2 — Medium: zero-point rows can materially inflate TOTAL TS%

Recovering attempts from positive, rounded TS% is acceptable as a small display approximation. Falling back to FGA for zero-point rows is not similarly bounded: it discards every missed personal-foul trip.

An offline reproduction produced a TOTAL of **50% instead of 25%**, using a zero-point row with ten missed trips plus a ten-point row with ten attempts.

Return the additive true-shooting attempt count and sum it directly; use it for usage fallback too.

References:

- [helpers.R:2048](../../../app/R/helpers.R#L2048)
- [server_tab5_traditional.R:274](../../../app/R/server_tab5_traditional.R#L274)

### 5. P3 — Low: the view substitution is not identifier-safe

The current five definition files stage their twelve declared index names correctly in an offline check. However, the global replacement of the qualified object name also rewrites longer names: basketball_test.foo_archive becomes basketball_test.foo__ftc_next_archive when staging foo.

Restrict replacements to complete identifiers and validate the resulting declaration and index targets.

Reference: [apply_ft_credit_migration.R:145](../../../scripts/apply_ft_credit_migration.R#L145).

## Conclusions for the remaining review areas

### Player Four Factors: no attribution-side defect found

The floor_data split is correct. Event ON/OFF follows the credited lineup; minutes and onoff_minutes follow physical lineups. No attribution-side error was found reading the function end to end.

Keeping lineup_stats physical is correct for its current shot-only measures. Add a comment or contract check against introducing free throws there.

Reference: [refresh_player_four_factors_by_game_for_games.sql](../../../sql/functions/refresh_player_four_factors_by_game_for_games.sql).

### Traditional readers: equivalent joins, with a GP limitation

All four sources implement equivalent possession/GP joins. A credit-only player retains possessions with gp = 0. That preserves the existing definition, but GP still means “games with a physical offensive possession end,” not every appearance. Per Game output can consequently be blank despite positive exposure.

Sources reviewed:

- [compute_player_traditional_by_game.sql](../../../sql/functions/compute_player_traditional_by_game.sql)
- [get_player_traditional_custom_clutch.sql](../../../sql/functions/get_player_traditional_custom_clutch.sql)
- [get_player_traditional_dynamic.sql](../../../sql/functions/get_player_traditional_dynamic.sql)
- [player_traditional_stats_mv.sql](../../../sql/materialized_views/player_traditional_stats_mv.sql)

### Clutch parity: shared join defect and pre-existing starters differences

The two readers' clock/margin eligibility and join-loss mechanism match. Their starters predicates remain different, as noted in the review request.

Additionally, credited starters currently filter the rows used for physical time. Separate those predicates when rewriting the joins.

### Nightly path: refresh order is correct; source validity remains a dependency

The order is correct: insert → clock refresh → credit refresh → downstream aggregates, inside the ETL transaction. A credit-function error rolls back publication.

Non-NULL segment credit still depends on valid source/parent segment keys. There is no runtime assertion enforcing it.

References:

- [refresh_df_pts_poss_lineups_longer_for_games.sql:173](../../../sql/functions/refresh_df_pts_poss_lineups_longer_for_games.sql#L173)
- [refresh_ft_credit_fields_for_games.sql](../../../sql/functions/refresh_ft_credit_fields_for_games.sql)
- [etl_full.R:1065](../../../etl/etl_full.R#L1065)

### Attribution rule: consistent with the agreed design

Crediting technical, unsportsmanlike, and and-one free throws to their matching parent follows the stated design. Only personal-foul parents enter the trip denominator. Subsequent rebounds remain physical, as specified.

This conclusion concerns consistency with the project's attribution contract.

## Validation and limits

The review used static inspection and offline R reproductions. The reproductions confirmed that G6 does not record failures for changed values, demonstrated the zero-point TOTAL denominator error, checked index-name staging for the five current definition files, and demonstrated the object-prefix substitution hazard.

Live database gates were not rerun. The nightly ETL was not exercised. No implementation files were changed during the review.

## Recommended order

1. Fix both clutch readers together, preserving credited events and physical time independently.
2. Make G6 enforce permitted changes and make G3 enforce complete, non-empty coverage.
3. Return additive true-shooting attempts for TOTAL and usage calculations.
4. Harden migration identifier substitution.
5. Verify credit columns and downstream invariants after a nightly ETL run before cleanup.
