# Ribbon Workstreams A/C continuation handoff, 2026-09-20

This continues:

- `docs/plans/2026-09-19-period-opening-lineup-anchors-design.md`
- `docs/plans/2026-09-19-stint-ribbon-correctness-plan.md`
- `docs/ribbon_straddles_and_subs_handoff_2026-09-20.md`

## Completed in this continuation

### 1. Corrected Workstream A's completion record

Workstream A's historical reprocessing population was selected with the
period-opening missing-attribution measure. That measure does not select a
segment that has attribution but carries the previous period's five.

The strict-inside re-audit is reproducible with:

```powershell
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' scripts/audit_period_anchor_reprocess_population.R
```

It finds five regulation straddles with a same-team substitution strictly
inside the overhang:

- two in excluded game 406;
- two three-second intervals in already-reprocessed game 62534;
- one 72-second interval in game 62452.

Game 100 is not a defect. Its Q2 substitutions occur exactly at the carried
segment's endpoint, so the previous five is valid until that endpoint. The
predicate must remain `sub.elapsed < segment_end`, not `<=`.

Game 62452 is the only genuinely omitted case. Its Q4 feed declares five OUT
substitutions at `10:00` and no matching incoming five; later states contain
only two or three players. The additive anchor rule cannot repair it, so there
is no additional anchor-compatible game to reprocess. The plan now records
that the original selection was wrong and does not present A as fully closed.

Updated documentation:

- `docs/plans/2026-09-19-stint-ribbon-correctness-plan.md`
- `docs/ribbon_straddles_and_subs_handoff_2026-09-20.md`

### 2. Implemented Workstream C

The Israeli ribbon query now detects a valid-five segment that crosses a
regulation boundary while the same team has a substitution strictly inside
the post-boundary overhang. It returns:

- `false_straddle_segments`;
- `false_straddle_seconds`.

`ribbon_health_message()` combines this with the existing invalid-lineup
warning. The EuroLeague query returns zeroes for the same reader contract.

Changed files:

- `app/R/global.R`
- `app/R/helpers.R`
- `app/tests/testthat/test-stint-ribbon-readers.R`

Live read-only verification:

| Game | Intervals | Seconds |
|---:|---:|---:|
| 100 | 0 | 0 |
| 62452 | 1 | 72 |
| 406 | 2 | 736 |
| 62534 | 2 | 6 |

The representative game-62452 query executed in about 58 ms. Re-run with:

```powershell
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' scripts/verify_ribbon_false_straddle_health.R
```

Both targeted test files pass:

```powershell
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' -e "testthat::test_file('app/tests/testthat/test-stint-ribbon.R', reporter='summary'); testthat::test_file('app/tests/testthat/test-stint-ribbon-readers.R', reporter='summary')"
```

The output contains the repository's existing `LC_ALL` warnings. Nine
database-gated reader tests are skipped unless `RUN_DB_TESTS` is enabled.

No database rows or schema objects were changed in this continuation.

## Next task: Workstream D

`scripts/audit_ribbon_excluded_segments.R` was drafted to reproduce the
ribbon's valid-five guard across all Israeli games and classify excluded
gameplay segments by recorded player count. It has **not been run yet**. Start
by reviewing and running it read-only, then compare its totals with the old
baseline of 67 segments, 19 team-games and 4,675 seconds.

Known prior evidence suggests the material residue should be concentrated in:

- game 178: sustained six/seven-player source failure;
- game 62452, team 11: the zero-to-two/three-player Q4 failure above;
- game 62479, team 7: a short six-player substitution failure.

Transient reset declarations should be distinguished from sustained gameplay
holes. Do not infer a missing or extra player without external evidence.

After D, the plan's next investigation is Workstream F (mid-quarter attribution
holes). The `subs` hot-table promotion still needs its separate acceptance
check after the next nightly ETL: confirm Phase 7 leaves `basketball_test.subs`
populated.

## Working-tree caution

The repository was already substantially dirty. In particular,
`app/R/global.R`, `app/R/helpers.R`, `app/tests/testthat/test-stint-ribbon.R`,
and the ribbon plan contained pre-existing user changes. The margin-boundary
work in those files was preserved. Review the scoped diff before committing;
do not stage the entire working tree.
