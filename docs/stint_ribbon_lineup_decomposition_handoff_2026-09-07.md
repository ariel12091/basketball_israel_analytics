# Stint ribbon -- lineup decomposition: handoff

Date: 2026-09-09 (updated)
Branch: `shiny/stint-ribbon` (unmerged, undeployed)
Plan: `docs/superpowers/plans/2026-09-07-stint-ribbon-lineup-decomposition.md`
Spec: `docs/superpowers/specs/2026-09-07-stint-ribbon-lineup-decomposition-design.md`

## Outcome and stopping point

The planned implementation work in Tasks 1-10 is present, but the feature is
intentionally being parked as **accepted work in progress**, not declared
finished. The user reviewed the current local app on port 7666, accepted the
latest game-log ordering correction, and chose to move to more pressing work.
Task 11's automated and database checks are complete; broader visual/product
acceptance remains open. The user asked not to spend more time on Playwright.

Clicking a stint now splits that bar into its five-player lineup windows and
inserts the lineup details directly beneath the clicked team's bars. Rows are
aggregated by five-player lineup and ordered by total duration; repeated
appearances show one aggregate row with separate timestamp windows. Hovering
or keyboard-focusing a row prominently marks all of its windows on the
selected player's lane and margin curve. Clicking the selected stint again,
pressing Enter/Space again, or pressing Escape clears it. The selected bar no
longer shows its overall stint +/-, and ordinary mouse hover no longer displays
the old data tooltip/detail text.

The resting ribbon remains unchanged. Opponent-side values remain in the
opponent's perspective, and Israeli and EuroLeague tabs share the same
implementation.

## Branch state

```text
63169f4  plan committed
3175a1a  Task 1 helper
693b272  Task 1 wiring                              <- Task 1 complete
07ee695  shared modal extraction
9ad5e82  collision-guard correction                <- Task 2 complete
afe5613  rate-limit ribbon open                     <- Task 3 complete
f70e46a  user's app/manifest.json commit
be8f28e  Tasks 4-7 implementation
31a58a4  Tasks 4-7 verification                     <- Tasks 4-7 complete
97effd9  original handoff
0901399  per-bar lineup segments                    <- Task 8 complete
d7758cb  click-to-split interaction                 <- Task 9 complete
2d147bf  hide selected bar's whole-stint +/-        <- Task 9 follow-up
b93cd14  lineup marks and Task 9 review fixes       <- Task 10 complete
9171662  linked-date orthogonal ordering fix
```

The handoff update itself follows these commits.

## Task status

| # | Task | State |
|---|---|---|
| 1 | Gate ribbon links on games with score data | Complete |
| 2 | Extract the shared ribbon modal observer | Complete |
| 3 | Rate-limit ribbon opening | Complete |
| 4 | Carry a lineup key on every lane row | Complete |
| 5 | Build the per-game lineup dictionary | Complete |
| 6 | Decompose a merged bar into lineup segments | Complete |
| 7 | Use each side's own perspective | Complete |
| 8 | Emit segment data and remove overlap machinery | Complete, reviewed and benchmarked |
| 9 | Split a stint on click and list its fives | Complete, reviewed |
| 10 | Mark the five on the lane and curve | Complete |
| 11 | Whole-feature verification | Engineering checks complete; user visual acceptance remains |

## Review findings and corrections

### Task 8

The builder retains pre-merge lane rows, computes one lineup dictionary in one
stable order, and serializes the per-bar segment decomposition against that
same dictionary. The old overlap helpers and payload were removed.

A real Israeli game with 95 merged bars was rendered five warm times before and
after Task 8. Median render time improved from 0.530 seconds to 0.430 seconds
(-18.9%). Individual runs were:

- before: 1.170, 0.580, 0.530, 0.520, 0.390 seconds
- after: 0.580, 0.420, 0.520, 0.430, 0.420 seconds

### Task 9

The implementation review found and fixed two input-parity regressions:

- Enter and Space now toggle the selected stint, matching mouse click.
- Touch/no-hover devices retain tap-to-focus while also selecting the stint.

The selected bar's whole-stint +/- is hidden using the selected-lane direct
child selector; segment values remain visible.

### Task 10

`marksOtherWindows()` is the single provisional scope predicate. It currently
returns `true`, so other windows for the same player and five are shown as
weaker echoes. Returning `false` narrows the behavior to the clicked stint
without changing the rest of the implementation.

The full and echo CSS selectors have specificity `(0,5,0)`; they beat the
existing focused-lane rule `(0,3,1)` and active rule `(0,4,1)`. Styling uses
existing design tokens and adds no raw color literals.

### Game-log date ordering

The discarded approach added a hidden `game_date_sort` column. The current
solution uses the visible linked date column only. A shared DataTables
orthogonal renderer strips the link and returns a numeric `YYYYMMDD` sort key
for later header clicks. Initial rendering uses `order = list()` so DataTables
preserves the authoritative R-side date/GN/game ordering instead of applying a
second browser-side sort. This is used by both Israeli game-log modes and
EuroLeague and adds no table column. The user verified the corrected ordering
in the local app on 2026-09-09.

## Verification on 2026-09-09

Passed:

- `node --check app/www/app.js`
- R parse checks for both game-log server files and the ribbon tests
- focused `test-stint-ribbon.R` and `test-tab11-euro-gamelog-minutes.R`:
  zero failures, errors, or skips after the final ordering correction
- live database `RUN_DB_TESTS=1` runs for
  `test-stint-ribbon.R` and `test-stint-ribbon-readers.R`: zero skips and
  zero failures
- `git diff --check`
- limited browser check: the Israeli log rendered all 440 entries in descending
  chronological default order with linked dates

The full suite currently has four failures. Running it in both the shared
checkout and a clean clone at the branch base produced the same failures, so
none are introduced by the ribbon/date changes:

- `test-companion-query-counts.R:293`: `object 'minutes' not found`
- `test-tab7-compare-server.R:308`: `date_split`
- `test-tab7-compare-server.R:308`: `gn_split`
- `test-tab7-compare-server.R:654`: Unicode minus/gap text mismatch

The plan's old expectation of exactly one full-suite failure is therefore stale
for the current branch/environment.

## Remaining handoff items

- Treat the current feature as WIP. Do not describe it as shipped or complete
  merely because Tasks 1-10 have implementations.
- Continue visual/product review in both leagues when work resumes: selected
  own/opponent stints, inline panel placement and sizing, segment sums, repeated
  lineup windows, marks/echoes, Escape/replacement behavior, and keyboard/touch
  parity.
- Decide whether the broad Task 10 echo scope should remain. The one-line switch
  is `marksOtherWindows()` in `app/www/app.js`.
- Merge and deployment have not been performed.

## Constraints worth preserving

- The scoreless-game set intentionally uses one cached reference key with no
  season dimension; it is tiny and changes only with ETL.
- EuroLeague uses `eurogl` for Shiny input ids and `eugl` for SVG clip ids.
  Do not normalize them into one prefix.
- A merged bar's first `lineup_key` is not the bar's lineup. Always use the
  pre-merge segment rows and the single shared dictionary ordering.
- Do not modify `CLAUDE.md`; `PROJECT.md` is the active project guide.
- The shared checkout contains substantial unrelated cold-start and user work.
  Do not sweep it into ribbon commits.
