# Period-opening lineup anchors — handoff for review, 2026-09-20

**Status: merged to `main` and live.** `d49a020` is the implementation merge
point; `main` has advanced past it with documentation commits. The nightly ETL
runs from `main`, so the next run (21:15 UTC) is the first to produce anchors
for new games.

Design, root cause and the full acceptance record live in
`docs/plans/2026-09-19-period-opening-lineup-anchors-design.md`. This document
is the review narrative: what shipped, what was verified, what was wrong, and
what is still open.

---

## 1. What shipped

```
d49a020 Merge branch 'etl/cold-storage-cumulative-backup'
3894bbc Merge branch 'etl/period-opening-anchors'
```

Behind `3894bbc`, in order:

| Commit | What |
|---|---|
| `42c11b2` | Wire anchors into `compute_lineups_lookup()` (pre-existing at session start) |
| `487bfc6` | Harden three gate defects; reverse the OT scope decision |
| `affa063` | Commit `etl/ot_lineup_recovery.R`'s design doc, untracked since June |
| `aae4181` | Gate 4 degrades; add the collision gate; exclude game 211; require `type` |
| `03d49b5` | Run the reject gates **before** Gate 4 drops anything |
| `0aaf757` | Record the shadow run; record the game 406 diagnosis |
| `da9f813` | Steps 6-7: reprocess 401/404, acceptance checks |
| `6db8ab3` | Steps 8-9: offline reprocess of 18 games; criterion 1 falsified |

Behind `d49a020`: `fa46e00`, making the cold-storage release backup cumulative.

**The rule.** `compute_lineups_lookup()` unions one payload-free anchor row per
team at each regulation period's first action from Q2 on. The existing
fill-down supplies the carried-forward five; a same-clock substitution still
wins the `slice_max` tie-break, so provider states are never overwritten.

---

## 2. What was verified, with numbers

**Shadow run (no writes), control arm with anchors suppressed:**

```
control (no anchors) -> anchored
  rows 1857 -> 1937 | lost 0 | added 80 | CHANGED on shared keys: NONE
```

**Additive at the `lineups_lookup` grain**: no lineup state the provider
produced was lost or altered. Note the limit — this control compared
`compute_lineups_lookup()` output only. The design's Step 2 also asked for a
`stints`/`pws` comparison proving that no *existing action* changed its
team/lineup assignment, and that was never run action-by-action. The exact
points reconciliation in the next table is strong indirect evidence for it
(attribution moving between teams would have to net to zero to hide), but
downstream semantic additivity is **not directly proven**, and cannot be
checked retroactively now that the pre-anchor rows are overwritten. A fresh
shadow on a not-yet-reprocessed game would be needed; 406 is the only
candidate, and it is held.

**Games 401 and 404, reprocessed through the real ETL:**

| Check | Before | After |
|---|---|---|
| 401 points missing (team 11 / 33) | 2 / 3 | **0 / 0** |
| 404 points missing (team 4 / 11) | 2 / 5 | **0 / 0** |
| 401 Q4 first attributed clock | 548 | 600 |
| 404 Q2 | 1732 | 1800 |
| 404 Q4 | 483 | 600 |
| 7 of the 8 named target actions | absent from MV | attributed |
| `lineups_lookup` rows | 776 / 584 | 788 / 610 |

The eighth target action is game 406, deliberately excluded. 401 Q5 is
unchanged at 286 — overtime, see section 3.

**18 historical games, reprocessed offline** — a set disjoint from 401/404,
because the affected-game survey ran *after* those two were fixed, so they no
longer showed a gap and dropped out of it. 33 of 36 team-games reconcile
exactly to the official box score. Residuals:

- **184** (both teams, -5): raw feed has zero Q3/Q4 actions. Known; the design
  doc's criterion 2 requires it to stay.
- **62526 team 24** (-1): a made free throw, id `625260653`, stamped at
  `clock = 1200` — exactly the Q3 opening. A boundary case anchors do not
  cover. **Open.**

**Tests:** 43 anchor tests green, including both Postgres tests under
`RUN_DB_TESTS=1`. Full suite 2951 assertions with three pre-existing failures
(`test-companion-query-counts.R`, `test-idle-restore-bookmarking.R`,
`test-source-encoding.R`) — byte-identical failing set before and after.

---

## 3. The three judgement calls, and why

**Overtime is excluded (`PERIOD_ANCHOR_MAX_QUARTER = 4`).** The design
originally scoped OT in, calling an anchor "a cheaper path to the same result".
It is a cheaper path to the same *lineup* but not the same *guarantee*:
`detect_ot_leading_lineup_gaps()` fires only when the first OT gameplay action
has a NULL lineup hash, and `etl_full.R` stages `lineups_lookup` before it
builds the provisional PWS — so an OT anchor closed that condition before the
detector read it, silently retiring `recover_ot_lineup_periods()` with its
event replay, audit trail and reject path. Gate 1 cannot substitute: it checks
`n_on == 5`, which a carry-forward satisfies by construction.

Not hypothetical — the module fired on **game 401 Q5** in production on
2026-09-16/17, and again during this session's ETL run
(`accepted_carry_forward`, both teams, 87 reconstructed rows). OT stays with
`ot_lineup_recovery.R`.

**Gate 4 degrades instead of rejecting.** A period opening stamped with a
later clock is a provider defect (the 398/399 class), not a fault in the anchor
rule, so it costs that period's anchors rather than the game's whole
transaction. The old behaviour also contradicted the helper's own comment,
which tolerates a clock that regresses inside a period.

**Game 211 is excluded outright.** Its regulation and OT action ids overlap,
and *both* halves of the mechanism are id-ordered — the anchor is a period's
lowest id, and the fill-down runs down an id-ordered window. An anchor there
would carry an arbitrary mid-game state forward with `n_on = 5`, which no gate
can distinguish from a healthy one. `ot_lineup_recovery.R` excludes the same
game for the same reason.

---

## 4. Defects found and fixed during review

Six gates now guard the write. Three defects came from a code review, one from
a follow-up review, and all were reproduced before being fixed.

1. **Gate 4 reported a phantom period on an unreadable clock.** `NA < x` is NA,
   and logical-NA row subsetting of a data frame returns an all-NA *row*, so a
   game aborted with `game NA QNA id NA at NA < NA`.
2. **A missing `period_anchor` marker silently emptied the frame.** Every gate
   reads it with `%in% TRUE`; on a frame without the column that is
   `logical(0)`, which subsets every row away — and the gate runner's return
   value is what gets written to `lineups_lookup`.
3. **The parity check was one-directional.** A `UNION ALL` yielding nothing
   passed every gate and reverted the ETL to pre-fix behaviour with no trace.
   Added `period_anchor_coverage_gaps()`, which requires each expected anchor
   to be either retained or explained by a provider state at the same
   period-opening clock. The reviewer's suggested floor ("at least one anchor
   per game") would have false-alarmed: the survey shows anchors are
   legitimately superseded in all but 31 of 1376 periods.
4. **Gate 4 could hide a Gate 6 collision** (found in the second review).
   Gate 4 drops anchor *rows*, and Gate 6 defines the windows it inspects from
   the anchors still present — so a period that was both misclocked and
   collided returned normally while the corrupted provider row was written.
   **Parity had the identical shape and had not been spotted.** Both reject
   gates now run on the frame exactly as collected, ahead of any R-side drop,
   because both test conditions created in SQL: dropping a row in R does not
   undo an upstream defect, it only removes the evidence. Both regression
   tests were verified to fail against the old ordering.

---

## 5. Two measurements that were wrong

Recorded because the wrong version was stated with confidence first.

**"Criterion 1 falls to 0" is not achievable.** Measure A reports the first
*attributed* clock, which is floored by when the first attributable event
occurs. A period whose opening seconds hold only the `team_id = 0` marker can
never reach the nominal opening. Game 66 Q2's stint now starts exactly at the
anchor id `660195`, and the `pws` row there is that marker, so
`first_attributed` stays at 1784. The baseline's own median gap of 12s was the
clue. **The criterion should be restated against points attribution, not the
clock proxy** — not yet done.

**"Refetching is safe" was based on too shallow a check.** Comparing action
ids and counts, all 19 affected games matched the provider exactly. Comparing
every column tells a different story: game **64902** returns **507 changed
`parent_action_id` and 26 changed `parameters_points`** — and
`parameters_points` is the 2PT/3PT split. Re-ingesting would have rewritten
shot attribution on a historical game and folded it into a change whose whole
verification story is "additive only". The 18 games were therefore reprocessed
**offline from the cold-storage archive**, not refetched. (62449 also shows one
changed `player_id`, most likely because the comparison omitted
`canonicalize_actions_player_ids`, which runs after `clean_actions`.)

---

## 6. Database state

- Anchors applied to **20 distinct games**: 401 and 404 (real ETL), plus the
  18 historical games (offline). The two sets do not overlap — the 19-game
  affected set was measured after 401/404 were already fixed, so neither
  appears in it: `66, 68, 88, 156, 177, 184, 223, 360, 406, 62449, 62506,
  62512, 62526, 62530, 62534, 62537, 62540, 62581, 64902`. Game **406** is
  excluded from the 19, leaving 18.
- `app_meta.etl_full_last_success` advanced to `2026-09-20 09:39:11` so the
  Shiny season caches pick up the offline reprocess. The ETL writes this marker
  with R's local clock (`etl/etl_full.R:129`), not the database's UTC `now()`.
- Cold tables hold the 18 games' `actions_clean`/`possessions` rows; the next
  Phase 7 exports and truncates them, which is the designed path.
- Local `exports/cold/` grew 446 -> **448 games** by merging the GH release,
  which held the only copy of games 404 and 406. Pre-merge copies are in
  `exports/cold/backup-2026-09-20-pre-release-merge/`.

---

## 7. Open items

| Item | Evidence | Priority |
|---|---|---|
| **Game 406 relabel** — labelled Q2 is really Q3; labelled Q4 is Q3+Q4 merged. Needs an ad-hoc script like the game 402 corrections (`b944aed`) | Owner-reported; explains 7 actions in "Q2" vs 280 in "Q4" | Deprioritized by owner. Holds the last Q2-Q4 late period and a -2 point gap |
| **62526 -1 point** | Made FT `625260653` at `clock = 1200`, the exact Q3 opening | Medium |
| **Restate criterion 1** against points, not the clock proxy | Section 5 | Medium — doc only |
| **CLAUDE.md backlog item 3** still calls the cold-storage backup broken | Fixed in `fa46e00` | Low. Left alone because that file has uncommitted local edits |
| **`etl/etl_lineups.R` does not parse** (stray prose, line 211) | Tracked, nothing sources it; helpers were extracted into `etl_full.R` | Low, cosmetic |

---

## 8. What is next

The ribbon plan (`docs/plans/2026-09-19-stint-ribbon-correctness-plan.md`)
named the anchors as its step 0. That prerequisite has landed, so its sequence
unblocks:

1. Re-measure faulty straddles — baseline **6 of 51**, expected 0
2. **D** — excluded segments; baseline 67 segments, 19 team-games, 4675s
3. **F** — mid-quarter attribution holes, 10 games, undiagnosed
4. **H (database half)** — `scripts/promote_subs_to_hot.R` with
   `CONFIRM_SUBS_PROMOTION=1`
5. **C** — the absent-segment warning; needs H
6. **G** cosmetic; **E** raise separately as feed defects

**One reordering, on evidence from this session.** H is not merely pending, it
is visibly incomplete: `basketball_test.subs` holds **178 rows** against
**79,238 rows / 447 games** in `exports/cold/subs.parquet`, and **zero rows for
games 401/404/406**. `subs` was promoted to hot precisely so the ribbon could
read substitution evidence at request time — with 178 rows it cannot. Steps 1
and 2 both want that same evidence (whether a team substituted inside a
straddle's overhang), so **H should run before the re-measure**, not fourth.
The script drops `subs_actions_clean_fk`, restores from parquet, and re-adds
the constraint `NOT VALID`; it is dry-run by default.

---

## 9. Notes for whoever picks this up

- **Attach `bit64` in any script that diffs these frames.** `n_on` and
  `num_starters` come back as `integer64`; without `bit64` attached,
  `is.numeric()` is FALSE and an `as.character()` fallback renders raw bit
  patterns. This produced a phantom "1793 changed rows" finding.
- **Snapshot the acceptance measure for every game in scope before writing**,
  not just the headline ones. Points were captured for 401/404/406 only, so the
  18 games' "33 of 36" cannot be split into improved versus already-correct.
- **`etl/etl_onoff.R` ends with a live `etl_update()` call.** Never `source()`
  it; parse to definitions only, the way `etl_full.R:197-210` does.
- Run the ETL through `scripts/run_etl_full.ps1`. Invoking `Rscript` directly
  sent the data-quality report to a temp directory and the findings page failed
  with "cannot open the connection"; the pipeline itself was unaffected.
- The offline reprocessor built for section 5 is **not committed** — it lives in
  the session scratchpad. If historical reprocessing recurs, promote it to
  `scripts/`. It restores archived `actions_clean`/`possessions` for one game,
  then calls the same helpers `etl_update()` calls, followed by one downstream
  pass: Phase 3's sub-lineup generation, the nine `refresh_*_for_games(int4[])`
  SQL functions, the MV refreshes, and `refresh_sub_lineups_stats_for_games`.
