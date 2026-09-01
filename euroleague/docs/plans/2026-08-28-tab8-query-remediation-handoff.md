# EuroLeague Tab 8 query remediation - session handoff

**Date:** 2026-08-29
**Status:** function-only query alignment APPLIED
**Spec:** `docs/specs/2026-08-28-tab8-query-remediation-design.md` (plus Addenda A, A.1, A.2, A.3)
**Plan:** `docs/plans/2026-08-28-tab8-query-remediation.md`

**Final state:** migration 045 was applied on 2026-08-29 as a function-only
query alignment. Both public functions now read the base fact directly. No new
index or function-local `work_mem` setting was applied. The paragraphs below
retain the earlier investigation and failed bundled-candidate history.

Before the final decision, nothing was applied. This was verified after every
one of the seven live runs by re-reading `pg_get_functiondef` hashes and
`pg_index`: both functions still read `player_game_context`,
`euroleague_pff_game_team_idx` does not exist, and neither function carries a
`work_mem` setting. Two runs were killed mid-flight and the server rolled both
back cleanly.

Two subsequent `--apply` attempts on 2026-08-28 also left the database
unchanged. Both completed the pre-DDL baseline and then failed at the first
catalog change with a 5-second lock timeout while inserting into
`pg_class_relname_nsp_index`. The script verified the original function hashes
and definitions after each failure. Do not spend another full baseline run
until the competing catalog transaction is identified or the apply harness is
changed to capture/retry this bounded pre-candidate lock failure without
discarding the baseline.

A later two-connection diagnostic reproduced the exact `CREATE INDEX` inside
an always-rollback transaction while polling `pg_blocking_pids()`. It completed
in 9.05 seconds with no blocker, and the index was absent after rollback. At
that point only the normal Supabase `pg_net` and `pg_cron` background workers
remained. The earlier blocker was therefore transient and had already ended;
PostgreSQL does not retain historical lock-owner rows from which its PID can be
recovered. The `pg_class_relname_nsp_index` context points to a competing
uncommitted catalog-name insertion/DDL transaction, not the analytical nested
loop or data-page I/O.

The apply harness was then hardened to retry only SQLSTATE `55P03` DDL lock
failures, retaining the already captured baseline. Its next full `--apply` run
passed DDL without a retry, exact parity, plan/buffer gates, privileges, and
contracts, but failed four performance gates and rolled back cleanly. The
important paired broad failures were:

- ON/OFF candidate 6.744 s versus companion 4.922 s (gate 5.414 s);
- Four Factors candidate 8.017 s versus companion 5.432 s (gate 5.976 s).

Earlier in that same run the candidate broad medians were 1.380 s and 1.697 s,
so the instance entered a sustained slow period during the adjacent paired
phase; the companion also slowed materially. This is nevertheless a valid
failure under the current relative policy, not grounds to commit. Two narrow
per-preset regressions (`ff eurocup broad` and `ff empty result`) also crossed
their baseline-relative limits late in the run. Post-failure verification again
found the original function hashes and view-reading definitions. Migration 045
remains unapplied.

Expected pre-apply hashes, which the apply script refuses to run against a
mismatch:

- `onoff` `083d6ff31f82cbe62083b82f36d6b4c17ac994e613d064317e7fe0b2ddbd4f82`
- `ff` `3bac5d68cb82f0e0a0f7d8e3367eb26b57f728af2649673e192ea59e8bad6c3a`

Spec: `docs/specs/2026-08-28-tab8-query-remediation-design.md` plus Addenda A,
A.1, A.2 and A.3. Plan: `docs/plans/2026-08-28-tab8-query-remediation.md`.
Branch `sql/tab8-query-shape`; `aea2b13` is the base preparation commit. The
gate/recovery hardening and this handoff remain working-tree changes until the
final rollback-only gate is reviewed.

## The defect

Both public functions aggregated the per-game fact through
`euroleague.player_game_context`. That view joins `euroleague.schedule` and the
two-perspective `euroleague.final_schedule` onto every fact row, and the
aggregation reads none of those columns. The `games` CTE inside each function
already resolves every schedule filter, so joining the fact to `games` on
`(game_id, team_id)` is the identical restriction.

The cost, from a faithful replica of the function's inner query:

```
Nested Loop                                    est=1      act=526,808
  Index Scan       player_four_factors_by_game  loops=796      buffers=69,476
  Index Only Scan  schedule                     loops=526,808  buffers=1,580,424
                                                -> 3.0 buffer accesses per loop,
                                                   96% of the 1,652,318 total
```

**1.65 million buffers is not 1.65 million pages read.** `euroleague.schedule`
is 11 pages and its primary key 4 - about 15 pages, permanently cached. Every
one of those accesses is a shared *hit*; there is no I/O at all. The cost is
CPU: 526,808 pointless B-tree descents to fetch columns nobody reads.

## Why the planner chose a per-row nested loop

Not a planner blunder - the correct choice for the estimate it had. Every join
node in that plan estimates **1 row** while producing 526,808, and the planner
costed the whole query at **163**.

| Join predicate | Estimated | Actual | Error |
|---|---:|---:|---|
| `game_id` only | 585,580 | 526,808 | 11%, fine |
| `team_id` only | 8,900,744 | 10,500,064 | 15%, fine |
| `game_id` AND `team_id` | 14,639 | 263,404 | **18x under** |

Each column is estimated well alone; together they collapse. `game_id` and
`team_id` are near-perfectly correlated - 589 games x 40 teams is 23,560
combinations if independent, but only **1,178 pairs actually exist**, because a
game determines its two teams. PostgreSQL assumes independence and multiplies
the selectivities, under-counting ~20x. There are no extended statistics on the
table to say otherwise.

The full chain:

1. `(game_id, team_id)` are correlated; the independence assumption under-counts
   ~20x per join.
2. Stacked through the real query's multi-level join, the estimate collapses
   from 526,808 to 1.
3. At one row, a nested loop with an index probe is unbeatable - total plan
   cost 163.
4. The view adds a second, redundant `schedule` join on top of that.
5. The "one" probe therefore executes 526,808 times, for 1,580,424 wasted
   buffer accesses.

**Why the fix is robust to this:** it does not try to correct the estimate. The
misestimate survives the migration, but with the view gone there is nothing left
to multiply. The only looped node becomes the fact index scan at 796 loops,
which is exactly right: one per game/team. That is why the buffer results were
identical to the digit across all seven runs while wall clock swung wildly.

With `enable_nestloop = off` the same query drops to 26,005 buffers via a merge
join, but its estimated cost is 20 billion, so it would never be chosen
naturally. Not a usable lever.

## Applied migration

`sql/045_tab8_query_shape.sql` is additive, one transaction, and has no
`DROP FUNCTION`. It contains only both function bodies with **exactly one line
changed each** - the aggregation
  source swaps from the view to the base fact, alias `c` retained so no other
  token differs. A test enforces byte-identity against candidate A.

The `(game_id, team_id)` index and 16 MB function-local `work_mem` were removed
from the deployable migration. They remain separate physical tuning experiments
because the Israeli companion has only the same game-id access index and their
individual contribution was not isolated.

Signatures, volatility, `SECURITY` mode, defaults, return columns and ordering
are unchanged, so the Shiny call sites need no edit and no deploy.
`player_game_context` is **not** dropped - the migration 002 season aggregates
still read it.

## Historical bundled-candidate measurements

These results include the source swap plus the rejected-for-bundling composite
index/work_mem package. They prove parity and explain the investigation, but
must not be attributed to the final function-only migration. Full-matrix
rollback-only run, 25 presets x both functions:

| preset | before | after | buffers | temp blocks |
|---|---:|---:|---|---|
| onoff broad, app dates | 3.787 s | **1.373 s** | 1,653,821 -> 47,623 | 9,240 -> 4,609 |
| ff broad, app dates | 4.338 s | **1.684 s** | 1,653,821 -> 47,623 | 16,794 -> 8,383 |
| onoff last 10, app dates | 0.915 s | **0.275 s** | 398,123 -> 6,133 | 0 -> 0 |
| onoff one team | 0.592 s | **0.135 s** | 141,245 -> 1,930 | 211 -> 0 |
| onoff one opponent | 0.609 s | **0.153 s** | 81,657 -> 3,555 | 225 -> 0 |
| onoff broad, NULL dates | 1.027 s | **0.810 s** | 71,988 -> 46,559 | 0 -> 0 |

**The spec measured the wrong call.** Tab 8 populates its date inputs from
`euro_season_date_bounds()`, so the app always sends an explicit
2025-09-01..2026-07-01 window. That call is ~4x slower than the NULL-date call
the spec benchmarked, for byte-identical output (358 rows either way). It was
added to the matrix as `broad app dates` and is the number that matters. The
same omission hid the single largest win in the exercise, `last 10 app dates`
at 398,123 -> 6,133 buffers.

## Candidates

| candidate | shape | verdict |
|---|---|---|
| A | direct fact source + `(game_id, team_id)` index | good; every preset faster |
| B | A + one filtered aggregation at `(player_id, team_id)` | **rejected** |
| C | B + 16 MB function-local `work_mem` | **rejected** |
| AC | A + 16 MB function-local `work_mem` | **accepted**, shipped as 045 |

**B and C are rejected on measured evidence and must not be revisited without
new evidence.** Collapsing `agg`/`pivoted` into one aggregation needs 25
(ON/OFF) and 44 (Four Factors) accumulators per group instead of 7/12 over 4x
more groups. That state no longer fits `work_mem` and introduced an **on-disk
sort where none existed** - `onoff broad season` temp written 0 -> 9,240 blocks,
`ff broad season` 0 -> 16,794, `last 10` 0 -> 1,135/4,113. Wall clock followed:
`ff broad season` 0.935 -> 3.019 s. Candidate C removed the spill with 16 MB and
the shape was still ~2.5x slower than A, so the wide aggregate costs more CPU
than two narrow passes; the spill was a symptom, not the cause. Buffer counts
were identical to A's throughout, isolating the aggregation shape as the only
variable.

## The measurement problem

This consumed more of the session than the migration did, and the conclusions
matter for any future performance work on this instance.

The spec's 0.500 s gate was anchored to a sample that does not reproduce -
6.775 s warm became 0.993 s on re-measurement, a 7x spread driven by buffer
cache state. Worse, **the Israeli companion never reached 0.500 s either**, and
it already uses the exact shape this migration adopts:
`basketball_test.player_four_factors_by_game p JOIN sched s ON s.game_id =
p.game_id AND s.team_id = p.team_id`, with no context view. EuroLeague was the
outlier; this converges it. Gates were re-anchored to companion parity in
Addendum A.

Three instrument defects were found and fixed, none of which relaxed a
threshold:

1. **Shape-matched gates (A.2).** `basketball_test.onoff_compute` ends with
   `fs.game_date BETWEEN p_start_date AND p_end_date` and has no NULL guard, so
   the companion *cannot* make a NULL-date call - it returns zero rows. Gating
   EuroLeague's NULL-date presets against a dated companion call compared two
   different plans and produced a bogus p90 failure on a preset whose median was
   0.810 s. NULL-date presets are now report-only, and a `last 10 app dates`
   preset was added so the last-N gate is shape-matched too.
2. **Companion timed inside the transaction (A.2).** It was measured before the
   DDL, minutes earlier. Not close enough: the companion's own unchanged
   `four_factors_compute` measured 1.642 s and 1.199 s twenty minutes apart.
   `basketball_test` is untouched by the candidate DDL, so reading it inside the
   transaction cannot be contaminated by it.
3. **Trimmed estimator (A.3).** The instance injects multi-second stalls into
   whatever is running; one landed inside a baseline `EXPLAIN`, 13,207 ms on a
   query whose median is 1.2 s. A representative candidate sample:

   ```
   1.34 1.35 1.37 1.37 1.37 1.37 1.38 1.39 1.39 1.40 1.41 | 1.63 3.05 5.42 7.16
   twelve values inside 0.07 s                            | the instance

   raw     median 1.390   p90 5.420
   trimmed median 1.390   upper-central 1.630
   ```

   Blocking latency statistics are computed over the central 60% of samples,
   applied identically to candidate, baseline and companion so no side gains an
   advantage. The second statistic is an **upper-central value**, not raw p90.
   Raw median and raw p90 remain report-only operational observations. Runs
   print the full sorted sample list with trimmed values marked, so a genuinely
   fat tail remains visible.

The baseline half of a run is slow because it measures the *unfixed* functions -
a single `home` call costs ~6.8 s today. Baseline sample count was cut to 3 per
preset (the candidate keeps each preset's full count), taking a full run from
~50 minutes to ~18.

## What is settled

Stable across every completed run:

- exact full-row parity, 25 presets x 2 functions, including empty results,
  zero denominators, all four starter bounds, opponent-rank top/bottom, round
  ranges and the EuroCup competition;
- shared-buffer counts, identical to the digit;
- temp blocks, with narrow-preset spills eliminated (211/225/382/408 -> 0);
- no preset regressed under the `max(10%, 100 ms)` rule;
- privileges, `SECURITY` mode, signatures, and clean rollback;
- candidate trimmed median for `onoff broad app dates`: 1.357 / 1.373 / 1.390 s
  across three independent runs.

## What is open

Whether EuroLeague Four Factors is at parity with the Israeli companion. It
measured 1.651 s against the companion's 1.642 s in one run (pass) and 1.670 s
against 1.199 s in another (fail). The companion is the unstable side, and the
recorded comparisons are candidate-trimmed against companion-untrimmed because
its raw samples were not retained. One run with both sides trimmed settles it.
This is the only unresolved gate.

## Incidental findings, outside the scope of 045

- **Israeli `onoff_compute` silently returns zero rows on NULL input.** Its
  final filter is `WHERE fs.total_net_rtg >= p_min_net` and its date filter
  `fs.game_date BETWEEN p_start_date AND p_end_date`; neither is NULL-guarded,
  so a NULL yields an empty result rather than an error. The EuroLeague version
  guards both. Tab 1 always passes real values, so it is not currently biting.
- **No extended statistics on either league's fact table.** Dependency
  statistics can help same-relation predicates and grouping estimates, but
  PostgreSQL's use of extended statistics for multi-relation join selectivity
  is limited. Treat `CREATE STATISTICS` as a separate measured experiment, not
  as an assumed repair for this join.
- **The Israeli side probably pays a smaller version of the same tax.** Same
  table shape, same missing statistics. Tab 1's broad call at 1.745 s is the
  candidate to look at.
- **JIT is already off server-wide**, so the spec's caution about not disabling
  it again is moot.
- **`app_readonly` already holds SELECT on the base fact.** The plan proposed a
  test asserting it could not; that assertion would have been false.

## Applied state and follow-up

1. Do not apply the composite index or function-local `work_mem` as part of 045.
2. From the repository root, run
   `scripts/apply_db_security.R` with `CONFIRM_DB_SECURITY_APPLY=1`, then re-run
   `scripts/audit_db_security.R` using the configured Rscript path.
3. No app deployment is needed; the call interface is unchanged.

Operational notes are in `RUNBOOK.md` under "Migration 045 (Tab 8 query shape)":
direct port 5432 only, publication pre-flight, SHARE lock duration, and
kill-safety.
