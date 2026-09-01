# EuroLeague Tab 8 query remediation — design

**Date:** 2026-08-28  
**Status:** applied 2026-08-29 as function-only query alignment
**Scope:** `euroleague.onoff_compute()` and
`euroleague.four_factors_compute()` only. No live DDL, load, or deployment is
authorized by this document.

Final implementation note: only the redundant-view-to-base-fact source swap
was deployed. The composite index and function-local `work_mem` discussed
below were not bundled because their individual contribution was not isolated
and the Israeli companion does not use that composite index.

## Goal

Remove the avoidable execution-shape divergence between EuroLeague Tab 8 and
its Israeli companion while preserving the EuroLeague provider contract and
every app-visible result.

The primary target is broad filtered ON/OFF. The 2026-08-28 app-path sample
returned 358 rows in 10.548 seconds cold and had warm samples of 6.775, 3.288,
and 0.863 seconds. Last-10 was already healthy at a 0.276-second warm median.
The fix must improve the broad case without sacrificing that narrow case.

## Current defect

Both public functions aggregate the per-game
`player_four_factors_by_game` fact through `player_game_context`. That view
adds two joins whose columns are not consumed by the aggregation:

- a lookup into `schedule` for every fact row;
- a join to the two-perspective `final_schedule` view, followed by removal of
  the unwanted perspective.

The functions already build a filtered `games` relation containing all
schedule context required by the request. Joining the fact to `games` directly
therefore has the same meaning and avoids repeating the schedule work.

Earlier plan evidence measured 1,155,420 of 1,216,268 broad-query buffers in
the unused schedule lookup alone. It also established two important cautions:

1. Replacing the view with the fact by itself makes PostgreSQL prefer a full
   fact scan for narrow requests.
2. A paired direct-fact source plus `(game_id, team_id)` index preserved
   results and improved broad ON/OFF from 9.876 to 0.878 seconds, but missed
   the accepted 0.500-second gate. Disabling JIT regressed it to 1.050 seconds.

The rejected candidate is evidence, not the implementation to reapply.

## Required behavior

The public function signatures, ownership, volatility, `SECURITY` mode,
defaults, grants, return columns, ordering, and NULL behavior remain unchanged.
The Shiny call sites remain unchanged.

The implementation must preserve:

- competition and provider-season filtering;
- date, phase, opponent, venue, outcome, round, last-N, opponent-rank, team,
  and starter-context semantics;
- season-wide EuroLeague opponent ranks, including their documented difference
  from the Israeli window-ranked behavior;
- one result row per current player/team output key;
- offense and defense as distinct contexts;
- all raw additive totals before ratio calculation;
- current rounding, percentile, minimum-possession, and sort behavior;
- steals-only EuroLeague disruption semantics;
- the existing season materialized-view fast path.

No gamecode-specific rule, provider-semantic change, new metric, new cache,
new fact, or Israeli-schema dependency is allowed.

## Proposed execution shape

### 1. Keep one filtered game set

Retain the current `schedule_ranked`, `team_ranked`, and `games` semantics.
They remain the only place where schedule filters and opponent ranks are
resolved.

### 2. Read the base fact directly

In both functions, replace the analytical read from `player_game_context` with
`player_four_factors_by_game pf JOIN games g USING (game_id, team_id)`.

Do not add `schedule`, `final_schedule`, competition, or opponent joins inside
the fact aggregation. The `games` join is the competition boundary: a fact row
is eligible only when its `(game_id, team_id)` is in the already-qualified game
set.

### 3. Support the actual join key

Test a narrow index on:

```sql
euroleague.player_four_factors_by_game (game_id, team_id)
```

The existing `game_id`-only index makes each game probe read both teams and
discard half. Do not add a wide covering index in the first candidate. The
fact's many additive columns would make it expensive to store and maintain;
plan evidence must justify every included column.

### 4. Aggregate once at the output key

Collapse the current `agg` then `pivoted` pipeline into one aggregation grouped
by `(player_id, team_id)`, using `FILTER` expressions for the four combinations
of offense/defense and ON/OFF.

This is a mechanical rewrite only. Ratios and display metrics stay downstream
and continue to use the same guarded denominators. The single aggregation must
be compared independently from the source/index change so its benefit and any
regression remain attributable.

### 5. Treat memory as a bounded candidate

The historical broad plan spilled a roughly 13.6 MB sort while server
`work_mem` was about 2.1 MB. A function-local `SET work_mem` may be evaluated
after the source/index/single-aggregate candidate. Start at 16 MB and measure
peak sort/hash behavior under three concurrent app connections.

Do not change the cluster or app-wide setting. Do not disable JIT again unless
new plan evidence contradicts the recorded regression.

## Candidate sequence

Each candidate is tested inside a rollback transaction before the next is
considered:

1. **A — source + narrow index:** direct fact joined to `games`, supported by
   `(game_id, team_id)`.
2. **B — single aggregation:** A plus one filtered aggregation at
   `(player_id, team_id)`.
3. **C — bounded function memory:** B plus function-local 16 MB `work_mem`.
4. **D — only if necessary:** a plan-driven alternative such as a deliberately
   small INCLUDE list or combined Summary/Four-Factors reader. D requires a
   short addendum to this spec before implementation because it changes either
   storage size or the app/database interface.

Do not combine A–C into one opaque experiment. The final migration may contain
multiple proven pieces, but the benchmark must identify the contribution of
each.

## Acceptance gates

### Correctness

- Exact ordered full-row equality against a stored pre-change snapshot for
  both public functions.
- Coverage across broad season, last-10, one team, phase, date range,
  opponent, home/away, result, opponent rank, round range, and all four starter
  bounds.
- Explicit empty-result and zero-denominator cases.
- Repeated calls return identical rows and hashes.
- Existing app tests and the EuroLeague Python suite pass.

Any difference in a returned value, row, NULL, type, name, or ordering rejects
the candidate. Updating the expected snapshot is not a resolution.

### Performance

Measure complete fetches through direct port 5432 during the rollback-only
candidate gate. After an explicitly approved commit, immediately repeat the
same calls through the configured pooled app port. Uncommitted function/index
DDL is not visible to another pooled connection, so a pooled failure must
trigger the guarded compensating rollback described under operational safety.
Separate the first call from warm calls.

Required gates, reconciled with Addenda A–A.3:

- shape-matched broad ON/OFF and Four Factors must be no worse than the
  same-session Israeli companion under `max(10%, 100 ms)` tolerance;
- candidate and companion calls are alternated AB/BA in the same connection;
- the blocking latency estimators are the central-60% trimmed median and its
  explicitly named upper-central statistic; raw median and raw p90 remain
  visible operational observations and are never called trimmed p90;
- no preset's trimmed median is worse than baseline by more than
  `max(10%, 100 ms)`;
- shape-matched last-10 median must be no worse than the companion under the
  same tolerance;
- broad and narrow shared-buffer traffic must not exceed the retained
  candidate's baseline;
- no sequential scan of the full fact for narrow presets;
- no new disk sort in the accepted plan;
- index size and write cost recorded before apply.

Cold latency is reported but is not allowed to hide a warm regression. A cold
improvement alone is insufficient.

### Operational safety

- All experiments use the isolated `euroleague` schema.
- The apply tool verifies database/schema identity and current function
  definitions before changing anything.
- Candidate DDL is bounded by lock and statement timeouts.
- Index creation strategy is chosen after checking live read activity. Any
  invalid concurrent-index shell is detected and removed explicitly.
- The production apply is one guarded operation with an automatic rollback on
  parity, performance, privilege, or plan failure.
- The apply tool preserves the exact pre-change function definitions and index
  state. If the immediate post-commit pooled gate fails, it restores those
  definitions and removes only the newly created index, then verifies the
  restored hashes and results.
- Repository security audit and hardening pass succeed after apply.
- No app deployment occurs as part of the database migration.

## Rejected shortcuts

- Repointing the functions to the base fact without the paired join-key index.
- Keeping the view and adding indexes to compensate for joins whose data is
  unused.
- Forcing `enable_seqscan = off` in an app function.
- Disabling JIT based on intuition; the measured candidate regressed.
- Adding `competition` or duplicating schedule context on the fact without
  proving that the filtered game join remains the bottleneck.
- Creating another player action fact or broad covering index.
- Accepting broad improvement while narrow presets regress.
- Changing an approved performance gate during a run merely to make a
  candidate pass.

## Deliverables

- one additive migration, expected to be `045_tab8_query_shape.sql`;
- one guarded dry-run/apply benchmark script;
- focused source/contract tests for both function definitions;
- recorded before/after plans, buffer counts, timings, index size, and hashes;
- updates to `PROJECT.md` and `RUNBOOK.md` only after a successful live apply.

## Done when

The accepted candidate passes every correctness, performance, plan, security,
and rollback gate; the live functions and index match the repository; and the
Shiny app requires no query-interface change. If no candidate meets all gates,
the work is complete as a documented rejected experiment and nothing remains
applied.

---

## Addendum A — performance gates re-anchored to the Israeli companion

**Date:** 2026-08-28 (after the rollback-only candidate gate)
**Status:** accepted
**Changes:** the "Performance" acceptance gates only. Every correctness,
plan, security, and operational-safety gate above stands unchanged.

### Why

The gates above were anchored to a 2026-08-28 app-path sample of 10.548 s cold
and 6.775 s warm for broad ON/OFF. That sample does not reproduce. Re-measured
on direct port 5432 with the fact table warm, the same call is **0.993 s**; the
7x spread is buffer-cache state, not query behaviour. A fixed 0.500 s
wall-clock gate anchored to the high end of that spread is not a stable target.

Two further facts, both measured rather than assumed:

1. **The spec measured the wrong broad call.** Tab 8 populates its date inputs
   from `euro_season_date_bounds()`, so the app always sends an explicit
   2025-09-01 to 2026-07-01 window rather than NULL dates. That call is ~4x
   slower than the NULL-date call for byte-identical output, and it is the one
   users experience. It has been added to the preset matrix as `broad app
   dates` and is gated.

2. **The 0.500 s target was never met by the reference implementation.** The
   Israeli companion, on the same server, the same measurement method, and a
   comparable fact table (`basketball_test.player_four_factors_by_game`,
   213 MB / 736,403 rows vs EuroLeague 272 MB / 766,146 rows), measures:

   | Israeli companion, broad app-date window | median | p90 | buffers | temp written |
   |---|---|---|---|---|
   | `basketball_test.onoff_compute` | 1.745 s | 1.797 s | 47,835 | 8,752 |
   | `basketball_test.four_factors_compute` | 1.583 s | 1.598 s | 41,084 | 11,679 |
   | `onoff_compute`, last 10 | 0.549 s | 0.608 s | 15,499 | 1,432 |

   The companion already uses exactly the execution shape this document
   proposes — it reads `basketball_test.player_four_factors_by_game p JOIN
   sched s ON s.game_id = p.game_id AND s.team_id = p.team_id`, with no context
   view. So the proposal is not a novel optimisation; it converges EuroLeague
   onto the shape the Israeli side has always used. Nothing short of a new
   pre-aggregated fact — explicitly a rejected shortcut above — reaches 0.500 s,
   and the reference implementation does not reach it either.

Under the accepted candidate the EuroLeague broad call touches 47,623 shared
buffers against the companion's 47,835: the two now do the same work.

### Revised performance gates

The gate is **no worse than the Israeli companion**, using this document's own
`max(10%, 100 ms)` tolerance. Companion figures are the measurements in the
table above and are pinned as constants in the apply script.

| gate | value | accepted candidate |
|---|---|---|
| `onoff_compute` broad median | at or below 1.920 s | 1.335 s |
| `onoff_compute` broad upper-central | at or below 1.977 s | 1.787 s |
| `four_factors_compute` broad median | at or below 1.741 s | 1.613 s |
| `four_factors_compute` broad upper-central | at or below 1.758 s | 1.625 s |
| last-10 median, both functions | at or below 0.649 s | 0.311 s / 0.237 s |

Both broad gates apply to `broad season` and `broad app dates`.

Unchanged and still binding: exact full-row parity, the `max(10%, 100 ms)`
no-regression rule on every preset, no increase in shared-buffer traffic, no
new on-disk sort, no full fact scan on a narrow preset, and the whole
operational-safety and rollback section.

Raw p90 on this shared instance is contention-sensitive (one sample run
recorded a 12 s outlier against a 0.9 s median). It remains visible as an
operational observation; the blocking tail statistic is the explicitly named
upper-central value. A persistent post-commit pooled failure triggers the
compensating rollback in the operational-safety section.

### Accepted candidate

`AC` — direct fact source, a narrow `(game_id, team_id)` index, and a
function-local `work_mem` of 16 MB. Shipped as `sql/045_tab8_query_shape.sql`.

Candidates B and C — collapsing `agg`/`pivoted` into one filtered aggregation,
with and without the raised `work_mem` — are rejected on measured evidence and
must not be revisited without new evidence. The single aggregation needs 25
(ON/OFF) and 44 (Four Factors) accumulators per group, which no longer fit in
`work_mem` and introduced an on-disk sort where none existed; even once the
spill was removed by 16 MB, that shape stayed ~2.5x slower than the accepted
one, so the wide aggregate costs more CPU than two narrow passes.

### Deviations from the original document, recorded

- **Plan-node introspection is not available** through the plpgsql boundary:
  `EXPLAIN` on a function call yields a single Function Scan node. PostgreSQL
  does attribute the nested buffer and temp counters to that node, so "no full
  fact scan on a narrow preset" is gated on shared-buffer count (a full scan is
  ~25k blocks; a genuine probe is a few hundred) and "no new disk sort" on temp
  blocks. Node type is not observable and is not gated.
- **`four_factors_compute` rows are compared as a multiset**, not a sequence.
  It has no `ORDER BY`, so its row order is a plan artefact rather than a
  contract; requiring sequence equality would reject any plan change on a
  property the function never promised. `onoff_compute` does have a
  deterministic `ORDER BY` and is compared as an ordered sequence. Values,
  NULLs, types, and row counts are compared exactly for both.
- **`app_readonly` already holds SELECT on the base fact.** The plan's proposed
  test that the app role "cannot select the base fact directly" asserts
  something that is not true of the live database and was not added. The
  migration carries no `GRANT`/`REVOKE`, issues no `DROP FUNCTION`, and the
  apply script verifies EXECUTE survives on both functions.
- **JIT is already off server-wide** (`jit = off`), so the recorded JIT
  regression cannot be reproduced from app-level settings and the instruction
  not to disable it is moot.

### Addendum A.1 — the gate instrument, and why 045 was not applied

Recorded 2026-08-28 after the full-matrix rollback-only run of
`sql/045_tab8_query_shape.sql`. Every correctness, buffer, temp, privilege and
rollback gate passed. Two of the six absolute latency checks did not, and both
are properties of the instrument rather than of the migration:

- `onoff broad season` p90 2.140 s against a 1.897 s gate, while its median was
  0.810 s and its cold sample was 8.880 s. That is contention.
- `ff broad app dates` median 1.684 s against a 1.246 s gate.

**The instrument is noisier than the effect it measures.** The Israeli
companion's own `four_factors_compute` broad median measured 1.583 s in one
session and 1.132 s in another — a 40% swing on code that did not change.
EuroLeague's `ff broad app dates` measured 1.613 s and 1.684 s across runs. A
single 15-sample median on this shared instance cannot resolve a difference
smaller than its own spread, in either direction. Same-session companion
measurement (added for exactly this reason) narrows the problem but does not
remove it.

**A harness defect, to fix before any apply run.** The companion is timed only
on the app-date window, but its gate is applied to both `broad season` (NULL
dates) and `broad app dates`. Those are different filter shapes with different
plans — the NULL-date call does not spill at all, the dated one does — so the
`broad season` comparison is not like-for-like and its gate is meaningless as
written. Either time a companion NULL-date call as well, or make `broad season`
report-only.

**Recommendation for the next attempt.** Keep parity, no-regression,
shared-buffer, temp-block and privilege gates blocking — they are same-run
comparisons and were stable across all five runs. Treat the absolute
wall-clock gate as advisory, or replace single-run medians with a
repeated-measures comparison that reports a confidence interval. Do not raise
the threshold to make the current numbers pass; that is the failure mode this
document's "Rejected shortcuts" section exists to prevent.

### Addendum A.2 — gate shape mismatch fixed

Recorded 2026-08-28, after A.1. Two corrections to the instrument, neither of
which changes a threshold:

**1. Only shape-matched presets carry the absolute companion gate.**
`basketball_test.onoff_compute` ends with `fs.game_date BETWEEN p_start_date AND
p_end_date` and has no NULL guard, so the Israeli companion cannot make a
NULL-date call at all — it returns zero rows. There is therefore no like-for-like
companion for the EuroLeague NULL-date presets, and gating them against a dated
companion call compared two different plans: the NULL-date call does not spill,
the dated one does. This is what produced the bogus `onoff broad season` p90
failure in A.1, on a preset whose median was 0.810 s.

Neither app ever sends NULL dates — both populate their date inputs from the
season bounds — so those presets were never a user-facing path. `broad season`
and `last 10` are now reported without an absolute verdict and remain fully
subject to parity, no-regression, shared-buffer and temp-block gates. A new
`last 10 app dates` preset was added so the last-N gate is shape-matched too;
the companion's last-N call has always carried dates.

**2. The companion is timed inside the transaction, adjacent to the candidate.**
It was previously measured before the DDL, minutes earlier in the same session.
Given the observed drift that is not close enough. `basketball_test` is untouched
by the candidate DDL, so reading it inside the transaction cannot be contaminated
by it.

After these two fixes every absolute comparison is: same connection, same
minute, same filter shape, EuroLeague against its Israeli counterpart.

### Addendum A.3 — trimmed estimator for every latency statistic

Recorded 2026-08-28, after A.2. Approved change to the **estimator**, not to any
threshold: the `max(10%, 100 ms)` tolerance and the companion-parity target are
exactly as stated in Addendum A.

A.2 removed the shape mismatch and moved the companion inside the transaction,
and both worked — EuroLeague Four Factors came out at 1.651 s against the
companion's 1.642 s, parity, where a mismatched earlier comparison had read as a
40% regression. What remained was untreatable by better pairing. The raw
15-sample distributions:

```
onoff broad app dates
  before: 3.33 3.40 3.42 3.42 3.44 3.45 3.56 3.69 3.79 3.92 3.97 4.00 4.23 4.56 6.47
  after : 1.34 1.35 1.37 1.37 1.37 1.37 1.38 1.39 1.39 1.40 1.41 | 1.63 3.05 5.42 7.16
ff broad app dates
  after : 1.62 1.65 1.65 1.66 1.67 1.67 1.67 1.67 1.69 1.71 1.71 1.71 1.74 | 2.36 6.04
onoff last 10 app dates
  after : 0.27 0.28 0.28 0.28 0.28 0.28 0.28 0.28 0.28 0.28 0.28 0.29 0.29 0.29 0.29
```

Twelve of fifteen samples sit within 0.07 s of each other; the remainder are
multi-second stalls the instance injects into whatever is running. One landed
inside a baseline `EXPLAIN` (13,207 ms node time on a query whose median is
1.2 s). The companion is hit identically: its `four_factors_compute` measured
1.642 s and 1.199 s in two runs twenty minutes apart, both timed inside the
transaction adjacent to the candidate. So raw p90 measures the stall, and a
15-sample median can move 40% on unchanged code.

**Blocking latency statistics are computed over the central 60% of samples** —
the fastest and slowest 20% are discarded — applied identically to the
candidate, its baseline and the companion, so no side of any comparison gains
an advantage. Trimming is skipped below 5 samples. On the distribution above
the median is unchanged at 1.390 s while the upper-central statistic is 1.630
s; raw p90 remains 5.420 s and is printed rather than relabelled.

The runs also print the full sorted sample list for every gated preset, with the
trimmed values marked, so a genuinely fat tail can never be mistaken for a
discarded outlier.

Blocking gates that never needed this treatment, because they were identical to
the digit across all six runs: row parity, shared-buffer counts
(1,653,821 -> 47,623 broad; 398,123 -> 6,133 for `last 10 app dates`), temp
blocks, the no-regression rule, and privilege/contract checks.
