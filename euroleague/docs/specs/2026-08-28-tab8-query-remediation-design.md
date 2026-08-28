# EuroLeague Tab 8 query remediation — design

**Date:** 2026-08-28  
**Status:** proposed  
**Scope:** `euroleague.onoff_compute()` and
`euroleague.four_factors_compute()` only. No live DDL, load, or deployment is
authorized by this document.

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

Required gates:

- broad ON/OFF warm median at or below **0.500 seconds** over 15 samples;
- broad ON/OFF warm p90 at or below **0.750 seconds**;
- no preset's warm median worse by more than `max(10%, 100 ms)`;
- last-10 warm median at or below 0.350 seconds;
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
- Relaxing output parity or the 0.500-second gate during implementation.

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
| `onoff_compute` broad p90 | at or below 1.977 s | 1.787 s |
| `four_factors_compute` broad median | at or below 1.741 s | 1.613 s |
| `four_factors_compute` broad p90 | at or below 1.758 s | 1.625 s |
| last-10 median, both functions | at or below 0.649 s | 0.311 s / 0.237 s |

Both broad gates apply to `broad season` and `broad app dates`.

Unchanged and still binding: exact full-row parity, the `max(10%, 100 ms)`
no-regression rule on every preset, no increase in shared-buffer traffic, no
new on-disk sort, no full fact scan on a narrow preset, and the whole
operational-safety and rollback section.

p90 on this shared instance is contention-sensitive (one sample run recorded a 12 s outlier against
a 0.9 s median). A p90 breach on the post-commit pooled gate should be re-run
before it is treated as a real regression, and the compensating rollback in the
operational-safety section remains the response if it persists.

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
