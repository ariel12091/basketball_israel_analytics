# EuroLeague Tab 8 query remediation — implementation plan

**Final outcome (2026-08-29):** migration 045 was applied as the two
function-source swaps only. No composite index or function-local `work_mem`
was shipped; those remain historical experiments in this plan.

**Goal:** Make broad EuroLeague Tab 8 ON/OFF healthy without regressing the
already-fast narrow filters or changing any result.

**Spec:** `docs/specs/2026-08-28-tab8-query-remediation-design.md`

**Authorization:** This plan authorizes repository work and read-only
diagnostics only. Creating an index, replacing a live function, or deploying
the app still requires explicit approval.

## Files

| File | Planned responsibility |
|---|---|
| `sql/045_tab8_query_shape.sql` | Additive function/index candidate after it passes rollback testing. |
| `scripts/apply_045_tab8_query_shape.py` | Snapshot, A/B candidates, parity, plan, timing, privilege, apply/rollback. |
| `tests/test_tab8_query_shape.py` | Static migration/apply-script safety and contract checks. |
| `PROJECT.md`, `RUNBOOK.md` | Record results only after apply; record rejection if nothing ships. |

Do not edit `sql/004_app_read_layer.sql`; it is historical migration evidence.
Migration 045 must carry complete replacement definitions for the two public
functions.

## Task 1 — Freeze the baseline and preset matrix

- [ ] Add a read-only `--baseline` mode to
  `scripts/apply_045_tab8_query_shape.py`.
- [ ] Fetch `pg_get_functiondef()` for both exact public signatures and save
  their SHA-256 hashes in the run output.
- [ ] Record relation/index sizes, index validity/readiness, PostgreSQL
  version, `work_mem`, JIT state, and privileges.
- [ ] Define one canonical parameter builder used by every baseline and
  candidate call.
- [ ] Define at least these presets for both functions where applicable:

  1. broad full season;
  2. last 10;
  3. one team;
  4. one phase;
  5. bounded dates;
  6. one opponent;
  7. home and away;
  8. win and loss;
  9. top/bottom opponent rank for off/def/net;
  10. round range;
  11. each starter minimum/maximum independently;
  12. combined team + last-N + starter bounds;
  13. a filter returning no rows.

- [ ] Store every baseline as ordered `row_to_json()` text in memory before
  candidate DDL. Never compare two post-change executions.
- [ ] For broad, last-10, and one-team presets capture
  `EXPLAIN (ANALYZE, BUFFERS, WAL, SETTINGS, FORMAT JSON)` and complete-fetch
  timings: one cold observation followed by 15 warm samples.
- [ ] Run the candidate baseline through direct port 5432. Separately record
  the current live app call through the configured pooled port; the pooled
  connection cannot see later uncommitted candidate DDL.

**Gate:** The baseline script must fail if a preset is missing, rows are
unstable, a call exceeds 30 seconds, or either function definition differs
from the expected current hash supplied to the script.

## Task 2 — Build a transaction-only candidate harness

- [ ] Parse candidate SQL with the repository SQL splitter.
- [ ] Reject executable references to `basketball` or `basketball_test`.
- [ ] Reject destructive table operations, schema creation, function signature
  changes, ownership changes, and unbounded DDL.
- [ ] Verify the target is the configured PostgreSQL database and isolated
  `euroleague` schema.
- [ ] Start a transaction and set local lock/statement timeouts.
- [ ] Apply candidate functions and a temporary candidate index inside the
  transaction.
- [ ] Run parity, plan, privilege, and timing gates, then roll back by default.
- [ ] Add `--apply` as the only commit path; it must run the same gates and
  commit only after all pass.
- [ ] On every exit, verify whether an invalid candidate index shell exists and
  report it explicitly.

**Gate:** A deliberate one-row mutation of an in-memory baseline must make the
parity gate fail. A deliberate slow threshold must make the performance gate
fail and leave function/index definitions unchanged after rollback.

## Task 3 — Candidate A: direct fact plus join-key index

- [ ] Add the candidate index:

  ```sql
  CREATE INDEX ... ON euroleague.player_four_factors_by_game (game_id, team_id)
  ```

  Use the final repository name only after confirming it does not collide with
  a live or invalid index.

- [ ] In both function bodies, change only the aggregation source:

  ```sql
  FROM euroleague.player_four_factors_by_game pf
  JOIN games g ON g.game_id = pf.game_id AND g.team_id = pf.team_id
  ```

- [ ] Keep every filter, grouping key, ratio, returned column, and ordering
  otherwise byte-equivalent to the current definitions.
- [ ] Run full-row parity for the entire preset matrix.
- [ ] Compare broad and narrow JSON plans. Confirm the view joins disappear
  and narrow requests use parameterized `(game_id, team_id)` probes rather
  than a full sequential scan.
- [ ] Record index size, build time, shared buffers, temp I/O, planning time,
  execution time, and complete-fetch latency.

**Decision:** Retain A in the transaction only if every parity check passes and
no preset breaches the 10%/100 ms no-regression rule. The shape-matched broad
and last-10 calls must also be no worse than the same-session Israeli companion
under the same tolerance.

## Task 4 — Candidate B: aggregate once

- [ ] Starting from A, replace the two-stage `agg`/`pivoted` shape with one
  `GROUP BY player_id, team_id` and filtered sums for offense/defense × ON/OFF.
- [ ] Preserve numeric casts before division and all `NULLIF` denominator
  guards exactly.
- [ ] For ON/OFF, verify points, possessions, minutes, and 2PT/3PT additive
  columns individually before comparing final rows.
- [ ] For Four Factors, verify points, possessions, TS possessions, rebounds,
  opportunities, turnovers, steals, deflections, FTA/FGA/FGM/3PM before final
  rate parity.
- [ ] Repeat the complete preset, plan, and latency gates.

**Decision:** Keep B only if it improves broad median or buffer/temp behavior
and does not regress A on any preset. Otherwise return to A.

## Task 5 — Candidate C: bounded function-local memory

- [ ] From the best of A/B, add function-local `SET work_mem = '16MB'`.
- [ ] Verify the broad sort/hash no longer spills and capture peak memory from
  the plan where PostgreSQL reports it.
- [ ] Run three simultaneous broad calls, matching the app pool's default
  maximum, and confirm no timeout or material tail-latency regression.
- [ ] Repeat full parity and the 15-sample broad/narrow timing gates.
- [ ] Do not add `SET jit = off`; it already regressed the paired candidate.

**Decision:** Keep C only if it provides a repeatable improvement and remains
safe at three concurrent calls. Otherwise remove the setting.

## Task 6 — Decide whether a migration exists

- [ ] Choose the smallest candidate that passes all spec gates.
- [ ] Require zero row differences and companion-relative gates from Addenda
  A–A.3: alternate candidate/companion calls AB/BA, gate on the central-60%
  trimmed median and named upper-central statistic, and report raw p90 without
  mislabelling it as a trimmed p90.
- [ ] Require no narrow full-fact sequential scan, no new disk sort, and no
  preset regression beyond 10%/100 ms.
- [ ] If none passes, roll back everything and write a rejection section in
  `PROJECT.md` containing candidate plans/timings. Stop; do not lower gates or
  invent a new fact.
- [ ] If one passes, write `sql/045_tab8_query_shape.sql` with only the proven
  index/function definitions and transactional comments explaining the query
  shape.
- [ ] Add expected pre-apply function hashes and index state to the apply
  script so it refuses a stale target.

## Task 7 — Repository tests before requesting live approval

- [ ] Add `tests/test_tab8_query_shape.py` covering:

  - EuroLeague marker and absence of Israeli schema references;
  - exact public signatures and unchanged return contracts;
  - both functions read `player_four_factors_by_game` directly;
  - neither function reads `player_game_context` in its analytical body;
  - the join includes both `game_id` and `team_id`;
  - candidate index uses exactly `(game_id, team_id)` unless the accepted plan
    justifies more;
  - app role remains EXECUTE-only on the functions and cannot select the base
    fact directly;
  - the apply script defaults to rollback.

- [ ] Run the required Python suite:

  ```powershell
  & euroleague/.venv/Scripts/python.exe -m unittest discover `
    -s euroleague/tests -v
  ```

- [ ] Run relevant app tests for EuroLeague ON/OFF routing, display contracts,
  auto minimums, and database security contracts using the configured Rscript
  path.
- [ ] Parse the proposed SQL and affected R files.

**Gate:** All tests pass locally. No live database call is needed for this task.

## Task 8 — Approval checkpoint and live rollback test

- [ ] Present the read-only baseline, proposed candidate DDL, expected index
  size/build cost, lock strategy, timeout bounds, and rollback procedure to the
  user.
- [ ] Request explicit approval for the rollback-only live DDL experiment.
  Do not treat approval for this test as approval to commit the migration.
- [ ] After approval, check that no EuroLeague publication is active and run
  `scripts/apply_045_tab8_query_shape.py` without `--apply` against direct port
  5432. It must finish with a rollback and prove definitions and index state
  are unchanged.
- [ ] Present the exact candidate, parity count, broad/narrow trimmed medians,
  upper-central statistics, raw p90s, plans/buffers, index size/build time, and
  rollback proof to the user.
- [ ] Request separate explicit approval for committed live DDL. Do not combine
  either approval with app deployment or a EuroLeague data load.

## Task 9 — Apply only after explicit approval

- [ ] Check for active EuroLeague publication and long-running readers.
- [ ] Resolve the exact index target and verify there is no invalid shell.
- [ ] Run the guarded apply tool with `--apply`.
- [ ] Re-read both function definitions, index validity/readiness, privileges,
  and result hashes after commit.
- [ ] Immediately run the same complete-fetch gate through the configured
  pooled port. If it fails, use the apply tool's preserved pre-change function
  definitions to run a compensating rollback, remove only migration 045's new
  index, and verify the restored hashes/results/index state.
- [ ] Run the repository database security audit and hardening pass required by
  `PROJECT.md`, then rerun the audit.
- [ ] Re-run direct and pooled broad/narrow measurements without changing the
  accepted thresholds.
- [ ] Do not deploy the Shiny app; its call interface is unchanged.

## Task 10 — Documentation and handoff

- [ ] Update `PROJECT.md` with migration status, exact measurements, result
  hashes/parity counts, index size, and rejected candidate evidence.
- [ ] Update `RUNBOOK.md` migration order and maintenance notes.
- [ ] State explicitly that future per-game publication needs no special
  backfill: the index is maintained normally and function results read the
  existing fact.
- [ ] Record whether `player_game_context` remains needed by season views or
  other consumers; do not drop it in migration 045.
- [ ] Record that no app deployment or data load occurred.

## Completion criteria

- Both public functions are output-identical across the full preset matrix.
- Shape-matched broad ON/OFF and Four Factors meet the companion-relative
  median and upper-central gates.
- Last-10 and every other narrow class stay within their regression limits.
- Plans contain no redundant `player_game_context` schedule joins and no
  narrow full-fact scan.
- Accepted DDL is additive, bounded, isolated to `euroleague`, and security
  audited.
- Repository files match the live definitions, or—if no candidate passes—the
  database is unchanged and the rejection evidence is documented.

## Explicitly out of scope

- Combining the Summary and Four Factors public interfaces.
- Changing Tab 8 Shiny routing or rendering.
- Modifying the Israeli companion.
- Adding competition to the per-game fact.
- Creating a new fact/cache, backfilling data, or changing publication logic.
- Lineups, Team readers, Player Stats, Game Logs, cold-start hosting policy, or
  app deployment.
