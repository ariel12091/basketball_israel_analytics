---
name: data-integrity-guards
description: Run repeatable data-integrity validation for ETL, SQL functions, and materialized views. Use before and after schema/logic changes, ETL runs, and deployments to prevent silent data regressions.
---

# Data Integrity Guards

## Safety Rules

- Never include credentials, hostnames, passwords, or tokens.
- Reference environment variables only by name.
- Avoid printing sensitive values in logs.

## When To Run

1. Before and after ETL changes.
2. Before and after SQL function or MV changes.
3. Before deployment.
4. After failed CI runs that indicate data drift.

## Integrity Workflow

1. Baseline snapshot
- Capture key row counts for core tables/MVs.
- Capture representative metric totals for current season and sampled filters.

2. Structural checks
- Null checks on required keys.
- Duplicate checks on expected-unique keys.
- Orphan checks for expected parent-child links.

3. Metric consistency checks
- Possession/minute sanity on app-facing outputs.
- Score/margin consistency where applicable.
- Offense/defense split totals reconcile to expected aggregates.

4. Contract parity checks
- Compare before/after outputs for sampled app queries.
- Check boundary cases (date ranges, clutch edges, game-type filters).

5. Refresh chain checks
- Ensure dependent MVs are refreshed in valid order.
- Confirm app-facing functions return non-empty, valid schema outputs.

## Minimum Query Set

- Row counts by season for core tables used in app tabs.
- Duplicate key scans for lookup/aggregate tables.
- Null-key scans for join-critical columns.
- Sampled tab-level output parity queries (same filters, before vs after).

## Failure Handling

1. Stop deploy if any integrity guard fails.
2. Isolate first failing check and identify owning object (table/function/MV).
3. Roll back or patch narrowly.
4. Re-run full guard set after fix.

## Done Criteria

- All structural checks pass.
- All metric consistency checks pass.
- Before/after parity within expected tolerance (exact where required).
- No secrets appear in scripts, logs, or docs.
