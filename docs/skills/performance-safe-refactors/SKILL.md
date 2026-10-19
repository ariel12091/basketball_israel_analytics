---
name: performance-safe-refactors
description: Improve performance in SQL and app hot paths without changing user-visible results. Use for expensive joins, correlated subqueries, CTE scope tuning, and before/after parity validation.
---

# Performance Safe Refactors

## Safety Rules

- Keep output contract stable unless explicitly requested.
- Do not mix behavior changes with performance changes in one patch.

## Refactor Strategy

1. Measure baseline on representative filters.
2. Change one bottleneck at a time.
3. Re-run parity checks before and after each change.
4. Keep rollback path simple.

## Common Optimizations

- Replace repeated correlated window calculations with pre-ranked CTEs.
- Scope heavy CTEs to filtered action/game sets early.
- Avoid unnecessary global scans on hot requests.

## Verification

- Row-level parity for sampled filters.
- Aggregate parity for key metrics.
- Runtime improvement confirmed with same query shape.

## Done Criteria

- Metrics and ranking semantics unchanged.
- Measurable performance gain recorded.
- Tests/parity scripts updated where needed.
