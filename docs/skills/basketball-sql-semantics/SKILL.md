---
name: basketball-sql-semantics
description: Preserve basketball metric semantics and cross-tab consistency when editing SQL functions or materialized views. Use for clutch filters, pre-possession margin logic, type_lineup offense/defense handling, and parity checks.
---

# Basketball SQL Semantics

## Safety Rules

- Never include credentials, hostnames, tokens, or secret values.
- Refer to environment variables only by name.

## Core Invariants

1. Use pre-possession margin for clutch eligibility unless a function is explicitly documented otherwise.
2. Keep offense/defense semantics consistent with `type_lineup` definitions.
3. Keep metric formulas aligned across app-facing functions for the same mode.
4. Preserve ranking scope intent (global vs filtered) and document any deliberate change.

## High-Risk Areas

- `sql/functions/get_player_traditional_dynamic.sql`
- `sql/functions/get_team_four_factors_dynamic.sql`
- `sql/functions/fetch_lineups_all.sql`
- `sql/functions/four_factors_compute.sql`

## Change Workflow

1. Define expected behavior before editing (especially clutch and ranking scope).
2. Make minimal SQL change in one function at a time.
3. Compare output before/after on representative filters.
4. Confirm parity across tabs using the same metric definitions.
5. Add notes when behavior intentionally changes.

## Verification Checklist

- No unexpected row-count drift.
- Boundary cases (clutch margin/time) behave as expected.
- Team/player outputs stay consistent across tabs for equivalent filters.
- Query plan does not introduce obvious repeated full scans.

## Done Criteria

- Semantics preserved or intentionally documented.
- Before/after checks completed.
- No secrets exposed in SQL, scripts, or logs.
