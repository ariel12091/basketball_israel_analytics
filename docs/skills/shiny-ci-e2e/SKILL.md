---
name: shiny-ci-e2e
description: Stabilize Shiny tests and CI workflows. Use for shinytest2 flakiness, reset-flow validation, app-dir wiring, dependency setup, and GitHub Actions R test reliability.
---

# Shiny CI E2E

## Safety Rules

- Never print or commit secrets.
- Keep CI configs free of hard-coded credentials.

## Test Stability Rules

1. Trigger UI actions via real clicks when behavior is button-driven.
2. Use explicit assertions inside every `test_that()` block.
3. Prefer polling for target state over generic idle waits.
4. Use deterministic selectors and avoid brittle timing assumptions.

## CI Wiring Checks

- App directory points to `app`.
- Required R packages installed for the selected test scope.
- Optional suites are gated by env flags (`RUN_E2E`, `RUN_DB_TESTS`) intentionally.
- Full/nightly path is manually runnable via `workflow_dispatch`.

## Debug Workflow

1. Identify first failing test and exact input/state that stalls.
2. Check if test is emulating behavior incorrectly (set value vs click).
3. Confirm reset semantics in server code (for multi-select use `character(0)`).
4. Increase timeout only after logic and selector correctness are proven.

## Done Criteria

- No empty tests.
- Failures reproducible locally or explained by CI-only environment differences.
- Nightly/full path runs with expected skips and no accidental omissions.
