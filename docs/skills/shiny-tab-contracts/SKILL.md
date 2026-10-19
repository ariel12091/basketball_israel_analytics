---
name: shiny-tab-contracts
description: Enforce cross-tab contract consistency in this Shiny app (filters, reset semantics, Summary/Four Factors parity, and DT wiring) without changing metric definitions.
---

# Shiny Tab Contracts

Use this skill when adding or changing UI/server logic in `app/R/ui_tab*.R` and `app/R/server_tab*.R`.

## Safety Rules

- Never include credentials, hostnames, tokens, passwords, or secret values in code, tests, logs, or docs.
- Use environment variables only by name (for example: `PG_HOST`, `PG_PORT`, `PG_DB`, `PG_USER`, `PG_PASS`, `PG_SSLMODE`).
- Keep `.Renviron` values out of commits and examples.

## Goal

Keep behavior consistent across tabs when filters or reset logic changes.

## Contract Checklist

1. Reset semantics
- For `selectizeInput(..., multiple = TRUE)`, clear with `character(0)`.
- Avoid `selected = ""` for multi-select resets.
- Tests should validate post-reset values explicitly.

2. Summary vs Four Factors parity
- A filter available in one mode should either exist in the other mode or be intentionally documented as mode-specific.
- Column naming and display labels should stay stable across tabs.

3. Reactivity wiring
- If a query uses a filter, that filter must be in the reactive trigger chain (`bindEvent` / reactive dependencies).
- Prevent stale outputs when GN/date/team/game-type/clutch filters change.

4. DataTable contract
- Keep hidden IDs/hash columns stable when click handlers depend on them.
- If sorting uses display HTML, map sort to numeric/raw columns where needed.

5. Filter chip contract
- Chip text must reflect actual filter values and units (for example, clutch duration and game type wording).
- Date ranges should render from active state, not stale defaults.

6. Default options contract
- Game type default labels must match product wording (for example, `Regular season`).
- Align defaults across tabs unless intentionally different.

## Implementation Workflow

1. Locate impacted tab files
- UI: `app/R/ui_tab*.R`
- Server: `app/R/server_tab*.R`
- Shared helpers: `app/R/global.R`

2. Apply change minimally
- Update only affected tab(s) first.
- If behavior is meant to be global, mirror change across all relevant tabs.

3. Add/adjust tests
- Unit/contract: `app/tests/testthat/test-*.R`
- E2E reset flow: `app/tests/testthat/test-e2e-tabs-shinytest2.R`
- Ensure each `test_that()` has at least one assertion.

4. Validate locally
- Parse-check modified R files.
- Run targeted tests first, then full `scripts/test_all.R` when practical.

5. CI readiness
- Avoid hard-coded local paths.
- Keep app-dir assumptions consistent (`app` directory root).

## Quick Commands (examples)

```powershell
# Parse-check touched files (adjust file list)
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' -e "parse(file='app/R/server_tab1.R'); parse(file='app/R/ui_tab1_onoff.R'); cat('OK\n')"

# Run tests
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' scripts/test_all.R
```

## Definition of Done

- Tab behavior is unchanged except for intended scope.
- Resets clear correctly and consistently.
- Filter chips match real state.
- Tests are non-empty and pass locally/CI.
- No secrets or environment values appear in diffs.
