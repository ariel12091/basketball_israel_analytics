# Tab 5 Player Stats — Stat-Column Filter Chips

**Date:** 2026-04-06
**Status:** Design approved, awaiting user spec review
**Scope:** Shiny app only (Tab 5 has no React equivalent)

## Problem

Users want to filter the Player Stats table by stat-column values (e.g. `PTS ≥ 15`, `MIN ≥ 20`, `GP ≥ 10`) without overcrowding the sidebar. The sidebar already carries date range, teams, Min GP, clutch, game filters, and opponent strength — adding numeric filters for ~20 stat columns there would be unusable.

## Solution

A `+ Filter` chip at the end of the existing filter-chip row above the DT table. Clicking it opens a small popover with three controls (column, operator, value); submitting adds a removable chip like `PTS ≥ 15 ×`. Multiple chips compose with AND. Filtering is applied in R on the already-fetched data frame — no SQL or MV changes.

## UI

### Location
Append to the existing `uiOutput("ts_filter_chips")` row in `app/R/ui_tab5_traditional.R`. Today that slot renders any active high-level chips; we add:

- Zero or more **stat-filter chips** (one per active filter), and
- A trailing **`+ Filter`** action chip (always present).

When no stat filters are active, the row looks identical to today plus a single `+ Filter` button.

### `+ Filter` chip
- Styled as `.filter-chip.filter-chip-add` (dashed border, amber on hover) — new CSS class added to the existing chips stylesheet.
- Triggers a `bslib::popover()` anchored to the chip.

### Popover contents
1. **Column** — `selectInput` with every numeric stat column from the rendered table:
   `GP, MIN, Poss, PTS, REB, AST, STL, BLK, TOV, FGM, FGA, FG%, 3PM, 3PA, 3P%, FTM, FTA, FT%, eFG%, TS%`
   (Single source of truth: a constant list `TS_FILTERABLE_COLS` in `server_tab5_traditional.R`, mapping display label → internal column name.)
2. **Operator** — two-button toggle: `≥` / `≤`. Default `≥`.
3. **Value** — `numericInput`, no min/max bounds. Placeholder hints percent columns expect 0–100 (e.g. `45` for 45%).
4. **Add** — `actionButton` that closes the popover and adds the chip.

### Active stat-filter chips
- Render as `<column> <op> <value> ×` (e.g. `PTS ≥ 15 ×`).
- Match existing `.filter-chip` styling. The `×` is an `<a>` with an inline `onclick` that calls `Shiny.setInputValue('ts_remove_stat_filter', <id>, {priority:'event'})`, mirroring the Tab 2 lineup-modal click pattern.

## State

Server-side, in `server_tab5_traditional.R`:

```r
ts_stat_filters <- reactiveVal(list())  # list of list(id, col, op, value)
```

Each entry has a stable `id` (incrementing integer) so the `×` removal lookup is unambiguous even with duplicates.

### Events
- **Add** (`input$ts_add_stat_filter`): append `list(id = next_id(), col, op, value)` and clear popover inputs.
- **Remove** (`input$ts_remove_stat_filter`): drop entry whose `id` matches.
- **Reset** (`input$ts_reset`): clear `ts_stat_filters()` along with the existing reset behavior.

## Filtering Logic

Applied in R on the already-fetched `data.frame` immediately before `datatable()` render — i.e. inside the existing reactive that builds the table data, after all current filters (Min GP, clutch, etc.). No SQL function changes. No MV changes.

```r
apply_stat_filters <- function(df, filters) {
  for (f in filters) {
    col <- f$col
    if (!col %in% names(df)) next
    v <- df[[col]]
    df <- df[!is.na(v) & (if (f$op == "ge") v >= f$value else v <= f$value), , drop = FALSE]
  }
  df
}
```

Semantics: AND across all chips. Multiple chips on the same column also AND, which lets users build a range (`PTS ≥ 10` + `PTS ≤ 20`).

NA handling: rows with NA in the filtered column are excluded. This matches user intent (`PTS ≥ 15` should not return players with no PTS recorded).

## Percent-Column Convention

Percentage columns (`FG%`, `3P%`, `FT%`, `eFG%`, `TS%`) are stored in the data frame as 0–100 numbers (matching display). Users type `45` meaning 45%. The popover shows a small helper line under the value input when a percent column is selected: *"Percent columns: enter as 0–100"*.

## Aesthetics & Theme

- Reuse existing `.filter-chip` CSS in inline `shared_css` (per the load-order constraint documented in `MEMORY.md` — no external stylesheet).
- Add `.filter-chip-add` rule for the dashed-border action chip.
- Popover styling: rely on `bslib::popover()` first. If the dark theme fights it (white background, light-mode text), fall back to a `conditionalPanel` inline expander under the chip row. Decision happens during implementation after a visual check.
- Popover `z-index` may need a bump to sit above the DT FixedHeader; verify in browser.

## Files Touched

- `app/R/ui_tab5_traditional.R` — no structural changes; the existing `uiOutput("ts_filter_chips")` slot is reused.
- `app/R/server_tab5_traditional.R` —
  - `TS_FILTERABLE_COLS` constant
  - `ts_stat_filters` reactiveVal + add/remove/reset observers
  - Popover UI generated inside `output$ts_filter_chips`
  - `apply_stat_filters()` helper called in the table-data reactive
- `app/R/global.R` — `.filter-chip-add` CSS rule appended to `shared_css`.

## Out of Scope

- Range sliders, header-cell filter icons, saved filter presets.
- OR logic across chips.
- React frontend (Tab 5 is Shiny-only per `CLAUDE.md`).
- SQL/MV changes.
- Filter persistence across sessions.

## Risks

1. **Popover vs FixedHeader z-index** — verify visually; bump z-index if needed.
2. **Dark-theme popover styling** — `bslib::popover` may render in light mode; fallback plan documented above.
3. **Interaction with auto Min GP** — Tab 5's Min GP slider already filters GP. A user-added `GP ≥ N` chip will compose with it (AND). Acceptable; document briefly in the explainer if confusion arises.
4. **Percent input ambiguity** — mitigated by the helper text. No automatic 0–1 vs 0–100 detection.

## Testing

Manual checklist:
- Add a single `PTS ≥ 15` filter; verify rows drop correctly.
- Add a second filter on a different column; verify AND.
- Add two filters on the same column (`PTS ≥ 10`, `PTS ≤ 20`); verify range behavior.
- Remove one chip via `×`; verify only that filter clears.
- Click **Reset Filters**; verify all stat chips clear.
- Filter on a percent column with value `45`; verify it matches displayed `45.0`.
- Filter on a column then change date range / clutch / teams; verify chips persist and recompose.
- Verify popover renders above FixedHeader and matches dark theme.
