# Tab 5 Stat-Column Filter Chips — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add user-defined numeric filters on Player Stats columns via a `+ Filter` chip + popover, with active filters rendered as removable chips alongside the existing filter chip row.

**Architecture:** Pure Shiny additions to Tab 5. State held in a `reactiveVal` of filter tuples. Filtering applied in R against the already-fetched data frame inside the existing `ts_display_context()` reactive — no SQL or MV changes. Chips merged into the existing `output$ts_filter_chips` renderUI alongside `build_filter_chips()` output.

**Tech Stack:** R 4.4.2, Shiny, bslib (popover), DT, dplyr.

**Spec:** `docs/superpowers/specs/2026-04-06-tab5-stat-filter-chips-design.md`

**Note on testing:** This project has no R unit-test harness. Each task ends with explicit manual verification in the running Shiny app (`"$RSCRIPT" -e "shiny::runApp('app')"`).

---

### Task 1: Add filterable-columns constant and `.filter-chip-add` CSS

**Files:**
- Modify: `app/R/server_tab5_traditional.R` (top of file, before `function(input, output, session, shared)`)
- Modify: `app/R/global.R` (append to `shared_css` definition)

- [ ] **Step 1: Add `TS_FILTERABLE_COLS` constant**

In `app/R/server_tab5_traditional.R`, near the top of the file (above the server function), add:

```r
# Display label -> column name in result_df()
TS_FILTERABLE_COLS <- list(
  "GP"    = "gp",
  "MIN"   = "minutes",
  "Poss"  = "poss_on_floor",
  "PTS"   = "pts",
  "REB"   = "reb",
  "AST"   = "ast",
  "STL"   = "stl",
  "BLK"   = "blk",
  "TOV"   = "tov",
  "FGM"   = "fgm",
  "FGA"   = "fga",
  "FG%"   = "fg_pct",
  "3PM"   = "3pm",
  "3PA"   = "3pa",
  "3P%"   = "tp_pct",
  "FTM"   = "ftm",
  "FTA"   = "fta",
  "FT%"   = "ft_pct",
  "eFG%"  = "efg",
  "TS%"   = "ts"
)

TS_PERCENT_COLS <- c("fg_pct", "tp_pct", "ft_pct", "efg", "ts")
```

- [ ] **Step 2: Add `.filter-chip-add` CSS rule**

In `app/R/global.R`, locate the `shared_css` definition (search for `.filter-chip` to find the existing chip styles). Append this rule inside the same string, immediately after the existing `.filter-chip` rules:

```css
.filter-chip-add {
  border: 1px dashed #6e7681 !important;
  background: transparent !important;
  color: #c9d1d9 !important;
  cursor: pointer;
}
.filter-chip-add:hover {
  border-color: #e8a435 !important;
  color: #e8a435 !important;
}
.ts-stat-popover .form-control,
.ts-stat-popover .form-select {
  margin-bottom: 6px;
}
```

- [ ] **Step 3: Manual verification**

Run: `"$RSCRIPT" -e "shiny::runApp('app')"`
Open Tab 5. Confirm the app loads with no errors and existing chips render unchanged. Stop the app.

- [ ] **Step 4: Commit**

```bash
git add app/R/server_tab5_traditional.R app/R/global.R
git commit -m "feat(tab5): add filterable-columns constant and chip-add CSS"
```

---

### Task 2: Add `ts_stat_filters` reactiveVal and add/remove/reset observers

**Files:**
- Modify: `app/R/server_tab5_traditional.R` (inside the server function, near the existing `ts_ref <- reactiveValues(...)` around line 23, and near `observeEvent(input$ts_reset, ...)` around line 87)

- [ ] **Step 1: Add reactiveVal and id counter**

Right after the existing `ts_ref <- reactiveValues(teams = NULL)` line, insert:

```r
ts_stat_filters <- reactiveVal(list())
ts_stat_filter_next_id <- reactiveVal(1L)
```

- [ ] **Step 2: Add `Add` observer**

Anywhere in the server body (group it near the other `observeEvent` blocks for Tab 5), add:

```r
observeEvent(input$ts_add_stat_filter, {
  col_label <- input$ts_stat_filter_col
  op <- input$ts_stat_filter_op %||% "ge"
  raw_val <- input$ts_stat_filter_value
  if (is.null(col_label) || !nzchar(col_label)) return()
  if (is.null(raw_val) || is.na(suppressWarnings(as.numeric(raw_val)))) return()
  if (!col_label %in% names(TS_FILTERABLE_COLS)) return()
  if (!op %in% c("ge", "le")) return()

  new_id <- ts_stat_filter_next_id()
  ts_stat_filter_next_id(new_id + 1L)

  current <- ts_stat_filters()
  current[[length(current) + 1]] <- list(
    id = new_id,
    label = col_label,
    col = TS_FILTERABLE_COLS[[col_label]],
    op = op,
    value = as.numeric(raw_val)
  )
  ts_stat_filters(current)

  # Reset popover inputs
  updateNumericInput(session, "ts_stat_filter_value", value = NA)
})
```

- [ ] **Step 3: Add `Remove` observer**

```r
observeEvent(input$ts_remove_stat_filter, {
  rm_id <- suppressWarnings(as.integer(input$ts_remove_stat_filter))
  if (is.na(rm_id)) return()
  current <- ts_stat_filters()
  keep <- vapply(current, function(f) !identical(as.integer(f$id), rm_id), logical(1))
  ts_stat_filters(current[keep])
}, ignoreInit = TRUE)
```

- [ ] **Step 4: Extend reset observer**

Find the existing `observeEvent(input$ts_reset, { ... })` block (around line 87) and append, just before its closing `})`:

```r
ts_stat_filters(list())
```

- [ ] **Step 5: Manual verification**

Run the app. Open Tab 5. Confirm it still loads (no UI changes yet). In the R console where Shiny is running, no errors should appear. Stop the app.

- [ ] **Step 6: Commit**

```bash
git add app/R/server_tab5_traditional.R
git commit -m "feat(tab5): add ts_stat_filters state and add/remove/reset observers"
```

---

### Task 3: Render `+ Filter` chip + popover and active stat chips

**Files:**
- Modify: `app/R/server_tab5_traditional.R` (`output$ts_filter_chips` around line 690)

- [ ] **Step 1: Replace the `output$ts_filter_chips` block**

Find the existing block:

```r
output$ts_filter_chips <- renderUI({
  build_filter_chips("ts", input, shared$season_date_bounds, reset_btn_id = "ts_reset")
})
```

Replace it with:

```r
output$ts_filter_chips <- renderUI({
  base_chips <- build_filter_chips("ts", input, shared$season_date_bounds, reset_btn_id = "ts_reset")

  # Active stat-filter chips
  stat_chips <- lapply(ts_stat_filters(), function(f) {
    op_sym <- if (identical(f$op, "ge")) "\u2265" else "\u2264"
    val_txt <- format(f$value, big.mark = ",", trim = TRUE)
    label <- sprintf("%s %s %s", f$label, op_sym, val_txt)
    js <- sprintf(
      "Shiny.setInputValue('ts_remove_stat_filter', '%d', {priority:'event'}); return false;",
      as.integer(f$id)
    )
    tags$span(
      class = "filter-chip chip-stat",
      label, " ",
      tags$a(href = "#", onclick = js, style = "margin-left:4px;color:inherit;", "\u00d7")
    )
  })

  # + Filter button (popover trigger)
  add_btn <- bslib::popover(
    trigger = tags$span(
      class = "filter-chip filter-chip-add",
      id = "ts_stat_filter_add_btn",
      tags$i(class = "bi bi-plus"), " Filter"
    ),
    title = "Add stat filter",
    placement = "bottom",
    div(
      class = "ts-stat-popover",
      style = "min-width: 220px;",
      selectInput(
        "ts_stat_filter_col", "Column",
        choices = c("Choose..." = "", names(TS_FILTERABLE_COLS)),
        selected = "",
        width = "100%"
      ),
      radioButtons(
        "ts_stat_filter_op", "Operator",
        choices = c("\u2265" = "ge", "\u2264" = "le"),
        selected = "ge",
        inline = TRUE
      ),
      numericInput(
        "ts_stat_filter_value", "Value",
        value = NA, width = "100%"
      ),
      tags$div(
        class = "small text-muted mb-2",
        "Percent columns (FG%, 3P%, FT%, eFG%, TS%): enter as 0\u2013100."
      ),
      actionButton(
        "ts_add_stat_filter", "Add",
        class = "btn-sm btn-primary w-100"
      )
    )
  )

  tagList(base_chips, stat_chips, add_btn)
})
```

- [ ] **Step 2: Manual verification**

Run the app, go to Tab 5. Verify:
1. Existing chips (Season, Team, etc.) still render.
2. A `+ Filter` chip appears at the end of the row with dashed border, amber on hover.
3. Clicking it opens a popover with Column dropdown, ≥/≤ radio, Value numeric input, helper text, Add button.
4. The popover is readable against the dark theme. If white-on-white or unreadable, note it (Task 5 has a fallback) but proceed.
5. Pick `PTS`, leave `≥`, type `15`, click Add. The popover closes (or stays open — either is OK for now). A new chip `PTS ≥ 15 ×` appears.
6. Click the `×` on that chip. Chip disappears.
7. Add a chip then click **Reset Filters** in the sidebar. Chip disappears.

Stop the app.

- [ ] **Step 3: Commit**

```bash
git add app/R/server_tab5_traditional.R
git commit -m "feat(tab5): render stat-filter chips and + Filter popover"
```

---

### Task 4: Apply stat filters to the displayed data

**Files:**
- Modify: `app/R/server_tab5_traditional.R` (`ts_display_context()` reactive around line 490)

- [ ] **Step 1: Add helper function**

Above the server function (alongside `TS_FILTERABLE_COLS`), add:

```r
apply_ts_stat_filters <- function(df, filters) {
  if (is.null(df) || !nrow(df) || !length(filters)) return(df)
  for (f in filters) {
    col <- f$col
    if (!col %in% names(df)) next
    v <- suppressWarnings(as.numeric(df[[col]]))
    keep <- !is.na(v) & (if (identical(f$op, "ge")) v >= f$value else v <= f$value)
    df <- df[keep, , drop = FALSE]
    if (!nrow(df)) break
  }
  df
}
```

- [ ] **Step 2: Wire it into `ts_display_context()`**

Open the file at the `ts_display_context <- reactive({` block (around line 490). The reactive computes a `df` and returns a list with `df`, `mode`, etc. Locate the line where the final `df` is determined (the value placed into the returned list). Immediately before the `list(df = ..., ...)` return, insert:

```r
df <- apply_ts_stat_filters(df, ts_stat_filters())
```

If `ts_display_context` has multiple return paths, insert the same line before each. Use `Read` on lines 490–550 first to identify all return points and apply consistently.

- [ ] **Step 3: Add reactive dependency**

The `ts_display_context()` reactive must re-fire when stat filters change. If it uses `bindEvent(...)`, append `ts_stat_filters()` to the trigger list. If it uses an implicit reactive (no `bindEvent`), the call to `ts_stat_filters()` inside it already creates the dependency — no change needed.

Verify by reading the bottom of the `ts_display_context` block. If you see `}) %>% bindEvent(...)`, add `ts_stat_filters()` to the argument list.

- [ ] **Step 4: Manual verification**

Run the app. On Tab 5:
1. Note total visible rows.
2. Add `PTS ≥ 15`. Verify the table shrinks to only players with PTS ≥ 15.
3. Add `MIN ≥ 20`. Verify further shrinkage (AND semantics).
4. Add `PTS ≤ 25`. Verify the high scorers drop out (range behavior on same column).
5. Remove the `PTS ≥ 15` chip. Verify rows return.
6. Add `FG% ≥ 45`. Verify it works with percent columns where the displayed value matches the input.
7. Click **Reset Filters**. Verify table returns to baseline.
8. Change the date range filter. Verify stat chips persist and the filtered set recomposes correctly.

Stop the app.

- [ ] **Step 5: Commit**

```bash
git add app/R/server_tab5_traditional.R
git commit -m "feat(tab5): apply stat filters to displayed table data"
```

---

### Task 5: Polish — popover theme + z-index + edge cases

**Files:**
- Modify: `app/R/global.R` (`shared_css`)
- Possibly modify: `app/R/server_tab5_traditional.R` if popover fallback needed

- [ ] **Step 1: Visual check**

Run the app. Open Tab 5, click `+ Filter`. Inspect:
- Is the popover background dark / readable?
- Does it sit above the DT FixedHeader when scrolled?
- Does the Add button look correct?

- [ ] **Step 2: Apply CSS fixes**

In `app/R/global.R`, append to `shared_css`:

```css
.popover {
  background-color: #161b22 !important;
  border: 1px solid #30363d !important;
  z-index: 1080 !important;
}
.popover .popover-header {
  background-color: #21262d !important;
  color: #c9d1d9 !important;
  border-bottom-color: #30363d !important;
}
.popover .popover-body {
  color: #c9d1d9 !important;
}
.popover .popover-arrow::after,
.popover .popover-arrow::before {
  border-bottom-color: #30363d !important;
}
.ts-stat-popover .form-control,
.ts-stat-popover .form-select {
  background-color: #0d1117 !important;
  color: #c9d1d9 !important;
  border-color: #30363d !important;
}
```

- [ ] **Step 3: Re-verify visually**

Run the app again. Confirm:
- Popover is dark, readable.
- Inputs inside popover are dark.
- Popover renders above the table header when the page is scrolled (add several chips, scroll the table, click `+ Filter` again — popover should not be hidden behind FixedHeader).

If the popover is still unreadable or hidden behind the table after this CSS, fall back to inline UI: replace the `bslib::popover(...)` call in Task 3 with a `conditionalPanel("input.ts_stat_filter_open % 2 == 1", ...)` driven by an `actionButton("ts_stat_filter_open", ...)` toggle. Document the fallback in the commit message.

- [ ] **Step 4: Edge-case checks**

Run the app and verify:
1. Click `+ Filter`, click Add with no column selected → no chip added, no error.
2. Click `+ Filter`, pick a column, leave value blank, click Add → no chip added, no error.
3. Add a filter that excludes every row → table shows empty state without error.
4. Switch seasons via global selector → stat chips persist and re-apply to new season's data.
5. Switch to another tab and back → stat chips still present.

- [ ] **Step 5: Commit**

```bash
git add app/R/global.R app/R/server_tab5_traditional.R
git commit -m "feat(tab5): dark-theme popover styling and z-index fix for stat filters"
```

---

## Self-Review

**Spec coverage:**
- `+ Filter` chip in chips row → Task 3 ✓
- Popover with column / op / value → Task 3 ✓
- Removable active chips → Tasks 2 + 3 ✓
- AND semantics + same-column range → Task 4 ✓
- Filtering in R, no SQL/MV changes → Task 4 ✓
- Reset Filters clears stat chips → Task 2 step 4 ✓
- Percent column 0–100 helper → Task 3 ✓
- NA exclusion → Task 4 helper ✓
- Dark-theme popover + z-index → Task 5 ✓
- `.filter-chip-add` CSS → Task 1 ✓
- `TS_FILTERABLE_COLS` single source of truth → Task 1 ✓

**Placeholder scan:** None — all code blocks are complete, all file paths exact, all manual verification steps concrete.

**Type consistency:** `ts_stat_filters` entries use fields `id, label, col, op, value` consistently across Tasks 2, 3, and 4. `op` values `"ge"`/`"le"` consistent. `TS_FILTERABLE_COLS` referenced by name in Tasks 2 and 3.
