# Compare Tab Side Labels Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make it clear what Side A and Side B represent in the Compare tab by using short preset labels for column headers and full verbose labels for summary cards.

**Architecture:** Split existing `side_summary()` into `side_label_short()` (preset name or "A"/"B") and `side_label_full()` (verbose filter description). DT column headers use short labels with colored badges in custom mode. Summary cards and detail subheader use full labels.

**Tech Stack:** R/Shiny, DT (headerCallback JS), existing `server_tab7_compare.R`

**Spec:** `docs/superpowers/specs/2026-03-26-compare-side-labels-design.md`

---

### Task 1: Add `side_label_short()` function

**Files:**
- Modify: `app/R/server_tab7_compare.R:230` (insert new function before existing `side_summary`)

- [ ] **Step 1: Add `side_label_short()` right before `side_summary()`** (line 230)

```r
  # Short label for DT column headers and detail grid headers
  side_label_short <- function(side) {
    # Players mode: presets not applicable
    if (identical(input$cmp_mode, "Players")) return(toupper(side))

    preset <- input$cmp_preset %||% ""
    if (!nzchar(preset)) return(toupper(side))

    switch(preset,
      starters_bench = if (side == "a") "Starters" else "Bench",
      clutch         = if (side == "a") "Clutch" else "Non-Clutch",
      home_away      = if (side == "a") "Home" else "Away",
      win_loss       = if (side == "a") "Win" else "Loss",
      top_bottom_rank = {
        n <- input[[paste0("cmp_", side, "_opp_rank_n")]] %||% ""
        if (nzchar(n)) {
          if (side == "a") paste0("vs Top ", n) else paste0("vs Bottom ", n)
        } else {
          toupper(side)
        }
      },
      date_split = {
        split_date <- parse_single_date(input$cmp_split_date)
        if (!is.na(split_date)) {
          d <- format(split_date, "%b %d")
          if (side == "a") paste0("Before ", d) else paste0("From ", d)
        } else {
          toupper(side)
        }
      },
      gn_split = {
        gn <- suppressWarnings(as.integer(input$cmp_split_gn %||% ""))
        if (is.finite(gn)) {
          if (side == "a") paste0("GN 1\u2013", gn) else paste0("GN ", gn + 1L, "+")
        } else {
          toupper(side)
        }
      },
      toupper(side)  # unknown preset fallback
    )
  }
```

- [ ] **Step 2: Rename `side_summary` to `side_label_full`**

Find `side_summary <- function(side)` at line ~230 (after the new function) and rename to `side_label_full`. This is the existing verbose builder — no logic changes.

Use `replace_all = true` to rename all references:
- `side_summary("a")` → `side_label_full("a")`
- `side_summary("b")` → `side_label_full("b")`
- `side_summary` → `side_label_full` (the function definition itself)

- [ ] **Step 3: Commit**

```bash
git add app/R/server_tab7_compare.R
git commit -m "feat(tab7): add side_label_short, rename side_summary to side_label_full"
```

---

### Task 2: Update DT table column headers + badge JS

> Line numbers below are pre-Task-1 references. Task 1 inserts ~40 lines, so search for code patterns rather than exact line numbers.

**Files:**
- Modify: `app/R/server_tab7_compare.R` — the `output$cmp_table` block (search for `output$cmp_table <- DT::renderDataTable`)

- [ ] **Step 1: Replace `side_label_full` with `side_label_short` for column names**

In `output$cmp_table`, change lines that set column names (currently ~line 2172-2187):

```r
    side_a_label <- side_label_short("a")
    side_b_label <- side_label_short("b")
```

(These were previously `side_label_full("a")` / `side_label_full("b")` after Task 1 rename.)

- [ ] **Step 2: Add `headerCallback` to the DT options for colored badges**

Inside the `options = list(...)` of `DT::datatable(...)`, add a `headerCallback`. Insert it after `rowCallback` — add a trailing comma to `rowCallback`'s closing paren first. The JS checks if columns 3 and 5 (0-indexed: 2 and 4) contain just "A" or "B" and wraps them in a colored badge span:

```r
        # existing rowCallback — ADD COMMA after closing paren:
        rowCallback = DT::JS(
          "function(row, data) { $(row).css('cursor', 'pointer'); }"
        ),
        # NEW: badge header callback
        headerCallback = DT::JS("
          function(thead, data, start, end, display) {
            var cells = $(thead).find('th');
            var badgeStyle = 'display:inline-block;border-radius:50%;padding:2px 8px;font-size:.7rem;font-weight:600;';
            var aStyle = badgeStyle + 'background:rgba(123,140,222,.2);color:#7b8cde;border:1px solid rgba(123,140,222,.4);';
            var bStyle = badgeStyle + 'background:rgba(232,164,53,.15);color:#e8a435;border:1px solid rgba(232,164,53,.35);';
            var aText = $(cells[2]).text().trim();
            var bText = $(cells[4]).text().trim();
            if (aText === 'A') $(cells[2]).html('<span style=\"' + aStyle + '\">A</span>');
            if (bText === 'B') $(cells[4]).html('<span style=\"' + bStyle + '\">B</span>');
          }
        ")
```

Note: `headerCallback` is the last item in `options` — no trailing comma.

- [ ] **Step 3: Verify locally**

Run: `"$RSCRIPT" -e "shiny::runApp('app')"`

Check:
1. Select "Home vs Away" preset → column headers say "Home" / "Away" (plain text)
2. Select "— Custom —" → column headers show colored A / B badges
3. Select "Starters vs Bench" → "Starters" / "Bench"
4. Summary cards always show full verbose description

- [ ] **Step 4: Commit**

```bash
git add app/R/server_tab7_compare.R
git commit -m "feat(tab7): short preset labels for DT columns, colored A/B badges in custom mode"
```

---

### Task 3: Update detail view column headers

> Line numbers below are pre-Task-1 references. Search for code patterns.

**Files:**
- Modify: `app/R/server_tab7_compare.R` — the `output$cmp_detail_view_ui` block (search for `sum_a <- side_label_full("a")`)

- [ ] **Step 1: Use `side_label_short` for detail grid column headers**

Currently (line ~1816-1830):
```r
    sum_a <- side_label_full("a"); sum_b <- side_label_full("b")
    ...
    tags$div(class = "cmp-col-header cmp-col-a ...", paste0("A \u00b7 ", sum_a)),
    ...
    tags$div(class = "cmp-col-header cmp-col-b ...", paste0("B \u00b7 ", sum_b))
```

Change to use short labels, avoiding "A · A" redundancy:

```r
    short_a <- side_label_short("a"); short_b <- side_label_short("b")
    full_a <- side_label_full("a"); full_b <- side_label_full("b")
    ...
    # Column headers: "A · Home" for preset, just "A" for custom (avoid "A · A")
    col_a_text <- if (identical(short_a, "A")) "A" else paste0("A \u00b7 ", short_a)
    col_b_text <- if (identical(short_b, "B")) "B" else paste0("B \u00b7 ", short_b)
```

Update the `all_cells` header to use `col_a_text` / `col_b_text`:
```r
    all_cells <- c(all_cells, list(
      tags$div(class = "cmp-col-header cmp-col-a cmp-cell cmp-first-row", col_a_text),
      tags$div(class = "cmp-col-header cmp-col-gap cmp-cell cmp-first-row",
        "Gap ", tags$span(id = "cmp-sort-icon", "\u2195")),
      tags$div(class = "cmp-col-header cmp-col-b cmp-cell cmp-first-row", col_b_text)
    ))
```

- [ ] **Step 2: Keep detail subheader using `side_label_full`**

The subheader line (~1934) already uses `side_label_full` after Task 1 rename:
```r
    tags$div(class = "cmp-team-subheader",
      paste0(full_a, " vs ", full_b, " \u00b7 ", gy, "-", as.integer(substr(gy, 3, 4)) + 1)),
```

Verify this line references `full_a` / `full_b` (the full labels), not the short ones. Adjust variable names if needed.

- [ ] **Step 3: Verify locally**

Run the app and in detail view:
1. Preset "Home vs Away" → detail columns say "A · Home" / "B · Away", subheader says "Home vs Away · 2025-26"
2. Custom mode → detail columns say just "A" / "B", subheader shows full verbose filters

- [ ] **Step 4: Commit**

```bash
git add app/R/server_tab7_compare.R
git commit -m "feat(tab7): use short labels in detail view column headers"
```
