# Compare Tab — Detail View Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a Detail view to the Compare tab (Tab 7) that shows ALL metrics for a single team/lineup side-by-side with diverging gap bars, accessible via row click or toggle+dropdown.

**Architecture:** The Detail view is a presentation-only addition to the existing Compare tab. No new SQL functions or data sources. A new `cmp_detail_data` reactive fetches BOTH ratings AND four-factors data for the selected entity (only when in Detail mode). The view uses inline HTML/CSS rendered via `renderUI`, matching the approved mockup at `.superpowers/brainstorm/compare-detail-mockup-v10.html`.

**Tech Stack:** R/Shiny (bslib BS5), DBI/RPostgres, DT (for row click callback), inline CSS grid layout.

**Spec:** `docs/superpowers/specs/2026-03-25-compare-detail-view-design.md`

---

## File Map

| Action | File | Responsibility |
|--------|------|----------------|
| Modify | `app/R/ui_tab7_compare.R` | Add League/Detail toggle, entity dropdown, detail view container |
| Modify | `app/R/server_tab7_compare.R` | Detail state management, data reactive, row click handler, detail renderUI, gap bar logic |

No new files needed. All CSS is inline (per existing pattern — external CSS breaks load order, see CLAUDE.md).

## SQL Column Reference

These are the actual column names returned by each SQL function (NOT the TEAM_METRICS chip mapping):

**`get_team_ratings_dynamic`:** `team_id`, `team_name`, `off_ppp`, `def_ppp`, `net_rtg`, `games_played`, `wins`, `losses`, `off_poss`, `def_poss`

**`get_team_four_factors_dynamic`:** `team_id`, `team_name`, `off_ts`, `off_oreb`, `off_tov`, `off_ftr`, `def_ts`, `def_oreb`, `def_tov`, `def_ftr`, `off_poss`, `def_poss`, `net_rtg`

**`fetch_lineups_csv_v2`:** `team_id`, `sub_lineup_hash`, `player_names_str`, `off_ppp`, `def_ppp`, `net_rtg`, `off_poss`, `def_poss` (plus shooting splits)

**`fetch_lineups_four_factors_csv`:** `team_id`, `sub_lineup_hash`, `player_names_str`, `off_ts`, `off_oreb`, `off_tov`, `off_ftr`, `def_ts`, `def_oreb`, `def_tov`, `def_ftr`, `off_poss`, `def_poss`, `net_rtg`

**Important:** FF columns are `off_ts` (not `off_ts_pct`). The `_pct` suffix in TEAM_METRICS is only for chip selection mapping, not actual SQL column names.

## Polarity & Gap Convention

From the approved spec — the gap encodes two dimensions:

| Metric | Better when | Gap sign |
|--------|-------------|----------|
| Win% | Higher | + |
| Net Rtg | Higher | + |
| Off Rtg | Higher | + |
| Def Rtg | Lower | − |
| TS% | Higher | + |
| TOV% | Lower | − |
| OREB% | Higher | + |
| FTR | Higher | + |
| Opp TS% | Lower | − |
| Opp TOV% | Higher | + |
| Opp OREB% | Lower | − |
| Opp FTR | Lower | − |

**Sign** = good(+) / bad(−) direction. **Color/bar** = who has the higher raw value (blue=A, amber=B). **Stat value colors** = polarity-aware winner (white) / loser (gray).

## Formatting

- Off Rtg / Def Rtg: PPP × 100 (e.g. 112.3)
- Net Rtg: signed (e.g. +8.2)
- Win%: percentage with % suffix
- TS%, TOV%, OREB%, FTR and Opp variants: × 100 with % suffix
- GP, W, L: integers
- Poss: integer with comma separator

---

## Task 1: Add UI State and Toggle Controls

**Files:**
- Modify: `app/R/ui_tab7_compare.R:190-231` (mainPanel, Teams/Lineups mode section)
- Modify: `app/R/server_tab7_compare.R:1-12` (add reactiveVals)

### Steps

- [x] **Step 1.1: Add `selected_detail_entity` and `detail_view_active` reactiveVals**

In `server_tab7_compare.R`, after the existing `reactiveValues` block (line 5-8), add:

```r
selected_detail_entity <- reactiveVal(NULL)  # NULL = league view, or list(key=..., name=...)
detail_view_active <- reactiveVal(FALSE)
```

- [x] **Step 1.3: Add League/Detail toggle and entity dropdown to UI**

In `ui_tab7_compare.R`, inside the `conditionalPanel` for `input.cmp_mode != 'Players'` (line 194), add a toggle bar ABOVE the metric chips div. Replace lines 196-230 with:

```r
        conditionalPanel(
          condition = "input.cmp_mode != 'Players'",

          # League / Detail toggle + entity dropdown
          div(
            class = "d-flex align-items-center gap-2 mb-3 flex-wrap",
            tags$span(class = "text-muted small text-uppercase", "View"),
            tags$button(
              id = "cmp_view_league_btn",
              type = "button",
              class = "btn btn-sm btn-warning",
              style = "border-radius: 20px; padding: 2px 12px; font-size: .76rem;",
              onclick = "Shiny.setInputValue('cmp_detail_toggle', 'league', {priority: 'event'})",
              "League"
            ),
            tags$button(
              id = "cmp_view_detail_btn",
              type = "button",
              class = "btn btn-sm btn-outline-secondary",
              style = "border-radius: 20px; padding: 2px 12px; font-size: .76rem;",
              onclick = "Shiny.setInputValue('cmp_detail_toggle', 'detail', {priority: 'event'})",
              "Detail"
            ),
            # Entity dropdown (only visible in detail mode)
            uiOutput("cmp_detail_entity_dropdown_ui")
          ),

          # League view: metric chips + summary cards + DT table
          uiOutput("cmp_league_view_ui"),

          # Detail view: back button + entity header + context bar + comparison grid
          uiOutput("cmp_detail_view_ui")
        ),
```

Remove the existing static metric chips div, summary cards fluidRow, and `DT::dataTableOutput("cmp_table")` — they move into `cmp_league_view_ui`.

- [ ] **Step 1.3: Run the app and verify the toggle buttons render**

Run: `"$RSCRIPT" -e "shiny::runApp('app', port = 7775)"`
Expected: Two toggle buttons ("League" / "Detail") appear above the Compare tab content. No errors in console.

- [ ] **Step 1.4: Commit**

```bash
git add app/R/ui_tab7_compare.R app/R/server_tab7_compare.R
git commit -m "feat(tab7): add League/Detail toggle and state reactiveVals"
```

---

## Task 2: League View Conditional Rendering

**Files:**
- Modify: `app/R/server_tab7_compare.R` (add `cmp_league_view_ui` renderUI, toggle observer, toggle button styling)

### Steps

- [x] **Step 2.1: Add toggle observer and button styling**

After the existing `selected_metric` observeEvent (around line 505-508), add:

```r
  observeEvent(input$cmp_detail_toggle, {
    is_detail <- identical(input$cmp_detail_toggle, "detail")
    detail_view_active(is_detail)
    if (!is_detail) selected_detail_entity(NULL)
  }, ignoreInit = TRUE)

  # Sync toggle button styles
  observe({
    is_detail <- detail_view_active()
    session$sendCustomMessage("toggle_cmp_view", list(detail = is_detail))
  })
```

Add a JS handler in the UI file (inside `shared_head_tags()` or as inline script) — but since `shared_head_tags()` is in `global.R`, add a `tags$script` in the `fluidPage` of `ui_tab7_compare.R`:

```r
tags$script(HTML("
  Shiny.addCustomMessageHandler('toggle_cmp_view', function(msg) {
    var league = document.getElementById('cmp_view_league_btn');
    var detail = document.getElementById('cmp_view_detail_btn');
    if (!league || !detail) return;
    if (msg.detail) {
      league.className = 'btn btn-sm btn-outline-secondary';
      detail.className = 'btn btn-sm btn-warning';
    } else {
      league.className = 'btn btn-sm btn-warning';
      detail.className = 'btn btn-sm btn-outline-secondary';
    }
    // Show/hide league and detail containers via plain JS (no shinyjs dependency)
    var leagueC = document.getElementById('cmp_league_container');
    var detailC = document.getElementById('cmp_detail_container');
    if (leagueC) leagueC.style.display = msg.detail ? 'none' : '';
    if (detailC) detailC.style.display = msg.detail ? '' : 'none';
  });
")),
```

- [x] **Step 2.2: Restructure the UI with static league/detail containers**

Keep the existing league view elements static in `ui_tab7_compare.R` but wrap them in a named div. Add a second div for the detail view, initially hidden via inline `style = "display: none;"`. No `shinyjs` dependency needed — the `toggle_cmp_view` JS handler from Step 2.1 handles both button styling and container visibility.

In `ui_tab7_compare.R`, the non-Players conditionalPanel becomes:

```r
        conditionalPanel(
          condition = "input.cmp_mode != 'Players'",

          # Toggle bar
          div(
            class = "d-flex align-items-center gap-2 mb-3 flex-wrap",
            tags$span(class = "text-muted small text-uppercase", "View"),
            tags$button(id = "cmp_view_league_btn", type = "button",
              class = "btn btn-sm btn-warning",
              style = "border-radius: 20px; padding: 2px 12px; font-size: .76rem;",
              onclick = "Shiny.setInputValue('cmp_detail_toggle', 'league', {priority: 'event'})",
              "League"),
            tags$button(id = "cmp_view_detail_btn", type = "button",
              class = "btn btn-sm btn-outline-secondary",
              style = "border-radius: 20px; padding: 2px 12px; font-size: .76rem;",
              onclick = "Shiny.setInputValue('cmp_detail_toggle', 'detail', {priority: 'event'})",
              "Detail"),
            uiOutput("cmp_detail_entity_dropdown_ui")
          ),

          # ── League view ──
          div(
            id = "cmp_league_container",
            # Metric chips (existing)
            div(
              class = "d-flex align-items-center gap-2 mb-3 flex-wrap",
              tags$span(class = "text-muted small text-uppercase", "Metric"),
              uiOutput("cmp_metric_chips_ui")
            ),
            # Summary cards (existing)
            fluidRow(
              column(4, div(class = "card bg-dark border-secondary p-3 mb-3",
                tags$div(class = "small text-uppercase", style = "color: #7b8cde;",
                         textOutput("cmp_summary_a_title", inline = TRUE)),
                tags$div(class = "fs-4 fw-bold", style = "color: #4caf7d;", textOutput("cmp_summary_a", inline = TRUE)),
                tags$div(class = "small text-muted", textOutput("cmp_summary_a_label", inline = TRUE)),
                tags$div(class = "small text-muted", textOutput("cmp_summary_a_delta", inline = TRUE)),
                tags$div(class = "small text-muted", textOutput("cmp_summary_a_poss", inline = TRUE))
              )),
              column(4, div(class = "card bg-dark border-secondary p-3 mb-3",
                tags$div(class = "small text-uppercase", style = "color: #e8a435;",
                         textOutput("cmp_summary_b_title", inline = TRUE)),
                tags$div(class = "fs-4 fw-bold", style = "color: #e05c5c;", textOutput("cmp_summary_b", inline = TRUE)),
                tags$div(class = "small text-muted", textOutput("cmp_summary_b_label", inline = TRUE)),
                tags$div(class = "small text-muted", textOutput("cmp_summary_b_delta", inline = TRUE)),
                tags$div(class = "small text-muted", textOutput("cmp_summary_b_poss", inline = TRUE))
              )),
              column(4, div(class = "card bg-dark border-secondary p-3 mb-3",
                tags$div(class = "small text-uppercase text-muted", "Avg Gap"),
                tags$div(class = "fs-4 fw-bold", style = "color: #e8a435;", textOutput("cmp_summary_gap", inline = TRUE)),
                tags$div(class = "small text-muted", "league-wide")
              ))
            ),
            # Results table (existing)
            DT::dataTableOutput("cmp_table")
          ),

          # ── Detail view (hidden by default via inline style) ──
          div(
            id = "cmp_detail_container",
            style = "display: none;",
            uiOutput("cmp_detail_view_ui")
          )
        ),
```

- [x] **Step 2.3: Add server-side observer to sync visibility**

The `toggle_cmp_view` custom message handler (from Step 2.1) handles the JS show/hide. The server just needs to send the message:

```r
  observe({
    is_detail <- detail_view_active()
    session$sendCustomMessage("toggle_cmp_view", list(detail = is_detail))
  })
```

No `shinyjs` dependency needed.

- [x] **Step 2.4: Reset detail state on mode change**

Find the existing mode change observer (around line 638-644) and add:

```r
  # In existing observeEvent for cmp_mode change:
  selected_detail_entity(NULL)
  detail_view_active(FALSE)
```

- [ ] **Step 2.5: Run app, test toggle**

Run: `"$RSCRIPT" -e "shiny::runApp('app', port = 7775)"`
Expected: Clicking "League" shows the existing league table. Clicking "Detail" hides it and shows the (empty) detail container. Mode switch resets to league view.

- [ ] **Step 2.6: Commit**

```bash
git add app/R/ui_tab7_compare.R app/R/server_tab7_compare.R
git commit -m "feat(tab7): toggle between league and detail views"
```

---

## Task 3: Row Click → Detail Drill-In

**Files:**
- Modify: `app/R/server_tab7_compare.R` (DT callback, row click observer, entity dropdown)

### Steps

- [x] **Step 3.1: Add row click JS callback to DT table**

Modify the `output$cmp_table` `DT::datatable()` call (around line 1468) to add a row click callback. Add `callback` as a **top-level argument** to `DT::datatable()` (NOT inside the `options` list — `callback` is a peer of `options`, `rownames`, `selection`, etc.):

```r
DT::datatable(
  show_df,
  callback = DT::JS("
    table.on('click', 'tbody tr', function() {
      var data = table.row(this).data();
      if (data) {
        Shiny.setInputValue('cmp_table_row_click', {
          entity_name: data[1],
          rand: Math.random()
        }, {priority: 'event'});
      }
    });
  "),
  options = list(...),
  rownames = FALSE, selection = "none",
  class = "compact stripe nowrap"
)
```

**Note:** `data[1]` is the Entity column (0-indexed: `#`=0, Entity=1). The `rand` field ensures each click is a new event even if the same row is clicked twice. This index depends on current column order — if columns are reordered, update the index.

- [x] **Step 3.2: Add row click observer**

```r
  observeEvent(input$cmp_table_row_click, {
    info <- input$cmp_table_row_click
    req(info$entity_name)

    mode <- input$cmp_mode
    df <- cmp_joined()
    req(df, nrow(df) > 0)

    entity_name <- info$entity_name

    if (mode == "Teams") {
      row <- df[df$team_name == entity_name, , drop = FALSE]
      if (nrow(row)) {
        selected_detail_entity(list(
          key = row$team_id[1],
          name = row$team_name[1],
          mode = "Teams"
        ))
      }
    } else if (mode == "Lineups") {
      row <- df[df$entity_name == entity_name, , drop = FALSE]
      if (nrow(row)) {
        selected_detail_entity(list(
          key = row$sub_lineup_hash[1],
          name = row$entity_name[1],
          mode = "Lineups"
        ))
      }
    }
    detail_view_active(TRUE)
  }, ignoreInit = TRUE)
```

- [x] **Step 3.3: Add entity dropdown for direct detail access**

```r
  output$cmp_detail_entity_dropdown_ui <- renderUI({
    if (!isTRUE(detail_view_active())) return(NULL)

    df <- cmp_joined()
    if (is.null(df) || !nrow(df)) return(NULL)

    choices <- setNames(df$entity_name, df$entity_name)
    current <- selected_detail_entity()
    sel <- if (!is.null(current)) current$name else ""

    tags$div(
      style = "min-width: 200px;",
      selectizeInput("cmp_detail_entity_select", NULL,
        choices = c("Select..." = "", choices),
        selected = sel,
        options = list(placeholder = "Select entity..."),
        width = "100%"
      )
    )
  })

  observeEvent(input$cmp_detail_entity_select, {
    req(nzchar(input$cmp_detail_entity_select))
    entity_name <- input$cmp_detail_entity_select
    # Guard: skip if already showing this entity (prevents loop when renderUI re-creates the dropdown)
    current <- selected_detail_entity()
    if (!is.null(current) && identical(current$name, entity_name)) return()
    mode <- input$cmp_mode
    df <- cmp_joined()
    req(df, nrow(df) > 0)

    if (mode == "Teams") {
      row <- df[df$team_name == entity_name, , drop = FALSE]
      if (nrow(row)) {
        selected_detail_entity(list(
          key = row$team_id[1],
          name = row$team_name[1],
          mode = "Teams"
        ))
      }
    } else if (mode == "Lineups") {
      row <- df[df$entity_name == entity_name, , drop = FALSE]
      if (nrow(row)) {
        selected_detail_entity(list(
          key = row$sub_lineup_hash[1],
          name = row$entity_name[1],
          mode = "Lineups"
        ))
      }
    }
  }, ignoreInit = TRUE)
```

- [ ] **Step 3.4: Run app, test row click drill-in**

Run: `"$RSCRIPT" -e "shiny::runApp('app', port = 7775)"`
Expected: Clicking a row in the league table switches to Detail mode. Toggle buttons update. Dropdown shows the selected entity. Clicking "League" returns to the table.

- [ ] **Step 3.5: Commit**

```bash
git add app/R/server_tab7_compare.R
git commit -m "feat(tab7): row click drill-in and entity dropdown for detail view"
```

---

## Task 4: Detail Data Reactive

**Files:**
- Modify: `app/R/server_tab7_compare.R` (add `cmp_detail_data` reactive)

This reactive fetches BOTH ratings AND four-factors data for BOTH sides, then joins them for the selected entity. It only fires when `selected_detail_entity()` is non-NULL.

### Steps

- [x] **Step 4.1: Add the `cmp_detail_data` reactive**

Place this after the existing `cmp_joined` reactive (after line 1354):

```r
  cmp_detail_data <- reactive({
    entity <- selected_detail_entity()
    req(entity)
    req(identical(input$main_tabs, "compare"))

    pa <- collect_side_params("a")
    pb <- collect_side_params("b")
    mode <- entity$mode

    if (mode == "Teams") {
      # Fetch both ratings and FF for each side
      ratings_a <- run_team_ratings(pa)
      ratings_b <- run_team_ratings(pb)
      ff_a <- run_team_ff(pa)
      ff_b <- run_team_ff(pb)

      # Filter to the selected team
      team_id <- entity$key
      ra <- ratings_a[ratings_a$team_id == team_id, , drop = FALSE]
      rb <- ratings_b[ratings_b$team_id == team_id, , drop = FALSE]
      fa <- ff_a[ff_a$team_id == team_id, , drop = FALSE]
      fb <- ff_b[ff_b$team_id == team_id, , drop = FALSE]

      if (!nrow(ra) && !nrow(fa)) return(NULL)
      if (!nrow(rb) && !nrow(fb)) return(NULL)

      list(
        mode = "Teams",
        entity_name = entity$name,
        ratings_a = if (nrow(ra)) ra[1, ] else NULL,
        ratings_b = if (nrow(rb)) rb[1, ] else NULL,
        ff_a = if (nrow(fa)) fa[1, ] else NULL,
        ff_b = if (nrow(fb)) fb[1, ] else NULL
      )

    } else if (mode == "Lineups") {
      summary_a <- run_lineups_summary(pa)
      summary_b <- run_lineups_summary(pb)
      ff_a <- run_lineups_ff(pa)
      ff_b <- run_lineups_ff(pb)

      hash <- entity$key
      sa <- summary_a[summary_a$sub_lineup_hash == hash, , drop = FALSE]
      sb <- summary_b[summary_b$sub_lineup_hash == hash, , drop = FALSE]
      fa <- ff_a[ff_a$sub_lineup_hash == hash, , drop = FALSE]
      fb <- ff_b[ff_b$sub_lineup_hash == hash, , drop = FALSE]

      if (!nrow(sa) && !nrow(fa)) return(NULL)
      if (!nrow(sb) && !nrow(fb)) return(NULL)

      list(
        mode = "Lineups",
        entity_name = entity$name,
        ratings_a = if (nrow(sa)) sa[1, ] else NULL,
        ratings_b = if (nrow(sb)) sb[1, ] else NULL,
        ff_a = if (nrow(fa)) fa[1, ] else NULL,
        ff_b = if (nrow(fb)) fb[1, ] else NULL
      )
    } else {
      NULL
    }
  })
```

- [ ] **Step 4.2: Verify reactive fires correctly**

Add temporary debug output:

```r
  observe({
    d <- cmp_detail_data()
    if (!is.null(d)) {
      message(sprintf("[tab7][detail] Got data for %s: ratings_a=%s, ff_a=%s",
        d$entity_name, !is.null(d$ratings_a), !is.null(d$ff_a)))
    }
  })
```

Run: `"$RSCRIPT" -e "shiny::runApp('app', port = 7775)"`
Expected: After clicking a row, console shows the debug message with data availability.

- [ ] **Step 4.3: Remove debug observer, commit**

```bash
git add app/R/server_tab7_compare.R
git commit -m "feat(tab7): add cmp_detail_data reactive for ratings+FF"
```

---

## Task 5: Detail View Rendering — CSS and Helper Functions

**Files:**
- Modify: `app/R/server_tab7_compare.R` (CSS string, metric definitions, formatting helpers, gap computation)

### Steps

- [x] **Step 5.1: Add detail view CSS**

Add a CSS string constant near the top of the server function (after the reactiveValues block):

```r
  DETAIL_CSS <- "
    .detail-container { max-width: 720px; margin: 0 auto; }
    .cmp-back-btn { font-size: .78rem; color: #7b8cde; cursor: pointer; margin-bottom: 12px; display: inline-block; }
    .cmp-back-btn:hover { text-decoration: underline; }
    .cmp-team-header { text-align: center; font-size: 1.1rem; font-weight: 700; color: #e6edf3; margin-bottom: 4px; }
    .cmp-team-subheader { text-align: center; font-size: .78rem; color: #6e7681; margin-bottom: 16px; }
    .cmp-context-bar {
      display: flex; justify-content: space-between; align-items: center;
      background: #161b22; border: 1px solid rgba(255,255,255,.08);
      border-radius: 8px; padding: 10px 20px; margin-bottom: 16px; font-size: .8rem;
    }
    .cmp-context-side { display: flex; align-items: center; gap: 6px; }
    .cmp-context-badge { font-size: .68rem; font-weight: 700; padding: 1px 8px; border-radius: 10px; }
    .cmp-context-badge.a { background: rgba(123,140,222,.2); color: #7b8cde; border: 1px solid rgba(123,140,222,.4); }
    .cmp-context-badge.b { background: rgba(232,164,53,.15); color: #e8a435; border: 1px solid rgba(232,164,53,.35); }
    .cmp-context-info { color: #8b949e; }
    .cmp-context-info strong { color: #c9d1d9; font-weight: 600; }
    .cmp-context-sep { color: #484f58; font-size: .7rem; }
    .cmp-compare-grid {
      display: grid; grid-template-columns: 1fr 140px 1fr; gap: 0 12px;
    }
    .cmp-col-a, .cmp-col-gap, .cmp-col-b {
      background: #161b22; border-left: 1px solid rgba(255,255,255,.08);
      border-right: 1px solid rgba(255,255,255,.08);
    }
    .cmp-grid-top .cmp-col-a, .cmp-grid-top .cmp-col-gap, .cmp-grid-top .cmp-col-b { border-top: 1px solid rgba(255,255,255,.08); }
    .cmp-grid-top .cmp-col-a { border-radius: 8px 0 0 0; }
    .cmp-grid-top .cmp-col-b { border-radius: 0 8px 0 0; }
    .cmp-grid-bottom .cmp-col-a, .cmp-grid-bottom .cmp-col-gap, .cmp-grid-bottom .cmp-col-b { border-bottom: 1px solid rgba(255,255,255,.08); }
    .cmp-grid-bottom .cmp-col-a { border-radius: 0 0 0 8px; }
    .cmp-grid-bottom .cmp-col-b { border-radius: 0 0 8px 0; }
    .cmp-col-header {
      padding: 10px 16px; font-size: .72rem; font-weight: 700;
      text-transform: uppercase; letter-spacing: .06em;
      border-bottom: 1px solid rgba(255,255,255,.06);
    }
    .cmp-col-a .cmp-col-header { color: #7b8cde; }
    .cmp-col-b .cmp-col-header { color: #e8a435; }
    .cmp-col-gap .cmp-col-header { color: #6e7681; text-align: center; cursor: pointer; user-select: none; }
    .cmp-section-title {
      text-align: center; padding: 12px 0 4px; font-size: .72rem; font-weight: 700;
      text-transform: uppercase; letter-spacing: .08em; color: #484f58;
    }
    .cmp-stat-row {
      display: flex; justify-content: space-between; align-items: center;
      padding: 9px 16px; border-bottom: 1px solid rgba(255,255,255,.04); min-height: 40px;
    }
    .cmp-stat-label { font-size: .78rem; color: #8b949e; }
    .cmp-stat-value { font-size: .92rem; font-weight: 600; }
    .cmp-stat-value.winner { color: #e6edf3; }
    .cmp-stat-value.loser { color: #8b949e; }
    .cmp-gap-row {
      display: flex; flex-direction: column; align-items: center; justify-content: center;
      padding: 9px 10px; border-bottom: 1px solid rgba(255,255,255,.04); gap: 3px; min-height: 40px;
    }
    .cmp-gap-num { font-size: .78rem; font-weight: 600; white-space: nowrap; }
    .cmp-gap-num.a-color { color: #7b8cde; }
    .cmp-gap-num.b-color { color: #e8a435; }
    .cmp-bar-container { width: 100%; height: 5px; background: rgba(255,255,255,.04); border-radius: 3px; position: relative; overflow: hidden; }
    .cmp-bar-center { position: absolute; top: 0; bottom: 0; left: 50%; width: 1px; background: rgba(255,255,255,.15); z-index: 1; }
    .cmp-bar { height: 100%; border-radius: 3px; position: absolute; top: 0; }
    .cmp-bar.toward-a { right: 50%; background: #7b8cde; }
    .cmp-bar.toward-b { left: 50%; background: #e8a435; }
    .cmp-section-sep { border-top: 1px solid rgba(255,255,255,.06); }
  "
```

- [x] **Step 5.2: Add metric definition table with polarity**

```r
  DETAIL_METRICS <- list(
    ratings = list(
      title = "Ratings",
      metrics = list(
        list(label = "Win%", col_ratings = "win_pct", col_ff = NULL, polarity = "higher", fmt = "pct"),
        list(label = "Net Rtg", col_ratings = "net_rtg", col_ff = NULL, polarity = "higher", fmt = "net"),
        list(label = "Off Rtg", col_ratings = "off_ppp", col_ff = NULL, polarity = "higher", fmt = "rtg"),
        list(label = "Def Rtg", col_ratings = "def_ppp", col_ff = NULL, polarity = "lower", fmt = "rtg")
      )
    ),
    off_ff = list(
      title = "Offensive Four Factors",
      metrics = list(
        list(label = "TS%", col_ratings = NULL, col_ff = "off_ts", polarity = "higher", fmt = "pct"),
        list(label = "TOV%", col_ratings = NULL, col_ff = "off_tov", polarity = "lower", fmt = "pct"),
        list(label = "OREB%", col_ratings = NULL, col_ff = "off_oreb", polarity = "higher", fmt = "pct"),
        list(label = "FTR", col_ratings = NULL, col_ff = "off_ftr", polarity = "higher", fmt = "pct")
      )
    ),
    def_ff = list(
      title = "Defensive Four Factors",
      metrics = list(
        list(label = "Opp TS%", col_ratings = NULL, col_ff = "def_ts", polarity = "lower", fmt = "pct"),
        list(label = "Opp TOV%", col_ratings = NULL, col_ff = "def_tov", polarity = "higher", fmt = "pct"),
        list(label = "Opp OREB%", col_ratings = NULL, col_ff = "def_oreb", polarity = "lower", fmt = "pct"),
        list(label = "Opp FTR", col_ratings = NULL, col_ff = "def_ftr", polarity = "lower", fmt = "pct")
      )
    )
  )
```

- [x] **Step 5.3: Add formatting and gap computation helpers**

```r
  detail_get_value <- function(row, col) {
    if (is.null(row) || is.null(col) || !(col %in% names(row))) return(NA_real_)
    suppressWarnings(as.numeric(row[[col]]))
  }

  detail_fmt <- function(val, fmt) {
    if (!is.finite(val)) return("\u2014")
    switch(fmt,
      pct = sprintf("%.1f%%", val * 100),
      rtg = sprintf("%.1f", val * 100),
      net = sprintf("%+.1f", val * 100),
      sprintf("%.1f", val)
    )
  }

  # Win% is derived: wins / games_played
  detail_win_pct <- function(row) {
    gp <- detail_get_value(row, "games_played")
    w <- detail_get_value(row, "wins")
    if (is.finite(gp) && gp > 0 && is.finite(w)) w / gp else NA_real_
  }

  detail_extract_value <- function(data_side_ratings, data_side_ff, metric_def) {
    if (!is.null(metric_def$col_ratings) && metric_def$col_ratings == "win_pct") {
      return(detail_win_pct(data_side_ratings))
    }
    if (!is.null(metric_def$col_ratings)) {
      return(detail_get_value(data_side_ratings, metric_def$col_ratings))
    }
    if (!is.null(metric_def$col_ff)) {
      return(detail_get_value(data_side_ff, metric_def$col_ff))
    }
    NA_real_
  }

  # Compute gap per spec:
  # Higher-is-better: gap = max(A,B) - min(A,B) = always positive
  # Lower-is-better: gap = min(A,B) - max(A,B) = always negative
  # Color: blue if A has higher raw value, amber if B has higher raw value
  detail_compute_gap <- function(val_a, val_b, polarity) {
    if (!is.finite(val_a) || !is.finite(val_b)) {
      return(list(gap = NA_real_, direction = "none", a_wins = NA))
    }
    raw_diff <- val_a - val_b  # positive = A has more
    abs_gap <- abs(raw_diff)

    # Direction: who has the higher raw value
    direction <- if (raw_diff > 0) "a" else if (raw_diff < 0) "b" else "none"

    # Sign: + if higher is better, - if lower is better
    signed_gap <- if (polarity == "higher") abs_gap else -abs_gap
    if (abs_gap == 0) signed_gap <- 0

    # Who actually wins (polarity-aware)
    a_wins <- if (abs_gap == 0) NA else {
      if (polarity == "higher") (val_a > val_b) else (val_a < val_b)
    }

    list(gap = signed_gap, direction = direction, a_wins = a_wins)
  }
```

- [ ] **Step 5.4: Commit**

```bash
git add app/R/server_tab7_compare.R
git commit -m "feat(tab7): detail view CSS, metric definitions, and gap helpers"
```

---

## Task 6: Detail View renderUI — Full Grid

**Files:**
- Modify: `app/R/server_tab7_compare.R` (add `cmp_detail_view_ui` renderUI)

### Steps

- [x] **Step 6.1: Add back button handler**

```r
  observeEvent(input$cmp_detail_back, {
    selected_detail_entity(NULL)
    detail_view_active(FALSE)
  }, ignoreInit = TRUE)
```

- [x] **Step 6.2: Add the main detail renderUI**

```r
  output$cmp_detail_view_ui <- renderUI({
    data <- cmp_detail_data()
    entity <- selected_detail_entity()

    if (is.null(entity) && isTRUE(detail_view_active())) {
      return(tags$div(
        class = "detail-container",
        tags$div(class = "text-muted text-center mt-4",
                 "Select a team or lineup from the dropdown to view detailed comparison.")
      ))
    }
    req(data)

    mode <- data$mode
    ra <- data$ratings_a
    rb <- data$ratings_b
    fa <- data$ff_a
    fb <- data$ff_b

    # Filter summary
    sum_a <- side_summary("a")
    sum_b <- side_summary("b")
    gy <- input$game_year

    # Context bar
    context_bar <- build_detail_context_bar(ra, rb, mode)

    # Build sections
    sections <- list()
    section_names <- names(DETAIL_METRICS)
    for (i in seq_along(section_names)) {
      sec_key <- section_names[i]
      sec <- DETAIL_METRICS[[sec_key]]

      # Skip Win% row for Lineups (no GP/W/L)
      metrics_list <- sec$metrics
      if (mode == "Lineups" && sec_key == "ratings") {
        metrics_list <- Filter(function(m) m$label != "Win%", metrics_list)
      }

      # Skip empty sections
      if (length(metrics_list) == 0) next

      # Compute all gaps for bar width scaling
      computed <- lapply(metrics_list, function(m) {
        va <- detail_extract_value(ra, fa, m)
        vb <- detail_extract_value(rb, fb, m)
        gap_info <- detail_compute_gap(va, vb, m$polarity)
        list(m = m, va = va, vb = vb, gap = gap_info)
      })

      max_abs_gap <- max(vapply(computed, function(x) {
        if (is.finite(x$gap$gap)) abs(x$gap$gap) else 0
      }, numeric(1)), na.rm = TRUE)
      if (max_abs_gap == 0) max_abs_gap <- 1  # avoid division by zero

      # Build rows for each column
      a_rows <- list()
      gap_rows <- list()
      b_rows <- list()

      for (j in seq_along(computed)) {
        x <- computed[[j]]
        m <- x$m
        va <- x$va; vb <- x$vb; gi <- x$gap

        # Winner/loser classes
        a_cls <- if (is.na(gi$a_wins)) "winner" else if (gi$a_wins) "winner" else "loser"
        b_cls <- if (is.na(gi$a_wins)) "winner" else if (gi$a_wins) "loser" else "winner"

        # Format values (FF values need *100 for display, ratings need *100)
        fmt_va <- detail_fmt(va, m$fmt)
        fmt_vb <- detail_fmt(vb, m$fmt)

        # Gap formatting
        gap_text <- if (!is.finite(gi$gap)) "\u2014" else {
          g_display <- gi$gap * if (m$fmt == "pct") 100 else if (m$fmt %in% c("rtg", "net")) 100 else 1
          pct_suffix <- if (m$fmt == "pct") "%" else ""
          if (g_display >= 0) sprintf("+%.1f%s", g_display, pct_suffix)
          else sprintf("\u2212%.1f%s", abs(g_display), pct_suffix)
        }

        gap_color_cls <- if (gi$direction == "a") "a-color" else if (gi$direction == "b") "b-color" else ""
        bar_pct <- if (is.finite(gi$gap) && max_abs_gap > 0) round(abs(gi$gap) / max_abs_gap * 50, 1) else 0
        bar_cls <- if (gi$direction == "a") "toward-a" else "toward-b"

        a_rows[[j]] <- tags$div(class = "cmp-stat-row", `data-idx` = j - 1,
          tags$span(class = "cmp-stat-label", m$label),
          tags$span(class = paste("cmp-stat-value", a_cls), fmt_va)
        )

        gap_rows[[j]] <- tags$div(class = "cmp-gap-row", `data-idx` = j - 1,
          `data-gap` = if (is.finite(gi$gap)) round(gi$gap * 100, 2) else 0,
          tags$span(class = paste("cmp-gap-num", gap_color_cls), gap_text),
          tags$div(class = "cmp-bar-container",
            tags$div(class = "cmp-bar-center"),
            if (bar_pct > 0) tags$div(class = paste("cmp-bar", bar_cls),
              style = sprintf("width: %.1f%%;", bar_pct))
          )
        )

        b_rows[[j]] <- tags$div(class = "cmp-stat-row", `data-idx` = j - 1,
          tags$span(class = "cmp-stat-label", m$label),
          tags$span(class = paste("cmp-stat-value", b_cls), fmt_vb)
        )
      }

      # Grid classes
      is_first <- (i == 1)
      is_last <- (i == length(section_names))
      grid_extra <- paste(
        if (is_first) "cmp-grid-top" else "",
        if (is_last) "cmp-grid-bottom" else ""
      )
      sep_cls <- if (!is_first) " cmp-section-sep" else ""

      sections[[length(sections) + 1]] <- tagList(
        tags$div(class = "cmp-section-title", sec$title),
        tags$div(class = paste("cmp-compare-grid", grid_extra), `data-group` = sec_key,
          tags$div(class = paste0("cmp-col-a", sep_cls),
            if (is_first) tags$div(class = "cmp-col-header", paste0("A \u00b7 ", sum_a)),
            do.call(tagList, a_rows)
          ),
          tags$div(class = paste0("cmp-col-gap", sep_cls),
            if (is_first) tags$div(class = "cmp-col-header",
              onclick = "Shiny.setInputValue('cmp_detail_sort', Math.random(), {priority: 'event'})",
              "Gap ", tags$span(class = "sort-icon", id = "cmp-sort-icon", "\u2195")),
            do.call(tagList, gap_rows)
          ),
          tags$div(class = paste0("cmp-col-b", sep_cls),
            if (is_first) tags$div(class = "cmp-col-header", paste0("B \u00b7 ", sum_b)),
            do.call(tagList, b_rows)
          )
        )
      )
    }

    tagList(
      tags$style(DETAIL_CSS),
      tags$div(class = "detail-container",
        tags$div(class = "cmp-back-btn",
          onclick = "Shiny.setInputValue('cmp_detail_back', Math.random(), {priority: 'event'})",
          "\u2190 Back to league view"),
        tags$div(class = "cmp-team-header", data$entity_name),
        tags$div(class = "cmp-team-subheader",
          paste0(sum_a, " vs ", sum_b, " \u00b7 ", gy, "-", as.integer(substr(gy, 3, 4)) + 1)),
        context_bar,
        do.call(tagList, sections)
      )
    )
  })
```

- [x] **Step 6.3: Add context bar builder**

```r
  build_detail_context_bar <- function(ra, rb, mode) {
    build_side <- function(row, badge_cls) {
      parts <- character(0)
      if (mode == "Teams" && !is.null(row)) {
        gp <- detail_get_value(row, "games_played")
        w <- detail_get_value(row, "wins")
        l <- detail_get_value(row, "losses")
        if (is.finite(gp)) parts <- c(parts, paste0("<strong>", round(gp), "</strong> GP"))
        if (is.finite(w) && is.finite(l)) parts <- c(parts, paste0("<strong>", round(w), "</strong>-<strong>", round(l), "</strong>"))
      }
      # Poss (available for both modes)
      if (!is.null(row)) {
        poss <- detail_get_value(row, "off_poss")
        if (!is.finite(poss)) poss <- detail_get_value(row, "total_poss")
        if (is.finite(poss)) parts <- c(parts, paste0("<strong>", format(round(poss), big.mark = ","), "</strong> Poss"))
      }
      if (length(parts) == 0) return(NULL)
      tags$div(class = "cmp-context-side",
        tags$span(class = paste("cmp-context-badge", badge_cls),
          if (badge_cls == "a") "A" else "B"),
        tags$span(class = "cmp-context-info", HTML(paste(parts, collapse = " \u00b7 ")))
      )
    }

    side_a <- build_side(ra, "a")
    side_b <- build_side(rb, "b")
    if (is.null(side_a) && is.null(side_b)) return(NULL)

    tags$div(class = "cmp-context-bar",
      side_a,
      tags$div(class = "cmp-context-sep", "|"),
      side_b
    )
  }
```

- [ ] **Step 6.4: Run app, test full detail view**

Run: `"$RSCRIPT" -e "shiny::runApp('app', port = 7775)"`
Expected: Clicking a team row shows the detail view with:
- Back button, entity header, context bar
- Three-section grid (Ratings, Off FF, Def FF)
- Correct values, gap signs, bar directions, winner/loser coloring

- [ ] **Step 6.5: Commit**

```bash
git add app/R/server_tab7_compare.R
git commit -m "feat(tab7): render detail view with gap bars and context bar"
```

---

## Task 7: Gap Sorting

**Files:**
- Modify: `app/R/server_tab7_compare.R` (sort state, JS-based sorting)

### Steps

- [x] **Step 7.1: Add client-side sorting via JS**

Since the detail view is rendered by the server and rows have `data-gap` attributes, we can sort client-side just like the mockup. Add a JS script block to the detail renderUI output (inside the tagList):

```r
  # Add after the sections in the detail renderUI:
  tags$script(HTML("
    (function() {
      var sortState = 0;
      var icons = {0: '\u2195', 1: '\u2193', 2: '\u2191'};
      var btn = document.getElementById('cmp-sort-icon');
      if (!btn) return;
      btn.parentElement.onclick = function() {
        sortState = (sortState + 1) % 3;
        btn.textContent = icons[sortState];
        ['ratings', 'off_ff', 'def_ff'].forEach(function(group) {
          var grid = document.querySelector('.cmp-compare-grid[data-group=\"' + group + '\"]');
          if (!grid) return;
          var colA = grid.children[0], colGap = grid.children[1], colB = grid.children[2];
          var gapRows = Array.from(colGap.querySelectorAll('.cmp-gap-row'));
          var aRows = Array.from(colA.querySelectorAll('.cmp-stat-row'));
          var bRows = Array.from(colB.querySelectorAll('.cmp-stat-row'));
          var indices = gapRows.map(function(r, i) { return {idx: i, gap: parseFloat(r.dataset.gap || 0)}; });
          if (sortState === 1) indices.sort(function(a, b) { return Math.abs(b.gap) - Math.abs(a.gap); });
          else if (sortState === 2) indices.sort(function(a, b) { return Math.abs(a.gap) - Math.abs(b.gap); });
          indices.forEach(function(x) {
            colA.appendChild(aRows[x.idx]);
            colGap.appendChild(gapRows[x.idx]);
            colB.appendChild(bRows[x.idx]);
          });
        });
      };
    })();
  "))
```

Remove the server-side `cmp_detail_sort` observer if added — sorting is purely client-side.

- [ ] **Step 7.2: Run app, test sorting**

Run: `"$RSCRIPT" -e "shiny::runApp('app', port = 7775)"`
Expected: Clicking "Gap ↕" cycles through default → largest gap first → smallest gap first, within each section independently.

- [ ] **Step 7.3: Commit**

```bash
git add app/R/server_tab7_compare.R
git commit -m "feat(tab7): client-side gap sorting for detail view"
```

---

## Task 8: Edge Cases and Polish

**Files:**
- Modify: `app/R/server_tab7_compare.R`

### Steps

- [x] **Step 8.1: Handle entity missing from results after filter change**

In the `cmp_detail_view_ui` renderUI, add handling when `selected_detail_entity()` is set but `cmp_detail_data()` returns NULL:

```r
  # At the top of cmp_detail_view_ui, after req checks:
  if (is.null(data) && !is.null(entity)) {
    return(tags$div(
      class = "detail-container",
      tags$div(class = "cmp-back-btn",
        onclick = "Shiny.setInputValue('cmp_detail_back', Math.random(), {priority: 'event'})",
        "\u2190 Back to league view"),
      tags$div(class = "text-muted text-center mt-4",
        "No data for this entity with current filters.")
    ))
  }
```

- [x] **Step 8.2: Handle equal values (A = B)**

Already handled by the gap computation: when `abs_gap == 0`, `a_wins = NA`, both sides get "winner" class (white), gap shows `\u00b10.0`, bar width is 0 (no bar drawn).

Verify this works by testing with a preset where A and B have the same filters.

- [x] **Step 8.3: Handle missing data source**

If `ratings_a` is NULL but `ff_a` is available (or vice versa), the `detail_extract_value` function returns `NA_real_` for metrics from the missing source. The `detail_fmt` function shows "—" for NA values. Verify this renders correctly.

- [ ] **Step 8.4: Verify Lineups mode hides Win% and shows correct context**

Switch to Lineups mode, drill into a lineup. Verify:
- Win% row is absent from Ratings section
- Context bar shows Poss but not GP/W/L
- Entity header shows player names

- [ ] **Step 8.5: Run full integration test**

Test the following flows:
1. Teams mode → click row → detail view → back button → league view
2. Teams mode → toggle to Detail → select from dropdown → view renders
3. Teams mode → detail view → change filters → data updates (or shows "No data" message)
4. Teams mode → detail view → switch to Lineups mode → resets to league view
5. Lineups mode → click row → detail view (no Win% row, correct context)
6. Detail view → click "Gap ↕" → rows sort by gap size
7. Players mode → no toggle visible (Players mode is unchanged)

- [ ] **Step 8.6: Commit**

```bash
git add app/R/ui_tab7_compare.R app/R/server_tab7_compare.R
git commit -m "feat(tab7): detail view edge cases and polish"
```

---

## Implementation Notes

### No External Dependencies
The show/hide toggle uses plain JS via `session$sendCustomMessage("toggle_cmp_view", ...)` and a `Shiny.addCustomMessageHandler` in the UI. No `shinyjs` or other external packages needed. The detail container starts hidden via `style = "display: none;"`.

### CSS Prefixing
All CSS classes are prefixed with `cmp-` to avoid collisions with other tabs. The mockup used unprefixed names (`detail-container`, `back-btn`, etc.) — the plan uses prefixed versions.

### DT Callback vs Selection
The existing DT table uses `selection = "none"`. The row click is handled via a custom JS callback on `tbody tr` rather than DT's built-in row selection, because we need to extract the entity name from the row data.

### Performance
The `cmp_detail_data` reactive makes 4 SQL calls (ratings + FF for each side). This is acceptable because:
1. It only fires when `selected_detail_entity()` is non-NULL
2. The SQL functions already have `guard_heavy_request` rate limiting
3. The calls reuse the existing `run_team_ratings` / `run_team_ff` / etc. runners

