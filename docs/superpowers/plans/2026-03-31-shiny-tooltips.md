# Shiny Tooltips Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add CSS-only tooltips to DT column headers, sidebar filter labels, and view toggles across all Shiny tabs.

**Architecture:** Central tooltip definitions in `global.R` + a shared `headerCallback` JS function that injects `data-tooltip` attributes on DT headers client-side. Sidebar filters use a `tt()` R helper. CSS-only `::after` pseudo-element for dark-themed tooltip rendering.

**Tech Stack:** R/Shiny, DT (headerCallback), CSS pseudo-elements, htmltools

---

### Task 1: Add tooltip CSS and central definitions to global.R

**Files:**
- Modify: `app/R/global.R`

- [ ] **Step 1: Add tooltip CSS to `shared_css`**

Insert just before the closing `")` of `shared_css` (line 690 in `global.R`), before the `")`):

```css
  /* ---- Tooltips ---- */
  [data-tooltip] { position: relative; cursor: help; }
  [data-tooltip]::after {
    content: attr(data-tooltip);
    position: absolute; bottom: 100%; left: 50%;
    transform: translateX(-50%); margin-bottom: 6px;
    background: #1c2333; color: #e6edf3; border: 1px solid #30363d;
    font-size: 0.72rem; font-weight: 400; line-height: 1.4;
    padding: 5px 9px; border-radius: 6px; white-space: normal;
    max-width: 260px; width: max-content;
    z-index: 9999; pointer-events: none;
    opacity: 0; transition: opacity 0.15s 0.4s;
  }
  [data-tooltip]:hover::after { opacity: 1; }
  /* DT header tooltips: position below since headers are at top */
  th[data-tooltip]::after {
    bottom: auto; top: 100%; margin-bottom: 0; margin-top: 6px;
  }
```

- [ ] **Step 2: Add `COLUMN_TOOLTIPS` named list**

Insert after the `COLS_REV` line (around line 30) in the `# Defaults` section:

```r
# ---- Tooltip definitions ----
COLUMN_TOOLTIPS <- c(
  # Efficiency
  "PPP"         = "Points per 100 possessions",
  "Off PPP"     = "Offensive points per 100 possessions",
  "Def PPP"     = "Defensive points per 100 possessions",
  "Net"         = "Offensive PPP minus Defensive PPP",
  "Net Rtg"     = "Offensive PPP minus Defensive PPP",
  "Net RTG"     = "Offensive PPP minus Defensive PPP",
  # On/Off diffs (Tab 1 Summary Net Impact)
  "Off"         = "Offensive PPP diff: On-court minus Off-court",
  "Def"         = "Defensive PPP diff: On-court minus Off-court",
  # On/Off FF total diff
  "Diff"        = "Net PPP impact: On-court minus Off-court",
  # Four Factors
  "TS%"         = "True Shooting: pts / (2 \u00d7 (FGA + FT trips))",
  "OREB%"       = "Off. rebound rate: OREBs / available misses",
  "TOV%"        = "Turnover rate: turnovers / possessions",
  "FTR"         = "Free throw rate: FTA / FGA",
  # Shooting
  "Off Shot"    = "2PT/3PT frequency and accuracy split",
  "Def Shot"    = "2PT/3PT frequency and accuracy split",
  "FG%"         = "Field goal percentage",
  "3P%"         = "Three-point percentage",
  "FT%"         = "Free throw percentage",
  "eFG%"        = "Effective FG%: (FGM + 0.5 \u00d7 3PM) / FGA",
  # Usage / Volume
  "On Poss"     = "Possessions while player is on court",
  "Off Poss"    = "Possessions while player is off court",
  "Poss"        = "Number of possessions",
  "Total Poss"  = "Offensive + Defensive possessions",
  "Off Poss"    = "Offensive possessions",
  "Def Poss"    = "Defensive possessions",
  "Min"         = "Minutes played",
  "GP"          = "Games played",
  "Poss On Floor" = "Total possessions while player on court",
  "# Starters"  = "Number of starters in this lineup",
  # Game context
  "GN"          = "Team's sequential game number this season",
  "W/L"         = "Win or Loss",
  "+/-"         = "Point differential while lineup was on court",
  "Off Pace"    = "Offensive possessions per 40 minutes",
  "Def Pace"    = "Defensive possessions per 40 minutes",
  # Traditional
  "PTS" = "Points", "REB" = "Rebounds", "OREB" = "Offensive rebounds",
  "DREB" = "Defensive rebounds", "AST" = "Assists", "STL" = "Steals",
  "BLK" = "Blocks", "TOV" = "Turnovers",
  "FGM" = "Field goals made", "FGA" = "Field goal attempts",
  "3PM" = "Three-pointers made", "3PA" = "Three-point attempts",
  "FTM" = "Free throws made", "FTA" = "Free throw attempts"
)

FILTER_TOOLTIPS <- c(
  "min_poss_side"     = "Minimum OFF + DEF possessions to appear in table",
  "min_on_poss"       = "Minimum ON-court possessions for percentile ranking",
  "own_starters"      = "Filter by number of starters in the team's lineup",
  "opp_starters"      = "Filter by number of starters in the opposing lineup",
  "gn"                = "Team's sequential game number this season",
  "last_n"            = "Only include the team's most recent N games",
  "opp_strength"      = "Filter games by opponent's league ranking",
  "clutch"            = "Close-game situations: margin, time remaining, score status",
  "group_size"        = "Number of players in each lineup combination (2-5)",
  "players_on"        = "Lineups must include all selected players",
  "players_off"       = "Lineups must exclude all selected players",
  "min_poss_lineup"   = "Minimum total possessions for lineup to appear",
  "view_summary"      = "PPP ratings and shooting splits",
  "view_ff"           = "TS%, OREB%, TOV%, FTR breakdown",
  "view_traditional"  = "Box-score counting stats"
)
```

- [ ] **Step 3: Add `tt()` helper function and `HEADER_TOOLTIP_JS`**

Insert right after the `FILTER_TOOLTIPS` definition:

```r
# Tooltip-wrapped label for sidebar inputs
tt <- function(label, key) {
  tip <- FILTER_TOOLTIPS[[key]]
  if (is.null(tip)) return(label)
  tags$span(label, `data-tooltip` = tip)
}

# Shared JS headerCallback for DT tables — injects data-tooltip on th elements
HEADER_TOOLTIP_JS <- DT::JS(paste0(
  "function(thead, data, start, end, display) {",
  "  var tips = ", jsonlite::toJSON(as.list(COLUMN_TOOLTIPS), auto_unbox = TRUE), ";",
  "  $(thead).find('th').each(function() {",
  "    var txt = $(this).text().trim();",
  "    if (tips[txt]) $(this).attr('data-tooltip', tips[txt]);",
  "  });",
  "}"
))
```

Note: This requires `jsonlite` which is typically available in Shiny apps. Check that it's loaded. If not, add `library(jsonlite)` at the top of `global.R`.

- [ ] **Step 4: Verify jsonlite is available**

Check if `jsonlite` is already loaded or used anywhere:

```bash
grep -r "jsonlite" app/R/
```

If not present, add `library(jsonlite)` after the existing library block at the top of `global.R` (after line 11).

- [ ] **Step 5: Commit**

```bash
git add app/R/global.R
git commit -m "feat: add tooltip CSS, definitions, and helpers to global.R"
```

---

### Task 2: Add headerCallback to all DT tables

**Files:**
- Modify: `app/R/server_tab1.R` (2 DT calls — summary + FF)
- Modify: `app/R/server_tab2.R` (4 DT calls — summary, FF, modal summary, modal FF)
- Modify: `app/R/server_tab3.R` (3 DT calls — summary, FF, traditional)
- Modify: `app/R/server_tab4.R` (2 DT calls — summary + FF)
- Modify: `app/R/server_tab5_traditional.R` (1 DT call)
- Modify: `app/R/server_tab6_team_stats.R` (1 DT call)

The same change applies to every `datatable()` call: add `headerCallback = HEADER_TOOLTIP_JS` inside the `options = list(...)`.

- [ ] **Step 1: Update server_tab1.R — Summary DT**

In `server_tab1.R`, find the summary `datatable()` call (around line 823):

```r
dt <- datatable(df, container = sketch_summary, rownames = FALSE,
                options = list(dom = "tip", pageLength = 30, scrollX = TRUE,
```

Add `headerCallback = HEADER_TOOLTIP_JS,` as the first item in the options list:

```r
dt <- datatable(df, container = sketch_summary, rownames = FALSE,
                options = list(headerCallback = HEADER_TOOLTIP_JS,
                               dom = "tip", pageLength = 30, scrollX = TRUE,
```

- [ ] **Step 2: Update server_tab1.R — FF DT**

Find the FF `datatable()` call (around line 1021):

```r
      dt <- datatable(df_final,
                      container = sketch_ff, rownames = FALSE, escape = FALSE,
                      options = list(
                        dom = "t", pageLength = 50, deferRender = TRUE, scrollX = TRUE,
```

Add `headerCallback = HEADER_TOOLTIP_JS,` as first option:

```r
                      options = list(
                        headerCallback = HEADER_TOOLTIP_JS,
                        dom = "t", pageLength = 50, deferRender = TRUE, scrollX = TRUE,
```

- [ ] **Step 3: Update server_tab2.R — FF DT (line ~657)**

Find:
```r
      dt <- DT::datatable(df, container = sketch_ff, rownames = FALSE, escape = FALSE,
                          options = list(
                            dom = "tip", pageLength = 50,
```

Add `headerCallback = HEADER_TOOLTIP_JS,` as first option:
```r
                          options = list(
                            headerCallback = HEADER_TOOLTIP_JS,
                            dom = "tip", pageLength = 50,
```

- [ ] **Step 4: Update server_tab2.R — Summary DT (line ~858)**

Find (long single line):
```r
      dt <- DT::datatable(df, colnames = final_labels, rownames = FALSE, escape = FALSE, filter = "top", options = list(pageLength = 50,
```

Add `headerCallback = HEADER_TOOLTIP_JS,` as first option in the options list:
```r
      dt <- DT::datatable(df, colnames = final_labels, rownames = FALSE, escape = FALSE, filter = "top", options = list(headerCallback = HEADER_TOOLTIP_JS, pageLength = 50,
```

- [ ] **Step 5: Update server_tab2.R — Modal FF DT (line ~1037)**

Find:
```r
        dt_ff <- DT::datatable(disp_ff, container = sketch_ff, rownames = FALSE, escape = FALSE,
```

Add `headerCallback = HEADER_TOOLTIP_JS,` as first option in its `options = list(...)`.

- [ ] **Step 6: Update server_tab2.R — Modal Summary DT (line ~1234)**

Find:
```r
        dt_m <- DT::datatable(disp_m, container = sketch_m, rownames = FALSE, escape = FALSE,
```

Add `headerCallback = HEADER_TOOLTIP_JS,` as first option in its `options = list(...)`.

- [ ] **Step 7: Update server_tab3.R — Summary DT (line ~1790)**

Find the long single-line `datatable` call:
```r
      dt <- datatable(disp_df, colnames = pretty_names, rownames = FALSE, escape = FALSE, options = list(dom = "t", pageLength = 50,
```

Add `headerCallback = HEADER_TOOLTIP_JS,` as first option:
```r
      dt <- datatable(disp_df, colnames = pretty_names, rownames = FALSE, escape = FALSE, options = list(headerCallback = HEADER_TOOLTIP_JS, dom = "t", pageLength = 50,
```

- [ ] **Step 8: Update server_tab3.R — FF DT (line ~1698)**

Find:
```r
      dt <- DT::datatable(disp_ff, container = sketch_ff, rownames = FALSE, escape = FALSE,
                          options = list(
                            dom = "t", pageLength = 50,
```

Add `headerCallback = HEADER_TOOLTIP_JS,`:
```r
                          options = list(
                            headerCallback = HEADER_TOOLTIP_JS,
                            dom = "t", pageLength = 50,
```

- [ ] **Step 9: Update server_tab3.R — Traditional DT (line ~1486)**

Find:
```r
      dt <- datatable(
        disp, rownames = FALSE,
        escape = FALSE,
        options = list(
          dom = "t", pageLength = 50, deferRender = TRUE, scrollX = TRUE, scrollY = "70vh", scrollCollapse = TRUE,
```

Add `headerCallback = HEADER_TOOLTIP_JS,`:
```r
        options = list(
          headerCallback = HEADER_TOOLTIP_JS,
          dom = "t", pageLength = 50, deferRender = TRUE, scrollX = TRUE, scrollY = "70vh", scrollCollapse = TRUE,
```

- [ ] **Step 10: Update server_tab4.R — Summary DT (line ~534)**

Find:
```r
      dt <- DT::datatable(disp, container = sketch, rownames = FALSE, escape = FALSE,
                          options = list(
                            dom = "tip", pageLength = 50,
```

Add `headerCallback = HEADER_TOOLTIP_JS,`:
```r
                          options = list(
                            headerCallback = HEADER_TOOLTIP_JS,
                            dom = "tip", pageLength = 50,
```

- [ ] **Step 11: Update server_tab4.R — FF DT (line ~611)**

Find:
```r
      dt <- DT::datatable(disp, container = sketch, rownames = FALSE, escape = FALSE,
                          options = list(
                            dom = "tip", pageLength = 50,
                            deferRender = TRUE, scrollX = TRUE,
```

Add `headerCallback = HEADER_TOOLTIP_JS,`:
```r
                          options = list(
                            headerCallback = HEADER_TOOLTIP_JS,
                            dom = "tip", pageLength = 50,
                            deferRender = TRUE, scrollX = TRUE,
```

- [ ] **Step 12: Update server_tab5_traditional.R (line ~609)**

Find:
```r
    dt <- DT::datatable(
      disp,
      rownames = FALSE,
      options = list(
        dom = "tip",
```

Add `headerCallback = HEADER_TOOLTIP_JS,`:
```r
      options = list(
        headerCallback = HEADER_TOOLTIP_JS,
        dom = "tip",
```

- [ ] **Step 13: Find and update server_tab6 DT call**

Find the `datatable()` call in the server_tab6 file (check `app/R/` for `server_tab6*.R`), and add `headerCallback = HEADER_TOOLTIP_JS,` to its options list following the same pattern.

- [ ] **Step 14: Commit**

```bash
git add app/R/server_tab1.R app/R/server_tab2.R app/R/server_tab3.R app/R/server_tab4.R app/R/server_tab5_traditional.R app/R/server_tab6*.R
git commit -m "feat: add tooltip headerCallback to all DT tables"
```

---

### Task 3: Add tooltip labels to sidebar filters (ui_tab1 through ui_tab7)

**Files:**
- Modify: `app/R/ui_tab1_onoff.R`
- Modify: `app/R/ui_tab2_lineup.R`
- Modify: `app/R/ui_tab3_team.R`
- Modify: `app/R/ui_tab4_gamelogs.R`
- Modify: `app/R/ui_tab5_traditional.R`
- Modify: `app/R/ui_tab6_team_stats.R`
- Modify: `app/R/ui_tab7_compare.R`

Replace plain-string labels with `tt()` calls for the relevant filter inputs. The `tt()` function wraps the label in a `<span data-tooltip="...">` which triggers the CSS tooltip.

**Important:** Only wrap labels where the tooltip adds value — skip obvious labels like "Teams", "Date range", "Opponents", etc.

- [ ] **Step 1: Update ui_tab1_onoff.R**

Make these replacements:

| Line | Old | New |
|------|-----|-----|
| ~85 | `"Min possessions per side (eligibility):"` | `tt("Min possessions per side (eligibility):", "min_poss_side")` |
| ~86 | `"Minimum ON possessions (for ranking):"` | `tt("Minimum ON possessions (for ranking):", "min_on_poss")` |
| ~35-36 | `"Own lineup starters"` (in selectInput label) | `tt("Own lineup starters", "own_starters")` |
| ~39-40 | `"Opponent lineup starters"` (in selectInput label) | `tt("Opponent lineup starters", "opp_starters")` |
| ~67-68 | `"From Game Number (GN)"` | `tt("From Game Number (GN)", "gn")` |
| ~69-70 | `"To Game Number (GN)"` | `tt("To Game Number (GN)", "gn")` |
| ~72 | `"Last N Team Games"` | `tt("Last N Team Games", "last_n")` |
| ~76 | `"Opponent Strength"` (accordion_panel title) | `tt("Opponent Strength", "opp_strength")` |

- [ ] **Step 2: Update ui_tab2_lineup.R**

Same filter labels as Tab 1, plus lineup-specific:

| Label | Key |
|-------|-----|
| `"Minimum possessions (Off + Def)"` | `"min_poss_lineup"` |
| `"Group size"` | `"group_size"` |
| `"Players On (exact/contains)"` | `"players_on"` |
| `"Players Off (exclude any)"` | `"players_off"` |
| `"Own lineup starters"` | `"own_starters"` |
| `"Opponent lineup starters"` | `"opp_starters"` |
| `"From Game Number (GN)"` | `"gn"` |
| `"To Game Number (GN)"` | `"gn"` |
| `"Last N Team Games"` | `"last_n"` |
| `"Opponent Strength"` | `"opp_strength"` |
| `"Clutch"` | `"clutch"` |

- [ ] **Step 3: Update ui_tab3_team.R**

Same shared filters:

| Label | Key |
|-------|-----|
| `"Clutch"` | `"clutch"` |
| `"Own lineup starters"` | `"own_starters"` |
| `"Opponent lineup starters"` | `"opp_starters"` |
| `"From Game Number (GN)"` | `"gn"` |
| `"To Game Number (GN)"` | `"gn"` |
| `"Last N Team Games"` | `"last_n"` |
| `"Opponent Strength"` | `"opp_strength"` |

- [ ] **Step 4: Update ui_tab4_gamelogs.R**

Same shared filters:

| Label | Key |
|-------|-----|
| `"Own lineup starters"` | `"own_starters"` |
| `"Opponent lineup starters"` | `"opp_starters"` |
| `"From Game Number (GN)"` | `"gn"` |
| `"To Game Number (GN)"` | `"gn"` |
| `"Last N Team Games"` | `"last_n"` |

- [ ] **Step 5: Update ui_tab5_traditional.R**

| Label | Key |
|-------|-----|
| `"From Game Number (GN)"` | `"gn"` |
| `"To Game Number (GN)"` | `"gn"` |
| `"Last N Team Games"` | `"last_n"` |
| `"Opponent Strength"` | `"opp_strength"` |
| `"Enable clutch filter"` (the checkbox label) | `"clutch"` |

- [ ] **Step 6: Update ui_tab6_team_stats.R**

| Label | Key |
|-------|-----|
| `"Clutch"` | `"clutch"` |
| `"From Game Number (GN)"` | `"gn"` |
| `"To Game Number (GN)"` | `"gn"` |
| `"Last N Team Games"` | `"last_n"` |
| `"Opponent Strength"` | `"opp_strength"` |

- [ ] **Step 7: Update ui_tab7_compare.R**

| Label | Key |
|-------|-----|
| `"Min possessions"` (line ~55) | `"min_poss_lineup"` |
| `"Clutch"` (cmp_a_clutch, cmp_b_clutch) | `"clutch"` |
| `"From Game Number (GN)"` / `"To Game Number (GN)"` | `"gn"` |

- [ ] **Step 8: Commit**

```bash
git add app/R/ui_tab*.R
git commit -m "feat: add tooltip labels to sidebar filters across all tabs"
```

---

### Task 4: Add tooltips to view-mode radio buttons

**Files:**
- Modify: `app/R/ui_tab1_onoff.R`
- Modify: `app/R/ui_tab2_lineup.R`
- Modify: `app/R/ui_tab3_team.R`
- Modify: `app/R/ui_tab4_gamelogs.R`

Shiny's `radioButtons()` doesn't support per-choice tooltips natively. The simplest approach is to add a small JS snippet in `shared_head_tags()` that applies `data-tooltip` to the radio `<label>` elements based on their text content.

- [ ] **Step 1: Add JS snippet to `shared_head_tags()` in global.R**

In `global.R`, modify `shared_head_tags()` to include a script that sets tooltips on radio labels after Shiny renders them:

```r
shared_head_tags <- function() {
  tags$head(
    tags$meta(name = "viewport", content = "width=device-width, initial-scale=1, maximum-scale=1"),
    tags$link(rel = "stylesheet", href = "https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=JetBrains+Mono:wght@400;500&family=Inter:wght@400;500;600;700&display=swap"),
    tags$link(rel = "stylesheet", href = "https://cdn.jsdelivr.net/npm/bootstrap-icons@1.11.3/font/bootstrap-icons.min.css"),
    tags$style(shared_css),
    tags$script(HTML("
      $(function() {
        var viewTips = {
          'Summary': 'PPP ratings and shooting splits',
          'Four Factors': 'TS%, OREB%, TOV%, FTR breakdown',
          'Traditional': 'Box-score counting stats'
        };
        $('.view-mode-container .radio label, .view-mode-container .shiny-options-group label').each(function() {
          var txt = $(this).text().trim();
          if (viewTips[txt]) $(this).attr('data-tooltip', viewTips[txt]);
        });
      });
    "))
  )
}
```

This targets all radio labels inside `.view-mode-container` (which wraps the Summary/FF/Traditional radio buttons on Tabs 1-4).

- [ ] **Step 2: Verify CSS tooltip positioning for radio labels**

The existing tooltip CSS positions `::after` above the element by default. For radio buttons inside the sidebar (top of the page), this should be fine. No CSS changes needed.

- [ ] **Step 3: Commit**

```bash
git add app/R/global.R
git commit -m "feat: add tooltips to view-mode radio buttons"
```

---

### Task 5: Smoke test

**Files:** None (manual testing)

- [ ] **Step 1: Run the Shiny app locally**

```bash
RSCRIPT="/c/Program Files/R/R-4.4.2/bin/Rscript.exe"
"$RSCRIPT" -e "shiny::runApp('app')"
```

- [ ] **Step 2: Verify DT column header tooltips**

Navigate through each tab and hover over column headers. Verify:
- Tab 1 Summary: "PPP", "Net", "Off Shot", "On Poss" show tooltips
- Tab 1 FF: "TS%", "OREB%", "TOV%", "FTR", "Diff" show tooltips
- Tab 2 Summary: "Off PPP", "Net RTG", "Total Poss", "Min" show tooltips
- Tab 2 FF: same FF metrics show tooltips
- Tab 3 Summary: "Off PPP", "Def PPP", "Net Rtg", "GP" show tooltips
- Tab 3 FF: same FF metrics
- Tab 3 Traditional: "PTS", "eFG%", "TS%" show tooltips
- Tab 4 Summary & FF: same metrics
- Tab 5: "GP", "PTS", "eFG%", "TS%", "Poss On Floor" show tooltips
- Tab 6: same traditional stat tooltips

- [ ] **Step 3: Verify sidebar filter tooltips**

Hover over filter labels:
- Tab 1: "Min possessions per side", "Minimum ON possessions", "Own lineup starters", "From Game Number (GN)", "Last N Team Games", "Opponent Strength" accordion title
- Tab 2: "Minimum possessions (Off + Def)", "Group size", "Players On", "Players Off", "Clutch"
- Other tabs: shared filter labels

- [ ] **Step 4: Verify view-mode radio tooltips**

Hover over "Summary", "Four Factors", "Traditional" radio labels on Tabs 1-4.

- [ ] **Step 5: Fix any issues found**

Address tooltip positioning, missing tooltips, or styling problems.

- [ ] **Step 6: Commit any fixes**

```bash
git add -A
git commit -m "fix: tooltip adjustments from smoke test"
```
