# Filter and Navigation UX Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make the filter chips the way filters are reached, give the table back the width the sidebar spends, let any row pivot to another tab carrying its context, and stop Home asking the questions its hub already answers.

**Architecture:** One shared chips row on every data tab becomes the single place a filter panel is opened, closed, and jumped into. Collapse is a body class applied over JS-tagged layout columns, so no tab file learns about it. Cross-tab pivots generalise the existing `shared$pending_*` handoff into one `pending_nav` value with a `consume_pending_nav()` reader, and entity identity travels on `<tr>` data attributes rather than injected HTML so the fail-closed DT escaping contract is untouched.

**Tech Stack:** R 4.4.2, Shiny, bslib (BS5), DT/DataTables, testthat 3e, vanilla JS (no build step), `localStorage`.

**Spec:** `docs/superpowers/specs/2026-09-02-app-design-review-design.md`

**Depends on:** `docs/superpowers/plans/2026-09-02-design-system-pass.md`. That plan introduces the `--ibpl-*` token layer; new CSS here uses it, so run it first. Nothing else is shared.

## Global Constraints

- Set `IBPL_CACHE_UI=false` for any manual app run. `www/app.css` and `www/app.js` are read at UI build time, so with the cache on an edit needs an app restart, not a browser reload.
- Launch with Run App / `runApp('app')`, **never** select-all + Ctrl+Enter. Health check: the served page contains 11 `nav-link` occurrences.
- Run tests from the repo root with `"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" scripts/test_all.R`. Single file from `app/`: `"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/<file>')"`.
- **Shiny input ids are unique per session.** A chip cannot host a live duplicate of a sidebar control. Chips therefore reveal and focus the owning input; they never re-declare it.
- **DT escaping is fail-closed** (`dt_escape_except()`, `app/R/helpers.R:10`) and `test-dt-security.R` fails the build on `escape = FALSE` anywhere in a `server_tab*.R`. Never widen an escape allowlist to carry interaction markup — put it on the `<tr>` instead.
- **Never put a data reactive in an `observeEvent()` trigger.** Triggers evaluate on every session and observers are not suspended by tab visibility; this cost a whole EuroLeague season pull on every Home visit (fixed in 4487c2f). Keep plain inputs in triggers and gate the handler on `input$main_tabs`.
- `""` is the blank sentinel for every single-select filter, and the clear-chip id is always `<prefix>_clear_<thing>`.
- Israeli and EuroLeague tabs share code. Generalise the existing function, never write a parallel `euro_` version.
- Branch: `shiny/filter-nav-ux`, created from `main` (or from the merged design-system branch) at Task 1.

---

### Task 1: Route every tab's chips through the shared row

Five tabs render `uiOutput("<prefix>_filter_chips")` bare and four go through
`filter_chips_row()`. The collapse control added in Task 2 lives in that shared
component, so every tab has to reach it first. This is a **move**: the rendered
output gains a wrapper and nothing else.

**Files:**
- Modify: `app/R/ui_tab3_team.R:193`, `app/R/ui_tab4_gamelogs.R:150`, `app/R/ui_tab5_traditional.R:97`, `app/R/ui_tab9_euro_team.R:92`, `app/R/ui_tab11_euro_gamelogs.R:72`
- Create: `app/tests/testthat/test-chips-row.R`

**Interfaces:**
- Consumes: `filter_chips_row(chips_output_id, ...)` from `app/R/global.R:1085`, unchanged.
- Produces: every data tab's chips are wrapped in `div.chips-row > div.chips-row-chips`.

- [ ] **Step 1: Create the branch**

```bash
cd /c/Users/ariel/documents/on_off_israel_pbp
git checkout -b shiny/filter-nav-ux
"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" scripts/test_all.R
```
Expected: 0 failures. Do not proceed from a red baseline.

- [ ] **Step 2: Write the failing test**

Create `app/tests/testthat/test-chips-row.R`:

```r
CHIPS_TABS <- list(
  list(file = "ui_tab1_onoff.R",         output = "on_filter_chips"),
  list(file = "ui_tab2_lineup.R",        output = "ld_filter_chips"),
  list(file = "ui_tab3_team.R",          output = "tr_filter_chips"),
  list(file = "ui_tab4_gamelogs.R",      output = "gl_filter_chips"),
  list(file = "ui_tab5_traditional.R",   output = "ts_filter_chips"),
  list(file = "ui_tab8_euro.R",          output = "euro_filter_chips"),
  list(file = "ui_tab9_euro_team.R",     output = "euroteam_filter_chips"),
  list(file = "ui_tab10_euro_lineups.R", output = "euro_ld_filter_chips"),
  list(file = "ui_tab11_euro_gamelogs.R", output = "eurogl_filter_chips")
)

test_that("every data tab reaches its chips through the shared row", {
  for (tab in CHIPS_TABS) {
    txt <- read_repo_txt("R", tab$file)
    expect_true(
      grepl(sprintf('filter_chips_row(\n          "%s"', tab$output), txt, fixed = TRUE) ||
        grepl(sprintf('filter_chips_row("%s"', tab$output), txt, fixed = TRUE) ||
        grepl(sprintf('filter_chips_row(\n        "%s"', tab$output), txt, fixed = TRUE),
      info = paste(tab$file, "does not wrap", tab$output, "in filter_chips_row()")
    )
    expect_false(
      grepl(sprintf('uiOutput("%s")', tab$output), txt, fixed = TRUE),
      info = paste(tab$file, "still renders", tab$output, "bare")
    )
  }
})
```

- [ ] **Step 3: Run the test to verify it fails**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-chips-row.R')"
```
Expected: FAIL for `ui_tab3_team.R`, `ui_tab4_gamelogs.R`, `ui_tab5_traditional.R`, `ui_tab9_euro_team.R`, `ui_tab11_euro_gamelogs.R`.

- [ ] **Step 4: Wrap the five bare call sites**

In each file, replace the bare output with the shared wrapper. `ui_tab3_team.R:193`:

```r
        uiOutput("tr_filter_chips"),
```
becomes
```r
        filter_chips_row("tr_filter_chips"),
```

Apply the identical one-line change at `ui_tab4_gamelogs.R:150` (`gl_filter_chips`), `ui_tab5_traditional.R:97` (`ts_filter_chips`), `ui_tab9_euro_team.R:92` (`euroteam_filter_chips`) and `ui_tab11_euro_gamelogs.R:72` (`eurogl_filter_chips`).

`filter_chips_row()` renders no controls div when called with no `...`, so the only DOM change is the two wrapper divs.

- [ ] **Step 5: Run the tests to verify they pass**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-chips-row.R')"
cd /c/Users/ariel/documents/on_off_israel_pbp && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" scripts/test_all.R
```
Expected: 0 failures. `test-tab-wiring.R` and `test-tab-parse.R` parse every UI file and must stay green.

- [ ] **Step 6: Confirm the chips still render on all nine tabs**

```bash
IBPL_CACHE_UI=false "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "shiny::runApp('app', port = 7666, launch.browser = FALSE)"
```
Visit each of the nine tabs under both leagues and confirm the chip bar is present, sits above the table, and its season chip reads correctly.

- [ ] **Step 7: Commit**

```bash
git add app/R/ui_tab3_team.R app/R/ui_tab4_gamelogs.R app/R/ui_tab5_traditional.R \
        app/R/ui_tab9_euro_team.R app/R/ui_tab11_euro_gamelogs.R \
        app/tests/testthat/test-chips-row.R
git commit -m "refactor: route every data tab's chips through filter_chips_row()

Five tabs rendered the chips output bare while four used the shared wrapper.
Everything the chips row grows from here has to land in one place, so the
nine tabs go through one component; a contract test keeps a tenth from
drifting."
```

---

### Task 2: Collapsible filter panel

The sidebar spends a quarter of the viewport on controls that are mostly at
defaults. Make it collapsible on every tab at once, without any tab file
learning about it.

**Files:**
- Modify: `app/R/global.R` (`filter_chips_row()`)
- Modify: `app/www/app.css` (append)
- Modify: `app/www/app.js` (append an IIFE)
- Create: `app/tests/testthat/test-filter-collapse.R`

**Interfaces:**
- Consumes: `filter_chips_row()` from Task 1.
- Produces: a `button.js-filters-toggle` in every chips row; body class `filters-collapsed`; JS-applied column classes `ibpl-filter-col` and `ibpl-main-col`; `localStorage` key `ibpl_filters_collapsed`.

- [ ] **Step 1: Write the failing test**

Create `app/tests/testthat/test-filter-collapse.R`:

```r
source(repo_file("R", "global.R"), local = TRUE)

test_that("the chips row carries an accessible filter toggle", {
  html <- htmltools::renderTags(filter_chips_row("demo_chips"))$html

  expect_match(html, "js-filters-toggle", fixed = TRUE)
  expect_match(html, 'aria-expanded="true"', fixed = TRUE)
  expect_match(html, "Filters", fixed = TRUE)
})

test_that("collapse is driven by a body class over tagged columns", {
  css <- read_repo_txt("www", "app.css")
  js <- read_repo_txt("www", "app.js")

  expect_true(grepl("body.filters-collapsed", css, fixed = TRUE))
  expect_true(grepl("ibpl-filter-col", css, fixed = TRUE))
  expect_true(grepl("ibpl-main-col", css, fixed = TRUE))

  # The columns are tagged in JS by looking for the sidebar's .well, so no
  # tab file has to be edited and no reliance on :has() is needed.
  expect_true(grepl("ibpl-filter-col", js, fixed = TRUE))
  expect_true(grepl("ibpl_filters_collapsed", js, fixed = TRUE))
})

test_that("the collapse state is remembered per browser", {
  js <- read_repo_txt("www", "app.js")

  expect_true(grepl("localStorage", js, fixed = TRUE))
  # Storage can throw outright in a private window; reads must be guarded.
  expect_true(grepl("try {", js, fixed = TRUE))
})
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-filter-collapse.R')"
```
Expected: FAIL — `js-filters-toggle` does not exist.

- [ ] **Step 3: Add the toggle to the shared chips row**

In `app/R/global.R`, replace `filter_chips_row()` at line 1085:

```r
filter_chips_row <- function(chips_output_id, ...) {
  controls <- list(...)
  tags$div(
    class = "chips-row",
    tags$div(class = "chips-row-chips", uiOutput(chips_output_id)),
    if (length(controls)) tags$div(class = "chips-row-controls", controls)
  )
}
```

with:

```r
# The chips row is where filters are read, so it is also where they are
# reached. The toggle is pure client state -- it collapses a layout column and
# nothing else -- so it is a plain button rather than a Shiny input: no
# round trip, and no filter state to keep in step.
filter_chips_row <- function(chips_output_id, ...) {
  controls <- list(...)
  tags$div(
    class = "chips-row",
    tags$button(
      type = "button",
      class = "chips-filters-toggle js-filters-toggle",
      `aria-expanded` = "true",
      `aria-label` = "Hide the filter panel",
      tags$i(class = "bi bi-sliders", `aria-hidden` = "true"),
      tags$span(class = "chips-filters-toggle-label", "Filters")
    ),
    tags$div(class = "chips-row-chips", uiOutput(chips_output_id)),
    if (length(controls)) tags$div(class = "chips-row-controls", controls)
  )
}
```

- [ ] **Step 4: Add the collapse styles**

Append to `app/www/app.css`:

```css

/* ---- Collapsible filter panel ---------------------------------------------
   The sidebar holds ten tabs' worth of controls that are usually at their
   defaults, against tables that want every pixel. Collapse is a body class
   over two columns tagged by app.js, so no tab file knows this exists and the
   ten sidebarLayout() calls stay untouched.
   -------------------------------------------------------------------------- */
.chips-filters-toggle {
  display: inline-flex; align-items: center; gap: 6px;
  padding: 4px 10px; border-radius: 16px;
  font-size: 0.78rem; font-weight: 600; line-height: 1.3;
  border: 1px solid var(--ibpl-border);
  background: var(--ibpl-surface-3);
  color: var(--ibpl-text-body);
  cursor: pointer; white-space: nowrap;
  transition: border-color .15s ease, color .15s ease;
}
.chips-filters-toggle:hover {
  border-color: var(--ibpl-accent);
  color: var(--ibpl-accent);
}
.chips-filters-toggle:focus-visible {
  outline: 2px solid var(--ibpl-accent);
  outline-offset: 2px;
}
.chips-filters-toggle[aria-expanded="false"] {
  border-style: dashed;
  color: var(--ibpl-text-muted);
}

body.filters-collapsed .ibpl-filter-col { display: none; }
body.filters-collapsed .ibpl-main-col {
  flex: 0 0 100%;
  max-width: 100%;
  width: 100%;
}

/* The panel is already collapsed behind the tab's own Show Filters button on
   narrow viewports, so the toggle would be a second control for one thing. */
@media (max-width: 767px) {
  .chips-filters-toggle { display: none; }
}
```

- [ ] **Step 5: Add the JS module**

Append to `app/www/app.js`:

```javascript

/* ---- Collapsible filter panel ---------------------------------------------
   Tags the two columns of each tab's sidebarLayout so CSS can collapse them,
   then toggles a body class. The sidebar column is identified by the .well it
   contains rather than by a class added in R, so the ten tab files stay
   untouched and no :has() support is assumed.

   State is client-only and persists per browser. Storage can throw outright
   in a private window, so every access is guarded and a failure just means
   the panel opens expanded.
   -------------------------------------------------------------------------- */
(function() {
  var STORE_KEY = "ibpl_filters_collapsed";

  function readStored() {
    try {
      return window.localStorage.getItem(STORE_KEY) === "1";
    } catch (e) {
      return false;
    }
  }

  function writeStored(collapsed) {
    try {
      window.localStorage.setItem(STORE_KEY, collapsed ? "1" : "0");
    } catch (e) {
      /* private window or blocked site data: the toggle still works, it just
         does not survive a reload. */
    }
  }

  function tagColumns() {
    var wells = document.querySelectorAll(".tab-pane .well");
    for (var i = 0; i < wells.length; i++) {
      var col = wells[i].closest("div[class*='col-sm-']");
      if (!col || col.classList.contains("ibpl-filter-col")) continue;
      col.classList.add("ibpl-filter-col");
      var main = col.nextElementSibling;
      if (main && main.className.indexOf("col-sm-") !== -1) {
        main.classList.add("ibpl-main-col");
      }
    }
  }

  function syncToggles(collapsed) {
    var buttons = document.querySelectorAll(".js-filters-toggle");
    for (var i = 0; i < buttons.length; i++) {
      buttons[i].setAttribute("aria-expanded", collapsed ? "false" : "true");
      buttons[i].setAttribute(
        "aria-label",
        collapsed ? "Show the filter panel" : "Hide the filter panel"
      );
    }
  }

  function apply(collapsed) {
    document.body.classList.toggle("filters-collapsed", collapsed);
    syncToggles(collapsed);
  }

  function init() {
    tagColumns();
    apply(readStored());

    document.addEventListener("click", function(e) {
      var btn = e.target.closest(".js-filters-toggle");
      if (!btn) return;
      e.preventDefault();
      var collapsed = !document.body.classList.contains("filters-collapsed");
      apply(collapsed);
      writeStored(collapsed);
      // DataTables sizes its header to the container width, so a column that
      // just changed width has to be told to remeasure.
      if (window.jQuery && window.jQuery.fn.dataTable) {
        window.jQuery.fn.dataTable.tables({ visible: true, api: true }).columns.adjust();
      }
    });

    // Tabs render lazily, so a tab shown for the first time brings untagged
    // columns with it.
    if (window.jQuery) {
      window.jQuery(document).on("shown.bs.tab shiny:value", function() {
        tagColumns();
        apply(document.body.classList.contains("filters-collapsed"));
      });
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
```

- [ ] **Step 6: Run the tests to verify they pass**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-filter-collapse.R')"
cd /c/Users/ariel/documents/on_off_israel_pbp && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" scripts/test_all.R
```
Expected: 0 failures.

- [ ] **Step 7: Verify in the browser**

```bash
IBPL_CACHE_UI=false "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "shiny::runApp('app', port = 7666, launch.browser = FALSE)"
```
Confirm: clicking **Filters** hides the sidebar and the table expands to full width with its header still aligned; clicking again restores it; the state survives a page reload; switching tabs keeps the state and the newly-shown tab's sidebar is also collapsed; the button is reachable and operable by keyboard with a visible focus ring; at a viewport under 768px the toggle is hidden and the tab's own "Show Filters" button still works.

- [ ] **Step 8: Commit**

```bash
git add app/R/global.R app/www/app.css app/www/app.js app/tests/testthat/test-filter-collapse.R
git commit -m "feat: collapsible filter panel on every data tab

The sidebar spent a quarter of the viewport on controls usually left at
their defaults. Collapse is a body class over two columns tagged by app.js
from the sidebar's .well, so the ten sidebarLayout() calls are untouched.
State persists per browser behind guarded storage access."
```

---

### Task 3: Chips reveal the control they describe

A chip says what a filter is set to and offers to clear it, but changing it
means crossing to the far-left sidebar. Make the chip the way in.

**Files:**
- Modify: `app/R/global.R` (`make_chip()`, `make_season_chip()`, `build_filter_chips()`)
- Modify: `app/www/app.css` (append)
- Modify: `app/www/app.js` (extend the delegated click handler)
- Create: `app/tests/testthat/test-chip-focus.R`

**Interfaces:**
- Consumes: `filter_chips_row()` and the collapse module from Task 2.
- Produces: `make_chip(label, clear_id, css_class = "", focus_id = NULL)`; chips carry `data-chip-focus="<input id>"`; `build_filter_chips()` gains `input_ids = NULL` (a named list overriding the `<prefix>_<thing>` default).

- [ ] **Step 1: Write the failing test**

Create `app/tests/testthat/test-chip-focus.R`:

```r
source(repo_file("R", "global.R"), local = TRUE)

test_that("make_chip carries a focus target when given one", {
  with_focus <- htmltools::renderTags(
    make_chip("Wins", "tr_clear_outcome", "chip-game", focus_id = "tr_outcome")
  )$html
  without <- htmltools::renderTags(
    make_chip("Wins", "tr_clear_outcome", "chip-game")
  )$html

  expect_match(with_focus, 'data-chip-focus="tr_outcome"', fixed = TRUE)
  expect_match(with_focus, "chip-focusable", fixed = TRUE)
  expect_false(grepl("data-chip-focus", without, fixed = TRUE))
})

test_that("clearing a chip is still reachable independently of focusing it", {
  html <- htmltools::renderTags(
    make_chip("Wins", "tr_clear_outcome", "chip-game", focus_id = "tr_outcome")
  )$html

  # The x keeps its own event id; focusing must not swallow the clear.
  expect_match(html, 'data-shiny-event="tr_clear_outcome"', fixed = TRUE)
})

test_that("the season chip is not focusable", {
  # Season lives in the navbar, not the filter panel, and is never cleared.
  html <- htmltools::renderTags(make_season_chip("2026"))$html

  expect_false(grepl("data-chip-focus", html, fixed = TRUE))
  expect_false(grepl("chip-x", html, fixed = TRUE))
})

test_that("app.js opens the panel before focusing a hidden control", {
  js <- read_repo_txt("www", "app.js")

  expect_true(grepl("data-chip-focus", js, fixed = TRUE))
  expect_true(grepl("chipFocus", js, fixed = TRUE))
  # A control inside a collapsed panel cannot take focus, so the panel has to
  # be opened first.
  expect_true(grepl("filters-collapsed", js, fixed = TRUE))
})
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-chip-focus.R')"
```
Expected: FAIL — `make_chip()` has no `focus_id` argument, so the call errors on an unused argument.

- [ ] **Step 3: Give `make_chip()` a focus target**

In `app/R/global.R`, replace `make_chip()` at line 834:

```r
make_chip <- function(label, clear_id, css_class = "") {
  tags$span(
    class = paste("filter-chip", css_class),
    label,
    tags$button(
      class = "chip-x",
      type = "button",
      `data-shiny-event` = clear_id,
      HTML("&times;")
    )
  )
}
```

with:

```r
# A chip reports a filter's value and offers to clear it. `focus_id` makes it
# the way in as well: clicking the chip body opens the filter panel and moves
# focus to the control that owns the value. It cannot host a live copy of that
# control -- Shiny input ids are unique per session -- so it reveals it.
make_chip <- function(label, clear_id, css_class = "", focus_id = NULL) {
  focusable <- !is.null(focus_id) && nzchar(as.character(focus_id))
  tags$span(
    class = paste("filter-chip", css_class, if (focusable) "chip-focusable" else ""),
    `data-chip-focus` = if (focusable) as.character(focus_id) else NULL,
    tabindex = if (focusable) "0" else NULL,
    role = if (focusable) "button" else NULL,
    label,
    tags$button(
      class = "chip-x",
      type = "button",
      `data-shiny-event` = clear_id,
      `aria-label` = paste("Clear", label),
      HTML("&times;")
    )
  )
}
```

- [ ] **Step 4: Pass focus targets from `build_filter_chips()`**

Add an `input_ids = NULL` argument to `build_filter_chips()` (in the signature at `app/R/global.R:860`, after `gn_label = "GN"`), and immediately after `chips <- list()` insert the resolver:

```r
  # Which input owns each chip. Defaults to the <prefix>_<thing> convention
  # every Israeli tab follows; a tab whose id differs passes an override, the
  # same way setup_chip_clears() already takes its ids.
  owner <- function(thing, default = paste0(prefix, "_", thing)) {
    if (is.null(input_ids)) return(default)
    val <- input_ids[[thing]]
    if (is.null(val) || !nzchar(as.character(val))) default else as.character(val)
  }
```

Then add the fourth argument to each `make_chip()` call in the function body:

| chip | call becomes |
|---|---|
| dates | `make_chip(lbl, paste0(prefix, "_clear_dates"), "chip-game", owner("dates", date_id))` |
| game type | `make_chip(paste(labels, collapse = ", "), paste0(prefix, "_clear_game_type"), "chip-game", owner("game_type", game_type_input_id %||% paste0(prefix, "_game_type")))` |
| teams | `make_chip(lbl, paste0(prefix, "_clear_teams"), "chip-game", owner("teams", if (prefix == "on") "teams" else paste0(prefix, "_teams")))` |
| opponents | `make_chip(lbl, paste0(prefix, "_clear_opponents"), "chip-game", owner("opponents"))` |
| home/away | `make_chip(if (ha == "home") "Home" else "Away", paste0(prefix, "_clear_home_away"), "chip-game", owner("home_away"))` |
| outcome | `make_chip(if (out_val == "win") "Wins" else "Losses", paste0(prefix, "_clear_outcome"), "chip-game", owner("outcome"))` |
| GN range | `make_chip(paste(parts, collapse = " "), paste0(prefix, "_clear_gn"), "chip-game", owner("gn", paste0(prefix, "_gn_min")))` |
| last N | `make_chip(paste("Last", last_n, "games"), paste0(prefix, "_clear_last_n"), "chip-game", owner("last_n"))` |
| opponent strength | `make_chip(parts, paste0(prefix, "_clear_opp_rank"), "chip-game", owner("opp_rank"))` |
| clutch | `make_chip(lbl, paste0(prefix, "_clear_clutch"), "chip-clutch", owner("clutch", paste0(prefix, "_clutch_margin")))` |
| players on | `make_chip(lbl, paste0(prefix, "_clear_players_on"), "chip-game", owner("players_on"))` |
| players off | `make_chip(lbl, paste0(prefix, "_clear_players_off"), "chip-game", owner("players_off"))` |

Leave the starters chip's call unchanged — it summarises two inputs, so there
is no single control to focus.

- [ ] **Step 5: Style the focusable chip**

Append to `app/www/app.css`:

```css

/* A chip that owns a control invites a click into it; one that summarises
   several (starters) or lives outside the panel (season) does not. */
.filter-chip.chip-focusable { cursor: pointer; }
.filter-chip.chip-focusable:hover { border-color: var(--ibpl-accent); }
.filter-chip.chip-focusable:focus-visible {
  outline: 2px solid var(--ibpl-accent);
  outline-offset: 2px;
}

/* Two seconds is long enough to find the control after the panel opens and
   short enough not to linger as decoration. */
@keyframes ibplChipReveal {
  0%   { box-shadow: 0 0 0 0 rgba(var(--ibpl-accent-rgb), 0.55); }
  100% { box-shadow: 0 0 0 8px rgba(var(--ibpl-accent-rgb), 0); }
}
.ibpl-chip-revealed {
  border-radius: 6px;
  animation: ibplChipReveal 1.1s ease-out 2;
}
@media (prefers-reduced-motion: reduce) {
  .ibpl-chip-revealed {
    animation: none;
    outline: 2px solid var(--ibpl-accent);
    outline-offset: 3px;
  }
}
```

- [ ] **Step 6: Handle the chip click in app.js**

In `app/www/app.js`, inside the delegated click handler that begins at line 291,
insert this branch **before** the existing `[data-shiny-event]` branch, so a
click on the chip body is handled here while a click on the `x` still falls
through to the clear event:

```javascript
    var chipFocus = e.target.closest("[data-chip-focus]");
    if (chipFocus && !e.target.closest(".chip-x")) {
      e.preventDefault();
      var targetId = chipFocus.dataset.chipFocus;

      // A control inside a collapsed panel cannot take focus, so open it
      // first and let the layout settle before reaching for the input.
      if (document.body.classList.contains("filters-collapsed")) {
        var toggle = document.querySelector(".js-filters-toggle");
        if (toggle) toggle.click();
      }

      window.setTimeout(function() {
        var el = document.getElementById(targetId);
        if (!el) return;

        // Bootstrap accordions hold most of these controls closed.
        var panel = el.closest(".accordion-collapse");
        if (panel && !panel.classList.contains("show") &&
            window.bootstrap && window.bootstrap.Collapse) {
          window.bootstrap.Collapse.getOrCreateInstance(panel).show();
        }

        var group = el.closest(".form-group, .shiny-input-container") || el;
        group.scrollIntoView({ block: "center", behavior: "smooth" });
        group.classList.add("ibpl-chip-revealed");
        window.setTimeout(function() {
          group.classList.remove("ibpl-chip-revealed");
        }, 2400);

        // Selectize replaces the original input with its own focusable node.
        var selectize = group.querySelector(".selectize-input");
        if (selectize) { selectize.click(); return; }
        if (typeof el.focus === "function") el.focus({ preventScroll: true });
      }, 80);
      return;
    }
```

Then add keyboard activation next to it, so a chip reached by Tab responds to
Enter and Space:

```javascript
  document.addEventListener("keydown", function(e) {
    if (e.key !== "Enter" && e.key !== " ") return;
    var chip = e.target.closest("[data-chip-focus]");
    if (!chip) return;
    e.preventDefault();
    chip.click();
  });
```

- [ ] **Step 7: Run the tests to verify they pass**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-chip-focus.R')"
cd /c/Users/ariel/documents/on_off_israel_pbp && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" scripts/test_all.R
```
Expected: 0 failures. `test-filter-chip-date-guards.R` covers `build_filter_chips()` and must stay green — the new argument is optional and every existing call site passes nothing.

- [ ] **Step 8: Verify in the browser**

```bash
IBPL_CACHE_UI=false "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "shiny::runApp('app', port = 7666, launch.browser = FALSE)"
```
On On/Off Impact set a team filter and a date range so two chips appear. Confirm: clicking the **x** still clears that filter and nothing else; clicking the chip **body** opens the panel if collapsed, expands the accordion section holding the control, scrolls it into view, pulses it twice and focuses it; Tab reaches the chip and Enter does the same; under `prefers-reduced-motion: reduce` the pulse is replaced by a static outline. Repeat on Lineup Data and on EuroLeague Tab 10, which pass their own input ids.

- [ ] **Step 9: Commit**

```bash
git add app/R/global.R app/www/app.css app/www/app.js app/tests/testthat/test-chip-focus.R
git commit -m "feat: chips open the control they describe

Filters were read above the table and edited in the far-left sidebar. A chip
cannot host a live copy of its control -- Shiny input ids are unique per
session -- so clicking one opens the panel, expands the accordion section,
scrolls to the input and focuses it. The x keeps its own clear event."
```

---

### Task 4: One generalised cross-tab handoff

Three `pending_*` reactive values cover three specific Home-card paths. Replace
the pattern with one value plus a reader, without touching the three that
already work.

**Files:**
- Modify: `app/R/helpers.R` (add `consume_pending_nav()`)
- Modify: `app/app.R:540-542` (add `pending_nav`)
- Modify: `app/tests/testthat/helper-server-mocks.R` (add it to `make_shared()`)
- Create: `app/tests/testthat/test-pending-nav.R`

**Interfaces:**
- Consumes: nothing.
- Produces: `shared$pending_nav` (a `reactiveVal` holding `NULL` or a named list with at least `target`), and `consume_pending_nav(shared, target)` returning the payload list when `target` matches — clearing it — and `NULL` otherwise.

- [ ] **Step 1: Write the failing test**

Create `app/tests/testthat/test-pending-nav.R`:

```r
source(repo_file("R", "helpers.R"), local = TRUE)

test_that("consume_pending_nav returns and clears a matching payload", {
  shiny::testServer(function(input, output, session) {
    shared <- list(pending_nav = shiny::reactiveVal(NULL))
    shared$pending_nav(list(target = "lineup_data", team_id = "7", player_id = "42"))

    got <- consume_pending_nav(shared, "lineup_data")

    expect_equal(got$team_id, "7")
    expect_equal(got$player_id, "42")
    expect_null(shiny::isolate(shared$pending_nav()))
  })
})

test_that("consume_pending_nav leaves another tab's payload alone", {
  shiny::testServer(function(input, output, session) {
    shared <- list(pending_nav = shiny::reactiveVal(NULL))
    shared$pending_nav(list(target = "game_logs", team_id = "7"))

    expect_null(consume_pending_nav(shared, "lineup_data"))
    expect_equal(shiny::isolate(shared$pending_nav())$target, "game_logs")
  })
})

test_that("consume_pending_nav is safe when nothing is pending", {
  shiny::testServer(function(input, output, session) {
    shared <- list(pending_nav = shiny::reactiveVal(NULL))
    expect_null(consume_pending_nav(shared, "lineup_data"))
  })
})

test_that("consume_pending_nav tolerates a shared list without the value", {
  # Server tests build partial shared lists; a missing value must not error.
  expect_null(consume_pending_nav(list(), "lineup_data"))
})
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-pending-nav.R')"
```
Expected: FAIL — `consume_pending_nav` is not defined.

- [ ] **Step 3: Add the reader**

In `app/R/helpers.R`, immediately after `shared_data_version()` (which ends at
line 133), add:

```r
# Cross-tab handoff. A source sets shared$pending_nav(list(target = "<tab>",
# ...)); the destination tab's init observer calls this, gets its payload once,
# and the value clears. One value rather than one reactiveVal per destination,
# so a new pivot target costs a case in the dispatcher instead of a new field
# in the shared list.
consume_pending_nav <- function(shared, target) {
  slot <- shared$pending_nav
  if (!is.function(slot)) return(NULL)

  pending <- slot()
  if (is.null(pending) || !identical(as.character(pending$target), as.character(target))) {
    return(NULL)
  }

  slot(NULL)
  pending
}
```

- [ ] **Step 4: Add the value to the shared list**

In `app/app.R`, after line 542 (`pending_compare_preset = reactiveVal(NULL),`), add:

```r
    # Generalised pivot handoff. The three pending_* values above predate it
    # and keep their own Home-card paths; new destinations use this one.
    pending_nav = reactiveVal(NULL),
```

- [ ] **Step 5: Add it to the server-test mock**

In `app/tests/testthat/helper-server-mocks.R`, find `make_shared()` and add
`pending_nav = shiny::reactiveVal(NULL)` alongside the existing
`pending_ld_team` entry, so the tab server smoke tests build a complete shared
list.

Run:
```bash
grep -n "pending_ld_team" app/tests/testthat/helper-server-mocks.R
```
and add the new field immediately after that line.

- [ ] **Step 6: Run the tests to verify they pass**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-pending-nav.R')"
cd /c/Users/ariel/documents/on_off_israel_pbp && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" scripts/test_all.R
```
Expected: 0 failures, including `test-server-tabs-smoke.R` which builds the shared list.

- [ ] **Step 7: Commit**

```bash
git add app/R/helpers.R app/app.R app/tests/testthat/helper-server-mocks.R \
        app/tests/testthat/test-pending-nav.R
git commit -m "feat: one generalised cross-tab handoff value

pending_ld_team, pending_gl_team and pending_compare_preset each exist for
one Home-card path. pending_nav plus consume_pending_nav() lets a new pivot
destination cost a dispatcher case rather than a new shared field. The three
existing values are untouched."
```

---

### Task 5: Pivot from any row

Clicking a team or player in a table offers the questions you would ask next,
carrying the entity with you.

**Files:**
- Modify: `app/R/helpers.R` (`onoff_summary_datatable()`, `onoff_four_factors_datatable()` — add `rowCallback`)
- Modify: `app/www/app.js` (append an IIFE)
- Modify: `app/www/app.css` (append)
- Modify: `app/app.R` (one dispatcher observer)
- Modify: `app/R/server_tab2.R:73` and `app/R/server_tab4.R:266` (read the new payload)
- Create: `app/tests/testthat/test-pivot-menu.R`

**Interfaces:**
- Consumes: `consume_pending_nav()` and `shared$pending_nav` from Task 4.
- Produces: Shiny input `pivot_action`, a list with `target`, `team_id`, `player_id`, `entity_name` and `rand`. `<tr>` elements carry `data-pivot-team` and `data-pivot-player`.

- [ ] **Step 1: Write the failing test**

Create `app/tests/testthat/test-pivot-menu.R`:

```r
test_that("row identity travels on the tr, never as injected HTML", {
  helpers_txt <- read_repo_txt("R", "helpers.R")

  expect_true(grepl("rowCallback", helpers_txt, fixed = TRUE))
  expect_true(grepl("data-pivot-team", helpers_txt, fixed = TRUE))
  expect_true(grepl("data-pivot-player", helpers_txt, fixed = TRUE))
})

test_that("the DT escaping contract is untouched", {
  # The pivot must not have widened any escape allowlist: entity names stay
  # escaped text and identity rides on the row's data attributes.
  server_files <- list.files(repo_file("R"), pattern = "^server_tab.*\\.R$", full.names = TRUE)
  code <- paste(unlist(lapply(server_files, readLines, warn = FALSE)), collapse = "\n")

  expect_false(grepl("escape\\s*=\\s*FALSE", code))
})

test_that("the pivot menu is keyboard dismissable and sends one event", {
  js <- read_repo_txt("www", "app.js")

  expect_true(grepl("pivot_action", js, fixed = TRUE))
  expect_true(grepl("data-pivot-team", js, fixed = TRUE))
  expect_true(grepl("Escape", js, fixed = TRUE))
  expect_true(grepl("role=\"menu\"", js, fixed = TRUE))
})

test_that("the dispatcher routes every advertised target", {
  app_txt <- read_repo_txt("app.R")

  expect_true(grepl("input$pivot_action", app_txt, fixed = TRUE))
  expect_true(grepl('"lineup_data"', app_txt, fixed = TRUE))
  expect_true(grepl('"game_logs"', app_txt, fixed = TRUE))
  # The trigger must be a plain input, never a data reactive: observers are
  # not suspended by tab visibility (4487c2f).
  expect_false(grepl("observeEvent\\(\\s*\\{?\\s*[a-z_]+\\(\\)\\s*,\\s*\\{[^}]*pivot_action", app_txt))
})

test_that("destination tabs read the generalised payload", {
  tab2 <- read_repo_txt("R", "server_tab2.R")
  tab4 <- read_repo_txt("R", "server_tab4.R")

  expect_true(grepl('consume_pending_nav(shared, "lineup_data")', tab2, fixed = TRUE))
  expect_true(grepl('consume_pending_nav(shared, "game_logs")', tab4, fixed = TRUE))
})
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-pivot-menu.R')"
```
Expected: FAIL on every test except the escaping contract, which already holds and must keep holding.

- [ ] **Step 3: Carry the id columns through to the rendered frame**

Neither renderer currently reaches the browser with `team_id` or `player_id`:
`onoff_summary_datatable()` drops them at `keep_cols` (`app/R/helpers.R:1945-1955`)
and `onoff_four_factors_datatable()` drops them when it builds `df_final`
(`app/R/helpers.R:2173`). They have to be carried and then hidden.

Confirm first:
```bash
cd /c/Users/ariel/documents/on_off_israel_pbp
sed -n '1945,1955p' app/R/helpers.R
sed -n '2173,2185p' app/R/helpers.R
```
Expected: neither `keep_cols` nor the `df_final` select mentions `team_id` or `player_id`.

In `onoff_summary_datatable()`, add the two ids to `keep_cols` at line 1946:

```r
      keep_cols <- c(
        "Team", "Player",
        # Carried only to reach the browser as row attributes for the pivot
        # menu; hidden below, and never rendered into a cell.
        "team_id", "player_id",
        "Net RTG Diff", "Off ON Diff", "Def ON Diff",
```

and add them to the hidden set at line 1967:

```r
      pr_cols <- names(df)[grep("^pr_", names(df))]
      hide_idx <- which(names(df) %in%
                          c(pr_cols, shot_raw_cols, shot_filter_cols,
                            "team_id", "player_id")) - 1
```

In `onoff_four_factors_datatable()`, add them to the `df_final` select at line 2173:

```r
      df_final <- df %>% select(all_of(vis_cols), any_of(c("team_id", "player_id")),
                                any_of(rank_cols), ends_with("_rank"), all_of(raw_cols_all))
```

and to `hide_cols` at line 2222:

```r
      hide_cols <- c(rank_cols, raw_cols_all, "team_id", "player_id",
                     names(df)[grep("_rank$", names(df))])
```

Note that line 2184 re-selects `final_col_order`; add `any_of(c("team_id", "player_id"))` to that select too, or the columns are dropped again immediately after being added. Read lines 2176-2185 and place them at the end of the order vector.

- [ ] **Step 4: Put identity on the row**

In `app/R/helpers.R`, add a `rowCallback` to the `options` list of both
`datatable()` calls (line 2084 and line 2283). Compute the indices with the
idiom the file already uses everywhere (`which(names(df) == "<col>") - 1`),
immediately above each `datatable()` call:

```r
      # Identity rides on the <tr> rather than inside a cell, so the pivot menu
      # never needs an escape allowlist widened for it. Names stay escaped
      # text; the menu reads them with textContent.
      pivot_team_idx <- which(names(df) == "team_id") - 1
      pivot_player_idx <- which(names(df) == "player_id") - 1
      pivot_cb <- if (length(pivot_team_idx) && length(pivot_player_idx)) {
        DT::JS(sprintf(
          paste0(
            "function(row, data) {",
            "  if (!row || !data) return;",
            "  var t = data[%d], p = data[%d];",
            "  if (t !== null && t !== undefined && t !== '') row.setAttribute('data-pivot-team', t);",
            "  if (p !== null && p !== undefined && p !== '') row.setAttribute('data-pivot-player', p);",
            "}"
          ),
          pivot_team_idx, pivot_player_idx
        ))
      } else {
        NULL
      }
```

Then add `rowCallback = pivot_cb,` to the `options = list(...)` of that call.
In `onoff_four_factors_datatable()` the frame is `df_final`, so use
`which(names(df_final) == "team_id") - 1` there.

A `NULL` entry in a DT `options` list is dropped, so a frame that happens to
lack the ids degrades to no pivot rather than to an error.

- [ ] **Step 5: Build the menu in app.js**

Append to `app/www/app.js`:

```javascript

/* ---- Pivot menu -----------------------------------------------------------
   A finding in one table is usually a question for another: a player with a
   large on/off gap raises "which lineups", a team raises "which games". The
   app already carries filter state between tabs for three Home cards; this
   opens the same road from any row.

   Identity is read from the row's data attributes, which DT sets from hidden
   id columns, and the label from textContent -- so nothing here depends on
   unescaped HTML reaching a cell.
   -------------------------------------------------------------------------- */
(function() {
  var menu = null;

  var ACTIONS = [
    { target: "lineup_data", label: "Lineups with this player", needs: "player" },
    { target: "lineup_data", label: "Lineups for this team", needs: "team" },
    { target: "game_logs", label: "Game log for this team", needs: "team" }
  ];

  function close() {
    if (!menu) return;
    menu.remove();
    menu = null;
  }

  function send(target, row, label) {
    if (!window.Shiny || typeof window.Shiny.setInputValue !== "function") return;
    window.Shiny.setInputValue("pivot_action", {
      target: target,
      team_id: row.getAttribute("data-pivot-team") || "",
      player_id: row.getAttribute("data-pivot-player") || "",
      entity_name: label,
      rand: Math.random()
    }, { priority: "event" });
  }

  function open(row, x, y) {
    close();
    var hasTeam = !!row.getAttribute("data-pivot-team");
    var hasPlayer = !!row.getAttribute("data-pivot-player");
    var firstCell = row.querySelector("td");
    var label = firstCell ? firstCell.textContent.trim() : "";

    var items = ACTIONS.filter(function(a) {
      return a.needs === "team" ? hasTeam : hasPlayer;
    });
    if (!items.length) return;

    menu = document.createElement("div");
    menu.className = "ibpl-pivot-menu";
    menu.setAttribute("role", "menu");

    items.forEach(function(a) {
      var btn = document.createElement("button");
      btn.type = "button";
      btn.className = "ibpl-pivot-item";
      btn.setAttribute("role", "menuitem");
      btn.textContent = a.label;
      btn.addEventListener("click", function() {
        send(a.target, row, label);
        close();
      });
      menu.appendChild(btn);
    });

    document.body.appendChild(menu);
    var box = menu.getBoundingClientRect();
    menu.style.left = Math.min(x, window.innerWidth - box.width - 8) + "px";
    menu.style.top = Math.min(y, window.innerHeight - box.height - 8) + "px";
    var first = menu.querySelector(".ibpl-pivot-item");
    if (first) first.focus();
  }

  document.addEventListener("click", function(e) {
    if (menu && !e.target.closest(".ibpl-pivot-menu")) { close(); return; }

    var cell = e.target.closest("td");
    if (!cell || cell.cellIndex > 1) return;
    var row = cell.closest("tr[data-pivot-team], tr[data-pivot-player]");
    if (!row) return;

    e.preventDefault();
    e.stopPropagation();
    open(row, e.clientX, e.clientY);
  });

  document.addEventListener("keydown", function(e) {
    if (e.key === "Escape") close();
  });

  window.addEventListener("resize", close);
  window.addEventListener("scroll", close, true);
})();
```

- [ ] **Step 6: Style the menu**

Append to `app/www/app.css`:

```css

/* ---- Pivot menu ---------------------------------------------------------- */
.ibpl-pivot-menu {
  position: fixed; z-index: 2000; min-width: 210px;
  background: var(--ibpl-surface);
  border: 1px solid var(--ibpl-border);
  border-radius: 8px; padding: 4px;
  box-shadow: 0 8px 24px rgba(0, 0, 0, 0.45);
}
.ibpl-pivot-item {
  display: block; width: 100%; text-align: left;
  padding: 7px 12px; border: none; border-radius: 5px;
  background: transparent; color: var(--ibpl-text-body);
  font-size: 0.82rem; cursor: pointer;
}
.ibpl-pivot-item:hover { background: var(--ibpl-surface-3); color: var(--ibpl-text); }
.ibpl-pivot-item:focus-visible {
  outline: 2px solid var(--ibpl-accent);
  outline-offset: -2px;
}

tr[data-pivot-team] td:first-child,
tr[data-pivot-team] td:nth-child(2),
tr[data-pivot-player] td:first-child,
tr[data-pivot-player] td:nth-child(2) { cursor: context-menu; }
```

- [ ] **Step 7: Dispatch the action**

In `app/app.R`, after the existing `go_compare` observer (around line 612), add:

```r
  # One dispatcher for every pivot. The trigger is a plain input: observers are
  # never suspended by tab visibility, so a data reactive here would run on
  # every session regardless of which tab is open (4487c2f).
  observeEvent(input$pivot_action, {
    action <- input$pivot_action
    req(!is.null(action), nzchar(as.character(action$target %||% "")))

    target <- as.character(action$target)
    if (!target %in% c("lineup_data", "game_logs")) return(invisible(NULL))

    shared$pending_nav(list(
      target = target,
      team_id = as.character(action$team_id %||% ""),
      player_id = as.character(action$player_id %||% "")
    ))
    updateTabsetPanel(session, "main_tabs", selected = target)
  })
```

- [ ] **Step 8: Read the payload in the destination tabs**

In `app/R/server_tab2.R`, replace line 73:

```r
    pending_team <- as.character(shared$pending_ld_team() %||% "")
```
with:
```r
    # Home cards still use pending_ld_team; row pivots arrive through the
    # generalised value. Either one supplies a team to preselect.
    nav <- consume_pending_nav(shared, "lineup_data")
    pending_team <- as.character(
      (if (!is.null(nav)) nav$team_id else NULL) %||% shared$pending_ld_team() %||% ""
    )
```

In `app/R/server_tab4.R`, replace line 266:

```r
    pending_team <- shared$pending_gl_team()
```
with:
```r
    nav <- consume_pending_nav(shared, "game_logs")
    pending_team <- (if (!is.null(nav)) nav$team_id else NULL) %||% shared$pending_gl_team()
```

- [ ] **Step 9: Run the tests to verify they pass**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-pivot-menu.R')"
cd /c/Users/ariel/documents/on_off_israel_pbp && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" scripts/test_all.R
```
Expected: 0 failures, including `test-dt-security.R`, `test-server-tabs-smoke.R` and `test-primary-table-render-smoke.R`.

- [ ] **Step 10: Verify in the browser**

```bash
IBPL_CACHE_UI=false "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "shiny::runApp('app', port = 7666, launch.browser = FALSE)"
```
On On/Off Impact, click a player's name: a menu appears at the cursor offering the lineup and game-log actions. Choosing "Lineups for this team" lands on Lineup Data with that team already selected. Confirm Escape closes the menu, clicking elsewhere closes it, scrolling closes it, and the menu's items are reachable with Tab. Then confirm the sorting and column-header clicks on the first two columns still work — the pivot handler calls `stopPropagation` only on `td`, never on `th`.

- [ ] **Step 11: Commit**

```bash
git add app/R/helpers.R app/R/server_tab2.R app/R/server_tab4.R app/app.R \
        app/www/app.js app/www/app.css app/tests/testthat/test-pivot-menu.R
git commit -m "feat: pivot from any row to the tab that answers the next question

A finding in one table was a dead end: seeing a player's on/off gap and
wanting their lineups meant a new tab and re-entering the team. Identity
rides on the tr via rowCallback rather than injected cell HTML, so the
fail-closed DT escaping contract is unchanged."
```

---

### Task 6: Home stops asking what the hub already answered

**Files:**
- Modify: `app/R/ui_tab0_home.R` (the Israeli block, lines 186-308)
- Modify: `app/www/app.css` (append)
- Modify: `app/tests/testthat/test-team-hub-ui.R`

**Interfaces:**
- Consumes: nothing.
- Produces: `home_nav_rail(items)` in `app/R/ui_tab0_home.R`, taking a list of `list(input_id =, icon =, label =)`. The five Israeli `go_*` input ids are unchanged, so `app/app.R`'s observers need no edit.

- [ ] **Step 1: Write the failing test**

Append to `app/tests/testthat/test-team-hub-ui.R`:

```r
test_that("the Israeli Home offers a rail, not a second set of question cards", {
  html <- htmltools::renderTags(ui_tab0_home())$html

  expect_match(html, "home-nav-rail", fixed = TRUE)
  # The five destinations keep their input ids, so app.R's observers are
  # untouched by the change of presentation.
  for (id in c("go_onoff", "go_lineups", "go_team", "go_gamelogs", "go_compare")) {
    expect_match(html, sprintf('data-input-id="%s"', id), fixed = TRUE)
  }
  # "Go ->" did no job the card was not already doing.
  expect_false(grepl("Go →", html, fixed = TRUE))
})

test_that("EuroLeague keeps its cards because it has no hub above them", {
  html <- htmltools::renderTags(ui_tab0_home())$html

  expect_match(html, 'data-input-id="go_euro_onoff"', fixed = TRUE)
  expect_match(html, "Who is helping my team?", fixed = TRUE)
  expect_match(html, "home-nav-card", fixed = TRUE)
})
```

- [ ] **Step 2: Run the test to verify it fails**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-team-hub-ui.R')"
```
Expected: FAIL — `home-nav-rail` does not exist and `Go →` appears eleven times.

- [ ] **Step 3: Add the rail builder**

At the top of `app/R/ui_tab0_home.R`, above `ui_tab0_home()`, add:

```r
# Under the Israeli league the team hub already answers the five questions the
# nav cards ask, so the cards become a rail: the same destinations and the same
# input ids, at the weight of navigation rather than of content. The EuroLeague
# block keeps its cards because it has no hub above them, and there the cards
# are the content.
home_nav_rail <- function(items) {
  tags$nav(
    class = "home-nav-rail",
    `aria-label` = "Go to a stats tab",
    lapply(items, function(item) {
      tags$button(
        type = "button",
        class = "home-nav-rail-item js-shiny-event",
        `data-input-id` = item$input_id,
        tags$i(class = paste("bi", item$icon), `aria-hidden` = "true"),
        tags$span(item$label)
      )
    })
  )
}
```

- [ ] **Step 4: Replace the five Israeli cards with the rail**

In `ui_tab0_home()`, in the `league-only-il` block, delete the three
`fluidRow(...)` blocks holding the five Israeli nav cards (lines 190-307) and
replace them with:

```r
      home_nav_rail(list(
        list(input_id = "go_onoff",       icon = "bi-person-fill",      label = "On/Off Impact"),
        list(input_id = "go_lineups",     icon = "bi-people-fill",      label = "Lineups"),
        list(input_id = "go_team",        icon = "bi-bar-chart-fill",   label = "Team Ratings"),
        list(input_id = "go_gamelogs",    icon = "bi-calendar-day-fill", label = "Game Logs"),
        list(input_id = "go_playerstats", icon = "bi-bar-chart-line",   label = "Player Stats"),
        list(input_id = "go_compare",     icon = "bi-arrow-left-right", label = "Compare")
      ))
```

Leave the entire `league-only-el` block exactly as it is.

- [ ] **Step 5: Style the rail**

Append to `app/www/app.css`:

```css

/* ---- Home nav rail --------------------------------------------------------
   Navigation under the hub, not content beside it: the hub already answers
   these questions, so the destinations are labelled by where they go rather
   than by what they ask.
   -------------------------------------------------------------------------- */
.home-nav-rail {
  display: flex; flex-wrap: wrap; gap: 8px;
  margin: 8px 0 24px;
}
.home-nav-rail-item {
  display: inline-flex; align-items: center; gap: 7px;
  padding: 9px 14px; border-radius: 8px;
  border: 1px solid var(--ibpl-border);
  background: var(--ibpl-surface);
  color: var(--ibpl-text-body);
  font-size: 0.85rem; font-weight: 600;
  cursor: pointer;
  transition: border-color .15s ease, color .15s ease, transform .15s ease;
}
.home-nav-rail-item:hover {
  border-color: var(--ibpl-accent);
  color: var(--ibpl-accent);
  transform: translateY(-1px);
}
.home-nav-rail-item:focus-visible {
  outline: 2px solid var(--ibpl-accent);
  outline-offset: 2px;
}
.home-nav-rail-item .bi { color: var(--ibpl-accent); font-size: 1rem; }

@media (prefers-reduced-motion: reduce) {
  .home-nav-rail-item:hover { transform: none; }
}
@media (max-width: 575px) {
  .home-nav-rail-item { flex: 1 1 100%; justify-content: flex-start; }
}
```

- [ ] **Step 6: Run the tests to verify they pass**

Run:
```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('tests/testthat/test-team-hub-ui.R')"
cd /c/Users/ariel/documents/on_off_israel_pbp && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" scripts/test_all.R
```
Expected: 0 failures. `test-tab-wiring.R` checks that every `go_*` input has an observer and must stay green — the ids are unchanged.

- [ ] **Step 7: Verify both leagues in the browser**

```bash
IBPL_CACHE_UI=false "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "shiny::runApp('app', port = 7666, launch.browser = FALSE)"
```
Under the Israeli league, Home shows the team selector, the hub, then the rail; each rail button lands on the right tab with the team preselected exactly as the cards did. Under EuroLeague, Home still shows the five question cards. Confirm the rail is keyboard reachable with a visible focus ring, and that clicking a rail item before the websocket connects still navigates once connected (the queue-and-replay path at `app/www/app.js:240` covers `js-shiny-event`).

- [ ] **Step 8: Commit**

```bash
git add app/R/ui_tab0_home.R app/www/app.css app/tests/testthat/test-team-hub-ui.R
git commit -m "feat: Home offers a nav rail under the hub instead of repeating it

The team hub answers the five questions the nav cards ask, so under the
Israeli league the cards became a rail at navigation weight. EuroLeague keeps
its cards -- it has no hub, so there the cards are the content. Input ids are
unchanged, so app.R's observers are untouched."
```

---

## Done criteria

- `"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" scripts/test_all.R` reports 0 failures.
- All nine data tabs render their chips through `filter_chips_row()`, enforced by `test-chips-row.R`.
- The filter panel collapses and restores on every data tab, the state survives a reload, and DataTables re-measures its header on toggle.
- Clicking a chip body opens the panel, expands the owning accordion section, scrolls to and focuses the control; clicking the `x` still only clears.
- Every interactive addition is reachable by keyboard with a visible focus ring, and honours `prefers-reduced-motion`.
- `consume_pending_nav()` clears on read and ignores another tab's payload.
- `test-dt-security.R` still passes: no escape allowlist was widened for the pivot menu.
- Under the Israeli league Home shows hub then rail; under EuroLeague it still shows the five cards.
