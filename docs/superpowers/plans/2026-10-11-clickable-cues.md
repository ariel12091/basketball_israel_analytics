# Clickable Cues Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Every element of the Shiny app that responds to a click or tap shows a visible cue at rest, on desktop and on a phone.

**Architecture:** Four cues (Open `›`, Menu `▾`, Explain dotted underline, Hint line) defined once in a "Clickable cues" block in `app/www/app.css` and attached to selectors the renderers already emit. Two small behaviour additions: the navbar view-mode block in `app.js` writes the active mode into the tab label, and a pure helper `table_hint()` in `helpers.R` renders the hint line for both leagues.

**Tech Stack:** R 4.4.2, Shiny (bslib BS5), DT, plain CSS and JS in `app/www/`, testthat.

**Spec:** `docs/superpowers/specs/2026-10-11-clickable-cues-design.md`

## Global Constraints

- A cue appears only where the interaction really exists.
- Cues are visible at rest. Hover may strengthen a cue, never be its only sign.
- No new colours: amber `var(--ibpl-accent)` for Open, `var(--ibpl-text-dim)` / `var(--ibpl-text-muted)` for carets and hint lines.
- Glyphs are drawn in CSS `::after` with escapes (`\25BE` caret, `\203A` chevron, `\00B7` middle dot). No extra columns, no wider cells.
- No raw non-ASCII in R source (`test-source-encoding.R`): use `intToUtf8()`.
- No `euro_` copy of anything: one helper, one selector list, both leagues.
- `helpers.R`, `global.R`, `app.R` have mixed line endings. After every edit to one of them run `git diff --stat -- <file>`; if the line count is far larger than the change, rebuild from `git show HEAD:<file>` with a `perl -0777` splice as described in `CLAUDE.md` § Deploy Scripts.
- The on/off renderer in `helpers.R` is a `sprintf(paste0(...))` template: do not edit it. Every cue on its cells is CSS only.
- Run the local app through the `run-shiny-local` skill with `IBPL_CACHE_UI=false`.
- Do not change what any click does.

Test commands (from the repo root, Git Bash):

```bash
RSCRIPT="/c/Program Files/R/R-4.4.2/bin/Rscript.exe"
# one file
(cd app && "$RSCRIPT" -e "testthat::test_file('tests/testthat/test-clickable-cues.R')")
# whole suite
(cd app && "$RSCRIPT" tests/testthat.R)
```

Local runs print a flood of `LC_ALL=C.UTF-8` warnings; those are pre-existing.

## Review Focus

1. **Navbar wraps or overflows once labels grow.** At 1280px and 1440px wide the bar must not gain a row compared with `main`. Pinned by Task 1 Step 6 (measured in the browser).
2. **A tab's mode label goes stale when the server changes the mode** (the Reset button calls `updateRadioButtons(..., selected = "Summary")`). The label must follow. Pinned by Task 1 Step 6.
3. **The pivot caret lands on the wrong cell or on rows with no menu** (the TOTAL row, tables built without `pivot`). Pinned by Task 2 Step 4.
4. **Glyphs leak into exports or copied text.** CSV export and a copied player name must not contain `›` or `▾`. `::after` content is not in the DOM text, so this holds by construction; Task 6 Step 3 confirms on a real export.
5. **Hint line claims something a table does not do.** Pinned by the `table_hint()` unknown-key test (Task 5) and the per-tab check in Task 6.

---

### Task 0: Branch and baseline

**Files:** none changed.

- [ ] **Step 1: Get onto the branch**

The spec and this plan are already committed on `shiny/clickable-cues`. The working tree is on `infra/intro-video` with uncommitted user edits to `app/www/app.js`, `app/R/global.R` and others. Do not stash, discard or commit those without the user's say-so; ask which they prefer (commit, stash, or build on top). Then:

```bash
git checkout shiny/clickable-cues
git status --short | head
```

- [ ] **Step 2: Baseline tests**

Run the whole suite. Record the pass/fail counts; any failure here is pre-existing and is reported, not fixed.

- [ ] **Step 3: Close the "to verify" rows of the spec**

Start the app with the `run-shiny-local` skill. Record, in the commit message of Task 3, what these look like at rest:

- Gameflow link in Game Logs (`.ribbon-link`) and the EuroLeague lineup link (`.euro-ld-unit`): neither has a CSS rule today, so expect browser-default or theme link styling.
- Lineup summary tokens (`.lineup-chips-token`): already bordered amber buttons. Expected: no change needed.
- Compare gap header (`.js-cmp-detail-sort`): already carries a sort icon. Expected: no change needed.
- Four Factors range toggle and luck explainer in the chips row: note whether they read as buttons.

Also click through Tabs 3, 5, 9 and note any row or cell interaction not in the spec's inventory. If one exists, add it to the spec table and give it one of the four cues in Task 3 or 4.

---

### Task 1: Navbar tabs show a caret and the active mode

**Files:**
- Modify: `app/www/app.js` (view-mode block, `initOne()`, around lines 2080-2141)
- Modify: `app/www/app.css` (append a "Clickable cues" block at the end of the file)
- Create: `app/tests/testthat/test-clickable-cues.R`

**Interfaces:**
- Produces: `.tab-mode-label` span inside each `.nav-link` that has a view-mode menu; the "Clickable cues" CSS block that later tasks append to.

- [ ] **Step 1: Write the failing test**

Create `app/tests/testthat/test-clickable-cues.R`:

```r
cues_css <- function() read_repo_txt("www", "app.css")

test_that("tabs with a view-mode menu carry a caret and a mode label", {
  js <- read_repo_txt("www", "app.js")
  css <- cues_css()

  expect_true(grepl('className = "tab-mode-label"', js, fixed = TRUE))
  expect_true(grepl("shiny:inputchanged", js, fixed = TRUE))
  expect_true(grepl(".nav-item.tab-has-dropdown > .nav-link::after", css, fixed = TRUE))
  expect_true(grepl(".tab-mode-label", css, fixed = TRUE))
})
```

- [ ] **Step 2: Run it and see it fail**

Run the one-file command. Expected: FAIL on the first `expect_true`.

- [ ] **Step 3: Add the label to `initOne()`**

In `app/www/app.js`, directly after `li.classList.add("tab-has-dropdown");`:

```js
    var modeLabel = document.createElement("span");
    modeLabel.className = "tab-mode-label";
    link.appendChild(modeLabel);
```

Replace the existing `syncMenu` function and the two listeners after it with:

```js
    function syncMenu() {
      var current = currentInputValue(config);
      modeLabel.textContent = current;
      menu.querySelectorAll(".thm-item").forEach(function(row) {
        setRowActive(row, row.dataset.value === current);
      });
    }

    syncMenu();
    link.addEventListener("shown.bs.tab", syncMenu);
    link.addEventListener("click", function() {
      setTimeout(syncMenu, 100);
    });
    // The server can change the mode too (Reset, bookmark restore). Shiny
    // announces every input change as a jQuery event on document.
    if (window.jQuery) {
      window.jQuery(document).on("shiny:inputchanged", function(e) {
        if (e.name === config.inputId) setTimeout(syncMenu, 0);
      });
    }
```

In the row click handler, after the `menu.querySelectorAll(".thm-item").forEach(...)` call that marks the clicked row active, add:

```js
        modeLabel.textContent = val;
```

- [ ] **Step 4: Add the CSS**

Append to `app/www/app.css`:

```css
/* ---- Clickable cues ---------------------------------------------------------
   Four cues, one meaning each, visible at rest:
     Open    amber text + trailing chevron   opens a modal, detail view or tab
     Menu    trailing caret                  opens a menu
     Explain dotted underline                explains on hover or tap
     Hint    one muted line above a table    says what the table responds to
   Glyphs live in ::after so they never reach exports or copied text.
   -------------------------------------------------------------------------- */

/* Menu cue: navbar tabs with view modes. */
.nav-item.tab-has-dropdown > .nav-link::after {
  content: "\25BE";
  margin-left: 5px;
  font-size: 0.75em;
  opacity: 0.7;
}
/* The mode is shown on the active tab only: on every tab it reads as noise
   and adds a navbar row. */
.tab-mode-label { display: none; font-weight: 500; }
.nav-link.active > .tab-mode-label { display: inline; }
.tab-mode-label::before { content: "\00B7"; margin: 0 5px; opacity: 0.7; }
```

This takes the spec's fallback (mode text on the active tab, caret on all) as the default, because the navbar already wraps today (see the comment on `.navbar .navbar-nav .nav-link`).

- [ ] **Step 5: Run the test and the navbar tests**

Run `test-clickable-cues.R`, `test-navbar-hover-menu.R` and `test-mobile-layer.R`. Expected: PASS.

- [ ] **Step 6: Check in the browser**

With the app running, at 1440px and 1280px wide:

- every tab in the `CFG` list shows a caret; Home does not;
- the active tab reads like `On/Off · Summary ▾`;
- pick Four Factors from the menu: the label changes at once;
- press the tab's Reset button: the label returns to `Summary`;
- the navbar has the same number of rows as on `main` at both widths;
- in the phone layout (burger menu) the mode label and inline menu both show for the active tab and do not duplicate each other awkwardly. If they do, add `body.ibpl-collapsed-nav .tab-mode-label { display: none !important; }` to `mobile.css`.

- [ ] **Step 7: Commit**

```bash
git add app/www/app.js app/www/app.css app/tests/testthat/test-clickable-cues.R
git commit -m "Navbar: show a caret and the active view mode on tabs with a menu"
```

---

### Task 2: Player and team name cells show the pivot menu cue

**Files:**
- Modify: `app/www/app.css` (Clickable cues block)
- Modify: `app/tests/testthat/test-clickable-cues.R`

**Interfaces:**
- Consumes: `td[data-pivot-trigger]`, set by the `createdRow` callback in `onoff_summary_datatable()` / `onoff_four_factors_datatable()` (Tabs 1 and 8 only), and `.ibpl-row-name[data-pivot-trigger]` (mobile floating name).

- [ ] **Step 1: Write the failing test**

Append to `test-clickable-cues.R`:

```r
test_that("pivot name cells are styled as menu triggers", {
  css <- cues_css()

  expect_true(grepl("td[data-pivot-trigger]", css, fixed = TRUE))
  expect_true(grepl("td[data-pivot-trigger]:first-child::after", css, fixed = TRUE))
  expect_true(grepl(".ibpl-row-name[data-pivot-trigger]::after", css, fixed = TRUE))
})
```

- [ ] **Step 2: Run it and see it fail**

- [ ] **Step 3: Add the CSS**

Append inside the Clickable cues block:

```css
/* Menu cue: on/off name cells that open the pivot menu. The attribute is set
   only on rows that have a menu, so the cue cannot appear without one. */
td[data-pivot-trigger] { cursor: pointer; }
td[data-pivot-trigger]:first-child::after,
.ibpl-row-name[data-pivot-trigger]::after {
  content: "\25BE";
  margin-left: 5px;
  font-size: 0.75em;
  color: var(--ibpl-text-dim);
}
td[data-pivot-trigger]:hover,
td[data-pivot-trigger]:focus-visible { color: var(--ibpl-accent); }
td[data-pivot-trigger]:hover::after,
td[data-pivot-trigger]:focus-visible::after { color: var(--ibpl-accent); }
```

- [ ] **Step 4: Check in the browser (Tabs 1 and 8, Summary and Four Factors)**

- the caret sits after the player name, in the first visible column, and not in the team column;
- clicking the name still opens the menu with the right items;
- no caret on a table row that opens no menu (click a name with a caret on each view to confirm each does open one);
- row height and the name column width are unchanged against `main`; if a long name now wraps, drop `margin-left` to `3px`;
- at phone width, scroll the table sideways: the floating row name shows the caret and opens the menu.

- [ ] **Step 5: Run `test-clickable-cues.R` and `test-pivot-menu.R`; commit**

```bash
git add app/www/app.css app/tests/testthat/test-clickable-cues.R
git commit -m "On/Off tables: mark player and team names as pivot menu triggers"
```

---

### Task 3: Open cue on everything that opens a modal, detail view or tab

**Files:**
- Modify: `app/www/app.css` (replace the `.ld-lineup-link` rule near line 1163; add to the cues block)
- Modify: `app/R/mod_team_hub.R` (three cards: `hub_go_team` ~line 890, `hub_go_players` ~939, `hub_go_lineups` ~1004)
- Modify: `app/tests/testthat/test-clickable-cues.R`

**Interfaces:**
- Consumes: `.ld-lineup-link` and `.euro-ld-unit` (`LD_LINEUP_TABLE_SPEC` / `EURO_LD_LINEUP_TABLE_SPEC` in `helpers.R`), `.ribbon-link` (`ribbon_link_cell()`), `#cmp_table` and `#cmp_team_players_table` (row click handlers in `server_tab7_compare.R`), `.hub-card`, `.hub-story-line`.
- Produces: `.hub-card-go` footer element.

- [ ] **Step 1: Write the failing test**

Append to `test-clickable-cues.R`:

```r
test_that("everything that opens something shares one Open cue", {
  css <- cues_css()
  open_targets <- c(".ld-lineup-link", ".euro-ld-unit", ".ribbon-link",
                    ".hub-story-line", ".hub-card-go")

  for (sel in open_targets) {
    expect_true(grepl(paste0(sel, "::after"), css, fixed = TRUE), info = sel)
  }
  expect_true(grepl("#cmp_table tbody tr", css, fixed = TRUE))
  expect_true(grepl("#cmp_team_players_table tbody tr", css, fixed = TRUE))
})

test_that("each hub card names where it goes", {
  hub <- read_repo_txt("R", "mod_team_hub.R")

  expect_equal(lengths(regmatches(hub, gregexpr('class = "hub-card-go"', hub, fixed = TRUE))), 3L)
})
```

- [ ] **Step 2: Run it and see it fail**

- [ ] **Step 3: CSS**

Replace the existing rule

```css
.ld-lineup-link {
  color: var(--ibpl-accent);
  text-decoration: underline;
  cursor: pointer;
}
```

with

```css
.ld-lineup-link,
.euro-ld-unit,
.ribbon-link {
  color: var(--ibpl-accent);
  text-decoration: underline;
  cursor: pointer;
}
```

Append inside the Clickable cues block:

```css
/* Open cue. */
.ld-lineup-link::after,
.euro-ld-unit::after,
.ribbon-link::after,
.hub-story-line::after,
.hub-card-go::after {
  content: "\203A";
  display: inline-block;
  margin-left: 4px;
  color: var(--ibpl-accent);
  text-decoration: none;
}
.hub-card-go {
  margin-top: 10px;
  color: var(--ibpl-accent);
  font-size: 0.78rem;
  font-weight: 600;
}
.hub-card:hover,
.hub-card:focus-visible { border-color: var(--ibpl-accent) !important; }

/* Open cue: Compare rows open the head-to-head detail. */
#cmp_table tbody tr,
#cmp_team_players_table tbody tr { cursor: pointer; }
#cmp_table tbody tr:hover > td,
#cmp_team_players_table tbody tr:hover > td { background-color: var(--ibpl-surface-2); }
#cmp_table tbody tr > td:first-child::after,
#cmp_team_players_table tbody tr > td:first-child::after {
  content: "\203A";
  margin-left: 4px;
  color: var(--ibpl-accent);
}
```

- [ ] **Step 4: Hub card footers**

In `app/R/mod_team_hub.R`, add a last child to the `card-body` of each of the three clickable cards. Read each `observeEvent(input$hub_go_*)` (lines ~1017-1045) first and make the text name the tab it really opens:

```r
        div(class = "hub-card-go", "Open team ratings")   # hub_go_team
        div(class = "hub-card-go", "Open on/off impact")  # hub_go_players
        div(class = "hub-card-go", "Open lineups")        # hub_go_lineups
```

Each needs a comma after the preceding child. Leave `.hub-card-static` cards alone.

- [ ] **Step 5: Check in the browser**

- Tab 2 and Tab 10 lineup names look identical (amber, underlined, `›`) and still open the modal;
- Game Logs (Tabs 4 and 11) Gameflow links match and still open the ribbon;
- Compare: in each of Teams, Lineups and Players modes, check which rows are clickable (the handler is bound in `server_tab7_compare.R` ~line 3996). If a mode's `cmp_table` rows do nothing, scope the three `#cmp_table` rules to the modes that do, using the class or attribute that mode's table carries, so no dead row shows a cue. Confirm the first visible column is the entity name;
- Home: each hub card shows its footer and lands on the tab the footer names; storylines show `›`;
- the `›` is not underlined on the links.

- [ ] **Step 6: Run `test-clickable-cues.R`, `test-team-hub-ui.R`, `test-stint-ribbon.R`; commit**

```bash
git add app/www/app.css app/R/mod_team_hub.R app/tests/testthat/test-clickable-cues.R
git commit -m "Open cue: one look for lineup, Gameflow, Compare and hub links"
```

---

### Task 4: Filter chips and explanations

**Files:**
- Modify: `app/R/global.R` (`make_chip()`, ~line 1189; mixed line endings)
- Modify: `app/www/app.css` (cues block)
- Modify: `app/tests/testthat/test-clickable-cues.R`

**Interfaces:**
- Consumes: `make_chip(label, clear_id, css_class, focus_id)`; `th[data-tooltip]`; `.onoff-net-tip`.
- Produces: `.chip-edit` icon inside focusable chips.

- [ ] **Step 1: Write the failing test**

Append to `test-clickable-cues.R`:

```r
test_that("a chip that opens its control says so; one that does not stays plain", {
  global_txt <- read_repo_txt("R", "global.R")

  expect_true(grepl('class = "bi bi-pencil chip-edit"', global_txt, fixed = TRUE))
  expect_true(grepl('if (focusable) tags$i(class = "bi bi-pencil chip-edit"',
                    global_txt, fixed = TRUE))
})

test_that("explained headers and tags are marked at rest", {
  css <- cues_css()

  expect_match(css, "th\\[data-tooltip\\]\\s*\\{[^}]*text-decoration:\\s*underline dotted")
  expect_match(css, "\\.onoff-net-tip\\s*\\{[^}]*text-decoration:\\s*underline dotted")
})
```

- [ ] **Step 2: Run it and see it fail**

- [ ] **Step 3: Chip edit mark**

In `make_chip()` in `app/R/global.R`, change

```r
    label,
    tags$button(
      class = "chip-x",
```

to

```r
    label,
    if (focusable) tags$i(class = "bi bi-pencil chip-edit", `aria-hidden` = "true"),
    tags$button(
      class = "chip-x",
```

Then `git diff --stat -- app/R/global.R`. Expected: 1 insertion. If it shows hundreds of lines, redo the edit with the raw-byte splice from Global Constraints.

- [ ] **Step 4: CSS**

Append inside the Clickable cues block:

```css
/* A chip that jumps to its control. */
.filter-chip .chip-edit { margin-left: 5px; font-size: 0.8em; opacity: 0.65; }
.filter-chip.chip-focusable:hover .chip-edit,
.filter-chip.chip-focusable:focus-visible .chip-edit { opacity: 1; }

/* Explain cue. */
th[data-tooltip] {
  text-decoration: underline dotted;
  text-decoration-color: var(--ibpl-text-dim);
  text-underline-offset: 3px;
}
.onoff-net-tip {
  text-decoration: underline dotted;
  text-decoration-color: var(--ibpl-text-dim);
  text-underline-offset: 3px;
}
```

The spec's `ⓘ` mark is left out: table headers have no room, and the tag sits inside the `sprintf` renderer that this plan does not edit. The dotted underline is the whole Explain cue.

- [ ] **Step 5: Check in the browser**

- set a team and an opponent filter: those chips show the pencil; the season chip does not; clicking the chip body still reveals the control and `×` still clears;
- chips do not wrap onto an extra line in a state where they fit before;
- headers with a tooltip are dotted-underlined, headers without are not, and the DT sort arrows are still readable;
- 3PT luck and small-sample tags are underlined and their hover box still appears.

- [ ] **Step 6: Run `test-clickable-cues.R`, `test-chips-row.R`, `test-filter-collapse.R`; commit**

```bash
git add app/R/global.R app/www/app.css app/tests/testthat/test-clickable-cues.R
git commit -m "Mark filter chips that open their control and headers that explain themselves"
```

---

### Task 5: Hint line above tables

**Files:**
- Modify: `app/R/helpers.R` (add after `ribbon_link_cell()`, ~line 4644; mixed line endings)
- Modify: `app/R/ui_tab1_onoff.R:190`, `ui_tab8_euro.R:104`, `ui_tab2_lineup.R:179`, `ui_tab10_euro_lineups.R:141`, `ui_tab4_gamelogs.R:152`, `ui_tab11_euro_gamelogs.R:74`, `ui_tab7_compare.R:404`
- Modify: `app/www/app.css` (cues block)
- Create: `app/tests/testthat/test-table-hint.R`

**Interfaces:**
- Produces: `table_hint(...)`, taking one or more keys from `names(TABLE_HINT_PHRASES)` and returning a `shiny.tag` (`div.table-hint` holding `span.hint-desktop` and `span.hint-touch`).

- [ ] **Step 1: Write the failing test**

Create `app/tests/testthat/test-table-hint.R`:

```r
test_that("table_hint writes one phrase per key, in order", {
  html <- as.character(table_hint("pivot", "sort"))

  expect_match(html, "Click a player or team for lineups and game logs", fixed = TRUE)
  expect_match(html, "Click a column header to sort", fixed = TRUE)
  expect_lt(regexpr("player or team", html), regexpr("to sort", html))
})

test_that("table_hint carries a touch wording beside the desktop one", {
  html <- as.character(table_hint("lineup", "explain"))

  expect_match(html, 'class="hint-desktop"', fixed = TRUE)
  expect_match(html, 'class="hint-touch"', fixed = TRUE)
  expect_match(html, "Hover over a column header for its definition", fixed = TRUE)
  expect_match(html, "Tap a lineup for its game log", fixed = TRUE)
  expect_match(html, "Tap a column header for its definition", fixed = TRUE)
})

test_that("table_hint refuses a claim it has no wording for", {
  expect_error(table_hint("pivot", "teleport"), "teleport")
  expect_error(table_hint(), "at least one")
})

test_that("every interactive table has a hint, in both leagues", {
  hinted <- c("ui_tab1_onoff.R", "ui_tab8_euro.R", "ui_tab2_lineup.R",
              "ui_tab10_euro_lineups.R", "ui_tab4_gamelogs.R",
              "ui_tab11_euro_gamelogs.R", "ui_tab7_compare.R")

  for (f in hinted) {
    expect_true(grepl("table_hint(", read_repo_txt("R", f), fixed = TRUE), info = f)
  }
})
```

If `table_hint` is not visible to the test, check how `tests/testthat/helper-server-mocks.R` sources `R/helpers.R` and follow the same route; do not copy the function into a mock.

- [ ] **Step 2: Run it and see it fail**

Expected: `could not find function "table_hint"`.

- [ ] **Step 3: Implement**

Add to `app/R/helpers.R` after `ribbon_link_cell()`:

```r
# One line above a table saying what it responds to. A phrase is written once
# here and shared by both leagues; `%s` takes the verb, which differs between
# pointer and touch.
TABLE_HINT_PHRASES <- c(
  pivot    = "%s a player or team for lineups and game logs",
  lineup   = "%s a lineup for its game log",
  gameflow = "%s Gameflow for a game's lineup timeline",
  compare  = "%s a row for the head-to-head detail",
  sort     = "%s a column header to sort",
  explain  = "%s a column header for its definition"
)

# Keys whose pointer verb is not "Click".
TABLE_HINT_POINTER_VERBS <- c(explain = "Hover over")

table_hint <- function(...) {
  keys <- c(...)
  if (!length(keys)) stop("table_hint() needs at least one interaction key")
  unknown <- setdiff(keys, names(TABLE_HINT_PHRASES))
  if (length(unknown)) {
    stop("table_hint(): no wording for ", paste(unknown, collapse = ", "))
  }

  line <- function(touch) {
    verbs <- if (touch) {
      rep("Tap", length(keys))
    } else {
      ifelse(keys %in% names(TABLE_HINT_POINTER_VERBS),
             TABLE_HINT_POINTER_VERBS[keys], "Click")
    }
    paste(sprintf(TABLE_HINT_PHRASES[keys], verbs),
          collapse = paste0(" ", intToUtf8(183L), " "))
  }

  tags$div(
    class = "table-hint",
    tags$span(class = "hint-desktop", line(FALSE)),
    tags$span(class = "hint-touch", line(TRUE))
  )
}
```

Run `git diff --stat -- app/R/helpers.R`; expected about 40 insertions and nothing else.

- [ ] **Step 4: CSS**

Append inside the Clickable cues block:

```css
/* Hint line. Wording follows the input device, not the viewport, so an iPad
   at desktop width still reads "Tap". */
.table-hint {
  margin: 2px 0 8px;
  color: var(--ibpl-text-muted);
  font-size: 0.76rem;
}
.table-hint .hint-touch { display: none; }
@media (hover: none) {
  .table-hint .hint-desktop { display: none; }
  .table-hint .hint-touch { display: inline; }
}
```

- [ ] **Step 5: Call it from each tab**

Insert the call as the sibling immediately before the table output (keep the trailing comma):

| File | Before | Insert |
|---|---|---|
| `ui_tab1_onoff.R` | `DTOutput("onoff_dt"),` | `table_hint("pivot", "sort", "explain"),` |
| `ui_tab8_euro.R` | `DTOutput("euro_dt")` | `table_hint("pivot", "sort", "explain"),` |
| `ui_tab2_lineup.R` | `DTOutput("ld_table")` | `table_hint("lineup", "sort", "explain"),` |
| `ui_tab10_euro_lineups.R` | `DTOutput("euro_ld_dt")` | `table_hint("lineup", "sort", "explain"),` |
| `ui_tab4_gamelogs.R` | `DTOutput("gl_table")` | `table_hint("gameflow", "sort", "explain"),` |
| `ui_tab11_euro_gamelogs.R` | `DTOutput("eurogl_table")` | `table_hint("gameflow", "sort", "explain"),` |
| `ui_tab7_compare.R` | `DT::dataTableOutput("cmp_table")` | `table_hint("compare", "sort"),` |

Drop `"compare"` from Tab 7 if Task 3 Step 5 found no clickable rows in the default mode; drop `"explain"` from any tab whose headers carry no `data-tooltip`. Tabs 3, 5 and 9 get no hint unless Task 0 Step 3 found an interaction there.

- [ ] **Step 6: Run the tests**

Run `test-table-hint.R`, then the whole suite. Expected: same counts as the Task 0 baseline plus the new tests, `test-source-encoding.R` green.

- [ ] **Step 7: Check in the browser**

Each hinted tab shows one muted line directly above its table, not wrapping at 1280px. In a phone emulation with touch enabled the wording reads "Tap". Each claim on each tab is true: do the thing it says.

- [ ] **Step 8: Commit**

```bash
git add app/R/helpers.R app/R/ui_tab1_onoff.R app/R/ui_tab8_euro.R app/R/ui_tab2_lineup.R app/R/ui_tab10_euro_lineups.R app/R/ui_tab4_gamelogs.R app/R/ui_tab11_euro_gamelogs.R app/R/ui_tab7_compare.R app/www/app.css app/tests/testthat/test-table-hint.R
git commit -m "Hint line above interactive tables, shared by both leagues"
```

---

### Task 6: Full pass

**Files:** fixes only, in the files above.

- [ ] **Step 1: Desktop pass (1440px)**

Walk every tab in both leagues against the spec's inventory table. For each of the 13 items confirm the cue is visible without hovering. Take one screenshot per tab into the session scratchpad.

- [ ] **Step 2: Phone pass (390px, touch emulation)**

Same walk. Also confirm: burger menu shows view modes; hint lines read "Tap"; no horizontal page scroll introduced; carets do not push names onto a second line in the sticky name column.

- [ ] **Step 3: Exports and copy**

Export CSV from Tab 1 and Tab 2. The files contain no `›` or `▾`. Select and copy a player name from Tab 1: the pasted text has no caret.

- [ ] **Step 4: Health check and suite**

Run the `run-shiny-local` health check (non-zero `nav-link` / `nav-item` counts, every local asset 200) and the whole test suite.

- [ ] **Step 5: Report**

Report to the user: what each tab now shows, anything from Task 0 Step 3 that was added to the spec, and anything that did not pass. Do not merge or push; `main` auto-deploys, so that is the user's call.
