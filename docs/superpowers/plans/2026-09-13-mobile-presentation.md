# Mobile Presentation Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Present the existing Shiny app usably on a phone without changing any functionality, metric, or query.

**Architecture:** One cached HTML page for every device, reshaped client-side by two new files — `app/www/mobile.css` and `app/www/mobile.js` — scoped to a `body.ibpl-mobile` class that `mobile.js` sets from a `matchMedia` query. No server file, reactive, SQL function, MV or cache key is touched. Four small R edits wire the layer in.

**Tech Stack:** R 4.4.2, Shiny, bslib (BS5), DT/DataTables, vanilla ES5 JS (matching `app.js` style), testthat, Playwright via the MCP browser tools.

**Spec:** `docs/superpowers/specs/2026-09-13-mobile-presentation-design.md` (committed `190ac2f`)

## Global Constraints

- **Breakpoint is `767.98px`**, matching the `d-md-none` filter toggles in all 11 tab UIs. Never a different value.
- **Mode carrier is `body.ibpl-mobile`.** All of `mobile.css` is scoped to it. Never a bare `@media` block in `mobile.css`.
- **No server changes.** No edits to any `server_tab*.R`, no new/renamed/removed Shiny input, no new reactive, no cache-key change. A change that appears to need one means the design was wrong.
- **All 41 `datatable()` calls stay exactly as they are.** `scrollX = TRUE` is left in place; it goes inert on its own when few columns are visible.
- **JS style matches `app.js`:** ES5, `var`, IIFE per concern, no arrow functions, no template literals.
- **`IBPL_CACHE_UI=false` is required in the shell while editing `mobile.css`/`mobile.js`**, because `includeCSS`/`includeScript` read them at UI build time. Without it an edit needs an app restart, not a browser reload.
- **Launch the app with `runApp()` only, never select-all + Ctrl+Enter.** The latter builds a BS3-style navbar and caches the broken build for the life of the process. Health check: the served page contains 11 `nav-link` occurrences.
- **Line endings are mixed and load-bearing.** Verified 2026-09-13:
  - `app/app.R` — 678 lines, **355 CRLF**. Line 51 is **LF**; lines 73, 74, 90, 91 are **CRLF**.
  - `app/R/global.R` — 1560 lines, **1030 CRLF**. Line 758 is **CRLF**.
  - `app/www/app.css` / `app.js` — CRLF in the worktree, **LF in the git blob** (`core.autocrlf=true` handles them).
  - `grep -c $'\r'` and `cat -A` **both report 0 CRs in `app.R` and lie**. Use `perl -ne 'print "$.\n" if /\r\n$/'` to locate them and `tr -cd '\r' | wc -c` to count them.
  - Every task that edits `app.R` or `global.R` MUST end with a `git diff --stat` plausibility check and a CR-count check.
- **New files (`mobile.css`, `mobile.js`) are written LF-only.** Git will apply `autocrlf` on checkout, matching `app.css`/`app.js`.

## File Structure

| File | Responsibility |
|---|---|
| `app/www/mobile.css` (new) | Every mobile style rule, all scoped `body.ibpl-mobile`. Also absorbs the three existing scattered `@media` blocks. |
| `app/www/mobile.js` (new) | Mode signal, table priority/expand layer, sheet component, navbar relocation, tooltip and popover relocation. One IIFE per concern, mirroring `app.js`. |
| `app/app.R` | 2 edits: the two `include*` calls behind the `IBPL_MOBILE` guard; `collapsible = TRUE`. |
| `app/R/global.R` | 2 edits: drop `maximum-scale=1`; define `IBPL_MOBILE`. |
| `app/tests/testthat/test-mobile-layer.R` (new) | Contract tests for the whole layer, following the existing `read_repo_txt` + `global_defs` patterns. |

## Findings that changed the spec

Three things were verified during planning that the spec got wrong or left open. They are corrected here and this plan is authoritative over the spec on these points.

1. **Row tap cannot be the expand affordance.** **Compare** binds genuine whole-row delegates — `table.on('click', 'tbody tr', ...)` at `server_tab7_compare.R:3324` and `:3980`, reaching `cmp_table` via `ui_tab7_compare.R:401`. Those two are the **only** `tbody tr` delegates in the repo. The expand control is therefore a **dedicated caret button** in the first priority cell — which is what the approved mockup showed (`+ Avdija`).

   **Correction (2026-09-13, found in the Task 3 review).** An earlier version of this finding also claimed Tab 2 bound a whole-row click, citing `input$ld_lineup_click` at `server_tab2.R:478`. That was wrong — inferred from the input's *existence* without checking how it is set. The input is set from `app.js:107` by a delegate on `a.ld-lineup-link` (`helpers.R:3043`), an anchor inside the "Players" column, which is one of the columns the priority rule **keeps**. Tab 2's link was therefore never at risk from column hiding. The conclusion is unchanged — Compare alone justifies the caret — but the evidence was half wrong, and any result of the form "`ld_lineup_click` stayed null" proves nothing about this hazard.

   **The live hazard is the child row, not the caret.** Task 3's sweep is unconditional over every `table.dataTable` on the page, so it already hides columns and inserts child rows on Compare's tables *before* Task 7 adds `cmp_table`'s override. A click on child-row *content* bubbles through the child `<tr>` and matches Compare's delegate. `stopPropagation()` on the caret does not cover that.
2. **Priority overrides must be keyed by column NAME, not index.** There are only **11 DT output ids** (`onoff_dt`, `ld_table`, `tr_table`, `gl_table`, `euro_dt`, `euro_ld_dt`, `euroteam_table`, `eurogl_table`, `ts_table`, `tst_table`, `cmp_table`) serving 41 `datatable()` calls, because one output renders a different column set per view mode. An index-keyed override would corrupt the other view modes. Name matching degrades safely to the default when it does not match.
3. **Compare's columns are `A` and `B`, not "Side A"/"Side B"** (`server_tab7_compare.R:3-9`, `:3300-3315`). The spec took those names from explainer prose. The override is `["Team", "Player", "A", "B", "Gap"]`.

4. **Gameflow / the stint ribbon was missed entirely by the spec**, and is now
   Task 8. The spec's Component 4 put "the rotation chart's hover tooltip" out
   of scope — a description that obscured that this is the **gameflow** feature
   reached from both game-log tabs. Two measured problems: the `gameflow`
   column is position 4, so the default priority rule hides the only entry
   point to the ribbon; and the ribbon SVG scales from a 1070-unit viewBox to
   ~370px, rendering its 11px labels at **3.8px**. The spec's out-of-scope line
   for the chart hover tooltip still stands — only the hover *preview* is lost
   on touch, because the ribbon also binds `click`/`focusin`/`keydown` and its
   lanes carry `tabindex="0"`.

Also verified: **`shinytest2` is NOT installed**, so `test-e2e-tabs-shinytest2.R` is dormant (`skip_if_not_installed`). Verification uses the Playwright MCP browser tools, which need no `npx` — memory records `npx` as broken under the Bash tool here.

---

### Task 1: Foundation — mode signal, kill switch, viewport fix

**Files:**
- Create: `app/www/mobile.css`
- Create: `app/www/mobile.js`
- Modify: `app/app.R` (header `tagList`, after line 74 and after line 90)
- Modify: `app/R/global.R:758` (viewport), and the env block near `REF_CACHE_TTL_SEC` (`global.R:236`)
- Test: `app/tests/testthat/test-mobile-layer.R`

**Interfaces:**
- Consumes: nothing.
- Produces: `body.ibpl-mobile` class; a `ibpl:mobilechange` DOM event with `detail.mobile` (boolean); global `IBPL_MOBILE` (logical) in R; `window.IBPL_MOBILE_MQ` (string `"(max-width: 767.98px)"`) for later tasks to reuse.

- [ ] **Step 1: Write the failing test**

Create `app/tests/testthat/test-mobile-layer.R`:

```r
test_that("the viewport meta allows pinch zoom", {
  shared_head_tags <- global_defs("shared_head_tags")$shared_head_tags
  # shared_head_tags() returns a bare tags$head(...): htmltools::renderTags()
  # hoists head content into $head (via takeHeads()), leaving $html empty for
  # this input regardless of content -- so the meta tag is asserted on $head.
  # Asserting on $html would make the first check impossible to pass and the
  # second vacuously true.
  html <- htmltools::renderTags(shared_head_tags())$head

  expect_match(html, "width=device-width", fixed = TRUE)
  # maximum-scale=1 blocks pinch zoom, which is an accessibility failure and is
  # unnecessary once the layout actually fits.
  expect_false(grepl("maximum-scale", html, fixed = TRUE))
})

test_that("the mobile layer is loaded after app.css and app.js", {
  app_r <- read_repo_txt("app.R")

  expect_true(grepl("www/mobile.css", app_r, fixed = TRUE))
  expect_true(grepl("www/mobile.js", app_r, fixed = TRUE))
  # Load order: app.css carries load-bearing !important rules the mobile layer
  # must be able to override without removing them.
  expect_lt(
    regexpr("www/app.css", app_r, fixed = TRUE),
    regexpr("www/mobile.css", app_r, fixed = TRUE)
  )
  expect_lt(
    regexpr("www/app.js", app_r, fixed = TRUE),
    regexpr("www/mobile.js", app_r, fixed = TRUE)
  )
})

test_that("the mobile layer has a kill switch defaulting to on", {
  global_r <- read_repo_txt("R", "global.R")

  expect_true(grepl("IBPL_MOBILE", global_r, fixed = TRUE))
  expect_true(grepl('Sys.getenv("IBPL_MOBILE", "true")', global_r, fixed = TRUE))
})

test_that("mode is carried by a body class, not a bare media query", {
  css <- read_repo_txt("www", "mobile.css")
  js <- read_repo_txt("www", "mobile.js")

  expect_true(grepl("body.ibpl-mobile", css, fixed = TRUE))
  expect_true(grepl("max-width: 767.98px", js, fixed = TRUE))
  # A bare @media in mobile.css would be a second source of truth for "are we
  # mobile", which could disagree with the class the JS sets.
  expect_false(grepl("@media (max-width", css, fixed = TRUE))
})
```

- [ ] **Step 2: Run the test to verify it fails**

```bash
RSCRIPT="/c/Program Files/R/R-4.4.2/bin/Rscript.exe"
"$RSCRIPT" -e "testthat::test_file('app/tests/testthat/test-mobile-layer.R')"
```

Expected: FAIL — `mobile.css` does not exist (`readLines` error), and `maximum-scale` is present.

- [ ] **Step 3: Create `app/www/mobile.js`**

```js
/* mobile.js -- viewport-driven mobile presentation layer.

   Loaded after app.js so it can override behaviour without editing it. Every
   rule in mobile.css is scoped to the body class this file sets, so there is
   exactly one answer to "are we in mobile mode" and the CSS and JS cannot
   disagree about it.

   ES5 and one IIFE per concern, matching app.js. */

window.IBPL_MOBILE_MQ = "(max-width: 767.98px)";

(function () {
  var BODY_CLASS = "ibpl-mobile";
  var last = null;

  function isMobile() {
    if (!window.matchMedia) return false;
    return window.matchMedia(window.IBPL_MOBILE_MQ).matches;
  }

  function applyMode() {
    var on = isMobile();
    // Only announce real transitions. resize fires continuously on a phone
    // when the URL bar collapses, and every listener downstream redraws tables.
    if (on === last) return;
    last = on;
    document.body.classList.toggle(BODY_CLASS, on);
    document.dispatchEvent(new CustomEvent("ibpl:mobilechange", {
      detail: { mobile: on }
    }));
  }

  function init() {
    applyMode();
    if (window.matchMedia) {
      var mq = window.matchMedia(window.IBPL_MOBILE_MQ);
      // addEventListener on a MediaQueryList is unsupported in older Safari,
      // where addListener is the only option.
      if (mq.addEventListener) {
        mq.addEventListener("change", applyMode);
      } else if (mq.addListener) {
        mq.addListener(applyMode);
      }
    }
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
```

- [ ] **Step 4: Create `app/www/mobile.css`**

```css
/* mobile.css -- every mobile style rule for the app.

   Scoped to body.ibpl-mobile, set by mobile.js from a matchMedia query, rather
   than to a bare @media block. One source of truth for the mode. */

body.ibpl-mobile {
  /* Tap targets below this are hard to hit reliably; referenced by the filter
     sheet and the caret controls. */
  --ibpl-m-tap: 44px;
}
```

- [ ] **Step 5: Add `IBPL_MOBILE` to `global.R`**

Insert immediately before the `REF_CACHE_TTL_SEC` line (`global.R:236`). Use a byte-exact insert that matches the surrounding **CRLF** endings:

```bash
perl -i -pe 'BEGIN{$d=0} if (!$d && /^REF_CACHE_TTL_SEC <- /) { print "# The mobile presentation layer (www/mobile.css + www/mobile.js). Set\r\n# IBPL_MOBILE=false to serve the desktop layout to every viewport.\r\nIBPL_MOBILE <- !tolower(trimws(Sys.getenv(\"IBPL_MOBILE\", \"true\"))) %in% c(\"false\", \"0\", \"no\")\r\n\r\n"; $d=1 }' app/R/global.R
```

- [ ] **Step 6: Drop `maximum-scale=1` from the viewport meta**

`global.R:758` is CRLF. Replace only the content string, byte-exactly:

```bash
perl -i -pe 's{content = "width=device-width, initial-scale=1, maximum-scale=1"}{content = "width=device-width, initial-scale=1"}' app/R/global.R
```

- [ ] **Step 7: Verify the `global.R` edit did not rewrite the file**

```bash
git diff --stat app/R/global.R
tr -cd '\r' < app/R/global.R | wc -c
```

Expected: about **5 insertions, 1 deletion** — not 1560. CR count **1034** (was 1030; the insert added 4 CRLF lines). If the diffstat shows the whole file, `git checkout app/R/global.R` and redo with `perl`, never with a whole-file rewrite.

- [ ] **Step 8: Wire the two includes into `app.R`**

Lines 73/74 and 90/91 are **CRLF**, so the inserted lines must be CRLF too:

```bash
perl -i -pe '
  if (/^    includeCSS\("www\/app\.css"\),\r?$/) {
    $_ .= qq{    if (IBPL_MOBILE) includeCSS("www/mobile.css"),\r\n};
  } elsif (/^    includeScript\("www\/app\.js"\),\r?$/) {
    $_ .= qq{    if (IBPL_MOBILE) includeScript("www/mobile.js"),\r\n};
  }' app/app.R
```

`tagList()` drops `NULL`, and a one-armed `if` returns `NULL` when false, so the guard needs no `else`.

- [ ] **Step 9: Verify the `app.R` edit**

```bash
git diff --stat app/app.R
tr -cd '\r' < app/app.R | wc -c
```

Expected: **2 insertions, 0 deletions**. CR count **357** (was 355). Anything larger means the file was rewritten — revert and redo.

- [ ] **Step 10: Run the tests to verify they pass**

```bash
"$RSCRIPT" -e "testthat::test_file('app/tests/testthat/test-mobile-layer.R')"
```

Expected: PASS, 4 tests.

- [ ] **Step 11: Launch the app and confirm it still boots**

```bash
IBPL_CACHE_UI=false "$RSCRIPT" -e "shiny::runApp('app', port = 7788, launch.browser = FALSE)"
```

Then with the Playwright MCP tools: `browser_navigate` to `http://127.0.0.1:7788`, `browser_resize` to 390x844, and `browser_evaluate` with
`() => ({ mobile: document.body.classList.contains('ibpl-mobile'), navLinks: document.querySelectorAll('.nav-link').length })`.

Expected: `mobile: true`, `navLinks: 11`. Then `browser_resize` to 1440x900 and re-evaluate: `mobile: false`. Check `browser_console_messages` for new errors.

- [ ] **Step 12: Commit**

```bash
git add app/www/mobile.css app/www/mobile.js app/app.R app/R/global.R app/tests/testthat/test-mobile-layer.R
git diff --cached --stat
git commit -m "feat(mobile): add viewport-driven mobile layer foundation"
```

Check the diffstat before committing: `app.R` +2, `global.R` +5/-1.

---

### Task 2: Migrate the three existing `@media` blocks into `mobile.css`

**Files:**
- Modify: `app/www/mobile.css`
- Modify: `app/www/app.css` (remove blocks at `:637`, `:923`, `:1401`)
- Test: `app/tests/testthat/test-mobile-layer.R`

**Interfaces:**
- Consumes: `body.ibpl-mobile` from Task 1.
- Produces: nothing new. This is a **move**, not a rewrite.

Verify this task by reversing the transform and diffing, not by writing new tests for moved rules. The three blocks are: `app.css:637-646` (DT font/padding, navbar brand, slider handles, legend, example grid), `app.css:923-925` (`.chips-row-controls`), `app.css:1401-1403` (`.chips-filters-toggle { display: none; }`).

- [ ] **Step 1: Write the failing test**

Append to `test-mobile-layer.R`:

```r
test_that("mobile rules live only in mobile.css", {
  app_css <- read_repo_txt("www", "app.css")
  mobile_css <- read_repo_txt("www", "mobile.css")

  # The three blocks that used to be scattered through app.css.
  expect_false(grepl("@media (max-width: 768px)", app_css, fixed = TRUE))
  expect_true(grepl(".chips-filters-toggle", mobile_css, fixed = TRUE))
  expect_true(grepl(".irs-handle", mobile_css, fixed = TRUE))
  expect_true(grepl(".chips-row-controls", mobile_css, fixed = TRUE))

  # Non-mobile media queries must NOT be dragged along.
  expect_true(grepl("prefers-reduced-motion", app_css, fixed = TRUE))
})
```

- [ ] **Step 2: Run it to verify it fails**

```bash
"$RSCRIPT" -e "testthat::test_file('app/tests/testthat/test-mobile-layer.R')"
```

Expected: FAIL — `@media (max-width: 768px)` is still in `app.css`.

- [ ] **Step 3: Append the migrated rules to `mobile.css`**

Same declarations, rescoped from `@media` to the body class:

```css
/* ---- Migrated from app.css (@media max-width:768px at :637, :923, :1401) ----
   Same declarations, rescoped to the body class. No behaviour change intended. */

body.ibpl-mobile table.dataTable tbody td { font-size: 0.8rem; padding: 4px 6px !important; }
body.ibpl-mobile table.dataTable thead th { font-size: 0.75rem; padding-top: 8px !important; padding-bottom: 8px !important; }
body.ibpl-mobile .navbar-brand { font-size: 0.9rem; }
body.ibpl-mobile .dataTables_wrapper { width: 100% !important; overflow-x: auto; }
body.ibpl-mobile .irs-handle { width: 32px !important; height: 32px !important; top: -8px !important; }
body.ibpl-mobile .irs-bar, body.ibpl-mobile .irs-line { height: 8px !important; }
body.ibpl-mobile .legend-box { flex-wrap: wrap; gap: 10px; padding: 8px 12px; font-size: 0.75rem; }
body.ibpl-mobile .example-grid { grid-template-columns: 1fr; }

body.ibpl-mobile .chips-row-controls { flex: 1 1 100%; flex-wrap: wrap; gap: 12px; }

/* The panel is already collapsed behind the tab's own Show Filters button on
   narrow viewports, so the toggle would be a second control for one thing. */
body.ibpl-mobile .chips-filters-toggle { display: none; }
```

- [ ] **Step 4: Delete the three blocks from `app.css`**

Delete highest line number first so earlier line numbers stay valid. Use exact-string edits — never a multi-line regex, which has truncated 170 lines in this repo before.

Delete `app.css:1400-1403` (the comment plus the block), `:923-925`, and `:636-646` (the `/* ---- Mobile Responsive ---- */` header plus the block).

- [ ] **Step 5: Verify the move was faithful**

```bash
git diff --stat app/www/app.css app/www/mobile.css
# Every declaration removed from app.css must appear in mobile.css.
git diff app/www/app.css | grep '^-' | grep -v '^---' | grep -oE '[a-z-]+\s*:' | sort -u > /tmp/removed.txt
grep -oE '[a-z-]+\s*:' app/www/mobile.css | sort -u > /tmp/present.txt
comm -23 /tmp/removed.txt /tmp/present.txt
```

Expected: `comm` prints nothing (every removed property is present in `mobile.css`). `app.css` about **-20 lines**, no other churn.

- [ ] **Step 6: Run the tests**

```bash
"$RSCRIPT" -e "testthat::test_file('app/tests/testthat/test-mobile-layer.R')"
"$RSCRIPT" -e "testthat::test_file('app/tests/testthat/test-navbar-hover-menu.R')"
"$RSCRIPT" -e "testthat::test_file('app/tests/testthat/test-chips-row.R')"
"$RSCRIPT" -e "testthat::test_file('app/tests/testthat/test-design-tokens.R')"
```

Expected: all PASS. Those three existing files read `app.css` directly and are the regression net for this move.

- [ ] **Step 7: Commit**

```bash
git add app/www/app.css app/www/mobile.css app/tests/testthat/test-mobile-layer.R
git commit -m "refactor(mobile): move scattered @media blocks into mobile.css"
```

---

### Task 3: Table layer — priority columns and caret expand

**Files:**
- Modify: `app/www/mobile.js`
- Modify: `app/www/mobile.css`
- Test: `app/tests/testthat/test-mobile-layer.R`

**Interfaces:**
- Consumes: `body.ibpl-mobile`, `ibpl:mobilechange` from Task 1.
- Produces: `window.IBPL_MOBILE_TABLE.priority` — the override map, `{ outputId: [columnName, ...] }`, so Task 7 can add the Compare entry. CSS classes `.ibpl-m-caret`, `.ibpl-m-detail`, `.ibpl-m-detail-row`, `.ibpl-m-detail-label`, `.ibpl-m-detail-value`.

Behaviour: keep the first 3 originally-visible columns (or a name-matched override), hide the rest, and add a caret button in the first cell that opens the hidden columns as a DataTables child row.

**Why a caret and not a row tap:** Compare (`server_tab7_compare.R:3324`) and Tab 2 (`input$ld_lineup_click`) already bind whole-row clicks. Stealing that click would break the lineup modal and the Compare detail view.

- [ ] **Step 1: Write the failing test**

Append to `test-mobile-layer.R`:

```r
test_that("the table layer keeps a caret, not a row tap", {
  js <- read_repo_txt("www", "mobile.js")

  # Compare and Tab 2 already bind tbody tr clicks for their own modals.
  expect_true(grepl("ibpl-m-caret", js, fixed = TRUE))
  expect_false(grepl('on("click", "table.dataTable > tbody > tr"', js, fixed = TRUE))
})

test_that("the table layer re-applies on every draw and guards re-entry", {
  js <- read_repo_txt("www", "mobile.js")

  # DT re-renders every cell on sort, page and filter, so per-cell state is
  # gone by the next draw.
  expect_true(grepl("draw.dt", js, fixed = TRUE))
  # column().visible() triggers a redraw, which fires draw.dt, which recurses.
  expect_true(grepl("applying", js, fixed = TRUE))
  # Hiding columns desyncs the header from the body without this.
  expect_true(grepl("columns.adjust()", js, fixed = TRUE))
})

test_that("priority overrides are keyed by column name, not index", {
  js <- read_repo_txt("www", "mobile.js")

  # One DT output id serves several view modes with different column sets, so
  # an index-keyed override would corrupt the other modes.
  expect_true(grepl("IBPL_MOBILE_TABLE", js, fixed = TRUE))
  expect_true(grepl("indexOf", js, fixed = TRUE))
  expect_true(grepl("render(\"display\")", js, fixed = TRUE))
})
```

- [ ] **Step 2: Run it to verify it fails**

```bash
"$RSCRIPT" -e "testthat::test_file('app/tests/testthat/test-mobile-layer.R')"
```

Expected: FAIL — `ibpl-m-caret` not found in `mobile.js`.

- [ ] **Step 3: Append the table layer to `mobile.js`**

```js
/* ---- Mobile table layer -------------------------------------------------
   Wide tables (up to 18 visible columns) are unusable on a phone. Keep a few
   priority columns and move the rest into a DataTables child row behind a
   caret.

   A caret rather than a row tap because Compare and Tab 2 already bind
   tbody tr clicks for their own modals.

   Re-applied on every draw: DataTables re-renders every cell on sort, page and
   filter, so nothing can be stored on a cell.
   ----------------------------------------------------------------------- */
window.IBPL_MOBILE_TABLE = {
  // Ordered preference lists keyed by DT output id. Matching is by column
  // HEADER NAME: one output id serves several view modes with different column
  // sets, so a name list simply fails to match in the wrong mode and falls
  // back to the default. Task 7 adds cmp_table.
  priority: {}
};

(function () {
  var DEFAULT_KEEP = 3;
  var MAX_KEEP = 4;
  var applying = false;

  function $() { return window.jQuery; }

  function escapeHtml(s) {
    return String(s).replace(/&/g, "&amp;").replace(/</g, "&lt;")
      .replace(/>/g, "&gt;").replace(/"/g, "&quot;");
  }

  function isMobile() {
    return document.body.classList.contains("ibpl-mobile");
  }

  function headerNames(api) {
    var out = [];
    api.columns().every(function () {
      var th = this.header();
      out[this.index()] = th ? (th.textContent || "").trim() : "";
    });
    return out;
  }

  function outputIdOf(node) {
    var wrap = $()(node).closest(".datatables");
    return wrap.length ? (wrap.attr("id") || "") : "";
  }

  // The set of columns visible the first time we see this table. Columns the R
  // side hid on purpose (the 16 raw shooting columns, the PR fields) must stay
  // hidden, so "restore" means restore to THIS set, never visible(true) on all.
  function origVisible(node, api) {
    var $node = $()(node);
    var rec = $node.data("ibplOrigVisible");
    if (rec) return rec;
    rec = [];
    api.columns().every(function () { if (this.visible()) rec.push(this.index()); });
    $node.data("ibplOrigVisible", rec);
    return rec;
  }

  function keepSet(api, outputId, orig) {
    var names = headerNames(api);
    var pref = window.IBPL_MOBILE_TABLE.priority[outputId];
    var keep = [];
    var i;

    if (pref) {
      for (i = 0; i < pref.length && keep.length < MAX_KEEP; i++) {
        var idx = names.indexOf(pref[i]);
        if (idx >= 0 && orig.indexOf(idx) >= 0) keep.push(idx);
      }
      // Fewer than two matches means this override belongs to a different view
      // mode of the same output. Fall through to the default rather than
      // rendering a one-column table.
      if (keep.length >= 2) return keep;
      keep = [];
    }

    for (i = 0; i < orig.length && keep.length < DEFAULT_KEEP; i++) keep.push(orig[i]);
    return keep;
  }

  function detailHtml(api, rowIdx, keep, orig) {
    var names = headerNames(api);
    var parts = [];
    for (var i = 0; i < orig.length; i++) {
      var c = orig[i];
      if (keep.indexOf(c) >= 0) continue;
      // render("display") returns the cell's rendered HTML, so the HeatCell /
      // ShotCell / FFCell gradient markup survives. Reading it as text would
      // lose the colour that carries the meaning. Escaping is whatever DT
      // already applied for that column -- identical to the main grid.
      parts.push(
        '<div class="ibpl-m-detail-row"><span class="ibpl-m-detail-label">' +
        escapeHtml(names[c]) +
        '</span><span class="ibpl-m-detail-value">' +
        api.cell(rowIdx, c).render("display") +
        "</span></div>"
      );
    }
    return '<div class="ibpl-m-detail">' + parts.join("") + "</div>";
  }

  function addCarets(api, keep, orig) {
    if (keep.length >= orig.length) return;
    var first = keep[0];
    api.rows({ page: "current" }).every(function () {
      // api.cell, not this.cell: inside rows().every() `this` is a row-scoped
      // API and addressing a cell through it is not a documented form.
      var cell = api.cell(this.index(), first);
      if (!cell) return;
      var node = cell.node();
      if (!node || node.querySelector(".ibpl-m-caret")) return;
      var btn = document.createElement("button");
      btn.type = "button";
      btn.className = "ibpl-m-caret";
      btn.setAttribute("aria-expanded", this.child.isShown() ? "true" : "false");
      btn.setAttribute("aria-label", "Show remaining columns");
      btn.textContent = this.child.isShown() ? "−" : "+";
      node.insertBefore(btn, node.firstChild);
    });
  }

  function removeCarets(node) {
    var carets = node.querySelectorAll(".ibpl-m-caret");
    for (var i = 0; i < carets.length; i++) {
      carets[i].parentNode.removeChild(carets[i]);
    }
  }

  function applyTable(api) {
    var node = api.table().node();
    var orig = origVisible(node, api);
    var keep = isMobile() ? keepSet(api, outputIdOf(node), orig) : orig;
    var changed = false;

    api.columns().every(function () {
      var want = keep.indexOf(this.index()) >= 0;
      if (this.visible() !== want) {
        this.visible(want, false);
        changed = true;
      }
    });

    if (!isMobile()) {
      api.rows().every(function () { if (this.child.isShown()) this.child.hide(); });
    }

    if (changed) {
      // columns.adjust() alone leaves the header measured against the old
      // layout; a redraw re-measures it and keeps the current page. The guarded
      // draw.dt handler skips this draw, so finish the caret pass below here.
      api.columns.adjust();
      api.draw(false);
    }
    if (isMobile()) {
      addCarets(api, keep, orig);
    } else {
      removeCarets(node);
    }
  }

  function applyAll() {
    if (!$() || !$().fn.dataTable) return;
    if (applying) return;
    applying = true;
    try {
      // Iterate DOM nodes and build one Api per table, the same construction
      // the draw.dt handler below uses. tables().every() is not a documented
      // idiom and would fail silently, leaving every table untouched.
      var nodes = document.querySelectorAll("table.dataTable");
      for (var i = 0; i < nodes.length; i++) {
        applyTable($().fn.dataTable.Api(nodes[i]));
      }
    } finally {
      applying = false;
    }
  }

  function bind() {
    if (!$()) return;

    $()(document).on("draw.dt", function (e) {
      if (applying) return;
      applying = true;
      try {
        applyTable($().fn.dataTable.Api(e.target));
      } finally {
        applying = false;
      }
    });

    $()(document).on("click", ".ibpl-m-caret", function (e) {
      e.preventDefault();
      e.stopPropagation();   // Compare and Tab 2 bind tbody tr clicks.
      var btn = this;
      var tableNode = $()(btn).closest("table.dataTable").get(0);
      if (!tableNode) return;
      var api = $().fn.dataTable.Api(tableNode);
      var row = api.row($()(btn).closest("tr").get(0));
      var orig = origVisible(tableNode, api);
      var keep = keepSet(api, outputIdOf(tableNode), orig);

      if (row.child.isShown()) {
        row.child.hide();
        btn.textContent = "+";
        btn.setAttribute("aria-expanded", "false");
      } else {
        row.child(detailHtml(api, row.index(), keep, orig)).show();
        btn.textContent = "−";
        btn.setAttribute("aria-expanded", "true");
      }
    });

    document.addEventListener("ibpl:mobilechange", applyAll);
    // Tables render lazily inside conditionalPanels, so a freshly shown table
    // has to be brought in line with the current mode.
    $()(document).on("shown.bs.tab shiny:value", applyAll);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bind);
  } else {
    bind();
  }
})();
```

- [ ] **Step 4: Append the table styles to `mobile.css`**

```css
/* ---- Mobile table layer ---- */
body.ibpl-mobile .ibpl-m-caret {
  appearance: none;
  background: var(--ibpl-surface-2);
  color: var(--ibpl-accent);
  border: 1px solid var(--ibpl-border);
  border-radius: 4px;
  width: 24px;
  height: 24px;
  min-width: 24px;
  margin-right: 6px;
  padding: 0;
  font-family: var(--bs-font-monospace, monospace);
  font-size: 0.9rem;
  line-height: 1;
  vertical-align: middle;
}
body.ibpl-mobile .ibpl-m-caret:focus-visible {
  outline: 2px solid var(--ibpl-accent);
  outline-offset: 1px;
}
/* The button is 24px but the row it sits in gives it the rest of the 44px. */
body.ibpl-mobile table.dataTable tbody td { min-height: var(--ibpl-m-tap); }

body.ibpl-mobile .ibpl-m-detail {
  display: grid;
  gap: 6px;
  padding: 8px 10px;
  background: var(--ibpl-surface);
}
body.ibpl-mobile .ibpl-m-detail-row {
  display: flex;
  align-items: baseline;
  justify-content: space-between;
  gap: 12px;
  border-bottom: 1px dotted var(--ibpl-border);
  padding-bottom: 4px;
}
body.ibpl-mobile .ibpl-m-detail-label {
  color: var(--ibpl-text-muted);
  font-size: 0.72rem;
  text-transform: uppercase;
  letter-spacing: 0.04em;
  flex: 0 0 auto;
}
body.ibpl-mobile .ibpl-m-detail-value {
  text-align: right;
  flex: 1 1 auto;
  min-width: 0;
}
```

- [ ] **Step 5: Run the tests to verify they pass**

```bash
"$RSCRIPT" -e "testthat::test_file('app/tests/testthat/test-mobile-layer.R')"
```

Expected: PASS, 7 tests.

- [ ] **Step 6: Verify in the browser — this is the real test**

Launch with `IBPL_CACHE_UI=false ... runApp('app', port = 7788)`. With the Playwright MCP tools: resize to 390x844, navigate, open the On/Off tab, wait for the table, then `browser_evaluate`:

```js
() => {
  const t = window.jQuery('#onoff_dt table.dataTable').DataTable();
  return {
    visible: t.columns(':visible').count(),
    carets: document.querySelectorAll('#onoff_dt .ibpl-m-caret').length,
    overflow: document.body.scrollWidth <= document.body.clientWidth
  };
}
```

Expected immediately after the first visibility change, without a second sort,
page, or filter action: `visible: 3`, `carets` equal to the row count,
`overflow: true` (no horizontal overflow). The redraw guard suppresses the
nested `draw.dt` event, so this initial pass must insert the carets itself.

Then `browser_click` the first caret and evaluate:

```js
() => {
  const d = document.querySelector('#onoff_dt .ibpl-m-detail');
  return { open: !!d, rows: d ? d.children.length : 0, html: d ? d.innerHTML.indexOf('<') >= 0 : false };
}
```

Expected: `open: true`, `rows` > 10, `html: true` — the child row contains **markup**, confirming the gradient cells survived. `html: false` means `render("display")` was flattened to text and the task is not done.

Finally resize to 1440x900 and confirm `visible` returns to its full count and
no caret remains in the DOM. Resize back to 390x844 and confirm the carets
return without another table interaction.

- [ ] **Step 7: Check the console**

`browser_console_messages` — expect no new errors. `NS_ERROR_CORRUPTED_CONTENT` on a cold load is a known pre-existing Connect Cloud issue, not caused here.

- [ ] **Step 8: Commit**

```bash
git add app/www/mobile.js app/www/mobile.css app/tests/testthat/test-mobile-layer.R
git commit -m "feat(mobile): priority columns and caret expand for DT tables"
```

---

### Task 4: Navigation — collapsible navbar and view-mode segmented control

**Files:**
- Modify: `app/app.R:51` (LF line — `navbarPage(`)
- Modify: `app/www/mobile.js`, `app/www/mobile.css`
- Test: `app/tests/testthat/test-mobile-layer.R`

**Interfaces:**
- Consumes: `body.ibpl-mobile`, `ibpl:mobilechange`.
- Produces: CSS class `.ibpl-m-viewmode` on the promoted radio group; a navbar
  cluster moved inside `.navbar-collapse` while mobile mode is active.

- [ ] **Step 1: Write the failing test**

```r
test_that("the navbar collapses into a burger", {
  app_r <- read_repo_txt("app.R")

  # Default is FALSE, so 7 tabs wrap or overflow on a phone without this.
  expect_true(grepl("collapsible = TRUE", app_r, fixed = TRUE))
})

test_that("mobile drives the real view-mode radios, not the hover menu", {
  js <- read_repo_txt("www", "mobile.js")
  css <- read_repo_txt("www", "mobile.css")

  # The hover menu is only ever a shortcut to .view-mode-container's radios.
  # Driving the radios directly cannot drift from the menu.
  expect_true(grepl(".view-mode-container", js, fixed = TRUE))
  expect_true(grepl("tab-hover-menu", css, fixed = TRUE))
  expect_true(grepl("ibpl-m-viewmode", css, fixed = TRUE))
})

test_that("the fixed navbar cluster is unfixed on mobile", {
  css <- read_repo_txt("www", "mobile.css")

  # app.R:93 sets position:fixed inline; it would sit on top of the burger.
  expect_true(grepl("navbar_right_cluster", css, fixed = TRUE))
  expect_true(grepl("position: static !important", css, fixed = TRUE))
  # body.league-* sets these to inline-flex; the override must not break the
  # league filtering that decides WHICH season selector shows.
  expect_true(grepl(".league-nav-il", css, fixed = TRUE))
  # Static positioning alone does not put a header node inside the burger.
  expect_true(grepl(".navbar-collapse", read_repo_txt("www", "mobile.js"), fixed = TRUE))
})
```

- [ ] **Step 2: Run it to verify it fails**

Expected: FAIL — `collapsible = TRUE` absent.

- [ ] **Step 3: Set `collapsible = TRUE`**

`app.R:51` is an **LF** line:

```bash
perl -i -pe 's{^  navbarPage\(\n?$}{  navbarPage(\n    collapsible = TRUE,\n}' app/app.R
```

Then verify: `git diff --stat app/app.R` → **+1**; `tr -cd '\r' < app/app.R | wc -c` → **357** (unchanged from Task 1).

- [ ] **Step 4: Add the navbar CSS**

```css
/* ---- Mobile navigation ---- */
/* app.R:93 sets position:fixed inline, which would sit on top of the burger. */
body.ibpl-mobile #navbar_right_cluster {
  position: static !important;
  display: flex !important;
  flex-wrap: wrap;
  gap: 8px;
  max-width: 100% !important;
  white-space: normal !important;
  padding: 8px 12px;
  border-top: 1px solid var(--ibpl-border);
}
/* body.league-il / -el set these to inline-flex and decide which season
   selector shows. Keep that decision, change only the box. */
body.ibpl-mobile.league-il .league-nav-il,
body.ibpl-mobile.league-el .league-nav-el { display: flex !important; width: 100%; }

/* Hover does not exist on touch, so the menu is unreachable; the radios it
   shortcuts to are promoted instead. */
body.ibpl-mobile .tab-hover-menu { display: none !important; }

body.ibpl-mobile .ibpl-m-viewmode {
  position: sticky;
  top: 0;
  z-index: 5;
  display: flex;
  gap: 4px;
  overflow-x: auto;
  padding: 6px 0;
  margin-bottom: 8px;
  background: var(--ibpl-bg);
}
body.ibpl-mobile .ibpl-m-viewmode .radio,
body.ibpl-mobile .ibpl-m-viewmode .shiny-options-group > div { margin: 0; }
body.ibpl-mobile .ibpl-m-viewmode label {
  display: inline-flex;
  align-items: center;
  min-height: var(--ibpl-m-tap);
  padding: 0 12px;
  border: 1px solid var(--ibpl-border);
  border-radius: 6px;
  white-space: nowrap;
}
body.ibpl-mobile .ibpl-m-viewmode input:checked + span,
body.ibpl-mobile .ibpl-m-viewmode label:has(input:checked) {
  border-color: var(--ibpl-accent);
  color: var(--ibpl-accent);
}
```

- [ ] **Step 5: Append the navbar-cluster relocation and view-mode promotion to `mobile.js`**

```js
/* ---- Mobile navigation relocation ---------------------------------------
   The fixed cluster is supplied through navbarPage(header = ...) outside the
   collapsed menu. Positioning it statically does not put it under the burger,
   so move the existing node into the collapse and restore it on desktop.
   The view-mode radios are also moved above each table on mobile. Preserve
   their sidebar positions with placeholders for the desktop transition.
   ----------------------------------------------------------------------- */
(function () {
  var clusterHome = null;

  function relocateCluster(on) {
    var cluster = document.getElementById("navbar_right_cluster");
    if (!cluster || !cluster.parentNode) return;
    if (!clusterHome) {
      clusterHome = document.createComment("navbar cluster home");
      cluster.parentNode.insertBefore(clusterHome, cluster);
    }
    if (on) {
      var tabs = document.getElementById("main_tabs");
      var collapse = tabs && tabs.closest(".navbar-collapse");
      if (collapse && cluster.parentNode !== collapse) collapse.appendChild(cluster);
    } else if (clusterHome.parentNode && cluster.parentNode !== clusterHome.parentNode) {
      clusterHome.parentNode.insertBefore(cluster, clusterHome.nextSibling);
    }
  }

  function promote(on) {
    var panes = document.querySelectorAll(".tab-pane");
    for (var i = 0; i < panes.length; i++) {
      var group = panes[i].querySelector(".view-mode-container");
      if (!group) continue;
      var main = panes[i].querySelector(".col-sm-9, .col-md-9, [role='main']");
      if (!main) continue;

      if (on) {
        if (group.getAttribute("data-ibpl-m-home")) continue;
        var holder = document.createElement("div");
        holder.className = "ibpl-m-viewmode";
        holder.setAttribute("data-ibpl-m-holder", "1");
        // Leave a placeholder in the sidebar. Moving the holder itself would
        // lose the original parent and restore the radios into the main panel.
        var home = document.createElement("span");
        home.style.display = "none";
        group.parentNode.insertBefore(home, group);
        holder.ibplHome = home;
        group.setAttribute("data-ibpl-m-home", "1");
        holder.appendChild(group);
        main.insertBefore(holder, main.firstChild);
      } else if (group.getAttribute("data-ibpl-m-home")) {
        var oldHolder = group.parentNode;
        group.removeAttribute("data-ibpl-m-home");
        if (oldHolder && oldHolder.getAttribute("data-ibpl-m-holder")) {
          var original = oldHolder.ibplHome;
          // document.contains, not just parentNode: a detached subtree still
          // has a parent, and inserting the live radios into one would remove
          // them from the page entirely.
          if (original && original.parentNode && document.contains(original)) {
            original.parentNode.insertBefore(group, original);
            original.parentNode.removeChild(original);
          } else {
            // A sidebar may have been re-rendered while the group was away.
            // Keep the live input in the page even if its marker disappeared.
            oldHolder.parentNode.insertBefore(group, oldHolder);
          }
          oldHolder.parentNode.removeChild(oldHolder);
        }
      }
    }
  }

  function sync() {
    var on = document.body.classList.contains("ibpl-mobile");
    relocateCluster(on);
    promote(on);
  }

  document.addEventListener("ibpl:mobilechange", sync);
  if (window.jQuery) window.jQuery(document).on("shown.bs.tab shiny:value", sync);
  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", sync);
  } else {
    sync();
  }
})();
```

Moving the existing nodes keeps their Shiny input bindings intact — Shiny binds
by `id`/`name`, not by position — so no input is re-registered and no observer
sees a change. The cluster and each view-mode group return to their original
parents when the viewport crosses back to desktop width.

- [ ] **Step 6: Run the tests**

Expected: PASS, 10 tests.

- [ ] **Step 7: Verify in the browser**

At 390x844, `browser_evaluate`:

```js
() => ({
  // Version-agnostic: a UI rendered outside an app context emits the BS3
  // class (navbar-toggle) while the real app emits BS5's (navbar-toggler).
  // The collapse attribute is present either way, so assert on that.
  burger: !!document.querySelector('.navbar [data-bs-toggle="collapse"], .navbar [data-toggle="collapse"]'),
  navLinks: document.querySelectorAll('.nav-link').length,
  clusterFixed: getComputedStyle(document.querySelector('#navbar_right_cluster')).position,
  clusterInBurger: !!document.querySelector('.navbar-collapse #navbar_right_cluster'),
  promoted: !!document.querySelector('.ibpl-m-viewmode .view-mode-container')
})
```

Expected: `burger: true`, `navLinks: 11`, `clusterFixed: "static"`,
`clusterInBurger: true`, `promoted: true`. Close the burger and verify the
cluster is hidden with its tabs; reopen it and verify the league and season
controls are usable.

`browser_click` the burger, then confirm only the current league's tabs are listed — count visible `.nav-item` and compare against `body.league-il`/`league-el`. Then click a view-mode option in the promoted control and confirm the table re-renders.

Resize to 1440x900 and confirm the cluster is outside `.navbar-collapse`, has
its original fixed positioning, and every `.view-mode-container` is back in its
sidebar. Resize to 390x844 once more and confirm both relocations still work.

- [ ] **Step 8: Commit**

```bash
git add app/app.R app/www/mobile.js app/www/mobile.css app/tests/testthat/test-mobile-layer.R
git commit -m "feat(mobile): collapsible navbar and promoted view-mode control"
```

---

### Task 5: Filter bottom sheet

**Files:**
- Modify: `app/www/mobile.js`, `app/www/mobile.css`
- Test: `app/tests/testthat/test-mobile-layer.R`

**Interfaces:**
- Consumes: `body.ibpl-mobile`.
- Produces: `window.IBPL_MOBILE_SHEET.open(titleString, contentNode)` and `.close()` — reused by Task 6 for tooltips and the stat-filter popover. CSS classes `.ibpl-m-sheet`, `.ibpl-m-sheet-backdrop`, `.ibpl-m-sheet-head`, `.ibpl-m-sheet-body`, `.ibpl-m-sheet-foot`.

All 11 filter toggles share one shape — `d-md-none w-100 mb-2`, `data-bs-toggle="collapse"`, `data-bs-target="#<prefix>-filters"` — so one generic transform covers every tab with **zero per-tab R edits**.

- [ ] **Step 1: Write the failing test**

```r
test_that("the filter sheet is generic over all 11 tabs", {
  js <- read_repo_txt("www", "mobile.js")

  # Matching the shared toggle shape means no per-tab R edit.
  expect_true(grepl('data-bs-target$=', js, fixed = TRUE))
  expect_true(grepl("-filters", js, fixed = TRUE))
  # Bootstrap keeps owning show/hide so the button, aria-expanded and the
  # chips-bar wiring all keep working untouched.
  expect_false(grepl("classList.remove(\"collapse\")", js, fixed = TRUE))
})

test_that("the sheet is a reusable component", {
  js <- read_repo_txt("www", "mobile.js")
  css <- read_repo_txt("www", "mobile.css")

  expect_true(grepl("IBPL_MOBILE_SHEET", js, fixed = TRUE))
  expect_true(grepl(".ibpl-m-sheet", css, fixed = TRUE))
  # dvh, with a vh fallback declared first, so browser chrome does not crop
  # the footer. Asserted generically here: this task introduces 85dvh for the
  # sheet, and Task 6 adds 100dvh for modals with its own ordering assertion.
  expect_true(grepl("dvh", css, fixed = TRUE))
})

test_that("the sheet body is cleared on open", {
  js <- read_repo_txt("www", "mobile.js")

  # Task 6 appends tooltip text into the sheet body directly. close() only
  # restores MOVED nodes, so without an explicit clear that text accumulates
  # across opens and leaks into the filter sheet.
  expect_true(grepl('body.innerHTML = ""', js, fixed = TRUE))
})

test_that("every tab still has its own filter toggle", {
  ui_files <- list.files(repo_file("R"), pattern = "^ui_tab.*\\.R$", full.names = TRUE)
  toggles <- sum(vapply(ui_files, function(f) {
    sum(grepl("d-md-none", readLines(f, warn = FALSE), fixed = TRUE))
  }, integer(1)))

  # 11 tab UIs each carry one. A drop here means a tab lost its filters.
  expect_gte(toggles, 11L)
})
```

- [ ] **Step 2: Run it to verify it fails**

Expected: FAIL — `IBPL_MOBILE_SHEET` not defined.

- [ ] **Step 3: Add the sheet component and the filter transform to `mobile.js`**

```js
/* ---- Bottom sheet ------------------------------------------------------
   One component, three users: the filter panel (this task), the tooltip text
   and the stat-filter popover (Task 6).

   The sheet MOVES the existing node rather than cloning it, so every Shiny
   input keeps its binding and its id. Cloning would register duplicate ids and
   silently break the filters.
   --------------------------------------------------------------------- */
window.IBPL_MOBILE_SHEET = (function () {
  var sheet = null, backdrop = null, body = null, head = null;
  var origin = null, content = null;

  function build() {
    if (sheet) return;
    backdrop = document.createElement("div");
    backdrop.className = "ibpl-m-sheet-backdrop";
    backdrop.addEventListener("click", close);

    sheet = document.createElement("div");
    sheet.className = "ibpl-m-sheet";
    sheet.setAttribute("role", "dialog");
    sheet.setAttribute("aria-modal", "true");

    head = document.createElement("div");
    head.className = "ibpl-m-sheet-head";

    body = document.createElement("div");
    body.className = "ibpl-m-sheet-body";

    var foot = document.createElement("div");
    foot.className = "ibpl-m-sheet-foot";
    var done = document.createElement("button");
    done.type = "button";
    done.className = "btn btn-warning w-100";
    done.textContent = "Apply";
    done.addEventListener("click", close);
    foot.appendChild(done);

    sheet.appendChild(head);
    sheet.appendChild(body);
    sheet.appendChild(foot);
    document.body.appendChild(backdrop);
    document.body.appendChild(sheet);
  }

  function open(title, node) {
    build();
    close();
    // close() returns a MOVED node to its origin, but it cannot know about
    // content that was appended directly (Task 6 injects tooltip text that
    // way). Without this, that text accumulates across opens and then shows
    // up above the filter panel on the next open.
    body.innerHTML = "";
    head.textContent = title || "";
    if (node) {
      // Remember exactly where it was so close() can put it back.
      origin = { parent: node.parentNode, next: node.nextSibling };
      content = node;
      body.appendChild(node);
    }
    document.body.classList.add("ibpl-m-sheet-open");
  }

  function close() {
    if (content && origin && origin.parent) {
      origin.parent.insertBefore(content, origin.next);
    }
    content = null;
    origin = null;
    document.body.classList.remove("ibpl-m-sheet-open");
  }

  function isOpen() {
    return document.body.classList.contains("ibpl-m-sheet-open");
  }

  document.addEventListener("keydown", function (e) {
    if (e.key === "Escape" && isOpen()) close();
  });

  return { open: open, close: close, isOpen: isOpen };
})();

/* ---- Filter panel into the sheet --------------------------------------
   All 11 tabs use the same toggle shape, so this is one selector rather than
   11 R edits. Bootstrap's collapse still owns show/hide, so the button, its
   aria-expanded state and the chips-bar wiring are untouched.
   --------------------------------------------------------------------- */
(function () {
  var SEL = '[data-bs-toggle="collapse"][data-bs-target$="-filters"]';

  function bind() {
    if (!window.jQuery) return;
    window.jQuery(document).on("click", SEL, function (e) {
      if (!document.body.classList.contains("ibpl-mobile")) return;
      e.preventDefault();
      e.stopPropagation();   // Do not let Bootstrap also toggle the collapse.
      var target = document.querySelector(this.getAttribute("data-bs-target"));
      if (!target) return;
      if (window.IBPL_MOBILE_SHEET.isOpen()) {
        window.IBPL_MOBILE_SHEET.close();
        return;
      }
      window.IBPL_MOBILE_SHEET.open("Filters", target);
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bind);
  } else {
    bind();
  }
})();
```

- [ ] **Step 4: Add the sheet CSS**

```css
/* ---- Bottom sheet ---- */
.ibpl-m-sheet, .ibpl-m-sheet-backdrop { display: none; }

body.ibpl-mobile.ibpl-m-sheet-open .ibpl-m-sheet-backdrop {
  display: block;
  position: fixed;
  inset: 0;
  background: rgba(0, 0, 0, 0.55);
  z-index: 2147482000;
}
body.ibpl-mobile.ibpl-m-sheet-open .ibpl-m-sheet {
  display: flex;
  flex-direction: column;
  position: fixed;
  left: 0;
  right: 0;
  bottom: 0;
  /* vh first as the fallback; dvh accounts for mobile browser chrome so the
     sticky Apply footer is never cropped. */
  max-height: 85vh;
  max-height: 85dvh;
  z-index: 2147482001;
  background: var(--ibpl-surface);
  border-top: 1px solid var(--ibpl-border);
  border-radius: 12px 12px 0 0;
}
body.ibpl-mobile .ibpl-m-sheet-head {
  flex: 0 0 auto;
  padding: 12px 16px;
  font-weight: 600;
  border-bottom: 1px solid var(--ibpl-border);
}
body.ibpl-mobile .ibpl-m-sheet-body {
  flex: 1 1 auto;
  overflow-y: auto;
  padding: 12px 16px;
  -webkit-overflow-scrolling: touch;
}
body.ibpl-mobile .ibpl-m-sheet-foot {
  flex: 0 0 auto;
  padding: 12px 16px;
  border-top: 1px solid var(--ibpl-border);
}
/* The collapse class would keep the moved panel hidden inside the sheet. */
body.ibpl-mobile .ibpl-m-sheet-body .collapse { display: block !important; height: auto !important; }
body.ibpl-mobile .ibpl-m-sheet-body .form-control,
body.ibpl-mobile .ibpl-m-sheet-body .form-select,
body.ibpl-mobile .ibpl-m-sheet-body .selectize-input,
body.ibpl-mobile .ibpl-m-sheet-body .btn {
  min-height: var(--ibpl-m-tap);
  width: 100%;
}
```

The `dvh` assertion is satisfied by this task's own `max-height: 85dvh`. Task 6
adds `100dvh` for modals and carries the stricter ordering assertion.

- [ ] **Step 5: Run the tests**

Expected: PASS, 13 tests.

- [ ] **Step 6: Verify in the browser**

At 390x844 on the On/Off tab, `browser_click` the "Show Filters" button, then evaluate:

```js
() => ({
  open: document.body.classList.contains('ibpl-m-sheet-open'),
  hasDateInput: !!document.querySelector('.ibpl-m-sheet-body #date_range'),
  teamsBound: !!(window.Shiny && Shiny.shinyapp.$inputValues.hasOwnProperty('teams'))
})
```

Expected all `true` — the real filter inputs moved in and are still bound. Set a team in the sheet, Apply, and confirm the chips bar shows it and the table re-queries. Then re-open and confirm the panel returned to its original parent (`browser_evaluate` on `document.querySelector('#onoff-filters').parentNode.className`).

- [ ] **Step 7: Commit**

```bash
git add app/www/mobile.js app/www/mobile.css app/tests/testthat/test-mobile-layer.R
git commit -m "feat(mobile): bottom sheet component and filter panel transform"
```

---

### Task 6: Popups — full-screen modals, tap tooltips, stat-filter popover

**Files:**
- Modify: `app/www/mobile.js`, `app/www/mobile.css`
- Test: `app/tests/testthat/test-mobile-layer.R`

**Interfaces:**
- Consumes: `window.IBPL_MOBILE_SHEET.open/close` from Task 5.
- Produces: nothing later tasks depend on.

Three distinct mechanisms, each needing its own selector:
1. `size = "xl"` modals — `server_tab2.R:866`, `server_tab10_euro_lineups.R:714`, `mod_ribbon_modal.R:49`. CSS only.
2. DT header tooltips — `HEADER_TOOLTIP_JS` (`global.R:212`) sets the **native `title`** on `th`. `HEADER_TOOLTIP_JS` is not modified; the mobile layer reads what it writes.
3. `tt()` sidebar labels — `data-tooltip` + CSS `::after` on `:hover` (`global.R:209`, `app.css:955`). A **different** mechanism.
4. Stat-filter popover — `helpers.R:546`, a `bslib::popover` with a `selectInput`, `radioButtons` and `numericInput`.

- [ ] **Step 1: Write the failing test**

```r
test_that("xl modals go full screen on mobile", {
  css <- read_repo_txt("www", "mobile.css")

  expect_true(grepl(".modal-dialog", css, fixed = TRUE))
  # dvh, not vh -- mobile browser chrome would crop the footer.
  expect_true(grepl("100dvh", css, fixed = TRUE))
  # A vh fallback must be declared FIRST for browsers without dvh.
  expect_lt(
    regexpr("height: 100vh", css, fixed = TRUE),
    regexpr("height: 100dvh", css, fixed = TRUE)
  )
})

test_that("both tooltip mechanisms get a tap path", {
  js <- read_repo_txt("www", "mobile.js")

  # Native title on th, written by HEADER_TOOLTIP_JS.
  expect_true(grepl("th[title]", js, fixed = TRUE))
  # data-tooltip on tt() labels -- a different mechanism, own selector.
  expect_true(grepl("[data-tooltip]", js, fixed = TRUE))
  expect_true(grepl("IBPL_MOBILE_SHEET.open", js, fixed = TRUE))
})

test_that("HEADER_TOOLTIP_JS is unchanged", {
  global_r <- read_repo_txt("R", "global.R")

  # The mobile layer reads the title attribute; it does not change how it is
  # written. Touching this would affect desktop too.
  expect_true(grepl("cell.attr('title', tips[txt])", global_r, fixed = TRUE))
})

test_that("the stat-filter popover keeps its input ids", {
  helpers <- read_repo_txt("R", "helpers.R")

  # Relocating the popover BODY must not rename inputs, or
  # apply_stat_filters() and every observer break.
  expect_true(grepl('paste0(prefix, "_stat_filter_col")', helpers, fixed = TRUE))
  expect_true(grepl('paste0(prefix, "_stat_filter_value")', helpers, fixed = TRUE))
})
```

- [ ] **Step 2: Run it to verify it fails**

Expected: FAIL — `.modal-dialog` absent from `mobile.css`.

- [ ] **Step 3: Add the popup CSS**

```css
/* ---- Full-screen modals ----
   An xl dialog on a 390px screen is a wide table squeezed into a small box.
   100dvh, with 100vh declared first as the fallback, so mobile browser chrome
   does not crop the sticky footer. */
body.ibpl-mobile .modal-dialog {
  margin: 0 !important;
  max-width: 100% !important;
  width: 100% !important;
  height: 100vh;
  height: 100dvh;
}
body.ibpl-mobile .modal-content {
  height: 100%;
  border-radius: 0;
  display: flex;
  flex-direction: column;
}
body.ibpl-mobile .modal-header {
  position: sticky;
  top: 0;
  z-index: 2;
  background: var(--ibpl-surface);
  flex: 0 0 auto;
}
body.ibpl-mobile .modal-body { flex: 1 1 auto; overflow-y: auto; }
body.ibpl-mobile .modal-header .btn-close {
  min-width: var(--ibpl-m-tap);
  min-height: var(--ibpl-m-tap);
}

/* ---- Tooltips ----
   Both mechanisms are hover-only and so unreachable on touch. Suppress the CSS
   one and route both through the sheet. */
body.ibpl-mobile [data-tooltip]:hover::after,
body.ibpl-mobile [data-tooltip]:hover::before { display: none !important; }
body.ibpl-mobile table.dataTable thead th[title],
body.ibpl-mobile [data-tooltip] {
  text-decoration: underline dotted var(--ibpl-text-muted);
  text-underline-offset: 3px;
}

/* The popover body is moved into the sheet; the floating box must not also
   render. */
body.ibpl-mobile.ibpl-m-sheet-open .popover { display: none !important; }
```

- [ ] **Step 4: Add the tooltip and popover routing to `mobile.js`**

```js
/* ---- Tooltips and the stat-filter popover into the sheet ---------------
   Three mechanisms, three selectors:
   - th[title]       native title, written by HEADER_TOOLTIP_JS (global.R:212)
   - [data-tooltip]  CSS ::after on :hover, written by tt() (global.R:209)
   - the bslib popover at helpers.R:546, whose Shiny input ids must survive
   --------------------------------------------------------------------- */
(function () {
  function textSheet(title, text) {
    var p = document.createElement("div");
    p.className = "ibpl-m-tip-text";
    p.textContent = text;
    window.IBPL_MOBILE_SHEET.open(title, null);
    document.querySelector(".ibpl-m-sheet-body").appendChild(p);
  }

  function bind() {
    if (!window.jQuery) return;
    var $ = window.jQuery;

    $(document).on("click", "table.dataTable thead th[title]", function (e) {
      if (!document.body.classList.contains("ibpl-mobile")) return;
      var tip = this.getAttribute("title");
      if (!tip) return;
      // Do not swallow the sort click: only the header TEXT opens the tip.
      e.stopPropagation();
      e.preventDefault();
      textSheet((this.textContent || "").trim(), tip);
    });

    $(document).on("click", "[data-tooltip]", function (e) {
      if (!document.body.classList.contains("ibpl-mobile")) return;
      var tip = this.getAttribute("data-tooltip");
      if (!tip) return;
      e.preventDefault();
      textSheet((this.textContent || "").trim(), tip);
    });

    // The popover BODY is moved, not cloned, so the selectInput /
    // radioButtons / numericInput keep their ids and every server observer and
    // apply_stat_filters() call keeps working.
    $(document).on("click", ".filter-chip-add", function (e) {
      if (!document.body.classList.contains("ibpl-mobile")) return;
      e.preventDefault();
      e.stopPropagation();
      var id = this.getAttribute("id") || "";
      var prefix = id.replace(/_stat_filter_add_btn$/, "");
      var panel = document.querySelector("." + prefix + "-stat-popover");
      if (!panel) return;
      window.IBPL_MOBILE_SHEET.open("Add stat filter", panel);
    });
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", bind);
  } else {
    bind();
  }
})();
```

- [ ] **Step 5: Run the tests**

Expected: PASS, 17 tests.

- [ ] **Step 6: Verify in the browser**

At 390x844 on Lineup Data, click a lineup row to open the modal, then evaluate:

```js
() => {
  const d = document.querySelector('.modal-dialog');
  const cs = getComputedStyle(d);
  return { w: d.clientWidth, marginLeft: cs.marginLeft, bodyScrolls: getComputedStyle(document.querySelector('.modal-body')).overflowY };
}
```

Expected: `w` about 390, `marginLeft: "0px"`, `bodyScrolls: "auto"`. Confirm the table inside shows 3 columns with carets (Task 3 applying inside the modal).

Then on On/Off, tap a column header and confirm the sheet opens with that column's tooltip text. Tap the `+ Filter` chip and confirm the sheet holds `#onoff_stat_filter_col`; set a column, a value, Apply, and confirm the table filters — that proves the input ids survived the move.

- [ ] **Step 7: Commit**

```bash
git add app/www/mobile.js app/www/mobile.css app/tests/testthat/test-mobile-layer.R
git commit -m "feat(mobile): full-screen modals, tap tooltips, popover into sheet"
```

---

### Task 7: Compare — two-up summary and the column override

**Files:**
- Modify: `app/www/mobile.css`, `app/www/mobile.js`
- Test: `app/tests/testthat/test-mobile-layer.R`

**Interfaces:**
- Consumes: `window.IBPL_MOBILE_TABLE.priority` from Task 3.
- Produces: nothing.

Compare's summary is three `column(4)` cards — A, B, Avg Gap (`ui_tab7_compare.R:351-370`). At 390px each would get about 120px, too little for an `fs-4` value plus four `small` lines. A and B go two-up; Gap goes full-width beneath.

The Compare table's real column names are `#, Team`/`Player, GP A, A, Total Poss A, GP B, B, Total Poss B, Gap` (`server_tab7_compare.R:3300-3315`) — **`A`/`B`, not "Side A"/"Side B"**. The default of "first 3 visible" would yield `#, Player, GP A`, deleting both compared values and the gap.

- [ ] **Step 1: Write the failing test**

```r
test_that("Compare keeps A and B adjacent on mobile", {
  css <- read_repo_txt("www", "mobile.css")

  # Three col-4 cards would give each ~120px. A and B two-up, Gap full width.
  expect_true(grepl("cmp-summary", css, fixed = TRUE))
})

test_that("the Compare table override names the real columns", {
  js <- read_repo_txt("www", "mobile.js")
  server <- read_repo_txt("R", "server_tab7_compare.R")

  expect_true(grepl("cmp_table", js, fixed = TRUE))
  # The columns are A and B. "Side A"/"Side B" appears only in explainer prose.
  expect_true(grepl('"A", "B", "Gap"', js, fixed = TRUE))
  expect_true(grepl('"Gap" = "gap"', server, fixed = TRUE))
})
```

- [ ] **Step 2: Run it to verify it fails**

Expected: FAIL — `cmp_table` not in `mobile.js`.

- [ ] **Step 3: Register the override**

Replace the empty `priority: {}` written in Task 3 with:

```js
  priority: {
    // Compare's columns are #, Team|Player, GP A, A, Total Poss A, GP B, B,
    // Total Poss B, Gap. The default "first 3 visible" would yield
    // #, Player, GP A -- deleting both compared values and the gap, which is
    // the entire point of the tab. Team and Player are alternatives: Teams
    // mode has one, Players mode the other, and only the present one matches.
    cmp_table: ["Team", "Player", "A", "B", "Gap"]
  }
```

- [ ] **Step 4: Add the Compare summary CSS**

```css
/* ---- Compare ----
   Three col-4 cards at 390px give each ~120px, too little for an fs-4 value
   plus four small lines. A and B stay adjacent because comparing them is the
   tab's only job; Gap reads fine as a full-width strip. */
body.ibpl-mobile .cmp-summary-row > [class*="col-"] { padding: 0 4px; }
body.ibpl-mobile .cmp-summary-row > [class*="col-"]:nth-child(1),
body.ibpl-mobile .cmp-summary-row > [class*="col-"]:nth-child(2) {
  flex: 0 0 50%;
  max-width: 50%;
}
body.ibpl-mobile .cmp-summary-row > [class*="col-"]:nth-child(3) {
  flex: 0 0 100%;
  max-width: 100%;
}
body.ibpl-mobile .cmp-summary-row .fs-4 { font-size: 1.1rem !important; }
body.ibpl-mobile .cmp-summary-row .card { padding: 10px !important; }
```

The summary `fluidRow` at `ui_tab7_compare.R:351` has no class, so add `class = "cmp-summary-row"` to that one `fluidRow` call. This is a **class-only** UI edit — no input, no logic. It is the one exception to "no per-tab R edits" and is limited to adding a hook for the CSS.

- [ ] **Step 5: Run the tests**

Expected: PASS, 19 tests.

- [ ] **Step 6: Verify in the browser**

At 390x844 on Compare, evaluate:

```js
() => {
  const t = window.jQuery('#cmp_table table.dataTable').DataTable();
  const heads = t.columns(':visible').header().toArray().map(h => h.textContent.trim());
  const cards = [...document.querySelectorAll('.cmp-summary-row > [class*="col-"]')].map(c => c.getBoundingClientRect().width);
  return { heads, cards };
}
```

Expected: `heads` contains `A`, `B` and `Gap` plus the identity column — never `#, Player, GP A`. `cards` shows two roughly equal widths then one about double.

Also confirm a row click still opens the Compare detail view — the caret must not have stolen it.

- [ ] **Step 7: Commit**

```bash
git add app/www/mobile.css app/www/mobile.js app/R/ui_tab7_compare.R app/tests/testthat/test-mobile-layer.R
git commit -m "feat(mobile): two-up Compare summary and column priority override"
```

---

### Task 8: Gameflow — keep the View link visible and the stint ribbon legible

**Files:**
- Modify: `app/www/mobile.css`, `app/www/mobile.js`
- Test: `app/tests/testthat/test-mobile-layer.R`

**Interfaces:**
- Consumes: `window.IBPL_MOBILE_TABLE.priority` (Task 3), the full-screen modal rules (Task 6).
- Produces: nothing.

The `gameflow` column is the "View" link that opens the stint ribbon
(`add_ribbon_link_column()`, `helpers.R:3665` → `ribbon_link_cell()`, `:3644` →
`mod_ribbon_modal.R:49`). Two problems, both measured:

**Problem 1 — the link is hidden by the default rule.** Column order in both
game-log tabs is `GN|Rd, Game Type|Phase, Date, Gameflow, Team, Opponent, W/L,
Score, Min, …` (`helpers.R:146-166`, `server_tab4.R:577`,
`server_tab11_euro_gamelogs.R:196`). "First 3 visible" yields `GN, Game Type,
Date`, dropping the link into the caret detail row.

**Problem 2 — the ribbon shrinks to illegibility.** The SVG is
`viewBox="0 0 1070 H"` (`helpers.R:3557`, `RIBBON_WIDTH <- 1070`) under
`.ibpl-ribbon { width: 100% }` (`app.css:1458`). At ~370px usable width the
scale is **0.346**, so the 14px lanes render at **4.8px** and the 11px labels
(`app.css:1494`) at **3.8px**.

The fix keeps the designed geometry exactly and scrolls instead of scaling.
`ribbon_geometry()` is server-side R, verified over 11 tasks and migration 054;
this task must not touch it.

**Known limitation, accept and document:** panning right scrolls the ~220-unit
name gutter off screen. Mitigation is already in place — lanes carry
`tabindex="0"` (`helpers.R:3442`) and an `aria-label`, and tap-to-select works
because the ribbon binds `click`/`focusin`/`keydown`, not only `mouseover`
(`app.js:871-897`). Only the hover *preview* is lost on touch.

- [ ] **Step 1: Write the failing test**

```r
test_that("the gameflow link survives the column priority rule", {
  js <- read_repo_txt("www", "mobile.js")

  # Gameflow is column 4 in both game-log tabs, so the "first 3 visible"
  # default would bury the only entry point to the stint ribbon.
  expect_true(grepl("gl_table", js, fixed = TRUE))
  expect_true(grepl("eurogl_table", js, fixed = TRUE))
  expect_true(grepl('"Gameflow"', js, fixed = TRUE))
})

test_that("the ribbon keeps its designed width on mobile", {
  helpers <- read_repo_txt("R", "helpers.R")
  css <- read_repo_txt("www", "mobile.css")

  # Hardcoding the width in CSS would silently drift from the R geometry.
  # Pin them together: this test fails if RIBBON_WIDTH ever changes.
  m <- regmatches(helpers, regexpr("RIBBON_WIDTH <- [0-9]+", helpers))
  expect_length(m, 1L)
  w <- sub("RIBBON_WIDTH <- ", "", m)

  expect_true(grepl(paste0("min-width: ", w, "px"), css, fixed = TRUE))
  # Scrolling, not scaling: app.css sets width:100%, which is what shrinks the
  # 11px labels to 3.8px at 390px.
  expect_true(grepl("overflow-x: auto", css, fixed = TRUE))
})
```

- [ ] **Step 2: Run it to verify it fails**

```bash
"$RSCRIPT" -e "testthat::test_file('app/tests/testthat/test-mobile-layer.R')"
```

Expected: FAIL — `gl_table` not in `mobile.js`.

- [ ] **Step 3: Add the two game-log overrides**

Extend the `priority` map from Task 3 (which already holds `cmp_table`):

```js
    // Gameflow is column 4 in both game-log tabs, so the default would bury
    // the only entry point to the stint ribbon behind a caret tap. On a phone
    // a game is identified by date and opponent, not by GN or round.
    // One list serves both leagues: Tab 4 leads GN|Game Type and Tab 11 leads
    // Rd|Phase, but Date, Opponent, Score and Gameflow are named identically.
    gl_table: ["Date", "Opponent", "Score", "Gameflow"],
    eurogl_table: ["Date", "Opponent", "Score", "Gameflow"]
```

**Caution for any future override:** the Four Factors header
(`gamelog_ff_header()`, `helpers.R:170`) repeats `PPP`, `eFG%`, `OREB%`, `TOV%`
and `FTR` across its Offense and Defense groups. `indexOf` takes the first
match, so an override must never name a duplicated header.

- [ ] **Step 4: Add the ribbon scroll rules to `mobile.css`**

```css
/* ---- Stint ribbon (gameflow) ----
   The SVG is viewBox 0 0 1070 H under .ibpl-ribbon { width: 100% }, so at
   ~370px of usable modal width the scale is 0.346: 14px lanes render at 4.8px
   and 11px labels at 3.8px. Pin the SVG to its viewBox width instead and let
   the container pan. Scale 1.0, designed size exactly.

   min-width MUST equal RIBBON_WIDTH in helpers.R -- pinned by a test. */
body.ibpl-mobile .modal-body { overflow-x: auto; -webkit-overflow-scrolling: touch; }
body.ibpl-mobile .ibpl-ribbon {
  width: 1070px;
  min-width: 1070px;
  max-width: none;
  height: auto;
}
```

`app.css:1458` sets `.ibpl-ribbon { width: 100% }` with no `!important`, so
`body.ibpl-mobile .ibpl-ribbon` wins on specificity without editing `app.css`.

- [ ] **Step 5: Run the tests**

Expected: PASS, 21 tests.

- [ ] **Step 6: Verify in the browser**

At 390x844, open Game Logs and evaluate:

```js
() => {
  const t = window.jQuery('#gl_table table.dataTable').DataTable();
  return {
    heads: t.columns(':visible').header().toArray().map(h => h.textContent.trim()),
    links: document.querySelectorAll('#gl_table a.ribbon-link').length
  };
}
```

Expected: `heads` contains `Gameflow`, `links` > 0 — the View link is on the
face of the table, not buried.

`browser_click` a View link, wait for the modal, then evaluate:

```js
() => {
  const svg = document.querySelector('.ibpl-ribbon');
  const scale = svg.getBoundingClientRect().width / svg.viewBox.baseVal.width;
  const body = document.querySelector('.modal-body');
  return {
    scale: scale,
    renderedFontPx: 11 * scale,
    renderedLanePx: 14 * scale,
    canPan: body.scrollWidth > body.clientWidth
  };
}
```

Expected: `scale` about 1.0, `renderedFontPx` about 11, `renderedLanePx` about
14, `canPan: true`. A `scale` near 0.35 means `app.css`'s `width: 100%` is still
winning and the task is not done.

Then tap a lane and confirm it selects (the `click`/`focusin` path), and confirm
the modal is full-screen from Task 6.

- [ ] **Step 7: Commit**

```bash
git add app/www/mobile.js app/www/mobile.css app/tests/testthat/test-mobile-layer.R
git commit -m "feat(mobile): keep gameflow link visible and ribbon at designed scale"
```

---

### Task 9: Verification sweep across all tabs and widths

**Files:**
- Modify: `app/www/mobile.js` (CFG overrides found by the sweep)
- Modify: `CLAUDE.md` (document `IBPL_MOBILE`)
- Create: `docs/mobile_verification_2026-09-13.md`

**Interfaces:**
- Consumes: everything.
- Produces: the record of what was checked.

This is the task that converts the plan's central assumption — "first 3 visible columns reads sensibly" — from assumption to fact. Expect a handful of new CFG entries.

- [ ] **Step 1: Run the full test suite as the regression baseline**

```bash
"$RSCRIPT" -e "testthat::test_dir('app/tests/testthat')"
```

Expected: no NEW failures versus `main`. Tasks 1-8 touch no server code, so a data or contract failure means something was broken that this work claims not to touch. `test-deployed-app-smoke.R` is known-stale (fails on the unwired `team_stats` tab) — not caused here.

- [ ] **Step 2: Launch the app once for the whole sweep**

```bash
IBPL_CACHE_UI=false "$RSCRIPT" -e "shiny::runApp('app', port = 7788, launch.browser = FALSE)"
```

**Select season 25-26 (`game_year` 2026) before sweeping — do not use the
default.** `DEFAULT_GAME_YEAR` is `"2027"` (26-27), which has only **10 games**
in `final_schedule_mv` against 442 for 2026 and 450 for 2025 (measured
2026-09-13). With ten games the min-possessions filters leave the On/Off table
empty, and a sweep that inspects an empty table verifies nothing: there are no
rows to carry carets, and `visibleCols` on a zero-row table cannot show whether
the priority set is usable. This is the same failure shape as checking overflow
instead of legibility — a green result observing nothing.

For each tab, record the row count alongside `visibleCols`. **A tab that
reports zero rows is NOT verified**; switch season (or filters) until it has
data, and say so in the verification record. The EuroLeague tabs need
`league_select` switched as well.

- [ ] **Step 3: Sweep all 11 tabs at 390x844**

For each of Home, On/Off, Lineup Data, Team Ratings, Game Logs, Player Stats, Compare, and the four EuroLeague tabs (switch `league_select` to reach them): `browser_resize` 390x844, navigate to the tab, `browser_take_screenshot`, then `browser_evaluate`:

```js
() => {
  const t = window.jQuery('table.dataTable:visible').first();
  const api = t.length ? t.DataTable() : null;
  return {
    overflow: document.body.scrollWidth - document.body.clientWidth,
    navLinks: document.querySelectorAll('.nav-link').length,
    visibleCols: api ? api.columns(':visible').header().toArray().map(h => h.textContent.trim()) : null
  };
}
```

Record `visibleCols` per tab. Acceptance: `overflow <= 0`, `navLinks === 11`, and `visibleCols` names a **usable** headline set — an identity column plus a metric that carries the tab's point. A set like `#, Player, GP A` fails and earns a CFG override.

**Overflow is not legibility.** An SVG with a `viewBox` and `width: 100%` never
overflows — it shrinks, so `overflow <= 0` passes on an unreadable chart. The
stint ribbon is exactly this case and it would have passed this step as
originally written. So also assert, wherever an SVG is on screen:

```js
() => {
  const t = document.querySelector('.ibpl-ribbon-name, svg text');
  if (!t) return { skipped: true };
  const svg = t.ownerSVGElement;
  const scale = svg.getBoundingClientRect().width / svg.viewBox.baseVal.width;
  return { scale: scale, renderedFontPx: 11 * scale };
}
```

Acceptance: `renderedFontPx >= 9`. Below that the label is not text.

- [ ] **Step 4: Add CFG overrides for every failing table**

Add name-based entries to `window.IBPL_MOBILE_TABLE.priority` for each, with a one-line comment naming the default it replaces and why. Re-run Step 3 for those tabs until every `visibleCols` passes.

- [ ] **Step 5: Repeat the sweep at 360x800 and 768x1024**

360x800 is the narrowest realistic Android. 768x1024 is **above** the 767.98px breakpoint, so it must show the **desktop** layout — confirm `document.body.classList.contains('ibpl-mobile')` is `false` there. That is the boundary test.

**Test the boundary itself, not just the two sides.** Task 2 unified three
blocks that previously used *two* breakpoints (768px and 767px) onto the
767.98px carrier, which deliberately changed behaviour at exactly 768px — iPad
portrait width — and in the `(767, 767.98]` band. Static analysis cannot
confirm that flip; only a browser can. At widths 767 and 768 evaluate:

```js
() => {
  const td = document.querySelector('table.dataTable tbody td');
  return {
    w: document.documentElement.clientWidth,
    mobile: document.body.classList.contains('ibpl-mobile'),
    tdFontPx: td ? getComputedStyle(td).fontSize : null,
    toggleHidden: getComputedStyle(document.querySelector('.chips-filters-toggle') || document.body).display
  };
}
```

Acceptance: at **767** → `mobile: true` and the compact DT font is in force; at
**768** → `mobile: false` and it is not. If both widths report the same
`tdFontPx`, the migrated rules are not actually switching and Task 2's move
only moved text.

- [ ] **Step 6: Regression control at 1440x900**

Confirm at 1440x900: no `ibpl-mobile` class, every table back to its full visible column count, no caret in the DOM, `#navbar_right_cluster` computed `position: fixed`, the view-mode group back in the sidebar, and the hover menus working.

```js
() => ({
  mobile: document.body.classList.contains('ibpl-mobile'),
  carets: document.querySelectorAll('.ibpl-m-caret').length,
  clusterPos: getComputedStyle(document.querySelector('#navbar_right_cluster')).position,
  hoverMenus: document.querySelectorAll('.tab-hover-menu').length
})
```

Expected: `mobile: false`, `carets: 0`, `clusterPos: "fixed"`, `hoverMenus: 10`.

- [ ] **Step 7: Check the console at every width**

`browser_console_messages` after each width. No new errors. The `hub_storylines` output-state errors and the favicon 404 on Home are known pre-existing (`project-home-load-console-defects`), not caused here.

- [ ] **Step 8: Document `IBPL_MOBILE` in CLAUDE.md**

Add `IBPL_MOBILE` to the Posit Connect Cloud optional-env list, which currently ends `REF_CACHE_TTL_SEC`, `APP_LOG_LEVEL`, `APP_LOG_FILE`. On Connect an unset var silently takes the committed default, so an undocumented flag is an invisible one. Add one line to the Shiny App file map for `www/mobile.css` and `www/mobile.js`, and a line to the UI section noting `IBPL_CACHE_UI=false` applies to them too.

- [ ] **Step 9: Write the verification record**

Create `docs/mobile_verification_2026-09-13.md` with: the four widths, the 11 tabs, the `visibleCols` actually observed per tab per width, every CFG override added and why, screenshot paths, and the console state. Record measured facts only — do not quote local timings as production numbers.

- [ ] **Step 10: Commit**

```bash
git add app/www/mobile.js CLAUDE.md docs/mobile_verification_2026-09-13.md
git commit -m "test(mobile): verify all tabs at four widths; document IBPL_MOBILE"
```

- [ ] **Step 11: Report, do not merge**

Summarise: tabs verified, overrides added, anything still wrong. Merging is a separate decision — use `superpowers:finishing-a-development-branch`. Deploying means pushing to `main`, which Connect Cloud builds from GitHub, so a merge is a deploy.

---

## Self-Review

**Spec coverage.** Every spec section maps to a task: Delivery mechanism, Breakpoint, Mode signal, Kill switch, Viewport → Task 1. The `@media` migration → Task 2. Component 1 Tables → Task 3. Component 2 Navigation → Task 4. Component 3 Filter sheet → Task 5. Component 4 Popups → Task 6. Component 5 Compare → Task 7. Testing and Risks → Task 9. Gameflow, raised by the user after the spec was written → Task 8. Out-of-scope items (glossary, React, tab 6, PWA) appear in no task, correctly.

**Deviations from the spec, all recorded above under "Findings that changed the spec":** caret instead of row tap (row clicks already taken); name-keyed instead of index-keyed overrides (11 output ids serve 41 tables); `A`/`B` instead of `Side A`/`Side B`; Playwright MCP instead of PowerShell `npx` (shinytest2 not installed, `npx` broken under Bash). One scope addition: Task 7 Step 4 adds a `class` to one `fluidRow` in `ui_tab7_compare.R` — a CSS hook only, no input or logic, and called out as the single exception to "no per-tab R edits".

**Type and name consistency.** `window.IBPL_MOBILE_MQ` (Task 1) is read in Task 1 only. `body.ibpl-mobile` and `ibpl:mobilechange` are produced in Task 1 and consumed in 3, 4, 5, 6. `window.IBPL_MOBILE_TABLE.priority` is created empty in Task 3 and populated in Task 7 and Task 8. `window.IBPL_MOBILE_SHEET.open(title, node)` / `.close()` / `.isOpen()` are defined in Task 5 and called in Task 6 with the same signature. `--ibpl-m-tap` is defined in Task 1 and used in 3, 4, 5, 6. `.ibpl-m-caret`, `.ibpl-m-detail*` are defined and used in Task 3 and asserted in Task 9.
