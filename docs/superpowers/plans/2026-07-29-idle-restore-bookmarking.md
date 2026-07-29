# Idle-Restore Bookmarking Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Replace the replay-based idle-session restore with native Shiny bookmarking, so a restored session starts with the saved tab and filters already applied instead of replaying them after connect.

**Architecture:** While a session is alive, a debounced observer calls `session$doBookmark()`; `onBookmarked()` pushes the resulting URL to the browser, which stores it (never shown in the address bar). After the idle timeout closes the session, the first user return navigates to that URL. Shiny's `restoreInput()` seeds every input — including the navbar tab — at UI build time, so there is no replay phase, no readiness polling, and no double query. Inputs whose choices are populated server-side read their saved value from `session$restoreContext` instead.

**Tech Stack:** R 4.4.2, Shiny 1.9.1, bslib 0.9.0, testthat, vanilla JS (`app/www/app.js`).

**Spec:** `docs/superpowers/specs/2026-07-29-idle-restore-bookmarking-design.md`

**One deviation from the spec, discovered while verifying the Shiny APIs:** the
spec proposed an `onRestore()` callback to feed the dependent Lineup/Compare
player filters. That is unnecessary. `app/R/mod_lineup_player_filter.R:87-119`
already intersects the current player selection with the freshly built choice
list, and `app/R/server_tab2.R:91-93` already carries the current team forward —
so those paths need only a value **source** that falls back to the restore
context (`restore_aware_selection()`, Task 2). Less new code than the spec
assumed, and it reuses logic that is already exercised by the season-change
path. Everything else follows the spec as written.

## Global Constraints

- Shiny 1.9.1 / bslib 0.9.0 behavior verified for this plan — do not assume newer APIs.
- Bookmark store is `"url"`. The bookmark URL must **never** be written to the address bar (no `updateQueryString()`); it lives in browser storage and in the one-shot restore navigation only.
- Bookmark URL format is `?_inputs_&<id>=<json>`; module inputs appear namespaced (`ld_lineup_filter-team`). Never hand-construct this URL — always generate it via `session$doBookmark()`.
- `restoreContext$input$get(id)` marks the value **used**; any server-side read after UI construction MUST pass `force = TRUE` or it returns `NULL`.
- R code style: 2-space indent, snake_case, base `lapply`/`vapply`/`Filter` (no purrr). Drive repetitive work from vectors/maps in one pass — no long if-chains.
- JS is ES5-style (`var`, no arrow functions) to match the existing file.
- `app/www/app.js` is stored LF; on Windows verify with `git diff --stat` that edits do not rewrite the whole file. If a whole-file diff appears, re-apply as byte-preserving surgery and stage with `git -c core.autocrlf=false add`.
- Existing env vars (`APP_IDLE_TIMEOUT_SEC`, `APP_IDLE_WARNING_SEC`, `APP_IDLE_CHECK_SEC`, `APP_IDLE_STATE_TTL_HOURS`, `APP_IDLE_CLOSE_SESSION`) keep their names and meanings.
- Branch: `shiny/idle-restore-bookmarking`. Commit after every task.
- Test command (run from repo root):
  `RSCRIPT="/c/Program Files/R/R-4.4.2/bin/Rscript.exe"; cd app && "$RSCRIPT" -e "testthat::test_file('tests/testthat/<file>.R')"`

---

### Task 0: Create the branch

- [ ] **Step 1: Branch from main**

```bash
git checkout main
git checkout -b shiny/idle-restore-bookmarking
```

- [ ] **Step 2: Confirm the working tree is on the new branch**

Run: `git status -sb | head -1`
Expected: `## shiny/idle-restore-bookmarking`

---

### Task 1: Bookmark exclusion list (pure helper)

Bookmarking captures *every* input unless excluded. This task builds the pure
function that decides what is excluded, so later tasks can wire it in.

**Files:**
- Modify: `app/R/helpers.R` (append near the other sanitizers, after `sanitize_single_choice`)
- Test: `app/tests/testthat/test-idle-restore-bookmarking.R` (create)

**Interfaces:**
- Consumes: nothing.
- Produces: `bookmark_excluded_ids(input_ids)` → `character` vector of ids to exclude, given the character vector of currently-known input ids.

- [ ] **Step 1: Write the failing test**

Create `app/tests/testthat/test-idle-restore-bookmarking.R`:

```r
test_that("bookmark exclusion drops actions, heartbeats and DT internals", {
  ids <- c(
    "game_year", "main_tabs", "teams", "ld_minposs", "ld_lineup_filter-team",
    "go_onoff", "go_lineups", "go_team", "go_gamelogs", "go_playerstats", "go_compare",
    "open_glossary", "ld_reset", "cmp_reset",
    "idle_activity_ts", "hub_remembered_team", "ibpl_restore_state",
    "ld_lineup_click", "cmp_table_row_click",
    "ld_table_rows_current", "ld_table_rows_all", "ld_table_rows_selected",
    "ld_table_state", "ld_table_search", "ld_table_cell_clicked",
    "ld_table_row_last_clicked", "ld_table_columns_selected", "ld_table_cells_selected",
    "ld_table_search_columns"
  )

  excluded <- bookmark_excluded_ids(ids)

  # kept: real filter state
  expect_false("game_year" %in% excluded)
  expect_false("main_tabs" %in% excluded)
  expect_false("teams" %in% excluded)
  expect_false("ld_minposs" %in% excluded)
  expect_false("ld_lineup_filter-team" %in% excluded)

  # dropped: everything that is an action, a heartbeat, or DT bookkeeping
  expect_true(all(c(
    "go_onoff", "go_compare", "open_glossary", "ld_reset", "cmp_reset",
    "idle_activity_ts", "hub_remembered_team", "ibpl_restore_state",
    "ld_lineup_click", "cmp_table_row_click",
    "ld_table_rows_current", "ld_table_rows_all", "ld_table_rows_selected",
    "ld_table_state", "ld_table_search", "ld_table_cell_clicked",
    "ld_table_row_last_clicked", "ld_table_columns_selected",
    "ld_table_cells_selected", "ld_table_search_columns"
  ) %in% excluded))
})

test_that("bookmark exclusion handles empty and NULL input safely", {
  expect_identical(bookmark_excluded_ids(character(0)), character(0))
  expect_identical(bookmark_excluded_ids(NULL), character(0))
})
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd app && "$RSCRIPT" -e "testthat::test_file('tests/testthat/test-idle-restore-bookmarking.R')"`
Expected: FAIL — `could not find function "bookmark_excluded_ids"`

- [ ] **Step 3: Implement the helper**

Append to `app/R/helpers.R`:

```r
# Inputs that must never enter a bookmark: one-shot actions, the idle
# heartbeat, and DataTable bookkeeping inputs. Everything else is filter
# state and is bookmarked automatically.
BOOKMARK_EXCLUDE_LITERALS <- c(
  "open_glossary", "idle_activity_ts", "hub_remembered_team",
  "ibpl_restore_state", "ld_lineup_click", "cmp_table_row_click"
)

BOOKMARK_EXCLUDE_PATTERNS <- c(
  "^go_",
  "_reset$",
  "^ibpl_",
  paste0(
    "_(rows_current|rows_all|rows_selected|state|search|search_columns|",
    "cell_clicked|cells_selected|columns_selected|row_last_clicked)$"
  )
)

bookmark_excluded_ids <- function(input_ids) {
  ids <- as.character(input_ids %||% character(0))
  if (!length(ids)) return(character(0))
  hit <- ids %in% BOOKMARK_EXCLUDE_LITERALS
  for (pattern in BOOKMARK_EXCLUDE_PATTERNS) {
    hit <- hit | grepl(pattern, ids)
  }
  ids[hit]
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd app && "$RSCRIPT" -e "testthat::test_file('tests/testthat/test-idle-restore-bookmarking.R')"`
Expected: PASS (2 tests)

- [ ] **Step 5: Commit**

```bash
git add app/R/helpers.R app/tests/testthat/test-idle-restore-bookmarking.R
git commit -m "Add bookmark exclusion helper for idle restore"
```

---

### Task 2: Restore-context read helper

Server-populated selectizes (teams, opponents, players, GN) are built with
`choices = NULL`, so the client reports them empty at startup and
`restoreInput()` alone cannot save them. Their observers must read the saved
value from the restore context instead.

**Files:**
- Modify: `app/R/helpers.R`
- Test: `app/tests/testthat/test-idle-restore-bookmarking.R`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `restored_input_value(session, id, default = character(0))` → saved value for `id`, or `default`.
  - `restore_aware_selection(session, id, current, choices)` → `character` vector: the current selection if non-empty, else the saved one, intersected with `choices`.

- [ ] **Step 1: Write the failing test**

Append to `app/tests/testthat/test-idle-restore-bookmarking.R`:

```r
fake_restore_session <- function(query_string) {
  ctx <- shiny:::RestoreContext$new(query_string)
  list(restoreContext = ctx)
}

test_that("restored_input_value reads saved values even after they were used", {
  s <- fake_restore_session('?_inputs_&teams=%5B%224%22%2C%227%22%5D&ld_minposs=120')

  expect_equal(as.character(restored_input_value(s, "teams")), c("4", "7"))
  # second read must still work: restoreInput() marks values used, so the
  # helper has to force the read
  expect_equal(as.character(restored_input_value(s, "teams")), c("4", "7"))
  expect_equal(as.character(restored_input_value(s, "ld_minposs")), "120")
})

test_that("restored_input_value falls back to the default", {
  s <- fake_restore_session('?_inputs_&teams=%5B%224%22%5D')
  expect_identical(restored_input_value(s, "missing_id"), character(0))
  expect_identical(restored_input_value(s, "missing_id", "fallback"), "fallback")

  no_ctx <- list(restoreContext = NULL)
  expect_identical(restored_input_value(no_ctx, "teams"), character(0))
})

test_that("restore_aware_selection prefers current, falls back to restored, filters to choices", {
  s <- fake_restore_session('?_inputs_&teams=%5B%224%22%2C%229%22%5D')
  choices <- c("Hapoel" = "4", "Maccabi" = "7")

  # no current selection -> use restored, dropping ids absent from choices
  expect_equal(restore_aware_selection(s, "teams", character(0), choices), "4")
  # current selection wins
  expect_equal(restore_aware_selection(s, "teams", "7", choices), "7")
  # current selection not in choices -> empty, not a stale restore
  expect_equal(restore_aware_selection(s, "teams", "99", choices), character(0))
  # no restore context and no current -> empty
  expect_equal(
    restore_aware_selection(list(restoreContext = NULL), "teams", character(0), choices),
    character(0)
  )
})
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd app && "$RSCRIPT" -e "testthat::test_file('tests/testthat/test-idle-restore-bookmarking.R')"`
Expected: FAIL — `could not find function "restored_input_value"`

- [ ] **Step 3: Implement the helpers**

Append to `app/R/helpers.R`:

```r
# restoreContext$input$get() marks a value as used; force = TRUE lets the
# server re-read values that the UI already consumed via restoreInput().
restored_input_value <- function(session, id, default = character(0)) {
  ctx <- tryCatch(session$restoreContext, error = function(e) NULL)
  if (is.null(ctx) || !isTRUE(ctx$active)) return(default)
  val <- tryCatch(ctx$input$get(id, force = TRUE), error = function(e) NULL)
  if (is.null(val) || !length(val)) default else val
}

restore_aware_selection <- function(session, id, current, choices) {
  candidate <- sanitize_persisted_choices(current)
  if (!length(candidate)) {
    candidate <- sanitize_persisted_choices(restored_input_value(session, id))
  }
  if (!length(candidate) || !length(choices)) return(character(0))
  intersect(candidate, as.character(unname(choices)))
}
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd app && "$RSCRIPT" -e "testthat::test_file('tests/testthat/test-idle-restore-bookmarking.R')"`
Expected: PASS (5 tests)

- [ ] **Step 5: Commit**

```bash
git add app/R/helpers.R app/tests/testthat/test-idle-restore-bookmarking.R
git commit -m "Add restore-context read helpers"
```

---

### Task 3: Enable bookmarking and push the URL to the browser

The app starts producing bookmark URLs. The old restore path still works; this
task only adds the new capture side.

**Files:**
- Modify: `app/app.R` (top-level near `shinyApp()`; server body after the idle observers, around lines 93-126)
- Test: `app/tests/testthat/test-idle-restore-bookmarking.R`

**Interfaces:**
- Consumes: `bookmark_excluded_ids()` (Task 1).
- Produces: custom message `ibpl_bookmark_url` with payload `list(url = <string>, v = <integer state version>)`, consumed by JS in Task 6.

- [ ] **Step 1: Write the failing contract test**

Append to `app/tests/testthat/test-idle-restore-bookmarking.R`:

```r
test_that("app enables url bookmarking and pushes urls without touching the address bar", {
  app_r_txt <- read_repo_txt("app.R")

  expect_match(app_r_txt, 'enableBookmarking(store = "url")', fixed = TRUE)
  expect_match(app_r_txt, "session$doBookmark()", fixed = TRUE)
  expect_match(app_r_txt, 'onBookmarked(function(url)', fixed = TRUE)
  expect_match(app_r_txt, '"ibpl_bookmark_url"', fixed = TRUE)
  expect_match(app_r_txt, "setBookmarkExclude(", fixed = TRUE)

  # the bookmark must never be written into the browser address bar
  expect_false(grepl("updateQueryString", app_r_txt, fixed = TRUE))
})
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd app && "$RSCRIPT" -e "testthat::test_file('tests/testthat/test-idle-restore-bookmarking.R')"`
Expected: FAIL — `enableBookmarking(store = "url")` not found in `app.R`

- [ ] **Step 3: Enable bookmarking**

In `app/app.R`, immediately after the `source()` block (after line 23) add:

```r
enableBookmarking(store = "url")
IBPL_RESTORE_STATE_VERSION <- 11L
```

- [ ] **Step 4: Point the injected browser config at the same version constant**

The state version is currently hardcoded twice. In `app/app.R:52-61`, replace the
literal `stateVersion:10` with the constant so R and JS can never disagree:

```r
    tags$script(HTML(sprintf(
      paste0(
        "window.IBPL_IDLE_CONFIG = {",
        "timeoutSec:%d,warningSec:%d,stateTtlHours:%s,stateVersion:%d",
        "};"
      ),
      APP_IDLE_TIMEOUT_SEC,
      APP_IDLE_WARNING_SEC,
      format(APP_IDLE_STATE_TTL_HOURS, scientific = FALSE, trim = TRUE),
      IBPL_RESTORE_STATE_VERSION
    ))),
```

Because `ui` is a function evaluated per request, `IBPL_RESTORE_STATE_VERSION`
must be defined at top level (Step 3), not inside `server`.

- [ ] **Step 5: Add the capture observer**

In `app/app.R`, inside `server`, after the idle-close observer (after line 125) add:

```r
  # ---- Bookmark capture ----
  # Snapshot every non-excluded input; re-bookmark only when that snapshot
  # actually changes, so the idle heartbeat cannot cause bookmark churn.
  bookmark_snapshot <- debounce(reactive({
    vals <- reactiveValuesToList(input)
    ids <- setdiff(names(vals), bookmark_excluded_ids(names(vals)))
    vals[sort(ids)]
  }), 1500)

  last_bookmark_snapshot <- reactiveVal(NULL)

  observe({
    snap <- bookmark_snapshot()
    if (identical(snap, isolate(last_bookmark_snapshot()))) return(invisible(NULL))
    last_bookmark_snapshot(snap)
    setBookmarkExclude(bookmark_excluded_ids(names(reactiveValuesToList(input))), session)
    tryCatch(session$doBookmark(), error = function(e) {
      app_log("bookmark", sprintf("doBookmark failed: %s", conditionMessage(e)),
              level = "WARN", session = session)
    })
  }, priority = -200)

  onBookmarked(function(url) {
    session$sendCustomMessage("ibpl_bookmark_url", list(
      url = url,
      v = IBPL_RESTORE_STATE_VERSION
    ))
  })
```

- [ ] **Step 6: Run the test to verify it passes**

Run: `cd app && "$RSCRIPT" -e "testthat::test_file('tests/testthat/test-idle-restore-bookmarking.R')"`
Expected: PASS (6 tests)

- [ ] **Step 7: Verify the app still starts and produces a URL**

Run: `"$RSCRIPT" -e "shiny::runApp('app', port = 7788, launch.browser = FALSE)"` (Ctrl-C to stop)
In the browser at `http://127.0.0.1:7788`, open the console and run:

```js
window.Shiny.addCustomMessageHandler("ibpl_bookmark_url", function(m) { console.log("BOOKMARK", m.url.length, m.url); });
```

Then change any filter. Expected: a `BOOKMARK` log appears within ~2s, the URL contains `?_inputs_&`, and **the address bar is unchanged**.

- [ ] **Step 8: Commit**

```bash
git add app/app.R app/tests/testthat/test-idle-restore-bookmarking.R
git commit -m "Enable url bookmarking and push bookmark urls to the browser"
```

---

### Task 4: Measure bookmark URL size

The spec caps the URL at ~6 KB. Measure before relying on it.

**Files:**
- Create: `scripts/measure_bookmark_url.R` (throwaway diagnostic, committed for reuse)

- [ ] **Step 1: Write the measurement script**

Create `scripts/measure_bookmark_url.R`:

```r
# Reports the size of the bookmark URL produced by the running app.
# Usage: open the app, set the heaviest filter state you can (Compare tab,
# both sides populated, many teams/players), then paste the JS block below
# into the browser console and record the number here.
cat(
  "Run in the browser console with the app open:\n\n",
  'window.Shiny.addCustomMessageHandler("ibpl_bookmark_url", function(m) {\n',
  '  console.log("bookmark bytes:", new Blob([m.url]).size);\n',
  "});\n\n",
  "Record the worst-case size in docs/superpowers/plans/",
  "2026-07-29-idle-restore-bookmarking.md (Task 4).\n",
  sep = ""
)
```

- [ ] **Step 2: Measure the worst case**

Start the app, go to Compare → Players with both sides fully populated (players, teams, opponents, dates, clutch on), then Lineup Data with team + 3 Players On + 2 Players Off, then read the logged byte count.

Record the observed worst case here: **_____ bytes** (fill in during execution).

- [ ] **Step 3: Decide**

If the worst case is under 6000 bytes: proceed, no fallback needed — note the number in the commit message.
If it exceeds 6000 bytes: stop and report to the user before continuing; the hidden-tab-dropping fallback from the spec becomes a required task rather than a contingency.

- [ ] **Step 4: Commit**

```bash
git add scripts/measure_bookmark_url.R
git commit -m "Add bookmark url size measurement note"
```

---

### Task 5: Make choice-populating observers restore-aware

Every observer that populates selectize choices at startup currently resets the
selection, which would wipe seeded values.

**Files:**
- Modify: `app/app.R:396-402` (teams / on_opponents / ld_opponents)
- Modify: `app/R/server_tab3.R:496` (tr_opponents)
- Modify: `app/R/server_tab4.R:250` (gl_opponents)
- Modify: `app/R/server_tab5_traditional.R:499-511` (ts_players, ts_teams, ts_opponents)
- Modify: `app/R/server_tab7_compare.R:1108-1127` (cmp_a/b teams, opponents, player list filters, GN selectizes)
- Test: `app/tests/testthat/test-idle-restore-bookmarking.R`

**Interfaces:**
- Consumes: `restore_aware_selection(session, id, current, choices)` (Task 2).
- Produces: no new API — behavior change only.

Note: `app/R/mod_lineup_player_filter.R:87-119` already intersects the current
selection with real choices (`selected_in_choices`), and `app/R/server_tab2.R:91-93`
already carries the current team forward. Those two need **no** change beyond
the value source, which Task 2's helper supplies through `input`-then-restore
fallback in Step 3 below.

- [ ] **Step 1: Write the failing contract test**

Append to `app/tests/testthat/test-idle-restore-bookmarking.R`:

```r
test_that("choice-populating observers preserve restored selections", {
  app_r_txt <- read_repo_txt("app.R")
  tab3_txt <- read_repo_txt("R", "server_tab3.R")
  tab4_txt <- read_repo_txt("R", "server_tab4.R")
  tab5_txt <- read_repo_txt("R", "server_tab5_traditional.R")
  tab7_txt <- read_repo_txt("R", "server_tab7_compare.R")
  mod_txt  <- read_repo_txt("R", "mod_lineup_player_filter.R")

  # the startup population path must not hard-reset selections any more
  expect_false(grepl(
    'updateSelectizeInput(session, "teams", choices = team_choices, selected = character(0)',
    app_r_txt, fixed = TRUE
  ))
  expect_match(app_r_txt, "restore_aware_selection(", fixed = TRUE)

  for (txt in list(tab3_txt, tab4_txt, tab5_txt, tab7_txt)) {
    expect_match(txt, "restore_aware_selection(", fixed = TRUE)
  }

  # the lineup module already intersects selections with real choices
  expect_match(mod_txt, "selected_in_choices(input$players_on, choices)", fixed = TRUE)
  expect_match(mod_txt, "restore_aware_selection(", fixed = TRUE)
})
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd app && "$RSCRIPT" -e "testthat::test_file('tests/testthat/test-idle-restore-bookmarking.R')"`
Expected: FAIL — `restore_aware_selection(` not found in `app.R`

- [ ] **Step 3: Update `app/app.R:396-402`**

Replace the observer body with a single vector-driven pass:

```r
  observeEvent(selected_game_year(), {
    td <- teams_for_year_df()
    team_choices <- stats::setNames(as.character(td$team_id), as.character(td$team_name))
    for (id in c("teams", "on_opponents", "ld_opponents")) {
      updateSelectizeInput(
        session, id,
        choices = team_choices,
        selected = restore_aware_selection(session, id, isolate(input[[id]]), team_choices),
        server = TRUE
      )
    }
  }, ignoreInit = FALSE)
```

- [ ] **Step 4: Update the tab observers**

Apply the same substitution in each file — replace `selected = character(0)`
(or `selected = ""`) with a `restore_aware_selection()` call using that input's
own id and choice vector:

- `app/R/server_tab3.R:496` — `tr_opponents` with `opponent_choices`
- `app/R/server_tab4.R:250` — `gl_opponents` with `opponent_choices`
- `app/R/server_tab5_traditional.R:510-511` — `ts_teams`, `ts_opponents` with `team_choices`
- `app/R/server_tab7_compare.R:1108-1114` — `cmp_a_teams`, `cmp_b_teams`, `cmp_a_opponents`, `cmp_b_opponents` with `team_choices`; `cmp_player_a_list_team_filter`, `cmp_player_b_list_team_filter` with `player_list_team_choices`
- `app/R/server_tab7_compare.R:1121-1127` — the seven GN selectizes with `gn_choices_with_blank`

Example for `server_tab3.R:496`:

```r
    updateSelectizeInput(
      session, "tr_opponents",
      choices = opponent_choices,
      selected = restore_aware_selection(session, "tr_opponents", isolate(input$tr_opponents), opponent_choices)
    )
```

Leave every `*_reset` handler untouched — those must keep clearing selections.

- [ ] **Step 5: Make the lineup module's team population restore-aware**

In `app/R/mod_lineup_player_filter.R`, change `update_team_choices()` so a blank
`selected` falls back to the restored value:

```r
    update_team_choices <- function(choices, selected = "") {
      selected <- restore_aware_selection(session, "team", selected, choices)
      updateSelectizeInput(session, "team", choices = choices, selected = selected, server = FALSE)
    }
```

`refresh_player_choices()` needs no change: it already intersects
`input$players_on` / `input$players_off` with the freshly built choices, and
those inputs carry the seeded values.

- [ ] **Step 6: Run the test to verify it passes**

Run: `cd app && "$RSCRIPT" -e "testthat::test_file('tests/testthat/test-idle-restore-bookmarking.R')"`
Expected: PASS (7 tests)

- [ ] **Step 7: Run the full suite for regressions**

Run: `cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', filter = 'tab|team-filter|date-reset', reporter = 'summary')"`
Expected: no new failures versus `main` (record any pre-existing failures before starting).

- [ ] **Step 8: Commit**

```bash
git add app/app.R app/R/server_tab3.R app/R/server_tab4.R app/R/server_tab5_traditional.R app/R/server_tab7_compare.R app/R/mod_lineup_player_filter.R app/tests/testthat/test-idle-restore-bookmarking.R
git commit -m "Make choice-populating observers preserve restored selections"
```

---

### Task 6: Browser side — store the URL, restore on return

Replace the JS replay engine with URL storage plus a return trigger.

**Files:**
- Modify: `app/www/app.js` (the idle IIFE, lines 251-1599)
- Modify: `app/www/app.css` (idle overlay → paused pill)
- Test: `app/tests/testthat/test-idle-restore-bookmarking.R`

**Interfaces:**
- Consumes: custom message `ibpl_bookmark_url` (`{url, v}`) from Task 3.
- Produces: `window.ibplDebugSavedSession()`, `window.ibplClearSavedSession()`, `window.ibplRestoreSavedSession()` (kept as debug hooks with the same names, new implementations).

- [ ] **Step 1: Write the failing contract test**

Append to `app/tests/testthat/test-idle-restore-bookmarking.R`:

```r
test_that("browser stores bookmark urls and restores by navigation", {
  js <- read_repo_txt("www", "app.js")

  expect_match(js, '"ibpl_bookmark_url"', fixed = TRUE)
  expect_match(js, "window.location.replace(url)", fixed = TRUE)
  expect_match(js, "ibpl_v", fixed = TRUE)

  # the replay engine is gone
  for (dead in c(
    "persistIds", "sendRestoreState", "attemptRestoreSend", "applyRestoreValues",
    "reapplyDependentPlayerInputs", "requestRestoreFinish", "ibpl_restore_applied",
    "restoreMaxSendAttempts", "restoreTabQueryParam"
  )) {
    expect_false(grepl(dead, js, fixed = TRUE), info = dead)
  }
})

test_that("restore triggers on user return, never on expiry itself", {
  js <- read_repo_txt("www", "app.js")

  expect_match(js, "function restoreOnReturn()", fixed = TRUE)
  expect_match(js, "idleExpired = true;", fixed = TRUE)
  # expiry marks state and shows the pill; it must not navigate
  expect_match(js, "showPausedPill();", fixed = TRUE)
})
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd app && "$RSCRIPT" -e "testthat::test_file('tests/testthat/test-idle-restore-bookmarking.R')"`
Expected: FAIL — `ibpl_bookmark_url` not found in `app.js`

- [ ] **Step 3: Replace the idle IIFE internals**

In `app/www/app.js`, replace the state/replay machinery of the idle IIFE
(current lines ~251-1599) with this structure. Keep the surrounding IIFE, the
activity events, the heartbeat, and the warning countdown; delete the payload
engine entirely.

```js
  var cfg = window.IBPL_IDLE_CONFIG || {};
  var timeoutMs = Math.max(1, Number(cfg.timeoutSec || 360)) * 1000;
  var warningMs = Math.max(1, Number(cfg.warningSec || 60)) * 1000;
  var ttlMs = Math.max(1, Number(cfg.stateTtlHours || 24)) * 60 * 60 * 1000;
  var stateVersion = Number(cfg.stateVersion || 1);
  warningMs = Math.min(warningMs, Math.max(1000, timeoutMs - 1000));

  var keyBase = "ibpl_idle_resume:" + location.pathname.replace(/\/+$/, "");
  var tabIdKey = keyBase + ":tab_id";
  var tabId = getOrCreateTabId();
  var urlKey = keyBase + ":tab:" + tabId + ":bookmark:v" + stateVersion;
  var skipRestoreKey = keyBase + ":tab:" + tabId + ":skip_restore";
  var restoredFlagKey = keyBase + ":tab:" + tabId + ":restored";

  var idleExpired = false;
  var navigating = false;

  function storeBookmarkUrl(url) {
    var payload = JSON.stringify({ url: url, savedAt: Date.now(), v: stateVersion });
    safeSessionSet(urlKey, payload);
    safeLocalSet(urlKey, payload);
  }

  function loadBookmarkUrl() {
    var raw = safeSessionGet(urlKey) || safeLocalGet(urlKey);
    if (!raw) return null;
    try {
      var parsed = JSON.parse(raw);
      if (!parsed || parsed.v !== stateVersion || !parsed.url) return null;
      if ((Date.now() - Number(parsed.savedAt)) > ttlMs) return null;
      return parsed.url;
    } catch (e) {
      return null;
    }
  }

  // One-shot restore navigation. The bookmark params are stripped from the
  // address bar as soon as the new session has been created.
  function restoreOnReturn() {
    if (navigating || !idleExpired) return;
    if (safeSessionGet(skipRestoreKey)) return;
    var url = loadBookmarkUrl();
    navigating = true;
    safeSessionSet(restoredFlagKey, String(Date.now()));
    if (!url) {
      window.location.reload();
      return;
    }
    window.location.replace(url);
  }

  function clearBookmarkParams() {
    if (!window.history || typeof window.history.replaceState !== "function") return;
    if (location.search.indexOf("_inputs_") === -1) return;
    window.history.replaceState(window.history.state, "", location.pathname + location.hash);
  }
```

Wire it up:

```js
  if (window.Shiny && typeof window.Shiny.addCustomMessageHandler === "function") {
    window.Shiny.addCustomMessageHandler("ibpl_bookmark_url", function(msg) {
      if (msg && msg.url) storeBookmarkUrl(msg.url + "&ibpl_v=" + stateVersion);
    });
  }
```

Expiry shows the pill and never navigates:

```js
  function checkIdleState() {
    if (idleExpired || document.visibilityState === "hidden") return;
    var remainingMs = timeoutMs - (Date.now() - lastActivity);
    if (remainingMs <= 0) {
      idleExpired = true;
      showPausedPill();
      return;
    }
    if (remainingMs <= warningMs) {
      setOverlayState("warning", formatSeconds(remainingMs));
    } else {
      hideIdleWarning();
    }
  }
```

Return triggers — activity, visibility, and a disconnect observed while visible:

```js
  function handleDisconnected() {
    if (document.visibilityState === "hidden") {
      idleExpired = true;
      toggleNativeDisconnectUi(true);
      return;
    }
    idleExpired = true;
    toggleNativeDisconnectUi(true);
    showPausedPill();
    restoreOnReturn();
  }
```

and inside `bindActivity()`:

```js
    var events = ["mousemove", "mousedown", "keydown", "scroll", "touchstart", "click"];
    for (var i = 0; i < events.length; i++) {
      document.addEventListener(events[i], function() {
        if (idleExpired) { restoreOnReturn(); return; }
        markActivity(false);
      }, { passive: true });
    }
    document.addEventListener("visibilitychange", function() {
      if (document.visibilityState !== "visible") return;
      if (idleExpired || !shinyReadyForRestore()) { restoreOnReturn(); return; }
      markActivity(true);
    });
```

Startup: strip params, clear the skip marker, and show the post-restore notice.

```js
  clearBookmarkParams();
  if (safeSessionGet(skipRestoreKey)) safeSessionRemove(skipRestoreKey);
  if (safeSessionGet(restoredFlagKey)) {
    safeSessionRemove(restoredFlagKey);
    window.setTimeout(showRestoredNotice, 400);
  }
```

Debug hooks keep their names:

```js
  window.ibplDebugSavedSession = function() {
    return { url: loadBookmarkUrl(), idleExpired: idleExpired, tabId: tabId };
  };
  window.ibplClearSavedSession = function() {
    safeSessionRemove(urlKey);
    safeLocalRemove(urlKey);
    safeSessionSet(skipRestoreKey, String(Date.now()));
  };
  window.ibplRestoreSavedSession = function() {
    idleExpired = true;
    restoreOnReturn();
  };
```

- [ ] **Step 4: Replace the expired overlay with the paused pill**

Add `showPausedPill()` / `showRestoredNotice()` next to the existing overlay
builders, reusing the existing `restore-notice` styling:

```js
  function showPausedPill() {
    var pill = document.getElementById("ibpl-idle-pill");
    if (!pill) {
      pill = document.createElement("div");
      pill.id = "ibpl-idle-pill";
      pill.className = "restore-notice";
      pill.innerHTML =
        '<span>Session paused — resuming on activity.</span>' +
        '<button type="button" id="ibpl-idle-fresh">Start fresh</button>';
      document.body.appendChild(pill);
      var freshBtn = document.getElementById("ibpl-idle-fresh");
      if (freshBtn) {
        freshBtn.addEventListener("click", function(e) {
          e.stopPropagation();
          window.ibplClearSavedSession();
          navigating = true;
          window.location.reload();
        });
      }
    }
    hideIdleWarning();
    pill.classList.add("visible");
  }

  function showRestoredNotice() {
    var notice = document.getElementById("ibpl-restore-notice");
    if (!notice) {
      notice = document.createElement("div");
      notice.id = "ibpl-restore-notice";
      notice.className = "restore-notice";
      notice.innerHTML =
        '<span>Restored your last tab and filters.</span>' +
        '<button type="button" id="ibpl-restore-clear">Start fresh</button>';
      document.body.appendChild(notice);
      var clearBtn = document.getElementById("ibpl-restore-clear");
      if (clearBtn) {
        clearBtn.addEventListener("click", function() {
          window.ibplClearSavedSession();
          window.location.reload();
        });
      }
    }
    notice.classList.add("visible");
    window.setTimeout(function() { notice.classList.remove("visible"); }, 6000);
  }
```

Delete the separate `ibpl_restore_applied` IIFE (current lines 1601-1638) — the
notice is now driven by the `restoredFlagKey` marker.

- [ ] **Step 5: Check the diff is surgical, not a whole-file rewrite**

Run: `git diff --stat app/www/app.js`
Expected: a few hundred changed lines, **not** ~1750. If the whole file shows as
changed, the editor rewrote line endings — redo the edit preserving LF and stage
with `git -c core.autocrlf=false add app/www/app.js`.

- [ ] **Step 6: Run the test to verify it passes**

Run: `cd app && "$RSCRIPT" -e "testthat::test_file('tests/testthat/test-idle-restore-bookmarking.R')"`
Expected: PASS (9 tests)

- [ ] **Step 7: Commit**

```bash
git -c core.autocrlf=false add app/www/app.js app/www/app.css app/tests/testthat/test-idle-restore-bookmarking.R
git commit -m "Restore idle sessions by navigating to a stored bookmark url"
```

---

### Task 7: Delete the R replay machinery

**Files:**
- Modify: `app/app.R` — delete lines 102-107 (`startup_restore_tab`), 127-315 (`restore_*` id lists and `restore_state_values()`), 317-357 (`pending_ld_lineup_restore` observer and `ibpl_restore_state` observer), and the `pending_ld_lineup_restore` reactiveVal at line 98
- Modify: `app/app.R:26-28` — drop `ibpl_restore_tab_from_query()` from `ui()`
- Modify: `app/app.R:502-509` — restore-aware startup gating
- Modify: `app/R/helpers.R` — delete `ibpl_restore_tab_from_query()`
- Test: `app/tests/testthat/test-idle-restore-bookmarking.R`, delete `app/tests/testthat/test-idle-restore-startup.R`

**Interfaces:**
- Consumes: `restored_input_value()` (Task 2).
- Produces: nothing new — deletions plus the reworked gate.

- [ ] **Step 1: Write the failing contract test**

Append to `app/tests/testthat/test-idle-restore-bookmarking.R`:

```r
test_that("the replay machinery is gone from R", {
  app_r_txt <- read_repo_txt("app.R")
  helpers_txt <- read_repo_txt("R", "helpers.R")

  for (dead in c(
    "restore_state_values", "ibpl_restore_state", "pending_ld_lineup_restore",
    "restore_selectize_ids", "restore_radio_ids", "restore_date_range_ids",
    "ibpl_restore_tab_from_query", "ibpl_restore_applied"
  )) {
    expect_false(grepl(dead, app_r_txt, fixed = TRUE), info = dead)
  }
  expect_false(grepl("ibpl_restore_tab_from_query", helpers_txt, fixed = TRUE))
})

test_that("home storylines stay gated when a bookmark restores another tab", {
  app_r_txt <- read_repo_txt("app.R")
  hub_txt <- read_repo_txt("R", "mod_team_hub.R")

  expect_match(app_r_txt, "startup_restore_pending", fixed = TRUE)
  expect_match(app_r_txt, 'restored_input_value(session, "main_tabs")', fixed = TRUE)
  expect_match(hub_txt, "suspendWhenHidden = TRUE", fixed = TRUE)
})
```

- [ ] **Step 2: Run the test to verify it fails**

Run: `cd app && "$RSCRIPT" -e "testthat::test_file('tests/testthat/test-idle-restore-bookmarking.R')"`
Expected: FAIL — `restore_state_values` still present in `app.R`

- [ ] **Step 3: Simplify `ui()`**

Replace lines 26-29 of `app/app.R` with:

```r
ui <- function(request) {
  navbarPage(
  id = "main_tabs",
```

(`bslib::navbarPage_` calls `restoreInput(id = "main_tabs")` internally, so the
`selected` argument is no longer needed — the bookmark supplies the tab.)

- [ ] **Step 4: Rework the startup gate**

Replace lines 102-107 with:

```r
  restored_tab <- sanitize_single_choice(restored_input_value(session, "main_tabs"))
  startup_restore_pending <- reactiveVal(
    nzchar(restored_tab) && !identical(restored_tab, "home")
  )
```

and lines 502-509 with:

```r
  observeEvent(selected_game_year(), {
    gy <- suppressWarnings(as.integer(selected_game_year()))
    if (isTRUE(startup_restore_pending())) {
      startup_restore_pending(FALSE)
      hub_storylines_ready_year(gy)
    } else if (nzchar(restored_tab)) {
      hub_storylines_ready_year(gy)
    } else {
      hub_storylines_ready_year(NA_integer_)
    }
  }, ignoreInit = FALSE, priority = 200)
```

- [ ] **Step 5: Delete the replay code**

Delete from `app/app.R`: the `pending_ld_lineup_restore` reactiveVal (line 98),
the six `restore_*_ids` vectors and `restore_id_allowed()` / `restore_chr_*()` /
`restore_state_values()` (lines 127-315), the `ld_lineup_filter-team` pending
observer (lines 317-340), and the `input$ibpl_restore_state` observer (lines
342-357). Delete `ibpl_restore_tab_from_query()` from `app/R/helpers.R`
(lines 593-608).

Keep `sanitize_persisted_choices()` and `sanitize_single_choice()` — they are
used by the modules and by Task 2's helpers.

- [ ] **Step 6: Delete the obsolete test file**

```bash
git rm app/tests/testthat/test-idle-restore-startup.R
```

- [ ] **Step 7: Run the test to verify it passes**

Run: `cd app && "$RSCRIPT" -e "testthat::test_file('tests/testthat/test-idle-restore-bookmarking.R')"`
Expected: PASS (11 tests)

- [ ] **Step 8: Run the full suite**

Run: `cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', reporter = 'summary')"`
Expected: no new failures versus the baseline recorded in Task 5 Step 7.

- [ ] **Step 9: Commit**

```bash
git add -A app/app.R app/R/helpers.R app/tests/testthat/
git commit -m "Delete the idle-restore replay machinery"
```

---

### Task 8: Manual regression pass

No automated test covers browser lifecycle behavior; this is the gate before merge.

**Files:** none (verification only)

Set `APP_IDLE_TIMEOUT_SEC=45` and `APP_IDLE_WARNING_SEC=15` in `app/.Renviron`
for the duration of testing, then run
`"$RSCRIPT" -e "shiny::runApp('app', port = 7788, launch.browser = FALSE)"`.

- [ ] **Step 1: Per-tab restore**

For each of On/Off, Lineup Data, Team Ratings, Game Logs, Traditional Stats,
Compare: set distinctive filters, idle out, move the mouse, confirm the page
returns to the same tab with the same filters, and that the table renders
**once** (watch the console — no default-then-restored double render).

- [ ] **Step 2: Dependent player filters**

Lineup Data with a team + 3 Players On + 2 Players Off → idle out → return.
Expected: team, both player lists intact. Repeat for Compare → Lineups.

- [ ] **Step 3: Season change**

Change the season, idle out, return. Expected: restored season, and filters
consistent with that season (selections not valid in it are dropped, not stale).

- [ ] **Step 4: Start fresh**

Idle out, click "Start fresh" on the pill. Expected: defaults on Home, and the
next idle cycle does not resurrect the discarded state.

- [ ] **Step 5: Two browser tabs**

Two tabs on different app tabs with different filters. Idle both out, return to
each. Expected: each restores its own state (tab-scoped keys).

- [ ] **Step 6: Mobile background**

On a phone (iOS Safari and Android Chrome if available): background the app past
the timeout, return. Expected: restore happens on return with no dialog, and no
popup appears merely from switching apps.

- [ ] **Step 7: Corrupt / stale state**

In the console: `sessionStorage.setItem(Object.keys(sessionStorage).find(function(k){return k.indexOf("bookmark")>-1;}), "{{{")` then idle out and return.
Expected: plain reload to defaults, app fully usable.

- [ ] **Step 8: Restore the timeout values**

Revert `APP_IDLE_TIMEOUT_SEC` / `APP_IDLE_WARNING_SEC` in `app/.Renviron`.

- [ ] **Step 9: Record results**

Note any failures in the PR description. All nine rows must pass before merge.

---

### Task 9: Rewrite the architecture doc

**Files:**
- Modify: `docs/idle_session_restore_architecture.md` (full rewrite)
- Modify: `CLAUDE.md` (session guardrails line)

- [ ] **Step 1: Rewrite the architecture doc**

Replace the contents with a description of the bookmarking model: purpose, the
capture/store/return lifecycle, the source map (`app.R`, `helpers.R`, `app.js`,
`app.css`), configuration table (unchanged env vars plus
`IBPL_RESTORE_STATE_VERSION`), the two-clock model (unchanged), the
restore-context rule for server-populated choices (including the
`force = TRUE` gotcha), failure handling, debug hooks, and the manual regression
matrix from Task 8. Keep a short "Previous design" section noting that the
replay architecture was removed on 2026-07-29 and why.

- [ ] **Step 2: Update CLAUDE.md**

In the "Session guardrails" bullet, replace
`idle-session timeout with client-side state restore (APP_IDLE_CLOSE_SESSION, enabled in deployed .Renviron)`
with
`idle-session timeout with bookmark-based restore (APP_IDLE_CLOSE_SESSION, enabled in deployed .Renviron) — see docs/idle_session_restore_architecture.md`.

- [ ] **Step 3: Commit**

```bash
git add docs/idle_session_restore_architecture.md CLAUDE.md
git commit -m "Document the bookmark-based idle restore architecture"
```

---

### Task 10: Merge

- [ ] **Step 1: Confirm the suite is green**

Run: `cd app && "$RSCRIPT" -e "testthat::test_dir('tests/testthat', reporter = 'summary')"`
Expected: no new failures versus baseline.

- [ ] **Step 2: Confirm Task 8 passed**

All nine manual rows must be recorded as passing. If any failed, stop and report.

- [ ] **Step 3: Merge and clean up**

```bash
git checkout main
git merge shiny/idle-restore-bookmarking
git push origin main
git branch -d shiny/idle-restore-bookmarking
```

- [ ] **Step 4: Note the deploy**

The live app is already behind `main`. Do **not** deploy as part of this work;
report to the user that `rsconnect::deployApp('app')` is pending along with the
other queued changes.
