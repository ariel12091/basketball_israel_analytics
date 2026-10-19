# EuroLeague Lineups UI Parity Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Tab 10 (`euro_ld`, EuroLeague Lineup Data) renders the same table and
the same sidebar as Tab 2 (`ld`, Israeli Lineup Data), by calling Tab 2's
renderers rather than by growing a parallel copy of them.

**Architecture:** `server_tab2.R`'s `renderDataTable` body moves into two
functions in `helpers.R`. A `spec` list carries the only two league
differences (anchor class, click statement). Tab 2 calls them with no visible
change. Tab 10 then renames its data frame to Tab 2's column contract and
calls the same two functions, and its sidebar is reordered to Tab 2's sequence.

**Tech Stack:** R 4.4.2, Shiny (bslib/BS5), DT, dplyr. No SQL, no migrations,
no deploy in tasks 1-3.

**Spec:** `docs/superpowers/specs/2026-08-14-euroleague-lineups-ui-parity-design.md`

## Global Constraints

- **`euroleague/CLAUDE.md` non-negotiable rule:** never write a parallel
  `euro_` implementation of logic that already exists. Shared code goes in
  `helpers.R`, never `global_euro.R`, and is named neutrally.
- **Step 1 is a move, not a rewrite.** Verify it by reversing the transform and
  diffing against `HEAD`. Do not write tests for moved code.
- **Line endings.** `app/R/helpers.R` (2064 CR / 2243 lines) and
  `app/R/server_tab2.R` (1215 CR / 1280 lines) have *mixed* line endings that
  currently match `HEAD` exactly. After every edit to either file, run
  `git diff --stat` and confirm the changed-line count is plausible. If the
  whole file shows as changed, the editor normalised the endings — revert and
  redo on bytes. `server_tab10_euro_lineups.R` and `ui_tab10_euro_lineups.R`
  are pure LF (CR=0).
- **No mixed-league data.** No ranked table mixes leagues; cache keys keep
  their league dimension. Nothing in this plan touches a query or a cache key.
- **`""` is the blank sentinel** for every single-select filter.
- **Do not rename `euro_ld_date_range`.** The id is invisible to users and
  renaming ripples through `setup_chip_clears()` and bookmarking for no gain.
- **Scope is table + sidebar.** Tab 10's explainer prose stays
  EuroLeague-specific. Tab 2's example box and its screenshots are not
  reproduced. The shot-splits legend *is* reproduced.
- **Base apply, not purrr** (`lapply`/`vapply`/`Filter`).
- Test command:
  `"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_dir('app/tests/testthat', filter='<name>')"`

---

## Two findings the spec does not record

Both were discovered reading the code and both change what the tasks must do.

### A. Polarity lives in the rank, not in the palette

The shared FF renderer applies `COLS_GRAD` to **every** `pr_*` column — it
never uses `COLS_REV`. Tab 2 bakes the inversion into the rank value upstream
(`server_tab2.R:239-249, 301-303`):

```r
df$pr_off_tov  <- pr_vec(df$off_tov, invert = TRUE)
df$pr_def_ppp  <- pr_vec(df$def_ppp, invert = TRUE)
df$pr_def_efg  <- pr_vec(df$def_efg, invert = TRUE)
df$pr_def_oreb <- pr_vec(df$def_oreb, invert = TRUE)
df$pr_def_ftr  <- pr_vec(df$def_ftr, invert = TRUE)
df$pr_def_tov  <- pr_vec(df$def_tov)            # not inverted
df$pr_ld_def_ppp_i <- pr_vec(df$def_ppp, invert = TRUE)
```

Tab 10 currently does the opposite: it stores an un-inverted rank and picks
`COLS_REV` at render time (`server_tab10_euro_lineups.R:630-645`). Task 2 must
move the inversion into `add_pct_ranks()` and delete the `COLS_REV` heat list,
or every defensive column on the EuroLeague tab renders with inverted colours.

Tab 2's `pr_*` names are also not Tab 10's:

| View | Tab 2 names (required) | Tab 10 today |
|---|---|---|
| Summary | `pr_ld_net`, `pr_ld_off_ppp`, `pr_ld_def_ppp_i` | `pr_net`, `pr_off_ppp`, `pr_def_ppp`, `pr_off_efg`, `pr_def_efg` |
| Four Factors | `pr_off_ppp`, `pr_off_efg`, `pr_off_oreb`, `pr_off_tov`, `pr_off_ftr`, `pr_def_ppp`, `pr_def_efg`, `pr_def_oreb`, `pr_def_tov`, `pr_def_ftr`, `pr_net` | `pr_off_ts`, `pr_off_tov`, `pr_off_oreb`, `pr_off_ftr`, `pr_def_ts`, `pr_def_tov`, `pr_def_oreb`, `pr_def_ftr` |

**Deliberate non-change:** Tab 10 ranks only rows above `adaptive_baseline()`
and leaves the rest `NA` (uncoloured); Tab 2 ranks every row. That is a
ranking *policy* difference, not a rendering one, and it stays. This plan
achieves parity of the table, not of the ranking threshold.

### B. Tab 10 must stop building its own TOTAL row

`euro_ld_display()` currently `rbind`s a summed TOTAL row
(`server_tab10_euro_lineups.R:435-452`). The shared renderer builds its own.
Leaving both produces two TOTAL rows. Task 2 deletes Tab 10's, and the chip
readout `NROW(euro_ld_display()) - 1L` loses its `- 1L` at the same time.

---

## File Structure

| File | Responsibility after this plan |
|---|---|
| `app/R/helpers.R` | Gains `lineup_ff_datatable()`, `lineup_summary_datatable()`, `LD_LINEUP_TABLE_SPEC`, `EURO_LD_LINEUP_TABLE_SPEC`. The only place either lineup table is defined. |
| `app/R/server_tab2.R` | Loses 395 lines of renderer; `output$ld_table` becomes a ~12-line dispatcher. Everything else unchanged. |
| `app/R/server_tab10_euro_lineups.R` | Gains the rename/derive map and stat-filter state; loses its two sketches, its `summary_cols`/`ff_cols`, its `renderDT` body, its TOTAL row and its `COLS_REV` heat list. |
| `app/R/ui_tab10_euro_lineups.R` | Sidebar reordered to Tab 2's sequence; gains the shot-splits legend; FF explainer switches TS% → eFG%. |
| `euroleague/CLAUDE.md` | Records the TS% → eFG% reversal. |

---

## Task 1: Extract Tab 2's renderers into `helpers.R`

Pure move. Tab 2 must look and behave exactly as it does now.

**Files:**
- Modify: `app/R/helpers.R` (append at end, after `onoff_clean_display_names`)
- Modify: `app/R/server_tab2.R:441-836`
- Test: `app/tests/testthat/test-primary-table-render-smoke.R` (existing, unchanged)

**Interfaces:**
- Produces:
  - `lineup_ff_datatable(df, stat_filters, spec, raw = df)` → a `DT::datatable` widget.
  - `lineup_summary_datatable(df, stat_filters, spec)` → a `DT::datatable` widget.
  - `spec` is a list with `link_class` (character) and `click_js` (character,
    a JS statement; `this` is the clicked anchor, carrying `data-hash` and
    `data-team-id`).
  - `LD_LINEUP_TABLE_SPEC` — the Israeli spec.
- Consumes: `apply_stat_filters()`, `add_shot_split_metrics()`,
  `shot_split_metric_cols()`, `dt_escape_except()` (all already in
  `helpers.R`); the `CUTS`, `COLS_GRAD`, `HEADER_TOOLTIP_JS`,
  `OFF_OREB_TOOLTIP`, `DEF_OREB_TOOLTIP` globals from `global.R`.

### The four permitted deviations from byte-identical

Anything else appearing in the reverse-diff is a mistake.

1. `ld_stat_filter_state$filters()` → the `stat_filters` argument (2 sites:
   old lines 471 and 671).
2. `raw <- ld_data()` (old line 475) → the `raw` parameter. The reactive
   cannot move into a plain function. Tab 2 passes `raw = ld_data()`, so the
   value is unchanged. Default `raw = df` serves Tab 10, whose display frame
   is already the un-selected frame.
3. `class=\"ld-lineup-link\"` → `class=\"%s\"` with `spec$link_class` appended
   to the `sprintf` argument list (2 sites: old lines 577 and 806).
4. The `callback =` string becomes a `sprintf` template over
   `spec$link_class` and `spec$click_js` (2 sites: old lines 592-597 and
   819-824). This changes indentation *inside the emitted JavaScript*, which
   is functionally inert. It is the only place the moved bytes differ.

Indentation of the moved bodies is left exactly as it was in `server_tab2.R`
(deeply indented inside `function() {`). `make_shot_render()` builds its JS
with a multi-line `sprintf` template, so re-indenting would change the emitted
JavaScript and stop this being a provable move. Follow the precedent of
`onoff_summary_datatable()` in the same file.

- [ ] **Step 1: Append both functions plus `LD_LINEUP_TABLE_SPEC` to `helpers.R`**

Copy `server_tab2.R:455-637` verbatim into the body of
`lineup_ff_datatable`, and `server_tab2.R:640-834` verbatim into the body of
`lineup_summary_datatable`, applying only the four deviations above. Then:

```r
# The Israeli spec. Tab 2's anchors are handled by window.handleLineupLinkClick,
# defined in www/app.js, which sets input$ld_lineup_click.
LD_LINEUP_TABLE_SPEC <- list(
  link_class = "ld-lineup-link",
  click_js   = "window.handleLineupLinkClick(this);"
)
```

The parameterised call sites read:

```r
      ff_players_render <- DT::JS(sprintf(
        "function(data, type, row, meta) {
           if (type !== 'display' || !row) return data;
           if (row[0] === 0) return data;
           var hash = row[%d];
           var tid = row[%d];
           var esc = function(x) { return $('<div/>').text(x == null ? '' : String(x)).html(); };
           return '<a href=\"#\" class=\"%s\" data-hash=\"' + esc(hash) + '\" data-team-id=\"' + esc(tid) + '\">' + esc(data) + '</a>';
         }", ff_hash_idx, ff_tid_idx, spec$link_class))
```

```r
                          callback = DT::JS(sprintf(
                            "table.on('click', 'a.%s', function(e) {
                               e.preventDefault();
                               %s
                             });", spec$link_class, spec$click_js)
                          ),
```

- [ ] **Step 2: Replace `server_tab2.R:441-836` with the dispatcher**

```r
  output$ld_table <- DT::renderDataTable({
    req(ld_params())
    df <- ld_data()
    mode <- input$ld_view_mode
    tmap <- team_name_vec()

    # Common: Map team names and create Players column
    if ("team_id" %in% names(df)) {
      df$Team <- unname(tmap[as.character(df$team_id)])
      df$Team[is.na(df$Team)] <- as.character(df$team_id[is.na(df$Team)])
    }
    if ("player_names_str" %in% names(df)) df$Players <- df$player_names_str

    if (identical(mode, "Four Factors")) {
      lineup_ff_datatable(df, ld_stat_filter_state$filters(),
                          LD_LINEUP_TABLE_SPEC, raw = ld_data())
    } else {
      lineup_summary_datatable(df, ld_stat_filter_state$filters(),
                               LD_LINEUP_TABLE_SPEC)
    }
  })
```

- [ ] **Step 3: Confirm the edits did not normalise line endings**

Run:
```bash
cd /c/Users/ariel/documents/on_off_israel_pbp
for f in app/R/helpers.R app/R/server_tab2.R; do
  printf "%-28s HEAD_CR=%s WT_CR=%s\n" "$f" \
    "$(git show HEAD:$f | tr -cd '\r' | wc -c)" "$(tr -cd '\r' < $f | wc -c)"
done
git diff --stat app/R/
```
Expected: `helpers.R` HEAD_CR=2064 and WT_CR=2064; `server_tab2.R` HEAD_CR=1215
and WT_CR=1215 (the appended/removed lines are LF, so the CR counts do not
move). `git diff --stat` shows roughly `+400 / -400` split across the two
files, **not** a whole-file rewrite. If either file shows every line changed,
`git checkout` it and redo the edit on bytes.

- [ ] **Step 4: Verify the move by reversing the transform**

```bash
cd /c/Users/ariel/documents/on_off_israel_pbp
git show HEAD:app/R/server_tab2.R > /tmp/tab2_head.R
```
Inline the two helper bodies back into a scratch copy of `server_tab2.R`,
undoing the four deviations, then:
```bash
diff /tmp/tab2_head.R /tmp/tab2_reversed.R
```
Expected: **no output**. Any hunk is a transcription error — fix it in
`helpers.R`, not in the scratch copy. Do not write new tests for this.

- [ ] **Step 5: Run the affected test file**

```bash
"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_dir('app/tests/testthat', filter='primary-table-render-smoke')"
```
Expected: PASS, same counts as before the change.

- [ ] **Step 6: Commit**

```bash
git -c core.autocrlf=false add app/R/helpers.R app/R/server_tab2.R
git commit -m "Extract the lineup DataTables into helpers.R

server_tab2.R's renderDataTable body becomes lineup_ff_datatable() and
lineup_summary_datatable(). A spec list carries the anchor class and the
click statement, the only two things that differ between leagues. Tab 2
calls them with the Israeli spec and is unchanged on screen.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 2: Tab 10 adopts Tab 2's column contract

**Files:**
- Modify: `app/R/server_tab10_euro_lineups.R`
- Modify: `app/R/helpers.R` (append `EURO_LD_LINEUP_TABLE_SPEC`)
- Test: `app/tests/testthat/test-euro-lineup-units.R`

**Interfaces:**
- Consumes: `lineup_ff_datatable()`, `lineup_summary_datatable()` from Task 1.
- Produces: `EURO_LD_LINEUP_TABLE_SPEC`, and a Tab 10 display frame carrying
  Tab 2's column names.

- [ ] **Step 1: Add the EuroLeague spec to `helpers.R`**

The shared renderer emits `data-hash` and `data-team-id`. Tab 10's existing
handler reads `data-unit`, which the shared renderer never writes, so the
click statement reads `data-hash` instead. The Shiny input id
(`euro_ld_clicked_unit`) and the modal observer are unchanged.

```r
# The EuroLeague spec. Tab 10 has no equivalent of app.js's
# handleLineupLinkClick, so it sets its input directly. data-hash carries the
# unit_key, which the rename map surfaces as sub_lineup_hash.
EURO_LD_LINEUP_TABLE_SPEC <- list(
  link_class = "euro-ld-unit",
  click_js   = paste0(
    "Shiny.setInputValue('euro_ld_clicked_unit', ",
    "this.getAttribute('data-hash'), {priority: 'event'});"
  )
)
```

- [ ] **Step 2: Move rank polarity into `add_pct_ranks()` and rename the ranks**

Replace `SUMMARY_RANKS` / `FF_RANKS` (`server_tab10_euro_lineups.R:486-492`)
with Tab 2's names, each carrying its own inversion flag. Finding A explains
why. Rewrite the loop in `add_pct_ranks()` (lines 464-482) so it reads a
`list(src =, invert =)` spec and inverts after ranking — note the loop
variable changes from a bare column name to the spec list, so the `src` line
changes too:

```r
    for (nm in names(specs)) {
      spec <- specs[[nm]]
      vals <- suppressWarnings(as.numeric(df[[spec$src]]))
      vals[!eligible] <- NA_real_
      pr <- rep(NA_real_, length(vals))
      ok <- !is.na(vals)
      if (sum(ok) > 1L) {
        pr[ok] <- (rank(vals[ok], ties.method = "average") - 1) / (sum(ok) - 1)
      } else if (sum(ok) == 1L) {
        pr[ok] <- 0.5
      }
      if (isTRUE(spec$invert)) pr <- 1 - pr
      df[[nm]] <- pr
    }
```

```r
  # Polarity is baked into the rank, not chosen at render time: the shared
  # renderer applies COLS_GRAD to every pr_* column, exactly as Tab 2 does.
  SUMMARY_RANKS <- list(
    pr_ld_off_ppp   = list(src = "off_ppp"),
    pr_ld_def_ppp_i = list(src = "def_ppp", invert = TRUE),
    pr_ld_net       = list(src = "net_rtg")
  )
  FF_RANKS <- list(
    pr_off_ppp  = list(src = "off_ppp"),
    pr_off_efg  = list(src = "off_efg"),
    pr_off_oreb = list(src = "off_oreb_pct"),
    pr_off_tov  = list(src = "off_tov_pct", invert = TRUE),
    pr_off_ftr  = list(src = "off_ftr"),
    pr_def_ppp  = list(src = "def_ppp",     invert = TRUE),
    pr_def_efg  = list(src = "def_efg",     invert = TRUE),
    pr_def_oreb = list(src = "def_oreb_pct", invert = TRUE),
    pr_def_tov  = list(src = "def_tov_pct"),
    pr_def_ftr  = list(src = "def_ftr",     invert = TRUE),
    pr_net      = list(src = "net_rtg")
  )
```

Note the sources are the `add_rates()` output names (`off_oreb_pct`,
`off_tov_pct`), which still hold at ranking time — the rename in Step 3 runs
downstream, in `euro_ld_display()`.

- [ ] **Step 3: Add the rename/derive map, counts before rates**

Add to `server_tab10_euro_lineups.R`, called from `euro_ld_display()` after
the min-poss filter:

```r
  # Tab 2's column contract. The count columns are COPIED, not renamed: the
  # Summary view still reads off_fg3_made for its shot splits, and off_fga /
  # off_oreb are still the FTR and OREB denominators. Copying first also makes
  # the ordering constraint structural rather than a rule to remember --
  # off_tov is a raw count here and a rate on Tab 2, so deriving the rate
  # before copying the count would leave the FF TOTAL row dividing a rate by
  # possessions.
  to_tab2_contract <- function(df) {
    if (!NROW(df)) return(df)
    df$Team            <- df$team_name
    df$Players         <- df$player_names_str
    df$sub_lineup_hash <- df$unit_key
    df$num_starters    <- df$unit_size          # constant; see Task 4
    df$plus_minus      <- as.numeric(df$off_pts) - as.numeric(df$def_pts)

    # --- raw counts the FF TOTAL row sums (copies) ---
    df$off_oreb_cnt  <- df$off_oreb
    df$off_oreb_opps <- df$off_oreb_opp
    df$off_tov_cnt   <- df$off_tov
    df$off_fga_cnt   <- df$off_fga
    df$off_fgm_cnt   <- df$off_fgm
    df$off_fg3m_cnt  <- df$off_fg3_made
    df$def_oreb_cnt  <- df$def_oreb
    df$def_oreb_opps <- df$def_oreb_opp
    df$def_tov_cnt   <- df$def_tov
    df$def_fga_cnt   <- df$def_fga
    df$def_fgm_cnt   <- df$def_fgm
    df$def_fg3m_cnt  <- df$def_fg3_made
    # off_ts_poss, off_fta, off_pts, def_pts, minutes already carry Tab 2's names.

    # --- rates, LAST: these overwrite off_oreb / off_tov ---
    df$off_oreb <- df$off_oreb_pct
    df$def_oreb <- df$def_oreb_pct
    df$off_tov  <- df$off_tov_pct
    df$def_tov  <- df$def_tov_pct
    # off_efg, def_efg, off_ftr, def_ftr, off_ppp, def_ppp, net_rtg,
    # total_poss, off_poss, def_poss, minutes already carry Tab 2's names.
    df
  }
```

- [ ] **Step 4: Delete Tab 10's TOTAL row**

In `euro_ld_display()`, delete the `count_cols` vector and the `total` /
`rbind(total, df)` block (`server_tab10_euro_lineups.R:435-452`) — Finding B.
The reactive ends `to_tab2_contract(df)`.

In `output$euro_ld_filter_chips`, change
`n_units <- max(NROW(euro_ld_display()) - 1L, 0L)` to
`n_units <- NROW(euro_ld_display())`.

- [ ] **Step 5: Replace the `renderDT` body with the shared calls**

Delete `summary_cols`, `ff_cols`, `sketch_summary`, `sketch_ff` and the whole
`output$euro_ld_dt` body (`server_tab10_euro_lineups.R:494-652`), replacing it
with:

```r
  output$euro_ld_dt <- renderDT({
    df <- euro_ld_display()
    if (!NROW(df)) {
      return(datatable(data.frame(Message = "No units match these filters."),
                       rownames = FALSE, options = list(dom = "t")))
    }
    if (identical(input$euro_ld_view_mode, "Four Factors")) {
      lineup_ff_datatable(df, euro_ld_stat_filter_state$filters(),
                          EURO_LD_LINEUP_TABLE_SPEC)
    } else {
      lineup_summary_datatable(df, euro_ld_stat_filter_state$filters(),
                               EURO_LD_LINEUP_TABLE_SPEC)
    }
  })
```

- [ ] **Step 6: Wire the stat filters**

Near the top of `server_tab10_euro_lineups()`, beside `auto_enabled`:

```r
  euro_ld_stat_filter_state <- make_stat_filter_state()
  euro_ld_stat_filter_cols <- reactive({
    if (identical(input$euro_ld_view_mode, "Four Factors")) LD_FF_FILTERABLE_COLS
    else LD_SUMMARY_FILTERABLE_COLS
  })
  setup_stat_filter_handlers("euro_ld", input, session,
                             euro_ld_stat_filter_cols, euro_ld_stat_filter_state)
```

`LD_SUMMARY_FILTERABLE_COLS` and `LD_FF_FILTERABLE_COLS` are already file-scope
globals in `server_tab2.R`; both files are sourced by `app.R`, so reuse them
rather than defining `euro_` copies.

In `output$euro_ld_filter_chips`, **concatenate** rather than replace — the
unit-size / row-count / min-poss readouts are not clearing filters:

```r
      extra_children = c(
        lapply(bits, function(b) tags$span(class = "filter-chip", b)),
        stat_filter_chips_ui("euro_ld", euro_ld_stat_filter_state,
                             euro_ld_stat_filter_cols)
      )
```

- [ ] **Step 7: Replay offline against real EuroLeague data**

A blanket rename on this pair has corrupted a join key once before, and
eyeballing did not catch it. Write a temp `.R` file (not `Rscript -e`, which
segfaults on long input) in the scratchpad that connects with
`connect_timeout=15L`, pulls `euroleague.sub_lineups_stats_mv` for the current
competition and season, and replays `add_rates()` → `add_pct_ranks()` →
`to_tab2_contract()` by sourcing the file-scope helpers. Assert on the result:

- every name in Tab 2's `keep_cols` (both views) is present;
- `off_tov_cnt` is integral and `off_tov` is a percentage — i.e.
  `max(off_tov, na.rm = TRUE) < 100` and `all(off_tov_cnt == round(off_tov_cnt))`;
- `off_oreb_cnt <= off_oreb_opps` for every row;
- `plus_minus == off_pts - def_pts` for every row;
- reconcile against a known invariant: summed `off_pts / off_poss * 100` over
  5-player units equals the season Off PPP the tab showed before this change;
- every `pr_*` column is in `[0, 1]` or `NA`, and `pr_def_ppp` is
  *anti*-correlated with `def_ppp` (`cor < 0`) while `pr_off_ppp` is
  positively correlated with `off_ppp`.

Report the actual numbers. Do not proceed on "looks right".

- [ ] **Step 8: Run the affected test file**

```bash
"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_dir('app/tests/testthat', filter='euro-lineup-units')"
```
`test-euro-lineup-units.R` asserts on `server_tab10_euro_lineups.R` *source
text*. Some assertions will legitimately need updating (the TS% columns are
gone). Update an assertion only where the behaviour it guarded genuinely
moved; a failure that indicates real breakage gets fixed in the code.

- [ ] **Step 9: Commit**

```bash
git -c core.autocrlf=false add app/R/helpers.R app/R/server_tab10_euro_lineups.R app/tests/testthat/test-euro-lineup-units.R
git commit -m "Render the EuroLeague lineups table through Tab 2's renderers

Tab 10 renames its frame to Tab 2's column contract and calls the shared
lineup DataTables. Rank polarity moves into add_pct_ranks() because the
shared renderer applies COLS_GRAD to every pr_* column, and Tab 10's own
TOTAL row is deleted because the renderer builds one.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 3: Sidebar parity

**Files:**
- Modify: `app/R/ui_tab10_euro_lineups.R`
- Modify: `app/R/server_tab10_euro_lineups.R` (reset handler only)
- Modify: `euroleague/CLAUDE.md`
- Test: `app/tests/testthat/test-server-tabs-smoke.R`, `test-tab-wiring.R`

- [ ] **Step 1: Reorder and relabel the sidebar**

Target sequence, matching `ui_tab2_lineup.R:30-61`: reset → min-poss slider +
`helpText` → group size → player filter → starters → dates → clutch → accordion
toggle → game context.

| Change | From | To |
|---|---|---|
| View label | `"Select View:"` | `"View:"` |
| Reset label | `"Reset to defaults"` | `"Reset Lineup Filters"` |
| Group size | `selectInput`, `"2 players"`–`"5 players"` | `radioButtons(..., choices = c("2","3","4","5"), selected = "5", inline = TRUE)` |
| Min poss | slider only | slider + `helpText("Higher minimums improve stability but remove smaller-sample lineups.")` |
| Player filter | `layout = "stacked"`, no help | Tab 2's arguments: `team_help`, `players_on_placeholder`, `players_off_placeholder`, default layout |

Keep the input id `euro_ld_group_size`. Keep `shared_head_tags()` — it stays
in Tab 10 and is not added to Tab 2. Keep `euro_ld_date_range`.

- [ ] **Step 2: Add the shot-splits legend**

Copy the `div(class = "legend-box", ...)` block from `ui_tab2_lineup.R:146-180`
verbatim, inside a `conditionalPanel(condition = "input.euro_ld_view_mode == 'Summary'", ...)`,
placed immediately above `uiOutput("euro_ld_filter_chips")`. It explains the
`Off Shot` / `Def Shot` cells Tab 10 gains in Task 2.

- [ ] **Step 3: Switch the FF explainer from TS% to eFG%**

In `ui_tab10_euro_lineups.R`, the FF explainer `intro` says "shooting (TS%)"
and a bullet says "TS% uses the EuroLeague denominator (FGA plus the last free
throw of a committed-foul trip), not the 0.44 x FTA estimate." The FF table no
longer shows TS%. Replace with eFG% wording:

- intro: `"... Break its Net into shooting (eFG%), rebounding (OREB%), turnovers (TOV%), and free-throw pressure (FTR) on both ends."`
- bullet: `"eFG% credits a made three as 1.5 field goals: (FGM + 0.5 x 3PM) / FGA."`
- Keep the bullet "Rates are computed after aggregating the selected games,
  never averaged from per-game ratios."

- [ ] **Step 4: Fix the reset handler**

`server_tab10_euro_lineups.R:64` — group size is a radio now:

```r
    updateRadioButtons(session, "euro_ld_group_size", selected = "5")
```

- [ ] **Step 5: Record the reversal in `euroleague/CLAUDE.md`**

`euroleague/CLAUDE.md` records TS% as a deliberate choice (EuroLeague's own
denominator, explicitly not `0.44 × FTA`). Tab 10's Four Factors view now
shows eFG%. Add a dated note next to that decision saying the display was
reversed in favour of parity with Tab 2, that the TS% denominator is still
computed and stored (`off_ts_poss`), and that only the displayed column
changed.

- [ ] **Step 6: Launch the app and confirm both views side by side**

```bash
"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "shiny::runApp('app')"
```
Open Tab 2 and Tab 10, in both Summary and Four Factors. Confirm: identical
column order and headers; per-column `filter = "top"` boxes on Summary; the
TOTAL row pinned at the top and not clickable; `Off Shot`/`Def Shot` bars
render on Tab 10; clicking a unit name opens the game-log modal; paging shows
50 with a 25/50/100/200/1000 length menu. The DB connects slowly on cold start
— an inert-looking UI before the first query returns is expected, not a bug.

- [ ] **Step 7: Run the full test suite, once**

```bash
"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_dir('app/tests/testthat')"
```
Expected: no new failures against the pre-change baseline. Record the baseline
before starting Task 3 if it is not already known — this suite includes
DB-backed files that can fail for unrelated reasons.

- [ ] **Step 8: Commit**

```bash
git -c core.autocrlf=false add app/R/ui_tab10_euro_lineups.R app/R/server_tab10_euro_lineups.R euroleague/CLAUDE.md
git commit -m "Give the EuroLeague lineups tab Tab 2's sidebar

Reorders and relabels the sidebar to Tab 2's sequence, adds the shot-splits
legend for the Off Shot / Def Shot cells, and switches the Four Factors
explainer from TS% to eFG% to match the table. Records the reversal in
euroleague/CLAUDE.md.

Co-Authored-By: Claude Opus 5 <noreply@anthropic.com>"
```

---

## Task 4: Real `# Starters` — OPTIONAL, DEFERRED

**Do not execute without separate approval.** The user marked this optional at
spec time. It is a visible product change on the live Israeli tab, not a
refactor.

Tab 2's `# Starters` is not a starter count: `fetch_lineups_all` returns
`s.num_lineup::numeric AS num_starters` while filtering
`WHERE s.num_lineup = p_num_lineup`, so it is a constant equal to the Group
size radio. Task 2 reproduces that constant on the EuroLeague side, which
keeps parity exact and leaves this a pure source swap on both leagues — the
shared renderer does not change at all.

Proposed definition: possession-weighted mean of on-court own starters over
the unit's offensive possessions, `Σ(own_starters × off_poss) / Σ(off_poss)`,
one decimal.

- EuroLeague — `lineup_totals_by_game.own_starters`; add the weighted
  numerator to `sub_lineups_stats_mv`, `fetch_lineups_pergame`,
  `fetch_lineups_dynamic`, `fetch_lineups_direct`.
- Israeli — `pws.num_starters_offense` via `df_pts_poss_lineups_longer_mv`;
  add the weighted numerator to `sub_lineups_stats`, and have
  `fetch_lineups_all` return it in place of `s.num_lineup::numeric`.

Two risks any executing plan must carry:

1. **DROP+CREATE wipes GRANTs** on `sub_lineups_stats`, and `DROP FUNCTION`
   wipes EXECUTE. Run `scripts/apply_db_security.R` with
   `CONFIRM_DB_SECURITY_APPLY=1` afterwards and read the audit **in full** —
   never `tail` it.
2. Reconcile the new column against a known invariant before deploy: for
   5-player units the weighted mean must equal the count of starters in that
   exact five, and the season-wide mean over all units must sit in `[0, 5]`.

---

## Verification summary

| Task | How it is proven |
|---|---|
| 1 | Reverse the transform, `diff` against `HEAD` — empty output. Run `test-primary-table-render-smoke`. No new tests for moved code. |
| 2 | Offline replay against real EuroLeague data with the named assertions and a reconciliation against the pre-change season Off PPP. Run `test-euro-lineup-units`. |
| 3 | Both tabs opened side by side in the running app, both views. Full suite once, compared against a recorded baseline. |
| 4 | Not executed. |
