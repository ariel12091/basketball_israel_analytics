# Four-Factor Point Impact Display Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Annotate every four-factor delta in Tab 1 and Tab 7 Compare with its estimated points-per-100-possessions impact, using league-calibrated regression weights.

**Architecture:** A pure helper (`FF_IMPACT_WEIGHTS` + `ff_impact_pts()`) in `app/R/helpers.R` is the single source of truth. Tab 1 renders the annotation inside the existing per-column DT JS render (weight injected from R via `sprintf`). Tab 7 computes annotations in R and adds them to the existing `pvp_stat_row` / detail-grid HTML builders. No SQL/MV/React changes.

**Tech Stack:** R 4.4.2, Shiny, DT (JS cell renders), testthat.

**Spec:** `docs/superpowers/specs/2026-07-07-ff-impact-display-design.md`

## Global Constraints

- Weights (pooled OLS, per spec): `efg = 1.45`, `tov = -1.36`, `oreb = 0.63`, `ftr = 0.13` pts/100 per 1pp.
- The word **"est."** must be visible in every annotation; captions/tooltips lead with "Estimated". Annotations are italic + muted (secondary text).
- Defense annotations read "pts allowed" (Tab 1, swing view); detail-view gap annotations are magnitude-only ("est. X.X pts") because direction is already encoded by the gap bar/colors.
- No summed impact column anywhere.
- 2-space indent, snake_case. Parameterized SQL only (no SQL in this plan anyway).
- Pure helpers go in `app/R/helpers.R` (no side effects at source time); never copy an implementation into test mocks.
- **Commit hygiene:** the working tree carries unrelated uncommitted changes in `app/R/helpers.R`, `app/R/server_tab1.R`, and others (scalability branch work). NEVER `git add` a whole modified file blindly — see Task 0.

---

### Task 0: Preflight — working tree and branch

The current branch `shiny/scalability-and-cleanup` has uncommitted modifications that overlap files this plan touches (`app/R/helpers.R`, `app/R/server_tab1.R`). Committing whole files would sweep unrelated work into feature commits.

- [ ] **Step 1: Show the user the working-tree state and ask how to proceed**

Run: `git status --short` and show the output. Ask the user to choose:
1. User commits the pending scalability work first (preferred — then this plan's tasks can commit normally on a fresh `shiny/ff-impact` branch), or
2. Implement now but **skip every commit step in this plan**; the user will commit manually later.

Do not proceed until answered. If option 1: after the user commits, run `git checkout -b shiny/ff-impact`.

---

### Task 1: Core helper — weights, converter, tooltip text

**Files:**
- Modify: `app/R/helpers.R` (append at end of file)
- Test: `app/tests/testthat/test-ff-impact.R` (create)

**Interfaces:**
- Produces: `FF_IMPACT_WEIGHTS` (named numeric: efg/tov/oreb/ftr), `ff_impact_pts(delta, factor) -> numeric` (vectorized, NA-safe, errors on unknown factor), `ff_impact_tooltip(factor) -> character(1)` (header-tooltip sentence). All later tasks consume these.

- [ ] **Step 1: Write the failing test**

Create `app/tests/testthat/test-ff-impact.R`:

```r
# Unit tests for the four-factor point-impact helpers (helpers.R).
# Weights are league-calibrated (see scripts/fit_ff_impact_weights.R);
# these tests pin the published values and the conversion semantics.

test_that("FF_IMPACT_WEIGHTS holds the published league weights", {
  expect_equal(
    FF_IMPACT_WEIGHTS,
    c(efg = 1.45, tov = -1.36, oreb = 0.63, ftr = 0.13)
  )
})

test_that("ff_impact_pts converts factor deltas to est. pts/100", {
  expect_equal(ff_impact_pts(2.1, "efg"), 2.1 * 1.45)
  # TOV carries a negative weight: more turnovers -> fewer points.
  expect_equal(ff_impact_pts(1, "tov"), -1.36)
  expect_equal(ff_impact_pts(-1.5, "tov"), 2.04)
  expect_equal(ff_impact_pts(3, "oreb"), 1.89)
  expect_equal(ff_impact_pts(0.8, "ftr"), 0.104)
})

test_that("ff_impact_pts is vectorized over delta and factor", {
  expect_equal(
    ff_impact_pts(c(1, 1, 1, 1), c("efg", "tov", "oreb", "ftr")),
    c(1.45, -1.36, 0.63, 0.13)
  )
  expect_equal(ff_impact_pts(c(2, -2), "efg"), c(2.9, -2.9))
})

test_that("ff_impact_pts propagates NA and fails closed on unknown factor", {
  expect_true(is.na(ff_impact_pts(NA_real_, "efg")))
  expect_equal(ff_impact_pts(c(1, NA), "tov"), c(-1.36, NA_real_))
  expect_error(ff_impact_pts(1, "ts"), "Unknown four-factor")
})

test_that("ff_impact_tooltip leads with Estimated and includes the weight", {
  tip <- ff_impact_tooltip("efg")
  expect_match(tip, "^Estimated")
  expect_match(tip, "1.45", fixed = TRUE)
  expect_match(tip, "100 poss", fixed = TRUE)
  expect_error(ff_impact_tooltip("nope"), "Unknown four-factor")
})
```

Note on semantics (documented here, asserted implicitly by the sign tests): inputs are deltas in each column's **natural orientation**; the output is the change in that side's rating. For defense columns the output is change in **points allowed** (negative = good) — display code, not the helper, adds the "allowed" wording.

- [ ] **Step 2: Run test to verify it fails**

```bash
RSCRIPT="/c/Program Files/R/R-4.4.2/bin/Rscript.exe"
"$RSCRIPT" -e "testthat::test_dir('app/tests/testthat', filter = 'ff-impact')"
```

Expected: FAIL — `object 'FF_IMPACT_WEIGHTS' not found`.

- [ ] **Step 3: Write minimal implementation**

Append to `app/R/helpers.R`:

```r
# ---- Four-factor point impact ----
# Estimated points per 100 poss. contributed by a 1pp change in each factor.
# Fit: weighted OLS off_ppp ~ off_efg + off_tov + off_oreb + off_ftr on
# team_metrics_by_game_mv (878 team-games, 2025-26 seasons, weights = off_poss,
# R^2 = 0.948). Stable per season and under team/opponent fixed effects.
# Refit after future seasons with scripts/fit_ff_impact_weights.R and update here.
FF_IMPACT_WEIGHTS <- c(efg = 1.45, tov = -1.36, oreb = 0.63, ftr = 0.13)

# Convert factor deltas (percentage points, natural column orientation) to
# estimated pts/100. Vectorized over delta and factor; NA in -> NA out.
# Output is the change in that side's rating: for defense columns this is
# points ALLOWED (negative = good) -- callers add the "allowed" wording.
ff_impact_pts <- function(delta, factor) {
  bad <- setdiff(unique(as.character(factor)), names(FF_IMPACT_WEIGHTS))
  if (length(bad)) stop("Unknown four-factor name(s): ", paste(bad, collapse = ", "))
  unname(FF_IMPACT_WEIGHTS[as.character(factor)] * delta)
}

# Header-tooltip sentence explaining the est. annotation for one factor.
ff_impact_tooltip <- function(factor) {
  bad <- setdiff(unique(as.character(factor)), names(FF_IMPACT_WEIGHTS))
  if (length(bad)) stop("Unknown four-factor name(s): ", paste(bad, collapse = ", "))
  sprintf(
    "Estimated impact: each 1pp of this factor ≈ %+.2f pts per 100 poss. (league-calibrated regression weight; an approximation, not a measured stat).",
    FF_IMPACT_WEIGHTS[[as.character(factor)]]
  )
}
```

- [ ] **Step 4: Run test to verify it passes**

Same command as Step 2. Expected: all PASS.

- [ ] **Step 5: Verify the full helper-dependent suite still passes**

```bash
"$RSCRIPT" -e "testthat::test_dir('app/tests/testthat', filter = 'tab-parse|ff-impact')"
```

Expected: PASS (sanity that sourcing helpers.R still works everywhere).

- [ ] **Step 6: Commit** (skip if Task 0 chose option 2)

```bash
git add app/tests/testthat/test-ff-impact.R
git add app/R/helpers.R   # ONLY if Task 0 option 1 was chosen (file has no other pending hunks)
git commit -m "feat: add four-factor point-impact weights and converter"
```

---

### Task 2: Fit script (provenance / refit tool)

**Files:**
- Create: `scripts/fit_ff_impact_weights.R`

**Interfaces:**
- Consumes: nothing from other tasks (standalone).
- Produces: a script whose printed coefficients are the source of `FF_IMPACT_WEIGHTS`. No app code imports it.

- [ ] **Step 1: Write the script**

Create `scripts/fit_ff_impact_weights.R`:

```r
# Fit the four-factor -> ORtg regression that produces FF_IMPACT_WEIGHTS
# (app/R/helpers.R). Rerun after each season; update the constants manually.
#
# Usage:  "Rscript.exe" scripts/fit_ff_impact_weights.R
# Reads app/.Renviron (readonly credentials). Prints pooled coefficients,
# per-season stability, and a team/opponent fixed-effects robustness check.

read_renviron <- function(path) {
  lines <- readLines(path)
  lines <- lines[grepl("=", lines, fixed = TRUE) & !grepl("^\\s*#", lines)]
  kv <- do.call(rbind, strsplit(lines, "=", fixed = TRUE))
  setNames(trimws(kv[, 2]), trimws(kv[, 1]))
}

env <- read_renviron(file.path("app", ".Renviron"))

con <- DBI::dbConnect(
  RPostgres::Postgres(),
  host = env[["PG_HOST"]], port = as.integer(env[["PG_PORT"]]),
  dbname = env[["PG_DB"]], user = env[["PG_USER"]], password = env[["PG_PASS"]],
  sslmode = env[["PG_SSLMODE"]], connect_timeout = 15L, bigint = "numeric"
)

df <- DBI::dbGetQuery(con, "
  SELECT game_year, game_id, team_id, opp_team_id,
         off_ppp, off_efg, off_tov, off_oreb, off_ftr, off_poss
  FROM basketball_test.team_metrics_by_game_mv
  WHERE off_ppp IS NOT NULL AND off_efg IS NOT NULL AND off_tov IS NOT NULL
    AND off_oreb IS NOT NULL AND off_ftr IS NOT NULL
")
DBI::dbDisconnect(con)

cat("rows:", nrow(df), " seasons:", paste(sort(unique(df$game_year)), collapse = ", "), "\n\n")

m0 <- lm(off_ppp ~ off_efg + off_tov + off_oreb + off_ftr,
         data = df, weights = df$off_poss)
cat("=== Pooled OLS (weights source) ===\n")
print(round(coef(summary(m0)), 4))
cat("R-squared:", round(summary(m0)$r.squared, 4), "\n\n")

cat("=== Per-season stability ===\n")
for (yr in sort(unique(df$game_year))) {
  d <- df[df$game_year == yr, ]
  m <- lm(off_ppp ~ off_efg + off_tov + off_oreb + off_ftr, data = d, weights = d$off_poss)
  cat(yr, " n=", nrow(d), "  ",
      paste(sprintf("%s=%+.3f", names(coef(m))[-1], coef(m)[-1]), collapse = "  "), "\n", sep = "")
}

# Robustness: team x season + opponent fixed effects (coefficients should
# barely move; if they drift materially, investigate before updating weights).
df$team_season <- interaction(df$team_id, df$game_year)
df$opp_season <- interaction(df$opp_team_id, df$game_year)
m2 <- lm(off_ppp ~ off_efg + off_tov + off_oreb + off_ftr + team_season + opp_season,
         data = df, weights = df$off_poss)
cat("\n=== Fixed-effects check ===\n")
print(round(coef(m2)[c("off_efg", "off_tov", "off_oreb", "off_ftr")], 4))

cat("\nUpdate FF_IMPACT_WEIGHTS in app/R/helpers.R with the pooled coefficients (2dp).\n")
```

- [ ] **Step 2: Run it once to verify it reproduces the published weights**

```bash
"$RSCRIPT" scripts/fit_ff_impact_weights.R
```

Expected: pooled coefficients ≈ `off_efg 1.4511, off_tov -1.3600, off_oreb 0.6329, off_ftr 0.1269`, R² ≈ 0.948. (Values drift slightly if the ETL added games since 2026-07-07 — that is fine; the helper keeps the published 2dp values until deliberately refit.)

- [ ] **Step 3: Commit** (skip if Task 0 chose option 2)

```bash
git add scripts/fit_ff_impact_weights.R
git commit -m "feat: add refit script for four-factor impact weights"
```

---

### Task 3: Tab 1 — est. line in factor cells, header tooltips, caption, CSS

**Files:**
- Modify: `app/R/global.R` (shared_css — add `.ff-impact-est` rule)
- Modify: `app/R/server_tab1.R` (FF mode: `metric_map` loop ~line 964, sketch ~line 1061, `datatable(...)` call ~line 1078)

**Interfaces:**
- Consumes: `FF_IMPACT_WEIGHTS`, `ff_impact_tooltip(factor)` from Task 1.
- Produces: CSS class `ff-impact-est` (reused by Task 4/5), R constant `FF_METRIC_FACTOR` (local to server_tab1.R).

- [ ] **Step 1: Add the CSS rule**

In `app/R/global.R`, inside the `shared_css` string (append near the other table/cell rules; it is one big CSS string — add before its closing quote):

```css
/* Four-factor estimated point impact annotation (Tabs 1 & 7) */
.ff-impact-est {
  display: block;
  font-style: italic;
  font-size: .72rem;
  color: #8b949e;
  margin-top: 2px;
  white-space: nowrap;
}
```

- [ ] **Step 2: Add the factor lookup and inject weights into the JS render**

In `app/R/server_tab1.R`, directly after the `metric_map` definition (after line ~925), add:

```r
      # Factor key per FF diff column -> impact weight + defense wording.
      FF_METRIC_FACTOR <- c(
        "Off eFG% Diff" = "efg", "Off OREB% Diff" = "oreb",
        "Off TOV% Diff" = "tov", "Off FTR Diff" = "ftr",
        "Def eFG% Diff" = "efg", "Def OREB% Diff" = "oreb",
        "Def TOV% Diff" = "tov", "Def FTR Diff" = "ftr"
      )
```

Then modify the per-column JS build inside `for (i in seq_along(metric_map))` (~line 964). Before `js_func <- JS(sprintf(...))`, add:

```r
          impact_w <- FF_IMPACT_WEIGHTS[[FF_METRIC_FACTOR[[diff_name]]]]
          impact_suffix <- if (startsWith(diff_name, "Def")) " pts allowed" else " pts"
```

Replace the existing `js_func <- JS(sprintf(...))` block with (changes: two new leading JS lines computing `estLine`, `+ estLine` appended to BOTH return branches, two new sprintf args at the front):

```r
          js_func <- JS(sprintf(
            "function(data, type, row, meta) {
               if (type === 'display') {
                 var w = %f;
                 var estLine = '';
                 if (data !== null && data !== '' && !isNaN(parseFloat(data))) {
                   var est = parseFloat(data) * w;
                   estLine = '<div class=\"ff-impact-est\">est. ' +
                             (est >= 0 ? '+' : '\\u2212') + Math.abs(est).toFixed(1) +
                             '%s</div>';
                 }
                 var diffVal = (data === null) ? '-' : (parseFloat(data) > 0 ? '+' + data : data);
                 var onVal   = row[%d] || '-';
                 var offVal  = row[%d] || '-';
                 var onPct   = row[%d];
                 var offPct  = row[%d];

                 if (onPct === null || onPct === undefined) {
                    return '<div class=\"diff-val unranked\">' + diffVal + '</div>' +
                           '<div class=\"rank-bar-container hidden\"></div>' +
                           '<div class=\"sub-text\" style=\"opacity:0.5;\">' + onVal + ' | ' + offVal + '</div>' +
                           estLine;
                 }

                 var rangeLineLeft  = Math.min(onPct, offPct);
                 var rangeLineWidth = Math.abs(onPct - offPct);

                 return '<div class=\"diff-val\">' + diffVal + '</div>' +
                        '<div class=\"rank-bar-container\">' +
                          '<div class=\"rank-track\"></div>' +
                          '<div class=\"range-connect\" style=\"left:' + rangeLineLeft + '%%; width:' + rangeLineWidth + '%%;\"></div>' +
                          '<div class=\"dot-off\" style=\"left:' + offPct + '%%;\" title=\"Off: ' + offVal + '\"></div>' +
                          '<div class=\"dot-on\" style=\"left:' + onPct + '%%;\" title=\"On: ' + onVal + '\"></div>' +
                        '</div>' +
                        '<div class=\"sub-text\">' +
                          '<span style=\"font-weight:700; color:#222;\">' + onVal + '</span>' +
                          ' <span style=\"opacity:0.6;\">|</span> ' +
                          '<span style=\"color:#666;\">' + offVal + '</span>' +
                        '</div>' +
                        estLine;
               }
               return data;
             }", impact_w, impact_suffix, on_val_idx, off_val_idx, on_rank_idx, off_rank_idx
          ))
```

(Note the sprintf arg order: `impact_w`, `impact_suffix`, then the four existing indices. `%%` stays for literal `%` in CSS. `\\u2212` is a minus sign — R string escape for JS `−`.)

- [ ] **Step 3: Header tooltips on the 8 factor columns**

In the `sketch_ff` definition (~lines 1069-1075), give each factor `th` a title. Offense row entries become:

```r
          th(class = "sub-head section-left-border", "Diff"),
          th(class = "sub-head", title = ff_impact_tooltip("efg"), "eFG%"),
          th(class = "sub-head", title = paste0(OFF_OREB_TOOLTIP, " — ", ff_impact_tooltip("oreb")), "OREB%"),
          th(class = "sub-head", title = ff_impact_tooltip("tov"), "TOV%"),
          th(class = "sub-head", title = ff_impact_tooltip("ftr"), "FTR"),
```

and the defense row identically but with `DEF_OREB_TOOLTIP` in the OREB entry.

- [ ] **Step 4: Caption under the table (FF mode only)**

In the FF-mode `datatable(...)` call (~line 1078), add a `caption` argument after `escape = ...`:

```r
                      caption = htmltools::tags$caption(
                        style = "caption-side: bottom; text-align: center; font-size: .72rem; color: #8b949e; padding-top: 6px;",
                        "Estimated factor impact (est.): points per 100 poss. from league-calibrated regression weights — an approximation, not a measured stat."
                      ),
```

- [ ] **Step 5: Run the smoke/regression suites**

```bash
"$RSCRIPT" -e "testthat::test_dir('app/tests/testthat', filter = 'server-tabs-smoke|tab-parse|tab-wiring|ff-impact')"
```

Expected: PASS.

- [ ] **Step 6: Manual visual check**

```bash
"$RSCRIPT" -e "shiny::runApp('app')"
```

Tab 1 → view mode "Four Factors": every factor cell shows an italic muted 4th line like `est. +3.0 pts` (defense: `est. +2.0 pts allowed`), sign matches the diff line, FTR lines are near zero. Hover a factor header → tooltip starts with "Estimated impact:". Caption visible under the table. Unranked (gray) cells also show the est. line.

- [ ] **Step 7: Commit** (skip if Task 0 chose option 2)

```bash
git add app/R/server_tab1.R app/R/global.R
git commit -m "feat: show estimated point impact in Tab 1 four-factor cells"
```

---

### Task 4: Tab 7 — FF Swing view annotations

**Files:**
- Modify: `app/R/server_tab7_compare.R` (`FF_SWING_STATS` ~line 2036, `pvp_stat_row` ~line 1973, `render_ff_swing_ui` ~line 2049)

**Interfaces:**
- Consumes: `ff_impact_pts(delta, factor)` (Task 1), CSS class `ff-impact-est` (Task 3).
- Produces: `pvp_stat_row(label, va, vb, fmt_fn, higher_is_better = TRUE, sub_a = NULL, sub_b = NULL)` — extended signature, backward compatible (all existing callers unchanged).

- [ ] **Step 1: Tag each factor stat with its factor key**

Replace `FF_SWING_STATS` (~line 2036) with (only change: `factor =` fields; Diff rows get none):

```r
  FF_SWING_STATS <- list(
    list(label = "Off Diff", col = "Off ON Diff", side = "off"),
    list(label = "eFG%",     col = "Off eFG% Diff", side = "off", factor = "efg"),
    list(label = "OREB%",    col = "Off OREB% Diff", side = "off", factor = "oreb"),
    list(label = "TOV%",     col = "Off TOV% Diff", side = "off", invert = TRUE, factor = "tov"),
    list(label = "FTR",      col = "Off FTR Diff", side = "off", factor = "ftr"),
    list(label = "Def Diff", col = "Def ON Diff", side = "def", invert = TRUE),
    list(label = "eFG%",     col = "Def eFG% Diff", side = "def", invert = TRUE, factor = "efg"),
    list(label = "OREB%",    col = "Def OREB% Diff", side = "def", invert = TRUE, factor = "oreb"),
    list(label = "TOV%",     col = "Def TOV% Diff", side = "def", factor = "tov"),
    list(label = "FTR",      col = "Def FTR Diff", side = "def", invert = TRUE, factor = "ftr")
  )
```

- [ ] **Step 2: Extend `pvp_stat_row` with optional per-side sub-lines**

Replace the `pvp_stat_row` function (~line 1973) with (changes: two new optional args; each value span is wrapped in a div that stacks the sub-line under it):

```r
  pvp_stat_row <- function(label, va, vb, fmt_fn, higher_is_better = TRUE,
                           sub_a = NULL, sub_b = NULL) {
    diff <- if (!is.na(va) && !is.na(vb)) abs(va - vb) else NA_real_
    if (higher_is_better) {
      a_better <- !is.na(va) && !is.na(vb) && va > vb
      b_better <- !is.na(va) && !is.na(vb) && vb > va
    } else {
      a_better <- !is.na(va) && !is.na(vb) && va < vb
      b_better <- !is.na(va) && !is.na(vb) && vb < va
    }
    diff_txt <- if (!is.na(diff) && diff > 0.05) sprintf("+%.1f", diff) else NULL
    left_badge <- if (a_better && !is.null(diff_txt)) tags$span(style = badge_css, diff_txt) else NULL
    right_badge <- if (b_better && !is.null(diff_txt)) tags$span(style = badge_css, diff_txt) else NULL

    tags$div(
      style = "display: flex; align-items: center; padding: 10px 0; border-bottom: 1px solid rgba(255,255,255,.06);",
      tags$div(
        style = "flex: 1; display: flex; align-items: center; justify-content: flex-end; gap: 10px;",
        left_badge,
        tags$div(
          style = "text-align: right;",
          tags$span(style = if (a_better) val_win_css else val_lose_css, fmt_fn(va)),
          sub_a
        )
      ),
      tags$div(
        style = "width: 130px; text-align: center; font-size: .85rem; font-weight: 600; color: #8b949e;",
        label
      ),
      tags$div(
        style = "flex: 1; display: flex; align-items: center; justify-content: flex-start; gap: 10px;",
        tags$div(
          style = "text-align: left;",
          tags$span(style = if (b_better) val_win_css else val_lose_css, fmt_fn(vb)),
          sub_b
        ),
        right_badge
      )
    )
  }
```

- [ ] **Step 3: Compute the est. sub-lines in `render_ff_swing_ui`**

Inside `render_ff_swing_ui` (~line 2049), after the `fmt_swing` definition (~line 2086), add:

```r
    est_sub <- function(v, stat) {
      if (is.null(stat$factor) || is.na(v)) return(NULL)
      est <- ff_impact_pts(v, stat$factor)
      suffix <- if (identical(stat$side, "def")) " pts allowed" else " pts"
      tags$div(class = "ff-impact-est", sprintf("est. %+.1f%s", est, suffix))
    }
```

In `make_rows` (~line 2098), pass the sub-lines through:

```r
    make_rows <- function(stats) {
      lapply(stats, function(stat) {
        va <- get_swing(row_a, onoff_a, stat)
        vb <- get_swing(row_b, onoff_b, stat)
        higher_is_better <- !isTRUE(stat$invert)
        pvp_stat_row(stat$label, va, vb, fmt_swing, higher_is_better,
                     sub_a = est_sub(va, stat), sub_b = est_sub(vb, stat))
      })
    }
```

Note: `stat$factor` on the Diff rows is absent → `stat$factor` returns `NULL` → `est_sub` returns `NULL`. No annotation on rating-diff rows (per spec: no summed/duplicate impact).

- [ ] **Step 4: Extend the caption line**

Replace the existing caption text (~line 2116):

```r
          "Swing values use the same diffs as Tab 1 Four Factors (plus Off/Def Diff). Estimated factor impact (est.): points per 100 poss. from league-calibrated regression weights — an approximation, not a measured stat."
```

- [ ] **Step 5: Run tests**

```bash
"$RSCRIPT" -e "testthat::test_dir('app/tests/testthat', filter = 'tab7|server-tabs-smoke|ff-impact')"
```

Expected: PASS. If `test-tab7-compare-server.R` asserts `pvp_stat_row` HTML or arity, update the assertion to the new optional-arg signature (backward compatible — existing 5-arg calls must still pass).

- [ ] **Step 6: Manual visual check**

Run the app → Compare tab → Players mode → FF Swing view: each factor row shows `est. ±X pts` (def rows: `pts allowed`) under both players' values; Off/Def Diff rows have no annotation; caption mentions "Estimated factor impact".

- [ ] **Step 7: Commit** (skip if Task 0 chose option 2)

```bash
git add app/R/server_tab7_compare.R
git commit -m "feat: estimated point impact annotations in Compare FF swing view"
```

---

### Task 5: Tab 7 — Detail view gap annotations + footnote

**Files:**
- Modify: `app/R/server_tab7_compare.R` (`DETAIL_METRICS` ~line 113-130, detail render loop ~line 3277-3295, `tagList` footer ~line 3299)

**Interfaces:**
- Consumes: `ff_impact_pts(delta, factor)` (Task 1), CSS class `ff-impact-est` (Task 3).
- Produces: nothing consumed later.

- [ ] **Step 1: Tag the FF detail metrics with factor keys**

In `DETAIL_METRICS`, add `factor =` to the 8 FF entries (off_ff and def_ff sections only; ratings/shooting untouched):

```r
    off_ff = list(
      title = "Offensive Four Factors",
      metrics = list(
        list(label = "eFG%", col_ratings = NULL, col_ff = "off_efg", polarity = "higher", fmt = "pct", factor = "efg"),
        list(label = "TOV%", col_ratings = NULL, col_ff = "off_tov", polarity = "lower", fmt = "pct", factor = "tov"),
        list(label = "OREB%", col_ratings = NULL, col_ff = "off_oreb", polarity = "higher", fmt = "pct", factor = "oreb"),
        list(label = "FTR", col_ratings = NULL, col_ff = "off_ftr", polarity = "higher", fmt = "pct", factor = "ftr")
      )
    ),
    def_ff = list(
      title = "Defensive Four Factors",
      metrics = list(
        list(label = "Opp eFG%", col_ratings = NULL, col_ff = "def_efg", polarity = "lower", fmt = "pct", factor = "efg"),
        list(label = "Opp TOV%", col_ratings = NULL, col_ff = "def_tov", polarity = "higher", fmt = "pct", factor = "tov"),
        list(label = "Opp OREB%", col_ratings = NULL, col_ff = "def_oreb", polarity = "lower", fmt = "pct", factor = "oreb"),
        list(label = "Opp FTR", col_ratings = NULL, col_ff = "def_ftr", polarity = "lower", fmt = "pct", factor = "ftr")
      )
    ),
```

- [ ] **Step 2: Annotate the gap cell**

In the detail render loop, just before the `all_cells <- c(all_cells, list(...))` block that builds the three cells (~line 3277), add:

```r
        # Estimated point size of this factor gap. Magnitude-only: direction
        # is already encoded by the gap bar/colors (spec: detail view).
        est_span <- NULL
        if (!is.null(m$factor) && is.finite(va) && is.finite(vb)) {
          est_span <- tags$span(
            class = "ff-impact-est",
            sprintf("est. %.1f pts", abs(ff_impact_pts(va - vb, m$factor)))
          )
        }
```

Then in the middle (gap) cell of that block, insert `est_span` after the gap number span:

```r
          tags$div(class = paste0("cmp-gap-row cmp-col-gap cmp-cell", last_cls),
            `data-idx` = j - 1, `data-group` = sec_key,
            `data-default-idx` = j - 1,
            `data-gap` = if (is.finite(gi$gap)) round(gi$gap, 4) else 0,
            tags$span(class = paste("cmp-gap-num", gap_color_cls), gap_text),
            est_span,
            tags$div(class = "cmp-bar-container",
              tags$div(class = "cmp-bar-center"),
              if (bar_pct > 0) tags$div(class = paste("cmp-bar", bar_cls),
                style = sprintf("width: %.1f%%;", bar_pct)))),
```

(Metrics without `factor` — ratings, shooting — get `m$factor` = `NULL` → no annotation.)

- [ ] **Step 3: Footnote under the grid**

In the closing `tagList` (~line 3299), after the `tags$div(class = "cmp-compare-grid", ...)` element, add a sibling:

```r
        tags$div(
          style = "text-align: center; font-size: .72rem; color: #6e7681; margin-top: 8px;",
          "Estimated factor impact (est.): points per 100 poss. from league-calibrated regression weights — an approximation, not a measured stat."
        ),
```

- [ ] **Step 4: Run tests**

```bash
"$RSCRIPT" -e "testthat::test_dir('app/tests/testthat', filter = 'tab7|server-tabs-smoke|ff-impact')"
```

Expected: PASS (update `test-tab7-compare-contracts.R` only if it pins `DETAIL_METRICS` list shapes — adding a `factor` field must not break metric iteration).

- [ ] **Step 5: Manual visual check**

Run the app → Compare tab → Teams → click a team into Detail view: the 8 FF rows show `est. X.X pts` under the gap number; ratings/shooting rows unchanged; footnote below the grid; gap sorting (`data-gap`) still works (est. span must not break the JS sort, which reads the `data-gap` attribute, not text).

- [ ] **Step 6: Commit** (skip if Task 0 chose option 2)

```bash
git add app/R/server_tab7_compare.R
git commit -m "feat: estimated point impact annotations in Compare detail view"
```

---

### Task 6: Docs

**Files:**
- Modify: `CLAUDE.md` (Four Factors Metrics section)

**Interfaces:** none.

- [ ] **Step 1: Document the feature**

In `CLAUDE.md`, append to the "Four Factors Metrics" section:

```markdown
**Estimated point impact:** FF deltas in Tabs 1/7 carry an italic `est. ±X pts` annotation = delta × `FF_IMPACT_WEIGHTS` (helpers.R: efg 1.45, tov −1.36, oreb 0.63, ftr 0.13 pts/100 per pp; league-fit, refit via `scripts/fit_ff_impact_weights.R`). Defense wording: "pts allowed". No summed impact column — it would duplicate the rating diff.
```

- [ ] **Step 2: Commit** (skip if Task 0 chose option 2)

```bash
git add CLAUDE.md
git commit -m "docs: document four-factor estimated point impact"
```

---

## Self-Review Notes

- Spec coverage: helper+weights (Task 1), fit script (Task 2), Tab 1 cells/tooltips/caption (Task 3), swing view (Task 4), detail view (Task 5), "est." labeling in every annotation (Tasks 3-5), tests (Task 1 + suite runs in 3-5). No summed column anywhere. Out-of-scope items untouched.
- `ff_impact_pts` signature identical across Tasks 1/4/5; `ff-impact-est` class defined once (Task 3) and reused.
- Commit steps are conditional on Task 0's outcome because of pre-existing uncommitted hunks in shared files.
