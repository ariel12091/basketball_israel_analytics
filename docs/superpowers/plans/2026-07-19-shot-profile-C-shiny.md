# Shot Profile Shiny Display Mode (Plan C) Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add a descriptive "Shot Profile" display mode (lay-up / dunk / rim / 3PA / corner-3-of-3PA / mid shares of FGA) to Shiny Tab 1 (On/Off), Tab 3 (Team Ratings), and Tab 7 (Compare: Teams detail + Players view), on both fast (MV) and filtered (SQL fn) paths.

**Architecture:** Plan B already shipped all data: `onoff_default_mv` / `onoff_compute` expose 28 player columns (`{off,def}_{on,off}_{layup,dunk,c3}_{made,att}` + `{off,def}_{on,off}_c3_known_att`), and `team_ppp_ratings_mv` / `get_team_ratings_dynamic` expose 12 team columns (`{off,def}_{fga,layup_att,dunk_att,fg3_att,c3_att,c3_known_att}`). This plan is R/Shiny-only: one pure share-math helper in `R/helpers.R`, then a render branch per tab. No SQL, no ETL, no new cache keys.

**Tech Stack:** R 4.4.2, Shiny (bslib BS5), DT with JS `columnDefs` renders, testthat (`app/tests/testthat`).

**Branch:** `shiny/shot-profile` (from current `main`). Merge with `superpowers:finishing-a-development-branch` when done.

## Global Constraints

- **No `est. ±pts`, no impact framing anywhere in Shot Profile** (spec non-goal 1 — shot-generation impact is ~90% eFG-mediated; a separate number double-counts the FF eFG annotation).
- **Corner share = `c3_att / c3_known_att`, NEVER `/ fg3_att`.** Unknown corner flag (`c3_known_att == 0` or NA inputs) renders `—`, never 0 (fail-open display rule).
- Lay-up and dunk stay **separate** columns; rim = layup + dunk; `mid = fga − rim − fg3a` and above-break are derived in R, not stored.
- **Neutral rendering** for shares: no green/red good/bad backgrounds (matches existing ShotCell convention where *frequencies* use neutral colors and only *accuracy* is red-green; also honors non-goal 1). Muting below 50 FGA as in Summary.
- All cell formatting in JS `columnDefs` renders with the guard `if (type !== 'display' || !row) return data;` — never `formatRound` on JS-rendered columns.
- Parameterized SQL only; the single fast-path SQL change is adding 12 column names to an existing SELECT list (Task 3).
- Pure helpers go in `app/R/helpers.R` (shared with the test suite). **Never copy helper implementations into `helper-server-mocks.R`** — it `source()`s the real helpers.
- R style: 2-space indent, snake_case, base `lapply`/`vapply` (no purrr), no long copy-pasted if-chains — drive from vectors/maps.
- Do **NOT** modify `CLAUDE.md` (AGENTS.md: historical reference). Do **NOT** commit `PROJECT.md`, `scripts/fit_ff_impact_weights.R`, or `app/rsconnect/.../onoff-shiny.dcf` (user WIP in working tree).
- Test runner: `"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_dir('app/tests/testthat')"` (single file: `testthat::test_file('app/tests/testthat/test-XXX.R')`). Run from repo root.

---

### Task 1: Share-math helpers in `helpers.R` (pure, TDD)

**Files:**
- Modify: `app/R/helpers.R` (insert directly after `add_shot_split_metrics`, which ends ~line 208)
- Create: `app/tests/testthat/test-shot-profile-helpers.R`

**Interfaces:**
- Produces: `shot_profile_metric_cols(label_prefix, col_prefix)` → named chr vector mapping display labels to `{col_prefix}{suffix}` column names; `add_shot_profile_metrics(df, specs)` where `specs` is a named list `prefix -> c(layup_att_col, dunk_att_col, fga_col, fg3_att_col, c3_att_col, c3_known_att_col)`; adds columns `{prefix}_layup_share`, `{prefix}_dunk_share`, `{prefix}_rim_share`, `{prefix}_fg3_share`, `{prefix}_mid_share`, `{prefix}_c3_pct3`, `{prefix}_fga` (all numeric, shares ×100 rounded 1dp, NA when denominator is 0). **Note: 3rd spec col is TOTAL FGA**, not fg2_att — Tab 1 callers must precompute `fga = fg2_att + fg3_att` first; the team MV has `off_fga`/`def_fga` natively.
- Consumed by: Tasks 2, 3, 4, 5.

- [ ] **Step 1: Write the failing test**

```r
# app/tests/testthat/test-shot-profile-helpers.R
# Pure share-math contracts for the Shot Profile display mode (Plan C).
# helpers.R is sourced by helper-server-mocks.R, so the real implementations run.

test_that("shot_profile_metric_cols maps labels to suffixed column names", {
  cols <- shot_profile_metric_cols("Off", "off_on")
  expect_equal(unname(cols["Off Rim%"]), "off_on_rim_share")
  expect_equal(unname(cols["Off C3% of 3PA"]), "off_on_c3_pct3")
  expect_length(cols, 6L)
})

test_that("add_shot_profile_metrics computes shares of FGA with mid as remainder", {
  df <- data.frame(
    la = c(20, 0), du = c(5, 0), fga = c(100, 80), f3 = c(40, 30),
    c3 = c(10, 0), c3k = c(35, 0)
  )
  out <- add_shot_profile_metrics(df, list(p = c("la", "du", "fga", "f3", "c3", "c3k")))
  expect_equal(out$p_layup_share, c(20, 0))
  expect_equal(out$p_dunk_share, c(5, 0))
  expect_equal(out$p_rim_share, c(25, 0))
  expect_equal(out$p_fg3_share, c(40, 37.5))
  expect_equal(out$p_mid_share, c(35, 62.5))
  # rim + fg3 + mid partitions FGA
  expect_equal(out$p_rim_share + out$p_fg3_share + out$p_mid_share, c(100, 100))
  expect_equal(out$p_fga, c(100, 80))
})

test_that("corner share divides by known 3PA and fails open to NA", {
  df <- data.frame(la = 1, du = 0, fga = 50, f3 = 20, c3 = 6, c3k = 15)
  out <- add_shot_profile_metrics(df, list(p = c("la", "du", "fga", "f3", "c3", "c3k")))
  expect_equal(out$p_c3_pct3, 40)  # 6/15, NOT 6/20
  df0 <- data.frame(la = 1, du = 0, fga = 50, f3 = 20, c3 = 0, c3k = 0)
  out0 <- add_shot_profile_metrics(df0, list(p = c("la", "du", "fga", "f3", "c3", "c3k")))
  expect_true(is.na(out0$p_c3_pct3))  # unknown != 0%
})

test_that("zero FGA yields NA shares and NA counts are treated as 0", {
  df <- data.frame(la = c(0, NA), du = c(0, 2), fga = c(0, 10), f3 = c(0, 4), c3 = c(0, 1), c3k = c(0, 3))
  out <- add_shot_profile_metrics(df, list(p = c("la", "du", "fga", "f3", "c3", "c3k")))
  expect_true(is.na(out$p_rim_share[1]))
  expect_true(is.na(out$p_mid_share[1]))
  expect_equal(out$p_rim_share[2], 20)  # NA layup -> 0, dunk 2 of 10
})

test_that("missing spec columns leave df untouched", {
  df <- data.frame(x = 1)
  out <- add_shot_profile_metrics(df, list(p = c("a", "b", "c", "d", "e", "f")))
  expect_identical(names(out), "x")
  expect_identical(add_shot_profile_metrics(NULL, list()), NULL)
})
```

- [ ] **Step 2: Run test to verify it fails**

Run: `"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('app/tests/testthat/test-shot-profile-helpers.R')"`
Expected: FAIL — `could not find function "shot_profile_metric_cols"`.

- [ ] **Step 3: Implement in `app/R/helpers.R`** (after `add_shot_split_metrics`)

```r
# ---- Shot Profile (shot-diet) share metrics ---------------------------------
# Descriptive shares of total FGA (Plan C). Corner-3 share is of KNOWN-location
# 3PA (c3_known_att), never of all 3PA — unknown fails open to NA, not 0.

SHOT_PROFILE_METRIC_SUFFIXES <- c(
  "_layup_share", "_dunk_share", "_rim_share", "_fg3_share", "_c3_pct3", "_mid_share"
)
SHOT_PROFILE_METRIC_LABELS <- c("Lay-up%", "Dunk%", "Rim%", "3PA%", "C3% of 3PA", "Mid%")

shot_profile_metric_cols <- function(label_prefix, col_prefix) {
  stats::setNames(
    paste0(col_prefix, SHOT_PROFILE_METRIC_SUFFIXES),
    paste(label_prefix, SHOT_PROFILE_METRIC_LABELS)
  )
}

add_shot_profile_metrics <- function(df, specs) {
  if (is.null(df) || !length(specs)) return(df)

  pct <- function(num, den) {
    out <- rep(NA_real_, length(den))
    ok <- is.finite(den) & den > 0
    out[ok] <- round(num[ok] / den[ok] * 100, 1)
    out
  }
  count_col <- function(col) {
    x <- suppressWarnings(as.numeric(df[[col]]))
    x[is.na(x)] <- 0
    x
  }

  for (prefix in names(specs)) {
    cols <- specs[[prefix]]
    if (length(cols) != 6L || !all(cols %in% names(df))) next

    layup <- count_col(cols[[1]])
    dunk  <- count_col(cols[[2]])
    fga   <- count_col(cols[[3]])
    fg3a  <- count_col(cols[[4]])
    c3a   <- count_col(cols[[5]])
    c3k   <- count_col(cols[[6]])
    rim   <- layup + dunk

    df[[paste0(prefix, "_layup_share")]] <- pct(layup, fga)
    df[[paste0(prefix, "_dunk_share")]]  <- pct(dunk, fga)
    df[[paste0(prefix, "_rim_share")]]   <- pct(rim, fga)
    df[[paste0(prefix, "_fg3_share")]]   <- pct(fg3a, fga)
    df[[paste0(prefix, "_mid_share")]]   <- pct(pmax(fga - rim - fg3a, 0), fga)
    df[[paste0(prefix, "_c3_pct3")]]     <- pct(c3a, c3k)
    df[[paste0(prefix, "_fga")]]         <- fga
  }
  df
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('app/tests/testthat/test-shot-profile-helpers.R')"`
Expected: PASS (all).

- [ ] **Step 5: Commit**

```bash
git add app/R/helpers.R app/tests/testthat/test-shot-profile-helpers.R
git commit -m "feat(shiny): shot-profile share helpers"
```

---

### Task 2: Tab 1 — Shot Profile view mode

**Files:**
- Modify: `app/R/ui_tab1_onoff.R` (radio ~line 14-17; explainer/legend conditionalPanels ~lines 94-234)
- Modify: `app/R/server_tab1.R` (filterable-col constants top of file; `on_stat_filter_cols` ~line 53; renderDT `output$onoff_dt` ~line 692)
- Modify: `app/tests/testthat/helper-server-mocks.R` (mock `onoff_default_mv` frame ~line 84)
- Modify: `app/tests/testthat/test-primary-table-render-smoke.R` (tab-1 block ~line 196)

**Interfaces:**
- Consumes: `add_shot_profile_metrics` / `shot_profile_metric_cols` (Task 1); existing `mv_result_df()` / `live_result_df()` (both already return the 28 Plan-B columns via `SELECT *`).
- Produces: display columns named `"Off <L> Diff"` / `"Def <L> Diff"` for `<L>` in `Lay-up, Dunk, Rim, 3PA, C3, Mid` (numeric Δ in percentage points, ON − OFF).

**Data-flow note (no data changes needed):** `result_df()`'s mode switch treats anything ≠ "Four Factors" as the Summary data path, so "Shot Profile" automatically gets the same MV/fallback logic, min-poss filters, and auto-min behavior (`resolve_poss_cols` Summary branch uses `"ON Poss"`). Do not touch those reactives.

- [ ] **Step 1: UI — add the mode and its explainer/legend** (`app/R/ui_tab1_onoff.R`)

Radio (line ~15): `choices = c("Summary", "Four Factors", "Shot Profile")`.

After the Four Factors explainer `conditionalPanel` (ends ~line 182), add:

```r
        conditionalPanel(
          condition = "input.onoff_view_mode == 'Shot Profile'",
          tab_explainer(
            id = "onoff_explainer_sp",
            title = "What This Tab Answers (Shot Profile)",
            intro = "How does the team's shot diet shift with the player on vs off the floor? Each cell shows the ON-minus-OFF change in share of team FGA, with ON | OFF values below.",
            bullets = c(
              "Shares are descriptive — they describe how the shot mix changes, not how good the change is. Efficiency lives in the Summary and Four Factors views.",
              "Rim = lay-ups + dunks (tag-based). Mid = everything else inside the arc. 3PA% is share of all FGA.",
              "C3% of 3PA splits threes into corner vs above-break, using shots with known court location; — means location unknown.",
              "Cells gray out below 50 team FGA on the ON side — small samples produce noisy shares."
            )
          )
        ),
```

After the Four Factors legend `conditionalPanel` (ends ~line 234), add:

```r
        conditionalPanel(
          condition = "input.onoff_view_mode == 'Shot Profile'",
          div(
            class = "legend-box",
            span(style = "font-weight:700; margin-right:10px;", "Shot Profile:"),
            span(style = "font-size:0.85em; color:#6e7681;",
                 "Share of team FGA while the player is on/off the floor · Δ = ON − OFF (percentage points) · C3% is share of 3PA with known location (— = unknown) · descriptive only — no point-impact estimate")
          )
        ),
```

- [ ] **Step 2: Server — filterable columns** (`app/R/server_tab1.R`)

After `ON_FF_FILTERABLE_COLS` (ends line 41) add:

```r
ON_SP_LABELS <- c("Lay-up", "Dunk", "Rim", "3PA", "C3", "Mid")

ON_SP_FILTERABLE_COLS <- c(
  stats::setNames(paste0("Off ", ON_SP_LABELS, " Diff"), paste("Off", ON_SP_LABELS, "Δ")),
  stats::setNames(paste0("Def ", ON_SP_LABELS, " Diff"), paste("Def", ON_SP_LABELS, "Δ")),
  "Min" = "minutes",
  "On Poss" = "ON Poss",
  "Off Poss" = "OFF Poss"
)
```

Replace `on_stat_filter_cols` body (line 53-55):

```r
  on_stat_filter_cols <- reactive({
    switch(input$onoff_view_mode %||% "Summary",
      "Four Factors" = ON_FF_FILTERABLE_COLS,
      "Shot Profile" = ON_SP_FILTERABLE_COLS,
      ON_SUMMARY_FILTERABLE_COLS
    )
  })
```

- [ ] **Step 3: Server — Shot Profile render branch** (`app/R/server_tab1.R`, inside `output$onoff_dt`)

The renderer currently branches `if (identical(mode, "Summary")) { ... } else { # FOUR FACTORS ... }`. Change the FF branch opener to `} else if (identical(mode, "Four Factors")) {` and append a final `} else { # SHOT PROFILE }` branch (after the FF branch's `return(dt)`, before the closing of the tryCatch-free renderer body):

```r
    } else {
      # === MODE 3: SHOT PROFILE (descriptive shot-diet shares) ===
      sp_prefixes <- c("off_on", "off_off", "def_on", "def_off")
      need_cols <- as.vector(outer(sp_prefixes, c("_layup_att", "_dunk_att", "_fg2_att", "_fg3_att", "_c3_att", "_c3_known_att"), paste0))
      if (!all(need_cols %in% names(df))) {
        return(DT::datatable(
          data.frame(Info = "Shot Profile columns unavailable for this dataset", check.names = FALSE),
          rownames = FALSE, options = list(dom = "t")
        ))
      }

      # Total FGA per split (helper takes total FGA, not fg2)
      for (p in sp_prefixes) {
        df[[paste0(p, "_fga_in")]] <- dplyr::coalesce(as.numeric(df[[paste0(p, "_fg2_att")]]), 0) +
          dplyr::coalesce(as.numeric(df[[paste0(p, "_fg3_att")]]), 0)
      }
      sp_specs <- stats::setNames(lapply(sp_prefixes, function(p) {
        paste0(p, c("_layup_att", "_dunk_att", "_fga_in", "_fg3_att", "_c3_att", "_c3_known_att"))
      }), sp_prefixes)
      df <- add_shot_profile_metrics(df, sp_specs)

      # Diff display columns: ON share - OFF share (pp), per side
      sp_metric_suffix <- c("layup_share", "dunk_share", "rim_share", "fg3_share", "c3_pct3", "mid_share")
      for (i in seq_along(sp_metric_suffix)) {
        m <- sp_metric_suffix[i]
        df[[paste0("Off ", ON_SP_LABELS[i], " Diff")]] <- round(df[[paste0("off_on_", m)]] - df[[paste0("off_off_", m)]], 1)
        df[[paste0("Def ", ON_SP_LABELS[i], " Diff")]] <- round(df[[paste0("def_on_", m)]] - df[[paste0("def_off_", m)]], 1)
      }

      if (!"minutes" %in% names(df)) df$minutes <- NA_real_
      sp_diff_cols <- c(paste0("Off ", ON_SP_LABELS, " Diff"), paste0("Def ", ON_SP_LABELS, " Diff"))
      sp_share_cols <- as.vector(outer(sp_prefixes, paste0("_", sp_metric_suffix), paste0))
      sp_fga_cols <- paste0(sp_prefixes, "_fga")
      keep_cols <- c("Team", "Player", sp_diff_cols, "minutes", "ON Poss", "OFF Poss",
                     sp_share_cols, sp_fga_cols)
      df_final <- df[, intersect(keep_cols, names(df))]
      df_final <- apply_stat_filters(df_final, on_stat_filter_state$filters())

      # JS render: signed diff headline + "ON | OFF" subtext; em-dash when NULL
      # (unknown corner); mute below 50 ON-side FGA.
      make_sp_render <- function(on_col, off_col, fga_col) {
        on_idx  <- which(names(df_final) == on_col) - 1L
        off_idx <- which(names(df_final) == off_col) - 1L
        fga_idx <- which(names(df_final) == fga_col) - 1L
        DT::JS(sprintf(
          "function(data, type, row, meta) {
             if (type !== 'display' || !row) return data;
             var onV = row[%d], offV = row[%d], fga = row[%d] || 0;
             if (data === null || onV === null || offV === null) {
               return '<div class=\"diff-val unranked\">—</div>';
             }
             var d = parseFloat(data);
             var head = (d > 0 ? '+' : '') + d.toFixed(1);
             var open = fga < 50 ? '<div style=\"opacity:0.45;\">' : '<div>';
             return open +
               '<div class=\"diff-val\">' + head + '</div>' +
               '<div class=\"sub-text\">' +
                 '<span style=\"font-weight:700;\">' + parseFloat(onV).toFixed(1) + '</span>' +
                 ' <span style=\"opacity:0.6;\">|</span> ' +
                 '<span style=\"color:#8b949e;\">' + parseFloat(offV).toFixed(1) + '</span>' +
               '</div></div>';
           }", on_idx, off_idx, fga_idx))
      }

      defs <- list()
      for (i in seq_along(sp_metric_suffix)) {
        m <- sp_metric_suffix[i]
        for (side in c("off", "def")) {
          disp <- paste0(ifelse(side == "off", "Off ", "Def "), ON_SP_LABELS[i], " Diff")
          tgt <- which(names(df_final) == disp) - 1L
          if (!length(tgt)) next
          defs[[length(defs) + 1L]] <- list(
            targets = tgt,
            render = make_sp_render(paste0(side, "_on_", m), paste0(side, "_off_", m), paste0(side, "_on_fga"))
          )
        }
      }
      hide_idx <- which(names(df_final) %in% c(sp_share_cols, sp_fga_cols)) - 1L
      if (length(hide_idx)) defs[[length(defs) + 1L]] <- list(targets = hide_idx, visible = FALSE)
      sec_idx <- which(names(df_final) %in% c("Off Lay-up Diff", "Def Lay-up Diff", "minutes")) - 1L
      if (length(sec_idx)) defs[[length(defs) + 1L]] <- list(targets = sec_idx, className = "section-left-border")
      defs[[length(defs) + 1L]] <- list(targets = "_all", className = "dt-center")

      c3_title <- "Corner 3s as % of 3PA with known court location; — = location unknown"
      sketch_sp <- htmltools::withTags(table(class = "display", thead(
        tr(
          th(class = "group-head", colspan = 2, ""),
          th(class = "group-head section-left-border", colspan = 6, "Offense Shot Diet (share of FGA, ON − OFF)"),
          th(class = "group-head section-left-border", colspan = 6, "Defense Shot Diet (share of FGA, ON − OFF)"),
          th(class = "group-head section-left-border", colspan = 3, "Usage")
        ),
        tr(
          th(class = "sub-head", "Team"), th(class = "sub-head", "Player"),
          th(class = "sub-head section-left-border", "Lay-up"), th(class = "sub-head", "Dunk"),
          th(class = "sub-head", "Rim"), th(class = "sub-head", "3PA"),
          th(class = "sub-head", title = c3_title, "C3%3PA"), th(class = "sub-head", "Mid"),
          th(class = "sub-head section-left-border", "Lay-up"), th(class = "sub-head", "Dunk"),
          th(class = "sub-head", "Rim"), th(class = "sub-head", "3PA"),
          th(class = "sub-head", title = c3_title, "C3%3PA"), th(class = "sub-head", "Mid"),
          th(class = "sub-head section-left-border", "Min"), th(class = "sub-head", "On Poss"), th(class = "sub-head", "Off Poss")
        )
      )))

      dt <- datatable(df_final, container = sketch_sp, rownames = FALSE,
                      options = list(headerCallback = HEADER_TOOLTIP_JS, dom = "tip",
                                     pageLength = 30, scrollX = TRUE,
                                     scrollY = "70vh", scrollCollapse = TRUE,
                                     order = list(list(which(names(df_final) == "Off Rim Diff") - 1L, "desc")),
                                     columnDefs = defs)) |>
        formatRound(intersect("minutes", names(df_final)), 1) |>
        formatCurrency(intersect(c("ON Poss", "OFF Poss"), names(df_final)),
                       currency = "", interval = 3, mark = ",", digits = 0)
      return(dt)
    }
```

Note: the Summary branch's `Player`/`Team` name-cleanup at the top of the renderer already runs for every mode — the Shot Profile branch relies on it. No `formatStyle` backgrounds (neutral rendering, Global Constraints).

- [ ] **Step 4: Extend the mock MV frame** (`app/tests/testthat/helper-server-mocks.R`)

In the `onoff_default_mv` mock data.frame (starts ~line 84), after the last `def_off_fg3_*` column, add 28 columns. Values must satisfy invariants `layup+dunk <= fg2_att` and `c3 <= c3_known <= fg3_att` against the existing mock counts (off_on fg2_att 160/150, fg3_att 100/94; off_off fg2 90/95, fg3 48/54; def_on fg2 150/155, fg3 92/98; def_off fg2 95/100, fg3 48/54):

```r
      off_on_layup_made = c(40L, 36L), off_on_layup_att = c(60L, 55L),
      off_on_dunk_made = c(9L, 7L), off_on_dunk_att = c(10L, 8L),
      off_on_c3_made = c(9L, 7L), off_on_c3_att = c(22L, 18L), off_on_c3_known_att = c(90L, 85L),
      off_off_layup_made = c(20L, 22L), off_off_layup_att = c(34L, 36L),
      off_off_dunk_made = c(3L, 4L), off_off_dunk_att = c(4L, 5L),
      off_off_c3_made = c(4L, 5L), off_off_c3_att = c(10L, 12L), off_off_c3_known_att = c(44L, 50L),
      def_on_layup_made = c(30L, 32L), def_on_layup_att = c(52L, 56L),
      def_on_dunk_made = c(5L, 6L), def_on_dunk_att = c(6L, 7L),
      def_on_c3_made = c(7L, 8L), def_on_c3_att = c(20L, 22L), def_on_c3_known_att = c(85L, 90L),
      def_off_layup_made = c(22L, 24L), def_off_layup_att = c(36L, 38L),
      def_off_dunk_made = c(2L, 3L), def_off_dunk_att = c(3L, 4L),
      def_off_c3_made = c(3L, 4L), def_off_c3_att = c(9L, 11L), def_off_c3_known_att = c(42L, 48L),
```

Also grep the mocks for an `onoff_compute` branch (`grepl("onoff_compute"`): if one exists, add the same 28 columns there with the same values so the fallback path stays column-complete.

- [ ] **Step 5: Extend the render smoke test** (`app/tests/testthat/test-primary-table-render-smoke.R`)

In the tab-1 `testServer` block (~line 197-207) append:

```r
    set_onoff_inputs(session, "Shot Profile")
    expect_silent(rendered <- output$onoff_dt)
    expect_primary_table_rendered(rendered)
```

- [ ] **Step 6: Run the touched test files**

Run: `"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('app/tests/testthat/test-primary-table-render-smoke.R'); testthat::test_file('app/tests/testthat/test-server-tabs-smoke.R'); testthat::test_file('app/tests/testthat/test-tab-parse.R')"`
Expected: PASS; the new Shot Profile smoke renders a data-shaped table (not "columns unavailable" — that message would trip the `no data` guard? No: assert manually that the rendered text does not contain "unavailable"; if it does, the mock columns are misnamed).

- [ ] **Step 7: Commit**

```bash
git add app/R/ui_tab1_onoff.R app/R/server_tab1.R app/tests/testthat/helper-server-mocks.R app/tests/testthat/test-primary-table-render-smoke.R
git commit -m "feat(shiny): tab 1 shot profile display mode"
```

---

### Task 3: Tab 3 — Shot Profile view mode

**Files:**
- Modify: `app/R/ui_tab3_team.R` (radio ~line 12-18; new explainer conditionalPanel after the FF one ~line 175)
- Modify: `app/R/server_tab3.R` (constants top; `tr_stat_filter_cols` ~line 65; `tr_data` fast-path SELECT ~line 969; renderDT `output$tr_table` mode chain ~line 1608/1839)
- Modify: `app/tests/testthat/helper-server-mocks.R` (mock `get_team_ratings_dynamic` frame ~line 161)
- Modify: `app/tests/testthat/test-primary-table-render-smoke.R` (tab-3 mode loop ~line 224)

**Interfaces:**
- Consumes: Task 1 helpers; `tr_data()` (both paths must now carry the 12 team count columns — the dynamic function already returns them via `SELECT *`; the fast path SELECT is extended here).
- Produces: display columns `off_layup`, `off_dunk`, `off_rim`, `off_fg3`, `off_c3`, `off_mid` (+ `def_` mirror) as HTML label cells, with numeric `sort__` twins.

- [ ] **Step 1: UI** (`app/R/ui_tab3_team.R`)

Radio: `choices = c("Summary", "Four Factors", "Shot Profile", "Traditional")`.

After the Four Factors explainer conditionalPanel (ends ~line 175), add:

```r
        conditionalPanel(
          condition = "input.tr_view_mode == 'Shot Profile'",
          tab_explainer(
            id = "team_explainer_sp",
            title = "What This Tab Answers (Shot Profile)",
            intro = "What does each team's shot diet look like, on offense and defense? Shares of FGA by shot type: lay-up, dunk, rim (lay-up + dunk), 3PA, corner-3 share of 3PA, and mid-range.",
            bullets = c(
              "Shares are descriptive — they describe the mix, not its quality. #1 means most of that shot type, not best.",
              "Defense columns are the shot diet teams allow their opponents.",
              "C3% of 3PA uses shots with known court location; — means location unknown.",
              "The same date/clutch-free filters apply as in Summary; use Poss columns to judge sample size."
            )
          )
        ),
```

- [ ] **Step 2: Server constants + filter cols** (`app/R/server_tab3.R`)

After `TR_FF_FILTERABLE_COLS` (ends line 32) add:

```r
TR_SP_FILTERABLE_COLS <- c(
  shot_profile_metric_cols("Off", "off"),
  "Off Poss" = "off_poss",
  shot_profile_metric_cols("Def", "def"),
  "Def Poss" = "def_poss",
  "Min" = "minutes"
)
```

In `tr_stat_filter_cols` (line 65-74) add a branch:

```r
    } else if (identical(mode, "Shot Profile")) {
      TR_SP_FILTERABLE_COLS
```

- [ ] **Step 3: Fast-path SELECT** (`app/R/server_tab3.R`, `tr_data` ~line 969)

Extend the MV query column list to:

```r
        "SELECT game_year, team_id, team_name, off_ppp, def_ppp, net_rtg,
                games_played, wins, losses, off_poss, def_poss,
                rank_net_rtg, rank_off_ppp, rank_def_ppp,
                off_fga, off_layup_att, off_dunk_att, off_fg3_att, off_c3_att, off_c3_known_att,
                def_fga, def_layup_att, def_dunk_att, def_fg3_att, def_c3_att, def_c3_known_att
           FROM basketball_test.team_ppp_ratings_mv
          WHERE game_year = $1::int4
          ORDER BY rank_net_rtg",
```

(Harmless for Summary mode; keeps one `tr_data` reactive for both — no new cache keys.)

- [ ] **Step 4: Render branch** (`app/R/server_tab3.R`, inside `output$tr_table`, insert `else if (identical(mode, "Shot Profile"))` between the Four Factors branch's `return(dt)` (~line 1837) and the final Summary `else`)

```r
    } else if (identical(mode, "Shot Profile")) {
      # ============================================================
      # SHOT PROFILE TEAM TABLE (descriptive shot-diet shares)
      # ============================================================
      df <- tr_data()
      if (is.null(df) || nrow(df) == 0) return(empty_dt("Shot Profile: no data for current filters"))
      need <- c("off_fga", "off_layup_att", "off_c3_known_att", "def_fga")
      if (!all(need %in% names(df))) return(empty_dt("Shot Profile columns unavailable"))
      df <- add_team_pace_cols(df, minutes_map = mins_map)
      df <- add_shot_profile_metrics(df, list(
        off = c("off_layup_att", "off_dunk_att", "off_fga", "off_fg3_att", "off_c3_att", "off_c3_known_att"),
        def = c("def_layup_att", "def_dunk_att", "def_fga", "def_fg3_att", "def_c3_att", "def_c3_known_att")
      ))
      df <- apply_stat_filters(df, tr_stat_filter_state$filters())
      if (is.null(df) || !nrow(df)) return(empty_dt("Shot Profile: no rows match stat filters"))

      # Rank = share magnitude order (descriptive: #1 = most of that shot type)
      sp_cols <- as.vector(outer(c("off", "def"), SHOT_PROFILE_METRIC_SUFFIXES, paste0))
      sp_disp <- gsub("_share$|_pct3$", "", sp_cols)  # off_layup, off_c3, ...
      fmt_share_cell <- function(vals, ranks) {
        v <- suppressWarnings(as.numeric(vals))
        r <- suppressWarnings(as.integer(ranks))
        ifelse(is.na(v), "—",
               paste0(format(round(v, 1), nsmall = 1, trim = TRUE), "%<br>",
                      ifelse(is.na(r), "#NA", paste0("#", r))))
      }

      df <- df %>% arrange(desc(off_rim_share))
      disp_sp <- data.frame(team_name = df$team_name, minutes = df$minutes, check.names = FALSE)
      for (i in seq_along(sp_cols)) {
        rk <- dplyr::min_rank(dplyr::desc(df[[sp_cols[i]]]))
        disp_sp[[sp_disp[i]]] <- fmt_share_cell(df[[sp_cols[i]]], rk)
      }
      disp_sp$off_poss <- df$off_poss
      disp_sp$def_poss <- df$def_poss
      # column order: team, min, off block + poss, def block + poss
      off_block <- sp_disp[startsWith(sp_disp, "off_")]
      def_block <- sp_disp[startsWith(sp_disp, "def_")]
      disp_sp <- disp_sp[, c("team_name", "minutes", off_block, "off_poss", def_block, "def_poss")]

      for (i in seq_along(sp_cols)) {
        vals <- suppressWarnings(as.numeric(df[[sp_cols[i]]]))
        vals[is.na(vals)] <- -Inf
        disp_sp[[paste0("sort__", sp_disp[i])]] <- vals
      }
      sp_sort_defs <- lapply(sp_disp, function(nm) {
        list(
          targets = which(names(disp_sp) == nm) - 1L,
          orderData = which(names(disp_sp) == paste0("sort__", nm)) - 1L,
          orderSequence = list("desc", "asc")
        )
      })

      c3_title <- "Corner 3s as % of 3PA with known court location; — = location unknown"
      sketch_sp <- htmltools::withTags(table(class = "display", thead(
        tr(
          th(class = "group-head", colspan = 2, ""),
          th(class = "group-head section-left-border", colspan = 7, "Offense Shot Diet (share of FGA)"),
          th(class = "group-head section-left-border", colspan = 7, "Defense Shot Diet (share of FGA)")
        ),
        tr(
          th(class = "sub-head", "Team"), th(class = "sub-head", "Min"),
          th(class = "sub-head section-left-border", "Lay-up"), th(class = "sub-head", "Dunk"),
          th(class = "sub-head", "Rim"), th(class = "sub-head", "3PA"),
          th(class = "sub-head", title = c3_title, "C3%3PA"), th(class = "sub-head", "Mid"),
          th(class = "sub-head", "Poss"),
          th(class = "sub-head section-left-border", "Lay-up"), th(class = "sub-head", "Dunk"),
          th(class = "sub-head", "Rim"), th(class = "sub-head", "3PA"),
          th(class = "sub-head", title = c3_title, "C3%3PA"), th(class = "sub-head", "Mid"),
          th(class = "sub-head", "Poss")
        )
      )))

      sp_hide_idx <- which(grepl("^sort__", names(disp_sp))) - 1L
      off_first_idx <- which(names(disp_sp) == "off_layup") - 1L
      def_first_idx <- which(names(disp_sp) == "def_layup") - 1L
      col_defs <- list(
        list(targets = sp_hide_idx, visible = FALSE),
        list(targets = "_all", className = "dt-center")
      )
      if (length(off_first_idx)) col_defs[[length(col_defs) + 1L]] <- list(targets = off_first_idx, className = "section-left-border dt-center")
      if (length(def_first_idx)) col_defs[[length(col_defs) + 1L]] <- list(targets = def_first_idx, className = "section-left-border dt-center")
      col_defs <- c(col_defs, sp_sort_defs)

      dt <- DT::datatable(disp_sp, container = sketch_sp, rownames = FALSE,
                          escape = dt_escape_except(disp_sp, sp_disp),
                          extensions = "Buttons",
                          options = list(
                            headerCallback = HEADER_TOOLTIP_JS,
                            dom = "Btip",
                            buttons = tr_csv_button(mode),
                            pageLength = 50, deferRender = TRUE, scrollX = TRUE,
                            scrollY = "70vh", scrollCollapse = TRUE,
                            order = list(list(which(names(disp_sp) == "off_rim") - 1L, "desc")),
                            columnDefs = col_defs
                          ))
      if ("minutes" %in% names(disp_sp)) dt <- DT::formatRound(dt, "minutes", 1)
      dt <- DT::formatCurrency(dt, intersect(c("off_poss", "def_poss"), names(disp_sp)),
                               currency = "", interval = 3, mark = ",", digits = 0)
      return(dt)
```

Check `dt_escape_except` signature matches the FF branch usage (`dt_escape_except(disp_ff, names(ff_sort_map))`) — pass the HTML label columns the same way. No `formatStyle` backgrounds.

- [ ] **Step 5: Mock + smoke** (`helper-server-mocks.R`, `test-primary-table-render-smoke.R`)

To the `get_team_ratings_dynamic` mock frame (~line 161) add:

```r
      off_fga = c(260L, 244L), off_layup_att = c(70L, 60L), off_dunk_att = c(12L, 9L),
      off_fg3_att = c(100L, 94L), off_c3_att = c(22L, 18L), off_c3_known_att = c(90L, 85L),
      def_fga = c(242L, 253L), def_layup_att = c(58L, 62L), def_dunk_att = c(7L, 8L),
      def_fg3_att = c(92L, 98L), def_c3_att = c(20L, 22L), def_c3_known_att = c(85L, 90L),
```

Also grep mocks for a `team_ppp_ratings_mv` branch; if present, add the same 12 columns there.

In `test-primary-table-render-smoke.R` change the tab-3 loop (~line 224) to:

```r
    for (mode in c("Summary", "Four Factors", "Shot Profile", "Traditional")) {
```

- [ ] **Step 6: Run touched tests**

Run: `"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('app/tests/testthat/test-primary-table-render-smoke.R'); testthat::test_file('app/tests/testthat/test-tab3-render-regressions.R'); testthat::test_file('app/tests/testthat/test-tab3-team-ratings-contracts.R')"`
Expected: PASS. (If tab3 contract tests assert the exact fast-path SELECT text, update the expected string to the extended column list — that is the intended change.)

- [ ] **Step 7: Commit**

```bash
git add app/R/ui_tab3_team.R app/R/server_tab3.R app/tests/testthat/helper-server-mocks.R app/tests/testthat/test-primary-table-render-smoke.R
git commit -m "feat(shiny): tab 3 shot profile display mode"
```

---

### Task 4: Tab 7 — Teams detail Shot Profile sections

**Files:**
- Modify: `app/R/server_tab7_compare.R` (`DETAIL_METRICS` ~line 103-151; `add_shooting_rates` area ~line 706; `cmp_detail_data` Teams branch ~line 3025-3053; `section_metrics` gating ~line 3236-3242)
- Modify: `app/tests/testthat/test-tab7-compare-contracts.R` (append contracts)

**Interfaces:**
- Consumes: Task 1 helper; `run_team_ratings()` rows (get_team_ratings_dynamic `SELECT *` — already includes the 12 count columns).
- Produces: derived columns `off_layup_share` … `def_c3_pct3` on the detail-view ratings rows; DETAIL_METRICS sections `off_shot_profile`, `def_shot_profile` (Teams mode only, polarity `"neutral"`, no `factor` field so no est.± renders).

- [ ] **Step 1: Add sections to `DETAIL_METRICS`** (after `def_shooting`, ~line 150; keep the trailing-comma structure valid)

```r
    ,
    # Teams-only descriptive shot-diet sections (gated in section_metrics).
    # polarity neutral: shares describe the mix, not quality — no winner, no est.±.
    off_shot_profile = list(
      title = "Offensive Shot Profile",
      metrics = list(
        list(label = "Lay-up%", col_ratings = "off_layup_share", col_ff = NULL, polarity = "neutral", fmt = "pct"),
        list(label = "Dunk%", col_ratings = "off_dunk_share", col_ff = NULL, polarity = "neutral", fmt = "pct"),
        list(label = "Rim%", col_ratings = "off_rim_share", col_ff = NULL, polarity = "neutral", fmt = "pct"),
        list(label = "3PA%", col_ratings = "off_fg3_share", col_ff = NULL, polarity = "neutral", fmt = "pct"),
        list(label = "C3% of 3PA", col_ratings = "off_c3_pct3", col_ff = NULL, polarity = "neutral", fmt = "pct"),
        list(label = "Mid%", col_ratings = "off_mid_share", col_ff = NULL, polarity = "neutral", fmt = "pct")
      )
    ),
    def_shot_profile = list(
      title = "Defensive Shot Profile",
      metrics = list(
        list(label = "Opp Lay-up%", col_ratings = "def_layup_share", col_ff = NULL, polarity = "neutral", fmt = "pct"),
        list(label = "Opp Dunk%", col_ratings = "def_dunk_share", col_ff = NULL, polarity = "neutral", fmt = "pct"),
        list(label = "Opp Rim%", col_ratings = "def_rim_share", col_ff = NULL, polarity = "neutral", fmt = "pct"),
        list(label = "Opp 3PA%", col_ratings = "def_fg3_share", col_ff = NULL, polarity = "neutral", fmt = "pct"),
        list(label = "Opp C3% of 3PA", col_ratings = "def_c3_pct3", col_ff = NULL, polarity = "neutral", fmt = "pct"),
        list(label = "Opp Mid%", col_ratings = "def_mid_share", col_ff = NULL, polarity = "neutral", fmt = "pct")
      )
    )
```

- [ ] **Step 2: Share derivation helper** (place next to `add_shooting_rates`, ~line 706)

```r
  add_team_shot_profile_shares <- function(row) {
    if (is.null(row) || !nrow(row)) return(row)
    add_shot_profile_metrics(row, list(
      off = c("off_layup_att", "off_dunk_att", "off_fga", "off_fg3_att", "off_c3_att", "off_c3_known_att"),
      def = c("def_layup_att", "def_dunk_att", "def_fga", "def_fg3_att", "def_c3_att", "def_c3_known_att")
    ))
  }
```

- [ ] **Step 3: Apply in `cmp_detail_data` Teams branch** (~line 3047-3048): change the two ratings assignments to

```r
        ratings_a = if (nrow(ra)) add_team_shot_profile_shares(ra[1, , drop = FALSE])[1, ] else NULL,
        ratings_b = if (nrow(rb)) add_team_shot_profile_shares(rb[1, , drop = FALSE])[1, ] else NULL,
```

- [ ] **Step 4: Gate to Teams mode** (`section_metrics`, ~line 3237): change the first line to

```r
      if (sk %in% c("def_shooting", "off_shot_profile", "def_shot_profile") && mode != "Teams") return(NULL)
```

(`detail_fmt` already renders non-finite as `—`, and `detail_compute_gap` already handles `polarity = "neutral"` — no winner, raw signed gap. No `factor` field means `est_span` stays NULL. Verify all three by reading those functions after editing.)

- [ ] **Step 5: Contract test** (append to `app/tests/testthat/test-tab7-compare-contracts.R`, following that file's existing style — if it parses source text, use this; otherwise adapt to its pattern)

```r
test_that("tab7 detail view registers Teams-only neutral shot-profile sections", {
  src <- paste(readLines(repo_file("R", "server_tab7_compare.R"), warn = FALSE), collapse = "\n")
  expect_true(grepl("off_shot_profile = list", src, fixed = TRUE))
  expect_true(grepl("def_shot_profile = list", src, fixed = TRUE))
  # Teams gating includes both new sections
  expect_true(grepl('c("def_shooting", "off_shot_profile", "def_shot_profile")', src, fixed = TRUE))
  # corner metric reads the known-denominator column, and no est/factor framing
  expect_true(grepl('col_ratings = "off_c3_pct3"', src, fixed = TRUE))
  sp_block <- sub('.*off_shot_profile = list', "", src)
  sp_block <- substr(sp_block, 1, regexpr("# --", sp_block, fixed = TRUE))
  expect_false(grepl("factor =", strsplit(sp_block, "PLAYER_VIEWS")[[1]][1], fixed = TRUE))
})
```

- [ ] **Step 6: Run tab7 tests**

Run: `"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('app/tests/testthat/test-tab7-compare-contracts.R'); testthat::test_file('app/tests/testthat/test-tab7-compare-server.R')"`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add app/R/server_tab7_compare.R app/tests/testthat/test-tab7-compare-contracts.R
git commit -m "feat(shiny): tab 7 teams detail shot-profile sections"
```

---

### Task 5: Tab 7 — Players "Shot Profile" view

**Files:**
- Modify: `app/R/server_tab7_compare.R` (`PLAYER_VIEWS` ~line 914-917; `pvp_stat_row` ~line 1975; new reactive next to `cmp_player_ff_raw` ~line 1948; new `render_shot_profile_ui` next to `render_ff_swing_ui` ~line 2154; dispatch in `cmp_pvp_ui` ~line 2167-2170)
- Modify: `app/tests/testthat/test-tab7-compare-contracts.R`

**Interfaces:**
- Consumes: `run_onoff_impact(p, team_csv)` (onoff_compute — includes the 28 player columns); `cmp_player_raw()`; `pvp_header` / `pvp_section_header` / `pvp_stat_row` / `cmp_player_state_card`; Task 1 helper.
- Produces: `PLAYER_VIEWS` entry `"Shot Profile" = "shot_profile"`; `pvp_stat_row(..., neutral = TRUE)` variant.

- [ ] **Step 1: Register the view**

```r
  PLAYER_VIEWS <- c(
    "Overall" = "overall",
    "Four Factors" = "ff_swing",
    "Shot Profile" = "shot_profile"
  )
```

(The chip renderer at ~line 1212 and the `cmp_player_view` observer at ~line 1302 both iterate `PLAYER_VIEWS` — no other registration needed.)

- [ ] **Step 2: Neutral variant of `pvp_stat_row`** (~line 1975). Add a `neutral = FALSE` arg; when neutral there is no winner/loser styling and no diff badge:

```r
  pvp_stat_row <- function(label, va, vb, fmt_fn, higher_is_better = TRUE,
                           sub_a = NULL, sub_b = NULL, neutral = FALSE) {
    diff <- if (!is.na(va) && !is.na(vb)) abs(va - vb) else NA_real_
    if (neutral) {
      a_better <- FALSE
      b_better <- FALSE
    } else if (higher_is_better) {
      a_better <- !is.na(va) && !is.na(vb) && va > vb
      b_better <- !is.na(va) && !is.na(vb) && vb > va
    } else {
      a_better <- !is.na(va) && !is.na(vb) && va < vb
      b_better <- !is.na(va) && !is.na(vb) && vb < va
    }
    diff_txt <- if (!neutral && !is.na(diff) && diff > 0.05) sprintf("+%.1f", diff) else NULL
    a_css <- if (neutral || a_better) val_win_css else val_lose_css
    b_css <- if (neutral || b_better) val_win_css else val_lose_css
```

…and use `a_css`/`b_css` in place of the two inline `if (a_better) val_win_css else val_lose_css` expressions in the body (rest of the function unchanged; `left_badge`/`right_badge` stay keyed off `diff_txt`, which is NULL when neutral).

- [ ] **Step 3: Data reactive** (after `cmp_player_ff_raw`, ~line 1948)

```r
  cmp_player_shot_raw <- reactive({
    req(identical(input$cmp_mode, "Players"))
    req(identical(selected_player_view(), "shot_profile"))
    data <- cmp_player_raw()
    req(data)

    onoff_a <- run_onoff_impact(data$pa, paste(data$team_ids_a, collapse = ","))
    onoff_b <- run_onoff_impact(data$pb, paste(data$team_ids_b, collapse = ","))
    on_a <- onoff_a[onoff_a$player_id == data$player_a_id_int, , drop = FALSE]
    on_b <- onoff_b[onoff_b$player_id == data$player_b_id_int, , drop = FALSE]
    if (!nrow(on_a) || !nrow(on_b)) return(NULL)

    list(
      onoff_a = on_a[1, , drop = FALSE], onoff_b = on_b[1, , drop = FALSE],
      name_a = data$name_a, name_b = data$name_b,
      team_a = data$team_a, team_b = data$team_b
    )
  })
```

- [ ] **Step 4: Renderer** (after `render_ff_swing_ui`, ~line 2154)

```r
  # -- Shot Profile view (descriptive shot-diet swing; no impact framing) --

  add_player_shot_profile_shares <- function(row) {
    if (is.null(row) || !nrow(row)) return(NULL)
    prefixes <- c("off_on", "off_off", "def_on", "def_off")
    need <- as.vector(outer(prefixes, c("_layup_att", "_dunk_att", "_fg2_att", "_fg3_att", "_c3_att", "_c3_known_att"), paste0))
    if (!all(need %in% names(row))) return(NULL)
    num0 <- function(col) {
      x <- suppressWarnings(as.numeric(row[[col]]))
      ifelse(is.na(x), 0, x)
    }
    for (p in prefixes) {
      row[[paste0(p, "_fga_in")]] <- num0(paste0(p, "_fg2_att")) + num0(paste0(p, "_fg3_att"))
    }
    add_shot_profile_metrics(row, stats::setNames(lapply(prefixes, function(p) {
      paste0(p, c("_layup_att", "_dunk_att", "_fga_in", "_fg3_att", "_c3_att", "_c3_known_att"))
    }), prefixes))
  }

  render_shot_profile_ui <- function() {
    trad_state <- cmp_player_raw_state()
    if (identical(trad_state$status, "pending")) {
      return(cmp_player_state_card("Preparing player compare..."))
    }
    data <- cmp_player_shot_raw()
    if (is.null(data)) {
      return(cmp_player_state_card("No player data for current filters."))
    }

    row_a <- add_player_shot_profile_shares(data$onoff_a)
    row_b <- add_player_shot_profile_shares(data$onoff_b)
    if (is.null(row_a) || is.null(row_b)) {
      return(cmp_player_state_card("Shot Profile columns unavailable for current filters."))
    }

    poss_a <- if ("ON Poss" %in% names(row_a)) as.numeric(row_a[["ON Poss"]]) else NA_real_
    poss_b <- if ("ON Poss" %in% names(row_b)) as.numeric(row_b[["ON Poss"]]) else NA_real_
    info_line <- function(poss, side) {
      parts <- character(0)
      if (is.finite(poss)) parts <- c(parts, paste0(round(poss), " ON Poss"))
      time_label <- player_side_time_label(side)
      if (nzchar(time_label)) parts <- c(parts, time_label)
      paste(parts, collapse = " · ")
    }

    sp_labels <- c("Lay-up%", "Dunk%", "Rim%", "3PA%", "C3% of 3PA", "Mid%")
    sp_suffix <- c("layup_share", "dunk_share", "rim_share", "fg3_share", "c3_pct3", "mid_share")

    swing <- function(row, side, m) {
      on_v <- suppressWarnings(as.numeric(row[[paste0(side, "_on_", m)]]))
      off_v <- suppressWarnings(as.numeric(row[[paste0(side, "_off_", m)]]))
      if (!is.finite(on_v) || !is.finite(off_v)) return(list(d = NA_real_, on = on_v, off = off_v))
      list(d = round(on_v - off_v, 1), on = on_v, off = off_v)
    }
    fmt_swing <- function(v) if (is.na(v)) "—" else sprintf("%+.1f", v)
    onoff_sub <- function(s) {
      if (!is.finite(s$on) || !is.finite(s$off)) return(NULL)
      tags$div(style = "font-size:.72rem; color:#6e7681;",
               sprintf("on %.1f | off %.1f", s$on, s$off))
    }

    make_rows <- function(side) {
      lapply(seq_along(sp_suffix), function(i) {
        sa <- swing(row_a, side, sp_suffix[i])
        sb <- swing(row_b, side, sp_suffix[i])
        pvp_stat_row(sp_labels[i], sa$d, sb$d, fmt_swing,
                     sub_a = onoff_sub(sa), sub_b = onoff_sub(sb), neutral = TRUE)
      })
    }

    tagList(
      pvp_header(
        data$name_a, data$team_a, info_line(poss_a, "a"),
        data$name_b, data$team_b, info_line(poss_b, "b")
      ),
      tags$div(
        style = "max-width: 520px; margin: 0 auto;",
        tags$div(
          style = "text-align: center; font-size: .72rem; color: #6e7681; margin-bottom: 8px;",
          "Team shot-diet shift with the player ON vs OFF the floor (share of team FGA, percentage points). Descriptive — no point-impact estimate. C3% is of 3PA with known location; — = unknown."
        ),
        pvp_section_header("Offensive Shot Diet (ON − OFF)"),
        do.call(tagList, make_rows("off")),
        pvp_section_header("Defensive Shot Diet (ON − OFF)"),
        do.call(tagList, make_rows("def"))
      )
    )
  }
```

- [ ] **Step 5: Dispatch** (in `output$cmp_pvp_ui`, after the `ff_swing` return ~line 2170)

```r
    if (identical(view, "shot_profile")) {
      return(render_shot_profile_ui())
    }
```

- [ ] **Step 6: Contract test** (append to `test-tab7-compare-contracts.R`)

```r
test_that("tab7 players mode registers the shot-profile view", {
  src <- paste(readLines(repo_file("R", "server_tab7_compare.R"), warn = FALSE), collapse = "\n")
  expect_true(grepl('"Shot Profile" = "shot_profile"', src, fixed = TRUE))
  expect_true(grepl("render_shot_profile_ui <- function", src, fixed = TRUE))
  expect_true(grepl('identical(view, "shot_profile")', src, fixed = TRUE))
  # neutral rows: the renderer must not use est/impact framing
  sp_fn <- sub(".*render_shot_profile_ui <- function", "", src)
  sp_fn <- strsplit(sp_fn, "# -- Overall PvP view --", fixed = TRUE)[[1]][1]
  expect_false(grepl("ff_impact", sp_fn, fixed = TRUE))
  expect_true(grepl("neutral = TRUE", sp_fn, fixed = TRUE))
})
```

- [ ] **Step 7: Run tab7 tests + full parse check**

Run: `"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "testthat::test_file('app/tests/testthat/test-tab7-compare-contracts.R'); testthat::test_file('app/tests/testthat/test-tab7-compare-server.R'); testthat::test_file('app/tests/testthat/test-tab-parse.R')"`
Expected: PASS.

- [ ] **Step 8: Commit**

```bash
git add app/R/server_tab7_compare.R app/tests/testthat/test-tab7-compare-contracts.R
git commit -m "feat(shiny): tab 7 players shot-profile view"
```

---

### Task 6: Full suite, live verification, docs

**Files:**
- Modify: `PROJECT.md` (append React-drift note — **working tree only, do NOT commit**; PROJECT.md is user WIP)
- No CLAUDE.md changes (AGENTS.md constraint).

- [ ] **Step 1: Run the whole suite**

Run: `"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" scripts/test_all.R`
Expected: 0 failures (the four known opt-in skips are fine).

- [ ] **Step 2: Live app verification against the real DB** (controller/inline — needs `app/.Renviron`)

Run `"/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e "shiny::runApp('app', port = 7788)"` in the background, then for each of Tab 1, Tab 3, Tab 7 (Teams detail + Players view) switch to Shot Profile and verify:
1. Tab 1: rows render; for one player, Off Rim Diff ≈ (ON rim share − OFF rim share) recomputed by hand from the MV row (`SELECT off_on_layup_att, off_on_dunk_att, off_on_fg2_att, off_on_fg3_att FROM basketball_test.onoff_default_mv WHERE "Year"=2026 LIMIT 1` etc.).
2. C3%3PA shows `—` (not 0) for at least the known coordless cases if any appear; league-wide most cells should be populated.
3. Tab 3: per-team rim+3PA+mid shares sum to ~100 (±0.2 rounding); offense C3% of 3PA is near the league ~9-10% corner share.
4. Tab 7 Teams detail shows the two new sections only in Teams mode (not Lineups); Players mode chip "Shot Profile" renders with no est.± anywhere.
5. Apply a date filter on Tabs 1/3 to force the SQL-function path and confirm the same columns populate (filtered/fast parity).

- [ ] **Step 3: PROJECT.md drift note (append at the end of PROJECT.md; leave uncommitted)**

```markdown
## Known drift: React frontend lacks Shot Profile (2026-07-19)

Shiny Tabs 1/3/7 gained a "Shot Profile" display mode (shot-diet shares incl.
corner-3-of-known-3PA). The React frontend (frontend-v2) intentionally does NOT
have this view yet — Plumber rename functions remain the contract point when it
is ported (spec: docs/superpowers/specs/2026-07-16-shot-profile-design.md §4/§7).
```

- [ ] **Step 4: Merge** — use `superpowers:finishing-a-development-branch`: merge `shiny/shot-profile` → `main`, push, delete branch. Verify `git status` shows PROJECT.md / fit_ff_impact_weights.R / onoff-shiny.dcf still uncommitted-modified (not swept into any commit).

---

## Self-review notes (spec coverage)

- Spec §5 Tab 1 columns: covered in Task 2 (lay-up/dunk/rim/3PA/C3-of-3PA/mid, ON/OFF shown as subtext with Δ headline, `—` for NULL corner, 50-FGA muting, JS-only formatting). *Deliberate interpretation:* "ON/OFF/Δ" renders as one FF-style cell per shot type (Δ headline + ON | OFF subtext) rather than 3 separate columns — matches the existing FF diff-cell idiom and keeps ~16 visible columns. *Deliberate deviation:* neutral coloring instead of green/red polarity gradient — value coloring would contradict spec non-goal 1 (no impact framing); existing ShotCell precedent colors frequencies neutrally. Flag both to the user at review.
- Spec §5 Tab 3: Task 3 (plain shares + rank labels + poss context, no Δ). Matchday rank-delta arrows (▲/▼) are NOT wired for this mode — SP cells show value + #rank only; descriptive mode, out of scope.
- Spec §5 Tab 7: Task 4 (Teams = tab-3-style shares as detail sections, A/B gap per compare contract) + Task 5 (Players = tab-1-style ON−OFF swing per side, A/B).
- Caches: no new cache keys (Tab 1 reuses `cached_season_df` pulls; Tab 3 extends the existing fast-path SELECT inside the same reactive; Tab 7 reuses `run_compare_query` keys).
- CSV: Tab 3 exports visible columns via the existing Buttons config. Tab 1's sidebar "Download CSV" button has **no server handler anywhere in the app** (pre-existing dead control, all modes) — out of scope; noted for the user.
- Tests: helper math (Task 1), render smoke both tabs (Tasks 2-3), tab7 contracts (Tasks 4-5), full suite + live parity check (Task 6). Mocks extended, never forked from helpers.
