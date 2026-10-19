# Four-Factor Point Impact Display — Design

**Date:** 2026-07-07
**Scope:** Shiny Tab 1 (On/Off Four Factors view) + Tab 7 Compare (FF Swing view, Detail view FF sections)
**Status:** Approved

## Problem

Four-factor percentages are not comparable to each other: a 1pp change in eFG% is worth
more points than a 1pp change in FTR. Users reading FF diffs/gaps cannot tell which factor
actually drives a rating swing.

## Solution

Translate each factor delta into **estimated points per 100 possessions** using
league-calibrated regression weights, and display the translation as a clearly-labeled
per-factor annotation. **No summed "impact" column** — the sum merely reconstructs the
rating diff already displayed (the factors explain ~95% of rating variance); the new
information is the per-factor decomposition.

## Weights

Fit on `basketball_test.team_metrics_by_game_mv` (878 team-games, seasons 2025–2026),
weighted OLS of `off_ppp ~ off_efg + off_tov + off_oreb + off_ftr`, weights = `off_poss`.
R² = 0.948. Coefficients stable per season and under team×season + opponent fixed effects
(shift ≤ ~2%). Defense side is the mirror of offense — same weights apply.

| factor | pts/100 per 1pp |
|--------|-----------------|
| eFG%   | **+1.45** |
| TOV%   | **−1.36** |
| OREB%  | **+0.63** |
| FTR    | **+0.13** |

Pooled (non-FE) coefficients used, per user decision.

## Components

### 1. Core helper — `app/R/helpers.R`

```r
# Estimated points per 100 poss. per 1pp of each factor.
# Fit: weighted OLS on team_metrics_by_game_mv (878 team-games, 2025-26, R^2=0.948).
# Refit after future seasons with scripts/fit_ff_impact_weights.R.
FF_IMPACT_WEIGHTS <- c(efg = 1.45, tov = -1.36, oreb = 0.63, ftr = 0.13)

# Vectorized: delta (pp, natural column orientation) x factor name -> est. pts/100.
ff_impact_pts <- function(delta, factor)
```

- Pure function, lives in `helpers.R` so the test suite gets the real implementation
  (per test-mock convention).
- NA in → NA out. Unknown factor name → error (fail closed).
- Sign convention: input deltas keep each column's natural orientation; output is the
  change in that side's rating. For defense columns the output is change in **points
  allowed** (negative = good). Good/bad color semantics stay with the existing
  cell-background gradients (offense normal, defense reversed) — unchanged.

### 2. Fit script — `scripts/fit_ff_impact_weights.R`

Standalone Rscript (reads `app/.Renviron`, readonly): pulls `team_metrics_by_game_mv`,
fits the weighted OLS, prints coefficients + per-season stability + FE robustness check.
Documents provenance; rerun after future seasons and update `FF_IMPACT_WEIGHTS` manually.

### 3. Tab 1 — Four Factors view (`app/R/server_tab1.R`)

- Each factor Diff cell (8 columns: Off/Def × eFG/OREB/TOV/FTR) gains a 4th line under
  the existing "on | off" sub-text: `est. +3.0 pts` — italic, muted color.
- Computed in the existing per-column JS render: R injects the column's weight constant
  via `sprintf`; JS multiplies the diff value and formats with explicit sign, 1 decimal.
  NULL/unranked diffs show no est. line.
- Header tooltips (existing `title=` mechanism on `sub-head` th): open with
  "Estimated points per 100 possessions this factor contributes…" including the weight.
- Muted caption under the table: "Estimated factor impact: points per 100 poss. from
  league-calibrated regression weights — an approximation, not a measured stat."

### 4. Tab 7 Compare (`app/R/server_tab7_compare.R`)

- **FF Swing view**: each factor row (`pvp_stat_row`) annotated with `est. ±X pts` per
  side (A and B), computed in R from each player's swing × weight. "Off Diff"/"Def Diff"
  rows (actual rating diffs) get no annotation. Existing caption line extended with the
  estimated-impact note.
- **Detail view** (`off_ff` / `def_ff` sections): each metric row's A−B gap annotated
  with `est. ±X pts`. Same caption note in those sections. Ratings/shooting sections
  unchanged.

### 5. Labeling requirement (user-critical)

The word **"est."** must be visible in every annotation; captions and header tooltips
lead with "Estimated". The annotation is styled as secondary text (italic, muted) so it
cannot be mistaken for a measured stat.

## Testing

`tests/testthat/` unit tests for `ff_impact_pts()`:
- known values (e.g. `ff_impact_pts(2.1, "efg") == 3.045` before display rounding)
- sign: TOV negative weight; defense semantics documented via test comment
- NA propagation; vectorization over mixed inputs; unknown factor errors

Tab 1/Tab 7 render regressions: existing tests updated only if they assert exact cell
HTML or column sets touched here.

## Out of scope

- Tabs 2/3/4 (lineups, teams, game logs), React frontend, Plumber API
- Any SQL function or MV change — this is pure R/JS display math
- Summed/aggregate impact columns (explicitly rejected: redundant with rating diffs)

## Risks / notes

- Weights are league+era specific; refit after each season (script provided). Values
  are hardcoded, not read from DB — a stale-weights drift of a few % is acceptable for
  an explicitly-estimated annotation.
- FTR weight (0.13) is honestly tiny — its est. line will read ~0.0-0.2 pts; that is
  the correct message, not a bug.
