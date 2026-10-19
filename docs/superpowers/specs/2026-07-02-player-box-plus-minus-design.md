# League-Calibrated Box Plus-Minus ("Revitalized Game Score")

**Date:** 2026-07-02
**Status:** Spec-reviewed 2026-07-02 (resolutions below); one open decision — RAPM/stint-adjusted target timing — before planning
**Related:** [Trends tab spec](2026-07-01-trends-tab-design.md) (this metric unblocks the Trends player headline)

## Problem

The Trends momentum board (tab 6) needs a **per-game player headline** that grades *the
player's own game*, not the team's performance while he was on court. On-court net rating /
plus-minus were rejected for the headline precisely because they measure the team, not the
individual.

Classic Hollinger Game Score is the obvious per-game individual grade, but its coefficients are
borrowed 1990s-NBA constants and its scoring term rewards inefficient volume. We want a
per-game grade whose weights reflect *what each box event is actually worth in this league*.

**Design intent (confirmed 2026-07-02):** the metric must **measure the impact of individual box
stats on team performance**. It decomposes team on-court net rating into per-stat individual
contributions: `value = Σ (player's box stat × that stat's league-average impact on team net
rtg)`. The player is credited only with *his own* box line × league-constant weights — so the
team's performance does not transfer to him wholesale (that is why raw on-court net rating was
rejected as the *headline* yet is the correct *regression target* here). Because the per-stat
impacts **are** the deliverable, the fitted coefficients' interpretability/identifiability is a
first-class requirement, not just predictive fit.

## Approach: Statistical / Box Plus-Minus (SPM/BPM)

Learn the weights instead of hand-picking them. Regress box-score production against on-court
plus-minus so each event's coefficient equals its empirical value in point differential. This is
the canonical BPM/SPM construction, calibrated to Israeli PBP data.

**Key property — additivity, not linearity.** The load-bearing constraint is that the fitted
model be **additively decomposable**: `value = Σ (per-stat contribution)`. Additivity is what
gives the offense/defense split for free and lets one fitted model apply at any granularity
(season / game / stint). Linearity is merely the *simplest* additive form. The true box→value
relationship is almost certainly nonlinear (usage–efficiency tradeoff, role interactions,
diminishing returns), so the **functional form is selected empirically** (see Step A) rather than
assumed linear — but always subject to the additive-decomposition constraint.

### Why not the alternatives

- **Classic Hollinger weights as-is** — keeps the volume-rewards flaw; not "ours."
- **Hand-tuned efficiency-anchored weights** — better than Hollinger but still subjective.
- **RAPM as the primary metric** — RAPM is a season-level impact estimate, cannot produce a
  per-game value, and is philosophically the on-court-impact metric that was rejected for the
  headline. It is scoped as a *future calibration target* (see Phasing), not the deliverable.

## The Math — Step A: Calibration (offline, infrequent)

1. Build **season-level per-100-possession box rates** per player from
   `df_pts_poss_lineups_longer_mv` (the same source `player_traditional_stats_mv` uses).
2. **Target:** on-court plus-minus per 100 possessions, season-aggregated per player, built from
   existing stint/lineup data.
3. **Functional-form selection (empirical, additivity required).** Fit and compare, by
   cross-validated error:
   - **Baseline — linear (ridge)** via glmnet, lambda by CV. Ridge is required regardless of the
     winning form — box stats are collinear (usage drives pts/fga/tov together), so raw OLS
     produces unstable / wrong-signed coefficients.
   - **Linear + engineered nonlinear features** — same linear-in-parameters fit with ratios
     (AST:FG / AST:TOV), squared terms, and targeted interactions (e.g. `fga×efficiency`) added
     as inputs. How real-world BPM handles curvature; still additive and decomposable.
   - **GAM (additive splines)** — `value = Σ fᵢ(rateᵢ)` (mgcv). Nonlinear per stat, still additive
     so decomposition + per-game application survive. More data-hungry.
   - **GBM / random forest** — fit **only as a reference ceiling** to quantify how much signal the
     additive forms leave behind. **Not eligible as the production model** — it breaks additive
     decomposition and can't score a stint.

   **Adopt the most flexible form that (a) beats the baseline on held-out error and (b) preserves
   additive decomposition.** On a thin single-league sample the baseline or the engineered-feature
   form usually wins; let the data decide.
4. **Collinearity + coefficient review (explicit step):** compute a collinearity diagnostic
   (VIF + correlation matrix) and inspect every fitted term for sign/magnitude sanity, comparing
   cross-validated fit across input variants. **Drop any term — counting or ratio — that is
   redundant (double-counts another input) or wrong-signed.** This enforces the "no double-counting"
   rule with evidence, not just a-priori judgment.
5. Store the fitted model in a small **`player_score_weights`** table, versioned by fit-date /
   season scope. For linear/engineered forms this is the coefficient vector; for a GAM it is the
   spline representation (knot/basis coefficients) needed to evaluate `fᵢ` at scoring time.
   **Not part of the ETL hot path** — the model is a league constant, refit occasionally (e.g.,
   once per season or on demand).

### Regression inputs

Action-type vocabulary confirmed against `df_pts_poss_longer.sql` (offense-side: `shot`,
`freeThrow`, `rebound`(off), `assist`, `turnover`, `foul-drawn`; defense-side: `steal`, `block`,
`deflection`, `foul`, `rebound`(def); with `parameters_made ∈ {made, missed, blocked}`).

**Core linear inputs — per-100-possession counting rates (`stat / poss × 100`):**

| # | Input | Side | Note |
|---|-------|------|------|
| 1 | `pts` | off | scoring output |
| 2 | `fga` | off | shot volume (expected negative — efficiency penalty) |
| 3 | `fgm` | off | makes |
| 4 | `3pm` | off | 3pt bonus over 2pt |
| 5 | `ast` | off | playmaking |
| 6 | `oreb` | off | offensive rebounding |
| 7 | `dreb` | def | defensive rebounding |
| 8 | `stl` | def | steals |
| 9 | `blk` | def | blocks (by the player) |
| 10 | `dfl` | def | deflections (available here; classic Game Score lacks it) |
| 11 | `tov` | off | turnovers (expected negative) |
| 12 | `foul-drawn` | off | fouls drawn — positive (draws FTs / possession value) |
| 13 | `PF` (`foul` committed) | def | personal fouls — expected negative |
| 14 | `blocked` (own shots blocked, `parameters_made='blocked'`) | off | expected negative (worse subset of misses) |

All 14 are uniform `stat/poss` rates — that shape is what lets one fitted vector apply cleanly at
season, game, and stint level and decompose offense vs defense. Ridge + the review step resolve
expected collinearities (`foul-drawn`↔`fta`, `blocked`↔`fga`).

**Experimental ratio inputs — fit, stabilize, keep only on evidence:**

| Candidate | Handling |
|-----------|----------|
| `AST:FG` (`ast/fgm`) | Creator-vs-finisher. Nonlinear (not `stat/poss`), unstable on small windows (÷ tiny/zero FGM), which degrades game/stint application. Fit with a stabilized form (min-FG floor / winsorized). Keep **only if** it materially improves cross-validated fit **and** can be applied at game level. |
| `AST:TOV` (`ast/tov`) | Decision-quality / ball-security. Same nonlinearity + stabilization + evidence bar as AST:FG. |

**Excluded a-priori as redundant (double-count the core linear block):**

- `TS%` — built from `pts`/`fga`/`fta`, all already core inputs; a nonlinear restatement of the
  scoring block.
- `Usage%` — collinear with the volume block (`fga`/`fta`/`tov`); a role descriptor, not
  incremental value.

## The Math — Step B: Scoring (per MV, cheap)

Apply the stored model to box rates (`fᵢ` is the per-stat contribution — `wᵢ·rateᵢ` for the
linear/engineered form, or the spline `fᵢ(rateᵢ)` for a GAM):

- **Rate** = Σ fᵢ(statᵢ per 100 poss) → minutes-neutral "how well he played." Grade basis.
- **Raw** = Rate × (poss_on_floor / 100) → volume-inclusive production total (VORP-style).
- Both derive from the *same* fitted model — the hybrid raw/rate output is free.
- **Offense/defense split:** sum offensive-term contributions vs defensive-term contributions →
  OBPM/DBPM-style breakdown at no extra cost (relies only on additivity).

## Outputs

### The grade

- Grade is the **Rate**, normalized against the season's qualified-player distribution
  (z-score → 0–100 percentile). High = good.
- **Displayed both ways:** the raw BPM-style number (e.g. `+4.2` = points/100 above average)
  *and* the 0–100 percentile (e.g. `87`).
- Minimum-possession floor to earn a grade; below the floor, show Raw only (mirrors the app's
  existing "unranked < 100 poss = gray" convention).

### Three granularities, one vector

| Level  | Output                                            | Use                                          |
|--------|---------------------------------------------------|----------------------------------------------|
| Season | Rate + Raw + grade                                | Leaderboard (Tab-5 style)                    |
| Game   | Rate + Raw + grade                                | **Trends headline** + per-game grade         |
| Stint  | Raw weighted box-value **+ team +/-** side-by-side | "Most productive stretch" drill-down (LAST)  |

**Stint caveat:** a stint is 2–6 minutes / a handful of possessions, so per-stint production is
lumpy. The league-relative grade is unstable at stint level — show **raw weighted score + a
within-game ranking**, not a league percentile. The team +/- column alongside is the insight:
divergence (produced a lot but team lost the minutes, or vice-versa) is what the drill-down
surfaces.

## Build Architecture

- **`player_game_box_mv`** — per-game box score: the existing `box_totals` CTE from
  `player_traditional_stats_mv` with `game_id` added to the grouping, plus the new counting
  columns (`foul-drawn`, `PF`, `blocked`).
- **Offline R calibration script** — builds season rates + PM target, fits the functional-form
  ladder (ridge / engineered-features / GAM, GBM as ceiling), runs the collinearity/coefficient
  review, selects the winning additive form, writes `player_score_weights`. Standalone (like
  `rebuild_all_mvs.R`), not in daily ETL.
- **`player_score_weights`** table — the fitted model, versioned (coefficient vector for
  linear/engineered forms; spline basis for a GAM).
- **`player_game_score_mv`** — applies weights → rate / raw / grade per player-game.
- **`player_score_rolling_mv`** — R3 / R5 / R10 rolling windows → feeds Trends (mirrors
  `team_metrics_rolling_mv`).
- **Trends integration** — player headline defaults to the **rolling Rate** (quality momentum),
  with Raw available as a toggle. Metric-aware polarity (high = good).

## Phasing

1. **v1** — calibration script + `player_score_weights` + game/season scoring MVs + grade.
   Unblocks the Trends player headline.
2. **v2** — per-stint side-by-side drill-down (his weighted box-value + team +/-).
3. **Future** — swap the calibration target from raw plus-minus → **RAPM** once that lane
   ships. Upgrades weight quality without changing any downstream MV.

## Gating Check (step 1 of implementation)

Count available **player-seasons** before finalizing. Fitting ~14–16 coefficients wants a few
hundred+ observations. If a single season is thin, **pool multiple seasons** for calibration —
the weights are league constants, not season-specific, so pooling is valid and preferred.

## Risks & Open Concerns

- **Sample size** — single-league data may be thin; mitigated by multi-season pooling and ridge
  regularization. Verified by the gating check.
- **Collinearity** — handled by ridge + the VIF/coefficient-review step.
- **Functional form / overfitting** — nonlinear forms (GAM, engineered interactions) can overfit a
  thin sample; guarded by cross-validated model selection and the additive-decomposition
  requirement, which rules out black-box models. If evidence is ambiguous, prefer the simpler form.
- **Defensive box under-capture** — steals/blocks/deflections/DREB poorly represent real defense;
  the DBPM component will be weak. Acknowledged limitation, not solved by box data alone (RAPM
  target upgrade helps later).
- **Ratio nonlinearity** — AST:FG / AST:TOV break the additive/multi-granularity property; only
  admitted if they clear the evidence bar and are stabilized.
- **Stint sample tininess** — addressed by showing raw + within-game rank instead of a league
  grade at stint level.
- **Weight staleness** — weights are refit occasionally; document the refit cadence when the
  calibration script lands.

## Out of Scope (this spec)

- RAPM as a standalone metric / leaderboard (separate future spec; here it is only a future
  calibration target).
- React frontend integration (Trends is Shiny-only per the Trends spec).
