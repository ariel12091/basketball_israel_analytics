# Four-Factor Point-Impact Research Memory

Date: 2026-07-20

Status: analysis completed for offensive rebounding; no production constants or
app behavior changed.

## Purpose

The app currently annotates four-factor gaps with coefficients from a weighted
team-game OLS regression:

`off_ppp ~ off_efg + off_tov + off_oreb + off_ftr`

The current hard-coded weights are:

- eFG: `+1.45`
- TOV: `-1.36`
- OREB: `+0.63`
- FTR: `+0.13`

Each coefficient is displayed as estimated points per 100 possessions for a
one-percentage-point factor gap. The source is `app/R/helpers.R`; the fitting
script is `scripts/fit_ff_impact_weights.R`.

This research asked whether OREB impact could instead be measured directly
from the play-by-play and what that implies for the four-factor methodology.

## Data scope and validation

The analysis used the populated `basketball_test` play-by-play and metric
tables for:

- 2024-25: 225 games, through 2025-06-12
- 2025-26: 221 games, through 2026-06-23
- Total: 446 games and 68,064 team possessions

The event-level extraction reconciled exactly to the dashboard aggregates:

- Rebound opportunities: `33,294`
- Offensive rebounds: `10,248`
- Points attributed to offensive-rebound continuations: `8,618`

No permanent analysis script was retained. Future work should turn the
validated query/model into a reproducible script before changing production.

## Exact OREB attribution definition

An offensive-rebound point is a point scored by the rebounding team after the
offensive rebound and before that rebound continuation ends.

For multiple offensive rebounds in one continuous possession, each rebound
starts a new continuation segment. Points are assigned to the segment in which
they occur, so realized points are not counted more than once.

Because a small number of upstream possession flags are malformed, the audit
used robust boundaries:

- the next marked possession end;
- a defensive rebound;
- an offensive action by the opponent;
- the next same-team offensive rebound, which starts a new segment; or
- game end as a final fallback.

Only points scored by the offensive-rebounding team were counted.

## Direct OREB result

Pooled calculation:

`points per OREB = 8,618 / 10,248 = 0.8409446`

`rebound opportunities per possession = 33,294 / 68,064 = 0.4891573`

A one-percentage-point OREB% increase creates `0.4891573` additional offensive
rebounds per 100 possessions. Therefore:

`0.8409446 * 0.4891573 = 0.4113541`

Recommended direct point-attribution weight:

`+0.411 points per 100 possessions for each +1 percentage point OREB%`

Use `0.41` for display; three decimals imply more stability than the two-season
sample supports.

Season results:

| Season | Exact OREB points | Exact points/100 | Direct weight |
|---|---:|---:|---:|
| 2024-25 | 4,440 | 13.2001 | 0.4219 |
| 2025-26 | 4,178 | 12.1355 | 0.4007 |
| Pooled | 8,618 | 12.6616 | 0.4114 |

Other pooled descriptive results:

- 9,018 unique offensive-rebound continuations
- 0.9556 points per unique continuation
- 0.8409 points per offensive-rebound event
- 44.83% of unique continuations scored
- League OREB%: 30.7803%

Multiplying the current regression coefficient by the full pooled OREB% gives
an illustrative model-implied `19.3916` points/100, versus `12.6616` directly
observed points/100. Across this sample that is approximately 13,199 model-
implied points versus 8,618 realized points. This from-zero comparison is only
illustrative: the app normally applies `0.63` to factor gaps, and the regression
coefficient is not a literal point total.

## Context adjustment and starter-count test

Two otherwise identical outcome-standardization models were fit on all 10,248
OREB events and standardized over the same rebound-opportunity population.

Base controls:

- offensive team-season;
- defensive team-season;
- two-point, three-point, or free-throw miss;
- blocked versus ordinary miss;
- quarter and time bucket;
- score-margin bucket; and
- home/away.

The expanded model added offensive and defensive starter counts as categorical
values from zero through five.

Results:

| Model | Adjusted points/OREB | Weight per +1pp OREB% | Game-clustered 95% CI |
|---|---:|---:|---:|
| Without starter counts | 0.84830 | 0.41495 | 0.40418-0.42572 |
| With starter counts | 0.84786 | 0.41474 | 0.40396-0.42551 |

Adding starter counts changed the standardized weight by only `-0.00021`,
approximately `-0.05%`.

Model fit:

- R-squared without starters: `0.01671`
- R-squared with starters: `0.01770`
- Adjusted R-squared without starters: `0.01014`
- Adjusted R-squared with starters: `0.01017`

The low R-squared means pre-rebound context cannot predict the result of an
individual continuation well. It does not prevent precise estimation of the
league-average yield from 10,248 events. Much of the individual outcome is
post-rebound shooting variation that should not be controlled away if the
target is total OREB impact.

A standard nested-model F-test for the ten starter-count terms gave:

- F: `1.0285`
- p-value: `0.4159`

This provides no evidence that starter counts materially improve the model.
The standardized mean confidence intervals were game-clustered. A formal test
of the starter block under clustered errors would use a game-clustered joint
Wald test; it was not run. That additional test is unlikely to affect the
practical conclusion because the adjusted estimate and adjusted R-squared are
virtually unchanged.

## Statistical interpretation

### Why `0.63` and `0.411` differ

- `0.63` is a partial association between team-game OREB% and offensive-rating
  level in the pooled four-factor OLS model.
- `0.411` is direct realized point attribution from rebound continuations.

The OLS coefficient can absorb correlated team quality, finishing, possession
interactions, and omitted context. It should be described as estimated
offensive-rating association, not literal points generated.

### Levels versus changes

The current regression explains offensive-rating levels, not changes:

`ORtg = intercept + factor coefficients + error`

Subtracting two fitted observations converts level coefficients into factor-gap
contributions, but only under the assumption that their remaining baseline is
comparable. That makes the annotations useful approximations, not causal
estimates of how ORtg would change if a factor were improved.

For a change-oriented interpretation, prefer a within-team or fixed-effects
model:

`ORtg = team-season baseline + opponent-season baseline + factors + error`

The existing fitting script checks team-season and opponent-season fixed
effects as a robustness exercise, but the app weights come from pooled OLS.

## Decision reached

- `0.411` is the supported direct OREB point-attribution estimate.
- `0.41` is the preferred display value.
- Starter-count adjustment is unnecessary for the league-average OREB weight.
- Do not call the current `0.63` value exact OREB points.
- Do not silently replace only OREB in the existing four-factor regression
  legend without noting that it would mix direct and regression estimands.
- No app constant was changed during this analysis.

## Proposed next-stage work

Keep two clearly separated families of quantities:

1. Predictive/associational weights from the four-factor rating regression.
2. Direct or structural marginal point impacts for point attribution.

To obtain comparable point-attribution weights for all four factors, build a
possession-state model and estimate average marginal effects:

- OREB: expected continuation points created by recovering a miss.
- eFG: points from converting misses to makes, net of second-chance value lost
  when the miss and its rebound opportunity disappear.
- TOV: expected continuation points lost when a non-turnover possession state
  becomes a turnover.
- FTR: expected free-throw points gained while accounting for whether the foul
  replaces or extends another scoring opportunity.

The factors interact:

- eFG changes the number of rebound opportunities;
- turnovers remove possible shots, free throws, and rebounds;
- offensive rebounds create more shots, fouls, rebounds, and turnovers; and
- free throws can replace or supplement field-goal attempts.

Therefore, four simple independent accounting ratios will not be additive.
The preferred next analysis is:

1. Build a reproducible possession-state dataset.
2. Estimate expected remaining points at each pre-outcome state.
3. Perturb one factor by one percentage point while holding observed context
   constant.
4. Average the point change over the observed possession distribution.
5. Bootstrap by game for confidence intervals.
6. Compare structural weights with pooled OLS and team/opponent fixed-effects
   weights.
7. Decide whether the UI should show predictive rating associations, direct
   point impacts, or both with explicit labels.

Raw lineup fixed effects and cross-fitted lineup strength were judged
unnecessary for OREB impact. Team/opponent-season controls are the practical
baseline; starter count is an optional low-dimensional proxy but empirically
does not move the average result.

