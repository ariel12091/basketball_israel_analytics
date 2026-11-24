# Gameflow: win probability and runs ranked by WPA (design, 2026-11-24)

## Why

User, 2026-11-17 backlog and 2026-11-24: Gameflow should rank stretches by how
much they moved the result, not by net points (a first-quarter 10-0 should not
outrank a 9-2 that decided the last two minutes), and the win chance should be
visible at every moment of the game, without a chart of its own.

The model is live since 2026-11-24 (PR #129, `fit_id 2`):
`basketball_test.game_win_probability(league, game_id)` returns P(home win)
after every possession, both leagues ('il' | 'euro'; EuroCup is 'euro').
Contract rows: "Win probability", "Win probability added, WPA".

## Goals

1. Every moment: the existing score readout (pointer on the margin band, a tap
   on phones) gains each team's win chance.
2. Runs: same run rule, ranked and the defining stretch chosen by win-chance
   swing; each run says its swing.
3. One computation on the server; the page and the Ask `gameflow` tool read the
   same numbers (rule 1).

## Non-goals

- No win-probability line, tint or chart (user: "not necessarily as its own chart";
  numbers once).
- No WPA on players, lineup rows or selected stints. A per-game player WPA
  winner is wanted LATER (user 2026-11-24, "doesn't need to be right now");
  the crediting rule (who acted vs on-floor) is still open.
- No change to the model, its SQL or its fit.

## Approaches considered (runs)

- A. Label only: runs and the defining rule unchanged, each run labelled with its
  swing. Leaves the ranking problem.
- **B. Rank by WPA (picked by the user).** Runs still meet +8 net / opponent <= 6;
  the three shown are the non-overlapping candidates with the biggest swing;
  the defining stretch is the winner's candidate with the biggest swing
  (replaces the 2026-11-17 "went ahead for good" rule).
- C. B plus a win-probability line on the chart. Dropped: user does not want a
  separate chart.

For "every moment", the score readout was the one sensible place: tinting the
band is decoration, a line is a chart.

## Design

1. **Read.** `server/gameflowRepo.ts` calls
   `SELECT ... FROM basketball_test.game_win_probability($1, $2)` inside the
   existing snapshot (league 'il' for Israeli, 'euro' for EuroLeague and
   EuroCup) and the existing cache. Rows are home-side; the repo knows whether
   the page team is home and flips to the page team's side (own / opp).
2. **Rule file** `rules/gameflowWinProb.ts`:
   - map each row to elapsed seconds on the ribbon's clock (period and
     seconds_left -> elapsed, using the model's `bounds`);
   - `winChanceAt(series, elapsed)`: the last row at or before that moment
     (tip-off = pre-game chance; the final row is 1 or 0);
   - `runSwing(series, run)`: own-side chance just before the run's first point
     to just after its last, from the run team's side.
3. **Runs** (`rules/gameflowRuns.ts`): `detectRuns` takes the series; candidates
   unchanged; rank by swing (ties: net, then shorter, then earlier); defining =
   winner's largest-swing candidate; each run carries `wpFrom`, `wpTo`. With no
   series (model missing for a game) it falls back to today's ranking and rule.
4. **Display.**
   - Score readout, every width and rating mode (also with ORtg off): a
     "Win 71%  |  Win 29%" row under each team.
   - Run chip tooltip / explanation and the phone Runs list: "win chance 41% -> 78%".
5. **API / Ask.** The payload gains the series and the runs' `wpFrom`/`wpTo`;
   the Ask `gameflow` tool's flattening passes them through.
6. **Contract.** `docs/shared_definitions.md`: "Gameflow run" row gets the new
   ranking and defining rule; WPA row's "no page shows it yet" becomes
   "Gameflow runs (since <date>)"; "never credited to players" stays until the
   player winner is designed.

## Checks

- Regenerate the three fixtures (`scripts/save_gameflow_fixture.R`) with the
  series; unit tests for `winChanceAt`, `runSwing`, the ranking and the
  defining rule, and the fallback with no series.
- A test that the final row matches the game's result side.
- Grep `e2e/` for changed wording ("defining stretch", run labels) and update
  the pins.
- Preview live checks must pass on the head; merge only on the user's "deploy".

## Left to the builder

Exact readout styling (follow `.ibpl-gameflow-score-grid`), rounding (whole
percent; show "<1%" / ">99%" rather than 0/100 before the final row), and the
fixture JSON shape.
