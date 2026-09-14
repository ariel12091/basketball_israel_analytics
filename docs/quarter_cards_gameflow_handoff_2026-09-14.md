# Game Flow quarter cards handoff — 2026-09-14

## Current objective

The mobile Game Flow ribbon should read naturally on a phone. The previous
mobile presentation showed a 954-unit compact SVG inside a roughly 316px
viewport, which required a long horizontal swipe to reach later quarters.

## Implemented direction

The mobile Game Flow result now includes:

- a small full-game score-margin overview;
- `Q1`–`Q4` jump buttons, plus `OT1`, etc. when overtime exists;
- one phone-width quarter card per period;
- a `View full timeline` button that restores the existing horizontally
  scrollable compact ribbon for users who want the continuous game view;
- the existing stint tap, lineup detail card, pinned labels, and keyboard
  behavior.

The quarter cards are views onto the same server-rendered SVG. They use the
SVG's `data-period-bounds` values to set a quarter-specific viewBox, so the
score margin, player bars, and period geometry remain sourced from the same
game data. No new database query or ETL path was added.

## Files changed

- `app/R/helpers.R` — adds `data-period-bounds` to the ribbon SVG.
- `app/R/mod_ribbon_modal.R` — adds the mobile overview and period controls to
  the inline Game Flow result.
- `app/www/mobile.js` — builds quarter cards from the compact SVG, fits each
  card to the available width, handles quarter jumps and the full-timeline
  toggle, and keeps the existing pinned-gutter logic.
- `app/www/mobile.css` — styles the overview, quarter buttons/cards, and full
  timeline states.
- `app/tests/testthat/test-mobile-layer.R` — checks overview periods and
  overtime controls.
- `app/tests/testthat/test-stint-ribbon.R` — checks period-boundary metadata.

## Browser pass — completed 2026-09-14

Run locally (`IBPL_CACHE_UI=false`) at 390×844 and 430×932 against a live
Shiny response. Games: Israeli 398 (regulation, Bnei Herzliya vs Hapoel
Eilat), Israeli 64942 (one OT, long team names), EuroLeague 729 (three OTs).

The first pass found five defects, all fixed in `mobile.js` / `mobile.css`:

| Defect | Fix |
|---|---|
| `preserveAspectRatio="none"` stretched chart text sideways — 0.85x on a regulation card, **1.7x** on a 5-minute OT card | One uniform scale per game, taken from Q1 and capped at 1.25. An OT card is now narrower rather than stretched, and a bar's width means the same minutes in every card. The pinned gutter takes the same scale (`pinGutter`). |
| A stint crossing a period edge printed its +/- cut in half in both cards (e.g. a stray `0` against the pin) | Each bar in a card **is** its period: number, tap detail (window, floor time, +/-, points for/against), lineups and accessible name. `ribbon_window_detail()` (helpers.R) builds a bar's detail for any time window and serves both the full chart and the cards; `ribbon_period_details()` emits one JSON entry per period as `data-period-detail` on compact lanes; `clipBarsToPeriod()` swaps it in and cuts the rect at the period edges, so app.js maps lineup segments onto the visible bar. A lineup segment crossing a period end is clipped and gets its own +/- on each side. Nothing is summed, because segments can span a period end (26 Israeli 2026, 454 EuroLeague). Two earlier attempts the same day are superseded: whole-stint number only in the midpoint card (left long bars blank), then per-period numbers with whole-stint tap detail. |
| Players who sat out a period left blank rows (Q2 of game 398: 6 of 17) | `collapseRows()` rewrites y values. It deliberately uses no transform on the shift layers: app.js strips those on every deselect and hit-tests raw rect `y`. Only the two margin curves are translated, which keeps their clip paths in their own user space. |
| Seven periods gave 35px jump buttons, below the 44px tap minimum, and overflowed | The jump row is a grid (`minmax(var(--ibpl-m-tap), 1fr)`), so it wraps to a second row. |
| The active jump button only followed clicks | A rAF-throttled scroll listener marks the card whose top has passed 120px. |

Also removed the top-row period markers inside cards. The card heading names
the period, and in a narrow OT card the pinned team name's backing cut the
marker into a sliver.

Verified after the fixes:

- Audited every card bar in games 398, 64942 and 729: 375 bars and 282 stints,
  with zero problems. Each bar's window, rect, lineup segments (in range,
  covering the window, +/- summing to the bar), number and aria-label agree
  and stay inside the period. Its period +/- and points-for sum back to the
  whole stint, and every bar with room carries a number (68 are too narrow).
- Desktop SVG output is byte-identical before and after the extraction, for
  both teams of all three games. The compact output differs only by the
  per-period attribute.
- Clean games are internally consistent per quarter: points for minus points
  against equals the quarter +/- for both teams in all 12 periods of 64942
  and 729.
- **Game 398's Q2 is a data problem, not the split.** From 8:19 to 19:59
  each team has one lineup segment (no substitutions for 11½ minutes), and
  the last 29 score events before the series passes 20:00 are all stamped at
  1200s. Herzliya's score jumps 22 → 43 there, and Eilat's Q2 points land
  in a one-second gap with no opponent lane, so the cards show Q2 as 0 for
  most bars. The full chart already showed it (a 10-second stint at
  "23 for, 20 against"). This is the known 398/399 clock game; see
  `docs/etl_clock_incident_handoff_2026-09-13.md`.
- Every card renders at identical x/y scale (0.897 at 390px, 1.098 for the
  triple-OT game, capped at 1.25 at 430px). Pinned names sit within 0.4px
  of their rows in every card of all three games.
- Tapping a stint in a card, including an opponent stint below collapsed
  rows, selects it. The detail card opens inside that card, the renamed clip
  id is unique in the document, the margin highlight lands on the curve, and
  a lineup row tap focuses its minutes.
- `View full timeline` restores the 954px chart with a 1:1 pinned gutter that
  stays sticky while scrolling. Toggling back refits the cards.
- A width change refits every card and keeps one pin per card. A height-only
  resize (the address bar) is ignored.
- The hidden full timeline holds no focusable element, and no card contains a
  stint outside its period.
- The only console error is the pre-existing favicon 404.

## Remaining, by choice

- The jump row scrolls away with the overview. A sticky row would cost ~50px
  of phone height on every scroll, so it was not added.
- The full-timeline SVG in the same page still carries `data-period-detail`
  on every lane, unused there (the cards strip it from their clones).
