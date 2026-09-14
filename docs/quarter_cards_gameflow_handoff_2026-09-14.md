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

## Validation completed

- R parsing passed for `helpers.R` and `mod_ribbon_modal.R`.
- `node --check app/www/mobile.js` passed.
- The targeted test directory run completed with exit code 0:
  `testthat::test_dir('app/tests/testthat', filter='mobile-layer|stint-ribbon')`.
- Before this change, the published app was inspected at 390×844. The existing
  compact ribbon rendered, measured 954px wide, and its tap/detail behavior was
  visible. The new quarter-card layer has not yet been browser-tested against a
  live Shiny response.

## Next checks

1. Run the app with `IBPL_CACHE_UI=false` and open Game Logs at a 390px viewport.
2. Open a regulation game and confirm four cards render, each card fits the
   phone width, and tapping a stint still creates the detail card.
3. Open an overtime game and confirm the extra card and `OT1` button render.
4. Click `View full timeline`, verify the original chart and pinned gutter are
   restored, then return to quarter cards.
5. Test a long team name, a player with multiple stints, and both Israeli and
   EuroLeague Game Logs.
6. Review duplicate accessibility IDs/labels in cloned SVGs and confirm that
   keyboard focus does not reach hidden or out-of-period lanes.

## Known implementation risk

The quarter cards clone the source SVG in the browser and remove out-of-period
lanes. SVG clip paths and `data-clip` references are renamed per clone. This
needs a live browser check because the existing app.js selection code relies on
those references, and because a quarter card may contain a stint crossing a
quarter boundary.
