# Navbar league dropdown — design

Date: 2026-08-12
Branch: `shiny/euro-tab1`

## Problem

The navbar currently exposes the league dimension through two unrelated
controls that appear in different places and behave differently:

- an `IL` / `EL` button toggle (`app.R:98-104`), owned by JS and
  `localStorage`, which filters the navbar tabs;
- a EuroLeague-only competition select (`global_euro.R:228`) holding
  EuroLeague / EuroCup, visible only once you are already in the EuroLeague
  section.

So choosing "EuroCup" takes two interactions through two different widgets,
and the Israeli league is never presented as a peer of the other two. A third
control — the two-button chooser on Home (`ui_tab0_home.R:33-45`) — offers only
two of the three options.

## Decision

Replace all of it with **one select listing all three competitions, always**:

```
[ EuroLeague ▾ ] [ 25-26 ▾ ]
   EuroLeague          value "E"
   EuroCup             value "U"
   Israeli League      value "il"
```

Option order as listed above. The list is **static**, not derived from
`euroleague.final_schedule_mv`, so all three are always present. Default on
first visit stays Israeli League.

Both competitions have live data as of this date — 292 EuroLeague and 195
EuroCup games in `final_schedule_mv` — so no option is a dead end.
(`euroleague/PROJECT.md:681` claims EuroCup is uncollected; that line is
stale.)

## Why one select and not three tabs / a filter

A league filter *inside* each tab would break the standing rule that no ranked
table mixes leagues. Keeping separate tab sets per league, switched by one
control, keeps that rule structural rather than conventional.

## Components

### The control — `league_select`

Named for the league dimension, not `euro_*`, because it now owns both
leagues. Lives in the navbar where the `.league-switch` used to be, rendered by
`euro_navbar_season_ui()` alongside the season selects.

Needs a wider CSS variant: `.navbar-season-select` is pinned at 90px, which
does not fit "Israeli League".

### Season selects — unchanged

`game_year` (Israeli, season-ending year) and `euro_game_year` (provider
season) stay two separate inputs with only the relevant one visible, swapped by
the `body.league-il` / `body.league-el` class. The two leagues number seasons
differently and every tab already reads its own. Visually it reads as one
season dropdown that never moves.

### JS — `app.js` league module

`change` on `#league_select` maps value to league:

```
leagueOf(v) = (v === "il") ? "il" : "el"
```

then runs what `setLeague()` does today: body class, nav-item filtering via
`TAB_LEAGUE`, `localStorage`, and the redirect-home-if-stranded rule. Still no
server round-trip for the visual switch.

Precedence on init is unchanged: a restored bookmark's active tab implies a
league and wins; otherwise `localStorage`; otherwise Israeli League. When the
stored league disagrees with the select, the select is reconciled to it before
Shiny binds, so the input reports the league actually displayed.

The `.league-switch` markup and its `data-league-btn` buttons are deleted, but
the delegated `data-league-btn` click handler stays — Home's cards reuse it.

### R — `global_euro.R`

- `euro_navbar_season_ui()` renders `league_select` (static three options) plus
  `euro_game_year`.
- `euro_selected_competition(input)` reads `input$league_select` and maps
  `"il"` to `EURO_DEFAULT_COMPETITION`. **No tab file changes** — all four
  EuroLeague tabs already go through this one helper, which is the only reader
  of the old `input$euro_competition`.
- `euro_init_season_inputs()` loses its competition-population observer (the
  list is static now). Its season observer triggers on `league_select` and
  returns early on `"il"`.

### Home — `ui_tab0_home.R`

The two-button `.league-chooser` becomes three cards with `data-league-btn` of
`"E"` / `"U"` / `"il"`, matching the dropdown exactly. The click handler writes
the value into the select and dispatches `change`, so both entry points run one
code path. The `.league-only-el` blurb is reworded to cover both competitions.

## Rejected

**Sticky last-EuroLeague competition.** Considered holding the last EL value
while `"il"` is selected, via a `reactiveVal` on `session$userData` reached
through `getDefaultReactiveDomain()`, to stop hidden EL tabs from refetching
when the mapped competition flips `U` to `E`. Rejected: the churn is
pre-existing (tab 8's heavy path at `server_tab8_euro.R:367` is not gated on
`input$main_tabs`, so changing competition from another tab already does this),
the queries are cached, and with a single dropdown there is no "return to where
I was" state to preserve. If it ever shows up in the logs the correct fix is
gating tab 8 on tab visibility, which fixes the pre-existing case too.

## Out of scope

Tabs 1-11 logic, SQL, cache keys (already `(competition, season)`-scoped),
`frontend-v2`.

## Risks

`app/app.R` has mixed line endings **in the committed blob** (324 CR of 538
lines), so its edit must be byte-preserving or the diff balloons to the whole
file. `app.js`, `app.css` and `ui_tab0_home.R` have pure-LF blobs with CRLF
checkouts, which `core.autocrlf=true` normalizes on staging; they are safe to
edit normally.

## Verification

Launch the app and confirm, per league: the three options are always listed;
picking each filters the navbar to that league's tabs; the season select swaps
and holds the right convention; Home's three cards drive the same selection;
a EuroCup selection actually returns EuroCup data in tabs 8-11; and
`git diff --stat` shows no whole-file rewrites.
