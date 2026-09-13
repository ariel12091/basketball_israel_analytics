# Mobile presentation for the Shiny app

**Date:** 2026-09-13
**Branch:** `shiny/mobile-presentation`
**Status:** design approved, not implemented

## Goal

Present the existing Shiny app usably on a phone. **Functionality is frozen.**
Every metric, filter, view mode, download and modal that exists today must still
exist and still produce identical numbers. This is a presentation layer only.

## Scope boundary

The constraint that makes "functionality is frozen" structural rather than a
matter of discipline: **nothing in this work touches a server file, a reactive, a
SQL function, an MV, or a cache key.** The entire layer is two new client files
plus exactly these four R edits:

1. `app.R` `build_ui()` header - add `includeCSS("www/mobile.css")` and
   `includeScript("www/mobile.js")`, both behind the `IBPL_MOBILE` guard
2. `app.R:51` - `navbarPage(collapsible = TRUE)`
3. `global.R:758` - drop `maximum-scale=1` from the viewport meta
4. `global.R` - define `IBPL_MOBILE` alongside the existing `IBPL_CACHE_UI`
   env resolution

If a change appears to require a `server_tab*.R` edit, that is a signal the
design was wrong, not a licence to widen the scope.

No Shiny input is created, renamed or removed. Tap-to-expand, the filter sheet
and the tooltip sheet are all client-side DOM state that no observer sees.

## Delivery mechanism

One page for every device, reshaped client-side. **Not** a user-agent branch.

The reason is `app.R:177`: the fully-rendered HTML page is cached per worker in
`.UI_RESPONSE`, and Connect Cloud kills the worker roughly 5s after the last
connection closes. A UA branch would need a second cache slot and a second cold
render per worker, on a startup path where `bs_theme_dependencies()` already
costs 2.3-3.5s. A viewport-driven layer keeps one cache slot and adds nothing to
cold start.

A second benefit: a desktop browser in a narrow window gets the mobile layout
too, which is the honest behaviour. UA sniffing would leave it broken.

## Breakpoint

**767.98px**, matching the `d-md-none` filter toggles already present in all 11
tab UIs. One pixel of disagreement here would leave the sidebar collapsed while
the mobile layer was still off, or the reverse.

## What already works (do not rebuild)

- `shared_head_tags()` emits a viewport meta - `global.R:758`
- Every tab collapses its sidebar behind a "Show Filters" button under
  `d-md-none` - `ui_tab1_onoff.R:22` and ten siblings
- Three small mobile CSS blocks - `app.css:637`, `:923`, `:1401`
- `home_nav_cards()` uses `column(width = 6)` (= `col-sm-6`), which already
  stacks below 576px. **Home needs no layout work.**
- The navbar already shows only one league's tabs at a time, via the
  `TAB_LEAGUE` map at `app.js:1605`. A phone sees ~7 tabs (Israeli) or ~6
  (EuroLeague), not 11.

## Architecture

### Files

Two new files, `app/www/mobile.css` and `app/www/mobile.js`, added to the
`header = tagList(...)` in `build_ui()` (`app.R:74-90`) immediately after the
existing `includeCSS("www/app.css")` and `includeScript("www/app.js")`. Load
order matters: `app.css` carries `!important` declarations that CLAUDE.md flags
as load-bearing, and the mobile layer must be able to override them without
removing them.

Both files are read at UI build time by `includeCSS`/`includeScript`, so
**`IBPL_CACHE_UI=false` is required while editing them** or an edit needs an app
restart rather than a browser reload.

### Mode signal

`mobile.js` sets `document.body.classList.toggle("ibpl-mobile", ...)` from
`matchMedia("(max-width: 767.98px)")`, on load and on a debounced `change`.

Everything in `mobile.css` is scoped to `body.ibpl-mobile`, **not** to a bare
`@media` query. A single source of truth for "are we in mobile mode" means the
CSS and the JS can never disagree about it. The three existing scattered
`@media (max-width: 768px)` blocks move into `mobile.css` under the same scoping,
with their effect unchanged.

The precedent for a `<body>` class as the carrier is the `ff-ranges-off` block at
`app.js:2210`, and its governing comment at `app.js:2201`: *"DataTables
re-renders every cell on sort, page and filter, so anything stored on a cell is
gone by the next draw. A class on `<body>` survives all of it and costs nothing
to re-apply."* That constraint applies identically here.

### Kill switch

`IBPL_MOBILE=false` skips both includes, following the existing `IBPL_CACHE_UI` /
`POOL_PREWARM` convention. It must be added to the Connect Cloud optional-env
list in CLAUDE.md, since an unset var silently takes the committed default.

### Viewport fix

Drop `maximum-scale=1` from `shared_head_tags()` (`global.R:758`). It blocks
pinch-zoom, which is an accessibility failure and is unnecessary once the layout
fits.

## Component 1 - Tables

The core of the work. All 41 `datatable()` calls use `scrollX = TRUE`, so today
mobile means swiping sideways through up to 18 visible columns.

**Behaviour:** show 3 priority columns; tap a row to open the rest as a detail
child row.

**Implementation:** a delegated `draw.dt` handler, mirroring the existing one at
`app.js:1951`, re-applied on every redraw because DT discards per-cell state.
When `body.ibpl-mobile` is set, for each visible table: hide non-priority columns
via `column().visible(false)`, and bind row tap to `row().child()`.

**Priority derivation.** Default: the **first 3 currently-visible columns**. This
works without per-table configuration because the tables already order
identity-then-headline - `helpers.R:2095` reads
`"Team", "Player", "Net RTG Diff", "Off ON Diff", "Def ON Diff", ...`.

Overrides live in a CFG map in `mobile.js` keyed by DT output id
(`#onoff_table`), deliberately the same shape as the nav CFG array at
`app.js:1469`. Only tables where the default reads badly get an entry. The one
override known at design time is the Compare table - see Component 5.

**Child row content** is built from `cell().render('display')`, so HeatCell /
ShotCell / FFCell markup survives intact. Those cells are HTML whose gradients
carry the meaning; rendering them as text would lose it. This is an explicit
acceptance criterion, not an implementation detail.

**Two hazards handled explicitly:**

1. `column().visible()` triggers a redraw, which fires `draw.dt`, which would
   recurse. Guarded by a re-entry flag, the same way the `pending` guard at
   `app.js:1943` guards a double `preDraw`.
2. `columns.adjust()` must run after any visibility change or the header desyncs
   from the body. The precedent and its reasoning are at `app.js:2044-2048`.

**`scrollX` needs no R change.** With 3 columns visible the table no longer
overflows, so `scrollX = TRUE` goes inert on its own. All 41 `datatable()` calls
stay exactly as they are.

**Rejected: DT's Responsive extension.** Responsive is incompatible with
`scrollX`, which every table sets. Enabling it would mean editing all 41 calls
and losing horizontal scroll on desktop, where 18 columns genuinely need it.

## Component 2 - Navigation

Set `collapsible = TRUE` on `navbarPage()` (`app.R:51`). The default is `FALSE`,
so the tabs currently wrap or overflow rather than folding into a burger. BS5
renders the burger itself; no custom nav markup.

`#navbar_right_cluster` (`app.R:93`) is `position: fixed; right: 10px; top: 8px`
and will sit on top of the burger. On mobile it becomes static and moves into the
collapsed panel as a stacked row: league select, season select, glossary,
last-updated.

`.league-nav-il` / `.league-nav-el` are `display: inline-flex` driven by
`body.league-*` (`app.css:1253-1256`). They need a `body.ibpl-mobile` override to
`display: flex` so the existing league filtering still governs which season
selector shows.

**View-mode menus.** The hover menus at `app.js:1467-1580` are unreachable on
touch. Rather than porting them to tap, suppress them on mobile and promote the
`radioButtons` group that already exists in every tab's sidebar
(`.view-mode-container`, e.g. `ui_tab1_onoff.R:15`) to a segmented control pinned
above the table. The hover menu is only ever a shortcut to that same input, so
driving the input directly cannot drift from it, and it is less new code than a
touch port.

## Component 3 - Filter sheet

All 11 filter toggles are identical in shape: `d-md-none w-100 mb-2`,
`data-bs-toggle="collapse"`, `data-bs-target="#<prefix>-filters"`. One generic
transform in `mobile.js` therefore covers every tab with **zero per-tab R
edits**: match `[data-bs-toggle="collapse"][data-bs-target$="-filters"]` and, on
mobile, present the target panel as a bottom sheet - fixed, `max-height: 85vh`,
its own scroll, a sticky Apply/Close footer, backdrop dismiss.

Bootstrap's `collapse` still owns show/hide, so the button, its `aria-expanded`
state, and the existing chips-bar wiring keep working untouched.
`.chips-filters-toggle` is already hidden below 767px (`app.css:1401`) and that
stays correct. Filter controls go full-width with tap targets of at least 44px.

## Component 4 - Popups

**xl modals** - `server_tab2.R:866`, `server_tab10_euro_lineups.R:714`,
`mod_ribbon_modal.R:49`. On mobile, `body.ibpl-mobile .modal-dialog` becomes
full-screen: `margin: 0; max-width: 100%; height: 100dvh`, with a sticky header
and close button. `100dvh` rather than `100vh` so mobile browser chrome does not
crop the footer. The DT inside each inherits Component 1 automatically.

**DT header tooltips** - `HEADER_TOOLTIP_JS` (`global.R:212`) sets the native
`title` attribute on `th`, which never appears on touch. On mobile, `mobile.js`
converts each `th[title]` into a tappable header that opens the text in the same
sheet component as Component 3. `HEADER_TOOLTIP_JS` is unchanged: it keeps
writing `title`, and the mobile layer reads it.

**`tt()` sidebar labels** - `data-tooltip` plus a CSS `::after` on `:hover`
(`global.R:209`, `app.css:955`). Same treatment: a tap target opening the same
sheet. These are a *different* mechanism from the header tooltips and need their
own selector.

**Stat-filter popover** - `helpers.R:546`, a `bslib::popover` holding a
`selectInput`, `radioButtons` and `numericInput`, anchored to a small chip. On
mobile its body is relocated into the sheet. The Shiny input ids are preserved,
so `apply_stat_filters()` and every server observer are untouched.

**Out of scope, agreed:** the glossary modal and the rotation chart's hover
tooltip (`helpers.R:3147`, `:3424`).

## Component 5 - Compare

Compare's summary is three `column(4)` cards - Side A, Side B, Avg Gap
(`ui_tab7_compare.R:351-370`). Three cards at 390px gives each about 120px, which
cannot hold an `fs-4` value plus four `small` lines.

**Layout:** A and B side-by-side at 50/50, Gap full-width beneath. This keeps the
two compared values adjacent, which is the tab's only job, and Gap reads fine as
a full-width strip.

The A/B *filter* inputs (16 `column(6)` pairs) stack normally - those are entered
one at a time.

**The Compare table is the one CFG override known at design time.** Its columns
are `#, Team, Side A, Total Poss A, Side B, Total Poss B, Gap`. The Component 1
default of "first 3 visible" yields `#, Team, Side A`, which deletes Side B and
the Gap and destroys the comparison. Override to `Team, Side A, Side B, Gap`.

## Testing

Playwright driven from PowerShell - `npx` is broken under the Bash tool in this
environment - against a local `runApp('app')` with `IBPL_CACHE_UI=false`.

**Widths:** 390x844 (iPhone), 360x800 (Android), 768x1024 (tablet), and 1440x900
as a no-regression control.

**Per tab, all 11:**

- screenshot
- no horizontal document overflow: `body.scrollWidth <= body.clientWidth`
- the served page still contains 11 `nav-link` occurrences - the health check
  CLAUDE.md specifies, which also catches a UI built without app context

**Interactions, touch-emulated:**

- burger opens and lists the correct league's tabs
- filter sheet opens, applies, closes; chips bar reflects the applied filters
- a table row expands, and the child row contains the hidden columns' **HTML**
  with gradient cells intact, not flattened to text
- a column header tap opens the tooltip sheet
- the view-mode segmented control changes the rendered table

**Regression:** the existing `testthat` suite. Components 1-5 touch no server
code, so a failure there means something was broken that this design claims not
to touch.

**Do not quote local timings as production numbers** - CLAUDE.md documents a
local launch measuring 13.7s against production's ~3s.

## Risks

**The "first 3 visible columns" default is an assumption.** It was verified
against `helpers.R:2095` and against the Compare table, not against all 41
tables. The per-tab screenshot pass is what converts it to fact, and it is
expected to produce a handful more CFG overrides. This is the single most likely
source of rework.

**The chips bar and the filter sheet both present filter state.** They must not
contradict each other once the sheet is applied. Covered by an explicit
interaction test rather than by inspection.

**`100dvh` support.** Fine on current iOS/Android browsers; a `100vh` fallback is
declared first so an old browser degrades to a slightly cropped sheet rather than
a broken one.

## Out of scope

- Any change to metrics, SQL, MVs, ETL or cache keys
- The React frontend in `frontend-v2/`
- The glossary modal and the rotation chart hover tooltip
- Tab 6 (`ui/server_tab6_team_stats.R`), which is not sourced in `app.R`
- Offline support, install prompts, or any PWA behaviour
