# Stats-tab layout overhaul (web/) — design

Date: 2026-10-28. Branch: `sql/web-foundation`. Status: approved in conversation (2026-10-28),
awaiting sign-off on this document.

## Why

The React app in `web/` was ported to match Shiny's features, numbers and look
(`web/CLAUDE.md` § Direction 1). That port is done for every tab. The user now
wants the planned redesign, starting with Home (shipped: `849cd9f`, `11751fc`,
`d42d397`) and continuing with the stats tabs. Their complaints, in their words
where possible:

- the stats pages are "too crowded and intimidating"; the filter sidebar should
  be a drawer;
- "way too many colors (and contrasting ones — green, red, blue, orange)";
- the legend is too big and too far from the table, so it is not intuitive;
- Min Poss / Min ON Poss sliders are hard to use on a phone;
- hover text that adds value (3PT luck, small sample, header definitions) must
  be obviously there;
- the possession columns (Min / On Poss / Off Poss) must be visible without
  scrolling sideways;
- people miss the Four Factors / Shot Profile views, so the view switch must be
  on the page.

Success: a first-time visitor sees the table, how much data stands behind each
row, and the other views, without hunting; every existing feature still works
and every number is unchanged.

## Goals

One shared layout pattern for every stats tab, built first on On/Off and then
applied to the others:

| Tab | Leagues |
|---|---|
| On/Off (`OnOffClient.tsx`) | IL, EuroLeague, EuroCup |
| Lineups (`LineupsClient.tsx`) | IL, EuroLeague, EuroCup |
| Team Ratings (`TeamRatingsClient.tsx`) | IL, EuroLeague, EuroCup |
| Game Logs (`GameLogsClient.tsx`) | IL, EuroLeague, EuroCup |
| Player Stats (`PlayerStatsClient.tsx`) | IL, EuroLeague, EuroCup |
| Compare (`CompareClient.tsx`) | IL — shared parts only, see "Compare" |

## Non-goals

- No change to any number, filter meaning, data route, SQL or API. Rule 1-10 of
  `docs/shared_definitions.md` still hold; this is presentation only.
- No new features and no removed features. Every control that exists today
  exists afterwards, reachable from the new layout.
- Home and Gameflow are not part of this (Home is already redone).
- Not a change to Shiny (`app/`). Shiny keeps its look.

## Anti-goals (would be a failure even if it works)

- A feature lost in the move (a filter, a mode, a pivot menu, a tooltip, CSV
  export, saved column order, chips' clear/edit actions, auto min-poss).
- Hover-only information that a phone user cannot reach.
- Two layouts for the same width (`web/CLAUDE.md` § Direction 2 still applies:
  one layout per viewport width, mobile first, width rules in
  `layoutClasses.ts`).
- Per-tab copies of the new pieces instead of shared ones in `TabKit`.

## Constraints

- Idiomatic React, the shared kit, TanStack Table (`web/CLAUDE.md` § Direction
  3-4). Show/hide, menus, popovers and the drawer are React state.
- `src/styles/shiny/app.css` and `mobile.css` are byte-for-byte copies of
  Shiny's files, pinned by `src/styles/shiny.test.ts`. **Do not edit them.**
  New styles go in `src/app/web.css` (or a new web-owned stylesheet), and must
  out-specify or replace the copied rules they supersede.
- UI text and constants come from the R export (`src/data/*.json`) where it
  exists; new web-only text goes in those JSON files' web sections (as Home did
  with `homeUi.json`), not inline in components.
- Navbar width is tight (root `CLAUDE.md` § "The navbar has almost no spare
  width"): nothing new goes in the navbar.
- Browser tests at 390 and 1440 px keep passing; tests that depend on the old
  structure are updated, not deleted, so they still prove the same behaviour.

## The pattern (decided with the user)

Top to bottom, on every stats tab:

### 1. Toolbar (replaces the sidebar)

One row (wrapping on narrow screens) directly under the navbar:

- **Filters button** with the count of active filters ("Filters · 2"). Opens the
  drawer.
- **Active-filter chips**, as today (`ChipsRow`, `contextChips`), with their
  clear (×) and edit (pencil) actions. The edit action now opens the drawer and
  focuses that control.
- **Possession minimums** (only on tabs that have them: On/Off has Min Poss /
  side and Min ON Poss; Lineups has Min Poss): **− / + steppers** replacing the
  sliders, the number itself tappable to type a value. Step and range are the
  slider's current ones (step 10; On/Off 0-2000 and 0-3000; Lineups 0-2000,
  EuroLeague up to `meta.maxPoss`). Auto min-poss behaves exactly as today:
  auto sets the value, a manual change turns auto off, a filter change turns it
  back on (root `CLAUDE.md` § Auto Min Possessions).
- **Stat filter** button (`StatFilters`), as today.

Everything that is in the sidebar today and is not listed above moves into the
drawer.

### 2. Filters drawer (frontend-v2 style)

Modelled on `frontend-v2/src/features/filters/FilterDrawer.tsx` and its CSS in
`frontend-v2/src/styles/layout.css` ("FILTER DRAWER"), which the user picked:

- Slides in from the **right**, about 340 px wide, below the navbar; it
  **pushes the page content left** (content gets a right margin) rather than
  covering it.
- Header: small uppercase "FILTERS" title and a square × close button. Escape
  closes it.
- A red **"Clear all filters"** button at the top, shown only when something is
  set (it does what "Reset to defaults" does today).
- **Collapsible sections** with small uppercase titles and a chevron, the first
  one open. The sections are the tab's existing filter groups (e.g. On/Off:
  dates; teams; lineup starters; game filters accordion; opponent strength /
  clutch where the tab has them). Keep each tab's existing grouping and order.
- On a phone (the `ibpl-collapsed-nav` width) it takes the **full screen**, with
  a button at the bottom that closes it showing the result count, e.g. "Show
  142 players" (builder's call on the exact wording per tab).
- Filters apply as they change, as today; the drawer does not need an Apply
  step.

### 3. View tabs on the page

The tab's display modes (On/Off: Summary / Four Factors / Shot Profile; the
others: whatever `ViewRadios` offers today) as an always-visible row of tabs
directly above the table, the active one underlined in amber. The navbar hover
menu stays as a second way in. Both drive the same state, so URLs and
bookmarks keep working.

### 4. A one-line key, at the table

The big "Show/Hide Example" legend box and the "What This Tab Answers" panel
above the table go away. Instead, one short line sits directly above the table
header, inside the table's frame, showing only the parts that apply to the
columns on screen:

> **Shading** = league rank: red bottom … green top · **Shot cells:** top =
> accuracy 2P% | 3P% (green good, red poor) · bar = shot frequency ▬ 2PT ▬ 3PT

The ▬ marks are drawn in the real bar colours. The Four Factors view shows only
the shading part (plus whatever its own legend carries today, e.g. the impact
annotation key). The content of the removed panels must stay reachable: the
"What This Tab Answers" text and the 3PT luck / small sample explanation move
behind the hover marker (section 6) on the relevant headers, cells and tags.
Builder: confirm by listing every removed block's text and where it now lives.

### 5. Colours: one muted good/bad scale

- **Heat shading:** muted green (good) to muted red (poor), replacing today's
  saturated gradient. Polarity rules are unchanged (offense green-high except
  TOV%; defense reversed; unranked gray/null — root `CLAUDE.md` § Four Factors
  Metrics). Mockup reference values: green `rgba(110,180,130,.08/.20/.35)` for
  the three upper bands, red `rgba(210,120,105,.10/.24)` for the lower ones;
  the builder picks the final band colours so text stays readable (contrast
  AA) and the middle reads as neutral.
- **Shot cells (Off Shot / Def Shot):** accuracy numbers in muted green/red
  *text* (good/poor vs the league average, same rule as today's `ShotCell`
  coloring); the frequency bar in **grey for 2PT and amber for 3PT**. No labels
  inside the bar.
- Nothing else uses colour for meaning. Blue disappears. Amber stays the accent
  (active tab, links, buttons, the 3PT bar).
- The heat colours today come from `ui.heat` (R export, `shinyUi.json`). The new
  palette is web-owned: define it as CSS variables / a web constant and keep
  the band cut points from `ui.heat.cuts` so the *ranking* is unchanged.

### 6. "There's more here" marker

- Any header or cell with extra information gets the same **dashed underline**
  (headers already use a dotted one — make them one style).
- Pointer devices: hover shows it. Touch: a tap shows it (today touch uses the
  small ⓘ button, `.ibpl-m-th-info`; replace it with the tap on the underlined
  text itself, keeping tap-to-sort reachable — builder's call how, e.g. sort
  arrows as their own tap target).
- **"small sample" and "3PT luck"** become small amber-outlined **tags** in the
  Net cell; the tag itself is the trigger for the explanation and the extra
  stats the tooltip shows today (`.onoff-tip` content). The separate "What do
  '3PT luck' and 'small sample' mean?" button goes.
- The `table_hint` line ("Click a team or player for lineups and game logs ·
  ...") stays, shortened if it wraps.

### 7. "How much data" columns first

The group that says how much data stands behind a row (On/Off: **Usage** = Min,
On Poss, Off Poss) moves to sit **right after the name columns** (Team,
Player), in every view of the tab. On a phone, the name columns and this group
are pinned on the left while the rest scrolls sideways. For the other tabs, the
equivalent group (possessions, minutes or games — e.g. Lineups' Poss/Min,
Player Stats' GP/Min, Team Ratings' GP/Poss) gets the same treatment; the
builder lists the group per tab in the plan. Saved column order
(`dt_col_order` equivalent) keeps working; a saved order from before the change
should not resurrect the old position (builder's call: version the storage
key).

## Decided by Claude (the user can overrule)

- Possession steppers step by the slider's current step (10), not 50 as said in
  conversation, so values match what the slider could produce.
- The phone drawer's close button shows the result count.
- The "What This Tab Answers" panel is removed and its text moves to the hover
  layer (section 4).
- The view tabs live on the page and in the navbar menu (both kept).
- The new palette lives in web CSS, not in the R export.
- Order of work: On/Off first (all three leagues), get the user's sign-off on
  the running page, then the remaining tabs one at a time.

## Left to the builder

- Component structure inside `TabKit` (e.g. `Toolbar`, `FilterDrawer`,
  `ViewTabs`, `TableKey`, `PossStepper`, a shared `MoreInfo` trigger), as long
  as every tab uses the shared ones.
- Exact spacing, sizes and transitions; the drawer's animation.
- How the drawer's section list is declared per tab.
- Focus management (focus into the drawer on open, back to the Filters button
  on close) and ARIA roles.

## Compare (user decision)

Compare's sidebar is the A/B comparison builder, the page's main control, so it
**stays visible** where it is. Compare gets only the shared parts: the colours
(section 5), the one-line key (4), the hover marker (6), the on-page view tabs
(3) and the data-columns-first order (7) where its tables have such a group. No
toolbar or drawer on Compare.

## Verification

- Unit tests and typecheck pass; lint adds nothing new.
- For each tab, the existing browser specs (390 and 1440 px) pass after being
  updated to the new structure, plus new checks for: drawer open/close and
  push (content not covered), Clear all, chip edit opening the drawer, view
  tabs switching modes and URL, steppers including auto min-poss, possession
  columns visible without horizontal scroll at 1440 px, every hover marker
  reachable by tap at 390 px, the key showing only relevant parts.
- Shiny parity tests (stored goldens) still pass: numbers and row order are
  unchanged.
- Screenshots at 390 and 1440 px shown to the user for On/Off before moving to
  the next tab.
