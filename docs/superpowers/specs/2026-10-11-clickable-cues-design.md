# Clickable cues: make every interaction visible

Date: 2026-10-11
Branch: `shiny/clickable-cues` (from `main`)
Status: approved; revised 2026-10-11 after `2026-10-11-clickable-cues-review.md` (all seven corrections checked against the code and taken)

## Goal

Ahead of the official launch, a first-time visitor should be able to see, without
hovering or guessing, every place in the Shiny app that does something when
clicked or tapped. This is about discoverability of what already exists.

Success means:

- Every interaction in the inventory below carries a visible cue at rest.
- The cue works on a phone, where there is no hover and no cursor.
- The same cue always means the same thing, on every tab and in both leagues.

## Out of scope

- A first-visit tour or a per-tab help panel (considered, not chosen).
- New features or new interactions.
- The React frontend (`frontend-v2/`).
- Changing what any click does.

## What is hidden today

| # | Interaction | Where | Signal today |
|---|---|---|---|
| 1 | Navbar tab opens a view-mode menu (Summary, Four Factors, Shot Profile, Traditional, per-game modes, Compare modes) | 10 tabs, `CFG` in `app/www/app.js` | Hover only. The in-page radios are `display: none !important` (`.view-mode-container`), so on desktop this menu is the only route to those views |
| 2 | Player or team name opens the pivot menu ("Lineups with this player", "Lineups for this team", "Game log for this team") | Tables built by `onoff_summary_datatable()` / `onoff_four_factors_datatable()` and the team tables; cells marked `data-pivot-trigger` | None. No style at all, not even a pointer cursor |
| 3 | Lineup name opens the game-log modal | Tab 2 (`.ld-lineup-link`), Tab 10 (`.euro-ld-unit`) | Tab 2: underlined amber, already clear. Tab 10: no CSS rule at all, so the two leagues do not match |
| 4 | Gameflow link opens the stint ribbon | Game logs (`.ribbon-link`) | To verify in the running app |
| 5 | Table row opens the Compare detail view; in the team-players table a row opens a second, player-level comparison | Tab 7 (`#cmp_table`, `#cmp_team_players_table`) | Pointer cursor only |
| 6 | Compare "gap" header sorts | Tab 7 (`.js-cmp-detail-sort`) | Carries its own sort icon |
| 7 | Filter chip jumps to its control | All tabs (`.chip-focusable`) | Pointer cursor only |
| 8 | "at least / exactly" and count tokens cycle a choice | Tabs 2, 10 (`.lineup-chips-token`) | Bordered amber buttons |
| 9 | Ribbon lanes, player names and detail rows select players; two or more selected reveal shared court time and shared lineups | Gameflow (`ribbon_pair_controls()`, `renderSharedLineups()`) | Pointer cursor; the status line says to select players but not what that gives |
| 10 | Hub cards and storylines navigate to a tab | Home (`.hub-card`, `.hub-story-line`) | Border change on hover |
| 11 | Column headers explain themselves | All tables: `HEADER_TOOLTIP_JS` writes `th[title]` (not `data-tooltip`, which is the `tt()` label mechanism). On touch, `mobile.js` adds an `.ibpl-m-th-info` button inside each `th[title]` | Pointer: `cursor: help` only. Touch: the info button |
| 12 | On/Off Net number and its tags (3PT luck, small sample) explain themselves | Tabs 1, 8: one `.onoff-net-tip` span wraps the number and the tags | `cursor: help` only |
| 13 | Four Factors range toggle, luck explainer | Tabs 1, 8 | To verify in the running app |
| 14 | `+ Filter` chip opens column / operator / value stat filtering | `stat_filter_chips_ui()` in `helpers.R`; Tab 5 builds its own copy of the chip (`server_tab5_traditional.R`) | A chip reading only "+ Filter", not distinguishable from game-context filters |
| 15 | Column headers can be dragged to reorder | Tab 5, and Tab 3 in Traditional view (`ColReorder`) | None |

Rows marked "to verify" are checked in the running app as the first
implementation step; any further interaction found there is added to this
table and gets a cue from the vocabulary below, not a new one.

## The cue vocabulary

Each cue has one meaning. A caret means a menu and nothing else.

| Cue | Meaning | Applies to |
|---|---|---|
| **Open**: amber underlined text with a trailing `›`; the row or card highlights on hover and on keyboard focus | Opens a modal, a detail view or another tab | 3, 4, 5, 10 |
| **Menu**: a small caret `▾` after the label | Opens a menu | 1, 2 |
| **Edit**: a small pencil | Takes you to the control that owns this value | 7 |
| **Explain**: dotted underline (pointer); the existing info button (touch) | Shows an explanation | 11, 12 |
| **Hint line**: one muted line by the table or chart | Says in words what it responds to | Tables with any of 2, 3, 4, 5, 15; Gameflow (9) |

Interactions that already have their own conventional indicator keep it and
get no new glyph:

- **Sorting** keeps the sort arrows (DataTables headers, the Compare gap
  header, item 6). No caret.
- **Cycling a choice** (the lineup tokens, item 8) keeps the bordered-button
  look. No caret.
- **Adding** (the stat filter, item 14) keeps its `+`, with clearer wording.

Rules:

- A cue appears only where the interaction really exists. The pivot caret is
  styled off `data-pivot-trigger`, which is set only on rows that have a menu.
- Cues are visible at rest. Hover may strengthen a cue but never be the only
  sign of it.
- Colour stays inside the existing theme: amber accent `#e8a435` for Open,
  muted text colour for carets, pencils and hint lines. No new colours.
- Dense tables stay dense: glyphs are drawn in CSS (`::after`), not extra
  columns or wider cells, so they never reach CSV exports, sort values or
  copied text.
- An added pseudo-element must not replace a sort indicator or sit beside a
  touch info button doing the same job.
- Cues are attached by selector to what the renderers emit, so they survive
  sorting, filtering, view changes and reactive redraws.

## Specific changes

### Navbar tabs (item 1)

- Each tab with a `CFG` entry gets a caret and shows its active mode:
  `On/Off · Four Factors ▾`. The mode text updates wherever the menu's
  active row already updates (`syncMenu`, the row click handler).
- The caret and mode label are built in the existing `initOne()` so no tab is
  listed twice.
- Mobile already shows the menu inline in the burger for the active tab;
  the mode label applies there as well, the caret does not.
- Risk: navbar width. With every label widened, the bar may wrap at common
  laptop widths. Fallback, decided by measuring: show the mode text only on
  the active tab and the caret on all.

### Pivot name cells (item 2)

- `td[data-pivot-trigger]` and `.ibpl-row-name[data-pivot-trigger]` get the
  Menu cue, a pointer cursor, and a hover and focus state.
- Only the name cell carries the caret. The handler accepts the first two
  cells, so the second keeps its behaviour and gains the pointer and hover
  state only.

### Open targets (items 3, 4, 5, 10)

- `.ld-lineup-link` keeps its look and gains the `›`; `.euro-ld-unit` and
  `.ribbon-link` are given the same look, so both leagues match.
- Compare rows take the Open cue at both levels: the league table and the
  team-players table, whose rows open a further player comparison.
- Hub cards get a visible footer line naming the destination
  ("Open lineups ›"); storylines get the `›`.

### Sorting, cycling and toggles (items 6, 8, 13)

- Items 6 and 8 already carry their own indicator and are left as they are.
- Item 13 is assigned a cue once its current look is checked.

### Filter chips (item 7)

- `.chip-focusable` chips get a small pencil, so they differ at rest from
  informational chips (season, starters). The `×` keeps its meaning.

### Stat filter entry point (item 14)

- The chip reads `+ Stat filter` instead of `+ Filter`, in both places that
  build it, so it is distinct from the game-context filters and from the
  pencil on active chips. The popover title already says "Add stat filter".

### Explanations (items 11, 12)

- Pointer devices: `th[title]` gets the dotted underline, and so does the
  `.onoff-net-tip` span, which covers the Net number and its tags as one
  target (one underline, not one per nested part).
- Touch devices (`body.ibpl-touch-tips`, which covers phones and tablets):
  the existing info button is the cue. No underline is added there, and the
  capture-phase handler that stops an info tap from also sorting is untouched.

### Gameflow (item 9)

- The status line in `ribbon_pair_controls()` says what selecting gives:
  "Select two or more players, by their names or bars, to see their shared
  court time and lineups." The same default string in `app.js` (the
  `.ibpl-ribbon-pair-status` updater) changes with it. The instructions
  already shown once the shared-lineups section appears are kept.
- Player names are underlined at rest, as the selectable handle.

### Hint line

- One helper, `table_hint()`, in `app/R/helpers.R`, taking the list of
  interactions the table supports and returning the muted line. Both leagues'
  tabs call the same helper; no `euro_` variant.
- Wording comes from one lookup keyed by interaction, each with a pointer
  sentence and a touch sentence written out in full, because the action
  itself differs and not only the verb:
  - pointer: "Hover over a column header for its definition";
    touch: "Tap a header's info button for its definition" (a plain tap on a
    header sorts).
  - "Drag a column header to reorder" exists for pointer only and is left out
    of the touch line.
- Both lines are emitted and `body.ibpl-touch-tips` picks one. That class is
  the app's existing touch classification and includes tablets at desktop
  width; a phone-width condition would be wrong.
- Compare shows a hint at both levels (league table, team-players table).
- The reorder hint appears only where `ColReorder` is on: Tab 5, and Tab 3 in
  Traditional view.

## Where the code goes

| File | Change |
|---|---|
| `app/www/app.css` | Cue classes and the selectors above, in one "Clickable cues" block |
| `app/www/mobile.css` | Touch variants; hint-line wording switch |
| `app/www/app.js` | Caret and mode label in the view-mode `CFG` block |
| `app/R/helpers.R` | `table_hint()` and its wording lookup; stat-filter chip wording |
| `app/R/ui_tab*.R` | One `table_hint()` call per table |
| `app/R/mod_team_hub.R` | Card footer line |
| `app/R/mod_ribbon_modal.R` | Gameflow status wording |
| `app/R/global.R` | Pencil in `make_chip()` |
| `app/R/server_tab5_traditional.R`, `server_tab7_compare.R` | Stat-filter chip wording; second-level Compare hint |

Constraints carried from `CLAUDE.md`:

- The on/off renderer is a `sprintf(paste0(...))` template: a literal `%`
  must be `%%`, and no `//` comments.
- `app.R`, `global.R`, `helpers.R` and `CLAUDE.md` have mixed line endings;
  edits there are spliced so the diff stays the size of the change.
- No raw non-ASCII in R source (`test-source-encoding.R`). Glyphs live in CSS
  `content` escapes, or `intToUtf8()` where R must emit them.
- Run the local app with `IBPL_CACHE_UI=false` while editing `www/` files.

## Testing

- Unit: `table_hint()` returns the expected phrases per interaction set and
  emits both click and tap variants.
- Renderer tests: each interactive renderer emits its cue class
  (lineup link, ribbon link, pivot trigger attributes unchanged).
- Existing `test-navbar-hover-menu.R`, `test-mobile-layer.R`,
  `test-lineup-player-chips.R` and `test-source-encoding.R` stay green.
- Browser, via the `run-shiny-local` skill, at desktop width and at phone
  width, for each tab in both leagues: every inventory item shows its cue at
  rest; the navbar does not wrap; table row height and column widths are
  unchanged apart from the glyphs.
- Cues are still present after sorting, filtering, a view change and a
  reactive redraw.
- On touch, tapping a header's info button explains and does not sort.
- CSV exports and sorted values contain no decorative glyph or hint text.
- Touch wording and info buttons work at tablet width as well as phone width.
- Israeli and EuroLeague lineup links look the same; Compare checks run at
  both levels.
- No added pseudo-element hides a sort arrow or doubles a touch info button.

## Order of work

1. Walk the running app; close the "to verify" rows.
2. Navbar caret and mode label (item 1).
3. Pivot cells (item 2).
4. Open targets, remaining menus, chips, explanations.
5. Hint line.
6. Full browser pass, desktop and phone.

Items 2 and 3 fix the two interactions with no signal at all and are worth
shipping on their own if the rest slips.

## Note on the starting state

The working tree is on `infra/intro-video`, which is unmerged and differs
from `main` in `app/www/app.js`, `mobile.css` and `mobile.js`, and there are
uncommitted edits to `app/www/app.js` and several `app/R` files. The branch
for this work starts from `main`; how to move onto it without disturbing
those edits is settled before implementation starts.
