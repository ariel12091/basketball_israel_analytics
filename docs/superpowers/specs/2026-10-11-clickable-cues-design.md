# Clickable cues: make every interaction visible

Date: 2026-10-11
Branch: `shiny/clickable-cues` (from `main`)
Status: design approved in conversation, awaiting spec review

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
| 3 | Lineup name opens the game-log modal | Tabs 2, 10 (`.ld-lineup-link`) | Underlined amber. Already clear |
| 4 | Gameflow link opens the stint ribbon | Game logs (`.ribbon-link`) | To verify in the running app |
| 5 | Table row opens the Compare detail view; player click in team players | Tab 7 | Pointer cursor only |
| 6 | Compare "gap" header sorts | Tab 7 (`.cmp-col-gap`) | Pointer cursor only |
| 7 | Filter chip jumps to its control | All tabs (`.chip-focusable`) | Pointer cursor only |
| 8 | "at least / exactly" and count tokens toggle | Tabs 2, 10 (`.lineup-chips-token`) | To verify in the running app |
| 9 | Ribbon lanes, player names and detail rows select or pin | Gameflow modal | Pointer cursor only |
| 10 | Hub cards and storylines navigate to a tab | Home (`.hub-card`, `.hub-story-line`) | Border change on hover |
| 11 | Column headers explain themselves | All tables (`th[data-tooltip]`, `COLUMN_TOOLTIPS`) | `cursor: help` only |
| 12 | On/Off Net tags (3PT luck, small sample) explain themselves | Tabs 1, 8 (`.onoff-net-tip`) | `cursor: help` only |
| 13 | Four Factors range toggle, luck explainer | Tabs 1, 8 | To verify in the running app |

Rows marked "to verify" are checked in the running app as the first
implementation step; any further interaction found there is added to this
table and gets a cue from the vocabulary below, not a new one.

## The cue vocabulary

Four cues. Each has one meaning.

| Cue | Meaning | Applies to |
|---|---|---|
| **Open**: amber text with a trailing `›`; the row or card highlights on hover and on keyboard focus | Opens a modal, a detail view or another tab | 3, 4, 5, 10 |
| **Menu**: a small caret `▾` after the label | Opens a menu or cycles a choice | 1, 2, 6, 8 |
| **Explain**: dotted underline, plus a small `ⓘ` where there is room | Shows an explanation on hover or tap | 11, 12 |
| **Hint line**: one muted line above the table | Says in words what this table responds to | Every table with at least one of 2, 3, 4, 5 |

Rules:

- A cue appears only where the interaction really exists. The pivot caret is
  styled off `data-pivot-trigger`, which is set only on rows that have a menu.
- Cues are visible at rest. Hover may strengthen a cue but never be the only
  sign of it.
- Colour stays inside the existing theme: amber accent `#e8a435` for Open,
  muted text colour for carets, info marks and hint lines. No new colours.
- Dense tables stay dense: the caret and `ⓘ` are small glyphs drawn in CSS
  (`::after`), not extra columns or wider cells. Header `ⓘ` is dropped where
  the header has no room, leaving the dotted underline.

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

- `.ld-lineup-link` keeps its look and gains the `›`.
- `.ribbon-link`, Compare rows and Compare player names take the Open cue.
- Hub cards get a visible footer line naming the destination
  ("Open lineups ›"); storylines get the `›`.

### Remaining menus and toggles (items 6, 8, 13)

- Compare gap header and the lineup summary tokens take the Menu cue.
- Item 13 is assigned a cue once its current look is checked.

### Filter chips (item 7)

- `.chip-focusable` chips get a small edit mark, so they differ at rest from
  informational chips (season, starters). The `×` keeps its meaning.

### Explanations (items 11, 12)

- `th[data-tooltip]` gets the dotted underline.
- The 3PT luck and small-sample tags get the dotted underline and `ⓘ`.

### Hint line

- One helper, `table_hint()`, in `app/R/helpers.R`, taking the list of
  interactions the table supports and returning the muted line. Both leagues'
  tabs call the same helper; no `euro_` variant.
- Wording comes from one lookup keyed by interaction, so a phrase is written
  once: "Click a player for their lineups", "Click a lineup for its game
  log", "Click a column to sort", "Hover a header for its definition".
- "Click" and "Hover" read "Tap" on touch layouts. Both variants are emitted
  and the existing viewport class picks one, so the server needs no device
  knowledge.

## Where the code goes

| File | Change |
|---|---|
| `app/www/app.css` | Cue classes and the selectors above, in one "Clickable cues" block |
| `app/www/mobile.css` | Touch variants; hint-line wording switch |
| `app/www/app.js` | Caret and mode label in the view-mode `CFG` block |
| `app/R/helpers.R` | `table_hint()` and its wording lookup; `›` in the link renderers |
| `app/R/ui_tab*.R` | One `table_hint()` call per table |
| `app/R/mod_team_hub.R` | Card footer line |

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
