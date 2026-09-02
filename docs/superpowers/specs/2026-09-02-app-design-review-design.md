# App design review — audit, decisions, and scope

Date: 2026-09-02. Covers the Shiny app (`app/`). `frontend-v2` is archival and
out of scope.

## 1. What the system is today

**Visual.** bslib BS5 theme at `app/app.R:56`: `bg #0d1117`, `fg #e6edf3`,
`primary #e8a435`, DM Sans + JetBrains Mono, no display face. `app/www/app.css`
is 1,068 lines with **277 raw hex occurrences across 24 distinct colours** and
no custom properties. A further **117 hex occurrences live in R files** as
inline `style=` attributes and DT JS renderers. `app/www/app.js` contains none.

**Interaction.** Twelve tabs, one shape each: `sidebarLayout(sidebarPanel(width
= 3), mainPanel(width = 9))`, a chips row, and a wide DataTable. The navbar
owns league + season. Home owns the team hub plus a 2x2+1 grid of nav cards.
There is no `plotOutput` anywhere in the app — every visualization is a CSS
micro-mark inside a table cell.

## 2. What is working and must be preserved

- **The Four Factors cell** (`app/www/app.css:341-364`): a large value, a
  league-range track, on-court and off-court dots positioned on that track, and
  small raw numbers beneath. This is the app's best idea and the grammar the
  rest of the app should converge on.
- **Question-shaped labels** on Home ("Who is helping my team?").
- **The team hub** — the only surface that answers before it is asked.
- **`filter_chips_row()`** (`app/R/global.R:1085`) and `build_filter_chips()`
  (`app/R/global.R:860`) as a genuinely shared, league-parameterised component.
- The delegated click handler at `app/www/app.js:291` with its pre-connect
  queue and replay.

## 3. Findings

### F1 — No design token layer

394 hard-coded colour values across CSS and R. Any palette change is a
394-site edit, so the palette is effectively frozen.

### F2 — The palette is GitHub Primer Dark, hex for hex

Competent and completely generic; it signals *developer tool*. The subject is a
warm, glare-lit, lacquered-maple, scoreboard world. Amber-on-cool-near-black is
also the single most common look AI-assisted design converges on.

### F3 — The diverging ramp is not colourblind-safe, and is not even monotonic

`COLS_GRAD <- colorRampPalette(c("#8b2020", "#6b5a20", "#1a6b38"))(20)` at
`app/R/global.R:88` drives every percentile-coloured cell. Measured WCAG
relative luminance across the 20 steps:

| | ends | span | strictly monotonic | min quintile ratio |
|---|---|---|---|---|
| current | 0.0663 .. 0.1102 | 1.7x | **no** (min step -0.00092) | **1.02x** |

Hue is the only reliable signal, and hue is exactly what red-green deficiency
removes. Quintiles 3 and 4 differ in luminance by 2%, so "average" and "good"
are indistinguishable once hue collapses. The bad end remains readable; the top
half of the scale does not.

### F4 — No display face, and no tabular numerals

`font-variant-numeric` appears nowhere in the codebase. DM Sans defaults to
proportional figures, so number columns shimmer and mis-align on sort — in an
app whose entire output is columns of numbers. There is also no display face at
all: the brand, section headers and big hub numbers are all body text.

### F5 — Colour is the sole encoding of the verdict, and everything is encoded

The On/Off Summary view colours 16 shooting columns plus net/off/def at equal
weight. The Four Factors view already solved this with hierarchy; Summary never
got the upgrade. Nothing carries a redundant non-colour cue (WCAG 1.4.1).

### F6 — Filters are read in one place and edited in another

Chips render above the table (`filter_chips_row()`); the controls live in the
far-left sidebar accordion. The chip is the summary and the dismiss affordance
but not the entry point.

### F7 — The sidebar permanently spends 25% of viewport width

`sidebarPanel(width = 3)` on all ten data tabs, on tables that want every pixel.
The "Show Filters" collapse button is `d-md-none` — mobile only.

### F8 — No cross-tab pivot

`shared$pending_ld_team` / `pending_gl_team` / `pending_compare_preset`
(`app/app.R:540-542`) show the handoff pattern was built for three specific
Home-card paths and never generalised. Finding a player in On/Off and wanting
their lineups means a new tab and re-entering the team and player.

### F9 — Home double-serves

The team hub answers the five questions; the five nav cards below then ask them
again as identical amber-icon-plus-"Go" cards. The "Go" affordance does no job
the card is not already doing.

### F10 — Two pre-existing duplications in the affected code

The range-plot cell JS exists twice (`app/R/helpers.R:1868-1885` and
`app/R/server_tab1.R:440-460`), and the stat-filter chip popover exists twice
(`app/R/helpers.R:434` and `app/R/server_tab5_traditional.R:1565`).

### F11 — Baseline test failure on `main`

`app/tests/testthat/test-tooltips-contracts.R:23` asserts
`tt("Min possessions per side (eligibility):", "min_poss_side")` appears in
`ui_tab1_onoff.R`. Commit 35ecb34 moved that slider onto the chips row, where
it now reads
`minposs_slider("min_all_poss", "Min Poss / side", "min_poss_side", ...)`
(`app/R/ui_tab1_onoff.R:180`). The contract is stale, not broken code.

## 4. Decisions

**D1 — Warm the ground, keep the amber.** Shift the neutral ramp from Primer's
cool blue-black (hue ~212 deg) to a warm near-black and warm greys (hue 30-36
deg), preserving each token's WCAG relative luminance so no contrast pair
changes. `#e8a435` is the identity and does not move. Verified
luminance-matched values, all deltas below 0.004:

| token | old | new | lum old | lum new |
|---|---|---|---|---|
| `--ibpl-bg` | `#0d1117` | `#14100C` | 0.00548 | 0.00546 |
| `--ibpl-bg-sunken` | `#141920` | `#1D1712` | 0.00948 | 0.00918 |
| `--ibpl-surface` | `#161b22` | `#1F1A14` | 0.01070 | 0.01081 |
| `--ibpl-surface-alt` | `#1a1f2b` | `#251E16` | 0.01374 | 0.01380 |
| `--ibpl-surface-2` | `#1c2333` | `#2A2117` | 0.01688 | 0.01642 |
| `--ibpl-surface-3` | `#21262d` | `#2A251F` | 0.01899 | 0.01914 |
| `--ibpl-surface-hover` | `#242d3d` | `#352B1F` | 0.02589 | 0.02584 |
| `--ibpl-border` | `#30363d` | `#3A342F` | 0.03604 | 0.03561 |
| `--ibpl-text-faint` | `#484f58` | `#534E47` | 0.07674 | 0.07743 |
| `--ibpl-text-dim` | `#6e7681` | `#7A756E` | 0.17857 | 0.17986 |
| `--ibpl-text-muted` | `#8b949e` | `#98938B` | 0.29137 | 0.29407 |
| `--ibpl-text-body` | `#c9d1d9` | `#D4D0CA` | 0.63028 | 0.63373 |
| `--ibpl-text` | `#e6edf3` | `#EEECE8` | 0.83862 | 0.83994 |

**D2 — Display face: Archivo.** Variable, free on Google Fonts, carries a real
width axis, and its condensed cut reads as scoreboard and jersey numerals
without being Bebas or Oswald. Roles: Archivo for the brand, section headers,
column headers and large numbers; DM Sans stays the body face; JetBrains Mono
stays for dense inline data. Loaded through the existing link in
`shared_head_tags()` (`app/R/global.R`).

**D3 — Keep green/red hue, fix the ramp and add a redundant cue.** Green-good
and red-bad is a strong convention in sports statistics and is not worth
breaking. Replace the anchors so luminance carries the signal on its own, and
give the verdict columns the range-track mark so position is a second,
non-colour encoding. Verified:

| | ends | span | strictly monotonic | min quintile ratio |
|---|---|---|---|---|
| current | 0.0663 .. 0.1102 | 1.7x | no (-0.00092) | 1.02x |
| `c("#6e2622", "#615641", "#2f7f4d")` | 0.0482 .. 0.1632 | 3.4x | **yes** (+0.00233) | **1.28x** |

**D4 — Filter panel collapses; chips reveal their control.** Shiny input ids
must be unique in a session, so a chip cannot host a live duplicate of the
sidebar's control. The chip therefore *opens the panel and focuses the owning
input* rather than editing in place. This is the honest version of chip-first
filtering under Shiny's constraint.

**D5 — One generalised handoff, not one reactiveVal per destination.**
`shared$pending_nav` holds `list(target = , ...payload)`; destinations call
`consume_pending_nav(shared, "<target>")`. The three existing `pending_*` vals
are left in place and untouched.

**D6 — Motion carries information only.** FLIP transitions on table redraw so
ranking movement is visible. Nothing ambient. Gated on `prefers-reduced-motion`.

## 5. Scope

In: design tokens, warm palette, display face and numerals, ramp fix, cell
hierarchy, FLIP motion, collapsible filters, chip-to-control reveal, pivot
menus, Home rail.

Out, deferred by decision:

- The **stint ribbon** (per-game player lanes over the game clock with score
  margin behind). It remains the strongest single addition available and the
  one the site would be known for, but it needs new SQL reads over `stints` and
  `df_pts_poss_lineups_longer_mv` and is a separate project.
- The **second half of F10**: the stat-filter popover duplicated between
  `app/R/helpers.R:434` and `app/R/server_tab5_traditional.R:1565`. Real
  duplication, but it sits outside the four workstreams chosen here and
  deduplicating it would bundle a restructure into a plan whose filter work is
  already touching `build_filter_chips()`. Worth its own small change. The
  range-cell duplication in the same finding *is* in scope, because Task 8 of
  the design-system plan has to touch both copies anyway.

## 6. Global constraints

- Israeli and EuroLeague tabs share code. Per root `CLAUDE.md`, never write a
  parallel `euro_` implementation — generalise the existing function and name it
  neutrally.
- Any change must hold for all twelve tabs, both leagues, or be explicitly
  scoped to one and say so.
- `IBPL_CACHE_UI=false` is required in the environment while editing
  `www/app.css` or `www/app.js`, or edits need an app restart rather than a
  browser reload.
- Launch with Run App / `runApp()`, never select-all + Ctrl+Enter.
- Navbar health check: the navbar markup carries 11 `class="nav-link"` occurrences, one per tab. The served page also inlines `app.css` (8 further `nav-link` mentions, in selectors) and `app.js` (1), so a naive whole-page grep reads about 20 and is not the check.
- Colour changes are verified by computed WCAG relative luminance, never by eye.
