# Web app light theme -- design (2026-11-15)

## Intent

The dark theme is sometimes hard to read or unappealing (user). Add a light
view with a toggle, covering every part of the web app (`web/`): Home, On/Off,
Lineups, Team Ratings, Game Logs, Player Stats, Compare, Gameflow, Ask, both
leagues, desktop and phone.

Success: a visitor who prefers light can switch once and read every tab
comfortably, with the heat/shot colours still telling good from bad at a
glance.

## Goals / non-goals / anti-goals

- Goal: a light palette with real contrast (WCAG AA for body text and table
  numbers), not a cosmetic tint.
- Goal: a sun/moon toggle in the navbar right cluster, desktop and phone,
  remembered per visitor.
- Non-goal: Shiny (`app/`). It stays dark and retires at the cut-over.
- Non-goal: changing anything in the dark theme. Dark output stays
  pixel-identical; the existing browser specs must pass unchanged.
- Anti-goal: a dark flash before light applies on every page load.
- Anti-goal: heat colours that become ambiguous or illegible on white.

## Constraints

- `src/styles/shiny/app.css` and `mobile.css` are byte-for-byte copies of
  Shiny's, pinned by `src/styles/shiny.test.ts`. They must not be edited.
- The vendor `bootstrap.min.css` was compiled dark (`--bs-white` is
  near-black); its `--bs-*` variables need light values too.
- Tab pages are CDN-cached for an hour, so the server cannot vary the HTML
  per visitor. The theme is applied client-side, before first paint.
- Budget: no extra requests per page (no new font/CSS fetches beyond the
  bundle).

## Approaches

- **A. Light token set + toggle (chosen by the user).** A new
  `src/app/light.css`, imported after `web.css`, redefines the `--ibpl-*`
  tokens and the `--bs-*` Bootstrap variables under
  `html[data-theme="light"]`, plus targeted overrides for the ~80
  hard-coded colours in `app.css`/`web.css` that the theme reaches.
- B. A second full stylesheet plus a light Bootstrap build: duplicates ~3,000
  lines that would drift. Rejected.
- C. `filter: invert()`: inverts the heat colours' meaning. Rejected.

## Design

1. **Theme state.** `<html data-theme="dark|light">`. An inline script in
   `layout.tsx`'s `<head>` (runs before paint) sets it from
   `localStorage["ibpl_theme"]` if present, otherwise from
   `prefers-color-scheme: light`, otherwise dark. Wrapped in try/catch
   (storage can throw); `<html>` gets `suppressHydrationWarning`.
2. **Toggle.** A button in `Navbar.tsx`'s right cluster, a sun icon in dark
   and a moon icon in light (bootstrap-icons, already loaded), with an
   `aria-label` naming the theme it switches to. Click flips the attribute and writes
   `localStorage`. It must not change navbar row count at 1280/1366/1440/
   1600 px after "Last updated" loads (the navbar has ~38 px spare at 1440;
   measure, and take the width from padding, not a new row).
3. **Palette (`light.css`).** Warm off-white ground matching the editorial
   look (e.g. bg ~#FAF8F4, surfaces white / ~#F2EEE7, borders ~#DDD6CC,
   text ~#1F1A14 / muted ~#5E574E). Accent amber darkened for contrast on
   white (text uses of `--ibpl-accent` must reach 4.5:1; a fill may stay
   brighter). Pos/neg/info and side/fg colours get darker variants. Exact
   values are the builder's, checked for contrast.
4. **Computed colours.** `DataTable.tsx`'s shot-accuracy `acc()` and the
   `#aaa`/`#bbb` muted colours, `teamRatings.ts`'s colour ramp and any other
   JS-built colour pick their endpoints per theme. Dark endpoints are the
   current numbers, so dark output is unchanged. Light endpoints are darker,
   more saturated text colours (or lighter fills for background-heat cells)
   chosen for contrast on white. React components read the theme from a
   small hook (`useTheme()`), driven by the `data-theme` attribute, so a
   toggle re-renders them; the first render matches the pre-paint attribute.
5. **Gameflow SVG, charts, Ask panel, modals, selectize, datepicker,
   range slider, DataTables.** All use the tokens or vendor CSS; each gets
   checked in light and fixed in `light.css` where it hard-codes colours.
6. **`color-scheme`** set to `light` / `dark` with the attribute, so
   scrollbars and native form controls follow.

## Verification

- Unit: the theme script's resolution order; the colour functions return
  today's values in dark and contrasting values in light.
- Browser: existing specs unchanged (dark). A new spec loads each tab at 390
  and 1440 px in light, checks no dark flash (attribute set before
  first paint), and checks the toggle persists across reload.
- Manual screenshot pass of every tab in light, desktop and phone, before
  the Preview deploy.

## Left to the builder

Exact light colour values; which hard-coded colours need overrides (found by
screenshotting each tab); icon choice; the hook's implementation.
