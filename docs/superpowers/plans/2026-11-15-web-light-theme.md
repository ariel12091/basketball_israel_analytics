# Web Light Theme Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A light view of the whole web app, switched by a navbar toggle and remembered per visitor, with dark output unchanged.

**Architecture:** `<html data-theme>` is set before first paint by an inline script (stored choice, else `prefers-color-scheme`, else dark). A new `src/app/light.css`, imported last, redefines the `--ibpl-*` and `--bs-*` variables under `html[data-theme="light"]` and overrides hard-coded colours. Colours computed in TypeScript keep their dark value inline, and also expose it as a CSS custom property plus a class, so `light.css` can tint (backgrounds) or shade (text) them with `color-mix()`. No React state is involved, so there's no hydration mismatch.

**Tech Stack:** Next.js (App Router), React 19, TypeScript, vitest, Playwright, plain CSS.

**Spec:** `docs/superpowers/specs/2026-11-15-web-light-theme-design.md`

## Global Constraints

- Never edit `web/src/styles/shiny/*` (byte copies of Shiny, pinned by `src/styles/shiny.test.ts`).
- Dark output must be identical: every existing unit test and browser spec passes unchanged (e.g. `e2e/heat.spec.ts` expects `rgb(110, 38, 34)`).
- localStorage key: `ibpl_theme`, values `"light"` | `"dark"`. Attribute: `html[data-theme="light"|"dark"]`.
- Every light rule is scoped under `html[data-theme="light"]`.
- Text contrast in light: at least 4.5:1 for body text, numbers in tables and amber text.
- The navbar row count at 1280/1366/1440/1600 px (after "Last updated" loads) must not change.
- Run npm via PowerShell (`npm.cmd`); write files containing backslashes with Edit/Write; never e2e while the user tests (workers 1-2).

## Review Focus

1. Storage blocked (private mode, `localStorage` throws): the page still renders in the system theme and the toggle still flips it for the session. Test in Task 1.
2. Toggling with a modal open (Gameflow, lineup modal, Glossary, Ask panel): everything already on screen flips at once, with no stale dark islands. Test in Task 4.
3. Phone: `mobile.js` moves `#navbar_right_cluster` into the collapsed menu, and the toggle must still work there. Test in Task 2.
4. Heat cells in light: the number must stay legible on the tinted background (cell text turns dark), including the TOTAL row, which has no heat. Test in Task 3.
5. Back/forward cache and CDN-cached HTML: a reload after toggling shows the new theme on first paint. Test in Task 1.

---

### Task 1: Theme resolution and pre-paint script

**Files:**
- Create: `web/src/lib/theme.ts`, `web/src/lib/theme.test.ts`, `web/src/components/shiny/ThemeScript.tsx`
- Modify: `web/src/app/layout.tsx` (render `<ThemeScript />` first in `<head>`; `<html suppressHydrationWarning>`)
- Test: `web/e2e/theme.spec.ts`

**Interfaces:**
- Produces: `type Theme = "light" | "dark"`; `resolveTheme(stored: string | null, prefersLight: boolean): Theme`; `THEME_KEY = "ibpl_theme"`; `THEME_SCRIPT: string` (sets `data-theme` and `style.colorScheme` on `document.documentElement`, try/catch around storage); `applyTheme(t: Theme): void` (sets attribute plus colorScheme, writes storage in try/catch).

- [ ] **Step 1: Write failing unit tests** in `theme.test.ts`: `resolveTheme("light", false) === "light"`; `resolveTheme("dark", true) === "dark"`; `resolveTheme(null, true) === "light"`; `resolveTheme(null, false) === "dark"`; `resolveTheme("bogus", true) === "light"`. Also a test that evaluates `THEME_SCRIPT` with a stubbed `localStorage` whose `getItem` throws, and expects `data-theme` to come from `matchMedia`.
- [ ] **Step 2: Run** `npm.cmd test -- theme` → FAIL (module missing).
- [ ] **Step 3: Implement** `theme.ts`. `THEME_SCRIPT` is a self-contained IIFE string that inlines the same rule as `resolveTheme` (keep the two in one file; the test pins them together). `ThemeScript.tsx` follows `BodyClassScript.tsx`'s pattern exactly (`type="text/plain"` in the browser).
- [ ] **Step 4: Run** `npm.cmd test -- theme` → PASS.
- [ ] **Step 5: Write** `e2e/theme.spec.ts`, which covers three cases:
  - With `localStorage.ibpl_theme = "light"` set via `addInitScript`, the first `document.documentElement.dataset.theme` read in a `DOMContentLoaded` listener added by `addInitScript` is `"light"`.
  - With `emulateMedia({ colorScheme: "light" })` and no storage, the theme is light.
  - The default is dark.
- [ ] **Step 6: Run** `npx.cmd playwright test theme --workers=1` → PASS.
- [ ] **Step 7: Commit** `feat(web): resolve theme before first paint`.

### Task 2: Navbar toggle

**Files:**
- Create: `web/src/components/shiny/ThemeToggle.tsx`
- Modify: `web/src/components/shiny/Navbar.tsx` (render `<ThemeToggle />` in the cluster, before the Glossary button)
- Test: `web/e2e/theme.spec.ts`

**Interfaces:**
- Consumes: `applyTheme`, `Theme` from `@/lib/theme`.
- Produces: `<ThemeToggle />`, `button#theme_toggle.nav-theme-btn`. It reads the current theme from `document.documentElement.dataset.theme` in an effect (render the dark-state icon on the server and first client render, then sync), shows `bi-sun` in dark and `bi-moon` in light, and has `aria-label` "Switch to light theme" / "Switch to dark theme". It uses the Glossary button's classes (`btn btn-sm btn-outline-secondary nav-help-btn`), is icon only, and fixes no width.

- [ ] **Step 1: Add e2e cases.**
  - Clicking `#theme_toggle` sets `data-theme="light"` and stores `ibpl_theme=light`. After a reload, the theme is still light and the button's label is "Switch to dark theme".
  - At 390 px, open the navbar menu, find `#theme_toggle` inside it, click it, and the theme flips.
  - At 1280/1366/1440/1600 px, once `#last_updated` has text, `#main_tabs` has the same `getBoundingClientRect().height` with and without the toggle. Compare against `main` by hard-coding the measured heights from a run on `main` into the test.
- [ ] **Step 2: Run** → FAIL (no button).
- [ ] **Step 3: Implement** `ThemeToggle.tsx` and wire it into `Navbar.tsx`. If the 1440 px row height changes, hide the button's border/padding (icon only), not the date.
- [ ] **Step 4: Run** `npx.cmd playwright test theme navbar --workers=1` → PASS.
- [ ] **Step 5: Commit** `feat(web): theme toggle in the navbar`.

### Task 3: Light palette, heat and shot colours

**Files:**
- Create: `web/src/app/light.css`
- Modify: `web/src/app/layout.tsx` (import `./light.css` after `./web.css`). In `web/src/components/shiny/DataTable.tsx`:
  - the `td` style at ~line 689 becomes `{ backgroundColor: cell.bg, "--ibpl-heat": cell.bg }` plus the class `ibpl-heat` when `cell.bg` is set;
  - `shotCell`'s `acc()` colours and the `#aaa`/`#bbb` muted colours set `--ibpl-acc` plus the class `ibpl-acc` alongside the inline `color`.

  In `TableKey.tsx` and `Blocks.tsx`, the swatches set `--ibpl-heat` plus `ibpl-heat` the same way.
- Test: `web/e2e/theme.spec.ts`

**Interfaces:**
- Produces:
  - CSS hooks `.ibpl-heat` (reads `--ibpl-heat`) and `.ibpl-acc` (reads `--ibpl-acc`).
  - The light tokens. Starting values, which the builder adjusts for contrast:

    | Group | Values |
    |---|---|
    | Background, surfaces | `--ibpl-bg #FAF8F4`, `--ibpl-bg-sunken #F2EEE7`, surfaces `#FFFFFF` / `#F6F3EE` / `#EFEAE2`, hover `#EAE3D8`, selected `#F6E7C8` |
    | Border, text | border `#DDD6CC`; text faint `#B5ADA2`, dim `#857D72`, muted `#5E574E`, body `#2E2924`, text `#1F1A14`, cell-text `#1F1A14` |
    | Accent | accent `#B5760F` (text), accent-hover `#9A630C` |
    | Status | pos `#0F8A5F`, neg `#C2413B`, info `#2F6FC0` |

    Also update every `*-rgb` twin, and the `--bs-*` variables from the vendor file, which runs inverted: `--bs-white` becomes light and `--bs-black` dark.
- Light rules:
  - `.ibpl-heat { background-color: color-mix(in srgb, var(--ibpl-heat) 30%, var(--ibpl-surface)) !important; color: var(--ibpl-text) }`
  - `.ibpl-acc { color: color-mix(in srgb, var(--ibpl-acc) 55%, #000) !important }`
  - The exact percentages are the builder's, checked to reach 4.5:1 against the tinted cell.

- [ ] **Step 1: Add e2e cases.**
  - In light, the `heat.spec.ts` "bad" cell's background is no longer `rgb(110, 38, 34)` and its luminance is above 0.6.
  - The cell's computed `color` against its background reaches at least 4.5 contrast; compute it in the test from `getComputedStyle`.
  - `body` background is `rgb(250, 248, 244)`.
  - Re-run `heat.spec.ts` unchanged in dark.
- [ ] **Step 2: Run** → FAIL.
- [ ] **Step 3: Implement** `light.css` (tokens, `--bs-*`, `.ibpl-heat`, `.ibpl-acc`, `color-scheme: light`) and the TS changes above.
- [ ] **Step 4: Run** `npm.cmd test` (all unit) and `npx.cmd playwright test heat theme --workers=1` → PASS.
- [ ] **Step 5: Commit** `feat(web): light palette and themed heat colours`.

### Task 4: Hard-coded colours, every tab

**Files:**
- Modify: `web/src/app/light.css` only.
- Test: `web/e2e/theme.spec.ts`

**Interfaces:**
- Consumes: the light tokens from Task 3.

- [ ] **Step 1: Screenshot every page in light at 390 and 1440 px**: Home, On/Off, Lineups, Team Ratings, Game Logs, Player Stats, Compare, Gameflow (plus the open modal), Ask panel, Glossary, lineup modal, the selectize dropdown, the datepicker and the range slider, for the Israeli league and EuroLeague. Save them to the scratchpad.
- [ ] **Step 2: For each dark island or illegible element, add an override to `light.css`.**
  - Starting list: the ~30 hex and ~53 `rgba()` values in `src/styles/shiny/app.css` and the 15 hex / 32 `rgba()` in `src/app/web.css`. Grep them, e.g. `rgba(0,0,0,...)` shadows and overlays, and white-on-white text.
  - Keep the overrides grouped by source selector, with a comment naming the file.
- [ ] **Step 3: Add an e2e case:** open the Gameflow modal in light, toggle to dark, and assert that the modal's background is the dark `--ibpl-surface` without a reload. This covers Review Focus 2.
- [ ] **Step 4: Re-screenshot and compare.** Run the full e2e suite once (`--workers=2`) to confirm dark is unchanged.
- [ ] **Step 5: Commit** `feat(web): light overrides for hard-coded colours`.

### Task 5: Preview

- [ ] **Step 1:** Push `react/light-theme` and open a PR; wait for `web-preview-live.yml` to pass on the head.
- [ ] **Step 2:** Spot-check the Preview in a browser in light (phone and desktop) and send the user the screenshots.
- [ ] **Step 3:** Stop. Merging deploys production and needs the user's explicit "deploy".
