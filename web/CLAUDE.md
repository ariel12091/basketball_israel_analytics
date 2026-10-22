# web/ -- the Next.js app replacing Shiny (rules for this directory)

Read `docs/phase0_execution_status.md` (START HERE section) for the current
state, and `docs/superpowers/specs/2026-10-20-react-migration-design.md` for
the design. The root `CLAUDE.md` and `docs/shared_definitions.md` still apply.

## Current resume point (2026-10-22)

Player Stats is implemented for all three competitions, all four display modes,
canonical selectors, TOTAL rows, rate eligibility and full-population heat ranks.
27 Shiny replays, 222 unit tests, 114 browser checks (including 48 shared
regressions) and six live SSR checks pass. Ready Preview:
https://ibpl-game-logs-91aourr8p-ariel12091-8759.vercel.app/il/player-stats?season=2026
Next: Gameflow + Home, then Compare. Commit and push checkpoints as requested.
Port 3100 currently runs the production build (not `next dev`); stop/check it
before changing server mode. The handoff has final verification details.
The Lineups/OnOff entries below are historical.

Lineups is implemented for all three competitions, with 92 browser checks,
125 unit tests and 36 Shiny replay presets passing. Ready Preview:
https://ibpl-game-logs-db5d7rnxc-ariel12091-8759.vercel.app/il/lineups?season=2026
Included in the Lineups migration checkpoint. Next: Team Ratings. The newest handoff entry
supersedes the historical On/Off continuation below.

Checkpoint `d361610` plus an uncommitted nuqs 2.10.2 dependency fix: On/Off
browser acceptance now passes locally (102/102, plus 28/28 repeated last-N and
interaction checks). Shared Game Logs regressions pass 40/40. Lineups is next.
The old Preview does not yet include the fix. See `../MEMORY.md` and the handoff for logs,
Preview, fixture generation and disk cleanup. After restart, check disk space
and whether port 3100 is listening before starting the one shared dev server.

## Direction (user, 2026-10-21)

1. **Features first.** Move every Shiny feature -- filters, behaviours and the
   table layouts -- to Next.js/React on Vercel, in the spirit of the Shiny app.
   Do not redesign and do not polish pixels: a later redesign will put all
   devices under one design and declutter the stats pages with a richer React
   layout. Stop at "same features, same numbers, Shiny's look".
2. **One layout per viewport width, mobile first.** Every device at a given
   width gets the same page; the phone layout is the default and wider
   screens add to it. Never port Shiny's user-agent device sniffing
   (`mobile.js`); width rules live in `src/components/shiny/layoutClasses.ts`.
3. **Idiomatic React, not a CSS remake.** Reusing Shiny's CSS for the look is
   fine until the redesign; the code underneath must be proper React/Next.js:
   - tables on TanStack Table (`components/shiny/DataTable.tsx`): Shiny's
     ordering rules are sorting functions, not a hand-written sorter;
   - static text as JSX from structured content (`components/shiny/Blocks.tsx`);
     no `dangerouslySetInnerHTML` (the pre-paint body-class script in
     `layout.tsx` is the one exception);
   - show/hide, menus and popovers driven by React state, not DOM class
     toggling; imperative DOM code only where the platform needs it (focus,
     scroll position);
   - pages are server components that render the first data load on the
     server with the same function the API route calls; the client takes
     over for later filter changes.
4. **Reuse the tab kit.** Every tab is built from `components/shiny/TabKit.tsx`
   (URL state, data loading, frame, filters accordion, chips, stat-filter
   popover) and `DataTable`. Extend them instead of copying a tab.

## How a tab moves across

- Start from the feature inventory
  (`docs/superpowers/specs/2026-10-19-api-mcp-feature-inventory.md`) and the
  tab's Shiny files read end to end; do not re-survey what the inventory lists.
- Data: an API route under `src/app/api/v1/[league]/...` calls the SQL owners
  Shiny calls, with their argument lists unchanged, as `app_readonly`, one
  snapshot per request (`server/db.ts`). Game-context filters go through
  `server/contextParams.ts`. Heavy readers get Shiny's 20 s budget
  (`new Deadline(25000, 20000)`) until Phase 8 makes them fast.
- Rules derived from returned rows go in `src/rules/` as faithful ports, each
  naming its R source (`helpers.R`, `server_tab*.R`), with R's rounding
  (`rRound`), `percent_rank` and `quantile(type = 7)` semantics.
- Static content, constants and Shiny's compiled CSS come from
  `scripts/export_web_shiny_ui.R` (run it after changing the R UI); never
  retype R text or constants. `npm run sync-styles` copies `app/www/app.css`
  and `mobile.css` byte for byte (pinned by `src/styles/shiny.test.ts`).
- Prove parity: browser tests against stored Shiny goldens at 390 and 1440 px
  (tests send `x-ibpl-client-data: 1` so they can substitute answers in the
  browser), plus the opt-in live smoke (`LIVE_DB_SMOKE=1`).

## Practicalities

- Run npm/npx through PowerShell (`npm.cmd`, `npx.cmd`), not the Bash tool.
- Write files containing backslashes with the Write/Edit tools; heredocs in
  the Bash tool collapse doubled backslashes.
- PowerShell `Get-Content`/`Set-Content` without `-Encoding utf8` corrupts
  non-ASCII (it read "×" as mojibake once); prefer the edit tools.
- `prettier --write src` must not touch generated files: `.prettierignore`
  lists `src/styles/shiny/`, `src/data/`, `public/images/`.
- Only one `next dev` may run per directory; the shared dev server is on
  port 3100 (`playwright.config.ts` reuses it).
