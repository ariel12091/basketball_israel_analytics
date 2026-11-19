# web/ -- the Next.js app replacing Shiny (rules for this directory)

Read `docs/phase0_execution_status.md` (START HERE section) for the current
state, and `docs/superpowers/specs/2026-10-20-react-migration-design.md` for
the design. The root `CLAUDE.md` and `docs/shared_definitions.md` still apply.

## Deploying (since 2026-10-27)

`web/` lives on `main`, and **Vercel Production
(https://ibpl-game-logs.vercel.app) builds every push to `main` that changes
`web/`**. A push to `main` also redeploys Shiny on Connect Cloud.

- Work on a short branch off `main` (`react/<name>`). Each push gets a Vercel
  Preview and the live checks (`.github/workflows/web-preview-live.yml`).
  Merge into `main` only after those passed on the branch's head commit.
- Changing visible wording (a label, a prompt, an idle state)? Grep `e2e/`
  for the old text before pushing: the live specs pin it, and the Preview
  check fails otherwise (PR #61: `gameflow-live.spec.ts` expected "Nothing
  selected").
- Load test: push the feature branch head to `load-test/<name>`, delete it
  afterwards (`web-preview-load.yml`). Never during the nightly ETL; each run
  costs about 0.35 GB of Fast Origin Transfer (Hobby: 10 GB/month).
- Rollback: Instant Rollback in the Vercel dashboard, or `vercel rollback`.
- The migration's history, handoffs and tests-only files are on the
  `sql/web-foundation` branch, which must not be merged whole (it also holds
  unapplied SQL/loader edits); the `docs/` paths named below live there.

## Gameflow (since 2026-11-12, live)

Runs, credited-lineup ratings and the phone layout are live (PRs #24-#26).
Contract rows: Gameflow stint rating, run, run standout, run cause. EuroLeague
credits a free throw to the lineup that drew the foul too since migration 062
(2026-11-13; D39 closed).
- Readers return `events` (each with its credited five) and `poss`;
  EuroLeague reads `euroleague.ribbon_events_v` (migration 061).
- Rules: `rules/gameflowRatings.ts` (`stintRatings`, `windowsRatings`),
  `rules/gameflowRuns.ts` (`detectRuns`, `runLineups`, `runStandouts`,
  `causeLine` via `runExplanation`, `periodClock`, `windowLabel`).
  `rules/gameflowSummary.ts` runs once on the server; the API returns
  `runs` / `players`, which the page and the Ask `gameflow` tool read.
- Every moment/window reads "Q3 2:25 – Q4 2:31" (`windowLabel`); durations
  stay m:ss.
- Phones (`GameflowView` compact): sticky header (strip + quarter tabs +
  Runs + menu), one quarter at a time (tabs or swipe), one bottom panel
  (`PhonePanel`) every tap reports to. Desktop keeps the old layout.
- Before any click (PR #61, 2026-11-15), each team's 5 most-played lineups
  (`gameLineups()`) fill the lineups area: wide behind a Hide/Show heading
  (folded, the chart is fitted as if nothing were open), on phones as the
  collapsed panel's default. A player, stint or run replaces them. Every
  lineup row's Net/ORtg/DRtg is tinted against the game's own rating
  (`ratingTone()`, `gameRating()`; no league constant).
- Club colours (every width, since 2026-11-17): each side in its own club's
  colours from the catalogue rows (`season_teams` / `euroleague.teams`,
  basket.co.il's colours where it has them), read by `server/teamIdentity.ts`
  into `meta.ownIdentity` / `oppIdentity`. `rules/teamColors.ts` pairs them
  (opponent on its second colour when the two clash, never a rival's colour;
  2:1 on the chart so yellow stays yellow) and `paletteVars()` sets both
  themes' `--gf-own-l/-d`, `--gf-opp-l/-d`; web.css picks the theme's. No
  colours on a row: the chart keeps amber/grey.
- Fixtures with events: `src/rules/__fixtures__/gameflow-{673,440,1147}.json`
  from `scripts/save_gameflow_fixture.R` (the real reader as app_readonly);
  e2e loads them with `loadFixture()`.
- Traps: `gameflow_export_sources()` also rewrites `gameflowLayout.json` and
  drops hand-added keys -- `git checkout` that file and commit only
  `gameflowSql.json`. mobile.css's `.ibpl-ribbon-inline-panel
  .ibpl-ribbon.is-compact { width: auto }` outranks web.css (a quarter chart
  overflowed its frame by 18 px); override with a more specific rule.
  Production live checks (`LIVE_DB_SMOKE=1`, `PREVIEW_URL` = production,
  any bypass value) hit Vercel's 429 when both widths run back to back: run
  phone and desktop separately, about 90 s apart.

## Player Stats views (since 2026-11-17, PR #84, live)

The user reads this table for league leaders and team rotations; looking
up one player belongs on a future player profile page, not here (user).
Spec: `docs/superpowers/specs/2026-11-17-player-stats-views-design.md`.
- `ViewTabs` pick the column set: Main (default), Shooting, All. URL key
  `cols` (`main|shooting|all`), kept out of the API path, so a switch never
  refetches. The mode (Totals / Per game / Per 60 poss / Per 30 min) is a
  `PillSwitch` beside the tabs; its URL key stays `view` with the old
  values, so old links still work.
- Column sets live in `playerViewColumns()` (`rules/playerStatsTable.ts`).
  The table draws every view in All's order filtered to that view, so a
  view's list must equal All's order restricted to it (unit test pins it;
  Poss/G broke this once). Add a column to Main only with the user.
- Columns menu: one stored order and hidden list for all views, each view
  listing only its own columns. Clear all keeps `cols` (user), resets mode.
- Counting columns carry `ibpl-heat-soft` (lighter tint, web.css); rates
  keep full heat.
- Stat filters apply after the mode conversion: in Per 60, FGA >= 15 means
  per 60 possessions, and they filter on columns the view hides.
- Known minors left: the Columns button can read "· 18/18" when the hidden
  columns are outside the view; Columns Reset clears every view.
- `e2e/player-stats.spec.ts` is not run in CI (only the live smoke is); its
  column-view test was written while C: was full and has not been run yet.

## Logos, headshots and heights (since 2026-11-19, PRs #91 #93 #95)

All from the database, both leagues; `images.json` and
`scripts/export_web_images.R` are deleted.
- Rows carry the URLs: `team_logo` / `opp_team_logo` (`applyTeamLogos()`,
  `server/teamIdentity.ts`, added by `decorateTeams()` in `teamNames.ts` and
  by Game Logs) and `player_photo` (`applyPlayerNames()`). `NameImage` reads
  the row; a new table gets images by calling those two, nothing else.
- `readPlayerNames()` has EuroLeague/EuroCup entries from
  `euroleague.player_season_profiles` (migration 067) with `en: null`: they
  rename nothing and carry height, position (G/F/C) and headshot, so
  EuroLeague lineups list shortest to tallest too. EuroLeague lineup strings
  are "FIRST LAST" in `player_ids` order, safe to split on ", ".
- Israeli headshots: `basket_player_registration.photo_url`, joined in
  `player_display_names_v` on identity and season, not team (a traded
  player keeps it on both teams); written by the Basket refresh.
- Provider headshots are ~500 KB PNGs: they must go through the optimiser
  (`next.config.ts` allows `media-cdn.cortextech.io`,
  `media-cdn.incrowdsports.com`). Hobby's transformation quota applies.

## Compare in every league (since 2026-11-19, PR #98)

- EuroLeague and EuroCup sides call the EuroLeague tabs' own readers
  (`readFacts`, `readEuroLineupRows`, `readPlayerFacts`, `euroOnOffCompute`,
  `euroFourFactors`); the Israeli path keeps `compareUi.json`'s SQL. A
  change to one of those readers changes Compare: the live pin "EuroLeague
  Compare sides equal their tabs" (`e2e/live-smoke.spec.ts`) holds them equal.
- The league travels on the resolved query (`resolveCompareQuery(raw,
  league)`, read with `compareLeague(q)`) and in `CompareLeagueContext` for
  the data hooks. CompareTab's `set()` drops `league` before patching: it is
  never a URL key.
- EuroLeague hides what it has no data for: Players Shot Profile, Players
  starters fields, Deflections and Disruptions; it says Phase and Round. The
  chat's Compare tool stays Israeli and refuses `phase`.

## Logs (since 2026-11-19)

One JSON line per event in Vercel's runtime log (`lib/logEvent.ts`); Hobby
keeps them only briefly, so they serve incidents, not trends.
- Privacy rule (user): a line names the route or page, league, status,
  timing and the kind of failure. Never an IP, cookie, session or client id,
  a query string, a refused parameter's name, a database error's message
  (only its SQLSTATE `code`), or anything a visitor typed.
- `api_error` (`handleError`): route, league, status, `reason` (param, busy,
  timeout, database, error), `code`, `ms`. Every API route's GET is wrapped
  in `logged(route, handler)` (`server/requestLog.ts`), which carries the
  route and league in AsyncLocalStorage; wrap a new route the same way.
- `slow_request`: an answer over `SLOW_MS` (4 s), with its route and time.
- `page_load_failed`: a server-rendered page's first load failed and it
  shows its error card; use `.catch(pageFailed(page, league))`, never a
  bare `.catch(() => null)`.
- `rate_limited` (`proxy.ts`): at most one line a minute per reader and
  status, with `since_last`, the refusals since the previous line.
- `db_client_error`, `ask_turn`, `ask_feedback`: see below and `server/db.ts`.

## Ask chat: logs and "Wrong answer?" (since 2026-11-19)

Privacy rule (user): a visitor's words are stored only when they flag an
answer and press Send.
- The per-question log line (`ask/log.ts`, `ask_turn`) carries tools,
  timing, cost and `questionChars`, never the question, IP or ids.
- "Wrong answer?" under each answer (`AskPanel` `WrongAnswer`) opens a
  consent line and an optional note; Send posts to `/api/v1/ask/feedback`,
  which calls `app_feedback.submit_ask_feedback()` in the one writing
  transaction the app opens (`withSnapshot(..., { write: true })`).
- `app_feedback` is its own schema (`sql/migrations/2026-11-19_ask_feedback.sql`,
  `scripts/apply_ask_feedback.R`): app_readonly may only EXECUTE the function,
  never read the table, because `basketball_test` grants it SELECT on every
  relation. Read the flags as the owner:
  `SELECT * FROM app_feedback.ask_feedback WHERE reviewed_at IS NULL`.
- Limits: 5 flags an hour per browser and per address (in memory), 200 an
  hour in total (the function).

## Working notes (2026-11-19)

- Read a protected Preview or Production answer with
  `node C:/nvm4w/nodejs/node_modules/vercel/dist/vc.js curl --yes <path> --deployment <url> -- -s -o <file>`
  (`vercel.cmd` mangles `&`). Runtime logs: `vc.js logs --since 10m --query 'environment:production' --json`.
- Lineups tables flag rows the other way round: `is_total === 0` is the TOTAL
  row (`sub_lineup_hash: "TOTAL"`), every lineup has 1.
- In e2e, `page.request` skips `e2e/preview.ts`'s routes, so it must send
  `x-vercel-protection-bypass` itself; use it for large JSON (a season's
  lineups overflow the inspector cache behind `page.goto(...).json()`).
- A live pin runs on every Preview push: keep its answers small (a filter,
  a context) or it spends Fast Origin Transfer and minutes.
- `readTabReferences()` returns `{ teams: {id, name}[], rounds, phases, seasons }`.

## Current resume point (2026-10-22)

Gameflow is implemented for all competitions: Game Logs links and direct SSR
pages, native desktop dialog, width-based quarter/OT cards, full mobile timeline
with pinned names, player/shared-lineup selection, details, score inspection and
warnings. Ten Shiny replays/live comparisons, 237 unit tests, 92 browser checks
and six live SSR checks pass. Ready Preview:
https://ibpl-game-logs-11drkc5az-ariel12091-8759.vercel.app/il/gameflow?game_id=390&team_id=2
Next: Home, then Compare. Home has been surveyed but not implemented.
One production-mode server runs on 3100 with PG_PORT=5432; check first.
The Player Stats and earlier entries below are historical.

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

1. **Features first -- done; the look may now drift (user, 2026-11-16).**
   Every Shiny feature moved across in Shiny's spirit. Shiny is being retired,
   so the UI no longer has to match it: modernise where it helps (see "Look
   and feel" below). Same features and same numbers still hold.
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

## Look and feel (since 2026-11-16, PRs #64-#73)

The modern styles live in blocks at the end of `src/app/web.css`; the shared
pieces are components. Reuse them, never a one-off:

- **Filters panel** (`FilterDrawer`): a Radix Popover restyled as a sheet
  (bottom on phones, right edge from 768 px). Section order on every tab:
  team picker first where the section has one, then dates, clutch, lineup
  starters, game filters.
- **States** (`OutputArea` in `TabKit.tsx`): pass `onClear={reset}`; an empty
  table shows `EmptyState` with Clear filters, loading shows a bar above the
  dimmed table, errors a card with Retry.
- **Lineup names** (`LineupNames`): middle dots, never commas; the label that
  floats over a sideways-scrolled row uses `Column.rowName` (Lineups: team,
  then `lineupSurnames`).
- **Columns menu** (`ColumnsMenu`, via `DataTable`'s `tools`): chips; tap
  hides, drag moves (window pointer events, state in a ref; touch = hold then
  drag), Alt+arrows. Stored with `lib/storedList.ts`.
- Links are amber with a soft underline and a `›`; buttons, pagers, chips and
  back links are pills; on/off settings are switches over the native checkbox.
- Before shipping a restyle or a new control: check light AND dark theme
  (the vendor datepicker hid days in light theme once), and repeat the
  interaction several times on desktop and phone widths (a drag that worked
  once failed after the first drop on a real device).
- Big visual changes start from a mock-up the user approves. When the user's
  words are ambiguous ("half pilled", "same place"), render each reading in
  the real app (inject CSS with Playwright) and send the screenshots.
- **Top bar** (`Navbar.tsx`, PR #72): one markup, arranged by width. From
  1024 px one sticky row: page pills, then the League switch and Season menu
  set apart by dividers and labels, then date, Glossary, theme; it drops the
  brand name below 1400, the labels below 1250 and turns the league switch
  into a menu below 1200 (re-measure 1024-1600 px in `theme.spec.ts` after
  adding anything). Narrower: all seven pages in a bottom tab bar, the
  Filters button as a tab on top of it, the update time in the season menu.
  League/season menus are Radix Popovers, navigation not ARIA menus.
- **Bottom-pinned UI** (tab bar, Filters tab, Ask button, Gameflow phone
  panel) sits above `--ibpl-tabbar-h` (0 on desktop). NEVER pad it with
  `env(safe-area-inset-bottom)`: the page does not set viewport-fit=cover, yet
  Android Chrome reports a 47 px inset while its address bar shows, which
  became an empty band under the bar (measured on the user's phone).
- **Views** (`ViewTabs`): a labelled group of `aria-pressed` buttons styled
  as top-rounded tabs on an amber line (not an ARIA tablist: the views change
  the URL and have no panels). The selected view scrolls into sight; both
  edges fade when the row overflows.
- **Row above the table**: row tags, the view's "?" (`ViewIntro`, a
  popover named by the question) and the table tools (CSV as a round arrow,
  Columns; `DataTable` portals them into `<ToolsSlot/>`) on the left, the
  colour key (`TableLegend`) at the top right. Two rows when crowded.
- Follow modern-web-guidance (`.claude/skills/modern-web-guidance`): 44 px
  touch targets via `::after` on `(pointer: coarse)`, `scroll-padding` for
  fixed bars, no document-level outside-click/Escape listeners.

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
