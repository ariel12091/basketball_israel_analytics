# Migration from R/Shiny to React/Next.js -- design (2026-10-20)

Companion documents: `2026-10-19-api-mcp-feature-inventory.md` (the 109 features, with IDs,
checked against the live app) and `docs/shared_definitions.md` (the contract every number
follows). This design replaces Plans 2-7 of `2026-10-19-api-mcp-design.md`; that design's
Plan 1 (Game Logs through a shared R layer and a Plumber API) is merged and becomes input
here, not a model to continue.

## Intent

Coaches and fans use the app during the season. When many log in at once it becomes too
slow: in the 20-user stress test (`docs/launch_stress_test_handoff_2026-09-10.md`) startup p90 was
35-42 s, against 9-11 s at 5 users. The cause is the stack, not one query: Shiny serves each
session from an R process that handles one request at a time, and Connect Cloud cold-starts
those processes. The user decided to replace Shiny with a React/Next.js application built for
concurrency, keeping every feature and every number.

## Users and success

- Coaches and fans, one app for everyone (no logins, no separate coach view).
- **Success = all four:**
  1. All 109 inventory features exist in the new app, both leagues (Israeli, EuroLeague,
     EuroCup), every tab.
  2. Every number equals what Shiny shows for the same filters, value for value (proved by
     goldens, below). Any difference that must remain goes in the deviation register of
     `docs/shared_definitions.md` the same day.
  3. Under 150 simulated concurrent users changing filters at random, **95% of
     filter-change responses arrive within 2 seconds and none takes more than 5 seconds**
     (page first load measured and reported too). If this cannot be met at $50, the order
     of what gives is: first spend up to the next Supabase compute size (~$15, total ~$40);
     every feature and the $50 ceiling are not traded away.
  4. Hosting costs at most **$50/month including Supabase**.

## Goals, non-goals, anti-goals

Goals: the four success criteria; a single codebase mostly written and maintained by Claude,
therefore with strong automated tests and documentation; Shiny keeps serving users until the
switch.

Non-goals:
- **No redesign.** Same screens, same controls, same behaviour. (The stats-tabs filter
  clean-up the user wants comes after the migration.)
- No logins, accounts or paid features.
- No change to the ETL (`etl/`, R, GitHub Actions), the EuroLeague loader (`euroleague/`,
  Python) or the data in the database.
- No MCP server in this project (Plan 8 of the earlier design is deferred).

Anti-goals (a working result that is still a failure):
- A feature from the inventory missing, or behaving differently, at the switch.
- A number that differs from Shiny without a register entry.
- A second implementation of a rule (rule R1) in the new app: TypeScript re-deriving a number
  the database owns. **Accepted temporary exception (user, 2026-10-20):** Shiny keeps its own
  R implementation of each rule until it is retired; it is not rewired to the new SQL owners.
  The nightly live parity (new app vs Shiny) is the guard, and the exception ends with
  Phase 10. New SQL owners are additive (`app_api`), so Shiny's running functions are not
  changed; where an existing owner Shiny calls needs a new parameter, a new function is added
  instead of altering the one Shiny uses.
- Separate phone and desktop versions: the user does not want to maintain several designs.

## Decisions

| # | Decision | By |
|---|---|---|
| 1 | Replace Shiny with React/Next.js (TypeScript) | User |
| 2 | Same app for coaches and fans; no logins | User |
| 3 | Every feature before anyone is switched over; nothing lost | User |
| 4 | Budget $50/month including Supabase; app is non-commercial for now | User |
| 5 | Claude writes and maintains most of the code | User |
| 6 | No redesign during the migration | User |
| 7 | Filter changes answer within 2 s on a busy night | User |
| 8 | Shiny stays live as a fallback for a few weeks after the switch, then is retired | User |
| 9 | **One responsive design for every device** -- no separate mobile layer | User |
| 10 | As soon as possible; no fixed date | User |
| 11 | Hosting: Vercel Hobby (free, allowed while non-commercial); Vercel Pro ($20) if it becomes commercial; Cloudflare Workers (~$5) as fallback if Vercel limits bind | Claude |
| 12 | Every number computed in Postgres; TypeScript does presentation only | Claude |
| 13 | Answers cached at the edge, keyed by the data version | Claude |
| 14 | Filtering split three ways by data size (below) | Claude |
| 15 | Build order: foundation + Game Logs first, Compare last | Claude |
| 16 | Shiny keeps its R logic until retired (temporary, registered exception to R1; nightly parity guards it) | User (on Claude's recommendation) |
| 17 | Load target: 95% of filter changes within 2 s, none over 5 s; the first thing to give is up to ~$15 more compute | User (on Claude's recommendation) |
| 18 | Old Shiny links may break: the old address forwards to the new app's home page | User |

## Architecture

```
Browser (Next.js pages, React, one responsive design)
   |  fetch /api/v1/...            (filters in the URL; responses cached at the edge)
Next.js route handlers on Vercel   (thin: validate params, call one SQL function, shape JSON)
   |  Supabase Data API (PostgREST) RPC, server-side only
Postgres on Supabase               (schemas basketball_test, euroleague; stored functions own every number)
   ^
ETL (R) and EuroLeague loader (Python), unchanged -- advance the data version when they publish
```

### Where the logic goes (decision 12)

`docs/shared_definitions.md` already says "the database computes; R, Plumber and React
present" (R1, R8). The migration completes that rule:

- **Postgres owns every number**: metrics, rates, totals and TOTAL rows, percentile ranks on
  the full population (`percent_rank()` matches dplyr's `percent_rank` definition), rank
  arrows, the On/Off 3PT-luck and small-sample tags, Four Factors impact estimates, Compare's
  per-side splits, gaps and winners, Player Stats rate modes and eligibility, Gameflow stints
  and per-window totals, Home hub cards and storylines. Each R-side rule in the inventory
  (marked R-pure / R-closure, about 80 of the 109) becomes a stored function or view, or a new
  parameter on an existing owner -- never a TypeScript re-derivation.
- **TypeScript owns presentation and interaction only**: URL/filter state, auto min-possessions
  (it chooses a bar from rows the database returned; the rule moves verbatim from
  `auto_minposs_from_df` / `setup_onoff_auto_min`), sorting with each column's best direction,
  heat colours from `pr_*` and polarity, CSV export (with the formula-defusing rule already in
  `docs/shared_definitions.md` § 3), Gameflow pair selection and shared-lineup
  recomputation (GF-06 is client-side today too).
- A new database surface: one schema (e.g. `app_api`) holding the functions the web app may
  call, each a thin wrapper over the owning function, granted to one read-only role. Only that
  schema is exposed through the Data API. The existing `app_readonly` grants and the
  security apply/audit scripts (`scripts/apply_db_security.R`) cover it.
- **Rounding is a contract item.** R's `round()` rounds halves to even (IEC 60559); Postgres
  `round(numeric)` rounds halves away from zero. Wherever Shiny rounded in R, the SQL owner must
  reproduce R's result or the difference is registered. The goldens catch every case.

### Data access

Next.js route handlers call the `app_api` functions through Supabase's Data API (PostgREST)
from the server only, with a server-side key for the read-only role -- the browser never
talks to the database or sees a key. This avoids holding Postgres connections in serverless
functions and the transaction-pooler prepared-statement defect (CLAUDE.md, port 6543). Phase 0
proves it under load; if PostgREST's limits bind, the fallback is `postgres.js` through the
pooler with prepared statements disabled.

### Caching (decision 13)

Data changes once a night. Every API response is cached at Vercel's edge with the data version
in the cache key (Israeli `app_meta.etl_full_last_success`; EuroLeague `euro_data_version()`).
When the ETL publishes, the version advances and new requests miss the old entries. The
default views most visitors open (season, all teams) are therefore served from the edge.

### Filtering (decision 14)

| Data size | Where filters run | Tabs (today's behaviour) |
|---|---|---|
| Small season pulls (a few thousand rows) | In the browser over one cached season download -- instant | Game Logs; per-game team and player totals where Shiny already filters in R |
| Large or event-level | In the database, one stored-function call per filter combination, result cached; requests debounced | On/Off filtered path, Lineups, clutch, opponent rank, starters, Compare |
| Slow stored functions | Measured and optimised until the 2 s target holds under load | e.g. `onoff_compute` (~4.6 s cold today), `get_team_four_factors_dynamic` under the starters presets (times out at 20 s today) |

Filter state lives in the URL, so every view can be shared and bookmarked (X-18).

### One responsive design (decision 9)

Today's phone/tablet layer (`app/www/mobile.js`, `mobile.css`) is not ported as a separate
layer. Every component adapts to screen width itself: tables, filter drawer, chips, Gameflow
(one component instead of a desktop modal plus touch quarter cards). The inventory's
behaviours (GF-04, X-15 and the rest) must still work on a phone.

## Proof that nothing is lost

1. **Goldens for every tab, captured from today's Shiny** with the existing harness
   (`app/tests/goldens/harness.R`, `scripts/capture_goldens.R`), preset lists per tab covering
   every filter family, both leagues, both paths (fast/filtered), every view and mode. Game
   Logs already has 15 presets.
2. **Each SQL owner is tested against the goldens** (value for value), and each new API route
   is tested against the same goldens through HTTP.
3. **Client behaviour** (sorting, auto min-poss, colours, CSV, Gameflow interactions, chips,
   reset, URL state) is pinned with browser tests (Playwright) against the inventory.
4. **The coverage gate** (`api/coverage.R`, reading the inventory) is reused or ported: the
   switch is blocked while any of the 109 IDs has no passing test.
5. **Nightly live parity**, new app vs Shiny on the live database, runs until Shiny is retired.

**Which values must match (Claude's call):** what Shiny hands its table -- the golden frame
captured at the `DT::datatable()` call, after Shiny's own R rounding -- plus the hidden
columns (`pr_*`, raw counts) in that frame. Rounding is therefore a contract item owned by
the SQL function (it reproduces R's half-to-even result where Shiny rounded in R), not
presentation.

**Where new SQL is developed and tested (Claude's call):** in the production database, as
additive functions in the new `app_api` schema that Shiny never calls. Golden presets use
**completed seasons** (Israeli 2026 = 2025-26, EuroLeague 2025), whose data no longer changes
nightly, so new SQL run against production can match goldens recorded from Shiny; the
current season is covered by the nightly live parity. If a completed season is reprocessed
(e.g. the parked final-buzzer minutes fix), the affected goldens are re-recorded the same
day, before any new SQL is compared to them. No second database (budget).

**Load tests (Claude's call):** against production, only at quiet hours (after the nightly
ETL, outside game days), announced to the user beforehand; any compute upgrade is the user's
decision.
6. **Load test**: 150 simulated users, random filters, against the preview deployment; must
   meet criterion 3 before the switch.

## Phases

| Phase | Content | Done when |
|---|---|---|
| 0 | Foundation: Next.js project, `app_api` schema + read-only role, Data API access, edge caching by data version, preview deployment; Game Logs end to end (both leagues); first load test | Game Logs matches its 15 goldens through the preview URL; 150-user test on Game Logs meets 2 s |
| 1 | Goldens for every remaining tab, from Shiny | Every inventory ID named by a golden or a browser-test spec |
| 2-7 | One tab family per phase: Player Stats, Team Ratings, On/Off, Lineups, Gameflow + Home, Compare (logic to SQL first, then route, then UI) | That family's goldens and browser tests pass; coverage gate shows its IDs |
| 8 | Full-app load test, performance work on slow functions | Criterion 3 holds for every tab |
| 9 | Switch: new address live for everyone; Shiny's old address points users to the new app's home page (old deep links are not translated, decision 18); Shiny kept reachable as fallback | Users on the new app |
| 10 | Retire Shiny, the Plumber API (`api/`) and the shared R layer's Shiny side after the fallback weeks | Shiny off; docs updated |

Phase 0 deliberately puts the riskiest assumptions first (Data API under load, edge caching,
the 2 s target) so a wrong call costs one phase, not seven.

## What happens to existing work

- `app/R/svc_game_logs.R`, `api/` and the Game Logs goldens: the goldens and presets become
  the acceptance tests for Phase 0; the svc layer is the specification of what moves to SQL;
  the Plumber API stops growing and is retired in Phase 10.
- `frontend-v2/` (React 19 + Vite, ~5,300 lines): its table, cell and filter components are a
  starting point, but it drifted from the app (it used its own copies of rules, e.g. D3, and a
  1-October season window, D22) -- every reused component is checked against the goldens.
- Plans 2-7 of the 2026-10-19 design (moving each tab into R) are cancelled.

## Constraints

- `docs/shared_definitions.md` stays normative; the migration edits it first for every
  definition it moves (its procedure: find the term, change its owner, update twins, run pins,
  register any remaining difference the same day).
- Database changes are deployed as today: staged view rebuilds, function deploys, then the
  security re-apply; nightly ETL must keep finishing within its 2-minute statement timeout.
- Read-only database role; parameterised calls only; untrusted inputs; the request caps and
  formula-defusing from the 2026-10-20 security audit carry over.
- No credentials in the browser or the repo.
- Shiny keeps working unchanged until the switch (decision 16).

## Risks

| Risk | Mitigation |
|---|---|
| Supabase Micro compute (1 GB RAM, ~60 connections) is not enough under 150 users | Edge caching first; measure in Phase 0; Small compute is ~$15 more and still fits the budget |
| Porting R logic to SQL changes a number (rounding, NA handling, ties) | Goldens per tab; rounding called out above; register any intentional difference |
| Vercel Hobby limits (bandwidth, function time) or a move to commercial use | Pro at $20 fits the budget; Cloudflare as fallback |
| Two systems live during the migration drift apart | Nightly live parity until Shiny is retired |

## Open

1. The new app's web address (custom domain or the host's default) -- needed by the switch.
2. Whether the app will become commercial (moves hosting to Vercel Pro).

## Left to the builder (Claude's calls, overrulable)

- Next.js routing style (App Router), component and state libraries, table library, CSS
  approach, test tooling (Vitest, Playwright, a load-test tool).
- Exact `app_api` function signatures and the JSON contract per route (the 2026-10-19 design's
  response contract is the starting point: rows, totals, meta with columns, polarity,
  default sort, auto min-poss inputs, data version, warnings).
- Preset lists per tab, and the per-function performance targets inside the 2 s budget.
- The order of tab families after Phase 0, if measurements suggest a better one.
