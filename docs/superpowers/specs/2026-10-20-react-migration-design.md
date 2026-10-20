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
  Python) or the data in the database -- **one exception (user, 2026-10-20):** the publication
  markers of "Caching and freshness" below, written by both loaders.
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
| 12 | One owner per rule: Postgres for anything needing raw play-by-play; one shared TypeScript module for everything derived from returned rows; existing R logic is ported faithfully where sound | Claude, revised with the user |
| 13 | Answers cached on the CDN, keyed by a data version that changes on every publication | Claude (revised after review) |
| 14 | Filtering split three ways by data size (below) | Claude |
| 15 | Build order: foundation + Game Logs first, Compare last | Claude |
| 16 | Shiny keeps its R logic until retired (temporary, registered exception to R1; nightly parity guards it) | User (on Claude's recommendation) |
| 17 | Load target: 95% of filter changes within 2 s, none over 5 s; the first thing to give is up to ~$15 more compute | User (on Claude's recommendation) |
| 18 | Old Shiny links may break: the old address forwards to the new app's home page | User |
| 19 | Our own API (Next.js routes) is the single front door for React, a future MCP server and other callers; behind it, a direct Postgres connection with `app_readonly`'s permissions, as Shiny has (login role `app_web`, decision 22) | User + Claude |
| 20 | Reuse what is proven; change only for a named, measured or documented reason | User |
| 21 | ETL exception: both loaders write a publication state (publishing / published / failed; stale = crashed) and a version | User |
| 22 | The web app connects as its own login role `app_web`, a member of `app_readonly`, with its own connection limit and role-default timeouts | Claude |
| 23 | Every writer of served data holds an advisory lock for its run, fences each write transaction on ownership, and finalizes through `finish_publication` | Claude |

## Guiding rule: reuse what is proven

Reuse what already works in this project and change something only for a measured or
documented reason, which this document names (user, 2026-10-20). Reused as-is: the read-only
`app_readonly` role and its grants, the stored functions and views, the rule definitions in
`docs/shared_definitions.md`, the security apply/audit scripts, the goldens-and-parity
method, and the R logic's behaviour (ported, decision 12). Every deviation below carries its
reason.

## Architecture

```
Browser (Next.js pages, React, one responsive design)
   |  fetch /api/v1/...            (filters in the URL; responses cached at the edge)
Our API: Next.js route handlers on Vercel  (/api/v1/...: the one front door for the React app,
   |                                          a future MCP server and other callers; validates params,
   |                                          calls SQL owners, applies the shared TS module, shapes JSON)
   |  direct Postgres connection as app_web, a member of app_readonly (pg via Supabase's pooler), server-side only
Postgres on Supabase               (schemas basketball_test, euroleague; stored functions own every number)
   ^
ETL (R) and EuroLeague loader (Python), unchanged -- advance the data version when they publish
```

### Where the logic goes (decision 12)

The principle is rule R1 of `docs/shared_definitions.md`: **one implementation per rule**,
with its owner listed in that document. Each rule gets exactly one of two owners, chosen by
what it needs:

| Owner | Computes | Why there |
|---|---|---|
| **Postgres** (stored functions and views -- mostly the existing ones) | Everything that needs raw play-by-play: points, possessions, minutes, shot counts, clutch and starters splits, opponent rank, per-game and per-lineup aggregates, Gameflow stints | The data is only there, and too large to ship to a browser |
| **One shared TypeScript module** (pure functions, no UI code; used by route handlers and the browser alike) | Everything derived from rows the database returned: rates from counts (R5, "rates are derived last"), percentile ranks over a returned population, TOTAL and multi-team TOTAL rows, Player Stats rate modes and eligibility, Compare gaps/winners/labels, On/Off 3PT-luck and small-sample tags, Four Factors impact estimates, shot league averages, auto min-possessions, rank-change arrows from two returned frames | Pure arithmetic on rows; instant in the browser (no server round trip on re-rank, rate-mode switch or stat filter), which also protects the 2 s target |

Rules:

- **Reuse what the app already does (user, 2026-10-20).** Where the R logic is sound, it is
  ported, not redesigned: each TypeScript function is a faithful translation of a named R
  function (`helpers.R`, `svc_game_logs.R`, the server files' pure helpers -- e.g.
  `pr_vec`, `adaptive_baseline`, `auto_minposs_from_df`, `setup_onoff_auto_min`'s rule,
  `onoff_3pt_luck`, `onoff_sample_flag`, `apply_ts_mode`, `ts_rate_threshold`,
  `add_ts_multi_team_totals`, `gl_build_*`, `gl_attach_percentiles`) and its doc comment names
  the R source. The R function's existing tests are ported with it. The existing SQL owners
  are called as they are; new SQL is written only for the R-closure logic that has no SQL or
  pure-R home yet (the inventory's R-closure rows, and the five inline-SQL places). A known
  defect is ported as-is and stays in the deviation register (e.g. D24, D25) -- fixing it is a
  separate, deliberate change with re-recorded goldens.
- **No rule has two owners**: a number the TypeScript module derives is never also derived in
  SQL, and the reverse. Each moved rule's owner is updated in `docs/shared_definitions.md` the
  same day.
- **A ranking is computed only where the population it ranks against is present.** When that
  population is too large to return (e.g. a full lineup population under filters), the rank is
  computed in SQL instead.
- **Rounding is one shared function** in the TypeScript module that reproduces R's `round()`
  (halves to even, IEC 60559); Postgres `round(numeric)` (halves away from zero) is never used
  for a displayed value without checking it against the goldens.
- **The UI only presents**: URL/filter state, sorting with each column's best direction,
  heat colours from `pr_*` and polarity, CSV export (with the formula-defusing rule in
  `docs/shared_definitions.md` § 3), Gameflow pair selection and shared-lineup recomputation
  (GF-06 is client-side today too; its existing `app.js` logic is ported).
- A new database surface: one schema (e.g. `app_api`) holding the functions the web app may
  call, each a thin wrapper over the owning function, executable by `app_readonly` only. The
  existing grants and the security apply/audit scripts (`scripts/apply_db_security.R`) are
  extended to cover it.

### Data access

Our API (the Next.js route handlers) is the only front door; nothing else talks to the
database from the web. Behind it, the route handlers connect **directly to Postgres through Supabase's pooler** with
the `pg` (node-postgres) driver, as a login role `app_web` that is a member of the existing
`app_readonly` role -- so it has exactly Shiny's permissions, grants and stored functions, with
no new grants to maintain. The separate login is a documented deviation from "reuse what is
proven" (review gap G10): it gives the web app its own pooler pool and its own
`CONNECTION LIMIT`, so it can never take the ETL's or Shiny's capacity. The browser never sees a
credential.

Reasons for not using Supabase's auto-generated Data API (PostgREST) for this internal hop,
from the 2026-10-20 review: its secret key runs as `service_role`, which bypasses row-level
security, and a custom role needs JWT minting and authenticator grants
([Supabase API keys](https://supabase.com/docs/guides/getting-started/api-keys),
[PostgREST auth](https://docs.postgrest.org/en/stable/references/auth.html)); it caps rows per
request, which could silently truncate a population; and it adds a second HTTP hop.

Driver and port (closes review gap G6): `postgres.js` is not used -- it pipelines queries by
default, which with Supabase's transaction pooler "can hang queries or return mismatched
rows", and it has no working switch to turn pipelining off
([Supabase Postgres.js guidance](https://supabase.com/docs/guides/database/postgres-js)).
`pg` does not pipeline: each parameterised query is sent as one self-contained exchange
(parse, bind, execute, sync), so the pooler cannot interleave another client's query into it
-- the failure mode of RPostgres's separate prepare and execute round trips on port 6543
(CLAUDE.md). Phase 0 tests the transaction pooler (6543) against the session pooler (5432)
with `pg`: concurrent, distinguishable requests -- parallel queries inside one request and
concurrent requests on one warm instance -- must each receive their own, complete rows, with
latency and timeouts measured. Only a combination that passes carries production traffic.
Connections per instance are capped so the total stays inside the database's limit.

**Identity proof (closes review gap G3):** the security apply and audit scripts
(`scripts/apply_db_security.R`, `sql/security/*.sql`) are extended to the `app_api` schema;
Phase 0 shows, with the app's real credential, that permitted reads succeed, writes and
functions outside the allowlist fail, and no other role is reachable with it. Credentials live
only in Vercel's server environment variables; rotation is a documented step.

### Concurrency (async)

The gain over Shiny is structural: Node.js handles I/O asynchronously, so one function
instance serves many requests while their queries wait on the database, and Vercel's Fluid
compute reuses warm instances for concurrent requests
([Vercel Function limits](https://vercel.com/docs/functions/limitations)). In practice:
- every database call is `async`; **one response is read from one database snapshot**
  (closes review gap G11): all frames a response combines -- the two Compare sides, current
  ranks and their rank-arrow baseline -- come from one SQL statement or one repeatable-read
  transaction on a single checked-out client
  ([node-postgres transactions](https://node-postgres.com/features/transactions)), so
  parallelism happens inside the database, not across connections. Separate parallel calls
  are allowed only when every frame reports `published` and the same version (with
  single-writer ownership, G12, no run can then be in progress); otherwise all frames are
  re-read in one transaction. Frames are never combined on matching markers alone. Phase 0
  tests a publication landing between the two Compare-side reads and between the current and
  baseline reads;
- nothing in a request handler blocks the event loop with heavy CPU work; the shared TS
  module's per-row work is linear and small, and anything heavier moves to SQL;
- in the browser, data loads are async with loading states (the skeletons Shiny shows today),
  filter changes are debounced, a filter change aborts the request it replaces, and the page
  ignores any response that is not for its latest request, so stale results never replace
  newer state.

**Bounding work end to end (closes review gaps G8 and G10).** Aborting a browser request does
not stop the server or the query
([Postgres.js cancellation](https://github.com/porsager/postgres#cancel) documents the races),
so the design bounds work instead of relying on cancellation.

*Per request (G8).* One deadline for the whole request: **5 s** (success criterion 3), split
into budgets -- about 0.5 s waiting in the instance's queue, 0.5 s acquiring a connection, at
most 3 s of database time, and the remainder for processing and sending the response. The
statement timeout is set per request from the budget left (`SET LOCAL statement_timeout`
inside the request's transaction; [PostgreSQL client settings](https://www.postgresql.org/docs/current/runtime-config-client.html)).
A request still queued when its deadline passes is dropped and never starts. The response
deadline (answer 503 with `Retry-After` at 5 s) is separate from the bound on database work
already running (its statement timeout); superseded queries are not cancelled mid-flight, so a
cancellation can never hit a later query reusing the connection.

*Waiting inside the pooler (G8).* In transaction mode the pooler assigns a database backend
only when a transaction's first statement arrives, so time waiting there is invisible to a
timeout that statement would set. Two bounds cover it:
- **role defaults enforced by the database**: `app_web` gets `statement_timeout = 3s` and a
  short `idle_in_transaction_session_timeout` (`ALTER ROLE ... SET`), so every statement is
  bounded even if no per-request setting arrives, and an abandoned transaction cannot hold a
  backend;
- **a client-side timer over the whole first exchange** (`BEGIN` and the timeout setup,
  including any wait in the pooler): if it is not answered within the connection budget, the
  client connection is destroyed, not returned to the pool, so nothing queued behind it can
  start later without its budget.
Pooler waiting is measured separately from the local queue and from connection establishment.
Phase 0 tests with all of `app_web`'s backend slots busy: a new request must meet its deadline
and leave no delayed or orphaned work.

*Across instances (G10).* Vercel scales out instances, each with its own pool, so local caps do
not bound the total. The global bound is enforced by the database: the `app_web` role's
`CONNECTION LIMIT` and its pooler pool size. A connection budget is written down in Phase 0
for the chosen pooler mode -- the web app's share (preview traffic included), a reserve for the
ETL, and Shiny's share while it runs -- inside the formula Supabase gives ("direct connections
+ Supavisor backend connections + PgBouncer backend connections < Postgres max connections";
[Supabase pooling and limits](https://supabase.com/docs/guides/database/connecting-to-postgres/pooling-and-limits)).
Each instance holds one or two connections, as Supabase recommends for serverless
([Supabase connecting](https://supabase.com/docs/guides/database/connecting-to-postgres));
a full pool appears as connection-wait time, capped by the 0.5 s acquisition budget and turned
into 503.

Phase 0 tests: queue saturation; requests running several queries; superseded queued requests;
rapid filter changes while watching `pg_stat_activity`; several function instances and a burst
of cache misses, observing connections on the client side and the database side. One warm
instance is not sufficient proof.

### Caching and freshness (decision 13; closes review gaps G2 and G9)

Data changes once a night, so responses are cached on Vercel's CDN, which caches GET
responses by URL on every plan when they carry `s-maxage`
([Vercel CDN Cache](https://vercel.com/docs/caching/cdn-cache)). Next.js 16's `'use cache'` is
not relied on for this: its default in-memory cache does not persist across requests on
serverless hosts ([Next.js use cache](https://nextjs.org/docs/app/api-reference/directives/use-cache)).

**Why a new publication boundary is needed.** Neither existing marker bounds what the database
serves: the Israeli ETL commits the refreshed views (Phase 4) and sub-lineup statistics
(Phase 5) before it inserts `etl_processed_games`, skips that insertion and the
`etl_full_last_success` stamp after a failure, and so can show new numbers under an old marker
for minutes or indefinitely (`etl/etl_full.R`). Shiny's caches, keyed on the same stamp, share
this gap today.

**The ETL change (the one exception to the ETL non-goal).** Each loader keeps, per league, a
publication **state** and **version** (e.g. `app_meta` keys `publication_state_<league>`,
`publication_started_<league>`, `data_version_<league>`), with four states:
1. `publishing` -- set before the run's first data commit, with its start time;
2. `published` -- set as the very last step, **only after the run's own validation passes**
   (Phase 6 for the Israeli ETL); the version is incremented;
3. `failed` -- set when a run that committed data then fails or fails validation; the version
   is still incremented (the data did change), but the state is not `published`;
4. `stale` -- not written by a loader: a `publishing` state whose owning run no longer holds
   its advisory lock (see "One writer at a time"), i.e. a crashed run.
A run that commits nothing changes neither. The next successful run sets `published` again.
The Israeli ETL (`etl/etl_full.R`) and the EuroLeague loader (`euroleague/`) both do this.

**One writer at a time (closes review gap G12).** Ownership is enforced on every data write,
not only at finalization, and a writer is only replaced once it is provably stopped.

- **Liveness by lock, not by elapsed time.** A run holds a session-level advisory lock
  (`pg_advisory_lock`) for its league on one dedicated connection for its whole life. A
  session lock needs a session -- the direct connection or the session pooler (5432), never the
  transaction pooler. If the process dies, its connection closes and Postgres releases the
  lock; a slow or paused run keeps it. `begin_publication(league, run_id)` must acquire that
  lock, so takeover is possible **only after the previous writer is verified gone**. The
  `stale` state of the four-state list is therefore no longer a time threshold: it means "a
  `publishing` state whose owner no longer holds the lock".
- **Fencing every write transaction.** Every transaction that writes served data -- on any of
  the run's connections -- starts by locking the publication row for its league
  (`SELECT ... FOR SHARE ... WHERE owner = run_id`) and aborts if the run is no longer the
  owner. A takeover updates that row, so it waits for any in-flight write transaction holding
  the row lock to finish, and every later write by the old run fails the owner check. A check
  that is not held until commit is not used.
- **Finalization** (`finish_publication(league, run_id, ok)`) sets the final state and advances
  the version in one atomic update that succeeds only for the current owner.
- **Coverage:** every writer of served data -- the scheduled ETL, manual `game_ids=` runs and
  retries, the EuroLeague loader, and manual repair procedures (`scripts/restage_matview.R`,
  the apply and migration scripts that rebuild served views or tables) -- claims, fences its
  write transactions and finalizes this way. The ETL workflows also get a GitHub Actions
  concurrency group so two runs of the same loader cannot start together.

Phase 0 tests: overlapping starts; finalization by the wrong run; a paused run A, takeover and
successful publication by B, then A resuming both a fresh write and a write transaction that
was already in flight -- neither may commit changes after B publishes; and a killed process
releasing its claim.

**The contract.**
- Every data function returns `(state, version)` read in the **same database snapshot** as its
  rows (one statement, or one repeatable-read transaction), so nothing can change between
  checking the version and reading the data.
- Data URLs carry `v=<version>`. The API caches an answer for a long time **only when** the
  snapshot says `published` and its version equals the requested one. In any other state
  (`publishing`, `failed`, `stale`) answers are cached for about 60 seconds. A request for a
  version that is not current is redirected to the current one and never cached.
- **Paged downloads (closes review gap G9)** are served only in the `published` state, where
  the population cannot change between pages. In any other state the API returns the whole
  population in one response if it fits the 4.5 MB budget, and otherwise answers 503 with
  `Retry-After` ("data is updating"). The browser discards a download, and restarts it, if any
  page carries a different `(state, version)`.
- `GET /api/v1/version` returns `(state, version)` per league, cached about 60 seconds. An
  open page checks it periodically and on focus, refetches when it changes, and updates the
  "last updated" stamp.
- Shiny may key its caches on the same marker; it is not required for the migration.

Tested in Phase 0 at the real commit boundaries: a request between the Phase 4 and Phase 6
commits; a failure after a refresh commit (must end `failed`, never `published`); a process
killed before its final step (must read as `stale`); a publication overlapping a request; a
multi-page download across real commits (no mixed population may reach ranking or totals,
including while the markers still match); an open browser across a version change; an
old-version URL that misses the cache.

### Filtering (decision 14)

| Data size | Where filters run | Tabs (today's behaviour) |
|---|---|---|
| Small season pulls (a few thousand rows) | In the browser over one cached season download -- instant | Game Logs; per-game team and player totals where Shiny already filters in R |
| Large or event-level | In the database, one stored-function call per filter combination, result cached; requests debounced | On/Off filtered path, Lineups, clutch, opponent rank, starters, Compare |
| Slow stored functions | Measured and optimised until the 2 s target holds under load | e.g. `onoff_compute` (~4.6 s cold today), `get_team_four_factors_dynamic` under the starters presets (times out at 20 s today) |

Filter state lives in the URL, so every view can be shared and bookmarked (X-18).

**Payload and completeness (closes review gap G4).** A Vercel Function response is limited to
4.5 MB, below the CDN's 10 MB
([Vercel Function limits](https://vercel.com/docs/functions/limitations)). Phase 0 measures the
serialized size of the largest season downloads (both leagues) against that budget and, where
it is exceeded, returns only the needed columns, compresses, or splits into pages. Every
population a rank or total is computed over is checked complete **from the same database
execution** (closes review gap G7): each data function returns its row count and control
totals computed in the same statement over the same filtered fact (window aggregates), so
runtime verification adds no second scan -- the shared-scan reader rules in `PROJECT.md`
apply. Independent reconciliation queries run only in tests, together with a query-count and
plan check that each route performs one expensive fact scan. Paged downloads carry the same
`(state, version)` on every page, and a page from a different version is refetched.

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
4. **The coverage gate reads test-run results, not source** (closes review gap G5). Every
   inventory ID must map to executed, passing, non-skipped tests of the new app -- Israeli,
   EuroLeague and EuroCup where they apply, at phone and desktop widths. A missing, skipped or
   failed required case blocks the switch. The source scanner in `api/coverage.R` stays only
   as a traceability aid.
5. **Nightly live parity**, new app vs Shiny on the live database, runs until Shiny is retired.

**Which values must match (Claude's call):** what Shiny hands its table -- the golden frame
captured at the `DT::datatable()` call, after Shiny's own R rounding -- plus the hidden
columns (`pr_*`, raw counts) in that frame. Rounding is therefore a contract item, owned by
the shared TypeScript rounding function (or by the SQL owner where SQL rounds today), not
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
| 0 | Foundation: Next.js project; `app_api` schema under `app_readonly` with the security scripts extended; direct Postgres access with `pg`, both ports tested for row ownership under concurrency (G6); the ETL publication markers and the freshness contract (G2); the `app_web` role and the written connection budget (G10); end-to-end request deadlines with queue, connection and SQL budgets (G8); the four-state publication markers, single-writer ownership for every writer and paging rules (G2, G9, G12); one snapshot per combined response (G11); `app_web` role-default timeouts and the pooler-wait bound (G8); completeness metadata from the same execution (G7); preview deployment; Game Logs end to end (both leagues); payload and completeness checks; **a load test of the expensive database paths too** (closes G1): filtered On/Off, starters and custom clutch, both leagues -- cold caches, distinct filter combinations and simultaneous identical requests, with duration, request rate, cache-hit mix and the latency measurement point written down first; errors and timeouts count as failures | Game Logs matches its 15 goldens through the preview URL; the identity proof (G3), the commit-boundary freshness tests (G2), the concurrency row-ownership tests (G6), the work-bounding and multi-instance tests (G8, G10) and the paged-download test (G9), the writer-ownership and write-fencing tests (G12), the cross-read publication tests (G11) and the full-pool test (G8) pass; the load test meets criterion 3, or the measured capacity limits and the changes needed are recorded before Phases 2-7 start |
| 1 | Goldens for every remaining tab, from Shiny | Every inventory ID named by a golden or a browser-test spec |
| 2-7 | One tab family per phase: Player Stats, Team Ratings, On/Off, Lineups, Gameflow + Home, Compare (logic to SQL first, then route, then UI) | That family's goldens and browser tests pass; coverage gate shows its IDs |
| 8 | Full-app load test, performance work on slow functions | Criterion 3 holds for every tab |
| 9 | Switch: new address live for everyone; Shiny's old address points users to the new app's home page (old deep links are not translated, decision 18); Shiny kept reachable as fallback | Users on the new app |
| 10 | Retire Shiny, the Plumber API (`api/`) and the shared R layer's Shiny side after the fallback weeks | Shiny off; docs updated |

Phase 0 deliberately puts the riskiest assumptions first (database access under load, the
expensive queries, caching and freshness, the 2 s target) so a wrong call costs one phase, not seven.

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

## Review record (2026-10-20)

Five reviews found twelve gaps: G1 expensive paths untested in Phase 0; G2 version freshness
and the publication boundary (reopened twice: failed runs, crashed runs); G3 the Data API
identity; G4 payload limits and population completeness; G5 coverage from source instead of
test runs; G6 driver pipelining on the transaction pooler; G7 completeness checks repeating an
expensive scan; G8 bounding the whole request, not only each statement; G9 paged downloads
mixing populations during publication; G10 deployment-wide connection limits; G11 one
consistent snapshot per combined response; G12 exclusive writer ownership of the publication
state (reopened by the fifth review: fencing data writes, not only finalization). G8 was also
reopened for waits inside the transaction pooler. Each has a
design response in the section marked "closes review gap Gn"; each is closed only when the
Phase 0 (or, for G5, the switch) tests named there pass.

## Open

1. The new app's web address (custom domain or the host's default) -- needed by the switch.
2. Whether the app will become commercial (moves hosting to Vercel Pro).

## How the Next.js app is built (Claude's calls, checked against current docs 2026-10-20)

- **Next.js 16, App Router.** Pages and layouts are Server Components by default; data is read
  on the server, close to the database, and passed to Client Components as props. Client
  Components only where there is interaction (tables, filter controls, Gameflow), marked
  `'use client'` at the smallest boundary. Modules holding secrets import `server-only`, so
  they cannot end up in the browser bundle (Next.js "Server and Client Components").
- **Rules as plain typed functions**, not R-style pipelines or reactive chains: the shared
  module (decision 12) is pure TypeScript functions over typed row arrays, each with unit tests
  (Vitest) seeded from the R tests and the goldens. Behaviour is ported; R's shape is not.
- **Filter state in the URL** with `nuqs` (type-safe search-params state for the App Router),
  so every view is shareable and the server can read the same state.
- **Tables** with TanStack Table (sorting, column order) and TanStack Virtual for long tables;
  client-side sorting when the full row set is present, server-side otherwise (TanStack docs).
- **Input validation** with one schema per route (`zod`), mirroring the fail-closed validators
  of `app/R/svc_params.R` and the request caps from the 2026-10-20 security audit.
- **Database access** with `pg` (node-postgres) from server code only, as `app_web` (see Data
  access). `app_api` is not exposed through Supabase's Data API.

Sources: [Next.js `use cache`](https://nextjs.org/docs/app/api-reference/directives/use-cache),
[Next.js Server and Client Components](https://nextjs.org/docs/app/getting-started/server-and-client-components),
[Vercel CDN Cache](https://vercel.com/docs/caching/cdn-cache),
[nuqs](https://blog.logrocket.com/managing-search-parameters-next-js-nuqs),
[TanStack Table virtualization](https://tanstack.com/table/latest/docs/framework/react/guide/virtualization).

## Left to the builder (Claude's calls, overrulable)

- CSS approach, component library (if any), the load-test tool; library versions at build time
  (e.g. TanStack Table v8 vs the newer v9 API).
- Exact `app_api` function signatures and the JSON contract per route (the 2026-10-19 design's
  response contract is the starting point: rows, totals, meta with columns, polarity,
  default sort, auto min-poss inputs, data version, warnings).
- Preset lists per tab, and the per-function performance targets inside the 2 s budget.
- The order of tab families after Phase 0, if measurements suggest a better one.
