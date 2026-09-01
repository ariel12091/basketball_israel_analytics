# Home cold-start performance handoff

Date: 2026-09-01

## User report and objective

The user reports that the Home tab still takes about 20 seconds when the
database/app is cold. Measure the real end-to-end cold path and identify
whether the delay is remote connection establishment, SQL, Shiny worker
startup, other startup queries, or browser rendering. The desired connected
Home query target is under 500 ms.

## Local implementation state

There are uncommitted, undeployed changes in:

- `app/R/mod_team_hub.R`
- `app/tests/testthat/helper-server-mocks.R`
- `app/tests/testthat/test-team-hub-ui.R`
- `docs/team_hub_storyline_performance.md`

The implementation combines the six Home datasets into one parameterized SQL
request and parses six JSON sections. It reads only existing objects:

- `team_ratings_preset_cache`
- `team_ppp_ratings_mv`
- `team_four_factors_mv`
- `onoff_default_mv`
- `player_traditional_stats_mv`
- `sub_lineups_stats`

It has required-section/column validation and falls back to the former readers
with a `hub_home_fallback` warning. No database object was created or changed.
Nothing has been committed or deployed.

Focused Home tests most recently passed with 52 assertions and zero failures.
The environment emits Windows `C.UTF-8` test-reporter warnings. A prior full
suite run had the same three unrelated pre-existing Tab 7 failures documented
in the working session.

## Measurements already obtained

Earlier read-only measurements through the configured app role and port 6543:

| Case | Result |
|---|---:|
| Former six Home SQL calls, already connected | about 2.09 s |
| Combined Home SQL, already connected, all-team median | 0.28-0.30 s |
| Combined Home SQL observed connected range | 0.26-0.36 s |
| Typical combined payload | about 81 kB |

All six payload sections matched the old readers. Direct lineup parity against
`fetch_lineups_csv_v2()` passed for all 14 teams.

On 2026-09-01 a fresh R-process measurement produced:

| Phase | Time |
|---|---:|
| Load DB/pool/JSON packages | 780 ms |
| Create pool (`minSize=1`, includes initial connection work) | 1,470 ms |
| First pool checkout | 260 ms |
| `SELECT 1` | 240 ms |
| Combined Home SQL | 810 ms |
| Parse all six JSON sections | 10 ms |
| Script total after R interpreter started | 3,620 ms |
| Shell-observed process wall time | about 4.12 s |

The response was 79,741 bytes with section row counts
`84,14,14,5,19,6`. This run does not explain a repeatable 20-second delay.
It does show that cold connection/setup is materially larger than JSON parsing.

A four-run repeat batch did not return results and was interrupted. The tool
reported a long orchestration duration, but no individual R timing output was
received, so do **not** attribute that delay to PostgreSQL. No `Rscript` or
`Rterm` process remained afterward.

## Benchmark script

The disposable read-only script is:

`euroleague/tmp_measure_home_cold.R`

Run it from `euroleague` one process at a time:

```powershell
& 'C:\Program Files\R\R-4.4.2\bin\Rscript.exe' tmp_measure_home_cold.R
```

It reads `app/.Renviron`, creates a pool matching the app configuration, runs
`SELECT 1`, executes `hub_dashboard_query_sql()` for season 2026/team 2, parses
all sections, and prints only timings/row counts. It does not print credentials
or mutate the database. Network access requires approval outside the sandbox.

Do not start a multi-run orchestration first. Run separate commands so a slow
attempt has visible phase output. Consider adding a bounded connection timeout
for the diagnostic before repeating many times.

## Recommended next measurements

1. Confirm whether the reported 20 seconds is on the deployed app or a local
   app using these uncommitted changes. The deployed app cannot validate this
   implementation until explicitly deployed.
2. Run the benchmark in 3-5 separate fresh R processes, recording shell wall
   time and every printed phase. Include the first attempt after genuine idle
   time if possible; do not claim that a normal new connection forces the
   managed database itself into a cold state.
3. Inspect the app logs for the same slow browser request:
   `hub_storylines_perf` reports `checkout_ms`, `sql_ms`, and `total_ms`;
   `hub_home_fallback` reveals whether the combined response was rejected and
   the six legacy readers ran instead.
4. Measure full Shiny startup separately from Home SQL. Time R interpreter and
   package loading, sourcing `global.R`/`app.R`, pool creation, first session,
   and first browser-visible Home completion. A 20-second user wait with a
   3-4-second DB script points to worker/application startup or other initial
   reactives, not this SQL alone.
5. If browser automation is used, read and follow the in-app-browser or
   Playwright skill first. Capture the exact target URL, navigation timing,
   request waterfall, console errors, and the time at which all Home cards
   become visible. Do not deploy without explicit approval.
6. Test the fallback deliberately with a malformed/missing combined section
   using mocks, and verify one clear warning plus successful legacy rendering.

## Interpretation guardrails

- `SELECT 1` before the combined query warms the connection and backend, so it
  is useful for phase separation but the combined SQL time is not a pure
  first-query measurement. Add a second diagnostic mode that runs the combined
  query as the first statement on a new connection.
- A new R process is not proof of a suspended/cold Supabase database.
- Pool creation with `minSize=1` can include connection setup, so do not add
  `pool_create` and `pool_checkout` as if both necessarily represent separate
  connections without verifying pool behavior.
- The 500 ms goal is currently met for an already-connected backend. It is not
  yet met for first-process startup, and a 20-second end-to-end report remains
  unresolved.

---

# Results — measured 2026-09-01 (evening session)

**The 20-second report is real and reproducible. The Home query is not the
cause.** The combined Home SQL is 0.36 s cold and 0.25-0.27 s warm; it is under
2% of the cold path. The cold path is dominated by serving a 1 MB UI document
and by the six prewarm reference queries.

## End-to-end, local app, Chromium, measured to "all four Home cards rendered"

Harness: the app loaded in a same-origin iframe, polled every 50 ms for
`hub_identity` / `hub_players` / `hub_lineups` / `hub_storylines` to be
non-empty. Parent page's Shiny session closed first so the single-threaded R
process was uncontended.

| Milestone (from navigation start) | Cold (1st load, fresh worker) | 2nd load | 3rd load (warm) |
|---|---:|---:|---:|
| Document TTFB (server builds the HTML) | **10,393 ms** | 990 ms | 1,085 ms |
| DOMContentLoaded | 10,621 ms | 1,136 ms | 1,199 ms |
| Shiny websocket connected | 12,520 ms | 1,870 ms | 1,867 ms |
| **All Home cards rendered** | **22,244 ms** | 10,092 ms | **3,761 ms** |

An earlier, independent run of the same page reached `nav->connected` at
21,709 ms with `nav->dom` 19,152 ms — same shape, larger first-request penalty.

## Server-side attribution for the same three sessions (app log)

| Server phase | Cold | 2nd | 3rd (warm) |
|---|---:|---:|---:|
| `server modules initialized` | 2.190 s | 6.790 s | 0.750 s |
| **`hub_storylines_perf` combined SQL** | **360 ms** (checkout 0.0) | cache hit, no query | cache hit, no query |
| `prewarm complete for season 2026` | 9.230 s | 7.790 s | 1.690 s |
| `client timing: nav->dom / dom->connected` | 10,617 / 1,870 ms | 1,132 / 691 ms | 1,197 / 630 ms |

No `hub_home_fallback` line appeared in any run: the combined reader was used
and accepted every time. Warm loads issue no Home SQL at all — the process-wide
cache serves them.

## Where the cold time actually goes

### 1. ~10 s serving the HTML document — the single largest item

The response is **1,073,417 bytes** on every page load. Timed directly in R
against `.UI_CACHED`:

| Operation | Time |
|---|---:|
| `sys.source("app.R")` (packages, `global.R`, one `build_ui()`) | 2.18 s |
| `build_ui()` alone (what `IBPL_CACHE_UI` avoids) | 3.46 s |
| `htmltools::renderTags(.UI_CACHED)` — **1st call** | **6.59 s** |
| `htmltools::renderTags(.UI_CACHED)` — 2nd / 3rd call | 1.17 / 1.22 s |
| Rendered HTML | 1,059,312 bytes, 18 dependencies |

Two separate costs, and the UI cache addresses neither:

- **One-time ~5.4 s**: the first `renderTags` pays bslib Sass compilation and
  htmltools/JIT warm-up. `.UI_CACHED` caches the *tag tree*, not its HTML.
- **Per-request ~1.2 s**: every page load re-serializes the whole tree. Isolated
  `curl` on a freshly started worker: first GET 10.48 s, then 3.66 / 4.14 /
  4.20 s; once fully warm, TTFB settles at ~1.0-1.1 s.

The document is almost entirely tabs the user is not looking at:

| Tab pane | HTML |
|---|---:|
| Team Ratings | 210 KB |
| On/Off Impact | 191 KB |
| Lineup Data | 173 KB |
| Compare | 173 KB |
| Game Logs | 143 KB |
| Player Stats | 80 KB |
| EuroLeague Lineups / On-Off / Team / Game Logs | 50 / 41 / 40 / 24 KB |
| **Home** | **20 KB** |
| Total panes | 1,145 KB |

**Home is 1.7% of the payload it waits behind.**

### 2. ~9 s of prewarm, not the Home query

`prewarm_for_year()` in `app.R` runs six serial queries after storylines are
released — `fetch_teams_distinct`, `fetch_teams_min`, `fetch_gn_values`,
`fetch_players_basic`, `hub_fetch_team_ratings`, `hub_fetch_team_ff`. Cold that
block is 9.23 s; warm it is 1.69 s. The cost is first-connection setup, not the
statements: a fresh pooled connection costs **1,750 ms** to check out (TCP +
TLS + auth + the `onCreate` `SET statement_timeout`), against 250-270 ms for the
query itself.

### 3. The Home query meets its target

Standalone, `app_readonly` on port 6543, pool config mirroring `global.R`
(`minSize=0`, `onCreate` SET, `idleTimeout=15000`):

| Phase | Time |
|---|---:|
| Package load | 660 ms |
| `dbPool()` create (no connection at `minSize=0`) | 10 ms |
| **First checkout (connect + TLS + auth + SET)** | **1,750 ms** |
| **Combined Home SQL, first statement on that connection** | **580 ms** |
| JSON parse, six sections | 20 ms |
| **Combined Home SQL, warm (5 reps)** | **250 / 260 / 260 / 270 / 270 ms** |
| Re-checkout from pool + query | 0 ms + 250 ms |
| Payload | 79,741 bytes, sections 84,14,14,5,19,6 |

Warm median **260 ms**, cold-connection **580 ms**, in-app cold **360 ms** —
the sub-500 ms goal is met for the connected case and effectively met cold.

## Answers to the handoff's open questions

1. The 20 s is the **local app with these uncommitted changes**, reproduced at
   22.2 s. Not a deployment artifact.
2. Repeat runs done in separate fresh processes; every phase printed. The
   earlier four-run batch failure was orchestration, not PostgreSQL — confirmed,
   single-run invocations never hung.
3. `hub_storylines_perf` reports 360-470 ms `total_ms`; `hub_home_fallback`
   never fired.
4. Startup measured separately: R interpreter + `library(shiny)` 0.42 s, worker
   to `Listening on` ~5 s, then the per-request UI cost above. Confirmed the
   20 s wait is worker/UI startup, **not** this SQL.

## Candidate fixes, in measured order of payoff

Not implemented; no code was changed by this measurement session.

1. **Stop shipping ten unopened tabs in the first document** (~1.0 MB, ~1.2 s
   per load plus the ~5.4 s one-time render). Render tab panes on first
   activation, or serve Home-only initially. Largest win by a wide margin.
2. **Pre-render the UI HTML once per worker**, not just the tag tree — cache the
   `renderTags` output rather than `.UI_CACHED`. Removes the ~5.4 s one-time
   penalty and the ~1.2 s per-request cost. Note this interacts with
   `is_bookmark_request()`, which must still take the uncached path.
3. **Warm one pooled connection at worker start.** The 1,750 ms first checkout
   is paid by whichever query runs first, and `minSize = 0` guarantees it lands
   on a user request. Cheap and independent of the above.
4. **Parallelise or defer `prewarm_for_year()`.** Six serial round trips gate
   nothing the user is looking at on Home.

## Benchmark scripts

- `euroleague/tmp_measure_home_cold.R` — original, unchanged.
- `euroleague/tmp_measure_home_phases.R` — new. Mirrors `global.R` pool config,
  runs the combined SQL as the **first** statement on a new connection (no
  `SELECT 1` warm-up), then N warm repeats, a re-checkout, and an optional
  idle-expiry probe. Read-only.

  ```powershell
  & 'C:\Program Files\R\R-4.4.2\bin\Rscript.exe' tmp_measure_home_phases.R 5 20
  ```

---

# Fixes applied — 2026-09-01 (evening session)

Two changes, both verified. Nothing deployed.

## Fix A — warm one pooled connection off the boot critical path

`app/R/global.R`. A `later::later()` checkout scheduled right after the pool is
created, so R connects once the event loop goes idle.

Why not `minSize = 1`, measured in isolation against the real pool config:

| | `minSize = 0` | `minSize = 1` |
|---|---:|---:|
| `dbPool()` create | 0 ms | 1,390 ms |
| **First checkout** | **2,240 ms** | 300 ms |
| Second checkout | 0 ms | 0 ms |

`minSize = 1` does move the cost to boot, but at app level it cost **+2.7 s of
boot** (5.0-7.7 s → 8.9-10.3 s over 3 alternating runs) against −1.7 s on the
request — a loss whenever the worker is booted by the request it then serves.
This also stands against the standing "don't fix it" note in `CLAUDE.md`, whose
2026-08-18 measurement was of *steady-state* latency across a 22 s idle gap,
where the connection was still pooled either way. That finding stands; it just
does not cover the first checkout.

The deferred warm-up costs boot nothing (4.98 / 5.28 / 5.05 s with it on;
5.06 / 4.74 s with `POOL_PREWARM=false`) and the first session's prewarm block
fell 9.23 s → 6.55 s, with `checkout_ms=0.0` on the Home query.

## Fix B — cache the rendered page, not just the tag tree

`app/app.R`. `.UI_CACHED` avoided rebuilding the tag tree but Shiny still ran
`renderTags` on ~1 MB for every request. `shiny:::uiHttpHandler` returns an
`httpResponse` verbatim (read against shiny 1.9.1), so `ui()` now returns a
cached rendered response, pre-rendered off the boot path by `later::later()`.

Bookmarked requests still take the uncached `build_ui()` path — `navbarPage()`
calls `restoreInput()` while the tree is built. Verified: a plain request is
~0.5 s / 3 ms, `?_inputs_&main_tabs="team_ratings"` takes 3.26 s and restores
onto Team Ratings with 15 populated dropdowns.

**Output identity.** Responses differ across workers only in `data-tabsetid`,
`bslib-accordion-*` ids, and the random default Home team — all generated at
`build_ui()` time and therefore already fixed per worker. Two *uncached*
workers differ from each other in exactly the same way, and after normalising
those ids all three pages (uncached worker A, uncached worker B, cached worker)
hash identically.

## Measured effect

Server-side `GET /`, isolated with `curl`:

| | Before | After |
|---|---:|---:|
| First request after worker start | 10,479 ms | **7.7 ms** |
| Steady-state | 1,243-1,305 ms | **3.0-4.7 ms** |

End-to-end, first session on a freshly booted worker:

| Milestone | Before | After |
|---|---:|---:|
| Document TTFB | 10,393 ms | **21-33 ms** |
| `nav->dom` (app log) | 10,617 ms | **221-332 ms** |
| `nav->connected` | 12,487 ms | **1,651-2,214 ms** |
| **All Home cards rendered** | **22,244 ms** | **9,614 ms** |
| `prewarm complete` | 9.230 s | 7.110-8.290 s |
| Home combined SQL | 360 ms | 470-510 ms (unchanged; already at target) |

Worker boot is unchanged (~4-5 s).

## Verification

- Full suite: **FAIL 0 | WARN 0 | SKIP 4 | PASS 1318**.
- `test-idle-restore-bookmarking.R` asserted the old one-line `ui()` body as
  source text; updated to assert the same property against the new shape (both
  caches sit inside the one `!is_bookmark_request(request)` guard).
- All 11 tabs clicked through in Chromium: every pane renders with populated
  dropdowns (On/Off 13/16, Lineup Data 14/18, Team Ratings 14/16, Game Logs
  10/12, EuroLeague tabs, Player Stats, Compare 31/41).
- Console shows only the 3 known pre-existing `hub_storylines` output-state
  errors, unchanged by this work.

## Why tab lazy-loading was not implemented

It was the top-ranked fix before Fix B, on the basis that ~1.0 MB of the 1.15 MB
payload is tabs the user has not opened. Fix B removes the cost that ranking was
built on. What remains for lazy-loading to win is the 1 MB transfer and browser
parse, now measured at **`nav->dom` 221-332 ms total, against a TTFB of 21-33 ms**
— so roughly **0.2 s of a 9.6 s cold load**.

Against that: deferring a tab's content means its inputs do not exist at session
start, so the server-populated choices pushed by `update*Input()` during session
init are dropped (`CLAUDE.md`'s own "`uiOutput`/`renderUI` causes NULL on startup
— use static inputs + `update*Input()`" pitfall). Making that correct requires
re-running per-tab initialisation on first activation across ten tabs.

Recommend leaving it. Worth revisiting only for slow mobile connections, where
the 1 MB (~130 kB gzipped) transfer matters more than it does here.

## Remaining cold budget (9.6 s)

Now server-side session work, not the UI:

| | |
|---|---:|
| Document | 0.03 s |
| Websocket connect (`dom->connected`, clicks dead) | 1.4-1.9 s |
| `server modules initialized` | 1.7-2.0 s |
| `prewarm_for_year()` — 6 serial reference queries | 7.1-8.3 s |
| of which Home combined SQL | 0.47-0.51 s |

The next real target is `prewarm_for_year()`.

---

# Corrections — same session, later

Three things above are wrong or need qualifying. Read this section before
acting on any number in this document.

## 1. The machine's disk was 100% full for the whole session

```
C:   953G size   950G used   3.2G avail   100% use
```

A benchmark loop failed outright with `No space left on device`. Every
measurement in this document was taken under that condition, which on Windows
degrades R package loading, temp files, page-file growth, `bslib`'s Sass cache
writes and `file.copy` of HTML dependencies.

It shows in the spread. Identical code, repeated runs:

| | observed range |
|---|---|
| Worker boot to `Listening on` | 3.7 - 15.1 s |
| First `renderPage` | 1.5 - 9.6 s |
| All Home cards | 6.5 - 26.4 s |

The project is not the cause (repo ~110 MB: `.git` 76M, `exports` 26M,
`etl/logs` 6.6M). **Re-measure the cold-start figures on a healthy disk before
quoting them.**

### What survives, and what does not

- **Survives**: the byte-identity verification of the rendered-HTML cache, and
  steady-state `GET /` of **1.24 s -> 0.004 s**. That is ~300x, measured many
  times across separate workers, far above this noise floor.
- **Does not survive**: the absolute cold figures, including "22,244 ms ->
  9,614 ms" in the tables above and in commit `512efe9`'s message. The
  direction is right; the magnitudes need redoing.

## 2. The headline improvement does not apply to "start the app, then open it"

The 9.6 s figure needs a worker that has been booted and idle long enough for
the `later::later()` pre-render to finish. Starting the app and loading it
immediately makes the first request race that pre-render, and it loses.
Reproduced end to end at **41.5 s**:

| | |
|---|---:|
| Worker boot to `Listening on` | 13.7 - 15.1 s |
| First `GET /` (races the pre-render) | 8.8 - 9.6 s |
| Session init | ~15 s |

Even here the cache still worked on the next request: `first_GET=8.81s`,
`second_GET=0.74s`. But manual testing by restarting the app will always pay
boot plus one full render. Leave the app running when testing by hand.

## 3. The tab-deferral recommendation is withdrawn

The "Why tab lazy-loading was not implemented" section above stands, but the
*replacement* proposal made after it -- deferring each tab's **server** module
to first activation, claimed to be worth 5.2 s -- does not survive.

What the profiler actually found, sampling session init at 2 ms:

| | R time | share |
|---|---:|---:|
| **`update*Input` (all choice/value pushes)** | **0.00 s** | 0.0% |
| DB (`result_fetch`/`result_create`/`result_bind`) | 0.59 s | 48.8% |
| `observe`/`observeEvent` registration | 0.41 s | 33.7% |
| `cached_ref_query` | 0.08 s | 6.7% |
| `fetch_gn_values` | 0.04 s | 3.4% |
| `renderDT` / DT setup | 0.00 s | 0.0% |

**Total R execution across a 9.03 s session-init window was 1.22 s** -- R was
idle ~7.8 s. Cross-checks: the browser recorded **0 long tasks** over a 12.5 s
load (256 bound inputs, 171 selectize widgets), and the whole session init is
**35 websocket messages, 17 kB**. Neither side is compute-bound and there is no
message storm.

1.22 s of R work cannot account for a claimed 5.2 s saving. That claim came
from one paired run of a log delta (`prewarm start` 8.91 s -> 3.67 s). Measured
end to end on the thing a user waits for, the ten-tabs-disabled build came in at
**10,379 ms**, sitting inside the full build's own 6,515 - 12,554 ms range.

**Method lesson: no single-run A/B on this machine.** Anything smaller than ~2x
is unmeasurable without n>=5 per configuration on a quiet, non-full disk.

## 4. Two smaller findings worth keeping

- **`log_startup()` measures from session start, not from the step it labels.**
  `startup_t0` is set at the top of `server()`. So `prewarm complete (9.230s)`
  never meant 9.2 s of prewarming. Marks placed around the block itself gave
  **0.28 s**, and its four real queries total 1,270 ms cold
  (`gn_values` 290, `players_basic` 430, `team_ratings` 270, `team_ff` 280 --
  all at the ~250 ms round-trip floor). `fetch_teams_distinct`/`fetch_teams_min`
  never touch the DB; they return a static roster. **`prewarm_for_year()` is not
  a performance target.**
- **The big Teams/Players pickers are already lazy.** They are `selectize` with
  `server = TRUE`, ship as an empty `<select>`, and fetch over XHR only on
  interaction -- `teams` and `on_opponents` never receive `<option>` tags at
  all. Only `*_gn_min` / `*_gn_max` / `*_last_n` (plus a team dropdown on four
  tabs and two player pickers on Compare) are pushed at session start.

## Next steps

1. Free disk space and confirm `C:` has real headroom.
2. Re-run the cold-start measurements and correct the figures in this document
   and in `CLAUDE.md`. Compare like with like: worker booted and idle, versus
   worker booted by the request.
3. Only then consider anything further. The remaining cold time is R *waiting*,
   not R computing, and what it waits on is still unexplained -- that is the
   open question, not `prewarm_for_year()` and not tab deferral.
4. `sass`/`bslib` theme compilation is 0.53 s of the first render and is
   currently redone in every worker, since the cache lives in the per-process
   `tempdir()`. A persistent `sass` cache directory is a plausible small win,
   unmeasured on a healthy disk.

## Benchmark scripts (both gitignored by `tmp_*`)

- `euroleague/tmp_measure_home_cold.R` -- original, unchanged.
- `euroleague/tmp_measure_home_phases.R` -- mirrors `global.R` pool config and
  runs the combined SQL as the first statement on a new connection, then N warm
  repeats plus an optional idle probe. Read-only.
