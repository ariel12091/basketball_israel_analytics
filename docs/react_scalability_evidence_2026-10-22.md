# React versus Shiny: scalability evidence, 2026-10-22

**Verdict: greater end-to-end scalability is not yet proved.** Feature parity,
browser checks and successful Preview requests do not establish concurrent-user
capacity. Pause Home/Compare migration while this question is resolved.

## Measurement completed

Ran `node scripts/benchmark-cache.mjs` from `web/` against the local Next.js
production server on port 3100, using two real Gameflow API answers (Israeli
League and EuroLeague). Warmed both URLs first; each measured stage then made
300 requests, consuming and parsing the entire JSON response. Both answers were
validated as successful nonempty Gameflow payloads. Output:
`output/scalability/warm-cache.json` (generated, ignored).

| Concurrent HTTP requests | Completed / attempted | p95 response + body + JSON parse | Maximum | Requests/s during burst |
| ---: | ---: | ---: | ---: | ---: |
| 1 | 300 / 300 | 4.6 ms | 8.4 ms | 316 |
| 10 | 300 / 300 | 28.9 ms | 40.3 ms | 487 |
| 50 | 300 / 300 | 106.0 ms | 176.5 ms | 515 |
| 150 | 300 / 300 | 506.6 ms | 525.2 ms | 532 |

All 1,200 measured requests succeeded. The preliminary warm-up requests took
1,886 ms and 913 ms. Responses were 156,166 and 115,712 bytes respectively.

This proves only that **this workstation's running production server delivered
these two warm answers under this short burst workload**. These are concurrent
HTTP requests, not 150 independent browser users. Measured stages lasted less
than one second each; warm-ups are excluded. The load generator
and server shared a machine. There was no Shiny run, no Vercel CDN measurement,
no sustained cache-expiry cycle, and no distinct-filter/cold-reader measurement.
Cache hits are inferred from the 60-second application cache and warmed URLs;
database query counts were not instrumented. No result here is a deployment
capacity guarantee or a Shiny-versus-React speedup ratio.

## What the implementation actually establishes

- API responses request 60-second CDN caching; readers have bounded per-process
  caches that share in-flight promises for identical keys. See
  `web/src/server/onoffRepo.ts` and the Gameflow equivalent.
- Browser-side display filters and table operations avoid some server work.
  That benefit depends on the specific control; game-context changes still
  request database-backed results.
- The database adapter defaults to two active snapshots and twenty waiting
  requests per process, with a 500 ms queue wait. It rejects overload. This
  protects one process; it does not prove fleet-wide database capacity.
  See `web/src/server/db.ts` and `deadline.ts`.
- Initial server-rendered pages call the readers directly. CDN headers on an
  API route therefore do not prove that initial page loads bypass the database.
- Expensive filtered paths still call the existing Shiny SQL functions, using
  up to the existing 20-second SQL budget. The handoff records filtered
  EuroLeague Four Factors at approximately 9.6 seconds on both builds. This is
  historical evidence of an unresolved bottleneck, not a new timed result.
- Shiny already has a shared process cache: default 64 MiB, one-hour age, keyed
  by its version/context. Its pool defaults to three connections. Merely adding
  a cache in React is not evidence of an advantage over this baseline.
- A fleet-wide web connection budget, actual CDN hit ratio, miss-path capacity,
  end-to-end browser latency under load and cost at that load remain unproved.

The September 10 Shiny stress handoff records 18/20 sessions, 72/80 actions,
p95 interaction 10.334 seconds and p90 startup 42.8 seconds. That run predates
later changes and used a different workload and environment. Comparing it with
the warm API burst above would be invalid.

## Required comparison before claiming an improvement

Use the same seasons, filters, user journeys and data snapshot on current Shiny
and Vercel Preview. Cover all three competitions and the migrated tabs, especially
filtered On/Off, starters, custom clutch, Lineups and Player Stats. Separate:

1. Warm repeated answers, including measured CDN hit/miss headers.
2. Cold application caches and simultaneous identical misses.
3. Distinct uncached filter combinations and cache-expiry churn.
4. Initial page loads and browser-visible table completion.

Ramp 1, 10, 20, 50, 100 and 150 virtual users with the same seeded sequence and
think times; sustain each stage and repeat trials. Record latency distributions,
successful actions/second, all errors/timeouts/rejections, response correctness,
payload sizes, DB connections/query durations, cache mix and hosting cost. Do
not discard failed requests from percentiles or call a 503 a successful result.
Measure per scenario/competition, not just an aggregate hiding a slow reader.

The existing migration criterion is p95 filter responses under two seconds,
none over five seconds, at 150 simulated users, within the $50/month budget.
Browser-visible completion should be reported separately from network timing.
Meeting a cached-only case does not pass this gate. A fair comparison must also
report Shiny's current capacity under exactly the same conditions.

The migration design requires production load tests only in announced quiet
hours after ETL and outside game days. The Preview shares the production DB,
so a full miss-heavy run needs that same scheduling constraint. This turn ran
only the bounded local warm-answer probe; no public Shiny or Vercel stress test
was performed and no DB/hosting settings were changed.
