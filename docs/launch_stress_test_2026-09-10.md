# Launch stress test - 2026-09-10

Target: the public Posit Connect deployment.

Method: independent browser contexts, one Shiny session per user, randomized
tab paths, 65%-135% think-time jitter, staggered starts, four actions per user.
Queries were allowed to overlap naturally. The test was read-only.

## Control matrix

| Users | Cadence | Successful sessions | Actions | Nav p90 | Action median | Action p95 |
|---:|---:|---:|---:|---:|---:|---:|
| 5 | 10s | 4/5 | 17/20 | 8.5s | 5.3s | 5.5s |
| 10 | 10s | 9/10 | 40/40 | 16.6s | 5.4s | 7.0s |
| 20 | 10s | 18/20 | 75/80 | 35.5s | 5.6s | 9.6s |
| 5 | 30s | 4/5 | 17/20 | 10.4s | 5.4s | 5.8s |
| 10 | 30s | 7/10 | 33/40 | 17.2s | 5.5s | 7.5s |
| 20 | 30s | 19/20 | 77/80 | 41.6s | 5.5s | 7.9s |
| 5 | 60s | 3/5 | 16/20 | 10.7s | 5.5s | 7.1s |
| 10 | 60s | 9/10 | 37/40 | 15.3s | 5.4s | 7.1s |
| 20 | 60s | 17/20 | 73/80 | 41.8s | 5.4s | 7.6s |

Totals: 90/105 successful full journeys and 385/420 completed actions.

Failure classification across the 105 control sessions:

- 10 navbar hover-menu click interceptions.
- 4 sessions remained Shiny-busy for more than 120 seconds.
- 1 session completed its actions but received three 404 responses from
  session-scoped selectize data endpoints.
- 318 browser console errors, almost exactly three per session, from the
  `hub_storylines` output entering inconsistent Shiny progress states.

## Mixed cadence

The same concurrent population contained shuffled, balanced 10s, 30s, and 60s
users. Starts, paths, and pauses remained independent, so requests overlapped.

| Users | Successful sessions | Actions | Nav p90 | Action median | Action p95 |
|---:|---:|---:|---:|---:|---:|
| 5 | 2/5 | 12/20 | 10.9s | 5.4s | 7.3s |
| 10 | 7/10 | 34/40 | 15.4s | 5.3s | 6.0s |

All six mixed-run failures were navbar click interceptions. The 20-user mixed
run was stopped at the maintainer's request; the completed 20-user controls are
the capacity evidence.

## Conclusion

Steady-state query latency did not collapse at 20 users: median action latency
stayed around 5.4-5.6 seconds, and control p95 stayed below 10 seconds. Startup
did degrade sharply: p90 navigation rose from roughly 9-11 seconds at five
users to 35-42 seconds at twenty users.

Recommendation: do not officially launch until the navigation interception and
the Shiny-busy/session-route failures are resolved. The primary risk is
reliability, not a simple database throughput cliff.

## Actions

1. Fix the navbar hover-menu trap in `app/www/app.css` and `app/www/app.js`.
   Pure `:hover` menus can cover wrapped adjacent tabs and keep intercepting the
   pointer. Test random cross-tab navigation at 1280px and 1440px widths.
2. Investigate the four >120-second Shiny-busy sessions in Connect logs and add
   per-session pool-wait/query/output timing. They occurred during initial load
   and after Player Stats, Game Logs, and Team Ratings paths.
3. Trace the three 404s for `dataobj/ld_opponents` and `dataobj/home_team` using
   session `bd78f1a8e05be326d33b2a64c4f9556c`. Confirm whether the worker or
   session was recycled while its WebSocket remained usable.
4. Remove the `hub_storylines` reactive self-invalidation/progress race. The
   dashboard reactive reads the shared data version and can update that same
   version while an output is rendering.
5. After fixes, rerun the mixed 20-user test. Launch gate: zero click failures,
   zero HTTP/request failures, zero >60-second busy hangs, and action p95 below
   10 seconds.
