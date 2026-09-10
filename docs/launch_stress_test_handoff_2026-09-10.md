# Launch stress-test handoff — 2026-09-10

## Current state

The public Posit Connect app is Git-deployed from `main`. The latest pushed
runtime commit is `86c77a4`.

Completed runtime fixes:

- `66a85bd`: navbar layering fix and `hub_storylines` reactive flush fix.
- `5b82dcb`: client-side team selectors to remove session-scoped selectize 404s.
- `4faa1f4`: slow/error database telemetry (`[db_perf]`) with pool checkout,
  SQL, total duration, session ID, and status.
- `86c77a4`: defer the scoreless-game scan until Game Logs needs ribbon links.

## Latest stress evidence

The 20-user mixed run used ten 10-second users and ten 30-second users, with
independent jitter and naturally overlapping actions.

- 18/20 successful sessions; 72/80 actions completed.
- Zero navigation click failures, HTTP errors, request failures, or console
  errors.
- Two sessions remained Shiny-busy for more than 60 seconds on first entry to
  Lineup Data and Player Stats.
- Interaction p95 was 10.334 seconds (gate: under 10 seconds).
- Startup navigation p90 was 42.8 seconds.

The launch gate therefore remains **not passed**. The dominant remaining risk
is first-load contention, not ordinary steady-state query latency.

## Next step

Run the same 20-user mixed test after the latest deployment has settled, while
collecting Connect log lines for the run window (`[db_perf]`, approximately
14:37:44–14:41:19 IDT from the prior run). Correlate the two >60-second users
with their database timings:

1. If `checkout_ms` dominates, tune or reduce concurrent pool demand.
2. If `sql_ms` dominates, optimize/cache the Lineup Data and Player Stats
   first-load queries.
3. If both are low, add per-output completion timing; the current global
   Shiny-busy signal may be waiting on an unrelated output.

Do not approve launch until a rerun reaches 20/20 sessions, 80/80 actions,
zero HTTP/request failures, zero >60-second busy hangs, and p95 interaction
latency below 10 seconds.

## Local verification

Relevant R suites passed with locale warnings only. Public browser checks after
the prior deployment showed zero console errors, navbar link z-index `10001`
above the hover menu's `10000`, and client-side `home_team`/`ld_opponents`
selectize controls.
