# ADR: API layer owns query construction (Phase-2 endgame)

**Date:** 2026-07-27 · **Status:** Accepted, deferred until trigger · **Context:** follow-up to the 2026-07-27 SQL function perf review

## Decision

When a tab is served exclusively by React + Plumber (no Shiny path left for it),
its mega-signature SQL function(s) are retired and the Plumber (or successor
FastAPI) route builds the SQL itself. Until that trigger, the stored functions
stay — they are the single source of truth shared by both frontends and must
not be duplicated into R query builders.

## Why the 29-arg functions exist (and why they are not a performance problem)

`fetch_lineups_all` / `fetch_lineups_csv_v2` and friends mirror the full filter
panel in one signature because Shiny (direct DB from shinyapps.io) and React
(via local Plumber) must run *identical* logic. The DB is the only layer both
share. Costs are purely maintenance: signature-generation overloads (cleaned
2026-07-27, migration `sql/migrations/2026-07-27_drop_stale_lineup_fn_overloads.sql`),
exact-signature DROPs, 29 positional binds per caller. Argument passing itself
costs nothing at execution time.

## What the API-owned model buys

1. **Planner sees only active predicates.** The route composes WHERE clauses
   from the filters actually set — no kitchen-sink `(p_x IS NULL OR …)` guards,
   which also removes the need for the `plan_cache_mode = force_custom_plan`
   workaround (kept while functions exist; the generic-plan cliff was
   reproduced live on 2026-07-27: 5s → >120s timeout).
2. **No SQL signatures to evolve.** Filters are named JSON/query params,
   validated fail-closed at the HTTP boundary (unknown keys → 400, not NULL).
3. **Ordinary versioning and testing** (routes + HTTP tests instead of
   CREATE/DROP FUNCTION migrations).

## Migration order (per tab, at trigger time)

1. Confirm the tab has no Shiny path (or Shiny consumes the API).
2. Port the function body into the route's parameterized SQL builder
   (`$1..$n` only — never string interpolation of user values).
3. Byte-diff route output vs function output on a representative case matrix
   (reuse the `scripts/perf_tuning_baseline.R` harness pattern).
4. Drop the stored function (exact signature) + re-run
   `scripts/apply_db_security.R` (CONFIRM=1).
5. Last function gone → tighten the DB role: drop the EXECUTE allowlist
   mechanism, keep SELECT + RLS only, and make the API the sole entry point.

## Security note

The current model (app_readonly = SELECT + EXECUTE allowlist + RLS) assumes
two DB entry points. Dynamic SQL from the API is only acceptable once the API
is the *single* entry point; injection safety then lives in the route builders
(parameterized statements, per the repo rule).

## Rejected alternatives

- **jsonb filter argument on the SQL functions now** — fail-open on typo'd
  keys unless an in-function allowlist is added; keeps all logic in plpgsql;
  doesn't remove signature churn for typed core args. Acceptable hybrid only
  if a large filter batch lands *before* the API trigger.
- **Duplicating the query builder in Shiny R + Plumber R now** — two sources
  of truth during the transition; drift risk outweighs arity ugliness.
