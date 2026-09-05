# `bind message supplies 1 parameters, but prepared statement "" requires 0`

Open. Seen 2026-09-05 on the Israeli Game Logs tab (tab 4), locally, shortly
after an app restart. Not reproduced since. Recorded so the next sighting
starts with evidence instead of a fresh investigation.

## Symptom

The tab fails to launch. Full text:

```
Failed to fetch row : ERROR:  bind message supplies 1 parameters,
but prepared statement "" requires 0
```

This is a PostgreSQL wire-protocol error, not a SQL error. RPostgres sends a
parameterised query as two messages, Parse then Bind. The error means Bind
arrived at a backend where the unnamed statement `""` had been prepared with no
parameters — i.e. the two messages did not reach the same backend, or the
statement was reset between them.

## Prior sighting

`docs/bookmark_restore_root_cause_2026-07-30.md` § "Known intermittent issue"
records the identical error on a tab 5 restore, attributes it to the Supabase
transaction pooler, notes it did not reproduce on a warm instance, and
explicitly leaves it unfixed as "a connection-pooling problem".

## Ruled out on 2026-09-05, with evidence

| Hypothesis | Evidence against |
|---|---|
| Tab 4's own queries are malformed | All three run clean against a direct connection: 442 / 36,829 / 36,829 rows. Each has exactly one `$1` and one parameter. |
| The shared wrapper mangles parameters | `db_get_query()` (`global.R`) is a plain passthrough to `DBI::dbGetQuery`; no rewriting. |
| The pool or the pooler is broken right now | 12/12 parameterised queries succeeded through a pool built with the app's exact config on port 6543. |
| The branch in flight caused it | `git diff main..HEAD -- app/R/` contained no line touching SQL, `params`, `db_get_query`, or any reactive/observer. The four Tab 4 changes were table headers, a CSV button and one display column. |
| `helpers.R` was corrupted by scripted edits | Function set compared against `main`: 104 → 107, three added, none removed, none changed. |

## NOT ruled out

- **A cold-worker race.** Tab 4 runs two `observeEvent(..., ignoreInit = FALSE)`
  observers (`server_tab4.R:261`, `:302`) that fire during startup, concurrent
  with the `later::later()` pool prewarm (`global.R:463`). This is the mechanism
  the 2026-07-30 note proposes.
- **Against that:** the deployed app does not show it, and every shinyapps.io
  worker starts cold, so the deployed app should be *more* exposed, not less.
  The user's judgement on 2026-09-05 was that warm-vs-cold is not the variable.
  That contradiction is unresolved and is the most useful thread to pull next.
- Whether the failure is deterministic or intermittent. Unknown — it was seen
  once and not retried before the app was restarted.

## What to capture next time

1. **Is it deterministic?** Reopen the tab without restarting. Once vs every
   time separates a code defect from a startup race.
2. **Which query?** The message names no relation. Add a temporary
   `app_log()` of the statement in `db_get_query()`'s error path, or run with
   `options(shiny.fullstacktrace = TRUE)`.
3. **How long had the worker been up?** Seconds vs minutes tests the race.
4. **Does it survive `git checkout main`?** The cheapest attribution test; it
   was proposed on 2026-09-05 but not run.

## Candidate fix, not applied

A single retry in `db_get_query()` guarded on `grepl("prepared statement", msg)`.
A retry checks out a fresh connection, so a Parse/Bind split does not recur. It
was written and reverted on 2026-09-05 because the diagnosis was not confirmed
and it would have changed every query path in the app while the bug was still
being characterised. Do not apply it before step 1 above establishes whether the
failure is even intermittent — a deterministic failure would be masked, not
fixed, by a retry.
