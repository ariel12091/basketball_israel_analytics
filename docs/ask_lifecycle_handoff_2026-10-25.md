# Ask cancellation checkpoint — 2026-10-25

Branch: `sql/web-foundation`, following player-name checkpoint `03ff981`.
The Ask cancellation and request-ownership gaps (R1/R2) are fixed in this
checkpoint. Production readiness remains open.

## Changes

- `web/src/ask/lifetime.ts`: owns the 40-second turn deadline, parent abort,
  cancellation-aware waits and timer/listener cleanup. Late provider failures
  are observed even when a provider ignores abort.
- `web/src/ask/turn.ts`: applies that lifetime to provider calls and tool waits,
  including history replays; cancellation prevents further model steps.
- `web/src/app/api/v1/ask/route.ts`: forwards request abort and response-stream
  cancellation, guards terminal writes/close and logs the turn once.
- `web/src/components/AskProvider.tsx`: assigns stable request/turn IDs; owns
  fetch, reader and timer cleanup; ignores stale responses; stops at final/error
  events; provides a 45-second browser timeout and permits retry. Reset,
  replacement and unmount cancel work. Closing the panel and ordinary in-app
  navigation preserve the conversation.

Shared repository cache fills retain their existing database deadlines.
Cancelling one visitor's answer does not cancel another visitor's shared read.
This checkpoint does not establish PostgreSQL cancellation or socket recovery.

## Validation

- Full local unit suite: **740/740 passed**, 34 files, zero skips.
- Local production-mode Ask browser suite: **52/52 passed**, zero skips/retries,
  at 390px and 1440px. Twelve new ownership checks cover all three competitions,
  stalled stream timeout/retry, reader-lock release despite stalled source
  cleanup, and trailing events after a final answer. The other 40 executions
  repeat 20 existing width/scenario combinations under both projects.
- Production build, its TypeScript check and targeted ESLint passed.
- React Doctor reported no findings in the changed Ask files. Its overall
  result remains 55/100 with one existing OnOff cleanup diagnostic and 172
  warnings. The flagged OnOff effect returns timer/listener cleanup.

Tests, fixtures and generated verification material remain local/unstaged under
the existing repository policy. Preserve them when cleaning disk; the commit
alone cannot reproduce the complete local suite. Local evidence:

- `output/feature-audit/ask-lifecycle-units-2026-10-25.json`
- `output/feature-audit/ask-lifecycle-browser-2026-10-25.json`
- `output/feature-audit/ask-lifecycle-doctor-2026-10-25.json`
- `docs/verification/ask_lifecycle_handoff_2026-10-25.md` (detailed local report)

Browser checks used `web/tmp_ask_playwright.config.ts` to run the fresh production
build on 3101 with Ask/followups enabled. That test server was stopped; the
existing server on 3100 was preserved and may serve an older build.

## Resume here

1. Implement version-aware cache invalidation (X-08) and dashboard request limits
   (X-20). Ask's per-process limiter does not protect every dashboard reader.
2. Address R3–R8: real connection recovery, fleet connection budget, cache
   retention/replacement races, limiter cardinality, late version polling and
   queue-configuration validation. Deterministic pool tests do not prove heap
   stability or actual PostgreSQL socket recovery.
3. Scope the fake-provider file-loading path: Next still warns that it traces
   the whole project. Inspect the actual release bundle.
4. Complete the inventory acceptance gate and live React/Shiny parity; verify
   real-provider disconnect behavior on the release Preview before enabling
   Ask in production. Offline mocks do not establish hosted cancellation or
   provider billing behavior.
5. Run the agreed quiet-window capacity/cost checks, then validate the exact
   release, deployment settings and rollback. Production promotion is separate.

No database/schema/ETL changes or shared-database load tests were performed.
This request commits the checkpoint; it does not request a push or deployment.
