# Runtime readiness checkpoint - 2026-10-25

Branch: `sql/web-foundation`, following `1356ede`. Application safeguards are
implemented locally. Production promotion remains pending the release gates below.

## Changes

- Repository caches check the existing Supabase publication version before reuse.
  Concurrent version probes coalesce without retaining a stale version by TTL.
  Cache keys include league/version/context, pending fills share work, and late
  evicted completions cannot remove replacements or corrupt byte accounting.
  Unknown/mismatched versions, partial Home results and failures are not retained.
  Existing entry caps remain; each cache additionally has a 16 MiB serialized
  retention budget. This is not a V8 heap measurement or aggregate fleet budget.
- Live dashboard/version HTTP responses use `no-store` so an unversioned CDN
  response cannot bypass the origin freshness check. This adds a version query
  even on cache hits; capacity testing must include that cost. Publication still
  relies on existing `app_meta`/`load_runs` signals; no ETL/schema change was made.
- The Next proxy applies admission to dashboard GET/HEAD readers, including SSR
  and API requests under the same per-reader allowance. Defaults: 50/session/min,
  5,000/IP/min; configure `DASHBOARD_SESSION_RATE_LIMIT` and
  `DASHBOARD_IP_RATE_LIMIT` with integers 1..10,000. Rejections include no-store
  and Retry-After; page rejections fit phone and desktop layouts. Static assets,
  the static guide and Ask retain their existing behavior.
- Dashboard and Ask limiters bound both key count and retained timestamps. They
  fail closed at capacity and do not charge an unrelated bucket on rejection.
  Limits are **best effort per warm instance**, not shared deployment quotas.
  IP attribution depends on the host's trusted forwarding-header behavior.
- `DB_POOL_MAX` accepts integers 1..20 (default 2); `DB_QUEUE_MAX` accepts 0..1,000
  (default 20). Invalid or blank values fail before pool creation. Idle client
  failures log only the error code/name. Fleet connection budgeting is separate.
- Version polling aborts superseded/unmounted requests and ignores late results,
  including responses whose JSON parsing started before cleanup.
- The externally supplied fake-provider script is excluded from bundle tracing.
  The local Ask route trace fell from 785 files/755.06 MiB to 200 files/2.46 MiB
  of referenced source, with zero golden/e2e/test paths. This is not a compressed
  hosted function size; inspect the release deployment too.

## Validation

- Full units: **787/787 passed**, zero failures/skips.
- Typecheck, production build and targeted lint passed.
- Full production-mode browser suite: **644 passed, 36 opt-in live checks
  skipped, zero failures/flaky retries**, across phone 390px and desktop 1440px.
  Includes six real Next proxy checks: all three leagues share SSR/API allowance,
  return 429/Retry-After/no-store, and render rejection pages without overflow.
  Test execution took 9.8 minutes. Windows hung during owned-server teardown;
  the isolated 3102 server was stopped explicitly to let the reporter finish.
  The report's 195-minute duration includes that teardown wait. Existing 3100
  was preserved. Browser fixture success does not certify live Supabase reads.
- React Doctor: 58/100, one error and 170 warnings across the branch. Its existing
  OnOff effect-cleanup diagnostic is a false positive: the effect explicitly
  clears its timer and removes all four listeners. Other findings in touched
  repository/TabKit files refer to unchanged code; no diagnostic targets the new
  polling helper. No suppressions or unrelated refactors were added.
- Evidence: `output/feature-audit/readiness-runtime-units-2026-10-25.json`
  and `output/feature-audit/readiness-browser-2026-10-25.json`; Doctor diagnostics:
  `output/feature-audit/readiness-doctor-2026-10-25.json`.

Tests, fixtures and generated reports remain local/unstaged under the existing
repository policy. The application checkpoint alone does not reproduce this
complete local verification suite. Preserve these local files.

## Remaining launch gates

1. Validate current-season Supabase reads, initial SSR and Shiny parity on the
   exact release Preview. The 36 opt-in live browser executions remain separate
   from offline fixture tests. Verify publication across warm instances.
2. Establish deployment-wide request protection and the total Supabase connection
   budget, including autoscaling. The proxy's in-memory allowance is not enough
   to claim fleet-wide enforcement.
3. Verify connection timeout/death/recovery on an isolated Supabase test target,
   and run a bounded post-warmup memory/listener/connection soak. Deterministic
   tests do not establish real socket or heap stability. A local PostgreSQL setup
   was attempted only for fault injection and stopped because its installation
   lacks bootstrap files; local PostgreSQL is **not an app or launch dependency**.
   The prepared local-only recovery script was not executed. No Supabase service
   or production connection was terminated.
4. Complete the inventory acceptance gate: the reviewed inventory has 109 IDs,
   while the executable gate still requires only seven Game Logs IDs and retains
   a stale GL-06 deferral. Broad browser success is not complete ID-level coverage.
5. In the agreed quiet window after ETL/outside games, execute matched workload
   and Preview capacity/cost checks: 150 users, p95 under 2s, no action over 5s,
   and the $50 monthly budget including Supabase. Include cold/distinct filters
   and the new version checks. No shared-database load test was run here.
6. If enabling Ask, verify real-provider/host disconnect and spend behavior on
   the release Preview; refresh evaluation as required by the Ask handoff.
7. Check exact SHA, hosted bundle, env flags, observability, production URL and
   rollback. Keep Shiny as fallback. Agree how tests/artifacts persist before
   claiming reproducible CI; they currently remain local by policy.

No push, deployment, schema/ETL change or production database mutation is part
of this checkpoint. See the earlier Ask checkpoint for cancellation details:
`docs/ask_lifecycle_handoff_2026-10-25.md`.
