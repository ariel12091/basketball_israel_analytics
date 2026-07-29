# Idle-Session Restore via Native Bookmarking — Design

**Date:** 2026-07-29
**Status:** Approved (replaces the replay architecture in `docs/idle_session_restore_architecture.md`)
**Branch:** `shiny/idle-restore-bookmarking`

## Problem

The current idle restore replays state into a running default session: reload →
new session boots with default inputs → JS polls for readiness → sends a payload
→ R fires dozens of `update*Input()` calls → dependent players restored via
extra round trips → JS defensively re-applies the same values with retries.

Consequences observed in production (all four confirmed by the user):

1. **Slow** — the restored tab runs its heavy query twice (defaults first, then
   restored values), behind polling delays and grace windows.
2. **Wrong/missing values** — two writers (R updates + JS re-apply) race each
   other and the choice-populating observers.
3. **Overlay glitches** — correctness depends on ~10 timing flags
   (`restore_intent`, `reconnecting`, `restore_complete`, suppression TTLs…).
4. **Mobile issues** — background/foreground heuristics interact with all of
   the above.

The root cause is structural: state arrives *after* the session starts, so a
replay phase — and everything coordinating it — is unavoidable in that design.

## Decision

Make saved state the session's **starting point** instead of replaying it.
Use Shiny's native bookmarking (`enableBookmarking("url")`): the server
serializes all inputs into a bookmark URL; the browser stores that URL; the
restore is a single navigation to it. `restoreInput()` inside every input
constructor — and `bslib::navbarPage_` for the selected tab — seeds every
widget at UI build time. There is no replay phase, no restore payload, no
handshake, no readiness polling, and no double query.

Validated against installed versions (shiny 1.9.1, bslib 0.9.0):

- All persisted input types use `restoreInput`: `selectInput`/`selectizeInput`,
  `radioButtons`, `sliderInput`, `checkboxInput`, `numericInput`, `dateInput`,
  `dateRangeInput`.
- `bslib::navbarPage_` restores the selected tab via `restoreInput` — this
  subsumes the `ibpl_restore_tab` query-param mechanism, which is deleted.
- `updateSelectizeInput()` accepts `selected` alongside `choices`/`server`.

## Lifecycle

```
alive session:  input change ──(debounce ~1.5s)──▶ session$doBookmark()
                                                     └▶ onBookmarked(url) ─▶ JS stores URL
idle timeout:   R closes session (unchanged clocks/env vars).
                JS shows non-blocking "Session paused — resuming on activity" pill.
user returns:   first activity or tab-focus ─▶ location.replace(storedBookmarkUrl)
new session:    Shiny seeds ALL inputs (tab included) from the URL at build time
                ─▶ JS strips bookmark params via history.replaceState ─▶ done
```

Auto-restore fires only on user **return**, never at expiry itself — a reload
at expiry would spawn a new session that idles out again (session-churn loop).

## R-side changes (`app/app.R`, modules)

1. **`enableBookmarking("url")`.** `ui` is already `function(request)`.
2. **Capture observer.** One debounced observer over persisted-input changes
   calls `session$doBookmark()`; `onBookmarked(url)` sends the URL to JS via a
   custom message. We do NOT call `updateQueryString()` — the address bar never
   shows the bookmark; the URL exists only in storage and during the one-shot
   restore navigation.
3. **Exclusion list replaces inclusion lists.** `setBookmarkExclude()` covers
   noise: `go_*` card buttons, `open_glossary`, `ld_lineup_click`,
   `cmp_table_row_click`, `idle_activity_ts`, `hub_remembered_team`, DT-generated
   inputs (enumerated by measurement during implementation). New filter inputs
   are bookmarked automatically — this retires the "manual persistence
   contract" (today's JS `persistIds` plus the seven parallel R ID lists in
   `restore_state_values()`, all deleted).
4. **Choice-populating observers become selection-preserving.** The startup
   observer (`app.R:396`) currently resets `teams`/`on_opponents`/
   `ld_opponents` to `selected = character(0)` — under seeding this would
   clobber restored values. It and module equivalents must pass
   `selected = <current input value>` when populating choices at startup.
   Season *changes* still reset selections; the reset keys off "season actually
   changed", not "observer ran".
5. **Dependent LD/Compare player filters.** An `onRestore` callback reads the
   saved team/players from the restore context into the pending reactive
   (replacing the JS-driven `pending_ld_lineup_restore` handoff). Ordering is
   unchanged: team → server populates player choices → players applied.
   Saved selections are intersected with real choices — no retries; worst case
   one filter comes back empty.
6. **Validation.** Restored values arrive through Shiny's normal input
   pipeline and are untrusted like any user input. The sanitizers
   (`sanitize_persisted_choices` etc.) move to the consuming observers — the
   correct security boundary; parameterized SQL remains the DB-layer guard.
   The per-tab allowlist is dropped: seeding hidden tabs is free (outputs
   suspended) and every tab now keeps its filters — a UX improvement.
7. **Startup gating.** `startup_restore_pending` keys off "restore context
   present and target tab ≠ home" instead of the query param. Home Storylines
   stays `suspendWhenHidden = TRUE`; prewarm handoff unchanged.
   `ibpl_restore_tab_from_query()` and its call sites are deleted.
8. **Schema guard.** We append `ibpl_v=<state schema>` to the stored URL; a
   mismatched or absent version at load → treat as no restore (fail open to
   defaults). Bump on incompatible input renames, as today.

## JS-side changes (`app/www/app.js`)

Kept (simplified):

- Activity tracking + throttled `idle_activity_ts` heartbeat (unchanged).
- Idle countdown warning overlay. On expiry: **no blocking dialog** — page dims
  with a small "Session paused — resuming on activity" pill including a
  "Start fresh" link.
- **URL store:** custom-message handler writes the latest bookmark URL to
  tab-scoped `sessionStorage` with a `localStorage` fallback (same tab-ID
  keying, TTL, and versioning scheme as today).
- **Return trigger:** after expiry/disconnect, the first
  `visibilitychange→visible` or activity event calls `location.replace(url)`.
  `skip_restore` marker still implements "Start fresh".
- Mobile: hidden → nothing to do (server already holds the latest bookmark);
  socket died while hidden → same return trigger on foreground. The 5-second
  resume-grace heuristics collapse to "if disconnected when visible again,
  navigate".
- Strip bookmark params after load (generalization of
  `clearRestoreTabQueryParam`).
- Native-disconnect-UI suppression shrinks to: patch the notifier once; hide
  Shiny's overlay while we own the expired state.
- Post-restore notice ("Restored your last tab and filters — Start fresh")
  driven by "this session started from a bookmark URL", not a handshake.

Deleted outright: `persistIds`, `readState`/`readInputValue` DOM scraping,
`sendRestoreState`, `attemptRestoreSend` polling, `applyRestoreValues` /
`reapplyDependentPlayerInputs` double-apply, `requestRestoreFinish` + 8s
fallback, `ibpl_restore_applied` handler, and the grace-window flags
(`reconnecting`, `restore_complete`, `suppressDisconnectUntil`,
`restoreCompletedRecently`, `backgroundResumeGraceUntil` in its current form).

## Failure handling

| Case | Behavior |
| --- | --- |
| No stored URL / storage blocked / TTL-stale / version mismatch | Plain reload to defaults — fail open to a working app. |
| Saved player/team no longer in roster | Selection intersected with real choices; that filter is empty, everything else restores. |
| Oversized URL | Measure with excludes in place; hard cap ~6 KB with fallback that drops hidden-tab params (active tab first-class). nginx/shinyapps.io limits comfortably exceed this. |
| Disconnect mid-reload | No dual ownership possible — the old page is gone; the new page loads or the browser errors. |
| Hard kill within debounce window (~1.5s) | Last debounced bookmark is restored; sub-2s of changes may be lost (accepted). |

## Testing

- Extend `app/tests/testthat/test-idle-restore-startup.R`:
  - exclusion-list contract (no `go_*`/click/heartbeat inputs bookmarked);
  - selection-preserving choice observers (seeded value survives startup
    population; season change still resets);
  - `onRestore` pending-players path for LD and Compare;
  - Home Storylines gating under a non-home bookmark.
- Manual regression rows retained from the old matrix: every tab, season
  change, LD/Compare dependent players, "Start fresh", two browser tabs with
  different states, iOS/Android background-return (socket survives / dies /
  close arrives after foregrounding), stale/corrupt/oversized URL.
- `docs/idle_session_restore_architecture.md` is rewritten to describe this
  model; the replay design is kept only as a historical note.

## Rollout

Single branch `shiny/idle-restore-bookmarking`, merged to `main` after the
regression matrix passes. Old sessionStorage payload state is ignored via key
versioning — no migration. `APP_IDLE_*` environment variables and the two-clock
model are unchanged.

## Accepted trade-offs

- Dependency on Shiny's bookmark URL format — stable since 2016 and only ever
  generated server-side via `doBookmark()`, never hand-constructed.
- The exclusion list inverts the maintenance failure mode: a forgotten exclude
  shows up visibly (URL bloat / replayed click), unlike today's forgotten
  include which silently fails to restore.
- Sub-debounce state loss on hard kills (see table above).
