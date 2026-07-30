# Idle-session restore architecture

**Updated:** 2026-07-30  
**Implementation:** native Shiny URL bookmarking  
**State version:** `IBPL_RESTORE_STATE_VERSION <- 11L`

## Purpose

The app closes inactive Shiny sessions so abandoned browser tabs do not keep
server workers and database resources alive. When the user returns, the browser
opens the last native Shiny bookmark, creating a new session whose initial tab
and filters come from the bookmark.

Restoration is not a JavaScript-to-R replay. JavaScript stores and opens a URL;
Shiny owns input serialization and restoration through `enableBookmarking()`,
`session$doBookmark()`, and `restoreInput()`.

## Lifecycle

```text
input state changes
  -> R debounces a filtered input snapshot for 1.5 seconds
  -> session$doBookmark()
  -> Shiny creates a URL bookmark
  -> onBookmarked() sends the URL to the browser
  -> JavaScript stores it under a per-browser-tab key

idle warning
  -> JavaScript shows the countdown
  -> activity keeps the current session alive

idle expiry / disconnect
  -> R closes the old session when APP_IDLE_CLOSE_SESSION is enabled
  -> JavaScript marks the page paused
  -> return activity or foregrounding opens the stored bookmark
  -> Shiny creates a new session with bookmarked inputs as initial state
  -> bookmark parameters are removed from the visible address bar
```

Neither the expiry timer nor a visible disconnect navigates immediately. They
leave the page paused until later user activity. Returning a hidden tab to the
foreground is itself treated as return activity and can begin restoration.

## Source map

| File | Responsibility |
| --- | --- |
| `app/app.R` | Enables URL bookmarking, builds request-time UI, captures changed input snapshots, excludes event inputs, sends bookmark URLs, and releases startup gating for a restored non-Home tab. |
| `app/R/helpers.R` | Defines bookmark exclusions plus `restored_input_value()` and `restore_aware_selection()`. |
| `app/R/mod_lineup_player_filter.R` | Preserves valid restored team/player selections when dependent choices arrive. |
| `app/R/server_tab3.R`, `server_tab4.R`, `server_tab5_traditional.R`, `server_tab7_compare.R` | Preserve restored selections while populating server-side choices. |
| `app/R/mod_team_hub.R` | Keeps hidden Home Storylines suspended during a non-Home restore. |
| `app/www/app.js` | Stores bookmark URLs, owns idle/return behavior, navigates to bookmarks, clears saved state, and exposes debug hooks. |
| `app/www/app.css` | Styles the idle warning, paused pill, restored notice, and disconnect suppression state. |
| `scripts/measure_bookmark_url.R` | Prints the browser-console snippet used to measure a worst-case bookmark URL. |

## Configuration

| Setting | Default | Meaning |
| --- | ---: | --- |
| `APP_IDLE_TIMEOUT_MIN` | unset | Preferred idle timeout in minutes; overrides the seconds setting when valid. |
| `APP_IDLE_TIMEOUT_SEC` | `360` | Idle timeout in seconds. |
| `APP_IDLE_WARNING_SEC` | derived | Warning period, clamped below the idle timeout. |
| `APP_IDLE_CHECK_SEC` | `15` | R-side interval for deciding when to close an idle session. |
| `APP_IDLE_STATE_TTL_HOURS` | `24` | Maximum age of a stored bookmark. |
| `APP_IDLE_CLOSE_SESSION` | `false` | Enables R-side idle session closure. |
| `IBPL_RESTORE_STATE_VERSION` | `11` | Versions browser storage keys and payloads. Increment after incompatible input changes. |

The version constant is passed into `window.IBPL_IDLE_CONFIG`; R and JavaScript
therefore use the same value.

## Bookmark capture

`enableBookmarking(store = "url")` enables Shiny's URL bookmark store.
`app.R` watches `reactiveValuesToList(input)`, removes IDs returned by
`bookmark_excluded_ids()`, sorts the remaining IDs, and debounces the snapshot.
A changed snapshot triggers `session$doBookmark()`.

The exclusion policy removes one-shot actions and bookkeeping rather than
maintaining an allowlist of every persistent filter. It covers:

- navigation and reset actions;
- idle heartbeat and remembered-team inputs;
- app-internal `ibpl_*` events;
- table clicks, selections, searches, and DataTables state.

New ordinary filters are consequently bookmarkable by default. New event-like
inputs must be added to the exclusion rules.

`onBookmarked()` sends the generated URL and state version in the
`ibpl_bookmark_url` custom message. The address bar is unchanged during normal
use.

## Restore-context rule for server-populated choices

Static inputs are restored by Shiny while `ui(request)` is constructed. The tab
UI definitions are functions for this reason: request-time construction lets
their `restoreInput()` calls see the active restore context.

Server-populated Selectize inputs need one extra bridge. Their restored value
may exist before their authoritative choices arrive, and a later
`updateSelectizeInput()` can otherwise erase it. Choice-populating observers use:

```r
restore_aware_selection(session, id, current, choices)
```

The helper prefers the current input, falls back to the native restore value,
sanitizes the candidate, and intersects it with the real choice values. Invalid
or season-incompatible selections are dropped.

`restored_input_value()` reads:

```r
session$restoreContext$input$get(id, force = TRUE)
```

The `force = TRUE` is essential. UI construction may already have consumed the
value through `restoreInput()`, but a server-side choice observer still needs to
read it. This is a narrow compatibility bridge for dynamic choices, not a
general replay system.

## Startup behavior

`bslib::navbarPage()` restores `main_tabs` natively. The server reads that same
value only to coordinate startup work:

- a restored non-Home tab releases the reference-data handoff immediately;
- Home Storylines remains `suspendWhenHidden = TRUE`;
- the hidden Home output does not have to run before the restored tab can load.

No observer loops over widget types, and there is no
`ibpl_restore_state`/`ibpl_restore_applied` handshake.

## Browser storage and return behavior

`app.js` creates a tab ID in `sessionStorage` and uses it in the bookmark key.
The bookmark payload contains:

```json
{
  "url": "?_inputs_&main_tabs=compare&...",
  "savedAt": 1785270000000,
  "v": 11
}
```

`sessionStorage` is primary and `localStorage` is a fallback. A payload is used
only when it parses, has the current version, contains a URL, and is younger
than the configured TTL. A state-version change selects a new storage key.

On return, `window.location.replace(url)` creates the restored session.
Afterward, `history.replaceState()` removes `_inputs_` parameters from the
visible URL without disturbing the already-created restore context.

“Start fresh” clears both stored copies, sets a one-load skip marker, and
reloads the clean location so discarded state is not immediately resurrected.

While a visible page is paused, mouse movement alone does not restore it. This
lets the user reach the explicit “Resume” and “Start fresh” controls. A click,
touch, scroll, or non-Tab key outside the paused pill also restores the
bookmark; events inside the pill remain available to its controls.

## Two-clock model

The server and browser deliberately keep separate clocks:

- R receives throttled `idle_activity_ts` heartbeats and closes the Shiny
  session after the configured timeout.
- JavaScript tracks local activity every second to render the warning and
  paused state promptly.

While the document is hidden, the browser does not show the warning or navigate.
When it becomes visible, it restores only if expiry/disconnection has already
been observed or Shiny is no longer ready.

## Failure handling

| Condition | Behavior |
| --- | --- |
| No bookmark, unavailable storage, stale payload, or corrupt JSON | Reload to a usable default session. |
| State version changed | Old state is ignored because its key/version no longer matches. |
| Saved dynamic choice no longer exists | The choice intersection drops that value. |
| `session$doBookmark()` fails | The error is logged; the previously stored bookmark remains. |
| Session dies while the page is hidden | Restoration waits until the page returns to the foreground. |
| User chooses Start fresh | Stored bookmark is removed before reloading defaults. |

Bookmark URLs are user-controlled input. Dynamic selections are sanitized and
validated against authoritative choices, and existing parameterized SQL and
request guards remain in force.

The plan requires measuring a worst-case URL before merge. That browser
measurement is still pending; no URL-compaction fallback is implemented unless
the observed URL exceeds 6 KB.

## Debug hooks

The browser exposes:

- `window.ibplDebugSavedSession()` — returns the current bookmark URL,
  `idleExpired`, and tab ID.
- `window.ibplRestoreSavedSession()` — forces the return/navigation path.
- `window.ibplClearSavedSession()` — clears stored state and marks the next load
  to skip restoration.

## Manual regression matrix

These checks are release gates, not automated unit-test substitutes.

| Check | Expected result | Current status |
| --- | --- | --- |
| Each analytics tab with distinctive filters | Same tab and filters return; table renders once. | Pending browser runtime |
| Lineup Data and Compare dependent player filters | Team and player lists survive restoration. | Pending browser runtime |
| Season change | Season returns; invalid choices are dropped. | Pending browser runtime |
| Start fresh | Home defaults return and discarded state stays cleared. | Pending browser runtime |
| Two browser tabs | Each tab restores its own bookmark. | Pending browser runtime |
| iOS Safari / Android Chrome background return | Restore occurs on return without a background popup. | Pending device check |
| Corrupt or stale storage | Defaults load and the app remains usable. | Pending browser runtime |
| Worst-case bookmark URL | Observed size is below 6 KB, or fallback work is added. | Pending browser runtime |
| Temporary 45s/15s test settings | `.Renviron` values are restored after testing. | Not changed |

The in-app browser runtime was unavailable on 2026-07-30 with
`failed to write kernel assets ... os error 3`, so the pending rows must be run
before merge.

## Previous design

The replay architecture was removed on 2026-07-29. It scraped/stored individual
DOM values, posted an `ibpl_restore_state` payload, replayed many
`update*Input()` calls in R, retried dependent widgets, and waited for an
`ibpl_restore_applied` handshake.

That design allowed default state to render before restored state and created
two competing writers for widgets. Native bookmark navigation instead makes the
restored state the initial state of a new Shiny session.
