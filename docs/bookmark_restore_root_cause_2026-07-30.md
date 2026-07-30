# Bookmark restore: root cause and fixes

**Date:** 2026-07-30
**Branch:** `shiny/idle-restore-bookmarking`
**Symptom:** after an idle restore, Lineup Data comes back with Team / Players On /
Players Off empty. Same for every other server-populated dropdown.

## Root causes

Two independent bugs, both in `app/www/app.js`, both producing an **inactive
server-side restore context**. Fixing only the first still leaves restore dead,
which is exactly what happened on the first attempt.

Confirmed in a real browser against a local instance. The decisive evidence was
a per-session log line:

```
restore context active=FALSE values=0 tab=- lineup_team= url_search_len=186 has_inputs=TRUE
```

The query string *reached* the server (`url_search_len=186`, `has_inputs=TRUE`)
yet the context was inactive — which rules out cause 1 and points straight at
cause 2. After both fixes:

```
restore context active=TRUE values=5 tab=lineup_data lineup_team=4
```

## Cause 1 — the query string was erased before Shiny read it

`app.js` stripped the bookmark query string **before Shiny's client told the
server about it**.

### The chain

1. Shiny builds the **server-side** restore context from `.clientdata_url_search`,
   not from the HTTP request. `shiny:::createAppHandlers` (shiny 1.9.1):

   ```r
   if (is.null(shinysession$restoreContext)) {
     bookmarkStore <- getShinyOption("bookmarkStore", default = "disable")
     if (bookmarkStore == "disable") shinysession$restoreContext <- RestoreContext$new()
     else {
       shinysession$restoreContext <- RestoreContext$new(msg$data$.clientdata_url_search)
       shinysession$createBookmarkObservers()
     }
   }
   ```

   and `shiny.min.js`: `url_search"] = window.location.search`, read when the
   client sends its `init` message (after DOM ready).

2. `app.js` erased `location.search` at **script-parse time**, long before that:

   ```js
   clearBookmarkParams();   // top-level, ran on load
   ```

3. Therefore `.clientdata_url_search` was `""` → `RestoreContext$new("")` →
   **inactive**, and every server-side read failed closed:

   ```r
   restored_input_value <- function(session, id, default = character(0)) {
     ctx <- tryCatch(session$restoreContext, error = function(e) NULL)
     if (is.null(ctx) || !isTRUE(ctx$active)) return(default)   # always taken
   ```

   `restore_seed` in `mod_lineup_player_filter.R` was empty, `restore_aware_selection()`
   had nothing to fall back to.

## Cause 2 — an input was sent before Shiny's `init` message (dominant)

Shiny builds the restore context from the **first websocket message it receives**
and never rebuilds it:

```r
if (is.null(shinysession$restoreContext)) {
  ...
  shinysession$restoreContext <- RestoreContext$new(msg$data$.clientdata_url_search)
  shinysession$createBookmarkObservers()
}
```

`.clientdata_url_search` only exists on the `init` message. And in `shiny.min.js`,
`shiny:connected` fires **inside `socket.onopen`, before `init` is sent**:

```js
$(document).trigger({type:"shiny:connected", socket:i}), t.onConnected(),
i.send(JSON.stringify({method:"init", data: t.$initialInput}))
```

Our `handleConnected()` ran in that window and called

```js
window.Shiny.setInputValue("idle_activity_ts", now, { priority: "event" });
```

`InputBatchSender` flushes `priority: "event"` **synchronously**, so the server's
first message was that `update` — no `.clientdata_url_search` →
`RestoreContext$new(NULL)` → inactive for the life of the session, no matter how
intact the URL was.

**Rule:** nothing may emit a Shiny input before `shiny:sessioninitialized`. That
event is triggered by the server's `config` message, so it is guaranteed to be
after `init` was received and the restore context built. `sendActivity()` now
carries a hard `sessionReady` guard, with a 10-second fallback so a missed event
cannot leave the idle heartbeat permanently silent (which would make R close
sessions the user is actively using).

### Why it looked selective

UI-time `restoreInput()` runs during the **HTTP GET**, which still carried the
query string. So static inputs restored fine and only server-populated choices
failed. The reported bookmark URL is a *re-capture* of the broken restored
session and shows exactly that fingerprint:

- restored: `main_tabs="lineup_data"`, `game_year="2026"`, `ld_clutch_status="all"`,
  `tr_trad_display_mode="Per Game"`
- empty: `teams=null`, `on_opponents=null`, `ld_opponents=null`,
  `ld_lineup_filter-team=""`, `players_on/off=null`, `ts_players=null`,
  `cmp_player_a=""`, `gl_team=""`

Home's `home_team="11"` survived because the hub re-seeds it from
`input$hub_remembered_team` (localStorage), not from the bookmark.

### Evidence the R logic was already correct

Driving the real `server_tab2` + module through `testServer` with a live
`RestoreContext` restored everything:

```
--- after startup flush (tab2 observer) ---
  ld_lineup_filter-team          choices=TRUE n=6 selected=1
  ld_lineup_filter-players_on    choices=TRUE n=2 selected=11
  ld_lineup_filter-players_off   choices=TRUE n=2 selected=12
```

So `f883643` fixed a real ordering problem; it just could never fire in a browser.

## Impact map

### A — restored correctly all along

Anything with static choices, restored by `restoreInput()` inside `ui(request)`:
`main_tabs`, `game_year`, dateRangeInputs, sliders, radios, checkboxes, and static
selects (`*_home_away`, `*_outcome`, `*_clutch_status`, `*_game_type`,
`*_num_starters_*_mode`, `*_opp_rank_*`, display modes).

### B — broken by the JS strip (had a bridge, could not use it)

| Where | Inputs |
| --- | --- |
| `app.R` | `teams`, `on_opponents`, `ld_opponents` |
| Tab 2 | `ld_lineup_filter-team` / `-players_on` / `-players_off` |
| Tab 3 | `tr_opponents` |
| Tab 4 | `gl_opponents` |
| Tab 5 | `ts_teams`, `ts_players`, `ts_opponents` |
| Tab 7 | `cmp_lu_filter-*`, `cmp_a/b_teams`, `cmp_a/b_opponents`, `cmp_player_a/b_list_team_filter`, `cmp_*_gn_*` |
| `app.R:159` | `restored_tab` → `startup_restore_pending` gating never fired |

### C — broken independently of the JS bug

These would still have failed after fixing the query string:

- `update_gn_last_n_choices()` hard-wrote `selected = ""` for
  `{on,ld,tr,gl,ts}_{gn_min,gn_max,last_n}` — 15 inputs, no bridge at all.
- `server_tab4.R` `gl_team`: `update_single_team_selectize(..., selected = "")`.
- `server_tab7_compare.R` `apply_default_players()`: seeded random top scorers
  because the restored inputs read blank at that moment.
- Tabs 4 and 5 used `ignoreInit = TRUE` on their `list(main_tabs, game_year)`
  observer, so on a restored session landing on those tabs the restore bridges
  never ran at all.

### D — Compare seeded the lineup module through `reset_inputs()`

Tab 7 populated `cmp_lu_filter` with

```r
cmp_lu_filter$reset_inputs(team_choices = ..., team_selected = "")
```

whose first statement is `restore_seed$available <- FALSE`. The seed was
destroyed before anything could read it, so the Compare lineup filter could
never restore. Tab 2 was fixed in `f883643`; Tab 7 uses a different call site and
was missed.

### E — Compare's player pair had no bridge, and the obvious place is dead code

`cmp_player_a` / `cmp_player_b` are written only by `refresh_player_choices(side)`,
which reads `input$cmp_player_a` — blank while a restored session is still
populating those choices.

`apply_default_players()` looks like the natural place to handle this, and a fix
was first written there. It is unreachable: `apply_defaults = TRUE` is never
passed at any of the three `ensure_cmp_player_refs_loaded()` call sites. **Side
finding:** the compare "default top scorers" feature therefore never runs on any
path.

### Rejected — the split-echo race

An earlier round added `restore_seed$pending` to the lineup module, guarding
against the browser echoing `team` in an earlier flush than `players_on/off`
(which would let the re-entrant `observeEvent(input$team)` clear what was just
restored). That was only ever reproduced in a `testServer` mock. In a real
browser the echo arrives batched and the module restores correctly without it,
so the guard and its `ignoreNULL = FALSE` observer changes were reverted. Do not
re-add without first observing the race.

## Fixes applied

| File | Change |
| --- | --- |
| `app/www/app.js` | `clearBookmarkParams()` → `scheduleBookmarkParamCleanup()`, which defers the strip to `shiny:sessioninitialized`. (cause 1) |
| `app/www/app.js` | `handleConnected()` no longer sends any input; `sendActivity()` and the `hub_remembered_team` set moved to `handleSessionInitialized()`, behind a `sessionReady` guard with a 10s fallback. (cause 2) |
| `app/app.R` | `IBPL_RESTORE_STATE_VERSION` 12 → 13, discarding bookmarks captured while restore was broken; per-session `[bookmark]` diagnostic line reporting `active` / `values` / `url_search_len` / `has_inputs`. |
| `app/R/helpers.R` | New `restore_once_selection()` + `restore_consumed_env()` / `restore_value_consumed()`: applies a bookmarked value once per session (tracked in `session$userData`), then lets later rebuilds clear as before. |
| `app/R/helpers.R` | `update_gn_last_n_choices()` drives its three ids from a map and seeds each through `restore_once_selection()`. |
| `app/R/server_tab4.R` | `gl_team` seeded via `restore_once_selection()`; observer `ignoreInit = FALSE`. (C) |
| `app/R/server_tab5_traditional.R` | Observer `ignoreInit = FALSE`. (C) |
| `app/R/server_tab7_compare.R` | Lineup module seeded with `update_team_choices()` + `refresh_player_choices(team_value = …)` instead of `reset_inputs()`. (D) |
| `app/R/server_tab7_compare.R` | `refresh_player_choices(side)` resolves its selection through `restore_once_selection()`. (E) |

`app/R/mod_lineup_player_filter.R` is unchanged from `f883643`.

### Why one-shot semantics

A restored value must be applied on the first rebuild but must not resurrect
after the user clears it. `restore_once_selection()` marks the id consumed in
`session$userData` on first use; the lineup module does the same with
`restore_seed$available`.

## Tests

Added to `app/tests/testthat/test-idle-restore-bookmarking.R`:

- `restore_once_selection()` applies a bookmarked value exactly once;
- GN / last-N rebuilds seed from the bookmark once, then clear;
- tab observers that own restore bridges run on the initial flush;
- compare restores its player pair and lineup filter (and is not seeded through
  `reset_inputs()`);
- no input is sent before shiny's init message;
- bookmark params survive until Shiny has created the session.

`test-idle-restore-bookmarking.R`: 119 passing. Broader tab suites
(`server-tabs-smoke`, `tab-wiring`, `date-reset-contracts`, `tab3-*`, `tab4-*`,
`tab5-*`, `tab7-*`, `team-hub-*`, `filter-chip-date-guards`, `tooltips`,
`dt-security`, `primary-table-render-smoke`) show no new failures; the 5 existing
failures in `tab3-render-regressions` and `primary-table-render-smoke` reproduce
identically with the changes stashed.

## Verified in a browser (2026-07-30)

Local instance, Playwright-driven, full user flow — load clean, pick Team +
Players On in Lineup Data, `ibplRestoreSavedSession()`, inspect:

| Check | Result |
| --- | --- |
| Capture | `ld_lineup_filter-team="4"`, `players_on=["1043","1044"]`, `players_off="1045"` present in the stored URL |
| Restore context | `active=TRUE values=5 tab=lineup_data lineup_team=4` |
| Restored UI | Team `HAPOEL JERUSALEM`, On `JARED HARPER` + `KHADEEN CARRINGTON`, Off `AUSTIN WILEY` |
| Second run | Team `HAPOEL HOLON`, On `JORDAN BONE` + `LIAD SHMUEL` |
| Correct tab | `lineup_data` |
| Address bar | cleaned after restore |
| Post-restore user clear | clearing Players On refreshed the table to 50 rows — the seed does not resurrect |

Note: a restored Team+2-players filter can legitimately show "No data available"
if that pairing has no qualifying lineups; clearing the players brought the table
back, so the empty result was the filter, not the restore.

Per-tab restore, driven by navigating straight to a bookmark URL:

| Tab | Inputs checked | Result |
| --- | --- | --- |
| 2 Lineup Data | team, players on/off, `ld_gn_min` | pass |
| 4 Game Logs | `gl_team`, `gl_opponents`, `gl_gn_min` | pass |
| 5 Player Stats | `ts_teams`, `ts_players`, `ts_gn_min` | pass |
| 7 Compare (Lineups) | `cmp_lu_filter-team` / `-players_on` / `-players_off` | pass after fix D |
| 7 Compare (Players) | `cmp_player_a`, `cmp_player_b` | pass after fix E |

Tab 5's `ts_players` choices are keyed `"<team_id>:<player_id>"` (e.g. `4:1043`),
not the bare player id — a synthetic test URL using bare ids will look like a
restore failure when it is not.

## Known intermittent issue

One Tab 5 restore on a cold instance aborted its startup observer with

```
ERROR: bind message supplies 1 parameters, but prepared statement "" requires 0
Warning: Closing open result set, cancelling previous query
```

the Supabase transaction-pooler unnamed-prepared-statement collision. It did not
reproduce on a warm instance, and the same observer runs cleanly on the ordinary
tab-click path. `ignoreInit = FALSE` makes these observers run during startup,
concurrent with the prewarm, which raises the chance of the collision — and every
shinyapps.io worker starts cold. Tabs 2/3/7 already ran with `ignoreInit = FALSE`,
so this is not new, but the exposure is wider. Not addressed here: it is a
connection-pooling problem, not a bookmark one.

## Still unverified

- Real idle expiry. Every run above forced the path via
  `ibplRestoreSavedSession()` or by navigating to a bookmark URL; the genuine
  timeout → paused pill → resume flow was not exercised.
- The mobile / background-return path.
- Tabs 1 and 3 restoring directly into their own tab.
- Deployed shinyapps.io behaviour — none of this is live yet.
