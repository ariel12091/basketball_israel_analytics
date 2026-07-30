# Bookmark restore: root cause and fixes

**Date:** 2026-07-30
**Branch:** `shiny/idle-restore-bookmarking`
**Symptom:** after an idle restore, Lineup Data comes back with Team / Players On /
Players Off empty. Same for every other server-populated dropdown.

## Root cause

`app/www/app.js` stripped the bookmark query string **before Shiny's client told
the server about it**, so the new session was created with an inactive restore
context.

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

### D — latent race in the lineup module

`refresh_player_choices()` consumed `restore_seed$available` when it *queued* the
`updateSelectizeInput()` calls. If the browser echoed `team` in an earlier flush
than `players_on/off`, the re-entrant `observeEvent(input$team)` saw blank player
inputs with the seed already spent and pushed `selected = character(0)`.
Reproduced:

```
--- after team echo only ---
  ld_lineup_filter-players_on   choices=TRUE n=2 selected=
  ld_lineup_filter-players_off  choices=TRUE n=2 selected=
```

## Fixes applied

| File | Change |
| --- | --- |
| `app/www/app.js` | `clearBookmarkParams()` → `scheduleBookmarkParamCleanup()`, which defers the strip to `shiny:sessioninitialized`. |
| `app/R/helpers.R` | New `restore_once_selection()` + `restore_consumed_env()` / `restore_value_consumed()`: applies a bookmarked value once per session (tracked in `session$userData`), then lets later rebuilds clear as before. |
| `app/R/helpers.R` | `update_gn_last_n_choices()` drives its three ids from a map and seeds each through `restore_once_selection()`. |
| `app/R/mod_lineup_player_filter.R` | `restore_seed$pending` holds values pushed but not yet echoed; `selection_with_restore_seed()` falls back to them; the three `observeEvent`s retire their pending value (`ignoreNULL = FALSE`, so a user clear also counts). |
| `app/R/server_tab4.R` | `gl_team` seeded via `restore_once_selection()`; observer `ignoreInit = FALSE`. |
| `app/R/server_tab5_traditional.R` | Observer `ignoreInit = FALSE`. |
| `app/R/server_tab7_compare.R` | `apply_default_players()` applies a restored `cmp_player_a`/`cmp_player_b` pair before falling back to default scorers. |

### Why one-shot semantics

A restored value must be applied on the first rebuild but must not resurrect
after the user clears it. `restore_once_selection()` marks the id consumed in
`session$userData` on first use; the lineup module does the same with
`restore_seed$available` plus the pending-echo window.

## Tests

Added to `app/tests/testthat/test-idle-restore-bookmarking.R`:

- lineup restore survives a split dependent-choice echo (regression for D);
- `restore_once_selection()` applies a bookmarked value exactly once;
- GN / last-N rebuilds seed from the bookmark once, then clear;
- tab observers that own restore bridges run on the initial flush;
- compare default players never overwrite a bookmarked pair;
- bookmark params survive until Shiny has created the session.

`test-idle-restore-bookmarking.R`: 119 passing. Broader tab suites
(`server-tabs-smoke`, `tab-wiring`, `date-reset-contracts`, `tab3-*`, `tab4-*`,
`tab5-*`, `tab7-*`, `team-hub-*`, `filter-chip-date-guards`, `tooltips`,
`dt-security`, `primary-table-render-smoke`) show no new failures; the 5 existing
failures in `tab3-render-regressions` and `primary-table-render-smoke` reproduce
identically with the changes stashed.

## Still unverified

Everything above is R- and source-level. The manual regression matrix in
`docs/idle_session_restore_architecture.md` remains **pending browser runtime** —
in particular:

- a real restore of Lineup Data with Team + Players On/Off;
- the same for Compare (both the lineup module instance and the player pair);
- Tabs 4 and 5 restoring directly into their own tab;
- confirming `shiny:sessioninitialized` fires before the address bar is cleaned,
  and that a reload after that point does **not** re-restore.

Check `window.ibplDebugSavedSession()` after a restore: the stored URL should
still carry the pre-idle values, and bookmark capture stays disarmed until
deliberate user activity.
