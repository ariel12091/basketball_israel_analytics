# Filter navigation UX — review fixes and Task 6 handoff

**Date:** 2026-09-03  
**Branch:** `shiny/filter-nav-ux`  
**Worktree:** `.worktrees/design-system-pass`

## Branch scope

This document was first written to cover the last two commits. The branch
holds all six tasks of `docs/superpowers/plans/2026-09-02-filter-nav-ux.md`,
newest first:

| Commit | |
|---|---|
| `d2b84fc` | this handoff |
| `d226eb7` | Task 6 reversed — cards restored, slimmer |
| `d1e72e5` | plan checkboxes and this handoff |
| `217494d` | Task 6 — Home nav rail (superseded by `d226eb7`) |
| `5857f37` | review findings (below) |
| `67e572f` | pivot routes to its own league's tabs |
| `ffa9aa7` | pivot filters by the player, not just their team |
| `c725dfc` | Task 5 — pivot from any row |
| `e35e38c` | Task 4 — one generalised cross-tab handoff value |
| `2efe7e2` | Task 3 — chips reveal the control they describe |
| `86bbe90` | Task 2 — collapsible filter panel |
| `b65d02e` | Task 1 — chips through `filter_chips_row()` |

**Both 2026-09-02 plans are now complete.** The design-system pass (10 tasks)
was merged to `main` as a fast-forward and `main` sits at `01993b5`. This
branch is ten commits ahead of that.

**Nothing is pushed.** `origin/main` is behind `main` by the whole design-system
pass, and this branch has no remote. Decide push/merge before treating any of
this as delivered.

## Completed commits

### `5857f37` — review findings

- Limit filter-sidebar collapsing to panes that contain their own toggle, so
  Compare cannot be left collapsed without a way to reopen it.
- Route filter-chip focus to the real Game Logs, namespaced lineup, and
  opponent-rank input ids.
- Make table pivot cells keyboard operable with Enter/Space, expose button/menu
  semantics, and restore focus when Escape closes the menu.
- Raise secondary heat-cell text to an AA-safe colour.
- Add focused regression coverage for each finding.

Validation completed before the commit:

- Focused filter-collapse, chip-focus, pivot-menu, design-token, and range-cell
  tests passed.
- `scripts/test_all.R` passed with four expected environment/integration skips.
- Modified R files parsed successfully, `node --check app/www/app.js` passed,
  and `git diff --check` was clean.

The full-suite run used the valid Windows locale
`English_United States.utf8`; the ambient `C.UTF-8` value is not valid in this
Windows R installation and was the cause of earlier false Compare failures.

Note for whoever picks this up: these findings were gaps in Task 3 as
originally implemented. Its tests asserted that the `input_ids` mechanism
existed, never that a chip's resolved id named a control that exists, so
several chips focused nothing. Assert the resolved target, not the mechanism.

### `217494d` — Task 6 Home navigation rail (superseded)

- Replace the six Israeli Home question cards with a compact navigation rail
  below the existing team hub.
- Preserve the original `go_*` input ids and `js-shiny-event` behavior, so the
  existing observers and pre-websocket queue/replay path remain unchanged.
- Keep the five EuroLeague Home cards intact because EuroLeague has no team hub
  above them.
- Add responsive, visible-focus, and reduced-motion styling.
- Add Home UI tests for the rail, destination ids, and retained EuroLeague
  cards.

Validation completed:

- `test-team-hub-ui.R` passed all 64 assertions after the Task 6 changes.
- Browser check of the six Israeli rail items, accessible nav label, absence of
  Israeli question cards, and five retained EuroLeague cards.

### `d226eb7` — the rail is reverted; the cards come back slimmer

The rail was rejected on review. Two reasons, both about what the card does
that a destination label cannot:

- **It works before the hub does.** The card is readable and clickable from
  the served HTML alone. The hub above it needs a database round trip before
  it says anything, and a click landing in that window is queued and replayed
  — so during the slowest part of a cold load the cards are the only part of
  Home that answers a question or accepts an intent.
- **The question explains the tab.** "Who is helping my team?" tells a
  first-time visitor what On/Off Impact is for; "On/Off Impact" does not.

So the reasoning behind the rail — the hub already answers these questions —
was right about the *content* and wrong about what the cards were for. What
was actually wrong was their weight, and that is what changed instead: the
icon moved inline beside the question rather than sitting as a 2rem block
above it, the "Go →" row went (the whole card is the affordance), and padding
and type tightened. Six cards occupy 263px where they needed roughly twice
that.

Both leagues now render from one `home_nav_cards()` builder taking a list of
`input_id` / `icon` / `title` / `sub`. The EuroLeague block had been a
hand-copied near-duplicate of the Israeli one; it is now five list entries.

Verified in a browser against live data at 1280px and 390px: eleven cards, six
visible under the Israeli league and five under EuroLeague; tokens resolving
(`#EEECE8` question, `#98938B` answer, amber icon); a click on the Israeli
Compare card and on the EuroLeague Team card each reaching its pane; one card
per row under 576px with the answer un-indented there. The horizontal overflow
that appears at 390px is `#main_tabs`, not the cards, and predates this branch.

Not re-raced: a click during the pre-connect window. That path is shared and
untouched — the same `.js-shiny-event` branch and the same `sendShinyEvent()`
queue the rail used and the cards used before it — but it was verified live
for the rail, not again for the cards.

## Verification status

`scripts/test_all.R` was re-run at the branch tip with a clean worktree and
passed, with the four expected skips (`RUN_DB_TESTS`, `RUN_DEPLOYED_SMOKE`,
and `shinytest2` not installed). The earlier "remaining verification" item is
closed.

### What was verified against the live app

Browser runs in this environment **did** reach Supabase and were checked
against real data, so behavioural verification is possible here — an earlier
draft of this document said otherwise, which was true of one sandbox but not of
the worktree runs:

- **Pivot, Israeli:** ZACK BRYANT → "Lineups with this player" lands on Lineup
  Data with BNEI HERZLIYA and ZACK BRYANT in Players On, 50 lineups all
  containing him. "Lineups for this team" clears the player.
- **Pivot, EuroLeague:** FC Barcelona → game log lands on `euro_game_logs` with
  the team selected; TORNIKE SHENGELIA → lineups lands on `euro_lineups` with
  FC Barcelona and SHENGELIA, TORNIKE.
- **Collapse:** main column 1171px → 1561px, all ten sidebar tabs tag and
  collapse, state survives a reload, lazily-rendered tabs tag on first show.
- **Chip focus:** from a collapsed panel, clicking a chip opens the panel and
  focuses `tr_outcome-selectized`; the `x` still only clears.
- **Idle restore:** bookmark-URL restore works for both leagues through the two
  helpers this branch modified, the collapse state and column tagging survive
  it, and a pivot correctly beats a stale restored value. Not tested: a real
  ten-minute idle timeout and reconnect, as opposed to a bookmark-URL load.

EuroCup needs no further work — it runs through the same EuroLeague tabs,
selected by the competition dropdown inside them.

## Findings worth keeping

Three traps here cost real time and none of them fail an R test.

- **`formatStyle()` owns `options$rowCallback`.** DT implements `formatStyle`
  by writing that option itself, so a hand-written `rowCallback` on a table
  that also uses `formatStyle` is overwritten; htmlwidgets then fails to
  evaluate the result (`SyntaxError: Unexpected token 'var'`) and the table
  stops rendering **entirely** while every R test stays green. Use
  `createdRow`, which has the same per-row timing and DT does not touch.
- **`update_restore_aware_selectize()` sends its choices server-side.** A
  follow-up `updateSelectizeInput(selected = )` arrives before the choices land
  and is dropped, so a selection must travel *with* the choices. That is why
  the helper now takes an optional `selected`.
- **A pivot row carries both ids.** The menu must send only the id its action
  is about, and the destination must distinguish `NULL` (no pivot: preserve the
  current selection) from `character(0)` (a team pivot: clear a player left by
  an earlier one). Sending both made "Lineups for this team" behave like the
  player action.

More generally: on this branch every defect that reached the browser was
invisible to the R suite, which reads source as text. Verify UI behaviour by
reading computed styles and DOM state in a running app, not by asserting that a
string appears in a file.

## Environment hazards

- **Intermittent DNS.** Several app processes died on
  `could not translate host name "aws-1-eu-north-1.pooler.supabase.com" to
  address: Unknown host`. Resolution is healthy between failures, so this is
  transient, not a standing fault. It kills the whole R process rather than one
  session, so the app simply vanishes. Check DNS before suspecting Supabase or
  the code.
- **Use one port.** Run the review instance on `7666` and restart it in place.
  Cycling ports during a review session led to a bug being reported against a
  build that had already been fixed on another port.
- **`IBPL_CACHE_UI=false`** means `www/app.css` and `www/app.js` edits need only
  a browser reload; only R changes need the process restarted.
- The app reads `.Renviron` from the directory R starts in, so run it from
  `app/`, or keep a copy at the worktree root. `runApp('app')` from the
  worktree root starts R in the root and silently falls back to
  `localhost:6543` with no data.
