# Stint Ribbon — Handoff, 2026-09-05

Paused mid-execution after Task 6. Branch `shiny/stint-ribbon`, unmerged, not deployed.

**Documents**
- Spec: `docs/superpowers/specs/2026-09-05-stint-ribbon-design.md`
- Plan: `docs/superpowers/plans/2026-09-05-stint-ribbon.md` (9 tasks)
- Execution ledger: `.superpowers/sdd/2026-09-05-stint-ribbon/progress.md` (git-ignored)

## What the feature is

Per-game player lanes over the game clock, with the score margin behind them,
opened by clicking a date in either game-log tab. Hovering a lane re-clips the
margin curve to that player's floor time, so the chart reads as a rotation map
at rest and answers the on/off question on demand. Server-built inline SVG, no
plotting library, one database round trip per open.

## Status

| Task | State |
|---|---|
| 0 — test baseline | complete |
| 1 — EuroLeague views + grants | complete, review clean |
| 2 — lane transforms | complete, review clean (1 fix round) |
| 3 — clock frame + margin series | complete, review clean (1 fix round) |
| 4 — SVG builder | complete, focused tests passing |
| 5 — readers | complete, live DB verified |
| 6 — CSS + hover JS | complete, focused tests + JS parse passing |
| 7 — Tab 4 wiring | not started |
| 8 — Tab 11 wiring | not started |

Shipped so far: the pure transforms, accessible inline-SVG builder, both
single-round-trip readers, ribbon CSS and hover/focus/touch interaction, 2
EuroLeague views, and both security enumerations.

**On resume:** read the ledger first, then start at **Task 7 (Tab 4 wiring)**.
Build the ribbon date link from source `df` before `select()` drops `game_id`
and `team_id`, in both Summary and Four Factors modes.

Task 3's scoped re-review arrived just after the pause and closed all three
findings, confirming the strengthened clamp test by mutation (flipping
`fromLast = TRUE` to `FALSE` makes it fail, as it must).

## Work completed after the first pause

| Task | Commit | Verification |
|---|---|---|
| 4 | `30f761b` | focused suite passed; append-only helper diff; `helpers.R` retained 2,670 CR bytes |
| 5 | `def8a3d` | focused suite passed with only DB-gated skips; both live readers verified |
| 6 | `a67680f` | focused suite passed; `node --check app/www/app.js` passed |

Task 5 live measurements (three timed opens per league): Israeli game 115/team
7 returned 350 raw lane rows and 341 margin states at 370.3 ms median;
EuroLeague game 25/team 25 returned 460 raw lane rows and 78 margin states at
293.4 ms median. Both returned `opp/own`, with no health warning.

Two plan defects were corrected during implementation:

- Task 5's proposed R regex literals used invalid single-backslash escapes;
  the committed tests use valid doubled escapes and assert the intended SQL.
- Task 6 placed the `sendShinyEvent` export outside the closure defining it.
  The committed code exports it inside the existing IIFE as
  `window.ibplSendShinyEvent`; the ribbon hover handler remains a separate
  IIFE, preserving queue-and-replay behavior for Task 7.

## Outstanding manual step

`scripts/apply_db_security.R` with `CONFIRM_DB_SECURITY_APPLY=1` has **not**
been run. It re-applies the whole database security surface, so it was
deliberately not delegated to a subagent.

This is not blocking: Task 1's deploy transaction granted `SELECT` on both new
views directly, and both view names are registered in
`sql/security/enable_readonly_rls.sql` and `sql/security/audit_app_access.sql`.
Running it makes those grants survive future `DROP`s, and should be a no-op
confirmation.

## Pre-existing failure, not ours

`test-companion-query-counts.R:293` — "EuroLeague Game Logs keeps …" fails with
`object 'minutes' not found` in a dplyr `transmute`. Present in the baseline
before any edit. It sits in the Tab 11 area that **Task 8** will modify, so do
not read it as a regression there, and do not fix it inside this plan.

Four test files skip on env gates as designed (`RUN_DB_TESTS`,
`RUN_DEPLOYED_SMOKE`, `RUN_E2E`).

## Traps this work has already paid for

**`helpers.R` has mixed line endings** — 2,670 CRLF lines and ~380 LF-only.
Both the **Edit tool** and **`sed -i`** silently normalise the whole file. Task 2
committed 384 rewritten lines that way (638 insertions / 384 deletions for what
should have been +90) and had to redo the commit; Task 3's mutation check
stripped every CR byte in the working tree before catching it.

It is not a function of edit size: Task 3's re-reviewer used the Edit tool to
change **one token** (`fromLast = TRUE` → `FALSE`) and the CR count jumped
2,670 → 3,194 with a 524-line diffstat. For a single-token change on this file,
a binary-mode exact-byte replace produced a true one-line diff. The route that
works: write the new block to a temp file outside the repo and `cat tmpfile >>`
it on. Verify before every commit:

```bash
git diff --stat                                    # insertions only, no deletions
git show HEAD:app/R/helpers.R | tr -cd '\r' | wc -c   # must stay 2670
```

**Never key a lane on a player name.** Both leagues carry same-name/different-id
players on one team (Israeli data has such a pair inside a single game+team, and
team 4 carries `NEW NEW` across three ids). A name key does not lose a lane — it
merges two people's floor time into one, which looks plausible.

**`own_lineup` and `player_ids` are sorted independently** in
`euroleague.lineup_totals_by_game` — names alphabetically, ids ascending. Pairing
them positionally is wrong in 31,907 of 40,000 pairs. Take the id array only and
fetch labels by id.

**Never filter `type_lineup = 'offense'`** on the Israeli MV: it loses 1.46% of
floor time. No `type_lineup` predicate is needed at all.

**Axis extent comes from the nominal period structure**, never `max(elapsed)` —
Israeli games end ragged, one at 961 seconds.

## Measurements worth not re-taking

- Pooler round-trip floor: **238 ms** (`SELECT 1`, n=10). Server-side execution
  for the ribbon queries is 0.5-21.6 ms. The feature is transport-bound, so the
  500 ms budget permits exactly **one** query per open — two would spend it on
  latency alone.
- One-query form measured at 268-272 ms (Israeli) and ~245 ms (EuroLeague).
- Segment timelines are shared across both teams: 10,219 of 10,241 Israeli 2026
  segments and all 38,904 EuroLeague ones carry both teams with identical
  boundaries. The mirrored layout needs no self-join.
- Starters derived from the first segment give exactly 5 per team-game across
  1,178 EuroLeague and 878 Israeli team-games — cleaner than the EuroLeague
  boxscore flag, which carries 40 stray ones.
- `stints` is ruled out permanently: 0 rows live (cold storage), and
  pre-canonical in shape. See spec §3.3.

## How the review loop has been earning its cost

Three defects reached a reviewer and none reached the branch:

1. Task 1's brief contained two regexes that could never match (missing DOTALL,
   a lazy quantifier stopping at the first tuple) — the test would have passed
   vacuously.
2. Task 2's commit rewrote 384 lines of line endings.
3. Task 3's reviewer used **mutation testing** — flipping `>` to `>=` and
   re-running — and found that a boundary test I had asked for could not detect
   the mutation it claimed to guard, and that a clamp test of mine could not
   fail on a broken tie-collapse at all.

Worth keeping: ask reviewers to probe and mutate rather than read, and require
implementers to *see* a new test fail before claiming it tests anything.
