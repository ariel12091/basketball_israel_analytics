# Stint ribbon — richer stint data: brainstorm inputs

**This is NOT a spec.** It is the state of an unfinished brainstorm, written
down because the session was cleared partway. Nothing here is approved and no
code has been written for it. The design questions below are still open.

Feature as it stands today: `shiny/stint-ribbon`, complete and ready to merge
(see `docs/stint_ribbon_handoff_2026-09-05.md`). This is about what to add next.

## What the user asked for

"Add some more stint specific data to the chart." Offered four options, and
picked **all four**:

1. **Per-stint +/-** — margin swing across each stint (margin at its end minus
   at its start).
2. **Per-player game totals** — a gutter column of minutes and total +/-.
3. **Points for / against on floor** — actual points scored and allowed per
   stint, not just the net swing.
4. **Stint context markers** — who came on/off together, score at each
   substitution.

## The finding that shapes the design

`fetch_stint_ribbon()` (`app/R/global.R:359`) already returns, per open:

- `lanes` — `team_id, start_elapsed, end_elapsed, player_id, player_label`
- `margin` — the complete step series `(elapsed, margin, order_key)` in the
  own-team perspective, completed to the full clock by
  `ribbon_complete_margin()`

So **items 1, 2 and 4 are free**: they are all derived by crossing a lane's
`[start, end]` window with the margin series that is already in hand. No new
column, no new query, no new latency.

**Item 3 is not.** Points for/against needs new columns in BOTH
`RIBBON_SQL_ISRAEL` and `RIBBON_SQL_EURO` (`app/R/global.R:270` and `:324`),
and on the EuroLeague side probably a change to
`euroleague/sql/053_stint_ribbon_read_layer.sql` — which means a migration, a
re-grant, and a re-run of `scripts/apply_db_security.R`.

Note the net swing (item 1) is NOT the same as points for/against (item 3):
+7 can be 20-13 or 7-0, and those are different stints. Item 3 is the one that
tells them apart. It is worth its cost only if that distinction is wanted.

## Constraints any design here must respect

- **One round trip per open.** Pooler latency is 238 ms against a 500 ms
  budget; server-side execution is 0.5-21.6 ms. Two queries blow the budget on
  latency alone. Anything added must ride the existing single query.
- **Density is the real problem.** The chart already carries lanes, names, a
  margin curve, a zero baseline, scale gridlines and two period-marker rows.
  Four more data layers is the design question — where each lives (bar face,
  gutter column, tooltip, hover-only) matters more than how each is computed.
- **No plotting library.** Server-built inline SVG in `build_stint_ribbon_svg()`
  (`app/R/helpers.R`). The spec argued out ggplot2/ggiraph/plotly in §7.
- **Both leagues or neither.** Israeli Tab 4 and EuroLeague Tab 11 share the
  builder. A EuroLeague-only or Israel-only metric splits the one thing this
  feature got right.
- **Never key anything on a player NAME.** Both leagues carry same-name/
  different-id players on one team; a name key merges two people's floor time
  into one lane, which looks plausible and is wrong.
- **`helpers.R` has mixed line endings** (2,670 CRLF + ~380 LF). Edit tool and
  `sed -i` both silently rewrite the whole file. Append via temp file + `cat`,
  or do exact-byte replaces matching each block's OWN eol.

## Open questions for the next session

1. Where does each of the four live? (bar face / gutter column / tooltip /
   hover-only) — this is the density budget, and it is the whole design.
2. Is the for/against split (item 3) worth a migration + re-grant + security
   re-run, given item 1 already gives the net? Or does it wait for a second
   pass once 1, 2 and 4 are on screen and the density is known?
3. Do the per-player totals reconcile with an existing app number
   (`player_traditional_stats_mv` minutes), or are they ribbon-local? If they
   must reconcile, the minutes definition matters — see the unattributed
   floor-time trap in CLAUDE.md.
4. Does hovering a lane re-scope the totals (this player's +/- while THIS
   opponent five was on) or stay static?

Start the next session by reading this file, then continue the brainstorm at
question 1. Do not start implementing — the design is not approved.
