# Stint ribbon — richer stint data: design

**Status:** approved 2026-09-06. Supersedes the unfinished brainstorm captured in
`docs/stint_ribbon_data_brainstorm_inputs_2026-09-06.md`, which should now be read
only as a record of how the questions were framed — three of its cost estimates
are corrected below by measurement.

Follow-on to the stint ribbon (`shiny/stint-ribbon`, complete;
`docs/superpowers/specs/2026-09-05-stint-ribbon-design.md`). This adds the four
data layers the user asked for, without adding a second round trip.

## 1. What is being added

| # | Addition | Where it lives |
|---|----------|----------------|
| 1 | Per-stint +/- | printed on the bar face |
| 2 | Per-player game totals (minutes, +/-) | two new gutter columns |
| 3 | Points for / against per stint | detail strip under the chart |
| 4 | Who was on the floor together | a band + lit overlaps on the chart itself, with windows and swings in the strip |

Decisions taken with the user, in order:

- **Bar faces carry the per-stint +/-; everything else is on hover.**
- **Every bar that fits gets a number, zeros included.** A blank bar therefore
  means one thing only: too short to label. It never means "level".
- **Hover is carried by a fixed detail strip below the chart**, not a floating
  tooltip and not the native `<title>`.
- **The gutter never re-scopes.** MIN and +/- always mean the whole game; hover
  changes only the clipped margin curve, the band, and the detail strip.
- **The lineup is answered on the chart, not in prose.** Any text about who was
  on together must carry a time window and a margin swing, so it maps onto
  something the reader can see. A bare list of names and durations was rejected
  for having no connection to either axis.

## 2. Measurements this design rests on

All taken 2026-09-06 against the live database. Nothing here is estimated.

**Bar geometry** — 15 Israeli 2026 games, real lanes after `merge_adjacent_stints()`:

- 72 stint bars per game (range 55–107) across 20 player lanes
- bar width quantiles (px, at the current 850-unit plot area):
  5% = 10, 25% = 56, **50% = 105**, 75% = 159, 95% = 278
- bars wide enough for `+12` (≥20px): **90.3%** — about 65 of 72 a game

**A bar is not one lineup.** 1,470 real merged stints, same 20-game sample:

- **86.3% of bars span more than one five.** Spells per bar: median 4, mean 5.2,
  90th percentile 10, max 31.
- The opening five lasts a median of **58 seconds** and covers a median of
  **26%** of the bar. In **69.5%** of bars it is gone before halfway.
- Distinct teammates sharing any floor time within one bar: median 6, max 10.

This is why the detail strip does not name "the five at the start" (it is stale
for most of most bars) and does not list the spell sequence (median 4 lines,
tail 31). §7 answers the lineup on the chart's own time axis instead.

**Minutes already reconcile.** Ribbon floor time vs
`player_four_factors_by_game` minutes (`is_on_key = 1`, `type_lineup = 'offense'`):

- Israel: **301 / 301** player-games agree to 7e-15
- EuroLeague: **323 / 323** agree to ≤0.002 min (the MV stores 3 decimals)

So the gutter minutes need no hedged label and no reconciliation work. This is a
regression test to write, not a risk to manage.

**The PBP rescinds baskets.** Summing positive score increments over a whole game
overstates both teams by the size of any credited-then-rescinded score: measured
15 own + 15 opp phantom points across 10 of 40 Israeli games, with `own_drops` +
`opp_drops` accounting for the inflation exactly. Final scores match
`final_schedule_mv` 40/40, so only the intermediate running score wobbles.
**The net-difference formula in §5 absorbs this; summing increments does not.**

**No step merges two teams' scoring.** Max |margin delta| is 3 in both leagues,
with zero steps above 3, so the sign of each step attributes points cleanly.
EuroLeague reconciled 40/40 exactly on both for and against.

## 3. The attribution decision, and what it costs

Two definitions of a stint's points were built and measured against the app's
published on-floor points (`player_four_factors_by_game.total_points`, which is
`sum(cd.team_score)` at segment grain — see
`sql/materialized_views/player_four_factors_by_game.sql:228`):

| | ties with published on-floor points | how it fails | cost |
|---|---|---|---|
| **Clock-based** (margin steps) | pf 395/492, pa 282/492, max diff 5 | redistributes ≤5 pts *between the two players in one substitution*; mean difference exactly 0 | free in both leagues |
| **Segment-based** (`SUM(team_score)`) | pf 373/492, pa 372/492, max diff 4 | *drops* points from excluded and zero-length segments — a whole-team undercount in games with lineup gaps | EuroLeague range join, new view, migration, re-grant, security re-run |

**Chosen: clock-based.** It reconciles no worse, costs no migration, and has a
property the segment version cannot have: the ribbon draws the margin curve
directly above the bars, so a bar's `+6` always equals the curve's rise across
that bar. A reader can verify it by eye. Segment-based numbers would visibly
contradict the curve drawn above them.

**Disclosed consequence:** a stint's for/against will differ from Tab 1's
possession-based on/off points by up to 5 points a game, because a possession can
straddle a substitution and the two systems attribute it differently. This is a
definitional difference, not a defect, and both numbers are internally correct.
It is not surfaced in the UI.

## 4. Data layer — one new column, no new query

The single-round-trip constraint is unchanged: 238 ms pooler latency against a
500 ms budget means anything added must ride the existing query.

`pf - pa` is the margin delta, already in the payload. `pf + pa` is not derivable
from the margin. So exactly one number beyond the margin is required.

**Israel** (`RIBBON_SQL_ISRAEL`, `app/R/global.R:270`) — add `own_team_score` to
the `marg` CTE. It is already a column on
`basketball_test.df_pts_poss_lineups_longer_mv`, the relation that CTE already
scans. No new relation, no new grant, no plan change.

**EuroLeague** (`euroleague/sql/054_ribbon_margin_own_score.sql`, new) — append
`own_team_score` to `euroleague.ribbon_margin_v`. The view already reads
`euroleague.action_team_context_actions`, which carries `own_team_score` and
`opp_team_score` and merely subtracts them (see
`euroleague/sql/053_stint_ribbon_read_layer.sql`). Appending a column at the end
of the select list is a `CREATE OR REPLACE VIEW`, which does **not** drop the
view and therefore preserves the `app_readonly` grant. Verify the grant survived
rather than assuming it — `sql/security/audit_app_access.sql` already enumerates
this view.

`RIBBON_SQL_EURO`'s `marg` CTE then selects the new column, keeping the two
readers' shapes identical, as they are today.

Payload cost: one integer per margin row. The Israeli series is 544–817 rows
(it is every action row, not only scoring ones), so ~6 KB on a 29–39 KB payload.
EuroLeague's series is ~90–110 rows (scoring events only) and grows trivially.

## 5. Computation — pure transforms in `helpers.R`

Two new pure functions, tested directly, added next to the existing ribbon
transforms:

**`ribbon_score_as_of(series, t)`** — value of a step series at time `t`, defined
as the last row with `elapsed <= t`, and 0 before the first row. Vectorised over
`t` via `findInterval()`.

**`ribbon_stint_points(lanes, margin)`** — per stint:

```
pf  = own(end) - own(start)
pa  = opp(end) - opp(start)      where opp = own - margin
pm  = pf - pa
```

Per-player gutter totals are the sums of their stints' `pf`, `pa`, `pm`.

**`ribbon_stint_overlaps(lanes, stint)`** — the intersection of one stint's
window with every other lane's bars on the same side, returning
`(player_key, start, end)` per overlapping span, ordered by shared duration
descending. Pure interval arithmetic over data already in hand. Each span's
margin swing reuses `ribbon_stint_points()` on the intersection window, so a
teammate's swing is computed by exactly the same code path as a stint's own —
there is no second definition of `+/-` to keep in step.

Three traps to encode as tests:

- **Use the raw series, not `ribbon_complete_margin()`'s output.** Completion pads
  the series to the full clock for path drawing; as-of lookups must not see
  padded points.
- **Never sum increments.** `sum(delta[delta > 0])` reintroduces the
  rescinded-basket bug measured in §2. The net difference across the window
  cancels a credit-then-rescind that falls inside the window.
- **The boundary rule is `elapsed <= t` at both ends**, so a score at the exact
  second of a substitution lands in the outgoing player's stint and forms the
  incoming player's baseline. This telescopes correctly and is what keeps a
  player's bars summing to their gutter total.

Excluded segments (the `cardinality(...) = 5` guard) are not drawn and so are not
counted in gutter totals. `ribbon_health_message()` already warns when a game has
any, and that stays the only disclosure.

## 6. Rendering

**Geometry.** `RIBBON_GUTTER` 150 → 220 and `RIBBON_WIDTH` 1000 → 1070, together,
so the plot area stays 850 units and every bar width measured in §2 is preserved
exactly. Within the gutter the name keeps its current right-anchor at
`RIBBON_GUTTER - 78` (unchanged geometry, new constant), MIN right-anchors at
`RIBBON_GUTTER - 35`, and +/- at `RIBBON_GUTTER - 8`.

The gutter now holds three text columns where it held one, so the clearance
between them must be **measured in a browser, not eyeballed** — this is exactly
how `RIBBON_BAND_GAP` was settled on 2026-09-06, where two boxes that looked fine
overlapped by 1.9px with long team names. Check the widest real case: a long
player name against a two-digit `MIN` against a signed two-digit `+/-`.

**Header.** The existing header row gains `MIN` and `+/-` labels at the same two
x anchors. No new row, so no added height.

**On-bar numbers.** Rendered in JetBrains Mono — the theme's mono face — so text
width is computable server-side with no font metrics:

```
required_px = nchar(text) * 0.6 * 9 + 6
```

(JetBrains Mono's advance is 600/1000 em.) A number is printed when the bar is at
least `required_px` wide, centred; otherwise the bar is left bare. Zero prints as
`0`; nonzero always carries its sign. Thresholds this yields: `0` = 11.4px,
`+6` = 16.8px, `+12` = 22.2px — consistent with the 90.3% coverage measured at
20px.

**Contrast must be measured, not eyeballed.** The ink sits on two different
fills — the amber own-bar and the muted opp-bar — and needs ≥4.5:1 on both (9px
is small text, so the 3:1 large-text allowance does not apply). Check the computed
values against the threshold; do not compare against what it replaced.

## 7. Hover: the lineup is answered on the chart

The measurement in §2 rules out naming a stint's five, because a bar is a median
of four different fives. But the chart is already a time axis with every lane
aligned, so who was on together is a **vertical slice** of it. The hover makes
that slice legible rather than describing it in prose.

**On the chart.** Hovering or focusing a bar:

1. draws a band spanning that stint's window down the full height of the chart;
2. lights the exact spans of other bars that fall inside the band, dimming
   everything else — so a teammate on for the whole stint reads differently from
   one who joined partway, at a glance and in the right place on the axis;
3. clips the margin curve to the same window, which it already does today.

Overlap spans come from `ribbon_stint_overlaps()` (§5). Computing them
server-side for every bar against every other bar is O(n²) markup on 72 bars, so
they are computed in `app/www/app.js` on hover from `data-start` / `data-end`
attributes already carried per lane — one pass over the lanes, no layout work.

**In the strip.** A single row below the SVG carries what the picture cannot: the
numbers. Fed by the existing `data-clip` hover/focus wiring (which already handles
pointer, keyboard focus and touch), holding the last lane touched rather than
clearing on exit, so the reader can look away from the chart to read it.

```
Player One  ·  Q2 4:12-8:30 (4:18)  ·  +6   14-8
with  Two 4:12-8:30 +6  ·  Thr 6:17-8:30 +4  ·  Fou 6:17-7:40 +1
```

Every teammate entry carries **both axes** — the shared window and the margin
swing across it — so each line of text maps onto a span the reader can see lit in
the chart above. Entries are ordered by shared duration, longest first. A median
of 6 entries and a maximum of 10 (§2) keeps this to one or two lines.

Resting state (nothing hovered yet) is a short instruction, not an empty box.

Teammates are keyed on `player_key`, **never on a player name** — both leagues
carry same-name / different-id players on one team, and a name key would merge
two people's spans into one.

**Accessibility.** The strip is `aria-hidden`; each lane's existing `aria-label` is
extended to carry the same facts, so screen-reader users get them without an
`aria-live` region firing on every hover. The lanes are already `tabindex="0"`,
and keyboard focus drives the band and the lit overlaps identically to hover —
there is no pointer-position dependence anywhere in this design.

## 8. Testing

Unit (pure, no DB):

- `ribbon_score_as_of()` — before the first row, exact hits, between rows, after
  the last row
- `ribbon_stint_points()` — a credit-then-rescind inside a window nets correctly;
  a score exactly on a substitution boundary lands in the outgoing stint
- fit rule — the three width thresholds above, and that a bare bar is emitted
  rather than a clipped or overflowing number
- gutter totals — a player's bars sum to their gutter `pf`, `pa`, `pm`
- `ribbon_stint_overlaps()` — a teammate on for the whole stint, one who joins
  partway, one who leaves partway, one who does both, and one who does not
  overlap at all; ordering by shared duration; and that a teammate's swing over
  the intersection equals `ribbon_stint_points()` on that same window

Reconciliation (`RUN_DB_TESTS=1`, both leagues):

- gutter minutes vs `player_four_factors_by_game` — the 301/301 and 323/323
  results in §2 become the assertion
- self-consistency: each bar's `+/-` equals the drawn curve's rise across that
  bar; each player's bars sum to their gutter total

Structural: the SVG contains the two new gutter columns, their header labels, and
a number on exactly the bars that clear the fit threshold.

## 9. Constraints carried forward from the existing feature

- One round trip per open; cache per `game_id` + `shared_data_version()`.
- No plotting library — server-built inline SVG.
- Both leagues or neither.
- Never key anything on a player name.
- **`app/R/helpers.R` has mixed line endings** (2,670 CRLF + ~380 LF). The Edit
  tool and `sed -i` both silently normalise the whole file. Append via a temp file
  and `cat tmpfile >>`; for a one-token change do a binary-mode exact-byte
  replace. Verify `git diff --stat` shows insertions only and that the CR count
  still reads 2,670.
- After any DDL, re-run `scripts/apply_db_security.R` with
  `CONFIRM_DB_SECURITY_APPLY=1` if — and only if — the grant audit shows a loss.

## 10. Out of scope

- Re-scoping the gutter to shared floor time on hover or on a pinned lane
  (offered and declined; the with/without question is a bigger feature).
- Colour-encoding bars by +/- (offered and declined in favour of printed numbers).
- Surfacing the ≤5-point divergence from Tab 1's possession-based attribution.
- Any change to how the margin curve itself is drawn or scaled.
- Naming the five on the floor at a stint's start (measured stale for most of
  most bars — §2), listing a bar's spell sequence (median 4 lines, tail 31), or
  subdividing bars with per-spell dividers (hundreds of dividers across the
  ~65 already-numbered bars).
- Pointer-position-sensitive hover, where the strip tracks which spell you are
  over within a bar. Rejected because keyboard focus has no cursor position and
  would need a separate rule.
