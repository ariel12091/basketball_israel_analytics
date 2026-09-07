# Stint ribbon — lineup decomposition: design

**Status:** approved 2026-09-07.

Follow-on to the stint ribbon (`docs/superpowers/specs/2026-09-05-stint-ribbon-design.md`)
and its richer-data layer (`docs/superpowers/specs/2026-09-06-stint-ribbon-richer-data-design.md`).
Both are implemented on `shiny/stint-ribbon` at `1f4caa3`, unmerged.

## 1. What prompted this

A manual browser pass on the implemented feature produced two findings from the
user:

1. **The teammate data in the hover is too crowded.**
2. **The player-by-player data does not make much sense basketball-wise — it
   should be lineup by lineup.**

Both point at the same fact, already measured in the richer-data spec and not
acted on there: **86.3% of stint bars span more than one five** (median 4 spells
per bar, mean 5.2, p90 10, max 31). A bar's `+/-` is therefore the *sum* over
roughly four different lineups. That number is correct — it is the player's real
margin change across the stint — but it is not **attributable** to any single
unit. The richer-data layer tried to answer "who was on together" in prose
inside a grain that cannot hold one answer, and the prose is what felt crowded.

The fix is not to re-key the chart to lineups. It is to keep the player lane and
**decompose it on demand** into the fives that composed it.

## 2. Decisions

| # | Decision | Rationale |
|---|---|---|
| L1 | **Lanes stay players.** The resting chart is unchanged: player lanes, both blocks, opponent mirrored below the curve, each bar carrying its stint `+/-` total | The occupancy read ("who plays when") is valid at player grain. Only the *attribution* was broken. Re-keying to lineups was explored and rejected — see §6 |
| L2 | **Click a stint → the bar splits in place** into its constituent fives, each segment carrying its own `+/-` where width allows | The decomposition is the missing attribution. Click, not hover, so the resting chart stays quiet |
| L3 | **The strip below lists those same fives** — window, membership, `+/-` — *replacing* the `with <player>` prose | This is the crowding fix. The list is click-gated, so the median 4 rows is a deliberate cost, not an accidental one |
| L4 | **Hover a five in the list → its window is marked on the clicked player's lane only**, and on the margin/time chart | Marking all five members' lanes was considered and rejected by the user: on a ~26-lane chart it moves too much, and membership is already spelled out in the row being pointed at |
| L5 | **Marking reaches every window that five played**, with the clicked stint at full strength and its other appearances as weaker echoes | User constraint, given twice: the broader answer is wanted, but "it should be clear what the original stint is" |
| L6 | **The bar keeps its stint `+/-` at rest**; segments add the per-five parts on click | The total is a real quantity, and the parts visibly sum back to it. Total and decomposition, checkable by eye |
| L7 | **Each side's numbers are in its own perspective.** An opponent bar or segment reads `+8` when that opponent five won those minutes by 8; points-for/against flip with it | Resolves final-review issue 1, which was an *unconsidered gap*, not a recorded decision. Matches Tab 2, where the same five looks up to the same sign |
| L8 | **The overlap machinery is deleted**, not adapted: `ribbon_stint_overlaps()`, `ribbon_overlap_label()`, the teammate band and the lit overlaps | Its question is now answered by L2-L4. It is also the dominant cost in the current render (final-review issue 3) |
| L9 | **The split is drawn client-side** from a compact per-bar data attribute, not emitted as hidden rects | ~175 segments per team-game would roughly triple the SVG's rect count against a payload the final review already measured at 97 KB |
| L10 | **Keyboard focus on a stint triggers the same split as a click**, and the list is reachable and hoverable by keyboard | The strip is the screen-reader path; parity is not optional |

## 3. Measurements

### 3.1 Taken 2026-09-07 against the live database

Israeli figures replicate the ribbon reader's own `segs` CTE — same `GROUP BY`,
same `HAVING MAX(segment_seconds) > 0` — so they describe exactly the rows the
feature sees. EuroLeague reads `euroleague.ribbon_segments_v`.

| | Israeli 2026 (442 team-games) | EuroLeague (1,178) |
|---|---|---|
| distinct lineups per team-game | med **17**, p90 23, max 34 | med **19**, p90 25, max 38 |
| contiguous spells per team-game | med **35**, p90 42, max 56 | med 36, max 58 |
| lineups with >= 1 min | med 11 | med 12 |
| lineups with >= 2 min | med 7, p90 9 | med 7, p90 9 |

Floor-time concentration (Israeli): the top 6 lineups hold **70.6%** of a team's
floor time, top 8 **79.9%**, top 10 **86.9%**, top 12 **91.9%**.

**Derived, not measured:** ~35 spells x 5 players ~= **175 player-segments per
team-game**, against 72 merged bars today. A spell averages 2400/35 ~= 69 s ~=
**24 px** at the current 850 px plot area — just above the ~20 px a `+12` label
needs, so most segments can carry a number and a real tail cannot.

### 3.2 Carried forward from the richer-data spec (measured 2026-09-06)

- 72 stint bars per game (range 55-107); bar width median 105 px; **90.3%** of
  bars >= 20 px, i.e. wide enough to label.
- **86.3% of bars span more than one five.** Spells per bar: median 4, mean 5.2,
  p90 10, **max 31**.
- The opening five covers a median 26% of its bar and is gone before halfway in
  **69.5%** of bars — which is why naming "the five at the start" was rejected,
  and why the decomposition has to be the whole sequence.

### 3.3 Measured and then made moot

Recorded so the next person does not re-take them. These were taken while
exploring a lineup-lane chart (§6), which was rejected:

- Gutter labels for a five, over 442 team-games restricted to players who
  actually take the floor: a 3-char surname abbreviation is ambiguous in
  **71/442** team-games, 4-char in 8/442, jersey numbers in **3/442**.
- `basketball_test.full_rosters.jerseynumber` and
  `euroleague.full_rosters.jersey_number` both exist and are **fully populated**
  (0 missing over 6,739 roster rows, max 2 digits). Surnames run median 6
  characters, p90 9, max 15.
- `full_rosters.inroster` is `0` or `2` — **never `1`**. A filter of
  `inroster = 1` returns the empty set.

## 4. Interaction model

**At rest.** Exactly today's chart. No new marks.

**On click of a stint bar.**

- The bar divides in place at each lineup change.
- Each segment shows its own `+/-` where the segment is wide enough, by the same
  width test the on-bar numbers already use.
- The strip below lists the fives in time order: window, the five's members, and
  that five's `+/-`. Capped with a `+N more` for the long tail (max 31).
- Exactly one stint is selected at a time. Clicking elsewhere, or `Esc`, clears.
- **Opponent stints behave identically.** A bar in the lower block splits, lists
  and marks exactly as an own-team bar does; only its sign convention differs
  (L7). There is no read-only side.

**On hover of a row in that list.**

- That five's window is marked on the clicked player's lane (L4) and on the
  margin/time chart.
- Other windows in which the same five played are marked as weaker echoes; the
  clicked stint's own occurrence stays at full strength (L5).

**Lane hover** keeps its current behaviour — it splits the margin curve, bright
while that player is on. Click layers on top of it; the two do not conflict.

## 5. Data contract

The decomposition **already exists upstream and is discarded**.
`merge_adjacent_stints()` (`helpers.R:3078`) collapses contiguous per-segment
rows into one bar, and the reader's pre-merge rows are already one per
`(segment, player)`, a segment having a constant lineup by construction. The
change is to keep what the merge throws away.

- **Both readers carry a lineup key per lane row.** Israeli has `s.lineup_hash`
  in `segs` already and currently drops it in the `unnest`. EuroLeague has no
  hash: `euroleague.ribbon_segments_v` carries `player_ids` directly, and that
  array — already sorted and verified 5/5 with zero fan-out in the original
  spec — *is* the identity, so its text form is the key. Do not mint a second
  identifier for the EuroLeague side.
- **`fetch_stint_ribbon()` returns the pre-merge segments beside the merged
  bars**, plus a per-game dictionary of lineup key -> the five player labels, so
  names are stored once per lineup rather than repeated on every segment row.
- **The builder emits per-bar segment data as one compact attribute** and the JS
  draws the split (L9).

## 6. Rejected: re-keying the chart to lineup lanes

Explored in full before landing on decomposition, and recorded because the
measurements make the case concrete. One lane per distinct five would mean
med 17 lanes per team (p90 23, max 34) — with both blocks kept, ~36 lanes
median and **~72 worst case**, roughly 780 px to 1,390 px against today's
510-610 px. Only ~7 lineups a game clear two minutes, so about ten of those
lanes are slivers.

It was rejected for a better reason than height: **a lineup lane cannot show
occupancy.** "Who plays when" is a legitimate question that only the player lane
answers, and re-keying trades a valid read for an attributable one when
decomposition delivers both. The user's own framing settled it — *"the player is
the lane, but it's composed out of those 5-men lineups."*

## 7. Invariants and tests

- **Sum of segment `+/-` = bar `+/-` = the margin curve's rise across the bar.**
  The existing DB reconciliation test asserts the second equality; the first is
  new and must be added. Both must fail when the behaviour is removed — this
  plan's predecessor produced **four** tests that passed against a stub, so every
  new assertion states what makes it non-vacuous and is proved by mutation.
- The existing reconciliation test **narrows to `side == "own"`**, because L7
  makes the property untrue of the opponent block by design.
- A segment's membership must equal the lineup dictionary entry for its key, for
  both leagues, against live data.
- Every emitted numeric must be finite: the branch's history includes a
  `NULL -> NaN` that blanked an entire SVG path, and a NULL-score family that
  still reaches production data (§8).

## 8. Folded in from the final whole-branch review

Independent of this redesign, fixed in the same wave:

- **Four games draw a fabricated flat "tied" curve.** Israeli 2026 games 139,
  140, 141 and 143 have `own_team_score`/`opp_team_score` NULL on every row for
  both teams. The margin frame empties, the fallback
  `data.frame(elapsed = c(0, 2400), margin = c(0, 0))` engages, and the chart
  asserts the game was level throughout with no health message. It must
  **disclose, not invent**: suppress the curve and say the score series is
  unavailable. Worth a separate ETL investigation.
- **The 46-line modal observer is cloned** between `server_tab4.R:331-376` and
  `server_tab11_euro_gamelogs.R:32-77`, differing only in five parameters. The
  one remaining league clone; extract it.
- **Ribbon open has no `guard_heavy_request()`**, unlike every other heavy path
  in the app, and the click target is a whole column of date cells.
- Issue 1 is decision L7; issue 3 is largely retired by L8.

## 9. Assumptions

Stated rather than asked, and cheap to overturn:

- Lane order and the gutter are unchanged — a row holds one player name, so the
  142 px name field is already sufficient.
- The strip list caps with `+N more` rather than scrolling.
- Nothing in this design changes the reader's single-round-trip budget (D8 of
  the original spec): the segments and the lineup dictionary ride the existing
  query.
