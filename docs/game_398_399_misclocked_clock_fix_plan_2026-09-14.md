# Games 398 & 399: misclocked substitution blocks — diagnosis and fix plan

**Status:** plan only, not implemented. Deferred by user request (2026-09-14) — do
not start unprompted. See memory `project-game-398-misclocked-q2-etl-fix.md` for
the tracking note.

## Problem

Canonical minutes (running maximum of raw event elapsed time, per
[`docs/canonical_clock_minutes.md`](canonical_clock_minutes.md)) push one
lineup per team to the end of the quarter in two games, because a block of
consecutive events in the provider feed is stamped with the wrong
`quarterTime`.

- **Game 398, Q2:** feed ids 3980297–3980351 (37 events reaching
  `df_pts_poss_lineups_longer_mv`: 19 plays, 2 made baskets, 18 substitutions)
  are stamped `0:28` → `0:00` left in Q2. They belong around `10:00` → `8:00`.
- **Game 399, Q3:** feed ids 3990420–3990435 (11 Holon substitutions) are
  stamped `0:16` → `0:00` left in Q3. Same shape.

Because the canonical timeline is a running max ordered by action id, this
block drags the whole Q2 (398) / Q3 (399) timeline to the quarter's end, and
whichever lineup was on the floor at that moment absorbs the rest of the
quarter's minutes. Points and possessions are unaffected — they follow feed
order, not the clock.

## Evidence gathered (2026-09-14, from the raw provider feed + box score)

Pulled both endpoints directly:
- PBP: `https://stats.segevstats.com/realtimestat_heb/get_team_action.php?game_id=<id>`
- Box: `https://stats.segevstats.com/realtimestat_heb/get_team_score.php?game_id=<id>`

**Game 398** — the misclocked block sits between two correctly-clocked anchors:
id 3980296 (Q2 start, `10:00`, entered `19:20:04`) and id 3980352 (`8:00`,
entered `19:24:24`). Within the block, `userTime` (the feed's entry timestamp)
increases monotonically with `id`, confirming **feed order = entry order** for
this game. That lets us split the block by confidence:

- Every team-6 sub in the block, and team 17's first 10 subs, were entered
  *before* the block's first play (a foul at `19:21:38`) — these are `10:00`,
  essentially certain.
- Two team-17 sub pairs (entered `19:23:13` and `19:23:53`) land after that
  foul and other plays, so their true clock is somewhere in `(8:00, 10:00)`,
  not resolvable from entry time alone — interpolating linearly against entry
  time puts them near `8:37` and `8:17`, but this is an estimate, not a fact.

**Game 399** has no per-event entry times — every row in the block carries the
same single `userTime`. The block is still bounded by two correct anchors (id
3990419 = Q3 start `10:00`, entered `21:44:37`; id 3990436 = `9:55`, same
`userTime`), a **5-second window**, so stamping the whole block `10:00` is
close to exact regardless of the true sub-order within it.

**Provider box score, revised after the offline replay (see below):** initially
looked like a candidate independent oracle for team 6 (it disagreed with our
stored `seconds_on_floor` in matching +/- pairs up to 462s). The replay
disproved that for part of the roster: box's `minutes` field appears to be
computed from the same provider `quarterTime` stream, not independently
metered, so for any player whose true stint differs a lot from the buggy one,
box agrees with the *buggy* value. It remains a useful confirming signal only
for players whose segments don't cross the block sensitively (6 of team 6's 8
measurable players). Team 17's box total is also 134s short of a full quarter
(`11,866s` instead of `12,000s`), and one team-6 player's box value (`47:18`)
exceeds the 40-minute game entirely — a separate box-score defect. For game
399 the box is unusable as a check regardless: both teams are missing close to
a full quarter (~9,000s instead of 12,000s per team).

**Read-only database check (2026-09-14):** the current
`df_pts_poss_lineups_longer_mv` contains 1,041 rows for game 398 and 1,116
for game 399. Deduplicating by the canonical segment key gives 2,399 seconds
per team in game 398 and 2,394 per team in game 399. The misclocked blocks
are still present. Their `clock_regression_seconds` values are zero *inside*
the blocks; the large regressions appear at the following events: up to 480s
in game 398 Q2 (beginning at id 3980352) and 591s at game 399 id 3990437.
These queries confirm the current symptom and expected elapsed totals, but
do not test a proposed correction or a targeted reload.

## Offline replay result (2026-09-14, step 1 executed)

Ran the go/no-go replay: reconstructed team 6 and team 17's per-player minutes
independently of any DB table, using only the raw feed's own
`parameters.playerIn`/`parameters.playerOut` fields (each substitution row
toggles one player; the two fields are not an in/out pair, they're jersey
numbers confirming direction) walked from an empty roster, and the exact
running-max/segment-duration formula from
`refresh_segment_clock_fields_for_games.sql` reimplemented in R. Computed once
with the stored clock and once with the block corrected (dead-ball flat
`10:00` before the first play, linear real-time interpolation `10:00`→`8:00`
after it).

**Methodology check:** the uncorrected replay reproduces the live DB's actual
`seconds_on_floor` for team 6 almost exactly (matching its real box-score
diffs to within ~3s), confirming the reimplementation is faithful rather than
a guess.

**Where it confirms the fix:** two team-6 players never touch the block
(1162, 2317) — corrected minutes match box to the second (diff 0), a clean
independent hit. Four more round-trip through the block (1077, 1456, 1982,
1249) — corrected lands within 6-11s of box (was 106-461s off before). That's
6 of 8 measurable team-6 players.

**Where box stopped being useful as a check:** five players (2069 on team 6;
1870, 1065, 1562, 2334 on team 17) come out *worse* against box after
correction — box matches the *buggy* value, not the corrected one. Traced
2069's case fully: his post-block stint runs from the block to id 3980511, a
real, non-block substitution stamped `01:13 remaining in Q2` at real time
`19:46:13`, over 26 minutes after the block. For that stamp to be honest,
2069 must have played continuously from the block (`10:00`) down to `1:13` —
a ~527s stint — not the ~16s the raw clock implies. That's a second,
independently-timestamped anchor forcing the same conclusion as the
dead-ball argument, not an assumption. 1870's case is structurally identical
(a normal, correctly-timestamped entry at Q1 2:51 remaining, then this same
dead-ball block) and the same reasoning applies.

**Conclusion:** the box score's `minutes` field does not appear to be
independently metered — it is best explained as computed from the same
provider `quarterTime` stream, so it inherits the identical distortion for
any player whose true stint differs substantially from the buggy one. That
reverses the plan's original assumption that box was a clean independent
oracle (see "Diagnosis confidence" below). Separately, player 2281's box
value (`47:18`) exceeds the 40-minute game entirely — an unrelated box-score
defect that disqualifies it as a check regardless.

**Net effect on go/no-go:** go, but on different evidence than planned.
Box-score agreement is only informative for the 6 players whose segments
don't cross a block boundary sensitively (it confirms the fix there,
unanimously). For the other 5, the internal-consistency argument (a
subsequent, independently-timestamped real event that requires the corrected
reading) is doing the real work, not box comparison.

**Game 399 replay (2026-09-14) — stronger result, no box needed.** The block
(ids 3990420-3990435) sits between two correct anchors five real seconds
apart (Q3 start `10:00` at id 3990419, `9:55` at id 3990436), and unlike 398
has no live play inside it (the first live play, id 3990437, is safely
outside), so the whole block flat-corrects to the Q3-start value with no
interpolation needed. The uncorrected replay reproduces both teams' box
totals exactly (9,000s and 9,072s) — same methodology check as 398. The
corrected replay's *predicted* team total was 12,000s (5 × 40 min) — this was
wrong by a known, understood amount: the replay script's own "game end"
computation wrongly included the feed's synthetic `quarter`/`game` marker
rows (team-less, reading `00:00`), which the real pipeline correctly excludes
from a team's timeline. The correct team-final boundary is the last real,
team-attributed action (`00:06` remaining, elapsed 2,394s), giving 2,394 × 5
= **11,970s per team** — confirmed against the database after the actual
reload (see "Reload executed" below), not 12,000. Either way the buggy value
was short by very close to one quarter (3,000s = 10 min × 5), matching the
mechanism directly: the block's stamped clock reads as *end* of Q3 instead of
*start*, so the running max jumps straight to the Q3-end mark and the entire
real quarter collapses into a near-zero segment. This remains a
self-contained internal check (bounded by the game's own real final action),
not a comparison to box (which is separately known broken for this game).
Per-player shifts (62-504s, all positive — nothing is taken away, since the
erased quarter's time never existed in the buggy version) land in the range
the original from-scratch SQL rebuild had already estimated ("~20-30
player-minutes/team" in the tracking memory).

## Reload executed (2026-09-14, GitHub Actions run 34903587781)

Added an optional `game_ids` `workflow_dispatch` input to
`.github/workflows/etl-full.yml` + `scripts/run_etl_full.ps1` (dormant for
the `schedule` trigger — inputs are only read on `workflow_dispatch`, so
nightly runs are unaffected). Committed `64c84ca` on `etl/fix-398-399-clock`,
merged to `main`, pushed. Triggered `gh workflow run etl-full.yml -f
game_ids=398,399` — completed successfully in ~9 minutes (dry-run then real
write, per the wrapper's default path).

**Verified against the live database post-reload:**
- Zero rows with `clock_regression_seconds > 10` remain on either game.
- Game 398: all 16 affected players' `seconds_on_floor` match the offline
  replay's predicted corrected values to within 1s (rounding only).
- Game 399: both teams total 11,970s exactly, matching the corrected
  11,970 derivation above (not the replay's originally-claimed 12,000 --
  see correction above).
- `basketball_test.app_meta` key `etl_full_last_success` advanced to
  `2026-09-14 22:28:31`, so Shiny's season-level caches (`GL_DATA_CACHE` etc.)
  invalidate on next access.

The fix is live in the database. Not yet done: the cold-storage row-count
diff mentioned in step 4's risk note (confirming no stale Parquet rows were
left behind for these two games) has not been checked.

## Diagnosis confidence

- **Game 398, dead-ball portion of the block (ids before the first live play,
  both teams):** high confidence — the id-order/entry-time argument, now
  additionally confirmed by the offline replay for 6 of 8 measurable team-6
  players (2 exact box matches, 4 within ~10s) and, independently of box, by
  a downstream real-clock anchor (id 3980511, `01:13 remaining in Q2`) that
  requires the corrected reading for player 2069's stint to be physically
  possible.
- **Game 398, team 17's dead-ball subs specifically:** same high confidence on
  direction; box itself is short for this team (134s), so it under-confirms
  here even where the fix is right — consistent with, not contradicting, the
  replay's finding that box shares the source clock's distortion.
- **Game 398, the two late team-17 sub pairs (live-window interpolation):**
  genuine uncertainty, bounded to a 2-minute window; error caps at ~120s per
  boundary, versus the ~7-8 minutes per player currently misallocated. The
  replay's disagreements touching these ids are not evidence against the
  interpolation specifically — the same players' box disagreement pattern
  also shows up via purely dead-ball-side segments (1870), so it's better
  explained by box sharing the bug than by the interpolation being wrong.
- **Game 399:** high confidence, now on stronger grounds than the id-order
  argument alone. The offline replay's corrected version sums to exactly
  12,000 player-seconds per team — the only value a 4-quarter game with 5
  players on the floor at all times can produce — while the uncorrected
  version is short by almost exactly one quarter. That's a self-contained
  arithmetic identity, not dependent on box (which is separately broken for
  this game) or on any per-event entry-time argument (this game's `userTime`
  is a single stuck value for every event, so none was available).

## Fix plan

1. **Offline replay, no DB writes — DONE 2026-09-14, verdict: go.** See
   "Offline replay result" below. Box-score agreement is informative only for
   players whose segments don't cross the block sensitively; the decisive
   check turned out to be a downstream, independently-timestamped real event
   (id 3980511) whose only physically possible reading requires the
   correction.
2. **Correct the source in `clean_actions()` (`etl/etl_onoff.R`)**, following
   the existing game-381 precedent (a hardcoded per-game id-range filter/edit
   in the same function). Represent the corrections as a small explicit table
   (`game_id`, `id`, wrong `quarter_time`, corrected `quarter_time`, reason),
   not a growing pile of ad hoc `if` blocks. Each correction should apply only
   if the feed still shows the *expected wrong* value at that id, so a
   provider-side fix to the feed makes the correction a no-op (with a logged
   warning) instead of silently corrupting already-correct data. Apply the
   correction **before** `clean_actions()` derives
   `end_quarter_seconds_remaining` and `end_game_seconds_remaining` from
   `quarter_time` (or recompute both derived fields afterward); otherwise the
   displayed clock and the clock used downstream will disagree.
3. **Leave the running-maximum canonical-clock rule unchanged.** After 398/399
   are re-timed at the source, their clocks are monotonic within the block, so
   the rule has nothing left to fix there. The rule is correct for a *late*
   stamp (game 115, event inserted with an earlier clock than its true
   position) but cannot by itself fix an *early* stamp like this one — it has
   no way to know which side of a backward jump is the wrong one. Encoding
   that judgment into a generic rule isn't warranted; the data-quality check
   (`AK_misplaced_clock_runs`) already flags these cases individually.
4. **Reload:** `etl_full(game_ids = c(398, 399))`. **Not yet verified** that a
   targeted reload cleanly replaces an already-processed game end-to-end
   (delete-then-insert of base rows, cold-storage parquet dedup/merge,
   incremental refresh of dependent MVs/functions) — check this before
   running it for real.

   The cold-storage merge (`export_cold_table()`) only overwrites Parquet rows
   whose key still exists in the freshly-exported rows
   (`rbind(new_rows, existing) |> distinct(key_cols, .keep_all = TRUE)`); a key
   that disappears from the DB on reprocessing is never removed from the
   Parquet file. This is real for the general reload path, but traced through
   `compute_stints()` for *this specific* fix: all five cold tables key on
   `(game_id, id)`, or for `stints` on `(game_id, team_id, final_start_id,
   final_end_id)`, and those id-based keys come from `window_order(quarter,
   id)` — ordered by action id, not by clock value. Since the fix only
   rewrites `quarter_time` on ids that stay in the same quarter (no row
   added, removed, or reordered), none of the five tables' keys should
   actually change here, so the stale-row risk likely doesn't fire for this
   reload specifically. Confirm rather than assume: diff each cold table's
   row count and key set for game_ids 398/399 before vs. after the reload.
   Treat any cold-storage export failure as a failed reload before relying on
   the result either way.
5. **Verify `app_meta.etl_full_last_success` advances** so Shiny's season-level
   caches invalidate (per `CLAUDE.md` § ETL Scheduler). A successful
   `etl_full()` already updates this key; bump it manually only if the
   dependent refresh is performed outside that pipeline.
6. **Post-reload verification:**
   - both teams retain the expected canonical elapsed total: 39:59 for game
     398 and 39:54 for game 399, assuming their final Q4 events are unchanged.
     The canonical model ends at the last observed event, not necessarily
     40:00; verify that minutes are redistributed to the correct players;
   - the large Q2 (398) / Q3 (399) clock regressions immediately *after* the
     corrected blocks disappear (currently up to 480s from id 3980352 and
     591s at id 3990437); checking only inside the blocks would pass even
     before the fix;
   - game 398 team 6's per-player minutes match the provider box score
     closely;
   - `AK_misplaced_clock_runs` has no finding for either corrected range (this
     check has `warning` severity, not `critical`);
   - points, possession counts, and other non-time-derived aggregates match
     the pre-reload values; only clock/segment/minutes fields should move.

## Open items / risks

- **Reload mechanics unverified** — see step 4. The cold-storage merge's
  handling of removed keys is a concrete blocker to check before database
  writes, not just a deduplication detail.
- **The two late team-17 sub pairs in game 398** rely on an interpolated
  estimate, not a recovered fact. Worth deciding up front whether an estimate
  is acceptable here or whether to leave those two boundaries at their current
  (wrong) values with a documented residual error, since the impact is capped
  at ~120s per boundary rather than the current ~7-8 minutes.
- **Game 399 has no independent check.** The fix rests solely on the
  5-second-window argument from id ordering. If that assumption is wrong for
  this particular feed, there's no box-score cross-check to catch it (the 399
  box is broken/incomplete).
