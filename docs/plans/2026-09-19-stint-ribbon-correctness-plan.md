# Stint Ribbon Correctness Plan

Date: 2026-09-19

One plan for everything currently known to be wrong with the Game Flow / stint
ribbon. Every number here was measured against production (451 games in
`df_pts_poss_lineups_longer_mv`) or against the cold-storage Parquet snapshot
(448 games after merging `exports/cold/` with the `cold-storage/latest`
release). Nothing is estimated.

**Prerequisite, owned elsewhere.** The biggest ribbon defect, the period-opening
attribution gap (A), is an ETL data-correctness fix that affects every tab, not
just this chart. It has its own plan,
`docs/plans/2026-09-19-period-opening-lineup-anchors-design.md`, which owns
everything about it: design, gates, shadow run, reprocessing and baselines.
This document keeps A only as a dependency and does not repeat its detail.
Everything here that reads lineup states (C, D, F) is re-measured after it
lands.

## Status at a glance

| # | Defect | Scope | Severity | Status |
|---|---|---|---|---|
| A | Period-opening attribution gap draws the **wrong five** across a boundary | 6 straddles, 5 games; **not present in EL** | High — asserts a false lineup | **Prerequisite — separate plan** (anchors design) |
| B | Margin curve padded flat across periods with no data | 2 games (IL); no-op in EL | Medium — 20 min of false line | **Done** |
| C | Ribbon cannot warn about an absent segment | All games | Medium — fault is silent | Code path identified; blocked on H database migration |
| D | Excluded segments leave undrawn holes | 19 team-games, 4675s | Medium | Investigate |
| E | Upstream feed defects (whole periods missing) | 3 games | High but **not ours** | Tracked, out of scope |
| F | Mid-quarter attribution holes | 10 games | Medium | Undiagnosed |
| G | Final lineup's bar stops short of the buzzer | 1 game | Low, cosmetic | Backlog |
| H | `subs` unreadable at request time (cold table) — prerequisite for C | schema-wide | Blocker for C | Code **done**, DB migration **not run** |

## How the ribbon gets its data

`ribbon_modal_server()` (`app/R/mod_ribbon_modal.R:61`) calls
`fetch_stint_ribbon()` (`app/R/global.R:411`), which runs `RIBBON_SQL_ISRAEL`
(`app/R/global.R:312`). That query has four parts:

- `segs` — one row per `(team_id, segment_id, lineup_hash)` from
  `df_pts_poss_lineups_longer_mv`, keeping `MAX(segment_seconds) > 0`.
- `lineup_players` — `lineups_lookup_on` grouped to a 5-player array.
  **A segment whose hash does not resolve to exactly five players produces no
  lane** (workstream D).
- `lanes` — `segs INNER JOIN lineup_players`, unnested to one row per player.
- `marg` — the scoring series for the clicked team.

Then in R: `ribbon_period_bounds(n_periods)` fixes the axis,
`ribbon_complete_margin()` closes the curve, `ribbon_normalise_lanes()` and
`ribbon_mark_starters()` shape the lanes, `build_stint_ribbon_svg()` draws.

Two facts about that pipeline drive most of this plan:

1. **The MV already excludes unattributed actions** (`WHERE lineup_hash IS NOT
   NULL`, `df_pts_poss_longer.sql:127`). An action with no lineup is not a
   NULL row the ribbon can detect — it is simply absent.
2. **A segment's end is the next segment's start**
   (`df_pts_poss_longer.sql:224`), ordered by `(segment_start_id, segment_id)`
   partitioned by `(game_id, team_id)`, with **no quarter partition**. So a
   period's closing segment silently absorbs the next period's unattributed
   opening.

## Guardrails — what NOT to do

- **Do not stop segments from crossing period boundaries.** Of 51 straddling
  segments, **45 are correct**: the team made no substitution in the overhang,
  so drawing one continuous bar is right. Partitioning segments by quarter
  would break 45 correct cases to fix 6.
- **Do not size the axis from observed data.** `ribbon_period_bounds()` already
  floors every game to regulation and extends only for OT. Its comment cites
  "one at 961" — game 184. Sizing from `max(elapsed)` would draw different
  games at different scales.
- **Do not patch the ribbon SQL to paper over missing attribution.** The gap is
  upstream in `lineups_lookup`; fixing it in the reader would make the ribbon
  disagree with every other tab reading the same MV.
- **Do not treat the two survey measures as one mechanism.** Game 184 has both
  a boundary straddle and a point deficit, and they are unrelated (see F).

---

## A. Period-opening attribution gap — PREREQUISITE, separate plan

**Owned by** `docs/plans/2026-09-19-period-opening-lineup-anchors-design.md`.
Design, root cause, acceptance gates, shadow run, reprocessing scope and
baselines all live there; do not copy them back here.

**What it means for the ribbon.** When one team does not substitute at a
period boundary, the previous period's closing segment runs on past the
boundary, and for the *other* team it draws the pre-substitution five. Game 404
Q4 shows the wrong five for 1:57. 6 of the 51 straddling segments are faulty
in this way (see Guardrails for why the other 45 must stay as they are). Once
the anchors land, lanes begin at the period boundary with no reader change.

**What this plan does when A lands:** re-measure the faulty-straddle count
(expected 6 -> 0), then C, D and F against the post-anchor data.

## B. Margin padded across periods with no data — DONE

The axis is correctly a constant 2400s, so a truncated game shows empty
**lanes** for periods it lacks. But `ribbon_complete_margin()` padded the
**curve** to that same full axis, so game 184 — last event at elapsed 961 —
drew a flat line from 961 to 2400: 24 minutes of confident straight line across
a half with no data, contradicting the empty lanes beside it.

**Fix.** Close the curve at the first period boundary at or after the last
recorded event, via a new optional `bounds` argument. Within a period that was
played, carrying the last score to the buzzer stays correct. Omitting `bounds`
preserves the old behaviour, so existing callers and tests are untouched.

| Game | Periods with data | Last event | Before | After | Removed |
|---|---:|---:|---:|---:|---|
| 184 | 2 | 961 | 2400 | 1200 | 20.0 min |
| 380 | 3 | 1754 | 2400 | 1800 | 10.0 min |

**449 of 451 games unchanged.** Files: `app/R/helpers.R` (`bounds` argument),
`app/R/global.R` (call site), 5 new tests in
`app/tests/testthat/test-stint-ribbon.R`. Both R files have mixed line endings
and were spliced on raw bytes with CR counts verified unchanged (2767 and
1034).

## C. The ribbon cannot warn about an absent segment

`ribbon_health_message()` (`helpers.R:4256`) is driven solely by
`excluded_segments`, which counts segments whose `lineup_hash` does not resolve
to five players. **For game 404 that count is 0**, so the app shows no warning
at all while drawing a false lineup.

This is structural: the bad interval is not an *excluded* segment but an
*absent* one that the neighbouring segment swallows. The reader cannot see what
the MV already filtered out.

**Proposed.** Add a second health signal computed from what the ribbon can now
see: for each period from Q2 on, a segment that starts before the boundary and
extends past it, **while that team has a substitution inside the overhang**, is
a false lineup.

**Code-unblocked 2026-09-19; operationally still blocked.** The substitution
evidence lives in `basketball_test.subs`, which used to be a cold-storage table — empty between
ETL runs, so the reader could never use it. The code now treats it as hot (see
CLAUDE.md "Cold Storage"), but the database promotion and historical backfill
have not run. Once those are complete, the ~79k-row table and its existing
`app_readonly` SELECT/RLS policy let the reader join `subs` directly. Until
then, C remains blocked in production.

Still sequenced after A, because A removes most of what there is to warn about;
the warning then covers the residue (game 211 and anything new).

## D. Excluded segments leave undrawn holes

The fault class the ribbon **does** warn about, but the volume is larger than
the warning implies: **19 team-games, 4675 seconds of lane time not drawn.**

| Game | Excluded segments | Seconds not drawn |
|---|---:|---:|
| 178 | 25 | 1460 |
| 370 | 13 | 1243 |
| 368 | 10 | 789 |
| 366 | 9 | 402 |
| 62452 | 6 | 524 |
| 363 | 3 | 154 |
| 62479 | 1 | 24 |

Game 178 loses over 24 minutes of lane coverage. There is an existing
investigation for it: `docs/game_178_invalid_q2_reset_2026-05-30.md`.

**Next step:** determine whether these hashes resolve to 4 or 6 players, and
whether the cause is the same lineup-state defect as A or something else. Do
this **after** A, since A changes which states exist.

## E. Upstream feed defects — tracked, not in scope

Four `(game, quarter)` pairs are entirely absent from the MV. **None is
anchor-fixable**; if the anchor appears to fix one, something is fabricating a
period.

| Game | Period(s) | What the raw feed holds |
|---|---|---|
| 184 | Q3, Q4 | **Zero** actions. Its two 10-minute quarters total exactly the official 59-70 with no clock resets — internally consistent but describing a 20-minute game. Cause unknown. |
| 406 | Q2 | **7** actions: a quarter marker plus 6 substitutions stamped at `00:01`/`00:00`, the period's *end* — the 398/399 misclocked-flurry signature. Q4 then carries 280 actions and 88 points, about double a normal quarter, while totals still reconcile to 99-79. Reads like Q2 gameplay labelled into Q4; **not established**, needs the raw JSON. |
| 380 | Q4 | The forfeit game (official 20-1). Only three quarters played. Legitimate. |

## F. Mid-quarter attribution holes — undiagnosed

Ten games lose points with **no** period-opening gap: 96, 139, 140, 141, 143,
184, 62500, 62504, 62584, 64893, deltas -1 to -5.

Game 184 is the worked example: its 5 lost points per team are actions
`1840321`-`1840340` at elapsed **961-995**, i.e. 03:59-03:25 into Q2 —
mid-quarter. It has both a harmless 66s boundary straddle that loses nothing
and a separate mid-quarter hole that loses ten points. The two measures
coinciding in one game was **coincidence, not mechanism**.

Distinct defect class. Needs its own investigation, after A.

## G. Final lineup's bar stops short of the buzzer — cosmetic

Game 160's lanes end at elapsed 2205 against a 2400 axis: the closing lineup's
bar stops 3:15 early, implying the five left the floor. The raw feed has only 2
actions after 2205 and **zero** scoring, and the game's points reconcile
exactly, so no data is lost. The last segment's end is
`game_end_elapsed_seconds` = the team's last *attributed* action, not the
buzzer.

Low priority. If fixed, extend the final segment to the axis end for the last
period **that has data** — the same rule workstream B applies to the margin.

## H. `subs` promoted out of cold storage — prerequisite for C

Workstream C needs to know whether a team substituted inside a straddle's
overhang. That evidence is `basketball_test.subs`, which was a cold-storage
table: exported to Parquet and TRUNCATEd after every ETL run, so it is **empty
whenever the app reads it**. C was therefore blocked on inventing a new
ETL-persisted flag.

`subs` is the smallest cold table (~9 MB, ~79k rows) against a **3237 MB**
database — the 500 MB free tier that justified cold storage has not bound for a
long time. Its `GRANT`, RLS policy and `app_readonly` SELECT already existed;
it was provisioned as if readable and simply always empty.

**Code (done).** `etl/cold_storage.R`: `subs` dropped from `COLD_TABLES`; FK
handling moved off a hardcoded `lineups_lookup` block onto a registry,
`HOT_FKS_INTO_COLD`, with `cold_fk_drop_sql()` / `cold_fk_readd_sql()` building
the statements Phase 7 runs around the TRUNCATE. Column order is load-bearing
and differs per constraint — `lineups_lookup` is `(game_id, id)`, `subs` is
`(id, game_id)` with `ON DELETE CASCADE`. `restore_cold_table()` now guards on
`names(COLD_TABLE_KEYS)` so the historical parquet stays loadable, while
`export_cold_table()` still guards on `COLD_TABLES` and correctly refuses
`subs`. Tests: `app/tests/testthat/test-cold-storage-fks.R`, 9 tests / 19
assertions.

**Database (NOT run).** `scripts/promote_subs_to_hot.R`, dry-run by default,
`CONFIRM_SUBS_PROMOTION=1` to apply. Order is forced by the live state:
`subs_actions_clean_fk` is currently **VALID** and `actions_clean` is empty
between runs, so restoring rows before relaxing the constraint would be
rejected row by row. The script does DROP -> restore -> re-ADD NOT VALID in one
transaction and refuses to run if `subs` is non-empty.

Known gap: the parquet holds 445 games against 451 ETL-processed, so 393, 397,
399, 400, 404 and 406 get their `subs` rows on the next ETL run for those games.
That is the same cold-storage durability issue now in CLAUDE.md's
Security/Resilience backlog.

**Architecture decision.** Keeping `subs` hot is approved as the pragmatic
short-term design: it is small, already secured for app reads, and avoids
inventing a second signal before the residual warning is understood. Workstream
C still requires all of the following:

1. Run the database promotion and complete the historical backfill before the
   app reader depends on the table.
2. Inspect the live indexes and add or confirm one matching the final ribbon
   predicate, expected to begin with `game_id`, `team_id` and period/action or
   clock fields. Validate the final query with `EXPLAIN (ANALYZE, BUFFERS)`;
   do not choose an index shape before the reader query is final.
3. Add an ETL assertion that Phase 7 leaves `subs` populated and that every
   processed game has its substitution rows available after cold cleanup.
4. Treat the `NOT VALID` foreign key into cold `actions_clean` as accepted
   operational debt. If repeated FK drop/re-add handling becomes fragile, or
   if more readers need this evidence, replace the raw hot table with a compact
   durable substitution-event fact containing only the reader fields and no
   dependency on the cold action table.

This does not block A: anchors repair attribution at the source. Hot `subs` is
for the residual diagnostic in C and remains sequenced after A.

## EuroLeague / EuroCup: audited, no equivalent defect

The EL ribbon shares the R half — `fetch_stint_ribbon()`, `ribbon_period_bounds()`,
`ribbon_complete_margin()`, `build_stint_ribbon_svg()` — but reads a different
SQL layer: `euroleague.ribbon_segments_v` over `matchup_segments_actions`
(`euroleague/sql/053_stint_ribbon_read_layer.sql`), not the Israeli stint MV. So
each defect had to be re-tested rather than assumed.

Measured over 589 EL games / 42,672 segments:

| Check | Result |
|---|---|
| Period-opening coverage at 600/1200/1800 | **1178 of 1178 team-periods covered, 0 gaps** |
| Segment contiguity | **0 holes, 0 overlaps**, 41,494 contiguous |
| Straddling segments | 442 across 195 games, **all with exactly 5 players** |
| Games with fewer than 4 periods | **0** |

**Workstream A does not exist in EuroLeague.** The Israeli bug needs an
unattributed window for a neighbouring segment to absorb; EL segment coverage is
complete and contiguous, with every period boundary covered, so there is no such
window. Its 442 straddles are the benign kind — the same five continuing across
a break.

**Workstream B already applies to EuroLeague** through the shared
`ribbon_complete_margin()`, and is correctly a **no-op** there: no EL game
reports fewer than 4 periods, and the 15 games whose last event is more than 60s
early (earliest 2268) all sit inside a played Q4, where padding to the buzzer
remains right.

**Workstream C** has nothing to warn about in EL.

No EuroLeague code change is required. This is a case where the CLAUDE.md
"EuroLeague tabs REUSE the Israeli implementation" rule is already satisfied by
the shared R layer, and the SQL layer that differs turns out to be the more
robust of the two.

## Sequencing

0. **Prerequisite — A** (anchors plan, its own sequence). Everything below
   that reads lineup states waits for it to land in production.
1. **Re-measure** the faulty-straddle count (expected 6 -> 0) against the
   post-anchor data.
2. **D**: re-measure excluded segments after A; investigate the residue.
3. **F**: investigate mid-quarter holes on the post-A data.
4. **H (database half)**: run `scripts/promote_subs_to_hot.R` with
   `CONFIRM_SUBS_PROMOTION=1`. Must precede C — C reads `subs` at request time.
   Verify after the next ETL run that Phase 7 left it populated.
5. **C**: add the second health signal once A has removed most of its subject
   and H has made `subs` readable.
6. **G**: cosmetic, any time.
7. **E**: raise separately — these are feed defects, not ribbon defects.

B and H's code half are done and independent of the rest. G and H's database
half do not depend on A and could be done while it is in progress.

## Regression baselines

Re-run before and after every step. These are the ribbon-facing measures only.
The period-gap and point-reconciliation baselines (31 affected periods, 22
mismatched team-games, 0 Q1 gaps) belong to the anchors plan and live in its
"Step 1 baseline" section, together with the queries. Its corrected period
enumeration matters here too: grouping `BY (game_id, quarter)` silently skips a
quarter with zero rows, which is how E was missed on the first pass.

| Measure | Baseline 2026-09-19 |
|---|---|
| Faulty straddles | 6 of 51 |
| Excluded segments | 67 segments, 19 team-games, 4675s |
| Games whose margin is padded across an unplayed period | 2 (now 0) |

## Test inventory

| File | Tests | Assertions | Covers |
|---|---:|---:|---|
| `test-cold-storage-fks.R` | 9 | 19 | H (registry, guards, SQL builders) |
| `test-stint-ribbon.R` | 151 | 442 | B and the ribbon's pure transforms |
| `test-stint-ribbon-readers.R` | 27 | 51 | reader contract |

Pre-existing suite failures, unrelated and unchanged by this work:
`test-companion-query-counts.R:293`, `test-idle-restore-bookmarking.R:169`,
`test-source-encoding.R:17` and `:21`.
