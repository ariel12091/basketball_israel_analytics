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
| D | Excluded segments leave undrawn holes | 36 segs / 2352s (2008s feed defects, 344s alias) | Medium | **Partly fixed 2026-09-20** — 23 games reprocessed; aliases 2052 + 2046 closed, 1982 blocked on 3 unfetchable cup games |
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

**Rollout correction, 2026-09-20: A is not finished.** The anchor rule and
its gates landed, but the historical reprocessing population was selected by
the period-opening *missing-attribution* measure. That measure cannot see a
straddle whose opening actions are present but carry the previous period's
five, so it omitted game 62452. Do not treat the merge or the first 18-game
reprocess as completion of A. Re-select the population using the
faulty-straddle predicate (a boundary-crossing segment plus a substitution for
that team inside the overhang), re-run the offline reprocess for that set, and
only then perform the post-A re-measurement and continue to C, D and F. See
`docs/ribbon_straddles_and_subs_handoff_2026-09-20.md` for the evidence and
known exclusions. A strict-inside re-audit corrected another preliminary
claim: game 100 is benign because its first Q2 substitutions occur exactly at
the previous segment's endpoint, so the carried five is valid throughout the
100-second window. Game 62452 is a real residual, but its Q4 opening is a
provider five-OUT/no-IN reset that the additive anchor rule cannot repair; it
must remain visible to C or be handled by a separate recovery path.

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

## C. The ribbon cannot warn about an absent segment — DONE

`ribbon_health_message()` (`helpers.R:4256`) is driven solely by
`excluded_segments`, which counts segments whose `lineup_hash` does not resolve
to five players. **For game 404 that count is 0**, so the app shows no warning
at all while drawing a false lineup.

This is structural: the bad interval is not an *excluded* segment but an
*absent* one that the neighbouring segment swallows. The reader cannot see what
the MV already filtered out.

**Implemented 2026-09-20.** A second health signal is computed from what the ribbon can now
see: for each period from Q2 on, a segment that starts before the boundary and
extends past it, **while that team has a substitution inside the overhang**, is
a false lineup. `RIBBON_SQL_ISRAEL` reads the hot `subs` table for the requested
game, applies the strict-inside predicate (`sub.elapsed < segment_end`), and
returns both the number of intervals and their total overhang seconds.
`ribbon_health_message()` combines that signal with the existing excluded-
segment warning. EuroLeague returns zeroes for the same reader contract because
its period-opening lineups are already explicit.

**UNBLOCKED 2026-09-20.** The substitution evidence lives in
`basketball_test.subs`, which used to be a cold-storage table — empty between
ETL runs, so the reader could never use it. Both halves are now done: the code
treats it as hot, and the database promotion and historical backfill ran on
2026-09-20 (section H). The table holds 79,416 rows across 448 games and its
existing `app_readonly` SELECT/RLS policy lets the reader join `subs` directly.
C is implemented and verified now.

Sequence it after the re-selected reprocess (sequencing step 1), not merely
after "A", because A's first pass left a residue — the warning should cover
what remains rather than the bulk. Measure the overhang duration in the signal:
the 2026-09-20 straddle pass found 10 of 17 flagged cases with overhangs under
3 seconds, which are boundary substitution flurries, not visible false lineups.
Live behavioral checks return game 100 = 0 intervals / 0s, game 62452 = 1 / 72s,
game 406 = 2 / 736s, and game 62534 = 2 / 6s. The representative game-62452
query executed in 58 ms. See `scripts/verify_ribbon_false_straddle_health.R`.

## D. Excluded segments leave undrawn holes — PARTLY FIXED 2026-09-20

The fault class the ribbon **does** warn about. Measured after Workstream A
with `scripts/audit_ribbon_excluded_segments.R`, which reproduces the reader's
own valid-five guard and reconciles against the reader's verbatim predicate
game by game.

| | Segments | Team-games | Games | Seconds |
|---|---:|---:|---:|---:|
| Measured 2026-09-20 (pre-fix) | 67 | 8 | 7 | 4596 |
| **After the D1 reprocess** | **36** | **6** | **5** | **2159** |

The pre-fix per-game table was byte-for-byte the one recorded on 2026-09-19, so
the anchor work neither fixed nor widened this class. The reduction below came
from canonicalising four games (see D1), not from the ribbon code.

| Game | Excluded segments | Seconds not drawn | State |
|---|---:|---:|---|
| 178 | 25 | 1460 | feed defect, see D2 |
| 62452 | 6 | 524 | feed defect, see D3 |
| 172 | 3 | 90 | alias residue, see D1 |
| 187 | 1 | 61 | alias residue, see D1 |
| 62479 | 1 | 24 | feed defect, see D2 |
| ~~370~~ | ~~13~~ | ~~1243~~ | **fixed** |
| ~~368~~ | ~~10~~ | ~~789~~ | **fixed** |
| ~~366~~ | ~~9~~ | ~~402~~ | **fixed** |
| ~~363~~ | ~~3~~ | ~~154~~ | **fixed** |


> **Update 2026-09-20, after a second reprocess.** Alias 2052 is now fully
> closed (0 games), and the holes in 172/187 are gone — but reprocessing game
> 210 orphaned game **206** (team 6, alias 1982) for 4 segments / 344s, so the
> ribbon total moved from 2159s to **2352s**: net 193 seconds worse. A partial
> scope relocates the defect rather than shrinking it. Closing 1982 (18 games)
> and 2046 (10 games) should leave only the three feed defects, 32 segs /
> 2008s. Full record in `docs/ribbon_workstream_d_handoff_2026-09-20.md` § 9.
>
> **Update 2: 23 games reprocessed in total.** Aliases **2052** and **2046**
> are now fully closed (0 games each). Alias **1982** is **blocked**: 3 of its
> 18 games (294, 304, 309) are State Cup games with an empty `pbp_link` and
> cannot be re-fetched, and reprocessing only the 15 fetchable ones was
> measured to orphan 15 segments / 1204s in games 304 and 309 against 344s
> fixed — net 860s worse, so it was not run. Seasons 2025 and 2027 carry no
> alias residue at all. Details and the exposure-gate query:
> `docs/ribbon_workstream_d_handoff_2026-09-20.md` §10-12.
### The headline baseline was a different measure, not a regression

"19 team-games, 4675s" never agreed with this section's own per-game table,
which sums to 8 team-games and 4596s. The audit's variant section reproduces
both; the difference is the `has_gameplay` filter:

| Variant | Segments | Team-games | Seconds |
|---|---:|---:|---:|
| reader (gameplay, seconds > 0) | 67 | 8 | 4596 |
| gameplay, any seconds | 68 | 8 | 4596 |
| any segment, seconds > 0 | 104 | 19 | 4675 |
| any segment, any seconds | 129 | 20 | 4675 |

The extra segments are substitution-only. The ribbon draws no lane time for
them, so they are not holes. **Quote 67 / 8 / 4596**; the old headline counted
a population the reader never had to draw.

### Cause classes — the question this workstream asked

`lineups_lookup_on` is derived: ETL Phase 3 copies the `is_on_verdict = 1`
rows out of `lineups_lookup`. Reading both counts separates a feed defect from
a join that cannot resolve.

| Cause | Segments | Seconds | Where |
|---|---:|---:|---|
| Alias id-space mismatch — nothing lost, join cannot resolve | 35 | 2588 | 363, 366, 368, 370, all team 9 |
| Source over-attribution — 6 or 7 on the floor | 26 | 1484 | 178 (25 segs), 62479 (1) |
| Source under-attribution — 2 or 3 on the floor | 6 | 524 | 62452, team 11 |

**No hash resolves to four players.** Outside the alias class the derived count
equals the source count exactly, so `lineups_lookup_on` is a faithful copy
everywhere it copied anything.

### D1. Alias id-space mismatch — FIXED for four games 2026-09-20

**Applied.** Canonicalisation is part of ETL, but only at *ingest*: Phase 2
loads the active aliases (`etl/etl_full.R:320`) and applies them per game via
`canonicalize_actions_player_ids()` / `canonicalize_roster_player_ids()` /
`canonicalize_starter_player_ids()`. ETL never revisits a processed game, so a
game ingested before its alias existed stays raw forever. These four were
ingested 2026-06-02 to 06-17; the 2052 alias was created **2026-06-18**. Game
372 (06-22) came out canonical, which is the whole difference.

The repo already owns the retroactive fix, so no new script was written:

```r
Sys.setenv(APP_ENV = "test")
source("etl/backfill_player_id_aliases.R")
backfill_player_id_aliases(dry_run = FALSE, seed_defaults = FALSE,
                           game_ids = c(363L, 366L, 368L, 370L))
```

It re-runs `etl_full()` for exactly those games — a transactional per-game
snapshot replace (DELETE + reinsert of `pws`, `stints`, `lineups_lookup`,
`possessions`, `subs`, `actions_clean`, `full_rosters`), so there is no
raw/canonical duplication. Ran 455.6s, `pipeline_ok = TRUE`, all four
validated and published, no phase failures.

**Verified before and after:**

| Invariant | Result |
|---|---|
| Final scores (96-70, 91-84, 88-102, 65-76) | unchanged |
| Team minutes from segments | unchanged (39.40 / 39.92 / 39.78 / 39.43) |
| Distinct players per team-game | unchanged (23 / 19 / 22 / 21) |
| Alias ids 1982/2046/2052 in `lineups_lookup` + `full_rosters` | 105 rows -> **0** |
| Ribbon excluded segments in these games | 35 / 2588s -> **0** |

Two deliberate changes, both benign and both worth knowing:

- **Segments fell by 3 per team-game** (MV rows by ~114 per game). Those are
  exactly the zero-second segments sitting on the Q1/Q2, Q2/Q3, Q3/Q4
  boundaries: every reprocessed game now has **0** of them, every
  non-reprocessed game in the season still has **6** (3 boundaries x 2 teams).
  They carried no seconds, which is why minutes are unchanged. This is current
  pipeline semantics (the 2026-09-12 monotonic-clock migration and the
  period-anchor work) landing on games last processed in June.
- **`etl_processed_games.processed_at` is NOT bumped by a reprocess** (363
  still reads 2026-06-02). It marks first ingest, not last processing. That is
  why game 365 looked June-processed yet held canonical ids — it had been
  reprocessed too.

**Residue: the scoped fix orphaned two neighbours.**
`cleanup_player_alias_lineup_derivatives()` deletes raw-hash rows from
`lineups_lookup_on` / `sub_lineups` at **season** scope `(team_id, game_year,
lineup_hash)`, not per game. Games 172 and 187 share two of the raw hashes and
were not reprocessed, so their MV now points at `_on` rows that no longer
exist: **4 new excluded segments, 151s**, same alias class. The audit's own
classifier catches them.

That is also the mechanism behind the original defect: an earlier partial run
cleaned the derivatives for 363/366/368/370 without reprocessing them, which is
why the source stayed raw while the derived side went canonical.

**A partial scope is therefore not a stable state.** To close 2052 entirely,
reprocess its remaining games: **166, 172, 182, 187, 197, 203, 210, 218, 225**.
Two other aliases are untouched and have the same latent behaviour: **1982**
(team 6, 19 games) and **2046** (team 13, 11 games) — 37 distinct games in all.

---

#### Why it happened (retained for reference)

The 2588 seconds lost in games 363/366/368/370 are neither a feed defect nor
lost data. Player id **2052 is not canonical**: `player_identity_map` row 493
(provider `segev`, season scope, game_year 2026, team 9, active, created
2026-06-18) maps source id 2052 to **canonical 1110**, NOAM AVIVI — "cross-team
season re-mint: id 2052 (Bnei Herzliya) is the same person as canonical 1110
(Galil Elion)".

A `lineup_hash` is computed from the player-id list, so the same five people
hash differently in the two id spaces, and the alias was applied to the two
sides at different times:

| Relation | id carried for games 363-370 | Consequence |
|---|---|---|
| `lineups_lookup` | raw `2052` | hash `b3f70bca…` |
| `full_rosters` | raw `2052` | — |
| `df_pts_poss_lineups_longer_mv` | raw `2052` hash, 35 segments | what the ribbon reads |
| `lineups_lookup_on` | canonical `1110` | hash `ead530af…` |
| `sub_lineups` | canonical `1110` | 250 rows, 10 hashes |

The ribbon joins the MV's raw hash to `lineups_lookup_on`'s canonical hash, and
misses. Nothing is absent.

Established, read-only:

- **All 10** missing hashes have an exact twin in `lineups_lookup_on` once 2052
  is rewritten to 1110 — same five people, different hash. A first pass
  compared the raw player sets only and wrongly reported "no set appears under
  another hash"; comparing *canonicalised* sets finds all ten.
- Those 10 canonical hashes hold **250 rows in `sub_lineups`**. An earlier note
  in this plan said they reach `sub_lineups` as 0 rows and that Tab 2 is
  therefore missing these lineups. **That was wrong and is retracted** — it
  queried the raw hashes. Tab 2 has them, under the canonical hash.
- The MV carries the raw hash for the 4 affected games (35 segments) and the
  **canonical** hash for games 365 and 372 (28 segments, which resolve fine).
  `full_rosters` splits the same way: 2052 up to game 370, 1110 for 365 and
  372. So the alias applies forward from roughly its 2026-06-18 creation.
- Games 172, 187 and 225 also carry raw 2052 and are **not** affected, because
  their `lineups_lookup_on` rows are raw too. The retroactive backfill
  (`etl/backfill_player_id_aliases.R`, scoped by
  `affected_player_alias_game_ids()`) canonicalised `lineups_lookup_on` and
  `sub_lineups` for some games and left the source side alone. The 35 excluded
  segments are exactly where the two sides disagree.
- Those 10 remain the only five-player `is_on_verdict = 1` hashes anywhere in
  the database with no `lineups_lookup_on` row, so today this mismatch is
  bounded to one player in one season.

**Do NOT "repair" this by inserting the raw hashes into `lineups_lookup_on`.**
An earlier draft of this section proposed exactly that (10 x 5 = 50 rows). It
would register the same five people twice under two hashes and corrupt every
lineup count Tab 2 derives. The fix is to make the two id spaces agree —
either extend the alias rewrite to `lineups_lookup` (and the MVs built from it)
for games 363/366/368/370, or roll `lineups_lookup_on` and `sub_lineups` back
to raw for those games. Both are writes, neither is in this workstream.

**Latent scope.** 2026 alone has 392 active identity maps over 350 source ids
(2025: 296, 2027: 209). Any future scoped alias backfill that rewrites the
derived side without the source side reproduces this exactly. A standing check
belongs in the data-quality report: count MV hashes that fail to resolve in
`lineups_lookup_on` but do resolve after applying the active alias map.

### D2. Source over-attribution — 6 or 7 players on the floor

Game 178 (25 segments, 1460s, both teams) and game 62479 team 7 (1 segment,
24s). `lineups_lookup` itself records six or seven players as on, so there is
nothing to propagate correctly. Game 178 has an existing investigation:
`docs/game_178_invalid_q2_reset_2026-05-30.md`. Not anchor-fixable; the ribbon
warning is behaving correctly by refusing to draw a six-player five.

### D3. Source under-attribution — 2 or 3 players on the floor

Game 62452 team 11, 6 segments, 524s. This is the Q4 defect already recorded in
`docs/ribbon_straddles_and_subs_handoff_2026-09-20.md`: the feed declares five
OUT substitutions at 10:00 with no matching incoming five, and later states
carry only two or three players. Confirmed here from the source side — the
derived count matches the source count, so again nothing was lost in
propagation.

**Next step:** D1 is an alias-consistency decision, not a ribbon fix, and it
belongs with the player-identity work rather than here. D2 and D3 are feed
defects and stay with workstream E's tracking. Nothing in D now blocks the
ribbon beyond the warning it already emits.

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

> **APPLIED 2026-09-20.** `basketball_test.subs` now holds **79,416 rows across
> 448 games** (was 418 rows / 3 games). `subs_pkey` valid,
> `subs_actions_clean_fk` present and NOT VALID as designed, `app_readonly`
> SELECT verified. **This unblocks C.**
>
> `scripts/promote_subs_to_hot.R` needed three fixes before it would run, and
> was itself untracked until `f72ebd2`:
> 1. It refused outright — `if (hot_now > 0) stop(...)`. Written when `subs`
>    was empty, but `subs` stopped being truncated at promotion, so Phase 2 had
>    been writing rows ever since.
> 2. A blanket restore would have failed anyway: `subs` has
>    `PRIMARY KEY (game_id, id)` and 240 of the 418 rows were already in the
>    archive, so `restore_cold_table()`'s unconditional append hits key
>    violations. It now loads only the missing keys.
> 3. The key comparison matched nothing, because the frame above it is a
>    `col_select = "game_id"` projection — `pq$id` was NULL and every key
>    became `"401|"`. Fixed by re-reading the full frame.
>
> The first apply also died on a `dbWriteTable` dispatch error; the transaction
> rolled back cleanly and the FK and row count were verified intact before
> retrying. **Still to verify:** that Phase 7 leaves `subs` populated after the
> next nightly run.

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

**Updated 2026-09-20.** Two steps are done and the remaining order changed,
because the anchors' reprocessing turned out to be incomplete.

0. ~~**Prerequisite — A**~~ — the anchor rule, its six gates and the 401/404
   acceptance **merged 2026-09-20** (`3894bbc`). **But A is NOT finished:** the
   games it reprocessed were selected by the period-opening gap, which cannot
   see the wrong-five class. The strict-inside re-audit found one omitted real
   case, game 62452, which the anchor rule cannot repair. See section A.
1. **Re-select the reprocessing population by the straddle measure** and re-run
   the audit over it. This is done: game 100 was an endpoint false positive;
   game 62452 is the only omitted real case and is not repairable by anchors.
   It passes to C/the future recovery path rather than being reprocessed.
2. ~~**H (database half)**~~ — **done 2026-09-20**: `subs` holds 79,416 rows
   across 448 games, constraints intact, `app_readonly` SELECT verified.
   **Still to check:** that Phase 7 leaves it populated after the next nightly.
3. ~~**C**~~ — **done 2026-09-20**: the second health signal reports strict-
   inside substitution straddles and total overhang duration. Live verification
   covers games 100, 406, 62452 and 62534.
4. **D**: re-measure excluded segments; investigate the residue.
5. **F**: investigate mid-quarter holes on the post-anchor data.
6. **G**: cosmetic, any time.
7. **E**: raise separately — these are feed defects, not ribbon defects.

B and H are done. G depends on nothing here.

## Regression baselines

Re-run before and after every step. These are the ribbon-facing measures only.
The period-gap and point-reconciliation baselines (31 affected periods, 22
mismatched team-games, 0 Q1 gaps) belong to the anchors plan and live in its
"Step 1 baseline" section, together with the queries. Its corrected period
enumeration matters here too: grouping `BY (game_id, quarter)` silently skips a
quarter with zero rows, which is how E was missed on the first pass.

| Measure | Baseline 2026-09-19 | Re-measured 2026-09-20 (post-anchor) |
|---|---|---|
| Faulty straddles | 6 of 51 | 17 of 49 flagged — **not comparable**, see note |
| Excluded segments | 67 segments, 19 team-games, 4675s (a looser measure — see D) | 67/8/4596 -> **36/5/2352** after 23 games were reprocessed (see handoff §8-11) |
| Games whose margin is padded across an unplayed period | 2 (now 0) | unchanged |

**On the straddle re-measure.** The 2026-09-19 query is not recorded here, so
the 2026-09-20 figure was reconstructed from workstream C's prose and is not
proven to be the same measure. The 17 are also not one population: 10 have
overhangs under 3 seconds (boundary substitution flurries, an invisible false
window), 4 belong to games excluded by decision (406's relabelling, 211's
action-id overlap), and the substantive residue was initially reported as two
games. That reconstruction used an inclusive endpoint and was not the intended
measure: a substitution exactly at the segment endpoint ends that segment; it
does not make the carried lineup wrong beforehand. The reproducible
strict-inside audit now lives in
`scripts/audit_period_anchor_reprocess_population.R`. It finds five regulation
cases: two in excluded game 406, two three-second cases in already-reprocessed
game 62534, and one substantive omitted residual in game 62452. Game 100 drops
out because both Q2 substitutions occur exactly at the 100-second endpoint.

## Test inventory

| File | Tests | Assertions | Covers |
|---|---:|---:|---|
| `test-cold-storage-fks.R` | 9 | 19 | H (registry, guards, SQL builders) |
| `test-stint-ribbon.R` | 151 | 442 | B and the ribbon's pure transforms |
| `test-stint-ribbon-readers.R` | 31 | 67 | reader contract and C health signal |

Pre-existing suite failures, unrelated and unchanged by this work:
`test-companion-query-counts.R:293`, `test-idle-restore-bookmarking.R:169`,
`test-source-encoding.R:17` and `:21`.
