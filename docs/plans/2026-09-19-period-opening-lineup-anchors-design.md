# Period-Opening Lineup Anchors Design

Date: 2026-09-19

A standalone ETL data-correctness fix. The missing period openings are absent
from `df_pts_poss_lineups_longer_mv` itself, so on/off, lineups, minutes, team
points and the Game Flow ribbon are all wrong for those windows; the ribbon is
only where the defect is most visible. This document owns everything about the
fix: design, gates, shadow run, reprocessing and baselines.

`docs/plans/2026-09-19-stint-ribbon-correctness-plan.md` lists this work as a
**prerequisite** and depends on it (its C, D and F are re-measured after this
lands). It does not own any of the detail below.

Supersedes the "Required fix direction" section of
`docs/gameflow_lineup_boundary_handoff_2026-09-19.md`, whose diagnosis of the
mechanism is corrected below.

## Objective

Give every team a lineup state at the opening of every period from Q2 onward,
seeded from that team's carried-forward state, so that stint construction can
attribute actions from the period boundary rather than from the later of the
two teams' first substitutions.

The anchor runtime path has not yet been wired. The pure helper and fixtures
exist.

The margin-curve fix that was first written up here is a ribbon-reader change,
not part of this fix; it now lives in the ribbon plan (its workstream B, done).

## Implementation-review amendments (2026-09-19)

The following are required before this design is considered complete:

1. **Make the rollout scope match the acceptance gate.** Reprocessing only
   games 401, 404 and 406 can validate those three games, but it cannot reduce
   the production-wide present-but-late count from 27 to at most 1. Either use
   a three-game acceptance gate first and then reprocess every affected game,
   or reprocess the complete affected set before applying the global gate. The
   affected ids must be regenerated from the baseline query rather than copied
   from a stale artefact. Check the regenerated set against cold-storage
   coverage: the offline re-derivation path can only rebuild games whose
   actions are in the Parquet snapshot (see Reprocessing).
2. **Define "additive only" semantically, not as a raw row diff.** Adding an
   opening stint can renumber every later `segment_id` because
   `compute_stints()` assigns a game-wide `DENSE_RANK()` over stint boundaries.
   Compare existing actions by stable fields such as action id, quarter,
   offense/defense teams and lineup hashes. Surrogate-id renumbering is
   expected; changing an existing action's lineup assignment is not.
   (`DENSE_RANK() OVER (PARTITION BY game_id ORDER BY final_start_id,
   final_end_id)`, `etl/etl_onoff.R:1032`, verified.)
3. **Enumerate expected OT periods independently of the target MV.** The
   current survey derives expected OT periods from `present`, so an OT period
   with raw actions but zero attributed MV rows is invisible. Use raw/cold
   actions or another authoritative period inventory. If it is unavailable,
   report the OT absence check as not evaluated rather than zero failures.
   No OT count was found in `schedule`. The natural home is inside the ETL,
   where `actions_clean` still holds the game being processed and so gives a
   complete period inventory without depending on the Parquet snapshot.
4. **Add pure-helper/SQL parity coverage.** After implementing the dbplyr
   `UNION ALL`, run the helper and SQL paths against the same fixture and assert
   identical anchor ids, teams, periods, clocks, NULL payloads and
   substitution-team skip behavior. Parity must be proven on **Postgres**, the
   only engine this SQL runs on — a different backend would not exercise the
   `UNION ALL` NULL typing or dbplyr's Postgres translation. Two layers:
   (a) a DB-gated test that feeds the fixture through `dbplyr::copy_inline()`
   (a `VALUES` subquery: no temp tables, which misbehave on the pooled ports,
   and read-only, so `app_readonly` suffices), skipped without credentials;
   (b) a runtime check in the ETL on every game, comparing the anchors the SQL
   retained against the pure helper run on that game's in-memory `actions_df`,
   failing the game on mismatch. Assert the explicit per-team `team_id`: a
   team-less anchor joins to no roster row and silently no-ops.
5. **Gate 1 degrades, it does not reject.** Resolved 2026-09-19. The base load
   is one transaction per game (`etl/etl_full.R:479`), and an error there rolls
   the whole game back ("base load FAILED"), leaving it with no rows at all.
   Hard-rejecting a malformed four- or six-player carry-forward would therefore
   turn a game that loads today with a few seconds of opening gap into a game
   with no data — worse than the defect being fixed. Instead, drop the
   offending anchor rows, log them, and let that period load exactly as it
   does today. Automatic regulation-period recovery via the OT module is
   separate, later work (see Fallback); nothing in this release implies it.
6. **Check `quarter_time` text agreement, not just the clock.** The
   `slice_max` de-duplication groups by the `quarter_time` **string** as well as
   `end_game_seconds_remaining`. Gate 4 only checks the numeric clock. If the
   period-marker row renders its clock differently from the boundary
   substitution rows (e.g. `10:00` vs `10:00.0`), the anchor is not superseded
   and that team gets two states at one instant. Assert in the shadow diff
   that, for every period where a team substitutes at the opening clock, the
   anchor's `quarter_time` equals the substitutions'.

## Current Failure Mode

### What the handoff established

Valid period-opening actions receive no lineup hash, so they are dropped by
`df_pts_poss_longer.sql:127` (`WHERE lineup_hash IS NOT NULL`) and are invisible
to every on/off, lineup, minutes and Game Flow view. Verified intervals:

| Game | Period | Scoring actions before first attributed action | First attributed action |
|---|---:|---|---:|
| 401 | Q4 | `4010683`, `4010686`, `4010688` | `4010698` |
| 404 | Q2 | `4040230` | `4040232` |
| 404 | Q4 | `4040629`, `4040637`, `4040662` | `4040670` |
| 406 | Q3 | `4060250` | `4060258` |

### Correction to the stated mechanism

The handoff attributes the loss to `compute_stints()`:

```r
final_start_id = pmax(start_id_offense, start_id_defense)
```

That is the symptom, not the cause. The cause is one level down, in
`compute_lineups_lookup()` (`etl/etl_onoff.R:919`):

```r
full_rosters |> ... |> inner_join(subs, by = c("team_id", "game_id"))
```

The row set is the roster joined to **substitution actions only**. A lineup
state is therefore materialised only at a substitution action id.
`fill(is_on, .direction = "down")` (`etl/etl_onoff.R:938`) is partitioned by
`(game_id, player_id)` and does carry on-court state across period boundaries
correctly — but with no row, there is no state.

Consequently a team that does not substitute at a period boundary has **no
opening segment for that period at all**. Its first row of the period is its
first substitution of the period. `pmax` then starts the shared stint there.

This reverses the reading of game 404 Q4. The Lofton Jr. (`2278`) entry at
`4040623` and the `1135` exit at `4040624`, both at `10:00`, *do* produce
lineup states for their team. The deficit comes from the **opponent**, whose
first Q4 substitution lands at roughly `08:03` (`4040670`).

The distinction matters for two reasons:

1. It determines which fix works. Anything that only repairs the team with
   boundary substitutions repairs the wrong team.
2. The affected population is every period boundary at which one team does not
   substitute — which the survey below measures at 2.0% of periods, not the
   majority. The four verified intervals are the subset where a scoring action
   fell inside the gap.

### What is not broken

The trailing edge of each period is sound. `end_segment` and `end_id` use
`lead()` within `(game_id, team_id)`, so a period's final segment extends
forward to the team's next state, and `quarter_offense == quarter_defense` in
`compute_stints()` prevents that segment from pairing across the boundary.
Only the leading edge loses coverage.

### What the user sees: a false lineup, not a gap

The Game Flow ribbon does not draw a hole over the unattributed window. It
draws the *previous* lineup across it. `df_pts_poss_longer.sql` sets

```sql
segment_end_elapsed_seconds = coalesce(next_segment_start_elapsed_seconds,
                                       game_end_elapsed_seconds)
```

ordered by `(segment_start_id, segment_id)` partitioned by
`(game_id, team_id)` — with **no quarter partition**. So the period's closing
segment simply absorbs the opening window of the next period.

Game 404 Q4: segment 31 runs elapsed 1727 -> 1917, crossing the boundary at
1800 and continuing 117s (1:57) into Q4. For Hapoel Jerusalem, who made six
substitutions at the buzzer, the ribbon draws **the wrong five** for those
1:57 — Huber (1049), Hanochi (1135) and Roddy (2279) are shown on court after
being subbed off, while Burg (2230), Smith (2276) and Lofton Jr. (2278) are
shown on the bench after entering. For Beer Sheva/Dimona, who made no
substitutions there, the same segment is **correct**.

**Crossing a period boundary is not itself a defect — do not try to stop it.**
Measured across all games: 51 straddling segments, of which **45 are correct**
(no substitution by that team inside the overhang) and only **6 are faulty**:

| Game | Team | Boundary | Overhang | Subs missed |
|---|---|---|---:|---:|
| 406 | 14 | Q1->Q2 | 668s | 10 |
| 404 | 4 | Q3->Q4 | 117s | 6 |
| 62452 | 11 | Q3->Q4 | 72s | 5 |
| 404 | 11 | Q1->Q2 | 68s | 2 |
| 406 | 14 | Q2->Q3 | 68s | 7 |
| 401 | 33 | Q3->Q4 | 52s | 2 |

A correct straddle still loses the window's *actions*, even though the drawn
lineup is right, so points and possessions go missing where the picture does
not. Anchoring fixes both at source: the next period gets its own segment
starting at the boundary, which caps the previous one there.

The ribbon's health warning cannot see this defect (for game 404 it reports 0
excluded segments). Adding a warning for it is the ribbon plan's workstream C.

## Proposed Fix: Anchor Rows

Add a period-opening anchor to the row set consumed by
`compute_lineups_lookup()`. The anchor set is, for each `(game_id, quarter)`
with `quarter >= 2`, the first `actions_clean` row of that period, carrying:

- `id`, `quarter`, `quarter_time`, `end_game_seconds_remaining` from that row;
- `player_id`, `parameters_player_in` and `parameters_player_out` NULL;
- **one row per team**, naming the team explicitly.

`UNION ALL` it into `subs` before the roster join.

**Corrected 2026-09-19.** An earlier draft of this section proposed a single
team-less anchor, on the reasoning that a NULL `team_id` would replicate to
both teams across the roster join. That is wrong: the join is
`inner_join(full_rosters, subs, by = c("team_id", "game_id"))`, which is
`ON rosters.team_id = subs.team_id`, and NULL equals nothing — a team-less
anchor would join to **no roster row at all** and the fix would be a silent
no-op. The anchor must name its team, so it is emitted once per team.

One exception, which real data forced: **skip the anchor for a team that
already has a substitution at the anchor id.** In 17 of 1355 real periods (445-game local snapshot) the
period's lowest-id action is itself a substitution. Anchoring that team again
at the same id would give one of its players two states at a single instant,
and the payload-free anchor could win the `slice_max` tie-break and blank the
substitution. The other team still needs its anchor there, and gets it.

### Why this produces correct segments with no other change

- **Seeding is free.** `is_on` is NA at the anchor and is filled down from the
  player's previous substitution, so the anchor state *is* the carried-forward
  closing lineup of the previous period. No separate seed lookup is needed.
- **Provider substitution timing is preserved exactly.** The existing
  de-duplication is
  `slice_max(order_by = .ord, n = 1, by = c(game_id, quarter, quarter_time, end_game_seconds_remaining, player_id, team_id))`
  (`etl/etl_onoff.R:941`), where `.ord` is a row number over
  `window_order(id, ...)`. For a team with `10:00` substitutions, the anchor
  and the substitutions share a clock; the substitution has the higher `id`,
  therefore the higher `.ord`, and wins. The retained row's `id` is the
  substitution's. Lofton Jr.'s `10:00` entrance keeps its exact timing and is
  not overwritten by the anchor.
- **The team without boundary substitutions gains a segment.** Its anchor is
  the only row it has at that clock, so its segment starts at the period's
  first action.
- **The shared stint then opens at the boundary.**
  `pmax(start_id_offense, start_id_defense)` for game 404 Q4 becomes
  `4040624`, which precedes the first scoring action `4040629`.
- **Nothing is backfilled.** The `09:36` and `08:49` substitutions remain
  separate states and therefore separate segments, satisfying the handoff's
  explicit requirement that the first later segment must not simply be extended
  backward.

## Scope

In scope:

- `quarter` 2 to 4. Regulation period openings only.
- `compute_lineups_lookup()` only. `compute_stints()`, `pws` construction,
  `df_pts_poss_longer.sql` and every app reader are unchanged.
- Israeli league only. EuroLeague has no equivalent defect: across 589 games
  its segment coverage is complete and contiguous, with 1178 of 1178
  team-period openings covered (audit in the ribbon plan's EuroLeague
  section). No EuroLeague change.

Out of scope, deliberately:

- **Overtime (`quarter >= 5`).** Reversed 2026-09-20; this section previously
  scoped OT in, on the reasoning that "an OT anchor is a cheaper path to the
  same result the existing recovery module produces by simulation." It is a
  cheaper path to the same *lineup*, but not to the same *guarantee*, and it
  removes the guarantee as a side effect.

  `detect_ot_leading_lineup_gaps()` (`etl/ot_lineup_recovery.R:604`) reports a
  period only when the first OT gameplay action carries a NULL `segment_id` /
  `lineup_hash_offense` / `lineup_hash_defense`. `etl/etl_full.R` stages
  `lineups_lookup` at line 564 and builds the provisional PWS at line 573, so
  an OT anchor closes that condition *before* the detector reads it: the
  detector returns zero rows, `recover_ot_lineup_periods()` never runs, and
  with it go the event replay, the `recovery$audit` trail and the
  reject-the-game path at `etl_full.R:652`. Gate 1 cannot substitute for it —
  it checks `n_on == 5`, which a carry-forward satisfies by construction. What
  is lost is detection of a provider-omitted OT substitution: the anchor loads
  a five that looks healthy and is wrong.

  This is not hypothetical, and OT is not an unserved gap. The module is live
  and fired in production on **game 401 Q5** on 2026-09-16 and 2026-09-17
  (`etl/logs/reports/`): both teams `accepted_carry_forward`, `unexplained=0`,
  87 reconstructed rows staged, final coverage passed on 62 action rows. Game
  401 is also one of the three shadow games, so an OT anchor would have made
  that game's shadow diff non-additive by suppressing those 87 rows.

  OT therefore stays with `etl/ot_lineup_recovery.R`. `PERIOD_ANCHOR_MAX_QUARTER`
  in `etl/period_opening_anchors.R` carries the reason; both the pure helper and
  the dbplyr twin honour it, verified against Postgres.
- **Q1.** There is no previous period to carry forward, so a fill-down from
  nothing yields `n_on = 0` and a meaningless `lineup_hash` at the game's first
  action. Q1 already works, because the provider declares the starting five as
  substitution rows at `10:00`. Seeding Q1 from `full_rosters.starter` is a
  different seed source with a different failure mode and belongs in separate
  work; bundling it would also destroy the additive-only shadow diff described
  under Verification.
- Repairing isolated null-lineup rows in the middle of a period.
- Any change to how a provider-derived state is computed. Anchors are purely
  additive rows.

## Acceptance Gates

**Revised 2026-09-20.** Gates 1 and 4 degrade, Gate 5 reports, and Gates 2, 3
and 6 fail the game's ETL transaction — which is already open at this point in
`etl/etl_full.R`. The dividing line is what the failure implicates: a defect in
the provider's data costs that period its anchors, a defect in the anchor rule
or the engine costs the game.

**Gate order is load-bearing, corrected 2026-09-20.** The reject gates run on
the frame exactly as collected, ahead of every R-side drop. Both conditions
they test were created in SQL — a wrong row from the `UNION ALL`, a doubled
`lineup_id` from the `string_agg` window — so removing an anchor row in R
undoes neither; it only removes the evidence. Gate 4 ran first in the initial
implementation and silenced both for any period it degraded: a misclocked
period that *also* collided returned normally and wrote the corrupted provider
row, and a misclocked period whose anchor the helper would never have chosen
passed parity. Reproduced both ways before the fix. The coverage gate is the
one that legitimately runs after Gate 4, and it is told which periods Gate 4
already handled.

1. **Degrade, don't reject.** Every anchor row should resolve to `n_on = 5` for
   its `(game_id, team_id, quarter)`. `compute_stints()` does **not** filter on
   `n_on = 5`, so a malformed carried state would otherwise become a silent
   four- or six-man stint. An anchor that fails this is **dropped** before the
   stint build and logged via `log_msg()` (game, team, quarter, `n_on`); that
   period then loads exactly as it does today. Rejecting instead would roll
   back the whole game (see amendment 5).

Hard-reject when any of the following does not hold:

2. Every anchor `id` exists in `actions_clean` for that game.
   `lineups_lookup` carries `lineups_lookup_actions_clean_fk`, and `pws` joins
   on `between(id, final_start_id, .join_end_id)`.
3. Two anchors per `(game_id, quarter >= 2)`, or exactly one when the period's
   lowest-id action is a substitution and that team is therefore skipped.
   Measured on the 445-game local snapshot: 1338 periods with two, 17 with one.
4. The anchor's `end_game_seconds_remaining` is the period's maximum for that
   game, i.e. the anchor really is the period's first action.

**How they are enforced (implemented 2026-09-19).** `apply_period_anchor_gates()`
in `etl/period_opening_anchors.R` runs on the collected `lineups_lookup` rows
at both call sites, before the write. Gate 4 runs
`period_anchor_clock_violations()` on the game's in-memory `actions_df`. Gates 2
and 3 are enforced as one **runtime parity check**: every anchor the SQL
retained must be one the pure helper selects from the same `actions_df` and
`roster_df` (both written unchanged to `actions_clean` / `full_rosters` in the
same transaction). The count form of Gate 3 cannot be checked on the output,
because an anchor superseded by a same-clock substitution is correctly gone
after `slice_max`; the count property is the helper's, covered by its unit
tests and by the Postgres parity test. Rows carry a `period_anchor` marker
from the `UNION ALL`, which the gate runner removes; it is not a table column.

**Hardened 2026-09-20**, after a review of the implementation:

- **Gate 4 no longer reports a phantom period.** `end_game_seconds_remaining`
  comes from `lubridate::ms()`, which returns NA on an unparseable clock.
  `NA < x` is NA, and logical-NA row subsetting of a data frame returns an
  all-NA *row* rather than no row, so an unreadable clock aborted the game
  with `game NA QNA id NA at NA < NA`. The unreadable and below-maximum cases
  are now separated, carried in a `reason` column, and subset by position.
- **The marker's absence is raised, not interpreted.** Every gate reads
  `period_anchor` with `%in% TRUE`; on a frame without the column that is
  `NULL %in% TRUE` -> `logical(0)`, which subsets *every* row away — and the
  gate runner's return value is what the caller writes to `lineups_lookup`.
  `require_lineup_columns()` now fails loudly instead.
- **Gate 5, coverage (report, do not reject).** The parity check is
  one-directional: it proves no SQL anchor is unexpected, not that any anchor
  arrived. A `UNION ALL` that silently yields nothing passed every gate and
  reverted the ETL to pre-fix behaviour with no trace. The counting form
  cannot run on this frame, because an anchor superseded by a same-clock
  substitution is correctly absent after `slice_max` — the common case. So
  each expected anchor must be either retained, or explained by a provider
  state for the same team at the same period-opening clock; what is left is an
  anchor that mattered and did not arrive. It is logged at WARN rather than
  rejected, for Gate 1's reason: the period then loads exactly as it did
  before anchors existed.
- **Gate 4 degrades instead of rejecting.** See the paragraph below the gate
  list. It drops the offending period's anchors, logs at WARN, and passes the
  period to Gate 5 as already-handled so the same period is not re-reported as
  a coverage gap.
- **Gate 6, the clock-window collision (reject).** `slice_max` de-duplicates on
  `(quarter, quarter_time, end_game_seconds_remaining, player, team)`, but the
  `lineup_id` window partitions on `(game_id, team_id, quarter,
  end_game_seconds_remaining)` alone. An anchor whose `quarter_time` *string*
  differs from a same-clock substitution — `"10:00"` against `"10:00.0"`, both
  600s to `lubridate::ms()` — survives the de-duplication and then lands in the
  same `string_agg` window, contributing a second row per player: a ten-entry
  `lineup_id` and a `lineup_hash` matching nothing in `sub_lineups`, on the
  provider row as well as the anchor. `n_on` partitions by `id`, so it stays 5
  and Gate 1 cannot see it. This was amendment 6, previously only an assertion
  to make in the shadow diff; it is now checked in code, by the invariant that
  no player may hold two states in one clock window. It rejects rather than
  degrades because the hashes are computed in SQL before these rows are
  collected, so dropping the anchor would leave the corrupted provider row.
- **Game 211 is excluded outright** (`PERIOD_ANCHOR_EXCLUDED_GAMES`). Its
  regulation and overtime action ids overlap, and *both* halves of the
  mechanism are id-ordered: the anchor is a period's lowest id, and
  `compute_lineups_lookup()` fills `is_on` down an id-ordered window. An anchor
  there would carry forward an arbitrary mid-game state with `n_on = 5`, which
  no gate can distinguish from a healthy one. `etl/ot_lineup_recovery.R`
  excludes the same game for the same reason.
- **`type` is a required column** of the actions frame. Without it the pure
  helper's same-id substitution skip silently never fires
  (`as.character(NULL)` is `character(0)`, so `identical()` is FALSE for every
  row) while the SQL twin errors on the missing column — a silent divergence
  between the two implementations the parity gate exists to keep in step.

Gate 1 is the one that will fire in practice. When it does, the period is a
genuine carry-forward failure. In this release it simply keeps today's
behaviour for that period. Adjudicating it properly is what the existing OT
recovery machinery is built for, but that is **not wired in this release** —
see Fallback below.

Gates 2, 3 and 6 hard-reject because a failure there means the anchor rule or
the ETL itself is wrong, not the provider data, and loading anyway would write
inconsistent rows. Gate 4 was in this list until 2026-09-20 and does not belong
in it: a period opening stamped with a later clock is precisely a provider
defect — the 398/399 class — so it now degrades like Gate 1. The old wording
also contradicted the helper's own contract, which tolerates a clock that
regresses inside a period.

## Fallback: The Existing Recovery Module (future work, not in this release)

`etl/ot_lineup_recovery.R` already implements the handoff's stated fix
direction: `ot_latest_valid_lineup(quarter - 1)` seeds,
`ot_apply_period_start_reset()` replays the boundary substitution group
sequentially, `recover_ot_lineup_periods()` cascades period by period, and
every outcome is audited and hard-rejected on inconsistency.

It is not the primary fix here, for one reason: it **deletes and replaces the
whole period's** `lineups_lookup` rows. That is proportionate for an OT period
with no coverage at all. For a regulation quarter it swaps provider-derived
states for reconstructed ones across the entire quarter, to repair a
leading-edge defect.

Reserve it for periods where Gate 1 fails — as a later upgrade from this
release's drop-and-log behaviour, not part of it. Two changes are required before it
can run against regulation:

1. `detect_ot_leading_lineup_gaps()` filters `pws_df$quarter >= 5`
   (`etl/ot_lineup_recovery.R:608`). Widen to the quarters being adjudicated.
2. `is_period_start <- clock_index == 1L && ... && clock_seconds >= 299`
   (`etl/ot_lineup_recovery.R:445`) hardcodes the 300-second OT period. A
   regulation quarter whose first row is stamped at `09:30` (570s) would
   satisfy `>= 299` and be wrongly given period-start reset semantics. Replace
   with `clock_seconds >= period_length - 1`, where `period_length` is 600 for
   `quarter <= 4` and 300 otherwise.

`OT_LINEUP_RECOVERY_EXCLUDED_GAMES` (game 211, overlapping regulation/OT action
ids) stays as is.

## Implementation Location

`compute_lineups_lookup()` in `etl/etl_onoff.R:919`, ahead of the roster join.

The function is dbplyr against live tables and is currently untestable without
a database. Build the anchor set as a **separate pure helper** taking an
actions data frame and returning the anchor rows, so the selection rule
(first action per period, `quarter >= 2`, NULL player fields) can be unit
tested against fixtures the way `app/tests/testthat/test-ot-lineup-recovery.R`
tests the recovery helpers. `compute_lineups_lookup()` then applies the same
rule in SQL, and a test asserts the two agree on a fixture game.

Both call sites in `etl/etl_full.R` (the provisional pass at line 566 and the
persisted pass at line 694) pick the change up without modification.

## Verification

### Step 1 baseline: MEASURED 2026-09-19

Run against production (451 games in `df_pts_poss_lineups_longer_mv`, 458 in
`final_schedule_mv`; the 7 unprocessed 2025 State Cup games are absent from the
MV and excluded by the join). Cold storage was empty, so the gap measure uses a
clock proxy rather than action ids: per `(game_id, quarter)`, the period's
nominal opening `end_game_seconds_remaining` minus the maximum value actually
attributed. Nominal openings were derived empirically and are exactly
2400/1800/1200/600 for Q1-Q4 and 300 for each OT period.

Conventions reused rather than invented: PBP points are
`SUM(team_score) FILTER (WHERE type_lineup = 'offense')` grouped by
`(game_id, team_id)`, matching `lineup_four_factors_by_game.sql:191`; official
points are `final_schedule_mv.team_score`, whose team ids are already mapped
through `schedule_team_dict` by `sched_long`.

#### Measure A: period-opening gap

**Corrected 2026-09-19 (second pass).** The first pass grouped
`BY (game_id, quarter)` over the MV, so a quarter with **zero** attributed rows
produced no group and was silently skipped rather than counted as a total loss.
It measured 1823 periods where 1827 exist. The corrected query enumerates
expected periods first and left-joins what the MV has:

```sql
WITH mv_games AS (SELECT DISTINCT game_id FROM basketball_test.df_pts_poss_lineups_longer_mv),
present AS (
  SELECT game_id, quarter, MAX(end_game_seconds_remaining) AS first_attributed
  FROM basketball_test.df_pts_poss_lineups_longer_mv GROUP BY 1,2),
expected AS (
  SELECT g.game_id, gs.q AS quarter FROM mv_games g CROSS JOIN generate_series(1,4) gs(q)
  UNION
  SELECT game_id, quarter FROM present WHERE quarter >= 5)
SELECT e.game_id, e.quarter, p.first_attributed, (p.game_id IS NULL) AS period_absent
FROM expected e LEFT JOIN present p USING (game_id, quarter);
```

Regulation quarters 1-4 are always expected; an OT period counts only when the
MV has it. An absent regulation period scores a full-period gap. This query is
therefore not sufficient to prove that no OT period is wholly absent; add the
independent OT inventory required by the implementation-review amendments
before final acceptance.

| Quarter | Expected | Absent | Present with gap | Max gap |
|---|---:|---:|---:|---:|
| Q1 | 451 | 0 | **0** | 0 |
| Q2 | 451 | 1 | 13 | 1800s (absent) |
| Q3 | 451 | 1 | 4 | 1200s (absent) |
| Q4 | 451 | 2 | 7 | 600s (absent) |
| Q5 | 21 | 0 | 3 | 277s |
| Q6 | 2 | 0 | 0 | 0 |

For `quarter >= 2`: **31 of 1376 periods affected across 24 of 451 games** — 27
present-but-late (total 782 unattributed opening seconds; median 12s, p90 68s,
p99 235s) plus **4 entirely absent**.

The four absent periods are **not anchor-fixable** and must not be counted as
this change's work:

| Game | Period | Why |
|---|---|---|
| 184 | Q3, Q4 | Raw feed has **zero** Q3/Q4 actions. Its two 10-minute quarters total exactly the official 59-70, with no clock resets — internally consistent but describing a 20-minute game. Cause unknown. |
| 406 | Q2 | Raw feed has **7** Q2 actions: a quarter marker plus 6 substitutions stamped at `00:01`/`00:00`, the period's *end* — the 398/399 misclocked-flurry signature. Q4 then carries 280 actions and 88 points, about double a normal quarter, while game totals still reconcile to 99-79. **Resolved 2026-09-20 (reported by the repo owner):** the period labels are wrong at the source — the period labelled Q2 is actually Q3, and the one labelled Q4 is Q3 and Q4 merged. It needs an ad-hoc relabelling script, like the game 402 Q4 corrections (`b944aed`) and `KNOWN_CLOCK_STAMP_CORRECTIONS`. Until that lands, treat 406's period structure as unreliable: the anchor the shadow run placed at "Q2" (id `4060228`, both teams, `n_on = 5`) is a symptom of the mislabelling, not a criterion-2 violation to adjudicate. |
| 380 | Q4 | The forfeit game — only three quarters were played. Legitimate. |

Two things this settles:

- **The blast radius is far smaller than assumed.** In practice both teams
  almost always substitute at a period boundary. The defect is a tail, not a
  systematic loss at every boundary.
- **Q1 has zero gaps and zero absences in 451 games.** The provider's opening
  five-in declaration always lands at 2400. This closes the Q1 open decision
  empirically: Q1 anchoring is unnecessary, not merely deferred.

#### Measure B: point reconciliation

22 team-games across **15 games** have PBP points != official points. Delta
distribution: `-15, -8, -5 x3, -4, -3 x2, -2 x6, -1 x6, +54, +65`.

Crossing the two measures per game:

| | no point delta | point delta |
|---|---:|---:|
| **no opening gap** | 418 | 10 |
| **opening gap** | 18 | 5 |

The 15 mismatched games break down as:

- **3 games this fix targets** — 401, 404, 406, the handoff's verified set.
  Every one of their 8 unattributed scoring actions sits in a period's opening
  window, confirmed action by action against the raw feed.
- **1 game already excluded** — 211, Q5 gap of 277s, delta -8/-15. This is the
  documented regulation/OT action-id overlap in
  `OT_LINEUP_RECOVERY_EXCLUDED_GAMES`. It needs the bucket-scoped stint
  boundary from the OT design's backlog, not an anchor.
- **1 forfeit artifact** — 380, official score 20-1 against 74/66 PBP points
  (deltas +54/+65). The MV holds three real quarters; the schedule holds an
  administrative forfeit score. Not a data defect; exclude it from
  reconciliation rather than chasing it.
- **10 games whose loss is NOT at a period opening** — 96, 139, 140, 141, 143,
  **184**, 62500, 62504, 62584, 64893, deltas between -1 and -5. **This fix
  will not touch them.** Game 184 is the worked example: its 5 lost points per
  team are actions `1840321`-`1840340` at elapsed 961-995, i.e. 03:59-03:25
  into Q2 — a mid-quarter attribution hole, nowhere near a boundary. A separate
  defect class needing its own investigation.

Also: **18 games have an opening gap but reconcile on points.** They still lose
minutes and Game Flow lane coverage, so the gap count, not the point delta, is
this change's primary success metric.

#### Game 184 is NOT this defect (resolved 2026-09-19)

An earlier pass grouped 184 with the target games because it showed both a Q2
opening gap and a `-5/-5` point delta, and flagged the pairing as suspicious
because five points per team inside a six-second window is implausible. It was
right to be suspicious. Checking the raw feed action by action, 184's lost
points are `1840321`-`1840340` at elapsed **961-995** — 03:59 to 03:25 into Q2,
mid-quarter.

So 184 has *both* a harmless 66s boundary straddle that loses no points *and* a
separate mid-quarter hole that loses ten. The two measures overlapping in one
game was a coincidence, not a mechanism. It belongs with the other nine.

#### Artefacts

`recon.csv` (all 907 team-games) and `gaps.csv` (all 1823 periods) were written
to the session scratchpad. Regenerate with the queries above before comparing;
do not treat a stale copy as the baseline.

### Helper validation against cold-storage parquet (2026-09-19)

`etl/period_opening_anchors.R` plus `app/tests/testthat/test-period-opening-anchors.R`
(31 assertions, TDD). Beyond the fixtures, the helper was run over the real
`exports/cold/actions_clean.parquet` snapshot — 277,176 actions, 445 games.

**Snapshot provenance.** The figures in this subsection (1355 anchored
periods, 17 substitution-first periods) come from that **445-game local-only**
snapshot. An earlier ribbon-plan copy quoted **1361** anchored periods from the
**448-game** snapshot (local merged with the `cold-storage/latest` release;
401 was already local, so the release contributes 404 and 406 — Q2-Q4 of two
games, exactly the +6 difference). The two are not comparable baselines. Before
implementation acceptance, regenerate every anchor/period count from one named
snapshot and label it.

Established:

- **Gate 4 passes on the real feed.** 1355 periods anchored, **0** clock
  violations: the period's lowest-id action always also carries the period's
  maximum remaining seconds.
- **Anchor selection matches the feed's shape.** 2693 anchor rows over 1355
  periods; the opening action is a `quarter` marker 2676 times and a
  `substitution` 17 times, which is what drives the skip rule above.
- **Game 401 Q4 is confirmed end to end.** The anchor is `4010679` at `10:00`.
  The three actions the handoff lists as lost — `4010683` (made 2pt, team 33),
  `4010686` (made FT, team 33), `4010688` (made 2pt, team 11) — all fall in
  `[4010679, 4010698)`. That is 3 points to team 33 and 2 to team 11, which
  matches the survey's measured deltas of `-3` and `-2` exactly.

Not established, and the reason Step 2 still has to run:

- A coarse R model of the pipeline (each team's first substitution as its first
  state, `pmax` over the two teams) flags 23 of the 24 survey-positive periods
  and drives all of them to a zero gap once anchored, but it reproduces the
  survey's exact **gap seconds** in only 5 of 23. The model ignores the
  `slice_max` tie-break that decides which id within a clock group becomes the
  segment start, so it is directional evidence only, **not** a substitute for
  recomputing `lineups_lookup`/`stints`/`pws` for real.
- **Games 404 and 406 are absent from the parquet.** The snapshot holds 445
  games against the database's 451, so the handoff's primary example (404 Q4,
  Lofton Jr.) could not be checked locally. Refresh cold storage before Step 2,
  or check those two against the database.

### Step 2: shadow run, additive-only

Restore cold storage (`scripts/restore_cold_storage.R`), recompute
`lineups_lookup`, `stints` and `pws` for games 401, 404 and 406 into scratch
tables, and diff against the current rows.

The assertion is **semantically additive only**: new lineup-attributed actions
at period openings, and **zero changes to any existing action's team/lineup
attribution**. Compare stable business fields and ignore expected renumbering
of surrogate `segment_id` values plus boundaries derived from the inserted
stint. If an existing action moves to a different lineup, stop — that falsifies
the `slice_max` tie-break reasoning above, and the anchor is overwriting
provider states rather than filling gaps.

### Step 3: the handoff's checklist

1. Game 404 Q4 shows Lofton Jr. entering at `10:00` and player `1135` exiting.
2. The boundary substitutions at `09:36` and `08:49` remain separate events.
3. Actions `4010683`, `4010686`, `4010688`, `4040230`, `4040629`, `4040637`,
   `4040662`, `4060250` receive lineup hashes.
4. PBP-derived team points equal the official box score for 401, 404 and 406.
5. Game Flow lanes begin at the period boundary, with no artificial `08:03`
   start for game 404 Q4.

### Step 4: re-run step 1

First apply these criteria to the three shadow games. Apply the production-wide
counts only after every affected historical game has been reprocessed;
reprocessing 401/404/406 alone cannot clear the global baseline.

Success criteria, revised against the measured baseline:

1. Present-but-late period count for `quarter` 2 to 4 falls from 24 to 0. The
   three Q5 periods in the 27-period baseline are no longer in scope (see
   Scope: Overtime) and belong to `etl/ot_lineup_recovery.R`; whether
   reprocessing closes them is an OT-module question, tracked separately.
   Game 211's Q5 gap survives either way, because its cause is the action-id
   overlap, not a missing anchor.
2. The 4 entirely-absent periods (184 Q3/Q4, 406 Q2, 380 Q4) are **unchanged**.
   They are upstream feed defects, not attribution gaps; if the anchor appears
   to fix one, something is fabricating a period.
3. Games 401, 404 and 406 reconcile on points. The raw feed totals match the
   official score **exactly** in all three, so every missing point is
   attribution loss and the target is known per action:

   | Game | Team | Official = raw | MV today | Recover | Unattributed action(s), by elapsed |
   |---|---|---:|---:|---:|---|
   | 401 | 11 / 33 | 103 / 101 | 101 / 98 | +2 / +3 | 1808, 1810, 1819 — all in Q4's opening 19s |
   | 404 | 4 / 11 | 83 / 69 | 81 / 64 | +2 / +5 | 668 (Q2 +68s); 1815, 1831, 1895 (Q4 opening) |
   | 406 | 6 / 14 | 99 / 79 | 99 / 77 | 0 / +2 | 1262 — Q3 +62s |

4. The 9 no-gap mismatches (96, 139, 140, 141, 143, 62500, 62504, 62584, 64893)
   are **unchanged**. If any of them moves, the anchor is doing something this
   design did not predict.
5. Game 380 is unchanged and stays excluded as a forfeit artifact.
6. The 418 games clean on both measures are byte-identical.

## Test Plan

Fixtures for the pure anchor helper and the reconstruction path:

1. Neither team substitutes at the boundary: both teams gain an anchor, both
   carry forward, the stint opens at the period's first action.
2. One team substitutes at `10:00`, the other does not: the substituting team's
   state keeps the substitution's `id` and post-substitution membership; the
   other team anchors.
3. Both teams substitute at `10:00`: anchors are superseded for both, and
   output is **byte-identical to today** for that period.
4. A scoring action falls between the boundary and the later team's first
   substitution: it receives a lineup hash (the game 404 Q4 shape).
5. Further substitutions at `09:36` and `08:49` remain distinct states.
6. Q1 produces no anchor.
7. OT period anchors carry forward from Q4, and a healthy OT game's output is
   unchanged.
8. A carried-forward state of four or six players trips Gate 1: the anchor is
   dropped and logged, the game still loads, and that period's output is
   byte-identical to today.
9. A period whose first action is not at the period's maximum clock trips
   Gate 4.
10. A game with no boundary gaps anywhere is unchanged end to end.

Run `app/tests/testthat/test-ot-lineup-recovery.R` unchanged as a regression
check that the recovery module's behaviour is untouched.

## Reprocessing

For the three verified games:

```bash
gh workflow run etl-full.yml -f game_ids=401,404,406
```

For a full historical re-derivation, do **not** re-fetch every game from the
provider. Restore cold storage, rebuild `lineups_lookup`, `stints` and `pws`
offline, then run the incremental refreshers
(`refresh_df_pts_poss_lineups_longer_for_games` and its downstream chain)
rather than `rebuild_all_mvs()`. Advance `app_meta.etl_full_last_success`
afterwards so the Shiny season caches invalidate.

**The offline path cannot reach every game.** It rebuilds only games whose
actions are in the cold-storage Parquet snapshot, and that snapshot has holes:
404 and 406 are absent from the local `exports/cold/`, and per the CLAUDE.md
backlog 393, 397, 399 and 400 are in neither the local snapshot nor the
`cold-storage/latest` release. Intersect the regenerated affected-game set with
Parquet coverage first; any game outside it must go through the provider path
(`gh workflow run etl-full.yml -f game_ids=...`).

## App Impact

No reader change. Every tab reads `df_pts_poss_lineups_longer_mv`, whose own
`WHERE lineup_hash IS NOT NULL` is correct and stays; once `pws` carries hashes
at period openings, the recovered actions appear everywhere at once. The Game
Flow ribbon reads the same MV through `RIBBON_SQL_ISRAEL`
(`app/R/global.R:312`, via `fetch_stint_ribbon()`), so its lanes begin at
`10:00` on their own. The handoff's "the ribbon should then consume those
seeded segments" requires no reader change.

After this lands, the ribbon plan re-measures its C, D and F workstreams
against the post-anchor data.

## Implementation Sequence

1. ~~Run the step 1 survey and record the baseline.~~ Done 2026-09-19; see
   Step 1 baseline above.
2. Add the pure anchor helper plus its unit fixtures. **Done 2026-09-19**:
   `etl/period_opening_anchors.R`, 12 tests / 31 assertions.
3. Wire the anchor `UNION ALL` into `compute_lineups_lookup()`. **Done
   2026-09-19**: `period_opening_anchors_tbl()` (dbplyr twin, NULLs cast to
   INTEGER for the union), sourced from the top of `etl/etl_onoff.R` so both
   `etl_full.R` and the legacy script body get it.
4. Add SQL/helper parity coverage and the four acceptance gates with
   structured `log_msg()` output. **Done 2026-09-19**: gates applied at both
   `compute_lineups_lookup()` call sites before the write. Gate 1 drops and
   logs; Gates 2-4 reject. Test file now 25 tests / 66 assertions, including
   two Postgres tests (`RUN_DB_TESTS=1`): helper/SQL parity via
   `dbplyr::copy_inline()`, and `EXPLAIN` of the full unioned
   `compute_lineups_lookup()` query against `basketball_test`, both green
   under `app_readonly`. Full suite: 641 tests, only the three pre-existing
   failures. **Not yet run against real data** -- `actions_clean` is empty
   between ETL runs; that is step 5.
5. ~~Shadow run games 401, 404 and 406; confirm the semantic diff is additive
   only, allowing surrogate segment-id renumbering.~~ **Done 2026-09-20.**
   Cold storage restored for the three games only (the release increment was
   merged into the local cumulative archive first — it held the only copy of
   404 and 406). `compute_lineups_lookup()` + `apply_period_anchor_gates()`
   run against a control arm with anchors suppressed via
   `PERIOD_ANCHOR_EXCLUDED_GAMES`, so anchor effects are separated from
   harness effects. Result, control -> anchored: **0 rows lost, 0 fields
   changed on any shared key, 80 rows added** at five period openings —
   401 Q4 `4010679`, 404 Q2 `4040211`, 404 Q4 `4040622`, 406 Q2 `4060228`,
   406 Q3 `4060239`. Four of the five are exactly the periods the Step 1
   baseline predicted; 406 Q2 is the mislabelling case (see the absent-period
   table). Gate 1 dropped the 406 Q3 anchor for team 14 (`n_on = 0`) and kept
   team 6's. Gate 4 stayed silent, correctly: 406 Q2's quarter marker is
   stamped at `10:00`/1800.
   Two measurements from the run that are **artifacts, not findings**: the 58
   rows "lost" and 11 `is_on_verdict` changes against the DB baseline are all
   game 401 Q5 and appear in the no-anchor arm too — OT-recovery rows the
   harness does not reproduce. A reported 1793 `num_starters` changes was a
   diff-script defect: without `bit64` attached, `is.numeric()` on an
   `integer64` column is FALSE and the `as.character()` fallback renders raw
   bit patterns. Attach `bit64` in any script that diffs these frames.
6. ~~Reprocess the three games; run the handoff checklist.~~ **Done
   2026-09-20 for 401 and 404.** 406 is held back until its period labels are
   relabelled (see the absent-period table), because its per-quarter
   expectations are not trustworthy until then. Run via
   `etl_full(game_ids = c(401, 404))` — the real production path, locally,
   because the nightly workflow builds from `main`. 181s, exit 0, all seven
   phases complete.
7. ~~Re-run the three-game acceptance checks.~~ **Done 2026-09-20. All pass.**

   | Check | Before | After | Predicted |
   |---|---|---|---|
   | 401 points missing (team 11 / 33) | 2 / 3 | **0 / 0** | +2 / +3 |
   | 404 points missing (team 4 / 11) | 2 / 5 | **0 / 0** | +2 / +5 |
   | 401 Q4 first attributed clock | 548 | **600** | period opening |
   | 404 Q2 first attributed clock | 1732 | **1800** | period opening |
   | 404 Q4 first attributed clock | 483 | **600** | the 1:57 gap |
   | 7 of the 8 target action ids | absent from MV | **attributed** | attributed |
   | `lineups_lookup` rows 401 / 404 | 776 / 584 | 788 / 610 | +12, +26 |

   The eighth target action (`4060250`) is game 406 and stays unattributed, as
   intended. 401 Q5 stays at 286: overtime, left to `ot_lineup_recovery.R`,
   which ran in this very ETL and logged `accepted_carry_forward` for both
   teams with 87 reconstructed rows — the OT path is demonstrably intact under
   the anchors, which is what the Scope reversal was for.

   Two side observations from the run, both checked rather than assumed:
   the lineup/stint coverage warning for game 401 **improved** from 11
   zero-match rows out of 477 (the 2026-09-16 run) to 2; and the data-quality
   report's `Overall status: FAIL` is pre-existing — the failing check set is
   byte-identical before and after, 14 checks either way.
8. ~~Regenerate the affected-game set, intersect it with cold-storage Parquet
   coverage, decide the historical re-derivation scope, and reprocess that
   scope (offline where covered, provider path otherwise).~~ **Done
   2026-09-20.** Affected set: 19 games, all present in local Parquet.
   Reprocessed 18 of them **offline** (406 held for its relabelling).

   **The offline branch is mandatory, not a preference.** A refetch is not
   equivalent to the archive: comparing today's `clean_actions()` output
   against the archived rows field-by-field, game 64902 comes back with **507
   changed `parent_action_id` and 26 changed `parameters_points`**, and 62449
   with one changed `player_id`. Re-ingesting would have bundled unrelated
   source/derivation drift — `parameters_points` is the 2PT/3PT split — into a
   change whose whole verification story is "additive only". An id-and-count
   comparison says "no drift" and is not sufficient; compare every column.

   The offline path restores the archived `actions_clean` and `possessions`
   for one game, then calls the same helpers `etl_update()` calls
   (`compute_lineups_lookup` + `apply_period_anchor_gates`, `compute_stints`,
   the `pws` join), followed by one downstream pass for all games: Phase 3's
   sub-lineup generation, the nine `refresh_*_for_games(int4[])` SQL
   functions, the seven MV refreshes, and `refresh_sub_lineups_stats_for_games`.
9. ~~Re-run the production-wide survey.~~ **Done 2026-09-20 — and it falsified
   criterion 1 as written.**

   Measure A is unchanged: 24 present-but-late periods, 21 in Q2-Q4, the same
   19 games. It did not move because **Measure A has a floor**: it reports the
   first *attributed* clock, which is capped by when the first attributable
   event occurs. A period whose opening seconds contain only the `team_id = 0`
   period marker can never reach the nominal opening clock. Game 66 Q2 now
   opens its stint exactly at the anchor id `660195`, and the `pws` row there
   is the marker with a NULL lineup hash, so `first_attributed` stays at 1784.
   The baseline's own median gap of 12s was the clue. **Criterion 1 ("falls to
   0") is not achievable and should be restated against points attribution,
   not the clock proxy.**

   The measure that does hold up is points. After the offline reprocess,
   **33 of 36 team-games reconcile exactly**. The residuals:

   - **184** (both teams, -5): the raw feed has zero Q3/Q4 actions. Known, and
     criterion 2 requires it to be unchanged.
   - **62526 team 24 (-1)**: a made free throw, id `625260653`, stamped at
     `clock = 1200` — *exactly* the Q3 opening. A boundary case anchors do not
     cover; worth its own look, not a regression.

   **Methodology gap, recorded so it is not repeated:** points were snapshotted
   before reprocessing only for 401/404/406, not for these 18. So "33 of 36"
   cannot be decomposed into improved-versus-already-correct. Snapshot the
   acceptance measure for every game in scope before writing, not just the
   headline ones.
9. Re-run the production-wide survey, including an OT period inventory that is
   independent of the target MV; only now confirm the global baselines clear.

## Open Decisions

- ~~**Q1 seeding from box-score starters.**~~ Resolved by the survey: zero Q1
  gaps in 451 games. Q1 stays out of scope on evidence rather than caution.
- **The 9 no-gap point mismatches.** A separate defect, deltas -1 to -5, out of
  scope here and currently undiagnosed. Needs its own investigation.
- **Historical re-derivation scope.** Whether to repair the full history or
  only the current season depends on the survey's game count (24 games) and on
  cold-storage Parquet coverage — not on database size: the 500 MB free-tier
  budget has not bound for a long time (`pg_database_size()` measured 3237 MB
  on 2026-09-19).
