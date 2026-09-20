# Post-merge continuation: subs promotion + ribbon step 1 — handoff, 2026-09-20

Follows `docs/period_opening_anchors_handoff_2026-09-20.md`, which covers the
anchor work through its merge. This document covers what happened after: the
`subs` promotion (ribbon workstream H) and the first ribbon re-measure — which
turned up a **population-selection error in the anchor reprocessing**.

---

## 1. Headline finding: the reprocessed population was selected by the wrong measure

The anchors plan selected games to reprocess with **Measure A** — the
period-opening attribution gap, "first attributed clock is later than the
period's nominal start". That found 19 games, 18 of which were reprocessed.

**Measure A cannot see the defect the ribbon plan is about.**
`df_pts_poss_longer.sql` sets `segment_end_elapsed_seconds` from the next
segment's start with **no quarter partition**, so a period's closing segment
absorbs the next period's opening window. Actions in that window therefore *do*
receive a lineup hash — the **previous period's five**. Attribution is present
and wrong, so Measure A reports no gap.

Measure A found the games where attribution was **lost** (points missing).
Those are fixed and reconcile exactly. The games where attribution is **wrong**
are a different, overlapping-but-not-identical population, and they were never
selected.

**Concrete, verified examples not in the 19-game set:**

| Game | Team | Period | Earliest lineup state | False window |
|---|---|---|---|---|
| 100 | 3 | Q2 | `rem = 500` | 100s |
| 100 | 8 | Q2 | `rem = 500` | 100s |
| 62452 | 11 | Q3 | `rem = 600`* | 72s |

*62452's Q3 has a state at the boundary but the straddle still carries 4
substitutions in its overhang; it needs a closer look than this pass gave it.

Neither game appears in `66, 68, 88, 156, 177, 184, 223, 360, 406, 62449,
62506, 62512, 62526, 62530, 62534, 62537, 62540, 62581, 64902`.

**Implication.** Reprocessing is not finished. The correct selection criterion
for the ribbon defect is the straddle measure below, not the period-opening
gap. Re-running the offline reprocess over a straddle-selected population is
the next substantive task.

---

## 2. Ribbon step 1: faulty straddles re-measured

Definition taken from the plan's workstream C: *a segment that starts before a
period boundary and extends past it, while that team has a substitution inside
the overhang, draws a false lineup.*

```
segments 43281 | subs rows usable 79416 | games with subs 448 of 452
STRADDLING SEGMENTS: 49
FAULTY (substitution inside the overhang): 17     [baseline 6 of 51]
```

**Do not read 6 -> 17 as a regression.** The baseline query is not recorded in
the plan, so this measure was reconstructed from its prose and is not proven
comparable. More importantly the 17 are not one population:

| Overhang | Cases | Reading |
|---|---|---|
| 668s | 406 (2 teams) | The mislabelled game; excluded from the anchors by decision |
| 277s | 211 (2 teams) | `PERIOD_ANCHOR_EXCLUDED_GAMES`; excluded by design |
| 100s | 100 (2 teams) | **Real, and never reprocessed** — see section 1 |
| 72s | 62452 | **Real, and never reprocessed** |
| 1-3s | 10 cases | Boundary substitution flurries. The false window is under 3 seconds; cosmetically invisible |

So the substantive residue is 4 straddles across 2 games plus the 2 games
excluded by decision — not 17.

**Game 404, the headline case, is clean.** Its Q4 first-attributed clock is
`600`, the true period start. The 1:57 false lineup that motivated the whole
plan is gone.

---

## 3. Workstream H applied: `subs` is hot and backfilled

`scripts/promote_subs_to_hot.R` ran. `basketball_test.subs` now holds
**79,416 rows across 448 games** (was 418 rows / 3 games). Both constraints
intact (`subs_pkey` valid, `subs_actions_clean_fk` NOT VALID as designed), and
`app_readonly` SELECT verified.

**This operationally unblocks workstream C.** The plan's C section says it is
blocked until "the database promotion and historical backfill" complete. They
have.

The migration needed two fixes first, both caught by the dry run:

1. It refused to run at all — `if (hot_now > 0) stop(...)`. It was written when
   `subs` was empty, but `subs` stopped being truncated when it was promoted,
   so Phase 2 had been writing rows ever since.
2. A blanket restore would have failed anyway: `subs` has
   `PRIMARY KEY (game_id, id)` and 240 of the 418 existing rows were already in
   the archive, so `restore_cold_table()`'s unconditional append would have hit
   key violations.

It now loads only the missing keys. A third bug surfaced on the first apply —
the key comparison silently matched nothing because `pq` above it is a
`col_select = "game_id"` projection, so `pq$id` was NULL and every key became
`"401|"`. Fixed by re-reading the full frame. The first apply attempt also died
on a `dbWriteTable` dispatch error; **the transaction rolled back cleanly** and
the FK and row count were verified intact before retrying.

The script was **untracked** until this session despite CLAUDE.md referencing
it; it is now committed (`f72ebd2`).

---

## 4. Why `subs` and not `stints`

Asked during the session, worth recording:

- **`stints` is still a cold table** (`COLD_TABLES`, `etl/cold_storage.R:10`),
  truncated by Phase 7 after every run. It currently holds 1,636 rows for 18
  games and will be empty after the next ETL. The Shiny app runs *between* ETL
  runs, which is the whole reason `subs` was promoted.
- **Using `stints` would be circular.** It is derived from `lineups_lookup`,
  the thing the anchor bug corrupted. A segment straddles *because* no lineup
  state was created, so `stints` can only restate the straddle. `subs` is the
  independent provider record and is the only source that can say "a
  substitution existed here even though no lineup state does" — the
  discriminator between a benign straddle and a false one.

---

## 5. Method note

`elapsed = ribbon_period_bounds(n_periods)[quarter] - end_quarter_seconds_remaining`
was validated against the MV's own `event_elapsed_seconds` before use:
**19,540 rows compared, 0 mismatches**. Use it rather than re-deriving the
clock convention; `subs` carries no elapsed column.

---

## 6. State and next steps

Committed to `main` this session after the anchor merge:

```
f72ebd2 scripts: make the subs promotion incremental and re-runnable
ede381e docs: qualify the additive-only claim and make the game count auditable
7c3ecf1 docs: handoff for review — period-opening anchors
```

Next, in the order I would take them:

1. **Re-select the reprocessing population by the straddle measure** and run
   the offline reprocess over it. Games 100 and 62452 are known members.
   Section 1 is the reason this is first.
2. **Recover the baseline straddle query** or accept the reconstructed one and
   restate the plan's "6 of 51" against it, reporting overhang duration so
   sub-second artifacts stop counting as defects.
3. **Workstream C** — now unblocked; add the second health signal joining
   `subs`.
4. **D** (excluded segments) and **F** (mid-quarter holes) against post-anchor
   data.
5. Still open from the anchors work: game 406's relabel, the 62526 boundary
   free throw, restating criterion 1 against points.

**Verify after the next nightly** that Phase 7 left `subs` populated — that is
the migration's own acceptance check, and it has not run yet.
