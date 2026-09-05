# Session handoff — 2026-09-05

Two threads: **game-log parity** (finished, merged, pushed) and a **minutes
conservation migration** (written, verified, deliberately NOT applied).

Baseline at session start: `main` at `9adfa97`.

## Status right now

| | |
|---|---|
| `origin/main` | 24 commits, all pushed |
| Database | **UNCHANGED.** Every run was `--dry-run` and rolled back |
| Minutes migration | **VERIFIED AND READY.** Full dry-run passes all six gates. Not applied — `--apply` is a manual step |
| App | Game-log work is merged but **has never been opened in a browser**, and never deployed |

---

## Thread 1 — Game Logs parity (DONE)

`21201b8`, `093b039`, `99b751a`, `73c79e5`, `f8d219f`, `fa094fe`, `7d44e28`,
`997a562`, `d378d35`, `2fd8ed1`.

### Shipped

- **Min column on Tab 11** (EuroLeague game logs), from
  `euroleague.lineup_totals_by_game.seconds`. No new ETL — the data existed.
- **Tab 11 default sort fixed.** It set no DataTables `order`, so it sorted Rd
  *ascending* — oldest game first — silently discarding its own
  `arrange(desc(game_date), ...)`.
- **`game_id` sort tiebreak on Tab 11.** DataTables' sort is stable, so rows
  tying on Date and Rd keep their input order. 79 of 100 date+round buckets hold
  more than one game; without the tiebreak a game's two rows scattered
  alphabetically — game 224's halves sat 11 rows apart.
- **CSV export on Tab 4**, via a shared `csv_export_button()` in `helpers.R`.
- **Tab 9's CSV leak closed** — it passed no `exportOptions`, so exports shipped
  every hidden `pr_*` column with no timestamped filename.
- **Table headers extracted** to `gamelog_summary_header()` /
  `gamelog_ff_header()` in `helpers.R`; Tab 11 adopted Tab 4's grouped
  Offense/Defense/Usage header. The extraction was verified byte-identical by
  reversing the transform, not by writing new tests for moved code.
- **`Net` added to Four Factors** on both tabs.
- **Export buttons made visible** — see the correction below.

### Corrections worth keeping

- **The first button fix did nothing.** I read `buttons.bootstrap5.min.js` and
  styled `.btn-secondary`. DT actually attaches `buttons.dataTables.min.css`,
  whose `div.dt-buttons > .dt-button` is (0,2,1) against my (0,2,0) — so the
  rule never applied, while my contrast tests passed against tokens that were
  not rendering. Fixed in `7d44e28`. Saved to memory: ask the widget what it
  loaded (`htmltools::resolveDependencies`), and note that a contrast
  measurement on a rule that never applies still passes.
- **Min is NOT comparable across the two tabs.** Israel ends the last segment at
  the last recorded action (~39.4 min/team-game); EuroLeague ends at the nominal
  period boundary (40.00). Same header, different definition.

### Audit — `docs/tab4_tab11_gamelog_parity_audit_2026-09-05.md`

Nine findings. Three run *toward* Israeli, not away: Tab 4 hand-rolls the
starters filter that `starter_context_filters_ui()` already provides, its team
filter is single-select where Tab 11's is multi, and Tab 11's Four Factors
carried `Net` that Tab 4 lacked. Finding **P10** records that I originally
mis-classified the sort tiebreak as cosmetic.

### Open from this thread

- Nothing verified in a browser. Tab 11's grouped header and both export buttons
  are unseen.
- Tab 4 still hand-rolls the starters filter (audit P2) — a byte-identical move.
- Tabs 3, 5, 6 still hold inline copies of the export contract. They do not
  leak; migrating them is tidying, not a fix.

---

## Thread 2 — Minutes conservation (NOT APPLIED)

### What is wrong

Minutes are undercounted across the schema. Canonical truth is **40.006
min/team-game**; the relations the app reads are short:

| Relation | Measured | Target |
|---|---|---|
| `mv_lineup_totals_by_day` | 39.421 | 40.006 |
| `onoff_default_mv` | 197.16 | 200.03 |
| `player_four_factors_by_game.minutes` | 197.10 | 200.03 |
| `player_four_factors_by_game.onoff_minutes` | 197.10 | 200.03 |
| `player_traditional_stats_mv` | **200.09** | correct — the reference |

**Every minute path has TWO independent bugs:**

- **(a) aggregation grain** — the sum is filtered on offense being present, or
  taken over a grain that still carries `type_lineup`.
- **(b) row existence** — the *stats* CTE drives the output rows and is grouped
  by `type_lineup`, so a slice with floor time but no offensive possession has
  no offense row for minutes to attach to, and the time vanishes.

`player_traditional_stats_mv` is the only relation that gets both right: its
`segment_times` CTE groups without `type_lineup` and filters nothing. It is the
reference implementation for any further work here.

### What is written

Fixes for (a) in four places, and for (b) — "fix B", emitting the missing
offense rows with zero counts and their real minutes — in three:
`sub_lineups_by_day` (+5,772 rows, 325.9 min recovered),
`lineup_four_factors_by_game`, and `player_four_factors_by_game.minutes`.

`scripts/apply_minutes_offense_filter_fix.R` plus
`scripts/minutes_migration_helpers.R` rebuild 7 relations in ONE transaction
with grants, an access audit and five gates; anything failing rolls back all of
it. Modes: no flag = measure only; `--dry-run` = full apply then rollback;
`--apply` = commit.

### Dry-run history

| Run | Result |
|---|---|
| 1 | Died in 18s — `cannot insert multiple commands into a prepared statement`. MV files carry their CREATE INDEX statements; fixed with `immediate = TRUE` (`1cf3891`) |
| 2 | Gate 1 failed, **700** team-games |
| 3 | Gate 4 failed, **642** |
| 4 | Gate 4 failed, **3** — all pre-existing bad data |
| 5 | **Gates 1-4 PASS.** Gate 5 (`onoff_minutes`) failed, **627** |
| 6 | **ALL SIX GATES PASS**, 18m11s. `gap 0.000`, 568/568 player-seasons agree, ETL parity PASS on all three |

Each rollback was verified clean: 7 relations present, minutes unchanged at
39.421, `app_readonly` grants intact.

### The blocker, and how it was closed

`rebuild_all_mvs` builds from the `.sql` MV files, but **ETL Phase 4 maintains
those relations incrementally through the refresh functions**, which held the
old minute logic. Applying without them would have been undone game by game by
the next nightly run.

Five refresh functions are now installed **inside the same transaction** as the
rebuild, `refresh_sub_lineups_stats()` runs there too, and a sixth gate,
`verify_minutes_refresh_parity`, asserts the incremental path agrees with the
rebuilt relations. All three parity checks pass.

### Final dry-run — all six gates

```
ETL parity PASS: onoff_default_mv / team_metrics_by_game_mv / sub_lineups_stats
onoff vs traditional: 568 player-seasons agree
AFTER  canonical truth        : 40.006 min/team-game
       mv_lineup_totals_by_day: 40.006  (gap 0.000)
DRY RUN COMPLETE -- rolled back. Nothing changed.
```

`gap 0.000` is exact conservation per team-game, not an average — the
distinction that rejected three earlier attempts. Gate 6 compares
`onoff_default_mv` against `player_traditional_stats_mv`, which this migration
does NOT rebuild, so it is an independent reference.

### Implementation audit (residual 2, closed)

The fix-B edits add zero-count rows to three relations. Verified they cannot
disturb anything else:

- Only `SUM`s consume the changed CTEs internally — all-zero rows are inert.
- Every downstream `COUNT`/`AVG` reads `df_pts_poss_lineups_longer_mv`, which
  is not rebuilt. `onoff_compute.sql:145`'s `AVG(tgp.off_ppp)` traces to the
  base table, not to any changed MV.
- **`auto_minposs_from_df()` is provably unchanged**: it sorts `total_poss`
  descending and takes the 150th largest, so zero rows sort last. With >=150
  non-zero rows the threshold is untouched; with fewer, the old branch returned
  `0L` and the new one computes `ceiling(0/10)*10 = 0`. Same answer.
- `onoff_default_mv` gains no rows (no `UNION` in `onoff_mv.sql`), so Tab 1's
  top-35% percentile is unaffected.

### Residuals before applying

1. **The `COMMIT` line itself has never executed.** `--dry-run` covers
   everything up to it and raises a sentinel instead.
2. **One unrelated behaviour change** rides along: an empty `game_ids` array no
   longer means "all games" in a refresh function's guard
   (`array_length(...) IS NULL` removed). Probably an improvement.
3. ~18 minutes with Tabs 1-4 unavailable; do not run during the nightly ETL.

### Verification lessons

- **Averages were the wrong instrument.** Every figure called "verified" during
  this session was a mean. The per-team-game gates rejected work that averaged
  correctly. 40.006 and 200.03 were true on average and wrong per game.
- **A gate was itself wrong.** Gate 4 deduplicated with `DISTINCT` on the
  minutes *value*, collapsing distinct slices holding equal numbers. That is why
  this relation was reported at 195.30; the truth is 197.10, matching
  `onoff_minutes` exactly — corroboration, not coincidence.
- Games **178** and **62452** are excluded by id with the evidence in the
  source. Both were verified broken *before* the migration (178 already
  +11.1/+16.4 from a documented invalid Q2 reset; 62452 already -8.7/-26.8), and
  the migration *improves* 62452. Excluded rather than absorbed by a wider
  tolerance, which at 27 minutes would stop catching real regressions.

### After applying, still manual

DQ checks T and X, `test-clock-minute-contracts.R` (game 115 moves off its
documented 39.867), and PROJECT.md:1390's `minutes < 39.0` ETL warning, which
exists *because* of this undercount.

---

## Other findings

- **`docs/unattributed_floor_time_2026-09-05.md`** — the measurement and
  mechanism behind the shortfall.
- **`docs/pooler_prepared_statement_error_2026-09-05.md`** — Tab 4 failed once
  with `bind message supplies 1 parameters, but prepared statement "" requires
  0`. Five hypotheses ruled out with evidence; the unresolved contradiction is
  that the deployed app is unaffected though its workers are always cold. Not
  reproduced since.
- **2025 State Cup has no play-by-play** — backlogged in `CLAUDE.md`.
  `game_type = 35`, 7 games a season, the only games with NULL `gn` (correct — a
  cup bracket has no league game number). The 2026 seven were processed; the 2025
  seven (ids 737922-738725) have zero rows in `etl_processed_games`. They are
  also the 14 team-games the migration gates skip as "absent from both sides".
- **`CLAUDE.md:387` was actively wrong** and told the next person to reintroduce
  the offense filter. It described the pre-2026-07 row-level sum, from before the
  gaps-and-islands rewrite moved aggregation to windows. Rewritten.

## Parked

**Stint ribbon** — architectural brainstorm paused at one question, full context
in memory (`project-stint-ribbon`). The data investigation is DONE: build on
`df_pts_poss_lineups_longer_mv`, never `stints` (empty *and* pre-canonical).
Open question: does a lane encode plain occupancy over an on/off-split margin
curve, or carry the per-stint verdict from day one?
