# And-One With an Own Rebound Counts Once — Implementation Plan (Israeli league, all three seasons)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A made and-one whose missed free throw the shooting team rebounds is one possession, not two, as it already is in EuroLeague and EuroCup.

**Architecture:** `compute_possessions()` gets one final step that removes the made shot's end when the team demonstrably kept the ball. Stored rows of 2024-25, 2025-26 and 2026-27 are corrected by the existing backfill, generalised to take a rule, because re-fetching old games is known to fail.

**Tech Stack:** R 4.4.2 + dbplyr (ETL rule), testthat + duckdb (fixture tests), PostgreSQL on Supabase, Parquet (cold archive).

**Spec:** this plan's Evidence section; D30 in `docs/shared_definitions.md`; check `AN_consecutive_same_team_possessions`.

## Evidence (measured 2026-11-03)

**The defect.** `etl/possession_rules.R:48` marks a made shot as an end, and line 58 removes that mark only when the same `parent_action_id` group holds a second end. A made free throw or a defensive rebound supplies one; a missed free throw followed by an offensive rebound does not, so the shot keeps its end while the team still has the ball. Its next end is then a second possession.

| Season | And-ones | Fix targets | Next end is the same team, same period | Same-team repeats now | Expected after |
|---|---|---|---|---:|---:|
| 2024-25 (2025) | 926 | 42 | 42 | 204 | 162 |
| 2025-26 (2026) | 922 | 38 | 37 | 200 | 163 |
| 2026-27 (2027) | 199 | 8 | 8 | 66 | 58 |
| **Total** | **2,047** | **88** | **87** | **470** | **383** |

Repeats are counted as check `AN` counts them: by action id within a period. None of the 88 has the same team's end just before it, so the removal creates no new repeat.

**EuroLeague does not have this.** Across all four competition-seasons, 611 missed free throws are followed by the same team's offensive rebound, and none is marked as an end. The check lists no such repeat there.

**Three feed shapes the rule must survive** (these changed the design):

1. **The rebound is not always next to the free throw.** An assist can sit between them, and in games 60, 158 and 62434 the rebound is logged after the put-back it led to. So the test is "an offensive rebound anywhere after the group's last free throw", never `lead()`. Selecting by adjacency finds 85 of the 88.
2. **Game 178, Q2 (shot 1780455): nothing ends after the rebound.** The team misses twice more, rebounds, and the period closes with no end. Removing the shot's end there would lose the possession altogether, so the end comes off only when the next end in the period is the same team's. This also protects a future case where the opponent's end comes next.
3. **Game 437, Q3 (shot 4370575): the offensive rebound belongs to another group**, and the shot's group holds a defensive rebound. The current rule already gives one end, on the free throw. It stays as it is.

**The 32 and-ones with no end on the shot or the free throw are not lost possessions.** An earlier count looked only at those two rows.

| Rows | What they are | Action |
|---|---|---|
| 17 (2025: 10, 2026: 7, 2027: 0) | Free throw missed, an assist row, then a defensive rebound. Line 49 puts the end on the row before the rebound, which is the assist | None. The count is right; only the row is odd |
| 14 | Technical or unsportsmanlike foul on a made basket: the team keeps the ball. The next end is the same team's in every case | None. Correct by the special-foul rule |
| 1 (game 53, shot 530359) | The same special-foul case, with a rebound holding the group's end | None in this plan; report it if the pin moves it |

## Global Constraints

- One definition, worded for both leagues: "A made shot does not end the possession when the team rebounds its own missed and-one free throw; the possession continues to the team's next end."
- Every database write and every push to `main` needs the user's explicit go-ahead at that point. A push to `main` also redeploys production.
- Database work on port 5432, outside the nightly ETL (about 22:00 UTC) and EuroLeague load hours. Check the disk before local writes.
- Never call `refresh_sub_lineups_stats_for_games()` in batches on a full season (it filled the database server's disk twice); the backfill uses the full refresh. Never re-fetch old games through `etl_full(game_ids=)` in bulk.
- Points and minutes must not move in any team-game. Only possession counts change.
- Run `tr -cd '\r' < <file> | wc -c` before editing each file. Where a file holds carriage returns, splice with the raw-bytes method in root `CLAUDE.md` § Deploy Scripts, and check that `git diff --stat` is the size of the change.

## Review Focus

1. **Consecutive and-ones in one possession** (and-one, miss, own rebound, and-one, miss, own rebound, lay-up). Only the lay-up may stay an end. Test in Task 1.
2. **The own rebound is the last thing before the period ends.** The existing end-of-quarter rule marks the rebound, so the shot's end comes off and the rebound's stays: one end in all. The new step must therefore run after that rule. Test in Task 1.
3. **A substitution between the made shot and the next end.** The shot's lineup loses one possession and keeps its points, and the later lineup is unchanged, so a lineup can show points with no possession. It affects 27 of the 87 and takes 1 lineup-game to zero. Rule fixture in Task 1; pinned on the real rows in Task 3.
4. **The ETL's actor `team_id` versus the view's perspective `team_id`.** The rule reads `actions_clean`, where `team_id` is the actor; the targets SQL reads the view's offense rows, where it is the offense. An end on a row owned by the other team could make the two disagree. Task 2's pin compares them on real games.
5. **2025 On/Off parity.** That season already fails `repair_onoff_d23.R`'s check on team 4 for an unknown reason. Task 3 records the gap first, so the backfill can be shown not to have caused or widened it.

---

## Review Findings (2026-11-03), resolved in the tasks below

Code review supported the diagnosis and the fix's shape. Its scope was code and plan only; it did not independently verify the Evidence section's database counts. Each finding below was checked against stored data and is now part of a task.

- [x] **ETL and SQL used different next-end predicates.** Both now use **the next end after the made shot**, the same pair check `AN` reports, so each removal clears exactly one repeat. Measured: no end is logged between the shot and the free throw in any of the 88, and both predicates select the same 87, so no target changes. Task 1 adds the interleaved fixture and Task 2 Step 3 runs it through both implementations, which also covers 2025 as far as a fixture can, since the archive cannot.
- [x] **`HEAD~1` could compare the new rule with itself.** The pin loads the old rule from fixed commit `dfd4667` (last change to `etl/possession_rules.R` before this plan) and prints that SHA (Task 2 Step 3).
- [x] **Measuring lineups does not test readers.** Measured: the credited lineup differs between the shot and the next end in 27 of the 87 (2025: 11, 2026: 13, 2027: 3), and exactly 1 lineup-game falls from 1 possession to 0. The state is not new: 65 lineup-days already show points with no possession. `fetch_lineups_csv_v2` divides through `NULLIF`, so the rating is NULL, and its `HAVING` keeps such a lineup only while its total possessions, offense plus defense, reach `p_min_poss`. Task 3 Steps 1 and 6 pin this on the real rows, including 2025.
- [x] **`repair_onoff_d23.R` prints no player-level differences.** Task 2 adds a read-only export and Task 3 compares it before and after. The gate differs from the note in one way: no new or larger difference is allowed, but a smaller one is reported, not failed, because the backfill's game refreshes could legitimately clear a stale row.
- [x] **Completion wording.** Task 4 Step 1 states the limit of the same-team guard when closing D30.

---

### Task 1: The rule, tests first

**Files:**
- Rename: `etl/tests/test_special_foul_possession.R` to `etl/tests/test_possession_rules.R` (update any reference to the old name)
- Create: `etl/tests/helper-possession-fixtures.R` (the helpers and this task's fixtures as a named list of frames, so Task 2 Step 3 reads the same rows)
- Modify: `etl/possession_rules.R` (`base`, and the final `mutate` at lines 97-105)

**Interfaces:**
- Produces: `compute_possessions(actions_tbl)`, signature unchanged; `final_end_poss` as below.

- [ ] **Step 1: Write the failing tests.** Reuse the file's `shot()`, `foul()`, `ft()` and `act()` helpers, and add `reb <- function(id, parent, kind, team = 1L) act(id, "rebound", parent, ptype = kind, team = team)` and `assist`. `s` is `shot(1), foul(2, "personal", parent = 1, team = 2L), ft(3, 1, 1, 1, made = "missed")`. Each line is one test asserting `ends(...)`:

| Test | Rows after `s` | Expected ends |
|---|---|---|
| own rebound, put-back | `reb(4, 1, "offensive"), shot(5)` | `5` |
| two own rebounds (4 of the 87 targets) | `reb(4, 1, "offensive"), shot(5, made = "missed"), reb(6, 5, "offensive"), shot(7)` | `7` |
| assist before the rebound | `act(4, "assist", 1), reb(5, 1, "offensive"), shot(6)` | `6` |
| substitution before the rebound (Review Focus 3) | `act(4, "substitution"), reb(5, 1, "offensive"), shot(6)` | `6` |
| another group's end logged before the free throw | rows `shot(1), act(2, "turnover", ptype = "bad-pass", team = 2L), foul(3, "personal", parent = 1, team = 2L), ft(4, 1, 1, 1, made = "missed"), reb(5, 1, "offensive"), shot(6)` | `c(1L, 2L, 6L)` |
| rebound logged after the put-back (game 158) | `shot(4), reb(5, 1, "offensive")` | `4` |
| own rebound, turnover | `reb(4, 1, "offensive"), act(5, "turnover", ptype = "bad-pass")` | `5` |
| own rebound, then the period ends (Review Focus 2) | `reb(4, 1, "offensive"), act(5, "period", ptype = "end-of-quarter")` | `4` |
| chain (Review Focus 1) | `reb(4, 1, "offensive"), shot(5), foul(6, "personal", parent = 5, team = 2L), ft(7, 5, 1, 1, made = "missed"), reb(8, 5, "offensive"), shot(9)` | `9` |
| no later end in the period (game 178) | `reb(4, 1, "offensive"), shot(5, made = "missed"), act(6, "turnover", ptype = "bad-pass", team = 2L, q = 2L)` | `c(1L, 6L)` |
| opponent's end comes next | `reb(4, 1, "offensive"), shot(5, team = 2L)` | `c(1L, 5L)` |
| made free throw, unchanged | rows `shot(1), foul(2, "personal", parent = 1, team = 2L), ft(3, 1, 1, 1), shot(4, team = 2L)` | `c(3L, 4L)` |
| defensive rebound, unchanged | `reb(4, 1, "defensive", team = 2L), shot(5, team = 2L)` | `c(3L, 5L)` |
| other group's own rebound (game 437), unchanged | `reb(4, 99, "offensive"), reb(5, 1, "defensive", team = 2L), act(6, "turnover", ptype = "bad-pass")` | `c(3L, 6L)` |
| special foul on a basket, unchanged (game 53) | rows `shot(1), foul(2, "unsportsmanlike", parent = 1, team = 2L), ft(3, 1, 1, 1, made = "missed"), reb(4, 1, "offensive"), shot(5)` | `5` |
| no made shot in the group, unchanged | rows `foul(1, "personal", team = 2L), ft(2, 1, 1, 2), ft(3, 1, 2, 2, made = "missed"), reb(4, 1, "offensive"), shot(5)` | `5` |

- [ ] **Step 2: Run and see the new cases fail.** From `etl/tests`: `"$RSCRIPT" -e "testthat::test_file('test_possession_rules.R')"`. Expected: the own-rebound cases fail with an extra end `1`; the "unchanged" cases and the existing special-foul tests pass. Any "unchanged" case that fails is a wrong fixture: correct the test from the rule's current output before going on.

- [ ] **Step 3: Implement.** In `base`, mark a candidate per group: the row is the parent (`id == parent_action_id`), a made shot, and the group's last free throw is missed with an offensive rebound at a higher id. In dbplyr this takes two `mutate()` calls, because the rebound test reads the group's last free-throw id. After the existing final `mutate`, compute over rows with `final_end_poss` TRUE, by `(game_id, quarter)` in `id` order, `lead(team_id)`; a candidate whose value equals its own `team_id` joins back and sets `final_end_poss = FALSE`. That value is the next end after the made shot, which is the definition Task 2's SQL must use too. Comment why this runs last (Review Focus 2). Drop the helper columns in the final `select`.

- [ ] **Step 4: Run all ETL tests.** `"$RSCRIPT" -e "testthat::test_dir('etl/tests')"`. Expected: all pass.

- [ ] **Step 5: Commit** the plan, the rule and the tests: `etl: a made and-one the team rebounds keeps the possession open`. Do not push to `main`.

### Task 2: Selection, pin and generalised tools (no writes)

**Files:**
- Create: `sql/migrations/2026-11-03_and_one_possession_targets.sql`
- Create: `scripts/pin_and_one_targets.R`
- Create: `scripts/export_onoff_parity.R` (read-only; `--year <y> --out <file.csv>`)
- Rename and modify: `scripts/apply_special_foul_backfill.R` to `scripts/apply_possession_end_backfill.R`
- Modify: `scripts/verify_special_foul_possessions.R`, renamed `scripts/verify_possession_rule.R`

**Interfaces:**
- Consumes: Task 1's rule.
- Produces: both scripts take `--rule special-foul|and-one`, from one table at the top of each file: `list(sql = <targets file>, backup = <folder>)`. The targets SQL takes `$1::int4[]` of seasons and returns `game_id, id, parent_action_id, quarter, type, parameters_type`, the shape the backfill already reads. Backup folder for this rule: `exports/cold/backup-2026-11-03-and-one`. The confirm variable becomes `CONFIRM_POSSESSION_BACKFILL=1`.

- [ ] **Step 1: Write the targets SQL.** Read `df_pts_poss_lineups_longer_mv` offense rows, one row per action. Return the made-shot parent rows with `final_end_poss`, whose group's last free throw is missed with an offensive rebound at a higher id, and whose next end after the made shot (by id, same game and quarter) is the same team's. Read the view in one CTE between the comment lines `-- rows:begin` and `-- rows:end`, so Step 3 can swap that CTE for fixture rows. Do not use a correlated subquery over a CTE: this view has about 75,000 ends, and that form ran over 2 minutes on 2026-11-03. Use window functions.

- [ ] **Step 2: Check it against the Evidence table.** Expected: 42, 37 and 8 rows; shot 1780455 of game 178 absent; shots of games 60, 158 and 62434 present. Any other count is a stop: report it.

- [ ] **Step 3: Write the pin.** `scripts/pin_and_one_targets.R` loads the previous rule from the fixed commit (`git show dfd4667:etl/possession_rules.R` into a temp file), prints that SHA, and compares both rules in two passes:
  - **Fixtures:** every frame in `helper-possession-fixtures.R`, including the interleaved one. Run the previous rule through duckdb, pass its output to the targets SQL in place of the `rows` CTE (as `VALUES`, read-only), and run the new rule. Expected: the ends the new rule removed equal the SQL's targets, in every fixture.
  - **Archive:** `exports/cold/actions_clean.parquet` through duckdb, for `game_year` 2026 and 2027 only (the archive does not reproduce 2025). Report ends removed, ends added and the difference from the SQL's targets for the same games. Expected: none added; the removed set equals the targets for games the archive holds.

  A difference in either pass is a stop (Review Focus 4). Also print the seconds taken per game.

- [ ] **Step 4: Confirm Review Focus 3's counts and write the parity export.** Expected: 27 of the 87 change credited lineup, and 1 lineup-game falls to zero possessions; record the game and lineup, because Task 3 pins them. `scripts/export_onoff_parity.R` compares the same sources as `repair_onoff_d23.R`'s `parity()`, with its `ON Poss` and `OFF Poss` of 20 or more sample and its tolerances, and writes one row per difference: `team_id, player_id, source, column, view_value, reader_value`. Run it on 2025 and check that it shows the team 4 differences.

- [ ] **Step 5: Generalise the two scripts.** Both keep the special-foul rule's behaviour. In the verifier, order repeats by id so it matches check `AN`, and word its output for the chosen rule's targets. Add `--rule` to every usage line.

- [ ] **Step 6: Dry-run.** `Rscript scripts/apply_possession_end_backfill.R --rule and-one --seasons 2025,2026,2027`. Expected: `DRY RUN: 87 possession ends to remove`, all `shot`, and nothing written.

- [ ] **Step 7: Run the ETL tests and commit:** `backfill: possession-end tools take a rule; and-one targets and pin`.

### Task 3: Apply (production writes, needs approval)

**Files:**
- Baselines (local only): `.superpowers/sdd/2026-11-03-and-one/baseline_<season>.csv`, `parity_2025_before.csv`, `lineups_before.csv`
- Create: `app/tests/testthat/test-and-one-lineups-db.R`

- [ ] **Step 1: Record the starting point.** `verify_possession_rule.R --rule and-one --season <s> --baseline <file>` for 2025, 2026 and 2027; `export_onoff_parity.R --year 2025`, plus the `repair_onoff_d23.R --year=2025` trial (expected: exit 1 on the team 4 gap, rows changed 0); and `lineups_before.csv`, holding offense and defense points and possessions of the shot's and the next end's credited lineups for the 27 targets.

- [ ] **Step 2: Ask the user to approve the `main` push.** Then overlay the rule and tests on `origin/main` (method in memory `feedback-prod-deploy-explicit-approval`; never merge this branch whole). New games now follow the new rule. Confirm CI passes.

- [ ] **Step 3: Ask the user to approve the backfill.** Then: `CONFIRM_POSSESSION_BACKFILL=1 Rscript scripts/apply_possession_end_backfill.R --rule and-one --seasons 2025,2026,2027`. It flips the rows, refreshes in Phase 4 order, runs the full sub-lineup refresh, patches the Parquet archive and advances the cache marker last. Use `--resume-at sub-lineups` if it fails at that step.

- [ ] **Step 4: Gates**, per season, from `--compare <baseline>`. Every one must hold:
  - same-team repeats equal the Evidence table's "Expected after", allowing only for games loaded since the baseline;
  - each team-game's `off_poss_raw` drops by exactly the targets it held;
  - points and minutes unchanged in every team-game;
  - mirror check no worse than the baseline;
  - zero targets remain.

- [ ] **Step 5: Repair On/Off.** `repair_onoff_d23.R`, trial then `--apply`, for 2027 and 2026; ON rows must change 0. Then the 2025 trial and `export_onoff_parity.R --year 2025`, compared with the baseline by `(team_id, player_id, source, column)`. No new row and no larger difference may appear: if one does, stop and report, and do not force the apply. A smaller or vanished difference is reported. If the trial still stops only on those differences, 2025's repair stays unapplied and the gap is reported to the user. The script cannot apply around it, so if the backfill's possession changes are what the 2025 trial would repair, the user decides how to proceed.

- [ ] **Step 6: Pin the lineups and run the DB tests.** Write `test-and-one-lineups-db.R`, reading `lineups_before.csv` for its expected values and including a 2025 target. For each listed target: the shot's credited lineup has one fewer offensive possession, with the same points; the next end's lineup is unchanged; the opponent's defensive possessions drop by one on the matching lineup. For the lineup-game that reaches zero: its season row from `fetch_lineups_csv_v2` at `p_min_poss = 0` is still present, its points are kept, and its offensive rating is NULL if the season total reaches zero, or computed from the remaining possessions if not. Then, with `RUN_DB_TESTS=1`, `PG_PORT=5432` and the ETL role, run it with `test-player-reader-paths-db.R`, `test-team-reader-paths-db.R`, `test-ft-credit-db.R` and `test-dq-consecutive-possessions.R`. Expected: 0 failed and 0 skipped. Then the fast suite. Report any failure by name.

- [ ] **Step 7: Check check `AN` through the report's own runner.** Expected: 162, 163 and 58 repeats plus any new games, and none classified as this shape.

### Task 4: Record it (docs, needs approval for `main`)

**Files:**
- Modify: `docs/shared_definitions.md` (Possession row, D29, D30)
- Modify: `etl/run_data_quality_report.R`, `etl/dq_findings_html.R` (remove the and-one cause from check `AN`'s text)

- [ ] **Step 1: Update the definitions.** Add the Global Constraints sentence to the Possession row. Mark D30 fixed, with its headline reworded to "a made and-one followed by a missed free throw and the team's own rebound", the 87 rows and the date. State the limit: the end comes off only when the team's own next end in the period is recorded, so a shape with no later end or with the opponent's end next keeps the made shot's end (game 178 today), and such a possession can stay miscounted. Correct D29's "the rest look like gaps in the feed" to EuroCup's actual split: 29 feed, 12 likely real, 5 needing a look, of 46.

- [ ] **Step 2: Update check `AN`'s text**, run `test-dq-consecutive-possessions.R` and `etl/tests/test_dq_findings_html.R`, and commit: `docs: and-one own rebound fixed; D30 closed, D29 corrected`.

- [ ] **Step 3: With the user's approval, overlay on `main`.** Main's `shared_definitions.md` lacks other branch lines, so change only these rows there. Then delete `.superpowers/sdd/2026-11-03-and-one` and keep `exports/cold/backup-2026-11-03-and-one` as the rollback copy.

---

## Not in scope

- Offensive rebounds just before a period ends (34 in three seasons, 5 this season), usually right after the team's own made shot, where an event is probably missing as well.
- A miss with no rebound where the same team plays on (15), and EuroLeague's personal-plus-technical trips (19, 7 of them this season): both need their own look.
- Repeats from missing feed events, which we cannot repair.
- The 17 ends sitting on an assist row: the count is right.
- The cause of the 2025 team 4 On/Off gap.
