# Phantom Possession Ends — Implementation Plan (Israeli league, all three seasons)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Remove two kinds of possession end that no possession earned: an offensive rebound at the end of a period logged after the put-back it led to, and the turnover of an `OffensiveFoulUnsportsmanlike` foul by a team that did not have the ball.

**Architecture:** `compute_possessions()` gets one last step, after the and-one step, which works on the finished ends. Stored rows of 2024-25, 2025-26 and 2026-27 are corrected by the existing backfill under a new rule name, `phantom-ends`, because re-fetching old games is known to fail. A targets SQL is the rule's twin, pinned on fixtures and on the archive, then watched nightly.

**Tech Stack:** R 4.4.2 + dbplyr (ETL rule), testthat + duckdb (fixture tests), PostgreSQL on Supabase, Parquet (cold archive).

**Spec:** this plan's Evidence and Rulings sections; D29 and D30 in `docs/shared_definitions.md`; check `AN_consecutive_same_team_possessions`.

## Evidence (measured 2026-11-03, read-only)

| Season | Put-backs | Foul turnovers, no ball | Foul turnovers, brawl | Ends removed | Same-team repeats now | Expected after |
|---|---:|---:|---:|---:|---:|---:|
| 2024-25 (2025) | 13 | 11 | 0 | 24 | 162 | 138 |
| 2025-26 (2026) | 15 | 11 | 2 | 28 | 163 | 137 |
| 2026-27 (2027) | 4 | 0 | 0 | 4 | 61 | 57 |
| **Total** | **32** | **22** | **2** | **56** | **386** | **332** |

The two brawl ends remove no repeat, because those turnovers alternate teams. Every other removal clears exactly one.

**Put-backs.** The feed logs miss, made put-back, then the rebound of that miss. When the rebound is the last row before `end-of-quarter`, `eoq_targets` (`etl/possession_rules.R:91`) marks it, so one possession has two ends. Example: game 62 Q4, ids 620960 (miss), 620961 (made, end), 620962 (rebound, parent 620960, end). In all 32 the rebound's `parent_action_id` is lower than the made shot's id, with at most 5 seconds left, and the shot and rebound share a credited lineup. Two have odd rows, and both are accepted: game 62586 Q4 logs two rebounds of one miss, and game 365 Q1 (id 3650191) has a stale parent id after the opponent's miss.

**Foul turnovers.** The provider labels some defensive fouls `OffensiveFoulUnsportsmanlike` and adds a turnover linked to the foul. `etl/possession_rules.R:51` makes it an end and line 60 keeps it on purpose. 50 such fouls exist; 26 of the turnovers stay ends.

**Side effects.** 56 lineup-games lose one offensive possession, and none reaches zero, by game or by season. Points and minutes do not move. The local archive holds 53 of the 56 and the `cold-storage/latest` release holds 3.

## Rulings (user, from video, 2026-11-03)

- **Game 55 Q1 4:05:** Maccabi Tel Aviv never had the ball. Rayman's foul was defensive, and it must not end a possession.
- **Game 358 Q2 4:57:** Harper's offensive unsportsmanlike foul and turnover are real. Henry's and Mobley's came from the brawl after it and are not turnovers.
- **Game 108 Q4 3:53:** both ordinary offensive-foul turnovers are real and unrelated. Same second does not mean same stoppage.
- **Game 75 Q4 4:24:** Grant's basket counted, then an offensive foul. Weird; no rule is built on it.
- **Ordinary offensive fouls stay possession ends.** Only the `OffensiveFoulUnsportsmanlike` label is treated as unreliable.
- **Games 62580, 62584 Q1 and 82:** real offensive fouls; the rule keeps them.
- **Games 62439 and 75 Q2:** defensive fouls; the rule removes them.
- **Game 188 Q1:** Williams lost the ball and fouled in the same play, one turnover on him. The feed has two rows: the 5:53 ball-handling turnover stays an end and the 5:46 turnover does not. His turnover count still includes both, under the open decision below.

## Global Constraints

- Possession row wording: "An offensive rebound logged after the put-back it led to is not a second end. The turnover of an offensive unsportsmanlike foul does not end the possession when the team's own end was the last one recorded and it has had no shot or offensive rebound since, or when it follows another such turnover at the same second with no live play between."
- Live play means a `shot`, `rebound`, `freeThrow`, `steal`, `block`, `assist` or `deflection` row, by either team.
- Turnover counts do not change in this plan. The provider's box score counts these turnovers (21 of 21 checkable foulers match the feed), and whether to drop them is the user's open decision.
- Every database write, every push to `main` and every upload to the release needs the user's explicit go-ahead at that point. A push to `main` also redeploys production.
- Database work on port 5432, outside the nightly ETL (about 22:00 UTC) and EuroLeague load hours (18-23 UTC Tue-Fri). Check the disk before local writes.
- Never call `refresh_sub_lineups_stats_for_games()` in batches on a full season, and never re-fetch old games through `etl_full(game_ids=)` in bulk. The backfill script already follows both.
- Points and minutes must not move in any team-game. Only possession counts change.
- Run `tr -cd '\r' < <file> | wc -c` before editing each file. Where a file holds carriage returns, splice with the raw-bytes method in root `CLAUDE.md` § Deploy Scripts, and check that `git diff --stat` is the size of the change.
- Stage only this plan's paths: the working tree holds unrelated changes. No extra worktrees.

## Review Focus

1. **The foul's link to its turnover.** The turnover's `parent_action_id` is the foul's id, but some foul rows carry a different parent of their own, and matching on the foul's parent alone missed 596 of 1,471 turnovers in an earlier count. Link on the foul's id or its parent. Task 2's archive pass and count gate catch a miss.
2. **The rule reads actors, the SQL reads perspectives.** In `actions_clean` a foul's `team_id` is the fouler; in the view's offense rows it is the fouled team. Only the turnover's `team_id` may be used, because it is the same in both. Task 2's pin compares them.
3. **Order of steps.** "Previous end" means after the and-one step, because stored rows are already backfilled and the SQL reads those. Put-back and foul checks both read the same finished ends, so one removal cannot change another's result. Fixture `and_one_then_foul_turnover` in Task 1.
4. **A real offensive foul by a team that regained the ball with no recorded end.** Its own previous end is the last one recorded, but it has shot or rebounded since, so the end stays. Fixture `team_regained_ball` in Task 1. No such case exists today, so only the fixture pins it.
5. **2025 On/Off parity.** That season already stops `repair_onoff_d23.R` on the team 4 gap. Task 3 records it first, so the backfill can be shown not to have widened it.

---

### Task 1: The rule, tests first

**Files:**
- Modify: `etl/tests/helper-possession-fixtures.R` (add `PHANTOM_END_FIXTURES`, a named list of the same shape as `POSSESSION_FIXTURES`, and `at_second(rows, ids, rem)`, which sets `end_game_seconds_remaining` on those ids)
- Modify: `etl/tests/test_possession_rules.R`
- Modify: `etl/possession_rules.R` (after the and-one step, around lines 131-142)

**Interfaces:**
- Produces: `compute_possessions(actions_tbl)`, signature unchanged. `PHANTOM_END_FIXTURES` for Task 2.

- [ ] **Step 1: Write the failing tests.** One `test_that` loops over `PHANTOM_END_FIXTURES`, as the existing fixture test does. `OFU` is `"OffensiveFoulUnsportsmanlike"`. `bye <- act(99, "period", ptype = "end-of-quarter")`. In a foul block, team 1 fouls: `foul(a, OFU, team = 1L), act(a + 1, "foul-drawn", a, team = 2L), act(a + 2, "turnover", a, ptype = "other", team = 1L)`, called `F(a)` below; `F(a, kind)` uses another label, and `F2(a)` swaps the teams. Free throws `ft(5, 2, 1, 2, team = 2L), ft(6, 2, 2, 2, team = 2L)` are `FTS`.

| Fixture | Rows | Expected ends |
|---|---|---|
| `putback_logged_before_rebound` | `shot(1, made = "missed"), shot(2), reb(3, 1, "offensive"), bye` | `2` |
| `putback_and_one_then_rebound` | `shot(1, made = "missed"), shot(2), foul(3, "personal", parent = 2, team = 2L), ft(4, 2, 1, 1, made = "missed"), reb(5, 1, "offensive"), bye` | `2` |
| `rebound_in_order_then_putback` | `shot(1, made = "missed"), reb(2, 1, "offensive"), shot(3), bye` | `3` |
| `buzzer_rebound_no_made_shot` | `shot(1, made = "missed"), reb(2, 1, "offensive"), bye` | `2` |
| `earlier_basket_later_own_rebound` | `shot(1), shot(2, made = "missed"), reb(3, 2, "offensive"), bye` | `c(1, 3)` |
| `opponent_scored_before_buzzer_rebound` | `shot(1, made = "missed"), shot(2, team = 2L), reb(3, 1, "offensive"), bye` | `c(2, 3)` |
| `foul_after_own_turnover` (game 55) | `act(1, "turnover", ptype = "ball-handling"), F(2), FTS, shot(7, team = 2L)` | `c(1, 7)` |
| `foul_after_own_basket` (game 75 Q2) | `shot(1), F(2), FTS, shot(7, team = 2L)` | `c(1, 7)` |
| `foul_no_free_throws` (game 384) | `shot(1, made = "missed"), reb(2, 1, "defensive", team = 2L), F(3), shot(6, team = 2L)` | `c(1, 6)` |
| `real_foul_after_opponent_end` | `shot(1, team = 2L), F(2), FTS, shot(7, team = 2L)` | `c(1, 4, 7)` |
| `team_regained_ball` | `act(1, "turnover", ptype = "bad-pass"), shot(2, made = "missed"), reb(3, 2, "offensive"), F(4), shot(7, team = 2L)` | `c(1, 6, 7)` |
| `first_in_period` | `F(1), shot(4, team = 2L)` | `c(3, 4)` |
| `ordinary_offensive_foul_stays` | `shot(1), F(2, "personal"), shot(5, team = 2L)` | `c(1, 4, 5)` |
| `brawl` (game 358), ids 3-13 at one second | `shot(1, made = "missed", team = 2L), reb(2, 1, "defensive"), F(3), F2(6), F(9)`, then `ft(12, 3, 1, 2, team = 2L), ft(13, 3, 2, 2, team = 2L), shot(14, team = 2L)` | `c(1, 5, 14)` |
| `ordinary_same_second_stays` (game 108) | `shot(1, made = "missed", team = 2L), reb(2, 1, "defensive"), F(3, "personal"), F2(6, "personal"), shot(9)`, ids 3-8 at one second | `c(1, 5, 8, 9)` |
| `same_second_after_live_play` | `shot(1, team = 2L), F(2), ft(5, 2, 1, 1, team = 2L), F2(6), shot(9)`, ids 2-8 at one second | `c(1, 4, 8, 9)` |
| `and_one_then_foul_turnover` | and-one with own rebound `shot(1), foul(2, "personal", parent = 1, team = 2L), ft(3, 1, 1, 1, made = "missed"), reb(4, 1, "offensive"), shot(5)`, then `F(6), shot(9, team = 2L)` | `c(5, 9)` |

`F(a)` puts the turnover at `a + 2`, and `F2` uses the same id layout with the teams swapped. Check the expected ends against that layout when you write them, and correct a fixture only if the layout, not the rule, is wrong. Keep the existing test at line 33: its team has the ball, so it must still give `c(2, 5)`.

- [ ] **Step 2: Run and see the new cases fail.** From `etl/tests`: `"$RSCRIPT" -e "testthat::test_file('test_possession_rules.R')"`. Expected: the fixtures that remove an end fail with the extra end; the rest pass. A fixture marked as unchanged that fails is a wrong fixture: correct it from the rule's current output before going on.

- [ ] **Step 3: Implement.** In one last step of `compute_possessions()`, on the finished ends, by `(game_id, quarter)` in `id` order, set `final_end_poss = FALSE` on:
  - **A put-back's rebound:** `type == "rebound"`, `parameters_type == "offensive"`, an end, the previous end is the same team's made shot, and `parent_action_id` is lower than that shot's id.
  - **A foul turnover**, meaning `type == "turnover"` whose `parent_action_id` matches the id or the parent of a foul row labelled `OffensiveFoulUnsportsmanlike` in the same game, when either:
    1. its previous end is the same team's, and that team has no `shot` or offensive `rebound` row at an id between that end and the turnover; or
    2. the previous foul turnover in the same game and quarter has the same `end_game_seconds_remaining`, with no live-play row between them.

  Both tests read the same ends, before any of these removals. Comment why this runs after the and-one step, and drop the helper columns in the final `select`. The ETL uses both duckdb (tests) and PostgreSQL, so stick to window functions and joins both translate.

- [ ] **Step 4: Run all ETL tests.** `"$RSCRIPT" -e "testthat::test_dir('etl/tests')"`. Expected: all pass, including every `POSSESSION_FIXTURES` case unchanged.

- [ ] **Step 5: Commit** the plan, the rule and the tests: `etl: phantom possession ends from put-backs and mislabelled foul turnovers`. Do not push to `main`.

### Task 2: Selection, pin and tools (no writes)

**Files:**
- Create: `sql/migrations/2026-11-03_phantom_possession_end_targets.sql`
- Rename and modify: `scripts/pin_and_one_targets.R` to `scripts/pin_possession_rule_targets.R`
- Modify: `scripts/apply_possession_end_backfill.R`, `scripts/verify_possession_rule.R` (add to their `RULES` tables)
- Create: `scripts/patch_cold_release.R`
- Create: `app/tests/testthat/test-phantom-end-targets-db.R`

**Interfaces:**
- Consumes: Task 1's rule and `PHANTOM_END_FIXTURES`.
- Produces: rule name `phantom-ends`; backup folder `exports/cold/backup-2026-11-03-phantom-ends`. The targets SQL takes `$1::int4[]` of seasons and returns `game_id, id, parent_action_id, quarter, type, parameters_type`, with the `-- rows:begin` / `-- rows:end` markers of the and-one SQL.

- [ ] **Step 1: Write the targets SQL.** Same definition as Task 1 Step 3, over one row per action from the view's offense rows, as in `2026-11-03_and_one_possession_targets.sql`, adding `end_game_seconds_remaining` to `rows`. Window functions only: a correlated subquery over a CTE on this view ran over 2 minutes.

- [ ] **Step 2: Check it against the Evidence table.** Expected: 24, 28 and 4 rows, 32 of them rebounds and 24 turnovers. Present: 620962, 550131, 3580307, 3580312, 625860814, 3650191. Absent: 3580302, 1080817, 1080820. Any other count is a stop: report it.

- [ ] **Step 3: Generalise the pin.** `--rule and-one|phantom-ends`, from one table at the top of the script holding the targets SQL, the previous rule's commit and the fixture list. Phantom ends use previous-rule commit `413fd0f` and `PHANTOM_END_FIXTURES`. Add `end_game_seconds_remaining` to `VIEW_COLS`. Keep the header's note that the and-one passes 2 and 3 are one-shot, and that this rule's are too once the backfill runs. Run `--rule phantom-ends`. Expected: `PIN OK`; none added; removed set equals the targets for the games the archive holds. A difference is a stop (Review Focus 1, 2).

- [ ] **Step 4: Add the rule to both scripts' `RULES` tables** and to their usage lines. Dry-run: `Rscript scripts/apply_possession_end_backfill.R --rule phantom-ends --seasons 2025,2026,2027`. Expected: `DRY RUN: 56 possession ends to remove`, 32 `rebound` and 24 `turnover`, and nothing written.

- [ ] **Step 5: Write `scripts/patch_cold_release.R <dir> <out>`.** It takes every `exports/cold/backup-*/targets.csv` and flips `final_end_poss` to FALSE on those rows in `possessions.parquet` and `pws.parquet` of `<dir>`. It writes to `<out>` with the original Arrow schema, then reads the files back and exits 1 unless the schema and row order are the same, only `final_end_poss` changed, only target rows flipped, and no target end remains. It uploads nothing.

- [ ] **Step 6: Write the nightly test.** `test-phantom-end-targets-db.R`, gated like `test-and-one-targets-db.R`:

```r
test_that("the phantom-end targets SQL finds nothing in the current season", { ... expect_identical(nrow(targets), 0L, info = ...) })
test_that("the ruled games hold the agreed ends", {
  # (game, id, is it an end?) on the offense row
  rows <- list(c(62, 620961, TRUE), c(62, 620962, FALSE), c(55, 550125, TRUE), c(55, 550131, FALSE),
               c(358, 3580302, TRUE), c(358, 3580307, FALSE), c(358, 3580312, FALSE),
               c(108, 1080817, TRUE), c(108, 1080820, TRUE))
  ...
})
```

It fails until the backfill runs. Do not push it to `main` before then.

- [ ] **Step 7: Run the ETL tests and commit:** `backfill: phantom-end targets, pin and release patch tool`.

### Task 3: Apply (production writes, each needs approval)

**Files:**
- Baselines (local only): `.superpowers/sdd/2026-11-03-phantom-ends/baseline_<season>.csv`, `parity_2025_before.csv`

- [ ] **Step 1: Record the starting point.** `verify_possession_rule.R --rule phantom-ends --season <s> --baseline <file>` for 2025, 2026 and 2027; `export_onoff_parity.R --year 2025`; and the `repair_onoff_d23.R --year=2025` trial (expected: exit 1 on the team 4 gap, rows changed 0).

- [ ] **Step 2: Ask the user to approve the `main` push.** Then overlay the rule and ETL tests on `origin/main` (method in memory `feedback-prod-deploy-explicit-approval`; never merge this branch whole). New games now follow the new rule. Confirm CI passes.

- [ ] **Step 3: Ask the user to approve the backfill.** Then: `CONFIRM_POSSESSION_BACKFILL=1 Rscript scripts/apply_possession_end_backfill.R --rule phantom-ends --seasons 2025,2026,2027`. Use `--resume-at sub-lineups` if it fails at that step.

- [ ] **Step 4: Gates**, per season, from `--compare <baseline>`. Every one must hold:
  - same-team repeats equal the Evidence table's "Expected after", allowing only for games loaded since the baseline;
  - each team-game's `off_poss_raw` drops by exactly the targets it held;
  - points and minutes unchanged in every team-game;
  - mirror check no worse than the baseline (2 team-games differ today, both game 62452);
  - zero targets remain.

- [ ] **Step 5: Repair On/Off.** `repair_onoff_d23.R`, trial then `--apply`, for 2027 and 2026 when its trial changes rows; ON rows must change 0. Then the 2025 trial and `export_onoff_parity.R --year 2025`, compared with the baseline by `(team_id, player_id, source, column)`. No new row and no larger difference may appear: if one does, stop and report. If the trial still stops only on those differences, 2025's repair stays unapplied and the gap is reported.

- [ ] **Step 6: Run the DB tests.** With `RUN_DB_TESTS=1`, `PG_PORT=5432` and the ETL role: `test-phantom-end-targets-db.R`, `test-and-one-targets-db.R`, `test-and-one-lineups-db.R`, `test-player-reader-paths-db.R`, `test-team-reader-paths-db.R`, `test-ft-credit-db.R`, `test-dq-consecutive-possessions.R`. Expected: 0 failed and 0 skipped. Then the fast suite. Report any failure by name.

- [ ] **Step 7: Ask the user to approve the release upload.** Outside the ETL window, with no ETL run in progress: download `cold-storage/latest` into `exports/cold/backup-2026-11-03-phantom-ends-release/`, run `patch_cold_release.R`, upload only `possessions.parquet` and `pws.parquet` with `--clobber`, then download again and check that the hashes equal the patched files and the other three assets are unchanged. Expected: 3 ends flipped in each table.

### Task 4: Record it (docs, needs approval for `main`)

**Files:**
- Modify: `docs/shared_definitions.md` (Possession row, D29, new D31)
- Modify: `etl/run_data_quality_report.R`, `etl/dq_findings_html.R` (check `AN` text, if it names either shape)

- [ ] **Step 1: Update the definitions.** Add the Global Constraints wording to the Possession row. Correct D29's "the turnover of an offensive unsportsmanlike foul is an end" to point at D31. Add D31 with: both shapes, the Rulings, the 56 ends in the Evidence table's split, the repeats before and after, and four stated limits: turnover counts still include these turnovers because the provider's box score does (open decision); the rule infers who had the ball, so a mislabelled foul whose previous end is the opponent's stays an end; 18 of the 22 no-ball removals were not checked on video (games 55, 62439, 75 and 188 Q1 were, and all agree); and game 365's stale parent id is accepted.

- [ ] **Step 2: Check `AN` through the report's own runner and run** `test-dq-consecutive-possessions.R` and `etl/tests/test_dq_findings_html.R`. Expected: 138, 137 and 57 repeats plus any new games. Commit: `docs: phantom possession ends fixed; D31`.

- [ ] **Step 3: With the user's approval, overlay on `main`.** The nightly test, targets SQL, Possession, D29 and D31 rows only, plus any `AN` text change. Confirm CI, then delete `.superpowers/sdd/2026-11-03-phantom-ends` and keep both backup folders.

---

## Not in scope

- **Dropping these turnovers from turnover counts:** the user's open decision. Largest team TOV% change would be about 0.10; these teams would differ from the provider's box score by up to 3 a season.
- **The 26 foul turnovers that stay ends.** The label may be wrong in some, but the data cannot show it; check on video if a rule is ever wanted.
- **The 14 ordinary offensive fouls after the team's own end, and game 75's basket-then-foul:** the user ruled that they stay.
- **Repeats from missing feed events (about 290, including possible held balls):** the feed has no jump-ball event, and simulated gap-filling made the largest gaps worse.
- **EuroLeague:** its parser has no end-of-quarter rebound step and its feed has no such foul label. Its repeats were not re-examined here.
- **The 2025 team 4 On/Off gap** and the four games with no analytics.
