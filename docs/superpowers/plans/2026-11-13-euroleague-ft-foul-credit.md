# EuroLeague Free Throws Credited to the Lineup That Drew the Foul: Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A EuroLeague or EuroCup free throw counts for the lineup that drew the foul, exactly as in the Israeli league. This closes deviations D1 and D39.

**Architecture:** This is the Israeli 2026-10-12 migration (`docs/superpowers/plans/2026-10-12-free-throw-foul-lineup-attribution.md`) carried to the `euroleague` schema.
- **Credit columns:** the central event fact, `euroleague.action_team_context_actions`, gains credit columns copied from each free throw's parent row. The parent is the foul, or the made shot of an and-one, from the same team's perspective. The fact's own refresh fills them, so the weekly load needs no new step.
- **Readers:** every reader that groups *events* by lineup switches to the credit columns. *Time* (stint and segment seconds) stays on the physical columns.
- **Rebuild:** derived per-game tables are rebuilt for every game and compared with a baseline before and after.

**Tech Stack:** PostgreSQL (Supabase, port 5432), the EuroLeague Python package (`euroleague/src/euroleague_possessions`, apply scripts in `euroleague/scripts/apply_0NN_*.py`, contract checks in `euroleague/scripts/euroleague_function_contract.py`), pytest. The web app and Shiny call the stored readers.

**Spec:** this conversation, 2026-11-13. The user asked to align the EuroLeague free-throw credit rule first, after the Basket work. They challenged the size ("it only should affect the instances where a sub was done between") and accepted it once every moved free throw was shown to have a substitution between its foul and itself. Contract: `docs/shared_definitions.md` rows "Credited lineup", "Free-throw trip", deviations D1 and D39.

### Facts measured 2026-11-13 (read-only)

- **Size:** 813 games carry facts: EuroLeague 2025-26 402, 2026-27 107; EuroCup 2025-26 195, 2026-27 109. The fact has 922,420 rows (both perspectives).
- **The sources are kept.** `refresh_actions_consumer_candidates` reads `euroleague.actions` (464k rows), `matchup_segments_actions`, `schedule`, `effective_period` and `full_rosters`, never `actions_raw`. `actions_raw` holds only part of the history. The fact can therefore be rebuilt for every game.
- **The parent link:** `euroleague.actions.synthetic_parent_order` is each free throw's parent: CM 26,888 free-throw rows, 2FGM 3,068 (and-ones), CMU 726, CMT 390, C 254, 3FGM 174, B 80, CMD 10, CMTI 4. Four FTA rows point to an FTA; they keep their own lineup.
- **How much moves:**

  | Competition, season | Free-throw rows | Rows that move | Points that move | Trips that move |
  |---|---|---|---|---|
  | EuroLeague 2025-26 | 31,598 | 8,997 | 7,074 of 24,766 | 3,736 of 8,677 |
  | EuroLeague 2026-27 | 8,374 | 2,541 | 1,950 of 6,460 | 1,054 of 2,320 |
  | EuroCup 2025-26 | 18,344 | 5,572 | 4,126 of 13,754 | 2,344 of 5,034 |
  | EuroCup 2026-27 | 9,224 | 2,706 | 1,982 of 6,848 | 1,133 of 2,533 |

- **Every move is a substitution.** All 8,997 moved rows (EuroLeague 2025-26) have an `IN`/`OUT` event of the same perspective between their parent and themselves. Another 5,441 have one and keep their lineup, because the other team substituted. Example: game 1, foul at order 70, team 2 subs Clark→Walker and Blatt→Dowtin at 72-75, free throws at 76-77.
- **Fact columns:** the physical lineup keys are `own_lineup`, `opp_lineup` (text arrays of player names), `own_starters`, `opp_starters` and `segment_id`. The trip markers are `synthetic_ft_trip_id`, `parent_play_type`, `ft_reverse_order` and `ts_possessions`, which is 1 on a trip's last free throw.
- **Live readers** of the fact or of `lineup_totals_by_game`: the functions `clutch_segment_durations`, `clutch_team_game_facts`, `fetch_lineups_direct`, `fetch_lineups_dynamic`, `fetch_lineups_pergame`, `filtered_team_game_facts`, `get_player_traditional_custom_clutch`, `get_player_traditional_standard_clutch`, `get_team_minutes_pergame`, `get_team_traditional_pergame`, `refresh_actions_consumer_candidates`, `refresh_default_clutch_for_games`, `refresh_default_clutch_player_for_games`, `refresh_lineup_totals_by_game`, `refresh_player_four_factors_by_game_for_games`, `refresh_player_stats_actions_for_games`, `refresh_player_traditional_by_game_for_games`, `refresh_sub_lineups`, `refresh_team_four_factors_by_game_for_games`, `select_team_game_facts`; the materialized view `sub_lineups_stats_mv`; the views `player_stats_action_context`, `ribbon_events_v`, `ribbon_margin_v`, `ribbon_segments_v`.
- **Load order:** the load publishes through `refresh_derived_for_games()` (`euroleague/src/euroleague_possessions/postgres_backend.py:629`), in this order: `refresh_actions_consumer_candidates`, `refresh_player_stats_actions_for_games`, `refresh_default_clutch_for_games`, `refresh_default_clutch_player_for_games`, `refresh_player_four_factors_by_game_for_games`, `refresh_team_four_factors_by_game_for_games`, `refresh_lineup_totals_by_game`, `refresh_player_traditional_by_game_for_games`, `refresh_sub_lineups`.

## Design

**The rule.** This is the Israeli rule, word for word:
- A free-throw row is credited to the lineup on its parent action's row, from the same team's perspective (`team_id`).
- The parent is a foul of any kind, or the made shot of an and-one.
- Every other row, and a free throw whose parent is itself a free throw, is credited to its own lineup.

**What "credited" covers.** It covers every event:
- points and possession ends (`final_end_poss`);
- shot and free-throw counts, rebounds and turnovers;
- the true-shooting trip (`ts_possessions`) and the offensive-rebound opportunity (`oreb_opportunities`).

It does not cover floor time.

**The five credit columns** on `euroleague.action_team_context_actions`:

| Column | Copied from the parent row's |
|---|---|
| `credit_own_lineup` | `own_lineup` |
| `credit_opp_lineup` | `opp_lineup` |
| `credit_own_starters` | `own_starters` |
| `credit_opp_starters` | `opp_starters` |
| `credit_segment_id` | `segment_id` |

**The reader rule.** It is the same as the Israeli one:
- **Events:** any aggregation that groups, filters or joins *event rows* on a physical lineup key uses the `credit_` column.
- **Time:** any expression that computes *time* (segment and stint seconds, event spans, window durations) keeps the physical column.
- **Clutch:** row-level clutch predicates (period, clock, scores) are untouched. A free throw is judged at the clock and score it was shot at.

**Decisions taken, so the executor does not reopen them:**

1. **Gameflow.** `ribbon_events_v` carries each event's credited five, which feeds the web's stint ratings. That closes D39, and the EuroLeague card then matches the Israeli one. `ribbon_segments_v` and `ribbon_margin_v` (bars, +/-, score) stay physical, as in the Israeli league.
2. **Team-level readers.** Team totals do not change. Only their starters filters move to the credited starters, as in the Israeli Task 6.
3. **Number and branch.** This is migration 062, on one branch, `sql/el-ft-credit`. Apply scripts follow `apply_061_ribbon_events_read_layer.py`.
4. **Scope.** Shiny's EuroLeague tabs and the web need no code change: they call the same readers. The contract rows change, and the web's EuroLeague Gameflow test fixtures are re-saved.

## Global Constraints

- **Writes:** every database write is a script that dry-runs by default (one transaction, every gate, then ROLLBACK) and commits only with an explicit flag, on port 5432. It is applied only after the user has seen the dry run and said yes.
- **Grants:** after any `DROP FUNCTION` or `DROP ... MATERIALIZED VIEW`, re-run `CONFIRM_DB_SECURITY_APPLY=1 Rscript scripts/apply_db_security.R`. Prefer `CREATE OR REPLACE` with the signature unchanged, which keeps grants.
- **SQL edits:** start from the **live** definition (`pg_get_functiondef` / `pg_get_viewdef`), never from an older migration file. Use exact string replacements, each required to match exactly once. Never use regex.
- **Timing:** time every rebuild on EuroLeague 2025-26, the largest season (402 games), and keep each per-game refresh call under the load's statement timeout. Never join a MATERIALIZED CTE per row (memory: "Time SQL changes on a FULL season").
- **Load windows:** do not run anything while the EuroLeague weekly load runs (GitHub Actions, hourly 18-23 UTC Tuesday to Friday). Check `gh run list` first.
- **Concurrency:** run database tests and gates with nothing else querying as `app_readonly`.
- **Python:** keep the package tests green (`cd euroleague && python -m pytest -q`).

## Review Focus

1. **A weekly-load game arrives with NULL credit columns.** If the fact's refresh re-inserts a game without filling them, every reader grouping on credit silently drops it. The fill happens inside `refresh_actions_consumer_candidates`, and the columns are `NOT NULL`. Pinned in Task 2 by gate G1 and the pytest contract check.
2. **A reader that groups events and time in one CTE.** Switching its keys moves floor time with the points. Pinned by gate G4 (floor time unchanged) in every reader task.
3. **The parent lookup must use the same game and the same perspective** (`game_id`, `team_id`). Otherwise a foul in a different perspective or game supplies the lineup. Pinned by G1.
4. **Starters filters.** A re-credited free throw takes the parent's starters counts too, or a "5 starters" filter keeps its points and drops its trip. Pinned by G5.
5. **A clutch window that holds the free throws but not the foul.** The free throws still count, under the foul's lineup. Pinned by G6. Measure the Israeli-style accepted shortfall, if any, and record it.

---

## Gates (implemented once, in Task 1)

`euroleague/scripts/verify_062_ft_credit.py`: `--baseline` stores snapshots in a scratch schema, `euroleague_ftc_baseline`, and `--check <object>` compares against them.

| Gate | Check | Pass |
|---|---|---|
| G1 | Credit columns well formed. No NULL. A free throw whose parent is a foul or a made shot carries the parent row's (same `game_id`, same `team_id`) lineup, starters and segment. Every other row carries its own | 0 violations |
| G2 | Team totals per team-game from the object (points, possessions, FGA, FTA, trips, OREB opportunities) equal the baseline | 0 differences |
| G3 | Each trip once: no `synthetic_ft_trip_id` lands in two credited lineups of one team-game | 0 |
| G4 | Floor time per team-game and per lineup unchanged | 0 differences |
| G5 | Starters: the rows a starters filter keeps carry the same credited lineup as their starters count | 0 violations |
| G6 | Clutch (last 5 minutes, margin 5): the free throws of a trip whose foul falls outside the window are counted under the foul's lineup | reported, not 0-gated (accepted deviation as in the Israeli run) |

Moved-points report: points and trips per competition and season that changed lineup. They must equal the "Facts" table, within the games loaded since 2026-11-13.

---

### Task 1: Baseline snapshot and gate script

**Files:**
- Create: `euroleague/scripts/verify_062_ft_credit.py` (psycopg, the connection helper the `apply_06x` scripts use).
- Test: `euroleague/tests/test_verify_062_ft_credit.py`, covering the pure comparison helpers only.

**Interfaces:**
- Produces `snapshot(conn, object_name) -> None` and `check(conn, object_name) -> list[str]` (problems, empty when it passes). Object names are the table, materialized view or reader names listed in Tasks 3-7.
- Produces `compare_frames(before, after, keys, columns) -> list[str]`, which is pure.

- [ ] **Step 1: Write the failing pure tests.**
  - `test_compare_frames_equal`: two equal frames give `[]`.
  - `test_compare_frames_reports_key_and_column`: a frame with one changed value names that key and column.
  - `test_compare_frames_missing_key`: a key present in one frame only is reported.
- [ ] **Step 2: Run them and see them fail.** `cd euroleague && python -m pytest -q tests/test_verify_062_ft_credit.py`. Expected: import error.
- [ ] **Step 3: Implement the helpers and the gate queries G1-G6 against the live objects.**
- [ ] **Step 4: Run the tests.** Expected: PASS.
- [ ] **Step 5: Take the baseline.** `python euroleague/scripts/verify_062_ft_credit.py --baseline` writes the scratch schema, after the user's yes. Expected: one snapshot per object in Tasks 3-7, with row counts printed.
- [ ] **Step 6: Commit** `euroleague: baseline and gates for the free-throw credit migration`.

### Task 2: Credit columns on the fact (migration 062)

**Files:**
- Create: `euroleague/sql/062_ft_credit_columns.sql`. It does three things:
  - `ALTER TABLE ... ADD COLUMN` for the five credit columns, nullable, then backfilled;
  - `CREATE OR REPLACE` of `refresh_actions_consumer_candidates`, built from its live definition, filling the credit columns in the same INSERT (a self-join on the inserted game's rows by `actions.synthetic_parent_order`);
  - `SET NOT NULL` after the backfill.
- Create: `euroleague/scripts/apply_062_ft_credit_columns.py`.
  - The backfill is one `UPDATE` per batch of games from the fact itself, with no refresh.
  - It prints the moved-points report.
  - It commits only with `--apply`.
- Modify: `euroleague/scripts/euroleague_function_contract.py`, so the contract knows the new columns.
- Test: `euroleague/tests/test_ft_credit_contract.py`.

- [ ] **Step 1: Write the failing contract test.** The function contract lists the five credit columns on the fact as `NOT NULL`, and `refresh_actions_consumer_candidates`'s definition file selects them.
- [ ] **Step 2: Run it and see it fail; write the migration and the apply script; run it again.** Expected: PASS.
- [ ] **Step 3: Dry run.** `python euroleague/scripts/apply_062_ft_credit_columns.py`. Expected:
  - G1 0 violations;
  - moved rows, points and trips per season equal the Facts table;
  - one game refreshed through the new function inside the transaction equals the backfilled values;
  - the per-game refresh time on a 2025-26 game is printed.
- [ ] **Step 4: STOP for the user's yes, then `--apply`.**
- [ ] **Step 5: Commit** `euroleague: migration 062 -- credit columns on the event fact`.

### The reader switch procedure (Tasks 3-7)

1. Read the object's **live** definition end to end, and its `refresh_*` function if it has one.
2. List every reference to `own_lineup`, `opp_lineup`, `own_starters`, `opp_starters` and `segment_id` that reads the fact, directly or through a CTE. Classify each as *event* or *time* by the reader rule, and put the classification in the commit message.
3. Replace each event reference with its `credit_` column by exact string edits. Where one CTE feeds both, select both the physical and the credit column under distinct names, and split the CTE only where ON/OFF membership is decided by the join (the Israeli `floor_data` lesson).
4. Write the new definition to `euroleague/sql/062_<step>_<object>.sql` and diff it against the live one: every changed line must be a classified event reference.
5. Apply it through that task's apply script, dry run first:
   - a function: `CREATE OR REPLACE`, then refresh every game in batches;
   - a materialized view: build it as `<name>__ftc_next`, gate it, grant it, then swap by rename in one short transaction.
6. Run `verify_062_ft_credit.py --check <object>`. Re-apply security if anything was dropped. Run pytest. Commit.

### Task 3: Lineup totals and the lineup readers

- **Objects:** `refresh_lineup_totals_by_game` (→ `lineup_totals_by_game`), `refresh_sub_lineups` (→ `sub_lineups`, `sub_lineups_stats_mv`), `fetch_lineups_direct`, `fetch_lineups_dynamic`, `fetch_lineups_pergame`, `refresh_default_clutch_for_games` (→ `default_clutch_lineup_totals_by_game`), `clutch_segment_durations` (time only: expect no change; confirm and record).
- **Gates:** G2, G3, G4, G5 on `lineup_totals_by_game` and `sub_lineups_stats_mv`; G6 on `fetch_lineups_dynamic` with a clutch window. Every lineup reader must return the same team totals as before.
- [ ] Apply the procedure to each object in the order listed. One commit per object, with the full-season refresh time in its message.

### Task 4: Player on/off and Four Factors

- **Objects:** `refresh_player_four_factors_by_game_for_games` and the on/off views built on it (`player_onoff_default_mv`, `player_advanced_stats_mv`: confirm from `pg_depend` which read it).
- **Gates:** G2 through the best-covered player per team-game (ON+OFF = team), G4.
- [ ] Apply the procedure. Expect the ON/OFF membership join to need its own physical-key CTE, as in the Israeli Task 4.

### Task 5: Player traditional and per-action player facts

- **Objects:** `refresh_player_stats_actions_for_games` (→ `player_stats_actions_by_game`), the view `player_stats_action_context`, `refresh_player_traditional_by_game_for_games`, `refresh_default_clutch_player_for_games`, `get_player_traditional_custom_clutch`, `get_player_traditional_standard_clutch`.
- **Gates:** G2 at team level, G4.
- [ ] Apply the procedure. **Games played must follow the floor lineup and possessions on floor the credited one.** The Israeli Task 7 lost five players' games played by switching both.

### Task 6: Team readers' starters filters

- **Objects:** `select_team_game_facts`, `filtered_team_game_facts`, `clutch_team_game_facts`, `refresh_team_four_factors_by_game_for_games`, `get_team_minutes_pergame` (time: expect no change), `get_team_traditional_pergame`.
- **Gates:**
  - G2 with no starters filter: identical, team totals unchanged;
  - G5 with a starters filter;
  - G4.
- [ ] Apply the procedure. Before switching anything, write a starters-filter test that fails against today's definitions; otherwise the switch is unproven (the Israeli Task 6 lesson).

### Task 7: Gameflow events and the contract

- **Objects:** `ribbon_events_v` (its credited five takes the credit columns). `ribbon_segments_v` and `ribbon_margin_v` stay physical; confirm and record.
- **Files:**
  - Modify: `docs/shared_definitions.md`. "Credited lineup" and "Free-throw trip" say "both leagues"; delete D1 and D39; the Gameflow stint rating row drops its D39 note.
  - Modify: `web/src/rules/__fixtures__/gameflow-*.json`. Re-save the EuroLeague fixtures with `scripts/save_gameflow_fixture.R`, then commit only the fixture JSON (the `gameflowLayout.json` trap in `web/CLAUDE.md`).
  - Modify: `euroleague/CLAUDE.md` line 3, which says "today: D13" only.
- **Gates:** on one EuroLeague game, the web Gameflow card's ORtg/DRtg per stint equal the lineup readers' for the same window. Unit tests 970+ pass (`npm.cmd run test -- --maxWorkers=1`).
- [ ] Apply the procedure to `ribbon_events_v`, then update the docs and fixtures. Push `sql/el-ft-credit`, wait for the Preview live checks, and merge only on the user's "deploy".

### Task 8: Data-quality finding and close-out

- **Files:**
  - Modify: `euroleague/scripts/run_euro_data_quality_report.R`. Add a finding for credit columns that disagree with the parent row (G1 as a finding), next to N12-N14.
  - Modify: this plan, adding an "As executed" section with the gate results, where the plan was wrong, and the accepted deviations.
- [ ] Run the report once. Expected: the new finding is 0. Commit, PR, deploy on "deploy".
- [ ] After the next weekly load: the new games' credit columns pass G1 (the load path, which a dry run cannot exercise). Then drop the scratch schema `euroleague_ftc_baseline`.

## Rollback

- Readers: re-apply the live definitions saved by each task's apply script (`euroleague/sql/062_*_before.sql`, written before any change). The credit columns can stay, since nothing reads them once the readers are back.
- Derived tables: refresh every game with the restored functions.
- The baseline schema holds the old values for comparison until Task 8.

## Order and stopping points

Tasks run in order. Stop for the user's yes before Task 1 Step 5 (baseline writes), Task 2 Step 4, and each reader task's first `--apply`. Tasks 3-6 can each be merged on their own; the numbers then move in steps, and that is acceptable because each step is correct on its own surface. Task 7's web change goes out only on "deploy".
