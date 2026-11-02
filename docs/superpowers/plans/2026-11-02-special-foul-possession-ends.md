# Special-Foul Free Throws Keep the Possession — Implementation Plan (both leagues, 2026-27 onward)

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** From the 2026-27 season on, free throws awarded for a technical, unsportsmanlike or disqualifying foul do not end the shooting team's possession, in both leagues; earlier seasons stay exactly as stored.

**Architecture:** Both providers renamed these foul codes for 2026-27, and both possession rules match fouls by exact name, so the new codes fall through to "ordinary free-throw trip ends the possession". The Israeli rule (`compute_possessions()`, R/dbplyr) gets a season-gated label-family match. The EuroLeague parser (`parser.py`) gets one foul-family predicate in place of its exact code sets. Then only the current season is reprocessed: Israeli 2027 games through the existing scoped ETL rerun, EuroLeague/EuroCup season 2026 through the offline re-stage and publish.

**Tech Stack:** R 4.4.2 + dbplyr (Israeli ETL), testthat + duckdb for the fixture test; Python 3 package `euroleague_possessions` + its tests; PostgreSQL (Supabase); GitHub Actions `etl-full.yml`.

**Spec:** this plan's evidence section, below; the investigation of 2026-11-02 (conversation record) and `docs/shared_definitions.md` § Possession.

## Evidence (measured 2026-11-02)

A possession end followed by the **same** team's next possession end in the same period ("same-team repeat") is the signature: the opponent never had the ball in between.

| League, season | Same-team repeats per game | From special-foul free throws |
|---|---|---|
| Israeli 2025, 2026 | 1.7, 1.5 | 123, 115 (unsportsmanlike / disqualifying; the rule never exempted them) |
| Israeli 2027 | 3.0 | 79 (new labels `technical-behavioral`, `technical-administrative`, `unsportsmanlike-flagrant`, `unsportsmanlike-disruptive`) |
| EuroLeague / EuroCup 2025 | 0.20 / 0.29 | ~0 (parser handles `CMT`, `CMU`) |
| EuroLeague / EuroCup 2026 | 1.21 / 1.86 | ~115 (new codes `CMT1`, `CMT2`, `CMU_DI`, `CMU_FL`; `CMT` and `CMU` now 0) |

Israeli rule: `etl/etl_onoff.R:976-993`, `sum_tech = parameters_type == "technical"`. EuroLeague rule: `euroleague/src/euroleague_possessions/parser.py:29-30` (`COMMITTED_FOULS`, `SPECIAL_FOULS`), and the exact checks at `:292` (`!= "CMU"`), `:386` (`== "CMT"`), `:485`, `:716`, `:723`, `:804`.

## Global Constraints

- **Not retroactive (user, 2026-11-02).** Israeli `game_year <= 2026` and EuroLeague/EuroCup `season <= 2025` must derive exactly what is stored today, including the Israeli unsportsmanlike defect. Record that defect as a deviation, do not fix it.
- First season under the new rule: Israeli `game_year = 2027`; EuroLeague and EuroCup `season = 2026`. Both mean 2026-27.
- One definition, worded identically for both leagues (rule 7 of `docs/shared_definitions.md`): "A free throw awarded for a technical, unsportsmanlike or disqualifying foul does not end the possession; the shooting team keeps the ball."
- Every database write (Tasks 3 and 5) needs the user's explicit go-ahead at that point; dry runs first. No production promotion of anything else.
- Database work on port 5432. Never run DB checks while a load test, the nightly ETL (~22:00 UTC) or EuroLeague load hours are running.
- Mixed line endings in `etl/etl_onoff.R`: splice with the raw-bytes method in root `CLAUDE.md` § Deploy Scripts, never the Edit tool; check `git diff --stat` is the size of the change.

## Review Focus

1. **A special-foul trip is the last event of a period.** The team keeps the ball but the period ends, so that trip's last free throw must still end the possession, or the period loses one. Israeli: fixture test in Task 2. EuroLeague: fixture test in Task 4.
2. **An unsportsmanlike foul on a made basket.** FIBA: the basket counts, free throws, and the ball goes back to the scoring team, so neither the made shot nor the free throws end the possession. Test in Tasks 2 and 4.
3. **Technical free throws shot by the team without the ball, and offsetting technicals.** Unchanged behaviour (no endpoint). Kept by the existing EuroLeague tests; Israeli fixture case in Task 2.
4. **A scoped Israeli run whose games span two seasons** (`etl_full(game_ids = c(old, new))`). The rule must follow each game's own season. `compute_possessions()` is called per game in `etl_full.R:557-559`; assert one season per call (Task 2).
5. **Re-running an old game.** An Israeli 2026 game or a EuroLeague 2025 game re-derived with the new code must equal what is stored. Israeli: fixture case with `game_year = 2026`. EuroLeague: offline re-stage comparison in Task 5, step 2.

---

### Task 1: Write the definition and the deviation

**Files:**
- Modify: `docs/shared_definitions.md` (the "Possession" row at line ~157, and the deviation register)

- [ ] **Step 1:** Add to the Possession row: the sentence from Global Constraints, "from 2026-27 (Israeli `game_year` 2027, EuroLeague `season` 2026)", and the two owners: `compute_possessions()` (`etl/etl_onoff.R`) and `is_special_foul()` (`euroleague/src/euroleague_possessions/parser.py`).
- [ ] **Step 2:** Add deviation **D29**: "Israeli 2024-25 and 2025-26 count the last made free throw of an unsportsmanlike or disqualifying foul as a possession end (about 120 phantom possessions a season, 0.35%; the fouled team's offensive rating reads about 0.3-0.5 pts/100 low). Kept by decision (user, 2026-11-02): the fix is not retroactive. EuroLeague 2025-26 and earlier are not affected." Status: open, accepted.
- [ ] **Step 3:** Commit: `docs: special-foul free throws keep the possession from 2026-27; D29 for earlier Israeli seasons`.

### Task 2: Israeli rule in `compute_possessions()`

**Files:**
- Modify: `etl/etl_onoff.R:946` (signature), `:976-993` (the rule), `:995-1015` (end-of-quarter targets), `:1270` (caller)
- Modify: `etl/etl_full.R:557-559` (caller)
- Test: `etl/tests/test_special_foul_possession.R` (new)
- Modify: `.github/workflows/r-tests.yml` — add `any::duckdb` to both dependency lists if it is absent

**Interfaces:**
- Produces: `compute_possessions(actions_tbl, game_year)`, where `game_year` is one integer. Also `SPECIAL_FOUL_FIRST_GAME_YEAR <- 2027L` and `SPECIAL_FOUL_PREFIXES <- c("technical", "unsportsmanlike", "disqualif")` next to it in `etl_onoff.R`.
- Consumes: nothing new.

- [ ] **Step 1: Write the failing tests** in `test_special_foul_possession.R`. Build small `actions_clean`-shaped frames (only the columns `compute_possessions()` reads), copy them into an in-memory duckdb connection with `dplyr::copy_to()`, run `compute_possessions(tbl, gy) |> collect()`, and assert `final_end_poss` per action id. Cases:
  - `unsportsmanlike-flagrant` foul by team B on team A, two made A free throws (`pct_ft` 0.5 and 1), then an A made shot: with `gy = 2027`, the second free throw is not an end and the shot is; with `gy = 2026`, both are ends (today's behaviour).
  - Each of `technical-behavioral`, `technical-administrative`, `unsportsmanlike-disruptive`, `unsportsmanlike`, `disqualifying` at 2027: the free throw is not an end.
  - `personal` foul trip at 2027: the last made free throw is an end (unchanged).
  - Plain `technical` at 2026: no end (unchanged).
  - Review Focus 1: special trip at 2027 followed by `end-of-quarter`: the last free throw **is** an end.
  - Review Focus 2: unsportsmanlike foul on a made lay-up in the same `parent_action_id` group at 2027: neither the shot nor the free throw is an end.
  - Review Focus 4: `compute_possessions(tbl, c(2026L, 2027L))` errors.
- [ ] **Step 2: Run and see them fail.** `"$RSCRIPT" -e 'testthat::test_file("etl/tests/test_special_foul_possession.R")'`. Expected: failures on the 2027 cases, and an "unused argument" error until the signature changes.
- [ ] **Step 3: Implement.**
  - Validate `game_year` (one non-NA integer, else `stop()`).
  - Choose the foul predicate in R before building the query. For `game_year >= SPECIAL_FOUL_FIRST_GAME_YEAR`, use `lower(parameters_type)` LIKE each prefix plus `%`, OR-ed. Otherwise keep exactly `parameters_type == "technical"`. Splice it into the `sum_tech` line with `!!`, so the dbplyr SQL for old seasons is unchanged.
  - In `eoq_targets`, for the new seasons only, add: a `freeThrow` whose group holds a special foul, when the next action in the quarter is `end-of-quarter`, is marked as an end.
  - Pass the game's season from both callers. In `etl_full.R` that is the `game_year` of `fetchable_sched`; add `stopifnot(length(unique(...)) == 1L)`.
- [ ] **Step 4: Run and see them pass**, then the existing ETL tests: `"$RSCRIPT" -e 'testthat::test_dir("etl/tests")'`. Expected: all pass.
- [ ] **Step 5: Before committing, confirm no non-foul label matches.** Run against `df_pts_poss_lineups_longer_mv` for 2027: `SELECT DISTINCT type, parameters_type ... WHERE lower(parameters_type) LIKE ANY (ARRAY['technical%','unsportsmanlike%','disqualif%'])`. Expected: only `type = 'foul'` rows. Any other type is a stop: report it.
- [ ] **Step 6: Commit** (raw-bytes splice for `etl_onoff.R`, then `git diff --stat`): `etl: special-foul free throws keep the possession from 2026-27`.

### Task 3: Reprocess the Israeli 2027 games (DB write, needs approval)

**Files:**
- Create: `scripts/verify_special_foul_possessions.R` (read-only; `--season 2027|2026`, `--baseline <file>`, `--compare <file>`)

**Interfaces:**
- Consumes: the Task 2 rule, deployed to `main` (the ETL workflow runs from `main`).
- Produces: the list of affected 2027 game ids, and before/after reports.

- [ ] **Step 1: Write the verifier.** It reuses the 2026-11-02 measurement queries (`docs/verification/special_foul_2026-11-02/poss_diff.R`, `poss_alt.R`, `poss_ft.R`; move their SQL, do not retype it; they read `S` for an output folder). It reports per season:
  - same-team repeats per game, and how many follow a special-foul free throw;
  - the off − def histogram;
  - the mirror check (team off = opponent def);
  - per team-game `off_poss_raw`, `def_poss_raw`, `off_points_raw`, `def_points_raw`, `off_minutes` from `team_metrics_by_game_mv`;
  - the affected game ids: 2027 games holding a free throw with `final_end_poss` whose parent foul label matches the prefixes.

  With `--baseline` it writes the per team-game table; with `--compare` it diffs against one.
- [ ] **Step 2:** Run `--season 2027 --baseline` (before any change). Commit the script.
- [ ] **Step 3:** With the user's go-ahead, deploy Task 2 to `main` (overlay method), then run the scoped ETL on the affected games: `gh workflow run etl-full.yml -f game_ids=<affected ids>`. Watch it to completion.
- [ ] **Step 4: Gates**, from `--season 2027 --compare`. Every one must hold:
  - special-foul free-throw endpoints = 0, except the end-of-period cases;
  - same-team repeats per game at most the 2026 residual excluding special fouls (~1.0);
  - mirror holds in every game;
  - points and minutes unchanged in every team-game;
  - each team-game's `off_poss_raw` changes by minus the number of special-foul trips it drew, and nothing else.

  A game failing a gate because the provider's re-fetched feed changed is reported, not forced.
- [ ] **Step 5:** Run `--season 2026` and confirm it matches the pre-change report exactly (not retroactive). Run the nightly DB tests that cover possessions (`test-ft-credit-db.R`, `test-team-reader-paths-db.R`).
- [ ] **Step 6:** Record the outcome and the run id in the D29 entry's sibling note in `docs/shared_definitions.md`. Commit.

### Task 4: EuroLeague parser — one foul-family predicate

**Files:**
- Modify: `euroleague/src/euroleague_possessions/parser.py:29-30, 292, 386, 485, 716, 723, 804`
- Modify: `euroleague/src/euroleague_possessions/staging.py:23` (`PARSER_VERSION` "0.2.1" → "0.2.2")
- Test: `euroleague/tests/test_parser.py` (new cases); fixtures under `euroleague/tests/fixtures/`

**Interfaces:**
- Produces, in `parser.py`:
  - `is_unsportsmanlike(code: str) -> bool`: `code == "CMU"` or it starts with `"CMU_"`;
  - `is_player_technical(code: str) -> bool`: `code == "CMT"` or `re.fullmatch(r"CMT\d", code)`; `CMTI` stays separate, as today;
  - `is_special_foul(code: str) -> bool`: the two above, or `code in {"B", "C", "CMTI"}`;
  - `is_committed_foul(code: str) -> bool`: `code in {"CM", "OF"}` or `is_special_foul(code)`.
- `CMD` stays outside the special set, as today. Adding it would change 2025 data, which this plan does not do.

- [ ] **Step 1: Write the failing tests**, in the style of `test_special_penalty_cluster_spans_administrative_rows`, with a fixture per new code:
  - `CMU_FL` foul then two `FTM` then a same-team `2FGM`: no endpoint on the free throws, one on the basket.
  - `CMT1` technical free throw with the other team in possession: no endpoint.
  - `CMU_DI` trip as the last event of a period: the trip's last free throw is the endpoint (`PERIOD_END_*` or the existing final-make reason; assert the endpoint exists, not the reason text).
  - Unsportsmanlike on a made basket (`2FGM` + `CMU_FL` + `FTM`): no endpoint on the basket or the free throw.
  - Predicate unit tests: `is_special_foul` is true for `CMT, CMT1, CMT2, CMTI, CMU, CMU_DI, CMU_FL, B, C` and false for `CM, CMD, CCH, OF, RV`.
- [ ] **Step 2: Run and see them fail.** `cd euroleague; .venv/Scripts/python.exe -m pytest tests/test_parser.py -q`. Expected: the new-code cases fail.
- [ ] **Step 3: Implement.** Replace every membership or equality test listed under Files with the predicates, and turn `COMMITTED_FOULS` / `SPECIAL_FOULS` into the functions (remove the frozensets, so nothing can keep using the exact sets). Bump `PARSER_VERSION`. Also use `is_committed_foul` for `fouls_committed` in `reconciliation.py:88`, so 2026 box-score reconciliation counts the renamed fouls.
- [ ] **Step 4: Run the whole package suite.** `.venv/Scripts/python.exe -m pytest -q`. Expected: all pass, including every existing parser test unchanged.
- [ ] **Step 5: Check the SQL consumers.** `sql/048_canonical_actions_consumer_refresh.sql:406` (and its predecessors 008, 011) lists foul codes by exact name for the event family. Query season 2026 player foul counts against the official box score (`fouls_committed`). If the renamed fouls are missing there, write migration `059_renamed_foul_codes.sql`: the same list plus `CMT1`, `CMT2`, `CMU_DI`, `CMU_FL`, as a gated numbered migration. If they are counted, record why, and do nothing.
- [ ] **Step 6: Commit:** `euroleague: renamed 2026-27 foul codes keep the possession (parser 0.2.2)`.

### Task 5: Reprocess EuroLeague and EuroCup 2026 (DB write, needs approval)

**Files:**
- Modify: the `--verify-only` check "one `parser_version` across the schema" (find it from `RUNBOOK.md` § What the verification checks) to "one `parser_version` per (competition, season)". Older seasons keep 0.2.1 by decision (not retroactive). Aggregates never mix seasons.
- Modify: `euroleague/RUNBOOK.md` (say so)

- [ ] **Step 1:** Change the version check and add a test for it next to the existing verification tests. Commit.
- [ ] **Step 2: Offline proof of non-retroactivity.** Re-stage 20 cached 2025 games of each competition with 0.2.2 (`load_games.py --skip-fetch`, staging only, no publish) and compare endpoints, reasons and possession numbers with the stored `euroleague.actions`. Expected: identical. Then confirm staging does not reuse a 0.2.1 checkpoint under 0.2.2. If it would, delete the 2026 staging checkpoints before step 4.
- [ ] **Step 3:** `scripts/probe_batched_publish.py --games 1-3 --season 2026` (rolls back). Expected: pass.
- [ ] **Step 4:** With the user's go-ahead, outside load hours: `load_games.py --games <all 2026 gamecodes> --season 2026 --competition E --skip-fetch --execute`, then the same with `--competition U`. `--verify-only` runs automatically and must pass.
- [ ] **Step 5: Gates.** Re-run the 2026-11-02 EuroLeague alternation query (`docs/verification/special_foul_2026-11-02/poss_alt_el_etl.R`, `poss_el_ft.R`; ETL role, since `app_readonly` cannot read `euroleague.actions`):
  - same-team repeats per game at or below the 2025 level (≤ 0.3);
  - free-throw endpoints parented to `CMT*` / `CMU*` = 0, except end-of-period;
  - possessions symmetric;
  - team points equal the box score;
  - season 2025 rows untouched (`parser_version` still 0.2.1, counts identical).
- [ ] **Step 6:** Update `euroleague/RUNBOOK.md` § Current checkpoint and `docs/shared_definitions.md` (owner row). Commit.

---

## Not in scope

- Israeli 2024-25 / 2025-26 unsportsmanlike defect: D29, accepted.
- The residual same-team repeats not caused by special fouls (about 1 per Israeli game, 0.2-0.3 per EuroLeague game; made shot → same-team shot, likely provider gaps such as a missing turnover or held ball). Not classified yet.
- Upper-case team names in about ten 2026 rows of `team_metrics_by_game_mv`.
