# Team Ratings Shooting Luck Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Tag a team's Net Rtg on the Team Ratings Summary view, all three leagues, when removing 3PT luck (own + opponents') and opponents' FT luck flips its sign.

**Architecture:** Every Team Ratings Summary reader appends six counts (`off_fg3_made`, `def_fg3_made`, `off_ftm`, `off_fta`, `def_ftm`, `def_fta`); a pure web rule `teamShootingLuck()` derives the luck against league-average rates; the Net cell shows a "Shooting luck" tag with the parts.

**Tech Stack:** PostgreSQL (Supabase; plpgsql functions, materialized views), Next.js/React/TypeScript (`web/`), vitest, R testthat for nightly DB pins.

**Spec:** `docs/superpowers/specs/2026-11-20-team-ratings-shooting-luck-design.md`

## Global Constraints

- Opponents' free throws only: `luck_ft_theirs = -100 x (def_ftm - rft x def_fta) / def_poss`. Own FT% is skill and is never a luck term.
- `luck_3pt_ours = 300 x (off_fg3_made - r3 x off_fg3_att) / off_poss`; `luck_3pt_theirs = -300 x (def_fg3_made - r3 x def_fg3_att) / def_poss`.
- `net_without_luck = net_rtg - luck_3pt_ours - luck_3pt_theirs - luck_ft_theirs`; flag when `net_rtg != 0 && net_without_luck * net_rtg <= 0`.
- League rates = league average over every team's own shots under the same game filters: `r3 = sum(off_fg3_made)/sum(off_fg3_att)`, `rft = sum(off_ftm)/sum(off_fta)`. Never from team-narrowed rows.
- All free throws count (technical/flagrant included). Predicates as the team readers use: FTA `type = 'freeThrow'`, FTM also `parameters_made = 'made'`; 3PM `type = 'shot' AND parameters_points = 3 AND parameters_made = 'made'`. Counts are from play-by-play only, never box scores.
- SQL changes only APPEND columns; every existing column must be row-identical before/after on a full season (2025-26).
- Branch `sql/team-luck` off `main`; PR; merge only after Preview live checks pass. Production deploy needs the user's explicit "deploy".
- After any DROP/CREATE: `scripts/apply_db_security.R` with `CONFIRM_DB_SECURITY_APPLY=1`, then curl a EuroLeague Team Ratings page and an Israeli one.
- Never run DB tests while another client queries as `app_readonly`; DB work on port 5432.
- Label "Shooting luck". Summary view only. No Shiny change (record a deviation).

## Review Focus

1. A team with zero opponent free throws (early season, clutch window) -> FT term 0, tag still computed from 3PT terms. Pinned in Task 4.
2. Clutch window with a team that has no defensive possessions -> NULL luck, no tag, no crash. Pinned in Task 4.
3. EuroLeague team filter (two teams selected) -> league rates still from all teams. Pinned in Task 5.
4. Net exactly 0 -> never tagged. Pinned in Task 4.
5. The previous-end read (rank arrows) on an old MV/function definition does not break the page: `previous` rows need no luck fields. Pinned in Task 5.

---

### Task 1: Israeli filtered function returns the counts

**Files:**
- Modify: `sql/functions/get_team_ratings_dynamic.sql` (`game_agg` ~L157-200, `base_agg`, `pivoted`, `final_calc`, `RETURNS TABLE` L28-55, final SELECT)
- Test: `app/tests/testthat/test-team-shooting-luck-db.R` (create)

**Interfaces:**
- Produces: `basketball_test.get_team_ratings_dynamic(...)` (same 23 params) returning, appended after `def_c3_known_att`: `off_fg3_made INT, def_fg3_made INT, off_ftm INT, off_fta INT, def_ftm INT, def_fta INT`.

- [ ] **Step 1: Snapshot the current output** for 2026 full season and one clutch call (`p_max_margin = 5, p_max_time_remaining = 300`) to `scratchpad` CSVs via a temp `.R` script (port 5432).
- [ ] **Step 2: Write the failing DB test** (gated `identical(Sys.getenv("RUN_DB_TESTS"), "1")`, loop `db_test_seasons()`): `test_that("ratings function own 3PM/FTM/FTA equal the Traditional reader")` -- for each team, `off_fg3_made == "3pm"`, `off_ftm == ftm`, `off_fta == fta` from `get_team_traditional_dynamic(..., p_side = 'offense')`, and `def_*` equal its `'defense'` side; both on the full season and the clutch call.
- [ ] **Step 3: Run it** (`RUN_DB_TESTS=1`, testthat on that file). Expected: FAIL, missing columns.
- [ ] **Step 4: Add the four sums to `game_agg`**, carry them through `base_agg` (SUM), `pivoted` (FILTER offense/defense), `final_calc`, and the final SELECT with `::int`. Deploy via DROP FUNCTION (exact signature) + CREATE through `scripts/deploy_sql_functions.R`, then the security re-apply. Call it once.
- [ ] **Step 5: Run the test.** Expected: PASS. If the Traditional reader disagrees, stop and report which definition differs (D-register question), do not adjust either.
- [ ] **Step 6: Diff the snapshot**: every pre-existing column identical for both calls. Time the full-season call (n>=5) before/after; report.
- [ ] **Step 7: Commit** `feat(sql): team ratings function returns 3PM and FT counts`.

### Task 2: Israeli season view returns the counts

**Files:**
- Modify: `sql/materialized_views/team_ppp_ratings_mv.sql`
- Test: `app/tests/testthat/test-team-shooting-luck-db.R`

**Interfaces:**
- Produces: `basketball_test.team_ppp_ratings_mv` with the same six appended columns.

- [ ] **Step 1: List dependents** (`pg_depend`/`pg_rewrite` on the view) and its current GRANTs; write them in the PR description. If any view depends on it, its rebuild goes in this task in L-order.
- [ ] **Step 2: Add the failing test** `test_that("season view counts equal the function on the full-season window")`: per team, the six counts and `off_poss`/`def_poss` from the view equal `get_team_ratings_dynamic` with the explicit full-season window.
- [ ] **Step 3: Run it.** Expected: FAIL.
- [ ] **Step 4: Extend the view** with the same predicates; stage it beside the live one, prove pre-existing columns identical (`scripts/restage_matview.R` pattern), swap, re-grant, security re-apply. Time `REFRESH MATERIALIZED VIEW` under `SET statement_timeout = '2min'`.
- [ ] **Step 5: Run the test.** Expected: PASS.
- [ ] **Step 6: Commit** `feat(sql): team_ppp_ratings_mv carries 3PM and FT counts`.

### Task 3: EuroLeague readers return the counts

**Files:**
- Create: `euroleague/sql/068_team_ratings_luck_counts.sql` (+ `068_..._before.sql` holding the live definitions it replaces)
- Modify: `euroleague/scripts/euroleague_function_contract.py`
- Test: `app/tests/testthat/test-team-shooting-luck-db.R`

**Interfaces:**
- Produces: the six appended columns on `euroleague.team_ppp_ratings_mv`, `get_team_ratings_pergame`, `get_team_ratings_dynamic`, `get_team_ratings_direct`, `get_team_dashboard_dynamic` (the readers `readFacts()` calls for the Summary view).

- [ ] **Step 1: Dump live definitions** (`pg_get_functiondef`, `pg_get_viewdef`) of all five into the `_before.sql`; diff them against 049/050/052 and note drift.
- [ ] **Step 2: Find each count's source.** `euroleague.team_four_factors_by_game` has `total_fg3_made` and `total_ft_attempts` but no FT made and no 3PA: add `total_ft_made` (and `total_fg3_att` if the ratings 3PA does not already come from the same fact) to it from the action fact it is built from, by rebuilding the table (no full-table UPDATE -- bloat rule). Direct/dynamic readers take the counts from the action rows they already scan.
- [ ] **Step 3: Add failing tests** (loop `db_test_euro_seasons()`, both competitions): pergame vs direct with the full-season window agree on the six counts; dashboard_dynamic vs dynamic agree; own counts equal `get_team_traditional_pergame` offence side.
- [ ] **Step 4: Run.** Expected: FAIL.
- [ ] **Step 5: Write the migration** (gated, idempotent, as prior numbered migrations), apply, re-grant, security re-apply, curl EuroLeague and EuroCup Team Ratings pages. Time each reader on a full 2025 season (n>=5) and the MV refresh.
- [ ] **Step 6: Run tests and the contract script.** Expected: PASS; existing columns identical to the `_before` snapshot.
- [ ] **Step 7: Commit** `feat(euroleague): team ratings readers return 3PM and FT counts (068)`.

### Task 4: `teamShootingLuck()` rule

**Files:**
- Modify: `web/src/rules/teamRatings.ts`
- Test: `web/src/rules/teamRatings.test.ts`

**Interfaces:**
- Produces: `export function leagueShootingRates(rows: Row[]): { r3: number | null; rft: number | null }` and `export function teamShootingLuck(rows: Row[], rates?: { r3: number | null; rft: number | null }): Row[]` adding `luck_3pt_ours`, `luck_3pt_theirs`, `luck_ft_theirs`, `net_without_luck`, `luck_flag` (rates default to `leagueShootingRates(rows)`); rows lacking any of `net_rtg, off_poss, def_poss, off_fg3_made, off_fg3_att, def_fg3_made, def_fg3_att, def_ftm, def_fta` come back unchanged.

- [ ] **Step 1: Write failing tests** in a `describe("teamShootingLuck")`:
  - two teams, `r3 = 0.35`, `rft = 0.75` given explicitly; team A `net_rtg 2.0, off_poss 1000, def_poss 1000, off_fg3_made 120, off_fg3_att 300, def_fg3_made 100, def_fg3_att 300, def_ftm 200, def_fta 250` -> `luck_3pt_ours 4.5`, `luck_3pt_theirs 1.5`, `luck_ft_theirs -1.25`, `net_without_luck -2.75`, `luck_flag true` (toBeCloseTo, 6).
  - same row with `net_rtg 10` -> `luck_flag false`.
  - `net_rtg 0` -> `luck_flag false`.
  - `def_fta 0, def_ftm 0` -> `luck_ft_theirs 0`.
  - `def_poss 0` -> theirs terms null, `net_without_luck null`, `luck_flag false`.
  - rows without `def_ftm` -> returned objects equal the input.
  - `leagueShootingRates` uses offence counts only: rows `{off_fg3_made 30, off_fg3_att 100, off_ftm 70, off_fta 100}` and `{off_fg3_made 40, off_fg3_att 100, off_ftm 80, off_fta 100}` -> `{ r3: 0.35, rft: 0.75 }`; zero attempts -> null.
- [ ] **Step 2: Run** `npm.cmd test -- teamRatings` in `web/`. Expected: FAIL (not exported).
- [ ] **Step 3: Implement** both functions in `teamRatings.ts`, mirroring `lineupThreePtLuck()` (`web/src/rules/lineups.ts:289`).
- [ ] **Step 4: Run.** Expected: PASS.
- [ ] **Step 5: Commit** `feat(web): teamShootingLuck rule`.

### Task 5: Apply the rule in the Team Ratings answer

**Files:**
- Modify: `web/src/server/teamRatingsRepo.ts` (`read()`, ~L262-300)
- Test: `web/src/server/teamRatingsRepo.test.ts`

**Interfaces:**
- Consumes: `teamShootingLuck`, `leagueShootingRates` (Task 4).
- Produces: Summary-view `rows` carry the five luck fields; `previous` untouched.

- [ ] **Step 1: Failing tests** with the file's existing fake `q`: (a) Summary view rows carry `luck_flag`; (b) EuroLeague with `teams = [a, b]`: rates come from an extra `readFacts` call made with `teams: []` (assert the fake saw a call without the team CSV, and the luck uses its rates); (c) `previous` rows lacking the counts do not throw; (d) Four Factors/Traditional views carry no luck fields.
- [ ] **Step 2: Run.** Expected: FAIL.
- [ ] **Step 3: Implement**: for `p.view === "summary"`, after `teamPace`, compute rates from `current` (or, when `euro && c.teams.length`, from a second read with `teams: []`), then `rows = teamShootingLuck(rows, rates)`.
- [ ] **Step 4: Run** the file and `npm.cmd test`. Expected: PASS.
- [ ] **Step 5: Commit** `feat(web): team ratings summary rows carry shooting luck`.

### Task 6: The "Shooting luck" tag on the Net cell

**Files:**
- Modify: `web/src/components/shiny/TeamRatingsTable.tsx` (Summary `metric("net_rtg", "Net Rtg")` ~L223), `web/src/components/shiny/LineupTable.tsx` (move `LuckNet` out), `web/src/data/layoutUi.json` (`tags.shooting`), `web/src/components/shiny/TableKey.tsx` / `tagText.ts` (`TagKind` gains `"shooting"`), `web/src/components/TeamRatingsClient.tsx` (legend tags)
- Create: `web/src/components/shiny/LuckNet.tsx`
- Test: `web/e2e/team-ratings.spec.ts`

**Interfaces:**
- Produces: `LuckNet({ row, value, parts })` where `parts` is a list of `{ label: string; value: number }` lines plus `without: number`; Lineups keeps its current lines ("lineup", "opponents"), Team Ratings shows "3PT, own", "3PT, opponents", "FT, opponents" and "Without shooting luck".

- [ ] **Step 1: Move `LuckNet`** to its own file, generalised to `parts`; Lineups output unchanged (run `npm.cmd test` and `lineups.spec.ts` render check).
- [ ] **Step 2: Add `tags.shooting`** to `layoutUi.json`: label `"Shooting luck"`, text `"Threes go in and out by chance, and a defence has no say over the other team's free throws. We re-scored every three, by both teams, at the league's 3P%, and the opponents' free throws at the league's FT%. If that flips this number from plus to minus (or back), it is marked: it rests mostly on shooting luck."`, and an example per league taken from a real flagged team in 2025-26 once Tasks 1-5 are applied.
- [ ] **Step 3: Write the e2e test**: Summary view, a season with a known flagged team -> its Net cell has a "Shooting luck" button whose popover shows the three part lines and "Without shooting luck"; an unflagged team has none. Check both themes.
- [ ] **Step 4: Wire the Net render** in `TeamRatingsTable` (Summary only, `r.luck_flag === true`) and the legend tag in `TeamRatingsClient`.
- [ ] **Step 5: Run** the spec with workers 1 (not while the user is testing). Expected: PASS.
- [ ] **Step 6: Commit** `feat(web): Shooting luck tag on Team Ratings`.

### Task 7: Contract, API, Ask, ship

**Files:**
- Modify: `docs/shared_definitions.md` (new row "Shooting luck (Team Ratings, since 2026-11-20)" beside the 3PT luck rows; deviation-register entry: Shiny Tab 3/9 have no tag, On/Off and Lineups have no FT term until phases 2-3), `web/src/server/apiCoverage.ts` and API shape tests if they list Team Ratings columns, `web/src/ask/semantics.ts` (team luck fields, following the lineup luck pattern)

- [ ] **Step 1: Write the contract row and deviations** with owners: counts `get_team_ratings_dynamic` / `team_ppp_ratings_mv` / EuroLeague 068 readers; rule `teamShootingLuck()`.
- [ ] **Step 2: API and Ask**: run `npm.cmd test`; update any column list that fails; add the fields to Ask's team-ratings semantics as lineup luck is.
- [ ] **Step 3: Full checks**: `npm.cmd test`, `npm.cmd run lint`, typecheck, R essential tests. Expected: all pass.
- [ ] **Step 4: Commit, push `sql/team-luck`, open the PR**, send the Preview link; wait for the Preview live checks. Merge only on the user's "deploy".
