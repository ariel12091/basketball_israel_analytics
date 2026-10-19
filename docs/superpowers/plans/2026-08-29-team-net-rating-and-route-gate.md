# Team metrics: canonical Net Rating + shared route gate — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Make Summary and Four Factors agree on Net Rating in both leagues by rounding once from additive counts, and give both Team tabs one shared clutch-activity test for the season fast-path gate.

**Architecture:** This is the **curated safe slice** of `docs/specs/2026-08-29-cross-league-team-metrics-design.md` — spec §8's Net Rating rule and the §5.1 clutch-activity predicate only. No new combined readers, no request object, no app rewiring, no feature flag, no legacy deprecation. Every SQL change is an expression-level edit that leaves signatures, column names, column order and types untouched, so each is verifiable by output diff. R changes are pure helpers covered by the existing offline test suite.

**Tech Stack:** R 4.4.2 / Shiny / testthat; PostgreSQL on Supabase (schemas `basketball_test`, `euroleague`); Python migration appliers under `euroleague/scripts/`.

**Spec:** `docs/specs/2026-08-29-cross-league-team-metrics-design.md` (§5.1, §8)

**Audit:** `docs/plans/2026-08-29-team-dashboard-parity-audit.md`

## Global Constraints

- **No DDL is applied by this plan.** Tasks 3-4 stage migration SQL and its applier; running `--apply` is a separate, explicitly approved step. Everything else is offline.
- **Ranks are out of scope and must not change.** `euroleague.team_ppp_ratings_mv.off_rank/def_rank/net_rank` are read as the opponent-strength filter by `get_team_{ratings,four_factors}_{pergame,dynamic,direct}` (see `euroleague/sql/037_pergame_team_readers.sql:90-93`), and `team_four_factors_mv.off_rank/def_rank/net_rank` feed `onoff_compute`/`four_factors_compute`. Changing a `dense_rank()` ORDER BY from rounded to unrounded would silently move teams between "top N"/"bottom N" opponent bands across Tabs 8, 9 and 10. Spec §8's rank clause is **deferred**, not implemented.
- **Only `net_rtg` value expressions change.** Do not touch `off_ppp`/`def_ppp` output expressions — they stay rounded to 1dp and their displayed values must be byte-identical before and after.
- **`CREATE OR REPLACE FUNCTION` preserves EXECUTE grants; `DROP` does not.** Use REPLACE for functions (signatures are unchanged). Materialized views cannot be replaced and must be DROP+CREATE, which wipes GRANTs — Task 4 re-grants explicitly.
- **Do not stack on unapplied migration 045.** Branch from `main`, not from `sql/tab8-query-shape`. The new EuroLeague migration number is **046**; 045 and 046 touch disjoint objects (045: `onoff_compute`/`four_factors_compute`; 046: team readers/MVs), so either order applies cleanly.
- **Canonical Net Rating expression**, used verbatim everywhere (only the relation alias varies):

```sql
ROUND(
  100.0 * off_pts / NULLIF(off_poss, 0)
  - 100.0 * def_pts / NULLIF(def_poss, 0)
, 1)
```

- Israeli test command: `cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e 'library(testthat); test_dir("tests/testthat", filter="<name>", reporter="summary")'`
- **Line endings will lie to you.** Measured against `main` on 2026-08-29:

  | File | CR in git blob | CR on disk | lines |
  |---|---:|---:|---:|
  | `app/R/server_tab9_euro_team.R` | 0 | 512 | 566 |
  | `app/R/server_tab3.R` | 2010 | 2010 | 2079 |
  | `app/R/helpers.R` | 2402 | 2402 | 2754 |
  | `sql/materialized_views/team_four_factors_mv.sql` | 0 | 64 | 131 |
  | `sql/functions/get_team_four_factors_dynamic.sql` | 0 | 491 | 491 |
  | `app/tests/testthat/test-euro-clutch.R` | 0 | 127 | 134 |

  Several targets are LF in git but CRLF on disk, and `server_tab3.R` / `helpers.R` are genuinely **mixed**. An editor that normalises endings will rewrite the whole file and bury a three-line change in a 2000-line diff. After every edit, run `git diff --stat <file>` and confirm the changed-line count matches what you actually changed; cross-check content with `git diff --ignore-cr-at-eol <file>`. If the count is implausible, revert and redo the edit on bytes (`sed`/`python`) rather than committing it. Do **not** reach for `-c core.autocrlf=false` as a fix — it is per-file and has restaged 363 unrelated lines in this repo before.

---

### Task 1: One shared clutch-activity predicate

Both Team tabs inline their own version of "has the user asked for a clutch window", and both are incomplete: `app/R/server_tab9_euro_team.R:102-111` omits `margin_status` **and** `ot_margin_filter`; `app/R/server_tab3.R:958` checks `margin_status` but omits `ot_margin_filter`.

This is **latent, not live**: `clutch_filter_ui()` (`app/R/global.R:549-565`) uses sliders for margin and minutes, so enabling clutch always sets `max_margin`; `resolve_clutch_params()` (`app/R/helpers.R:869-877`) forces `ot_margin_filter = FALSE` when clutch is off; and neither tab bookmark-restores these inputs. The fix is de-duplication that closes the gap before some future caller (a preset, a URL restore, a non-slider control) makes it reachable. Do not describe it in the commit message as a live user-visible bug.

**Files:**
- Modify: `app/R/helpers.R` (add `clutch_active()` after `clutch_reader_kind()`, which ends at line 653)
- Modify: `app/R/server_tab3.R:958`
- Modify: `app/R/server_tab9_euro_team.R:102-111`
- Test: `app/tests/testthat/test-euro-clutch.R`

**Interfaces:**
- Consumes: nothing from earlier tasks.
- Produces: `clutch_active(p)` -> single `TRUE`/`FALSE`. Reads `p$max_margin`, `p$margin_status`, `p$max_time_remaining`, `p$ot_margin_filter`. Tasks 2-4 do not use it.

- [ ] **Step 1: Write the failing test**

Append to `app/tests/testthat/test-euro-clutch.R`:

```r
test_that("clutch_active() classifies all four clutch fields", {
  none <- list(max_margin = NA_integer_, margin_status = NA_character_,
               max_time_remaining = NA_integer_, ot_margin_filter = FALSE)
  expect_false(clutch_active(none))

  # Each field alone activates.
  expect_true(clutch_active(modifyList(none, list(max_margin = 5L))))
  expect_true(clutch_active(modifyList(none, list(max_time_remaining = 300L))))
  expect_true(clutch_active(modifyList(none, list(margin_status = "leading"))))
  expect_true(clutch_active(modifyList(none, list(ot_margin_filter = TRUE))))

  # "all" and "" are the blank sentinels, not a filter.
  expect_false(clutch_active(modifyList(none, list(margin_status = "all"))))
  expect_false(clutch_active(modifyList(none, list(margin_status = ""))))
})

test_that("both Team tabs gate the season fast path through clutch_active()", {
  team_server <- read_repo_txt("R", "server_tab9_euro_team.R")
  il_server <- read_repo_txt("R", "server_tab3.R")
  expect_match(team_server, "clutch_active(p)", fixed = TRUE)
  expect_match(il_server, "clutch_active(p)", fixed = TRUE)
  # Neither tab may keep its own copy of the margin/time test.
  expect_false(grepl("!is.na(p$max_margin) || !is.na(p$max_time_remaining)",
                     team_server, fixed = TRUE))
})
```

- [ ] **Step 2: Run it and confirm it fails**

```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e 'library(testthat); test_dir("tests/testthat", filter="euro-clutch", reporter="summary")'
```

Expected: FAIL — `could not find function "clutch_active"`.

- [ ] **Step 3: Add the helper**

Insert into `app/R/helpers.R` immediately after `clutch_reader_kind()` (after line 653). It reuses that function's own normalisation so the two can never disagree:

```r
# The one test for "did the user ask for a clutch window". Both Team tabs use
# it to decide whether the untouched-season fast path is still valid. It must
# consider all four clutch fields: a status-only or OT-only request is still a
# clutch request, and answering it from the unfiltered season MV would silently
# drop the filter. clutch_reader_kind() picks WHICH reader; this picks WHETHER
# the season cache is allowed at all.
clutch_active <- function(p) {
  status <- blank_to_na_character(p$margin_status)
  status <- if (length(status) == 1L && !is.na(status)) status else "all"
  is_set <- function(x) {
    x <- suppressWarnings(as.integer(x))
    length(x) == 1L && is.finite(x)
  }
  is_set(p$max_margin) || is_set(p$max_time_remaining) ||
    !identical(status, "all") || isTRUE(p$ot_margin_filter)
}
```

- [ ] **Step 4: Route the EuroLeague gate through it**

In `app/R/server_tab9_euro_team.R`, replace the clutch line inside `et_fallback_needed()` (currently `!is.na(p$max_margin) || !is.na(p$max_time_remaining) ||`) so the reactive body reads:

```r
  et_fallback_needed <- reactive({
    p <- et_params()
    (p$start_d != p$bounds$start) || (p$end_d != p$bounds$end) ||
      !is.na(p$team_ids_csv) || !is.na(p$phase_csv) || !is.na(p$opp_ids_csv) ||
      !is.na(p$home_away) || !is.na(p$outcome) || !is.na(p$rank_side) ||
      clutch_active(p) ||
      !is.na(p$min_gn) || !is.na(p$max_gn) || !is.na(p$last_n) ||
      !is.na(p$st_off_min) || !is.na(p$st_off_max) ||
      !is.na(p$st_def_min) || !is.na(p$st_def_max)
  })
```

- [ ] **Step 5: Route the Israeli gate through it**

In `app/R/server_tab3.R`, replace line 958:

```r
    has_clutch <- !is.na(p$max_margin) || (!is.na(p$margin_status) && p$margin_status != "all") || !is.na(p$max_time_remaining)
```

with:

```r
    has_clutch <- clutch_active(p)
```

- [ ] **Step 6: Run the focused tests, then the full suite**

```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e 'library(testthat); test_dir("tests/testthat", filter="euro-clutch", reporter="summary")'
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e 'library(testthat); test_dir("tests/testthat", filter="tab3", reporter="summary")'
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" tests/testthat.R
```

Expected: all PASS. The full suite matters here because `server_tab3.R` is live.

- [ ] **Step 7: Commit**

```bash
git add app/R/helpers.R app/R/server_tab3.R app/R/server_tab9_euro_team.R app/tests/testthat/test-euro-clutch.R
git commit -m "Share one clutch-activity test across both Team tabs"
```

---

### Task 2: Israeli canonical Net Rating

Israeli **Ratings** already rounds once from unrounded inputs (`sql/materialized_views/team_ppp_ratings_mv.sql:62`, `sql/functions/get_team_ratings_dynamic.sql:288`) — leave both alone; they are the reference. Israeli **Four Factors** subtracts already-rounded 1dp rates in three places. Those three are the defect.

Every affected relation already carries `off_pts`, `off_poss`, `def_pts`, `def_poss` in the same CTE (aliased `p`), so the canonical expression is a local substitution with no new joins.

**Files:**
- Modify: `sql/materialized_views/team_four_factors_mv.sql:124`
- Modify: `sql/functions/get_team_four_factors_dynamic.sql:318` and `:476`
- Test: `app/tests/testthat/test-tab3-team-ratings-contracts.R`

**Interfaces:**
- Consumes: nothing.
- Produces: no R-visible interface change. `net_rtg` stays `numeric`, same column name and ordinal position.

- [ ] **Step 1: Write the failing contract test**

Append to `app/tests/testthat/test-tab3-team-ratings-contracts.R`:

```r
test_that("Israeli Four Factors Net Rating rounds once from additive counts", {
  read_sql <- function(...) paste(readLines(file.path("..", "..", "..", "sql", ...),
                                            warn = FALSE), collapse = "\n")
  mv <- read_sql("materialized_views", "team_four_factors_mv.sql")
  fn <- read_sql("functions", "get_team_four_factors_dynamic.sql")

  # The defect: subtracting two values that were each already rounded to 1dp.
  expect_false(grepl("ROUND(p.off_ppp - p.def_ppp, 1)", mv, fixed = TRUE))
  expect_false(grepl("ROUND(p.off_ppp - p.def_ppp, 1)", fn, fixed = TRUE))

  # The canonical form, matching Israeli Ratings and spec section 8.
  canonical <- "100.0 * p.off_pts / NULLIF(p.off_poss, 0)"
  expect_match(mv, canonical, fixed = TRUE)
  expect_match(fn, canonical, fixed = TRUE)
})
```

The relative path assumes testthat runs with the test file's directory as working directory. If the `readLines` call errors, print `getwd()` from inside the test and correct the number of `".."` segments — do not weaken the assertions to make it pass.

- [ ] **Step 2: Run it and confirm it fails**

```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e 'library(testthat); test_dir("tests/testthat", filter="tab3-team-ratings-contracts", reporter="summary")'
```

Expected: FAIL on the two `expect_false` assertions.

- [ ] **Step 3: Fix the materialized view**

In `sql/materialized_views/team_four_factors_mv.sql`, replace line 124:

```sql
  ROUND(p.off_ppp - p.def_ppp, 1) AS net_rtg
```

with:

```sql
  -- Round once, from the additive counts. Subtracting two values that were
  -- each already rounded to 1dp disagreed with Ratings by 0.1 for 4 of 14
  -- teams in the 2026-08-29 audit.
  ROUND(
    100.0 * p.off_pts / NULLIF(p.off_poss, 0)
    - 100.0 * p.def_pts / NULLIF(p.def_poss, 0)
  , 1) AS net_rtg
```

- [ ] **Step 4: Fix both sites in the dynamic function**

In `sql/functions/get_team_four_factors_dynamic.sql`, apply the identical substitution at line 318 and line 476. Both currently read `      ROUND(p.off_ppp - p.def_ppp, 1) AS net_rtg`; both become:

```sql
      ROUND(
        100.0 * p.off_pts / NULLIF(p.off_poss, 0)
        - 100.0 * p.def_pts / NULLIF(p.def_poss, 0)
      , 1) AS net_rtg
```

Use exact-string edits, never a regex — a lazy multi-line regex has truncated a SQL body in this repo before.

- [ ] **Step 5: Verify the diff is exactly three expressions**

```bash
git diff --stat sql/
git diff sql/
```

Expected: two files changed; only `net_rtg` lines and the added comment appear. If any `off_ppp`/`def_ppp` output line moved, revert and redo — those must stay byte-identical.

- [ ] **Step 6: Run the tests**

```bash
cd app && "/c/Program Files/R/R-4.4.2/bin/Rscript.exe" -e 'library(testthat); test_dir("tests/testthat", filter="tab3", reporter="summary")'
```

Expected: PASS.

- [ ] **Step 7: Commit**

```bash
git add sql/materialized_views/team_four_factors_mv.sql sql/functions/get_team_four_factors_dynamic.sql app/tests/testthat/test-tab3-team-ratings-contracts.R
git commit -m "Israeli Team Four Factors: round Net Rating once from additive counts"
```

---

### Task 3: EuroLeague migration 046 — Ratings readers

The EuroLeague asymmetry is the mirror image of Israel's: all three **Four Factors** readers already round once — leave them alone. All three **Ratings** readers subtract pre-rounded rates.

Live definitions to supersede (verified as the newest definition of each name across `euroleague/sql/*.sql`):

| Function | Newest definition | Defect line |
|---|---|---|
| `euroleague.get_team_ratings_pergame` | `037_pergame_team_readers.sql` | 139 |
| `euroleague.get_team_ratings_dynamic` | `019_clutch_read_layer.sql` | 465 |
| `euroleague.get_team_ratings_direct` | `031_direct_team_custom_readers.sql` | 116 |

Functions only in this task; the two season MVs are Task 4 because they need DROP+CREATE and re-granting.

**Files:**
- Create: `euroleague/sql/046_team_net_rating_single_round.sql`
- Test: `euroleague/tests/test_team_net_rating.py`

**Interfaces:**
- Consumes: nothing.
- Produces: three `CREATE OR REPLACE FUNCTION` statements with **identical signatures and RETURNS TABLE column lists** to the definitions above. Task 4 appends the two MV statements to the same migration file.

- [ ] **Step 1: Write the failing test**

Create `euroleague/tests/test_team_net_rating.py`:

```python
"""Migration 046 must round Net Rating once, and must not touch ranks."""
from pathlib import Path

MIGRATION = (Path(__file__).resolve().parent.parent
             / "sql" / "046_team_net_rating_single_round.sql")


def _sql():
    return MIGRATION.read_text(encoding="utf-8")


def _squashed():
    return "".join(_sql().split())


def test_migration_exists():
    assert MIGRATION.is_file(), "046 has not been written yet"


def test_no_pre_rounded_net_rating():
    squashed = _squashed()
    assert "round(r.off_ppp-r.def_ppp,1)" not in squashed, (
        "pre-rounded Net Rating still present"
    )


def test_uses_canonical_additive_expression():
    squashed = _squashed()
    assert "100.0*r.off_pts/nullif(r.off_poss,0)" in squashed.lower()
    assert "100.0*r.def_pts/nullif(r.def_poss,0)" in squashed.lower()


def test_ranks_are_left_on_rounded_inputs():
    """Ranks feed the opponent-strength filter in Tabs 8/9/10. Out of scope."""
    squashed = _squashed()
    assert "dense_rank()OVER(ORDERBYr.off_ppp-r.def_pppDESC)" in squashed, (
        "rank expressions must be carried over unchanged"
    )


def test_functions_are_replaced_not_dropped():
    """DROP FUNCTION wipes app_readonly EXECUTE grants; REPLACE preserves them."""
    upper = _sql().upper()
    assert "DROP FUNCTION" not in upper
    assert upper.count("CREATE OR REPLACE FUNCTION") == 3
```

- [ ] **Step 2: Run it and confirm it fails**

```bash
cd euroleague && python -m pytest tests/test_team_net_rating.py -v
```

Expected: FAIL — `046 has not been written yet`.

- [ ] **Step 3: Write the migration**

Create `euroleague/sql/046_team_net_rating_single_round.sql`. For each of the three functions, **copy the entire current definition verbatim** from the file named in the table above, then change exactly one line — the `net_rtg` projection. Do not retype the bodies from memory and do not reformat them; copy the text and verify with a diff in Step 4.

Header comment for the file:

```sql
-- Migration 046: canonical single-round Net Rating for EuroLeague Team Ratings.
--
-- Defect: each reader computed off_ppp and def_ppp rounded to 1dp, then
-- subtracted those rounded values. The Four Factors readers already round
-- once from additive counts, so Summary and Four Factors disagreed by 0.1 for
-- 8 of 20 teams (broad) and 5 of 20 (last-10) in the 2026-08-29 audit.
--
-- Scope: the net_rtg VALUE ONLY. off_ppp/def_ppp outputs are unchanged, and
-- every dense_rank() ORDER BY is carried over exactly as-is -- those ranks are
-- read as the opponent-strength filter by Tabs 8, 9 and 10, so changing them
-- would move teams between top-N/bottom-N bands. That is deferred.
--
-- CREATE OR REPLACE (never DROP) so app_readonly EXECUTE grants survive.
```

In `get_team_ratings_pergame`, line 139 currently reads:

```sql
  round(r.off_ppp-r.def_ppp,1),r.games_played,r.wins,r.losses,
```

and becomes (the `rated` CTE aliases to `r` and its `a.*` carries the counts through):

```sql
  round(100.0*r.off_pts/nullif(r.off_poss,0)
      - 100.0*r.def_pts/nullif(r.def_poss,0),1),r.games_played,r.wins,r.losses,
```

In `get_team_ratings_dynamic`, line 465 currently reads:

```sql
    r.off_ppp, r.def_ppp, round(r.off_ppp - r.def_ppp, 1),
```

and becomes — keep the first two terms exactly, replace only the third:

```sql
    r.off_ppp, r.def_ppp,
    round(100.0 * r.off_pts / nullif(r.off_poss, 0)
        - 100.0 * r.def_pts / nullif(r.def_poss, 0), 1),
```

In `get_team_ratings_direct`, line 116 currently reads:

```sql
  round(r.off_ppp-r.def_ppp,1),r.games_played,r.wins,r.losses,r.off_poss,r.def_poss,
```

and becomes:

```sql
  round(100.0*r.off_pts/nullif(r.off_poss,0)
      - 100.0*r.def_pts/nullif(r.def_poss,0),1),
  r.games_played,r.wins,r.losses,r.off_poss,r.def_poss,
```

Leave every `dense_rank()` and every `ORDER BY r.off_ppp-r.def_ppp DESC NULLS LAST` untouched.

- [ ] **Step 4: Verify the migration is a copy plus three expressions**

Extract each function body from 046 and from its source migration and diff them:

```bash
cd euroleague
for pair in "get_team_ratings_pergame:sql/037_pergame_team_readers.sql" "get_team_ratings_dynamic:sql/019_clutch_read_layer.sql" "get_team_ratings_direct:sql/031_direct_team_custom_readers.sql"; do
  fn="${pair%%:*}"; src="${pair##*:}"
  echo "=== $fn ==="
  diff <(sed -n "/FUNCTION euroleague.$fn/,/^\$function\$;/p" "$src") \
       <(sed -n "/FUNCTION euroleague.$fn/,/^\$function\$;/p" sql/046_team_net_rating_single_round.sql)
done
```

Expected: for each function, the only difference is the `net_rtg` term. This is the verification — reversing the transform and diffing against the source, not writing new tests over copied code.

- [ ] **Step 5: Run the tests**

```bash
cd euroleague && python -m pytest tests/test_team_net_rating.py -v
```

Expected: all PASS.

- [ ] **Step 6: Commit**

```bash
git add euroleague/sql/046_team_net_rating_single_round.sql euroleague/tests/test_team_net_rating.py
git commit -m "Prepare EuroLeague migration 046: single-round Net Rating (readers)"
```

---

### Task 4: Migration 046 — season MVs, grants, and the gated applier

The two EuroLeague season MVs carry the same defect: `euroleague.team_ppp_ratings_mv` (newest definition `012_actions_consumer_cutover.sql:355`) and `euroleague.team_four_factors_mv` (`006_team_four_factors.sql:242`). A materialized view cannot be replaced in place, so this is DROP+CREATE — which **wipes its GRANTs**, drops its unique index, and can CASCADE.

**Files:**
- Modify: `euroleague/sql/046_team_net_rating_single_round.sql`
- Create: `euroleague/scripts/apply_046_team_net_rating.py`
- Test: `euroleague/tests/test_team_net_rating.py`

**Interfaces:**
- Consumes: the three-function migration from Task 3, in the same file.
- Produces: a `--baseline` / `--candidate` / `--apply` applier following `euroleague/scripts/apply_045_tab8_query_shape.py`. **This task does not run `--apply`.**

- [ ] **Step 1: Check for dependents and record current grants before writing any DROP**

```bash
cd euroleague && python - <<'PY'
from pathlib import Path
import sys
ROOT = Path.cwd()
sys.path.insert(0, str(ROOT / "src"))
from euroleague_possessions.postgres_backend import connect_from_env_file
conn = connect_from_env_file(ROOT.parent / "etl" / ".Renviron", port=5432)
with conn.cursor() as cur:
    for mv in ("team_ppp_ratings_mv", "team_four_factors_mv"):
        cur.execute("""
          SELECT DISTINCT dependent.relname, dependent.relkind
          FROM pg_depend d
          JOIN pg_rewrite rw ON rw.oid = d.objid
          JOIN pg_class dependent ON dependent.oid = rw.ev_class
          JOIN pg_class source ON source.oid = d.refobjid
          JOIN pg_namespace n ON n.oid = source.relnamespace
          WHERE n.nspname = 'euroleague' AND source.relname = %s
            AND dependent.relname <> source.relname
        """, (mv,))
        print(mv, "dependents ->", cur.fetchall())
        cur.execute("""
          SELECT grantee, privilege_type
          FROM information_schema.role_table_grants
          WHERE table_schema='euroleague' AND table_name=%s
        """, (mv,))
        print(mv, "grants ->", cur.fetchall())
        cur.execute("""
          SELECT indexname, indexdef FROM pg_indexes
          WHERE schemaname='euroleague' AND tablename=%s
        """, (mv,))
        print(mv, "indexes ->", cur.fetchall())
PY
```

Paste all three outputs into the migration header as a comment. **If either MV has a dependent view, stop and report** — the plan's blast-radius assumption is wrong and needs a decision before proceeding.

- [ ] **Step 2: Add the failing assertions**

Append to `euroleague/tests/test_team_net_rating.py`:

```python
def test_mvs_are_recreated_with_grants_restored():
    sql = _sql()
    upper = sql.upper()
    assert upper.count("DROP MATERIALIZED VIEW") == 2
    assert upper.count("CREATE MATERIALIZED VIEW") == 2
    # DROP wipes GRANTs. Every dropped MV must be re-granted in the same file.
    for mv in ("team_ppp_ratings_mv", "team_four_factors_mv"):
        assert f"GRANT SELECT ON euroleague.{mv} TO app_readonly" in sql


def test_unique_indexes_recreated():
    """DROP removed them; REFRESH ... CONCURRENTLY needs them back."""
    assert _sql().upper().count("CREATE UNIQUE INDEX") == 2
```

Run and confirm both fail:

```bash
cd euroleague && python -m pytest tests/test_team_net_rating.py -v
```

- [ ] **Step 3: Append the MV statements to migration 046**

For each MV, copy its newest definition verbatim (`012_actions_consumer_cutover.sql` for `team_ppp_ratings_mv`, `006_team_four_factors.sql` for `team_four_factors_mv`), change only the `net_rtg` projection to the canonical additive expression, and carry every `dense_rank()` over unchanged. Wrap each as:

```sql
DROP MATERIALIZED VIEW IF EXISTS euroleague.team_ppp_ratings_mv;
CREATE MATERIALIZED VIEW euroleague.team_ppp_ratings_mv AS
-- ... verbatim copy of the 012 body, net_rtg term replaced ...
WITH NO DATA;

-- DROP wiped both of these; the app cannot read the MV until they are back.
CREATE UNIQUE INDEX euroleague_team_ppp_ratings_mv_pk
  ON euroleague.team_ppp_ratings_mv (competition, game_year, team_id);
GRANT SELECT ON euroleague.team_ppp_ratings_mv TO app_readonly;
REFRESH MATERIALIZED VIEW euroleague.team_ppp_ratings_mv;
```

Copy the exact index name and column list from the Step 1 `pg_indexes` output rather than trusting the example above. Repeat for `team_four_factors_mv`.

- [ ] **Step 4: Write the gated applier**

Create `euroleague/scripts/apply_046_team_net_rating.py`, modelled on `apply_045_tab8_query_shape.py`: `--baseline` is read-only; `--candidate` applies inside a transaction and **rolls back**; `--apply` is the only commit path and runs the identical gates first. Baselines are captured before any candidate DDL and held in memory, so no comparison is ever made between two post-change executions.

Gates, all of which must pass:

1. **Pre-change definition hashes** of the three readers and the two MVs match the values recorded from `--baseline` (fail closed if a live object is not what 019/031/037/012/006 say it is).
2. **Additive parity:** for every team, `off_pts`, `off_poss`, `def_pts`, `def_poss`, `games_played`, `wins`, `losses` are identical before and after.
3. **`off_ppp` / `def_ppp` identical** before and after — this change must not move them.
4. **`net_rtg` moves by at most 0.1** for any team, and every difference is explained by `round(before_off_ppp - before_def_ppp, 1) != round(unrounded_diff, 1)`. Any larger delta fails.
5. **Ranks identical** before and after: `rank_net_rtg`, `rank_off_ppp`, `rank_def_ppp`, `off_rank`, `def_rank`, `net_rank`. This is the enforcement of the deferred-ranks constraint.
6. **Summary/Four Factors agreement:** for the same request, `get_team_ratings_pergame` and `get_team_four_factors_pergame` return identical `net_rtg` for every team — the outcome this whole change exists for. Assert the same for the `_dynamic` and `_direct` pairs.
7. **Grants present** on both MVs for `app_readonly` after apply, and both unique indexes exist.

Run the preset matrix over at least: broad season, last-10, a phase filter, an opponent filter, home/away, and an opponent-rank filter (gate 5 matters most on the last one).

- [ ] **Step 5: Run the baseline and the rollback candidate**

```bash
cd euroleague && python scripts/apply_046_team_net_rating.py --baseline
cd euroleague && python scripts/apply_046_team_net_rating.py --candidate A
```

Expected: baseline reports live hashes, grants and current values; the candidate run reports every gate PASS and ends with an explicit rollback. Record how many teams moved by 0.1 and confirm it is consistent with the audit's 8-of-20 broad and 5-of-20 last-10. A count far outside that range means the change is doing more than intended — stop and investigate.

- [ ] **Step 6: Run the tests**

```bash
cd euroleague && python -m pytest tests/test_team_net_rating.py -v
```

Expected: all PASS.

- [ ] **Step 7: Commit — do not apply**

```bash
git add euroleague/sql/046_team_net_rating_single_round.sql euroleague/scripts/apply_046_team_net_rating.py euroleague/tests/test_team_net_rating.py
git commit -m "Prepare EuroLeague migration 046: single-round Net Rating (season MVs + applier)"
```

`--apply`, the Israeli MV rebuild (`sql/rebuild_all_mvs.R`), and the follow-up `scripts/apply_db_security.R` run with `CONFIRM_DB_SECURITY_APPLY=1` are a separate approval, not part of this plan.

---

## What this plan deliberately does not do

Carried forward from the spec, still unimplemented, each needing its own decision:

- **Ranks on unrounded inputs** (spec §8) — blocked on the opponent-strength blast radius described in Global Constraints. This is the highest-value deferred item and the natural next decision.
- **Canonical `team_metrics_request` object and `team_metrics_route()`** (spec §4, §5) — `clutch_reader_kind()` plus Task 1's `clutch_active()` already cover the four-route classification. The remaining work is the request object, which only pays off alongside the combined readers.
- **Combined `get_team_metrics_*` readers**, legacy functions as thin projections, feature flag, telemetry, cutover, deprecation (spec §7, §10-13).
- **Israeli fast paths** over `team_metrics_by_game_mv` / `lineup_four_factors_by_game`, and splitting shot profile out of Ratings (spec §9.1) — the largest measured win (0.98s -> ~0.1s broad) and the natural next slice, but it edits live `server_tab3.R` routing.
