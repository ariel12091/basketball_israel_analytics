# EuroLeague SQL functions: history, dead code, drift risk, and the 047 cleanup

Date: 2026-08-30
Scope: every function in `euroleague` (39) and `basketball_test` (27), app-facing
and internal, from the schema's inception on 2026-08-06 to migration 047.

**This is the single place for this work.** `PROJECT.md` and `RUNBOOK.md` carry
pointers here rather than copies — the findings, the migration and the operating
instructions all live in this file, so there is one thing to keep current.

Contents: §1–§4 what exists and how it got that way; §5–§6 what nothing reaches;
§7–§9 the drift and source-of-truth risks; §10 the risk register; **§11 migration
047, including how to apply it**; §12 the remaining recommendations.

## How the facts here were established

Every claim below came from the live database or the repository, not from
reading migration prose. Where a check turned out to be unsound, that is said
so explicitly rather than quietly dropped.

| Question | Method |
|---|---|
| What exists | `pg_proc` × `pg_namespace` × `pg_language` for both schemas |
| What each migration did | `CREATE`/`DROP FUNCTION` grep over `euroleague/sql/0*.sql` |
| When it landed | `git log --diff-filter=A` per migration file |
| Whether it changed afterwards | full `git log` per migration file, plus `git show` on the editing commits |
| Whether the deployed body matches the file | `pg_proc.prosrc` diffed against the `$tag$…$tag$` body in the committed migration |
| Review trail | `gh pr list --state all` |
| Who calls what, in-database | `pg_get_viewdef` for views/MVs + `pg_proc.prosrc` for function bodies |
| Who calls what, in code | `app/R/*.R`, `euroleague/src/**/*.py`, `euroleague/scripts/*.py`, `etl/*.R`, `scripts/*.R`, `frontend-v2/server` |
| App signature match | app-side `$N` placeholders vs `pg_proc.pronargs` — **this check was wrong, see §7** |
| Cross-schema similarity | `difflib.SequenceMatcher` over whitespace- and schema-normalised `prosrc` |

**Limits worth stating.** "Orphan" means no static reference was found in any of
the paths above. Text similarity is a crude proxy — two functions can be 2%
alike and both correct, because the EuroLeague versions are genuinely smaller.
Similarity is used here only to show that companions were written
*independently*, never to claim one is wrong.

---

## 1. The inventory today

| | EuroLeague | Israeli |
|---|---:|---:|
| total functions | **39** | **27** |
| app-facing readers | 17 | 11 |
| ETL / refresh | 10 | 12 |
| internal helpers | 9 | 3 |
| orphans (no consumer) | **3** | **0** |
| overloaded names | 0 | 1 (intentional) |

EuroLeague has 44% more functions than the league it shadows, while covering
*fewer* product surfaces. That ratio is the headline risk, and §4–§6 explain
where it came from.

The one Israeli overload is not stale: `compute_player_traditional_by_game`
exists as `(int[])` — an 89-character body — and `(int[], boolean)`. The short
one just calls the long one with `FALSE`. That is a default-argument wrapper,
not drift.

---

## 2. Timeline — 46 migrations in 25 days

`001` landed 2026-08-06; `046` landed 2026-08-30. Five distinct phases:

| Phase | Migrations | Dates | Theme |
|---|---|---|---|
| Foundation | 001–004 | 08-06 → 08-07 | Shadow schema, analytics compatibility, app read layer |
| Surface build-out | 005–014 | 08-07 → 08-10 | Team ratings, four factors, action fact, lineup units |
| Correctness | 015–017, 029, 039, 040 | 08-13 → 08-19 | Overtime clock, join order, starters numerator, defence seconds |
| **Performance** | 018–028, 030–038, 041–043 | 08-12 → 08-28 | Clutch fast paths, direct readers, per-game readers, covering indexes |
| Consolidation | 044–046 | 08-28 → 08-30 | Function shape, query shape, combined reader |

Two-thirds of all migrations are in the performance phase. That is the origin
of the function count: **each performance step added a reader rather than
changing one.**

Migration `003` was prepared and deliberately never applied — superseded by
`004`. It remains in the tree, correctly marked.

### A migration file is not a record of what was applied

Mining `git log` per migration file rather than only its creation date changes
how §2 and §3 should be read. **Nine of the 46 migration files were edited after
the commit that introduced them:**

| Migration | Commits | Notable |
|---|---:|---|
| `004_app_read_layer` | 5 | all on 08-07, including a same-day `onoff_compute` alias fix |
| `008_action_team_context` | 5 | all on 08-08, ending in "Close the residual findings from the whole-branch review" |
| `009_consumers_read_the_fact` | 3 | 08-09 |
| `014_lineup_units_read_layer` | 3 | 08-10 |
| `005`, `006`, `007`, `011`, `012` | 2 each | **011 and 012 were edited on 08-18, eight days after introduction** |

So "migration 011" names at least two different texts, and the rewrite counts in
§3 are a floor rather than an exact number: they count migrations that issued a
`CREATE OR REPLACE`, not the times each file's contents changed.

The 08-18 edit is the important one. Commit `8352f94` ("Freeze each action's 5v5
pair before joining lineup metadata") changed 74 lines of `011` and 4 of `012`
— four days *after* migrations 016 and 017 had text-patched those same two
functions in the database. That is exactly the ordering that makes a text-patch
migration dangerous, and the outcome was mixed: `012` came out consistent,
`011` did not. §9 has the verified detail.

---

## 3. Which functions were rewritten, and how often

Counting every migration that issued a `CREATE OR REPLACE` for the function.
These are floors, not exact counts — see the note above on migration files
being edited after they were applied.

| Function | Rewrites | Migrations |
|---|---:|---|
| `refresh_app_materialized_views` | **6** | 003, 004, 005, 006, 014, 021 |
| `fetch_lineups_dynamic` | **4** | 014, 019, 029, 039 |
| `refresh_player_four_factors_by_game_for_games` | **4** (+1 patch) | 002, 007, 009, 012, *017 text-patch* |
| `clutch_team_game_facts` | 3 | 019, 030, 040 |
| `refresh_team_four_factors_by_game_for_games` | 3 | 006, 009, 012 |
| `refresh_player_stats_actions_for_games` | 3 | 027, 028, 030 |
| `four_factors_compute` | 2 | 004, 045 |
| `onoff_compute` | 2 | 004, 045 |
| `get_team_ratings_dynamic` | 2 | 006, 019 |
| `get_team_four_factors_dynamic` | 2 | 006, 019 |
| `get_team_minutes_dynamic` | 2 | 018, 019 |
| `filtered_team_game_facts` | 2 | 019, 020 |
| `clutch_segment_durations` | 2 | 019, 025 |
| `get_player_traditional_clutch` | 2 | 024, 026 |
| `get_player_traditional_custom_clutch` | 2 | 026, 044 |
| `fetch_lineups_direct` | 2 | 035, 039 |
| `fetch_lineups_pergame` | 2 | 038, 039 |
| `refresh_lineup_totals_by_game` | 2 | 013, 043 |
| `refresh_actions_consumer_candidates` | 1 (+1 patch) | 011, *016 text-patch* |
| all others | 1 | — |

`refresh_app_materialized_views` being rewritten six times is structural, not
churn: it is the publication entry point, and every migration that added an MV
had to extend it. It is the one place where "add a surface" correctly means
"edit the existing function."

**Two functions were created and later deleted**, and are gone from the live
schema:

- `refresh_stint_timing_for_games` — created 002, dropped 012
- `refresh_action_team_context_for_games` — created 008, dropped 012

Migration 012 ("promote actions-based consumers") is the only migration in the
whole series that *removed* anything. That is the root of §5.

---

## 4. Why they changed — the four recurring drivers

**a. The fast-path fan-out (the dominant driver).** The clutch filter has three
regimes — no clutch, standard clutch (5 points / 300 seconds), custom clutch.
Rather than branch inside one function, each surface grew three:

```
get_team_ratings_{pergame, dynamic, direct}
get_team_four_factors_{pergame, dynamic, direct}
get_team_minutes_{pergame, dynamic, direct}
fetch_lineups_{pergame, dynamic, direct}
get_player_traditional_{pergame, standard_clutch, custom_clutch}
```

That is **15 of EuroLeague's 39 functions** — one product concept, five
surfaces, three variants each. The selection lives in R
(`clutch_reader_kind()`, `helpers.R:637`), which returns `"pergame"`,
`"dynamic"` or `"direct"`.

The rationale was sound per-migration: the per-game reader takes 19 or 23
arguments because it has no time/margin parameters at all, so a single function
would have carried four permanently-NULL arguments and a branch the planner
could not prune. The cost is that a change to filter semantics now has three
landing sites per surface.

**b. Reading the fact directly instead of through a view.** Migrations 009 and
012 moved consumers off `action_team_context` onto the columnar fact; 045 did
the same for `onoff_compute` / `four_factors_compute`, moving them off
`player_game_context`. Same motivation each time: a view was joining schedule
dimensions onto every fact row that the aggregation never read.

**c. Correctness repairs.** 015 (overtime clock), 029 (filter lineup identities
before expanding), 039 (starters numerator), 040 (restore an offense-only
seconds guard that migration 030 had dropped). 040 is notable — a performance
migration silently removed a correctness guard, and it took until 2026-08-19 to
restore.

**d. Israeli-shape alignment.** 026, 031, 033, 035 are explicitly described as
"Israeli-shaped" adaptations. The intent was convergence; the effect was a
second implementation with the same name (§8).

---

## 5. Dead functions — three EuroLeague orphans

No app path, no ETL path, no in-database referrer:

| Function | Body | Origin | Why it died |
|---|---:|---|---|
| `get_player_traditional_dynamic` | 10,051 chars | 021 (08-13) | Superseded by the `pergame` / `standard_clutch` / `custom_clutch` trio (023, 024, 026). Tab 5's EuroLeague branch never selects it. |
| `get_player_traditional_clutch` | 827 chars | 024 (08-13) | A **dispatcher**: it chose between standard and custom clutch inside SQL. Migration 026 plus the R-side `clutch_reader_kind()` moved that decision into R. Nothing calls it; it still calls the two live readers. |
| `select_player_clutch_counts` | 2,328 chars | 024 (08-13) | Helper for the dispatcher above. Orphaned with it. |

All three date from a single day — 2026-08-13 — when the Player Stats clutch
path was redesigned three times (023 → 024 → 026). The redesign superseded them
but never dropped them.

`get_player_traditional_dynamic` is the sharpest case: 10 KB of live,
grantable, `app_readonly`-executable SQL that nothing reaches, **sharing a name
with a live Israeli function that Tab 7 Compare calls every session.** Anyone
grepping `get_player_traditional_dynamic` finds both and cannot tell from the
name which is load-bearing.

**Israeli side: zero orphans.** Every one of its 27 functions has a consumer.

---

## 6. Dead views — two (an earlier draft said three)

| View | Origin | Status |
|---|---|---|
| `player_onoff_by_season` | 002 | No referrer. Superseded by `player_onoff_default_mv`. |
| `player_four_factors_by_season` | 002 | No referrer. Superseded by `player_advanced_stats_mv`. |
| `player_game_context` | 002 / 004 | **NOT an orphan — see the correction below.** |

### Correction (2026-08-30, after this audit was first written)

`player_game_context` was listed here as a third orphan. **That was wrong.**

Migration 045 did remove the two *function* reads of it, which is what the
first pass measured. But `euroleague/scripts/load_games.py:172` reads it in the
published-game QA check that cross-validates team-grain four factors against
the player-grain fact divided by five:

```sql
ply AS (
  SELECT game_id, team_id, sum(ts_poss_count)/5 ts, ...
    FROM euroleague.player_game_context
   WHERE type_lineup='offense' AND is_on_key=1 GROUP BY 1,2)
```

`euroleague/tests/test_tab8_query_shape.py:25` also names it, asserting the
functions no longer read it.

**Why the first pass missed it:** the view-referrer scan covered `app/R/*.R`
and `euroleague/src/**/*.py` but not `euroleague/scripts/*.py`. The
*function*-referrer scan did cover `euroleague/scripts/`, so the three orphan
functions were correctly identified — only the view scan had the gap. Dropping
it would have broken game loading.

The view stays, and `scripts/apply_047_drop_orphans.py` carries it in a
`PROTECTED` list that refuses to run if a future migration tries to drop it.

The first two are already flagged in the memory index ("don't confuse live
`*_default_mv` with the dead migration-002 `*_by_season` views") — evidence
that these orphans have already cost someone time.

---

## 7. Drift risk in the app — none found, and a correction

**Result: no signature drift.** Every app call site matches its deployed
function's arity.

| Call site | Function | app | deployed |
|---|---|---:|---:|
| `server_tab1.R:177` | `basketball_test.onoff_compute` | 23 | 23 |
| `server_tab1.R:198` | `basketball_test.four_factors_dashboard_compute` | 20 | 20 |
| `server_tab8_euro.R:183` | `euroleague.onoff_compute` | 22 | 22 |
| `server_tab8_euro.R:215` | `euroleague.four_factors_dashboard_compute` | 19 | 19 |
| `server_tab9_euro_team.R` | `get_team_*_pergame` / `_dynamic` / `_direct` | 19 / 23 | 19 / 23 |
| `server_tab10_euro_lineups.R` | `fetch_lineups_pergame` / `_dynamic` / `_direct` | 23 / 27 | 23 / 27 |
| `server_tab5_traditional.R` | `get_player_traditional_pergame` / `_standard_clutch` / `_custom_clutch` | 15 / 15 / 19 | 15 / 15 / 19 |
| `server_tab2/3/7` | Israeli lineup, team and compare readers | 29 / 23 / 18 / 20 | match |

**A first version of this check reported 8 mismatches. All 8 were false.** The
check counted `$N` placeholders and treated that as arity. Three legitimate
patterns break that assumption:

- `mod_team_hub.R` calls `get_team_ratings_dynamic($1::int4,
  p_num_starters_off_min := 3, ...)` — **named arguments** with defaults for the
  rest. One placeholder, 23 parameters.
- `server_tab7_compare.R:1617` passes 18 arguments of which 17 are `NULL::type`
  literals, not placeholders.
- `person_display_name(p.display_name)` takes a column; the scan over-read into
  the next clause and found a `$2` belonging to a different predicate.

Recorded because the *shape* of that error is the recurring one in this
project: a check that looks authoritative, produces a plausible defect list,
and is measuring the wrong thing.

### The real app-side risk is name composition, not arity

Nothing statically connects a EuroLeague call site to its function. Tab 9 builds
the name from **two fragments**:

```r
# app/R/server_tab9_euro_team.R:167
list(sql = paste0("SELECT * FROM euroleague.", base, "_", kind, "(", sig, ")"), ...)
#   base = "get_team_ratings" | "get_team_four_factors" | "get_team_minutes"
#   kind = clutch_reader_kind(p)  ->  "pergame" | "dynamic" | "direct"
```

So the string `get_team_minutes_direct` **never appears anywhere in the R
source.** Tabs 10 and 5 are milder — they hold bare names in a `switch()`, so
the name exists but the schema does not.

Consequences:

1. A rename or drop is invisible to grep, to tests, and to review. It surfaces
   only at runtime, on the one filter combination that selects that reader.
2. Coverage is unprovable statically. `get_player_traditional_dynamic` was found
   dead only by enumerating what `clutch_reader_kind()` can return and diffing
   against the catalog.
3. My own first consumer scan reported nine live readers as dead for exactly
   this reason.

The Israeli tabs write their function names as literals, so they do not have
this problem.

---

## 8. Drift risk between companion functions

Ten names exist in both schemas. Similarity is over normalised bodies with the
schema prefix stripped:

| Function | EL args | IL args | EL lang | IL lang | similar |
|---|---:|---:|---|---|---:|
| `four_factors_compute` | 19 | **20** | plpgsql | plpgsql | 39% |
| `refresh_player_stats_actions_for_games` | 1 | 1 | plpgsql | plpgsql | 24% |
| `four_factors_dashboard_compute` | 19 | **20** | sql | sql | 21% |
| `refresh_player_four_factors_by_game_for_games` | 1 | 1 | plpgsql | plpgsql | 18% |
| `refresh_player_traditional_by_game_for_games` | 1 | 1 | plpgsql | plpgsql | 14% |
| `onoff_compute` | 22 | **23** | plpgsql | plpgsql | 11% |
| `get_player_traditional_custom_clutch` | 19 | **18** | plpgsql | plpgsql | 8% |
| `get_player_traditional_dynamic` | 19 | **18** | **sql** | **plpgsql** | 5% |
| `get_team_ratings_dynamic` | 23 | 23 | **sql** | **plpgsql** | 5% |
| `get_team_four_factors_dynamic` | 23 | 23 | **sql** | **plpgsql** | 2% |

Three separate hazards here:

**a. Same name, same arity, different everything else.**
`get_team_ratings_dynamic` and `get_team_four_factors_dynamic` take 23
arguments in both schemas — so a call site can be moved between leagues without
any error — yet they are 5% and 2% textually alike and written in different
languages. The interchangeability is an illusion the signature actively
encourages.

**b. Same name, different arity.** Five pairs differ. Some of that is
legitimate and documented (EuroLeague takes `competition` first; Israeli carries
two legacy scalar starter parameters). But the difference is discoverable only
by reading both signatures.

**c. A dead function shadowing a live one.** `get_player_traditional_dynamic`
is dead in EuroLeague and live in Israeli (Tab 7). Same name, different arity,
different language, 5% alike.

### What is *not* at risk

The four-factor metric formulas. The project rule puts formula fixes in the base
MVs (`player_four_factors_by_game`), which both leagues' functions read, so a
genuine formula change propagates to both automatically.

The exposure is the **filter and gate logic** — `schedule_ranked`, `games`,
opponent-rank resolution, starter predicates, last-N windowing. That logic is
duplicated, and this repo has precedent for it drifting:
`fetch_lineups_all.sql` / `fetch_lineups_four_factors.sql` carry a standing
manual-sync warning, and the `schedule_ranked` last-N pattern had to be applied
across seven functions at once.

### A third implementation of the same concept, in the React API

`frontend-v2/server/plumber.R:288-300` still issues the **pre-046 two-call
pattern** — but as a SQL-level `LEFT JOIN` rather than an R-side one:

```r
"SELECT ff.*, oo.\"Net RTG Diff\", oo.\"Off ON Diff\", oo.\"Def ON Diff\" ",
"FROM ",      SCHEMA, ".four_factors_compute(...$1..$20) ff ",
"LEFT JOIN ", SCHEMA, ".onoff_compute(...$2..$24) oo ",
"ON ff.player_id = oo.player_id AND ff.team_id = oo.team_id"
```

So "filtered Four Factors plus rating differences" now exists in **three**
shapes:

| Consumer | Implementation | Calls |
|---|---|---:|
| Shiny Tab 1 | `basketball_test.four_factors_dashboard_compute` (wraps `four_factors_compute`) | 1 |
| Shiny Tab 8 | `euroleague.four_factors_dashboard_compute` (reimplements it) | 1 |
| Plumber `/api/onoff/four-factors` | `four_factors_compute LEFT JOIN onoff_compute` | 2 |

`SCHEMA` is hardcoded to `basketball_test` (`plumber.R:27`) and there is no
`euroleague` reference anywhere in `frontend-v2/server` or `frontend-v2/src`,
so the React API cannot reach the EuroLeague schema at all.

Severity is low — `CLAUDE.md` designates `frontend-v2` as archival and
explicitly out of scope — but it is worth recording that migration 046 covered
two of the three call paths, and that this third one is a *fourth* consumer of
`basketball_test.four_factors_compute` (after Tab 7, the dashboard wrapper, and
the perf baseline scripts).

### Within EuroLeague itself

`four_factors_compute` and `four_factors_dashboard_compute` are **52%
identical**, duplicating `schedule_ranked`, `games`, `agg`, `rates`, the
opponent-rank resolution, the starter predicates and the last-N windowing. They
agree today — verified live, zero mismatching cells across 43 shared columns on
broad / last-10 / home — but nothing enforces it. All 9 tests in
`test_player_dashboard_reader.py` are static string assertions against the SQL
file and never touch the database.

---

## 9. The source-of-truth break — three migrations, verified against the database

**Corrected 2026-08-30 after mining the git history.** An earlier draft named
two patching migrations and asserted that neither target's body could be
reconstructed. Both halves needed fixing.

### There are three, not two

`015`, `016` and `017` all read a function's body out of the catalog, transform
the text, and re-execute it:

| Migration | Target | Transform |
|---|---|---|
| 015 | `refresh_actions_consumer_candidates` | `a.period` → `euroleague.effective_period(a.period, a.minute, a.play_type)`, and `'actions-v1'` → `'actions-v2'` |
| 016 | `refresh_actions_consumer_candidates` | injects a rewritten `event_lineups AS MATERIALIZED` block |
| 017 | `refresh_player_four_factors_by_game_for_games` | `player_minutes AS (` / `counts AS (` → `… AS MATERIALIZED (` |

015 was missed by the first pass because the scan keyed on `CREATE FUNCTION`
and 015 contains none — it only mutates an existing body. It is also the most
consequential of the three, because it rewrites *semantics* (the overtime
period calculation) and bumps a **derivation-version marker** that downstream
code checks.

### What is actually deployed, checked rather than assumed

Comparing `pg_proc.prosrc` against the body in each committed migration:

| Function | Committed file | Live body | Match |
|---|---|---|---|
| `refresh_player_four_factors_by_game_for_games` | 012 | 5,237 chars | **identical** |
| `refresh_actions_consumer_candidates` | 011 | 15,433 vs 15,138 chars | **8 differing hunks** |

So the risk is **half-resolved and half-live**:

- **Resolved.** On 2026-08-18, commit `8352f94` folded the `MATERIALIZED` hints
  into `011` and `012` directly. `012`'s committed body now reproduces the
  deployed function exactly. Someone already fixed this case.
- **Still live.** `refresh_actions_consumer_candidates` cannot be reproduced
  from any committed file. The deployed body carries 015's substitutions:

```diff
-      CASE WHEN a.period <= 4 THEN (a.period - 1) * 600
+      CASE WHEN euroleague.effective_period(a.period, a.minute, a.play_type) <= 4
+           THEN (euroleague.effective_period(a.period, a.minute, a.play_type) - 1) * 600
-    'actions-v1'
+    'actions-v2'
```

while the committed `011` carries a comment and an indentation change the live
body does not have. Neither is a superset of the other. Reproducing the
deployed function means applying 011, then 015, then 016, in that order — and
even then the comment would differ.

### Why this matters more than a tidiness complaint

`'actions-v1'` → `'actions-v2'` is a **derivation-version marker**, and
`scripts/apply_015_effective_overtime_periods.py` verifies it with
`LIKE '%actions-v2%'`. The committed `011` still says `actions-v1`. Anyone
re-applying `011` from the file would silently revert both the overtime-period
semantics and the version marker that is supposed to detect exactly that.

`replace()` on a non-matching string is a no-op that raises nothing, so a
partial or out-of-order replay fails silently in both directions.

### No review trail

`gh pr list --state all` returns **three pull requests for the entire
repository**, all from March 2026 (`infra/branching-conventions`,
`infra/pre-push-hook`, `shiny/landing-page`). **None of the 46 EuroLeague
migrations went through a pull request** — all merged directly to `main`. There
is no review record for any of the schema work, which is consistent with a
one-person project but means the migration files and this document are the only
account of what happened and why.

## 10. Risk register

| # | Risk | Severity | Evidence |
|---|---|---|---|
| 1 | Dynamic name composition defeats all static verification | **High** | `server_tab9_euro_team.R:167`; nine readers first misreported as dead |
| 2 | 015/016/017 patch bodies by text; `refresh_actions_consumer_candidates` matches no committed file, incl. a stale `actions-v1` version marker | **High** | §9, `prosrc` diffed against 011 |
| 3 | Companion functions share names/arity but not implementations | **Medium-high** | 23-arg pairs at 2% and 5% similarity |
| 4 | 3 orphan functions, one shadowing a live Israeli name | Medium | §5, all reachability paths checked |
| 5 | EL `four_factors_compute` vs `_dashboard_compute` unguarded 52% duplicate | Medium | live parity holds; only static tests exist |
| 6 | 2 orphan views (`*_by_season`, migration 002) | Low-medium | §6 |
| 7 | Fast-path fan-out: 15 functions for 5 surfaces | Low (by design) | §4a |

## 11. Migration 047 — the cleanup, and how to run it

**Status: prepared and gated, NOT applied.** The database is unchanged.

Files: `euroleague/sql/047_drop_orphaned_objects.sql`,
`euroleague/scripts/apply_047_drop_orphans.py`.

### What it removes

| Object | Origin | Superseded by |
|---|---|---|
| `get_player_traditional_clutch` | 024 | R-side `clutch_reader_kind()` (026) |
| `select_player_clutch_counts` | 024 | its dispatcher, orphaned with it |
| `get_player_traditional_dynamic` | 021 | the `pergame`/`standard_clutch`/`custom_clutch` trio |
| `player_onoff_by_season` | 002 | `player_onoff_default_mv` |
| `player_four_factors_by_season` | 002 | `player_advanced_stats_mv` |

### How to apply

`apply_shadow_schema()` **refuses any DDL containing `DROP `** by design
(`postgres_backend.py:285`). That guard is correct and must not be relaxed, so
047 applies its statements directly, exactly as the 045 and 046 applicators do:

```bash
# rollback-only gate
euroleague/.venv/Scripts/python.exe euroleague/scripts/apply_047_drop_orphans.py

# commit
euroleague/.venv/Scripts/python.exe euroleague/scripts/apply_047_drop_orphans.py --apply

# MANDATORY afterwards -- DROP FUNCTION wipes EXECUTE grants
CONFIRM_DB_SECURITY_APPLY=1 "$RSCRIPT" scripts/apply_db_security.R
```

### What the gate does before dropping anything

1. Refuses to run if the migration would drop a `PROTECTED` object.
2. Re-verifies in the live catalog that every target has zero referrers among
   euroleague views, materialized views and function bodies.
3. Smoke-runs all 18 app-reachable readers, and again after the drop, failing if
   any row count changes. The lineup readers are narrowed to two-player units
   over the last two games — an unfiltered five-player expansion exceeded the
   statement timeout on the first run.

Gate result on 2026-08-30: every target unreferenced, all 18 readers returning
identical row counts, rolled back cleanly.

### `player_game_context` is PROTECTED and must never be dropped

Migration 045 removed the two *function* reads of it, which makes it look
orphaned. `scripts/load_games.py` reads it for the published-game QA check that
cross-validates team-grain four factors against the player-grain fact divided by
five. Dropping it would break game loading. The applicator encodes the
protection; see the correction in §6.

### Known consequence

`scripts/apply_042_player_traditional_pergame.py` benchmarks
`get_player_traditional_dynamic` and cannot be re-run as-is afterwards. It is a
historical applicator whose migration is already applied; accepted.

---

## 12. Remaining recommendations, in order

1. **Apply migration 047** (§11). Removes 13 KB of grantable SQL and eliminates
   the dead/live name collision on `get_player_traditional_dynamic`. Prepared,
   gated and waiting.
2. **Fix `refresh_actions_consumer_candidates`'s source of truth.** Capture the
   live body from `pg_get_functiondef` and commit it as a literal
   `CREATE OR REPLACE`, the way `8352f94` already did for `012` on 2026-08-18.
   Until then the committed `011` says `actions-v1` while the database says
   `actions-v2`, and re-applying the file reverts the overtime-period
   semantics. 017's target needs no action — its committed body already
   matches.
3. **Add a reachability test.** Assert that every `app_readonly`-executable
   function in `euroleague` is either in the set `clutch_reader_kind()` can
   compose, or has an in-database referrer. That test would have caught all
   three orphans on 2026-08-13.
4. **Make the composed names greppable.** Have the R hold the full name per
   branch (as Tabs 10 and 5 already do) rather than assembling `base + "_" +
   kind`. Cheap, and it restores grep, review and static checking for Tab 9.
5. **Add a behavioural parity test** for `four_factors_compute` vs
   `four_factors_dashboard_compute` across filter presets — or retire the
   former by repointing `player_advanced_stats_mv` (costs +45,000 buffers per
   publication; see the 046 handoff).
6. **Treat the `analytics_common` adapter as the real fix** for §8. The two
   fact tables are already structurally identical (36 shared columns, zero
   Israeli-only) and a `UNION ALL` with a league discriminator prunes correctly,
   so one cross-league function is feasible. Verify first that widening
   `game_id`/`team_id` to `bigint` does not defeat index access on the Israeli
   branch for *filtered* queries — the pruning test used a broad aggregate that
   seq-scans regardless.

---

## Appendix: EuroLeague functions by reachability

**App-reachable (17):** `onoff_compute`, `four_factors_dashboard_compute`,
`get_team_ratings_{pergame,dynamic,direct}`,
`get_team_four_factors_{pergame,dynamic,direct}`,
`get_team_minutes_{pergame,dynamic,direct}`,
`fetch_lineups_{pergame,dynamic,direct}`, `get_player_traditional_pergame`,
`get_player_traditional_standard_clutch`,
`get_player_traditional_custom_clutch`, `person_display_name`

**ETL-reachable (11):** the ten `refresh_*` functions called from
`euroleague/src/euroleague_possessions/postgres_backend.py`, plus
`effective_period`

**Reachable via another function or MV (8):** `four_factors_compute` (via
`player_advanced_stats_mv`), `clutch_event_qualifies`,
`clutch_margin_qualifies`, `clutch_segment_durations`, `clutch_team_game_facts`,
`filtered_team_game_facts`, `select_team_game_facts`

**Orphaned (3):** `get_player_traditional_dynamic`,
`get_player_traditional_clutch`, `select_player_clutch_counts`
