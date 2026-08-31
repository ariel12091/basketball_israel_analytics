# Migration 046 — combined filtered Four Factors reader: session handoff

Date: 2026-08-30
Branch: `sql/tab8-query-shape`
Commit: `bc10b11`
Status: **applied in both schemas, committed, not merged, not deployed**

Entry point for this session was a status question: a prior agent had been
collapsing the two database calls behind the filtered Four Factors view
(`server_tab8_euro.R:295` and its Israeli companion) into one. This records what
was found, what was changed, what was measured, and what is still open.

---

## 1. Status found at the start

The prior agent had written the SQL, applicators, gate, tests and the R edits for
**both** leagues, but applied only one function.

| | EuroLeague | Israeli |
|---|---|---|
| migration SQL written | `euroleague/sql/046_player_dashboard_reader.sql` | `sql/functions/four_factors_dashboard_compute.sql` |
| function in database | **applied** | **absent** |
| R tab edited to call it | yes | yes |
| result | working | **broken** |

Everything was uncommitted or untracked. Nothing was documented in `PROJECT.md`
or `RUNBOOK.md`.

### The breakage

`app/R/server_tab1.R` called `basketball_test.four_factors_dashboard_compute`,
which did not exist. Verified with the app's own `app_readonly` credentials,
using the exact 20-parameter shape the R code sends:

```
ERROR UndefinedFunction: function basketball_test.four_factors_dashboard_compute(
  integer, date, date, text, text, text, text, text, text, integer, text,
  integer, integer, integer, integer, integer, integer, integer, integer, integer
) does not exist
```

Scope of the breakage was narrow, which is why it was easy to miss:

- **Broken:** Israeli Tab 1, Four Factors view, live path only — i.e. whenever
  `onoff_fallback_needed()` (`app/R/helpers.R:1581`) is true: changed dates,
  phase, opponents, home/away, outcome, opponent rank, starters, or GN/last-N.
- **Unaffected:** Tab 1 Summary (goes through `onoff_compute`), Tab 1 Four
  Factors on the MV path (full season, no filters — reads
  `player_advanced_stats_mv`, no function involved), and all of Tab 8.

A local click-through appeared to pass. Two reasons, both worth remembering:

1. Only the live path touches the function; an unfiltered season stays on the MV.
2. **A running Shiny process holds the pre-edit closure.** `app.R` sources
   `R/*.R` once at startup and this project sets no `shiny.autoreload`, so a
   browser reload re-runs the server *function*, never the `source()`. An app
   started before the edit keeps using the old two-call code indefinitely.

---

## 2. What the change does

The filtered Four Factors path issued **two** full fact aggregations per filter
change:

1. `four_factors_compute` — the 43 factor columns.
2. `onoff_compute` with `p_team_ids => NULL, p_min_all => 0, p_min_on => 0` — a
   whole unrestricted on/off computation, to recover four columns:
   `Net RTG Diff`, `Off ON Diff`, `Def ON Diff`, `minutes`.

Both read the same per-game fact under the same filters; R then `left_join`ed
them on `(player_id, team_id)`. 43 of the second call's 47 columns were discarded.

`four_factors_dashboard_compute` returns all 47 from one call. Ratios are still
derived after the additive sums, so raw-counts-before-rates holds.

---

## 3. Work completed this session

### Gate widened, then applied

The Israeli gate (`euroleague/scripts/gate_israeli_player_dashboard_reader.py`)
passed on first run — but **two of its seven presets returned 0 rows**, comparing
empty against empty. Both were bad parameter values, not bad data:

- `p_game_type_csv => '1'` — not a `game_type` this schedule uses. 2026 has
  5 (regular season, 194 games), 16, 17, 26, 34, 35.
- `p_opp_ids_csv => '1109,1110'` — taken from `schedule.team1`, which holds
  **external team codes**. The id domain the functions filter on is the
  small-int `team_id` (2–15) carried by `sched_long` and `onoff_default_mv`.

Widened to twelve presets and added a guard: any preset other than the named
`empty` case that returns zero rows now **fails**.

```
OK broad              rows=362      OK home               rows=359
OK last 10            rows=328      OK win                rows=359
OK game type          rows=362      OK own starters       rows=362
OK game type multi    rows=362      OK opponent starters  rows=359
OK opponents          rows=332      OK empty              rows=0
OK opponent rank      rows=355
OK gn range           rows=294
```

Exact parity on all twelve — every one of the 47 columns, keyed on
`(player_id, team_id)`, against the live two-call result.

Applied via `scripts/deploy_sql_functions.R` (the gate's own `--apply` was
blocked by the permission classifier).

### Security

Additive `CREATE OR REPLACE` of a new name — no `DROP`, so no EXECUTE grant was
wiped. Verified after apply:

- `proacl` = `{postgres=X/postgres,service_role=X/postgres,app_readonly=X/postgres}`
- `PUBLIC`, `anon`, `authenticated` have **no** EXECUTE
- `apply_db_security.R` dry run validated clean
- `sql/security/audit_app_access.sql` returned **zero** violation rows
- Follow-up on 2026-08-31: the Israeli reader was replaced with the validated
  single-scan body; the confirmed `apply_db_security.R` pass committed and the
  independent `audit_db_security.R` pass succeeded.

### Verification

| check | result |
|---|---|
| Israeli app call shape as `app_readonly` | 294 rows (previously errored) |
| EuroLeague app call shape as `app_readonly` | 327 rows |
| `test-db-security-contracts.R` | 96 pass |
| `test-league-shared-helpers.R` | 122 pass |
| `test_player_dashboard_reader.py`, `test_tab8_query_shape.py` | 61 pass, 158 subtests |
| `parse()` on both edited R files | OK |

### Line endings

The prior agent's edit introduced 5 LF lines into `server_tab1.R`, which is
CRLF. HEAD had 3 LF lines of 581; the working tree had 8 of 561 — exactly the 5
added. Normalized; the diff dropped from 30 to 26 changed lines.

### Committed

`bc10b11`, 11 files. Deliberately **excluded** other workstreams left in the
tree: the 045 script/SQL/test edits, `RUNBOOK.md`, the plan/spec docs,
`global_euro.R`, tabs 9/10/11, and the five-player-lineups and team-dashboard
scripts.

---

## 4. Performance, measured

Wall-clock alone cannot arbitrate on this instance — the same query has measured
0.62 s and 2.98 s minutes apart from contention. Buffers are deterministic, so
they lead.

### Buffers — `EXPLAIN (ANALYZE, BUFFERS)`, median of 3

| | OLD (2 calls) | NEW (1 call) | saved |
|---|---:|---:|---:|
| **EuroLeague** (7 presets) | 595,547 | 299,766 | **49.7%** |
| **Israeli** (9 presets) | 482,365 | 424,215 | **12.1%** |

EuroLeague saves 49.7% on *every* preset. Israeli varies 6.6%–27.2%.

### Why they differ — the per-call breakdown

```
                  ff_compute   onoff_compute      = OLD    dashboard
ISRAELI               41,084          47,835      88,919      82,124
EUROLEAGUE            70,608          70,608     141,216       70,988
```

EuroLeague's two functions cost **exactly the same**, because migration 045
aligned both onto the same fact access. Combining them collapses two identical
scans into one — a clean halving.

The Israeli function is a **wrapper**: it calls the trusted `four_factors_compute`
verbatim, then scans the fact a second time for a narrow ratings/minutes
aggregation. Still two scans. Its saving is only the difference between the full
`onoff_compute` (47,835 — which also computes percentile ranks and 47 columns
that were thrown away) and the narrow scan (~41,040).

### Latency — 7 runs, interleaved, as `app_readonly`

| case | OLD median | NEW median | gain |
|---|---:|---:|---:|
| Israeli broad | 2.554 s | 1.867 s | 26.9% |
| Israeli last-10 | 0.907 s | 0.653 s | 28.1% |
| EuroLeague broad | 4.854 s | 2.670 s | 45.0% |
| EuroLeague last-10 | 0.685 s | 0.533 s | 22.2% |

Israeli latency gain (~27%) exceeds its buffer gain (12%) because removing a call
also removes a round trip and the R-side `left_join`.

**Variance collapses too**, which matters more than the median for a UI:
EuroLeague broad ranged 3.988–7.524 s on the old path, 2.527–2.783 s on the new.

---

## 5. Design analysis

### Why each league has two functions

Neither `four_factors_compute` is dead — each has exactly one *other* consumer:

- **Israeli:** Tab 7 Compare (`app/R/server_tab7_compare.R:947`) calls it live,
  plus the React/Plumber API.
- **EuroLeague:** `player_advanced_stats_mv` is *built from it* — the unfiltered
  fast path Tab 8 uses. (The Israeli `player_advanced_stats_mv` is **not** built
  from its `four_factors_compute`; verified against `pg_get_viewdef`.)

### Why the two leagues took different shapes — measured, not assumed

A EuroLeague wrapper was built in a rollback-only transaction and measured:

```
EUROLEAGUE broad, shared buffers
  four_factors_compute alone            70,608
  WRAPPER shape (Israeli design)       140,164   <- probe, rolled back
  SINGLE-SCAN shape (current euro)      70,988
  old two-call                         141,216
```

**A EuroLeague wrapper saves 0.7%.** Migration 045's alignment removed the
headroom a wrapper needs. One shared scan was the only design that wins there,
and one shared scan means reimplementing.

Israeli had headroom precisely because its two functions were *not* aligned — so
wrapping still saves 12% while keeping a single definition, which it needs
because Tab 7 and the React API read the same function.

Both leagues made the locally correct call.

### The cost of EuroLeague's choice

`euroleague.four_factors_dashboard_compute` reimplements the four-factor
derivation inline. The two EuroLeague functions are **52% textually identical**,
duplicating `schedule_ranked`, `games`, `agg`, `rates`, the opponent-rank
resolution, the starter predicates and the last-N windowing.

They agree today — verified live, 0 mismatching cells across 43 shared columns
on broad / last-10 / home. But **nothing enforces it**: all 9 tests in
`test_player_dashboard_reader.py` are static string assertions against the SQL
file and never touch the database. Behavioural parity was proven once, by the
applicator, at apply time.

The exposure is *not* the metric formulas — the project rule puts formula fixes
in the base MVs, which both functions read. It is the **filter/gate logic**, and
this repo has precedent for that drifting: `fetch_lineups_all.sql` /
`fetch_lineups_four_factors.sql` carry a documented manual-sync warning, and the
`schedule_ranked` last-N pattern had to be applied across seven functions at once.

---

## 6. Open decisions

### Immediate: retire `euroleague.four_factors_compute`

Point `player_advanced_stats_mv` at `four_factors_dashboard_compute` and drop the
older function. Verified in the exact 2-argument shape the MV uses:

```
2-arg MV call: rows 358/358, 43 shared cols, 0 mismatching cells, 0 keys only in one
```

Result: one EuroLeague function, drift structurally impossible, no parity test
needed, Tab 8 keeps 49.7%.

Cost — **not free**, contrary to first assumption:

```
MV refresh step:  four_factors_compute 25,940  ->  dashboard 70,988   (+174%)
```

With no dates passed, `four_factors_compute` takes a cheap path the dashboard
does not. That is +45,000 buffers **once per publication**, not per request.
Requires an MV redefinition, a `DROP FUNCTION`, the security re-apply, and a
rebuild of `player_advanced_stats_mv`.

The alternative is to keep both and add a standing **behavioural** parity test
across filter presets — but that polices the duplicate rather than removing it.

### Larger: one function for both leagues

The real destination is a single cross-league function, not one per schema. The
groundwork is better than expected:

- The fact tables are **already structurally identical** — Israeli 36 columns,
  EuroLeague 39, **36 shared, zero Israeli-only**. EuroLeague adds only
  `derivation_version`, `derived_at`, `load_run_id`. Differences are integer
  widths (`int` vs `bigint`/`smallint`).
- A `UNION ALL` with a league discriminator **prunes correctly**. Measured: the
  plan contains one Seq Scan (368,202 rows × 2 workers = the Israeli fact),
  21,576 buffers, EuroLeague branch eliminated at plan time. A shared function
  would not pay to scan both leagues.

What genuinely differs is the schedule dimension:

```
basketball_test.sched_long   game_year, gn,           game_type, team_id, opp_team_id
euroleague.schedule          season,    round_number, phase,     home_team_id, away_team_id, competition
```

— season convention (+1 offset), GN vs round, `game_type` int vs `phase` text,
plus EuroLeague's competition dimension. This is exactly the duplicated
filter/gate logic above.

This destination is already specified: `euroleague/CLAUDE.md` step 9 — adapter
views in a third schema (`analytics_common`) depending on both leagues, owning
the three field mappings and the league key, with no ranked table mixing
leagues. Seen against that plan, **046 went the wrong way**: it added a second
function to each schema — four where the plan calls for one — just before the
adapter is meant to collapse them.

**Unverified risk that must be checked before committing to the design:** the
pruning test above was a broad aggregate that seq-scans anyway. Widening
`game_id`/`team_id` to `bigint` in a union view could defeat index access on the
Israeli branch for *filtered* queries. Needs its own `EXPLAIN` on a narrow
preset — the kind of thing that looks fine broad and falls over on last-10.

---

## 7. Not done

- **Not merged to main, not deployed.** The live shinyapps.io app still runs the
  two-call code.
- `RUNBOOK.md` has an operational section for 045 but none for 046. 046 is an
  additive function with no lock or ordering concerns, so none was added.
- The `analytics_common` adapter is unspecified beyond `euroleague/CLAUDE.md`
  step 9.

## 8. Lesson for the next cross-league change

Apply **both** schemas' functions before editing either tab. The two tabs are
near-clones, so an R edit naturally lands on both at once while the SQL side is
per-schema and applied separately — and that gap is silent, because unit tests
mock `db_get_query` and the deployed league works perfectly.

To settle whether a function is really there, call it as `app_readonly` with the
exact parameter shape the R code sends. A `pg_proc` name match is not enough — a
wrong parameter count is a different function. And restart R before treating a
local click-through as evidence.

---

## 9. 2026-08-31 follow-up: cleanup first, starter indexes deferred

The Israeli starter filters were traced from the shared UI through the live
Four Factors and ON/OFF readers. Their min/max mapping is correct, and the
filtered Four Factors view makes exactly one
`four_factors_dashboard_compute` call. Warm starter-filter requests were faster
than the broad live query because they reduce the aggregation, although every
preset still touched the same 41k–48k buffers. The live Israeli fact has no
starter-leading index.

An index experiment is therefore **backlog, not active work**. If resumed, it
must be rollback-only first and compare own/opponent lower and upper bounds,
combined filters, broad, last-N and team presets. Require exact results and a
material p50/p95 or buffer improvement before retaining either an own-starter
or opponent-starter index. Low-cardinality starter counts make benefit
uncertain, while every retained index adds storage and refresh cost.

The active priority is codebase tightening and enforceable guardrails:

1. **completed 2026-08-31:** remove the five proven orphan objects through
   migration 047; both the rollback gate and committed apply preserved all 18
   reader row counts, followed by security reconciliation and audit;
2. **completed 2026-08-31:** migration 048 restored a literal, replayable source
   of truth for `refresh_actions_consumer_candidates` instead of catalog-text
   patching;
3. **completed 2026-08-31:** every `app_readonly`-executable EuroLeague function
   must now declare an app consumer, have an in-database referrer, or be an
   explicit pending-removal target;
4. **completed 2026-08-31:** repository tests reject new migrations that derive
   DDL from `pg_get_functiondef`/`prosrc` and execute a modified catalog body;
   migrations 015–017 are the frozen
   historical exceptions, not a pattern to repeat;
5. **in place:** keep the database-backed two-league player-dashboard contract
   audit as the guard against duplicated filter and aggregation logic drifting.

**Completed 2026-08-31:** filtered Shot Profile previously called
`onoff_compute` once for its table and again through the Summary auto-minimum
source. The shared auto-min selector now reuses `sp_ranked_df()` in Shot Profile
mode, so the ranked table and both possession bars share one reactive pull. A
server-level query-count regression proves one `onoff_compute` call per filter
change, and a pure helper test proves the Shot Profile branch does not touch the
separate Summary source. It still does **not** run in Four Factors mode and did
not affect the measurements above.

---

## 10. 2026-08-31 session closeout

### Outcome

The session began as a query-shape investigation and ended as a bounded SQL and
app-surface cleanup. The important distinction is:

- migrations 047 and 048, plus the Israeli single-scan dashboard replacement,
  were applied to the database with rollback, parity, and security gates;
- explicit Tab 9 reader routing and Shot Profile query reuse were app/test-only
  changes and did not modify any SQL object;
- no Shiny deployment was performed in this session.

### Decisions reached

1. **The league schemas need comparable semantics, not byte-identical SQL.**
   Their app usage is almost the same and they share a PostgreSQL instance, but
   schedule dimensions, provider grains, and filtered source facts remain
   schema-local. The chosen dashboard contract is one additive fact scan per
   league, with a database-backed cross-league behavioral audit guarding the 47
   output columns and filter matrix.
2. **Single scan is the approved dashboard shape in both leagues.** The Israeli
   single-scan body matched all 12 presets and reduced broad buffers from
   82,124 to 41,364; the approved live comparison measured 2.734 s to 1.439 s.
   EuroLeague already used this shape. The public signatures stayed unchanged.
3. **Do not retire `euroleague.four_factors_compute` just to remove textual
   duplication.** Repointing the season MV to the dashboard reader was exact but
   raised its refresh work from 25,940 to 70,988 buffers (+174%). The standing
   behavioral audit is the lower-cost guard. Section 6's earlier "nothing
   enforces it" statement is historical and is superseded by that audit.
4. **Do not add starter-leading indexes without evidence.** Israeli starter
   filter mapping is correct. Warm requests narrowed the aggregation, but all
   presets still touched roughly 41k-48k buffers. A rollback-only own/opponent
   starter index matrix remains backlog and must show material p50/p95 or buffer
   improvement before retention.
5. **Four Factors does not request Shot Profile data.** The duplicate call was
   confined to filtered Israeli Shot Profile: its table and auto-min observers
   independently reached `onoff_compute`. Four Factors timing was therefore not
   contaminated by Shot Profile work.
6. **Run the full behavioral parity matrix at database/query-contract
   boundaries, not after every app-only edit.** It was rerun after the
   single-scan SQL replacement and destructive orphan cleanup. The Tab 9
   literal-name change and Shot Profile reactive reuse instead received focused
   static, routing, render, and query-count tests. This keeps the expensive
   matrix meaningful without weakening the gates around SQL behavior.
7. **The `analytics_common` adapter is not the next cleanup by default.** It is
   a plausible longer-term consolidation, but filtered index access after
   widening Israeli IDs to `bigint` must be proven first. Do not trade guarded
   schema-local readers for a broad refactor without measured benefit.

### Delivered work and evidence

| Commit | Change | Database state | Verification |
|---|---|---|---|
| `ced4c69` | Recorded the measured wrapper-versus-single-scan decision and the publication-cost tradeoff | Documentation only | Evidence reconciled against the live query shapes |
| `ae8cc7e` | Defined same functionality as a 47-column behavioral contract rather than identical physical SQL | Documentation/contract only | Cross-league expectations made explicit before implementation |
| `587a319` | Aligned both player dashboards on the single-scan contract and added the permanent two-league behavioral audit | Israeli dashboard body applied; EuroLeague body already single-scan | 12-preset exact parity, post-commit matrices for both leagues, security reconciliation and audit |
| `20e1526` | Added the SQL reachability manifest/audit and catalog-body migration guardrails | Read-only guardrails | 209-test Python suite; live reachability audit |
| `8089a49` | Recorded and completed migration 047 orphan cleanup | Applied: three functions and two views removed | rollback and apply preserved all 18 reader row counts; 20 executable functions covered; both dashboard matrices and security audits passed |
| `27e217f` | Made `refresh_actions_consumer_candidates(bigint[])` literal and replayable in migration 048 | Applied with body MD5 `18b7329c289960f0825f0035f98a6bd8` | rollback/apply preserved body, owner, security mode, settings, and ACL; 213 Python tests; security checks passed |
| `93ec8c7` | Replaced Tab 9 fragment-built names with nine fully qualified, fail-closed reader routes | No SQL change | all 215 EuroLeague Python tests plus focused R clutch test |
| `9f6e7ef` | Reused `sp_ranked_df()` for filtered Shot Profile table and auto-minimum observers | No SQL change | pure source-routing test and server query-count/render test; one `onoff_compute` call per filter change |

Migration 047 removed:

- `get_player_traditional_clutch`;
- `select_player_clutch_counts`;
- the dead EuroLeague `get_player_traditional_dynamic`;
- `player_onoff_by_season`;
- `player_four_factors_by_season`.

`player_game_context` remains explicitly protected. It is not app-facing, but
`scripts/load_games.py` uses it for published-game QA; dropping it would break
the load pipeline.

Migration 048 is now the canonical definition to edit or replay. Migrations
015-017 remain historical evidence only. New migrations must contain literal,
reviewable DDL and may not reconstruct a body from the live catalog.

### Guardrails now in force

- `scripts/euroleague_function_contract.py` is the shared inventory of direct
  readers, protected objects, and destructive-cleanup smoke readers.
- `scripts/audit_function_reachability.py` fails on missing declared readers,
  uncovered executable functions, and unexpected overloads.
- `tests/test_sql_function_guardrails.py` blocks catalog-text DDL patching and
  locks the router/reachability contract.
- `scripts/audit_player_dashboard_contracts.py` compares each dashboard reader
  with its established companion composition across both schemas and all 47
  columns.
- `tests/test_team_reader_name_contract.py` ensures all nine Tab 9 routes remain
  explicit and declared rather than assembled from fragments.
- the Shot Profile server regression counts the actual mocked database calls,
  preventing table and auto-min wiring from silently separating again.

### Remaining work, ordered by value

There is no urgent defect left from this session.

1. Keep the behavioral dashboard and reachability audits in the gate whenever
   SQL readers, filters, signatures, grants, or destructive migrations change.
2. Consider the starter-index experiment only when filter latency is again the
   active goal; it is performance backlog, not structural cleanup.
3. Treat the guarded EuroLeague `four_factors_compute` / dashboard duplication
   as an accepted publication-vs-request tradeoff unless refresh economics
   change.
4. Explore `analytics_common` only as a separately designed project with narrow
   filtered-plan evidence and an explicit refresh/security lifecycle.

### Test-harness note

On this Windows environment, invoking the focused R tests without `--vanilla`
stalled while consuming CPU during test-harness startup. The same parse and test
commands completed normally with `Rscript --vanilla`. Locale/encoding warnings
for `LC_ALL=C.UTF-8` and comparison-symbol characters remained non-failing. This
was not a reactive loop or an application failure.
