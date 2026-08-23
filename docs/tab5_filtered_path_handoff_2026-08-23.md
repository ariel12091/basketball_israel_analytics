# Tab 5 filtered path exceeds the statement timeout — handoff

**Date:** 2026-08-23
**Branch:** `main` at `ae3cec7`, in sync with `origin/main`
**Author:** session of 2026-08-23 (Claude Opus 5 + Ariel)
**Status:** investigation complete, one decision outstanding, no fix implemented

---

## 1. Decision required before the next deploy

**Do not redeploy the Shiny app until you have decided what Tab 5 should do.**

The pooler-drops-`options` fix (`3083592`, merged, **not yet deployed**) makes the 20 s
`statement_timeout` effective for the first time. Tab 5's filtered path measures **59.56 s**.
The redeploy therefore converts a slow-but-working feature into a hard error.

| | Today (deployed) | After redeploy |
|---|---|---|
| `statement_timeout` in force | Supabase default, 120 s | 20 s (`global.R:214`) |
| Filtered Tab 5 request | returns in ~60 s | **cancelled — error shown to user** |
| Tabs 1 / 2 / 3 | fine | fine (measured, section 4) |

Three options. **Pick one and record it here.**

1. **Deploy as-is.** A 60 s query is already unusable; failing in 20 s is more honest than a
   minute-long hang and makes the problem visible. Cheapest. Visible regression for anyone
   currently using Tab 5 filters.
2. **Raise the timeout for that one call site.** In `run_player_traditional_dynamic`
   (`app/R/server_tab5_traditional.R:851`), issue `SET LOCAL statement_timeout = 90000` on the
   connection before the query. Preserves today's behaviour until section 6 lands. Keeps a bad
   experience rather than replacing it with a broken one.
3. **Fix first, deploy after.** Correct, but blocks the deploy on new ETL work plus a storage
   estimate against an instance already over budget.

No option is free. Option 1 is the recommendation, but the choice is the product owner's because
it is user-visible.

---

## 2. Repository state — what is done

All four commits are pushed to `origin/main`. Nothing is left on a branch.

| Commit | What it does |
|---|---|
| `135e755` | Pin the timeout lift in the `finish_load_run` statement order |
| `efc4b87` | Merge `etl/euro-data-quality-report` (14 commits): the EuroLeague DQ report, migrations 040 + 041, the 106-game load-gap runbook fix |
| `bd99818` | Delete the stale app-routing greps from the EuroLeague migration tests |
| `ae3cec7` | Route Tab 5's EuroLeague reader through `clutch_reader_kind()` |

Net vs the previous head `3083592`: **15 files, +2892 / −113**.

Branch `etl/euro-data-quality-report` was merged and **deleted** (it was local-only; no remote
copy existed).

### Verification performed

| Suite | Command | Result |
|---|---|---|
| EuroLeague Python | `cd euroleague && ./.venv/Scripts/python.exe -m pytest tests -q` | `103 passed` |
| Shiny R | `cd app && Rscript -e "testthat::test_dir('tests/testthat')"` | `FAIL 0 \| SKIP 4 \| PASS 1190` |

The Python suite is green **for the first time** — two assertions had been failing on `main`
before this session (see section 7.3). The 4 R skips are E2E tests gated behind `RUN_E2E=1`.

`pytest` is not in `euroleague/pyproject.toml`; it was installed ad hoc into `euroleague/.venv`
during this session. If you rebuild that venv you must reinstall it.

### Not done

- No fix for Tab 5 (section 1 decision blocks it).
- No storage estimate for the proposed per-game fact (section 6).
- The orphaned SQL function and the duplicated SQL rule are untouched (section 7).
- `app/rsconnect/.../onoff-shiny.dcf` carries an uncommitted bundle-id stamp. Deploy noise;
  commit or discard at will.

---

## 3. The problem, precisely

### 3.1 What triggers it

`fallback_needed` at `app/R/server_tab5_traditional.R:957` returns `TRUE` for **any** of:

- a date range differing from the season bounds,
- a `game_type` / phase, opponent, home-away, outcome or opponent-rank filter,
- a GN range or last-N,
- `clutch_enabled` being ticked.

When true, Tab 5 calls `run_player_traditional_dynamic`
(`app/R/server_tab5_traditional.R:851`), which issues one unconditional query against
`basketball_test.get_player_traditional_dynamic` — 18 parameters, no routing, clutch arguments
passed straight through at `$12–$15`.

When false, Tab 5 reads `basketball_test.player_traditional_stats_mv` and is fast. **The
unfiltered case is not affected.**

### 3.2 Where the timeout is set

- `app/R/global.R:214` — `PG_STATEMENT_TIMEOUT_MS` defaults to `20000`.
- `app/R/global.R:436` — applied via `DBI::dbExecute(con, sprintf("SET statement_timeout = %d", ...))`
  in the pool's `onCreate`.

`.Renviron` is gitignored *and* deployed, so a `PG_STATEMENT_TIMEOUT_MS` set there overrides the
committed default invisibly. **Check the deployed `.Renviron` before assuming 20 s.**

### 3.3 Root cause

`CLAUDE.md` states the architecture rule:

> SQL functions only `SUM` pre-computed columns from MVs — they don't recompute raw counts.

`get_player_traditional_dynamic` is the only filtered-path function that breaks it. Count of raw
action-counting expressions (`SUM(CASE WHEN a.type ...)`) per function:

```
get_player_traditional_dynamic    16     <- sole violator, sole function over the timeout
onoff_compute                      0
four_factors_compute               0
get_team_ratings_dynamic           0
fetch_lineups_all                  0
```

It reads `basketball_test.df_pts_poss_lineups_longer_mv` (action grain) at
`sql/functions/get_player_traditional_dynamic.sql:209` and rebuilds every counting stat — points,
rebounds, assists, steals, blocks, deflections, FG/FT splits — from individual play-by-play
events, on every filtered request.

### 3.4 Why EuroLeague does not have this problem

Not a query-design difference — a **data-source** difference.

| | Source of pts / reb / ast / stl / blk / FG | Consequence |
|---|---|---|
| Israel | derived: `SUM(CASE WHEN a.type = 'shot' AND a.parameters_made = 'made' ...)` over the action fact | action scan is mandatory |
| EuroLeague | `full_rosters.boxscore_stats ->> 'Points'` etc. — the provider's official box score | per-game facts are cheap; no action scan unless clutch demands it |

Israeli `full_rosters` contributes only `firstname` / `lastname` to
`player_traditional_stats_mv` — names, no statistics. There is no box-score feed on that side.

Israel **does** have per-game aggregates (`player_four_factors_by_game`,
`lineup_four_factors_by_game`, `team_metrics_by_game_mv`), but none carries traditional counting
stats. `player_traditional_stats_mv` is season grain
(`GROUP BY awy.game_year, awy.team_id, awy.player_id`), so it can serve only the unfiltered case.

---

## 4. Measurements

Live DB as `app_readonly` through the pooler (port 6543), season 2026, 221 games,
`statement_timeout` raised to obtain the numbers. Two runs each; run 1 cold, run 2 warm.

### Tab 5 — `get_player_traditional_dynamic`

| Preset | Run 1 | Run 2 | Rows | vs 20 s |
|---|---:|---:|---:|---|
| Full season, no filters *(synthetic — app would use the MV)* | 82.68 s | 89.21 s | 313 | over |
| **Second half only (a realistic filter)** | 61.29 s | **59.56 s** | 266 | **3× over** |
| Custom clutch, 3 pt / 4:00, full season | 16.24 s | 10.04 s | 210 | ok |

### Tabs 1 / 2 / 3 — all clear

| Preset | Cold | Warm | Rows | vs 20 s |
|---|---:|---:|---:|---|
| Tab 1 · `onoff_compute`, date-narrowed | 6.30 s | 1.45 s | 128 | ok |
| Tab 3 · `get_team_ratings_dynamic`, date-narrowed | 2.49 s | 0.69 s | 14 | ok |
| Tab 2 · `fetch_lineups_csv_v2`, **full season** | 3.88 s | 2.16 s | 4634 | ok |
| Tab 2 · `fetch_lineups_csv_v2`, date-narrowed | 1.00 s | 1.09 s | 2513 | ok |

Tab 2 was given the full-season case deliberately: per `CLAUDE.md` it has **no MV fast path**, so
3.88 s cold is its tab-open cost, not a worst case. Still 5× inside the limit.

### Counter-intuitive result worth remembering

**Israeli clutch is ~8× faster than no clutch** (10.04 s vs 82.68 s). Israeli clutch parameters are
extra `WHERE` predicates on an action scan that happens anyway, so they *narrow* it. EuroLeague
clutch does the reverse — it forces the request off the per-game fact onto the action scan.

**Do not reason about one league's clutch cost from the other's.** Same word, opposite direction.

### 4.2 App-limit verification after handoff review (2026-08-23)

A fresh read-only pass used `app_readonly`, the app pooler (6543), one new
connection per measured call, and the actual 20-second `statement_timeout`.
This was an app-survivability check, not a cold/warm benchmark:

| Israeli Player Stats path | Result | Rows | 20 s limit |
|---|---:|---:|---|
| Season MV | 0.36 s | 313 | pass |
| Second-half non-clutch | cancelled at 20 s | — | **fail** |
| Standard clutch, margin <= 5 / final 5:00 | cancelled at 20 s | — | **fail** |
| Custom clutch, margin <= 3 / final 4:00 | 10.16 s | 210 | pass, limited margin |

This adds an important distinction to the earlier measurements: the proposed
full-game per-game fact is required for filtered non-clutch requests, but it
does not solve the dominant standard clutch preset. Matching the proven
EuroLeague routing architecture requires a second per-game additive cache for
exactly 5 / all / 5:00, while arbitrary custom clutch definitions remain on
the action path.

The reproducible harness is `scripts/benchmark_tab5_traditional_paths.R`.
It supports the normal app-limit pass and an optional raised-limit diagnostic.
The raised-limit follow-up attempted during this review was stopped because
pooler contention made it exceed its trustworthy measurement window; no timing
from that attempt is reported.

### 4.1 Reproducing the measurement

The scripts were written to a session scratchpad and are gone. This is the whole thing; save it
to a temp `.R` file and run with `Rscript` (do not use `Rscript -e` — long inline scripts segfault
on this machine).

```r
readRenviron("app/.Renviron")   # run from the repo root
suppressPackageStartupMessages(library(DBI)); library(RPostgres)
con <- dbConnect(Postgres(),
  host = Sys.getenv("PG_HOST"), port = as.integer(Sys.getenv("PG_PORT")),
  dbname = Sys.getenv("PG_DB"), user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"), sslmode = Sys.getenv("PG_SSLMODE"),
  bigint = "numeric", connect_timeout = 15L)
on.exit(dbDisconnect(con))
dbExecute(con, "SET statement_timeout = 90000")   # else the app default kills the probe

gy <- 2026L
i <- dbGetQuery(con, "select min(game_date) mn, max(game_date) mx
                        from basketball_test.final_schedule_mv where game_year=$1", list(gy))
s <- as.Date(i$mn); e <- as.Date(i$mx); mid <- s + as.integer((e - s) * 0.5)

sql <- paste0("SELECT * FROM basketball_test.get_player_traditional_dynamic(",
  "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,$7::text,$8::text,$9::text,",
  "$10::int4,$11::text,$12::int4,$13::text,$14::int4,$15::bool,$16::int4,$17::int4,$18::int4)")
P <- function(start) list(gy, start, e, NA, NA, NA, "all", "all", "all", NA, "net",
                          NA, "all", NA, FALSE, NA, NA, NA)

for (lab in c("full", "narrowed")) {
  p <- if (lab == "full") P(s) else P(mid)
  for (k in 1:2) {
    t0 <- Sys.time(); d <- dbGetQuery(con, sql, params = p)
    cat(sprintf("%-10s run%d %6.2fs rows=%d\n", lab, k,
        as.numeric(difftime(Sys.time(), t0, units = "secs")), nrow(d)))
    flush.console()
  }
}
```

For Tabs 1 / 2 / 3, use **named-parameter notation** so every unset argument keeps its catalog
`DEFAULT` — this is what makes the probe match the app's request shape without transcribing 29
positional arguments:

```r
dbGetQuery(con, "SELECT * FROM basketball_test.get_team_ratings_dynamic(
   p_game_year=>$1::int4, p_start_date=>$2::date, p_end_date=>$3::date)", list(gy, mid, e))
```

Get real signatures from the catalog rather than from `CLAUDE.md` — **its parameter counts are
stale** (it says `fetch_lineups_csv_v2` takes 20; it takes 29):

```sql
select p.proname, pg_get_function_arguments(p.oid)
  from pg_proc p join pg_namespace n on n.oid = p.pronamespace
 where n.nspname = 'basketball_test' and p.proname = 'fetch_lineups_csv_v2';
```

**Operational note:** run these in the background writing to a log file, not piped through `tail`.
A first attempt piped a 10-minute job through `tail` and lost all output when it was killed.

---

## 5. What cannot be measured this way

`get_player_traditional_dynamic` is `LANGUAGE plpgsql` (`sql/functions/get_player_traditional_dynamic.sql:56`),
so `EXPLAIN (ANALYZE, BUFFERS)` on the call shows a single opaque `Function Scan`. Internal node
costs cannot be attributed without extracting the body into a standalone query.

**This is the main open question.** Narrowing the date range to half the season bought only
`B/A = 0.72`, not the ~0.5 you would expect. Whether that is fixed setup cost, a non-selective
index, or something else is **unverified**. Anyone sizing the fix in section 6 should extract the
body and profile it first.

---

## 6. Implemented fix — per-game traditional fact

**The Israeli database migration was applied on 2026-08-23. The non-clutch
Shiny routing was deployed by the maintainer the same day.**

The `SUM(CASE WHEN ...)` expressions already exist verbatim in
`sql/materialized_views/player_traditional_stats_mv.sql`. The change is to run them grouped by
`game_id` as well as `(game_year, team_id, player_id)`, materialise that as a per-game fact, and
have the new `get_player_traditional_from_games` reader sum it instead of
scanning `df_pts_poss_lineups_longer_mv`. Clutch requests continue to use
`get_player_traditional_dynamic`.

Filtered non-clutch requests now sum a small table. This mirrors what EuroLeague migrations
037 / 038 did for team and lineup readers.

### 6.1 Prototype and migration result (2026-08-23)

`scripts/prototype_israeli_tab5_traditional_by_game.R` built an
Israeli full-game candidate in a PostgreSQL temporary table using
`app_readonly` and established the design before DDL. A separate ETL-credential
smoke test installed the proposed objects inside a transaction, validated them,
and rolled everything back before the permanent apply.

| Measurement | Result |
|---|---:|
| Final live full build | 21.19 s |
| Persisted rows / games | 8,630 / 439 |
| Persisted size including indexes | 2,170,880 bytes (2.07 MiB) |
| `app_readonly` 2026 full-season reader | 0.74 s, 313 rows |
| `app_readonly` 2026 second-half reader | 0.34 s, 266 rows |
| Transactional single-game refresh | 1.14 s, 19 rows touched |

The same filtered request exceeded the app's 20-second limit on the current
dynamic path. The prototype therefore establishes both selectivity and useful
size: this is not a speculative optimization.

The initial USG mismatch was caused by aggregating team denominators only over
games in which each player had a fact row. The existing Israeli semantics use
all selected team games. Aggregating team-game denominators independently made
USG match all season rows exactly; the free-throw parent-ID collision hypothesis
was tested and rejected (zero affected player-seasons and team-seasons).

Storing additive `seconds_on_floor` and rounding only after aggregation removed
all minute differences. An intermediate build still differed on Adam Smith's
`reb` and `dreb`: it had dropped a rostered actor-only player-game with no
resolved lineup exposure. Retaining roster-eligible actor rows while continuing
to exclude opponent-perspective actors restored exact parity without doubling
the table. Final 2026 verification through `app_readonly` reports identical
keys and zero differing cells against the refreshed season MV.

Implemented objects and integration:

- `player_traditional_by_game`, with a unique game/team/player key and filter index;
- `compute_player_traditional_by_game(int4[])` and incremental
  `refresh_player_traditional_by_game_for_games(int4[])`;
- app-only `get_player_traditional_from_games(...)`, which rejects clutch parameters;
- incremental ETL registration and L3 rebuild-registry registration;
- `app_readonly` SELECT/EXECUTE grants plus RLS, while internal compute/refresh
  functions remain inaccessible to the app role;
- local Tab 5 routing: non-clutch uses the new reader, clutch keeps the action reader.

### 6.2 Standard clutch cache (completed 2026-08-23)

The separate fixed-scope Israeli fact is now live for exactly margin <= 5,
status `all`, final 5:00, and unrestricted overtime. Arbitrary custom clutch
remains on the action path; no general custom-clutch cache was added.

| Measurement | Result |
|---|---:|
| Live build | 8.13 s |
| Persisted rows / games | 2,756 / 213 |
| App-role persisted size including indexes | 778,240 bytes (0.74 MiB) |
| App-role 2026 full-season reader, first / second run | 0.78 s / 0.31 s |
| Full-season cached-vs-dynamic parity | exact, 231 rows |
| Transactional single-game refresh | 0.52 s, 18 rows touched |

Implementation details:

- `compute_player_traditional_by_game(int4[], boolean)` adds one explicit
  standard-clutch mode while the existing one-argument function remains the
  non-clutch wrapper;
- `default_clutch_player_totals_by_game` stores the same additive player and
  team denominators as the non-clutch fact, using qualifying-action segment
  extrema for clutch minutes;
- `refresh_default_clutch_player_totals_for_games(int4[])` is wired into the
  level-3 incremental ETL publication path;
- `get_player_traditional_from_games(...)` accepts either no clutch predicate
  or the exact standard preset and rejects every custom definition;
- Tab 5 routes `pergame` and `dynamic` request kinds to that cached reader,
  while `direct` continues to call `get_player_traditional_dynamic(...)`;
- the post-migration database security audit passed, and `app_readonly` can
  read/call only the fact and reader, not either compute or refresh function.

The remaining release action is deploying the new Tab 5 routing code. The
database cache and incremental publication lifecycle are already live.

---

## 7. Other open items

Ranked. None blocks the deploy.

### 7.1 Split the `refresh_derived_for_games()` transaction — EuroLeague

Carried over from the merged DQ work. All eight refresh functions run in **one transaction per
game**, so the migration-030 clutch failure rolled back the actions-consumer, four-factors, lineup
and player facts too — one bad column cost 53 games of unrelated statistics. Fixed by migration
040, but the fragility remains.

### 7.2 Orphaned SQL function with a live grant

`euroleague.get_player_traditional_clutch`
(`euroleague/sql/026_player_stats_single_action_scan.sql:222`) is a plpgsql standard-vs-custom
dispatcher that **nothing calls** — verified by grep across `.R`, `.sql` and `.py`. It still holds
an `app_readonly` EXECUTE grant and appears in `sql/security/enable_readonly_rls.sql:75` and
`sql/security/audit_app_access.sql:58`.

Drop it together with both security entries, or wire it up. Do not leave it granted.

### 7.3 The routing rule still exists in two places

After `ae3cec7` the three-way clutch classification lives in exactly two:

- `clutch_reader_kind()` — `app/R/helpers.R:636`, used by Tabs 5, 9, 10.
- `euroleague.select_team_game_facts` — `euroleague/sql/020_default_clutch_fast_path.sql:142`,
  an `IF / ELSIF` on the identical conditions.

They agree today (the SQL copy uses `coalesce(nullif(btrim(x),''),'all')`, matching the helper),
but nothing enforces it. The branch is therefore decided twice per request: R picks the function
family, SQL picks the fact source again.

**Two is a large improvement over five** — Tabs 5, 9, 10, plus this SQL copy, plus the deleted
grep-based tests all encoded it before this session.

### 7.4 Latent trap: the `""` blank-sentinel convention

`CLAUDE.md` states that `""` is the blank sentinel for every single-select filter. The clutch
status selector deliberately breaks that, using `choices = c("All" = "all", ...)`.

**That break is load-bearing.** Tab 5's old inline copy normalised status with `msv %||% "all"`,
and `%||%` is NULL-only (`app/R/helpers.R:35`), so an `NA` status stayed `NA` and failed
`identical(., "all")`. Because the selector never produces a blank, the bug was unreachable.

`ae3cec7` removed that copy, so the trap is gone from Tab 5 — but if anyone "fixes" the selector to
follow the documented convention, re-check every consumer of `margin_status`.

---

## 8. Assumptions and limits

Read before quoting any number above.

**Measurements**

- **One season only** — 2026, 221 games. Other seasons unsampled.
- **Two runs per preset**; three presets on Tab 5, one or two per other tab. Scope was
  deliberately limited. Directional, not a sizing basis.
- **Shared instance.** Timings move with contention: Tab 5's baseline went *up* between runs
  (82.68 → 89.21 s), which is contention, not caching. Minimums are reported for that reason.
- **The timeout was raised to obtain the figures** (90 s for Tab 5, 60 s for the tab sweep).
  Nothing here ran under the app's real 20 s limit.
- **Connection path matches the app** — `app_readonly`, pooler 6543 — so that is not a confound.

**Unverified**

- Why date filtering scales at only `B/A = 0.72` (section 5).
- Whether 20 s is the right limit. It was treated as fixed. If Tab 5's cost is inherent, the limit
  itself may deserve the argument.
- Whether the deployed `.Renviron` overrides `PG_STATEMENT_TIMEOUT_MS`. Not inspected.

**Scope of the `ae3cec7` equivalence proof**

Both versions of Tab 5's reader were replayed over **180 inputs** spanning every
`resolve_clutch_params()` combination, diffing composed SQL and parameter list: **0 differing**,
all three readers exercised (90 `_dynamic`, 88 `_custom_clutch`, 2 `_standard_clutch`).

That covers the app's whole reachable input space, **not** every input the function accepts. Two
hypothetical inputs do diverge (blank or `NA` status with a set margin); both are unreachable only
because of the convention break in 7.4.

---

## 9. Related documents

- `euroleague/CLAUDE.md` — EuroLeague review remarks and the staleness notice.
- `euroleague/PROJECT.md` — EuroLeague status and plan.
- `docs/adr_api_owns_query_construction.md` — the trigger for retiring mega-signature SQL
  functions, relevant if Tab 5 ever goes React-only.
- `sql/rebuild_all_mvs.R` — MV rebuild order, required reading before adding a relation.
