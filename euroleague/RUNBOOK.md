# EuroLeague load runbook

How to load games into the `euroleague` schema without needing an agent session.

## Current checkpoint (2026-08-12)

- Games 1-100 were loaded and passed the scoped verification.
- Cached source coverage was subsequently extended through game 402. PBP was
  known missing for game 396 and games 400-402 before the latest retry; use
  `--allow-missing-inputs` if intentionally publishing around such gaps.
- Load run 12 committed base `actions` snapshots for 80 games, but its grouped
  derived refresh failed on provider gamecode 249. The package lineup contains
  `BOLOMBOY, JOEL` twice, violating the unique-player lineup constraint.
- Derived refreshes now run one game per transaction. A future failure records
  that game and continues; it no longer rolls back neighboring derived games.
- The EuroLeague consumer refresh uses the migration-016 non-cross-product
  action-lineup design.
- Migration 020 is implemented but not applied. Once approved and applied, the
  standard clutch preset is served from an exact per-game cache refreshed only
  for changed games; custom clutch definitions remain action-level.

## TL;DR

```bash
cd euroleague

# 1. See what it would do. Collects and stages; writes NOTHING to the database.
.venv/Scripts/python.exe scripts/load_games.py --games 85-150

# 2. Actually load (only after explicit approval).
.venv/Scripts/python.exe scripts/load_games.py --games 85-150 --execute

# 3. Re-check a load at any later time.
.venv/Scripts/python.exe scripts/load_games.py --games 85-150 --verify-only
```

Exit code is `0` if every check passed, `1` otherwise.

## What a "load" actually is

Four steps, all wrapped by `scripts/load_games.py`:

1. **Collect box scores** — provider request per game, cached under
   `data/raw/boxscores/`.
2. **Collect play-by-play** — provider request per game, cached under
   `data/raw/pbp/`, plus a combined CSV the stager reads.
3. **Stage** — *offline*. Runs the possession parser and the package lineup
   enrichment over the cached raw data and writes a checkpoint per game under
   `data/staging/`. No network, no database. It produces the canonical
   columnar `actions` rows directly, including `lineup_a`, `lineup_b`, and the
   possession-end marker and ownership fields.
4. **Publish** — one base-snapshot transaction per game into the `euroleague`
   schema. After the complete base batch commits, derived consumers refresh in
   independent per-game transactions, followed by one app-view refresh.

**Everything is resumable.** Both collectors skip payloads already cached and
staging reuses checkpoints, so re-running after a failure only redoes the work
that is genuinely missing. Re-running a game that is already published replaces
it.

## Arguments worth knowing

| flag | meaning |
|---|---|
| `--games` | `85-150`, `1,2,3`, or `1-10,25,40-42` |
| `--season` | provider season. **2025 = the 2025-26 season** |
| `--competition` | `E` = EuroLeague, `U` = EuroCup |
| `--execute` | actually publish. Without it, nothing touches the database |
| `--verify-only` | just re-run the checks against what is already loaded |
| `--throttle` | seconds between provider requests (default 0.75) |
| `--collect-workers` | collector concurrency; use `1` for a cautious run |
| `--stage-workers` | staging concurrency; use `1` for a cautious run |
| `--fetch-batch-size` | provider-fetch batch size (default 20) |
| `--fetch-batch-sleep` | cooldown between fetch batches (default 30 seconds) |
| `--fetch-only` | fetch/cache provider payloads, then stop before staging |
| `--boxscores-only` | fetch/cache only box scores, then stop |
| `--skip-fetch` | reuse cached payloads and start at staging/publication |
| `--allow-missing-inputs` | with `--skip-fetch`, omit missing/invalid PBP games with a warning |

## Independent operator workflow

Run these commands from the `euroleague` directory in PowerShell:

```powershell
Set-Location C:\Users\ariel\documents\on_off_israel_pbp\euroleague
& .venv\Scripts\python.exe scripts\load_games.py `
  --games '101-110' --season 2025 --competition E
```

This is a dry run: it collects cached provider data and stages checkpoints,
but does not write to PostgreSQL. Publish only after inspecting the staging
output:

```powershell
& .venv\Scripts\python.exe scripts\load_games.py `
  --games '101-110' --season 2025 --competition E `
  --execute --collect-workers 1 --stage-workers 1
```

Publication commits each game's base snapshot independently. Once the complete
base batch is present, derived consumers refresh in independent per-game
transactions, followed by one app materialized-view refresh. A failed game does
not roll back earlier games. Verification runs
automatically and the command returns exit code 0 only when every check passes.

Note what that exit code does and does not mean. Verification asserts that
every gamecode **you requested** is present and sound. It cannot tell you the
range itself was wrong: a game outside `--games` is not checked, not reported,
and not counted as missing. Exit code 0 says "what you asked for arrived", never
"you asked for everything". The season-completeness checks in the data quality
report are what answer the second question.

Verify without collecting or writing:

```powershell
& .venv\Scripts\python.exe scripts\load_games.py `
  --games '101-110' --season 2025 --competition E --verify-only
```

Use `--competition U` only for EuroCup. `E` and `U` are provider competition
codes; do not mix them in one load range.

### Two-phase large load

For a large range, separate provider traffic from database processing. **Both
phases must use the identical `<FIRST>-<LAST>` range.** They are independent
arguments, so a phase 2 range narrower than phase 1 leaves the difference cached
but unpublished, with no error — see the warning under *Partial-cache and
box-score workflows* for what that cost once.

Phase 1 fetches all payloads in 20-game batches and pauses between batches:

```powershell
& .venv\Scripts\python.exe scripts\load_games.py `
  --games '<FIRST>-<LAST>' --season 2025 --competition E `
  --fetch-only --collect-workers 1 --fetch-batch-size 20 --fetch-batch-sleep 60
```

After all payloads are cached, phase 2 rebuilds the combined PBP input from the
cache and performs staging/publication without contacting the provider:

```powershell
& .venv\Scripts\python.exe scripts\load_games.py `
  --games '<FIRST>-<LAST>' --season 2025 --competition E `
  --skip-fetch --execute --stage-workers 1
```

This is restartable. If phase 1 stops at an API threshold, rerun the same
`--fetch-only` command; cached games are skipped and only missing games are
requested. If phase 2 fails, rerun it with `--skip-fetch`.

### Partial-cache and box-score workflows

> **Substitute your own range for `<FIRST>-<LAST>` below, and check it against
> what is actually cached before running.** These commands previously carried a
> literal `217-402`. On 2026-08-11 a fetch swept games 100 through 250+, but the
> publish that followed was copied from here with its literal range intact and
> started at 217. Gamecodes **111-216** — rounds 12-21 plus six of round 22 —
> were therefore fetched, validated and cached, and never staged or published.
> They stayed missing for eight days while the app served team ratings computed
> without them, understating every team's games played by up to eleven.
>
> Nothing caught it: `--verify-only` asserts that every *requested* gamecode is
> present, and a range nobody requested is never checked. The fetch range and
> the publish range are independent inputs, and **a publish range narrower than
> what you cached fails silently** — there is no error, just absent games.
>
> The report now has two checks for exactly this
> (`Rscript scripts/run_euro_data_quality_report.R`):
> `N10_regular_season_gamecode_gaps` finds holes inside the loaded regular
> season, and `N11_cached_payloads_never_loaded` finds cached payloads with no
> schedule row. Run it after any load whose range you composed by hand.

PBP is fetched before box scores in the current orchestrator because it is the
main API bottleneck. To fill only box-score gaps without touching PBP or the
database:

```powershell
& .venv\Scripts\python.exe scripts\load_games.py `
  --games '<FIRST>-<LAST>' --season 2025 --competition E `
  --boxscores-only --collect-workers 1 `
  --fetch-batch-size 20 --fetch-batch-sleep 60
```

To publish every available cached game while deliberately omitting missing PBP:

```powershell
& .venv\Scripts\python.exe scripts\load_games.py `
  --games '<FIRST>-<LAST>' --season 2025 --competition E `
  --skip-fetch --allow-missing-inputs --execute --stage-workers 1
```

`--skip-fetch` always rebuilds the combined PBP CSV from validated per-game
cache files. With `--allow-missing-inputs`, a missing PBP file produces a
warning and that game is excluded from staging and verification. No empty game
is synthesized.

### Recovery and restart

- Rerunning the same range is safe: cached inputs and checkpoints are reused,
  and published games are replaced transactionally.
- If one base-game transaction fails, other games remain committed; rerun the
  range after addressing the reported failure.
- If a derived game refresh fails, its base snapshot remains committed and other
  games continue; fix or isolate the failing game, then rerun it.
- Do not truncate the schema or point this loader at Israeli schemas.
- Inspect `euroleague.load_runs` after an interrupted process; a `running` row
  may be stale and should be resolved before another live load.

### Database prerequisites

Publication requires `etl/.Renviron` and direct PostgreSQL port 5432. Apply the
EuroLeague migrations must be applied in order through the current repository
migration. Migration 020 is currently pending explicit live-DDL approval; do
not publish with backend code that expects its cache table before applying it.
Keep the target schema as `euroleague`.

## How long it takes

| step | rate |
|---|---|
| collect (box score + PBP) | ~1-2 games/sec |
| stage | ~1.3 games/sec |
| publish | the slow one — see below |

The table below is historical. It predates migration 012; the simplified write
path now inserts seven snapshot relations and needs a fresh multi-game timing
sample before publication capacity is forecast.

Measured per game against the live schema at 84 games loaded before migration
012:

| publication phase | before migration 007 | after 007 | after 008 | after 009 |
|---|---|---|---|---|
| `begin` (resolve schedule + dimensions) | ~0.5 s | ~0.5 s | ~0.5 s | ~0.5 s |
| delete the game's replaceable rows | ~1.2 s | ~1.2 s | ~1.5 s | ~1.5 s |
| insert the whole snapshot | ~1.8 s | ~1.9 s | ~1.9 s | ~1.9 s |
| validate | 24-38 s | **~1.5 s** | **1.86-2.32 s** | **1.54-2.11 s** |
| **total** | **~28-42 s** | **~5 s** | **~5-6 s** | **~5-6 s** |

Validation was ~95% of a publication before migration 007, effectively all of
it the single
call to `refresh_player_four_factors_by_game_for_games()`. Migration 007 fixed
that function's query plan; see its header for the diagnosis. Migration 008
added `refresh_action_team_context_for_games()` — which rebuilds the persisted
event x team-perspective fact and the matchup-segment table — as the first
statement inside `validate_game()`, ahead of the four-factor refreshes it will
eventually replace; that accounts for the small validate increase over 007.
Migration 009 then made that replacement: both four-factor refreshes read the
fact instead of re-deriving the expansion themselves, so the derivation was
paid once per publication rather than three times.

Migration 012 replaced that fact refresh with
`refresh_actions_consumer_candidates()`, sourced from canonical `actions`.
Do not use the pre-012 **5-6 s/game** result as a current performance promise.

After migration 020, publication also calls
`refresh_default_clutch_for_games()` immediately after the action consumers.
The refresh deletes and rebuilds only the published game IDs. Measure this
incremental cost before forecasting large-load throughput.

## What the verification checks

`--verify-only` (also run automatically after `--execute`) asserts:

- the latest load run is `completed`, not `partial` or `running`
- every requested gamecode is present
- `round_number`, `phase` and `scheduled_at` are non-NULL — **these drive every
  date, round and phase filter, and NULLs silently empty those filters instead
  of erroring**
- one `parser_version` across the schema, so aggregates do not mix derivations
- team points match the official box score exactly
- possessions are symmetric between opponents
- team four factors match the independently-derived player fact ÷ 5
- every game has team analytics, not only player analytics
- every game has rows in the persisted event x team-perspective fact
  (`action_team_context_actions`)
- canonical `actions` has exactly the same event keys as `actions_raw`
- all 22 package fields reconstructed from `actions` exactly equal `raw_event`
- possession endpoint numbers are gap-free on canonical `actions`
- the team aggregate is exactly reproducible from
  `action_team_context_actions`
- the ratings MV agrees with the dynamic function

It also prints the `game_qa` status breakdown and the schema's size per game.

## Proving a publication-path change is safe

`scripts/probe_batched_publish.py` republishes already-loaded games through the
real backend against the real database and then **rolls back**. Nothing is
committed; only id sequences advance.

```bash
.venv/Scripts/python.exe scripts/probe_batched_publish.py --games 1-3
```

It compares stable projections of the directly persisted snapshots,
`actions`, `matchup_segments_actions`, `action_team_context_actions`, and both
four-factor facts before and after a real validation pass. It then rolls the
transaction back and proves the original rows were restored. Run it after any
publication-path or actions-consumer change.

## When something goes wrong

**A load run is left `partial`.** At least one game failed and rolled back. The
others committed. Re-run the same `--games` range with `--execute`; published
games are replaced and missing ones are filled in.

**`existing euroleague schema has unknown tables: [...]`.** A migration added a
table that the loader's allowlist does not know about. Add it to `expected` in
`assert_shadow_schema_compatible()` (`postgres_backend.py`) in the same change
that creates the table. This guard fires *before* any write.

**`checkpoint failed integrity check`.** The checkpoints were staged by an older
code version (`STAGE_FORMAT_VERSION` changed) or the cached inputs changed.
Delete that checkpoint directory and re-run; staging will rebuild it.

**Schedule fields are NULL.** The provider schedule lookup failed at staging
time — it is advisory and never aborts a load. Re-stage, or backfill from the
schedule endpoint.

## Things this does NOT do

- **EuroCup has never been collected.** `--competition U` will attempt it, but
  nothing has validated that EuroCup box scores carry the `IsStarter` flags the
  lineup engine bootstraps from, or that its play-type vocabulary matches. Try
  one game before trusting a batch.
- **No scheduling.** There is no cron/Task Scheduler entry; this is run by hand.
- **No cold storage.** Unlike the Israeli ETL, nothing is truncated or exported
  to Parquet after a run. Everything stays hot.

## After a migration: re-run the security pass

The `euroleague` schema is covered by the repository-wide database security
contract (`sql/security/*.sql`), applied and audited from the repository root:

```bash
"$RSCRIPT" scripts/audit_db_security.R                              # read-only, expects zero violations
CONFIRM_DB_SECURITY_APPLY=1 "$RSCRIPT" scripts/apply_db_security.R  # apply (dry-run without the env var)
```

Run the apply step after **any** EuroLeague migration, for two reasons:

- `CREATE OR REPLACE FUNCTION` on a new signature, and any `DROP FUNCTION`,
  leaves the function executable by `PUBLIC` and wipes `app_readonly`'s
  EXECUTE grants. This is how `anon` and `authenticated` came to hold EXECUTE
  on all fourteen EuroLeague functions, including the mutating `refresh_*`
  ones (fixed 2026-08-12).
- A new base table is created without RLS, and the app's read policy only
  exists because the script creates it.

Two things differ from the Israeli schemas, both deliberate and both enforced
by the audit:

- **Curated relation grants.** `app_readonly` gets SELECT on the enumerated
  read layer only — not `GRANT SELECT ON ALL TABLES`. Raw provider evidence
  (`actions_raw`, `source_artifacts`), load bookkeeping (`game_qa`,
  `qa_incidents`, `reconciliation_metrics`) and the large derived facts stay
  closed. **A new app-facing relation must be added to `euro_app_relations` in
  both `sql/security/*.sql` files**, or the audit fails with
  `app_required_relation_select_missing`; conversely anything readable beyond
  that list fails with `app_unexpected_relation_select`.
- **No `service_role`.** The schema is deliberately outside Supabase's managed
  surface and must never be added to the Data-API exposed schemas.

The clutch-aware readers and their internal selector use scoped
`SECURITY DEFINER` access because their source facts are intentionally closed
to `app_readonly`. Their `search_path` values are pinned and relations are
schema-qualified. Migration 020 grants no direct table access and no execution
on its mutating refresh function or internal selector.

Publication is unaffected by RLS: every `euroleague` table is owned by
`postgres`, no table sets `FORCE ROW LEVEL SECURITY`, and a table owner
bypasses its own RLS policies.

## Prerequisites

- `euroleague/.venv` with the project installed.
- `etl/.Renviron` with write credentials. Publication requires the **direct**
  port 5432, not the 6543 pooler; the loader refuses otherwise.
- Migrations applied in order: `001 -> 002 -> 004 -> 005 -> 006 -> 007 -> 008 -> 009 -> 010 -> 011 -> 012 -> 013 -> 014 -> 015 -> 016 -> 017 -> 018 -> 019 -> 020`.
  **`003` is superseded and must not be applied.**
- Apply migration 020 only after explicit approval:
  `.venv/Scripts/python.exe scripts/apply_020_default_clutch_fast_path.py`.
  The script checks exact cache parity and benchmarks the standard preset.
  Run the security apply/audit pass immediately afterward.
