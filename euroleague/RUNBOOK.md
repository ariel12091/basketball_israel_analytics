# EuroLeague load runbook

How to load games into the `euroleague` schema without needing an agent session.

## TL;DR

```bash
cd euroleague

# 1. See what it would do. Collects and stages; writes NOTHING to the database.
.venv/Scripts/python.exe scripts/load_games.py --games 85-150

# 2. Actually load.
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
3. **Stage** — *offline*. Runs the possession and lineup engine over the cached
   raw data and writes a checkpoint per game under `data/staging/`. No network,
   no database. This is where possessions, lineups, stints and pws are derived.
4. **Publish** — one transaction per game into the `euroleague` schema, each
   validated before it commits, under a single load run.

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

## How long it takes

| step | rate |
|---|---|
| collect (box score + PBP) | ~1-2 games/sec |
| stage | ~1.3 games/sec |
| publish | the slow one — see below |

Measured per game against the live schema at 84 games loaded:

| publication phase | before migration 007 | after |
|---|---|---|
| `begin` (resolve schedule + dimensions) | ~0.5 s | ~0.5 s |
| delete the game's replaceable rows | ~1.2 s | ~1.2 s |
| insert the whole snapshot | ~1.8 s | ~1.9 s |
| validate | 24-38 s | **~1.5 s** |
| **total** | **~28-42 s** | **~5 s** |

Validation used to be ~95% of a publication, effectively all of it the single
call to `refresh_player_four_factors_by_game_for_games()`. Migration 007 fixed
that function's query plan; see its header for the diagnosis. Budget roughly
**5 s/game**, so under 30 minutes for the ~318 games remaining in the season.

Publication is now round-trip bound again. If it needs to get faster, the next
target is the ~1.2 s spent deleting the game's replaceable rows, which is 13
serial statements.

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
- the ratings MV agrees with the dynamic function

It also prints the `game_qa` status breakdown and the schema's size per game.

## Proving a publication-path change is safe

`scripts/probe_batched_publish.py` republishes already-loaded games through the
real backend against the real database and then **rolls back**. Nothing is
committed; only id sequences advance.

```bash
.venv/Scripts/python.exe scripts/probe_batched_publish.py --games 1-3
```

It compares a *natural-key projection* of everything the generated ids wire
together — lineups, lineup members, action lineups, stints, pws and the
downstream `player_game_context` fact — with each lineup written as
`(team, lineup_hash)` and each stint as `(team, stint_number)`. Surrogate ids
differ on every insert, so only that projection is comparable, and it must be
identical to what is already stored. Run it after any change to how rows are
inserted or how generated ids are resolved.

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

## Prerequisites

- `euroleague/.venv` with the project installed.
- `etl/.Renviron` with write credentials. Publication requires the **direct**
  port 5432, not the 6543 pooler; the loader refuses otherwise.
- Migrations applied in order: `001 → 002 → 004 → 005 → 006 → 007`. **`003` is
  superseded and must not be applied.**
