# Basket shooting-history ingestion

## Scope

This ingestion creates a raw, auditable source layer for luck adjustment. It
does not yet calculate expected points or change any app query.

`cYear` is the season's ending year: `cYear=2026` means 2025-26 and maps to the
project's `game_year=2026`.

## Source and grain

- Accumulated stats: one row per `(game_year, basket_player_id)`, with exact
  2PT, 3PT and FT makes/attempts. Pagination uses `c=1,2,...`.
- Player profile: one row per season-registration ID. Position, listed height,
  and nationality are season observations, not durable person attributes.
- A Basket `PlayerId` is a registration ID and may change between seasons.
  Never join different seasons solely on that ID or on a player name.

The source endpoints are:

```text
https://basket.co.il/stats-accumulate.asp?StatsBoard=0&c=1&cYear=2026&lang=he&local=0&maxYear=2026&minYear=2026&sType=TO&selectedTeam=0&stats_options=1
https://basket.co.il/player.asp?PlayerId=21797
```

## Files

- `sql/migrations/2026-09-22_basket_shooting_history.sql`: additive ETL tables,
  provenance, identity-review table, and resolved view.
- `sql/migrations/2026-09-22_basket_shooting_history_english.sql`: adds
  `player_name_en` (shooting) and `position_en` (profiles) and exposes both in
  the views. Required by `--write`; preflight stops if it is missing.
- `scripts/apply_basket_shooting_history_migration.R`: transaction-wrapped DDL;
  validates and rolls back unless `--commit` is supplied. `--file=PATH` selects
  a migration other than the base one.
- `scripts/backfill_basket_shooting_history.R`: scraper and idempotent upsert.
  It is dry-run by default. Profile requests are opt-in because they require one
  additional request per season-player. A run is hard-limited to five seasons.
- `etl/tests/test_basket_shooting_history_parser.R`: offline HTML fixtures.

Status 2026-09-22 (evening): offline tests pass, 58 assertions
(`testthat::test_file()`). Both migrations are applied, including the English
columns. The scraper has run against the site but has **not** completed a write:
668 of 1160 profiles and 25 of 30 `accumulate-en/` pages are cached, and the
last run ended on the maqaf position described below. Rerunning resumes from
the cache.

## Suggested execution

Use the configured R 4.4.2 executable from `PROJECT.md`.

```powershell
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' scripts/apply_basket_shooting_history_migration.R
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' scripts/apply_basket_shooting_history_migration.R --commit
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' scripts/apply_basket_shooting_history_migration.R --file=sql/migrations/2026-09-22_basket_shooting_history_english.sql --commit

# Consolidation to three relations. Carries existing rows across with
# INSERT ... SELECT and verifies them before dropping anything; the cache is
# not consulted. Dry run first -- it rolls back without --commit.
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' scripts/apply_basket_shooting_history_migration.R --file=sql/migrations/2026-09-23_basket_shooting_history_consolidate.sql
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' scripts/apply_basket_shooting_history_migration.R --file=sql/migrations/2026-09-23_basket_shooting_history_consolidate.sql --commit

# Network/parser dry run for one season, including profiles.
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' scripts/backfill_basket_shooting_history.R --seasons=2026 --with-profiles

# Recommended first five-season pass: stop cleanly after 100 real HTTP requests.
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' scripts/backfill_basket_shooting_history.R --seasons=2022:2026 --with-profiles --stop-after-checkpoint

# Resume from cache and write after reviewing the checkpoint report.
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' scripts/backfill_basket_shooting_history.R --seasons=2022:2026 --with-profiles --write
```

The default season range is 2022–2026. More than five unique seasons is rejected.

Network responses are cached below `exports/cache/basket_shooting_history` and
reused on subsequent runs. Cache writes are atomic, and cache hits do not sleep
or contact the website. Use `--refresh-cache` when current-season data should be
downloaded again, or `--cache-dir=PATH` to relocate the cache.

New requests identify the project with a descriptive User-Agent and wait one
second between successful requests by default. Failed requests retry at most
five times with exponential backoff, jitter, and a 60-second cap. Controls are
`--pause-seconds=1`, `--profile-pause-seconds=1`, `--max-attempts=5`, and
`--initial-backoff=2`.

Every invocation writes a timestamped sanity log in the cache directory. The
script reports at five gates:

1. Preflight, before requests or SQL writes: validates configuration, the
   five-season cap, cache location, and (with `--write`) required SQL tables and
   current database size.
2. After the first 100 real HTTP attempts: reports successes, cache hits,
   downloaded/cache sizes, parsed rows, and profile-field completeness. Change
   the threshold with `--checkpoint-requests=N`. Cached files are not counted as
   HTTP attempts. `--stop-after-checkpoint` exits successfully at this gate and
   cannot be combined with `--write`; rerunning resumes from the cache. Without
   it the gate only reports and the run continues.
3. Before the SQL backfill: validates duplicate keys, made/attempted
   arithmetic, page/profile foreign keys, and height ranges.
4. After the first 100 SQL rows: the first N player-seasons (ordered by season,
   page, row) are upserted, read back, and compared value-by-value with the
   parsed rows before the rest are written. Change N with `--checkpoint-rows=N`.
   The whole write is one transaction, so a mismatch commits nothing.
5. After the backfill: reconciles SQL row counts, aggregate 2PA/3PA/FTA and
   profile counts, repeats the row-level comparison for every row, and reports
   final database size.

Any failed invariant stops the run *before the write*. If a small or fully
cached run never makes 100 HTTP requests, the pre-backfill report states that
explicitly instead of forcing unnecessary requests.

A repeated page fingerprint or the first empty page terminates pagination.
Malformed shooting totals, duplicate IDs, or makes greater than attempts stop
the run instead of silently dropping data.

## The download phase never halts

Changed 2026-09-22 after an unmapped position killed a 30-minute run at profile
668 of 1160. Nothing in the fetch loops is fatal any more: a profile or page
that will not download or parse is recorded in a failure log and the loop moves
on, so the download always reaches the end and the cache is complete when it
does.

The failure log then decides whether the write may proceed. Any entry at all
blocks it: the run prints every failure, writes a `DOWNLOAD COMPLETE WITH
FAILURES` sanity report, and exits without touching the database. Because the
cache is whole by that point, fixing the cause and rerunning costs no refetching.

Three consequences worth knowing:

- A stats page that fails is not read as the end of the season. Pagination
  continues past it, but a season gives up after
  `BASKET_MAX_CONSECUTIVE_PAGE_FAILURES` (3) consecutive failures rather than
  retrying every page to `--max-pages` while the site is down.
- A Hebrew/English disagreement in `merge_english_names()` is recorded instead
  of thrown. It is still a correctness stop — it blocks the write like any
  other entry — but it no longer costs the rest of the download.
- A parse failure still deletes the offending cached file, so a bad response
  served as 200 is refetched next run.

## English names and positions

The Hebrew accumulated pages remain the source of record. For every season the
script also fetches the same pages with `lang=en` (cached under
`accumulate-en/`) and takes `player_name_en` from them. They must list exactly
the same PlayerIds with identical games and made/attempted totals, or the run
stops. Verified 2026-09-22 on the 2026 first page: 50/50 IDs, identical totals.

`position_en` is not scraped. It is translated from the Hebrew profile label by
`BASKET_POSITION_EN`, built from one English profile per Hebrew label (G, PG,
G-F, SF, PF, F, F-C, C; `פ.פורוורד`/`פ. פורוורד` are both PF,
`פורוורד-סנטר`/`פורוורד/סנטר` both F-C). Nationality needs no translation:
`nationality_code` is language-neutral.

Translation runs **after** the download, in `resolve_profile_positions()`, not
inside the profile parse. An unlisted label is a lookup gap, not a bad page: it
blocks the write and keeps its cached file, but costs no refetching.

**The separator is normalised before the lookup.** The site writes one compound
position three ways that all render as a hyphen — ASCII `-` (n=75), `/` (n=3)
and the Hebrew maqaf `U+05BE` (n=1, season 2025, player 19704). That single
maqaf stopped a run on 2026-09-22; it is invisible next to a hyphen in an error
message, which is why unknown-position errors now print code points and name
every distinct unknown rather than only the first. `BASKET_POSITION_SEPARATORS`
folds the maqaf and the Unicode dashes to `-`, and spacing around a separator is
collapsed, so punctuation stays out of the lookup table. An unrecognised *word*
still fails closed.

## Tables

Three relations, all in `basketball_test`:

| relation | holds |
|---|---|
| `basket_player_season` | one row per `(game_year, basket_player_id)`: names, shooting totals, position/height/nationality, both provenance sets |
| `basket_shooting_import_runs` | one row per run: seasons requested, status, counts |
| `basket_shooting_source_pages` | one row per fetched stats page: URL, content hash, parsed rows |

`basket_player_season` is the only table to query. Player names are
`player_name` (Hebrew) and `player_name_en` — **not** on any other relation.
Profile columns (`position_name`, `position_en`, `height_m`,
`nationality_name`, `nationality_code`, `profile_*` provenance) are NULL unless
the run used `--with-profiles`, since each costs one extra request per
season-player. Two constraints carry invariants the scraper also enforces:
a fetched profile must yield at least one attribute, and a Hebrew position must
carry its English abbreviation.

Re-running upserts without deleting rows that disappeared from a later scrape.
The shooting upsert creates the row; the profile write then fills columns on it
and fails if it does not touch exactly one row per parsed profile.

Consolidated from five relations on 2026-09-23 by
`sql/migrations/2026-09-23_basket_shooting_history_consolidate.sql`.
`basket_player_season_shooting` and `basket_player_season_profiles` were 1:1 on
the same key with a view existing only to rejoin them.
`basket_player_identity_map` and `basket_player_identity_candidates_v` were
dropped unbuilt: the map was empty, and Basket PlayerIds do not match
`resolved_player_identity_v.source_player_id`, so the identity design should
follow the matching work rather than precede it.

## Identity is not solved

Nothing links these rows to the play-by-play players. Basket PlayerIds are
season-registration ids from a different id space (checked against four
players), so no automatic join exists. Until that work happens this layer
stands alone — useful on its own terms, not joinable to the app's tables.

## Quality checks before modeling

For every imported season:

1. Compare player count and aggregate 2PA/3PA/FTA with the source page.
2. Review profile rows with any missing attribute; missing profile data is
   allowed and does not invalidate shooting totals.
3. Review all ambiguous and unmapped identity candidates.
4. Check for one identity linked to implausibly overlapping players/teams.
5. Freeze a dated snapshot before estimating priors, so reruns are reproducible.

## EuroLeague adapter contract

Do not make the Basket scraper understand EuroLeague HTML. Add a provider
adapter that emits the same canonical columns:

```text
competition, season_code, provider_player_id, player_name,
games, minutes, points, fg2_made, fg2_attempted,
fg3_made, fg3_attempted, ft_made, ft_attempted,
position_name, height_m, nationality_name, nationality_code,
source_url, source_content_hash, fetched_at
```

Map `provider_player_id` to `euroleague.players` only within its documented
competition/season scope. If EuroLeague's person code is stable across seasons,
retain that as explicit identity evidence; otherwise use a review table like
Basket's. The luck model should consume a normalized provider-neutral view, not
either scraper's raw tables.

## Luck-model handoff

For a game on date `D`, calculate each player's prior from games strictly before
`D`. Combine current-season attempts, recency-weighted prior seasons, and a
league/role fallback with a beta-binomial shrinkage model. Store the chosen
prior source and effective sample size. Actual game points can then replace
`3PM * 3 + FTM` with `3PA * expected_3P% * 3 + FTA * expected_FT%`; 2PT luck can
be added later, preferably after shot-location/role controls.
