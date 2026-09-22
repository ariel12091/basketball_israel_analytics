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

Status 2026-09-23: **loaded and linked.** 1160 player-team-seasons across
2022-2026 are in `basket_player_season`, reconciled row by row, and all 533
registrations in the two seasons with play-by-play coverage (2025-2026) are
matched to identities. Offline tests pass, 64 assertions
(`testthat::test_file('etl/tests/test_basket_shooting_history_parser.R')`).
All migrations applied. The 210 MB scrape cache means any re-run is
cache-only.

Next season: `game_year` 2027 needs a scrape once the regular season starts
(`game_type = 5` rows appearing for 2027; as of 2026-09-23 the schedule has
only cup and preseason games). Nothing schedules it.

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

## Identity

**Solved for 2025-2026: 533 of 533 registrations linked (100%).** Run
`etl/basket_identity_match.R` (dry run by default, `--write` to fill the
columns). 2022-2024 keep a NULL status — the play-by-play side starts at 2025,
so there is nothing to match against, and the schema distinguishes that from
having looked and failed.

Basket PlayerIds are **per-team-season registration ids** in their own id
space, re-minted yearly in non-overlapping ascending blocks (2022: 12723-12963
… 2026: 21777-25995, every pairwise intersection empty). They never join
directly to anything. Matching therefore runs per season:

1. Resolve `basket_team_name` to a `team_id` through
   `schedule_team_dict.team_name_basket`.
2. Candidates are identities on that same team-season whose English **or**
   Hebrew name normalises equally. Names are folded for the punctuation that
   differs between sources — initials, quoted nicknames, hyphens, generational
   suffixes, and the Hebrew maqaf.
3. Candidates are deduplicated on `canonical_player_id`, since the alias
   machinery already collapses duplicate identity records onto one.
4. A unique surname within the team is a *proposal*, never auto-accepted.
5. Ten reviewed matches are recorded as data in
   `manual_basket_identity_matches()`, so a re-run reproduces them. A manual
   entry contradicting an automatic match raises rather than overriding.

**Restrict the candidate pool to real roster appearances.**
`resolved_player_identity_v` fans every active season mapping across all of
that team-season's games, so a player with a single stray roster row appears
in every game of a team he never played for. Join it to `full_rosters` on
`game_id` as well as team. Skipping this put Rishon Lezion's DJ Burns in Bnei
Herzliya's pool and manufactured three ambiguities that were not real.

### Neither key identifies a person across seasons

This was asserted wrongly twice before being measured. Both keys are
**season-scoped**:

| key | use | why not a career key |
|---|---|---|
| `canonical_player_id` | join to play-by-play facts **within** a season | recycled: 18 of 712 belong to two people, e.g. 1119 is AMIT GERSHON in 2025 and MICHAEL FOSTER JR. in 2026 |
| `identity_id` | the resolved identity for that registration | split by re-minting: `identity_key` embeds the source id, so `segev:1025:TAMIR BLATT` and `segev:1091:TAMIR BLATT` are one man; only 109 of 732 span a season boundary |

So always pair `canonical_player_id` with `game_year`. There is no reliable
cross-season person key in the schema today. `date_of_birth` is the strongest
candidate for building one: it is on every Basket row and on no play-by-play
relation at all.

`identity_id` is also **not** a `player_identities` foreign key. The view
COALESCEs it to the raw source id when a player has no curated identity
(`resolution_scope = 'source'`), which is rare but real — 18 rows, one player,
across 2025-2026.

### What the link found

Date of birth, being independent of both sources' naming, doubles as an audit.
Grouped by identity, no Basket rows disagree on it. It also caught a wrong
merge in `player_id_aliases`: Bnei Herzliya's 1982 and Rishon Lezion's 1143
were recorded as one re-minted player, but they appear on opposite sides of
game 147, and Basket has them differing in birth date, height, shirt number
and shooting profile. Retired, with game 210's genuine one-game id reuse moved
to `player_id_game_overrides`.

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
