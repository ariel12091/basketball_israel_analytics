# Basket shooting layer + identity linking — handoff 2026-09-23

Session ran from a half-finished scrape to a complete, identity-linked layer,
and found two real defects in the play-by-play identity data on the way.
Everything is committed to **`etl/basket-shooting-history`** (7 commits,
unmerged, not pushed). The database changes are **already applied**.

Read this with `docs/basket_shooting_history_ingestion.md`, which is the
reference for the layer itself. This file is the "what happened and what's
next" part.

---

## 1. State of the world

| | |
|---|---|
| branch | `etl/basket-shooting-history`, 7 commits, **unmerged**, **not pushed** |
| data | 1160 player-team-seasons, 2022-2026, in `basketball_test.basket_player_season` |
| identity | **533/533 linked** for 2025-2026 (100%), 10 of them manual |
| relations | `basket_player_season`, `basket_shooting_import_runs`, `basket_shooting_source_pages`, plus `schedule_team_dict.team_name_basket` |
| tests | 64 assertions passing |
| cache | 210 MB in `exports/cache/basket_shooting_history` — any re-run is cache-only |
| consumers | **none yet** — nothing in the app reads this layer |

Migrations applied today, in order:

```
2026-09-22_basket_shooting_history.sql              (base, applied yesterday)
2026-09-22_basket_shooting_history_english.sql      (English names/positions)
2026-09-23_basket_shooting_history_consolidate.sql  (5 relations -> 3)
2026-09-23_basket_player_season_team_dob.sql        (team + date of birth)
2026-09-23_schedule_team_dict_basket_name.sql       (team name mapping)
2026-09-23_basket_player_season_identity.sql        (identity columns)
2026-09-23_basket_player_season_identity_fix.sql    (drop a wrong unique index)
2026-09-23_basket_identity_keys.sql                 (fix the access-path indexes)
2026-09-23_basket_identity_fk_drop.sql              (drop a wrong FK)
2026-09-23_basket_identity_key_comments.sql         (column comments)
```

---

## 2. THE ONE THING TO DO FIRST

**Game 210 needs a scoped ETL re-run.** This is the only outstanding data
correctness issue and it is small and contained.

```powershell
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' -e "Sys.setenv(APP_ENV='test'); source('etl/etl_full.R'); etl_full(game_ids = 210)"
```

**Why.** A `player_id_game_overrides` row was added today mapping
`game 210, team 6, 1143 -> 1982`. Corrections are applied *while ETL processes
a game* (`canonicalize_actions_player_ids()`, `canonicalize_roster_player_ids()`,
`canonicalize_starter_player_ids()` in `etl_full.R` around lines 376-419), so an
override added afterwards does not retro-fix base data. Right now:

| table | rows still under the wrong id |
|---|---|
| `lineups_lookup` | **23** |
| `player_traditional_by_game` | 1 |
| `full_rosters` | 1 |

Bnei Herzliya's D.J. Burns played game 210 (about 21 minutes, 18 points) and
those minutes are currently credited to Rishon Lezion's DJ Burns. For contrast,
the 21 existing DeAndre Williams overrides have **zero** such residue — they
were processed while their overrides existed. That is the shape to match.

`etl_full(game_ids = 210)` re-fetches the game, applies the override, rewrites
the facts and refreshes the MVs in its Phase 4. **No separate MV refresh is
needed**, for this or for anything else done today: no MV or SQL function
references identity resolution. The only consumer is
`app/R/server_tab5_traditional.R:869`, which reads `resolved_player_identity_v`
live (it caches per season in-process via `cached_ref_query`, so a *running*
app holds a stale copy until restart or the next ETL bumps `data_version`).

**Verify afterwards** — all three should return 0:

```sql
SELECT count(*) FROM basketball_test.lineups_lookup
 WHERE game_id = 210 AND player_id = 1143;
SELECT count(*) FROM basketball_test.player_traditional_by_game
 WHERE game_id = 210 AND player_id = 1143;
SELECT count(*) FROM basketball_test.full_rosters
 WHERE game_id = 210 AND player_id = 1143;
```

Also re-check that Tab 5 no longer drops `team 6 / source 1143` as ambiguous:

```sql
SELECT team_id, source_player_id, count(DISTINCT identity_id) AS identities
FROM basketball_test.resolved_player_identity_v
WHERE game_year = 2026
GROUP BY team_id, source_player_id HAVING count(DISTINCT identity_id) > 1;
```
Before the re-run this returns three rows: `5/2060` and `12/1183` (both
pre-existing, from known id reuse) and `6/1143` (ours).

---

## 3. Bugs found in the play-by-play data

### 3.1 A wrong identity merge — FIXED

`player_id_aliases` recorded Bnei Herzliya's **1982** as a "cross-team season
re-mint" of Rishon Lezion's canonical **1143**. They are two different players
named D.J. Burns.

**Proof:** in **game 147** they appear on opposite sides of the same game
(1143 for Rishon, 1982 for Herzliya). Four independent attributes agree:

| | 1143 | 1982 |
|---|---|---|
| roster | Rishon, 29 games, jersey 55 | Herzliya, 18 games, jersey 30 |
| birth date (Basket) | 2001-05-16 | 2000-10-13 |
| height (Basket) | 2.02 m | 2.06 m |
| shooting (Basket) | 24 g, 15/55 from three | 13 g, 1/1 from three |

The merge was prompted by a single stray roster row — game 210, where the
provider stamped Herzliya's Burns with Rishon's id while keeping his own
number 30. That is a per-game reuse, not a season re-mint.

**Fixed in `etl/player_id_aliases.R`:** moved to
`retired_default_player_id_aliases()`, and game 210 added to
`default_player_id_game_overrides()`. Dictionary re-synced (identities
736 -> 737). Base data still needs §2.

**Side effect, accepted:** Herzliya's 1982 now has no auto-assigned identity,
because Herzliya holds two source ids displaying the same name and the
dictionary declines to self-map either. He resolves through the view's
documented fallback (`resolution_scope = 'source'`, `identity_id` = raw id).
If you want him to have a real `player_identities` row, that is a manual
dictionary entry — deliberately not done unilaterally.

### 3.2 `resolved_player_identity_v` fans season mappings across every game

Its `mapped_source_players` CTE joins each active season mapping to **every
game of that team-season**. One stray roster row therefore makes a player
appear in all 30 of a team's games. This is what let Rishon's Burns into
Herzliya's candidate pool and manufactured three "ambiguities" that were not
real (Burns, Josh Hagins, DeAndre Williams).

**Worked around, not fixed.** The matcher now joins `full_rosters` on
`game_id` as well as team, restricting the pool to real appearances. The view
itself is untouched. Anything else that counts *from* this view will
over-count.

### 3.3 That same CTE ignores `provider`

```sql
FROM basketball_test.player_identity_map m
JOIN roster_games rg USING (game_year, team_id)
WHERE m.active AND m.game_id IS NULL      -- no provider predicate
```

The view's two final joins filter `provider = 'segev'`; this CTE does not.
Harmless today because only segev rows exist, but any active non-segev row
would surface as a phantom source player carrying a foreign id in a segev id
column. **This is why the Basket link lives on `basket_player_season` and not
in `player_identity_map`.** A one-line fix if you want it:
`AND m.provider = 'segev'`.

### 3.4 Possible duplicate: Holon 2138 / 2150

Two source ids, identical Hebrew name (יונתן חדד), on the same seven bench
sheets. Only **2150** ever played — one game, 25 seconds, 0 points. **2138**
has no rows in `player_traditional_by_game` at all and wears jersey 98 (having
worn 7 once). Looks like a provider duplicate. Not investigated, no impact
since 2138 has no minutes. Basket's single Hadad registration was matched to
2150 on exactly that evidence.

---

## 4. Mistakes I made, so they are not repeated

1. **Claimed `canonical_player_id` identifies a person.** It is recycled
   between seasons — 18 of 712 belong to two people (1119 is AMIT GERSHON in
   2025 and MICHAEL FOSTER JR. in 2026). I published a "multi-season career"
   table built on it that was summing different players.
2. **Then claimed `identity_id` does.** It is split by re-minting:
   `identity_key` embeds the source id, so `segev:1025:TAMIR BLATT` and
   `segev:1091:TAMIR BLATT` are one man. Only 109 of 732 span a season.
   **Both keys are season-scoped — always pair with `game_year`.** Now
   recorded as column comments on the table itself.
3. **Claimed cross-season linking was unsolvable.** Wrong — see §5.
4. **Invented a unique index** on `(game_year, canonical_player_id)`. Basket
   registers per player-**team**-season, so a transfer holds two registrations
   with per-stint totals. Dropped.
5. **Added a foreign key** from `identity_id` to `player_identities`. The view
   COALESCEs that column to the raw source id when unmapped, so the FK was
   wrong in principle. Dropped.
6. **Used the last name token as the surname**, so `CLARENCE DANIELS II`
   had surname `II`. Fixed with suffix stripping.
7. **Recommended re-running the scraper to repopulate a reshaped table** when
   the verified rows were already in the database. `INSERT ... SELECT` in the
   migration was the right answer.

---

## 5. Next step with the most value: cross-season person linking

**The data is already on disk.** Each cached profile page carries a career
table listing every season with *that season's* PlayerId. Willy Workman's 2026
page (21828) lists 17555 for 2024-25, 13256 for 2023-24, back to 9978 in
2014-15.

Measured across the 1160 cached profiles:

| | |
|---|---|
| profiles carrying other-season ids | 755 |
| distinct id pairs | 2,466 |
| pairs where both ids are loaded registrations | 758 |
| **pairs linking a 2025 to a 2026 registration** | **99** |

against **109** play-by-play identities that span a season boundary.

Because every registration is already linked to a season-scoped identity,
those pairs **transitively merge the re-minted identity records** — they solve
§4.2, which is a play-by-play problem the play-by-play data cannot solve
alone. No fetching required; the parser simply does not read that table yet.

Sketch: extract `player.asp?PlayerId=N` from each `<tr>` carrying a
`20\d\d-\d\d` season string in the profile HTML, store as a
registration-to-registration edge table, take connected components as the
person, and cross-check with `date_of_birth`, which should be constant inside
a component. A scratch extractor that produced the numbers above is in this
session's scratchpad (`career_coverage.py`); it is throwaway, not committed.

**Second, independent check available:** `date_of_birth` is on all 1160 Basket
rows and on **no** play-by-play relation. Grouped by identity today, zero rows
disagree on it.

---

## 6. Other open items

- **Branch is unmerged and unpushed**, and nothing consumes the layer. Decide
  whether it merges to `main` as-is.
- **2027 scrape** is due when the regular season starts. Trigger: `game_type = 5`
  rows appearing for `game_year` 2027. As of today the 2027 schedule holds 13
  games, all `game_type` 10 and 34 (cup/preseason); 2026's regular season was
  194 `game_type = 5` games from 2025-10-12. Command and caveats in the memory
  note `project-basket-2027-rerun-due`.
- **One 33.6-minute stall** during the scrape, on a single profile fetch, never
  reproduced. `options(timeout=)` does not interrupt a response that keeps
  trickling; a curl low-speed-limit is the fix if it recurs.
- **19 of 1160 rows have no nationality** (1.6%). No gate depends on it.
- **Two more fallback identities in 2027** (`resolution_scope = 'source'`),
  predating everything here.

---

## 7. Commits

```
b4ece23  etl: basket.co.il shooting-history backfill (2022-2026)
6353399  etl: capture team and date of birth from the Basket profile pages
5e218cb  etl: map basket.co.il team names in schedule_team_dict
c838924  etl: link Basket registrations to play-by-play identities
69851e7  etl: split the two D.J. Burnses, and stop matching on phantom roster rows
f8bfb5b  etl: resolve the last 11 Basket registrations; 533/533 linked
90ccfa8  docs: record the identity layer and correct the key guidance
```

Files: `scripts/backfill_basket_shooting_history.R`,
`scripts/apply_basket_shooting_history_migration.R`,
`etl/basket_identity_match.R`, `etl/player_id_aliases.R`,
`etl/tests/test_basket_shooting_history_parser.R`,
`docs/basket_shooting_history_ingestion.md`, and 10 migrations.

## 8. Commands

```powershell
# Scrape (cache-only unless a season is new)
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' scripts/backfill_basket_shooting_history.R `
    --seasons=2022:2026 --with-profiles --write `
    --profile-pause-seconds=3 --initial-backoff=15

# Identity matching (dry run; add --write)
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' etl/basket_identity_match.R

# Tests
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' -e "testthat::test_file('etl/tests/test_basket_shooting_history_parser.R')"

# Migrations (dry run by default; --commit to apply, --file to pick one)
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' scripts/apply_basket_shooting_history_migration.R --file=<path>
```
