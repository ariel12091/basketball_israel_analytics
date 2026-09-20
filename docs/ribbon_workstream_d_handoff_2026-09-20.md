# Ribbon Workstream D continuation handoff, 2026-09-20

This continues `docs/ribbon_workstreams_a_c_handoff_2026-09-20.md`, whose
"Next task" was to review and run `scripts/audit_ribbon_excluded_segments.R`
read-only and compare against the old baseline.

Sections 1-7 are the read-only audit and changed no database rows. **Section
8 then applied a write**: four games were reprocessed to canonicalise player
ids. Read section 8 before treating any number in sections 2-6 as current.

## 1. The draft audit script was reviewed and corrected

Two issues found before running it:

- It joined `basketball_test.final_schedule_mv` directly to get `game_year`.
  That MV is built from `sched_long` — **two rows per `game_id`**, one per
  team — so the join fans every segment row out twofold. The existing
  `GROUP BY` happened to absorb it (`game_year` is identical on both rows, and
  `MIN`/`MAX`/`BOOL_OR` are duplicate-insensitive), but any `SUM` added later
  would have silently doubled. Replaced with a `SELECT DISTINCT game_id,
  game_year` CTE.
- It had no way to show it was measuring the same thing the ribbon measures.
  Added section 4, which runs `RIBBON_SQL_ISRAEL`'s `excluded_segments`
  predicate verbatim, per game, and reconciles. **7/7 games agree.**

Also added: the source-vs-derived player counts and an alias-resolution step
that answer the workstream's actual question (sections 3d-3g), and a variant
table that reconciles the old headline baseline (section 5). Section 3d
rewrites each unresolved hash's player set through `player_identity_map` and
looks for a canonical twin in `lineups_lookup_on`, so the script now detects
the alias class itself rather than leaving it to be inferred.

Run it with:

```powershell
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' scripts/audit_ribbon_excluded_segments.R
```

## 2. Result as first measured (pre-fix): 67 segments, 8 team-games, 4596s

Superseded by section 8 — now 36 segments, 6 team-games, 2159s.

Unchanged from 2026-09-19 in both segment count and per-game distribution, so
the anchor work neither fixed nor widened this class.

## 3. The old headline "19 team-games, 4675s" was a different measure

It never agreed with the plan's own per-game table, which sums to 8 team-games
and 4596s. The variant section reproduces both exactly — the difference is the
`has_gameplay` filter:

| Variant | Segments | Team-games | Seconds |
|---|---:|---:|---:|
| reader (gameplay, seconds > 0) | 67 | 8 | 4596 |
| gameplay, any seconds | 68 | 8 | 4596 |
| any segment, seconds > 0 | 104 | 19 | 4675 |
| any segment, any seconds | 129 | 20 | 4675 |

The extra segments are substitution-only; the ribbon draws no lane time for
them, so they were never holes. This was a reporting defect, **not** a
regression. The plan records 67 / 8 / 4596 as the measured pre-fix figure
(36 / 6 / 2159 after section 8) and keeps the old headline labelled as the
looser measure.

## 4. Cause classes — the answer to "4 or 6 players?"

`lineups_lookup_on` is derived: ETL Phase 3 copies the `is_on_verdict = 1`
rows out of `lineups_lookup`. Reading both counts separates a feed defect from
a join that cannot resolve.

| Cause | Segments | Seconds | Where |
|---|---:|---:|---|
| Alias id-space mismatch — nothing lost | 35 | 2588 | 363, 366, 368, 370, all team 9 |
| Source over-attribution — 6 or 7 on the floor | 26 | 1484 | 178 (25 segs), 62479 (1) |
| Source under-attribution — 2 or 3 on the floor | 6 | 524 | 62452, team 11 |

**No hash resolves to four players.** Outside the alias class the derived count
equals the source count exactly.

The prior expectation in the previous handoff — residue concentrated in 178,
62452 and 62479 — holds for 2008 of the 4596 seconds. It missed the largest
class, which turns out not to be a data defect at all.

## 5. The largest class is an alias id-space mismatch, not lost data

Player id **2052 is not canonical.** `player_identity_map` row 493 (provider
`segev`, season scope, game_year 2026, team 9, active, created 2026-06-18)
maps source 2052 to **canonical 1110**, NOAM AVIVI — "cross-team season
re-mint: id 2052 (Bnei Herzliya) is the same person as canonical 1110 (Galil
Elion)".

A `lineup_hash` is computed from the player-id list, so the same five people
hash differently in the two id spaces, and the alias reached the two sides at
different times:

| Relation | id for games 363-370 | Consequence |
|---|---|---|
| `lineups_lookup` | raw `2052` | hash `b3f70bca…` |
| `full_rosters` | raw `2052` | — |
| `df_pts_poss_lineups_longer_mv` | raw `2052`, 35 segments | what the ribbon reads |
| `lineups_lookup_on` | canonical `1110` | hash `ead530af…` |
| `sub_lineups` | canonical `1110` | 250 rows, 10 hashes |

The ribbon joins the MV's raw hash to `lineups_lookup_on`'s canonical hash and
misses. Established, read-only:

- **All 10** missing hashes have an exact twin in `lineups_lookup_on` once 2052
  is rewritten to 1110. My first pass compared raw player sets only and
  reported "no set appears under another hash" — that was wrong; canonicalised
  sets match all ten.
- Those 10 canonical hashes hold **250 rows in `sub_lineups`**. I earlier wrote
  that these lineups reach `sub_lineups` as 0 rows and are therefore missing
  from the universe Tab 2 reads. **Retracted** — that queried the raw hashes.
  Tab 2 has them, under the canonical hash.
- The MV carries the raw hash for the 4 affected games and the **canonical**
  hash for games 365 and 372 (28 segments, which resolve fine). `full_rosters`
  splits the same way: 2052 through game 370, 1110 for 365 and 372.
- Games 172, 187, 225 also carry raw 2052 and are **not** affected, because
  their `lineups_lookup_on` rows are raw too. The scoped retroactive backfill
  (`etl/backfill_player_id_aliases.R`, via `affected_player_alias_game_ids()`)
  canonicalised the derived side for some games and left the source side alone.
  The 35 excluded segments are exactly where the two sides disagree.
- Those 10 remain the only five-player `is_on_verdict = 1` hashes anywhere in
  the database with no `lineups_lookup_on` row, so the mismatch is bounded
  today to one player in one season.

## 6. The decision that was taken — SUPERSEDED BY SECTION 8

The scope question below was resolved as "just the four games"; section 8
records what that ran and what it left behind. The warning against an
in-place UPDATE still stands.

**Do not insert the raw hashes into `lineups_lookup_on`.** An earlier version
of this handoff proposed exactly that (10 x 5 = 50 rows, "additive and safe").
It would register the same five people twice under two hashes and corrupt
every lineup count Tab 2 derives from `sub_lineups`. That recommendation is
withdrawn.

The real fix makes the two id spaces agree, and is a write either way:

- extend the alias rewrite to `lineups_lookup` (and the MVs built from it) for
  games 363/366/368/370; or
- roll `lineups_lookup_on` and `sub_lineups` back to raw ids for those games.

This belongs with the player-identity work, not with the ribbon. Nothing in D
now blocks the ribbon beyond the warning it already emits correctly.

**Latent scope worth a standing check.** 2026 alone has 392 active identity
maps over 350 source ids (2025: 296, 2027: 209). Any future scoped alias
backfill that rewrites the derived side without the source side reproduces
this. Suggested data-quality check: count MV hashes that fail to resolve in
`lineups_lookup_on` but do resolve after applying the active alias map.

D2 (games 178, 62479) and D3 (game 62452) are genuine feed defects with no
propagation component. They belong to workstream E's tracking.

After that, the plan's next investigation is **Workstream F** (mid-quarter
attribution holes). The `subs` hot-table promotion still needs its separate
acceptance check after the next nightly ETL: confirm Phase 7 leaves
`basketball_test.subs` populated.

## 7. Incidental notes

- One parameterised query against the pooler (port 6543) failed once with
  `Query requires 0 params; 1 supplied`, then succeeded on every subsequent
  run of the identical statement. Treat a single occurrence of that error as
  pooler flakiness, not a script bug; re-run before investigating.
- Heredocs in this harness collapse **doubled** backslashes to single, even
  when quoted (`<<'EOF'`), and whether the target is a file or a command's
  stdin; a single backslash survives intact. So a Python patch script written
  through a heredoc cannot carry a literal backslash-n — it arrives as a real
  newline, and every match against R source then silently fails. What worked:
  write the replacement text as its own data file with a `cat` heredoc using
  single backslashes, then splice it in by line number. Build any backslash a
  script must construct with `chr(92)`.


## 8. APPLIED: four games canonicalised (2026-09-20)

Superseding section 6's "next task" — the fix was run.

**Canonicalisation is part of ETL, but only at ingest.** Phase 2 loads the
active aliases (`etl/etl_full.R:320`) and applies them per game via
`canonicalize_actions_player_ids()` / `canonicalize_roster_player_ids()` /
`canonicalize_starter_player_ids()`. ETL never revisits a processed game, so a
game ingested before its alias existed stays raw forever. Games 363/366/368/370
were ingested 2026-06-02 to 06-17; the 2052 alias was created **2026-06-18**.
Game 372 (06-22) came out canonical. That is the entire difference.

The repo already owned the retroactive fix, so nothing new was written:

```r
Sys.setenv(APP_ENV = "test")
source("etl/backfill_player_id_aliases.R")
backfill_player_id_aliases(dry_run = FALSE, seed_defaults = FALSE,
                           game_ids = c(363L, 366L, 368L, 370L))
```

`seed_defaults = FALSE` keeps the run from activating aliases that were not
already active. The script re-runs `etl_full()` for exactly those games, which
is a transactional per-game snapshot replace (DELETE + reinsert of `pws`,
`stints`, `lineups_lookup`, `possessions`, `subs`, `actions_clean`,
`full_rosters`), so no raw/canonical duplication is possible. 455.6s,
`pipeline_ok = TRUE`, 4/4 validated and published, no phase failures.

### Verified before and after

| Invariant | Result |
|---|---|
| Final scores (96-70, 91-84, 88-102, 65-76) | unchanged |
| Team minutes from segments | unchanged (39.40 / 39.92 / 39.78 / 39.43) |
| Distinct players per team-game | unchanged (23 / 19 / 22 / 21) |
| Alias ids in `lineups_lookup` + `full_rosters` | 105 rows -> **0** |
| Ribbon excluded segments, these 4 games | 35 / 2588s -> **0** |
| Ribbon excluded segments, database-wide | 67 / 4596s -> **36 / 2159s** |

Snapshots: `snapshot_before.txt` / `snapshot_after.txt` were taken with the
same script; only the blocks above differ.

### Two deliberate changes the drift check caught

- **Segments fell by 3 per team-game** (~114 MV rows per game). They are
  exactly the zero-second segments on the Q1/Q2, Q2/Q3, Q3/Q4 boundaries:
  reprocessed games now have **0**, every non-reprocessed game in the season
  still has **6** (3 boundaries x 2 teams). Zero seconds, hence minutes
  unchanged. This is current pipeline semantics — the 2026-09-12
  monotonic-clock migration and the period-anchor work — landing on games last
  processed in June. Expected, and an improvement.
- **`etl_processed_games.processed_at` is not bumped by a reprocess** (363
  still reads 2026-06-02). It marks first ingest, not last processing. That
  resolves the earlier puzzle about game 365 looking June-processed while
  holding canonical ids.

### The scoped fix orphaned two neighbours — 151s

`cleanup_player_alias_lineup_derivatives()` deletes raw-hash rows from
`lineups_lookup_on` / `sub_lineups` at **season** scope
`(team_id, game_year, lineup_hash)`, not per game. Games **172** and **187**
share two of those raw hashes and were not reprocessed, so their MV now points
at `_on` rows that no longer exist: 4 new excluded segments, 151 seconds, same
alias class. The audit's section 3d classifier catches them.

That is also the mechanism behind the original defect: an earlier partial run
cleaned the derivatives for 363/366/368/370 without reprocessing them, leaving
the source raw and the derived side canonical.

**So a partial scope is not a stable state.** Closing 2052 needs its remaining
games: **166, 172, 182, 187, 197, 203, 210, 218, 225**. Two other active
aliases are untouched and behave the same way: **1982** (team 6, 19 games) and
**2046** (team 13, 11 games). 37 distinct games in all.

### Still open in D

| Game | Segments | Seconds | Class |
|---|---:|---:|---|
| 178 | 25 | 1460 | feed defect (D2) |
| 62452 | 6 | 524 | feed defect (D3) |
| 172 | 3 | 90 | alias residue |
| 187 | 1 | 61 | alias residue |
| 62479 | 1 | 24 | feed defect (D2) |

One caveat to carry: the run reported `data_quality_status: FAIL`, and the
findings page failed to write ("cannot open the connection"). The DQ suite had
pre-existing failures before this run, so FAIL is not by itself evidence of a
new defect — but it was **not** compared against a pre-run baseline, so treat
it as unverified rather than clean.


## 9. Nine more games reprocessed (2026-09-20) — alias 2052 closed, ribbon net WORSE

`backfill_player_id_aliases(dry_run = FALSE, seed_defaults = FALSE,
game_ids = c(166L, 172L, 182L, 187L, 197L, 203L, 210L, 218L, 225L))`.
`pipeline_ok = TRUE`, 9/9 validated and published, no phase failures.

### What went right

| Measure | Before | After |
|---|---|---|
| Games carrying alias **2052** | 9 | **0** — gone from `full_rosters`, `lineups_lookup`, `lineups_lookup_on` |
| Games carrying 1982 / 2046 | 19 / 11 | 18 / 10 (games 210 and 172 folded in) |
| Ribbon holes in games 172 / 187 | 3 segs / 90s, 1 seg / 61s | **0** |
| Scores, all 9 games | — | unchanged |
| Team minutes | — | unchanged except game 218: 39.85 -> **40.00** |
| False straddles in all 13 reprocessed games | 0 | **0** — no regression |

The false-straddle check closes the verification gap noted in section 8: the
reprocess changed period-boundary handling, and it did **not** introduce a
false straddle in any of the 13 games.

### What went wrong — the ribbon moved backwards

The side effect predicted before the run happened, and it cost more than the
run fixed. Game **210** carries alias 1982 (team 6). Reprocessing it deleted
raw-hash `lineups_lookup_on` rows at season scope; game **206** (team 6, not
reprocessed) still points at two of them, on the sets
`{1981,1982,2041,2066,2069}` and `{1073,1982,2041,2066,2069}`.

| | Segments | Seconds |
|---|---:|---:|
| Ribbon holes before this run | 36 | 2159 |
| Fixed (172, 187) | −4 | −151 |
| **Newly orphaned (206)** | **+4** | **+344** |
| Ribbon holes after this run | 36 | **2352** |

**Net +193 seconds worse for the ribbon**, even though the identity fix itself
succeeded. A partial scope does not converge; it relocates the defect.

### Stale rows in `player_four_factors_by_game` — pre-existing, not app-facing

That MV retains rows for alias ids after the source has been canonicalised:
1982 in 31 games (1020 rows), 2046 in 15 (502), 2052 in 12 (235) — including
games 365, 372 and 140, which predate this session, so the behaviour is
pre-existing. `refresh_player_four_factors_by_game_for_games()` evidently
upserts rather than replacing a game's rows, so a player id that vanishes from
a game keeps its old rows.

The app-facing MVs are clean — `player_traditional_stats_mv`,
`player_advanced_stats_mv` and `onoff_default_mv` all hold **only** 1110, three
team-season rows, no 2052. So there is no visible double count in Tab 5 or Tab
1's fast path. **Not checked:** Tab 1's *filtered* path calls
`four_factors_compute`, which reads `player_four_factors_by_game` directly.
Whether an orphan id surfaces or is dropped by the roster join is unverified.

### To reach a stable state

Reprocess the remaining alias games — **1982**: 134, 139, 147, 156, 161, 168,
175, 184, 189, 196, 206, 217, 227, 294, 304, 309, 356, 361 (18) and **2046**:
167, 179, 191, 193, 204, 215, 221, 364, 367, 371 (10). 28 games. That empties
the alias class and should leave only the three feed defects: **32 segments /
2008 seconds** in games 178, 62452 and 62479.

Note game 184 is in the 1982 list and is a Workstream E game (zero actions in
Q3/Q4). Reprocessing will not repair it and should not be expected to.


## 10. Ten more games (2026-09-20) — alias 2046 closed, zero ribbon cost

`backfill_player_id_aliases(dry_run = FALSE, seed_defaults = FALSE,
game_ids = c(167L, 179L, 191L, 193L, 204L, 215L, 221L, 364L, 367L, 371L))`.
`pipeline_ok = TRUE`, 10/10 validated and published, no phase failures.

| Measure | Before | After |
|---|---|---|
| Games carrying alias **2046** | 10 | **0** |
| Ribbon holes, database-wide | 36 segs / 2352s | **36 segs / 2352s — unchanged** |
| New orphans | — | **none** |
| Scores, all 10 games | — | unchanged |
| Team minutes | — | unchanged |
| Segments per team-game | — | −3 (the usual boundary artefacts; game 204 −1) |

Game 221 team 13 lost one distinct player (22 -> 21): the alias and its
canonical id were **both** on that roster, so the merge collapsed one person
who had been listed twice. Correct, and worth knowing that this case exists.

### Why this run cost nothing and the previous one cost 193s

Before running, the exposure was measured rather than assumed: *which
non-reprocessed games share a hash that this run's season-scope cleanup would
delete?* For 2046 the answer was **zero**, and the run indeed orphaned nothing.
That check is the gate every future alias reprocess should pass.

```sql
WITH doomed AS (
  SELECT DISTINCT game_year, team_id, lineup_hash
  FROM basketball_test.lineups_lookup
  WHERE game_id IN (<games to reprocess>)
    AND player_id = <alias_id> AND is_on_verdict = 1)
-- then count gameplay segments on those hashes in games NOT being reprocessed
```

## 11. Alias 1982 is BLOCKED — do not reprocess it piecemeal

18 games remain. **Three of them cannot be re-fetched**: 294, 304 and 309 are
State Cup games (`game_type = 35`, `gn` NULL) with an empty `pbp_link`, so
`etl_full` skips them ("no usable schedule/PBP source row").

Running only the 15 fetchable ones was measured with the gate above and would:

| | Segments | Seconds |
|---|---:|---:|
| Fix game 206 | −4 | −344 |
| **Orphan cup game 304** | +7 | +463 |
| **Orphan cup game 309** | +8 | +741 |
| Net | +11 | **+860 worse** |

So the 15-game run is strictly harmful and was **not** executed. Options:

1. Load the three cup games first, then run all 18. CLAUDE.md points at
   `etl/run_state_cup_final_etl.ps1` and `docs/state_cup_final_309.json.md` as
   how game 309 was originally loaded; that route is **unverified** here.
2. Leave 1982 alone. Game 206 keeps 4 segs / 344s and nothing worsens.

## 12. Season scope — 2025 and 2026-27 are clean

| Season | Games | Active aliases (`player_id_aliases`) | Games carrying a raw alias id | Ribbon holes |
|---|---:|---:|---:|---|
| 2025 | 225 | 1 | **0** | 7 segs / 548s (feed defects 62452, 62479) |
| 2026 | 221 | 5 | 18 (alias 1982 only) | 29 segs / 1804s |
| 2027 | 13 | 0 | **0** | **0** |

2027 (2026-09-08 to 09-19) is the live season and is clean *because* ETL
canonicalises at ingest and every one of its games was ingested after the
existing aliases were created — the same reason game 372 was clean while 370
was not.

**The trap reopens the moment an alias is created for a 2027 player.** Every
2027 game already ingested would keep the raw id while the derived side is
stripped at season scope. Two mitigations:

1. Run `backfill_player_id_aliases()` immediately after seeding a new alias,
   while the affected-game count is still small — and run the exposure gate
   above first.
2. Add a data-quality check: MV hashes that fail to resolve in
   `lineups_lookup_on` but **do** resolve after applying the active alias map.
   That fires the night a desync appears rather than months later.

**Correction to section 5's figures:** it cited "392 active maps for 2026, 350
source ids" from `player_identity_map`. The table the ETL actually reads for
corrections is `player_id_aliases`, which holds **1 active row for 2025 and 5
for 2026, none for 2027**. Those are different tables; the smaller numbers are
the ones that drive canonicalisation.


## 13. The standing check — AL_reader_lineup_hash_unresolved_in_on_table

Added so this defect class reports itself the night it appears, instead of
being found three months later by hand.

**Why a new check was needed.** `Q_persisted_rows_without_lineup_match` joins
the MV to `lineups_lookup` — the **source**. The app's lineup readers join
`lineups_lookup_on` — the **derived** table. An alias desync lives exactly in
that gap: the source holds a valid five, the derived table holds nothing, and Q
reports nothing while the ribbon silently drops the segment. That is how 2588
seconds went missing across four games without a single failing check.

`C_active_correction_residue_game_scoped_tables` and
`F_lineup_derivative_active_alias_residue` are the neighbours, and neither
covers this either: C flags a raw source, F flags alias ids left in the derived
tables. AL flags the **disagreement between them**, which is the thing the app
actually trips over.

**What it reports.** One row per game/team, with `alias_resolvable`:

| Game | Team | alias_resolvable | Source players | Segments | Seconds |
|---|---:|---|---:|---:|---:|
| 206 | 6 | **TRUE** | 5 | 4 | 344 |
| 178 | 4 | FALSE | 6 | 16 | 782 |
| 178 | 11 | FALSE | 7 | 9 | 678 |
| 62452 | 11 | FALSE | 3 | 6 | 524 |
| 62479 | 7 | FALSE | 6 | 1 | 24 |

`TRUE` means a reprocess recovers the time; `FALSE` means the feed never
recorded five and it belongs to Q and R. Severity is `error`, but
`problem_count_col` is `actionable_seconds`, which counts **only** the
alias-resolvable rows — so the check returns to `pass` once the id spaces
agree, instead of sitting red forever on feed defects. The catalog entry's
`tier_fn` mirrors that split: critical when resolvable, low when not. Runs in
~1.2s.

**Files:** `etl/run_data_quality_report.R` (the check),
`etl/dq_findings_html.R` (`DQ_FINDING_CATALOG` entry, required — the existing
`etl/tests/test_dq_findings_html.R` fails without one),
`app/tests/testthat/test-dq-alias-desync-check.R` (13 assertions).

**Two bugs found while building it, both worth remembering:**

- The first version reported `alias_resolvable = TRUE` for **every** row,
  including the 6- and 7-player feed defects — their unchanged set matched
  itself in `lineups_lookup_on`, since that table faithfully copies a
  six-player hash. The fix requires all three of: the alias actually rewrote
  the set, the rewritten set has cardinality 5, and the twin found is itself a
  five. The total (2352s) was right the whole time; only the classification
  column was wrong, which is precisely the kind of error a plausible-looking
  total hides. The tests pin all three conditions.
- `impact_seconds` is populated by an `impact = function(r)` on the catalog
  entry, **not** by a detail column named `seconds`. An attempt to wire it by
  renaming the SQL column was wrong and was reverted.

## Working-tree and database caution

**The database was written to.** Sections 8-10 reprocessed **23 games** through
`etl_full()`: 363, 366, 368, 370, then 166, 172, 182, 187, 197, 203, 210, 218,
225, then 167, 179, 191, 193, 204, 215, 221, 364, 367, 371. That is not
reversible from the working tree; undoing it means reprocessing from the
provider feed, which may no longer return what it returned on 2026-09-20. The
before/after snapshots in those sections are the record of what changed.

The runs also advanced `app_meta.etl_full_last_success` (which versions Shiny's
season caches, so the first app load after this is a cold one) and re-exported
the cold-storage Parquet files in `exports/cold/` (gitignored).
`etl/known_invalid_pbp_actions.csv` appeared untracked during this session; it
was not created deliberately here — check it before staging.

**Code changed in this session** (all previously clean, all LF-only, verified
by diffstat):

| File | Change |
|---|---|
| `scripts/audit_ribbon_excluded_segments.R` | new, untracked — the Workstream D audit |
| `etl/run_data_quality_report.R` | +133 lines, 0 deletions — the AL check |
| `etl/dq_findings_html.R` | +31 lines, 0 deletions — the AL catalog entry |
| `app/tests/testthat/test-dq-alias-desync-check.R` | new — 13 assertions |
| `docs/ribbon_workstream_d_handoff_2026-09-20.md` | new — this document |
| `docs/plans/2026-09-19-stint-ribbon-correctness-plan.md` | Workstream D section, status row, baselines row |

`etl/dq_findings_html.R` was briefly truncated by a bad splice during this
session (1072 -> 562 lines) and was rebuilt from `HEAD`; it now diffs as
insertions only. Worth re-checking the diffstat before committing.

Working tree, unchanged from the previous handoff: the repository was already
substantially dirty, and the plan document carried pre-existing user changes
before this session touched it. Review the scoped diff before committing; do
not stage the entire working tree.
