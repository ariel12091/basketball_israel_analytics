# Shared Definitions and Rules

**This document is the contract.** Every surface of the product -- SQL
functions and materialized views, the Shiny app, the Plumber API and React
frontend, the R and Python ETL, both leagues, every tab -- must use the
definitions below and follow the rules below.

- Where code disagrees with this document, it is a bug in the code, unless the
  difference is listed in section 5 (the deviation register).
- A definition is changed **here first**, then in its single owner, then the
  pins in section 7 are run. Never the other way round.
- Written 2026-10-12 from an audit of drift between the schema and the code.
  Section 1 is what that audit found; the rules are derived from it, not from
  taste.

Related: `docs/database_context.md` describes how the database is built and
queried. This document says what the numbers and controls **mean**. If the two
disagree, this one wins and the other is stale.

---

## 1. What went wrong, and why

Ten differences were found and fixed in one session, and an eleventh the day's audit turned up afterwards (Tab 3 pace on nominal game length), and a twelfth after that: Tab 3's rank arrows meant "since each team's own last game" in the default state and "since the last league game day" under any filter or on the defence side. Pinning the player fast paths the same evening found four more (D18, D19, D21, D22). None was a typo. Each was
a second copy of something that already existed.

| # | What differed | Where the two copies lived | Root cause |
|---|---|---|---|
| 1 | "Last N games": whole-season ranking vs ranking after the other filters | Stored functions vs inline R in Tabs 3, 4, 11 | A |
| 2 | Game-type labels | Typed into R constants; nothing in the schema | B |
| 3 | Team names (five teams spelled differently in 2027) | `full_rosters`, `schedule_team_dict`, `final_schedule_mv`, a static R list | B |
| 4 | "2PT Jumper" category | Derived in code from an unchecked assumption about the feed | E |
| 5 | Tab 3 Traditional: minutes, clutch margin, games played | Inline SQL in R next to stored functions that defined each differently | A |
| 6 | TS%: `FGA + 0.44 x FTA` vs real free-throw trips | Traditional views vs Four Factors views | A |
| 7 | Free-throw trips counted twice (TS% 1.8 points low) | Events grouped on the physical segment, which changes at every substitution | D |
| 8 | Clutch minutes 611 vs 704; "5 starters" meaning two things | `fetch_lineups_four_factors` vs `fetch_lineups_all` | C |
| 9 | Clutch segment time counted only with an offensive action | Both lineup readers; the pattern had already been fixed elsewhere | C |
| 10 | Lineup minutes 0.1 apart between two tabs | `ROUND(SUM(per-game minutes))` vs `ROUND(SUM(seconds) / 60)` | C |

Root causes:

- **A. One concept, two implementations.** A formula or filter was written
  again -- in R beside a stored function, or in a second SQL file -- instead of
  being called. The copies agreed on the day they were written.
- **B. A catalogue that lived in code.** Labels and names had no row in the
  schema, so each consumer typed its own.
- **C. Twins with nothing holding them together.** Two functions that must
  return the same thing (Summary and Four Factors readers; fast path and
  filtered path; Israeli and EuroLeague) were edited one at a time. One fix
  was applied to a starters branch and not to the branch beside it.
- **D. Events and time treated as the same kind of thing.** Both were grouped
  on the same key, though one follows who earned the event and the other who
  was on the floor.
- **E. A check that could not fail.** A migration gate printed differences
  and exited 0; a category depended on a data assumption nobody tested.

---

## 2. Rules

Each rule names the cause it prevents and how it is checked.

**R1. One definition, one implementation.** *(A)*
A metric, filter or classification is computed in exactly one place per
league. Everything else calls it.
- The database owns computation: stored functions and materialized views.
- R, Plumber and React own presentation: formatting, colour, sorting, ranking
  for display, and recombining **additive counts** (R5).
- R must not contain SQL that recomputes something a stored function already
  computes. Reading a relation is fine; re-deriving a metric from the base
  action table inside an R string is not.
- When a tab needs a variant, add a parameter to the owner. Do not copy it.

**R2. Catalogues live in tables.** *(B)*
Anything a user reads as a name or label -- game types, team names, phases --
has a row in a table, and code reads a generated copy
(`app/R/catalogues_generated.R`, built by `scripts/sync_catalogues.R`).
Never type a label into code, and never edit the generated file.

**R3. One control, one meaning.** *(A, C)*
A control with the same label means the same thing on every tab, on the fast
path and the filtered path, and in both leagues. Section 4 is the list. If a
tab cannot honour a meaning, it does not show the control.

**R4. No twin without a pin.** *(C)*
Sometimes two implementations must exist (two readers over one fact, a
precomputed path and a dynamic one, two leagues). Then a test must call both
with the same arguments and require equal results on every shared column.
Adding a twin without adding its pin is not allowed. An edit to one twin is
not finished until the same edit is made, or shown unnecessary, in the other.

**R5. Counts cross boundaries; rates are derived last.** *(A, C)*
- Sum raw counts, then divide once. Never average percentages.
- Never rebuild a count from a rounded rate. A reader whose rows may be
  recombined returns the additive count behind every rate it returns.
- Round once, at the end, from the exact total. For time, that total is
  seconds: `ROUND(SUM(seconds) / 60, 1)`, never `ROUND(SUM(minutes), 1)`.

**R6. Events follow credit; time follows the floor.** *(D)*
- Events -- points, possession ends, shots, rebounds, turnovers, free-throw
  trips, and any filter applied to events -- group on the `credit_*` columns.
- Floor time, games played and Gameflow use the physical lineup columns.
- When one query needs both, aggregate each on its own key and stack the
  results (`UNION ALL`). Do not join events to time: a credited event may have
  no time row to match, and an inner join drops it silently.

**R7. EuroLeague reuses the Israeli implementation.** *(A, C)*
Tabs pair 8-1, 9-3, 10-2, 11-4. A EuroLeague surface calls the shared
helper with league arguments. The legitimate differences are enumerable and
belong in one descriptor: schema name, function names and signatures, season
convention, round vs game number, phase text vs integer game type, and
feature flags. A difference in what a **number means** is never a league
difference; it goes in section 5 with a date and an owner, or it is fixed.

**R8. Every layer that is not SQL is a consumer.**
- ETL (R and Python) writes facts. It does not compute a displayed metric that
  a view or function also computes.
- Plumber's rename functions are the only SQL-to-React column contract.
- Plumber and React do not carry their own copy of a filter or formula that
  `app/R/helpers.R` owns; they call it or they are listed in section 5.

**R9. A check must be able to fail.** *(E)*
- A gate exits non-zero on an unexpected difference. Known, explained
  differences are declared with their reason; they are not silently tolerated.
- A test is watched failing before the fix that makes it pass.
- A label that depends on an assumption about the feed gets a nightly test of
  that assumption.
- The live-database tests are not flaky. Do not run them while anything else
  is querying as `app_readonly` (see `CLAUDE.md`, port 6543).

**R10. The fast path is an optimisation, not a second definition.** *(C)*
A precomputed path must return exactly what the dynamic path returns for the
same arguments. An explicit full-season date window takes the fast path too.

---

## 3. Definitions

"Owner" is the single place the definition is implemented. "EuroLeague" says
whether that league is known to match: **same**, **differs** (see section 5),
or **not re-verified** against this document.

### Seasons and identity

| Term | Definition | Owner | EuroLeague |
|---|---|---|---|
| Season (Israeli) | `game_year` = the year the season ends (2026 = 2025-26) | `DEFAULT_GAME_YEAR`, `global.R` | differs by design: provider season = starting year. Derived by `euro_default_season()`; one bump rolls both leagues |
| Team display name | The row in `season_teams` for `(game_year, team_id)` | `season_teams` | not re-verified |
| Game type label | The row in `game_types` | `game_types` | differs by design: phase text, labelled by `euro_phase_label()` |
| Player identity | `(game_year, team_id, player_id)`. There is no cross-season person key: ids are recycled and re-minted | identity dictionary | not re-verified |

### Events

| Term | Definition | Owner |
|---|---|---|
| Possession | One row with `final_end_poss`, counted in the credited lineup | `df_pts_poss_lineups_longer_mv` |
| Points | Sum of `team_score` (the increment on a made shot or free throw), in the credited lineup. Final scores and W/L come from `final_schedule_mv` | same |
| Credited lineup | A free throw belongs to the lineup on its parent action's row: the foul, or the made shot of an and-one. Every other event belongs to its own lineup | `refresh_ft_credit_fields_for_games()` |
| Free-throw trip | One distinct personal-foul parent action. An and-one free throw hangs off the made shot and is not a trip. Counted once, in the lineup that drew the foul | same |
| 2PT / 3PT | `parameters_points`. `parameters_type` is a shot label, never the split | feed |

EuroLeague credits a trip to the lineup on the floor for its **last** free
throw. Same count, different lineup. Open decision: section 5, D1.

### Rates

All are computed from summed counts and shown as percentages to one decimal.

| Metric | Formula |
|---|---|
| Rating (`off_ppp`, `def_ppp`) | `100 x points / possessions`. The column is named `ppp` for history; it is per 100 possessions |
| Net rating | `off - def` |
| TS% | `points / (2 x (FGA + free-throw trips))`. **No view uses `0.44 x FTA`** |
| eFG% | `(FGM + 0.5 x 3PM) / FGA` |
| OREB% | `offensive rebounds / (missed or blocked shots + missed last personal-foul free throws)` |
| TOV% | `turnovers / possessions` |
| FTR | `all free throws / FGA`, technical and flagrant included on purpose |
| Usage | `(TS attempts + turnovers) x team possessions / ((team TS attempts + team turnovers) x possessions on floor)` |
| Pace | `40 x possessions / minutes`, minutes as defined below (measured floor time) |

A rate with a zero denominator is NULL, not 0. Unranked (under 100
possessions) is NULL, not 0 or 50.

### Time and games

| Term | Definition | Owner |
|---|---|---|
| Segment | `(game_id, team_id, lineup_hash, segment_id)`. `segment_id` repeats across games | base table |
| Minutes | `MAX(segment_seconds)` once per segment, summed over **every** segment, physical lineup. No offence filter, no grouping by quarter. Never the nominal game length (`40 + 5 per overtime`): a game with truncated play-by-play has partial possessions and must have partial time | team totals: `get_team_minutes_dynamic`; per-player reference: `player_traditional_stats_mv.segment_times` |
| Minutes inside a clutch window | Last minus first canonical event time among the segment's in-window rows, **including** substitution and timeout rows, over every segment | team totals: `get_team_minutes_dynamic`; lineups: `fetch_lineups_all` clutch path |
| Minutes rounding | Round the second total once | R5 |
| Games played, team | A game counts when at least one action survives the filters | `get_team_ratings_dynamic` |
| Games played, player | Physical floor presence in the game, never a credited join | `floor_lineup_hash` in the player traditional sources |

### Game Logs and the data version

| Term | Definition | Owner | Twins |
|---|---|---|---|
| Game Logs table (both leagues, both views) | Filters, per-team-game metrics, percentiles against the whole-season population, shot cells, header labels, rounding, scoreless-game flag | `svc_game_logs()` and its `gl_*` helpers, `app/R/svc_game_logs.R`; parameters parsed by `svc_game_logs_params()` over `svc_params.R` | Shiny Tab 4 (`server_tab4.R`) and Tab 11 (`server_tab11_euro_gamelogs.R`) reactives and the API handler `api/R/handlers_game_logs.R` are consumers of it, not copies. Deviations D24, D25 |
| League differences in Game Logs | Shot columns, header labels, date-range handling, round vs GN | descriptor `GL_LEAGUE` (and `GL_IL_VIEWS`, `GL_SHOT_COLS`, `GL_ROUND_1`) | -- |
| ETL data version (X-08) | The last successful ETL stamp, `app_meta.etl_full_last_success`, that every season-level cache is keyed on | `etl_last_success_db()`, `global.R`, normalised by `shared_data_version()` | Shiny cache keys and the API's `GET /v1/meta/data-version` read the same function |

---

## 4. Controls

| Control | Meaning everywhere |
|---|---|
| Last N games | Rank each team's **whole season** by `game_date DESC, game_id DESC`; keep the last N; then apply the other filters. It is not "the last N games that pass the filter" |
| Date range | Inclusive schedule dates. The full-season window equals no window (R10) |
| GN / Round | Inclusive range. Israeli: schedule game number. EuroLeague: round |
| Home / Away, Outcome, Opponent | From the team-perspective schedule row; join on `game_id` **and** `team_id` |
| Opponent rank | Top or bottom N by season-wide rank; other filters do not recompute the ranks |
| Clutch margin | Absolute margin **before** the event (subtract the row's own points) |
| Clutch status | Leading, trailing or tied at the pre-event score, in the row team's perspective |
| Clutch time | Seconds remaining in regulation. Overtime always qualifies |
| Overtime and margin | Overtime bypasses margin and status unless `p_ot_margin_filter` is true |
| Starters (offence) | Number of the row team's own starters on the floor |
| Starters (defence) | Number of the opponent's starters on the floor |
| Starters, how applied | To every row of the team's perspective, never per side. Events test the credited starters, time tests the physical starters |
| Min possessions (lineups) | Threshold on `off_poss + def_poss`, applied **after** ranking the full population |
| Min possessions (on/off) | Threshold on ON possessions |
| Players on / any of / off | `A AND (at least or exactly k of B, C, D) AND NOT E`. "Any of" adds a clause; it never relaxes one |
| Rank-change arrows (team tabs) | Today's rank against the rank on the **last league game day before the latest one**, under the same filters: the stored reader called again with that end date. Never a per-team cut (each team rolled back to before its own latest game), which compares a table that never existed |
| Blank | `""` for every single-select |

Percentile ranks are computed on the full population before any local
narrowing, and no ranked table mixes leagues.

---

## 5. Deviation register

Known places where a surface does not yet follow this document. Each is a
debt, not a second definition. Add an entry the day a deviation is created or
found; remove it the day it is closed.

| # | Deviation | Rule | Status |
|---|---|---|---|
| D1 | EuroLeague credits a free-throw trip to the lineup of its last free throw, not the lineup that drew the foul | R7 | Open decision (user) |
| D2 | Player readers do not return TS attempts, so the multi-team TOTAL row and the usage fallback imply them from a rounded TS%; a scoreless row loses its missed trips. App side ready (`ts_attempts()`) | R5 | Shelved 2026-10-12, both leagues |
| D3 | Plumber has its own `apply_lineup_local_filters()` with no count or "exactly" support | R1, R8 | Open |
| D4 | React `autoMinPoss()` only lowers the bar; Shiny raises to undo its own lowering | R3 | Open |
| D5 | Tab 7 Compare "any of" always means at least one; Tabs 2 and 10 take a count | R3 | Open (UI question) |
| D6 | `four_factors_compute` returns base rates as fractions; other readers return percentages | R3 | Open; confirm scale before combining |
| D7 | `onoff_compute` takes the season as text and recomputes opponent ranks under filters | R3 | Open, documented exception |
| D8 | `player_four_factors_by_game` holds partial rows for players not on a game's roster | data | Open |
| D9 | A side with possessions and no points returns a NULL rating, not 0 (`four_factors_dashboard_compute`), which also hides the row's minutes | R3 | Found 2026-10-12, open |
| D10 | `refresh_sub_lineups_stats()` writes all-zero rows for lineups with no base rows; the Four Factors reader never returns them | R4, R10 | 4 rows deleted 2026-10-12; a full refresh recreates them |
| D11 | `global.R`: the Gameflow readers (physical segments and margins for one game) and `fetch_scoreless_games()` read the base table. These read detail rows; they do not re-derive a metric | R1 | Audited 2026-10-12: allowed |
| D16 | The Traditional reader rounds team minutes to one decimal; the owner returns three. Same time, coarser figure (up to 0.05 min per team) | R5 | Explained 2026-10-12; harmless while nothing recombines them |
| D17 | `server_tab6_team_stats.R` is unwired and still holds inline SQL over the base table | R1 | Dead code; delete or ignore |
| D18 | **On/Off Summary, default state: the "On Net RTG" colour never reaches the top shades.** `onoff_default_mv` ranks `pr_on_net` over every row, NULLs included, so it tops out near 0.84 (2026: 0.842, 2027: 0.804); `onoff_compute` ranks the non-NULL rows and reaches 0.98. Same player, up to 0.18 of a percentile apart between the unfiltered and any filtered view. Every other percentile column agrees within 0.03 | R10 | **Fixed 2026-10-13.** `onoff_mv.sql` and its twin `refresh_onoff_default_for_games` rank only the rows that have a rating and leave the percentile NULL where there is none; all seasons refreshed. The pin now holds `pr_on_net` to the same 0.03 as the others and requires the best value to be 1. `onoff_compute` was left as it is: it drops players with no ON or OFF bucket before ranking, so it has no NULL to rank |
| D19 | On/Off Summary view and reader differ at the edge of the sample: the view keeps players with no ON or no OFF bucket (54 in 2026, at most 3 ON possessions), which the reader drops; a side with possessions and no points is NULL in the view and 0 in the reader (one player, 2026). Same quirk as D9 | R10 | Open; never visible above the possession minimum |
| D20 | On/off minutes: the view rounds the season's second total to one decimal; `onoff_compute` sums per-game values already cut to three decimals. Up to 0.06 min apart | R5 | Open, display only |
| D21 | **Compare shows different player minutes from Player Stats.** Tab 7 calls `get_player_traditional_dynamic`, which measures the span between a segment's first and last event even with no clutch window; Tab 5's view and `get_player_traditional_from_games` use segment time. 190 of 312 players differ for 2026, by up to 6 minutes. Every other column is equal. `fetch_player_traditional_filtered()` in `helpers.R` already routes to the right readers and takes the same 18 parameters | R1, R3 | **Fixed 2026-10-13.** Compare calls `fetch_player_traditional_filtered()`; its default-player lookup calls `get_player_traditional_from_games`. No app file names `get_player_traditional_dynamic` any more (`test-shared-definitions.R` forbids it); the function still exists in the database and on the allowlist, unused by Shiny, and can be retired once nothing else calls it |
| D22 | **A season's date window is defined three ways.** Shiny: 1 September to 1 July (`season_date_bounds_for_year()`). React and Plumber: 1 October to 1 July, and they always send it, so games played in September are left out -- 1 of 221 in 2025-26, **23 of 30 so far in 2026-27**. The two lineup readers' full-season sentinel is also 1 October, so Shiny's window takes their slower path (same numbers) | R3 | Found 2026-10-12, open |
| D23 | **A player who joins a team mid-season has two different OFF sides.** The season tables (`onoff_default_mv`, `player_advanced_stats_mv`) are recomputed whole from the current roster, so his OFF side covers every game the team has played. `player_four_factors_by_game`, which both filtered readers sum, is refreshed one game at a time, so it holds OFF rows for him only from the game he first appeared in. Seven players on 2026-10-13 (teams 6, 9, 14, 17, first seen in games 413-415): e.g. player 1384, OFF possessions 171 in the default view and 67 under any filter. Hidden on 2026-10-12 because the credit migration had just refreshed every game. `test-player-reader-paths-db.R` fails on it for 2026-27 | R10 | Found by the pin 2026-10-13, open. Repair: refresh that table for the season's games; cause: the nightly run should do the same for a team whose roster gained a player |
| D24 | **EuroLeague Game Logs "Last N" counts rows, not games.** It keeps each team's last N starter-split rows of `team_four_factors_by_game` instead of its last N games (`svc_game_logs.R` `gl_filter_games()`, which both leagues share; formerly `gl_filter_euro_rows()`), so "last 5 + home" showed 9 rows for 2025-26 against about 50 expected. The Israeli schedule has one row per team-game, so the same code is correct there | R7, R3 | Found 2026-10-19 by the Game Logs goldens, open. Fix = slice games, then re-capture the el_* goldens deliberately |
| D25 | EuroLeague Game Logs: a date range with one blank end shows no games; Israeli Game Logs treats the blank end as open (`svc_game_logs.R` `gl_filter_games()`, `GL_LEAGUE$*$date_na`). Both filters were merged into one function; this difference is kept behind the descriptor so the extraction does not change what Shiny shows | R7, R3 | Found 2026-10-19 during the Game Logs extraction; kept as is, open |
| D15 | A team's five-man lineup minutes do not add up to its floor time: per team, the lineup readers' sum is up to about 10 minutes away from `get_team_minutes_dynamic` over a season (2026), more than row rounding explains. Likely lineups the readers do not list as five known players | data | Seen 2026-10-12, not traced |
| D12 | The React Shot Profile port differs from Shiny | R3 | Intentional, recorded |
| D13 | EuroLeague clutch, last-N, minutes and starters rules have not been re-verified against sections 3 and 4 | R7 | Not re-verified |
| D14 | `docs/database_context.md` still describes Traditional TS% as `0.44 x FTA` | -- | Stale text |

Not deviations, though they look like one:

- **Gameflow and on/off disagree on about 3.5% of points.** Gameflow shows who
  was on the floor when each point was scored (R6: physical). On/off credits
  the lineup that drew the foul (R6: credit).
- **The two leagues label seasons differently.** A convention, held in one
  derived value.

---

## 6. Before you change anything

1. Find the term in section 3 or 4. If it is not there, add it before writing
   code, with one owner.
2. Find the owner. Change it there. If you are about to write the formula in a
   second file, stop: add a parameter to the owner (R1).
3. List the twins: the other reader, the other path, the other league, the
   Plumber route. Make the same change in each, or show why it is not needed
   (R4, R7).
4. Decide for each quantity whether it is an event or time, and group it
   accordingly (R6).
5. If rows of the result can be recombined, return the counts (R5).
6. Run the pins in section 7. Add one if the change created a twin.
7. If something must stay different, write it in section 5 the same day.

---

## 7. What holds this together

| Pin | Guards |
|---|---|
| `test-ft-credit-db.R` (nightly, live database) | Credit columns filled; each trip counted once; ON + OFF adds to the team; lineup day totals add to the team; no Israeli reader uses 0.44; Traditional TS% equals Four Factors TS%; **the two lineup readers agree** in five filter combinations |
| `test-ts-trips.R` | TS% from trips; attempts summed, not implied, when returned; no `0.44` in the app's R |
| `test-game-types-db.R`, `test-season-teams-db.R` (nightly) | The generated catalogue copy equals the tables |
| `test-data-shape-db.R` (nightly) | The feed assumption behind "2PT Jumper" |
| `test-league-shared-helpers.R`, `test-lineup-player-chips.R` | Shared helpers behave the same for both leagues; every reader of the player filter lists its inputs |
| `test-team-minutes-db.R` (nightly) | Team floor time: the owner equals the Traditional reader (to its one decimal), the clutch dashboard reader (exactly) and measured segment time; its per-game path equals its action-row path |
| `test-team-reader-paths-db.R` (nightly) | R10: the Traditional reader's per-game path equals its action-row path on every column, both sides, six filter sets; the two team views behind Tab 3's default state equal their dynamic readers |
| `test-player-reader-paths-db.R` (nightly) | R10 for the player tabs: `player_traditional_stats_mv` equals `get_player_traditional_from_games` on all 24 shared columns; `player_advanced_stats_mv` equals `four_factors_compute` and `four_factors_dashboard_compute` on all 37; `onoff_default_mv` equals `onoff_compute` on every value column for players with a real sample and within 0.03 on every percentile, with D19-D20 declared |
| `test-shared-definitions.R` | R1 as a ratchet: the R files that query the base action table directly are frozen at the D11/D17 list, which may only shrink; Tab 3 takes pace minutes and rank arrows from stored readers only; both `CLAUDE.md` files point here |
| `scripts/verify_ft_credit.R` | Migration gates that fail on an unexpected change |
| `test-svc-game-logs.R`, `test-golden-game-logs.R`, `api/tests/testthat/test-api-game-logs.R` | Game Logs: the service function `svc_game_logs()` (owner of the table: filters, per-game metrics, percentiles, header labels, rounding) returns what Shiny's Tab 4 / Tab 11 rendered when the goldens were recorded; the API handler returns what the service returns; Shiny and the API call the same function |
| `test-api-parity-live-db.R` (nightly, live database) | Game Logs, both leagues: the API answer equals the Shiny golden against the live database |
| `test-api-coverage-gate.R` | Every inventory id in a finished section (`API_DONE_PREFIXES` in `api/coverage.R`, now `GL`) is named by a `covers()` call in some test; the gate has its own can-fail test |
| `.github/workflows/catalogue-sync.yml` | Fails when the schedule holds a game type with no label |

Pins that do not exist yet. Each is a twin without a test (R4):

- Israeli and EuroLeague readers return the same number for the same metric on
  a constructed game.
- `apply_local_lineup_filters()` (R) and `apply_lineup_local_filters()`
  (Plumber) agree.
- "Last N games" means the whole-season ranking on every surface that offers
  the control.
- No R file contains SQL that aggregates the base action table (would turn D11
  from a list into a failing test).
