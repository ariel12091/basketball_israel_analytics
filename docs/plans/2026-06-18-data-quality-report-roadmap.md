# Data Quality Check Map

Date: 2026-06-18

## Current Quality Status

The current report passes most implemented checks, but it does not yet test the full basketball data pipeline.

Verified current finding:

- Game `62426`, team `4`, contains three provider placeholder players:
  - `33406 NEW NEW`
  - `33407 NEW NEW`
  - `33408 NEW NEW`
- They appear in `onoff_default_mv` and `player_advanced_stats_mv` as rostered
  nonparticipants with no ON possessions and `131` OFF possessions.
- They have no valid ON-floor participation.

These are non-participating provider placeholders. They remain visible in the
source roster and app aggregates, but they do not affect scoring, possessions,
lineups, or minutes. Report them as a low-priority warning rather than an ETL
failure.

Reviewed non-actionable cases:

- Games `184` and `380` ended early. Their short reconstructed timelines are
  expected and should be reported as reviewed exceptions.
- Game `380` has an official schedule result of `20-1`, while played-event
  scoring reconstructs to `74-66`. Treat this as an official-result exception,
  not an ETL score defect.
- Game `157` has four five-row substitution batches at period boundaries with
  no opponent starter context. These rows have no offense/defense ownership and
  do not contribute to statistical aggregates.

Known source defects already documented:

- Game `178`: invalid late Q2 reset block at `10:00`.
- Game `381`: conflicting duplicate action IDs around the Q2/Q3 boundary.

## Required Checks

### Priority 0: App-facing invalid player rows

These checks address the active `NEW NEW` issue.

#### DQ-P01: Placeholder player names

Flag roster or aggregate names such as:

- blank names;
- `NEW NEW`;
- `UNKNOWN UNKNOWN`;
- other explicitly identified provider placeholders.

Severity: warning when the placeholder never participates.

#### DQ-P02: Nonparticipant player in app aggregates

Review a player aggregate when all are true:

- the player is not roster-active for any included game;
- the player has no ON possessions or minutes;
- the aggregate still assigns OFF possessions or team results.

Important: a rostered DNP with zero minutes, zero ON possessions, and positive
OFF possessions is valid under the current all-roster-player aggregate model.
It must not be counted as invalid.

#### DQ-P03: Roster player never validly on court

Flag players carried into lineup or player aggregates without any valid `is_on_verdict = 1` occurrence.

This should distinguish:

- valid bench players who did not play and may appear with zero ON-floor time;
- active players with actual floor time;
- provider placeholders or malformed roster entries.

Expected result: only players with valid floor time appear in player-stat aggregates.

### Priority 0: Raw play-by-play integrity

These checks must run before source corrections and cleaning.

#### DQ-R01: Duplicate action IDs

Per game, flag duplicate action IDs and retain all conflicting source rows.

Report:

- action ID;
- source-row positions;
- quarter and clock;
- action type;
- team and player;
- `user_time`;
- whether rows are identical or semantically conflicting.

Unknown conflicts are errors. Known, narrowly corrected conflicts remain visible as corrected warnings.

#### DQ-R02: Invalid quarter clock sequence

Flag:

- clock increases within a live quarter;
- a quarter reset after the quarter reached `00:00`;
- end-of-quarter at an impossible clock;
- quarter actions appearing out of chronological order.

This directly catches the game `178` defect.

#### DQ-R03: Quarter boundary integrity

For each played quarter:

- exactly one valid start marker;
- exactly one valid end marker;
- no Q2 end marker reused as a Q3 start;
- overtime markers match the observed overtime periods.

This directly catches the game `381` defect.

#### DQ-R04: Parent-action references

Every nonzero parent action must reference an action in the same game.

Expected result: zero missing parents.

#### DQ-R05: Action player and team validity

Flag basketball actions where:

- player ID is not present in the game roster;
- team ID is not one of the two game teams;
- player belongs to the wrong team;
- a player-required action uses a placeholder player ID.

Team rebounds and control events must be handled separately from player-required actions.

### Priority 0: Substitution and lineup integrity

#### DQ-L01: Valid substitution transition

For every substitution:

- outgoing player must currently be on court;
- incoming player must currently be off court;
- incoming and outgoing players must belong to the same team;
- incoming and outgoing IDs must differ.

Expected result: zero invalid transitions after known source corrections.

#### DQ-L02: Five-player live lineup

At every live action:

- each team has exactly five players on court;
- the five player IDs are distinct;
- no placeholder player is on court.

Quarter initialization rows may use separate, explicit rules.

Report both:

- invalid lineup states;
- total lineup states;
- invalid percentage;
- number of affected games.

#### DQ-L03: Lineup hash consistency

For each lineup hash:

- it always resolves to the same sorted five-player set;
- it belongs to one team and season context;
- `n_on = 5`;
- no active corrected alias ID remains.

Expected result: zero conflicting hashes.

#### DQ-L04: Possession-to-stint match

Every possession row must match exactly one valid stint.

Flag:

- zero matching stints;
- more than one matching stint;
- duplicate stint keys;
- segments with reversed or overlapping boundaries.

This is the downstream failure that exposed game `178`.

The report must include two separate measurements:

1. ETL-time source match coverage, before cold-storage purge:
   - total possession/action rows entering the stint join;
   - rows with zero valid lineup/stint matches;
   - rows with more than one valid match;
   - percentages and affected game IDs.
2. Persisted app-row coverage:
   - total rows in `df_pts_poss_lineups_longer_mv`;
   - rows whose `(game_id, team_id, lineup_hash)` does not resolve to exactly five ON players in `lineups_lookup`;
   - unmatched percentage across the full dataset;
   - unmatched rows out of rows belonging to affected games;
   - per-game unmatched percentages and affected game IDs.

The persisted check alone is insufficient because rows with a null lineup hash are filtered out while building `df_pts_poss_lineups_longer_mv`.

#### DQ-L05: Team-minute coverage

Per game and team:

- regulation should reconstruct approximately 40 minutes;
- overtime should add five minutes per overtime;
- the two teams should cover the same game duration.

Minute tolerance should account only for event-timeline resolution, not broad missing periods.

Report:

- invalid team-game rows out of total team-game rows;
- invalid percentage;
- minimum and maximum reconstructed minutes;
- number of affected games.

### Priority 1: Score and possession integrity

#### DQ-S01: Reconstructed final score

Reconstruct points from made shots and free throws, then compare with schedule final scores.

Expected result: exact equality for both teams.

Implemented in the report using schedule scores, reconstructed traditional points,
lineup offense points, and opponent defense points.

#### DQ-S02: Running score consistency

Where source score strings exist:

- every scoring action advances the correct team by the correct amount;
- score never decreases;
- non-scoring actions do not change the score.

#### DQ-S03: Team points reconciliation

For each game and team, reconcile:

- reconstructed action points;
- offense lineup points;
- opponent defense points;
- game-log score;
- schedule score.

Expected result: exact equality.

#### DQ-S04: Possession reconciliation

For each game:

- team A offensive possessions equal team B defensive possessions;
- team B offensive possessions equal team A defensive possessions;
- lineup totals equal team totals;
- player ON/OFF population totals reconcile with team possessions under the defined model.

Team offense-versus-opponent-defense and team defense-versus-opponent-offense
parity are implemented. Player ON/OFF closure remains outstanding.

#### DQ-S05: Possession ending validity

Flag possessions with:

- no terminal event;
- multiple terminal events;
- terminal event assigned to the wrong team;
- impossible quarter carryover.

### Priority 1: Roster and player identity quality

#### DQ-I01: One player ID, multiple real names

One team-season-player ID should not resolve to multiple real people unless a reviewed game-scoped correction exists.

#### DQ-I02: One real player, multiple IDs

The same normalized real name with multiple provider IDs should be reviewed.

Do not auto-merge placeholder or common names.

#### DQ-I03: Aggregate name is roster-valid

Every app aggregate full name must exist as one atomic roster name for that player context.

This catches hybrid names assembled from separate first-name and last-name aggregation.

#### DQ-I04: Corrected player residue

After correction and rebuild:

- no alias ID remains in base player rows;
- no stale lineup or sub-lineup hash contains the alias;
- aggregate rows use the intended player identity.

### Priority 1: Aggregate and app-output integrity

#### DQ-A01: Unique aggregate keys

Expected zero duplicates at each documented aggregate grain.

Examples:

- `onoff_default_mv`: year, team, player;
- `player_advanced_stats_mv`: year, team, player;
- game-grain player tables: game, team, player;
- lineup tables: game/team/lineup/type as defined by their keys.

#### DQ-A02: Required metric inputs

Flag rows where a displayed metric has:

- missing denominator;
- negative attempts, possessions, points, or minutes;
- makes greater than attempts;
- percentages outside valid bounds;
- impossible starter counts outside `0-5`.

Each check must return `invalid_rows / total_rows` and an invalid percentage. A single pass/fail flag is not sufficient.

#### DQ-A03: Fast-path versus dynamic-path parity

For default filters, compare materialized-view results with dynamic SQL results.

Required parity:

- same entity keys;
- same possessions and points;
- same displayed metrics within only documented rounding tolerance.

#### DQ-A04: Cross-tab parity

For the same game/team filters:

- Team Ratings must reconcile with Game Logs;
- lineup totals must reconcile with team totals;
- traditional player totals must reconcile with source action totals;
- Four Factors components must reconcile across player, lineup, and team levels.

Player-minute conservation is implemented at team-game grain. Both sides of
the comparison use the same reconstructed lineup segments: the sum of player
ON-floor minutes must equal five times reconstructed team lineup minutes.
Official-duration completeness is checked separately.

### Priority 2: Completeness and freshness

#### DQ-C01: Processed-game completeness

A processed game must have all required non-purged base and downstream rows.

#### DQ-C02: Loaded-but-unpublished games

A successfully loaded and refreshed game must have an `etl_processed_games` record.

#### DQ-C03: Schedule coverage

Every completed schedule game expected in scope must be either:

- processed successfully;
- explicitly excluded with a documented reason;
- currently failed with a visible quality finding.

#### DQ-C04: Refresh freshness

App-facing objects must include every successfully processed game from the latest clean ETL run.

## Recommended Implementation Order

1. Automate raw duplicate-action and clock/quarter checks.
2. Add ETL publication-funnel completeness.
3. Add source-schema and event-distribution drift checks.
4. Add substitution transition validation.
5. Add possession-ending integrity.
6. Add fast-path/full-rebuild and hot/cold parity.
7. Add failure trends across ETL runs.

## KPI Backlog

### Publication funnel

For each ETL run, report counts and percentages for:

- completed scheduled games in scope;
- successfully fetched games;
- base-loaded games;
- fully refreshed and published games;
- failed, skipped, or explicitly excluded games, grouped by reason.

### Raw PBP integrity

Report:

- duplicate or missing action IDs;
- non-monotonic clocks and invalid quarter transitions;
- unknown event types;
- missing required team, player, quarter, or clock fields;
- known corrected conflicts separately from unknown blocking conflicts.

### Source-schema and distribution drift

Compare each run with a recent baseline:

- new, removed, or renamed JSON fields;
- null-rate changes for required fields;
- event-type frequency changes;
- abrupt changes in rows per game, substitutions, possessions, or scoring
  events.

### Substitution coherence

At live substitution transitions, verify:

- outgoing player is on court;
- incoming player is off court;
- both belong to the correct team;
- the resulting lineup contains five distinct players.

Administrative period-boundary declarations, such as game `157`, must be
classified separately.

### Possession integrity

Report:

- possessions with zero or multiple terminal events;
- unexplained offense-team switches;
- quarter-crossing possessions;
- unusually long or event-empty possessions;
- offense/defense and lineup/team possession parity.

### Published-data parity

Compare:

- incremental refresh with a full rebuild sample;
- materialized-view output with dynamic SQL under default filters;
- core points, possessions, minutes, and entity keys across app surfaces.

### Storage consistency

Report:

- hot-database versus cold-storage row counts by game/table;
- missing cold snapshots for published games;
- cold snapshots modified by failed game iterations;
- duplicate or missing game partitions.

### Failure trends

Persist per-run history for:

- new versus known findings;
- critical pass rate;
- issue counts by check;
- repeatedly failing games;
- age of unresolved findings.

## Definition of a Clean Game

A game is clean only when:

- raw actions have no unknown structural conflict;
- score reconstructs exactly;
- all player/team references are valid;
- every substitution is valid;
- both teams maintain five-player lineups;
- every possession maps to exactly one stint;
- points, possessions, and minutes reconcile across downstream tables;
- no placeholder participates in a live lineup or statistical event;
- non-participating source placeholders remain visible as reviewed warnings.
