# EuroLeague data exploration handoff

Last updated: 2026-08-07  
Status: 84 games loaded; two EuroLeague tabs live in the Shiny app behind a
league switch; standalone load framework in place

> **Everything below the "Operational handoff snapshot" section is design
> history from the exploratory phase.** It remains accurate as a record of how
> the possession engine, reconciliation and QA were validated, but it describes
> the project as it stood at three controlled games. For current state, read the
> snapshot section only.

## Current status summary

- **Package-first rule:** use pinned `euroleague-api==0.1.1` for schedules,
  play-by-play, box scores, starters, and reconstructed lineups. Custom code is
  limited to persistence/reliability, possessions, reconciliation, QA, and
  schema mapping.
- **Possessions:** deterministic Python and independent R implementations agree
  exactly on all 56,463 events in the 100-game sample and both produce 14,684
  possessions. All 4,122 free throws resolve and all games pass hard structural
  QA.
- **Official validation:** all 16 PBP-versus-box-score metrics match across all
  200 team-games. Strict score progression passes 97/100 games; the remaining
  three are exact adjacent one-event-ahead provider snapshots and pass the
  bounded reconciliation rule, producing 100/100 reconciled games.
- **Lineups:** the package supplies the lineup engine. All 56,463 events have
  two unique five-player lineups; 37 package-invalid actor rows are retained as
  QA rather than corrected by a second lineup implementation.
- **Schema:** the isolated `euroleague` schema follows the Israeli core
  layers (`schedule`, `full_rosters`, `actions_clean`, `possessions`, `stints`,
  and `pws`) while separating immutable raw evidence and avoiding wide table
  duplication. Migrations 001, 002, 004, 005 and 006 are applied.
  **Migration 003 is SUPERSEDED by 004 and must never be applied.**
- **Loadability:** the database-free schema coverage audit passes 100/100 games
  with zero blocking identity, action-key, lineup, endpoint, or offense-team
  mapping issues. Thirty-five coach/bench pseudo-actor rows remain raw-only.
- **Transaction contract:** the driver-independent per-game writer has commit,
  rollback, preflight-validation, and database-validation tests. The 100-game
  load plan expands to 2,800 deterministic operations and 100 planned commits.
- **Batch execution:** bounded, restartable collectors and deterministic
  per-game stage checkpoints are implemented. A fresh offline stage of all 100
  cached games completed 100/100 in 56.544 seconds; two resumptions of the same
  validated checkpoints took 3.874 and 7.706 seconds. No batch was written to
  PostgreSQL.
- **Analytics:** the additive SQL compatibility layer is applied. It preserves
  raw numerators/denominators for player ON/OFF PPP, ratings, and four factors;
  the three controlled games contain 6,240 validated player/context fact rows.
- **Live data:** `E/2025/1-84` (rounds 1-9) is loaded under `load_run_id=4`,
  84 requested, 84 successful, zero failed, parser `0.2.0` throughout. Twenty
  teams at 8-9 games each. All verification passes: box-score metrics exact on
  84/84, score progression reconciled 84/84, lineup structure valid 84/84, team
  four factors matching the independently derived player fact, and MV/dynamic
  parity.
- **Current boundary:** no Israeli-schema table, production ETL flow, or Israeli
  application query was changed. `app_readonly` was granted SELECT on the
  EuroLeague read layer and EXECUTE on its four functions. Further live games
  are run through `scripts/load_games.py` and no longer need a bespoke session.

## Operational handoff snapshot

This section is the starting point for the next work session. The detailed
design history and evidence remain below it.

### Repository state versus live state

| Concern | Repository | Live `euroleague` schema |
|---|---|---|
| Live data | `scripts/load_games.py` loads any gamecode range | `E/2025/1-84`, `load_run_id=4` |
| Possession parser | `0.2.0` | `0.2.0` throughout |
| Stage checkpoint format | `2` | n/a |
| Core schema | Migration 001 | Applied |
| Additive per-game analytics | Migration 002 | Applied |
| App read layer | Migration 004 (**supersedes 003**) | Applied |
| Team ratings | Migration 005 | Applied |
| Team four factors + dynamic team path | Migration 006 | Applied |
| Four-factor refresh query plan | Migration 007 | Applied |
| App integration | Tabs 8 and 9 in `app/` | Reads via `app_readonly` |

Apply order is **001 → 002 → 004 → 005 → 006 → 007**. Migration 003 is superseded and
must never be applied.

The publication code calls `euroleague.refresh_app_materialized_views()` when a
load run finishes. This is a deliberate fail-closed dependency: a load must not
be marked complete while its app-facing snapshots are stale. Any migration that
adds a derived table must also register it in `assert_shadow_schema_compatible()`
(`postgres_backend.py`), or publication refuses to start.

### Storage

181 MB for 84 games (~2.2 MB/game), projecting to roughly 880 MB for a full
402-game season. The instance is shared with `basketball_test`; the operator has
confirmed 5 GB available, so storage is not currently a constraint. The largest
per-game relations are `player_four_factors_by_game`, `actions_raw` and
`action_lineups`.

### Latest completed work (2026-08-07)

Twelve commits on branch `shiny/euro-tab1`, from `6674564` to `0f0a6e0`.

**Read layer.** Migration 004 (app read layer: `player_game_context`,
`onoff_compute`, `four_factors_compute`, three MVs), 005 (team ratings:
`team_game_ratings_mv` raw counts, `team_ppp_ratings_mv` season aggregate), and
006 (team four factors at team grain plus `get_team_*_dynamic`). One definition
of team rating now serves both the ratings surface and the opponent-strength
filter; opponent ranks are season-wide.

**App.** Two tabs, both league-scoped: `euro` (On/Off Impact) and `euro_team`
(Team Ratings), each with Summary and Four Factors. Every league's tabs stay
statically defined and only one league's are visible, so the navbar does not
grow; the league lives in JS plus `localStorage` and is chosen from a Home
chooser or an `IL|EL` navbar switch. One shared competition+season selector
serves the whole EuroLeague section.

**Loading.** `scripts/load_games.py` plus `RUNBOOK.md` — collect, stage, publish
and verify in one command, runnable without an agent session, safe by default,
resumable, exit code 1 on any failed check.

**Schedule metadata.** `staging.build_staged_game` previously hardcoded
`round_number`, `phase` and `scheduled_at` to `None`, so every date, round and
phase predicate evaluated to NULL and filtered the game out. Any filtered query
returned zero rows while the unfiltered season path looked correct.
`schedule_collector.fetch_season_schedule_meta()` now supplies them at staging
time.

**Verified on the 84-game load.** Parser `0.1.0` to `0.2.0` is genuinely
output-identical apart from lineage: re-deriving gamecodes 1-3 reproduced 432
possessions and 6,240 analytics facts exactly.

### Known issues

- **70 of 84 games carry `game_qa.publication_status = 'review'`.** Every hard
  gate passes; the flag comes from `possession_review_status`, driven by an
  average of 0.21 same-team transitions per game plus three games whose score
  progression was not exact but did reconcile. QA status is **not** a
  publication filter — the season aggregates include all 84 games regardless —
  so this count should be surfaced in the UI.
- **The player four-factor refresh still re-derives the event fact.** Migration
  007 fixed its query plan (28-42s/game publication → ~5s), but the structural
  gap remains: the Israeli pipeline reads a persisted event × team-perspective
  fact (`df_pts_poss_lineups_longer_mv`) that already carries `type_lineup`,
  lineup, starter context and segment seconds, while this function rebuilds all
  of it from `actions_raw` on every refresh — regex clock parsing, a running-max
  window, a five-way event join, the perspective expansion and six chained
  window CTEs for segments. That is table remark 7 in `CLAUDE.md`, and it is
  still the right next decision before more metrics are added.
- **`pws` is write-only.** Nothing reads it; both analytics derivations join
  `action_lineups` and `possessions` directly. It survives only as an integrity
  gate — four NOT NULL lineup/stint foreign keys mean an unattributable
  possession fails the load. Move that assertion into `game_qa` before dropping
  the table.
- **EuroCup has never been collected.** No `U` data exists anywhere.
- The four-factor `est. ±X pts` annotation is suppressed for EuroLeague because
  the weights were fitted on Israeli data.

### Ordered upcoming plan

#### 1. Season-scoped lineup identity

`lineups.lineup_hash` hashes provider player ids, the least stable identifier in
the system: when a provider id is re-minted next season the same five players
hash differently and lineup history splits silently. Hash the internal
`player_id` instead. Separately, lineup identity is per game
(`UNIQUE (game_id, team_id, lineup_hash)`), so the same five across 30 games are
30 rows — never aggregate on `lineup_id`. A season-scoped lineup relation is the
prerequisite for any lineup-combos surface.

#### 2. Player identity layer, before a second season

`euroleague.players` is `UNIQUE (competition, provider_player_id)` with no season
scope and no identity dictionary. Provider ids are re-minted per season and
recycled to different people. Build the identity layer *with* the second season,
not after it, and add the two data-quality checks at the same time: one
normalized name with more than one provider id, and one provider id whose name
or team changes across seasons.

#### 3. Load the rest of the season

~318 games remain, at ~5s/game after migration 007 — under 30 minutes. The
publication path was validated on
2026-08-07: `scripts/probe_batched_publish.py` republished gamecodes 1-3
through the real backend against the live database and rolled back, and the
natural-key projection of every lineup, lineup member, action lineup, stint,
pws row and downstream `player_game_context` fact was identical to what the
previous one-row-at-a-time code had written. No id mapping is at risk, so this
can now be run at scale.

#### 4. EuroCup viability check

Collect a single `U` game and run schema coverage, reconciliation and possession
QA against it. Three things decide whether EuroCup is viable at all: box-score
`IsStarter` flags, the play-type vocabulary, and PBP completeness.

#### 5. Operations

Scheduled collection and publication, per-run logs, a last-success marker the UI
can display, and a game-keyed publication marker so "what is new since last
night" can be answered without scanning `game_qa`.

### Handoff completion criteria

The next milestone is complete when season-scoped lineup identity exists and is
hashed on internal player ids, the player identity layer is in place, and a
second season has been validated through it without silently merging or
splitting players.

## Executive summary

The `euroleague-api` Python package is a useful extraction layer for schedules,
play-by-play, box scores, shots, standings, and reconstructed lineups. It should
not be the application's live data layer or the only retained copy of the data.
The recommended architecture is:

1. use pinned Python code to fetch and checkpoint provider data;
2. preserve immutable raw responses or lossless per-game extracts;
3. normalize them into a separate PostgreSQL `euroleague` schema that follows
   the grains and audit principles of the Israeli schema;
4. derive event relationships and possessions deterministically, while using
   the package's reconstructed lineups with retained QA fields;
5. integrate with the app only after reconciliation gates pass.

The highest-risk missing source field is `parent_action_id`. Deterministic R and
typed Python parsers now reconstruct synthetic parents, FT trips, and possession
endpoints without using clock as an identifier. They were tested on 100 complete
games: all games passed hard structural QA, all 4,122 FTs were assigned, and
both implementations returned the same 14,684 possessions and exactly the same
decisions on all 56,463 events.

This is strong enough for a shadow schema. It is not yet evidence that every
derived possession is semantically correct, because EuroLeague does not supply
ground-truth parent or possession identifiers.

## Decisions reached

### Package-first implementation rule

Use the pinned `euroleague-api` package whenever it already provides the
required source capability. This includes schedules, play-by-play, box scores,
shots, standings, standard statistics, basic source cleanup, event ordering,
starters, and reconstructed lineups. Do not reproduce those basketball
transformations in project code.

Custom code is limited to missing or project-specific requirements:
restartable collection, caching, immutable persistence, throttling and retry
policy, provenance, deterministic relationship and possession logic,
reconciliation, QA, schema mapping, and application outputs. A collector may
call the official endpoint directly only when the package cannot expose the
raw response or provide the required operational guarantees. Such a wrapper
must not create competing basketball semantics.

### Store data or call the package live?

Persist the data in our own schema. Continue using the package for extraction.

Reasons:

- reproducibility when an endpoint or package behavior changes;
- stable keys for actions, parents, FT trips, lineups, and possessions;
- incremental loads and audit history;
- faster application queries without external API dependence;
- the ability to compare EuroLeague rules with Israeli rules without forcing
  both providers into one raw representation;
- retention of raw evidence for ambiguous sequences.

### Separate database or separate schema?

Use a separate schema in the existing PostgreSQL database by default. A
separate physical database adds operational overhead without solving a current
problem. Schema isolation is enough to prevent EuroLeague source differences
from contaminating canonical Israeli tables while still permitting controlled
cross-competition analysis later.

The isolated `euroleague` schema was created for the initial one-game trial on
2026-08-06 and now contains the approved three-game controlled batch. It
remains separate from `basketball` and `basketball_test`.

### Reuse the Israeli schema exactly?

Follow its principles and table grains, not every source-specific column.
EuroLeague needs its own raw and normalized layer because its event vocabulary,
identifiers, substitutions, penalty sequences, and relationship evidence are
different. Cross-league views should be built only above normalized canonical
tables.

### What cross-league compatibility means

Compatibility is architectural and semantic, not exact-output parity. Both
projects should retain the same recognizable progression:

1. immutable or recoverable source evidence;
2. normalized schedule, roster, action, and lineup relations;
3. deterministic possession and stint derivation;
4. additive per-game analytics facts;
5. league-aware query and application adapters.

The common analytics concepts are game/team/player identity, ON/OFF state,
offense/defense perspective, points, possessions, minutes, and the additive
numerators and denominators needed for ratings and four factors. Each league
may keep additional provider-specific evidence and metrics.

The implementations need not use the same language or produce identical row
eligibility and rounding. The Israeli pipeline can remain R/SQL while the
EuroLeague pipeline uses Python and `euroleague-api`. Cross-league validation
should verify common grains and metric meanings, then validate each league
against its own source totals and invariants; Israeli row-for-row output parity
is not a EuroLeague release requirement.

### Python concurrency policy

Independent games are valid Python concurrency units for both extraction and
pure transformation. Concurrency must be bounded and adaptive because broad
parallel package calls already triggered provider throttling during
exploration.

- Fetch one endpoint/game task at a time within each worker and checkpoint the
  completed artifact immediately.
- Use a small configurable worker limit, bounded retry/backoff, and a shared
  cooldown that pauses request starts on `429` or equivalent
  throttling responses.
- Transform games independently, then sort outputs by the deterministic game
  and event keys before validation or persistence.
- Preserve one database transaction per game. Concurrent staging is safe;
  publication concurrency must be limited by the connection pool and must not
  interleave two replacements of the same game.
- A rerun from the same cached artifacts must produce the same normalized rows
  regardless of worker completion order.

### Implemented concurrency and measured effectiveness

The Python implementation follows the policy above without changing the
Israeli R/SQL pipeline:

- PBP extraction calls the package's
  `PlayByPlay.get_game_play_by_play_data()` method and atomically checkpoints
  one game at a time.
- Full raw box-score collection uses a documented direct reliability wrapper
  because package version 0.1.1 does not expose the complete raw response.
- Both collectors support bounded workers, globally coordinated request
  spacing/cooldown, bounded retries, isolated per-game failures, deterministic
  manifests, and cache-aware restart.
- Pure per-game staging can run concurrently, but publication is deliberately
  sequential: one shared auditable load run per competition/season batch and
  one atomic database transaction per game.

Actual cached-data timings on this machine were:

| Workload | Workers | Result | Throughput |
|---|---:|---:|---:|
| Fresh 10-game staging | 1 | 5.470 s | 1.828 games/s |
| Fresh 10-game staging | 2 | 5.541 s | 1.805 games/s |
| Fresh 10-game staging | 4 | 6.115 s | 1.635 games/s |
| Fresh 100-game staging | 1 | 56.544 s, 100/100 succeeded | 1.769 games/s |
| Resume 100 validated checkpoints | 1 | 3.874-7.706 s, 100/100 cached | 12.977-25.812 games/s |

Threaded staging was slower for this CPU/DataFrame/JSON workload, so its
measured default is one worker while the option remains configurable. Bounded
concurrency is most useful for network-bound extraction; no fresh live fetch
was performed for this benchmark. Restart was approximately 14.6 times faster
at best and 7.3 times faster in the repeat run. Completion order did not affect
manifest order or content.

## Package assessment

Inspected and installed version: `euroleague-api==0.1.1`, in the isolated
project environment at `euroleague/.venv`.

The package is a relatively thin wrapper around EuroLeague endpoints. Its main
surfaces cover:

- schedules and gamecodes;
- play-by-play;
- game, team, and player statistics;
- box scores and starters;
- shots;
- standings;
- reconstructed play-by-play lineups.

The package adds `TRUE_NUMBEROFPLAY` because provider `NUMBEROFPLAY` is often
not in chronological order. That generated sequence is the appropriate source
ordering input, after namespacing it by competition, season, game, and period.

Operational findings:

- package installation is useful and simpler than reimplementing every
  endpoint;
- broad concurrent fetching triggered rate limiting during exploration;
- safe ingestion should be per-game, throttled, checkpointed, retryable, and
  restartable;
- package output should be treated as source data, not a stable storage API;
- the installed metadata declares GPLv3 with a commercial-license option, so
  licensing should be reviewed before production distribution or tight code
  integration.

The dependency is pinned in `euroleague/requirements.txt` and
`euroleague/pyproject.toml`.

## Israeli and EuroLeague source comparison

| Concern | Israeli pipeline | EuroLeague source/package | Decision |
|---|---|---|---|
| Event order | Canonical action IDs and row order | `NUMBEROFPLAY` can be unordered; package creates `TRUE_NUMBEROFPLAY` | Preserve both; use the generated source order within game and period. |
| Parent relationship | Raw `parent_action_id` is available | No parent field | Derive `synthetic_parent_order`, retain confidence, and later map it to normalized action IDs. |
| Substitution meaning | A substitution records player-in/player-out semantics | Provider emits separate `IN` and `OUT` rows | Treat them as the same basketball concept, but pair and validate the EuroLeague rows. |
| Starting lineups | Reconstructed and stored by the Israeli ETL | Package reads box-score `IsStarter` rows | Preserve starter provenance and validate exactly five per team. |
| Event lineups | Canonical lineup/stint tables | Package computes `Lineup_A`/`Lineup_B` | Use the package output as the baseline and retain its validation flags. |
| Possessions | `compute_possessions()` has provider-specific rules and parents | No canonical possession table | Use a separate deterministic EuroLeague state machine. |
| Clock | Context plus canonical elapsed-time corrections | Many unrelated actions can share one clock | Never use clock as an event or incident key. |

## Substitutions and package lineups

Both datasets contain in/out substitution semantics. The representation differs:
EuroLeague records separate `IN` and `OUT` play types, while the Israeli action
model exposes the paired players on a substitution action.

The package's lineup method does construct lineups, but they are derived rather
than supplied as authoritative event-level lineups. It:

1. gets each team's starters from box-score `IsStarter` flags;
2. walks play-by-play in generated source order;
3. pairs later opposite `IN`/`OUT` rows for the same team and clock;
4. replaces the outgoing player in the current five;
5. optionally produces `validate_on_court_player`.

The package itself documents delayed substitutions, assists credited after a
player has been substituted, missing matching substitutions, and players not
found in the current five. Therefore the lineups are already *constructed* and
will be used as the EuroLeague baseline. We will retain the package validation
evidence rather than create a second substitution engine without a measured
need.

Recommended lineup gates:

- five unique players per team at every live interval;
- starter count and identity reconcile to the box score;
- every `IN`/`OUT` pair is traceable and same-team;
- on-court player actions reconcile or have an explicit provider exception;
- no negative, overlapping, or unexplained stint duration;
- package version and `validate_on_court_player` are retained with each load.

### Package-lineup audit on 100 games

The package lineup method was applied offline to the existing 100-game PBP
sample using the already cached official box scores. No new API calls and no
independent substitution logic were used.

| Diagnostic | Result |
|---|---:|
| Games processed | 100 / 100 |
| Event rows enriched | 56,463 / 56,463 |
| Games with exactly five starters per team | 100 / 100 |
| Rows with two five-player lineups | 56,463 / 56,463 |
| Rows with duplicate players in either lineup | 0 |
| Package-invalid on-court actor rows | 37 (0.0655%) |
| Games containing an invalid-actor flag | 28 |

Of the 37 package-invalid actor rows, 29 are assists. This is consistent with
the package's documented source-timing cases, such as an assist being credited
after the passer has left the floor. The remaining eight are two turnovers,
two offensive rebounds, and one each of foul drawn, made two-pointer,
defensive rebound, and missed two-pointer. These rows remain visible in the QA
export; the package lineups are not silently altered.

## Why the missing parent ID matters

The absence of `parent_action_id` was rated high severity because relationships
drive more than presentation. They determine:

- which foul awarded a free throw;
- whether a made shot and FT are an and-one;
- whether multiple FTs are one or several trips;
- whether an offensive foul plus turnover is one endpoint or two;
- whether a rebound closes or continues a possession;
- how technical and unsportsmanlike penalties affect entitlement;
- which action receives `final_end_poss`.

It is manageable because the source has stable within-game sequence and enough
event evidence to reconstruct most relationships. The mitigation is not a
single guessed parent column: it is the combination of synthetic parent, FT
trip, endpoint, confidence, and QA status.

Every event receives a non-null parent. A root or singleton parents itself;
children point to a root in the same game and period. Synthetic IDs are derived
from source identity and ordering, never from game clock.

## Deterministic possession implementations

The typed Python implementation is the EuroLeague canonical candidate:

- `euroleague/src/euroleague_possessions/models.py` defines immutable events,
  mutable per-event decisions, grouping statuses, endpoint reasons, and explicit
  pending-penalty state;
- `euroleague/src/euroleague_possessions/parser.py` normalizes input, executes
  fixed relationship/endpoint passes, and emits a rule-by-rule
  `decision_trace` for every event;
- `euroleague/src/euroleague_possessions/counter.py` emits possession rows,
  team/reason totals, sequential possession numbers, and game QA;
- `euroleague/src/euroleague_possessions/cli.py` provides a file-based
  diagnostic and optional CSV outputs;
- `euroleague/tests/` verifies manual labels, input-order independence,
  deterministic complete outputs, traces, numbering, and structural QA.

The original pure R transformation remains an independent reference:

- `etl/euroleague/group_events.R` normalizes package columns and derives
  `synthetic_parent_order`, `synthetic_ft_trip_id`, `final_end_poss`, endpoint
  reason, status, and confidence;
- `etl/euroleague/count_possessions.R` always recomputes grouping from raw
  columns and returns event-level output, one row per possession, team totals,
  reason totals, and game QA;
- `etl/euroleague/evaluate_grouping_sample.R` reports structural diagnostics for
  an unlabelled sample;
- `etl/euroleague/fixtures/event_grouping_edge_cases.csv` stores raw evidence
  and manual expectations;
- three `etl/tests/test_euroleague_*.R` scripts protect grouping, endpoints,
  deterministic counting, and structural invariants.

`euroleague/scripts/export_r_reference.R` and
`euroleague/scripts/compare_r_reference.py` compare every material derived
field between languages. The Python design is typed and more auditable, but it
deliberately preserves the already validated basketball semantics of the R
reference rather than changing rules during a language port.

### Python and R semantic parity

The Python implementation intentionally applies the same basketball logic as
the R reference. Both versions use the same:

- event ordering and period boundaries;
- shot, assist, block, and rebound relationships;
- offensive-foul, unsportsmanlike, turnover, and steal bundles;
- committed-foul and foul-drawn pairing;
- FT-parent candidate scoring and deterministic tie behavior;
- FT-trip partition rules;
- and-one and different-shooter dead-ball-FT treatment;
- technical, compound, and retained-ball penalty treatment;
- possession endpoint rules and endpoint reasons;
- grouping status, confidence, and game-QA definitions.

The 100-game comparison covered 56,463 events and found zero differences in:

| Derived field | Python/R mismatches |
|---|---:|
| `synthetic_parent_order` | 0 |
| `synthetic_ft_trip_id` | 0 |
| `final_end_poss` | 0 |
| `end_reason` | 0 |
| `grouping_status` | 0 |
| `grouping_confidence_pct` | 0 |

Both implementations produce exactly 14,684 possession rows.

The implementation structure differs without changing those semantics:

| R reference | Python candidate |
|---|---|
| Mutable vectors and procedural passes | Typed `Event`, `EventDecision`, and `PendingPenalty` objects |
| Relationship state is mostly implicit | Pending penalty state is explicit |
| No per-row explanation field | Rule-by-rule `decision_trace` on every event |
| Script sourced by the existing ETL | Installable package and command-line diagnostic |
| Relies on dataframe input assumptions | Rejects missing required fields and duplicate source identities |

The Python parser remains staged and multi-pass. This was deliberate: the
initial port established exact parity before attempting algorithmic changes.
A future single-pass or otherwise optimized parser must first reproduce the
same labelled fixtures and broad-sample decisions, with any intentional
basketball-rule divergence reviewed and documented separately.

Core design rules:

- order by season, game, period, and source event order;
- keep incident identity, FT-trip identity, and possession endpoint separate;
- allow at most one endpoint per incident;
- made baskets end on the shot unless a compatible scoring/penalty sequence
  deliberately moves the endpoint to final FT resolution;
- missed shots end on opponent control or period end, not on an offensive
  rebound;
- turnovers end once; offensive-foul and steal annotations do not add another
  endpoint;
- ordinary FT trips end on the final make, or after opponent control following
  a final miss;
- technical and retained-ball penalties do not automatically end possession;
- contradictory sequences remain visible and are flagged instead of being
  altered merely to force alternation.

No rule contains a gamecode-specific exception.

## Edge cases covered

The labelled regression data covers 20 patterns:

- clock-shifted and-one;
- and-one with substitutions before the FT;
- four-point play;
- bench technical after a made basket;
- personal and technical FT trips at the same clock;
- opposing FT trips at the same clock;
- interleaved personal and unsportsmanlike penalties;
- coach technical with a later-clock FT;
- blocked shot with block annotations and defensive rebound;
- blocked shot resolved by period end without a rebound;
- technical then personal trips with different shooters;
- offsetting technicals beside an ordinary personal trip;
- throw-in-foul FT followed by a separate personal trip;
- retained possession after a made basket;
- retained possession after a challenge;
- offensive rebound at period end;
- made basket followed by dead-ball FTs for a different teammate;
- a new foul closing an earlier special trip even with the same shooter;
- `CMU -> TO -> RV` unsportsmanlike sequence with retained ball;
- a provider offensive-rebound row between foul and FTs.

### Concrete and-one example

Season 2025, game 1, period 1:

- order 36: Istanbul made two-pointer by Isaia Cordinier at 06:01;
- order 37: Tel Aviv committed foul;
- order 38: Cordinier drew the foul;
- order 39: Cordinier made the FT at the provider's later 05:53 clock.

All four actions attach to shot root 36 and the possession ends only on FT 39
with reason `and_one_final_ft`. Manual confidence: 99%. The clock difference is
why equality of clock cannot be the grouping key.

## Rules added during broader testing

The 100-game audit produced several general improvements:

- `TOUT_TV` is administrative and does not close relationship searches;
- `CMU -> same-team TO -> opponent RV` is one unsportsmanlike incident;
- transparent administrative and rebound rows can occur between a foul and its
  FT trip;
- child turnovers inside an unsportsmanlike/offensive-foul bundle are not new
  FT search boundaries;
- rebound ownership is determined from rebound team relative to shooter team,
  even when the provider's `O`/`D` label conflicts;
- multiple rebound rows and intervening foul annotations can be scanned before
  resolving a miss;
- a made basket and compatible dead-ball FT trip by a different teammate are
  grouped as one scoring sequence, distinct from an and-one;
- a new committed foul closes an older pending FT parent after that parent has
  already emitted an FT, even when shooter and team are unchanged.

## Validation performed

### Labelled fixtures

The current fixture suite contains:

- 20 fixtures;
- 210 raw events;
- 73 manually labelled incidents;
- 34 FT rows;
- 42 possession endpoints.

The implementation exactly matches every labelled synthetic parent, FT parent,
FT-trip partition, possession endpoint, and endpoint reason. All three R test
scripts pass.

This 100% result is a regression guarantee for known cases, not an estimate of
accuracy on unseen EuroLeague games.

### Initial complete-game sample

An initial 23-game sample contained 12,674 events and produced 3,336
possessions. All 900 FTs were resolved, with no duplicate endpoints or missing
parents. Its two same-team endpoint transitions were both inspected and found
to be retained-ball sequences.

### Expanded 100-game sample

The broader convenience sample contained 100 complete season-2025 games and
56,463 raw events. Sequential early-season games were supplemented with
distributed later gamecodes. The final sample had no fetch failures.

| Diagnostic | Result |
|---|---:|
| Derived possessions | 14,684 |
| FT rows assigned | 4,122 / 4,122 (100%) |
| Provisional FT rows | 235 (5.70%) |
| Unresolved FT rows | 0 |
| Provisional endpoints | 26 (0.18%) |
| Duplicate-endpoint incidents | 0 |
| Missing parent targets | 0 |
| Games passing hard structural QA | 100 / 100 |
| Same-team endpoint transitions | 13 / 14,283 (0.091%) |
| Full count runtime | approximately 7.7 seconds |

Two independent executions returned identical complete result objects and
14,684 possession rows.

The team possession-count difference was zero in 32 games, one in 51, two in
16, and three in one. Differences above one are review signals, not automatic
errors, because retained possession and period boundaries can interrupt simple
alternation.

The conservative game QA marked 75 games `review` and 25 `clear`. A review can
be caused by one provisional FT, one same-team transition, or count difference
above one; it is not a structural failure.

The 2026-08-06 warning audit replaced the original arbitrary +/-8-row
special-penalty test with a live-play-bounded penalty cluster. Of the original
267 provisional FT rows, 215 remained provisional, 52 were separated from the
special penalty by a live boundary, and 20 previously missed rows joined the
same penalty cluster through transparent administrative/annotation rows. The
change affects warning status only: all 14,684 possession endpoints and their
reasons are unchanged. Python and R remain exactly equal across all 56,463
events.

### Official box-score and score-progression reconciliation

All 100 official box scores were collected into restartable per-game cache
files. Across 200 team-games, all 16 compared totals matched exactly: points,
2FG makes/attempts, 3FG makes/attempts, FT makes/attempts, offensive and
defensive rebounds, assists, steals, turnovers, blocks for/against, and fouls
committed/received.

Strict event-by-event score progression matched in 97 games. The other three
games each contained one adjacent pair where the provider score snapshot on
the first scoring row already included the next scoring action. The following
row then had no score delta. This exact one-event-lead pattern occurred in
games 10, 54, and 67; all three final scores and event totals matched. The
tightly bounded snapshot-lead reconciliation therefore passed 100/100 games,
while the strict result remains recorded as 97/100.

This validates event completeness and metric mappings. Box-score agreement
does not independently prove every possession boundary.

### Effect of audit-driven rules

On the same 100 games, the first pass had two unresolved FTs, 26 same-team
transitions, and 14,675 endpoints. After general-rule corrections it had zero
unresolved FTs, 13 same-team transitions, and 14,684 endpoints.

## Remaining same-team transitions

All 13 were inspected:

| Class | Count | Games / periods | Treatment |
|---|---:|---|---|
| Legitimate retained or compound possession | 5 | 5/P2, 60/P2, 38/P3, 70/P3, 75/P3 | Keep both endpoints; retain provisional status when entitlement is inferred. |
| Provider or control gap | 6 | 18/P1, 28/P1, 46/P1, 77/P2, 84/P3, 56/P4 | Preserve deterministic output and flag QA; do not invent an unrecorded change of control. |
| Mixed special-penalty sequence | 2 | 73/P3, 73/P4 | Keep provisional and manually reconcile before publication. |

Provider/control gaps include consecutive duplicate-looking turnovers and cases
where an opponent rebound is followed by the original offense's next shot with
no recorded turnover or other control change.

## Endpoint composition in 100 games

| Rule | Possessions |
|---|---:|
| Made field goal | 5,592 |
| Miss plus defensive rebound | 4,024 |
| Turnover | 2,539 |
| Ordinary made final FT | 1,385 |
| Final missed FT plus defensive rebound | 358 |
| Blocked shot plus defensive rebound | 308 |
| And-one final FT | 279 |
| Miss at period end | 177 |
| Offensive rebound at period end | 10 |
| Blocked miss at period end | 6 |
| Compound-penalty resolution | 3 |
| Made basket plus different-shooter dead-ball FTs | 3 |

## Confidence and effectiveness

Directly measured:

- exact labelled regression agreement: 100%;
- FT structural resolution in 100 games: 100%;
- hard structural QA pass: 100/100 games;
- deterministic rerun equality: exact;
- Python/R parity across 56,463 events: zero differences in parent, FT trip,
  endpoint, endpoint reason, status, or confidence.

Not directly measurable yet:

- overall semantic possession accuracy across the 100 games;
- correctness of every provisional penalty entitlement;
- whether each of the 37 package-invalid actor rows reflects delayed source
  attribution or a materially wrong stint boundary.

Indicative relationship confidence from observed patterns:

- ordinary shots, rebounds, turnovers, and ordinary FTs: 98-99%;
- explicit observed and-ones and four-point plays: 99%;
- technical, throw-in, interleaved, and retained-ball penalties: 90-95%;
- provider/control-gap cases: manual review required.

It would be misleading to call the whole system 100% accurate. The current
evidence supports "structurally reliable enough for shadow ETL," not
"production canonical truth."

## EuroLeague shadow schema

The reviewed DDL is in `euroleague/sql/001_core_shadow_schema.sql`. It was first
applied for the approved one-game trial on 2026-08-06. It is isolated to the
`euroleague` schema and contains no application views, grants, destructive
statements, or references to `basketball`/`basketball_test`.

| Relation | Grain / purpose |
|---|---|
| `load_runs` | One restartable load with package, collector, timing, and outcome metadata |
| `teams` / `players` | Stable internal keys for trimmed provider identifiers |
| `schedule` | One competition/season/gamecode with home/away teams |
| `source_artifacts` | One immutable cached payload or storage manifest entry |
| `full_rosters` | One game/team/player from package box scores, including starter provenance |
| `team_boxscores` | One official game/team total with explicit reconciliation metrics |
| `actions_raw` | One package-normalized event plus lossless source JSON |
| `actions_clean` | One event with synthetic parent, internal FT-trip identity, endpoint, confidence, and trace |
| `possessions` | One deterministic possession endpoint |
| `lineups` / `lineup_players` | One package lineup composition and its ordered source members |
| `action_lineups` | Package `Lineup_A`/`Lineup_B` and validation at one event |
| `stints` | One contiguous team/package-lineup interval using half-open event boundaries |
| `pws` | One narrow possession-to-offense/defense-lineup and stint association |
| `reconciliation_metrics` | One PBP-versus-official metric result per game/team/load |
| `game_qa` | One combined possession, score, box-score, and lineup release gate |
| `qa_incidents` | One detailed source contradiction or review item |

The design inherits the Israeli project's useful conventions: explicit primary
keys for deterministic upsert, source-preserving raw fields, raw-to-derived
separation, half-open stint intervals, per-game incremental grains, package and
parser lineage, and publication checks. It does not copy Israeli provider
columns or reconstruct lineups already supplied by `euroleague-api`.

FT-trip grouping is not materialized as its own table. It exists only as
`actions_clean.synthetic_ft_trip_id`, because its purpose is to make possession
counting deterministic and auditable rather than to provide a separate query
surface.

### Israeli guidance and deliberate improvements

| Israeli relation/pattern | EuroLeague decision | Reason |
|---|---|---|
| `schedule` | Keep the familiar relation and `game_id`; add competition, season, gamecode, and load lineage | Preserves downstream shape while namespacing EuroLeague source identity correctly. |
| `full_rosters` | Keep the game/team/player grain and starter flag; reference stable `teams`/`players` dimensions | Applies the Israeli identity lessons from the beginning rather than correcting reused IDs later. |
| `actions_clean` | Split immutable `actions_raw` evidence from one-to-one `actions_clean` derivations | Prevents a parser rerun from overwriting package/provider evidence. |
| Wide `possessions` copy of every action | Store only possession endpoints; keep event decisions on `actions_clean` | Avoids repeating all PBP columns while preserving the exact possession count. |
| Reconstructed `lineups_lookup` states | Store package `Lineup_A`/`Lineup_B` in normalized `lineups`, `lineup_players`, and `action_lineups` | Uses package capability directly and avoids ten player-state rows per event. A compatibility view can be added only if an app query needs it. |
| Overlapped paired `stints` | Store simple contiguous team-lineup stints | Package already supplies both event lineups; pairing belongs in the possession bridge, not the base stint identity. |
| Wide action-level `pws` | Store a narrow one-row-per-possession bridge to offense/defense lineups and stints | Retains the Israeli query concept without duplicating every action column again. |
| Publication inferred from downstream presence | Use `load_runs`, `game_qa`, and explicit publication status | Makes partial loads and failed QA visible and restartable. |

### Database-free schema coverage

`euroleague_possessions.schema_coverage` maps the package and possession
outputs to the schema's natural-key contract without connecting to PostgreSQL.
It checks schedule sides, starters, roster identity, action keys, package lineup
membership, possession offense teams, and endpoint-lineup availability.

The 100-game sample produced:

| Diagnostic | Result |
|---|---:|
| Schema-ready games | 100 / 100 |
| Blocking mapping issues | 0 |
| Package lineup names missing from rosters | 0 |
| Ambiguous package lineup names | 0 |
| Possession endpoints without a package lineup | 0 |
| Invalid possession offense teams | 0 |
| Non-roster pseudo-actor rows retained as raw-only | 35 |
| Package-invalid on-court actor rows retained for QA | 37 |

The 35 non-roster actor rows use coach/bench-style identifiers such as `CO_A`,
`CO_B`, and `AC_A` and have no player name. They are not missing players. The
loader must preserve their provider ID in `actions_raw` while leaving the
normalized `player_id` foreign key null. A named PBP player missing from the
box-score roster would instead block the game.

### Deterministic 100-game staged load plan

`euroleague_possessions.load_plan` resolves package lineup names to provider
player IDs, hashes lineup composition, counts contiguous team-lineup runs, maps
every possession endpoint to package lineups, and reports the exact planned
rows without opening a database connection.

All 100 games are loadable and the current staged plan reports zero issues.
Shared team/player counts below are unique upsert candidates; the remaining
counts are additive per-game rows:

| Table | Planned rows |
|---|---:|
| `load_runs` | 1 |
| `teams` | 20 |
| `players` | 322 |
| `source_artifacts` | 300 |
| `schedule` | 100 |
| `full_rosters` | 2,388 |
| `team_boxscores` | 200 |
| `actions_raw` | 56,463 |
| `actions_clean` | 56,463 |
| `possessions` | 14,684 |
| `lineups` | 5,573 |
| `lineup_players` | 27,865 |
| `action_lineups` | 56,463 |
| `stints` | 6,873 |
| `pws` | 14,684 |
| `player_four_factors_by_game` | 216,660 |
| `reconciliation_metrics` | 3,200 |
| `game_qa` | 100 |
| `qa_incidents` | 0 |

Each game checkpoint retains its own schedule, PBP, and box-score artifact,
which makes restart validation independent and explains the 300 source
artifacts. `qa_incidents` is not pre-populated by the plan; package actor
validity already lives on `action_lineups`, and detailed incident rows should
be created only for review items that require their own lifecycle.

The offline analytics validator independently projects the same 216,660
`player_four_factors_by_game` rows from the 2,388 roster appearances,
including 186 DNP appearances. All 100 game clocks consume the exact game-time
budget; every player ON+OFF partition is exact; and every additive metric
reconciles to official totals.

### Driver-independent transaction contract

`euroleague_possessions.transaction_writer` defines and tests the atomic
per-game replacement boundary without importing a database driver or opening a
connection. For each loadable game it plans:

1. begin a transaction and resolve/upsert the schedule natural key;
2. delete replaceable game rows child-first;
3. insert a complete staged snapshot parent-first;
4. run final database-side integrity and publication checks;
5. commit only if every prior operation succeeds, otherwise roll back.

The base-snapshot writer produces 2,800 operations: 100 begins, 1,300 deletes,
1,200 non-empty table inserts, 100 validations, and 100 commits. Every game's
sequence is gap-free and there are no duplicate `(season, gamecode, sequence)`
keys. Re-running the same manifest is deterministic. The SQL analytics refresh
runs inside database validation and is intentionally not counted as a second
Python insert operation per derived row.

The contract deliberately excludes immutable `source_artifacts` and run-level
or shared dimension writes from game replacement. A PostgreSQL adapter must
also delete `lineup_players` through the game's `lineups`, because
`lineup_players` has no direct `game_id`. The adapter must resolve generated
lineup/stint identities from natural keys before inserting dependent rows.

This contract was initially verified without live SQL. The live adapter now
implements the same ordering, natural-key resolution, generated lineup/stint
ID resolution, current-run audit replacement, database count checks, and
rollback behavior using pinned `psycopg==3.2.9`.

### PostgreSQL trial and controlled batch

On 2026-08-06, the reviewed migrations were applied on direct PostgreSQL port
5432. The initial `E/2025/1` load and rollback probe were followed by a
controlled `E/2025/1-3` publication. The batch reused validated checkpoints,
replaced game 1 idempotently, added games 2 and 3, and wrote only to the
`euroleague` schema.

All three games share `load_run_id=3`, whose final audit is `completed` with
three requested, three successful, and zero failed games:

| Gamecode / game ID | Actions | Possessions | Lineups / members | Stints | Analytics facts | Status | Review evidence |
|---|---:|---:|---:|---:|---:|---|---|
| 1 / 1 | 546 | 140 | 51 / 255 | 64 | 1,824 | `review` | 4 provisional FT rows; 1 invalid actor |
| 2 / 4 | 603 | 154 | 56 / 280 | 74 | 2,496 | `review` | 2 invalid actors |
| 3 / 5 | 497 | 138 | 50 / 250 | 55 | 1,920 | `clear` | None |
| **Total** | **1,646** | **432** | **157 / 785** | **193** | **6,240** | — | 0 unresolved FTs |

Every game has 24 normalized roster players, two team box scores, three source
artifacts, 32 exact reconciliation metrics, one current-run QA row, and equal
`possessions`/`pws` counts. The read-only post-publication audit found zero
checkpoint mismatches. Box-score metrics, score progression, lineup structure,
and structural possession checks pass for all three. Review statuses therefore
preserve narrow evidence warnings rather than hiding or repairing source rows.
The original game 1 rollback probe also passed, restoring all 140 `pws` rows.

#### Disposition of the seven warning rows

The four provisional FT warnings and three package-invalid actor warnings were
reviewed against their surrounding raw events:

- game 1 event 214 is the first FT in an unsportsmanlike sequence; event 216 is
  the second FT in the same parent/trip. Substitutions and a delayed assist fall
  between them, so both remain provisional together;
- game 1 events 438 and 443 are retained-ball bench/technical penalty FTs and
  remain provisional;
- game 1 event 461 is a clear and-one. A live shot boundary separates it from a
  later unsportsmanlike incident, so it is now confirmed;
- game 1 event 215 and game 2 events 437 and 486 are assists emitted after each
  assister's recorded `OUT` substitution. The actors are genuinely absent from
  the package lineup at those rows, so the flags are retained as delayed source
  attribution; no lineup is rewritten.

The live `load_run_id=3` snapshot was built with parser `0.1.0`. It still has
the same four-warning count, possession rows, and analytics totals, but its
game-1 warning identities reflect the old rule (214, 438, 443, 461 rather than
214, 216, 438, 443). Parser `0.2.0` and stage format `2` invalidate those stale
derived checkpoints. No database republish was performed during this audit.

### PPP and player on/off analytic requirement

The EuroLeague analytic layer should follow the Israeli raw-count method while
using unambiguous names:

- literal PPP = `points / possessions`;
- offensive rating = `100 * offensive points / offensive possessions`;
- defensive rating = `100 * points allowed / defensive possessions`;
- player net on/off = `(OffRtg_ON - OffRtg_OFF) - (DefRtg_ON - DefRtg_OFF)`.

The Israeli `onoff_compute` function calls its per-100 result `ppp_calc`; the
EuroLeague layer should expose both `*_ppp` and `*_rtg` rather than overloading
one label.

The deterministic calculation grain is action/team context, not only the
possession endpoint:

1. Derive event points from made 2PT, 3PT, and FT actions.
2. Use package `action_lineups` to attach each team's lineup at that scoring
   action.
3. Use `possessions.endpoint_source_event_order` to count exactly one offensive
   and one mirrored defensive possession at each endpoint.
4. Expand each action into home-team and away-team perspectives, crediting
   points scored/allowed and offensive/defensive possession flags separately.
5. Cross each team's game roster to those contexts and use lineup membership to
   assign `is_on=true/false` for every player, including players who never enter.
6. Sum raw points and possessions by game/team/player/ON-OFF/side. Calculate
   ratios only after the requested games are aggregated.

Lineup changes never create or end possessions. A substitution within a free-
throw or and-one sequence can legitimately attach a scoring event and the final
possession endpoint to different lineups; retaining action-level lineup context
matches the Israeli method and avoids forcing the full sequence onto one lineup.

The base schema retains the required evidence in `actions_raw`,
`actions_clean`, `action_lineups`, `possessions`, `lineups`, `lineup_players`,
and `full_rosters`. Migration
`euroleague/sql/002_existing_analytics_compatibility.sql` now adds the derived
contract while keeping the facts additive:

- `player_four_factors_by_game` stores raw ON/OFF offense/defense points,
  possessions, shot/rebound/turnover numerators, and minutes by starter context;
- `refresh_stint_timing_for_games()` derives canonical stint durations without
  overwriting raw provider clocks;
- `refresh_player_four_factors_by_game_for_games()` refreshes only the affected
  games inside the publication validation transaction;
- `final_schedule`, `player_onoff_by_season`, and
  `player_four_factors_by_season` remain the always-current semantic views;
- `final_schedule_mv`, `player_onoff_by_season_mv`, and
  `player_four_factors_by_season_mv` are indexed app-facing materialized views
  defined by migration `003_app_materialized_views.sql`.

The MVs refresh once when a load run finishes, after all per-game
`player_four_factors_by_game` refreshes succeed. This follows the Israeli mixed
architecture: per-game facts are incrementally maintained physical rows, while
bounded application aggregates are materialized. Migration 003 is prepared and
tested in the repository but has not been applied to the live shadow schema.

Package aggregate `Team`/`Total` rows are excluded from normalized players and
rosters but remain in raw evidence. Remaining application work is limited to
real package schedule dates/round/phase values, explicit publication filtering
for `review`/`blocked` games, and the eventual league-aware app adapter.

The read-only worked query is
`euroleague/sql/analytics/player_onoff_ppp_readonly.sql`. On game `E/2025/1`,
team totals reconcile to 85 points / 70 possessions for IST and 78 / 70 for
TEL. Two single-game player examples are:

| Player | Off ON | Off OFF | Def ON | Def OFF | Net ON/OFF |
|---|---:|---:|---:|---:|---:|
| Shane Larkin (IST) | 74/58 = 127.6 | 11/12 = 91.7 | 66/57 = 115.8 | 12/13 = 92.3 | +12.4 |
| Jaylen Hoard (TEL) | 64/57 = 112.3 | 14/13 = 107.7 | 72/56 = 128.6 | 13/14 = 92.9 | -31.1 |

Values after `=` are ratings per 100 possessions. These are one-game examples
with small OFF samples, not stable player evaluations.

## Hurdles and action items

### High severity

1. **Relationship reconstruction** — implemented and structurally validated;
   continue adding a general fixture for every new pattern.
2. **Possession reconciliation** — completed on the 100-game sample; repeat the
   same box-score and score-progression gates for every future load.
3. **Lineup validity** — package output passed the structural audit; persist its
   37 invalid-actor flags and review them without a second lineup engine.
4. **Provider gaps** — persist QA incidents and create a documented correction
   layer rather than editing raw events.
5. **Storage reproducibility** — checkpointed collection, applied shadow DDL,
   staged row construction, PostgreSQL identity resolution, exact database
   validation, idempotent replacement, analytics refresh, live rollback, and a
   shared-run three-game publication are proven. The 100-game offline batch and
   restart also pass. A deliberately interrupted/partial live recovery drill is
   the remaining transaction-lifecycle test before broader use.

### Medium severity

1. Normalize team/player identity across seasons and competitions.
2. Record API/package version and retrieval provenance.
3. Handle rate limiting and partial loads explicitly.
4. Review package licensing for the intended deployment/distribution model.
5. Define cross-league canonical views only after EuroLeague tables stabilize.

## Recommended next sequence

1. Review and apply `003_app_materialized_views.sql` to the isolated schema;
   this changes derived database objects but does not load another game.
2. Re-stage the three controlled games offline with parser `0.2.0` and validate
   that counts, endpoints, analytics, and reconciliation are unchanged.
3. Republish only those three games under a new load run with explicit approval;
   this changes parser/warning lineage, not possession totals.
4. Run the safe partial-run/restart drill with explicit approval after the
   corrected three-game snapshots and app MVs are verified.
5. Validate at least one additional season when production integration becomes
   the objective.
6. Only then design cross-league views and app integration.

## Reproduction

Install the sub-project into a local environment using any Python 3.10+
interpreter:

```powershell
python -m venv euroleague/.venv
& euroleague/.venv/Scripts/python.exe -m pip install -e euroleague
```

Run the Python tests and 100-game diagnostic:

```powershell
& euroleague/.venv/Scripts/python.exe -m unittest discover `
  -s euroleague/tests -v
& euroleague/.venv/Scripts/euroleague-possessions.exe `
  C:\tmp\euroleague_pbp_2025_100games.csv
```

Apply the package's lineups to the cached sample and export its QA evidence:

```powershell
& euroleague/.venv/Scripts/python.exe -m `
  euroleague_possessions.package_lineups `
  C:\tmp\euroleague_pbp_2025_100games.csv `
  euroleague/data/raw/boxscores `
  --output-dir euroleague/data/exports/package_lineups_100
```

Validate the schema contract without connecting to PostgreSQL:

```powershell
& euroleague/.venv/Scripts/python.exe -m `
  euroleague_possessions.schema_coverage `
  C:\tmp\euroleague_pbp_2025_100games.csv `
  euroleague/data/raw/boxscores `
  --output-dir euroleague/data/exports/schema_coverage_100
```

Generate the deterministic per-table load plan without database writes:

```powershell
& euroleague/.venv/Scripts/python.exe -m `
  euroleague_possessions.load_plan `
  C:\tmp\euroleague_pbp_2025_100games.csv `
  euroleague/data/raw/boxscores `
  --output-dir euroleague/data/exports/load_plan_100
```

Validate the additive ON/OFF and four-factor contract over the cached sample:

```powershell
& euroleague/.venv/Scripts/euroleague-validate-analytics.exe `
  C:\tmp\euroleague_pbp_2025_100games.csv `
  euroleague/data/raw/boxscores
```

Build or resume deterministic per-game stage checkpoints without touching the
database:

```powershell
& euroleague/.venv/Scripts/euroleague-batch.exe `
  C:\tmp\euroleague_pbp_2025_100games.csv `
  euroleague/data/raw/boxscores `
  --checkpoint-dir euroleague/data/staging/batch_100 `
  --stage-workers 1
```

The batch command is offline unless `--execute` is supplied. Multi-game
publication additionally requires `--confirm-multiple-games`; do not use those
flags for new games without explicit approval.

Audit published games against their immutable stage checkpoints without
modifying the database:

```powershell
& euroleague/.venv/Scripts/python.exe `
  euroleague/scripts/audit_live_batch.py `
  --season 2025 --gamecodes 1,2,3 `
  --checkpoint-dir euroleague/data/staging/batch_100
```

Reproduce the local warning-context audit without API or database I/O:

```powershell
& euroleague/.venv/Scripts/python.exe `
  euroleague/scripts/audit_review_warnings.py `
  C:\tmp\euroleague_pbp_2025_100games.csv `
  euroleague/data/raw/boxscores `
  --season 2025 --gamecodes 1,2,3 --radius 12 `
  --output-dir euroleague/data/exports/review_warning_audit_3games
```

Expand that plan into the deterministic per-game transaction manifest, still
without importing a database driver or writing to PostgreSQL:

```powershell
& euroleague/.venv/Scripts/python.exe -m `
  euroleague_possessions.transaction_writer `
  euroleague/data/exports/load_plan_100/load_plan_games.csv `
  --output euroleague/data/exports/transaction_plan_100.csv
```

Reproduce the one-game staging pass without a database connection by omitting
`--execute`. The following is the explicitly mutating form used for the
approved trial; do not run it for additional games without approval:

```powershell
& euroleague/.venv/Scripts/python.exe -m `
  euroleague_possessions.postgres_trial `
  C:\tmp\euroleague_pbp_2025_100games.csv `
  euroleague/data/raw/boxscores `
  --season 2025 --gamecode 1 `
  --execute --apply-schema --probe-rollback
```

Export the R reference and verify event-level parity:

```powershell
& 'C:\Program Files\R\R-4.4.2\bin\Rscript.exe' `
  euroleague/scripts/export_r_reference.R `
  C:\tmp\euroleague_pbp_2025_100games.csv `
  tmp/euroleague_r_grouped_100.csv
& euroleague/.venv/Scripts/python.exe `
  euroleague/scripts/compare_r_reference.py `
  C:\tmp\euroleague_pbp_2025_100games.csv `
  tmp/euroleague_r_grouped_100.csv
```

Run the independent R regression tests using the commands in
`euroleague/AGENTS.md` after any semantic rule change.

## Artifact index

- `euroleague/AGENTS.md` — inherited and EuroLeague-specific operating rules
- `euroleague/pyproject.toml` — installable typed Python sub-project
- `euroleague/requirements.txt` — pinned extraction dependency
- `euroleague/src/euroleague_possessions/` — Python parser, counter, models,
  CLI, bounded/restartable PBP and box-score collectors, official
  reconciliation, concurrent checkpoints, analytics validation, and thin
  package-lineup/schema-coverage/load-planning/staging/PostgreSQL adapters
- `euroleague/tests/` — 54 Python regression, transaction, concurrency, and
  determinism tests
- `euroleague/sql/001_core_shadow_schema.sql` — applied isolated schema migration
- `euroleague/sql/002_existing_analytics_compatibility.sql` — applied additive
  ON/OFF and four-factor analytics contract
- `euroleague/sql/003_app_materialized_views.sql` — prepared, not-yet-applied
  indexed app read layer and batch refresh function
- `euroleague/scripts/export_r_reference.R` — R decision export
- `euroleague/scripts/compare_r_reference.py` — cross-language parity check
- `euroleague/scripts/audit_live_batch.py` — read-only live/checkpoint count,
  QA, analytics, and batch-lineage audit
- `euroleague/scripts/audit_review_warnings.py` — deterministic local context
  export for provisional FTs and package-invalid lineup actors
- `docs/euroleague_event_grouping_spec.md` — relationship contract
- `docs/euroleague_event_grouping_effectiveness_2026-08-05.md` — initial
  fixture and 23-game effectiveness report
- `docs/euroleague_possession_audit_100_games_2026-08-05.md` — detailed
  100-game audit
- `etl/euroleague/group_events.R` — relationship/endpoint state machine
- `etl/euroleague/count_possessions.R` — deterministic counter and QA outputs
- `etl/euroleague/evaluate_grouping_sample.R` — broad-sample diagnostics
- `etl/euroleague/fixtures/event_grouping_edge_cases.csv` — manual labels
- `etl/tests/test_euroleague_event_grouping_fixtures.R`
- `etl/tests/test_euroleague_group_events.R`
- `etl/tests/test_euroleague_count_possessions.R`

The 100-game CSV remains outside the repository. The reproducible local Python
environment is `euroleague/.venv` and is ignored by Git. No database,
production ETL orchestrator, or application file was changed by this work.
