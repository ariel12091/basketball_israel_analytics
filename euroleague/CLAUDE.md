# CLAUDE.md — EuroLeague sub-project

Operating notes and review remarks for the EuroLeague shadow project. Read
`euroleague/PROJECT.md` (status and plan) and `euroleague/AGENTS.md` (rules)
first; this file records how the plan lines up with the Israeli project and what
to adjust. The Israeli references are the root `CLAUDE.md`/`PROJECT.md` and
`docs/database_context.md`.

Last reviewed: 2026-08-06, against `PROJECT.md` at the three-game controlled
batch (`E/2025/1-3`, `load_run_id=3`) with migration 003 prepared but not
applied.

## What the plan is

Six ordered steps: apply migration 003 (app-facing MVs), re-stage the three
controlled games offline with parser `0.2.0`, republish them under a new load
run, run a partial/interrupted recovery drill, validate a second season, and
only then design a league-aware app adapter. Each database step is gated on
explicit approval. The staged shadow approach, the package-first rule, the
raw/derived split, and the refusal to make Israeli row-for-row parity a release
gate are all correct and should stay.

## Verified as aligned with the Israeli contract

- **Canonical timing.** `refresh_stint_timing_for_games()` reproduces the
  Israeli clock contract rather than the raw `MAX - MIN` pattern that project
  abandoned: period-aware elapsed seconds, a running-maximum monotonic guard
  against source clock regression, the next stint's start as this stint's end,
  and game end for the last stint. Keep it.
- **TS and OREB denominators.** `ts_possessions` = FGA plus the last free throw
  of a committed-foul trip; `oreb_opportunities` = missed FGA plus that missed
  last personal-foul FT. These match the Israeli definitions, which means
  EuroLeague TS% is deliberately *not* `PTS / (2 * (FGA + 0.44 * FTA))`. The
  Israeli project keeps both denominators for different questions — an adapter
  must never mix them under one label.
- **Raw counts before rates.** The season views sum numerators and denominators
  and divide once. That is the Israeli rule that prevents averaged percentages.
- **`*_ppp` versus `*_rtg`.** A genuine improvement over the Israeli `ppp_calc`
  naming. Do the renaming in the adapter; do not "fix" the Israeli names.

## Step 1 — applying migration 003

- The three MVs are `SELECT *` over the ordinary views, so the column list is
  resolved at creation. A column added later to `player_onoff_by_season` never
  reaches `player_onoff_by_season_mv`, and the planned bidirectional `EXCEPT`
  parity check then fails on column count rather than on content. Either pin
  explicit column lists or make "drop and recreate the MV whenever migration 002
  changes a view" an explicit rule. Same family as the Israeli lesson that
  `REFRESH` re-runs the stored definition and only `DROP`+`CREATE` changes it.
- The refresh is non-concurrent and runs inside the publication transaction, so
  it holds `ACCESS EXCLUSIVE` on all three app-facing relations for the length
  of the commit. Irrelevant at three games; with an app pointed at the schema and
  a full season loaded, it is a read outage on every publication. The unique
  indexes needed for `REFRESH ... CONCURRENTLY` already exist, but `CONCURRENTLY`
  cannot run inside a transaction block — so the fail-closed in-transaction
  design and concurrent refresh are mutually exclusive. Keep the current design
  while EuroLeague is a shadow schema; before broadening, move the refresh after
  the commit and keep it fail-closed by marking the run `completed` only after
  the refresh succeeds (the Israeli `etl_processed_games` / `app_meta` marker
  pattern).
- Verify with catalog `relkind`, not the `_mv` suffix. The Israeli schema is full
  of `_mv` names that are physical tables, and scripts branching on the name are
  a documented hazard there.
- Useful corollary of the fail-closed dependency: the live schema currently
  *cannot* complete a load run, because the publication code calls a function
  that does not exist yet. That is the desired safety property — record it in the
  runbook so nobody "fixes" it by deleting the call.

## Steps 2-3 — re-stage and republish the three games

Agreed and low risk. Add one numeric gate: the accepted diff is confined to
warning identities and parser/derivation lineage. Possessions (432), analytics
facts (6,240), endpoints, endpoint reasons, and every reconciliation metric must
be identical between the `0.1.0` and `0.2.0` checkpoint exports. Diff the two
exports directly rather than only re-running the validators — the Israeli rule
for refactors that must be output-identical.

## Step 4 — recovery drill

Extend the drill to the publication marker, not only the base rows. It should
prove that a run with one failed game cannot report `completed`, that a `partial`
run leaves the MVs consistent with the *committed* facts rather than the
requested set, and that a retry re-refreshes them. This is the exact failure the
Israeli project guards with "never mark a game processed before downstream
validation completes".

## Step 5 — identity comes before the second season

The largest omission in the current order. `euroleague.players` is
`UNIQUE (competition, provider_player_id)` with no season scope and no identity
dictionary, and every season aggregate is keyed `(game_year, team_id,
player_id)`. The Israeli project learned that provider IDs are re-minted per
season *and* recycled to different people — which is why `player_identities`,
`player_identity_map`, and `resolved_player_identity_v` exist. A second
EuroLeague season loaded onto the current dimension will merge or split people
silently, and the damage lands in aggregates rather than in a load error.

Build the identity layer *with* the second season, not after it, and add two
data-quality checks at the same time: same normalized name with more than one
provider ID, and one provider ID whose name or team changes across seasons.
EuroLeague also has mid-season transfers and E/U cross-competition movement, so
scope (`season`, `game`) and evidence columns matter as much as in the Israeli
map. The same argument applies more weakly to
`teams (competition, provider_team_code)`.

## Step 6 — semantic mismatches to settle before an adapter exists

False friends: the EuroLeague column already carries an Israeli name with a
different meaning, and each gets harder to change once anything reads it.

| Field | Israeli meaning | Current EuroLeague mapping | Adjustment |
|---|---|---|---|
| `game_year` | Season-ending year (2026 = 2025-26); drives the global season selector and every filter | `s.season AS game_year`, and the provider labels 2025-26 as season 2025 | Pin the convention. Recommended: keep the provider season in the base table and expose `game_year = season + 1` in the view/adapter layer, so one selector value never means two different seasons. |
| `gn` | Schedule game number, filtered as an inclusive range by `p_min_gn`/`p_max_gn` | `s.gamecode AS gn` | `gamecode` is a season-wide game identifier, not a round. `schedule.round_number` is the real analogue; a GN range over gamecodes would mean "league games 5-10", which is not what the UI promises. |
| `game_type` | Integer schedule code, filtered via `p_game_type_csv` | `s.phase AS game_type` (text) | Needs an explicit phase-to-code mapping, not a cast. |

Two adapter-level items the step does not mention:

- **Access.** The `euroleague` schema has no grants and no RLS. The Israeli app
  connects as `app_readonly` with relation SELECT plus an EXECUTE allowlist, and
  `DROP FUNCTION` wipes those grants — so re-running any EuroLeague migration
  after the adapter exists must be followed by the security apply/audit step. The
  app also hardcodes `basketball_test` in its SQL.
- **Caching and reference lookups.** The Shiny app's cross-session caches key on
  `game_year` plus the ETL data version, and four canonical lookups
  (`fetch_teams_distinct()`, `fetch_teams_min()`, `fetch_gn_values()`,
  `fetch_players_basic()`) feed every dropdown. A league dimension must enter
  those cache keys and lookups, or the two leagues will serve each other's teams
  and players.

## Content-level remarks on what is already built

- **Permanently zero columns.** `deflection_count`, `c3_made`, `c3_att`, and
  `c3_known_att` are hardcoded `0` in
  `refresh_player_four_factors_by_game_for_games()`. Two consequences. First,
  `disruption_rate = (steals + deflections) / poss` in
  `player_four_factors_by_season` is a steals-only rate wearing a name that
  claims more than the source records — rename it or document the difference
  before anything displays it. Second, the corner-3 columns are dead weight until
  the shots endpoint is collected (`source_artifacts.artifact_type` already
  permits `'shots'`). If corner-3 is wanted, collect coordinates and re-derive
  the zone rule in EuroLeague's own coordinate system and units; do not copy the
  Israeli `y <= 285` threshold, which is a tangent line fitted to that provider's
  raw coordinates.
- **Layup and dunk flags** come from `play_info ILIKE '%lay%up%'` / `'%dunk%'` on
  free text, where the Israeli side uses an enumerated `parameters_type` tag and
  deliberately counts `dunk` plus `allyhoop` as dunks. Before these are shown
  anywhere, profile the distinct `play_info` vocabulary over the 100-game sample,
  decide alley-oop handling, and name the columns after the field they actually
  read.
- **QA status is not a publication filter.** The season views aggregate every
  published game regardless of `game_qa`, so the two `review` games are already
  inside the three-game totals. The Israeli project tolerates the same thing but
  pairs it with a standing data-quality report. Expose at least a
  `review`/`blocked` count next to the season aggregates so a consumer can tell.
- **Add the Israeli minute invariants to game QA**, now that the timing function
  is good enough to be checked: team minutes equal five times game length per
  team, the five on-court players' minutes sum to five times team lineup minutes,
  and no negative or overlapping stints. Those three checks surface the Israeli
  `T`, `X`, and `R` class defects.

## The plan does not mention storage, and it should

The Israeli database lives under a Supabase size constraint — the entire reason
cold storage, Parquet export, and the five-table truncate exist — and it has
already drifted past the budget that design targeted. The EuroLeague plan puts
its data in the same instance with no size estimate and no retention policy.

The 100-game plan is 56,463 `actions_raw` rows with lossless per-event JSON,
56,463 `actions_clean`, 56,463 `action_lineups`, 216,660 analytics facts, and 300
`source_artifacts` whose `payload jsonb` may hold whole responses. A 20-team
EuroLeague season is on the order of 400 games including playoffs — roughly four
times that plan — and the stated goal is more than one season.

Do this before step 5, not after: measure the real per-game footprint from the
three loaded games with `pg_total_relation_size` per relation, project a season
and a decade, then decide two things — prefer `storage_uri` over inline `payload`
for artifacts (the existing CHECK already allows either), and define the
EuroLeague cold-storage boundary now (which relations are rebuildable from
Parquet plus checkpoints, and which must stay hot). The Israeli project had to
retrofit exactly this, and the retrofit is where its FK-before-truncate and
stale-key problems came from.

## The plan needs a second half

Steps 1-5 answer one question well — is this pipeline trustworthy — and that is
the genuinely uncertain part. But step 6 is a single sentence covering most of
the remaining work, and nothing in the plan builds the operational layer a live
app section needs. As written, the plan reaches "trustworthy shadow data with one
tab's worth of read layer", not "EuroLeague in the app".

The continuation below keeps the first half's discipline: each step is
separately approvable, and no step loads games or touches the Israeli schema
unless it says so.

### Step 0 — decide scope, and decide it first

Currently the scope decision sits inside step 6, *after* the validation work
whose size it determines. Settle three things before broadening:

- **Competition.** EuroLeague only, or EuroLeague plus EuroCup. EuroCup is where
  several Israeli-league clubs actually play, which makes it the more
  interesting option for this app and a defensible same-team comparison in a way
  that Israeli-league versus EuroLeague is not. The package supports `U`
  throughout and the project's own collectors are already parameterized; what is
  unproven is the data (see step 5b).
- **Season depth.** Current season only, or history. This is the single largest
  input to the storage projection.
- **Surface set.** Which Israeli tabs get a EuroLeague counterpart. See the
  scope note at the end of this file; the defensible minimum is schedule, team
  ratings, and player on/off.

### Step 4b — thin vertical slice, before broadening

Insert between the recovery drill and second-season validation. Build schedule
plus team ratings for the three already-loaded games, behind a league switch,
read-only, local, not deployed. It is small, and it forces every unresolved
contract question — season convention, GN meaning, grants, cache keys, league
isolation in the UI — to surface while changing them is still cheap. This is the
parent project's own migration doctrine: incremental, parity-tested by vertical
slice, existing product stays live.

### Step 5b — one EuroCup game, if EuroCup is in scope

Database-free. Collect a single `U` game and run schema coverage,
reconciliation, and possession QA against it. Three things decide whether
EuroCup is viable at all: box-score `IsStarter` flags (the lineup engine
bootstraps from them), the play-type vocabulary the penalty rules depend on, and
PBP completeness, which is historically thinner outside EuroLeague. Cheap
experiment, large decision.

### Step 7 — read layer beyond Tab 1

The three MVs cover player on/off only. Add, in cost order:

1. Team season ratings and team four factors — cheapest, most useful, and the
   only major surface that does not depend on package-reconstructed lineups.
2. Team per-game facts, which give game logs almost for free.
3. Traditional player stats — but promote the box-score columns out of
   `full_rosters.boxscore_stats jsonb` first. EuroLeague's official player box
   scores reconcile exactly, so this surface is *better* sourced than its
   Israeli counterpart.

Decide here whether the event × team-perspective fact gets materialized (item 7
of the table remarks). It is the difference between one action scan per load and
one per metric, and it gets harder to retrofit with every metric added.

### Step 8 — access and security

The `euroleague` schema has never been through the Israeli security pass.

- Run `sql/security/audit_app_access.sql` against `euroleague` *before* any grant
  exists, to confirm nothing was inherited from Supabase defaults. Never add the
  schema to Supabase's Data-API exposed schemas.
- Grant `app_readonly` SELECT and extend `sql/security/*.sql`,
  `scripts/apply_db_security.R`, and `test-db-security-contracts.R` in the same
  change. Use one policy naming convention from the start.
- Remember that `DROP FUNCTION` wipes EXECUTE grants: any later EuroLeague
  migration must be followed by the security apply step.

A dedicated `euroleague_etl` owner role is **not** needed while loads are manual.
Reusing the Israeli write role is fine here: every EuroLeague transaction sets
`search_path` to `euroleague, public`, and `basketball_test` is on neither, so an
unqualified statement cannot reach an Israeli table by accident — it would take an
explicitly `basketball_test.`-qualified statement, which the DDL applier already
rejects. Revisit only at step 11: once publication is scheduled and unattended,
a credential is executing writes with no human in the loop, and least privilege
starts paying for its overhead.

### Step 9 — the adapter

Put the adapter views in a third schema (`analytics_common` or similar) that
depends on both leagues, so neither league's schema depends on the other and a
`DROP ... CASCADE` cannot cross the boundary. The adapter owns:

- the three field mappings (`game_year` off-by-one, `gn` versus `round_number`,
  `game_type` phase-to-code);
- the league key, which must travel with every game and player ID — the two
  leagues' surrogate `game_id` values collide numerically;
- the `*_ppp` / `*_rtg` renaming into whatever the app expects, without
  renaming anything on the Israeli side;
- a hard rule that no ranked table mixes leagues. Two independent possession
  engines, different competition quality, and different shot-type derivations
  make cross-league ranking a fabricated comparison.

Also: the `est. ±X pts` four-factor annotations use weights fitted to the
Israeli league. Refit them on EuroLeague data or suppress them for that league.
Do not reuse the Israeli coefficients.

### Step 10 — app integration

Shiny is the product; `frontend-v2` is archival and stays untouched.

- Thread a league dimension through the global season selector, the four
  canonical lookups (`fetch_teams_distinct()`, `fetch_teams_min()`,
  `fetch_gn_values()`, `fetch_players_basic()`), and every `GL_DATA_CACHE` /
  `cached_ref_query()` key, or the leagues will serve each other's dropdowns.
- The shared `data_version` must reflect whichever league's load is newest, or
  caches will not invalidate after a EuroLeague publication.
- New section with its own tabs rather than a league filter inside existing
  tabs — it keeps the "no mixed ranked tables" rule structural.
- Inherit the existing guardrails: `guard_heavy_request()`, statement timeout,
  idle-session restore, filter chips, loading skeletons.

### Step 11 — operations

A live section needs data to arrive without a human. Nothing in steps 1-6 builds
this.

- A game-keyed publication marker (table remarks item 6) is the prerequisite:
  incremental "what is new since last night" cannot be answered from
  `game_qa (load_run_id, game_id)`.
- Scheduled collection and publication, per-run logs, and a last-success marker
  the UI can display — the `app_meta.etl_full_last_success` pattern.
- Explicit failure handling: a partial run must be visible, not silent, and must
  not leave the app-facing MVs describing games that rolled back.
- A storage watch with a threshold, given the shared instance and the budget
  already exceeded.

### Completion criteria for the second half

The goal is met when a EuroLeague (and, if in scope, EuroCup) section is live in
the deployed Shiny app, fed by scheduled loads with no manual step, reading
through the adapter schema as `app_readonly` under the audited security
contract, with no ranked view mixing leagues, and with the storage projection
for the chosen scope inside the instance budget.

## Table-by-table remarks (migration 001)

### Where the EuroLeague tables are better than the Israeli originals

Keep these; they are not deviations to justify, they are fixes.

- **Composite same-game foreign keys.** `action_lineups`, `pws`, and `stints`
  reference `(game_id, lineup_id)` and `(game_id, stint_id)` rather than the bare
  surrogate. This makes the Israeli `segment_id`-repeats-across-games pitfall
  structurally impossible instead of a documented convention.
- **Half-open stint intervals** with `end_event_order_exclusive` and a CHECK.
  This is the Israeli half-open convention without the ETL hack of advancing the
  final range's upper bound by one to keep the last action attributable.
- **Fail-closed publication in the schema itself.** `stints.publishable` requires
  `lineup_structure_valid AND qa_status = 'clear'`, and
  `game_qa.publication_status = 'clear'` requires all four gates. The Israeli side
  enforces this only in ETL code.
- **`full_rosters.roster_source = 'pbp_recovered'`** encodes the Israeli game
  62461 lesson (a PBP participant missing from the box-score roster) as a column
  from day one.
- **Generated `difference`/`matches`** on `reconciliation_metrics`, and the
  deferrable self-referencing parent FK on `actions_clean`.

### Table design issues to settle

1. **Lineup identity is per game, not per season.** `lineups` is
   `UNIQUE (game_id, team_id, lineup_hash)` with a per-row surrogate, so the same
   five players across 30 games are 30 rows (5,573 lineups / 27,865 members per
   100 games). The Israeli model scopes lineup identity to the season
   (`lineups_lookup_on`, `sub_lineups` keyed on `team_id`, `lineup_hash`,
   `game_year`), which is what makes any season-level lineup question answerable.
   Never aggregate EuroLeague lineups on `lineup_id`; aggregate on
   `(team_id, season, lineup_hash)`. If season lineup combos ever enter scope,
   add that season-scoped identity rather than grouping surrogates after the
   fact.
2. **`lineup_hash` hashes provider player IDs.** That ties a persisted key to the
   least stable identifier in the system: when a provider ID is re-minted next
   season, the same human five hash differently and lineup history splits
   silently. Hash the internal `player_id` (or the future `identity_id`) instead.
   Cheap now, expensive after the hashes are persisted keys — this is the same
   identity problem seen from the table side.
3. **`decision_trace jsonb` on every `actions_clean` row.** Together with
   `actions_raw.raw_event jsonb` that is two JSON documents per event, ~113k
   documents per 100 games, and it is very likely the largest object in the
   schema at season scale. `raw_event` is immutable evidence and should stay;
   the trace is a debugging artifact that also exists in the stage checkpoints.
   Restrict it to rows where `grouping_status <> 'confirmed'`, or keep it out of
   the database entirely.
4. **`action_lineups` is home/away while everything downstream is
   team-perspective.** Migration 002 already expands it to offense/defense with a
   `CROSS JOIN LATERAL`. Publish that expansion once as a view and make it the
   only place the mapping exists, or a later query will re-derive home/away →
   offense/defense slightly differently. The Israeli central fact solved this by
   mirroring each event into both perspectives at the fact level.
5. **`pws` cannot represent an unattributable possession.** Both lineup IDs and
   both stint IDs are `NOT NULL`, so a possession whose endpoint has no valid
   package lineup fails the load rather than being persisted and flagged. The
   Israeli project publishes such rows and reports them (its `Q`-class defects).
   Fail-closed is the better default here, but document it: when it fires it will
   look like a loader bug, and the correct response is the schema-coverage audit.
6. **No publication marker keyed by game.** Publication state lives in
   `game_qa (load_run_id, game_id)`, so "is game X currently published and clean"
   means finding its latest load run. The Israeli project moved to a flat
   `etl_processed_games` marker precisely because inferring publication from
   downstream state was error-prone. Add the equivalent — game ID primary key,
   current load run, parser version, status — and drive incremental "what needs
   reloading" from it.
7. **No persisted event × team-perspective fact.** The Israeli backbone is
   `df_pts_poss_lineups_longer_mv`; every filtered/clutch surface reads it. In
   EuroLeague that expansion exists only inside
   `refresh_player_four_factors_by_game_for_games()`, so any *new* metric means
   re-scanning `actions_raw` and re-deriving perspective. Decide deliberately
   whether that fact gets materialized before more metrics are added — it is the
   difference between one scan per load and one scan per metric.
8. **Player box scores live in `full_rosters.boxscore_stats jsonb`.** Traditional
   player stats (the Israeli Tab 5) would need JSONB unnesting. If that surface
   enters the common subset, promote the columns out of JSON first.
9. **No shot relation.** `source_artifacts.artifact_type` allows `'shots'` but
   nothing stores them. If corner-3 / shot profile is wanted, design it as the
   Israeli `shot_zones` is designed: persistent, per-shot, and explicitly never
   part of any purge set.
10. **No correction layer.** High-severity item 4 asks for "a documented
    correction layer rather than editing raw events", but there is no analogue of
    `player_id_aliases`, `player_id_game_overrides`, or `player_identity_map`.
    `qa_incidents` records that something is wrong; nothing records the decided
    correction.
11. **Indexes built ahead of queries.** `actions_raw` carries three secondary
    indexes and `action_lineups` three, on the two largest tables, and the
    current analytics path joins on `(game_id, source_event_order)` — the primary
    key. `euroleague_actions_raw_team_type_idx` and the player index have no
    present consumer. Follow the Israeli rule: audit indexes on query-plan
    evidence, and remember that project dropped ~16 MB of redundant lineup
    indexes once it looked.
12. **`schedule.game_id` is a surrogate**, not the provider gamecode — correct,
    since gamecode is unique only within competition and season. The consequence
    for the adapter: EuroLeague and Israeli `game_id` values collide numerically
    while meaning different games, so a game ID must never cross the league
    boundary without its league key.

### Israeli relations with no EuroLeague counterpart

`sub_lineups` / `sub_lineups_stats` (lineup combos), the central event-team fact,
`team_metrics_by_game_mv` / `team_metrics_rolling_mv`, `player_traditional_stats`,
`shot_zones`, the identity trio, `etl_processed_games`, and `app_meta`. Items
6-10 above are the ones worth adding regardless of how much of the product ever
ships for EuroLeague; the rest follow from the common-subset decision in step 6.

## Coexisting with the Israeli schema in the same database

The "no Israeli object was changed" claim holds on inspection: every EuroLeague
statement is `euroleague.`-qualified, `apply_shadow_schema()` refuses DDL
containing `DROP` or a `BASKETBALL.`/`BASKETBALL_TEST.` reference, each write
transaction opens with `BEGIN` followed by
`SET LOCAL search_path TO euroleague, public` (correct — `SET LOCAL` outside a
transaction would be a no-op on this autocommit connection), and
`assert_shadow_schema_compatible()` rejects an unexpected table in the schema.
Table-level isolation is real.

What is *not* isolated is everything below the tables. Treat these as shared
resources, because `basketball_test` and `euroleague` live in one Supabase
instance:

- **Privileges.** The loader connects with `etl/.Renviron` — the Israeli
  write-capable role — on direct port 5432. Isolation is therefore enforced by a
  text guard in Python, not by the server: the same session could write to
  `basketball_test` if a statement asked it to. Before the schema grows past a
  controlled batch, create a dedicated owner role (`euroleague_etl`) with CREATE
  on only the `euroleague` schema and let the server enforce the boundary.
- **The security contract has never covered this schema.** The Israeli DDL
  revokes schema/table/function privileges from `PUBLIC`, `anon`, and
  `authenticated` per schema, and `app_readonly` gets SELECT plus an EXECUTE
  allowlist. `euroleague` was created outside that pass. Run
  `sql/security/audit_app_access.sql` against it now — before any grant exists —
  to confirm nothing was inherited from Supabase defaults, and never add
  `euroleague` to Supabase's Data-API exposed schemas. When step 6 grants
  `app_readonly` access, extend `sql/security/*.sql`,
  `scripts/apply_db_security.R`, and `test-db-security-contracts.R` in the same
  change, and use one policy naming convention from the start — the apply script
  does not remove legacy policy names, which is why `basketball_test` still
  carries duplicates.
- **Size.** One `pg_database_size` covers both schemas, plus the ~30-40 MB the
  Supabase dashboard reports on top. EuroLeague growth spends the Israeli
  project's budget. See the storage section above.
- **Connections and timing.** The app holds pooler connections on 6543
  (`POOL_MAX=3`); EuroLeague publishes on direct 5432, the same port the Israeli
  DDL and backfills use, where the connection limit is small. Keep EuroLeague
  publications out of the nightly `run_etl_full.ps1` window so a long load
  transaction never competes with Israeli DDL or leaves locks behind for it.
- **Backup and restore lifecycle.** Point-in-time restore is per database. Rolling
  the instance back to undo a EuroLeague mistake would roll back Israeli data
  too. Per-game transactions and the checkpoint/audit path are the real recovery
  mechanism here — which is another reason the recovery drill in step 4 matters
  more than it looks. If EuroLeague ever needs its own restore lifecycle, that is
  the trigger to revisit the separate-database decision.

**Cross-schema reads, when the adapter arrives.** Two shapes are possible.
Putting adapter views inside `basketball_test` that read `euroleague.*` creates a
dependency in both directions — a EuroLeague migration's `DROP ... CASCADE` can
reach an Israeli-schema view, and `app_readonly` needs USAGE on `euroleague`
regardless because the functions are `SECURITY INVOKER`. Prefer a third schema
(`analytics_common` or similar) that owns only the adapter views and depends on
both leagues, so neither league's schema depends on the other and the cascade
blast radius stays outside both. That also matches the `AGENTS.md` rule against
joining the EuroLeague schema directly into existing Israeli queries.

**One pre-existing decision to settle first.** The ETL still models
`APP_ENV=prod` as the `basketball` schema, but the catalog audit found no objects
there — only `basketball_test` is populated, and the app hardcodes it. Do not
build a league-aware adapter on top of that unresolved drift; decide whether
`basketball` is retired or populated first, or the adapter will bake a
three-way naming inconsistency in permanently.

## Scope note: how much of the Israeli product this covers

The compatibility layer covers Tab 1 (player on/off) and its four factors. The
Israeli product's other surfaces have no EuroLeague counterpart: lineup combos
(`sub_lineups` / `sub_lineups_stats`, Tab 2), team ratings and team four factors
(Tab 3), game logs (Tab 4), traditional player stats (Tab 5), Compare (Tab 7),
clutch filtering, and shot profile. That is appropriate for a shadow schema, but
step 6's "deliberate common subset" should be named explicitly rather than
discovered during integration. A defensible minimum is schedule, team ratings,
and player on/off. Lineup combos are the expensive one — the Israeli side
generates every 2/3/4-player combination plus a season stats table per team — and
deserve their own decision with their own storage estimate.
