# EuroLeague sub-project instructions

These instructions supplement and inherit the repository-root `AGENTS.md`.
The root rules still apply unless this file explicitly adds a EuroLeague-specific
rule. Read `euroleague/PROJECT.md` before changing this sub-project or the
related implementation in `etl/euroleague/`.

## Scope and isolation

- Treat EuroLeague data work as an isolated project even though the approved
  EuroLeague read layer is now used by two league-scoped Shiny tabs.
- As of 2026-08-09, the isolated `euroleague` schema contains
  `E/2025/1-84` under completed `load_run_id=4`. Migrations 010-012 are applied
  and the obsolete normalized middle tables have been removed. Further live
  loads require explicit approval.
- Do not write EuroLeague data into the Israeli `basketball` or
  `basketball_test` schemas.
- Keep the database target as the separate `euroleague` schema in the existing
  PostgreSQL instance, not a separate database.
- Do not broaden the existing EuroLeague app integration unless the user
  explicitly requests it.
- Preserve raw provider fields and provenance. Derived fields must be additive
  and reproducible.

## Israeli-schema guidance

- Use the Israeli pipeline's basketball grains as the design reference:
  schedule, roster, canonical event, possession endpoint, lineup exposure,
  two team perspectives, and additive player/team game facts.
- Do not require identical provider columns or preserve historical duplication
  merely for naming parity. A EuroLeague deviation must have a documented
  source, integrity, storage, or queryability benefit.
- Prefer familiar relation names where the basketball grain is materially the
  same. Document grain differences explicitly when reusing a familiar name.
- The approved EuroLeague physical model is immutable package evidence in
  `actions_raw`, one typed canonical `actions` row per PBP event, and the
  actions-derived `action_team_context_actions` and
  `matchup_segments_actions` consumer facts. Do not restore `actions_clean`,
  `possessions`, `lineups`, `lineup_players`, `action_lineups`, `stints`,
  `pws`, `action_team_context`, or `matchup_segments` merely for Israeli naming
  parity.
- Only box-score/PBP actors that resolve to a named roster player receive a
  `players`/`full_rosters` foreign key. Preserve coach, bench, and other
  pseudo-actor IDs in `actions_raw.provider_player_id` with `player_id` null.
- Run the database-free schema-coverage audit before a load. Missing named
  roster players, ambiguous lineup names, invalid action keys, unresolved
  possession teams, or missing endpoint lineups are blocking issues.
- Compatibility with the Israeli project means comparable layers, grains,
  lineage, and basketball metric semantics. It does not require identical
  physical columns, implementation languages, row eligibility, rounding, or
  output parity. Validate each provider against its own source and invariants.
- Do not make Israeli-result parity a EuroLeague publication gate. A future
  cross-league adapter should expose only the deliberate common subset and
  retain league-specific fields below or alongside it.

## Python

- Python is allowed and preferred for API extraction and direct use of
  `euroleague-api`.
- Use a project-local virtual environment such as `euroleague/.venv`; never
  install project dependencies into the global Python environment.
- Pin direct dependencies in `euroleague/pyproject.toml` and keep the extraction
  pin visible in `euroleague/requirements.txt`. Record package
  version, competition, season, retrieval time, and source endpoint for every
  persisted load.
- Treat `euroleague-api` as the extraction adapter and the baseline lineup
  constructor, not as the database. Persist its source/version metadata and
  validation output rather than recreating capabilities it already provides.
- API collectors must be restartable: fetch per game, checkpoint completed
  games, throttle requests, use bounded retries/backoff, and report failures.
- Bounded Python concurrency is allowed for independent per-game extraction
  and transformation. Respect provider rate limits, coordinate cooldown after
  throttling responses, keep each game's result isolated, and publish results
  in a deterministic order. Use measured defaults: staging currently defaults
  to one worker because threads were slower for that local workload. Do not let
  concurrency weaken per-game transactions or reproducible output.
- Do not commit downloaded full-game or full-season datasets. Store exploratory
  extracts in `C:\tmp` or a configured ignored data directory.
- Never store credentials, tokens, or connection strings in Python source or
  committed environment files.

## Package-first rule

- Before writing an endpoint client or source transformation, check whether
  the pinned `euroleague-api` package already provides that capability.
- Use package methods for schedules, play-by-play, box scores, shots,
  standings, common statistics, source cleanup, event ordering, starters, and
  reconstructed lineups.
- Add custom code only for capabilities the package does not provide or cannot
  provide safely for this project: checkpointing, immutable persistence,
  throttling/retries, provenance, deterministic possession logic,
  reconciliation, QA, schema mapping, and application-specific outputs.
- A reliability wrapper may call the same official endpoint directly when the
  package cannot expose the raw response or support restartable collection.
  Document the reason and do not duplicate the package's basketball semantics.
- Package lineups are the baseline. Preserve `Lineup_A`, `Lineup_B`, package
  version, and `validate_on_court_player`; do not create a second lineup engine
  without a measured failure that requires one.

## Python and R deterministic transformations

- The typed Python parser in `euroleague/src/euroleague_possessions/` is the
  EuroLeague canonical implementation. The R implementation remains an
  independent regression reference.
- Use `C:\Program Files\R\R-4.4.2\bin\Rscript.exe`, as required by the root
  project.
- Keep both transformation implementations pure and free of database I/O.
- Do not add gamecode-specific exceptions. New behavior must be a general rule
  with a labelled regression fixture and exact Python/R parity unless a
  deliberate divergence is documented.

## EuroLeague event rules

- Primary ordering is `(season, gamecode, period, source_event_order)` derived
  from provider sequence. Never use game clock as an identifier.
- Do not assume provider `NUMBEROFPLAY` is ordered.
- Every normalized event must have a same-game, same-period synthetic parent;
  singleton events parent themselves.
- Keep incident identity, FT-trip identity, and possession endpoints separate.
- A free throw must resolve to exactly one trip or remain explicitly
  `unresolved`; never attach it by clock alone.
- Retain `grouping_status`, confidence, and QA reasons. Never silently coerce a
  provisional or contradictory sequence merely to force team alternation.
- Determine rebound control from the rebound team relative to the shooting team
  before relying on provider `O`/`D` text.

## Lineups and substitutions

- EuroLeague `IN` and `OUT` rows represent the same underlying substitution
  concept as the Israeli data, but they are separate provider rows that must be
  paired by the package and validated before publication.
- The package's `Lineup_A` and `Lineup_B` values are reconstructed from
  box-score starters and substitution rows. Use them as the EuroLeague lineup
  baseline; do not build a second lineup engine unless measured package
  failures establish a concrete need.
- Persist lineup validation results. Invalid lineup cardinality or duplicate
  members are blocking; package-invalid action actors may be retained only with
  explicit QA evidence and must not be silently used to rewrite the lineup.
- The current schema has no `lineup_id`. `lineup_a` and `lineup_b` are
  five-player arrays on `actions`; `segment_id` is game/team-local and is not a
  cross-game lineup identity.

## Transactional loading

- Treat one game's staged snapshot as the unit of transaction and retry.
- Delete replaceable rows child-first, insert the complete snapshot
  parent-first, run database-side validation, and only then commit.
- The direct snapshot relations are `full_rosters`, `team_boxscores`,
  `actions_raw`, `actions`, `reconciliation_metrics`, `game_qa`, and
  `qa_incidents`.
- Delete `action_team_context_actions` and `matchup_segments_actions`
  child-first before replacing their game's canonical `actions`; rebuild both
  inside validation with `refresh_actions_consumer_candidates()`.
- Keep immutable source artifacts and shared dimensions outside destructive
  per-game replacement.
- Do not create the schema or execute a live load without explicit user
  approval.

## PPP and player on/off analytics

- Preserve raw additive points and possession counts; calculate PPP/ratings
  only after aggregating the requested games. Never average stored ratios.
- Use action-level package lineups for scoring exposure and deterministic
  endpoint lineups for possession exposure. A lineup change never creates a
  possession.
- Build each player's ON and OFF rows from the complete game roster, not only
  players present in an on-court lineup.
- Use canonical `actions` as the event and endpoint source,
  `action_team_context_actions` for additive team-perspective event metrics,
  and `matchup_segments_actions` for duration. Do not use `actions_raw` as the
  analytical scoring fact.
- Exclude package aggregate rows with provider IDs `Team` and `Total` from
  normalized player/roster relations; retain them only in raw evidence and
  team-total reconciliation.
- Store offense and defense as separate contexts. Defensive rating is opponent
  points per defensive possession, so lower is better.
- Any 2-5 player unit fact must expand only units actually present in each
  five-player lineup, use resolved internal player IDs for stable keys, and
  store additive counts/seconds rather than pre-aggregated ratios.
- Keep lightweight ordinary views as the live semantic layer. App-facing
  schedule and season aggregates must use indexed materialized views or
  incrementally maintained physical tables with an explicit refresh lifecycle.

## Validation

- Run the Python tests first:

```powershell
& euroleague/.venv/Scripts/python.exe -m unittest discover `
  -s euroleague/tests -v
```

- Run the three EuroLeague R tests after grouping or possession changes:

```powershell
Set-Location etl/tests
& 'C:\Program Files\R\R-4.4.2\bin\Rscript.exe' `
  test_euroleague_event_grouping_fixtures.R
& 'C:\Program Files\R\R-4.4.2\bin\Rscript.exe' `
  test_euroleague_group_events.R
& 'C:\Program Files\R\R-4.4.2\bin\Rscript.exe' `
  test_euroleague_count_possessions.R
```

- Database publication additionally requires box-score and score-progression
  reconciliation plus review of all unresolved and material provisional cases.
- Follow the root `PROJECT.md` Supabase/DDL process before creating or changing
  any database objects.
