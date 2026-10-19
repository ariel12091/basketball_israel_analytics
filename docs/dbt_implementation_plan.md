# dbt Implementation Plan for the ETL Pipeline

## 1. Objective

Improve database change tracking first, then introduce dbt where its lineage,
contracts, and transformation testing provide enough value to justify the
additional runtime and maintenance burden. A full dbt migration is not required
to meet the immediate schema-governance goal.

The target split of responsibilities is:

- **R remains responsible for extraction and loading:** provider API calls,
  JSON normalization, roster recovery, player-ID corrections, game-level
  transactions, sub-lineup combination generation, Parquet cold storage, and
  operational publication metadata.
- **Versioned migrations become the schema source of truth:** tables, columns,
  functions, indexes, materialized views, grants, RLS policies, and deployment
  history.
- **Catalog snapshots detect drift:** the repository records a normalized
  representation of the expected live PostgreSQL schema and CI compares it
  with migration output or an isolated database.
- **dbt is initially optional and narrow:** source declarations,
  transformation documentation, lineage, model contracts, and SQL data-quality
  tests. It becomes responsible for production transformations only after an
  explicit value and operational-readiness decision.
- **PostgreSQL functions remain temporarily as compatibility surfaces:** app
  reader functions and complex incremental refresh functions are migrated only
  after dbt models have demonstrated parity and acceptable performance.

dbt is not the schema migration tool for this project. It does not replace the
need to version and deploy PostgreSQL functions, indexes, grants, RLS policies,
or general table alterations. This is an incremental adoption, not a big-bang
rewrite.

## 2. Current pipeline and proposed boundary

The current orchestrator is `etl/etl_full.R`:

1. Setup, connection, and identity synchronization.
2. Per-game extraction, transformation, and base-table loading.
3. Sub-lineup identity generation.
4. Materialized-view and incremental-table refreshes.
5. Sub-lineup-stat refresh.
6. Validation and publication to `etl_processed_games`.
7. Parquet cold storage and hot-table purge.

The immediate work should add migration history and schema-drift detection
without changing this runtime. If dbt is later promoted into the production
path, its first safe boundary is after current Phase 3 and before publication.
It should replace transformations in Phases 4 and 5 one family at a time and
add tests alongside Phase 6. The R orchestrator must continue to decide which
games are eligible, invoke the transformation step, publish only successful
games, and perform cold storage only after the complete pipeline passes.

```text
Provider APIs
    |
    v
R extract/load (one transaction per game)
    |
    v
PostgreSQL source tables + sub-lineup identities
    |
    v
Existing SQL refreshes, or optional dbt production graph after approval
    |
    v
R operational validation and publication
    |
    v
Parquet export and coordinated hot-table purge
```

## 3. Immediate priority: schema governance

Schema governance should be implemented whether or not dbt is adopted. The
repository already has `sql/migrations/`, but it needs a single enforceable
workflow rather than relying on scattered DDL in R and deployment scripts.

### Migration source of truth

- Store every forward database change in a uniquely numbered, immutable file
  under `sql/migrations/`.
- Use a name such as `NNNN_description.sql`, with one number applied exactly
  once and no editing of migrations already deployed outside development.
- Include all relevant PostgreSQL objects: schemas, tables, columns,
  constraints, indexes, functions and overloads, views/materialized views,
  owners, grants, and RLS policies.
- Keep data backfills explicit and separate from structural DDL when either can
  run for a long time. Document lock and transaction expectations.
- Add a database table such as `schema_migrations` containing migration ID,
  checksum, applied timestamp, duration, environment, and deployment identity.
- Add an R or PowerShell migration runner using the configured direct DDL port.
  It must acquire an advisory lock, verify checksums, apply pending migrations
  in order, fail on drift, and record success only after commit.
- Do not place secrets in migration files, logs, or repository configuration.

Existing SQL files are not automatically historical migrations. First create a
reviewed baseline representing the current accepted schema, then require all
new changes to be incremental migrations. Keep reusable function/view model SQL
in its current logical directories if helpful, but make the migration file the
auditable deployment event that installs that version.

### Normalized catalog and drift detection

Add a deterministic catalog-export command that records at least:

- Relation names, schemas, owners, and actual relation types (`relkind`).
- Columns, ordinal positions, PostgreSQL types, nullability, defaults, identity,
  and generated expressions.
- Primary, unique, foreign-key, and check constraints.
- Index definitions and predicates.
- View and materialized-view definitions and dependencies.
- Function signatures, result types, volatility/security attributes, and
  normalized definitions.
- Grants, default privileges, RLS enabled/forced flags, and policies.

Normalize unstable values such as object IDs, timestamps, whitespace, and
server-generated ordering before committing the snapshot. Never include role
passwords or connection values. Store the accepted snapshot under a path such
as `docs/schema/expected_catalog.json` and produce a human-readable Markdown
summary for reviews.

CI should build or migrate an isolated PostgreSQL schema/database from the
baseline, export its catalog, and fail when it differs from the committed
expected catalog. A read-only live audit may report production drift, but pull
request CI must not change the live database. Intentional differences between
test and production schemas must be maintained as explicit allowlisted
exceptions with reasons.

### Schema-change workflow

For each change:

1. Add a forward migration and, where practical, a documented rollback or
   compatibility procedure.
2. Update reusable SQL definitions and database documentation.
3. Apply migrations to an isolated/test database from a clean baseline.
4. Export and review the normalized catalog diff.
5. Run structural, data-quality, app-contract, and security tests.
6. Review query plans and locks for performance-sensitive changes.
7. Deploy through the migration runner, then perform a read-only live drift
   audit.
8. Update `docs/database_context.md` when grain, ownership, refresh order, or
   application contracts change.

Schema governance is complete when a developer can answer: what changed, which
migration deployed it, when and where it was applied, whether live state
matches the repository, and how the application remains compatible.

## 4. Decision gate: is dbt needed?

Do not make dbt a production dependency merely to track DDL. Proceed beyond a
small evaluation only if at least one of these conditions is true:

- The analytical transformation graph is continuing to grow and dependency
  ordering is difficult to understand or maintain.
- Multiple contributors need generated lineage, shared model documentation,
  and consistent SQL review conventions.
- Model-level contracts and reusable generic data tests would materially reduce
  regressions beyond the existing R/database tests.
- New transformations can be authored more safely as `ref()`-based models than
  as refresh functions and manually ordered SQL.
- A measured proof of concept shows acceptable runtime, locking, incremental
  behavior, and operational complexity.

Keep dbt documentation/test-only, or stop the adoption, if migrations plus the
catalog audit solve the main problem and production model migration has no
clear measured benefit. This decision should be recorded after the first shadow
model proof of concept.

## 5. Recommended dbt project structure

Create a dbt project at `dbt/` while keeping the existing `etl/`, `sql/`, and
`app/` layouts intact during migration.

```text
dbt/
  dbt_project.yml
  packages.yml
  models/
    staging/
      _sources.yml
      _staging_models.yml
      stg_schedule.sql
      stg_actions_clean.sql
      stg_possessions.sql
      stg_pws.sql
      stg_lineups_lookup.sql
      stg_sub_lineups.sql
    intermediate/
      _intermediate_models.yml
      int_final_schedule.sql
      int_event_team_fact.sql
      int_lineup_totals_by_game.sql
    marts/
      core/
      players/
      lineups/
      teams/
      _marts_models.yml
  macros/
    canonical_clock.sql
    game_scope.sql
    grants.sql
  tests/
    assertions/
  seeds/
  snapshots/                 # only if a future slowly-changing dimension needs it
```

Use schemas separated by purpose during development:

- Existing `basketball_test`: production-compatible sources and app-facing
  relations.
- A developer/CI schema such as `dbt_<user>`: isolated dbt builds.
- An optional `basketball_dbt_shadow`: shared parity-testing environment.

Do not commit credentials. Define a local/CI `profiles.yml` outside the repo and
map its connection fields to the existing `PG_HOST`, `PG_PORT`, `PG_DB`,
`PG_USER`, `PG_PASS`, and `PG_SSLMODE` environment variables. Use the direct DDL
port when dbt must create or alter relations; do not assume the transaction
pooler is suitable for DDL.

## 6. Model design rules

### Sources and staging

Declare R-owned relations as dbt sources, beginning with `schedule`,
`actions_clean`, `full_rosters`, `possessions`, `pws`, `lineups_lookup`,
`lineups_lookup_on`, `sub_lineups`, player-identity relations, and operational
metadata. Staging models should:

- Rename or cast fields only where a stable contract requires it.
- Preserve raw provider clock and score fields for auditability.
- Normalize known mixed-case provider columns explicitly.
- Avoid business aggregation.
- Document source ownership as `R ETL`; dbt must never truncate or rebuild these
  relations.

Because the hot tables are purged after Parquet export, ordinary dbt source
freshness and full-history reconstruction from `actions_clean`, `possessions`,
or `pws` are not sufficient. Incremental dbt models must persist their own
historical result and process touched games before Phase 7.

### Intermediate and mart models

Each model must declare its grain and unique key in YAML. Calculate additive
counts and canonical duration at the lowest correct grain, then calculate PPP,
rates, percentages, and ranks in downstream models.

Recommended dependency order:

1. Final schedule and event-team fact.
2. Lineup/game, player/game, and team/game additive facts.
3. Season/team/player and rolling aggregates.
4. App-facing reporting relations.

Preserve the existing canonical-clock rules, including half-open lineup ranges,
real action IDs as persisted endpoints, duration deduplication at segment grain,
and the planner safeguard currently required by the clock refresh.

### Materializations

- Use `view` for lightweight staging projections.
- Use `incremental` for durable game-grain facts currently maintained by
  `refresh_*_for_games(int4[])` functions.
- Use `table` for small, rebuilt reporting datasets where replacement is cheap.
- Use `materialized_view` only after verifying dbt-postgres support and refresh
  behavior against the installed dbt adapter version. Until then, retain the
  existing PostgreSQL materialized views or model them as tables.

Incremental models should accept a run variable such as
`--vars '{game_ids: [123, 456]}'`, delete/replace all rows for those games, and
use an explicit unique key. An empty game scope must fail safely or perform no
work; it must never silently become a full rebuild. Full refreshes require a
separate, explicit operational command.

## 7. Phased delivery

### Phase 0 - Establish schema governance

Deliverables:

- Inventory current migration and DDL entry points.
- Create and review the accepted baseline migration/catalog.
- Add `schema_migrations` and the checksum-verifying migration runner.
- Add deterministic catalog export and diff commands.
- Add isolated CI migration, structural tests, and security audit.
- Document the schema-change and production deployment workflow.

Exit criteria:

- A clean isolated database can reproduce the accepted schema.
- Intentional schema changes create a readable catalog diff.
- Unrecorded live or test-schema drift is detected.
- This phase delivers value without installing or running dbt.

### Phase 1 - dbt baseline and go/no-go decisions

Deliverables:

- Pin supported Python, `dbt-core`, and `dbt-postgres` versions.
- Inventory every Phase 4/5 relation with its real PostgreSQL type, grain,
  unique key, dependencies, row count, size, owner, grants, RLS policy, and app
  consumer.
- Capture representative outputs for at least one regular-season game, one cup
  game, one overtime game, and a multi-game incremental run.
- Record current transformation duration, lock behavior, and app-query p50/p90.
- Decide whether the production dbt runner uses the existing ETL database role
  or a narrower dedicated transform role.

Exit criteria:

- The live relation inventory agrees with `docs/database_context.md`.
- Rollback and backup procedures are written and tested in the test schema.
- No production relation has been changed.

### Phase 2 - Scaffold dbt and establish contracts

Deliverables:

- Add the `dbt/` project, environment-driven profile example, `.gitignore`
  entries for dbt artifacts, and commands for PowerShell/CI.
- Declare R-owned sources and create thin staging views in an isolated schema.
- Add model descriptions, column descriptions for keys and metric fields, and
  ownership metadata.
- Add foundational tests: not-null keys, unique composite keys, accepted values
  for lineup perspective and verdict fields, and relationships to schedule,
  teams, players, and games.

Exit criteria:

- `dbt deps`, `dbt parse`, `dbt build --select staging`, and `dbt docs generate`
  pass from a clean environment.
- dbt cannot write to or replace R-owned source tables.

### Phase 3 - Shadow the lowest-risk derived models

Start with small, well-bounded relations rather than the central event fact.
Recommended first candidates are the final schedule model and a game-level team
or lineup aggregate whose SQL already has a documented key.

Deliverables:

- Build shadow models under new names; do not point the app at them.
- Convert existing SQL to `source()`/`ref()` dependencies without changing
  formulas.
- Add singular parity tests comparing legacy and dbt results for row sets,
  nulls, additive counts, scores, possessions, and canonical minutes.
- Benchmark complete fetches and cold/warm database execution.

Exit criteria:

- Exact parity for identifiers and additive fields.
- Numeric differences are zero or have a documented, approved tolerance.
- No material regression in refresh time, app latency, or lock duration.
- Record a go/no-go decision. A no-go result keeps the migration/catalog system
  and may retain dbt only for documentation and tests.

### Phase 4 - Migrate durable game-grain facts

Migrate one fact family at a time:

1. `team_metrics_by_game_mv` (despite its historical `_mv` name, it is a
   physical incremental table).
2. `player_four_factors_by_game` and player traditional/default-clutch facts.
3. `df_pts_poss_lineups_longer_mv` last, because it is the central event-team
   fact and carries canonical-clock risk.

Deliverables for each family:

- A dbt incremental model scoped by touched `game_ids`.
- Unique-key, referential-integrity, and no-duplicate tests.
- Domain-specific assertions for score symmetry, possession counts, lineup
  coverage, and canonical minutes.
- A backfill command that runs independently from the daily incremental path.
- A compatibility view or unchanged relation name for current app readers.

Exit criteria for each family:

- At least several scheduled shadow runs complete successfully.
- Targeted reprocessing is idempotent: running the same game IDs twice produces
  the same rows.
- A deliberately failing test prevents publication and cold-table purge.
- The legacy refresh function remains available for immediate rollback.

### Phase 5 - Migrate season, rolling, and app-facing aggregates

Migrate downstream aggregates only after their game-grain parents are stable.
Candidates include lineup totals, team PPP/four-factor marts, player on/off and
advanced-stat marts, rolling team metrics, traditional season stats, and
sub-lineup stats.

Keep PostgreSQL reader functions used by Shiny in place. Change their underlying
relations only when column names, types, null behavior, keys, grants, and query
plans are compatible.

Exit criteria:

- All default app surfaces match legacy outputs for representative filter
  combinations.
- Existing application database tests and security-contract tests pass.
- App-role grants and RLS policies are applied and audited after dbt relation
  replacement.

### Phase 6 - Integrate dbt into `etl_full()`

Add a small R wrapper that invokes a pinned dbt executable and captures its
structured run artifacts. The orchestration should become:

1. Run current R Phases 1-3.
2. Write the touched game IDs to the dbt invocation as a JSON/YAML variable.
3. Run `dbt build` for the production selector in dependency order.
4. Treat any dbt model or test failure as a pipeline failure.
5. Run remaining operational R validations.
6. Publish only clean game IDs to `etl_processed_games`.
7. Export/purge hot tables and update `app_meta` only after success.

Update `scripts/run_etl_full.ps1` to activate the pinned dbt environment and log
the dbt invocation, exit code, and artifact paths. Preserve the existing lock
file so two pipelines cannot overlap.

Exit criteria:

- Dry run reports the dbt selector and game scope without changing data.
- Nonzero dbt exit status stops publication and Phase 7.
- `run_results.json` and `manifest.json` are retained with the ETL log for a
  practical troubleshooting period, without retaining secrets.
- A targeted `game_ids` run and the daily auto-detected run both pass end to
  end.

### Phase 7 - Cutover and cleanup

Deliverables:

- Switch one relation family at a time to dbt ownership.
- Observe multiple successful scheduled production runs before removing its
  legacy refresh call.
- Remove superseded SQL only after rollback is no longer needed; retain migration
  history and compatibility readers.
- Update `PROJECT.md`, `docs/database_context.md`, the deployment runbook, and
  the database source index with the new ownership boundaries.

Exit criteria:

- dbt owns the agreed transformation graph in production.
- R owns ingestion, publication, and cold storage with no duplicated refreshes.
- The security audit, data-quality report, app tests, and operational recovery
  exercise all pass.

## 8. Test strategy

Use three layers of checks:

1. **dbt generic tests:** unique, not-null, relationships, accepted values, and
   explicit column contracts.
2. **dbt singular assertions:** basketball invariants such as team-minute
   budgets, score symmetry, one duration per segment, valid ON lineups, and no
   orphaned game/player/team identifiers.
3. **Existing R/database acceptance tests:** end-to-end output parity, Shiny
   reader contracts, query-count limits, security grants/RLS, and the full data
   quality report.

Tests must be non-vacuous: first assert that the selected fixture/game exists
and returns rows, then compare the complete keyed result. A test passing because
both sides are empty is a failure of the test design.

Suggested CI commands:

```powershell
dbt deps --project-dir dbt
dbt parse --project-dir dbt
dbt build --project-dir dbt --target ci
& 'C:/Program Files/R/R-4.4.2/bin/Rscript.exe' -e "testthat::test_dir('app/tests/testthat')"
```

Database-backed tests should use an isolated schema and a controlled fixture or
snapshot. They must not target the live app schema from pull-request CI.

## 9. Deployment and rollback

Deployment should remain additive:

1. Build the new dbt model under a shadow name/schema.
2. Test data parity, security, and performance.
3. Pause the scheduler or hold its existing lock for the short cutover window.
4. Apply app-role grants and RLS policies explicitly.
5. Change the compatibility relation or reader to the new model.
6. Run smoke tests and resume the scheduler.

Rollback is per model family: point the compatibility surface back to the
legacy relation and restore the legacy refresh call in `etl_full()`. Avoid
`DROP ... CASCADE`; inventory dependencies before any rename or replacement.

## 10. Risks and mitigations

| Risk | Mitigation |
|---|---|
| Hot source tables are purged | Run dbt before cold storage; persist historical incremental facts; prohibit accidental full rebuild from incomplete hot sources. |
| dbt incremental semantics differ from current delete/reinsert functions | Pass explicit game IDs, use deterministic game-level replacement, and prove idempotency. |
| Existing `_mv` names hide tables, views, and materialized views | Inventory `relkind`; choose materialization from actual type and behavior, not the suffix. |
| Canonical-clock SQL produces slow plans | Migrate it last, preserve the planner guard, and compare `EXPLAIN (ANALYZE, BUFFERS, SETTINGS)`. |
| dbt relation replacement loses grants or RLS | Implement explicit post-build grants/policies and run `scripts/audit_db_security.R`. |
| App functions depend on exact schemas | Retain compatibility names; add contract and complete-result parity tests before cutover. |
| A test failure occurs after base games commit | Do not publish those games or purge their hot rows; allow the next targeted run to rebuild downstream results. |
| Full refresh causes locks or incomplete history | Make full refresh a separately approved runbook operation using restored/complete sources and maintenance-window controls. |
| dbt is treated as a migration manager | Keep all general DDL, functions, indexes, grants, and RLS in the versioned migration workflow. |
| Catalog snapshots contain noisy or sensitive data | Normalize unstable metadata, export definitions rather than data, and exclude credentials and secret settings. |
| Existing deployed schema cannot be reproduced cleanly | Create a reviewed baseline, test it in isolation, and record explicit legacy exceptions rather than silently accepting drift. |

## 11. Initial backlog

The first implementation iteration should be limited to schema governance:

1. Inventory DDL entry points and existing migration state.
2. Define the immutable migration naming, checksum, and deployment policy.
3. Produce and review the baseline migration/catalog from the accepted test
   schema.
4. Implement the migration-history table and advisory-locking runner.
5. Implement normalized catalog export and human-readable diff reporting.
6. Add isolated CI reconstruction plus structural and security checks.
7. Document the mandatory schema-change workflow in `PROJECT.md`.

Only after that iteration succeeds should the dbt evaluation begin:

8. Pin dbt versions and scaffold the project/profile example.
9. Add source declarations and staging contracts for schedule and principal
   R-owned fact inputs.
10. Implement `int_final_schedule` as a shadow model with parity assertions.
11. Measure maintenance value, runtime, locking, and CI cost.
12. Record the dbt go/no-go decision and, if approved, select the first
    game-grain incremental fact based on measured risk and value.

This backlog produces meaningful schema history and drift detection before dbt
is allowed to modify the operational ETL orchestrator.

## 12. Definition of done

The immediate schema-governance effort is complete when:

- Every structural change is represented by an immutable, checksummed migration.
- Applied migrations are queryable by environment and deployment time.
- The accepted schema can be reproduced in an isolated database.
- CI and read-only production audits detect unexplained drift in relations,
  columns, constraints, indexes, functions, grants, and RLS.
- Schema changes update the relevant grain, refresh-order, and app-contract
  documentation.

If the dbt decision is no-go, this is a valid final outcome: the repository has
meaningful schema governance without adding dbt to production.

If dbt is approved, its adoption is complete when:

- Every dbt-owned model has an explicit grain, key, description, owner, tests,
  and materialization rationale.
- Daily and targeted ETL runs pass game scope explicitly and are idempotent.
- Failed dbt builds prevent publication, cold purge, and last-success updates.
- App-facing results, performance, grants, and RLS match the approved baseline.
- Backfill and rollback have been exercised successfully.
- R and dbt have a single, documented ownership boundary with no duplicate
  transformation refreshes.
