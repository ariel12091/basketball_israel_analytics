# EuroLeague data sub-project

This is an exploratory shadow project for evaluating EuroLeague play-by-play,
lineups, and possessions alongside the Israeli analytics pipeline.

Start with [PROJECT.md](PROJECT.md). It contains the complete handoff: package
assessment, schema recommendation, source differences, implemented possession
rules, live-versus-repository state, 100-game results, warning dispositions,
deployment dependencies, validation commands, and the ordered next-work plan.

Current executable code remains in the repository's established ETL layout:

- `../etl/euroleague/group_events.R`
- `../etl/euroleague/count_possessions.R`
- `../etl/euroleague/evaluate_grouping_sample.R`
- `../etl/euroleague/fixtures/event_grouping_edge_cases.csv`
- `../etl/tests/test_euroleague_*.R`

The typed Python implementation lives inside this sub-project:

- `src/euroleague_possessions/parser.py`
- `src/euroleague_possessions/counter.py`
- `src/euroleague_possessions/models.py`
- `src/euroleague_possessions/concurrency.py`
- `src/euroleague_possessions/pbp_collector.py`
- `src/euroleague_possessions/boxscore_collector.py`
- `src/euroleague_possessions/reconciliation.py`
- `src/euroleague_possessions/package_lineups.py`
- `src/euroleague_possessions/schema_coverage.py`
- `src/euroleague_possessions/load_plan.py`
- `src/euroleague_possessions/transaction_writer.py`
- `src/euroleague_possessions/staging.py`
- `src/euroleague_possessions/postgres_backend.py`
- `src/euroleague_possessions/postgres_trial.py`
- `src/euroleague_possessions/batch_pipeline.py`
- `src/euroleague_possessions/analytics_validation.py`
- `tests/test_parser.py`
- `tests/test_counter.py`
- `tests/test_reconciliation.py`
- `tests/test_package_lineups.py`
- `tests/test_schema_coverage.py`
- `tests/test_load_plan.py`
- `tests/test_transaction_writer.py`
- `tests/test_staging.py`
- `tests/test_postgres_backend.py`
- `tests/test_concurrent_pipeline.py`
- `tests/test_schema_draft.py`
- `sql/001_core_shadow_schema.sql` (applied isolated schema migration)
- `sql/002_existing_analytics_compatibility.sql` (applied additive analytics)
- `sql/003_app_materialized_views.sql` (prepared indexed app read layer; not applied)
- `sql/analytics/player_onoff_ppp_readonly.sql` (read-only worked calculation)
- `scripts/export_r_reference.R`
- `scripts/compare_r_reference.py`
- `scripts/audit_live_batch.py` (read-only checkpoint/database reconciliation)
- `scripts/audit_review_warnings.py` (local warning-context export; no API/DB I/O)

The isolated `euroleague` PostgreSQL schema contains the approved controlled
batch `E/2025/1-3`. All three persisted base and analytics snapshots match
their checkpoints under one completed batch run; the original rollback probe
also passed. A restartable 100-game batch was staged and tested entirely
offline but was not published. No Israeli-schema table or production-app
dependency was changed. Further live game loads require explicit approval;
see `PROJECT.md` for results and release gates.

The app-facing materialized-view migration is implemented in the repository
but has not been applied to the live shadow schema. The ordinary views remain
the always-current semantic layer; eventual app queries should use the indexed
`*_mv` relations after the migration is reviewed and applied.
