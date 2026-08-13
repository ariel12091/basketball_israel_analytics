# EuroLeague data sub-project

This is the isolated EuroLeague data project. Canonical play-by-play events,
package lineups, and possession endpoints live together in `actions`.

Start with [PROJECT.md](PROJECT.md). It is the current consolidated handoff:
ETL flow, schema contract, Israeli comparison, validation evidence, known gaps,
and next-work sequence. `CLAUDE.md` is historical reference only.

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
- `tests/test_raw_event_backfill.py`
- `sql/001_core_shadow_schema.sql` (applied isolated schema migration)
- `sql/002_existing_analytics_compatibility.sql` (applied additive analytics)
- `sql/003_app_materialized_views.sql` (**superseded; never apply**)
- `sql/004_app_read_layer.sql` (applied app read layer)
- `sql/005_team_ratings.sql` (applied team ratings)
- `sql/006_team_four_factors.sql` (applied team four factors)
- `sql/007_four_factor_refresh_performance.sql` (applied refresh optimization)
- `sql/008_action_team_context.sql` (applied historical fact migration)
- `sql/009_consumers_read_the_fact.sql` (applied historical consumer migration)
- `sql/010_canonical_actions.sql` (canonical columnar event table)
- `sql/011_actions_consumer_candidates.sql` (actions-derived consumer facts)
- `sql/012_actions_consumer_cutover.sql` (applied simplified-schema cutover)
- `sql/021_player_traditional_stats.sql` (applied Player Stats read layer)
- `sql/022_default_clutch_player_stats.sql` (applied standard-clutch player cache)
- `sql/023_player_stats_standard_clutch_fast_path.sql` (cache-only standard-clutch reader)
- `sql/024_player_stats_clutch_source_selector.sql` (shared cached/custom clutch reader)
- `sql/025_custom_clutch_action_segment_minutes.sql` (shared fast custom-clutch duration)
- `sql/026_player_stats_single_action_scan.sql` (Israeli-shaped single-scan custom reader)
- `sql/027_player_stats_action_fact.sql` (narrow incremental Player Stats action fact)
- `sql/028_player_stats_refresh_lineage.sql` (pending canonical-source refresh repair)
- `sql/029_lineup_filter_before_expand.sql` (pending verified Lineups query-order fix)
- `sql/analytics/player_onoff_ppp_readonly.sql` (read-only worked calculation)
- `scripts/export_r_reference.R`
- `scripts/compare_r_reference.py`
- `scripts/audit_live_batch.py` (read-only checkpoint/database reconciliation)
- `scripts/audit_review_warnings.py` (local warning-context export; no API/DB I/O)
- `scripts/verify_actions_schema.py` (raw/canonical/fact verification)
- `scripts/apply_012_actions_cutover.py` (migration-012-only guarded entry point)

The isolated live schema contains `E/2025/1-84`. Migration 012 removed the
obsolete normalized lineup, stint, possession, and bridge tables after exact
fact and app-output parity. See `PROJECT.md` and `RUNBOOK.md` for the current
relation contract and load procedure.

The app now includes player on/off, team ratings, 2-5 player lineup units,
game logs, and a shared Player Stats implementation. Migrations 021-027 are
applied to the isolated EuroLeague schema. Migration 027 is a narrow private
action-grain fact for interactive custom-clutch Player Stats and is refreshed
per changed game; it does not store precomputed percentages or basketball
aggregates. The custom reader follows the Israeli `stats FROM acts` shape and
does not repeat roster membership resolution for every filtered action.
Traditional counts come from the official box score; TS% and USG% use the
canonical PBP free-throw-trip, turnover, possession, and lineup facts.
