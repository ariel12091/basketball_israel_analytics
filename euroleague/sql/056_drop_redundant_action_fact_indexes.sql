-- EuroLeague shadow schema: migration 056 -- drop two redundant covering indexes.
--
-- euroleague_player_stats_actions_lineups_idx has the same 8-column key as
-- both indexes below and INCLUDEs every column _team_idx carries, so it can
-- serve every scan they served. Measured 2026-09-23 with
-- scripts/benchmark_psa_index_drop.py (9 reps, variant order rotated, drops
-- inside rolled-back transactions): every app reader and both load-time
-- refreshes returned identical results and stayed within 30 ms of baseline.
-- Saves 104 MB. _minutes_idx was tested too and is KEPT: without it broad
-- custom Team Ratings went 1.2s -> 2.9s.
--
-- Supersedes the 2026-08-24 decision to retain _team_idx (Team Ratings
-- 0.90s vs 1.04s then; 0.35s either way after the index compaction).
-- Recreate from migrations 027 (_filter_idx) and 032 (_team_idx) if needed.
--
-- CONCURRENTLY: each statement runs on its own, outside a transaction.
-- Applied by scripts/apply_056_drop_redundant_action_fact_indexes.py.

DROP INDEX CONCURRENTLY IF EXISTS euroleague.euroleague_player_stats_actions_team_idx;

DROP INDEX CONCURRENTLY IF EXISTS euroleague.euroleague_player_stats_actions_filter_idx;
