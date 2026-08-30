-- EUROLEAGUE SHADOW SCHEMA -- migration 047: drop orphaned functions and views.
--
-- Destructive but reference-free. Every object below was verified to have zero
-- referrers on 2026-08-30 across: euroleague views and materialized views,
-- euroleague function bodies, app/R, etl/, scripts/, frontend-v2/server,
-- euroleague/src, euroleague/scripts and euroleague/tests.
--
-- NOTE: this migration contains DROP, so apply_shadow_schema() refuses it by
-- design. Use scripts/apply_047_drop_orphans.py, which re-verifies that each
-- target is unreferenced immediately before dropping and rolls back unless
-- --apply is passed.
--
-- Audit: docs/sql_function_history_and_risk_2026-08-30.md
--
-- DELIBERATELY NOT DROPPED: euroleague.player_game_context. An earlier draft of
-- the audit listed it as orphaned. That was wrong -- scripts/load_games.py
-- reads it for the published-game QA check that cross-validates team-grain four
-- factors against the player-grain fact divided by five. It stays.

BEGIN;
SET LOCAL search_path TO euroleague, public;

-- 1. Player Stats clutch dispatcher, superseded on 2026-08-13.
--
-- Migration 024 introduced get_player_traditional_clutch to choose between the
-- standard and custom clutch readers inside SQL, with select_player_clutch_counts
-- as its helper. Migration 026 plus the R-side clutch_reader_kind() (helpers.R)
-- moved that decision into the application, which now names the standard or
-- custom reader directly. The dispatcher has had no caller since.
DROP FUNCTION IF EXISTS euroleague.get_player_traditional_clutch(
  text, integer, date, date, text, text, text, text, text, text, integer, text,
  integer, text, integer, boolean, integer, integer, integer);

DROP FUNCTION IF EXISTS euroleague.select_player_clutch_counts(
  bigint[], integer, text, integer, boolean);

-- 2. Superseded traditional-stats reader, orphaned the same day.
--
-- Migration 021 created get_player_traditional_dynamic. Migrations 023, 024 and
-- 026 replaced it with the pergame / standard_clutch / custom_clutch trio that
-- Tab 5's EuroLeague branch selects between. Nothing has called it since.
--
-- It also shares a name with basketball_test.get_player_traditional_dynamic,
-- which IS live (Tab 7 Compare) and takes 18 arguments rather than 19. Removing
-- the EuroLeague copy removes that collision.
--
-- Known consequence: scripts/apply_042_player_traditional_pergame.py benchmarks
-- this function and can no longer be re-run as-is. It is a historical
-- applicator whose migration is already applied; that is accepted.
DROP FUNCTION IF EXISTS euroleague.get_player_traditional_dynamic(
  text, integer, date, date, text, text, text, text, text, text, integer, text,
  integer, text, integer, boolean, integer, integer, integer);

-- 3. Migration 002 season views, superseded by the app-facing materialized views.
--
-- player_onoff_by_season and player_four_factors_by_season were the original
-- season aggregates. The app reads player_onoff_default_mv and
-- player_advanced_stats_mv instead. No view, function, script or test refers to
-- either. RESTRICT (the default) is used deliberately: if anything does depend
-- on them, this fails loudly rather than cascading.
DROP VIEW IF EXISTS euroleague.player_onoff_by_season;
DROP VIEW IF EXISTS euroleague.player_four_factors_by_season;

COMMIT;
