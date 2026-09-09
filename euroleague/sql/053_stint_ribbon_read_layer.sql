-- EUROLEAGUE SHADOW SCHEMA -- migration 053: stint ribbon read layer.
--
-- Narrow read layer for the app's stint ribbon (Tab 11 game-log modal).
-- app_readonly is denied on euroleague.actions and
-- euroleague.matchup_segments_actions on purpose: the euro schema keeps raw
-- provider evidence closed and grants only an enumerated read layer. These two
-- views expose exactly what the ribbon needs and nothing else.
--
-- This is a relocation, not a new design: the views were prepared and applied
-- to the live database directly (outside the euroleague/sql/NNN_*.sql
-- convention, as sql/euroleague/ribbon_views.sql) during the stint-ribbon
-- feature. They are reproduced here so a schema rebuild from migrations does
-- not lose them. Applying this migration re-creates the same objects that
-- already exist in the live database (CREATE OR REPLACE VIEW is idempotent
-- here); see scripts/apply_053_stint_ribbon_read_layer.py.
--
-- ribbon_margin_v is the exception to "byte-for-byte": its original
-- definition had a live-data bug (NULL scores before either side's first
-- basket blanked the whole margin curve -- see the comment at its CREATE OR
-- REPLACE VIEW below) found in the 2026-09-05 final review. The corrected
-- definition was applied directly to the live database as an urgent hotfix
-- (the bug affected every EuroLeague game), using this exact DDL, so the file
-- and the live database already agree on the CURRENT (fixed) definition.
-- ribbon_segments_v was not touched and is unchanged.
--
-- Editing either view means DROP + CREATE, which wipes the app_readonly grant.
-- Re-run scripts/apply_db_security.R with CONFIRM_DB_SECURITY_APPLY=1 after.

BEGIN;
SET LOCAL search_path TO euroleague, public;

-- Lanes are keyed on player_id, never on player names.
--
-- matchup_segments_actions stores lineups as text[] of NAMES and carries no
-- ids. Resolving those names one by one against full_rosters would be a
-- name-keyed join, the exact shape that manufactured 258 false findings in the
-- 2026-08-19 data-quality report. Instead this reuses the lineup -> player_id
-- link the on/off system is already built on: lineup_totals_by_game holds
-- own_lineup and player_ids for the same lineup. Measured 2026-09-05: all
-- 22,597 distinct segment lineups match on (game_id, team_id, own_lineup),
-- with zero fan-out.
--
-- Do NOT pair own_lineup and player_ids positionally. The two arrays are
-- sorted independently (names alphabetically, ids ascending), so names[i] is
-- unrelated to ids[i] -- measured 31,907 mismatches in 40,000 pairs. Only the
-- id SET is trustworthy; labels come from full_rosters by id.
--
-- opp_lineup is deliberately not exposed: both teams have a row for every
-- segment, so the opponent's lanes are the other team's own rows.
CREATE OR REPLACE VIEW euroleague.ribbon_segments_v AS
SELECT
  m.game_id,
  m.team_id,
  m.segment_id,
  m.start_elapsed_seconds,
  m.end_elapsed_seconds,
  l.player_ids
FROM euroleague.matchup_segments_actions m
JOIN (
  SELECT DISTINCT game_id, team_id, own_lineup, player_ids
  FROM euroleague.lineup_totals_by_game
) l
  ON l.game_id    = m.game_id
 AND l.team_id    = m.team_id
 AND l.own_lineup = m.own_lineup
-- 45% of rows are zero-length (two substitutions at the same clock). Dropping
-- them here is load-bearing: they would occupy lane slots while rendering as
-- invisible slivers.
WHERE m.segment_seconds > 0;

-- SUPERSEDED DEFINITION (do not resurrect): the original version of this view
-- read euroleague.actions.points_a/points_b directly and derived elapsed
-- seconds from period + marker_time. Found broken in the 2026-09-05 final
-- review: the provider leaves the running score NULL until that side has
-- scored (measured: 990 NULL-score rows across ALL 593 of 593 games), so
-- points_a - points_b was NA for every event before the second team's first
-- basket. That NA propagated through ribbon_sign_margin() /
-- ribbon_complete_margin() into ribbon_margin_path(), which emitted an
-- invalid "V NaN" into the SVG path `d` -- an SVG path containing NaN is
-- invalid, so the browser draws NOTHING, blanking the ENTIRE curve on every
-- EuroLeague game, not just the affected span.
--
-- CURRENT DEFINITION: reuses euroleague.action_team_context_actions, the same
-- source and `points > 0` predicate the clutch read layer already uses
-- (euroleague/sql/019_clutch_read_layer.sql, the score_states CTE). Verified
-- 2026-09-05: own_team_score/opp_team_score are NULL-free across 109,184
-- scoring rows / 589 games, and the resulting final margin reconciles with
-- euroleague.schedule's recorded result in 1,178 of 1,178 team-games -- an
-- end-to-end correctness proof of the source. It is also already
-- team-perspective (own_team_score/opp_team_score per team_id), so the reader
-- needs no home/away sign flip: RIBBON_SQL_EURO's marg CTE now reads exactly
-- like the Israeli one, filtering on (game_id, team_id) directly.
--
-- `period` is kept (not in the original ask) because the reader's n_periods
-- lookup (`SELECT MAX(period) FROM ribbon_margin_v WHERE game_id = $1`, in
-- app/R/global.R) depends on it for period-boundary/OT-label rendering.
CREATE OR REPLACE VIEW euroleague.ribbon_margin_v AS
SELECT
  game_id,
  team_id,
  period,
  event_elapsed_seconds AS elapsed_seconds,
  (own_team_score - opp_team_score) AS margin,
  source_event_order
FROM euroleague.action_team_context_actions
WHERE points > 0;

GRANT SELECT ON euroleague.ribbon_segments_v TO app_readonly;
GRANT SELECT ON euroleague.ribbon_margin_v TO app_readonly;

COMMIT;

-- ---------------------------------------------------------------------------
-- MIGRATION NOTE
--
-- Both views already exist in the live database and are already granted to
-- app_readonly -- see sql/security/audit_app_access.sql and
-- sql/security/enable_readonly_rls.sql, which already list 'ribbon_segments_v'
-- and 'ribbon_margin_v'. This migration is a reproducibility fix, not a
-- deploy: do not run it against the live database as a routine migration
-- (the objects already exist and CREATE OR REPLACE VIEW would just redefine
-- them to the same thing). It exists so a schema rebuilt from
-- euroleague/sql/*.sql in order includes these two views instead of silently
-- omitting them. The one exception is the ribbon_margin_v hotfix noted above,
-- which was applied live on 2026-09-05 using this exact DDL, ahead of this
-- migration file being written down.
--
-- Verify (read-only, safe to run any time):
--   * app_readonly can SELECT both views;
--   * app_readonly has NO grant on euroleague.actions,
--     euroleague.matchup_segments_actions, or
--     euroleague.action_team_context_actions (the whole point of this layer);
--   * euroleague.ribbon_segments_v has zero rows with NULL/empty player_ids
--     and no duplicate (game_id, team_id, segment_id);
--   * euroleague.ribbon_margin_v has zero rows with a NULL margin.
-- ---------------------------------------------------------------------------
