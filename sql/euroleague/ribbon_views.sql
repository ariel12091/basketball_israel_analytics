-- Narrow read layer for the stint ribbon.
--
-- app_readonly is denied on euroleague.actions and
-- euroleague.matchup_segments_actions on purpose: the euro schema keeps raw
-- provider evidence closed and grants only an enumerated read layer. These two
-- views expose exactly what the ribbon needs and nothing else.
--
-- Editing either view means DROP + CREATE, which wipes the app_readonly grant.
-- Re-run scripts/apply_db_security.R with CONFIRM_DB_SECURITY_APPLY=1 after.

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

-- The provider records a per-period countdown (marker_time) plus a period
-- number; the ribbon needs elapsed seconds. Periods 1-4 run 10:00 and period
-- 5+ runs 05:00, verified against observed game lengths of exactly
-- 2400 / 2700 / 3000 / 3300 seconds.
-- home_team_id comes from euroleague.schedule, the authoritative mapping, not
-- from aggregating actions.is_home_team over a 211 MB table.
CREATE OR REPLACE VIEW euroleague.ribbon_margin_v AS
SELECT
  a.game_id,
  a.period,
  (
    (CASE WHEN a.period <= 4 THEN (a.period - 1) * 600
          ELSE 2400 + (a.period - 5) * 300 END)
    + ((CASE WHEN a.period <= 4 THEN 600 ELSE 300 END)
       - (split_part(a.marker_time, ':', 1)::int * 60
          + split_part(a.marker_time, ':', 2)::int))
  )::numeric AS elapsed_seconds,
  a.source_event_order,
  a.points_a,
  a.points_b,
  sc.home_team_id
FROM euroleague.actions a
JOIN euroleague.schedule sc ON sc.game_id = a.game_id
WHERE a.marker_time IS NOT NULL
  AND (a.points_a IS NOT NULL OR a.points_b IS NOT NULL);
