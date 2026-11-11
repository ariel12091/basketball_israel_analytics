-- EUROLEAGUE SHADOW SCHEMA -- migration 061: possession ends and counted
-- events for the web Gameflow (stint ratings, run results, ORtg tooltip).
--
-- docs/shared_definitions.md, "Gameflow stint rating" and "Gameflow ORtg":
-- the web Gameflow sums these rows into Net / ORtg / DRtg per player and
-- stint, and reads the possession ends for each team's ORtg. Until now the
-- EuroLeague reader returned no possession ends (D35): they live in
-- action_team_context_actions, which app_readonly may not read. This view is
-- the curated slice of it, one row per team perspective per event, and adds
-- no new derivation -- every count is the column the on/off tables sum.
--
-- Credit (D38): action_team_context_actions assigns each row to own_lineup,
-- the lineup the on/off tables use, so these rows match EuroLeague On/Off
-- (a free-throw trip goes to the lineup of its last free throw, D1).
--
-- player_ids comes from lineup_totals_by_game on (game_id, team_id,
-- own_lineup), exactly as ribbon_segments_v (053) does. Never pair own_lineup
-- and player_ids positionally: the arrays are sorted independently. The join
-- is LEFT so a lineup with no match keeps its row with NULL player_ids; the
-- nightly pin (test-gameflow-events-db.R) fails on any such row instead of
-- the events vanishing.
--
-- New view: CREATE only, granted here and also listed in
-- sql/security/enable_readonly_rls.sql so scripts/apply_db_security.R keeps
-- the grant after any later DROP + CREATE.

BEGIN;
SET LOCAL search_path TO euroleague, public;

CREATE OR REPLACE VIEW euroleague.ribbon_events_v AS
SELECT
  a.game_id,
  a.team_id,
  a.period,
  a.event_elapsed_seconds AS elapsed_seconds,
  a.source_event_order,
  a.type_lineup,
  l.player_ids,
  a.points,
  a.fg2_att,
  a.fg2_made,
  a.fg3_att,
  a.fg3_made,
  a.ft_attempts,
  a.turnovers,
  a.orebounds,
  a.final_end_poss,
  a.own_team_score,
  a.opp_team_score
FROM euroleague.action_team_context_actions a
LEFT JOIN (
  SELECT DISTINCT game_id, team_id, own_lineup, player_ids
  FROM euroleague.lineup_totals_by_game
) l
  ON l.game_id    = a.game_id
 AND l.team_id    = a.team_id
 AND l.own_lineup = a.own_lineup
WHERE a.type_lineup IN ('offense', 'defense')
  AND (a.final_end_poss IS TRUE
       OR a.points > 0
       OR a.fg2_att > 0
       OR a.fg3_att > 0
       OR a.ft_attempts > 0
       OR a.turnovers > 0
       OR a.orebounds > 0);

GRANT SELECT ON euroleague.ribbon_events_v TO app_readonly;

COMMIT;

-- ---------------------------------------------------------------------------
-- VERIFY after applying and the security re-apply (read-only):
--
--   SELECT has_table_privilege('app_readonly',
--            'euroleague.ribbon_events_v', 'SELECT');          -- true
--   SELECT has_table_privilege('app_readonly',
--            'euroleague.action_team_context_actions', 'SELECT'); -- false
--   SELECT count(*) FILTER (WHERE player_ids IS NULL)
--   FROM euroleague.ribbon_events_v;                            -- 0
-- ---------------------------------------------------------------------------
