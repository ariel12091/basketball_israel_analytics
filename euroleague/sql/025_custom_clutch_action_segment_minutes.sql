-- EuroLeague migration 025: shared Israeli-style custom-clutch duration.
--
-- For interactive custom presets, use the same action-segment convention as
-- the Israeli read layer: last qualifying action minus first qualifying action
-- within each game/team/lineup segment. Event counts and possessions remain
-- exact. The standard preset remains exact because migration 020 serves its
-- precomputed segment/window intersections without calling this function.

BEGIN;
SET LOCAL search_path TO euroleague, public;

CREATE OR REPLACE FUNCTION euroleague.clutch_segment_durations(
    p_game_ids BIGINT[],
    p_max_margin INTEGER,
    p_margin_status TEXT,
    p_max_time_remaining INTEGER,
    p_ot_margin_filter BOOLEAN
)
RETURNS TABLE (game_id BIGINT, team_id BIGINT, segment_id INTEGER, seconds NUMERIC)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, euroleague, public
AS $function$
  SELECT
    atc.game_id,
    atc.team_id,
    atc.segment_id,
    greatest(
      max(atc.event_elapsed_seconds) - min(atc.event_elapsed_seconds),
      0::numeric
    )::numeric AS seconds
  FROM euroleague.action_team_context_actions atc
  WHERE atc.game_id = ANY(coalesce(p_game_ids, ARRAY[]::bigint[]))
    AND atc.segment_id IS NOT NULL
    AND atc.event_elapsed_seconds IS NOT NULL
    AND euroleague.clutch_event_qualifies(
          atc.period,
          atc.event_elapsed_seconds,
          atc.own_team_score
            - CASE WHEN atc.event_team_id = atc.team_id THEN atc.points ELSE 0 END,
          atc.opp_team_score
            - CASE WHEN atc.event_team_id = atc.opponent_team_id THEN atc.points ELSE 0 END,
          p_max_margin, p_margin_status, p_max_time_remaining,
          p_ot_margin_filter
        )
  GROUP BY atc.game_id, atc.team_id, atc.segment_id
$function$;

REVOKE ALL ON FUNCTION euroleague.clutch_segment_durations(
  bigint[], integer, text, integer, boolean
) FROM PUBLIC;

COMMIT;
