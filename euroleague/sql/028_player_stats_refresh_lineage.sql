-- EuroLeague migration 028: repair incremental Player Stats action refresh.
-- Migration 027 repoints player_stats_action_context to the physical fact, so
-- that consumer view must never be used as the refresh source.

BEGIN;
SET LOCAL search_path TO euroleague, public;

CREATE OR REPLACE FUNCTION euroleague.refresh_player_stats_actions_for_games(
    game_ids BIGINT[]
)
RETURNS BIGINT
LANGUAGE plpgsql
AS $function$
DECLARE
  target_ids BIGINT[];
  inserted_count BIGINT := 0;
BEGIN
  IF game_ids IS NULL OR array_length(game_ids, 1) IS NULL THEN
    SELECT array_agg(game_id ORDER BY game_id)
      INTO target_ids FROM euroleague.schedule;
    DELETE FROM euroleague.player_stats_actions_by_game;
  ELSE
    SELECT array_agg(DISTINCT x ORDER BY x)
      INTO target_ids FROM unnest(game_ids) x;
    DELETE FROM euroleague.player_stats_actions_by_game
     WHERE game_id = ANY(target_ids);
  END IF;

  IF target_ids IS NULL THEN RETURN 0; END IF;

  INSERT INTO euroleague.player_stats_actions_by_game (
    game_id, team_id, source_event_order, own_lineup, segment_id,
    event_elapsed_seconds, type_lineup, possession_flag, action_player_id,
    points, play_type, turnovers, fgm, fga, fg3_made, fg3_att,
    ft_attempts, ts_possessions, is_overtime,
    regulation_seconds_remaining, pre_margin, pre_abs_margin, pre_status
  )
  SELECT
    atc.game_id, atc.team_id, atc.source_event_order, atc.own_lineup,
    atc.segment_id, atc.event_elapsed_seconds, atc.type_lineup,
    atc.possession_flag, atc.action_player_id, atc.points, atc.play_type,
    atc.turnovers, atc.fgm, atc.fga, atc.fg3_made, atc.fg3_att,
    atc.ft_attempts, atc.ts_possessions,
    (atc.period > 4),
    greatest(2400 - atc.event_elapsed_seconds, 0)::numeric,
    (atc.own_team_score
       - CASE WHEN atc.event_team_id = atc.team_id THEN atc.points ELSE 0 END)
      - (atc.opp_team_score
       - CASE WHEN atc.event_team_id = atc.opponent_team_id THEN atc.points ELSE 0 END),
    abs(
      (atc.own_team_score
       - CASE WHEN atc.event_team_id = atc.team_id THEN atc.points ELSE 0 END)
      - (atc.opp_team_score
       - CASE WHEN atc.event_team_id = atc.opponent_team_id THEN atc.points ELSE 0 END)
    ),
    sign(
      (atc.own_team_score
       - CASE WHEN atc.event_team_id = atc.team_id THEN atc.points ELSE 0 END)
      - (atc.opp_team_score
       - CASE WHEN atc.event_team_id = atc.opponent_team_id THEN atc.points ELSE 0 END)
    )::smallint
  FROM euroleague.action_team_context_actions atc
  WHERE atc.game_id = ANY(target_ids);

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$function$;

REVOKE ALL ON FUNCTION euroleague.refresh_player_stats_actions_for_games(
  bigint[]
) FROM PUBLIC;

COMMIT;
