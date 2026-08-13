-- EuroLeague shadow schema -- migration 022: default-clutch player totals.
--
-- Player Stats needs player-attributed action totals for the standard clutch
-- preset. This cache is additive and game-local; custom clutch definitions
-- continue to use the canonical action reader.

BEGIN;

SET LOCAL search_path TO euroleague, public;

CREATE OR REPLACE FUNCTION euroleague.refresh_default_clutch_player_for_games(
  game_ids BIGINT[]
)
RETURNS BIGINT
LANGUAGE plpgsql
AS $function$
DECLARE
  target_game_ids BIGINT[];
  inserted_count BIGINT := 0;
BEGIN
  IF game_ids IS NULL OR array_length(game_ids, 1) IS NULL THEN
    SELECT array_agg(s.game_id ORDER BY s.game_id) INTO target_game_ids
    FROM euroleague.schedule s;
    DELETE FROM euroleague.default_clutch_player_totals_by_game;
  ELSE
    SELECT array_agg(DISTINCT x ORDER BY x) INTO target_game_ids
    FROM unnest(game_ids) x;
    DELETE FROM euroleague.default_clutch_player_totals_by_game c
    WHERE c.game_id = ANY(target_game_ids);
  END IF;

  IF target_game_ids IS NULL OR array_length(target_game_ids, 1) IS NULL THEN
    RETURN 0;
  END IF;

  INSERT INTO euroleague.default_clutch_player_totals_by_game (
    game_id, team_id, player_id, pts, reb, oreb, dreb, ast, stl, blk, tov,
    fg2m, fg2a, fg3m, fg3a, ftm, fta, player_ts_poss, player_tov,
    derivation_version
  )
  SELECT
    atc.game_id,
    atc.team_id,
    atc.action_player_id,
    sum(atc.points)::numeric,
    sum(atc.orebounds + CASE WHEN atc.play_type = 'D' THEN 1 ELSE 0 END)::numeric,
    sum(atc.orebounds)::numeric,
    sum(CASE WHEN atc.play_type = 'D' THEN 1 ELSE 0 END)::numeric,
    sum(CASE WHEN atc.play_type = 'AS' THEN 1 ELSE 0 END)::numeric,
    sum(CASE WHEN atc.play_type = 'ST' THEN 1 ELSE 0 END)::numeric,
    sum(CASE WHEN atc.play_type = 'FV' THEN 1 ELSE 0 END)::numeric,
    sum(atc.turnovers)::numeric,
    sum(atc.fg2_made)::numeric,
    sum(atc.fg2_att)::numeric,
    sum(atc.fg3_made)::numeric,
    sum(atc.fg3_att)::numeric,
    sum(CASE WHEN atc.play_type = 'FTM' THEN 1 ELSE 0 END)::numeric,
    sum(atc.ft_attempts)::numeric,
    sum(atc.ts_possessions)::numeric,
    sum(atc.turnovers)::numeric,
    'default-clutch-player-v1'
  FROM euroleague.action_team_context_actions atc
  JOIN euroleague.players p ON p.player_id = atc.action_player_id
  WHERE atc.game_id = ANY(target_game_ids)
    AND atc.type_lineup = 'offense'
    AND atc.action_player_id IS NOT NULL
    AND lower(p.provider_player_id) NOT IN ('team', 'total')
    AND euroleague.clutch_event_qualifies(
      atc.period, atc.event_elapsed_seconds,
      atc.own_team_score - CASE WHEN atc.event_team_id = atc.team_id THEN atc.points ELSE 0 END,
      atc.opp_team_score - CASE WHEN atc.event_team_id = atc.opponent_team_id THEN atc.points ELSE 0 END,
      5, 'all', 300, false
    )
  GROUP BY atc.game_id, atc.team_id, atc.action_player_id;

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$function$;

SELECT euroleague.refresh_default_clutch_player_for_games(NULL::bigint[]);

REVOKE ALL ON TABLE euroleague.default_clutch_player_totals_by_game FROM PUBLIC;
GRANT SELECT ON euroleague.default_clutch_player_totals_by_game TO app_readonly;
REVOKE ALL ON FUNCTION euroleague.refresh_default_clutch_player_for_games(BIGINT[]) FROM PUBLIC;

COMMIT;
