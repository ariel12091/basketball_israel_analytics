-- Credit-attribution definition of default_clutch_rebuild, EuroLeague migration 062 (EUROLEAGUE SHADOW SCHEMA),
-- written by scripts/apply_062_readers.py from the live definition.

CREATE OR REPLACE FUNCTION euroleague.refresh_default_clutch_for_games(game_ids bigint[])
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
DECLARE
  target_game_ids bigint[];
  inserted_count bigint := 0;
BEGIN
  IF game_ids IS NULL OR array_length(game_ids, 1) IS NULL THEN
    SELECT array_agg(s.game_id ORDER BY s.game_id)
      INTO target_game_ids
      FROM euroleague.schedule s;
    DELETE FROM euroleague.default_clutch_lineup_totals_by_game;
  ELSE
    SELECT array_agg(DISTINCT x ORDER BY x)
      INTO target_game_ids
      FROM unnest(game_ids) x;
    DELETE FROM euroleague.default_clutch_lineup_totals_by_game c
     WHERE c.game_id = ANY(target_game_ids);
  END IF;

  IF target_game_ids IS NULL OR array_length(target_game_ids, 1) IS NULL THEN
    RETURN 0;
  END IF;

  INSERT INTO euroleague.default_clutch_lineup_totals_by_game (
    game_id, team_id, own_lineup, own_starters, opp_starters, type_lineup,
    possessions, points, fg2_made, fg2_att, fg3_made, fg3_att,
    ts_possessions, fgm, fga, ft_attempts, orebounds,
    oreb_opportunities, turnovers, steals, seconds,
    derivation_version
  )
  SELECT
    f.game_id, f.team_id, f.own_lineup, f.own_starters, f.opp_starters,
    f.type_lineup, f.possessions, f.points, f.fg2_made, f.fg2_att,
    f.fg3_made, f.fg3_att, f.ts_possessions, f.fgm, f.fga,
    f.ft_attempts, f.orebounds, f.oreb_opportunities, f.turnovers,
    f.steals, f.seconds, 'default-clutch-v1'
  FROM euroleague.clutch_team_game_facts(
    target_game_ids, 5, 'all', 300, false
  ) f;

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$function$;
