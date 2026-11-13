-- Credit-attribution definition of team_four_factors_by_game, EuroLeague migration 062 (EUROLEAGUE SHADOW SCHEMA),
-- written by scripts/apply_062_readers.py from the live definition.

CREATE OR REPLACE FUNCTION euroleague.refresh_team_four_factors_by_game_for_games(game_ids bigint[])
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
DECLARE
  inserted_count bigint := 0;
BEGIN
  IF game_ids IS NULL OR array_length(game_ids, 1) IS NULL THEN
    DELETE FROM euroleague.team_four_factors_by_game;
  ELSE
    DELETE FROM euroleague.team_four_factors_by_game WHERE game_id = ANY(game_ids);
  END IF;

  INSERT INTO euroleague.team_four_factors_by_game (
    game_id, team_id, game_year, own_starters, opp_starters,
    off_pts, off_poss, off_ts_poss, off_oreb, off_oreb_opp, off_tov,
    off_fta, off_fga, off_fgm, off_fg3m,
    def_pts, def_poss, def_ts_poss, def_oreb, def_oreb_opp, def_tov,
    def_fta, def_fga, def_fgm, def_fg3m, def_steals,
    derivation_version
  )
  SELECT
    atc.game_id,
    atc.team_id,
    s.season::smallint,
    atc.credit_own_starters,
    atc.credit_opp_starters,
    coalesce(sum(atc.points) FILTER (WHERE atc.type_lineup = 'offense'), 0)::numeric,
    coalesce(sum(atc.possession_flag) FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.ts_possessions) FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.orebounds) FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.oreb_opportunities) FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.turnovers) FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.ft_attempts) FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.fga) FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.fgm) FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.fg3_made) FILTER (WHERE atc.type_lineup = 'offense'), 0)::bigint,
    coalesce(sum(atc.points) FILTER (WHERE atc.type_lineup = 'defense'), 0)::numeric,
    coalesce(sum(atc.possession_flag) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.ts_possessions) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.orebounds) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.oreb_opportunities) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.turnovers) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.ft_attempts) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.fga) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.fgm) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.fg3_made) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    coalesce(sum(atc.steals) FILTER (WHERE atc.type_lineup = 'defense'), 0)::bigint,
    'actions-v1'
  FROM euroleague.action_team_context_actions atc
  JOIN euroleague.schedule s ON s.game_id = atc.game_id
 WHERE game_ids IS NULL OR atc.game_id = ANY(game_ids)
 GROUP BY atc.game_id, atc.team_id, s.season,
          atc.credit_own_starters, atc.credit_opp_starters;

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$function$;
