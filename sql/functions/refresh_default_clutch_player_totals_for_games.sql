CREATE OR REPLACE FUNCTION basketball_test.refresh_default_clutch_player_totals_for_games(
  p_game_ids int4[]
)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
  inserted_count bigint;
BEGIN
  IF p_game_ids IS NULL OR array_length(p_game_ids, 1) IS NULL THEN
    DELETE FROM basketball_test.default_clutch_player_totals_by_game;
  ELSE
    DELETE FROM basketball_test.default_clutch_player_totals_by_game
    WHERE game_id = ANY(p_game_ids);
  END IF;

  INSERT INTO basketball_test.default_clutch_player_totals_by_game
  SELECT *
  FROM basketball_test.compute_player_traditional_by_game(p_game_ids, TRUE);

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$$;
