CREATE OR REPLACE FUNCTION basketball_test.refresh_player_traditional_by_game_for_games(
  p_game_ids int4[]
)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
  inserted_count bigint;
BEGIN
  -- NULL = every game; an empty array = nothing, as in the other
  -- refresh_*_for_games functions. Until 2026-11-14 an empty array took the
  -- NULL branch's DELETE, and compute_*(empty) inserts nothing: the table was
  -- left empty.
  IF p_game_ids IS NOT NULL AND cardinality(p_game_ids) = 0 THEN
    RETURN 0;
  END IF;

  IF p_game_ids IS NULL THEN
    DELETE FROM basketball_test.player_traditional_by_game;
  ELSE
    DELETE FROM basketball_test.player_traditional_by_game
    WHERE game_id = ANY(p_game_ids);
  END IF;

  INSERT INTO basketball_test.player_traditional_by_game
  SELECT *
  FROM basketball_test.compute_player_traditional_by_game(p_game_ids);

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$$;
