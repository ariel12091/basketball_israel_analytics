-- refresh_player_traditional_stats_for_games(p_game_ids): rebuild basketball_test.player_traditional_stats_mv
-- for these games (their whole seasons). NULL = everything; an empty array = nothing.
--
-- Called by etl/etl_full.R Phase 4 for the night's games. Until 2026-11-14 the
-- table was a materialized view rebuilt in full every night with games.
CREATE OR REPLACE FUNCTION basketball_test.refresh_player_traditional_stats_for_games(p_game_ids int4[])
RETURNS bigint
LANGUAGE plpgsql
SET plan_cache_mode = force_custom_plan
SET work_mem = '256MB'
AS $function$
DECLARE
  v_rows bigint;
  v_years int4[];
BEGIN
  IF p_game_ids IS NOT NULL AND cardinality(p_game_ids) = 0 THEN
    RETURN 0;
  END IF;

  -- The seasons of these games.
  SELECT array_agg(DISTINCT fs.game_year) INTO v_years
  FROM basketball_test.final_schedule_mv fs
  WHERE p_game_ids IS NULL OR fs.game_id = ANY (p_game_ids);
  IF v_years IS NULL THEN
    RETURN 0;
  END IF;

  DELETE FROM basketball_test.player_traditional_stats_mv
  WHERE game_year = ANY (v_years);

  INSERT INTO basketball_test.player_traditional_stats_mv
  SELECT * FROM basketball_test.compute_player_traditional_stats(v_years);

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RETURN v_rows;
END;
$function$;
