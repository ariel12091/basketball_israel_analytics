-- refresh_mv_lineup_totals_by_day_for_games(p_game_ids): rebuild basketball_test.mv_lineup_totals_by_day
-- for these games. NULL = everything; an empty array = nothing.
--
-- Called by etl/etl_full.R Phase 4 for the night's games. Until 2026-11-14 the
-- table was a materialized view rebuilt in full every night with games.
CREATE OR REPLACE FUNCTION basketball_test.refresh_mv_lineup_totals_by_day_for_games(p_game_ids int4[])
RETURNS bigint
LANGUAGE plpgsql
SET plan_cache_mode = force_custom_plan
SET work_mem = '256MB'
AS $function$
DECLARE
  v_rows bigint;
BEGIN
  IF p_game_ids IS NOT NULL AND cardinality(p_game_ids) = 0 THEN
    RETURN 0;
  END IF;

  DELETE FROM basketball_test.mv_lineup_totals_by_day
  WHERE p_game_ids IS NULL OR game_id = ANY (p_game_ids);

  INSERT INTO basketball_test.mv_lineup_totals_by_day
  SELECT * FROM basketball_test.compute_mv_lineup_totals_by_day(p_game_ids);

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RETURN v_rows;
END;
$function$;
