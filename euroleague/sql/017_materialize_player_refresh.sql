-- EuroLeague shadow schema -- migration 017: batch player refresh plan guard.
--
-- Preserve the Israeli ETL's measured plan fix after the actions-based cutover:
-- the two heavy aggregates must be materialized before their final joins.

BEGIN;

SET LOCAL search_path TO euroleague, public;

DO $migration$
DECLARE
  definition text;
BEGIN
  SELECT pg_get_functiondef(p.oid)
    INTO definition
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
   WHERE n.nspname = 'euroleague'
     AND p.proname = 'refresh_player_four_factors_by_game_for_games'
     AND pg_get_function_identity_arguments(p.oid) = 'game_ids bigint[]';

  IF definition IS NULL THEN
    RAISE EXCEPTION 'refresh_player_four_factors_by_game_for_games(bigint[]) is missing';
  END IF;
  IF strpos(definition, 'player_minutes AS MATERIALIZED') = 0 THEN
    definition := replace(definition, 'player_minutes AS (', 'player_minutes AS MATERIALIZED (');
  END IF;
  IF strpos(definition, 'counts AS MATERIALIZED') = 0 THEN
    definition := replace(definition, 'counts AS (', 'counts AS MATERIALIZED (');
  END IF;
  IF strpos(definition, 'player_minutes AS MATERIALIZED') = 0
     OR strpos(definition, 'counts AS MATERIALIZED') = 0 THEN
    RAISE EXCEPTION 'migration 017 could not materialize both player refresh aggregates';
  END IF;
  EXECUTE definition;
END
$migration$;

COMMIT;
