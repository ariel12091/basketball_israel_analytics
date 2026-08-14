-- EuroLeague shadow schema -- migration 015: cumulative-minute overtime clock.
--
-- The provider can keep PERIOD=5 for every overtime while MINUTE continues
-- through 41-45, 46-50, and later intervals.  Preserve PERIOD as immutable
-- package evidence and derive an effective analytics period instead.

BEGIN;

SET LOCAL search_path TO euroleague, public;

CREATE OR REPLACE FUNCTION euroleague.effective_period(
  provider_period smallint,
  provider_minute integer,
  provider_play_type text
)
RETURNS smallint
LANGUAGE sql
IMMUTABLE
PARALLEL SAFE
AS $function$
  SELECT CASE
    WHEN provider_period <= 4 OR provider_minute IS NULL THEN provider_period
    ELSE (
      5 + greatest(
        provider_minute - 41
          - CASE WHEN upper(btrim(coalesce(provider_play_type, ''))) IN ('EP', 'EG')
                 THEN 1 ELSE 0 END,
        0
      ) / 5
    )::smallint
  END
$function$;

-- Replace only the reviewed expressions inside migration 011's existing
-- refresh function.  Exact occurrence checks make this fail closed if that
-- definition has drifted rather than silently patching the wrong SQL.
DO $migration$
DECLARE
  definition text;
  needle text;
  expected integer;
  actual integer;
BEGIN
  SELECT pg_get_functiondef(p.oid)
    INTO definition
    FROM pg_proc p
    JOIN pg_namespace n ON n.oid = p.pronamespace
   WHERE n.nspname = 'euroleague'
     AND p.proname = 'refresh_actions_consumer_candidates'
     AND pg_get_function_identity_arguments(p.oid) = 'game_ids bigint[]';

  IF definition IS NULL THEN
    RAISE EXCEPTION 'refresh_actions_consumer_candidates(bigint[]) is missing';
  END IF;

  FOR needle, expected IN
    VALUES
      ('a.period <= 4', 4),
      ('(a.period - 1)', 2),
      ('(a.period - 5)', 2),
      ('max(a.period)', 1),
      ('sd.period,', 1),
      ('''actions-v1''', 2)
  LOOP
    actual := (length(definition) - length(replace(definition, needle, '')))
              / length(needle);
    IF actual <> expected THEN
      RAISE EXCEPTION 'unexpected occurrence count for %: expected %, found %',
        needle, expected, actual;
    END IF;
  END LOOP;

  definition := replace(
    definition,
    'a.period <= 4',
    'euroleague.effective_period(a.period, a.minute, a.play_type) <= 4'
  );
  definition := replace(
    definition,
    '(a.period - 1)',
    '(euroleague.effective_period(a.period, a.minute, a.play_type) - 1)'
  );
  definition := replace(
    definition,
    '(a.period - 5)',
    '(euroleague.effective_period(a.period, a.minute, a.play_type) - 5)'
  );
  definition := replace(
    definition,
    'max(a.period)',
    'max(euroleague.effective_period(a.period, a.minute, a.play_type))'
  );
  definition := replace(
    definition,
    'sd.period,',
    'euroleague.effective_period(sd.period, sd.minute, sd.play_type),'
  );
  definition := replace(definition, '''actions-v1''', '''actions-v2''');

  EXECUTE definition;
END
$migration$;

GRANT EXECUTE ON FUNCTION euroleague.effective_period(smallint, integer, text)
  TO app_readonly;

COMMIT;
