CREATE OR REPLACE FUNCTION app_api.version()
RETURNS TABLE(league text, state text, version bigint)
LANGUAGE sql STABLE SET search_path = ''
AS $fn$
  SELECT p.league, s.state, s.version
  FROM basketball_test.publication p
  CROSS JOIN LATERAL basketball_test.publication_status(p.league) s
  ORDER BY p.league
$fn$;
REVOKE ALL ON FUNCTION app_api.version() FROM PUBLIC, anon, authenticated;
