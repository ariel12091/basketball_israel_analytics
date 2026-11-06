-- app_api.version(): (state, version) per league for GET /api/v1/version.
--
-- Requires the publication markers (sql/publication/001_publication.sql,
-- deployed by scripts/apply_publication.R) -- apply those first.
-- publication_status() reports 'stale' for a 'publishing' row whose run no
-- longer holds its advisory lock, so this returns all four states.
-- EXECUTE comes only from the allowlist in sql/security/enable_readonly_rls.sql.
CREATE OR REPLACE FUNCTION app_api.version()
RETURNS TABLE(league text, state text, version bigint)
LANGUAGE sql STABLE
SET search_path = '' SET plan_cache_mode = force_custom_plan
AS $fn$
  SELECT p.league, s.state, s.version
  FROM basketball_test.publication p
  CROSS JOIN LATERAL basketball_test.publication_status(p.league) s
  ORDER BY p.league
$fn$;

REVOKE ALL ON FUNCTION app_api.version() FROM PUBLIC, anon, authenticated;
