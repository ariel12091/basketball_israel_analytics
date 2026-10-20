-- The web app's database surface (spec, "A new database surface").
--
-- app_api holds only the functions the web app may call, each a thin wrapper
-- over the owning function in basketball_test / euroleague. It is not exposed
-- through Supabase's Data API and holds no relations.
--
-- Function EXECUTE is NOT granted here. sql/security/enable_readonly_rls.sql
-- revokes every function grant in app_api and restores only its enumerated
-- allowlist (app_api_function_names, mirrored in audit_app_access.sql), so a
-- grant made here alone would vanish on the next security re-apply (review
-- R9). A new app_api function is one entry in each of those two lists.
--
-- Deployed by scripts/apply_web_foundation.R, which runs the security
-- re-apply and audit in the same transaction.

CREATE SCHEMA IF NOT EXISTS app_api;

COMMENT ON SCHEMA app_api IS
  'Web app read API. EXECUTE for app_readonly via the allowlist in sql/security/enable_readonly_rls.sql only.';

REVOKE ALL PRIVILEGES ON SCHEMA app_api FROM PUBLIC;
REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA app_api FROM PUBLIC;
-- This per-schema revoke cannot remove PUBLIC's global (built-in) EXECUTE
-- default: per-schema default privileges only add to the global ones. A
-- function created here is therefore executable by PUBLIC until the next
-- security re-apply revokes it. Safety does not rest on this line: PUBLIC,
-- anon and authenticated have no USAGE on app_api, so they cannot reach the
-- function, and every deploy of sql/app_api/*.sql ends with the re-apply.
ALTER DEFAULT PRIVILEGES IN SCHEMA app_api REVOKE ALL PRIVILEGES ON FUNCTIONS FROM PUBLIC;

DO $$
DECLARE
  v_role text;
BEGIN
  FOREACH v_role IN ARRAY ARRAY['anon', 'authenticated']
  LOOP
    IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = v_role) THEN
      EXECUTE format('REVOKE ALL PRIVILEGES ON SCHEMA app_api FROM %I', v_role);
      EXECUTE format('REVOKE ALL PRIVILEGES ON ALL FUNCTIONS IN SCHEMA app_api FROM %I', v_role);
    END IF;
  END LOOP;
END
$$;

REVOKE CREATE ON SCHEMA app_api FROM app_readonly;
GRANT USAGE ON SCHEMA app_api TO app_readonly;
