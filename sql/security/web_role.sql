-- Login role for the web app (Next.js route handlers), spec decision 22.
--
-- app_web is a member of app_readonly and nothing else, so it holds exactly
-- the app's read-only grants, function allowlist and RLS policies (policies
-- written TO app_readonly apply to its members). It exists as its own login
-- only so the web app gets its own connection limit and role-default
-- timeouts and can never take the ETL's or Shiny's capacity.
--
-- Deployed by scripts/apply_web_foundation.R, which substitutes the
-- connection limit (env APP_WEB_CONNECTION_LIMIT, default 10) and sets the
-- password separately from env APP_WEB_PASSWORD. No credential is ever
-- written in this file. Idempotent: an existing role is brought back to the
-- attributes below.

DO $$
BEGIN
  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_readonly') THEN
    RAISE EXCEPTION 'Required role app_readonly does not exist';
  END IF;

  IF NOT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'app_web') THEN
    CREATE ROLE app_web
      LOGIN
      INHERIT
      NOSUPERUSER
      NOCREATEDB
      NOCREATEROLE
      NOREPLICATION
      NOBYPASSRLS
      CONNECTION LIMIT __APP_WEB_CONNECTION_LIMIT__
      IN ROLE app_readonly;
  ELSE
    ALTER ROLE app_web
      LOGIN
      INHERIT
      NOSUPERUSER
      NOCREATEDB
      NOCREATEROLE
      NOREPLICATION
      NOBYPASSRLS
      CONNECTION LIMIT __APP_WEB_CONNECTION_LIMIT__;
    GRANT app_readonly TO app_web;
  END IF;
END
$$;

-- Database-enforced bounds (spec, "Waiting inside the pooler"): every
-- statement is bounded even when no per-request SET LOCAL arrives, and an
-- abandoned transaction cannot hold a backend.
ALTER ROLE app_web SET statement_timeout = '3s';
ALTER ROLE app_web SET idle_in_transaction_session_timeout = '5s';
