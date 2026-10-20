-- Publication markers and single-writer ownership (plan Task 2; spec
-- "Caching and freshness", "One writer at a time").
--
-- Deploy order: THIS FILE FIRST (scripts/apply_publication.R), then
-- sql/app_api/*.sql (scripts/apply_web_foundation.R), whose readers call
-- publication_status(). __SCHEMA__ is substituted with a quoted identifier,
-- function bodies included: basketball_test live, or an isolated pubtest_*
-- schema for the DB tests (review R3). The schema name is part of the
-- advisory-lock key, so a test schema can never contend with the live lock.
--
-- States: 'published' / 'publishing' / 'failed' are stored; 'stale' is only
-- reported by publication_status(), for a 'publishing' row whose run no
-- longer holds the league's session-level advisory lock (a crashed run).
-- Writers: take pg_advisory_lock(publication_lock_key(league)) on a session
-- connection (never the transaction pooler), begin_publication(), call
-- fence_publication() first in every write transaction, finish_publication().
-- Grants (review R9): only publication_status and publication_lock_key are
-- readable by app_readonly, via the allowlist in
-- sql/security/enable_readonly_rls.sql (a grant here alone would not survive
-- the next security re-apply). begin/fence/finish stay owner-only.
CREATE TABLE IF NOT EXISTS __SCHEMA__.publication (
  league text PRIMARY KEY CHECK (league IN ('il', 'euroleague', 'eurocup')),
  state text NOT NULL CHECK (state IN ('publishing', 'published', 'failed')),
  version bigint NOT NULL CHECK (version > 0),
  owner_run uuid,
  started_at timestamptz,
  finished_at timestamptz
);
INSERT INTO __SCHEMA__.publication (league, state, version)
VALUES ('il', 'published', 1), ('euroleague', 'published', 1), ('eurocup', 'published', 1)
ON CONFLICT (league) DO NOTHING;

CREATE OR REPLACE FUNCTION __SCHEMA__.publication_lock_key(p_league text)
RETURNS bigint LANGUAGE sql IMMUTABLE SET search_path = '' AS $$
  SELECT pg_catalog.hashtextextended('__SCHEMA__:publication:' || p_league, 0)
         & 9223372036854775807::bigint
$$;

CREATE OR REPLACE FUNCTION __SCHEMA__.begin_publication(p_league text, p_run uuid)
RETURNS void LANGUAGE plpgsql SET search_path = '' AS $$
DECLARE k bigint := __SCHEMA__.publication_lock_key(p_league);
BEGIN
  IF p_run IS NULL OR NOT EXISTS (
    SELECT 1 FROM pg_catalog.pg_locks
    WHERE locktype = 'advisory' AND pid = pg_catalog.pg_backend_pid() AND granted
      AND database = (SELECT d.oid FROM pg_catalog.pg_database d
                      WHERE d.datname = pg_catalog.current_database())
      AND classid = (k >> 32)::oid AND objid = (k & 4294967295)::oid AND objsubid = 1
  ) THEN
    RAISE EXCEPTION 'publication advisory lock required';
  END IF;
  -- FOR UPDATE waits for all fenced, in-flight write transactions to finish.
  PERFORM 1 FROM __SCHEMA__.publication WHERE league = p_league FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'unknown publication league'; END IF;
  UPDATE __SCHEMA__.publication SET version = version + 1, state = 'failed', finished_at = clock_timestamp()
    WHERE league = p_league AND state = 'publishing';
  UPDATE __SCHEMA__.publication SET state = 'publishing', owner_run = p_run,
    started_at = clock_timestamp(), finished_at = NULL WHERE league = p_league;
END
$$;

CREATE OR REPLACE FUNCTION __SCHEMA__.fence_publication(p_league text, p_run uuid)
RETURNS void LANGUAGE plpgsql SET search_path = '' AS $$
BEGIN
  PERFORM 1 FROM __SCHEMA__.publication
    WHERE league = p_league AND owner_run = p_run AND state = 'publishing' FOR SHARE;
  IF NOT FOUND THEN RAISE EXCEPTION 'fenced_out'; END IF;
END
$$;

CREATE OR REPLACE FUNCTION __SCHEMA__.finish_publication(p_league text, p_run uuid, p_ok boolean)
RETURNS bigint LANGUAGE plpgsql SET search_path = '' AS $$
DECLARE v bigint;
BEGIN
  UPDATE __SCHEMA__.publication SET state = CASE WHEN p_ok THEN 'published' ELSE 'failed' END,
    version = version + 1, finished_at = clock_timestamp()
    WHERE league = p_league AND owner_run = p_run AND state = 'publishing'
    RETURNING version INTO v;
  IF NOT FOUND THEN RAISE EXCEPTION 'fenced_out'; END IF;
  RETURN v;
END
$$;

CREATE OR REPLACE FUNCTION __SCHEMA__.publication_status(p_league text)
RETURNS TABLE(state text, version bigint) LANGUAGE sql STABLE SET search_path = '' AS $$
  SELECT CASE WHEN p.state = 'publishing' AND NOT EXISTS (
    SELECT 1 FROM pg_catalog.pg_locks l
    WHERE l.locktype = 'advisory' AND l.granted
      AND l.database = (SELECT d.oid FROM pg_catalog.pg_database d
                        WHERE d.datname = pg_catalog.current_database())
      AND l.classid = (__SCHEMA__.publication_lock_key(p.league) >> 32)::oid
      AND l.objid = (__SCHEMA__.publication_lock_key(p.league) & 4294967295)::oid
      AND l.objsubid = 1
  ) THEN 'stale' ELSE p.state END, p.version
  FROM __SCHEMA__.publication p WHERE p.league = p_league
$$;

REVOKE ALL ON __SCHEMA__.publication FROM PUBLIC;
REVOKE ALL ON FUNCTION __SCHEMA__.publication_lock_key(text),
  __SCHEMA__.begin_publication(text, uuid), __SCHEMA__.fence_publication(text, uuid),
  __SCHEMA__.finish_publication(text, uuid, boolean), __SCHEMA__.publication_status(text) FROM PUBLIC;
GRANT SELECT ON __SCHEMA__.publication TO app_readonly;
GRANT EXECUTE ON FUNCTION __SCHEMA__.publication_lock_key(text),
  __SCHEMA__.publication_status(text) TO app_readonly;
