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
-- reported by publication_status(), for a 'publishing' row whose OWNING
-- backend -- the pid and backend_start recorded by begin_publication() -- no
-- longer holds the league's advisory lock in ExclusiveLock mode (a crashed
-- run). Another session holding the key cannot mask that crash, and a reused
-- pid cannot either (its backend_start differs).
-- Writers: take pg_advisory_lock(publication_lock_key(league)) -- session
-- level, exclusive -- on a session connection (never the transaction pooler),
-- call begin_publication() and finish_publication() on that same connection,
-- and fence_publication() first in every write transaction on any
-- connection. begin refuses no lock, a shared lock and a transaction-level
-- lock; finish refuses unless its caller is the live owner, so 'published'
-- always comes from a run that still held its lock.
-- Grants (review R9): app_readonly executes only publication_status and
-- publication_lock_key, via the allowlist in
-- sql/security/enable_readonly_rls.sql (a grant here alone would not survive
-- the next security re-apply). publication_status is SECURITY DEFINER because
-- pg_stat_activity hides other roles' backend_start from app_readonly
-- (verified 2026-10-20); it only reads. begin/fence/finish and
-- publication_owner_live are on no app allowlist. The existing hardening does
-- grant service_role EXECUTE on every function in this schema, but
-- service_role has no UPDATE on publication, so its calls to the writers
-- fail: in practice only the owner (the ETL / loader login) can run them.
CREATE TABLE IF NOT EXISTS __SCHEMA__.publication (
  league text PRIMARY KEY CHECK (league IN ('il', 'euroleague', 'eurocup')),
  state text NOT NULL CHECK (state IN ('publishing', 'published', 'failed')),
  version bigint NOT NULL CHECK (version > 0),
  owner_run uuid,
  owner_pid integer,
  owner_backend_start timestamptz,
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

-- TRUE when backend p_pid, started at p_backend_start, holds the league's
-- advisory lock in ExclusiveLock mode in this database: the one liveness
-- predicate, used by publication_status() and finish_publication().
CREATE OR REPLACE FUNCTION __SCHEMA__.publication_owner_live(
  p_league text, p_pid integer, p_backend_start timestamptz)
RETURNS boolean LANGUAGE sql STABLE SET search_path = '' AS $$
  SELECT EXISTS (
    SELECT 1
    FROM pg_catalog.pg_locks l
    JOIN pg_catalog.pg_stat_activity a ON a.pid = l.pid
    WHERE l.locktype = 'advisory' AND l.granted AND l.mode = 'ExclusiveLock'
      AND l.pid = p_pid AND a.backend_start = p_backend_start
      AND l.database = (SELECT d.oid FROM pg_catalog.pg_database d
                        WHERE d.datname = pg_catalog.current_database())
      AND l.classid = (__SCHEMA__.publication_lock_key(p_league) >> 32)::oid
      AND l.objid = (__SCHEMA__.publication_lock_key(p_league) & 4294967295)::oid
      AND l.objsubid = 1
  )
$$;

CREATE OR REPLACE FUNCTION __SCHEMA__.begin_publication(p_league text, p_run uuid)
RETURNS void LANGUAGE plpgsql SET search_path = '' AS $$
DECLARE
  k bigint := __SCHEMA__.publication_lock_key(p_league);
  v_start timestamptz;
BEGIN
  IF p_run IS NULL THEN
    RAISE EXCEPTION 'publication advisory lock required (no run id)';
  END IF;
  -- The caller must hold the lock at SESSION level in EXCLUSIVE mode. pg_locks
  -- cannot tell session from transaction level, so probe it:
  -- 1. take a transaction-level exclusive hold of our own (false if another
  --    session holds the key); it keeps the lock held through step 2;
  -- 2. pg_advisory_unlock releases only a session-level exclusive hold, and is
  --    false when there is none (no lock, a shared lock, or only an xact lock);
  -- 3. re-take the session hold, immediately, because step 1 still holds it.
  IF NOT pg_catalog.pg_try_advisory_xact_lock(k) THEN
    RAISE EXCEPTION 'publication advisory lock required (held by another session)';
  END IF;
  IF NOT pg_catalog.pg_advisory_unlock(k) THEN
    RAISE EXCEPTION 'publication advisory lock required (session-level, exclusive)';
  END IF;
  PERFORM pg_catalog.pg_advisory_lock(k);
  SELECT a.backend_start INTO v_start
    FROM pg_catalog.pg_stat_activity a WHERE a.pid = pg_catalog.pg_backend_pid();
  -- FOR UPDATE waits for all fenced, in-flight write transactions to finish.
  PERFORM 1 FROM __SCHEMA__.publication WHERE league = p_league FOR UPDATE;
  IF NOT FOUND THEN RAISE EXCEPTION 'unknown publication league'; END IF;
  -- A 'publishing' row here is necessarily stale: we hold the exclusive lock.
  UPDATE __SCHEMA__.publication SET version = version + 1, state = 'failed',
    finished_at = clock_timestamp()
    WHERE league = p_league AND state = 'publishing';
  UPDATE __SCHEMA__.publication SET state = 'publishing', owner_run = p_run,
    owner_pid = pg_catalog.pg_backend_pid(), owner_backend_start = v_start,
    started_at = clock_timestamp(), finished_at = NULL
    WHERE league = p_league;
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

-- Only the live owner finishes: the caller must be the recorded owner backend
-- and still hold the lock, so 'published' implies a live owner.
CREATE OR REPLACE FUNCTION __SCHEMA__.finish_publication(p_league text, p_run uuid, p_ok boolean)
RETURNS bigint LANGUAGE plpgsql SET search_path = '' AS $$
DECLARE v bigint;
BEGIN
  UPDATE __SCHEMA__.publication p
    SET state = CASE WHEN p_ok THEN 'published' ELSE 'failed' END,
        version = p.version + 1, finished_at = clock_timestamp()
    WHERE p.league = p_league AND p.owner_run = p_run AND p.state = 'publishing'
      AND p.owner_pid = pg_catalog.pg_backend_pid()
      AND __SCHEMA__.publication_owner_live(p.league, p.owner_pid, p.owner_backend_start)
    RETURNING p.version INTO v;
  IF NOT FOUND THEN
    RAISE EXCEPTION 'fenced_out (not the live owner of this publication)';
  END IF;
  RETURN v;
END
$$;

CREATE OR REPLACE FUNCTION __SCHEMA__.publication_status(p_league text)
RETURNS TABLE(state text, version bigint)
LANGUAGE sql STABLE SECURITY DEFINER SET search_path = '' AS $$
  SELECT CASE WHEN p.state = 'publishing'
               AND NOT __SCHEMA__.publication_owner_live(p.league, p.owner_pid,
                                                         p.owner_backend_start)
              THEN 'stale' ELSE p.state END,
         p.version
  FROM __SCHEMA__.publication p WHERE p.league = p_league
$$;

REVOKE ALL ON __SCHEMA__.publication FROM PUBLIC;
REVOKE ALL ON FUNCTION __SCHEMA__.publication_lock_key(text),
  __SCHEMA__.publication_owner_live(text, integer, timestamptz),
  __SCHEMA__.begin_publication(text, uuid), __SCHEMA__.fence_publication(text, uuid),
  __SCHEMA__.finish_publication(text, uuid, boolean), __SCHEMA__.publication_status(text)
  FROM PUBLIC;
GRANT SELECT ON __SCHEMA__.publication TO app_readonly;
GRANT EXECUTE ON FUNCTION __SCHEMA__.publication_lock_key(text),
  __SCHEMA__.publication_status(text) TO app_readonly;
