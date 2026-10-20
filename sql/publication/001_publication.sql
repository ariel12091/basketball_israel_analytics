-- Substitute __SCHEMA__ with a quoted identifier, including in function bodies.
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
