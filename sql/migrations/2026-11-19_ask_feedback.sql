-- Ask chat feedback (2026-11-19): a visitor who presses "Wrong answer?" and
-- then Send stores that one question, the chat's answer, the lookups it ran
-- and an optional note. Nothing else about the visitor is stored (no IP, no
-- session or browser id). Applied by scripts/apply_ask_feedback.R.
--
-- Its own schema, not basketball_test: that schema grants app_readonly
-- SELECT on every relation (sql/security/enable_readonly_rls.sql, blanket
-- mode), and visitors' questions must not be readable by the app's role.
-- app_readonly gets USAGE on the schema and EXECUTE on the one function, and
-- nothing on the table; the function runs as its owner (SECURITY DEFINER)
-- with a fixed search_path.
--
-- The function caps every text (the table's CHECKs say the same) and refuses
-- once 200 flags arrived in the last hour, so a flood cannot grow the table
-- without bound whatever the per-instance limit in the web app lets through.

CREATE SCHEMA IF NOT EXISTS app_feedback;
REVOKE ALL ON SCHEMA app_feedback FROM PUBLIC;

CREATE TABLE IF NOT EXISTS app_feedback.ask_feedback (
  feedback_id bigint GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
  created_at timestamptz NOT NULL DEFAULT now(),
  league text NOT NULL CHECK (league IN ('il', 'euroleague', 'eurocup')),
  question text NOT NULL CHECK (char_length(question) BETWEEN 1 AND 500),
  answer text CHECK (char_length(answer) <= 4000),
  calls jsonb CHECK (octet_length(calls::text) <= 8000),
  note text CHECK (char_length(note) <= 500),
  model text CHECK (char_length(model) <= 100),
  reviewed_at timestamptz
);
COMMENT ON TABLE app_feedback.ask_feedback IS
  'Ask chat answers a visitor flagged as wrong and agreed to send (web /api/v1/ask/feedback). No visitor identifiers.';
REVOKE ALL ON app_feedback.ask_feedback FROM PUBLIC;
ALTER TABLE app_feedback.ask_feedback ENABLE ROW LEVEL SECURITY;

CREATE OR REPLACE FUNCTION app_feedback.submit_ask_feedback(
  p_league text,
  p_question text,
  p_answer text,
  p_calls jsonb,
  p_note text,
  p_model text
) RETURNS bigint
LANGUAGE sql
SECURITY DEFINER
SET search_path = pg_catalog, pg_temp
AS $function$
  INSERT INTO app_feedback.ask_feedback (league, question, answer, calls, note, model)
  SELECT p_league, left(btrim(p_question), 500), left(p_answer, 4000),
         CASE WHEN octet_length(p_calls::text) <= 8000 THEN p_calls END,
         NULLIF(left(btrim(p_note), 500), ''), left(p_model, 100)
  WHERE (SELECT count(*) FROM app_feedback.ask_feedback
         WHERE created_at > now() - interval '1 hour') < 200
  RETURNING feedback_id
$function$;
REVOKE ALL ON FUNCTION app_feedback.submit_ask_feedback(text, text, text, jsonb, text, text) FROM PUBLIC;

GRANT USAGE ON SCHEMA app_feedback TO app_readonly;
GRANT EXECUTE ON FUNCTION app_feedback.submit_ask_feedback(text, text, text, jsonb, text, text) TO app_readonly;
