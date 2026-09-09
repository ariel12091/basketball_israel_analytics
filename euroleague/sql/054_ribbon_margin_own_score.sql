-- EUROLEAGUE SHADOW SCHEMA -- migration 054: own_team_score on the ribbon
-- margin view.
--
-- The stint ribbon prints each stint's points FOR and AGAINST, taken as net
-- differences across the stint window. pf - pa is the margin delta, which
-- ribbon_margin_v already exposes; pf + pa is NOT derivable from a margin,
-- so exactly one more column is required.
--
-- euroleague.action_team_context_actions -- which this view already reads --
-- carries own_team_score and opp_team_score and merely subtracts them, so
-- the column is free: no new relation, no new grant, no extra round trip.
--
-- This is CREATE OR REPLACE VIEW with the new column APPENDED at the end of
-- the select list, which is the one shape Postgres allows without dropping
-- the view. That matters: DROP + CREATE would wipe app_readonly's SELECT
-- grant (see the note at the head of 053), and the grant audit would then
-- report the ribbon as unreadable. Verify the grant after applying rather
-- than assuming it survived.

BEGIN;
SET LOCAL search_path TO euroleague, public;

CREATE OR REPLACE VIEW euroleague.ribbon_margin_v AS
SELECT
  game_id,
  team_id,
  period,
  event_elapsed_seconds AS elapsed_seconds,
  (own_team_score - opp_team_score) AS margin,
  source_event_order,
  own_team_score
FROM euroleague.action_team_context_actions
WHERE points > 0;

COMMIT;

-- ---------------------------------------------------------------------------
-- VERIFY after applying (read-only, safe to run any time):
--
--   -- 1. the column is there
--   SELECT own_team_score FROM euroleague.ribbon_margin_v LIMIT 1;
--
--   -- 2. the grant SURVIVED the replace -- expect true
--   SELECT has_table_privilege('app_readonly',
--            'euroleague.ribbon_margin_v', 'SELECT');
--
--   -- 3. the raw sources are still closed -- expect false for each
--   SELECT has_table_privilege('app_readonly',
--            'euroleague.action_team_context_actions', 'SELECT');
--
-- If (2) comes back false, re-run scripts/apply_db_security.R with
-- CONFIRM_DB_SECURITY_APPLY=1. If it comes back true, do NOT re-run it --
-- nothing was lost and there is nothing to restore.
-- ---------------------------------------------------------------------------
