-- EuroLeague migration 062: free throws credited to the lineup that drew the
-- foul, for the EUROLEAGUE SHADOW SCHEMA
-- (docs/superpowers/plans/2026-11-13-euroleague-ft-foul-credit.md, Task 2).
--
-- The Israeli rule (docs/shared_definitions.md, "Credited lineup"): a free
-- throw belongs to the lineup on its parent action's row -- the foul, or the
-- made shot of an and-one -- from the same team's perspective. Every other row
-- belongs to its own lineup. Until now EuroLeague counted each free throw with
-- the lineup on the floor when it was shot (deviation D1); on 2026-11-13 that
-- moved about 30% of free-throw points, every one across a substitution made
-- between the foul and the shot.
--
-- This migration only adds and fills the credit columns. Readers keep reading
-- the physical ones until their own task switches them (events to credit_*,
-- time stays physical). The fact's refresh fills the columns itself, so the
-- weekly load needs no new step:
--   * refresh_actions_consumer_candidates writes each row's own lineup into
--     the credit columns, then calls refresh_ft_credit_fields_for_games;
--   * that function re-credits the free throws from their parent rows.
-- scripts/apply_062_ft_credit_columns.py builds the new
-- refresh_actions_consumer_candidates from its LIVE definition with three
-- exact edits and appends it below this file's statements when it runs.

ALTER TABLE euroleague.action_team_context_actions
  ADD COLUMN credit_own_lineup text[],
  ADD COLUMN credit_opp_lineup text[],
  ADD COLUMN credit_own_starters smallint,
  ADD COLUMN credit_opp_starters smallint,
  ADD COLUMN credit_segment_id integer;

COMMENT ON COLUMN euroleague.action_team_context_actions.credit_own_lineup IS
  'Lineup an event counts for: a free throw takes its parent row''s (the foul, or the made shot of an and-one; same team perspective), every other row its own. Events group on credit_*, time on the physical columns (migration 062).';

CREATE OR REPLACE FUNCTION euroleague.refresh_ft_credit_fields_for_games(game_ids bigint[])
RETURNS bigint
LANGUAGE plpgsql
SET plan_cache_mode = force_custom_plan
AS $function$
DECLARE
  recredited bigint := 0;
BEGIN
  -- Every row first takes its own lineup.
  UPDATE euroleague.action_team_context_actions f
     SET credit_own_lineup = f.own_lineup,
         credit_opp_lineup = f.opp_lineup,
         credit_own_starters = f.own_starters,
         credit_opp_starters = f.opp_starters,
         credit_segment_id = f.segment_id
   WHERE (game_ids IS NULL OR f.game_id = ANY(game_ids))
     AND (f.credit_own_lineup IS DISTINCT FROM f.own_lineup
       OR f.credit_opp_lineup IS DISTINCT FROM f.opp_lineup
       OR f.credit_own_starters IS DISTINCT FROM f.own_starters
       OR f.credit_opp_starters IS DISTINCT FROM f.opp_starters
       OR f.credit_segment_id IS DISTINCT FROM f.segment_id);

  -- A free throw then takes its parent row's, when the parent is not itself
  -- a free throw: same game, same team perspective.
  UPDATE euroleague.action_team_context_actions f
     SET credit_own_lineup = p.own_lineup,
         credit_opp_lineup = p.opp_lineup,
         credit_own_starters = p.own_starters,
         credit_opp_starters = p.opp_starters,
         credit_segment_id = p.segment_id
    FROM euroleague.actions a,
         euroleague.action_team_context_actions p
   WHERE (game_ids IS NULL OR f.game_id = ANY(game_ids))
     AND f.play_type IN ('FTM', 'FTA')
     AND a.game_id = f.game_id
     AND a.source_event_order = f.source_event_order
     AND a.synthetic_parent_order <> f.source_event_order
     AND p.game_id = f.game_id
     AND p.source_event_order = a.synthetic_parent_order
     AND p.team_id = f.team_id
     AND p.play_type NOT IN ('FTM', 'FTA');
  GET DIAGNOSTICS recredited = ROW_COUNT;
  RETURN recredited;
END;
$function$;
