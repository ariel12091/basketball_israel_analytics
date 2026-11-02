-- The possession ends the special-foul rule removes from stored rows of 2024-25 and 2025-26:
-- the same rule as compute_possessions() (etl/possession_rules.R), applied to
-- df_pts_poss_lineups_longer_mv, whose rows carry everything the rule reads.
--   * a row with final_end_poss in a (game_id, parent_action_id) group holding a foul labelled
--     technical*, *unsportsmanlike* or disqualif*,
--   * except a turnover (the offense did lose the ball) and an offensive rebound marked as a period end,
--   * except the last possession end of its period when nothing but substitutions and period
--     markers follows it (the period ended the possession).
-- Read-only. scripts/apply_special_foul_backfill.R applies the result.
WITH games AS (
  SELECT DISTINCT game_id FROM basketball_test.final_schedule_mv WHERE game_year = ANY($1::int4[])
),
actions AS (
  SELECT DISTINCT p.game_id, p.id, p.parent_action_id, p.quarter, p.type, p.parameters_type, p.final_end_poss
  FROM basketball_test.df_pts_poss_lineups_longer_mv p JOIN games USING (game_id)
),
special_groups AS (
  SELECT DISTINCT game_id, parent_action_id
  FROM actions
  WHERE lower(parameters_type) LIKE '%technical%'
     OR lower(parameters_type) LIKE '%unsportsmanlike%'
     OR lower(parameters_type) LIKE '%disqualif%'
),
last_play AS (
  SELECT game_id, quarter, max(id) AS last_id
  FROM actions
  WHERE type <> 'substitution' AND coalesce(parameters_type, '') <> 'end-of-quarter' AND type NOT IN ('period', 'timeout')
  GROUP BY 1, 2
)
SELECT a.game_id, a.id, a.parent_action_id, a.quarter, a.type, a.parameters_type
FROM actions a
JOIN special_groups g USING (game_id, parent_action_id)
JOIN last_play l ON l.game_id = a.game_id AND l.quarter = a.quarter
WHERE a.final_end_poss
  AND a.type NOT IN ('turnover', 'rebound')
  AND NOT (a.type = 'freeThrow' AND a.id = l.last_id)
ORDER BY 1, 2
