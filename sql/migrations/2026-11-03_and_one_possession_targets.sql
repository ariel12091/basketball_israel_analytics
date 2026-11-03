-- The possession ends the and-one rule removes from stored rows of 2024-25, 2025-26 and 2026-27:
-- the same rule as compute_possessions() (etl/possession_rules.R), applied to
-- df_pts_poss_lineups_longer_mv. A made shot loses its possession end when
--   * the last free throw of its group (game_id, parent_action_id) was missed,
--   * the group holds an offensive rebound at a higher id than that free throw, and
--   * the next end after the made shot (by id, same game and quarter) is the same team's.
-- A shot that is not an end now (the old rule moved the end to a made last free throw) is not selected.
-- Window functions only: a correlated subquery over a CTE ran over 2 minutes on this view.
-- Read-only. scripts/apply_possession_end_backfill.R --rule and-one applies the result.
-- $1 is the int4[] of seasons; scripts/pin_possession_rule_targets.R swaps the text between the
-- rows markers for fixture rows (one row per action, offense perspective) and runs it without $1.
-- rows:begin
WITH games AS (
  SELECT DISTINCT game_id FROM basketball_test.final_schedule_mv WHERE game_year = ANY($1::int4[])
),
rows AS (
  SELECT p.game_id, p.id,
         max(p.parent_action_id) AS parent_action_id, max(p.quarter) AS quarter, max(p.type) AS type,
         max(p.parameters_type) AS parameters_type, max(p.parameters_made) AS parameters_made,
         max(p.team_id) AS team_id, bool_or(coalesce(p.final_end_poss, FALSE)) AS final_end_poss
  FROM basketball_test.df_pts_poss_lineups_longer_mv p JOIN games USING (game_id)
  WHERE p.type_lineup = 'offense'
  GROUP BY p.game_id, p.id
),
-- rows:end
grp AS (
  SELECT r.*,
         max(CASE WHEN type = 'freeThrow' THEN id END) OVER w AS last_ft_id
  FROM rows r
  WINDOW w AS (PARTITION BY game_id, parent_action_id)
),
grp2 AS (
  SELECT g.*,
         bool_or(type = 'freeThrow' AND id = last_ft_id AND parameters_made = 'missed') OVER w AS last_ft_missed,
         bool_or(type = 'rebound' AND parameters_type = 'offensive' AND id > last_ft_id) OVER w AS own_rebound_after
  FROM grp g
  WINDOW w AS (PARTITION BY game_id, parent_action_id)
),
next_end AS (
  SELECT game_id, id, lead(team_id) OVER (PARTITION BY game_id, quarter ORDER BY id) AS next_team
  FROM rows
  WHERE final_end_poss
)
SELECT c.game_id, c.id, c.parent_action_id, c.quarter, c.type, c.parameters_type
FROM grp2 c
JOIN next_end n ON n.game_id = c.game_id AND n.id = c.id
WHERE c.id = c.parent_action_id AND c.type = 'shot' AND c.parameters_made = 'made' AND c.final_end_poss
  AND c.last_ft_missed AND c.own_rebound_after AND n.next_team = c.team_id
ORDER BY 1, 2
