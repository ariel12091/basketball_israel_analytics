-- The phantom possession ends the phantom-ends rule removes from stored rows of 2024-25, 2025-26
-- and 2026-27: the same rule as the last step of compute_possessions() (etl/possession_rules.R),
-- applied to df_pts_poss_lineups_longer_mv after the and-one step. An end is removed when it is
--   (1) the rebound of a put-back: an offensive rebound that is an end, whose previous end (same
--       game and quarter, by id) is the same team's made shot, and whose parent_action_id is lower
--       than that shot's id;
--   (2) a foul turnover (a turnover whose parent_action_id is the id or the parent of an
--       OffensiveFoulUnsportsmanlike foul in the same game) that is an end and either
--       (a) has the same team's end before it, with no shot or offensive rebound of that team at
--           an id between that end and the turnover, or
--       (b) follows the previous foul turnover of the game and quarter in the same
--           end_game_seconds_remaining, with no live play (shot, rebound, freeThrow, steal, block,
--           assist, deflection, by either team) between them.
-- In the offense rows of the view a foul's, steal's, block's and defensive rebound's team_id is the
-- other team's (the perspective); only turnovers, shots and offensive rebounds name the team itself,
-- Team ids are compared on the previous end whatever its type; stored ends are shots, turnovers,
-- free throws, assists, rebounds and one foul-drawn row on 2026-11-03, with no foul, block,
-- deflection or steal row.
-- Window functions only: a correlated subquery over a CTE on this view ran over 2 minutes.
-- Read-only. scripts/apply_possession_end_backfill.R --rule phantom-ends applies the result.
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
         max(p.team_id) AS team_id, max(p.end_game_seconds_remaining) AS end_game_seconds_remaining,
         bool_or(coalesce(p.final_end_poss, FALSE)) AS final_end_poss
  FROM basketball_test.df_pts_poss_lineups_longer_mv p JOIN games USING (game_id)
  WHERE p.type_lineup = 'offense'
  GROUP BY p.game_id, p.id
),
-- rows:end
flags AS (
  SELECT r.*,
         final_end_poss AS is_end,
         (type = 'foul' AND parameters_type = 'OffensiveFoulUnsportsmanlike') AS ofu,
         CASE WHEN type IN ('shot', 'rebound', 'freeThrow', 'steal', 'block', 'assist', 'deflection') THEN id END AS live_id,
         CASE WHEN type = 'shot' OR (type = 'rebound' AND parameters_type = 'offensive') THEN id END AS own_id
  FROM rows r
),
foul_parent AS (
  SELECT f.*,
         bool_or(ofu) OVER (PARTITION BY game_id, parent_action_id) AS ofu_same_parent,
         CASE WHEN ofu THEN id WHEN type = 'turnover' THEN parent_action_id END AS foul_key
  FROM flags f
),
foul_id AS (
  SELECT f.*,
         coalesce(bool_or(ofu) OVER (PARTITION BY game_id, foul_key), FALSE) AS ofu_same_id
  FROM foul_parent f
),
kinds AS (
  SELECT f.*,
         (type = 'turnover' AND (ofu_same_parent OR ofu_same_id)) AS foul_turnover
  FROM foul_id f
),
live_cum AS (
  SELECT k.*, max(live_id) OVER (PARTITION BY game_id, quarter ORDER BY id) AS live_cum
  FROM kinds k
),
live AS (
  SELECT c.*, lag(live_cum) OVER (PARTITION BY game_id, quarter ORDER BY id) AS live_before
  FROM live_cum c
),
prev_end AS (
  SELECT l.*,
         lag(id) OVER w AS prev_end_id, lag(team_id) OVER w AS prev_end_team,
         lag(type) OVER w AS prev_end_type, lag(parameters_made) OVER w AS prev_end_made
  FROM live l
  WINDOW w AS (PARTITION BY game_id, quarter, is_end ORDER BY id)
),
prev_ftt AS (
  SELECT e.*,
         lag(id) OVER w AS prev_ftt_id, lag(end_game_seconds_remaining) OVER w AS prev_ftt_secs
  FROM prev_end e
  WINDOW w AS (PARTITION BY game_id, quarter, foul_turnover ORDER BY id)
),
own_cum AS (
  SELECT t.*, max(own_id) OVER (PARTITION BY game_id, quarter, team_id ORDER BY id) AS own_cum
  FROM prev_ftt t
),
own AS (
  SELECT c.*, lag(own_cum) OVER (PARTITION BY game_id, quarter, team_id ORDER BY id) AS own_before
  FROM own_cum c
)
SELECT o.game_id, o.id, o.parent_action_id, o.quarter, o.type, o.parameters_type
FROM own o
WHERE o.is_end AND (
  -- (1) the rebound of a put-back
  (o.type = 'rebound' AND o.parameters_type = 'offensive' AND o.prev_end_team = o.team_id
     AND o.prev_end_type = 'shot' AND o.prev_end_made = 'made' AND o.parent_action_id < o.prev_end_id)
  -- (2a) a foul turnover of the team whose end came just before, with no shot or offensive rebound since
  OR (o.foul_turnover AND o.prev_end_team = o.team_id AND (o.own_before IS NULL OR o.own_before <= o.prev_end_id))
  -- (2b) a second foul turnover in the same second with no live play between
  OR (o.foul_turnover AND o.prev_ftt_id IS NOT NULL AND o.prev_ftt_secs = o.end_game_seconds_remaining
     AND (o.live_before IS NULL OR o.live_before < o.prev_ftt_id))
)
ORDER BY 1, 2
