CREATE OR REPLACE FUNCTION basketball_test.compute_player_stats_actions_by_game(
  p_game_ids int4[] DEFAULT NULL
)
RETURNS TABLE (
  game_year int4,
  game_id int4,
  team_id int4,
  action_id int4,
  lineup_hash text,
  segment_id bigint,
  event_elapsed_seconds numeric,
  type_lineup text,
  possession_flag int2,
  action_player_id int4,
  points int2,
  oreb int2,
  dreb int2,
  assists int2,
  steals int2,
  blocks int2,
  deflections int2,
  turnovers int2,
  fgm int2,
  fga int2,
  fg3_made int2,
  fg3_att int2,
  ftm int2,
  ft_attempts int2,
  shot_ts_possession int2,
  personal_ft_trip_id int4,
  is_overtime boolean,
  regulation_seconds_remaining numeric,
  pre_margin int4,
  pre_abs_margin int4,
  pre_status int2
)
LANGUAGE sql
STABLE
AS $$
WITH source_games AS (
  SELECT DISTINCT fs.game_year::int4, fs.game_id::int4, fs.team_id::int4
  FROM basketball_test.final_schedule_mv fs
  WHERE p_game_ids IS NULL OR fs.game_id = ANY(p_game_ids)
),
source_actions AS MATERIALIZED (
  SELECT d.*, sg.game_year
  FROM basketball_test.df_pts_poss_lineups_longer_mv d
  JOIN source_games sg
    ON sg.game_id = d.game_id
   AND sg.team_id = d.team_id
),
personal_foul_parents AS MATERIALIZED (
  SELECT DISTINCT a.game_id, a.id AS action_id, a.parent_action_id
  FROM source_actions a
  JOIN basketball_test.df_pts_poss_lineups_longer_mv parent
    ON parent.game_id = a.game_id
   AND parent.id = a.parent_action_id
   AND parent.type = 'foul'
   AND parent.parameters_type = 'personal'
  WHERE a.type = 'freeThrow'
    AND a.parent_action_id IS NOT NULL
)
SELECT
  a.game_year,
  a.game_id::int4,
  a.team_id::int4,
  a.id::int4,
  a.lineup_hash,
  a.segment_id::bigint,
  a.event_elapsed_seconds::numeric,
  a.type_lineup,
  CASE WHEN a.type_lineup = 'offense' AND a.final_end_poss THEN 1 ELSE 0 END::int2,
  a.player_id::int4,
  CASE
    WHEN a.type = 'shot' AND a.parameters_made = 'made' AND a.type_lineup = 'offense'
      THEN COALESCE(a.parameters_points, 0)
    WHEN a.type = 'freeThrow' AND a.parameters_made = 'made' AND a.type_lineup = 'offense'
      THEN 1
    ELSE 0
  END::int2,
  CASE WHEN a.type = 'rebound' AND a.type_lineup = 'offense'
             AND a.parameters_type = 'offensive' THEN 1 ELSE 0 END::int2,
  CASE WHEN a.type = 'rebound' AND a.type_lineup = 'defense'
             AND a.parameters_type = 'defensive' THEN 1 ELSE 0 END::int2,
  CASE WHEN a.type = 'assist' AND a.type_lineup = 'offense' THEN 1 ELSE 0 END::int2,
  CASE WHEN a.type = 'steal' AND a.type_lineup = 'defense' THEN 1 ELSE 0 END::int2,
  CASE WHEN a.type = 'block' AND a.type_lineup = 'defense' THEN 1 ELSE 0 END::int2,
  CASE WHEN a.type = 'deflection' AND a.type_lineup = 'defense' THEN 1 ELSE 0 END::int2,
  CASE WHEN a.type = 'turnover' AND a.type_lineup = 'offense' THEN 1 ELSE 0 END::int2,
  CASE WHEN a.type = 'shot' AND a.parameters_made = 'made'
             AND a.type_lineup = 'offense' THEN 1 ELSE 0 END::int2,
  CASE WHEN a.type = 'shot' AND a.type_lineup = 'offense' THEN 1 ELSE 0 END::int2,
  CASE WHEN a.type = 'shot' AND a.parameters_made = 'made'
             AND a.parameters_points = 3 AND a.type_lineup = 'offense' THEN 1 ELSE 0 END::int2,
  CASE WHEN a.type = 'shot' AND a.parameters_points = 3
             AND a.type_lineup = 'offense' THEN 1 ELSE 0 END::int2,
  CASE WHEN a.type = 'freeThrow' AND a.parameters_made = 'made'
             AND a.type_lineup = 'offense' THEN 1 ELSE 0 END::int2,
  CASE WHEN a.type = 'freeThrow' AND a.type_lineup = 'offense' THEN 1 ELSE 0 END::int2,
  CASE WHEN a.type = 'shot' AND a.type_lineup = 'offense' THEN 1 ELSE 0 END::int2,
  CASE WHEN a.type_lineup = 'offense' THEN pfp.parent_action_id END::int4,
  (a.quarter > 4),
  a.end_game_seconds_remaining::numeric,
  (
    CASE
      WHEN a.type_lineup = 'offense'
        THEN (COALESCE(a.own_team_score, 0) - COALESCE(a.team_score, 0))
             - COALESCE(a.opp_team_score, 0)
      ELSE COALESCE(a.own_team_score, 0)
           - (COALESCE(a.opp_team_score, 0) - COALESCE(a.team_score, 0))
    END
  )::int4,
  ABS(
    CASE
      WHEN a.type_lineup = 'offense'
        THEN (COALESCE(a.own_team_score, 0) - COALESCE(a.team_score, 0))
             - COALESCE(a.opp_team_score, 0)
      ELSE COALESCE(a.own_team_score, 0)
           - (COALESCE(a.opp_team_score, 0) - COALESCE(a.team_score, 0))
    END
  )::int4,
  SIGN(
    CASE
      WHEN a.type_lineup = 'offense'
        THEN (COALESCE(a.own_team_score, 0) - COALESCE(a.team_score, 0))
             - COALESCE(a.opp_team_score, 0)
      ELSE COALESCE(a.own_team_score, 0)
           - (COALESCE(a.opp_team_score, 0) - COALESCE(a.team_score, 0))
    END
  )::int2
FROM source_actions a
LEFT JOIN personal_foul_parents pfp
  ON pfp.game_id = a.game_id
 AND pfp.action_id = a.id
$$;
