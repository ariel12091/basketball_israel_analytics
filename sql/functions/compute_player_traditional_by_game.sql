CREATE OR REPLACE FUNCTION basketball_test.compute_player_traditional_by_game(
  p_game_ids int4[] DEFAULT NULL
)
RETURNS TABLE (
  game_year int4,
  game_id int4,
  team_id int4,
  player_id int4,
  has_actor_stats boolean,
  gp int4,
  poss_on_floor int4,
  seconds_on_floor numeric,
  pts int4,
  reb int4,
  oreb int4,
  dreb int4,
  ast int4,
  stl int4,
  blk int4,
  dfl int4,
  tov int4,
  fgm int4,
  fga int4,
  "3pm" int4,
  "3pa" int4,
  ftm int4,
  fta int4,
  player_ts_poss_count int4,
  team_ts_poss_count int4,
  team_tov int4,
  team_poss int4
)
LANGUAGE sql
STABLE
AS $$
WITH source_games AS (
  SELECT DISTINCT fs.game_year::int4, fs.game_id::int4, fs.team_id::int4
  FROM basketball_test.final_schedule_mv fs
  WHERE p_game_ids IS NULL OR fs.game_id = ANY(p_game_ids)
),
roster_season AS MATERIALIZED (
  SELECT DISTINCT sg.game_year, fr.team_id::int4, fr.player_id::int4
  FROM basketball_test.full_rosters fr
  JOIN (SELECT DISTINCT game_year, team_id FROM source_games) sg
    ON sg.game_year = fr.game_year AND sg.team_id = fr.team_id
  WHERE fr.player_id IS NOT NULL AND fr.player_id > 0
),
lineup_map AS (
  SELECT DISTINCT ll.game_id, ll.team_id, ll.lineup_hash, ll.player_id
  FROM basketball_test.lineups_lookup ll
  JOIN source_games sg ON sg.game_id = ll.game_id AND sg.team_id = ll.team_id
  WHERE COALESCE(ll.is_on_verdict, 0)::int = 1
),
actions_base AS (
  SELECT d.*, sg.game_year
  FROM basketball_test.df_pts_poss_lineups_longer_mv d
  JOIN source_games sg ON sg.game_id = d.game_id AND sg.team_id = d.team_id
),
complex_flags AS (
  SELECT DISTINCT ON (d.game_id, d.id)
    d.game_id, d.id AS main_id,
    parent.type AS parent_type,
    parent.parameters_type AS parent_param
  FROM actions_base d
  JOIN basketball_test.df_pts_poss_lineups_longer_mv parent
    ON parent.game_id = d.game_id
   AND parent.id = d.parent_action_id
   AND parent.type = 'foul'
  WHERE d.parent_action_id IS NOT NULL
  ORDER BY d.game_id, d.id
),
actor_game AS (
  SELECT
    d.game_year, d.game_id, d.team_id, d.player_id,
    (SUM(CASE WHEN d.type = 'shot' AND d.parameters_made = 'made' AND d.type_lineup = 'offense'
              THEN COALESCE(d.parameters_points, 0) ELSE 0 END)
     + SUM(CASE WHEN d.type = 'freeThrow' AND d.parameters_made = 'made' AND d.type_lineup = 'offense'
                THEN 1 ELSE 0 END))::int4 AS pts,
    SUM(CASE WHEN d.type = 'rebound' AND d.type_lineup = 'offense' AND d.parameters_type = 'offensive' THEN 1 ELSE 0 END)::int4 AS oreb,
    SUM(CASE WHEN d.type = 'rebound' AND d.type_lineup = 'defense' AND d.parameters_type = 'defensive' THEN 1 ELSE 0 END)::int4 AS dreb,
    SUM(CASE WHEN d.type = 'assist' AND d.type_lineup = 'offense' THEN 1 ELSE 0 END)::int4 AS ast,
    SUM(CASE WHEN d.type = 'steal' AND d.type_lineup = 'defense' THEN 1 ELSE 0 END)::int4 AS stl,
    SUM(CASE WHEN d.type = 'block' AND d.type_lineup = 'defense' THEN 1 ELSE 0 END)::int4 AS blk,
    SUM(CASE WHEN d.type = 'deflection' AND d.type_lineup = 'defense' THEN 1 ELSE 0 END)::int4 AS dfl,
    SUM(CASE WHEN d.type = 'turnover' AND d.type_lineup = 'offense' THEN 1 ELSE 0 END)::int4 AS tov,
    SUM(CASE WHEN d.type = 'shot' AND d.parameters_made = 'made' AND d.type_lineup = 'offense' THEN 1 ELSE 0 END)::int4 AS fgm,
    SUM(CASE WHEN d.type = 'shot' AND d.type_lineup = 'offense' THEN 1 ELSE 0 END)::int4 AS fga,
    SUM(CASE WHEN d.type = 'shot' AND d.parameters_made = 'made' AND d.parameters_points = 3 AND d.type_lineup = 'offense' THEN 1 ELSE 0 END)::int4 AS "3pm",
    SUM(CASE WHEN d.type = 'shot' AND d.parameters_points = 3 AND d.type_lineup = 'offense' THEN 1 ELSE 0 END)::int4 AS "3pa",
    SUM(CASE WHEN d.type = 'freeThrow' AND d.parameters_made = 'made' AND d.type_lineup = 'offense' THEN 1 ELSE 0 END)::int4 AS ftm,
    SUM(CASE WHEN d.type = 'freeThrow' AND d.type_lineup = 'offense' THEN 1 ELSE 0 END)::int4 AS fta,
    (COUNT(CASE WHEN d.type = 'shot' AND d.type_lineup = 'offense' THEN 1 END)
     + COUNT(DISTINCT CASE
         WHEN d.type = 'freeThrow' AND d.type_lineup = 'offense'
          AND cf.parent_type = 'foul' AND cf.parent_param = 'personal'
         THEN d.parent_action_id END))::int4 AS player_ts_poss_count
  FROM actions_base d
  LEFT JOIN complex_flags cf ON cf.game_id = d.game_id AND cf.main_id = d.id
  WHERE d.player_id IS NOT NULL AND d.player_id > 0
  GROUP BY d.game_year, d.game_id, d.team_id, d.player_id
),
poss_end AS (
  SELECT DISTINCT d.game_year, d.game_id, d.team_id, d.lineup_hash, d.id AS poss_end_id
  FROM actions_base d
  WHERE d.type_lineup = 'offense' AND d.final_end_poss
    AND d.id IS NOT NULL AND d.lineup_hash IS NOT NULL
),
usage_totals AS (
  SELECT pe.game_year, pe.game_id, lm.team_id, lm.player_id,
         1::int4 AS gp,
         COUNT(DISTINCT (pe.game_id, pe.team_id, pe.poss_end_id))::int4 AS poss_on_floor
  FROM poss_end pe
  JOIN lineup_map lm
    ON lm.game_id = pe.game_id AND lm.team_id = pe.team_id AND lm.lineup_hash = pe.lineup_hash
  GROUP BY pe.game_year, pe.game_id, lm.team_id, lm.player_id
),
team_possession_totals AS (
  SELECT pe.game_year, pe.game_id, pe.team_id,
         COUNT(DISTINCT (pe.game_id, pe.team_id, pe.poss_end_id))::int4 AS team_poss
  FROM poss_end pe
  GROUP BY pe.game_year, pe.game_id, pe.team_id
),
segment_times AS (
  SELECT d.game_year, d.game_id, d.team_id, d.lineup_hash, d.segment_id,
         MAX(d.segment_seconds)::numeric AS segment_seconds
  FROM actions_base d
  WHERE d.lineup_hash IS NOT NULL AND d.segment_id IS NOT NULL AND d.segment_seconds IS NOT NULL
  GROUP BY d.game_year, d.game_id, d.team_id, d.lineup_hash, d.segment_id
),
seconds_totals AS (
  SELECT st.game_year, st.game_id, lm.team_id, lm.player_id,
         SUM(st.segment_seconds)::numeric AS seconds_on_floor
  FROM segment_times st
  JOIN lineup_map lm
    ON lm.game_id = st.game_id AND lm.team_id = st.team_id AND lm.lineup_hash = st.lineup_hash
  GROUP BY st.game_year, st.game_id, lm.team_id, lm.player_id
),
player_exposure AS (
  SELECT
    COALESCE(u.game_year, s.game_year)::int4 AS game_year,
    COALESCE(u.game_id, s.game_id)::int4 AS game_id,
    COALESCE(u.team_id, s.team_id)::int4 AS team_id,
    COALESCE(u.player_id, s.player_id)::int4 AS player_id,
    COALESCE(u.gp, 0)::int4 AS gp,
    COALESCE(u.poss_on_floor, 0)::int4 AS poss_on_floor,
    COALESCE(s.seconds_on_floor, 0)::numeric AS seconds_on_floor
  FROM usage_totals u
  FULL JOIN seconds_totals s
    ON s.game_year = u.game_year AND s.game_id = u.game_id
   AND s.team_id = u.team_id AND s.player_id = u.player_id
),
team_usage AS (
  SELECT
    d.game_year, d.game_id, d.team_id,
    (COUNT(CASE WHEN d.type = 'shot' AND d.type_lineup = 'offense' THEN 1 END)
     + COUNT(DISTINCT CASE
         WHEN d.type = 'freeThrow' AND d.type_lineup = 'offense'
          AND cf.parent_type = 'foul' AND cf.parent_param = 'personal'
         THEN d.parent_action_id END))::int4 AS team_ts_poss_count,
    SUM(CASE WHEN d.type = 'turnover' AND d.type_lineup = 'offense' THEN 1 ELSE 0 END)::int4 AS team_tov,
    MAX(COALESCE(tp.team_poss, 0))::int4 AS team_poss
  FROM actions_base d
  LEFT JOIN complex_flags cf ON cf.game_id = d.game_id AND cf.main_id = d.id
  LEFT JOIN team_possession_totals tp
    ON tp.game_year = d.game_year AND tp.game_id = d.game_id AND tp.team_id = d.team_id
  GROUP BY d.game_year, d.game_id, d.team_id
)
SELECT
  COALESCE(a.game_year, e.game_year)::int4,
  COALESCE(a.game_id, e.game_id)::int4,
  COALESCE(a.team_id, e.team_id)::int4,
  COALESCE(a.player_id, e.player_id)::int4,
  (a.player_id IS NOT NULL),
  COALESCE(e.gp, 0)::int4,
  COALESCE(e.poss_on_floor, 0)::int4,
  COALESCE(e.seconds_on_floor, 0)::numeric,
  COALESCE(a.pts, 0)::int4,
  COALESCE(a.oreb + a.dreb, 0)::int4,
  COALESCE(a.oreb, 0)::int4,
  COALESCE(a.dreb, 0)::int4,
  COALESCE(a.ast, 0)::int4,
  COALESCE(a.stl, 0)::int4,
  COALESCE(a.blk, 0)::int4,
  COALESCE(a.dfl, 0)::int4,
  COALESCE(a.tov, 0)::int4,
  COALESCE(a.fgm, 0)::int4,
  COALESCE(a.fga, 0)::int4,
  COALESCE(a."3pm", 0)::int4,
  COALESCE(a."3pa", 0)::int4,
  COALESCE(a.ftm, 0)::int4,
  COALESCE(a.fta, 0)::int4,
  COALESCE(a.player_ts_poss_count, 0)::int4,
  COALESCE(t.team_ts_poss_count, 0)::int4,
  COALESCE(t.team_tov, 0)::int4,
  COALESCE(t.team_poss, 0)::int4
FROM actor_game a
FULL JOIN player_exposure e
  ON e.game_year = a.game_year AND e.game_id = a.game_id
 AND e.team_id = a.team_id AND e.player_id = a.player_id
LEFT JOIN team_usage t
  ON t.game_year = COALESCE(a.game_year, e.game_year)
 AND t.game_id = COALESCE(a.game_id, e.game_id)
 AND t.team_id = COALESCE(a.team_id, e.team_id)
LEFT JOIN roster_season r
  ON r.game_year = COALESCE(a.game_year, e.game_year)
 AND r.team_id = COALESCE(a.team_id, e.team_id)
 AND r.player_id = COALESCE(a.player_id, e.player_id)
WHERE (a.player_id IS NOT NULL AND r.player_id IS NOT NULL)
   OR COALESCE(e.gp, 0) > 0
   OR COALESCE(e.poss_on_floor, 0) > 0
   OR COALESCE(e.seconds_on_floor, 0) > 0
$$;
