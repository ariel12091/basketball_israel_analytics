-- basketball_test.player_traditional_stats_mv source

CREATE MATERIALIZED VIEW basketball_test.player_traditional_stats_mv AS
WITH lineup_map AS (
  SELECT DISTINCT
    ll.game_id,
    ll.team_id,
    ll.lineup_hash,
    ll.player_id
  FROM basketball_test.lineups_lookup ll
  WHERE ll.game_year IS NOT NULL
    AND COALESCE(ll.is_on_verdict, 0)::int = 1
), 
sched_year AS (
  SELECT DISTINCT
    fs.game_id,
    fs.game_year
  FROM basketball_test.final_schedule_mv fs
),
actions_with_year AS (
  SELECT
    d.id,
    d.game_id,
    d.team_id,
    d.lineup_hash,
    d.segment_id,
    d.end_game_seconds_remaining,
    d.type,
    d.parameters_type,
    d.parameters_made,
    d.parameters_points,
    d.player_id,
    d.type_lineup,
    d.final_end_poss,
    d.final_end_id,
    sy.game_year
  FROM basketball_test.df_pts_poss_lineups_longer_mv d
  JOIN sched_year sy
    ON sy.game_id = d.game_id
),
box_totals AS (
  SELECT
    awy.game_year,
    awy.team_id,
    awy.player_id,
    SUM(CASE WHEN awy.type = 'shot' AND awy.parameters_made = 'made' AND awy.type_lineup = 'offense' THEN COALESCE(awy.parameters_points, 0) ELSE 0 END) +
      SUM(CASE WHEN awy.type = 'freeThrow' AND awy.parameters_made = 'made' AND awy.type_lineup = 'offense' THEN 1 ELSE 0 END) AS pts,
    SUM(CASE WHEN awy.type = 'rebound'  AND awy.type_lineup = 'offense' THEN 1 ELSE 0 END) AS reb,
    SUM(CASE WHEN awy.type = 'assist'   AND awy.type_lineup = 'offense' THEN 1 ELSE 0 END) AS ast,
    SUM(CASE WHEN awy.type = 'steal' AND awy.type_lineup = 'offense' THEN 1 ELSE 0 END) AS stl,
    SUM(CASE WHEN awy.type = 'block' AND awy.type_lineup = 'offense' THEN 1 ELSE 0 END) AS blk,
    SUM(CASE WHEN awy.type = 'turnover' AND awy.type_lineup = 'offense' THEN 1 ELSE 0 END) AS tov,
    SUM(CASE WHEN awy.type = 'shot' AND awy.parameters_made = 'made' AND awy.type_lineup = 'offense' THEN 1 ELSE 0 END) AS fgm,
    SUM(CASE WHEN awy.type = 'shot' AND awy.type_lineup = 'offense' THEN 1 ELSE 0 END) AS fga,
    SUM(CASE WHEN awy.type = 'shot' AND awy.parameters_made = 'made' AND awy.parameters_points = 3 AND awy.type_lineup = 'offense' THEN 1 ELSE 0 END) AS "3pm",
    SUM(CASE WHEN awy.type = 'shot' AND awy.parameters_points = 3 AND awy.type_lineup = 'offense' THEN 1 ELSE 0 END) AS "3pa",
    SUM(CASE WHEN awy.type = 'freeThrow' AND awy.parameters_made = 'made' AND awy.type_lineup = 'offense' THEN 1 ELSE 0 END) AS ftm,
    SUM(CASE WHEN awy.type = 'freeThrow' AND awy.type_lineup = 'offense' THEN 1 ELSE 0 END) AS fta
  FROM actions_with_year awy
  WHERE awy.player_id IS NOT NULL AND awy.player_id > 0
  GROUP BY awy.game_year, awy.team_id, awy.player_id
),
poss_end AS (
  SELECT DISTINCT
    awy.game_year,
    awy.game_id,
    awy.team_id,
    awy.lineup_hash,
    awy.id AS poss_end_id
  FROM actions_with_year awy
  WHERE awy.type_lineup = 'offense'
    AND awy.final_end_poss
    AND awy.id IS NOT NULL
    AND awy.lineup_hash IS NOT NULL
),
usage_totals AS (
  SELECT
    pe.game_year,
    lm.team_id,
    lm.player_id,
    COUNT(DISTINCT pe.game_id) AS gp,
    COUNT(DISTINCT (pe.game_id, pe.team_id, pe.poss_end_id)) AS poss_on_floor
  FROM poss_end pe
  JOIN lineup_map lm
    ON lm.game_id = pe.game_id
   AND lm.team_id = pe.team_id
   AND lm.lineup_hash = pe.lineup_hash
  GROUP BY pe.game_year, lm.team_id, lm.player_id
),
segment_times AS (
  SELECT
    awy.game_year,
    awy.game_id,
    awy.team_id,
    awy.lineup_hash,
    awy.segment_id,
    MAX(awy.end_game_seconds_remaining) - MIN(awy.end_game_seconds_remaining) AS seg_seconds
  FROM actions_with_year awy
  WHERE awy.lineup_hash IS NOT NULL
    AND awy.segment_id IS NOT NULL
    AND awy.end_game_seconds_remaining IS NOT NULL
  GROUP BY awy.game_year, awy.game_id, awy.team_id, awy.lineup_hash, awy.segment_id
),
minutes_totals AS (
  SELECT
    st.game_year,
    lm.team_id,
    lm.player_id,
    ROUND((SUM(st.seg_seconds) / 60.0)::numeric, 1) AS minutes
  FROM segment_times st
  JOIN lineup_map lm
    ON lm.game_id = st.game_id
   AND lm.team_id = st.team_id
   AND lm.lineup_hash = st.lineup_hash
  GROUP BY st.game_year, lm.team_id, lm.player_id
),
roster_names AS (
  SELECT DISTINCT
    fr.game_year,
    fr.team_id,
    fr.player_id,
    fr.team_name,
    fr.firstname,
    fr.lastname,
    trim(coalesce(fr.firstname, '') || ' ' || coalesce(fr.lastname, '')) AS player_name
  FROM basketball_test.full_rosters fr
)
SELECT
  bt.game_year,
  bt.team_id,
  bt.player_id,
  rn.team_name,
  rn.firstname,
  rn.lastname,
  rn.player_name,
  COALESCE(ut.gp, 0)::int AS gp,
  COALESCE(ut.poss_on_floor, 0)::int AS poss_on_floor,
  COALESCE(mt.minutes, 0)::numeric(10,1) AS minutes,
  bt.pts::int,
  bt.reb::int,
  bt.ast::int,
  bt.stl::int,
  bt.blk::int,
  bt.tov::int,
  bt.fgm::int,
  bt.fga::int,
  bt."3pm"::int,
  bt."3pa"::int,
  bt.ftm::int,
  bt.fta::int,
  CASE
    WHEN bt.fga > 0 THEN ROUND(bt.fgm::numeric / bt.fga::numeric * 100, 1)
    ELSE NULL
  END AS fg_pct,
  CASE
    WHEN bt."3pa" > 0 THEN ROUND(bt."3pm"::numeric / bt."3pa"::numeric * 100, 1)
    ELSE NULL
  END AS tp_pct,
  CASE
    WHEN bt.fta > 0 THEN ROUND(bt.ftm::numeric / bt.fta::numeric * 100, 1)
    ELSE NULL
  END AS ft_pct,
  CASE
    WHEN bt.fga > 0 THEN ROUND((bt.fgm + 0.5 * bt."3pm")::numeric / bt.fga::numeric * 100, 1)
    ELSE NULL
  END AS efg,
  CASE
    WHEN (bt.fga + 0.44 * bt.fta) > 0 THEN ROUND(bt.pts::numeric / (2 * (bt.fga + 0.44 * bt.fta)::numeric) * 100, 1)
    ELSE NULL
  END AS ts
FROM box_totals bt
LEFT JOIN usage_totals ut
  ON ut.game_year = bt.game_year
 AND ut.team_id = bt.team_id
 AND ut.player_id = bt.player_id
LEFT JOIN minutes_totals mt
  ON mt.game_year = bt.game_year
 AND mt.team_id = bt.team_id
 AND mt.player_id = bt.player_id
LEFT JOIN roster_names rn
  ON rn.game_year = bt.game_year
 AND rn.team_id = bt.team_id
 AND rn.player_id = bt.player_id
ORDER BY bt.game_year DESC, bt.pts DESC, rn.team_name, rn.lastname, rn.firstname
WITH DATA;

CREATE UNIQUE INDEX player_traditional_stats_mv_uq
  ON basketball_test.player_traditional_stats_mv (game_year, team_id, player_id);

CREATE INDEX player_traditional_stats_mv_year_team_name_idx
  ON basketball_test.player_traditional_stats_mv (game_year, team_name);
