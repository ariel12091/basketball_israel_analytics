-- basketball_test.player_onoff_by_game source
-- Pre-aggregated player on/off stats per game.
-- Joins lineups_lookup (player-lineup mapping) with df_pts_poss_lineups_longer_mv
-- so both own_starters and opp_starters context are available per row.
-- Used by onoff_compute() for ~24x speedup.

CREATE MATERIALIZED VIEW basketball_test.player_onoff_by_game
TABLESPACE pg_default
AS WITH base0 AS (
    SELECT DISTINCT
      ll.player_id,
      ll.team_id,
      ll.lineup_hash,
      COALESCE(ll.is_on_verdict, 0::numeric)::integer AS is_on_key
    FROM basketball_test.lineups_lookup ll
  ),
  sched AS (
    SELECT DISTINCT
      fs.game_id,
      fs.game_year
    FROM basketball_test.final_schedule_mv fs
  ),
  lineup_totals AS (
    SELECT
      d.game_id,
      s.game_year,
      d.team_id,
      d.lineup_hash,
      d.type_lineup,
      d.own_starters,
      d.opp_starters,
      SUM(d.team_score) AS total_pts,
      SUM(CASE WHEN d.final_end_poss IS TRUE THEN 1 ELSE 0 END) AS total_poss,
      SUM(CASE WHEN d.type = 'shot' AND d.parameters_points = 2 AND d.parameters_made = 'made' THEN 1 ELSE 0 END) AS fg2_made,
      SUM(CASE WHEN d.type = 'shot' AND d.parameters_points = 2 THEN 1 ELSE 0 END) AS fg2_att,
      SUM(CASE WHEN d.type = 'shot' AND d.parameters_points = 3 AND d.parameters_made = 'made' THEN 1 ELSE 0 END) AS fg3_made,
      SUM(CASE WHEN d.type = 'shot' AND d.parameters_points = 3 THEN 1 ELSE 0 END) AS fg3_att
    FROM basketball_test.df_pts_poss_lineups_longer_mv d
    JOIN sched s
      ON s.game_id = d.game_id
    GROUP BY d.game_id, s.game_year, d.team_id, d.lineup_hash, d.type_lineup, d.own_starters, d.opp_starters
  ),
  lineup_segments AS (
    SELECT
      d.game_id,
      s.game_year,
      d.team_id,
      d.lineup_hash,
      d.type_lineup,
      d.own_starters,
      d.opp_starters,
      d.segment_id,
      GREATEST(
        MAX(d.end_game_seconds_remaining) - MIN(d.end_game_seconds_remaining),
        0
      )::numeric AS seg_seconds
    FROM basketball_test.df_pts_poss_lineups_longer_mv d
    JOIN sched s
      ON s.game_id = d.game_id
    WHERE d.lineup_hash IS NOT NULL
      AND d.segment_id IS NOT NULL
      AND d.end_game_seconds_remaining IS NOT NULL
    GROUP BY d.game_id, s.game_year, d.team_id, d.lineup_hash, d.type_lineup, d.own_starters, d.opp_starters, d.segment_id
  ),
  lineup_minutes AS (
    SELECT
      game_id,
      game_year,
      team_id,
      lineup_hash,
      type_lineup,
      own_starters,
      opp_starters,
      CASE
        WHEN type_lineup = 'offense' THEN ROUND(SUM(seg_seconds) / 60.0, 3)
        ELSE 0::numeric
      END AS minutes
    FROM lineup_segments
    GROUP BY game_id, game_year, team_id, lineup_hash, type_lineup, own_starters, opp_starters
  )
SELECT
  b0.player_id,
  b0.team_id,
  m.game_id,
  m.game_year,
  b0.is_on_key,
  m.type_lineup,
  m.own_starters AS num_starters,
  m.own_starters,
  m.opp_starters,
  SUM(m.total_pts)  AS total_pts,
  SUM(m.total_poss) AS total_poss,
  SUM(m.fg2_made) AS fg2_made,
  SUM(m.fg2_att)  AS fg2_att,
  SUM(m.fg3_made) AS fg3_made,
  SUM(m.fg3_att)  AS fg3_att,
  SUM(COALESCE(lm.minutes, 0)) AS minutes
FROM base0 b0
JOIN lineup_totals m
  ON m.lineup_hash = b0.lineup_hash
 AND m.team_id = b0.team_id
LEFT JOIN lineup_minutes lm
  ON lm.game_id = m.game_id
 AND lm.game_year = m.game_year
 AND lm.team_id = m.team_id
 AND lm.lineup_hash = m.lineup_hash
 AND lm.type_lineup = m.type_lineup
 AND lm.own_starters = m.own_starters
 AND lm.opp_starters = m.opp_starters
GROUP BY b0.player_id, b0.team_id, m.game_id, m.game_year, b0.is_on_key, m.type_lineup, m.own_starters, m.opp_starters
WITH DATA;

-- View indexes:
CREATE INDEX idx_pobg_game_team ON basketball_test.player_onoff_by_game USING btree (game_id, team_id);
CREATE INDEX idx_pobg_gy ON basketball_test.player_onoff_by_game USING btree (game_year);
CREATE UNIQUE INDEX idx_pobg_pk ON basketball_test.player_onoff_by_game USING btree (player_id, team_id, game_id, is_on_key, type_lineup, own_starters, opp_starters);
