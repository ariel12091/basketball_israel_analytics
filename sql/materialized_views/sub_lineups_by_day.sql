-- basketball_test.mv_lineup_totals_by_day source
-- Minutes computed per segment across ALL rows (not filtered by type_lineup)
-- to capture full floor time including defense-to-offense transitions.

CREATE MATERIALIZED VIEW basketball_test.mv_lineup_totals_by_day
TABLESPACE pg_default
AS
WITH
-- Stint duration per segment (no type_lineup - captures full floor time)
segment_times AS (
    SELECT
        d.team_id,
        d.lineup_hash,
        d.game_id,
        s.game_date AS g_date,
        s.game_year,
        d.segment_id,
        MAX(d.segment_seconds) AS stint_seconds
    FROM df_pts_poss_lineups_longer_mv d
    JOIN schedule s USING (game_id)
    WHERE d.segment_seconds IS NOT NULL
    GROUP BY d.team_id, d.lineup_hash, d.game_id, s.game_date, s.game_year, d.segment_id
),
-- Poss/pts per segment per type_lineup
segment_stats AS (
    SELECT
        d.team_id,
        d.lineup_hash,
        d.type_lineup,
        d.game_id,
        s.game_date AS g_date,
        s.game_year,
        d.segment_id,
        MAX(d.num_starters) AS num_starters,
        SUM(CASE WHEN COALESCE(d.final_end_poss, false) THEN 1 ELSE 0 END) AS total_poss,
        COALESCE(SUM(d.team_score), 0) AS total_pts,
        SUM(CASE WHEN d.type = 'shot' AND d.parameters_points = 2 AND d.parameters_made = 'made' THEN 1 ELSE 0 END) AS fg2_made,
        SUM(CASE WHEN d.type = 'shot' AND d.parameters_points = 2 THEN 1 ELSE 0 END) AS fg2_att,
        SUM(CASE WHEN d.type = 'shot' AND d.parameters_points = 3 AND d.parameters_made = 'made' THEN 1 ELSE 0 END) AS fg3_made,
        SUM(CASE WHEN d.type = 'shot' AND d.parameters_points = 3 THEN 1 ELSE 0 END) AS fg3_att
    FROM df_pts_poss_lineups_longer_mv d
    JOIN schedule s USING (game_id)
    GROUP BY d.team_id, d.lineup_hash, d.type_lineup, d.game_id, s.game_date, s.game_year, d.segment_id
)
SELECT
    ss.team_id,
    ss.lineup_hash,
    ss.type_lineup,
    ss.g_date,
    ss.game_id,
    ss.game_year,
    SUM(ss.total_poss) AS total_poss,
    SUM(ss.total_pts) AS total_pts,
    SUM(ss.fg2_made) AS fg2_made,
    SUM(ss.fg2_att) AS fg2_att,
    SUM(ss.fg3_made) AS fg3_made,
    SUM(ss.fg3_att) AS fg3_att,
    MAX(ss.num_starters) AS num_starters,
    -- Minutes from segment_times, but only count once per segment (use offense to avoid double)
    SUM(st.stint_seconds) FILTER (WHERE ss.type_lineup = 'offense') / 60.0 AS minutes
FROM segment_stats ss
JOIN segment_times st
  ON st.team_id = ss.team_id
  AND st.lineup_hash = ss.lineup_hash
  AND st.game_id = ss.game_id
  AND st.segment_id = ss.segment_id
GROUP BY ss.team_id, ss.lineup_hash, ss.type_lineup, ss.g_date, ss.game_id, ss.game_year
WITH DATA;

-- View indexes:
CREATE INDEX idx_mv_ltotals_day_date ON basketball_test.mv_lineup_totals_by_day USING btree (g_date, lineup_hash, type_lineup);
CREATE UNIQUE INDEX idx_mv_ltotals_day_pk ON basketball_test.mv_lineup_totals_by_day USING btree (lineup_hash, type_lineup, g_date, num_starters);
