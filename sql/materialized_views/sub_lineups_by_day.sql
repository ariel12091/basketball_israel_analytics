-- basketball_test.mv_lineup_totals_by_day source
-- Keyed by opp_starters since 2026-07 (starters fast path). own starter count
-- is num_starters (a property of the lineup itself). Minutes are GAP-EXCLUSIVE:
-- summed per contiguous opponent-window (gaps-and-islands on opp_starters
-- within a segment), computed across ALL rows (no type_lineup filter) to
-- capture full floor time, then attached to offense rows only (as before).

CREATE MATERIALIZED VIEW basketball_test.mv_lineup_totals_by_day
TABLESPACE pg_default
AS
WITH
base AS (
    SELECT
        d.team_id,
        d.lineup_hash,
        d.type_lineup,
        d.game_id,
        d.segment_id,
        d.opp_starters,
        d.num_starters,
        d.id,
        d.event_elapsed_seconds,
        d.final_end_poss,
        d.team_score,
        d.type,
        d.parameters_points,
        d.parameters_made,
        (ROW_NUMBER() OVER (
            PARTITION BY d.team_id, d.lineup_hash, d.game_id, d.segment_id
            ORDER BY d.id
         ) - ROW_NUMBER() OVER (
            PARTITION BY d.team_id, d.lineup_hash, d.game_id, d.segment_id, d.opp_starters
            ORDER BY d.id
         )) AS opp_island
    FROM df_pts_poss_lineups_longer_mv d
),
-- Gap-exclusive stint time: one span per contiguous opp_starters window.
window_times AS (
    SELECT
        b.team_id,
        b.lineup_hash,
        b.game_id,
        s.game_date AS g_date,
        s.game_year,
        b.segment_id,
        b.opp_starters,
        GREATEST(
            MAX(b.event_elapsed_seconds) - MIN(b.event_elapsed_seconds),
            0
        ) AS window_seconds
    FROM base b
    JOIN schedule s USING (game_id)
    WHERE b.event_elapsed_seconds IS NOT NULL
    GROUP BY
        b.team_id,
        b.lineup_hash,
        b.game_id,
        s.game_date,
        s.game_year,
        b.segment_id,
        b.opp_starters,
        b.opp_island
),
window_minutes AS (
    SELECT
        wt.team_id,
        wt.lineup_hash,
        wt.game_id,
        wt.g_date,
        wt.game_year,
        wt.opp_starters,
        SUM(wt.window_seconds) / 60.0 AS minutes
    FROM window_times wt
    GROUP BY
        wt.team_id,
        wt.lineup_hash,
        wt.game_id,
        wt.g_date,
        wt.game_year,
        wt.opp_starters
),
day_stats AS (
    SELECT
        b.team_id,
        b.lineup_hash,
        b.type_lineup,
        s.game_date AS g_date,
        b.game_id,
        s.game_year,
        b.opp_starters,
        MAX(b.num_starters) AS num_starters,
        SUM(CASE WHEN COALESCE(b.final_end_poss, false) THEN 1 ELSE 0 END) AS total_poss,
        COALESCE(SUM(b.team_score), 0) AS total_pts,
        SUM(CASE WHEN b.type = 'shot' AND b.parameters_points = 2 AND b.parameters_made = 'made' THEN 1 ELSE 0 END) AS fg2_made,
        SUM(CASE WHEN b.type = 'shot' AND b.parameters_points = 2 THEN 1 ELSE 0 END) AS fg2_att,
        SUM(CASE WHEN b.type = 'shot' AND b.parameters_points = 3 AND b.parameters_made = 'made' THEN 1 ELSE 0 END) AS fg3_made,
        SUM(CASE WHEN b.type = 'shot' AND b.parameters_points = 3 THEN 1 ELSE 0 END) AS fg3_att
    FROM base b
    JOIN schedule s USING (game_id)
    GROUP BY
        b.team_id,
        b.lineup_hash,
        b.type_lineup,
        s.game_date,
        b.game_id,
        s.game_year,
        b.opp_starters
)
SELECT
    ds.team_id,
    ds.lineup_hash,
    ds.type_lineup,
    ds.g_date,
    ds.game_id,
    ds.game_year,
    ds.opp_starters,
    ds.total_poss,
    ds.total_pts,
    ds.fg2_made,
    ds.fg2_att,
    ds.fg3_made,
    ds.fg3_att,
    ds.num_starters,
    CASE WHEN ds.type_lineup = 'offense' THEN wm.minutes END AS minutes
FROM day_stats ds
LEFT JOIN window_minutes wm
  ON wm.team_id = ds.team_id
 AND wm.lineup_hash = ds.lineup_hash
 AND wm.game_id = ds.game_id
 AND wm.opp_starters IS NOT DISTINCT FROM ds.opp_starters
WITH DATA;

-- View indexes:
CREATE INDEX idx_mv_ltotals_day_date
  ON basketball_test.mv_lineup_totals_by_day
  USING btree (g_date, lineup_hash, type_lineup);
CREATE UNIQUE INDEX idx_mv_ltotals_day_pk
  ON basketball_test.mv_lineup_totals_by_day
  USING btree (lineup_hash, type_lineup, g_date, num_starters, opp_starters)
  NULLS NOT DISTINCT;
