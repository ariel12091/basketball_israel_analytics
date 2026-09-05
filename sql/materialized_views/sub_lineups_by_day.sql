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
        d.segment_start_elapsed_seconds,
        d.segment_seconds,
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
-- Identify each contiguous opponent-starters window inside the canonical
-- segment. Canonical segment_seconds remains the total time budget.
window_bounds AS (
    SELECT
        b.team_id,
        b.lineup_hash,
        b.game_id,
        b.segment_id,
        b.opp_starters,
        b.opp_island,
        MIN(b.id) AS first_id,
        (
            ARRAY_AGG(b.event_elapsed_seconds ORDER BY b.id)
            FILTER (WHERE b.event_elapsed_seconds IS NOT NULL)
        )[1] AS first_event_elapsed_seconds,
        COALESCE(
            MAX(b.segment_start_elapsed_seconds),
            MIN(b.event_elapsed_seconds),
            0
        ) AS segment_start_elapsed_seconds,
        MAX(b.segment_seconds) AS segment_seconds
    FROM base b
    WHERE b.segment_seconds IS NOT NULL
    GROUP BY
        b.team_id,
        b.lineup_hash,
        b.game_id,
        b.segment_id,
        b.opp_starters,
        b.opp_island
),
ordered_windows AS (
    SELECT
        wb.*,
        ROW_NUMBER() OVER (
            PARTITION BY wb.team_id, wb.lineup_hash, wb.game_id, wb.segment_id
            ORDER BY wb.first_id
        ) AS window_number
    FROM window_bounds wb
),
window_start_candidates AS (
    SELECT
        ow.*,
        CASE
            WHEN ow.window_number = 1 THEN ow.segment_start_elapsed_seconds
            ELSE GREATEST(
                ow.segment_start_elapsed_seconds,
                LEAST(
                    ow.segment_start_elapsed_seconds + ow.segment_seconds,
                    COALESCE(
                        ow.first_event_elapsed_seconds,
                        ow.segment_start_elapsed_seconds
                    )
                )
            )
        END AS window_start_candidate
    FROM ordered_windows ow
),
normalized_window_starts AS (
    SELECT
        wsc.*,
        MAX(wsc.window_start_candidate) OVER (
            PARTITION BY wsc.team_id, wsc.lineup_hash, wsc.game_id, wsc.segment_id
            ORDER BY wsc.first_id
            ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ) AS window_start
    FROM window_start_candidates wsc
),
window_times AS (
    SELECT
        nws.team_id,
        nws.lineup_hash,
        nws.game_id,
        nws.segment_id,
        nws.opp_starters,
        nws.opp_island,
        GREATEST(
            COALESCE(
                LEAD(nws.window_start) OVER (
                    PARTITION BY nws.team_id, nws.lineup_hash, nws.game_id, nws.segment_id
                    ORDER BY nws.first_id
                ),
                nws.segment_start_elapsed_seconds + nws.segment_seconds
            ) - nws.window_start,
            0
        ) AS window_seconds
    FROM normalized_window_starts nws
),
window_minutes AS (
    SELECT
        wt.team_id,
        wt.lineup_hash,
        wt.game_id,
        wt.opp_starters,
        SUM(wt.window_seconds) / 60.0 AS minutes
    FROM window_times wt
    GROUP BY
        wt.team_id,
        wt.lineup_hash,
        wt.game_id,
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
),
-- Slices with floor time but no offense-perspective row.
--
-- day_stats groups by type_lineup, so an offense row exists only where `base`
-- had offense rows for that slice. A lineup that was on court against a given
-- opponent-starters configuration without recording an offensive possession
-- therefore had a defence row only, and since minutes attach to the offense
-- row its floor time had nowhere to land -- 5,772 slices holding 325.9
-- minutes, i.e. 0.371 per team-game, measured 2026-09-05.
--
-- These are emitted as offense rows with zero counts and their real minutes.
-- The row is true: the lineup played, and took no shot. The defence row of
-- the same slice supplies the date, season and starter count.
slices_without_offense AS (
    SELECT
        wm.team_id,
        wm.lineup_hash,
        wm.game_id,
        wm.opp_starters,
        wm.minutes,
        MAX(ds.g_date)       AS g_date,
        MAX(ds.game_year)    AS game_year,
        MAX(ds.num_starters) AS num_starters
    FROM window_minutes wm
    JOIN day_stats ds
      ON ds.team_id = wm.team_id
     AND ds.lineup_hash = wm.lineup_hash
     AND ds.game_id = wm.game_id
     AND ds.opp_starters IS NOT DISTINCT FROM wm.opp_starters
    WHERE NOT EXISTS (
        SELECT 1 FROM day_stats d
         WHERE d.team_id = wm.team_id
           AND d.lineup_hash = wm.lineup_hash
           AND d.game_id = wm.game_id
           AND d.opp_starters IS NOT DISTINCT FROM wm.opp_starters
           AND d.type_lineup = 'offense')
    GROUP BY wm.team_id, wm.lineup_hash, wm.game_id, wm.opp_starters, wm.minutes
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
UNION ALL
SELECT
    swo.team_id,
    swo.lineup_hash,
    'offense'::text AS type_lineup,
    swo.g_date,
    swo.game_id,
    swo.game_year,
    swo.opp_starters,
    0::bigint  AS total_poss,
    0::numeric AS total_pts,
    0::bigint  AS fg2_made,
    0::bigint  AS fg2_att,
    0::bigint  AS fg3_made,
    0::bigint  AS fg3_att,
    swo.num_starters,
    swo.minutes
FROM slices_without_offense swo
WITH DATA;

-- View indexes:
CREATE INDEX idx_mv_ltotals_day_date
  ON basketball_test.mv_lineup_totals_by_day
  USING btree (g_date, lineup_hash, type_lineup);
CREATE INDEX idx_mv_ltotals_day_starters
  ON basketball_test.mv_lineup_totals_by_day
  USING btree (
    game_year,
    num_starters,
    opp_starters,
    team_id,
    lineup_hash,
    type_lineup
  );
CREATE UNIQUE INDEX idx_mv_ltotals_day_pk
  ON basketball_test.mv_lineup_totals_by_day
  USING btree (lineup_hash, type_lineup, g_date, num_starters, opp_starters)
  NULLS NOT DISTINCT;
