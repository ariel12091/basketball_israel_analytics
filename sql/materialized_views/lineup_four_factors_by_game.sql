-- Pre-aggregated four-factor counts per lineup_hash per game per type_lineup.
-- Keyed by both own starters (num_starters) and opp_starters. Minutes use the
-- same gap-exclusive opponent-window model as mv_lineup_totals_by_day.

CREATE MATERIALIZED VIEW basketball_test.lineup_four_factors_by_game
TABLESPACE pg_default
AS
WITH
base AS (
  SELECT
    d.id,
    d.game_id,
    d.lineup_hash,
    d.team_id,
    d.team_score,
    d.type,
    d.parameters_type,
    d.parameters_made,
    d.parameters_points,
    d.pct_ft,
    d.parent_action_id,
    d.type_lineup,
    d.num_starters,
    d.opp_starters,
    d.segment_id,
    d.end_game_seconds_remaining,
    d.event_elapsed_seconds,
    d.segment_start_elapsed_seconds,
    d.segment_seconds,
    CASE WHEN d.final_end_poss IS TRUE THEN 1 ELSE 0 END AS final_end_flag,
    (ROW_NUMBER() OVER (
        PARTITION BY d.team_id, d.lineup_hash, d.game_id, d.segment_id
        ORDER BY d.id
     ) - ROW_NUMBER() OVER (
        PARTITION BY d.team_id, d.lineup_hash, d.game_id, d.segment_id, d.opp_starters
        ORDER BY d.id
     )) AS opp_island
  FROM basketball_test.df_pts_poss_lineups_longer_mv d
),
complex_flags AS (
  SELECT DISTINCT ON (d.id)
    d.id AS main_id,
    t2.type AS parent_type,
    t2.parameters_type AS parent_param
  FROM basketball_test.df_pts_poss_lineups_longer_mv d
  JOIN basketball_test.df_pts_poss_lineups_longer_mv t2
    ON t2.id = d.parent_action_id
    AND t2.game_id = d.game_id
    AND t2.type = 'foul'::text
  WHERE d.parent_action_id IS NOT NULL
  ORDER BY d.id
),
combined_data AS (
  SELECT
    b.lineup_hash,
    b.team_id,
    b.game_id,
    s.game_year,
    b.type_lineup,
    b.num_starters,
    b.opp_starters,
    b.segment_id,
    b.end_game_seconds_remaining,
    b.team_score,
    b.final_end_flag,
    b.type,
    b.parameters_type,
    b.parameters_made,
    b.parameters_points,
    b.pct_ft,
    b.parent_action_id,
    cf.parent_type,
    cf.parent_param
  FROM base b
  JOIN basketball_test.schedule s ON b.game_id = s.game_id
  LEFT JOIN complex_flags cf ON b.id = cf.main_id
),
-- Identify each contiguous opponent-starters window inside the canonical
-- segment. Canonical segment_seconds remains the total time budget.
window_bounds AS (
  SELECT
    b.lineup_hash,
    b.team_id,
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
    MAX(b.segment_seconds) AS segment_seconds,
    BOOL_OR(b.type_lineup = 'offense') AS has_offense
  FROM base b
  WHERE b.segment_seconds IS NOT NULL
  GROUP BY
    b.lineup_hash,
    b.team_id,
    b.game_id,
    b.segment_id,
    b.opp_starters,
    b.opp_island
),
ordered_windows AS (
  SELECT
    wb.*,
    ROW_NUMBER() OVER (
      PARTITION BY wb.lineup_hash, wb.team_id, wb.game_id, wb.segment_id
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
      PARTITION BY wsc.lineup_hash, wsc.team_id, wsc.game_id, wsc.segment_id
      ORDER BY wsc.first_id
      ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
    ) AS window_start
  FROM window_start_candidates wsc
),
window_times AS (
  SELECT
    nws.lineup_hash,
    nws.team_id,
    nws.game_id,
    nws.segment_id,
    nws.opp_starters,
    nws.opp_island,
    nws.has_offense,
    GREATEST(
      COALESCE(
        LEAD(nws.window_start) OVER (
          PARTITION BY nws.lineup_hash, nws.team_id, nws.game_id, nws.segment_id
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
    wt.lineup_hash,
    wt.team_id,
    wt.game_id,
    wt.opp_starters,
    SUM(wt.window_seconds) FILTER (WHERE wt.has_offense) / 60.0 AS minutes
  FROM window_times wt
  GROUP BY
    wt.lineup_hash,
    wt.team_id,
    wt.game_id,
    wt.opp_starters
),
-- Four-factor stats per segment per type_lineup and opponent-starters key.
segment_stats AS (
  SELECT
    cd.lineup_hash,
    cd.team_id,
    cd.game_id,
    cd.game_year,
    cd.type_lineup,
    cd.num_starters,
    cd.opp_starters,
    cd.segment_id,
    sum(cd.team_score)       AS total_points,
    sum(cd.final_end_flag)   AS total_poss,
    count(CASE WHEN cd.type = 'shot' THEN 1 END)
      + count(DISTINCT CASE
          WHEN cd.type = 'freeThrow'
            AND cd.parent_type = 'foul'
            AND cd.parent_param = 'personal'
          THEN cd.parent_action_id
        END)                 AS ts_poss_count,
    count(CASE WHEN cd.type = 'rebound' AND cd.parameters_type = 'offensive' THEN 1 END) AS oreb_count,
    count(CASE
      WHEN cd.type = 'shot' AND cd.parameters_made IN ('missed', 'blocked') THEN 1
      WHEN cd.type = 'freeThrow' AND cd.parameters_made = 'missed'
        AND cd.pct_ft = 1::numeric
        AND cd.parent_type = 'foul' AND cd.parent_param = 'personal' THEN 1
    END)                     AS oreb_opportunities,
    count(CASE WHEN cd.type = 'turnover' THEN 1 END) AS tov_count,
    count(CASE WHEN cd.type = 'freeThrow' THEN 1 END) AS total_ft_attempts,
    count(CASE WHEN cd.type = 'shot' THEN 1 END) AS total_fga,
    count(CASE WHEN cd.type = 'shot' AND cd.parameters_made = 'made' THEN 1 END) AS total_fgm,
    count(CASE WHEN cd.type = 'shot' AND cd.parameters_made = 'made' AND cd.parameters_points = 3 THEN 1 END) AS total_fg3_made
  FROM combined_data cd
  GROUP BY
    cd.lineup_hash,
    cd.team_id,
    cd.game_id,
    cd.game_year,
    cd.type_lineup,
    cd.num_starters,
    cd.opp_starters,
    cd.segment_id
)
SELECT
  ss.lineup_hash,
  ss.team_id,
  ss.game_id,
  ss.game_year,
  ss.type_lineup,
  ss.num_starters,
  ss.opp_starters,
  SUM(ss.total_points)::numeric       AS total_points,
  SUM(ss.total_poss)::bigint          AS total_poss,
  SUM(ss.ts_poss_count)::bigint       AS ts_poss_count,
  SUM(ss.oreb_count)::bigint          AS oreb_count,
  SUM(ss.oreb_opportunities)::bigint  AS oreb_opportunities,
  SUM(ss.tov_count)::bigint           AS tov_count,
  SUM(ss.total_ft_attempts)::bigint   AS total_ft_attempts,
  SUM(ss.total_fga)::bigint           AS total_fga,
  SUM(ss.total_fgm)::bigint           AS total_fgm,
  SUM(ss.total_fg3_made)::bigint      AS total_fg3_made,
  CASE WHEN ss.type_lineup = 'offense' THEN wm.minutes END AS minutes
FROM segment_stats ss
LEFT JOIN window_minutes wm
  ON wm.lineup_hash = ss.lineup_hash
 AND wm.team_id = ss.team_id
 AND wm.game_id = ss.game_id
 AND wm.opp_starters IS NOT DISTINCT FROM ss.opp_starters
GROUP BY
  ss.lineup_hash,
  ss.team_id,
  ss.game_id,
  ss.game_year,
  ss.type_lineup,
  ss.num_starters,
  ss.opp_starters,
  wm.minutes
WITH DATA;

-- Indexes for the dynamic function.
CREATE INDEX idx_lff_game_id
  ON basketball_test.lineup_four_factors_by_game USING btree (game_id);
CREATE INDEX idx_lff_lineup_hash
  ON basketball_test.lineup_four_factors_by_game USING btree (lineup_hash);
CREATE INDEX idx_lff_starters
  ON basketball_test.lineup_four_factors_by_game
  USING btree (
    game_year,
    num_starters,
    opp_starters,
    team_id,
    lineup_hash,
    type_lineup
  );
CREATE UNIQUE INDEX idx_lff_pk
  ON basketball_test.lineup_four_factors_by_game
  USING btree (
    lineup_hash,
    team_id,
    game_id,
    type_lineup,
    num_starters,
    opp_starters
  )
  NULLS NOT DISTINCT;
