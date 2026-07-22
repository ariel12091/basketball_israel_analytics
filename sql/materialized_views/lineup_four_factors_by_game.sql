-- Pre-aggregated four-factor counts per lineup_hash per game per type_lineup.
-- Same computation logic as player_four_factors_by_game but without player-level
-- grouping (no lineups_lookup join / is_on_key split).
-- The dynamic function fetch_lineups_four_factors() sums from this MV
-- instead of re-scanning df_pts_poss_lineups_longer_mv each time.

CREATE MATERIALIZED VIEW basketball_test.lineup_four_factors_by_game
TABLESPACE pg_default
AS
WITH clean_stats AS (
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
    d.segment_id,
    d.end_game_seconds_remaining,
    d.segment_seconds,
    CASE WHEN d.final_end_poss IS TRUE THEN 1 ELSE 0 END AS final_end_flag
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
    cs.lineup_hash,
    cs.team_id,
    cs.game_id,
    s.game_year,
    cs.type_lineup,
    cs.num_starters,
    cs.segment_id,
    cs.end_game_seconds_remaining,
    cs.segment_seconds,
    cs.team_score,
    cs.final_end_flag,
    cs.type,
    cs.parameters_type,
    cs.parameters_made,
    cs.parameters_points,
    cs.pct_ft,
    cs.parent_action_id,
    cf.parent_type,
    cf.parent_param
  FROM clean_stats cs
  JOIN basketball_test.schedule s ON cs.game_id = s.game_id
  LEFT JOIN complex_flags cf ON cs.id = cf.main_id
),
-- Stint duration per segment (no type_lineup - captures full floor time)
segment_times AS (
  SELECT
    cd.lineup_hash,
    cd.team_id,
    cd.game_id,
    cd.game_year,
    cd.segment_id,
    MAX(cd.segment_seconds) AS stint_seconds
  FROM combined_data cd
  WHERE cd.segment_seconds IS NOT NULL
  GROUP BY cd.lineup_hash, cd.team_id, cd.game_id, cd.game_year, cd.segment_id
),
-- Four-factor stats per segment per type_lineup
segment_stats AS (
  SELECT
    cd.lineup_hash,
    cd.team_id,
    cd.game_id,
    cd.game_year,
    cd.type_lineup,
    cd.num_starters,
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
  GROUP BY cd.lineup_hash, cd.team_id, cd.game_id, cd.game_year, cd.type_lineup, cd.num_starters, cd.segment_id
)
SELECT
  ss.lineup_hash,
  ss.team_id,
  ss.game_id,
  ss.game_year,
  ss.type_lineup,
  ss.num_starters,
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
  -- Minutes from segment_times, count once per segment (use offense filter)
  SUM(st.stint_seconds) FILTER (WHERE ss.type_lineup = 'offense') / 60.0 AS minutes
FROM segment_stats ss
JOIN segment_times st
  ON st.lineup_hash = ss.lineup_hash
  AND st.team_id = ss.team_id
  AND st.game_id = ss.game_id
  AND st.segment_id = ss.segment_id
GROUP BY ss.lineup_hash, ss.team_id, ss.game_id, ss.game_year, ss.type_lineup, ss.num_starters
WITH DATA;

-- Indexes for the dynamic function
CREATE INDEX idx_lff_game_id ON basketball_test.lineup_four_factors_by_game USING btree (game_id);
CREATE INDEX idx_lff_lineup_hash ON basketball_test.lineup_four_factors_by_game USING btree (lineup_hash);
CREATE UNIQUE INDEX idx_lff_pk ON basketball_test.lineup_four_factors_by_game
  USING btree (lineup_hash, team_id, game_id, type_lineup, num_starters);
