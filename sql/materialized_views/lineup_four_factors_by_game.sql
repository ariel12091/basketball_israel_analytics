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
    d.pct_ft,
    d.parent_action_id,
    d.type_lineup,
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
    cs.team_score,
    cs.final_end_flag,
    cs.type,
    cs.parameters_type,
    cs.parameters_made,
    cs.pct_ft,
    cs.parent_action_id,
    cf.parent_type,
    cf.parent_param
  FROM clean_stats cs
  JOIN basketball_test.schedule s ON cs.game_id = s.game_id
  LEFT JOIN complex_flags cf ON cs.id = cf.main_id
)
SELECT
  cd.lineup_hash,
  cd.team_id,
  cd.game_id,
  cd.game_year,
  cd.type_lineup,
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
  count(CASE WHEN cd.type = 'shot' THEN 1 END) AS total_fga
FROM combined_data cd
GROUP BY cd.lineup_hash, cd.team_id, cd.game_id, cd.game_year, cd.type_lineup
WITH DATA;

-- Indexes for the dynamic function
CREATE INDEX idx_lff_game_id ON basketball_test.lineup_four_factors_by_game USING btree (game_id);
CREATE INDEX idx_lff_lineup_hash ON basketball_test.lineup_four_factors_by_game USING btree (lineup_hash);
CREATE UNIQUE INDEX idx_lff_pk ON basketball_test.lineup_four_factors_by_game
  USING btree (lineup_hash, team_id, game_id, type_lineup);
