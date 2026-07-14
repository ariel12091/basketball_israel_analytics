-- Pre-aggregated four-factor counts per player per game per on/off per type.
-- Stored as a physical table for incremental refresh by game_id.

DROP MATERIALIZED VIEW IF EXISTS basketball_test.player_four_factors_by_game;
DROP TABLE IF EXISTS basketball_test.player_four_factors_by_game;

CREATE TABLE basketball_test.player_four_factors_by_game AS
WITH base0 AS (
  SELECT DISTINCT
    ll.player_id,
    ll.team_id,
    ll.lineup_hash,
    COALESCE(ll.is_on_verdict, 0::numeric)::integer AS is_on_key
  FROM basketball_test.lineups_lookup ll
),
clean_stats AS (
  SELECT
    d.id,
    d.game_id,
    d.team_id,
    d.lineup_hash,
    d.team_score,
    d.type,
    d.parameters_type,
    d.parameters_made,
    d.parameters_points,
    d.player_id AS action_player_id,
    d.pct_ft,
    d.parent_action_id,
    d.type_lineup,
    d.own_starters,
    d.opp_starters,
    d.segment_id,
    d.end_game_seconds_remaining,
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
lineup_totals AS (
  SELECT
    cs.game_id,
    s.game_year,
    cs.team_id,
    cs.lineup_hash,
    cs.type_lineup,
    cs.own_starters,
    cs.opp_starters,
    SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 2 AND cs.parameters_made = 'made' THEN 1 ELSE 0 END) AS fg2_made,
    SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 2 THEN 1 ELSE 0 END) AS fg2_att,
    SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 3 AND cs.parameters_made = 'made' THEN 1 ELSE 0 END) AS fg3_made,
    SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 3 THEN 1 ELSE 0 END) AS fg3_att
  FROM clean_stats cs
  JOIN basketball_test.schedule s ON s.game_id = cs.game_id
  GROUP BY cs.game_id, s.game_year, cs.team_id, cs.lineup_hash, cs.type_lineup, cs.own_starters, cs.opp_starters
),
onoff_lineup_segments AS (
  SELECT
    cs.game_id,
    cs.team_id,
    cs.lineup_hash,
    cs.type_lineup,
    cs.own_starters,
    cs.opp_starters,
    cs.segment_id,
    GREATEST(MAX(cs.end_game_seconds_remaining) - MIN(cs.end_game_seconds_remaining), 0)::numeric AS seg_seconds
  FROM clean_stats cs
  WHERE cs.lineup_hash IS NOT NULL
    AND cs.segment_id IS NOT NULL
    AND cs.end_game_seconds_remaining IS NOT NULL
  GROUP BY cs.game_id, cs.team_id, cs.lineup_hash, cs.type_lineup, cs.own_starters, cs.opp_starters, cs.segment_id
),
onoff_lineup_minutes AS (
  SELECT
    game_id,
    team_id,
    lineup_hash,
    type_lineup,
    own_starters,
    opp_starters,
    CASE
      WHEN type_lineup = 'offense' THEN ROUND(SUM(seg_seconds) / 60.0, 3)
      ELSE 0::numeric
    END AS minutes
  FROM onoff_lineup_segments
  GROUP BY game_id, team_id, lineup_hash, type_lineup, own_starters, opp_starters
),
onoff_player AS (
  SELECT
    b0.player_id,
    b0.team_id,
    lt.game_id,
    lt.game_year,
    b0.is_on_key,
    lt.type_lineup,
    lt.own_starters,
    lt.opp_starters,
    SUM(lt.fg2_made) AS fg2_made,
    SUM(lt.fg2_att) AS fg2_att,
    SUM(lt.fg3_made) AS fg3_made,
    SUM(lt.fg3_att) AS fg3_att,
    SUM(COALESCE(lm.minutes, 0)) AS onoff_minutes
  FROM base0 b0
  JOIN lineup_totals lt
    ON lt.lineup_hash = b0.lineup_hash
   AND lt.team_id = b0.team_id
  LEFT JOIN onoff_lineup_minutes lm
    ON lm.game_id = lt.game_id
   AND lm.team_id = lt.team_id
   AND lm.lineup_hash = lt.lineup_hash
   AND lm.type_lineup = lt.type_lineup
   AND lm.own_starters = lt.own_starters
   AND lm.opp_starters = lt.opp_starters
  GROUP BY b0.player_id, b0.team_id, lt.game_id, lt.game_year, b0.is_on_key,
           lt.type_lineup, lt.own_starters, lt.opp_starters
),
combined_data AS (
  SELECT
    b0.player_id,
    b0.team_id,
    b0.is_on_key,
    s.game_year,
    cs.game_id,
    cs.type_lineup,
    cs.own_starters AS num_starters,
    cs.own_starters,
    cs.opp_starters,
    cs.team_score,
    cs.final_end_flag,
    cs.type,
    cs.parameters_type,
    cs.parameters_made,
    cs.parameters_points,
    cs.action_player_id,
    cs.pct_ft,
    cs.parent_action_id,
    cf.parent_type,
    cf.parent_param,
    cs.segment_id,
    cs.end_game_seconds_remaining
  FROM base0 b0
  JOIN clean_stats cs ON b0.lineup_hash = cs.lineup_hash AND b0.team_id = cs.team_id
  JOIN basketball_test.schedule s ON cs.game_id = s.game_id
  LEFT JOIN complex_flags cf ON cs.id = cf.main_id
),
segment_times AS (
  SELECT
    cd.player_id,
    cd.team_id,
    cd.game_id,
    cd.game_year,
    cd.is_on_key,
    cd.num_starters,
    cd.own_starters,
    cd.opp_starters,
    cd.segment_id,
    MAX(cd.end_game_seconds_remaining) - MIN(cd.end_game_seconds_remaining) AS stint_seconds
  FROM combined_data cd
  WHERE cd.segment_id IS NOT NULL
    AND cd.end_game_seconds_remaining IS NOT NULL
  GROUP BY cd.player_id, cd.team_id, cd.game_id, cd.game_year,
           cd.is_on_key, cd.num_starters, cd.own_starters, cd.opp_starters,
           cd.segment_id
),
segment_stats AS (
  SELECT
    cd.player_id,
    cd.team_id,
    cd.game_id,
    cd.game_year,
    cd.is_on_key,
    cd.type_lineup,
    cd.num_starters,
    cd.own_starters,
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
    count(CASE WHEN cd.type = 'shot' AND cd.parameters_made = 'made' AND cd.parameters_points = 3 THEN 1 END) AS total_fg3_made,
    count(CASE WHEN cd.action_player_id = cd.player_id AND cd.type = 'shot' AND cd.type_lineup = 'offense' THEN 1 END)
      + count(DISTINCT CASE
          WHEN cd.action_player_id = cd.player_id
            AND cd.type = 'freeThrow'
            AND cd.type_lineup = 'offense'
            AND cd.parent_type = 'foul'
            AND cd.parent_param = 'personal'
          THEN cd.parent_action_id
        END) AS player_ts_poss_count,
    count(CASE
      WHEN cd.action_player_id = cd.player_id
        AND cd.type = 'turnover'
        AND cd.type_lineup = 'offense'
      THEN 1
    END) AS player_tov_count
  FROM combined_data cd
  GROUP BY cd.player_id, cd.team_id, cd.game_id, cd.game_year, cd.is_on_key,
           cd.type_lineup, cd.num_starters, cd.own_starters, cd.opp_starters,
           cd.segment_id
),
ff AS (
SELECT
  ss.player_id,
  ss.team_id,
  ss.game_id,
  ss.game_year,
  ss.is_on_key,
  ss.type_lineup,
  ss.num_starters,
  ss.own_starters,
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
  SUM(ss.player_ts_poss_count)::bigint AS player_ts_poss_count,
  SUM(ss.player_tov_count)::bigint     AS player_tov_count,
  SUM(st.stint_seconds) FILTER (WHERE ss.type_lineup = 'offense') / 60.0 AS minutes
FROM segment_stats ss
LEFT JOIN segment_times st
  ON st.player_id = ss.player_id
 AND st.team_id = ss.team_id
 AND st.game_id = ss.game_id
 AND st.game_year = ss.game_year
 AND st.is_on_key = ss.is_on_key
 AND st.num_starters = ss.num_starters
 AND st.own_starters = ss.own_starters
 AND st.opp_starters = ss.opp_starters
 AND st.segment_id = ss.segment_id
GROUP BY ss.player_id, ss.team_id, ss.game_id, ss.game_year, ss.is_on_key,
         ss.type_lineup, ss.num_starters, ss.own_starters, ss.opp_starters
)
SELECT
  ff.player_id,
  ff.team_id,
  ff.game_id,
  ff.game_year,
  ff.is_on_key,
  ff.type_lineup,
  ff.num_starters,
  ff.own_starters,
  ff.opp_starters,
  ff.total_points,
  ff.total_poss,
  ff.ts_poss_count,
  ff.oreb_count,
  ff.oreb_opportunities,
  ff.tov_count,
  ff.total_ft_attempts,
  ff.total_fga,
  ff.total_fgm,
  ff.total_fg3_made,
  ff.player_ts_poss_count,
  ff.player_tov_count,
  ff.minutes,
  op.fg2_made::int AS fg2_made,
  op.fg2_att::int AS fg2_att,
  op.fg3_made::int AS fg3_made,
  op.fg3_att::int AS fg3_att,
  op.onoff_minutes
FROM ff
LEFT JOIN onoff_player op
  ON op.player_id = ff.player_id
 AND op.team_id = ff.team_id
 AND op.game_id = ff.game_id
 AND op.is_on_key = ff.is_on_key
 AND COALESCE(op.type_lineup, '~') = COALESCE(ff.type_lineup, '~')
 AND COALESCE(op.own_starters, -1) = COALESCE(ff.own_starters, -1)
 AND COALESCE(op.opp_starters, -1) = COALESCE(ff.opp_starters, -1)
;

-- Indexes for the dynamic function
CREATE INDEX idx_pff_game_id ON basketball_test.player_four_factors_by_game USING btree (game_id);
CREATE INDEX idx_pff_game_year ON basketball_test.player_four_factors_by_game USING btree (game_year);
CREATE UNIQUE INDEX idx_pff_pk ON basketball_test.player_four_factors_by_game
  USING btree (player_id, team_id, game_id, is_on_key, type_lineup, own_starters, opp_starters);
