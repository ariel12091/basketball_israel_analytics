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
    opp_starters
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
