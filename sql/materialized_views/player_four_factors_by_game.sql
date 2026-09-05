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
    SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 3 THEN 1 ELSE 0 END) AS fg3_att,
    SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 2 AND cs.parameters_type = 'lay-up' AND cs.parameters_made = 'made' THEN 1 ELSE 0 END) AS layup_made,
    SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 2 AND cs.parameters_type = 'lay-up' THEN 1 ELSE 0 END) AS layup_att,
    SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 2 AND cs.parameters_type IN ('dunk', 'allyhoop') AND cs.parameters_made = 'made' THEN 1 ELSE 0 END) AS dunk_made,
    SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 2 AND cs.parameters_type IN ('dunk', 'allyhoop') THEN 1 ELSE 0 END) AS dunk_att,
    SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 3 AND z.is_corner3 IS TRUE AND cs.parameters_made = 'made' THEN 1 ELSE 0 END) AS c3_made,
    SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 3 AND z.is_corner3 IS TRUE THEN 1 ELSE 0 END) AS c3_att,
    SUM(CASE WHEN cs.type = 'shot' AND cs.parameters_points = 3 AND z.is_corner3 IS NOT NULL THEN 1 ELSE 0 END) AS c3_known_att
  FROM clean_stats cs
  JOIN basketball_test.schedule s ON s.game_id = cs.game_id
  LEFT JOIN basketball_test.shot_zones z ON z.game_id = cs.game_id AND z.id = cs.id
  GROUP BY cs.game_id, s.game_year, cs.team_id, cs.lineup_hash, cs.type_lineup, cs.own_starters, cs.opp_starters
),
onoff_lineup_segments AS (
  SELECT
    cs.game_id,
    cs.team_id,
    cs.lineup_hash,
    cs.own_starters,
    cs.opp_starters,
    cs.segment_id,
    MAX(cs.segment_seconds)::numeric AS seg_seconds
  FROM clean_stats cs
  WHERE cs.lineup_hash IS NOT NULL
    AND cs.segment_id IS NOT NULL
    AND cs.segment_seconds IS NOT NULL
  GROUP BY cs.game_id, cs.team_id, cs.lineup_hash, cs.own_starters, cs.opp_starters, cs.segment_id
),
onoff_lineup_minutes AS (
  SELECT
    game_id,
    team_id,
    lineup_hash,
    own_starters,
    opp_starters,
    ROUND(SUM(seg_seconds) / 60.0, 3) AS minutes
  FROM onoff_lineup_segments
  GROUP BY game_id, team_id, lineup_hash, own_starters, opp_starters
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
    SUM(lt.layup_made) AS layup_made,
    SUM(lt.layup_att) AS layup_att,
    SUM(lt.dunk_made) AS dunk_made,
    SUM(lt.dunk_att) AS dunk_att,
    SUM(lt.c3_made) AS c3_made,
    SUM(lt.c3_att) AS c3_att,
    SUM(lt.c3_known_att) AS c3_known_att,
    SUM(CASE WHEN lt.type_lineup = 'offense'
             THEN COALESCE(lm.minutes, 0) ELSE 0 END) AS onoff_minutes
  FROM base0 b0
  JOIN lineup_totals lt
    ON lt.lineup_hash = b0.lineup_hash
   AND lt.team_id = b0.team_id
  LEFT JOIN onoff_lineup_minutes lm
    ON lm.game_id = lt.game_id
   AND lm.team_id = lt.team_id
   AND lm.lineup_hash = lt.lineup_hash
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
    cs.end_game_seconds_remaining,
    cs.segment_seconds
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
    MAX(cd.segment_seconds) AS stint_seconds
  FROM combined_data cd
  WHERE cd.segment_id IS NOT NULL
    AND cd.segment_seconds IS NOT NULL
  GROUP BY cd.player_id, cd.team_id, cd.game_id, cd.game_year,
           cd.is_on_key, cd.num_starters, cd.own_starters, cd.opp_starters,
           cd.segment_id
),
player_minutes AS (
  SELECT
    player_id, team_id, game_id, game_year, is_on_key,
    num_starters, own_starters, opp_starters,
    SUM(stint_seconds) / 60.0 AS minutes
  FROM segment_times
  GROUP BY player_id, team_id, game_id, game_year, is_on_key,
           num_starters, own_starters, opp_starters
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
    count(CASE WHEN cd.type = 'steal' THEN 1 END) AS steal_count,
    count(CASE WHEN cd.type = 'deflection' THEN 1 END) AS deflection_count,
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
  SUM(ss.steal_count)::bigint         AS steal_count,
  SUM(ss.deflection_count)::bigint    AS deflection_count,
  SUM(ss.total_ft_attempts)::bigint   AS total_ft_attempts,
  SUM(ss.total_fga)::bigint           AS total_fga,
  SUM(ss.total_fgm)::bigint           AS total_fgm,
  SUM(ss.total_fg3_made)::bigint      AS total_fg3_made,
  SUM(ss.player_ts_poss_count)::bigint AS player_ts_poss_count,
  SUM(ss.player_tov_count)::bigint     AS player_tov_count,
  MAX(pm.minutes) FILTER (WHERE ss.type_lineup = 'offense') AS minutes
FROM segment_stats ss
LEFT JOIN player_minutes pm
  ON pm.player_id = ss.player_id
 AND pm.team_id = ss.team_id
 AND pm.game_id = ss.game_id
 AND pm.game_year = ss.game_year
 AND pm.is_on_key = ss.is_on_key
 AND pm.num_starters = ss.num_starters
 AND pm.own_starters = ss.own_starters
 AND pm.opp_starters = ss.opp_starters
GROUP BY ss.player_id, ss.team_id, ss.game_id, ss.game_year, ss.is_on_key,
         ss.type_lineup, ss.num_starters, ss.own_starters, ss.opp_starters
UNION ALL
-- Slices with floor time but no offense-perspective row in segment_stats.
-- segment_stats drives the rows above and is grouped by type_lineup, so a
-- slice in which the player's lineup recorded no offensive possession had no
-- offense row for MAX(pm.minutes) FILTER (...) to land on, and its minutes
-- were dropped entirely. Emitted here as offense rows with zero counts and
-- their real minutes -- the same treatment sub_lineups_by_day and
-- lineup_four_factors_by_game get. They cannot collide with idx_pff_pk:
-- by construction no offense row exists for the slice.
SELECT
  pm.player_id,
  pm.team_id,
  pm.game_id,
  pm.game_year,
  pm.is_on_key,
  'offense'::text AS type_lineup,
  pm.num_starters,
  pm.own_starters,
  pm.opp_starters,
  0::numeric AS total_points,
  0::bigint  AS total_poss,
  0::bigint  AS ts_poss_count,
  0::bigint  AS oreb_count,
  0::bigint  AS oreb_opportunities,
  0::bigint  AS tov_count,
  0::bigint  AS steal_count,
  0::bigint  AS deflection_count,
  0::bigint  AS total_ft_attempts,
  0::bigint  AS total_fga,
  0::bigint  AS total_fgm,
  0::bigint  AS total_fg3_made,
  0::bigint  AS player_ts_poss_count,
  0::bigint  AS player_tov_count,
  pm.minutes
FROM player_minutes pm
WHERE NOT EXISTS (
    SELECT 1 FROM segment_stats s2
     WHERE s2.player_id = pm.player_id
       AND s2.team_id = pm.team_id
       AND s2.game_id = pm.game_id
       AND s2.game_year = pm.game_year
       AND s2.is_on_key = pm.is_on_key
       AND s2.num_starters = pm.num_starters
       AND s2.own_starters = pm.own_starters
       AND s2.opp_starters = pm.opp_starters
       AND s2.type_lineup = 'offense')
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
  ff.steal_count,
  ff.deflection_count,
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
  op.layup_made::int AS layup_made,
  op.layup_att::int AS layup_att,
  op.dunk_made::int AS dunk_made,
  op.dunk_att::int AS dunk_att,
  op.c3_made::int AS c3_made,
  op.c3_att::int AS c3_att,
  op.c3_known_att::int AS c3_known_att,
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
