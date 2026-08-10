-- Read-only validation of the established player four-factor formulas against
-- the current EuroLeague actions-derived facts. No objects or data are changed.

WITH target_game AS (
  SELECT game_id
    FROM euroleague.schedule
   WHERE competition = 'E' AND season = 2025 AND gamecode = 1
),
agg AS (
  SELECT
    f.game_id,
    f.team_id,
    f.player_id,
    f.is_on_key,
    f.type_lineup,
    sum(f.total_points) AS total_points,
    sum(f.total_poss) AS total_poss,
    sum(f.ts_poss_count) AS ts_poss_count,
    sum(f.oreb_count) AS oreb_count,
    sum(f.oreb_opportunities) AS oreb_opportunities,
    sum(f.tov_count) AS tov_count,
    sum(f.steal_count) AS steal_count,
    sum(f.deflection_count) AS deflection_count,
    sum(f.total_ft_attempts) AS total_ft_attempts,
    sum(f.total_fga) AS total_fga,
    sum(f.total_fgm) AS total_fgm,
    sum(f.total_fg3_made) AS total_fg3_made,
    sum(f.fg2_made) AS fg2_made,
    sum(f.fg2_att) AS fg2_att,
    sum(f.fg3_made) AS fg3_made,
    sum(f.fg3_att) AS fg3_att
  FROM euroleague.player_four_factors_by_game f
  JOIN target_game tg ON tg.game_id = f.game_id
  GROUP BY f.game_id, f.team_id, f.player_id, f.is_on_key, f.type_lineup
),
calc_rates AS (
  SELECT
    a.*,
    a.total_points / (2.0 * NULLIF(a.ts_poss_count, 0)::numeric) AS ts_pct,
    (a.total_fgm + 0.5 * a.total_fg3_made)::numeric
      / NULLIF(a.total_fga, 0)::numeric AS efg_pct,
    a.oreb_count::numeric
      / NULLIF(a.oreb_opportunities, 0)::numeric AS oreb_pct,
    a.tov_count::numeric / NULLIF(a.total_poss, 0)::numeric AS tov_pct,
    (a.steal_count + a.deflection_count)::numeric
      / NULLIF(a.total_poss, 0)::numeric AS disruption_rate,
    a.total_ft_attempts::numeric / NULLIF(a.total_fga, 0)::numeric AS ft_rate,
    100.0 * a.total_points / NULLIF(a.total_poss, 0)::numeric AS rating
  FROM agg a
),
team_totals AS (
  SELECT
    f.game_id,
    f.team_id,
    side.type_lineup,
    sum(side.total_points) AS total_points,
    sum(side.total_poss) AS total_poss,
    sum(side.ts_poss_count) AS ts_poss_count,
    sum(side.oreb_count) AS oreb_count,
    sum(side.oreb_opportunities) AS oreb_opportunities,
    sum(side.tov_count) AS tov_count,
    sum(side.total_ft_attempts) AS total_ft_attempts,
    sum(side.total_fga) AS total_fga,
    sum(side.total_fgm) AS total_fgm,
    sum(side.total_fg3_made) AS total_fg3_made
  FROM euroleague.team_four_factors_by_game f
  JOIN target_game tg ON tg.game_id = f.game_id
  CROSS JOIN LATERAL (
    VALUES
      ('offense', f.off_pts, f.off_poss, f.off_ts_poss, f.off_oreb,
       f.off_oreb_opp, f.off_tov, f.off_fta, f.off_fga, f.off_fgm, f.off_fg3m),
      ('defense', f.def_pts, f.def_poss, f.def_ts_poss, f.def_oreb,
       f.def_oreb_opp, f.def_tov, f.def_fta, f.def_fga, f.def_fgm, f.def_fg3m)
  ) AS side(
    type_lineup, total_points, total_poss, ts_poss_count, oreb_count,
    oreb_opportunities, tov_count, total_ft_attempts, total_fga, total_fgm,
    total_fg3_made
  )
  GROUP BY f.game_id, f.team_id, side.type_lineup
),
player_partition_validation AS (
  SELECT
    a.team_id,
    a.player_id,
    a.type_lineup,
    sum(a.total_points) = max(tt.total_points)
      AND sum(a.total_poss) = max(tt.total_poss)
      AND sum(a.ts_poss_count) = max(tt.ts_poss_count)
      AND sum(a.oreb_count) = max(tt.oreb_count)
      AND sum(a.oreb_opportunities) = max(tt.oreb_opportunities)
      AND sum(a.tov_count) = max(tt.tov_count)
      AND sum(a.total_ft_attempts) = max(tt.total_ft_attempts)
      AND sum(a.total_fga) = max(tt.total_fga)
      AND sum(a.total_fgm) = max(tt.total_fgm)
      AND sum(a.total_fg3_made) = max(tt.total_fg3_made)
      AS partition_exact
  FROM agg a
  JOIN team_totals tt
    ON tt.game_id = a.game_id
   AND tt.team_id = a.team_id
   AND tt.type_lineup = a.type_lineup
  GROUP BY a.team_id, a.player_id, a.type_lineup
),
team_validation AS (
  SELECT
    tt.team_id,
    tt.total_points = tb.points
      AND tt.total_fga = tb.fg2_attempted + tb.fg3_attempted
      AND tt.total_fgm = tb.fg2_made + tb.fg3_made
      AND tt.total_fg3_made = tb.fg3_made
      AND tt.total_ft_attempts = tb.ft_attempted
      AND tt.oreb_count = tb.offensive_rebounds
      AND tt.tov_count = tb.turnovers
      AS official_additive_totals_exact
  FROM team_totals tt
  JOIN euroleague.team_boxscores tb
    ON tb.game_id = tt.game_id AND tb.team_id = tt.team_id
  WHERE tt.type_lineup = 'offense'
)
SELECT
  t.provider_team_code AS team,
  player.provider_player_id,
  player.display_name AS player_name,
  cr.type_lineup,
  cr.is_on_key,
  cr.total_points,
  cr.total_poss,
  cr.ts_poss_count,
  cr.oreb_count,
  cr.oreb_opportunities,
  cr.tov_count,
  cr.total_ft_attempts,
  cr.total_fga,
  cr.total_fgm,
  cr.total_fg3_made,
  cr.fg2_made,
  cr.fg2_att,
  cr.fg3_made,
  cr.fg3_att,
  round(cr.rating, 1) AS rating,
  round(100 * cr.ts_pct, 1) AS ts_pct,
  round(100 * cr.efg_pct, 1) AS efg_pct,
  round(100 * cr.oreb_pct, 1) AS oreb_pct,
  round(100 * cr.tov_pct, 1) AS tov_pct,
  round(100 * cr.ft_rate, 1) AS ft_rate,
  ppv.partition_exact,
  tv.official_additive_totals_exact
FROM calc_rates cr
JOIN euroleague.teams t ON t.team_id = cr.team_id
JOIN euroleague.players player ON player.player_id = cr.player_id
JOIN player_partition_validation ppv
  ON ppv.team_id = cr.team_id
 AND ppv.player_id = cr.player_id
 AND ppv.type_lineup = cr.type_lineup
JOIN team_validation tv ON tv.team_id = cr.team_id
ORDER BY t.provider_team_code, player.display_name, cr.type_lineup, cr.is_on_key;
