-- Read-only compatibility test for the existing Israeli player ON/OFF SQL.
--
-- Source adapter: EuroLeague raw actions, deterministic endpoints, and package
-- lineups are mapped to the additive columns consumed by the established SQL.
-- Rate formulas: copied from sql/functions/four_factors_compute.sql.
-- ON/OFF partition: follows sql/functions/onoff_compute.sql.
--
-- This query creates no objects and changes no data.

WITH target_game AS (
  SELECT s.game_id, s.home_team_id, s.away_team_id
  FROM euroleague.schedule s
  WHERE s.competition = 'E' AND s.season = 2025 AND s.gamecode = 1
),
event_base AS (
  SELECT
    ar.game_id,
    ar.source_event_order,
    ar.team_id AS event_team_id,
    ar.player_id AS action_player_id,
    ar.play_type,
    ac.synthetic_parent_order,
    ac.synthetic_ft_trip_id,
    root.play_type AS parent_play_type,
    al.home_lineup_id,
    al.away_lineup_id,
    tg.home_team_id,
    tg.away_team_id,
    p.offense_team_id AS endpoint_offense_team_id,
    row_number() OVER (
      PARTITION BY ar.game_id, ac.synthetic_ft_trip_id
      ORDER BY ar.source_event_order DESC
    ) AS ft_reverse_order
  FROM target_game tg
  JOIN euroleague.actions_raw ar ON ar.game_id = tg.game_id
  JOIN euroleague.actions_clean ac
    ON ac.game_id = ar.game_id
   AND ac.source_event_order = ar.source_event_order
  JOIN euroleague.actions_raw root
    ON root.game_id = ac.game_id
   AND root.source_event_order = ac.synthetic_parent_order
  JOIN euroleague.action_lineups al
    ON al.game_id = ar.game_id
   AND al.source_event_order = ar.source_event_order
  LEFT JOIN euroleague.possessions p
    ON p.game_id = ar.game_id
   AND p.endpoint_source_event_order = ar.source_event_order
),
event_metrics AS (
  SELECT
    eb.*,
    CASE eb.play_type
      WHEN '2FGM' THEN 2 WHEN '3FGM' THEN 3 WHEN 'FTM' THEN 1 ELSE 0
    END AS points,
    CASE WHEN eb.play_type IN ('2FGM', '2FGA', '3FGM', '3FGA') THEN 1
         WHEN eb.play_type IN ('FTM', 'FTA')
          AND eb.parent_play_type = 'CM'
          AND eb.ft_reverse_order = 1 THEN 1 ELSE 0 END AS ts_possessions,
    CASE WHEN eb.play_type = 'O' THEN 1 ELSE 0 END AS orebounds,
    CASE WHEN eb.play_type IN ('2FGA', '3FGA') THEN 1
         WHEN eb.play_type = 'FTA'
          AND eb.parent_play_type = 'CM'
          AND eb.ft_reverse_order = 1 THEN 1 ELSE 0 END AS oreb_opportunities,
    CASE WHEN eb.play_type = 'TO' THEN 1 ELSE 0 END AS turnovers,
    CASE WHEN eb.play_type = 'ST' THEN 1 ELSE 0 END AS steals,
    0 AS deflections,
    CASE WHEN eb.play_type IN ('FTM', 'FTA') THEN 1 ELSE 0 END AS ft_attempts,
    CASE WHEN eb.play_type IN ('2FGM', '2FGA', '3FGM', '3FGA') THEN 1 ELSE 0 END AS fga,
    CASE WHEN eb.play_type IN ('2FGM', '3FGM') THEN 1 ELSE 0 END AS fgm,
    CASE WHEN eb.play_type = '3FGM' THEN 1 ELSE 0 END AS fg3_made,
    CASE WHEN eb.play_type = '2FGM' THEN 1 ELSE 0 END AS fg2_made,
    CASE WHEN eb.play_type IN ('2FGM', '2FGA') THEN 1 ELSE 0 END AS fg2_att,
    CASE WHEN eb.play_type = '3FGM' THEN 1 ELSE 0 END AS fg3_made_split,
    CASE WHEN eb.play_type IN ('3FGM', '3FGA') THEN 1 ELSE 0 END AS fg3_att
  FROM event_base eb
),
team_event_context AS (
  SELECT
    em.game_id,
    em.source_event_order,
    side.team_id,
    side.opponent_team_id,
    side.lineup_id,
    CASE WHEN em.event_team_id = side.team_id THEN em.points ELSE 0 END AS off_points,
    CASE WHEN em.event_team_id = side.opponent_team_id THEN em.points ELSE 0 END AS def_points,
    CASE WHEN em.endpoint_offense_team_id = side.team_id THEN 1 ELSE 0 END AS off_possessions,
    CASE WHEN em.endpoint_offense_team_id = side.opponent_team_id THEN 1 ELSE 0 END AS def_possessions,
    CASE WHEN em.event_team_id = side.team_id THEN em.ts_possessions ELSE 0 END AS off_ts_possessions,
    CASE WHEN em.event_team_id = side.opponent_team_id THEN em.ts_possessions ELSE 0 END AS def_ts_possessions,
    CASE WHEN em.event_team_id = side.team_id THEN em.orebounds ELSE 0 END AS off_orebounds,
    CASE WHEN em.event_team_id = side.opponent_team_id THEN em.orebounds ELSE 0 END AS def_orebounds,
    CASE WHEN em.event_team_id = side.team_id THEN em.oreb_opportunities ELSE 0 END AS off_oreb_opportunities,
    CASE WHEN em.event_team_id = side.opponent_team_id THEN em.oreb_opportunities ELSE 0 END AS def_oreb_opportunities,
    CASE WHEN em.event_team_id = side.team_id THEN em.turnovers ELSE 0 END AS off_turnovers,
    CASE WHEN em.event_team_id = side.opponent_team_id THEN em.turnovers ELSE 0 END AS def_turnovers,
    CASE WHEN em.event_team_id = side.team_id THEN em.steals ELSE 0 END AS off_steals,
    CASE WHEN em.event_team_id = side.opponent_team_id THEN em.steals ELSE 0 END AS def_steals,
    CASE WHEN em.event_team_id = side.team_id THEN em.deflections ELSE 0 END AS off_deflections,
    CASE WHEN em.event_team_id = side.opponent_team_id THEN em.deflections ELSE 0 END AS def_deflections,
    CASE WHEN em.event_team_id = side.team_id THEN em.ft_attempts ELSE 0 END AS off_ft_attempts,
    CASE WHEN em.event_team_id = side.opponent_team_id THEN em.ft_attempts ELSE 0 END AS def_ft_attempts,
    CASE WHEN em.event_team_id = side.team_id THEN em.fga ELSE 0 END AS off_fga,
    CASE WHEN em.event_team_id = side.opponent_team_id THEN em.fga ELSE 0 END AS def_fga,
    CASE WHEN em.event_team_id = side.team_id THEN em.fgm ELSE 0 END AS off_fgm,
    CASE WHEN em.event_team_id = side.opponent_team_id THEN em.fgm ELSE 0 END AS def_fgm,
    CASE WHEN em.event_team_id = side.team_id THEN em.fg3_made ELSE 0 END AS off_fg3_made,
    CASE WHEN em.event_team_id = side.opponent_team_id THEN em.fg3_made ELSE 0 END AS def_fg3_made,
    CASE WHEN em.event_team_id = side.team_id THEN em.fg2_made ELSE 0 END AS off_fg2_made,
    CASE WHEN em.event_team_id = side.opponent_team_id THEN em.fg2_made ELSE 0 END AS def_fg2_made,
    CASE WHEN em.event_team_id = side.team_id THEN em.fg2_att ELSE 0 END AS off_fg2_att,
    CASE WHEN em.event_team_id = side.opponent_team_id THEN em.fg2_att ELSE 0 END AS def_fg2_att,
    CASE WHEN em.event_team_id = side.team_id THEN em.fg3_made_split ELSE 0 END AS off_fg3_made_split,
    CASE WHEN em.event_team_id = side.opponent_team_id THEN em.fg3_made_split ELSE 0 END AS def_fg3_made_split,
    CASE WHEN em.event_team_id = side.team_id THEN em.fg3_att ELSE 0 END AS off_fg3_att,
    CASE WHEN em.event_team_id = side.opponent_team_id THEN em.fg3_att ELSE 0 END AS def_fg3_att
  FROM event_metrics em
  CROSS JOIN LATERAL (
    VALUES
      (em.home_team_id, em.away_team_id, em.home_lineup_id),
      (em.away_team_id, em.home_team_id, em.away_lineup_id)
  ) AS side(team_id, opponent_team_id, lineup_id)
),
player_exposure AS (
  SELECT
    tec.*,
    fr.player_id,
    p.display_name AS player_name,
    EXISTS (
      SELECT 1 FROM euroleague.lineup_players lp
      WHERE lp.lineup_id = tec.lineup_id AND lp.player_id = fr.player_id
    )::integer AS is_on_key
  FROM team_event_context tec
  JOIN euroleague.full_rosters fr
    ON fr.game_id = tec.game_id AND fr.team_id = tec.team_id
  JOIN euroleague.players p ON p.player_id = fr.player_id
  WHERE p.provider_player_id NOT IN ('Team', 'Total')
),
player_context AS (
  SELECT
    pe.game_id, pe.team_id, pe.player_id, pe.player_name, pe.is_on_key,
    context.type_lineup,
    context.total_points, context.total_poss, context.ts_poss_count,
    context.oreb_count, context.oreb_opportunities, context.tov_count,
    context.steal_count, context.deflection_count,
    context.total_ft_attempts, context.total_fga, context.total_fgm,
    context.total_fg3_made, context.fg2_made, context.fg2_att,
    context.fg3_made, context.fg3_att
  FROM player_exposure pe
  CROSS JOIN LATERAL (
    VALUES
      ('offense', pe.off_points, pe.off_possessions, pe.off_ts_possessions,
       pe.off_orebounds, pe.off_oreb_opportunities, pe.off_turnovers,
       pe.off_steals, pe.off_deflections, pe.off_ft_attempts, pe.off_fga,
       pe.off_fgm, pe.off_fg3_made, pe.off_fg2_made, pe.off_fg2_att,
       pe.off_fg3_made_split, pe.off_fg3_att),
      ('defense', pe.def_points, pe.def_possessions, pe.def_ts_possessions,
       pe.def_orebounds, pe.def_oreb_opportunities, pe.def_turnovers,
       pe.def_steals, pe.def_deflections, pe.def_ft_attempts, pe.def_fga,
       pe.def_fgm, pe.def_fg3_made, pe.def_fg2_made, pe.def_fg2_att,
       pe.def_fg3_made_split, pe.def_fg3_att)
  ) AS context(
    type_lineup, total_points, total_poss, ts_poss_count, oreb_count,
    oreb_opportunities, tov_count, steal_count, deflection_count,
    total_ft_attempts, total_fga, total_fgm, total_fg3_made,
    fg2_made, fg2_att, fg3_made, fg3_att
  )
),
agg AS (
  SELECT
    pc.team_id, pc.player_id, pc.player_name, pc.is_on_key, pc.type_lineup,
    sum(pc.total_points) AS total_points,
    sum(pc.total_poss) AS total_poss,
    sum(pc.ts_poss_count) AS ts_poss_count,
    sum(pc.oreb_count) AS oreb_count,
    sum(pc.oreb_opportunities) AS oreb_opportunities,
    sum(pc.tov_count) AS tov_count,
    sum(pc.steal_count) AS steal_count,
    sum(pc.deflection_count) AS deflection_count,
    sum(pc.total_ft_attempts) AS total_ft_attempts,
    sum(pc.total_fga) AS total_fga,
    sum(pc.total_fgm) AS total_fgm,
    sum(pc.total_fg3_made) AS total_fg3_made,
    sum(pc.fg2_made) AS fg2_made,
    sum(pc.fg2_att) AS fg2_att,
    sum(pc.fg3_made) AS fg3_made,
    sum(pc.fg3_att) AS fg3_att
  FROM player_context pc
  GROUP BY pc.team_id, pc.player_id, pc.player_name, pc.is_on_key, pc.type_lineup
),
-- Exact existing rate definitions from four_factors_compute.sql.
calc_rates AS (
  SELECT
    a.*,
    a.total_points / (2.0 * NULLIF(a.ts_poss_count, 0)::numeric) AS ts_pct,
    (a.total_fgm + 0.5 * a.total_fg3_made)::numeric
      / NULLIF(a.total_fga, 0)::numeric AS efg_pct,
    a.oreb_count::numeric / NULLIF(a.oreb_opportunities, 0)::numeric AS oreb_pct,
    a.tov_count::numeric / NULLIF(a.total_poss, 0)::numeric AS tov_pct,
    (a.steal_count + a.deflection_count)::numeric
      / NULLIF(a.total_poss, 0)::numeric AS disruption_rate,
    a.total_ft_attempts::numeric / NULLIF(a.total_fga, 0)::numeric AS ft_rate,
    100.0 * a.total_points / NULLIF(a.total_poss, 0)::numeric AS rating
  FROM agg a
),
team_validation AS (
  SELECT
    tec.team_id,
    sum(tec.off_points) = tb.points AS points_exact,
    sum(tec.off_fga) = tb.fg2_attempted + tb.fg3_attempted AS fga_exact,
    sum(tec.off_fgm) = tb.fg2_made + tb.fg3_made AS fgm_exact,
    sum(tec.off_fg3_made) = tb.fg3_made AS fg3m_exact,
    sum(tec.off_ft_attempts) = tb.ft_attempted AS fta_exact,
    sum(tec.off_orebounds) = tb.offensive_rebounds AS oreb_exact,
    sum(tec.off_turnovers) = tb.turnovers AS turnovers_exact
  FROM team_event_context tec
  JOIN euroleague.team_boxscores tb
    ON tb.game_id = tec.game_id AND tb.team_id = tec.team_id
  GROUP BY tec.team_id, tb.points, tb.fg2_attempted, tb.fg3_attempted,
           tb.fg2_made, tb.fg3_made, tb.ft_attempted,
           tb.offensive_rebounds, tb.turnovers
),
player_partition_validation AS (
  SELECT
    a.team_id, a.player_id, a.type_lineup,
    bool_and(
      a.total_points = team.total_points
      AND a.total_poss = team.total_poss
      AND a.ts_poss_count = team.ts_poss_count
      AND a.oreb_count = team.oreb_count
      AND a.oreb_opportunities = team.oreb_opportunities
      AND a.tov_count = team.tov_count
      AND a.total_ft_attempts = team.total_ft_attempts
      AND a.total_fga = team.total_fga
      AND a.total_fgm = team.total_fgm
      AND a.total_fg3_made = team.total_fg3_made
    ) AS partition_exact
  FROM (
    SELECT team_id, player_id, type_lineup,
      sum(total_points) total_points, sum(total_poss) total_poss,
      sum(ts_poss_count) ts_poss_count, sum(oreb_count) oreb_count,
      sum(oreb_opportunities) oreb_opportunities, sum(tov_count) tov_count,
      sum(total_ft_attempts) total_ft_attempts, sum(total_fga) total_fga,
      sum(total_fgm) total_fgm, sum(total_fg3_made) total_fg3_made
    FROM agg GROUP BY team_id, player_id, type_lineup
  ) a
  JOIN (
    SELECT team_id, type_lineup,
      sum(total_points) total_points, sum(total_poss) total_poss,
      sum(ts_poss_count) ts_poss_count, sum(oreb_count) oreb_count,
      sum(oreb_opportunities) oreb_opportunities, sum(tov_count) tov_count,
      sum(total_ft_attempts) total_ft_attempts, sum(total_fga) total_fga,
      sum(total_fgm) total_fgm, sum(total_fg3_made) total_fg3_made
    FROM player_context
    WHERE player_id = (
      SELECT min(pc2.player_id) FROM player_context pc2
      WHERE pc2.team_id = player_context.team_id
    )
    GROUP BY team_id, type_lineup
  ) team USING (team_id, type_lineup)
  GROUP BY a.team_id, a.player_id, a.type_lineup
),
pivoted AS (
  SELECT
    cr.team_id, cr.player_id, cr.player_name,
    max(cr.rating) FILTER (WHERE cr.type_lineup = 'offense' AND cr.is_on_key = 1) AS off_rating_on,
    max(cr.rating) FILTER (WHERE cr.type_lineup = 'offense' AND cr.is_on_key = 0) AS off_rating_off,
    max(cr.rating) FILTER (WHERE cr.type_lineup = 'defense' AND cr.is_on_key = 1) AS def_rating_on,
    max(cr.rating) FILTER (WHERE cr.type_lineup = 'defense' AND cr.is_on_key = 0) AS def_rating_off,
    max(cr.ts_pct) FILTER (WHERE cr.type_lineup = 'offense' AND cr.is_on_key = 1) AS off_ts_on,
    max(cr.ts_pct) FILTER (WHERE cr.type_lineup = 'offense' AND cr.is_on_key = 0) AS off_ts_off,
    max(cr.efg_pct) FILTER (WHERE cr.type_lineup = 'offense' AND cr.is_on_key = 1) AS off_efg_on,
    max(cr.efg_pct) FILTER (WHERE cr.type_lineup = 'offense' AND cr.is_on_key = 0) AS off_efg_off,
    max(cr.oreb_pct) FILTER (WHERE cr.type_lineup = 'offense' AND cr.is_on_key = 1) AS off_oreb_on,
    max(cr.oreb_pct) FILTER (WHERE cr.type_lineup = 'offense' AND cr.is_on_key = 0) AS off_oreb_off,
    max(cr.tov_pct) FILTER (WHERE cr.type_lineup = 'offense' AND cr.is_on_key = 1) AS off_tov_on,
    max(cr.tov_pct) FILTER (WHERE cr.type_lineup = 'offense' AND cr.is_on_key = 0) AS off_tov_off,
    max(cr.ft_rate) FILTER (WHERE cr.type_lineup = 'offense' AND cr.is_on_key = 1) AS off_ftr_on,
    max(cr.ft_rate) FILTER (WHERE cr.type_lineup = 'offense' AND cr.is_on_key = 0) AS off_ftr_off,
    bool_and(ppv.partition_exact) AS player_partition_exact
  FROM calc_rates cr
  JOIN player_partition_validation ppv
    ON ppv.team_id = cr.team_id AND ppv.player_id = cr.player_id
   AND ppv.type_lineup = cr.type_lineup
  GROUP BY cr.team_id, cr.player_id, cr.player_name
)
SELECT
  t.provider_team_code AS team,
  player.provider_player_id,
  cr.player_name,
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
  ppv.partition_exact AS player_partition_exact,
  tv.points_exact AND tv.fga_exact AND tv.fgm_exact AND tv.fg3m_exact
    AND tv.fta_exact AND tv.oreb_exact AND tv.turnovers_exact
    AS official_additive_totals_exact
FROM calc_rates cr
JOIN euroleague.teams t ON t.team_id = cr.team_id
JOIN euroleague.players player ON player.player_id = cr.player_id
JOIN player_partition_validation ppv
  ON ppv.team_id = cr.team_id AND ppv.player_id = cr.player_id
 AND ppv.type_lineup = cr.type_lineup
JOIN team_validation tv ON tv.team_id = cr.team_id
ORDER BY t.provider_team_code, cr.player_name, cr.type_lineup, cr.is_on_key;
