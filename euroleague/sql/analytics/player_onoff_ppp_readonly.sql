-- Read-only EuroLeague player on/off PPP/ratings walkthrough.
--
-- Every event already has both package lineups in canonical actions. The
-- team-perspective fact supplies action measures; the endpoint annotation on
-- actions supplies possession ownership. Ratios are calculated only after the
-- requested game/player buckets have been aggregated.

WITH target_game AS (
  SELECT s.game_id
    FROM euroleague.schedule s
   WHERE s.competition = 'E'
     AND s.season = 2025
     AND s.gamecode = 1
),
player_event_exposure AS (
  SELECT
    atc.team_id,
    fr.player_id,
    p.display_name AS player_name,
    fr.source_player_name = ANY(atc.own_lineup) AS is_on,
    CASE WHEN atc.type_lineup = 'offense' THEN atc.points ELSE 0 END
      AS offensive_points,
    CASE WHEN atc.type_lineup = 'defense' THEN atc.points ELSE 0 END
      AS defensive_points,
    CASE WHEN a.end_possession
               AND a.possession_offense_team_id = atc.team_id
         THEN 1 ELSE 0 END AS offensive_possessions,
    CASE WHEN a.end_possession
               AND a.possession_offense_team_id = atc.opponent_team_id
         THEN 1 ELSE 0 END AS defensive_possessions
  FROM target_game tg
  JOIN euroleague.action_team_context_actions atc
    ON atc.game_id = tg.game_id
  JOIN euroleague.actions a
    ON a.game_id = atc.game_id
   AND a.source_event_order = atc.source_event_order
  JOIN euroleague.full_rosters fr
    ON fr.game_id = atc.game_id
   AND fr.team_id = atc.team_id
  JOIN euroleague.players p ON p.player_id = fr.player_id
   AND lower(p.provider_player_id) NOT IN ('team', 'total')
),
player_buckets AS (
  SELECT
    team_id,
    player_id,
    player_name,
    is_on,
    sum(offensive_points)::integer AS offensive_points,
    sum(offensive_possessions)::integer AS offensive_possessions,
    sum(defensive_points)::integer AS defensive_points,
    sum(defensive_possessions)::integer AS defensive_possessions
  FROM player_event_exposure
  GROUP BY team_id, player_id, player_name, is_on
),
player_pivot AS (
  SELECT
    team_id,
    player_id,
    player_name,
    coalesce(sum(offensive_points) FILTER (WHERE is_on), 0) AS off_on_points,
    coalesce(sum(offensive_possessions) FILTER (WHERE is_on), 0)
      AS off_on_possessions,
    coalesce(sum(offensive_points) FILTER (WHERE NOT is_on), 0)
      AS off_off_points,
    coalesce(sum(offensive_possessions) FILTER (WHERE NOT is_on), 0)
      AS off_off_possessions,
    coalesce(sum(defensive_points) FILTER (WHERE is_on), 0) AS def_on_points,
    coalesce(sum(defensive_possessions) FILTER (WHERE is_on), 0)
      AS def_on_possessions,
    coalesce(sum(defensive_points) FILTER (WHERE NOT is_on), 0)
      AS def_off_points,
    coalesce(sum(defensive_possessions) FILTER (WHERE NOT is_on), 0)
      AS def_off_possessions
  FROM player_buckets
  GROUP BY team_id, player_id, player_name
),
ratings AS (
  SELECT
    pp.*,
    100.0 * pp.off_on_points / NULLIF(pp.off_on_possessions, 0) AS off_rtg_on,
    100.0 * pp.off_off_points / NULLIF(pp.off_off_possessions, 0) AS off_rtg_off,
    100.0 * pp.def_on_points / NULLIF(pp.def_on_possessions, 0) AS def_rtg_on,
    100.0 * pp.def_off_points / NULLIF(pp.def_off_possessions, 0) AS def_rtg_off
  FROM player_pivot pp
)
SELECT
  t.provider_team_code AS team,
  r.player_name,
  r.off_on_points,
  r.off_on_possessions,
  round(r.off_rtg_on, 1) AS off_rtg_on,
  r.off_off_points,
  r.off_off_possessions,
  round(r.off_rtg_off, 1) AS off_rtg_off,
  r.def_on_points,
  r.def_on_possessions,
  round(r.def_rtg_on, 1) AS def_rtg_on,
  r.def_off_points,
  r.def_off_possessions,
  round(r.def_rtg_off, 1) AS def_rtg_off,
  round(r.off_rtg_on - r.off_rtg_off, 1) AS offensive_on_off,
  round(r.def_rtg_on - r.def_rtg_off, 1) AS defensive_on_off,
  round(
    (r.off_rtg_on - r.def_rtg_on)
    - (r.off_rtg_off - r.def_rtg_off),
    1
  ) AS net_on_off
FROM ratings r
JOIN euroleague.teams t ON t.team_id = r.team_id
ORDER BY t.provider_team_code, r.player_name;
