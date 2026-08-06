-- Read-only EuroLeague player on/off PPP/ratings walkthrough.
--
-- Israeli-compatible convention:
--   offensive rating = 100 * points scored / offensive possessions
--   defensive rating = 100 * points allowed / defensive possessions
--   net on/off impact = (OffRtg_ON - OffRtg_OFF)
--                     - (DefRtg_ON - DefRtg_OFF)
--
-- Points use the package lineup at the scoring action. Possessions use the
-- package lineup at the deterministic possession endpoint. This preserves
-- substitutions within and-one/free-throw sequences instead of forcing all
-- points onto the endpoint lineup.

WITH target_game AS (
  SELECT
    s.game_id,
    s.home_team_id,
    s.away_team_id
  FROM euroleague.schedule s
  WHERE s.competition = 'E'
    AND s.season = 2025
    AND s.gamecode = 1
),
event_context AS (
  SELECT
    ar.game_id,
    ar.source_event_order,
    ar.team_id AS event_team_id,
    al.home_lineup_id,
    al.away_lineup_id,
    tg.home_team_id,
    tg.away_team_id,
    CASE ar.play_type
      WHEN '2FGM' THEN 2
      WHEN '3FGM' THEN 3
      WHEN 'FTM' THEN 1
      ELSE 0
    END AS event_points,
    p.offense_team_id AS endpoint_offense_team_id
  FROM target_game tg
  JOIN euroleague.actions_raw ar
    ON ar.game_id = tg.game_id
  JOIN euroleague.action_lineups al
    ON al.game_id = ar.game_id
   AND al.source_event_order = ar.source_event_order
  LEFT JOIN euroleague.possessions p
    ON p.game_id = ar.game_id
   AND p.endpoint_source_event_order = ar.source_event_order
),
team_event_context AS (
  SELECT
    ec.game_id,
    ec.source_event_order,
    ec.home_team_id AS team_id,
    ec.home_lineup_id AS lineup_id,
    CASE WHEN ec.event_team_id = ec.home_team_id
      THEN ec.event_points ELSE 0 END AS offensive_points,
    CASE WHEN ec.event_team_id = ec.away_team_id
      THEN ec.event_points ELSE 0 END AS defensive_points,
    CASE WHEN ec.endpoint_offense_team_id = ec.home_team_id
      THEN 1 ELSE 0 END AS offensive_possessions,
    CASE WHEN ec.endpoint_offense_team_id = ec.away_team_id
      THEN 1 ELSE 0 END AS defensive_possessions
  FROM event_context ec

  UNION ALL

  SELECT
    ec.game_id,
    ec.source_event_order,
    ec.away_team_id AS team_id,
    ec.away_lineup_id AS lineup_id,
    CASE WHEN ec.event_team_id = ec.away_team_id
      THEN ec.event_points ELSE 0 END AS offensive_points,
    CASE WHEN ec.event_team_id = ec.home_team_id
      THEN ec.event_points ELSE 0 END AS defensive_points,
    CASE WHEN ec.endpoint_offense_team_id = ec.away_team_id
      THEN 1 ELSE 0 END AS offensive_possessions,
    CASE WHEN ec.endpoint_offense_team_id = ec.home_team_id
      THEN 1 ELSE 0 END AS defensive_possessions
  FROM event_context ec
),
player_event_exposure AS (
  SELECT
    tec.game_id,
    tec.team_id,
    fr.player_id,
    p.display_name AS player_name,
    EXISTS (
      SELECT 1
      FROM euroleague.lineup_players lp
      WHERE lp.lineup_id = tec.lineup_id
        AND lp.player_id = fr.player_id
    ) AS is_on,
    tec.offensive_points,
    tec.defensive_points,
    tec.offensive_possessions,
    tec.defensive_possessions
  FROM team_event_context tec
  JOIN euroleague.full_rosters fr
    ON fr.game_id = tec.game_id
   AND fr.team_id = tec.team_id
  JOIN euroleague.players p
    ON p.player_id = fr.player_id
   -- The package's box-score frame contains aggregate Team/Total rows. They
   -- remain in the trial's raw evidence but are not player exposure records.
   AND p.provider_player_id NOT IN ('Team', 'Total')
),
player_buckets AS (
  SELECT
    pee.team_id,
    pee.player_id,
    pee.player_name,
    pee.is_on,
    SUM(pee.offensive_points)::integer AS offensive_points,
    SUM(pee.offensive_possessions)::integer AS offensive_possessions,
    SUM(pee.defensive_points)::integer AS defensive_points,
    SUM(pee.defensive_possessions)::integer AS defensive_possessions
  FROM player_event_exposure pee
  GROUP BY
    pee.team_id,
    pee.player_id,
    pee.player_name,
    pee.is_on
),
player_pivot AS (
  SELECT
    pb.team_id,
    pb.player_id,
    pb.player_name,
    COALESCE(SUM(pb.offensive_points)
      FILTER (WHERE pb.is_on), 0) AS off_on_points,
    COALESCE(SUM(pb.offensive_possessions)
      FILTER (WHERE pb.is_on), 0) AS off_on_possessions,
    COALESCE(SUM(pb.offensive_points)
      FILTER (WHERE NOT pb.is_on), 0) AS off_off_points,
    COALESCE(SUM(pb.offensive_possessions)
      FILTER (WHERE NOT pb.is_on), 0) AS off_off_possessions,
    COALESCE(SUM(pb.defensive_points)
      FILTER (WHERE pb.is_on), 0) AS def_on_points,
    COALESCE(SUM(pb.defensive_possessions)
      FILTER (WHERE pb.is_on), 0) AS def_on_possessions,
    COALESCE(SUM(pb.defensive_points)
      FILTER (WHERE NOT pb.is_on), 0) AS def_off_points,
    COALESCE(SUM(pb.defensive_possessions)
      FILTER (WHERE NOT pb.is_on), 0) AS def_off_possessions
  FROM player_buckets pb
  GROUP BY pb.team_id, pb.player_id, pb.player_name
),
ratings AS (
  SELECT
    pp.*,
    100.0 * pp.off_on_points
      / NULLIF(pp.off_on_possessions, 0) AS off_rtg_on,
    100.0 * pp.off_off_points
      / NULLIF(pp.off_off_possessions, 0) AS off_rtg_off,
    100.0 * pp.def_on_points
      / NULLIF(pp.def_on_possessions, 0) AS def_rtg_on,
    100.0 * pp.def_off_points
      / NULLIF(pp.def_off_possessions, 0) AS def_rtg_off
  FROM player_pivot pp
)
SELECT
  t.provider_team_code AS team,
  r.player_name,
  r.off_on_points,
  r.off_on_possessions,
  ROUND(r.off_rtg_on, 1) AS off_rtg_on,
  r.off_off_points,
  r.off_off_possessions,
  ROUND(r.off_rtg_off, 1) AS off_rtg_off,
  r.def_on_points,
  r.def_on_possessions,
  ROUND(r.def_rtg_on, 1) AS def_rtg_on,
  r.def_off_points,
  r.def_off_possessions,
  ROUND(r.def_rtg_off, 1) AS def_rtg_off,
  ROUND(r.off_rtg_on - r.off_rtg_off, 1) AS offensive_on_off,
  ROUND(r.def_rtg_on - r.def_rtg_off, 1) AS defensive_on_off,
  ROUND(
    (r.off_rtg_on - r.def_rtg_on)
    - (r.off_rtg_off - r.def_rtg_off),
    1
  ) AS net_on_off
FROM ratings r
JOIN euroleague.teams t
  ON t.team_id = r.team_id
ORDER BY t.provider_team_code, r.player_name;
