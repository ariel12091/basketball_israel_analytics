-- EUROLEAGUE SHADOW SCHEMA: existing Israeli analytics compatibility layer.
-- All objects remain isolated to euroleague. Raw provider clocks are unchanged.

BEGIN;

CREATE TABLE IF NOT EXISTS euroleague.player_four_factors_by_game (
  player_id bigint NOT NULL REFERENCES euroleague.players(player_id),
  team_id bigint NOT NULL REFERENCES euroleague.teams(team_id),
  game_id bigint NOT NULL REFERENCES euroleague.schedule(game_id) ON DELETE CASCADE,
  game_year smallint NOT NULL,
  is_on_key smallint NOT NULL CHECK (is_on_key IN (0, 1)),
  type_lineup text NOT NULL CHECK (type_lineup IN ('offense', 'defense')),
  num_starters smallint NOT NULL CHECK (num_starters BETWEEN 0 AND 5),
  own_starters smallint NOT NULL CHECK (own_starters BETWEEN 0 AND 5),
  opp_starters smallint NOT NULL CHECK (opp_starters BETWEEN 0 AND 5),
  total_points numeric NOT NULL DEFAULT 0 CHECK (total_points >= 0),
  total_poss bigint NOT NULL DEFAULT 0 CHECK (total_poss >= 0),
  ts_poss_count bigint NOT NULL DEFAULT 0 CHECK (ts_poss_count >= 0),
  oreb_count bigint NOT NULL DEFAULT 0 CHECK (oreb_count >= 0),
  oreb_opportunities bigint NOT NULL DEFAULT 0 CHECK (oreb_opportunities >= 0),
  tov_count bigint NOT NULL DEFAULT 0 CHECK (tov_count >= 0),
  steal_count bigint NOT NULL DEFAULT 0 CHECK (steal_count >= 0),
  deflection_count bigint NOT NULL DEFAULT 0 CHECK (deflection_count >= 0),
  total_ft_attempts bigint NOT NULL DEFAULT 0 CHECK (total_ft_attempts >= 0),
  total_fga bigint NOT NULL DEFAULT 0 CHECK (total_fga >= 0),
  total_fgm bigint NOT NULL DEFAULT 0 CHECK (total_fgm >= 0),
  total_fg3_made bigint NOT NULL DEFAULT 0 CHECK (total_fg3_made >= 0),
  player_ts_poss_count bigint NOT NULL DEFAULT 0 CHECK (player_ts_poss_count >= 0),
  player_tov_count bigint NOT NULL DEFAULT 0 CHECK (player_tov_count >= 0),
  minutes numeric(10,3) NOT NULL DEFAULT 0 CHECK (minutes >= 0),
  fg2_made integer NOT NULL DEFAULT 0 CHECK (fg2_made >= 0),
  fg2_att integer NOT NULL DEFAULT 0 CHECK (fg2_att >= 0),
  fg3_made integer NOT NULL DEFAULT 0 CHECK (fg3_made >= 0),
  fg3_att integer NOT NULL DEFAULT 0 CHECK (fg3_att >= 0),
  layup_made integer NOT NULL DEFAULT 0 CHECK (layup_made >= 0),
  layup_att integer NOT NULL DEFAULT 0 CHECK (layup_att >= 0),
  dunk_made integer NOT NULL DEFAULT 0 CHECK (dunk_made >= 0),
  dunk_att integer NOT NULL DEFAULT 0 CHECK (dunk_att >= 0),
  c3_made integer NOT NULL DEFAULT 0 CHECK (c3_made >= 0),
  c3_att integer NOT NULL DEFAULT 0 CHECK (c3_att >= 0),
  c3_known_att integer NOT NULL DEFAULT 0 CHECK (c3_known_att >= 0),
  onoff_minutes numeric(10,3) NOT NULL DEFAULT 0 CHECK (onoff_minutes >= 0),
  load_run_id bigint NOT NULL REFERENCES euroleague.load_runs(load_run_id),
  derivation_version text NOT NULL,
  derived_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (
    player_id, team_id, game_id, is_on_key, type_lineup,
    own_starters, opp_starters
  )
);

CREATE INDEX IF NOT EXISTS euroleague_pff_game_idx
  ON euroleague.player_four_factors_by_game (game_id);

CREATE INDEX IF NOT EXISTS euroleague_pff_season_team_idx
  ON euroleague.player_four_factors_by_game
  (game_year, team_id, player_id, type_lineup, is_on_key);

CREATE OR REPLACE VIEW euroleague.final_schedule AS
SELECT
  s.game_id,
  s.competition,
  s.season AS game_year,
  s.gamecode AS gn,
  s.scheduled_at::date AS game_date,
  s.phase AS game_type,
  side.team_id,
  side.opp_team_id,
  side.is_home,
  CASE
    WHEN s.home_points IS NULL OR s.away_points IS NULL THEN NULL
    WHEN side.is_home THEN s.home_points > s.away_points
    ELSE s.away_points > s.home_points
  END AS has_won,
  side.team_points,
  side.opp_points
FROM euroleague.schedule s
CROSS JOIN LATERAL (
  VALUES
    (s.home_team_id, s.away_team_id, true, s.home_points, s.away_points),
    (s.away_team_id, s.home_team_id, false, s.away_points, s.home_points)
) AS side(team_id, opp_team_id, is_home, team_points, opp_points);

CREATE OR REPLACE FUNCTION euroleague.refresh_stint_timing_for_games(
  game_ids bigint[]
)
RETURNS bigint
LANGUAGE plpgsql
AS $function$
DECLARE
  updated_count bigint := 0;
BEGIN
  WITH clock_parts AS (
    SELECT
      ar.game_id,
      ar.source_event_order,
      ar.period,
      CASE WHEN ar.period <= 4 THEN (ar.period - 1) * 600
           ELSE 2400 + (ar.period - 5) * 300 END::numeric AS period_start,
      CASE WHEN ar.period <= 4 THEN 600 ELSE 300 END::numeric AS period_length,
      CASE
        WHEN ar.marker_time ~ '^\d{1,2}:\d{2}$' THEN
          split_part(ar.marker_time, ':', 1)::integer * 60
          + split_part(ar.marker_time, ':', 2)::integer
      END::numeric AS clock_remaining
    FROM euroleague.actions_raw ar
    WHERE game_ids IS NULL OR ar.game_id = ANY(game_ids)
  ),
  raw_elapsed AS (
    SELECT
      cp.*,
      CASE
        WHEN cp.clock_remaining IS NOT NULL THEN
          cp.period_start + cp.period_length
          - least(greatest(cp.clock_remaining, 0), cp.period_length)
        ELSE NULL
      END::numeric AS raw_event_elapsed_seconds
    FROM clock_parts cp
  ),
  event_clock AS (
    SELECT
      re.game_id,
      re.source_event_order,
      coalesce(
        max(re.raw_event_elapsed_seconds) OVER (
          PARTITION BY re.game_id
          ORDER BY re.source_event_order
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ),
        re.period_start
      )::numeric AS event_elapsed_seconds
    FROM raw_elapsed re
  ),
  game_ends AS (
    SELECT
      ar.game_id,
      (2400 + greatest(max(ar.period) - 4, 0) * 300)::numeric
        AS game_end_elapsed_seconds
    FROM euroleague.actions_raw ar
    WHERE game_ids IS NULL OR ar.game_id = ANY(game_ids)
    GROUP BY ar.game_id
  ),
  stint_starts AS (
    SELECT
      st.stint_id,
      st.game_id,
      st.team_id,
      st.stint_number,
      ec.event_elapsed_seconds AS start_elapsed_seconds
    FROM euroleague.stints st
    JOIN event_clock ec
      ON ec.game_id = st.game_id
     AND ec.source_event_order = st.start_event_order
    WHERE game_ids IS NULL OR st.game_id = ANY(game_ids)
  ),
  stint_order AS (
    SELECT
      ss.*,
      lead(ss.start_elapsed_seconds) OVER (
        PARTITION BY ss.game_id, ss.team_id
        ORDER BY ss.stint_number
      ) AS next_start_elapsed_seconds
    FROM stint_starts ss
  ),
  durations AS (
    SELECT
      so.stint_id,
      so.start_elapsed_seconds,
      coalesce(so.next_start_elapsed_seconds, ge.game_end_elapsed_seconds)
        AS end_elapsed_seconds,
      greatest(
        coalesce(so.next_start_elapsed_seconds, ge.game_end_elapsed_seconds)
        - so.start_elapsed_seconds,
        0
      )::numeric AS duration_seconds
    FROM stint_order so
    JOIN game_ends ge ON ge.game_id = so.game_id
  )
  UPDATE euroleague.stints st
  SET
    start_elapsed_seconds = d.start_elapsed_seconds,
    end_elapsed_seconds = d.end_elapsed_seconds,
    duration_seconds = d.duration_seconds
  FROM durations d
  WHERE st.stint_id = d.stint_id;

  GET DIAGNOSTICS updated_count = ROW_COUNT;
  RETURN updated_count;
END;
$function$;

CREATE OR REPLACE FUNCTION euroleague.refresh_player_four_factors_by_game_for_games(
  game_ids bigint[]
)
RETURNS bigint
LANGUAGE plpgsql
AS $function$
DECLARE
  inserted_count bigint := 0;
BEGIN
  PERFORM euroleague.refresh_stint_timing_for_games(game_ids);

  IF game_ids IS NULL OR array_length(game_ids, 1) IS NULL THEN
    DELETE FROM euroleague.player_four_factors_by_game;
  ELSE
    DELETE FROM euroleague.player_four_factors_by_game
    WHERE game_id = ANY(game_ids);
  END IF;

  INSERT INTO euroleague.player_four_factors_by_game (
    player_id, team_id, game_id, game_year, is_on_key, type_lineup,
    num_starters, own_starters, opp_starters, total_points, total_poss,
    ts_poss_count, oreb_count, oreb_opportunities, tov_count, steal_count,
    deflection_count, total_ft_attempts, total_fga, total_fgm,
    total_fg3_made, player_ts_poss_count, player_tov_count, minutes,
    fg2_made, fg2_att, fg3_made, fg3_att, layup_made, layup_att,
    dunk_made, dunk_att, c3_made, c3_att, c3_known_att, onoff_minutes,
    load_run_id, derivation_version
  )
  WITH target_games AS (
    SELECT s.*
    FROM euroleague.schedule s
    WHERE game_ids IS NULL OR s.game_id = ANY(game_ids)
  ),
  real_roster AS (
    SELECT fr.game_id, fr.team_id, fr.player_id
    FROM euroleague.full_rosters fr
    JOIN euroleague.players p ON p.player_id = fr.player_id
    JOIN target_games tg ON tg.game_id = fr.game_id
    WHERE lower(p.provider_player_id) NOT IN ('team', 'total')
      AND lower(btrim(p.display_name)) NOT IN ('team', 'total')
  ),
  clock_parts AS (
    SELECT
      ar.game_id,
      ar.source_event_order,
      ar.period,
      CASE WHEN ar.period <= 4 THEN (ar.period - 1) * 600
           ELSE 2400 + (ar.period - 5) * 300 END::numeric AS period_start,
      CASE WHEN ar.period <= 4 THEN 600 ELSE 300 END::numeric AS period_length,
      CASE
        WHEN ar.marker_time ~ '^\d{1,2}:\d{2}$' THEN
          split_part(ar.marker_time, ':', 1)::integer * 60
          + split_part(ar.marker_time, ':', 2)::integer
      END::numeric AS clock_remaining
    FROM euroleague.actions_raw ar
    JOIN target_games tg ON tg.game_id = ar.game_id
  ),
  raw_elapsed AS (
    SELECT
      cp.*,
      CASE
        WHEN cp.clock_remaining IS NOT NULL THEN
          cp.period_start + cp.period_length
          - least(greatest(cp.clock_remaining, 0), cp.period_length)
        ELSE NULL
      END::numeric AS raw_event_elapsed_seconds
    FROM clock_parts cp
  ),
  event_clock AS (
    SELECT
      re.game_id,
      re.source_event_order,
      coalesce(
        max(re.raw_event_elapsed_seconds) OVER (
          PARTITION BY re.game_id
          ORDER BY re.source_event_order
          ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
        ),
        re.period_start
      )::numeric AS event_elapsed_seconds
    FROM raw_elapsed re
  ),
  game_ends AS (
    SELECT
      ar.game_id,
      (2400 + greatest(max(ar.period) - 4, 0) * 300)::numeric
        AS game_end_elapsed_seconds
    FROM euroleague.actions_raw ar
    JOIN target_games tg ON tg.game_id = ar.game_id
    GROUP BY ar.game_id
  ),
  event_base AS (
    SELECT
      ar.game_id,
      ar.source_event_order,
      ar.team_id AS event_team_id,
      ar.player_id AS action_player_id,
      ar.play_type,
      ar.play_info,
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
    FROM target_games tg
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
      END::integer AS points,
      CASE WHEN eb.play_type IN ('2FGM', '2FGA', '3FGM', '3FGA') THEN 1
           WHEN eb.play_type IN ('FTM', 'FTA')
            AND eb.synthetic_ft_trip_id IS NOT NULL
            AND eb.parent_play_type = 'CM'
            AND eb.ft_reverse_order = 1 THEN 1 ELSE 0 END::integer
        AS ts_possessions,
      CASE WHEN eb.play_type = 'O' THEN 1 ELSE 0 END::integer AS orebounds,
      CASE WHEN eb.play_type IN ('2FGA', '3FGA') THEN 1
           WHEN eb.play_type = 'FTA'
            AND eb.synthetic_ft_trip_id IS NOT NULL
            AND eb.parent_play_type = 'CM'
            AND eb.ft_reverse_order = 1 THEN 1 ELSE 0 END::integer
        AS oreb_opportunities,
      CASE WHEN eb.play_type = 'TO' THEN 1 ELSE 0 END::integer AS turnovers,
      CASE WHEN eb.play_type = 'ST' THEN 1 ELSE 0 END::integer AS steals,
      CASE WHEN eb.play_type IN ('FTM', 'FTA') THEN 1 ELSE 0 END::integer
        AS ft_attempts,
      CASE WHEN eb.play_type IN ('2FGM', '2FGA', '3FGM', '3FGA') THEN 1 ELSE 0 END::integer AS fga,
      CASE WHEN eb.play_type IN ('2FGM', '3FGM') THEN 1 ELSE 0 END::integer AS fgm,
      CASE WHEN eb.play_type = '3FGM' THEN 1 ELSE 0 END::integer AS fg3_made,
      CASE WHEN eb.play_type = '2FGM' THEN 1 ELSE 0 END::integer AS fg2_made,
      CASE WHEN eb.play_type IN ('2FGM', '2FGA') THEN 1 ELSE 0 END::integer AS fg2_att,
      CASE WHEN eb.play_type IN ('3FGM', '3FGA') THEN 1 ELSE 0 END::integer AS fg3_att,
      CASE WHEN eb.play_type = '2FGM' AND eb.play_info ILIKE '%lay%up%' THEN 1 ELSE 0 END::integer AS layup_made,
      CASE WHEN eb.play_type IN ('2FGM', '2FGA') AND eb.play_info ILIKE '%lay%up%' THEN 1 ELSE 0 END::integer AS layup_att,
      CASE WHEN eb.play_type = '2FGM' AND eb.play_info ILIKE '%dunk%' THEN 1 ELSE 0 END::integer AS dunk_made,
      CASE WHEN eb.play_type IN ('2FGM', '2FGA') AND eb.play_info ILIKE '%dunk%' THEN 1 ELSE 0 END::integer AS dunk_att
    FROM event_base eb
  ),
  team_event_context AS (
    SELECT
      em.game_id,
      em.source_event_order,
      side.team_id,
      side.opponent_team_id,
      side.own_lineup_id,
      side.opp_lineup_id,
      own_lineup.starter_count AS own_starters,
      opp_lineup.starter_count AS opp_starters,
      ec.event_elapsed_seconds,
      ge.game_end_elapsed_seconds,
      CASE WHEN em.event_team_id = side.team_id THEN em.action_player_id END
        AS off_action_player_id,
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
      CASE WHEN em.event_team_id = side.team_id THEN em.steals ELSE 0 END AS def_steals,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.steals ELSE 0 END AS off_steals,
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
      CASE WHEN em.event_team_id = side.team_id THEN em.fg3_att ELSE 0 END AS off_fg3_att,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.fg3_att ELSE 0 END AS def_fg3_att,
      CASE WHEN em.event_team_id = side.team_id THEN em.layup_made ELSE 0 END AS off_layup_made,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.layup_made ELSE 0 END AS def_layup_made,
      CASE WHEN em.event_team_id = side.team_id THEN em.layup_att ELSE 0 END AS off_layup_att,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.layup_att ELSE 0 END AS def_layup_att,
      CASE WHEN em.event_team_id = side.team_id THEN em.dunk_made ELSE 0 END AS off_dunk_made,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.dunk_made ELSE 0 END AS def_dunk_made,
      CASE WHEN em.event_team_id = side.team_id THEN em.dunk_att ELSE 0 END AS off_dunk_att,
      CASE WHEN em.event_team_id = side.opponent_team_id THEN em.dunk_att ELSE 0 END AS def_dunk_att
    FROM event_metrics em
    JOIN event_clock ec
      ON ec.game_id = em.game_id
     AND ec.source_event_order = em.source_event_order
    JOIN game_ends ge ON ge.game_id = em.game_id
    CROSS JOIN LATERAL (
      VALUES
        (em.home_team_id, em.away_team_id, em.home_lineup_id, em.away_lineup_id),
        (em.away_team_id, em.home_team_id, em.away_lineup_id, em.home_lineup_id)
    ) AS side(team_id, opponent_team_id, own_lineup_id, opp_lineup_id)
    JOIN euroleague.lineups own_lineup ON own_lineup.lineup_id = side.own_lineup_id
    JOIN euroleague.lineups opp_lineup ON opp_lineup.lineup_id = side.opp_lineup_id
  ),
  player_exposure AS (
    SELECT
      tec.*,
      rr.player_id,
      CASE WHEN lp.player_id IS NULL THEN 0 ELSE 1 END::smallint AS is_on_key,
      CASE WHEN tec.off_action_player_id = rr.player_id
        THEN tec.off_ts_possessions ELSE 0 END AS off_player_ts_possessions,
      CASE WHEN tec.off_action_player_id = rr.player_id
        THEN tec.off_turnovers ELSE 0 END AS off_player_turnovers
    FROM team_event_context tec
    JOIN real_roster rr
      ON rr.game_id = tec.game_id AND rr.team_id = tec.team_id
    LEFT JOIN euroleague.lineup_players lp
      ON lp.lineup_id = tec.own_lineup_id AND lp.player_id = rr.player_id
  ),
  player_context AS (
    SELECT
      pe.game_id, pe.team_id, pe.player_id, pe.is_on_key,
      pe.own_starters, pe.opp_starters, context.type_lineup,
      context.total_points, context.total_poss, context.ts_poss_count,
      context.oreb_count, context.oreb_opportunities, context.tov_count,
      context.steal_count, context.total_ft_attempts, context.total_fga,
      context.total_fgm, context.total_fg3_made,
      context.player_ts_poss_count, context.player_tov_count,
      context.fg2_made, context.fg2_att, context.fg3_made, context.fg3_att,
      context.layup_made, context.layup_att, context.dunk_made, context.dunk_att
    FROM player_exposure pe
    CROSS JOIN LATERAL (
      VALUES
        ('offense', pe.off_points, pe.off_possessions, pe.off_ts_possessions,
         pe.off_orebounds, pe.off_oreb_opportunities, pe.off_turnovers,
         pe.off_steals, pe.off_ft_attempts, pe.off_fga, pe.off_fgm,
         pe.off_fg3_made, pe.off_player_ts_possessions,
         pe.off_player_turnovers, pe.off_fg2_made, pe.off_fg2_att,
         pe.off_fg3_made, pe.off_fg3_att, pe.off_layup_made,
         pe.off_layup_att, pe.off_dunk_made, pe.off_dunk_att),
        ('defense', pe.def_points, pe.def_possessions, pe.def_ts_possessions,
         pe.def_orebounds, pe.def_oreb_opportunities, pe.def_turnovers,
         pe.def_steals, pe.def_ft_attempts, pe.def_fga, pe.def_fgm,
         pe.def_fg3_made, 0, 0, pe.def_fg2_made, pe.def_fg2_att,
         pe.def_fg3_made, pe.def_fg3_att, pe.def_layup_made,
         pe.def_layup_att, pe.def_dunk_made, pe.def_dunk_att)
    ) AS context(
      type_lineup, total_points, total_poss, ts_poss_count, oreb_count,
      oreb_opportunities, tov_count, steal_count, total_ft_attempts,
      total_fga, total_fgm, total_fg3_made, player_ts_poss_count,
      player_tov_count, fg2_made, fg2_att, fg3_made, fg3_att,
      layup_made, layup_att, dunk_made, dunk_att
    )
  ),
  counts AS (
    SELECT
      pc.game_id, pc.team_id, pc.player_id, pc.is_on_key, pc.type_lineup,
      pc.own_starters, pc.opp_starters,
      sum(pc.total_points)::numeric AS total_points,
      sum(pc.total_poss)::bigint AS total_poss,
      sum(pc.ts_poss_count)::bigint AS ts_poss_count,
      sum(pc.oreb_count)::bigint AS oreb_count,
      sum(pc.oreb_opportunities)::bigint AS oreb_opportunities,
      sum(pc.tov_count)::bigint AS tov_count,
      sum(pc.steal_count)::bigint AS steal_count,
      sum(pc.total_ft_attempts)::bigint AS total_ft_attempts,
      sum(pc.total_fga)::bigint AS total_fga,
      sum(pc.total_fgm)::bigint AS total_fgm,
      sum(pc.total_fg3_made)::bigint AS total_fg3_made,
      sum(pc.player_ts_poss_count)::bigint AS player_ts_poss_count,
      sum(pc.player_tov_count)::bigint AS player_tov_count,
      sum(pc.fg2_made)::integer AS fg2_made,
      sum(pc.fg2_att)::integer AS fg2_att,
      sum(pc.fg3_made)::integer AS fg3_made,
      sum(pc.fg3_att)::integer AS fg3_att,
      sum(pc.layup_made)::integer AS layup_made,
      sum(pc.layup_att)::integer AS layup_att,
      sum(pc.dunk_made)::integer AS dunk_made,
      sum(pc.dunk_att)::integer AS dunk_att
    FROM player_context pc
    GROUP BY pc.game_id, pc.team_id, pc.player_id, pc.is_on_key,
             pc.type_lineup, pc.own_starters, pc.opp_starters
  ),
  starter_contexts AS (
    SELECT DISTINCT game_id, team_id, own_starters, opp_starters
    FROM team_event_context
  ),
  complete_grid AS (
    SELECT
      rr.game_id, rr.team_id, rr.player_id, state.is_on_key,
      side.type_lineup, sc.own_starters, sc.opp_starters
    FROM real_roster rr
    JOIN starter_contexts sc
      ON sc.game_id = rr.game_id AND sc.team_id = rr.team_id
    CROSS JOIN (VALUES (0::smallint), (1::smallint)) AS state(is_on_key)
    CROSS JOIN (VALUES ('offense'::text), ('defense'::text)) AS side(type_lineup)
  ),
  joint_lagged AS (
    SELECT
      tec.*,
      lag(tec.own_lineup_id) OVER (
        PARTITION BY tec.game_id, tec.team_id ORDER BY tec.source_event_order
      ) AS previous_own_lineup_id,
      lag(tec.opp_lineup_id) OVER (
        PARTITION BY tec.game_id, tec.team_id ORDER BY tec.source_event_order
      ) AS previous_opp_lineup_id
    FROM team_event_context tec
  ),
  joint_marked AS (
    SELECT
      jl.*,
      CASE
        WHEN jl.previous_own_lineup_id IS DISTINCT FROM jl.own_lineup_id
          OR jl.previous_opp_lineup_id IS DISTINCT FROM jl.opp_lineup_id
        THEN 1 ELSE 0
      END AS new_segment
    FROM joint_lagged jl
  ),
  joint_numbered AS (
    SELECT
      jm.*,
      sum(jm.new_segment) OVER (
        PARTITION BY jm.game_id, jm.team_id
        ORDER BY jm.source_event_order
        ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
      ) AS segment_number
    FROM joint_marked jm
  ),
  joint_starts AS (
    SELECT
      jn.game_id, jn.team_id, jn.segment_number,
      jn.own_lineup_id, jn.opp_lineup_id,
      jn.own_starters, jn.opp_starters,
      min(jn.source_event_order) AS segment_start_order,
      min(jn.event_elapsed_seconds) AS segment_start_elapsed_seconds,
      max(jn.game_end_elapsed_seconds) AS game_end_elapsed_seconds
    FROM joint_numbered jn
    GROUP BY jn.game_id, jn.team_id, jn.segment_number,
             jn.own_lineup_id, jn.opp_lineup_id,
             jn.own_starters, jn.opp_starters
  ),
  joint_ordered AS (
    SELECT
      js.*,
      lead(js.segment_start_elapsed_seconds) OVER (
        PARTITION BY js.game_id, js.team_id ORDER BY js.segment_number
      ) AS next_segment_start_elapsed_seconds
    FROM joint_starts js
  ),
  joint_segments AS (
    SELECT
      jo.*,
      greatest(
        coalesce(jo.next_segment_start_elapsed_seconds,
                 jo.game_end_elapsed_seconds)
        - jo.segment_start_elapsed_seconds,
        0
      )::numeric AS segment_seconds
    FROM joint_ordered jo
  ),
  player_minutes AS (
    SELECT
      rr.game_id, rr.team_id, rr.player_id,
      CASE WHEN lp.player_id IS NULL THEN 0 ELSE 1 END::smallint AS is_on_key,
      js.own_starters, js.opp_starters,
      round(sum(js.segment_seconds) / 60.0, 3) AS minutes
    FROM joint_segments js
    JOIN real_roster rr
      ON rr.game_id = js.game_id AND rr.team_id = js.team_id
    LEFT JOIN euroleague.lineup_players lp
      ON lp.lineup_id = js.own_lineup_id AND lp.player_id = rr.player_id
    GROUP BY rr.game_id, rr.team_id, rr.player_id,
             CASE WHEN lp.player_id IS NULL THEN 0 ELSE 1 END,
             js.own_starters, js.opp_starters
  )
  SELECT
    cg.player_id,
    cg.team_id,
    cg.game_id,
    tg.season AS game_year,
    cg.is_on_key,
    cg.type_lineup,
    cg.own_starters AS num_starters,
    cg.own_starters,
    cg.opp_starters,
    coalesce(c.total_points, 0),
    coalesce(c.total_poss, 0),
    coalesce(c.ts_poss_count, 0),
    coalesce(c.oreb_count, 0),
    coalesce(c.oreb_opportunities, 0),
    coalesce(c.tov_count, 0),
    coalesce(c.steal_count, 0),
    0,
    coalesce(c.total_ft_attempts, 0),
    coalesce(c.total_fga, 0),
    coalesce(c.total_fgm, 0),
    coalesce(c.total_fg3_made, 0),
    coalesce(c.player_ts_poss_count, 0),
    coalesce(c.player_tov_count, 0),
    CASE WHEN cg.type_lineup = 'offense'
      THEN coalesce(pm.minutes, 0) ELSE 0 END,
    coalesce(c.fg2_made, 0),
    coalesce(c.fg2_att, 0),
    coalesce(c.fg3_made, 0),
    coalesce(c.fg3_att, 0),
    coalesce(c.layup_made, 0),
    coalesce(c.layup_att, 0),
    coalesce(c.dunk_made, 0),
    coalesce(c.dunk_att, 0),
    0, 0, 0,
    CASE WHEN cg.type_lineup = 'offense'
      THEN coalesce(pm.minutes, 0) ELSE 0 END,
    tg.last_seen_load_run_id,
    'existing-israeli-contract-v1'
  FROM complete_grid cg
  JOIN target_games tg ON tg.game_id = cg.game_id
  LEFT JOIN counts c
    ON c.game_id = cg.game_id
   AND c.team_id = cg.team_id
   AND c.player_id = cg.player_id
   AND c.is_on_key = cg.is_on_key
   AND c.type_lineup = cg.type_lineup
   AND c.own_starters = cg.own_starters
   AND c.opp_starters = cg.opp_starters
  LEFT JOIN player_minutes pm
    ON pm.game_id = cg.game_id
   AND pm.team_id = cg.team_id
   AND pm.player_id = cg.player_id
   AND pm.is_on_key = cg.is_on_key
   AND pm.own_starters = cg.own_starters
   AND pm.opp_starters = cg.opp_starters;

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$function$;

CREATE OR REPLACE VIEW euroleague.player_onoff_by_season AS
WITH agg AS (
  SELECT
    pf.game_year,
    pf.team_id,
    pf.player_id,
    pf.is_on_key,
    pf.type_lineup,
    sum(pf.total_points)::numeric AS total_points,
    sum(pf.total_poss)::bigint AS total_poss,
    sum(pf.fg2_made)::bigint AS fg2_made,
    sum(pf.fg2_att)::bigint AS fg2_att,
    sum(pf.fg3_made)::bigint AS fg3_made,
    sum(pf.fg3_att)::bigint AS fg3_att,
    sum(pf.onoff_minutes)::numeric AS minutes
  FROM euroleague.player_four_factors_by_game pf
  GROUP BY pf.game_year, pf.team_id, pf.player_id,
           pf.is_on_key, pf.type_lineup
),
pivoted AS (
  SELECT
    a.game_year, a.team_id, a.player_id,
    max(a.total_points) FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 1) AS off_on_points,
    max(a.total_poss) FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 1) AS off_on_poss,
    max(a.total_points) FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 0) AS off_off_points,
    max(a.total_poss) FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 0) AS off_off_poss,
    max(a.total_points) FILTER (WHERE a.type_lineup = 'defense' AND a.is_on_key = 1) AS def_on_points,
    max(a.total_poss) FILTER (WHERE a.type_lineup = 'defense' AND a.is_on_key = 1) AS def_on_poss,
    max(a.total_points) FILTER (WHERE a.type_lineup = 'defense' AND a.is_on_key = 0) AS def_off_points,
    max(a.total_poss) FILTER (WHERE a.type_lineup = 'defense' AND a.is_on_key = 0) AS def_off_poss,
    max(a.minutes) FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 1) AS minutes_on,
    max(a.fg2_made) FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 1) AS off_on_fg2_made,
    max(a.fg2_att) FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 1) AS off_on_fg2_att,
    max(a.fg3_made) FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 1) AS off_on_fg3_made,
    max(a.fg3_att) FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 1) AS off_on_fg3_att
  FROM agg a
  GROUP BY a.game_year, a.team_id, a.player_id
)
SELECT
  p.*,
  player.display_name AS player_name,
  team.display_name AS team_name,
  100 * p.off_on_points / nullif(p.off_on_poss, 0)::numeric AS off_rating_on,
  100 * p.off_off_points / nullif(p.off_off_poss, 0)::numeric AS off_rating_off,
  100 * p.def_on_points / nullif(p.def_on_poss, 0)::numeric AS def_rating_on,
  100 * p.def_off_points / nullif(p.def_off_poss, 0)::numeric AS def_rating_off,
  (
    100 * p.off_on_points / nullif(p.off_on_poss, 0)::numeric
    - 100 * p.off_off_points / nullif(p.off_off_poss, 0)::numeric
  ) - (
    100 * p.def_on_points / nullif(p.def_on_poss, 0)::numeric
    - 100 * p.def_off_points / nullif(p.def_off_poss, 0)::numeric
  ) AS net_on_off
FROM pivoted p
JOIN euroleague.players player ON player.player_id = p.player_id
JOIN euroleague.teams team ON team.team_id = p.team_id;

CREATE OR REPLACE VIEW euroleague.player_four_factors_by_season AS
WITH agg AS (
  SELECT
    pf.game_year,
    pf.team_id,
    pf.player_id,
    pf.is_on_key,
    pf.type_lineup,
    sum(pf.total_points)::numeric AS total_points,
    sum(pf.total_poss)::bigint AS total_poss,
    sum(pf.ts_poss_count)::bigint AS ts_poss_count,
    sum(pf.oreb_count)::bigint AS oreb_count,
    sum(pf.oreb_opportunities)::bigint AS oreb_opportunities,
    sum(pf.tov_count)::bigint AS tov_count,
    sum(pf.steal_count)::bigint AS steal_count,
    sum(pf.deflection_count)::bigint AS deflection_count,
    sum(pf.total_ft_attempts)::bigint AS total_ft_attempts,
    sum(pf.total_fga)::bigint AS total_fga,
    sum(pf.total_fgm)::bigint AS total_fgm,
    sum(pf.total_fg3_made)::bigint AS total_fg3_made
  FROM euroleague.player_four_factors_by_game pf
  GROUP BY pf.game_year, pf.team_id, pf.player_id,
           pf.is_on_key, pf.type_lineup
),
rates AS (
  SELECT
    a.*,
    a.total_points / (2 * nullif(a.ts_poss_count, 0))::numeric AS ts_pct,
    (a.total_fgm + 0.5 * a.total_fg3_made)
      / nullif(a.total_fga, 0)::numeric AS efg_pct,
    a.oreb_count / nullif(a.oreb_opportunities, 0)::numeric AS oreb_pct,
    a.tov_count / nullif(a.total_poss, 0)::numeric AS tov_pct,
    (a.steal_count + a.deflection_count)
      / nullif(a.total_poss, 0)::numeric AS disruption_rate,
    a.total_ft_attempts / nullif(a.total_fga, 0)::numeric AS ft_rate
  FROM agg a
)
SELECT
  r.game_year,
  r.team_id,
  r.player_id,
  player.display_name AS player_name,
  team.display_name AS team_name,
  max(r.ts_pct) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 1) AS off_on_ts,
  max(r.ts_pct) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 0) AS off_off_ts,
  max(r.ts_pct) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 1) AS def_on_ts,
  max(r.ts_pct) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 0) AS def_off_ts,
  max(r.efg_pct) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 1) AS off_on_efg,
  max(r.efg_pct) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 0) AS off_off_efg,
  max(r.efg_pct) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 1) AS def_on_efg,
  max(r.efg_pct) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 0) AS def_off_efg,
  max(r.oreb_pct) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 1) AS off_on_oreb,
  max(r.oreb_pct) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 0) AS off_off_oreb,
  max(r.oreb_pct) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 1) AS def_on_oreb,
  max(r.oreb_pct) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 0) AS def_off_oreb,
  max(r.tov_pct) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 1) AS off_on_tov,
  max(r.tov_pct) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 0) AS off_off_tov,
  max(r.tov_pct) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 1) AS def_on_tov,
  max(r.tov_pct) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 0) AS def_off_tov,
  max(r.ft_rate) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 1) AS off_on_ftr,
  max(r.ft_rate) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 0) AS off_off_ftr,
  max(r.ft_rate) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 1) AS def_on_ftr,
  max(r.ft_rate) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 0) AS def_off_ftr,
  max(r.total_poss) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 1) AS off_on_poss,
  max(r.total_poss) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 0) AS off_off_poss,
  max(r.total_poss) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 1) AS def_on_poss,
  max(r.total_poss) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 0) AS def_off_poss
FROM rates r
JOIN euroleague.players player ON player.player_id = r.player_id
JOIN euroleague.teams team ON team.team_id = r.team_id
GROUP BY r.game_year, r.team_id, r.player_id,
         player.display_name, team.display_name;

COMMIT;
