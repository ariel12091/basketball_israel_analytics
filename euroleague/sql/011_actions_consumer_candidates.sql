-- EuroLeague shadow schema -- migration 011: actions-based consumer candidates.
--
-- Additive proof layer for removing the normalized lineup/possession middle
-- tables. These two candidate facts are derived only from canonical actions,
-- schedule, and rosters. The final migration promotes them only after the
-- exact comparisons at the bottom of this file pass.

BEGIN;

SET LOCAL search_path TO euroleague, public;

CREATE TABLE IF NOT EXISTS euroleague.matchup_segments_actions (
  game_id bigint NOT NULL REFERENCES euroleague.schedule(game_id) ON DELETE CASCADE,
  team_id bigint NOT NULL REFERENCES euroleague.teams(team_id),
  segment_id integer NOT NULL CHECK (segment_id > 0),
  own_lineup text[] NOT NULL CHECK (cardinality(own_lineup) = 5),
  opp_lineup text[] NOT NULL CHECK (cardinality(opp_lineup) = 5),
  own_starters smallint NOT NULL CHECK (own_starters BETWEEN 0 AND 5),
  opp_starters smallint NOT NULL CHECK (opp_starters BETWEEN 0 AND 5),
  start_event_order integer NOT NULL,
  end_event_order_exclusive integer NOT NULL,
  start_elapsed_seconds numeric NOT NULL,
  end_elapsed_seconds numeric NOT NULL,
  segment_seconds numeric NOT NULL CHECK (segment_seconds >= 0),
  load_run_id bigint REFERENCES euroleague.load_runs(load_run_id),
  derivation_version text NOT NULL,
  derived_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (game_id, team_id, segment_id),
  CHECK (end_event_order_exclusive > start_event_order)
);

CREATE TABLE IF NOT EXISTS euroleague.action_team_context_actions (
  game_id bigint NOT NULL,
  source_event_order integer NOT NULL,
  team_id bigint NOT NULL REFERENCES euroleague.teams(team_id),
  opponent_team_id bigint NOT NULL REFERENCES euroleague.teams(team_id),
  period smallint NOT NULL,
  type_lineup text CHECK (type_lineup IS NULL OR type_lineup IN ('offense', 'defense')),
  own_lineup text[] NOT NULL CHECK (cardinality(own_lineup) = 5),
  opp_lineup text[] NOT NULL CHECK (cardinality(opp_lineup) = 5),
  own_starters smallint NOT NULL CHECK (own_starters BETWEEN 0 AND 5),
  opp_starters smallint NOT NULL CHECK (opp_starters BETWEEN 0 AND 5),
  event_team_id bigint,
  action_player_id bigint,
  play_type text NOT NULL,
  play_info text,
  synthetic_ft_trip_id text,
  parent_play_type text NOT NULL,
  ft_reverse_order integer NOT NULL,
  points integer NOT NULL DEFAULT 0,
  ts_possessions integer NOT NULL DEFAULT 0,
  orebounds integer NOT NULL DEFAULT 0,
  oreb_opportunities integer NOT NULL DEFAULT 0,
  turnovers integer NOT NULL DEFAULT 0,
  steals integer NOT NULL DEFAULT 0,
  ft_attempts integer NOT NULL DEFAULT 0,
  fga integer NOT NULL DEFAULT 0,
  fgm integer NOT NULL DEFAULT 0,
  fg2_made integer NOT NULL DEFAULT 0,
  fg2_att integer NOT NULL DEFAULT 0,
  fg3_made integer NOT NULL DEFAULT 0,
  fg3_att integer NOT NULL DEFAULT 0,
  layup_made integer NOT NULL DEFAULT 0,
  layup_att integer NOT NULL DEFAULT 0,
  dunk_made integer NOT NULL DEFAULT 0,
  dunk_att integer NOT NULL DEFAULT 0,
  possession_flag smallint NOT NULL DEFAULT 0 CHECK (possession_flag IN (0, 1)),
  final_end_poss boolean NOT NULL DEFAULT false,
  endpoint_reason text,
  event_elapsed_seconds numeric NOT NULL,
  segment_id integer NOT NULL,
  own_team_score integer NOT NULL DEFAULT 0,
  opp_team_score integer NOT NULL DEFAULT 0,
  load_run_id bigint REFERENCES euroleague.load_runs(load_run_id),
  derivation_version text NOT NULL,
  derived_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (game_id, source_event_order, team_id),
  FOREIGN KEY (game_id, source_event_order)
    REFERENCES euroleague.actions(game_id, source_event_order) ON DELETE CASCADE,
  FOREIGN KEY (game_id, team_id, segment_id)
    REFERENCES euroleague.matchup_segments_actions(game_id, team_id, segment_id),
  CHECK (team_id <> opponent_team_id)
);

CREATE INDEX IF NOT EXISTS euroleague_action_team_context_actions_agg_idx
  ON euroleague.action_team_context_actions(game_id, team_id, type_lineup);

CREATE OR REPLACE FUNCTION euroleague.refresh_actions_consumer_candidates(
  game_ids bigint[]
)
RETURNS bigint
LANGUAGE plpgsql
AS $function$
DECLARE
  inserted_count bigint := 0;
BEGIN
  IF game_ids IS NULL OR array_length(game_ids, 1) IS NULL THEN
    DELETE FROM euroleague.action_team_context_actions;
    DELETE FROM euroleague.matchup_segments_actions;
  ELSE
    DELETE FROM euroleague.action_team_context_actions
     WHERE game_id = ANY(game_ids);
    DELETE FROM euroleague.matchup_segments_actions
     WHERE game_id = ANY(game_ids);
  END IF;

  INSERT INTO euroleague.matchup_segments_actions (
    game_id, team_id, segment_id, own_lineup, opp_lineup,
    own_starters, opp_starters,
    start_event_order, end_event_order_exclusive,
    start_elapsed_seconds, end_elapsed_seconds, segment_seconds,
    load_run_id, derivation_version
  )
  WITH target_games AS (
    SELECT s.*
      FROM euroleague.schedule s
     WHERE game_ids IS NULL OR s.game_id = ANY(game_ids)
  ),
  clock_parts AS (
    SELECT
      a.game_id,
      a.source_event_order,
      a.period,
      CASE WHEN a.period <= 4 THEN (a.period - 1) * 600
           ELSE 2400 + (a.period - 5) * 300 END::numeric AS period_start,
      CASE WHEN a.period <= 4 THEN 600 ELSE 300 END::numeric AS period_length,
      CASE
        WHEN a.marker_time ~ '^\d{1,2}:\d{2}$' THEN
          split_part(a.marker_time, ':', 1)::integer * 60
          + split_part(a.marker_time, ':', 2)::integer
      END::numeric AS clock_remaining
    FROM euroleague.actions a
    JOIN target_games tg ON tg.game_id = a.game_id
  ),
  raw_elapsed AS (
    SELECT cp.*,
      CASE WHEN cp.clock_remaining IS NOT NULL THEN
        cp.period_start + cp.period_length
        - least(greatest(cp.clock_remaining, 0), cp.period_length)
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
    SELECT a.game_id,
           (2400 + greatest(max(a.period) - 4, 0) * 300)::numeric
             AS game_end_elapsed_seconds
      FROM euroleague.actions a
      JOIN target_games tg ON tg.game_id = a.game_id
     GROUP BY a.game_id
  ),
  game_bounds AS (
    SELECT a.game_id, max(a.source_event_order) + 1 AS end_event_order_exclusive
      FROM euroleague.actions a
      JOIN target_games tg ON tg.game_id = a.game_id
     GROUP BY a.game_id
  ),
  unique_lineups AS MATERIALIZED (
    SELECT DISTINCT x.game_id, x.team_id, x.lineup
      FROM (
        SELECT a.game_id, tg.home_team_id AS team_id,
               ARRAY(SELECT x FROM unnest(a.lineup_a) x ORDER BY x) AS lineup
          FROM euroleague.actions a JOIN target_games tg ON tg.game_id = a.game_id
        UNION ALL
        SELECT a.game_id, tg.away_team_id AS team_id,
               ARRAY(SELECT x FROM unnest(a.lineup_b) x ORDER BY x) AS lineup
          FROM euroleague.actions a JOIN target_games tg ON tg.game_id = a.game_id
      ) x
  ),
  starter_counts AS MATERIALIZED (
    SELECT ul.game_id, ul.team_id, ul.lineup,
           count(fr.player_id) FILTER (WHERE fr.is_starter)::smallint AS starters
      FROM unique_lineups ul
      LEFT JOIN euroleague.full_rosters fr
        ON fr.game_id = ul.game_id
       AND fr.team_id = ul.team_id
       AND fr.source_player_name = ANY(ul.lineup)
     GROUP BY ul.game_id, ul.team_id, ul.lineup
  ),
  lineup_sided AS MATERIALIZED (
    SELECT
      a.game_id,
      a.source_event_order,
      ec.event_elapsed_seconds,
      ge.game_end_elapsed_seconds,
      side.team_id,
      side.own_lineup,
      side.opp_lineup,
      own_count.starters AS own_starters,
      opp_count.starters AS opp_starters,
      tg.last_seen_load_run_id
    FROM euroleague.actions a
    JOIN target_games tg ON tg.game_id = a.game_id
    JOIN event_clock ec
      ON ec.game_id = a.game_id
     AND ec.source_event_order = a.source_event_order
    JOIN game_ends ge ON ge.game_id = a.game_id
    CROSS JOIN LATERAL (
      VALUES
        (
          tg.home_team_id,
          tg.away_team_id,
          ARRAY(SELECT x FROM unnest(a.lineup_a) x ORDER BY x),
          ARRAY(SELECT x FROM unnest(a.lineup_b) x ORDER BY x)
        ),
        (
          tg.away_team_id,
          tg.home_team_id,
          ARRAY(SELECT x FROM unnest(a.lineup_b) x ORDER BY x),
          ARRAY(SELECT x FROM unnest(a.lineup_a) x ORDER BY x)
        )
    ) AS side(team_id, opponent_team_id, own_lineup, opp_lineup)
    JOIN starter_counts own_count
      ON own_count.game_id = a.game_id
     AND own_count.team_id = side.team_id
     AND own_count.lineup = side.own_lineup
    JOIN starter_counts opp_count
      ON opp_count.game_id = a.game_id
     AND opp_count.team_id = side.opponent_team_id
     AND opp_count.lineup = side.opp_lineup
  ),
  lineup_lagged AS (
    SELECT ls.*,
           lag(ls.own_lineup) OVER w AS previous_own_lineup,
           lag(ls.opp_lineup) OVER w AS previous_opp_lineup
      FROM lineup_sided ls
    WINDOW w AS (
      PARTITION BY ls.game_id, ls.team_id ORDER BY ls.source_event_order
    )
  ),
  lineup_marked AS (
    SELECT ll.*,
           CASE WHEN ll.previous_own_lineup IS DISTINCT FROM ll.own_lineup
                  OR ll.previous_opp_lineup IS DISTINCT FROM ll.opp_lineup
                THEN 1 ELSE 0 END AS new_segment
      FROM lineup_lagged ll
  ),
  numbered AS (
    SELECT lm.*,
           sum(lm.new_segment) OVER (
             PARTITION BY lm.game_id, lm.team_id
             ORDER BY lm.source_event_order
             ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
           )::integer AS segment_number
      FROM lineup_marked lm
  ),
  starts AS (
    SELECT game_id, team_id, segment_number,
           own_lineup, opp_lineup, own_starters, opp_starters,
           min(source_event_order) AS start_event_order,
           min(event_elapsed_seconds) AS start_elapsed_seconds,
           max(game_end_elapsed_seconds) AS game_end_elapsed_seconds,
           max(last_seen_load_run_id) AS load_run_id
      FROM numbered
     GROUP BY game_id, team_id, segment_number,
              own_lineup, opp_lineup, own_starters, opp_starters
  ),
  ordered AS (
    SELECT s.*,
           lead(s.start_event_order) OVER w AS next_start_event_order,
           lead(s.start_elapsed_seconds) OVER w AS next_start_elapsed_seconds
      FROM starts s
    WINDOW w AS (
      PARTITION BY s.game_id, s.team_id ORDER BY s.segment_number
    )
  )
  SELECT
    o.game_id,
    o.team_id,
    o.segment_number,
    o.own_lineup,
    o.opp_lineup,
    o.own_starters,
    o.opp_starters,
    o.start_event_order,
    coalesce(o.next_start_event_order, gb.end_event_order_exclusive),
    o.start_elapsed_seconds,
    coalesce(o.next_start_elapsed_seconds, o.game_end_elapsed_seconds),
    greatest(
      coalesce(o.next_start_elapsed_seconds, o.game_end_elapsed_seconds)
      - o.start_elapsed_seconds,
      0
    )::numeric,
    o.load_run_id,
    'actions-v1'
  FROM ordered o
  JOIN game_bounds gb ON gb.game_id = o.game_id;

  INSERT INTO euroleague.action_team_context_actions (
    game_id, source_event_order, team_id, opponent_team_id, period,
    type_lineup, own_lineup, opp_lineup, own_starters, opp_starters,
    event_team_id, action_player_id, play_type, play_info,
    synthetic_ft_trip_id, parent_play_type, ft_reverse_order,
    points, ts_possessions, orebounds, oreb_opportunities, turnovers,
    steals, ft_attempts, fga, fgm, fg2_made, fg2_att, fg3_made, fg3_att,
    layup_made, layup_att, dunk_made, dunk_att,
    possession_flag, final_end_poss, endpoint_reason,
    event_elapsed_seconds, segment_id,
    own_team_score, opp_team_score, load_run_id, derivation_version
  )
  WITH target_games AS (
    SELECT s.*
      FROM euroleague.schedule s
     WHERE game_ids IS NULL OR s.game_id = ANY(game_ids)
  ),
  clock_parts AS (
    SELECT
      a.game_id,
      a.source_event_order,
      a.period,
      CASE WHEN a.period <= 4 THEN (a.period - 1) * 600
           ELSE 2400 + (a.period - 5) * 300 END::numeric AS period_start,
      CASE WHEN a.period <= 4 THEN 600 ELSE 300 END::numeric AS period_length,
      CASE
        WHEN a.marker_time ~ '^\d{1,2}:\d{2}$' THEN
          split_part(a.marker_time, ':', 1)::integer * 60
          + split_part(a.marker_time, ':', 2)::integer
      END::numeric AS clock_remaining
    FROM euroleague.actions a
    JOIN target_games tg ON tg.game_id = a.game_id
  ),
  raw_elapsed AS (
    SELECT cp.*,
      CASE WHEN cp.clock_remaining IS NOT NULL THEN
        cp.period_start + cp.period_length
        - least(greatest(cp.clock_remaining, 0), cp.period_length)
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
  unique_lineups AS MATERIALIZED (
    SELECT DISTINCT x.game_id, x.team_id, x.lineup
      FROM (
        SELECT a.game_id, tg.home_team_id AS team_id,
               ARRAY(SELECT x FROM unnest(a.lineup_a) x ORDER BY x) AS lineup
          FROM euroleague.actions a JOIN target_games tg ON tg.game_id = a.game_id
        UNION ALL
        SELECT a.game_id, tg.away_team_id AS team_id,
               ARRAY(SELECT x FROM unnest(a.lineup_b) x ORDER BY x) AS lineup
          FROM euroleague.actions a JOIN target_games tg ON tg.game_id = a.game_id
      ) x
  ),
  starter_counts AS MATERIALIZED (
    SELECT ul.game_id, ul.team_id, ul.lineup,
           count(fr.player_id) FILTER (WHERE fr.is_starter)::smallint AS starters
      FROM unique_lineups ul
      LEFT JOIN euroleague.full_rosters fr
        ON fr.game_id = ul.game_id
       AND fr.team_id = ul.team_id
       AND fr.source_player_name = ANY(ul.lineup)
     GROUP BY ul.game_id, ul.team_id, ul.lineup
  ),
  event_base AS (
    SELECT
      a.*,
      root.play_type AS parent_play_type,
      tg.home_team_id,
      tg.away_team_id,
      row_number() OVER (
        PARTITION BY a.game_id, a.synthetic_ft_trip_id
        ORDER BY a.source_event_order DESC
      ) AS ft_reverse_order
    FROM euroleague.actions a
    JOIN target_games tg ON tg.game_id = a.game_id
    JOIN euroleague.actions root
      ON root.game_id = a.game_id
     AND root.source_event_order = a.synthetic_parent_order
  ),
  event_metrics AS (
    SELECT
      eb.*,
      CASE eb.play_type
        WHEN '2FGM' THEN 2 WHEN '3FGM' THEN 3 WHEN 'FTM' THEN 1 ELSE 0
      END::integer AS metric_points,
      CASE WHEN eb.play_type IN ('2FGM', '2FGA', '3FGM', '3FGA') THEN 1
           WHEN eb.play_type IN ('FTM', 'FTA')
            AND eb.synthetic_ft_trip_id IS NOT NULL
            AND eb.parent_play_type = 'CM'
            AND eb.ft_reverse_order = 1 THEN 1 ELSE 0 END::integer
        AS metric_ts_possessions,
      CASE WHEN eb.play_type = 'O' THEN 1 ELSE 0 END::integer AS metric_orebounds,
      CASE WHEN eb.play_type IN ('2FGA', '3FGA') THEN 1
           WHEN eb.play_type = 'FTA'
            AND eb.synthetic_ft_trip_id IS NOT NULL
            AND eb.parent_play_type = 'CM'
            AND eb.ft_reverse_order = 1 THEN 1 ELSE 0 END::integer
        AS metric_oreb_opportunities,
      CASE WHEN eb.play_type = 'TO' THEN 1 ELSE 0 END::integer AS metric_turnovers,
      CASE WHEN eb.play_type = 'ST' THEN 1 ELSE 0 END::integer AS metric_steals,
      CASE WHEN eb.play_type IN ('FTM', 'FTA') THEN 1 ELSE 0 END::integer
        AS metric_ft_attempts,
      CASE WHEN eb.play_type IN ('2FGM', '2FGA', '3FGM', '3FGA') THEN 1 ELSE 0 END::integer
        AS metric_fga,
      CASE WHEN eb.play_type IN ('2FGM', '3FGM') THEN 1 ELSE 0 END::integer
        AS metric_fgm,
      CASE WHEN eb.play_type = '3FGM' THEN 1 ELSE 0 END::integer AS metric_fg3_made,
      CASE WHEN eb.play_type = '2FGM' THEN 1 ELSE 0 END::integer AS metric_fg2_made,
      CASE WHEN eb.play_type IN ('2FGM', '2FGA') THEN 1 ELSE 0 END::integer
        AS metric_fg2_att,
      CASE WHEN eb.play_type IN ('3FGM', '3FGA') THEN 1 ELSE 0 END::integer
        AS metric_fg3_att,
      CASE WHEN eb.play_type = '2FGM' AND eb.play_info ILIKE '%lay%up%'
           THEN 1 ELSE 0 END::integer AS metric_layup_made,
      CASE WHEN eb.play_type IN ('2FGM', '2FGA') AND eb.play_info ILIKE '%lay%up%'
           THEN 1 ELSE 0 END::integer AS metric_layup_att,
      CASE WHEN eb.play_type = '2FGM' AND eb.play_info ILIKE '%dunk%'
           THEN 1 ELSE 0 END::integer AS metric_dunk_made,
      CASE WHEN eb.play_type IN ('2FGM', '2FGA') AND eb.play_info ILIKE '%dunk%'
           THEN 1 ELSE 0 END::integer AS metric_dunk_att
    FROM event_base eb
  ),
  cum_scores AS MATERIALIZED (
    SELECT em.game_id, em.source_event_order,
           side.team_id AS score_team_id,
           sum(CASE WHEN em.team_id = side.team_id
                    THEN em.metric_points ELSE 0 END)
             OVER (
               PARTITION BY em.game_id, side.team_id
               ORDER BY em.source_event_order
               ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW
             )::integer AS team_running_score
      FROM event_metrics em
      JOIN target_games tg ON tg.game_id = em.game_id
      CROSS JOIN LATERAL (
        VALUES (tg.home_team_id), (tg.away_team_id)
      ) AS side(team_id)
  ),
  sided AS MATERIALIZED (
    SELECT
      em.*,
      ec.event_elapsed_seconds,
      side.team_id AS perspective_team_id,
      side.opponent_team_id,
      side.own_lineup,
      side.opp_lineup,
      own_count.starters AS own_starters,
      opp_count.starters AS opp_starters,
      CASE
        WHEN em.team_id IS NULL THEN NULL
        WHEN em.play_type IN (
               '2FGM','2FGA','3FGM','3FGA','FTM','FTA','AS','TO','RV','O')
          THEN CASE WHEN em.team_id = side.team_id
                    THEN 'offense' ELSE 'defense' END
        WHEN em.play_type IN (
               'ST','FV','D','CM','CMU','CMT','CMTI','CMD','OF','B','C')
          THEN CASE WHEN em.team_id = side.team_id
                    THEN 'defense' ELSE 'offense' END
        ELSE NULL
      END AS type_lineup,
      CASE WHEN em.end_possession THEN 1 ELSE 0 END::smallint AS possession_flag
    FROM event_metrics em
    JOIN event_clock ec
      ON ec.game_id = em.game_id
     AND ec.source_event_order = em.source_event_order
    CROSS JOIN LATERAL (
      VALUES
        (
          em.home_team_id,
          em.away_team_id,
          ARRAY(SELECT x FROM unnest(em.lineup_a) x ORDER BY x),
          ARRAY(SELECT x FROM unnest(em.lineup_b) x ORDER BY x)
        ),
        (
          em.away_team_id,
          em.home_team_id,
          ARRAY(SELECT x FROM unnest(em.lineup_b) x ORDER BY x),
          ARRAY(SELECT x FROM unnest(em.lineup_a) x ORDER BY x)
        )
    ) AS side(team_id, opponent_team_id, own_lineup, opp_lineup)
    JOIN starter_counts own_count
      ON own_count.game_id = em.game_id
     AND own_count.team_id = side.team_id
     AND own_count.lineup = side.own_lineup
    JOIN starter_counts opp_count
      ON opp_count.game_id = em.game_id
     AND opp_count.team_id = side.opponent_team_id
     AND opp_count.lineup = side.opp_lineup
  )
  SELECT
    sd.game_id,
    sd.source_event_order,
    sd.perspective_team_id,
    sd.opponent_team_id,
    sd.period,
    sd.type_lineup,
    sd.own_lineup,
    sd.opp_lineup,
    sd.own_starters,
    sd.opp_starters,
    sd.team_id,
    sd.player_id,
    sd.play_type,
    sd.play_info,
    sd.synthetic_ft_trip_id,
    sd.parent_play_type,
    sd.ft_reverse_order,
    sd.metric_points,
    sd.metric_ts_possessions,
    sd.metric_orebounds,
    sd.metric_oreb_opportunities,
    sd.metric_turnovers,
    sd.metric_steals,
    sd.metric_ft_attempts,
    sd.metric_fga,
    sd.metric_fgm,
    sd.metric_fg2_made,
    sd.metric_fg2_att,
    sd.metric_fg3_made,
    sd.metric_fg3_att,
    sd.metric_layup_made,
    sd.metric_layup_att,
    sd.metric_dunk_made,
    sd.metric_dunk_att,
    sd.possession_flag,
    sd.end_possession,
    sd.endpoint_reason,
    sd.event_elapsed_seconds,
    ms.segment_id,
    coalesce(own_score.team_running_score, 0),
    coalesce(opp_score.team_running_score, 0),
    sd.load_run_id,
    'actions-v1'
  FROM sided sd
  JOIN euroleague.matchup_segments_actions ms
    ON ms.game_id = sd.game_id
   AND ms.team_id = sd.perspective_team_id
   AND sd.source_event_order >= ms.start_event_order
   AND sd.source_event_order < ms.end_event_order_exclusive
  LEFT JOIN cum_scores own_score
    ON own_score.game_id = sd.game_id
   AND own_score.source_event_order = sd.source_event_order
   AND own_score.score_team_id = sd.perspective_team_id
  LEFT JOIN cum_scores opp_score
    ON opp_score.game_id = sd.game_id
   AND opp_score.source_event_order = sd.source_event_order
   AND opp_score.score_team_id = sd.opponent_team_id;

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$function$;

SELECT euroleague.refresh_actions_consumer_candidates(NULL::bigint[]);

DO $migration$
BEGIN
  IF EXISTS (
    WITH old_lineups AS (
      SELECT l.lineup_id,
             array_agg(
               fr.source_player_name
               ORDER BY fr.source_player_name, lp.package_slot
             )
               AS player_names
        FROM euroleague.lineups l
        JOIN euroleague.lineup_players lp ON lp.lineup_id = l.lineup_id
        JOIN euroleague.full_rosters fr
          ON fr.game_id = l.game_id
         AND fr.team_id = l.team_id
         AND fr.player_id = lp.player_id
       GROUP BY l.lineup_id
    ),
    compared AS (
      SELECT
        old.game_id,
        old.source_event_order,
        old.team_id,
        old.opponent_team_id,
        old.period,
        old.type_lineup,
        old.own_starters,
        old.opp_starters,
        old.event_team_id,
        old.action_player_id,
        old.play_type,
        old.play_info,
        old.synthetic_ft_trip_id,
        old.parent_play_type,
        old.ft_reverse_order,
        old.points,
        old.ts_possessions,
        old.orebounds,
        old.oreb_opportunities,
        old.turnovers,
        old.steals,
        old.ft_attempts,
        old.fga,
        old.fgm,
        old.fg2_made,
        old.fg2_att,
        old.fg3_made,
        old.fg3_att,
        old.layup_made,
        old.layup_att,
        old.dunk_made,
        old.dunk_att,
        old.possession_flag,
        old.final_end_poss,
        old.endpoint_reason,
        old.event_elapsed_seconds,
        old.segment_id,
        old.own_team_score,
        old.opp_team_score,
        old.load_run_id,
        own_old.player_names AS old_own_lineup,
        opp_old.player_names AS old_opp_lineup,
        ARRAY(SELECT x FROM unnest(new.own_lineup) x ORDER BY x) AS new_own_lineup,
        ARRAY(SELECT x FROM unnest(new.opp_lineup) x ORDER BY x) AS new_opp_lineup,
        new.game_id AS new_game_id,
        new.source_event_order AS new_source_event_order,
        new.team_id AS new_team_id,
        new.opponent_team_id AS new_opponent_team_id,
        new.period AS new_period,
        new.type_lineup AS new_type_lineup,
        new.own_starters AS new_own_starters,
        new.opp_starters AS new_opp_starters,
        new.event_team_id AS new_event_team_id,
        new.action_player_id AS new_action_player_id,
        new.play_type AS new_play_type,
        new.play_info AS new_play_info,
        new.synthetic_ft_trip_id AS new_synthetic_ft_trip_id,
        new.parent_play_type AS new_parent_play_type,
        new.ft_reverse_order AS new_ft_reverse_order,
        new.points AS new_points,
        new.ts_possessions AS new_ts_possessions,
        new.orebounds AS new_orebounds,
        new.oreb_opportunities AS new_oreb_opportunities,
        new.turnovers AS new_turnovers,
        new.steals AS new_steals,
        new.ft_attempts AS new_ft_attempts,
        new.fga AS new_fga,
        new.fgm AS new_fgm,
        new.fg2_made AS new_fg2_made,
        new.fg2_att AS new_fg2_att,
        new.fg3_made AS new_fg3_made,
        new.fg3_att AS new_fg3_att,
        new.layup_made AS new_layup_made,
        new.layup_att AS new_layup_att,
        new.dunk_made AS new_dunk_made,
        new.dunk_att AS new_dunk_att,
        new.possession_flag AS new_possession_flag,
        new.final_end_poss AS new_final_end_poss,
        new.endpoint_reason AS new_endpoint_reason,
        new.event_elapsed_seconds AS new_event_elapsed_seconds,
        new.segment_id AS new_segment_id,
        new.own_team_score AS new_own_team_score,
        new.opp_team_score AS new_opp_team_score,
        new.load_run_id AS new_load_run_id
      FROM euroleague.action_team_context old
      JOIN old_lineups own_old ON own_old.lineup_id = old.own_lineup_id
      JOIN old_lineups opp_old ON opp_old.lineup_id = old.opp_lineup_id
      FULL JOIN euroleague.action_team_context_actions new
        ON new.game_id = old.game_id
       AND new.source_event_order = old.source_event_order
       AND new.team_id = old.team_id
    )
    SELECT 1
      FROM compared c
     WHERE c.game_id IS DISTINCT FROM c.new_game_id
        OR c.source_event_order IS DISTINCT FROM c.new_source_event_order
        OR c.team_id IS DISTINCT FROM c.new_team_id
        OR c.opponent_team_id IS DISTINCT FROM c.new_opponent_team_id
        OR c.period IS DISTINCT FROM c.new_period
        OR c.type_lineup IS DISTINCT FROM c.new_type_lineup
        OR c.own_starters IS DISTINCT FROM c.new_own_starters
        OR c.opp_starters IS DISTINCT FROM c.new_opp_starters
        OR c.event_team_id IS DISTINCT FROM c.new_event_team_id
        OR c.action_player_id IS DISTINCT FROM c.new_action_player_id
        OR c.play_type IS DISTINCT FROM c.new_play_type
        OR c.play_info IS DISTINCT FROM c.new_play_info
        OR c.synthetic_ft_trip_id IS DISTINCT FROM c.new_synthetic_ft_trip_id
        OR c.parent_play_type IS DISTINCT FROM c.new_parent_play_type
        OR c.ft_reverse_order IS DISTINCT FROM c.new_ft_reverse_order
        OR c.points IS DISTINCT FROM c.new_points
        OR c.ts_possessions IS DISTINCT FROM c.new_ts_possessions
        OR c.orebounds IS DISTINCT FROM c.new_orebounds
        OR c.oreb_opportunities IS DISTINCT FROM c.new_oreb_opportunities
        OR c.turnovers IS DISTINCT FROM c.new_turnovers
        OR c.steals IS DISTINCT FROM c.new_steals
        OR c.ft_attempts IS DISTINCT FROM c.new_ft_attempts
        OR c.fga IS DISTINCT FROM c.new_fga
        OR c.fgm IS DISTINCT FROM c.new_fgm
        OR c.fg2_made IS DISTINCT FROM c.new_fg2_made
        OR c.fg2_att IS DISTINCT FROM c.new_fg2_att
        OR c.fg3_made IS DISTINCT FROM c.new_fg3_made
        OR c.fg3_att IS DISTINCT FROM c.new_fg3_att
        OR c.layup_made IS DISTINCT FROM c.new_layup_made
        OR c.layup_att IS DISTINCT FROM c.new_layup_att
        OR c.dunk_made IS DISTINCT FROM c.new_dunk_made
        OR c.dunk_att IS DISTINCT FROM c.new_dunk_att
        OR c.possession_flag IS DISTINCT FROM c.new_possession_flag
        OR c.final_end_poss IS DISTINCT FROM c.new_final_end_poss
        OR c.endpoint_reason IS DISTINCT FROM c.new_endpoint_reason
        OR c.event_elapsed_seconds IS DISTINCT FROM c.new_event_elapsed_seconds
        OR c.segment_id IS DISTINCT FROM c.new_segment_id
        OR c.own_team_score IS DISTINCT FROM c.new_own_team_score
        OR c.opp_team_score IS DISTINCT FROM c.new_opp_team_score
        OR c.load_run_id IS DISTINCT FROM c.new_load_run_id
        OR c.old_own_lineup IS DISTINCT FROM c.new_own_lineup
        OR c.old_opp_lineup IS DISTINCT FROM c.new_opp_lineup
  ) THEN
    RAISE EXCEPTION
      'actions-based event fact differs from the legacy EuroLeague fact';
  END IF;

  IF EXISTS (
    WITH old_lineups AS (
      SELECT l.lineup_id,
             array_agg(
               fr.source_player_name
               ORDER BY fr.source_player_name, lp.package_slot
             )
               AS player_names
        FROM euroleague.lineups l
        JOIN euroleague.lineup_players lp ON lp.lineup_id = l.lineup_id
        JOIN euroleague.full_rosters fr
          ON fr.game_id = l.game_id
         AND fr.team_id = l.team_id
         AND fr.player_id = lp.player_id
       GROUP BY l.lineup_id
    )
    SELECT 1
      FROM euroleague.matchup_segments old
      JOIN old_lineups own_old ON own_old.lineup_id = old.own_lineup_id
      JOIN old_lineups opp_old ON opp_old.lineup_id = old.opp_lineup_id
      FULL JOIN euroleague.matchup_segments_actions new
        ON new.game_id = old.game_id
       AND new.team_id = old.team_id
       AND new.segment_id = old.segment_id
     WHERE old.game_id IS DISTINCT FROM new.game_id
        OR old.team_id IS DISTINCT FROM new.team_id
        OR old.segment_id IS DISTINCT FROM new.segment_id
        OR old.own_starters IS DISTINCT FROM new.own_starters
        OR old.opp_starters IS DISTINCT FROM new.opp_starters
        OR old.start_event_order IS DISTINCT FROM new.start_event_order
        OR old.end_event_order_exclusive IS DISTINCT FROM new.end_event_order_exclusive
        OR old.start_elapsed_seconds IS DISTINCT FROM new.start_elapsed_seconds
        OR old.end_elapsed_seconds IS DISTINCT FROM new.end_elapsed_seconds
        OR old.segment_seconds IS DISTINCT FROM new.segment_seconds
        OR old.load_run_id IS DISTINCT FROM new.load_run_id
        OR own_old.player_names IS DISTINCT FROM
           ARRAY(SELECT x FROM unnest(new.own_lineup) x ORDER BY x)
        OR opp_old.player_names IS DISTINCT FROM
           ARRAY(SELECT x FROM unnest(new.opp_lineup) x ORDER BY x)
  ) THEN
    RAISE EXCEPTION
      'actions-based matchup segments differ from legacy EuroLeague segments';
  END IF;
END;
$migration$;

COMMIT;
