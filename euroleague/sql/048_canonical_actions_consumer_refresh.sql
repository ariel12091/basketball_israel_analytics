-- EuroLeague shadow schema -- migration 048: canonical consumer refresh definition.
--
-- Literal source of truth for refresh_actions_consumer_candidates(bigint[]).
-- Migrations 015 and 016 historically patched the catalog definition at apply
-- time; this snapshot captures their resulting semantics as reviewable SQL.

BEGIN;
SET LOCAL search_path TO euroleague, public;

CREATE OR REPLACE FUNCTION euroleague.refresh_actions_consumer_candidates(game_ids bigint[])
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
      CASE WHEN euroleague.effective_period(a.period, a.minute, a.play_type) <= 4 THEN (euroleague.effective_period(a.period, a.minute, a.play_type) - 1) * 600
           ELSE 2400 + (euroleague.effective_period(a.period, a.minute, a.play_type) - 5) * 300 END::numeric AS period_start,
      CASE WHEN euroleague.effective_period(a.period, a.minute, a.play_type) <= 4 THEN 600 ELSE 300 END::numeric AS period_length,
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
           (2400 + greatest(max(euroleague.effective_period(a.period, a.minute, a.play_type)) - 4, 0) * 300)::numeric
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
  event_lineups AS MATERIALIZED (
    SELECT
      a.game_id,
      a.source_event_order,
      ec.event_elapsed_seconds,
      ge.game_end_elapsed_seconds,
      tg.home_team_id,
      tg.away_team_id,
      ARRAY(SELECT x FROM unnest(a.lineup_a) x ORDER BY x) AS lineup_a,
      ARRAY(SELECT x FROM unnest(a.lineup_b) x ORDER BY x) AS lineup_b,
      tg.last_seen_load_run_id
    FROM euroleague.actions a
    JOIN target_games tg ON tg.game_id = a.game_id
    JOIN event_clock ec
      ON ec.game_id = a.game_id
     AND ec.source_event_order = a.source_event_order
    JOIN game_ends ge ON ge.game_id = a.game_id
  ),
  event_sides AS MATERIALIZED (
    SELECT
      el.game_id, el.source_event_order,
      el.event_elapsed_seconds, el.game_end_elapsed_seconds,
      el.home_team_id AS team_id, el.away_team_id AS opponent_team_id,
      el.lineup_a AS own_lineup, el.lineup_b AS opp_lineup,
      el.last_seen_load_run_id
    FROM event_lineups el
    UNION ALL
    SELECT
      el.game_id, el.source_event_order,
      el.event_elapsed_seconds, el.game_end_elapsed_seconds,
      el.away_team_id, el.home_team_id,
      el.lineup_b, el.lineup_a,
      el.last_seen_load_run_id
    FROM event_lineups el
  ),
  lineup_sided AS MATERIALIZED (
    SELECT
      es.game_id,
      es.source_event_order,
      es.event_elapsed_seconds,
      es.game_end_elapsed_seconds,
      es.team_id,
      es.own_lineup,
      es.opp_lineup,
      own_count.starters AS own_starters,
      opp_count.starters AS opp_starters,
      es.last_seen_load_run_id
    FROM event_sides es
    JOIN starter_counts own_count
      ON own_count.game_id = es.game_id
     AND own_count.team_id = es.team_id
     AND own_count.lineup = es.own_lineup
    JOIN starter_counts opp_count
      ON opp_count.game_id = es.game_id
     AND opp_count.team_id = es.opponent_team_id
     AND opp_count.lineup = es.opp_lineup
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
    'actions-v2'
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
      CASE WHEN euroleague.effective_period(a.period, a.minute, a.play_type) <= 4 THEN (euroleague.effective_period(a.period, a.minute, a.play_type) - 1) * 600
           ELSE 2400 + (euroleague.effective_period(a.period, a.minute, a.play_type) - 5) * 300 END::numeric AS period_start,
      CASE WHEN euroleague.effective_period(a.period, a.minute, a.play_type) <= 4 THEN 600 ELSE 300 END::numeric AS period_length,
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
    euroleague.effective_period(sd.period, sd.minute, sd.play_type),
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
    'actions-v2'
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


REVOKE ALL ON FUNCTION euroleague.refresh_actions_consumer_candidates(bigint[])
  FROM PUBLIC, anon, authenticated, app_readonly, service_role;

COMMIT;
