-- Rebuild canonical event/segment timing without overwriting raw source clocks.
-- Segment durations are defined by consecutive lineup-segment starts. Interior
-- annotation rows therefore cannot inflate minutes through an extreme clock.

CREATE OR REPLACE FUNCTION basketball_test.refresh_segment_clock_fields_for_games(game_ids int4[])
RETURNS bigint
LANGUAGE plpgsql
-- Forbid nested-loop joins for this statement. The final UPDATE joins the
-- ~100k-row ordered_actions CTE, and the planner badly under-estimates that
-- join's cardinality (the min(id)=oa.id filter yields rows=1 estimates). At
-- scale, or once a prior batch's UPDATE has bloated the table mid-transaction,
-- it flips to a nested loop with the big CTE on the inner side and effectively
-- never completes. Hash/merge joins are stable and fast here regardless of
-- input size or in-transaction bloat.
SET enable_nestloop = off
AS $$
DECLARE
  updated_count bigint := 0;
BEGIN
  WITH action_grain AS (
    SELECT
      d.game_id,
      d.team_id,
      d.id,
      max(d.lineup_hash) AS lineup_hash,
      max(d.segment_id)::int AS segment_id,
      max(
        CASE
          WHEN d.quarter BETWEEN 1 AND 4
            AND d.end_game_seconds_remaining BETWEEN
              (4 - d.quarter) * 600 AND (5 - d.quarter) * 600
            THEN 2400 - d.end_game_seconds_remaining
          WHEN d.quarter >= 5
            AND d.end_game_seconds_remaining BETWEEN 0 AND 300
            THEN 2400 + (d.quarter - 5) * 300
              + (300 - d.end_game_seconds_remaining)
        END
      )::numeric AS event_elapsed_seconds
    FROM basketball_test.df_pts_poss_lineups_longer_mv d
    WHERE game_ids IS NULL OR d.game_id = ANY(game_ids)
    GROUP BY d.game_id, d.team_id, d.id
  ),
  action_order AS (
    SELECT
      ag.*,
      lag(ag.event_elapsed_seconds) OVER (
        PARTITION BY ag.game_id, ag.team_id
        ORDER BY ag.id
      ) AS previous_event_elapsed_seconds
    FROM action_grain ag
  ),
  ordered_actions AS (
    SELECT
      ao.*,
      greatest(
        coalesce(ao.previous_event_elapsed_seconds - ao.event_elapsed_seconds, 0),
        0
      )::numeric AS clock_regression_seconds
    FROM action_order ao
  ),
  segment_keys AS (
    SELECT
      d.game_id,
      d.team_id,
      d.lineup_hash,
      d.segment_id,
      min(d.id)::bigint AS segment_start_id
    FROM basketball_test.df_pts_poss_lineups_longer_mv d
    WHERE (game_ids IS NULL OR d.game_id = ANY(game_ids))
      AND d.lineup_hash IS NOT NULL
      AND d.segment_id IS NOT NULL
    GROUP BY d.game_id, d.team_id, d.lineup_hash, d.segment_id
  ),
  segment_starts AS (
    SELECT
      sk.*,
      oa.event_elapsed_seconds AS segment_start_elapsed_seconds
    FROM segment_keys sk
    JOIN ordered_actions oa
      ON oa.game_id = sk.game_id
     AND oa.team_id = sk.team_id
     AND oa.id = sk.segment_start_id
  ),
  game_ends AS (
    SELECT
      game_id,
      team_id,
      max(event_elapsed_seconds)::numeric AS game_end_elapsed_seconds
    FROM ordered_actions
    GROUP BY game_id, team_id
  ),
  segment_order AS (
    SELECT
      ss.*,
      ge.game_end_elapsed_seconds,
      lead(ss.segment_start_elapsed_seconds) OVER (
        PARTITION BY ss.game_id, ss.team_id
        ORDER BY ss.segment_start_id, ss.segment_id
      ) AS next_segment_start_elapsed_seconds
    FROM segment_starts ss
    JOIN game_ends ge USING (game_id, team_id)
  ),
  segment_durations AS (
    SELECT
      so.game_id,
      so.team_id,
      so.lineup_hash,
      so.segment_id,
      so.segment_start_elapsed_seconds,
      coalesce(
        so.next_segment_start_elapsed_seconds,
        so.game_end_elapsed_seconds
      )::numeric AS segment_end_elapsed_seconds,
      greatest(
        coalesce(
          so.next_segment_start_elapsed_seconds,
          so.game_end_elapsed_seconds
        ) - so.segment_start_elapsed_seconds,
        0
      )::numeric AS segment_seconds
    FROM segment_order so
  )
  UPDATE basketball_test.df_pts_poss_lineups_longer_mv d
  SET
    event_elapsed_seconds = oa.event_elapsed_seconds,
    clock_regression_seconds = oa.clock_regression_seconds,
    segment_start_elapsed_seconds = sd.segment_start_elapsed_seconds,
    segment_end_elapsed_seconds = sd.segment_end_elapsed_seconds,
    segment_seconds = sd.segment_seconds
  FROM ordered_actions oa
  JOIN segment_durations sd
    ON sd.game_id = oa.game_id
   AND sd.team_id = oa.team_id
   AND sd.lineup_hash = oa.lineup_hash
   AND sd.segment_id = oa.segment_id
  WHERE d.game_id = oa.game_id
    AND d.team_id = oa.team_id
    AND d.id = oa.id
    AND d.lineup_hash = oa.lineup_hash
    AND d.segment_id = oa.segment_id
    AND (game_ids IS NULL OR d.game_id = ANY(game_ids));

  GET DIAGNOSTICS updated_count = ROW_COUNT;
  RETURN updated_count;
END;
$$;
