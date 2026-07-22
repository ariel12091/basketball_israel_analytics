-- basketball_test.df_pts_poss_lineups_longer_mv source

DROP MATERIALIZED VIEW IF EXISTS basketball_test.df_pts_poss_lineups_longer_mv;
DROP TABLE IF EXISTS basketball_test.df_pts_poss_lineups_longer_mv;

CREATE TABLE basketball_test.df_pts_poss_lineups_longer_mv AS
WITH cum_scores AS (
    SELECT game_id, id,
      SUM(COALESCE(team_score, 0)) OVER (PARTITION BY game_id ORDER BY id) AS total_cum,
      SUM(COALESCE(team_score, 0)) OVER (PARTITION BY game_id, team_id ORDER BY id) AS team_cum
    FROM possessions
)
SELECT quarter,
    parameters_type,
    parameters_points,
    parameters_made,
    id,
    parent_action_id,
    type,
    player_id,
    team_id,
    game_id,
    end_game_seconds_remaining,
    pct_ft,
    team_score,
    final_end_poss,
    segment_id,
    final_end_id,
    own_team_score,
    opp_team_score,
    CASE
      WHEN type = 'rebound' THEN
        CASE parameters_type
          WHEN 'offensive' THEN 'offense'
          WHEN 'defensive' THEN 'defense'
          ELSE NULL
        END
      WHEN type IN ('shot', 'freeThrow', 'assist', 'turnover', 'foul-drawn') THEN 'offense'
      WHEN type IN ('steal', 'block', 'deflection', 'foul') THEN 'defense'
      ELSE NULL
    END AS event_owner_side,
    type_lineup,
    lineup_hash,
    num_starters,
    own_starters,
    opp_starters
    ,NULL::numeric AS event_elapsed_seconds
    ,NULL::numeric AS clock_regression_seconds
    ,NULL::numeric AS segment_start_elapsed_seconds
    ,NULL::numeric AS segment_end_elapsed_seconds
    ,NULL::numeric AS segment_seconds
   FROM ( -- Base row: keep the original event team perspective from pws.team_id.
          -- type_lineup is assigned dynamically by event type/parameters_type ownership.
          -- Mirrored row duplicates the same event for the opposite team (pws.team_id_defense)
          -- and flips type_lineup to the opposite side.
          SELECT pws.quarter,
            pws.parameters_type,
            pws.parameters_points,
            pws.parameters_made,
            pws.id,
            pws.parent_action_id,
            pws.type,
            pws.player_id,
            pws.team_id,
            pws.game_id,
            pws.end_game_seconds_remaining,
            pws.pct_ft,
            pws.team_score,
            pws.final_end_poss,
            pws.segment_id,
            pws.final_end_id,
            cs.team_cum AS own_team_score,
            cs.total_cum - cs.team_cum AS opp_team_score,
            CASE
              WHEN pws.type = 'rebound' THEN
                CASE pws.parameters_type
                  WHEN 'offensive' THEN 'offense'
                  WHEN 'defensive' THEN 'defense'
                  ELSE NULL
                END
              WHEN pws.type IN ('shot', 'freeThrow', 'assist', 'turnover', 'foul-drawn') THEN 'offense'
              WHEN pws.type IN ('steal', 'block', 'deflection', 'foul') THEN 'defense'
              ELSE NULL
            END AS type_lineup,
            pws.lineup_hash_offense AS lineup_hash,
            pws.num_starters_offense AS num_starters,
            pws.num_starters_offense AS own_starters,
            pws.num_starters_defense AS opp_starters
           FROM pws
           LEFT JOIN cum_scores cs ON pws.game_id = cs.game_id AND pws.id = cs.id
        UNION ALL
         SELECT pws.quarter,
            pws.parameters_type,
            pws.parameters_points,
            pws.parameters_made,
            pws.id,
            pws.parent_action_id,
            pws.type,
            pws.player_id,
            pws.team_id_defense,
            pws.game_id,
            pws.end_game_seconds_remaining,
            pws.pct_ft,
            pws.team_score,
            pws.final_end_poss,
            pws.segment_id,
            pws.final_end_id,
            cs.total_cum - cs.team_cum AS own_team_score,
            cs.team_cum AS opp_team_score,
            CASE
              WHEN pws.type = 'rebound' THEN
                CASE pws.parameters_type
                  WHEN 'offensive' THEN 'defense'
                  WHEN 'defensive' THEN 'offense'
                  ELSE NULL
                END
              WHEN pws.type IN ('shot', 'freeThrow', 'assist', 'turnover', 'foul-drawn') THEN 'defense'
              WHEN pws.type IN ('steal', 'block', 'deflection', 'foul') THEN 'offense'
              ELSE NULL
            END AS type_lineup,
            pws.lineup_hash_defense AS lineup_hash,
            pws.num_starters_defense AS num_starters,
            pws.num_starters_defense AS own_starters,
            pws.num_starters_offense AS opp_starters
           FROM pws
           LEFT JOIN cum_scores cs ON pws.game_id = cs.game_id AND pws.id = cs.id) longer
  WHERE lineup_hash IS NOT NULL
;

-- Populate canonical timing after the base event table exists. Raw quarter and
-- clock fields remain unchanged for source auditing.
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
    WHERE d.lineup_hash IS NOT NULL
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
  AND d.segment_id = oa.segment_id;

-- Table indexes:
CREATE INDEX dfppl_game_id_idx ON basketball_test.df_pts_poss_lineups_longer_mv USING btree (game_id);
CREATE INDEX dfppl_team_game_idx ON basketball_test.df_pts_poss_lineups_longer_mv USING btree (team_id, game_id);
CREATE INDEX dfppl_id_game_type_idx ON basketball_test.df_pts_poss_lineups_longer_mv USING btree (game_id, id, type);
CREATE INDEX dfppl_parent_game_idx ON basketball_test.df_pts_poss_lineups_longer_mv USING btree (game_id, parent_action_id) WHERE (parent_action_id IS NOT NULL);
CREATE INDEX idx_df_longer_game_team_lineup ON basketball_test.df_pts_poss_lineups_longer_mv USING btree (game_id, team_id, lineup_hash);
CREATE INDEX idx_df_longer_game_team_segment ON basketball_test.df_pts_poss_lineups_longer_mv USING btree (game_id, team_id, segment_id);
