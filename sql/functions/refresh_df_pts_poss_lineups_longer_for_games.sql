CREATE OR REPLACE FUNCTION basketball_test.refresh_df_pts_poss_lineups_longer_for_games(game_ids int4[])
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
  inserted_count bigint := 0;
BEGIN
  IF game_ids IS NULL OR array_length(game_ids, 1) IS NULL THEN
    DELETE FROM basketball_test.df_pts_poss_lineups_longer_mv;
  ELSE
    DELETE FROM basketball_test.df_pts_poss_lineups_longer_mv
    WHERE game_id = ANY(game_ids);
  END IF;

  INSERT INTO basketball_test.df_pts_poss_lineups_longer_mv (
    quarter,
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
    event_owner_side,
    type_lineup,
    lineup_hash,
    num_starters,
    own_starters,
    opp_starters
  )
  WITH cum_scores AS (
    SELECT
      game_id,
      id,
      SUM(COALESCE(team_score, 0)) OVER (PARTITION BY game_id ORDER BY id) AS total_cum,
      SUM(COALESCE(team_score, 0)) OVER (PARTITION BY game_id, team_id ORDER BY id) AS team_cum
    FROM basketball_test.possessions
    WHERE game_ids IS NULL OR game_id = ANY(game_ids)
  )
  SELECT
    quarter,
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
  FROM (
    -- Base row: keep the original event team perspective from pws.team_id.
    -- type_lineup is assigned dynamically by event type/parameters_type ownership.
    -- Mirrored row duplicates for pws.team_id_defense and flips type_lineup.
    SELECT
      pws.quarter,
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
    FROM basketball_test.pws pws
    LEFT JOIN cum_scores cs
      ON pws.game_id = cs.game_id
     AND pws.id = cs.id
    WHERE game_ids IS NULL OR pws.game_id = ANY(game_ids)

    UNION ALL

    SELECT
      pws.quarter,
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
    FROM basketball_test.pws pws
    LEFT JOIN cum_scores cs
      ON pws.game_id = cs.game_id
     AND pws.id = cs.id
    WHERE game_ids IS NULL OR pws.game_id = ANY(game_ids)
  ) longer
  WHERE lineup_hash IS NOT NULL;

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  PERFORM basketball_test.refresh_segment_clock_fields_for_games(game_ids);
  RETURN inserted_count;
END;
$$;
