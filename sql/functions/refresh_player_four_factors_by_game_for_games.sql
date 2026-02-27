CREATE OR REPLACE FUNCTION basketball_test.refresh_player_four_factors_by_game_for_games(game_ids int4[])
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
  inserted_count bigint := 0;
BEGIN
  IF game_ids IS NULL OR array_length(game_ids, 1) IS NULL THEN
    DELETE FROM basketball_test.player_four_factors_by_game;
  ELSE
    DELETE FROM basketball_test.player_four_factors_by_game
    WHERE game_id = ANY(game_ids);
  END IF;

  INSERT INTO basketball_test.player_four_factors_by_game (
    player_id, team_id, game_id, game_year, is_on_key, type_lineup,
    num_starters, own_starters, opp_starters, total_points, total_poss,
    ts_poss_count, oreb_count, oreb_opportunities, tov_count,
    total_ft_attempts, total_fga
  )
  WITH base0 AS (
    SELECT DISTINCT
      ll.player_id,
      ll.team_id,
      ll.lineup_hash,
      COALESCE(ll.is_on_verdict, 0::numeric)::integer AS is_on_key
    FROM basketball_test.lineups_lookup ll
  ),
  clean_stats AS (
    SELECT
      d.id,
      d.game_id,
      d.team_id,
      d.lineup_hash,
      d.team_score,
      d.type,
      d.parameters_type,
      d.parameters_made,
      d.pct_ft,
      d.parent_action_id,
      d.type_lineup,
      d.own_starters,
      d.opp_starters,
      CASE WHEN d.final_end_poss IS TRUE THEN 1 ELSE 0 END AS final_end_flag
    FROM basketball_test.df_pts_poss_lineups_longer_mv d
    WHERE game_ids IS NULL OR d.game_id = ANY(game_ids)
  ),
  complex_flags AS (
    SELECT DISTINCT ON (d.id)
      d.id AS main_id,
      t2.type AS parent_type,
      t2.parameters_type AS parent_param
    FROM basketball_test.df_pts_poss_lineups_longer_mv d
    JOIN basketball_test.df_pts_poss_lineups_longer_mv t2
      ON t2.id = d.parent_action_id
      AND t2.game_id = d.game_id
      AND t2.type = 'foul'::text
    WHERE d.parent_action_id IS NOT NULL
      AND (game_ids IS NULL OR d.game_id = ANY(game_ids))
    ORDER BY d.id
  ),
  combined_data AS (
    SELECT
      b0.player_id,
      b0.team_id,
      b0.is_on_key,
      s.game_year,
      cs.game_id,
      cs.type_lineup,
      cs.own_starters AS num_starters,
      cs.own_starters,
      cs.opp_starters,
      cs.team_score,
      cs.final_end_flag,
      cs.type,
      cs.parameters_type,
      cs.parameters_made,
      cs.pct_ft,
      cs.parent_action_id,
      cf.parent_type,
      cf.parent_param
    FROM base0 b0
    JOIN clean_stats cs ON b0.lineup_hash = cs.lineup_hash AND b0.team_id = cs.team_id
    JOIN basketball_test.schedule s ON cs.game_id = s.game_id
    LEFT JOIN complex_flags cf ON cs.id = cf.main_id
  )
  SELECT
    cd.player_id,
    cd.team_id,
    cd.game_id,
    cd.game_year,
    cd.is_on_key,
    cd.type_lineup,
    cd.num_starters,
    cd.own_starters,
    cd.opp_starters,
    sum(cd.team_score) AS total_points,
    sum(cd.final_end_flag) AS total_poss,
    count(CASE WHEN cd.type = 'shot' THEN 1 END)
      + count(DISTINCT CASE
          WHEN cd.type = 'freeThrow'
            AND cd.parent_type = 'foul'
            AND cd.parent_param = 'personal'
          THEN cd.parent_action_id
        END) AS ts_poss_count,
    count(CASE WHEN cd.type = 'rebound' AND cd.parameters_type = 'offensive' THEN 1 END) AS oreb_count,
    count(CASE
      WHEN cd.type = 'shot' AND cd.parameters_made IN ('missed', 'blocked') THEN 1
      WHEN cd.type = 'freeThrow' AND cd.parameters_made = 'missed'
        AND cd.pct_ft = 1::numeric
        AND cd.parent_type = 'foul' AND cd.parent_param = 'personal' THEN 1
    END) AS oreb_opportunities,
    count(CASE WHEN cd.type = 'turnover' THEN 1 END) AS tov_count,
    count(CASE WHEN cd.type = 'freeThrow' THEN 1 END) AS total_ft_attempts,
    count(CASE WHEN cd.type = 'shot' THEN 1 END) AS total_fga
  FROM combined_data cd
  GROUP BY cd.player_id, cd.team_id, cd.game_id, cd.game_year, cd.is_on_key, cd.type_lineup, cd.num_starters, cd.own_starters, cd.opp_starters;

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$$;
