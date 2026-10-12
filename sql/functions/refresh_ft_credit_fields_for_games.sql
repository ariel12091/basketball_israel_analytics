-- Credit every free throw to the lineup on its parent action's row (the foul,
-- or the made shot of an and-one), from the same team's same perspective.
-- Every other row is credited to its own lineup. Free throws with no parent
-- row keep their own lineup. game_ids NULL = every game.
--
-- (game_id, id, team_id, type_lineup) identifies a row: 0 duplicates in
-- 558,362 rows on 2026-10-12.
CREATE OR REPLACE FUNCTION basketball_test.refresh_ft_credit_fields_for_games(game_ids int4[])
RETURNS bigint
LANGUAGE plpgsql
SET plan_cache_mode = force_custom_plan
AS $$
DECLARE
  v_rows bigint;
BEGIN
  UPDATE basketball_test.df_pts_poss_lineups_longer_mv d
  SET credit_lineup_hash  = src.lineup_hash,
      credit_segment_id   = src.segment_id,
      credit_num_starters = src.num_starters,
      credit_own_starters = src.own_starters,
      credit_opp_starters = src.opp_starters
  FROM (
    SELECT a.game_id, a.id, a.team_id, a.type_lineup,
           COALESCE(p.lineup_hash, a.lineup_hash) AS lineup_hash,
           CASE WHEN p.id IS NULL THEN a.segment_id   ELSE p.segment_id   END AS segment_id,
           CASE WHEN p.id IS NULL THEN a.num_starters ELSE p.num_starters END AS num_starters,
           CASE WHEN p.id IS NULL THEN a.own_starters ELSE p.own_starters END AS own_starters,
           CASE WHEN p.id IS NULL THEN a.opp_starters ELSE p.opp_starters END AS opp_starters
    FROM basketball_test.df_pts_poss_lineups_longer_mv a
    LEFT JOIN basketball_test.df_pts_poss_lineups_longer_mv p
      ON a.type = 'freeThrow'
     AND p.game_id = a.game_id
     AND p.id = a.parent_action_id
     AND p.team_id = a.team_id
     AND p.type_lineup IS NOT DISTINCT FROM a.type_lineup
    WHERE game_ids IS NULL OR a.game_id = ANY(game_ids)
  ) src
  WHERE d.game_id = src.game_id
    AND d.id = src.id
    AND d.team_id = src.team_id
    AND d.type_lineup IS NOT DISTINCT FROM src.type_lineup
    AND (d.credit_lineup_hash, d.credit_segment_id, d.credit_num_starters,
         d.credit_own_starters, d.credit_opp_starters)
        IS DISTINCT FROM
        (src.lineup_hash, src.segment_id, src.num_starters, src.own_starters, src.opp_starters);
  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RETURN v_rows;
END;
$$;
