CREATE OR REPLACE FUNCTION basketball_test.refresh_team_ratings_preset_cache_for_games(
  game_ids int4[]
)
RETURNS bigint
LANGUAGE plpgsql
AS $$
DECLARE
  inserted_count bigint := 0;
BEGIN
  WITH affected_years AS (
    SELECT DISTINCT s.game_year
    FROM basketball_test.schedule s
    WHERE s.game_year IS NOT NULL
      AND (game_ids IS NULL OR s.game_id = ANY(game_ids))
  )
  DELETE FROM basketball_test.team_ratings_preset_cache c
  WHERE c.game_year IN (SELECT ay.game_year FROM affected_years ay);

  WITH affected_years AS (
    SELECT DISTINCT s.game_year
    FROM basketball_test.schedule s
    WHERE s.game_year IS NOT NULL
      AND (game_ids IS NULL OR s.game_id = ANY(game_ids))
  )
  INSERT INTO basketball_test.team_ratings_preset_cache (
    preset_variant,
    game_year,
    team_id,
    team_name,
    off_ppp,
    def_ppp,
    net_rtg,
    games_played,
    wins,
    losses,
    off_poss,
    def_poss,
    rank_net_rtg,
    rank_off_ppp,
    rank_def_ppp,
    off_fga,
    off_layup_att,
    off_dunk_att,
    off_fg3_att,
    off_c3_att,
    off_c3_known_att,
    def_fga,
    def_layup_att,
    def_dunk_att,
    def_fg3_att,
    def_c3_att,
    def_c3_known_att
  )
  SELECT preset_rows.*
  FROM affected_years ay
  CROSS JOIN LATERAL (
    SELECT 'starters_hi'::text AS preset_variant, r.*
    FROM basketball_test.get_team_ratings_dynamic(
      ay.game_year,
      p_num_starters_off_min := 3,
      p_num_starters_off_max := 5
    ) r

    UNION ALL

    SELECT 'starters_lo'::text AS preset_variant, r.*
    FROM basketball_test.get_team_ratings_dynamic(
      ay.game_year,
      p_num_starters_off_min := 0,
      p_num_starters_off_max := 2
    ) r

    UNION ALL

    SELECT 'clutch'::text AS preset_variant, r.*
    FROM basketball_test.get_team_ratings_dynamic(
      ay.game_year,
      p_max_margin := 5,
      p_max_time_remaining := 300
    ) r

    UNION ALL

    SELECT 'last10'::text AS preset_variant, r.*
    FROM basketball_test.get_team_ratings_dynamic(
      ay.game_year,
      p_last_n_games := 10
    ) r

    UNION ALL

    SELECT 'top4'::text AS preset_variant, r.*
    FROM basketball_test.get_team_ratings_dynamic(
      ay.game_year,
      p_opp_rank_side := 'top',
      p_opp_rank_n := 4,
      p_opp_rank_metric := 'net'
    ) r

    UNION ALL

    SELECT 'bottom4'::text AS preset_variant, r.*
    FROM basketball_test.get_team_ratings_dynamic(
      ay.game_year,
      p_opp_rank_side := 'bottom',
      p_opp_rank_n := 4,
      p_opp_rank_metric := 'net'
    ) r
  ) preset_rows;

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$$;

REVOKE ALL ON FUNCTION
  basketball_test.refresh_team_ratings_preset_cache_for_games(int4[])
  FROM PUBLIC, app_readonly;
