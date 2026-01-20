DROP FUNCTION IF EXISTS basketball_test.get_team_ratings_dynamic;

CREATE OR REPLACE FUNCTION basketball_test.get_team_ratings_dynamic(
    p_game_year       INT,
    p_start_date      DATE DEFAULT NULL,
    p_end_date        DATE DEFAULT NULL,
    p_game_type_csv   TEXT DEFAULT NULL,
    p_opp_team_ids_csv TEXT DEFAULT NULL,
    p_home_away       TEXT DEFAULT 'all',
    p_outcome         TEXT DEFAULT 'all',
    p_opp_rank_side   TEXT DEFAULT 'all',
    p_opp_rank_n      INT  DEFAULT NULL,
    p_opp_rank_metric TEXT DEFAULT 'net'
)
RETURNS TABLE (
    game_year      INT,
    team_id        INT,
    team_name      TEXT,
    off_ppp        NUMERIC,
    def_ppp        NUMERIC,
    net_rtg        NUMERIC,
    rank_net_rtg   BIGINT,
    rank_off_ppp   BIGINT,
    rank_def_ppp   BIGINT
) 
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_game_types      int4[];
  v_opp_ids         int4[];
  v_home_away       text;
  v_outcome         text;
  v_opp_rank_side   text;
  v_opp_rank_metric text;
BEGIN
  -- [Input Normalization]
  v_home_away       := COALESCE(NULLIF(btrim(p_home_away), ''), 'all');
  v_outcome         := COALESCE(NULLIF(btrim(p_outcome), ''), 'all');
  v_opp_rank_side   := COALESCE(NULLIF(btrim(p_opp_rank_side), ''), 'all');
  v_opp_rank_metric := COALESCE(NULLIF(btrim(p_opp_rank_metric), ''), 'net');

  -- Parse CSVs
  IF p_game_type_csv IS NOT NULL AND length(btrim(p_game_type_csv)) > 0 THEN
      v_game_types := ARRAY(SELECT DISTINCT x::int4 FROM unnest(string_to_array(regexp_replace(p_game_type_csv, '\s+', '', 'g'), ',')) x WHERE x <> '' ORDER BY 1);
  END IF;
  
  IF p_opp_team_ids_csv IS NOT NULL AND length(btrim(p_opp_team_ids_csv)) > 0 THEN
      v_opp_ids := ARRAY(SELECT DISTINCT x::int4 FROM unnest(string_to_array(regexp_replace(p_opp_team_ids_csv, '\s+', '', 'g'), ',')) x WHERE x <> '' ORDER BY 1);
  END IF;

  RETURN QUERY
  WITH 
  -- CTE 1: Games Base (Filter Schedule)
  games_base AS (
    SELECT fs.game_id, fs.team_id, fs.game_year, fs.opp_team_id
    FROM basketball_test.final_schedule_mv fs
    WHERE fs.game_year = p_game_year
      AND (p_start_date IS NULL OR fs.game_date >= p_start_date)
      AND (p_end_date   IS NULL OR fs.game_date <= p_end_date)
      AND (v_game_types IS NULL OR fs.game_type = ANY(v_game_types))
      AND (v_opp_ids    IS NULL OR fs.opp_team_id = ANY(v_opp_ids))
      AND (v_home_away = 'all' OR (v_home_away = 'home' AND fs.is_home) OR (v_home_away = 'away' AND NOT fs.is_home))
      AND (v_outcome = 'all'   OR (v_outcome = 'win' AND fs.has_won IS TRUE) OR (v_outcome = 'loss' AND fs.has_won IS FALSE))
  ),

  -- CTE 2: Games Ranked (Join Ratings MV to get Opponent Ranks)
  games_ranked AS (
    SELECT gb.game_id, gb.team_id, gb.game_year,
           CASE WHEN v_opp_rank_side IN ('top', 'bottom') THEN
             CASE v_opp_rank_metric 
               WHEN 'off' THEN r.rank_off_ppp 
               WHEN 'def' THEN r.rank_def_ppp 
               ELSE r.rank_net_rtg 
             END
           ELSE NULL END AS opp_rank,
           
           CASE WHEN v_opp_rank_side = 'bottom' THEN
             MAX(CASE v_opp_rank_metric 
                   WHEN 'off' THEN r.rank_off_ppp 
                   WHEN 'def' THEN r.rank_def_ppp 
                   ELSE r.rank_net_rtg 
                 END) OVER (PARTITION BY gb.game_year)
           ELSE NULL END AS max_rank
    FROM games_base gb
    LEFT JOIN basketball_test.team_ppp_ratings_mv r 
      ON r.game_year::integer = gb.game_year 
      AND r.team_id::integer  = gb.opp_team_id
      AND v_opp_rank_side IN ('top', 'bottom')
  ),

  -- CTE 3: Games Filtered (Apply Rank Filter)
  games_filtered AS (
    SELECT gr.game_id, gr.team_id, gr.game_year
    FROM games_ranked gr
    WHERE v_opp_rank_side = 'all' OR p_opp_rank_n IS NULL
       OR (v_opp_rank_side = 'top'    AND gr.opp_rank <= p_opp_rank_n)
       OR (v_opp_rank_side = 'bottom' AND gr.opp_rank >= (gr.max_rank - p_opp_rank_n + 1))
  ),

  -- CTE 4: Base Aggregation (Join Valid Games to Stats Table)
  base_agg AS (
      SELECT 
        gf.game_year,
        gf.team_id,
        dppllm.type_lineup,
        sum(dppllm.team_score) / NULLIF(sum(dppllm.final_end_poss::integer), 0)::numeric AS ppp
      FROM basketball_test.df_pts_poss_lineups_longer_mv dppllm
      JOIN games_filtered gf ON gf.game_id = dppllm.game_id AND gf.team_id = dppllm.team_id
      GROUP BY gf.game_year, gf.team_id, dppllm.type_lineup
  ),

  -- CTE 5: Pivot (Offense/Defense)
  pivoted AS (
      SELECT 
        base_agg.game_year,
        base_agg.team_id,
        max(base_agg.ppp) FILTER (WHERE base_agg.type_lineup = 'offense'::text) AS off_ppp_raw,
        max(base_agg.ppp) FILTER (WHERE base_agg.type_lineup = 'defense'::text) AS def_ppp_raw
      FROM base_agg
      GROUP BY base_agg.game_year, base_agg.team_id
  ),

  -- CTE 6: Final Calculation & Naming
  final_calc AS (
      SELECT 
        p.game_year,
        p.team_id,
        fr.team_name,
        round(p.off_ppp_raw, 3) * 100::numeric AS off_ppp,
        round(p.def_ppp_raw, 3) * 100::numeric AS def_ppp,
        round(p.off_ppp_raw - p.def_ppp_raw, 3) * 100::numeric AS net_rtg
      FROM pivoted p
      JOIN basketball_test.full_rosters fr 
        ON fr.game_year = p.game_year AND fr.team_id = p.team_id
      GROUP BY p.game_year, p.team_id, fr.team_name, p.off_ppp_raw, p.def_ppp_raw
  )

  -- Final Select with Ranks
  SELECT 
    fc.game_year,
    fc.team_id,
    fc.team_name,
    fc.off_ppp,
    fc.def_ppp,
    fc.net_rtg,
    dense_rank() OVER (PARTITION BY fc.game_year ORDER BY fc.net_rtg DESC NULLS LAST) AS rank_net_rtg,
    dense_rank() OVER (PARTITION BY fc.game_year ORDER BY fc.off_ppp DESC NULLS LAST) AS rank_off_ppp,
    dense_rank() OVER (PARTITION BY fc.game_year ORDER BY fc.def_ppp ASC NULLS LAST)  AS rank_def_ppp
  FROM final_calc fc;
END;
$$;