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
    p_opp_rank_metric TEXT DEFAULT 'net',
    p_max_margin      INT  DEFAULT NULL,
    p_margin_status   TEXT DEFAULT 'all',
    p_max_time_remaining INT DEFAULT NULL,
    p_ot_margin_filter BOOLEAN DEFAULT FALSE,
    p_min_gn           INT DEFAULT NULL,
    p_max_gn           INT DEFAULT NULL,
    p_last_n_games     INT DEFAULT NULL,
    p_num_starters_off INT DEFAULT NULL,
    p_num_starters_def INT DEFAULT NULL,
    p_num_starters_off_min INT DEFAULT NULL,
    p_num_starters_off_max INT DEFAULT NULL,
    p_num_starters_def_min INT DEFAULT NULL,
    p_num_starters_def_max INT DEFAULT NULL
)
RETURNS TABLE (
    game_year      INT,
    team_id        INT,
    team_name      TEXT,
    off_ppp        NUMERIC,
    def_ppp        NUMERIC,
    net_rtg        NUMERIC,
    games_played   INT,
    wins           INT,
    losses         INT,
    off_poss       INT,
    def_poss       INT,
    rank_net_rtg   BIGINT,
    rank_off_ppp   BIGINT,
    rank_def_ppp   BIGINT,
    off_fga        INT,
    off_layup_att  INT,
    off_dunk_att   INT,
    off_fg3_att    INT,
    off_c3_att     INT,
    off_c3_known_att INT,
    def_fga        INT,
    def_layup_att  INT,
    def_dunk_att   INT,
    def_fg3_att    INT,
    def_c3_att     INT,
    def_c3_known_att INT
) 
LANGUAGE plpgsql
STABLE
SET plan_cache_mode = force_custom_plan
AS $$
DECLARE
  v_game_types      int4[];
  v_opp_ids         int4[];
  v_home_away       text;
  v_outcome         text;
  v_opp_rank_side   text;
  v_opp_rank_metric text;
  v_margin_status   text;
BEGIN
  -- [Input Normalization]
  v_home_away       := COALESCE(NULLIF(btrim(p_home_away), ''), 'all');
  v_outcome         := COALESCE(NULLIF(btrim(p_outcome), ''), 'all');
  v_opp_rank_side   := COALESCE(NULLIF(btrim(p_opp_rank_side), ''), 'all');
  v_opp_rank_metric := COALESCE(NULLIF(btrim(p_opp_rank_metric), ''), 'net');
  v_margin_status   := COALESCE(NULLIF(btrim(p_margin_status), ''), 'all');

  -- Parse CSVs
  IF p_game_type_csv IS NOT NULL AND length(btrim(p_game_type_csv)) > 0 THEN
      v_game_types := ARRAY(SELECT DISTINCT x::int4 FROM unnest(string_to_array(regexp_replace(p_game_type_csv, '\s+', '', 'g'), ',')) x WHERE x <> '' ORDER BY 1);
  END IF;
  
  IF p_opp_team_ids_csv IS NOT NULL AND length(btrim(p_opp_team_ids_csv)) > 0 THEN
      v_opp_ids := ARRAY(SELECT DISTINCT x::int4 FROM unnest(string_to_array(regexp_replace(p_opp_team_ids_csv, '\s+', '', 'g'), ',')) x WHERE x <> '' ORDER BY 1);
  END IF;

  RETURN QUERY
  WITH
  schedule_ranked AS (
    SELECT
      fsr.game_id,
      fsr.team_id,
      fsr.game_year,
      ROW_NUMBER() OVER (
        PARTITION BY fsr.team_id, fsr.game_year
        ORDER BY fsr.game_date DESC NULLS LAST, fsr.game_id DESC
      ) AS rn_recent
    FROM basketball_test.final_schedule_mv fsr
    WHERE fsr.game_year = p_game_year
  ),
  -- CTE 1: Games Base (Filter Schedule)
  games_base AS (
    SELECT fs.game_id, fs.team_id, fs.game_year, fs.opp_team_id, fs.has_won
    FROM basketball_test.final_schedule_mv fs
    JOIN schedule_ranked sr
      ON sr.game_id = fs.game_id
     AND sr.team_id = fs.team_id
     AND sr.game_year = fs.game_year
    WHERE fs.game_year = p_game_year
      AND (p_start_date IS NULL OR fs.game_date >= p_start_date)
      AND (p_end_date   IS NULL OR fs.game_date <= p_end_date)
      AND (v_game_types IS NULL OR fs.game_type = ANY(v_game_types))
      AND (v_opp_ids    IS NULL OR fs.opp_team_id = ANY(v_opp_ids))
      AND (v_home_away = 'all' OR (v_home_away = 'home' AND fs.is_home) OR (v_home_away = 'away' AND NOT fs.is_home))
      AND (v_outcome = 'all'   OR (v_outcome = 'win' AND fs.has_won IS TRUE) OR (v_outcome = 'loss' AND fs.has_won IS FALSE))
      AND (p_min_gn IS NULL OR fs.gn >= p_min_gn)
      AND (p_max_gn IS NULL OR fs.gn <= p_max_gn)
      AND (p_last_n_games IS NULL OR sr.rn_recent <= p_last_n_games)
  ),

  -- CTE 2: Games Ranked (Join Ratings MV to get Opponent Ranks)
  games_ranked AS (
    SELECT gb.game_id, gb.team_id, gb.game_year, gb.has_won,
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
    SELECT gr.game_id, gr.team_id, gr.game_year, gr.has_won
    FROM games_ranked gr
    WHERE v_opp_rank_side = 'all' OR p_opp_rank_n IS NULL
       OR (v_opp_rank_side = 'top'    AND gr.opp_rank <= p_opp_rank_n)
       OR (v_opp_rank_side = 'bottom' AND gr.opp_rank >= (gr.max_rank - p_opp_rank_n + 1))
  ),

  -- CTE 4: single clutch-filtered scan of the raw MV, pre-aggregated per game
  -- (tiny materialized set — one row per game/team/type_lineup — consumed by
  -- qualifying_games and base_agg; replaces the former double scan)
  -- NOTE: Use pre-shot margin (subtract points scored from current score)
  game_agg AS (
      SELECT
        gf.game_year,
        gf.team_id,
        gf.game_id,
        gf.has_won,
        dppllm.type_lineup,
        SUM(dppllm.team_score) AS pts,
        SUM(CASE WHEN dppllm.final_end_poss IS TRUE THEN 1 ELSE 0 END) AS poss,
        SUM(CASE WHEN dppllm.type = 'shot' THEN 1 ELSE 0 END) AS fga,
        SUM(CASE WHEN dppllm.type = 'shot' AND dppllm.parameters_points = 2 AND dppllm.parameters_type = 'lay-up' THEN 1 ELSE 0 END) AS layup_att,
        SUM(CASE WHEN dppllm.type = 'shot' AND dppllm.parameters_points = 2 AND dppllm.parameters_type IN ('dunk', 'allyhoop') THEN 1 ELSE 0 END) AS dunk_att,
        SUM(CASE WHEN dppllm.type = 'shot' AND dppllm.parameters_points = 3 THEN 1 ELSE 0 END) AS fg3_att,
        SUM(CASE WHEN dppllm.type = 'shot' AND dppllm.parameters_points = 3 AND z.is_corner3 IS TRUE THEN 1 ELSE 0 END) AS c3_att,
        SUM(CASE WHEN dppllm.type = 'shot' AND dppllm.parameters_points = 3 AND z.is_corner3 IS NOT NULL THEN 1 ELSE 0 END) AS c3_known_att
      FROM basketball_test.df_pts_poss_lineups_longer_mv dppllm
      JOIN games_filtered gf ON gf.game_id = dppllm.game_id AND gf.team_id = dppllm.team_id
      LEFT JOIN basketball_test.shot_zones z
        ON z.game_id = dppllm.game_id
       AND z.id = dppllm.id
      WHERE (p_max_margin IS NULL
             OR ABS(CASE WHEN dppllm.type_lineup = 'offense'
                         THEN (dppllm.own_team_score - COALESCE(dppllm.team_score, 0)) - dppllm.opp_team_score
                         ELSE dppllm.own_team_score - (dppllm.opp_team_score - COALESCE(dppllm.team_score, 0))
                    END) <= p_max_margin
             OR (dppllm.quarter > 4 AND NOT COALESCE(p_ot_margin_filter, FALSE)))
        AND (v_margin_status = 'all'
             OR (v_margin_status = 'leading'  AND
                 CASE WHEN dppllm.type_lineup = 'offense'
                      THEN (dppllm.own_team_score - COALESCE(dppllm.team_score, 0)) > dppllm.opp_team_score
                      ELSE dppllm.own_team_score > (dppllm.opp_team_score - COALESCE(dppllm.team_score, 0))
                 END)
             OR (v_margin_status = 'trailing' AND
                 CASE WHEN dppllm.type_lineup = 'offense'
                      THEN (dppllm.own_team_score - COALESCE(dppllm.team_score, 0)) < dppllm.opp_team_score
                      ELSE dppllm.own_team_score < (dppllm.opp_team_score - COALESCE(dppllm.team_score, 0))
                 END)
             OR (v_margin_status = 'tied'     AND
                 CASE WHEN dppllm.type_lineup = 'offense'
                      THEN (dppllm.own_team_score - COALESCE(dppllm.team_score, 0)) = dppllm.opp_team_score
                      ELSE dppllm.own_team_score = (dppllm.opp_team_score - COALESCE(dppllm.team_score, 0))
                 END)
             OR (dppllm.quarter > 4 AND NOT COALESCE(p_ot_margin_filter, FALSE)))
        AND (p_max_time_remaining IS NULL OR dppllm.end_game_seconds_remaining <= p_max_time_remaining OR dppllm.quarter > 4)
        AND (COALESCE(p_num_starters_off_min, p_num_starters_off) IS NULL OR dppllm.own_starters >= COALESCE(p_num_starters_off_min, p_num_starters_off))
        AND (COALESCE(p_num_starters_off_max, p_num_starters_off) IS NULL OR dppllm.own_starters <= COALESCE(p_num_starters_off_max, p_num_starters_off))
        AND (COALESCE(p_num_starters_def_min, p_num_starters_def) IS NULL OR dppllm.opp_starters >= COALESCE(p_num_starters_def_min, p_num_starters_def))
        AND (COALESCE(p_num_starters_def_max, p_num_starters_def) IS NULL OR dppllm.opp_starters <= COALESCE(p_num_starters_def_max, p_num_starters_def))
      GROUP BY gf.game_year, gf.team_id, gf.game_id, gf.has_won, dppllm.type_lineup
  ),

  -- CTE 4a: distinct games with at least one qualifying row
  qualifying_games AS (
      SELECT DISTINCT ga.game_year, ga.team_id, ga.game_id, ga.has_won
      FROM game_agg ga
  ),

  -- CTE 4b: Win/Loss counts (from qualifying games only)
  win_loss AS (
    SELECT qg.game_year,
           qg.team_id,
           COUNT(*) FILTER (WHERE qg.has_won = TRUE) AS wins,
           COUNT(*) FILTER (WHERE qg.has_won = FALSE) AS losses
    FROM qualifying_games qg
    GROUP BY qg.game_year, qg.team_id
  ),

  -- CTE 5: Base Aggregation over the per-game pre-aggregates
  base_agg AS (
      SELECT
        ga.game_year,
        ga.team_id,
        ga.type_lineup,
        sum(ga.pts) / NULLIF(sum(ga.poss), 0)::numeric AS ppp,
        sum(ga.poss) AS total_poss,
        COUNT(DISTINCT ga.game_id) AS games_count,
        SUM(ga.fga) AS fga,
        SUM(ga.layup_att) AS layup_att,
        SUM(ga.dunk_att) AS dunk_att,
        SUM(ga.fg3_att) AS fg3_att,
        SUM(ga.c3_att) AS c3_att,
        SUM(ga.c3_known_att) AS c3_known_att
      FROM game_agg ga
      GROUP BY ga.game_year, ga.team_id, ga.type_lineup
  ),

  -- CTE 6: Pivot (Offense/Defense)
  pivoted AS (
      SELECT
        base_agg.game_year,
        base_agg.team_id,
        max(base_agg.ppp) FILTER (WHERE base_agg.type_lineup = 'offense'::text) AS off_ppp_raw,
        max(base_agg.ppp) FILTER (WHERE base_agg.type_lineup = 'defense'::text) AS def_ppp_raw,
        max(base_agg.games_count) AS games_played,
        wl.wins,
        wl.losses,
        max(base_agg.total_poss) FILTER (WHERE base_agg.type_lineup = 'offense'::text) AS off_poss,
        max(base_agg.total_poss) FILTER (WHERE base_agg.type_lineup = 'defense'::text) AS def_poss,
        max(base_agg.fga) FILTER (WHERE base_agg.type_lineup = 'offense'::text) AS off_fga,
        max(base_agg.layup_att) FILTER (WHERE base_agg.type_lineup = 'offense'::text) AS off_layup_att,
        max(base_agg.dunk_att) FILTER (WHERE base_agg.type_lineup = 'offense'::text) AS off_dunk_att,
        max(base_agg.fg3_att) FILTER (WHERE base_agg.type_lineup = 'offense'::text) AS off_fg3_att,
        max(base_agg.c3_att) FILTER (WHERE base_agg.type_lineup = 'offense'::text) AS off_c3_att,
        max(base_agg.c3_known_att) FILTER (WHERE base_agg.type_lineup = 'offense'::text) AS off_c3_known_att,
        max(base_agg.fga) FILTER (WHERE base_agg.type_lineup = 'defense'::text) AS def_fga,
        max(base_agg.layup_att) FILTER (WHERE base_agg.type_lineup = 'defense'::text) AS def_layup_att,
        max(base_agg.dunk_att) FILTER (WHERE base_agg.type_lineup = 'defense'::text) AS def_dunk_att,
        max(base_agg.fg3_att) FILTER (WHERE base_agg.type_lineup = 'defense'::text) AS def_fg3_att,
        max(base_agg.c3_att) FILTER (WHERE base_agg.type_lineup = 'defense'::text) AS def_c3_att,
        max(base_agg.c3_known_att) FILTER (WHERE base_agg.type_lineup = 'defense'::text) AS def_c3_known_att
      FROM base_agg
      LEFT JOIN win_loss wl ON wl.game_year = base_agg.game_year AND wl.team_id = base_agg.team_id
      GROUP BY base_agg.game_year, base_agg.team_id, wl.wins, wl.losses
  ),

  -- CTE 6b: one name per team for the requested season (1:1 join, no dedupe GROUP BY)
  team_names AS (
      SELECT fr.team_id, MIN(fr.team_name) AS team_name
      FROM basketball_test.full_rosters fr
      WHERE fr.game_year = p_game_year
      GROUP BY fr.team_id
  ),

  -- CTE 7: Final Calculation & Naming
  final_calc AS (
      SELECT
        p.game_year,
        p.team_id,
        tn.team_name,
        round(p.off_ppp_raw, 3) * 100::numeric AS off_ppp,
        round(p.def_ppp_raw, 3) * 100::numeric AS def_ppp,
        round(p.off_ppp_raw - p.def_ppp_raw, 3) * 100::numeric AS net_rtg,
        p.games_played,
        p.wins,
        p.losses,
        p.off_poss,
        p.def_poss,
        p.off_fga,
        p.off_layup_att,
        p.off_dunk_att,
        p.off_fg3_att,
        p.off_c3_att,
        p.off_c3_known_att,
        p.def_fga,
        p.def_layup_att,
        p.def_dunk_att,
        p.def_fg3_att,
        p.def_c3_att,
        p.def_c3_known_att
      FROM pivoted p
      JOIN team_names tn ON tn.team_id = p.team_id
  )

  -- Final Select with Ranks
  SELECT
    fc.game_year,
    fc.team_id,
    fc.team_name,
    fc.off_ppp,
    fc.def_ppp,
    fc.net_rtg,
    fc.games_played::int,
    COALESCE(fc.wins, 0)::int AS wins,
    COALESCE(fc.losses, 0)::int AS losses,
    fc.off_poss::int,
    fc.def_poss::int,
    dense_rank() OVER (PARTITION BY fc.game_year ORDER BY fc.net_rtg DESC NULLS LAST) AS rank_net_rtg,
    dense_rank() OVER (PARTITION BY fc.game_year ORDER BY fc.off_ppp DESC NULLS LAST) AS rank_off_ppp,
    dense_rank() OVER (PARTITION BY fc.game_year ORDER BY fc.def_ppp ASC NULLS LAST)  AS rank_def_ppp,
    fc.off_fga::int,
    fc.off_layup_att::int,
    fc.off_dunk_att::int,
    fc.off_fg3_att::int,
    fc.off_c3_att::int,
    fc.off_c3_known_att::int,
    fc.def_fga::int,
    fc.def_layup_att::int,
    fc.def_dunk_att::int,
    fc.def_fg3_att::int,
    fc.def_c3_att::int,
    fc.def_c3_known_att::int
  FROM final_calc fc;
END;
$$;
