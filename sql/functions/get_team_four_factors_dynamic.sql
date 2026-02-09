DROP FUNCTION IF EXISTS basketball_test.get_team_four_factors_dynamic;

CREATE OR REPLACE FUNCTION basketball_test.get_team_four_factors_dynamic(
    p_game_year        INT,
    p_start_date       DATE DEFAULT NULL,
    p_end_date         DATE DEFAULT NULL,
    p_game_type_csv    TEXT DEFAULT NULL,
    p_opp_team_ids_csv TEXT DEFAULT NULL,
    p_home_away        TEXT DEFAULT 'all',
    p_outcome          TEXT DEFAULT 'all',
    p_opp_rank_side    TEXT DEFAULT 'all',
    p_opp_rank_n       INT  DEFAULT NULL,
    p_opp_rank_metric  TEXT DEFAULT 'net',
    p_max_margin       INT  DEFAULT NULL,
    p_margin_status    TEXT DEFAULT 'all',
    p_max_time_remaining INT DEFAULT NULL,
    p_ot_margin_filter BOOLEAN DEFAULT FALSE
)
RETURNS TABLE (
    team_id        INT,
    game_year      INT,
    team_name      TEXT,
    off_ts         NUMERIC,
    off_oreb       NUMERIC,
    off_tov        NUMERIC,
    off_ftr        NUMERIC,
    off_ppp        NUMERIC,
    off_poss       INT,
    off_pts        INT,
    off_ts_poss    INT,
    off_oreb_cnt   INT,
    off_oreb_opps  INT,
    off_tov_cnt    INT,
    off_fta        INT,
    off_fga_cnt    INT,
    def_ts         NUMERIC,
    def_oreb       NUMERIC,
    def_tov        NUMERIC,
    def_ftr        NUMERIC,
    def_ppp        NUMERIC,
    def_poss       INT,
    def_pts        INT,
    def_ts_poss    INT,
    def_oreb_cnt   INT,
    def_oreb_opps  INT,
    def_tov_cnt    INT,
    def_fta        INT,
    def_fga_cnt    INT,
    net_rtg        NUMERIC
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
  v_margin_status   text;
  v_clutch_active   boolean;
BEGIN
  -- [Input Normalization]
  v_home_away       := COALESCE(NULLIF(btrim(p_home_away), ''), 'all');
  v_outcome         := COALESCE(NULLIF(btrim(p_outcome), ''), 'all');
  v_opp_rank_side   := COALESCE(NULLIF(btrim(p_opp_rank_side), ''), 'all');
  v_opp_rank_metric := COALESCE(NULLIF(btrim(p_opp_rank_metric), ''), 'net');
  v_margin_status   := COALESCE(NULLIF(btrim(p_margin_status), ''), 'all');
  v_clutch_active   := (p_max_margin IS NOT NULL OR v_margin_status <> 'all' OR p_max_time_remaining IS NOT NULL);

  -- Parse CSVs
  IF p_game_type_csv IS NOT NULL AND length(btrim(p_game_type_csv)) > 0 THEN
      v_game_types := ARRAY(SELECT DISTINCT x::int4 FROM unnest(string_to_array(regexp_replace(p_game_type_csv, '\s+', '', 'g'), ',')) x WHERE x <> '' ORDER BY 1);
  END IF;

  IF p_opp_team_ids_csv IS NOT NULL AND length(btrim(p_opp_team_ids_csv)) > 0 THEN
      v_opp_ids := ARRAY(SELECT DISTINCT x::int4 FROM unnest(string_to_array(regexp_replace(p_opp_team_ids_csv, '\s+', '', 'g'), ',')) x WHERE x <> '' ORDER BY 1);
  END IF;

  IF v_clutch_active THEN
  -- ============================================================
  -- CLUTCH PATH: Inline four-factor CASE logic from raw MV
  -- ============================================================
  RETURN QUERY
  WITH
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
  games_filtered AS (
    SELECT gr.game_id, gr.team_id, gr.game_year
    FROM games_ranked gr
    WHERE v_opp_rank_side = 'all' OR p_opp_rank_n IS NULL
       OR (v_opp_rank_side = 'top'    AND gr.opp_rank <= p_opp_rank_n)
       OR (v_opp_rank_side = 'bottom' AND gr.opp_rank >= (gr.max_rank - p_opp_rank_n + 1))
  ),
  -- Clutch-filtered raw data from df_pts_poss_lineups_longer_mv
  -- NOTE: Use pre-shot margin (subtract points scored from current score)
  clean_stats AS (
    SELECT
      d.id, d.game_id, d.team_id, d.team_score, d.type,
      d.parameters_type, d.parameters_made, d.pct_ft,
      d.parent_action_id, d.type_lineup,
      CASE WHEN d.final_end_poss IS TRUE THEN 1 ELSE 0 END AS final_end_flag
    FROM basketball_test.df_pts_poss_lineups_longer_mv d
    JOIN games_filtered gf ON gf.game_id = d.game_id AND gf.team_id = d.team_id
    WHERE (p_max_margin IS NULL
           OR ABS(CASE WHEN d.type_lineup = 'offense'
                       THEN (d.own_team_score - COALESCE(d.team_score, 0)) - d.opp_team_score
                       ELSE d.own_team_score - (d.opp_team_score - COALESCE(d.team_score, 0))
                  END) <= p_max_margin
           OR (d.quarter > 4 AND NOT COALESCE(p_ot_margin_filter, FALSE)))
      AND (v_margin_status = 'all'
           OR (v_margin_status = 'leading'  AND
               CASE WHEN d.type_lineup = 'offense'
                    THEN (d.own_team_score - COALESCE(d.team_score, 0)) > d.opp_team_score
                    ELSE d.own_team_score > (d.opp_team_score - COALESCE(d.team_score, 0))
               END)
           OR (v_margin_status = 'trailing' AND
               CASE WHEN d.type_lineup = 'offense'
                    THEN (d.own_team_score - COALESCE(d.team_score, 0)) < d.opp_team_score
                    ELSE d.own_team_score < (d.opp_team_score - COALESCE(d.team_score, 0))
               END)
           OR (v_margin_status = 'tied'     AND
               CASE WHEN d.type_lineup = 'offense'
                    THEN (d.own_team_score - COALESCE(d.team_score, 0)) = d.opp_team_score
                    ELSE d.own_team_score = (d.opp_team_score - COALESCE(d.team_score, 0))
               END)
           OR (d.quarter > 4 AND NOT COALESCE(p_ot_margin_filter, FALSE)))
      AND (p_max_time_remaining IS NULL OR d.end_game_seconds_remaining <= p_max_time_remaining OR d.quarter > 4)
  ),
  -- complex_flags joins full MV (parent foul may precede clutch window)
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
    ORDER BY d.id
  ),
  combined_data AS (
    SELECT
      cs.team_id,
      cs.game_id,
      p_game_year AS game_year,
      cs.type_lineup,
      cs.team_score,
      cs.final_end_flag,
      cs.type,
      cs.parameters_type,
      cs.parameters_made,
      cs.pct_ft,
      cs.parent_action_id,
      cf.parent_type,
      cf.parent_param
    FROM clean_stats cs
    LEFT JOIN complex_flags cf ON cs.id = cf.main_id
  ),
  team_agg AS (
    SELECT
      cd.team_id,
      cd.game_year,
      cd.type_lineup,
      SUM(cd.team_score)       AS total_points,
      SUM(cd.final_end_flag)   AS total_poss,
      COUNT(CASE WHEN cd.type = 'shot' THEN 1 END)
        + COUNT(DISTINCT CASE
            WHEN cd.type = 'freeThrow'
              AND cd.parent_type = 'foul'
              AND cd.parent_param = 'personal'
            THEN cd.parent_action_id
          END)                 AS ts_poss_count,
      COUNT(CASE WHEN cd.type = 'rebound' AND cd.parameters_type = 'offensive' THEN 1 END) AS oreb_count,
      COUNT(CASE
        WHEN cd.type = 'shot' AND cd.parameters_made IN ('missed', 'blocked') THEN 1
        WHEN cd.type = 'freeThrow' AND cd.parameters_made = 'missed'
          AND cd.pct_ft = 1::numeric
          AND cd.parent_type = 'foul' AND cd.parent_param = 'personal' THEN 1
      END)                     AS oreb_opportunities,
      COUNT(CASE WHEN cd.type = 'turnover' THEN 1 END) AS tov_count,
      COUNT(CASE WHEN cd.type = 'freeThrow' THEN 1 END) AS total_ft_attempts,
      COUNT(CASE WHEN cd.type = 'shot' THEN 1 END) AS total_fga
    FROM combined_data cd
    GROUP BY cd.team_id, cd.game_year, cd.type_lineup
  ),
  pivoted AS (
    SELECT
      ta.team_id, ta.game_year,
      ROUND(SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'offense')::numeric / (2.0 * NULLIF(SUM(ta.ts_poss_count) FILTER (WHERE ta.type_lineup = 'offense'), 0)::numeric) * 100, 1) AS off_ts,
      ROUND(SUM(ta.oreb_count) FILTER (WHERE ta.type_lineup = 'offense')::numeric / NULLIF(SUM(ta.oreb_opportunities) FILTER (WHERE ta.type_lineup = 'offense'), 0)::numeric * 100, 1) AS off_oreb,
      ROUND(SUM(ta.tov_count) FILTER (WHERE ta.type_lineup = 'offense')::numeric / NULLIF(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'offense'), 0)::numeric * 100, 1) AS off_tov,
      ROUND(SUM(ta.total_ft_attempts) FILTER (WHERE ta.type_lineup = 'offense')::numeric / NULLIF(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'offense'), 0)::numeric * 100, 1) AS off_ftr,
      ROUND(SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'offense')::numeric / NULLIF(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'offense'), 0) * 100, 1) AS off_ppp,
      COALESCE(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_poss,
      COALESCE(SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_pts,
      COALESCE(SUM(ta.ts_poss_count) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_ts_poss,
      COALESCE(SUM(ta.oreb_count) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_oreb_cnt,
      COALESCE(SUM(ta.oreb_opportunities) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_oreb_opps,
      COALESCE(SUM(ta.tov_count) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_tov_cnt,
      COALESCE(SUM(ta.total_ft_attempts) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_fta,
      COALESCE(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_fga_cnt,
      ROUND(SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'defense')::numeric / (2.0 * NULLIF(SUM(ta.ts_poss_count) FILTER (WHERE ta.type_lineup = 'defense'), 0)::numeric) * 100, 1) AS def_ts,
      ROUND(SUM(ta.oreb_count) FILTER (WHERE ta.type_lineup = 'defense')::numeric / NULLIF(SUM(ta.oreb_opportunities) FILTER (WHERE ta.type_lineup = 'defense'), 0)::numeric * 100, 1) AS def_oreb,
      ROUND(SUM(ta.tov_count) FILTER (WHERE ta.type_lineup = 'defense')::numeric / NULLIF(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'defense'), 0)::numeric * 100, 1) AS def_tov,
      ROUND(SUM(ta.total_ft_attempts) FILTER (WHERE ta.type_lineup = 'defense')::numeric / NULLIF(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'defense'), 0)::numeric * 100, 1) AS def_ftr,
      ROUND(SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'defense')::numeric / NULLIF(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'defense'), 0) * 100, 1) AS def_ppp,
      COALESCE(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_poss,
      COALESCE(SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_pts,
      COALESCE(SUM(ta.ts_poss_count) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_ts_poss,
      COALESCE(SUM(ta.oreb_count) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_oreb_cnt,
      COALESCE(SUM(ta.oreb_opportunities) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_oreb_opps,
      COALESCE(SUM(ta.tov_count) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_tov_cnt,
      COALESCE(SUM(ta.total_ft_attempts) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_fta,
      COALESCE(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_fga_cnt
    FROM team_agg ta
    GROUP BY ta.team_id, ta.game_year
  ),
  final_calc AS (
    SELECT
      p.team_id, p.game_year, fr.team_name,
      p.off_ts, p.off_oreb, p.off_tov, p.off_ftr, p.off_ppp, p.off_poss,
      p.off_pts, p.off_ts_poss, p.off_oreb_cnt, p.off_oreb_opps, p.off_tov_cnt, p.off_fta, p.off_fga_cnt,
      p.def_ts, p.def_oreb, p.def_tov, p.def_ftr, p.def_ppp, p.def_poss,
      p.def_pts, p.def_ts_poss, p.def_oreb_cnt, p.def_oreb_opps, p.def_tov_cnt, p.def_fta, p.def_fga_cnt,
      ROUND(p.off_ppp - p.def_ppp, 1) AS net_rtg
    FROM pivoted p
    JOIN basketball_test.full_rosters fr
      ON fr.game_year = p.game_year AND fr.team_id = p.team_id
    GROUP BY p.team_id, p.game_year, fr.team_name,
             p.off_ts, p.off_oreb, p.off_tov, p.off_ftr, p.off_ppp, p.off_poss,
             p.off_pts, p.off_ts_poss, p.off_oreb_cnt, p.off_oreb_opps, p.off_tov_cnt, p.off_fta, p.off_fga_cnt,
             p.def_ts, p.def_oreb, p.def_tov, p.def_ftr, p.def_ppp, p.def_poss,
             p.def_pts, p.def_ts_poss, p.def_oreb_cnt, p.def_oreb_opps, p.def_tov_cnt, p.def_fta, p.def_fga_cnt
  )
  SELECT
    fc.team_id, fc.game_year, fc.team_name,
    fc.off_ts, fc.off_oreb, fc.off_tov, fc.off_ftr, fc.off_ppp, fc.off_poss,
    fc.off_pts, fc.off_ts_poss, fc.off_oreb_cnt, fc.off_oreb_opps, fc.off_tov_cnt, fc.off_fta, fc.off_fga_cnt,
    fc.def_ts, fc.def_oreb, fc.def_tov, fc.def_ftr, fc.def_ppp, fc.def_poss,
    fc.def_pts, fc.def_ts_poss, fc.def_oreb_cnt, fc.def_oreb_opps, fc.def_tov_cnt, fc.def_fta, fc.def_fga_cnt,
    fc.net_rtg
  FROM final_calc fc;

  ELSE
  -- ============================================================
  -- NON-CLUTCH PATH: Use pre-aggregated lineup_four_factors_by_game MV
  -- ============================================================
  RETURN QUERY
  WITH
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
  games_filtered AS (
    SELECT gr.game_id, gr.team_id, gr.game_year
    FROM games_ranked gr
    WHERE v_opp_rank_side = 'all' OR p_opp_rank_n IS NULL
       OR (v_opp_rank_side = 'top'    AND gr.opp_rank <= p_opp_rank_n)
       OR (v_opp_rank_side = 'bottom' AND gr.opp_rank >= (gr.max_rank - p_opp_rank_n + 1))
  ),
  team_agg AS (
    SELECT
      gf.team_id,
      gf.game_year,
      lf.type_lineup,
      SUM(lf.total_points)       AS total_points,
      SUM(lf.total_poss)         AS total_poss,
      SUM(lf.ts_poss_count)      AS ts_poss_count,
      SUM(lf.oreb_count)         AS oreb_count,
      SUM(lf.oreb_opportunities) AS oreb_opportunities,
      SUM(lf.tov_count)          AS tov_count,
      SUM(lf.total_ft_attempts)  AS total_ft_attempts,
      SUM(lf.total_fga)          AS total_fga
    FROM basketball_test.lineup_four_factors_by_game lf
    JOIN games_filtered gf ON gf.game_id = lf.game_id AND gf.team_id = lf.team_id
    WHERE lf.game_year = p_game_year
    GROUP BY gf.team_id, gf.game_year, lf.type_lineup
  ),
  pivoted AS (
    SELECT
      ta.team_id, ta.game_year,
      ROUND(SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'offense')::numeric / (2.0 * NULLIF(SUM(ta.ts_poss_count) FILTER (WHERE ta.type_lineup = 'offense'), 0)::numeric) * 100, 1) AS off_ts,
      ROUND(SUM(ta.oreb_count) FILTER (WHERE ta.type_lineup = 'offense')::numeric / NULLIF(SUM(ta.oreb_opportunities) FILTER (WHERE ta.type_lineup = 'offense'), 0)::numeric * 100, 1) AS off_oreb,
      ROUND(SUM(ta.tov_count) FILTER (WHERE ta.type_lineup = 'offense')::numeric / NULLIF(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'offense'), 0)::numeric * 100, 1) AS off_tov,
      ROUND(SUM(ta.total_ft_attempts) FILTER (WHERE ta.type_lineup = 'offense')::numeric / NULLIF(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'offense'), 0)::numeric * 100, 1) AS off_ftr,
      ROUND(SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'offense')::numeric / NULLIF(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'offense'), 0) * 100, 1) AS off_ppp,
      COALESCE(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_poss,
      COALESCE(SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_pts,
      COALESCE(SUM(ta.ts_poss_count) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_ts_poss,
      COALESCE(SUM(ta.oreb_count) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_oreb_cnt,
      COALESCE(SUM(ta.oreb_opportunities) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_oreb_opps,
      COALESCE(SUM(ta.tov_count) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_tov_cnt,
      COALESCE(SUM(ta.total_ft_attempts) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_fta,
      COALESCE(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'offense'), 0)::int4 AS off_fga_cnt,
      ROUND(SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'defense')::numeric / (2.0 * NULLIF(SUM(ta.ts_poss_count) FILTER (WHERE ta.type_lineup = 'defense'), 0)::numeric) * 100, 1) AS def_ts,
      ROUND(SUM(ta.oreb_count) FILTER (WHERE ta.type_lineup = 'defense')::numeric / NULLIF(SUM(ta.oreb_opportunities) FILTER (WHERE ta.type_lineup = 'defense'), 0)::numeric * 100, 1) AS def_oreb,
      ROUND(SUM(ta.tov_count) FILTER (WHERE ta.type_lineup = 'defense')::numeric / NULLIF(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'defense'), 0)::numeric * 100, 1) AS def_tov,
      ROUND(SUM(ta.total_ft_attempts) FILTER (WHERE ta.type_lineup = 'defense')::numeric / NULLIF(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'defense'), 0)::numeric * 100, 1) AS def_ftr,
      ROUND(SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'defense')::numeric / NULLIF(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'defense'), 0) * 100, 1) AS def_ppp,
      COALESCE(SUM(ta.total_poss) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_poss,
      COALESCE(SUM(ta.total_points) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_pts,
      COALESCE(SUM(ta.ts_poss_count) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_ts_poss,
      COALESCE(SUM(ta.oreb_count) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_oreb_cnt,
      COALESCE(SUM(ta.oreb_opportunities) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_oreb_opps,
      COALESCE(SUM(ta.tov_count) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_tov_cnt,
      COALESCE(SUM(ta.total_ft_attempts) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_fta,
      COALESCE(SUM(ta.total_fga) FILTER (WHERE ta.type_lineup = 'defense'), 0)::int4 AS def_fga_cnt
    FROM team_agg ta
    GROUP BY ta.team_id, ta.game_year
  ),
  final_calc AS (
    SELECT
      p.team_id, p.game_year, fr.team_name,
      p.off_ts, p.off_oreb, p.off_tov, p.off_ftr, p.off_ppp, p.off_poss,
      p.off_pts, p.off_ts_poss, p.off_oreb_cnt, p.off_oreb_opps, p.off_tov_cnt, p.off_fta, p.off_fga_cnt,
      p.def_ts, p.def_oreb, p.def_tov, p.def_ftr, p.def_ppp, p.def_poss,
      p.def_pts, p.def_ts_poss, p.def_oreb_cnt, p.def_oreb_opps, p.def_tov_cnt, p.def_fta, p.def_fga_cnt,
      ROUND(p.off_ppp - p.def_ppp, 1) AS net_rtg
    FROM pivoted p
    JOIN basketball_test.full_rosters fr
      ON fr.game_year = p.game_year AND fr.team_id = p.team_id
    GROUP BY p.team_id, p.game_year, fr.team_name,
             p.off_ts, p.off_oreb, p.off_tov, p.off_ftr, p.off_ppp, p.off_poss,
             p.off_pts, p.off_ts_poss, p.off_oreb_cnt, p.off_oreb_opps, p.off_tov_cnt, p.off_fta, p.off_fga_cnt,
             p.def_ts, p.def_oreb, p.def_tov, p.def_ftr, p.def_ppp, p.def_poss,
             p.def_pts, p.def_ts_poss, p.def_oreb_cnt, p.def_oreb_opps, p.def_tov_cnt, p.def_fta, p.def_fga_cnt
  )
  SELECT
    fc.team_id, fc.game_year, fc.team_name,
    fc.off_ts, fc.off_oreb, fc.off_tov, fc.off_ftr, fc.off_ppp, fc.off_poss,
    fc.off_pts, fc.off_ts_poss, fc.off_oreb_cnt, fc.off_oreb_opps, fc.off_tov_cnt, fc.off_fta, fc.off_fga_cnt,
    fc.def_ts, fc.def_oreb, fc.def_tov, fc.def_ftr, fc.def_ppp, fc.def_poss,
    fc.def_pts, fc.def_ts_poss, fc.def_oreb_cnt, fc.def_oreb_opps, fc.def_tov_cnt, fc.def_fta, fc.def_fga_cnt,
    fc.net_rtg
  FROM final_calc fc;

  END IF;
END;
$$;
