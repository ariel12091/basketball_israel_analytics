-- fetch_lineups_four_factors: array-based inner function
-- fetch_lineups_four_factors_csv: CSV wrapper (called from the Shiny app)

DROP FUNCTION IF EXISTS basketball_test.fetch_lineups_four_factors(int2, _int4, _int4, _int4, bool, date, date, int4, int4, text, text, text, text, text, int4, text, int4, text, int4);

CREATE OR REPLACE FUNCTION basketball_test.fetch_lineups_four_factors(
  p_num_lineup      SMALLINT,
  p_team_ids        INT[]    DEFAULT NULL,
  p_player_ids      INT[]    DEFAULT NULL,
  p_player_off_ids  INT[]    DEFAULT NULL,
  p_exact           BOOLEAN  DEFAULT TRUE,
  p_start_date      DATE     DEFAULT NULL,
  p_end_date        DATE     DEFAULT NULL,
  p_min_poss        INT      DEFAULT 20,
  p_game_year       INT      DEFAULT NULL,
  p_game_type_csv   TEXT     DEFAULT NULL,
  p_opp_team_ids_csv TEXT    DEFAULT NULL,
  p_home_away       TEXT     DEFAULT 'all',
  p_outcome         TEXT     DEFAULT 'all',
  p_opp_rank_side   TEXT     DEFAULT 'all',
  p_opp_rank_n      INT      DEFAULT NULL,
  p_opp_rank_metric TEXT     DEFAULT 'net',
  p_max_margin      INT      DEFAULT NULL,
  p_margin_status   TEXT     DEFAULT 'all',
  p_max_time_remaining INT   DEFAULT NULL,
  p_ot_margin_filter BOOLEAN DEFAULT FALSE
)
RETURNS TABLE (
  team_id           INT,
  sub_lineup_hash   TEXT,
  num_lineup        SMALLINT,
  player_ids        INT[],
  player_names      TEXT[],
  player_names_str  TEXT,
  off_ts            NUMERIC,
  off_oreb          NUMERIC,
  off_tov           NUMERIC,
  off_ftr           NUMERIC,
  off_poss          INT,
  off_pts           INT,
  off_ppp           NUMERIC,
  def_ts            NUMERIC,
  def_oreb          NUMERIC,
  def_tov           NUMERIC,
  def_ftr           NUMERIC,
  def_poss          INT,
  def_pts           INT,
  def_ppp           NUMERIC,
  net_rtg           NUMERIC,
  -- Raw counts for client-side TOTAL row aggregation
  off_ts_poss       INT,
  off_oreb_cnt      INT,
  off_oreb_opps     INT,
  off_tov_cnt       INT,
  off_fta           INT,
  off_fga_cnt       INT,
  def_ts_poss       INT,
  def_oreb_cnt      INT,
  def_oreb_opps     INT,
  def_tov_cnt       INT,
  def_fta           INT,
  def_fga_cnt       INT,
  game_year         INT
)
LANGUAGE plpgsql
STABLE
AS $function$
DECLARE
  v_ids_norm  int4[];
  v_sel_cnt   int;
  v_off_norm  int4[];
  v_game_types int4[];
  v_opp_ids    int4[];
  v_home_away       text;
  v_outcome         text;
  v_opp_rank_side   text;
  v_opp_rank_metric text;
  v_margin_status   text;
  v_clutch_active   boolean;
BEGIN
  -- [Input Normalization]
  IF p_player_ids IS NOT NULL THEN
    SELECT ARRAY(SELECT DISTINCT x FROM unnest(p_player_ids) x ORDER BY x), cardinality(p_player_ids) INTO v_ids_norm, v_sel_cnt;
  ELSE
    v_ids_norm := NULL; v_sel_cnt := 0;
  END IF;

  IF p_player_off_ids IS NOT NULL THEN
    SELECT ARRAY(SELECT DISTINCT x FROM unnest(p_player_off_ids) x ORDER BY x) INTO v_off_norm;
  ELSE
    v_off_norm := NULL;
  END IF;

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
    SELECT fs.game_id, fs.team_id, fs.game_year, fs.opp_team_id, fs.is_home, fs.has_won
    FROM basketball_test.final_schedule_mv fs
    WHERE (p_game_year IS NULL OR fs.game_year = p_game_year)
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
  sub_identity AS (
    SELECT s.team_id, s.game_year, s.sub_lineup_hash, s.player_ids, s.num_lineup, s.lineup_hash
    FROM basketball_test.sub_lineups s
    WHERE p_num_lineup IN (2,3,4) AND s.num_lineup = p_num_lineup
      AND (p_team_ids IS NULL OR s.team_id = ANY(p_team_ids))
      AND (p_game_year IS NULL OR s.game_year = p_game_year)
      AND (v_ids_norm IS NULL OR
           CASE WHEN NOT p_exact THEN s.player_ids @> v_ids_norm
                WHEN v_sel_cnt = s.num_lineup THEN s.player_ids @> v_ids_norm AND s.player_ids <@ v_ids_norm
                WHEN v_sel_cnt < s.num_lineup THEN s.player_ids @> v_ids_norm
                ELSE FALSE END)
      AND (v_off_norm IS NULL OR NOT (s.player_ids && v_off_norm))
    UNION ALL
    SELECT l.team_id, l.game_year, l.lineup_hash::text AS sub_lineup_hash,
           ARRAY_AGG(DISTINCT l.player_id ORDER BY l.player_id)::int4[] AS player_ids,
           5::int2 AS num_lineup, l.lineup_hash
    FROM basketball_test.lineups_lookup_on l
    WHERE p_num_lineup = 5
      AND (p_team_ids IS NULL OR l.team_id = ANY(p_team_ids))
      AND (p_game_year IS NULL OR l.game_year = p_game_year)
    GROUP BY l.team_id, l.game_year, l.lineup_hash
    HAVING cardinality(ARRAY_AGG(DISTINCT l.player_id)) = 5
      AND (v_ids_norm IS NULL OR
           CASE WHEN NOT p_exact THEN ARRAY_AGG(l.player_id) @> v_ids_norm
                WHEN cardinality(v_ids_norm) = 5 THEN ARRAY_AGG(l.player_id) @> v_ids_norm AND ARRAY_AGG(l.player_id) <@ v_ids_norm
                WHEN cardinality(v_ids_norm) < 5 THEN ARRAY_AGG(l.player_id) @> v_ids_norm
                ELSE FALSE END)
      AND (v_off_norm IS NULL OR NOT (ARRAY_AGG(l.player_id) && v_off_norm))
  ),
  -- Clutch-filtered raw data from df_pts_poss_lineups_longer_mv
  clean_stats AS (
    SELECT
      d.id, d.game_id, d.lineup_hash, d.team_id, d.team_score, d.type,
      d.parameters_type, d.parameters_made, d.pct_ft,
      d.parent_action_id, d.type_lineup,
      CASE WHEN d.final_end_poss IS TRUE THEN 1 ELSE 0 END AS final_end_flag
    FROM basketball_test.df_pts_poss_lineups_longer_mv d
    JOIN games_filtered gf ON gf.game_id = d.game_id AND gf.team_id = d.team_id
    WHERE (p_game_year IS NULL OR gf.game_year = p_game_year)
      AND (p_max_margin IS NULL OR ABS(d.own_team_score - d.opp_team_score) <= p_max_margin OR (d.quarter > 4 AND NOT COALESCE(p_ot_margin_filter, FALSE)))
      AND (v_margin_status = 'all'
           OR (v_margin_status = 'leading'  AND d.own_team_score > d.opp_team_score)
           OR (v_margin_status = 'trailing' AND d.own_team_score < d.opp_team_score)
           OR (v_margin_status = 'tied'     AND d.own_team_score = d.opp_team_score)
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
      cs.lineup_hash,
      cs.team_id,
      cs.game_id,
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
  lineup_ff AS (
    SELECT
      cd.lineup_hash::text AS lineup_hash,
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
    GROUP BY cd.lineup_hash, cd.type_lineup
  )
  SELECT
    si.team_id, si.sub_lineup_hash, si.num_lineup, si.player_ids, sls.player_names, sls.player_names_str,
    ROUND(SUM(cr.total_points) FILTER (WHERE cr.type_lineup = 'offense')::numeric / (2.0 * NULLIF(SUM(cr.ts_poss_count) FILTER (WHERE cr.type_lineup = 'offense'), 0)::numeric) * 100, 1) AS off_ts,
    ROUND(SUM(cr.oreb_count) FILTER (WHERE cr.type_lineup = 'offense')::numeric / NULLIF(SUM(cr.oreb_opportunities) FILTER (WHERE cr.type_lineup = 'offense'), 0)::numeric * 100, 1) AS off_oreb,
    ROUND(SUM(cr.tov_count) FILTER (WHERE cr.type_lineup = 'offense')::numeric / NULLIF(SUM(cr.total_poss) FILTER (WHERE cr.type_lineup = 'offense'), 0)::numeric * 100, 1) AS off_tov,
    ROUND(SUM(cr.total_ft_attempts) FILTER (WHERE cr.type_lineup = 'offense')::numeric / NULLIF(SUM(cr.total_fga) FILTER (WHERE cr.type_lineup = 'offense'), 0)::numeric * 100, 1) AS off_ftr,
    COALESCE(SUM(cr.total_poss) FILTER (WHERE cr.type_lineup = 'offense'), 0)::int4 AS off_poss,
    COALESCE(SUM(cr.total_points) FILTER (WHERE cr.type_lineup = 'offense'), 0)::int4 AS off_pts,
    ROUND(NULLIF(SUM(cr.total_points) FILTER (WHERE cr.type_lineup = 'offense'), 0)::numeric / NULLIF(SUM(cr.total_poss) FILTER (WHERE cr.type_lineup = 'offense'), 0) * 100, 1) AS off_ppp,
    ROUND(SUM(cr.total_points) FILTER (WHERE cr.type_lineup = 'defense')::numeric / (2.0 * NULLIF(SUM(cr.ts_poss_count) FILTER (WHERE cr.type_lineup = 'defense'), 0)::numeric) * 100, 1) AS def_ts,
    ROUND(SUM(cr.oreb_count) FILTER (WHERE cr.type_lineup = 'defense')::numeric / NULLIF(SUM(cr.oreb_opportunities) FILTER (WHERE cr.type_lineup = 'defense'), 0)::numeric * 100, 1) AS def_oreb,
    ROUND(SUM(cr.tov_count) FILTER (WHERE cr.type_lineup = 'defense')::numeric / NULLIF(SUM(cr.total_poss) FILTER (WHERE cr.type_lineup = 'defense'), 0)::numeric * 100, 1) AS def_tov,
    ROUND(SUM(cr.total_ft_attempts) FILTER (WHERE cr.type_lineup = 'defense')::numeric / NULLIF(SUM(cr.total_fga) FILTER (WHERE cr.type_lineup = 'defense'), 0)::numeric * 100, 1) AS def_ftr,
    COALESCE(SUM(cr.total_poss) FILTER (WHERE cr.type_lineup = 'defense'), 0)::int4 AS def_poss,
    COALESCE(SUM(cr.total_points) FILTER (WHERE cr.type_lineup = 'defense'), 0)::int4 AS def_pts,
    ROUND(NULLIF(SUM(cr.total_points) FILTER (WHERE cr.type_lineup = 'defense'), 0)::numeric / NULLIF(SUM(cr.total_poss) FILTER (WHERE cr.type_lineup = 'defense'), 0) * 100, 1) AS def_ppp,
    ROUND(
      (NULLIF(SUM(cr.total_points) FILTER (WHERE cr.type_lineup = 'offense'), 0)::numeric / NULLIF(SUM(cr.total_poss) FILTER (WHERE cr.type_lineup = 'offense'), 0) * 100) -
      (NULLIF(SUM(cr.total_points) FILTER (WHERE cr.type_lineup = 'defense'), 0)::numeric / NULLIF(SUM(cr.total_poss) FILTER (WHERE cr.type_lineup = 'defense'), 0) * 100)
    , 1) AS net_rtg,
    COALESCE(SUM(cr.ts_poss_count) FILTER (WHERE cr.type_lineup = 'offense'), 0)::int4 AS off_ts_poss,
    COALESCE(SUM(cr.oreb_count) FILTER (WHERE cr.type_lineup = 'offense'), 0)::int4 AS off_oreb_cnt,
    COALESCE(SUM(cr.oreb_opportunities) FILTER (WHERE cr.type_lineup = 'offense'), 0)::int4 AS off_oreb_opps,
    COALESCE(SUM(cr.tov_count) FILTER (WHERE cr.type_lineup = 'offense'), 0)::int4 AS off_tov_cnt,
    COALESCE(SUM(cr.total_ft_attempts) FILTER (WHERE cr.type_lineup = 'offense'), 0)::int4 AS off_fta,
    COALESCE(SUM(cr.total_fga) FILTER (WHERE cr.type_lineup = 'offense'), 0)::int4 AS off_fga_cnt,
    COALESCE(SUM(cr.ts_poss_count) FILTER (WHERE cr.type_lineup = 'defense'), 0)::int4 AS def_ts_poss,
    COALESCE(SUM(cr.oreb_count) FILTER (WHERE cr.type_lineup = 'defense'), 0)::int4 AS def_oreb_cnt,
    COALESCE(SUM(cr.oreb_opportunities) FILTER (WHERE cr.type_lineup = 'defense'), 0)::int4 AS def_oreb_opps,
    COALESCE(SUM(cr.tov_count) FILTER (WHERE cr.type_lineup = 'defense'), 0)::int4 AS def_tov_cnt,
    COALESCE(SUM(cr.total_ft_attempts) FILTER (WHERE cr.type_lineup = 'defense'), 0)::int4 AS def_fta,
    COALESCE(SUM(cr.total_fga) FILTER (WHERE cr.type_lineup = 'defense'), 0)::int4 AS def_fga_cnt,
    si.game_year
  FROM sub_identity si
  JOIN lineup_ff cr ON cr.lineup_hash = si.lineup_hash::text
  LEFT JOIN basketball_test.sub_lineups_stats sls
    ON sls.team_id = si.team_id AND sls.sub_lineup_hash::text = si.sub_lineup_hash::text AND sls.game_year = si.game_year
  GROUP BY si.team_id, si.sub_lineup_hash, si.num_lineup, si.player_ids, sls.player_names, sls.player_names_str, si.game_year
  HAVING (COALESCE(SUM(cr.total_poss), 0)) >= p_min_poss;

  ELSE
  -- ============================================================
  -- NON-CLUTCH PATH: Use pre-aggregated lineup_four_factors_by_game MV
  -- ============================================================
  RETURN QUERY
  WITH
  games_base AS (
    SELECT fs.game_id, fs.team_id, fs.game_year, fs.opp_team_id, fs.is_home, fs.has_won
    FROM basketball_test.final_schedule_mv fs
    WHERE (p_game_year IS NULL OR fs.game_year = p_game_year)
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
  sub_identity AS (
    SELECT s.team_id, s.game_year, s.sub_lineup_hash, s.player_ids, s.num_lineup, s.lineup_hash
    FROM basketball_test.sub_lineups s
    WHERE p_num_lineup IN (2,3,4) AND s.num_lineup = p_num_lineup
      AND (p_team_ids IS NULL OR s.team_id = ANY(p_team_ids))
      AND (p_game_year IS NULL OR s.game_year = p_game_year)
      AND (v_ids_norm IS NULL OR
           CASE WHEN NOT p_exact THEN s.player_ids @> v_ids_norm
                WHEN v_sel_cnt = s.num_lineup THEN s.player_ids @> v_ids_norm AND s.player_ids <@ v_ids_norm
                WHEN v_sel_cnt < s.num_lineup THEN s.player_ids @> v_ids_norm
                ELSE FALSE END)
      AND (v_off_norm IS NULL OR NOT (s.player_ids && v_off_norm))
    UNION ALL
    SELECT l.team_id, l.game_year, l.lineup_hash::text AS sub_lineup_hash,
           ARRAY_AGG(DISTINCT l.player_id ORDER BY l.player_id)::int4[] AS player_ids,
           5::int2 AS num_lineup, l.lineup_hash
    FROM basketball_test.lineups_lookup_on l
    WHERE p_num_lineup = 5
      AND (p_team_ids IS NULL OR l.team_id = ANY(p_team_ids))
      AND (p_game_year IS NULL OR l.game_year = p_game_year)
    GROUP BY l.team_id, l.game_year, l.lineup_hash
    HAVING cardinality(ARRAY_AGG(DISTINCT l.player_id)) = 5
      AND (v_ids_norm IS NULL OR
           CASE WHEN NOT p_exact THEN ARRAY_AGG(l.player_id) @> v_ids_norm
                WHEN cardinality(v_ids_norm) = 5 THEN ARRAY_AGG(l.player_id) @> v_ids_norm AND ARRAY_AGG(l.player_id) <@ v_ids_norm
                WHEN cardinality(v_ids_norm) < 5 THEN ARRAY_AGG(l.player_id) @> v_ids_norm
                ELSE FALSE END)
      AND (v_off_norm IS NULL OR NOT (ARRAY_AGG(l.player_id) && v_off_norm))
  ),
  lineup_ff AS (
    SELECT lf.lineup_hash, lf.type_lineup,
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
    WHERE (p_game_year IS NULL OR lf.game_year = p_game_year)
    GROUP BY lf.lineup_hash, lf.type_lineup
  )
  SELECT
    si.team_id, si.sub_lineup_hash, si.num_lineup, si.player_ids, sls.player_names, sls.player_names_str,
    ROUND(SUM(cr.total_points) FILTER (WHERE cr.type_lineup = 'offense')::numeric / (2.0 * NULLIF(SUM(cr.ts_poss_count) FILTER (WHERE cr.type_lineup = 'offense'), 0)::numeric) * 100, 1) AS off_ts,
    ROUND(SUM(cr.oreb_count) FILTER (WHERE cr.type_lineup = 'offense')::numeric / NULLIF(SUM(cr.oreb_opportunities) FILTER (WHERE cr.type_lineup = 'offense'), 0)::numeric * 100, 1) AS off_oreb,
    ROUND(SUM(cr.tov_count) FILTER (WHERE cr.type_lineup = 'offense')::numeric / NULLIF(SUM(cr.total_poss) FILTER (WHERE cr.type_lineup = 'offense'), 0)::numeric * 100, 1) AS off_tov,
    ROUND(SUM(cr.total_ft_attempts) FILTER (WHERE cr.type_lineup = 'offense')::numeric / NULLIF(SUM(cr.total_fga) FILTER (WHERE cr.type_lineup = 'offense'), 0)::numeric * 100, 1) AS off_ftr,
    COALESCE(SUM(cr.total_poss) FILTER (WHERE cr.type_lineup = 'offense'), 0)::int4 AS off_poss,
    COALESCE(SUM(cr.total_points) FILTER (WHERE cr.type_lineup = 'offense'), 0)::int4 AS off_pts,
    ROUND(NULLIF(SUM(cr.total_points) FILTER (WHERE cr.type_lineup = 'offense'), 0)::numeric / NULLIF(SUM(cr.total_poss) FILTER (WHERE cr.type_lineup = 'offense'), 0) * 100, 1) AS off_ppp,
    ROUND(SUM(cr.total_points) FILTER (WHERE cr.type_lineup = 'defense')::numeric / (2.0 * NULLIF(SUM(cr.ts_poss_count) FILTER (WHERE cr.type_lineup = 'defense'), 0)::numeric) * 100, 1) AS def_ts,
    ROUND(SUM(cr.oreb_count) FILTER (WHERE cr.type_lineup = 'defense')::numeric / NULLIF(SUM(cr.oreb_opportunities) FILTER (WHERE cr.type_lineup = 'defense'), 0)::numeric * 100, 1) AS def_oreb,
    ROUND(SUM(cr.tov_count) FILTER (WHERE cr.type_lineup = 'defense')::numeric / NULLIF(SUM(cr.total_poss) FILTER (WHERE cr.type_lineup = 'defense'), 0)::numeric * 100, 1) AS def_tov,
    ROUND(SUM(cr.total_ft_attempts) FILTER (WHERE cr.type_lineup = 'defense')::numeric / NULLIF(SUM(cr.total_fga) FILTER (WHERE cr.type_lineup = 'defense'), 0)::numeric * 100, 1) AS def_ftr,
    COALESCE(SUM(cr.total_poss) FILTER (WHERE cr.type_lineup = 'defense'), 0)::int4 AS def_poss,
    COALESCE(SUM(cr.total_points) FILTER (WHERE cr.type_lineup = 'defense'), 0)::int4 AS def_pts,
    ROUND(NULLIF(SUM(cr.total_points) FILTER (WHERE cr.type_lineup = 'defense'), 0)::numeric / NULLIF(SUM(cr.total_poss) FILTER (WHERE cr.type_lineup = 'defense'), 0) * 100, 1) AS def_ppp,
    ROUND(
      (NULLIF(SUM(cr.total_points) FILTER (WHERE cr.type_lineup = 'offense'), 0)::numeric / NULLIF(SUM(cr.total_poss) FILTER (WHERE cr.type_lineup = 'offense'), 0) * 100) -
      (NULLIF(SUM(cr.total_points) FILTER (WHERE cr.type_lineup = 'defense'), 0)::numeric / NULLIF(SUM(cr.total_poss) FILTER (WHERE cr.type_lineup = 'defense'), 0) * 100)
    , 1) AS net_rtg,
    COALESCE(SUM(cr.ts_poss_count) FILTER (WHERE cr.type_lineup = 'offense'), 0)::int4 AS off_ts_poss,
    COALESCE(SUM(cr.oreb_count) FILTER (WHERE cr.type_lineup = 'offense'), 0)::int4 AS off_oreb_cnt,
    COALESCE(SUM(cr.oreb_opportunities) FILTER (WHERE cr.type_lineup = 'offense'), 0)::int4 AS off_oreb_opps,
    COALESCE(SUM(cr.tov_count) FILTER (WHERE cr.type_lineup = 'offense'), 0)::int4 AS off_tov_cnt,
    COALESCE(SUM(cr.total_ft_attempts) FILTER (WHERE cr.type_lineup = 'offense'), 0)::int4 AS off_fta,
    COALESCE(SUM(cr.total_fga) FILTER (WHERE cr.type_lineup = 'offense'), 0)::int4 AS off_fga_cnt,
    COALESCE(SUM(cr.ts_poss_count) FILTER (WHERE cr.type_lineup = 'defense'), 0)::int4 AS def_ts_poss,
    COALESCE(SUM(cr.oreb_count) FILTER (WHERE cr.type_lineup = 'defense'), 0)::int4 AS def_oreb_cnt,
    COALESCE(SUM(cr.oreb_opportunities) FILTER (WHERE cr.type_lineup = 'defense'), 0)::int4 AS def_oreb_opps,
    COALESCE(SUM(cr.tov_count) FILTER (WHERE cr.type_lineup = 'defense'), 0)::int4 AS def_tov_cnt,
    COALESCE(SUM(cr.total_ft_attempts) FILTER (WHERE cr.type_lineup = 'defense'), 0)::int4 AS def_fta,
    COALESCE(SUM(cr.total_fga) FILTER (WHERE cr.type_lineup = 'defense'), 0)::int4 AS def_fga_cnt,
    si.game_year
  FROM sub_identity si
  JOIN lineup_ff cr ON cr.lineup_hash = si.lineup_hash::text
  LEFT JOIN basketball_test.sub_lineups_stats sls
    ON sls.team_id = si.team_id AND sls.sub_lineup_hash::text = si.sub_lineup_hash::text AND sls.game_year = si.game_year
  GROUP BY si.team_id, si.sub_lineup_hash, si.num_lineup, si.player_ids, sls.player_names, sls.player_names_str, si.game_year
  HAVING (COALESCE(SUM(cr.total_poss), 0)) >= p_min_poss;

  END IF;
END;
$function$;


-- CSV wrapper function (called from the Shiny app)
DROP FUNCTION IF EXISTS basketball_test.fetch_lineups_four_factors_csv(int4, text, text, text, bool, date, date, int4, int4, text, text, text, text, text, int4, text, int4, text, int4);

CREATE OR REPLACE FUNCTION basketball_test.fetch_lineups_four_factors_csv(
  p_num_lineup      INT,
  p_team_ids_csv    TEXT,
  p_player_ids_csv  TEXT,
  p_player_off_csv  TEXT,
  p_exact           BOOLEAN,
  p_start_date      DATE,
  p_end_date        DATE,
  p_min_poss        INT,
  p_game_year       INT      DEFAULT NULL,
  p_game_type_csv   TEXT     DEFAULT NULL,
  p_opp_team_ids_csv TEXT    DEFAULT NULL,
  p_home_away       TEXT     DEFAULT 'all',
  p_outcome         TEXT     DEFAULT 'all',
  p_opp_rank_side   TEXT     DEFAULT 'all',
  p_opp_rank_n      INT      DEFAULT NULL,
  p_opp_rank_metric TEXT     DEFAULT 'net',
  p_max_margin      INT      DEFAULT NULL,
  p_margin_status   TEXT     DEFAULT 'all',
  p_max_time_remaining INT   DEFAULT NULL,
  p_ot_margin_filter BOOLEAN DEFAULT FALSE
)
RETURNS TABLE (
  team_id           INT,
  sub_lineup_hash   TEXT,
  num_lineup        SMALLINT,
  player_ids        INT[],
  player_names      TEXT[],
  player_names_str  TEXT,
  off_ts            NUMERIC,
  off_oreb          NUMERIC,
  off_tov           NUMERIC,
  off_ftr           NUMERIC,
  off_poss          INT,
  off_pts           INT,
  off_ppp           NUMERIC,
  def_ts            NUMERIC,
  def_oreb          NUMERIC,
  def_tov           NUMERIC,
  def_ftr           NUMERIC,
  def_poss          INT,
  def_pts           INT,
  def_ppp           NUMERIC,
  net_rtg           NUMERIC,
  off_ts_poss       INT,
  off_oreb_cnt      INT,
  off_oreb_opps     INT,
  off_tov_cnt       INT,
  off_fta           INT,
  off_fga_cnt       INT,
  def_ts_poss       INT,
  def_oreb_cnt      INT,
  def_oreb_opps     INT,
  def_tov_cnt       INT,
  def_fta           INT,
  def_fga_cnt       INT,
  game_year         INT
)
LANGUAGE plpgsql
STABLE
AS $function$
DECLARE
  v_team_ids   int4[];
  v_player_ids int4[];
  v_off_ids    int4[];
BEGIN
  v_team_ids :=
    CASE
      WHEN p_team_ids_csv IS NULL OR length(btrim(p_team_ids_csv)) = 0 THEN NULL
      ELSE ARRAY(
        SELECT DISTINCT x::int4
        FROM unnest(string_to_array(regexp_replace(p_team_ids_csv, '\s+', '', 'g'), ',')) AS x
        WHERE x <> ''
        ORDER BY 1
      )
    END;

  v_player_ids :=
    CASE
      WHEN p_player_ids_csv IS NULL OR length(btrim(p_player_ids_csv)) = 0 THEN NULL
      ELSE ARRAY(
        SELECT DISTINCT x::int4
        FROM unnest(string_to_array(regexp_replace(p_player_ids_csv, '\s+', '', 'g'), ',')) AS x
        WHERE x <> ''
        ORDER BY 1
      )
    END;

  v_off_ids :=
    CASE
      WHEN p_player_off_csv IS NULL OR length(btrim(p_player_off_csv)) = 0 THEN NULL
      ELSE ARRAY(
        SELECT DISTINCT x::int4
        FROM unnest(string_to_array(regexp_replace(p_player_off_csv, '\s+', '', 'g'), ',')) AS x
        WHERE x <> ''
        ORDER BY 1
      )
    END;

  RETURN QUERY
  SELECT *
  FROM basketball_test.fetch_lineups_four_factors(
    p_num_lineup::int2,
    v_team_ids,
    v_player_ids,
    v_off_ids,
    p_exact,
    p_start_date,
    p_end_date,
    p_min_poss::int4,
    p_game_year,
    p_game_type_csv,
    p_opp_team_ids_csv,
    p_home_away,
    p_outcome,
    p_opp_rank_side,
    p_opp_rank_n,
    p_opp_rank_metric,
    p_max_margin,
    p_margin_status,
    p_max_time_remaining,
    p_ot_margin_filter
  );
END;
$function$;
