-- DROP FUNCTION basketball_test.fetch_lineups_all(int2, _int4, _int4, _int4, bool, date, date, int4, int4, text, text, text, text, text, int4, text);

CREATE OR REPLACE FUNCTION basketball_test.fetch_lineups_all(p_num_lineup smallint, p_team_ids integer[] DEFAULT NULL::integer[], p_player_ids integer[] DEFAULT NULL::integer[], p_player_off_ids integer[] DEFAULT NULL::integer[], p_exact boolean DEFAULT true, p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date, p_min_poss integer DEFAULT 20, p_game_year integer DEFAULT NULL::integer, p_game_type_csv text DEFAULT NULL::text, p_opp_team_ids_csv text DEFAULT NULL::text, p_home_away text DEFAULT 'all'::text, p_outcome text DEFAULT 'all'::text, p_opp_rank_side text DEFAULT 'all'::text, p_opp_rank_n integer DEFAULT NULL::integer, p_opp_rank_metric text DEFAULT 'net'::text, p_max_margin integer DEFAULT NULL::integer, p_margin_status text DEFAULT 'all'::text, p_max_time_remaining integer DEFAULT NULL::integer, p_ot_margin_filter boolean DEFAULT false)
 RETURNS TABLE(team_id integer, sub_lineup_hash text, num_lineup smallint, player_ids integer[], player_names text[], player_names_str text, off_poss integer, off_pts integer, off_ppp numeric, def_poss integer, def_pts integer, def_ppp numeric, net_rtg numeric, minutes numeric, game_year integer)
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
  v_use_fast_path boolean;
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

  -- Fast Path Check
  v_use_fast_path := (p_start_date IS NULL AND p_end_date IS NULL)
    AND v_game_types IS NULL AND v_opp_ids IS NULL
    AND v_home_away = 'all' AND v_outcome = 'all'
    AND (v_opp_rank_side = 'all' OR p_opp_rank_n IS NULL)
    AND NOT v_clutch_active;

  -- 1) Fast path
  IF v_use_fast_path THEN
    RETURN QUERY
    SELECT
      s.team_id, s.sub_lineup_hash::text, s.num_lineup, s.player_ids, s.player_names, s.player_names_str,
      s.off_poss, s.off_pts, s.off_ppp, s.def_poss, s.def_pts, s.def_ppp,
      ROUND(s.off_ppp - s.def_ppp, 1) AS net_rtg, s.minutes, s.game_year
    FROM basketball_test.sub_lineups_stats s
    WHERE s.num_lineup = p_num_lineup
      AND (p_team_ids  IS NULL OR s.team_id   = ANY(p_team_ids))
      AND (p_game_year IS NULL OR s.game_year = p_game_year)
      AND (v_ids_norm IS NULL OR
           CASE WHEN NOT p_exact THEN s.player_ids @> v_ids_norm
                WHEN v_sel_cnt = s.num_lineup THEN s.player_ids @> v_ids_norm AND s.player_ids <@ v_ids_norm
                WHEN v_sel_cnt < s.num_lineup THEN s.player_ids @> v_ids_norm
                ELSE FALSE END)
      AND (v_off_norm IS NULL OR NOT (s.player_ids && v_off_norm))
      AND (COALESCE(s.off_poss,0) + COALESCE(s.def_poss,0)) >= p_min_poss;
    RETURN;
  END IF;

  -- 2) Filtered Path
  IF v_clutch_active THEN
  -- ============================================================
  -- CLUTCH PATH: Use raw df_pts_poss_lineups_longer_mv
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
  -- Clutch: aggregate from raw MV with clutch WHERE
  -- NOTE: Use pre-shot margin (subtract points scored from current score)
  -- Filter clutch-qualifying actions first
  clutch_actions AS (
    SELECT d.team_id, d.game_id, gf.game_year, d.lineup_hash::text AS lineup_hash,
           d.type_lineup, d.segment_id, d.end_game_seconds_remaining,
           CASE WHEN d.final_end_poss IS TRUE THEN 1 ELSE 0 END AS final_end_flag,
           d.team_score
    FROM basketball_test.df_pts_poss_lineups_longer_mv d
    JOIN games_filtered gf ON gf.game_id = d.game_id AND gf.team_id = d.team_id
    WHERE (p_game_year IS NULL OR gf.game_year = p_game_year)
      AND (p_max_margin IS NULL
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
  -- Stint duration per segment (no type_lineup - captures full floor time)
  segment_times AS (
    SELECT ca.team_id, ca.game_year, ca.lineup_hash, ca.game_id, ca.segment_id,
           MAX(ca.end_game_seconds_remaining) - MIN(ca.end_game_seconds_remaining) AS stint_seconds
    FROM clutch_actions ca
    GROUP BY ca.team_id, ca.game_year, ca.lineup_hash, ca.game_id, ca.segment_id
  ),
  -- Poss/pts per segment per type_lineup
  segment_stats AS (
    SELECT ca.team_id, ca.game_year, ca.lineup_hash, ca.game_id, ca.type_lineup, ca.segment_id,
           SUM(ca.final_end_flag) AS total_poss,
           SUM(ca.team_score) AS total_pts
    FROM clutch_actions ca
    GROUP BY ca.team_id, ca.game_year, ca.lineup_hash, ca.game_id, ca.type_lineup, ca.segment_id
  ),
  lineup_totals AS (
    SELECT ss.team_id, ss.game_year, ss.lineup_hash, ss.type_lineup,
           SUM(ss.total_poss) AS total_poss,
           SUM(ss.total_pts) AS total_pts,
           -- Minutes from segment_times, count once per segment (use offense filter)
           SUM(st.stint_seconds) FILTER (WHERE ss.type_lineup = 'offense') / 60.0 AS minutes
    FROM segment_stats ss
    JOIN segment_times st
      ON st.team_id = ss.team_id
      AND st.game_year = ss.game_year
      AND st.lineup_hash = ss.lineup_hash
      AND st.game_id = ss.game_id
      AND st.segment_id = ss.segment_id
    GROUP BY ss.team_id, ss.game_year, ss.lineup_hash, ss.type_lineup
  )
  SELECT
    si.team_id, si.sub_lineup_hash, si.num_lineup, si.player_ids, sls.player_names, sls.player_names_str,
    COALESCE(SUM(lt.total_poss) FILTER (WHERE lt.type_lineup='offense'), 0)::int4,
    COALESCE(SUM(lt.total_pts)  FILTER (WHERE lt.type_lineup='offense'), 0)::int4,
    ROUND(NULLIF(SUM(lt.total_pts) FILTER (WHERE lt.type_lineup='offense'),0)::numeric / NULLIF(SUM(lt.total_poss) FILTER (WHERE lt.type_lineup='offense'),0)*100, 1),
    COALESCE(SUM(lt.total_poss) FILTER (WHERE lt.type_lineup='defense'), 0)::int4,
    COALESCE(SUM(lt.total_pts)  FILTER (WHERE lt.type_lineup='defense'), 0)::int4,
    ROUND(NULLIF(SUM(lt.total_pts) FILTER (WHERE lt.type_lineup='defense'),0)::numeric / NULLIF(SUM(lt.total_poss) FILTER (WHERE lt.type_lineup='defense'),0)*100, 1),
    ROUND(
      (NULLIF(SUM(lt.total_pts) FILTER (WHERE lt.type_lineup='offense'),0)::numeric / NULLIF(SUM(lt.total_poss) FILTER (WHERE lt.type_lineup='offense'),0)*100) -
      (NULLIF(SUM(lt.total_pts) FILTER (WHERE lt.type_lineup='defense'),0)::numeric / NULLIF(SUM(lt.total_poss) FILTER (WHERE lt.type_lineup='defense'),0)*100), 1
    ) AS net_rtg,
    ROUND(COALESCE(SUM(lt.minutes) FILTER (WHERE lt.type_lineup = 'offense'), 0)::numeric, 1) AS minutes,
    si.game_year
  FROM sub_identity si
  JOIN lineup_totals lt
    ON lt.team_id = si.team_id AND lt.game_year = si.game_year
    AND lt.lineup_hash = si.lineup_hash::text
  LEFT JOIN basketball_test.sub_lineups_stats sls
    ON sls.team_id = si.team_id AND sls.sub_lineup_hash::text = si.sub_lineup_hash::text AND sls.game_year = si.game_year
  GROUP BY si.team_id, si.sub_lineup_hash, si.num_lineup, si.player_ids, sls.player_names, sls.player_names_str, si.game_year
  HAVING (COALESCE(SUM(lt.total_poss),0)) >= p_min_poss;

  ELSE
  -- ============================================================
  -- NON-CLUTCH PATH: Use pre-aggregated mv_lineup_totals_by_day
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
  lineup_totals AS (
    SELECT lt.team_id, lt.game_year, lt.lineup_hash, lt.type_lineup,
           SUM(lt.total_poss) AS total_poss, SUM(lt.total_pts) AS total_pts,
           SUM(lt.minutes) AS minutes
    FROM basketball_test.mv_lineup_totals_by_day lt
    JOIN games_filtered gf ON gf.game_id = lt.game_id AND gf.team_id = lt.team_id
    WHERE (p_game_year IS NULL OR lt.game_year = p_game_year)
    GROUP BY lt.team_id, lt.game_year, lt.lineup_hash, lt.type_lineup
  )
  SELECT
    si.team_id, si.sub_lineup_hash, si.num_lineup, si.player_ids, sls.player_names, sls.player_names_str,
    COALESCE(SUM(lt.total_poss) FILTER (WHERE lt.type_lineup='offense'), 0)::int4,
    COALESCE(SUM(lt.total_pts)  FILTER (WHERE lt.type_lineup='offense'), 0)::int4,
    ROUND(NULLIF(SUM(lt.total_pts) FILTER (WHERE lt.type_lineup='offense'),0)::numeric / NULLIF(SUM(lt.total_poss) FILTER (WHERE lt.type_lineup='offense'),0)*100, 1),
    COALESCE(SUM(lt.total_poss) FILTER (WHERE lt.type_lineup='defense'), 0)::int4,
    COALESCE(SUM(lt.total_pts)  FILTER (WHERE lt.type_lineup='defense'), 0)::int4,
    ROUND(NULLIF(SUM(lt.total_pts) FILTER (WHERE lt.type_lineup='defense'),0)::numeric / NULLIF(SUM(lt.total_poss) FILTER (WHERE lt.type_lineup='defense'),0)*100, 1),
    ROUND(
      (NULLIF(SUM(lt.total_pts) FILTER (WHERE lt.type_lineup='offense'),0)::numeric / NULLIF(SUM(lt.total_poss) FILTER (WHERE lt.type_lineup='offense'),0)*100) -
      (NULLIF(SUM(lt.total_pts) FILTER (WHERE lt.type_lineup='defense'),0)::numeric / NULLIF(SUM(lt.total_poss) FILTER (WHERE lt.type_lineup='defense'),0)*100), 1
    ) AS net_rtg,
    ROUND(COALESCE(SUM(lt.minutes) FILTER (WHERE lt.type_lineup = 'offense'), 0)::numeric, 1) AS minutes,
    si.game_year
  FROM sub_identity si
  JOIN lineup_totals lt
    ON lt.team_id = si.team_id AND lt.game_year = si.game_year
    AND lt.lineup_hash = si.lineup_hash::text
  LEFT JOIN basketball_test.sub_lineups_stats sls
    ON sls.team_id = si.team_id AND sls.sub_lineup_hash::text = si.sub_lineup_hash::text AND sls.game_year = si.game_year
  GROUP BY si.team_id, si.sub_lineup_hash, si.num_lineup, si.player_ids, sls.player_names, sls.player_names_str, si.game_year
  HAVING (COALESCE(SUM(lt.total_poss),0)) >= p_min_poss;

  END IF;
END;
$function$
;
