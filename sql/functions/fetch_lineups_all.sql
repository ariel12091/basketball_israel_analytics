-- DROP FUNCTION basketball_test.fetch_lineups_all(int2, _int4, _int4, _int4, bool, date, date, int4, int4, text, text, text, text, text, int4, text);

CREATE OR REPLACE FUNCTION basketball_test.fetch_lineups_all(p_num_lineup smallint, p_team_ids integer[] DEFAULT NULL::integer[], p_player_ids integer[] DEFAULT NULL::integer[], p_player_off_ids integer[] DEFAULT NULL::integer[], p_exact boolean DEFAULT true, p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date, p_min_poss integer DEFAULT 20, p_game_year integer DEFAULT NULL::integer, p_game_type_csv text DEFAULT NULL::text, p_opp_team_ids_csv text DEFAULT NULL::text, p_home_away text DEFAULT 'all'::text, p_outcome text DEFAULT 'all'::text, p_opp_rank_side text DEFAULT 'all'::text, p_opp_rank_n integer DEFAULT NULL::integer, p_opp_rank_metric text DEFAULT 'net'::text)
 RETURNS TABLE(team_id integer, sub_lineup_hash text, num_lineup smallint, player_ids integer[], player_names text[], player_names_str text, off_poss integer, off_pts integer, off_ppp numeric, def_poss integer, def_pts integer, def_ppp numeric, net_rtg numeric, game_year integer)
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
    AND (v_opp_rank_side = 'all' OR p_opp_rank_n IS NULL);

  -- 1) Fast path
  IF v_use_fast_path THEN
    RETURN QUERY
    SELECT
      s.team_id, s.sub_lineup_hash::text, s.num_lineup, s.player_ids, s.player_names, s.player_names_str,
      s.off_poss, s.off_pts, s.off_ppp, s.def_poss, s.def_pts, s.def_ppp,
      ROUND(s.off_ppp - s.def_ppp, 1) AS net_rtg, s.game_year
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
  RETURN QUERY
  WITH 
  -- CTE 1: Games Base
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
  
  -- CTE 2: Games Ranked (With Explicit Aliases)
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

  -- CTE 3: Games Filtered (FIXED: Added 'gr.' alias to avoid ambiguity)
  games_filtered AS (
    SELECT gr.game_id, gr.team_id, gr.game_year
    FROM games_ranked gr
    WHERE v_opp_rank_side = 'all' OR p_opp_rank_n IS NULL
       OR (v_opp_rank_side = 'top'    AND gr.opp_rank <= p_opp_rank_n)
       OR (v_opp_rank_side = 'bottom' AND gr.opp_rank >= (gr.max_rank - p_opp_rank_n + 1))
  ),
  
  -- CTE 4: Sub Identity
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

  -- CTE 5: Totals (Uses gf alias which is safe now)
  lineup_totals AS (
    SELECT lt.team_id, lt.game_year, lt.lineup_hash, lt.type_lineup,
           SUM(lt.total_poss) AS total_poss, SUM(lt.total_pts) AS total_pts
    FROM basketball_test.mv_lineup_totals_by_day lt
    JOIN games_filtered gf ON gf.game_id = lt.game_id AND gf.team_id = lt.team_id
    WHERE (p_game_year IS NULL OR lt.game_year = p_game_year)
    GROUP BY lt.team_id, lt.game_year, lt.lineup_hash, lt.type_lineup
  )

  -- Final Select
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
    si.game_year
  FROM sub_identity si
  JOIN lineup_totals lt 
    ON lt.team_id = si.team_id AND lt.game_year = si.game_year 
    AND lt.lineup_hash = si.lineup_hash::text
  LEFT JOIN basketball_test.sub_lineups_stats sls
    ON sls.team_id = si.team_id AND sls.sub_lineup_hash::text = si.sub_lineup_hash::text AND sls.game_year = si.game_year
  GROUP BY si.team_id, si.sub_lineup_hash, si.num_lineup, si.player_ids, sls.player_names, sls.player_names_str, si.game_year
  HAVING (COALESCE(SUM(lt.total_poss),0)) >= p_min_poss;
END;
$function$
;
