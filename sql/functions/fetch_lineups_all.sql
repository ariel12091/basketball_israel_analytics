-- DROP FUNCTION basketball_test.fetch_lineups_all(int2, _int4, _int4, _int4, bool, date, date, int4, int4);

CREATE OR REPLACE FUNCTION basketball_test.fetch_lineups_all(p_num_lineup smallint, p_team_ids integer[] DEFAULT NULL::integer[], p_player_ids integer[] DEFAULT NULL::integer[], p_player_off_ids integer[] DEFAULT NULL::integer[], p_exact boolean DEFAULT true, p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date, p_min_poss integer DEFAULT 20, p_game_year integer DEFAULT NULL::integer)
 RETURNS TABLE(team_id integer, sub_lineup_hash text, num_lineup smallint, player_ids integer[], player_names text[], player_names_str text, off_poss integer, off_pts integer, off_ppp numeric, def_poss integer, def_pts integer, def_ppp numeric, net_rtg numeric, game_year integer)
 LANGUAGE plpgsql
 STABLE
AS $function$
DECLARE
  v_ids_norm  int4[];
  v_sel_cnt   int;
  v_off_norm  int4[];
BEGIN
  -- normalize ON list
  IF p_player_ids IS NOT NULL THEN
    SELECT ARRAY(SELECT DISTINCT x FROM unnest(p_player_ids) x ORDER BY x),
           cardinality(p_player_ids)
    INTO v_ids_norm, v_sel_cnt;
  ELSE
    v_ids_norm := NULL;
    v_sel_cnt  := 0;
  END IF;

  -- normalize OFF list
  IF p_player_off_ids IS NOT NULL THEN
    SELECT ARRAY(SELECT DISTINCT x FROM unnest(p_player_off_ids) x ORDER BY x)
    INTO v_off_norm;
  ELSE
    v_off_norm := NULL;
  END IF;

  -------------------------------------------------------------------
  -- 1) All-time (sub_lineups_stats) – now filtered by p_game_year
  -------------------------------------------------------------------
  IF p_start_date IS NULL AND p_end_date IS NULL THEN
    RETURN QUERY
    SELECT
      s.team_id,
      s.sub_lineup_hash::text,
      s.num_lineup,
      s.player_ids,
      s.player_names,
      s.player_names_str,
      s.off_poss,
      s.off_pts,
      s.off_ppp,
      s.def_poss,
      s.def_pts,
      s.def_ppp,
      ROUND(s.off_ppp - s.def_ppp, 1) AS net_rtg,
      s.game_year
    FROM basketball_test.sub_lineups_stats s
    WHERE s.num_lineup = p_num_lineup
      AND (p_team_ids  IS NULL OR s.team_id   = ANY(p_team_ids))
      AND (p_game_year IS NULL OR s.game_year = p_game_year)
      -- ON logic (hybrid semantics)
      AND (
        v_ids_norm IS NULL OR
        CASE
          WHEN NOT p_exact THEN s.player_ids @> v_ids_norm
          WHEN v_sel_cnt = s.num_lineup THEN (s.player_ids @> v_ids_norm AND s.player_ids <@ v_ids_norm)
          WHEN v_sel_cnt < s.num_lineup THEN s.player_ids @> v_ids_norm
          ELSE FALSE
        END
      )
      -- OFF logic (exclude any overlap)
      AND (v_off_norm IS NULL OR NOT (s.player_ids && v_off_norm))
      -- MIN TOTAL POSSESSIONS (off + def)
      AND (COALESCE(s.off_poss,0) + COALESCE(s.def_poss,0)) >= p_min_poss;
    RETURN;
  END IF;

  -------------------------------------------------------------------
  -- 2) Date-filtered path – season aware via game_year
  -------------------------------------------------------------------
  RETURN QUERY
  WITH
  -- 2/3/4-man identities from sub_lineups (season-filtered)
  sub_identity_234 AS (
    SELECT DISTINCT
      s.team_id,
      s.game_year,
      s.sub_lineup_hash::text AS sub_lineup_hash,
      s.player_ids,
      s.num_lineup
    FROM basketball_test.sub_lineups s
    WHERE s.num_lineup = p_num_lineup
      AND (p_team_ids  IS NULL OR s.team_id   = ANY(p_team_ids))
      AND (p_game_year IS NULL OR s.game_year = p_game_year)
      AND (
        v_ids_norm IS NULL OR
        CASE
          WHEN NOT p_exact THEN s.player_ids @> v_ids_norm
          WHEN v_sel_cnt = s.num_lineup THEN (s.player_ids @> v_ids_norm AND s.player_ids <@ v_ids_norm)
          WHEN v_sel_cnt < s.num_lineup THEN s.player_ids @> v_ids_norm
          ELSE FALSE
        END
      )
      AND (v_off_norm IS NULL OR NOT (s.player_ids && v_off_norm))
  ),

  -- 5-man identities built from lineups_lookup_on (season-filtered)
  full_lineups AS (
    SELECT
      l.team_id,
      l.game_year,
      l.lineup_hash,
      ARRAY_AGG(DISTINCT l.player_id ORDER BY l.player_id)::int4[] AS player_ids
    FROM basketball_test.lineups_lookup_on l
    WHERE (p_team_ids  IS NULL OR l.team_id   = ANY(p_team_ids))
      AND (p_game_year IS NULL OR l.game_year = p_game_year)
    GROUP BY l.team_id, l.game_year, l.lineup_hash
  ),
  sub_identity_5 AS (
    SELECT
      fl.team_id,
      fl.game_year,
      fl.lineup_hash::text AS sub_lineup_hash,
      fl.player_ids,
      5::int2 AS num_lineup
    FROM full_lineups fl
    WHERE p_num_lineup = 5
      AND cardinality(fl.player_ids) = 5
      AND (
        v_ids_norm IS NULL OR
        CASE
          WHEN NOT p_exact THEN fl.player_ids @> v_ids_norm
          WHEN cardinality(v_ids_norm) = 5 THEN (fl.player_ids @> v_ids_norm AND fl.player_ids <@ v_ids_norm)
          WHEN cardinality(v_ids_norm) < 5 THEN fl.player_ids @> v_ids_norm
          ELSE FALSE
        END
      )
      AND (v_off_norm IS NULL OR NOT (fl.player_ids && v_off_norm))
  ),

  -- all sub-identity rows (2/3/4 and 5)
  sub_identity AS (
    SELECT * FROM sub_identity_234
    UNION ALL
    SELECT * FROM sub_identity_5
  ),

  -- map 2/3/4 sub_lineups → lineup_hash (season-aware)
  sub_map_234 AS (
    SELECT DISTINCT
      s.team_id,
      s.game_year,
      s.sub_lineup_hash::text AS sub_lineup_hash,
      s.lineup_hash::text     AS lineup_hash
    FROM basketball_test.sub_lineups s
    JOIN sub_identity w
      ON w.team_id        = s.team_id
     AND w.sub_lineup_hash = s.sub_lineup_hash::text
     AND w.game_year       = s.game_year
    WHERE s.num_lineup = p_num_lineup
  ),

  -- for 5-man, sub_lineup_hash == lineup_hash
  sub_map_5 AS (
    SELECT
      w.team_id,
      w.game_year,
      w.sub_lineup_hash,
      w.sub_lineup_hash::text AS lineup_hash
    FROM sub_identity w
    WHERE w.num_lineup = 5
  ),

  sub_map AS (
    SELECT * FROM sub_map_234
    UNION ALL
    SELECT * FROM sub_map_5
  ),

  needed_lineups AS (
    SELECT DISTINCT lineup_hash FROM sub_map
  ),

  -- aggregate lineup totals once per (lineup_hash, type_lineup) over the date window
  lineup_totals AS (
    SELECT
      lt.lineup_hash::text AS lineup_hash,
      lt.type_lineup,
      SUM(lt.total_poss) AS total_poss,
      SUM(lt.total_pts)  AS total_pts
    FROM basketball_test.mv_lineup_totals_by_day lt
    JOIN needed_lineups nl
      ON nl.lineup_hash = lt.lineup_hash::text
    WHERE (p_start_date IS NULL OR lt.g_date >= p_start_date)
      AND (p_end_date   IS NULL OR lt.g_date <= p_end_date)
    GROUP BY lt.lineup_hash::text, lt.type_lineup
  ),

  -- totals per (team, sub_lineup_hash, type_lineup, game_year)
  per_type AS (
    SELECT
      sm.team_id,
      sm.game_year,
      sm.sub_lineup_hash,
      si.player_ids,
      si.num_lineup,
      lt.type_lineup,
      SUM(lt.total_poss) AS total_poss,
      SUM(lt.total_pts)  AS total_pts
    FROM sub_map sm
    JOIN sub_identity si
      ON si.team_id        = sm.team_id
     AND si.sub_lineup_hash = sm.sub_lineup_hash
     AND si.game_year       = sm.game_year
    JOIN lineup_totals lt
      ON lt.lineup_hash = sm.lineup_hash
    GROUP BY
      sm.team_id,
      sm.game_year,
      sm.sub_lineup_hash,
      si.player_ids,
      si.num_lineup,
      lt.type_lineup
  )
  SELECT
    p.team_id,
    p.sub_lineup_hash,
    p.num_lineup,
    p.player_ids,
    sls.player_names,
    sls.player_names_str,
    COALESCE(SUM(p.total_poss) FILTER (WHERE p.type_lineup='offense'), 0)::int4 AS off_poss,
    COALESCE(SUM(p.total_pts)  FILTER (WHERE p.type_lineup='offense'), 0)::int4 AS off_pts,
    ROUND(
      NULLIF(SUM(p.total_pts)  FILTER (WHERE p.type_lineup='offense'), 0)::numeric
      / NULLIF(SUM(p.total_poss) FILTER (WHERE p.type_lineup='offense'), 0) * 100,
      1
    ) AS off_ppp,
    COALESCE(SUM(p.total_poss) FILTER (WHERE p.type_lineup='defense'), 0)::int4 AS def_poss,
    COALESCE(SUM(p.total_pts)  FILTER (WHERE p.type_lineup='defense'), 0)::int4 AS def_pts,
    ROUND(
      NULLIF(SUM(p.total_pts)  FILTER (WHERE p.type_lineup='defense'), 0)::numeric
      / NULLIF(SUM(p.total_poss) FILTER (WHERE p.type_lineup='defense'), 0) * 100,
      1
    ) AS def_ppp,
    ROUND(
      ROUND(
        NULLIF(SUM(p.total_pts) FILTER (WHERE p.type_lineup='offense'), 0)::numeric
        / NULLIF(SUM(p.total_poss) FILTER (WHERE p.type_lineup='offense'), 0) * 100,
        1
      )
      -
      ROUND(
        NULLIF(SUM(p.total_pts) FILTER (WHERE p.type_lineup='defense'), 0)::numeric
        / NULLIF(SUM(p.total_poss) FILTER (WHERE p.type_lineup='defense'), 0) * 100,
        1
      ),
      1
    ) AS net_rtg,
    p.game_year
  FROM per_type p
  LEFT JOIN basketball_test.sub_lineups_stats sls
    ON sls.team_id         = p.team_id
   AND sls.sub_lineup_hash = p.sub_lineup_hash
   AND sls.game_year       = p.game_year
  GROUP BY
    p.team_id,
    p.sub_lineup_hash,
    p.num_lineup,
    p.player_ids,
    sls.player_names,
    sls.player_names_str,
    p.game_year
  HAVING
    (
      COALESCE(SUM(p.total_poss) FILTER (WHERE p.type_lineup='offense'), 0)
      +
      COALESCE(SUM(p.total_poss) FILTER (WHERE p.type_lineup='defense'), 0)
    ) >= p_min_poss;
END;
$function$
;
