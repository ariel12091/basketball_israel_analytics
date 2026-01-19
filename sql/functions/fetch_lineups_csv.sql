-- DROP FUNCTION basketball_test.fetch_lineups_csv_v2(int4, text, text, text, bool, date, date, int4, int4, text, text, text, text, text, int4, text);

CREATE OR REPLACE FUNCTION basketball_test.fetch_lineups_csv_v2(p_num_lineup integer, p_team_ids_csv text, p_player_ids_csv text, p_player_off_csv text, p_exact boolean, p_start_date date, p_end_date date, p_min_poss integer, p_game_year integer DEFAULT NULL::integer, p_game_type_csv text DEFAULT NULL::text, p_opp_team_ids_csv text DEFAULT NULL::text, p_home_away text DEFAULT 'all'::text, p_outcome text DEFAULT 'all'::text, p_opp_rank_side text DEFAULT 'all'::text, p_opp_rank_n integer DEFAULT NULL::integer, p_opp_rank_metric text DEFAULT 'net'::text)
 RETURNS TABLE(team_id integer, sub_lineup_hash text, num_lineup smallint, player_ids integer[], player_names text[], player_names_str text, off_poss integer, off_pts integer, off_ppp numeric, def_poss integer, def_pts integer, def_ppp numeric, net_rtg numeric, game_year integer)
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
  FROM basketball_test.fetch_lineups_all(
    p_num_lineup::int2,
    v_team_ids,
    v_player_ids,
    v_off_ids,
    p_exact,
    p_start_date,
    p_end_date,
    p_min_poss::int4,
    p_game_year,

    -- NEW passthrough
    p_game_type_csv,
    p_opp_team_ids_csv,
    p_home_away,
    p_outcome,
    p_opp_rank_side,
    p_opp_rank_n,
    p_opp_rank_metric
  );
END;
$function$
;
