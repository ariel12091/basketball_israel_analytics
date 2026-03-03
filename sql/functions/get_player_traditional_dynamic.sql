DROP FUNCTION IF EXISTS basketball_test.get_player_traditional_dynamic;

CREATE OR REPLACE FUNCTION basketball_test.get_player_traditional_dynamic(
    p_game_year           INT,
    p_start_date          DATE DEFAULT NULL,
    p_end_date            DATE DEFAULT NULL,
    p_team_ids_csv        TEXT DEFAULT NULL,
    p_game_type_csv       TEXT DEFAULT NULL,
    p_opp_team_ids_csv    TEXT DEFAULT NULL,
    p_home_away           TEXT DEFAULT 'all',
    p_outcome             TEXT DEFAULT 'all',
    p_opp_rank_side       TEXT DEFAULT 'all',
    p_opp_rank_n          INT DEFAULT NULL,
    p_opp_rank_metric     TEXT DEFAULT 'net',
    p_max_margin          INT DEFAULT NULL,
    p_margin_status       TEXT DEFAULT 'all',
    p_max_time_remaining  INT DEFAULT NULL,
    p_ot_margin_filter    BOOLEAN DEFAULT FALSE,
    p_min_gn              INT DEFAULT NULL,
    p_max_gn              INT DEFAULT NULL,
    p_last_n_games        INT DEFAULT NULL
)
RETURNS TABLE (
    player_id      INT,
    team_id        INT,
    team_name      TEXT,
    player_name    TEXT,
    gp             INT,
    poss_on_floor  INT,
    minutes        NUMERIC,
    pts            INT,
    reb            INT,
    ast            INT,
    stl            INT,
    blk            INT,
    tov            INT,
    fgm            INT,
    fga            INT,
    "3pm"          INT,
    "3pa"          INT,
    ftm            INT,
    fta            INT,
    fg_pct         NUMERIC,
    tp_pct         NUMERIC,
    ft_pct         NUMERIC,
    efg            NUMERIC,
    ts             NUMERIC
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_team_ids         int4[];
  v_game_types       int4[];
  v_opp_ids          int4[];
  v_home_away        text;
  v_outcome          text;
  v_opp_rank_side    text;
  v_opp_rank_metric  text;
  v_margin_status    text;
BEGIN
  v_home_away       := COALESCE(NULLIF(btrim(p_home_away), ''), 'all');
  v_outcome         := COALESCE(NULLIF(btrim(p_outcome), ''), 'all');
  v_opp_rank_side   := COALESCE(NULLIF(btrim(p_opp_rank_side), ''), 'all');
  v_opp_rank_metric := COALESCE(NULLIF(btrim(p_opp_rank_metric), ''), 'net');
  v_margin_status   := COALESCE(NULLIF(btrim(p_margin_status), ''), 'all');

  IF p_team_ids_csv IS NOT NULL AND length(btrim(p_team_ids_csv)) > 0 THEN
    v_team_ids := ARRAY(
      SELECT DISTINCT x::int4
      FROM unnest(string_to_array(regexp_replace(p_team_ids_csv, '\s+', '', 'g'), ',')) x
      WHERE x <> ''
      ORDER BY 1
    );
  END IF;

  IF p_game_type_csv IS NOT NULL AND length(btrim(p_game_type_csv)) > 0 THEN
    v_game_types := ARRAY(
      SELECT DISTINCT x::int4
      FROM unnest(string_to_array(regexp_replace(p_game_type_csv, '\s+', '', 'g'), ',')) x
      WHERE x <> ''
      ORDER BY 1
    );
  END IF;

  IF p_opp_team_ids_csv IS NOT NULL AND length(btrim(p_opp_team_ids_csv)) > 0 THEN
    v_opp_ids := ARRAY(
      SELECT DISTINCT x::int4
      FROM unnest(string_to_array(regexp_replace(p_opp_team_ids_csv, '\s+', '', 'g'), ',')) x
      WHERE x <> ''
      ORDER BY 1
    );
  END IF;

  RETURN QUERY
  WITH games_base AS (
    SELECT
      fs.game_id,
      fs.team_id,
      fs.game_year,
      fs.opp_team_id,
      fs.game_date
    FROM basketball_test.final_schedule_mv fs
    WHERE fs.game_year = p_game_year
      AND (p_start_date IS NULL OR fs.game_date >= p_start_date)
      AND (p_end_date IS NULL OR fs.game_date <= p_end_date)
      AND (v_team_ids IS NULL OR fs.team_id = ANY(v_team_ids))
      AND (v_game_types IS NULL OR fs.game_type = ANY(v_game_types))
      AND (v_opp_ids IS NULL OR fs.opp_team_id = ANY(v_opp_ids))
      AND (
        v_home_away = 'all'
        OR (v_home_away = 'home' AND fs.is_home)
        OR (v_home_away = 'away' AND NOT fs.is_home)
      )
      AND (
        v_outcome = 'all'
        OR (v_outcome = 'win' AND fs.has_won IS TRUE)
        OR (v_outcome = 'loss' AND fs.has_won IS FALSE)
      )
      AND (p_min_gn IS NULL OR fs.gn >= p_min_gn)
      AND (p_max_gn IS NULL OR fs.gn <= p_max_gn)
      AND (
        p_last_n_games IS NULL OR COALESCE((
          SELECT fsr.rn_recent
          FROM (
            SELECT
              fs2.game_id,
              ROW_NUMBER() OVER (
                PARTITION BY fs2.team_id, fs2.game_year
                ORDER BY fs2.game_date DESC NULLS LAST, fs2.game_id DESC
              ) AS rn_recent
            FROM basketball_test.final_schedule_mv fs2
            WHERE fs2.team_id = fs.team_id
              AND fs2.game_year = fs.game_year
          ) fsr
          WHERE fsr.game_id = fs.game_id
        ), 2147483647) <= p_last_n_games
      )
  ),
  games_ranked AS (
    SELECT
      gb.game_id,
      gb.team_id,
      gb.game_year,
      CASE
        WHEN v_opp_rank_side IN ('top', 'bottom') THEN
          CASE v_opp_rank_metric
            WHEN 'off' THEN r.rank_off_ppp
            WHEN 'def' THEN r.rank_def_ppp
            ELSE r.rank_net_rtg
          END
        ELSE NULL
      END AS opp_rank,
      CASE
        WHEN v_opp_rank_side = 'bottom' THEN
          MAX(
            CASE v_opp_rank_metric
              WHEN 'off' THEN r.rank_off_ppp
              WHEN 'def' THEN r.rank_def_ppp
              ELSE r.rank_net_rtg
            END
          ) OVER (PARTITION BY gb.game_year)
        ELSE NULL
      END AS max_rank
    FROM games_base gb
    LEFT JOIN basketball_test.team_ppp_ratings_mv r
      ON r.game_year::int4 = gb.game_year
     AND r.team_id::int4 = gb.opp_team_id
     AND v_opp_rank_side IN ('top', 'bottom')
  ),
  games_filtered AS (
    SELECT gr.game_id, gr.team_id, gr.game_year
    FROM games_ranked gr
    WHERE v_opp_rank_side = 'all' OR p_opp_rank_n IS NULL
       OR (v_opp_rank_side = 'top' AND gr.opp_rank <= p_opp_rank_n)
       OR (v_opp_rank_side = 'bottom' AND gr.opp_rank >= (gr.max_rank - p_opp_rank_n + 1))
  ),
  acts AS (
    SELECT
      d.id,
      d.game_id,
      d.team_id,
      d.lineup_hash,
      d.segment_id,
      d.end_game_seconds_remaining,
      d.type,
      d.parameters_type,
      d.parameters_made,
      d.parameters_points,
      d.player_id,
      d.event_owner_side,
      d.type_lineup,
      d.final_end_poss,
      d.quarter,
      d.own_team_score,
      d.opp_team_score
    FROM basketball_test.df_pts_poss_lineups_longer_mv d
    JOIN games_filtered gf
      ON gf.game_id = d.game_id
     AND gf.team_id = d.team_id
    WHERE (
      p_max_margin IS NULL
      OR ABS(COALESCE(d.own_team_score, 0) - COALESCE(d.opp_team_score, 0)) <= p_max_margin
      OR (d.quarter > 4 AND NOT COALESCE(p_ot_margin_filter, FALSE))
    )
    AND (
      v_margin_status = 'all'
      OR (v_margin_status = 'leading' AND COALESCE(d.own_team_score, 0) > COALESCE(d.opp_team_score, 0))
      OR (v_margin_status = 'trailing' AND COALESCE(d.own_team_score, 0) < COALESCE(d.opp_team_score, 0))
      OR (v_margin_status = 'tied' AND COALESCE(d.own_team_score, 0) = COALESCE(d.opp_team_score, 0))
      OR (d.quarter > 4 AND NOT COALESCE(p_ot_margin_filter, FALSE))
    )
    AND (
      p_max_time_remaining IS NULL
      OR d.end_game_seconds_remaining <= p_max_time_remaining
      OR d.quarter > 4
    )
  ),
  lineup_map AS (
    SELECT DISTINCT
      ll.game_id,
      ll.team_id,
      ll.lineup_hash,
      ll.player_id
    FROM basketball_test.lineups_lookup ll
    JOIN games_filtered gf
      ON gf.game_id = ll.game_id
     AND gf.team_id = ll.team_id
    WHERE ll.game_year = p_game_year
      AND COALESCE(ll.is_on_verdict, 0)::int = 1
  ),
  poss_end AS (
    SELECT DISTINCT
      a.game_id,
      a.team_id,
      a.lineup_hash,
      a.id AS poss_end_id
    FROM acts a
    WHERE a.type_lineup = 'offense'
      AND a.final_end_poss
      AND a.id IS NOT NULL
      AND a.lineup_hash IS NOT NULL
  ),
  player_usage AS (
    SELECT
      lm.player_id,
      pe.team_id,
      COUNT(DISTINCT pe.game_id)::int AS gp,
      COUNT(DISTINCT (pe.game_id, pe.team_id, pe.poss_end_id))::int AS poss_on_floor
    FROM poss_end pe
    JOIN lineup_map lm
      ON lm.game_id = pe.game_id
     AND lm.team_id = pe.team_id
     AND lm.lineup_hash = pe.lineup_hash
    GROUP BY lm.player_id, pe.team_id
  ),
  seg_times AS (
    SELECT
      a.game_id,
      a.team_id,
      a.lineup_hash,
      a.segment_id,
      MAX(a.end_game_seconds_remaining) - MIN(a.end_game_seconds_remaining) AS seg_seconds
    FROM acts a
    WHERE a.lineup_hash IS NOT NULL
      AND a.segment_id IS NOT NULL
      AND a.end_game_seconds_remaining IS NOT NULL
    GROUP BY a.game_id, a.team_id, a.lineup_hash, a.segment_id
  ),
  player_minutes AS (
    SELECT
      lm.player_id,
      st.team_id,
      ROUND(SUM(COALESCE(st.seg_seconds, 0))::numeric / 60.0, 1) AS minutes
    FROM seg_times st
    JOIN lineup_map lm
      ON lm.game_id = st.game_id
     AND lm.team_id = st.team_id
     AND lm.lineup_hash = st.lineup_hash
    GROUP BY lm.player_id, st.team_id
  ),
  stats AS (
    SELECT
      a.player_id,
      a.team_id,
      (
        SUM(CASE WHEN a.type = 'shot' AND a.parameters_made = 'made' AND a.type_lineup = 'offense'
                 THEN COALESCE(a.parameters_points, 0) ELSE 0 END)
        + SUM(CASE WHEN a.type = 'freeThrow' AND a.parameters_made = 'made' AND a.type_lineup = 'offense'
                   THEN 1 ELSE 0 END)
      )::int AS pts,
      SUM(CASE WHEN a.type = 'rebound' AND (
                (a.type_lineup = 'offense' AND a.parameters_type = 'offensive')
                OR (a.type_lineup = 'defense' AND a.parameters_type = 'defensive')
              ) THEN 1 ELSE 0 END)::int AS reb,
      SUM(CASE WHEN a.type = 'assist' AND a.type_lineup = 'offense' THEN 1 ELSE 0 END)::int AS ast,
      SUM(CASE WHEN a.type = 'steal' AND a.type_lineup = 'defense' THEN 1 ELSE 0 END)::int AS stl,
      SUM(CASE WHEN a.type = 'block' AND a.type_lineup = 'defense' THEN 1 ELSE 0 END)::int AS blk,
      SUM(CASE WHEN a.type = 'turnover' AND a.type_lineup = 'offense' THEN 1 ELSE 0 END)::int AS tov,
      SUM(CASE WHEN a.type = 'shot' AND a.parameters_made = 'made' AND a.type_lineup = 'offense' THEN 1 ELSE 0 END)::int AS fgm,
      SUM(CASE WHEN a.type = 'shot' AND a.type_lineup = 'offense' THEN 1 ELSE 0 END)::int AS fga,
      SUM(CASE WHEN a.type = 'shot' AND a.parameters_made = 'made' AND a.parameters_points = 3 AND a.type_lineup = 'offense' THEN 1 ELSE 0 END)::int AS "3pm",
      SUM(CASE WHEN a.type = 'shot' AND a.parameters_points = 3 AND a.type_lineup = 'offense' THEN 1 ELSE 0 END)::int AS "3pa",
      SUM(CASE WHEN a.type = 'freeThrow' AND a.parameters_made = 'made' AND a.type_lineup = 'offense' THEN 1 ELSE 0 END)::int AS ftm,
      SUM(CASE WHEN a.type = 'freeThrow' AND a.type_lineup = 'offense' THEN 1 ELSE 0 END)::int AS fta
    FROM acts a
    WHERE a.player_id IS NOT NULL
      AND a.player_id > 0
    GROUP BY a.player_id, a.team_id
  ),
  names_df AS (
    SELECT
      fr.player_id,
      fr.team_id,
      MIN(btrim(fr.team_name)) AS team_name,
      MIN(btrim(CONCAT_WS(' ', fr.firstname, fr.lastname))) AS player_name
    FROM basketball_test.full_rosters fr
    WHERE fr.game_year = p_game_year
    GROUP BY fr.player_id, fr.team_id
  ),
  final_rows AS (
    SELECT
      s.player_id,
      s.team_id,
      nd.team_name,
      nd.player_name,
      COALESCE(pu.gp, 0)::int AS gp,
      COALESCE(pu.poss_on_floor, 0)::int AS poss_on_floor,
      COALESCE(pm.minutes, 0)::numeric AS minutes,
      s.pts,
      s.reb,
      s.ast,
      s.stl,
      s.blk,
      s.tov,
      s.fgm,
      s.fga,
      s."3pm",
      s."3pa",
      s.ftm,
      s.fta,
      CASE WHEN s.fga > 0 THEN ROUND((s.fgm::numeric / s.fga::numeric) * 100, 1) ELSE NULL END AS fg_pct,
      CASE WHEN s."3pa" > 0 THEN ROUND((s."3pm"::numeric / s."3pa"::numeric) * 100, 1) ELSE NULL END AS tp_pct,
      CASE WHEN s.fta > 0 THEN ROUND((s.ftm::numeric / s.fta::numeric) * 100, 1) ELSE NULL END AS ft_pct,
      CASE WHEN s.fga > 0 THEN ROUND(((s.fgm::numeric + 0.5 * s."3pm"::numeric) / s.fga::numeric) * 100, 1) ELSE NULL END AS efg,
      CASE WHEN (s.fga + 0.44 * s.fta) > 0 THEN ROUND((s.pts::numeric / (2.0 * (s.fga::numeric + 0.44 * s.fta::numeric))) * 100, 1) ELSE NULL END AS ts
    FROM stats s
    LEFT JOIN player_usage pu
      ON pu.player_id = s.player_id
     AND pu.team_id = s.team_id
    LEFT JOIN player_minutes pm
      ON pm.player_id = s.player_id
     AND pm.team_id = s.team_id
    LEFT JOIN names_df nd
      ON nd.player_id = s.player_id
     AND nd.team_id = s.team_id
  )
  SELECT
    fr.player_id,
    fr.team_id,
    fr.team_name,
    fr.player_name,
    fr.gp,
    fr.poss_on_floor,
    fr.minutes,
    fr.pts,
    fr.reb,
    fr.ast,
    fr.stl,
    fr.blk,
    fr.tov,
    fr.fgm,
    fr.fga,
    fr."3pm",
    fr."3pa",
    fr.ftm,
    fr.fta,
    fr.fg_pct,
    fr.tp_pct,
    fr.ft_pct,
    fr.efg,
    fr.ts
  FROM final_rows fr
  WHERE fr.player_name IS NOT NULL
    AND fr.player_name <> ''
    AND fr.team_name IS NOT NULL
    AND fr.team_name <> ''
    AND (COALESCE(fr.gp, 0) > 0 OR COALESCE(fr.poss_on_floor, 0) > 0 OR COALESCE(fr.minutes, 0) > 0)
  ORDER BY fr.pts DESC, fr.minutes DESC, fr.team_name, fr.player_name;
END;
$$;
