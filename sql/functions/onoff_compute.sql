DROP FUNCTION IF EXISTS basketball_test.onoff_compute(date, date, text, int4, int4, numeric, text, text, text, text, text, text, int4, text);

CREATE OR REPLACE FUNCTION basketball_test.onoff_compute(
    p_start_date      DATE,
    p_end_date        DATE,
    p_team_ids        TEXT,
    p_min_all         INTEGER,
    p_min_on          INTEGER,
    p_min_net         NUMERIC,
    p_game_year       TEXT,
    p_game_type_csv   TEXT DEFAULT NULL,
    p_opp_ids_csv     TEXT DEFAULT NULL,
    p_home_away       TEXT DEFAULT 'all',
    p_outcome         TEXT DEFAULT 'all',
    p_opp_rank_side   TEXT DEFAULT NULL,
    p_opp_rank_n      INTEGER DEFAULT NULL,
    p_opp_rank_metric TEXT DEFAULT NULL
)
RETURNS TABLE (
    "Team" text, "First Name" text, "Last Name" text,
    "Net RTG Diff" numeric, "Off ON Diff" numeric, "Def ON Diff" numeric,
    "Off ON PPP" numeric, "Def ON PPP" numeric, "On Net RTG" numeric,
    "Off OFF PPP" numeric, "Def OFF PPP" numeric, "Off Net RTG" numeric,
    "ON Poss" numeric, "OFF Poss" numeric,
    pr_net double precision, pr_off_on double precision, pr_off_off double precision,
    pr_def_on_inv double precision, pr_def_off_inv double precision,
    pr_off_on_d double precision, pr_def_on_d double precision, pr_def_on_d_inv double precision,
    pr_on_net double precision, pr_off_net double precision,
    player_id integer, team_id integer
)
LANGUAGE plpgsql
STABLE
AS $$
DECLARE
  v_team_ids        int4[];
  v_game_types      int4[];
  v_opp_ids         int4[];
  v_home_away       text;
  v_outcome         text;
  v_opp_rank_side   text;
  v_opp_rank_metric text;
  v_game_year       text;
BEGIN
  -- [Input Normalization]
  v_home_away       := COALESCE(NULLIF(btrim(p_home_away), ''), 'all');
  v_outcome         := COALESCE(NULLIF(btrim(p_outcome), ''), 'all');
  v_opp_rank_side   := NULLIF(btrim(p_opp_rank_side), '');
  v_opp_rank_metric := NULLIF(btrim(p_opp_rank_metric), '');
  v_game_year       := NULLIF(btrim(p_game_year), '');

  -- Parse CSVs
  IF p_team_ids IS NOT NULL AND length(btrim(p_team_ids)) > 0 THEN
    v_team_ids := ARRAY(SELECT DISTINCT x::int4 FROM unnest(string_to_array(regexp_replace(p_team_ids, '\s+', '', 'g'), ',')) x WHERE x <> '' ORDER BY 1);
  END IF;

  IF p_game_type_csv IS NOT NULL AND length(btrim(p_game_type_csv)) > 0 THEN
    v_game_types := ARRAY(SELECT DISTINCT x::int4 FROM unnest(string_to_array(regexp_replace(p_game_type_csv, '\s+', '', 'g'), ',')) x WHERE x <> '' ORDER BY 1);
  END IF;

  IF p_opp_ids_csv IS NOT NULL AND length(btrim(p_opp_ids_csv)) > 0 THEN
    v_opp_ids := ARRAY(SELECT DISTINCT x::int4 FROM unnest(string_to_array(regexp_replace(p_opp_ids_csv, '\s+', '', 'g'), ',')) x WHERE x <> '' ORDER BY 1);
  END IF;

  RETURN QUERY
  WITH
  /* ------------------------------------------------------------
     Opponent strength ranking — uses mv_lineup_totals_by_day
     (pre-aggregated) instead of df_pts_poss_lineups_longer_mv
  ------------------------------------------------------------ */
  sched_base_for_ranks AS (
    SELECT DISTINCT
      fs.game_id,
      fs.game_year,
      fs.game_date,
      fs.game_type
    FROM basketball_test.final_schedule_mv fs
    WHERE fs.game_date BETWEEN p_start_date AND p_end_date
      AND (v_game_year IS NULL OR fs.game_year::text = v_game_year)
      AND (v_game_types IS NULL OR fs.game_type = ANY(v_game_types))
  ),
  team_game_raw AS (
    SELECT
      m.game_id,
      m.team_id,
      m.type_lineup,
      SUM(m.total_pts)  AS pts,
      SUM(m.total_poss) AS poss
    FROM basketball_test.mv_lineup_totals_by_day m
    JOIN sched_base_for_ranks sb
      ON sb.game_id = m.game_id
    GROUP BY m.game_id, m.team_id, m.type_lineup
  ),
  team_game_ppp AS (
    SELECT
      tgr.game_id,
      tgr.team_id,
      MAX(CASE WHEN tgr.type_lineup = 'offense'
               THEN ROUND(tgr.pts / NULLIF(tgr.poss,0)::numeric * 100, 2) END) AS off_ppp,
      MAX(CASE WHEN tgr.type_lineup = 'defense'
               THEN ROUND(tgr.pts / NULLIF(tgr.poss,0)::numeric * 100, 2) END) AS def_ppp
    FROM team_game_raw tgr
    GROUP BY tgr.game_id, tgr.team_id
  ),
  opp_season_metrics AS (
    SELECT
      tgp.team_id,
      ROUND(AVG(tgp.off_ppp), 2) AS off_ppp,
      ROUND(AVG(tgp.def_ppp), 2) AS def_ppp,
      ROUND(AVG(tgp.off_ppp - tgp.def_ppp), 2) AS net_ppp
    FROM team_game_ppp tgp
    GROUP BY tgp.team_id
  ),
  opp_ranked AS (
    SELECT
      osm.*,
      CASE
        WHEN COALESCE(v_opp_rank_metric,'') = 'off' THEN osm.off_ppp
        WHEN COALESCE(v_opp_rank_metric,'') = 'def' THEN -osm.def_ppp
        ELSE osm.net_ppp
      END AS metric_for_rank
    FROM opp_season_metrics osm
  ),
  opp_topbottom AS (
    SELECT opp_ranked.team_id
    FROM opp_ranked
    WHERE v_opp_rank_side IS NOT NULL
      AND p_opp_rank_n IS NOT NULL
      AND v_opp_rank_metric IS NOT NULL
    ORDER BY
      CASE WHEN v_opp_rank_side = 'bottom' THEN metric_for_rank END ASC NULLS LAST,
      CASE WHEN v_opp_rank_side = 'top'    THEN metric_for_rank END DESC NULLS LAST
    LIMIT CASE WHEN p_opp_rank_n IS NULL OR p_opp_rank_n <= 0 THEN 0 ELSE p_opp_rank_n END
  ),

  -- schedule filtered by date/year + all game filters
  sched AS (
    SELECT DISTINCT
      fs.game_id,
      fs.team_id,
      fs.opp_team_id,
      fs.game_date,
      fs.game_year,
      fs.game_type,
      fs.is_home,
      fs.has_won
    FROM basketball_test.final_schedule_mv fs
    WHERE fs.game_date BETWEEN p_start_date AND p_end_date
      AND (v_game_year IS NULL OR fs.game_year::text = v_game_year)
      -- team filter
      AND (v_team_ids IS NULL OR fs.team_id = ANY(v_team_ids))
      -- game type filter
      AND (v_game_types IS NULL OR fs.game_type = ANY(v_game_types))
      -- explicit opponent filter
      AND (v_opp_ids IS NULL OR fs.opp_team_id = ANY(v_opp_ids))
      -- opponent rank filter (top/bottom N)
      AND (
        v_opp_rank_side IS NULL OR p_opp_rank_n IS NULL OR v_opp_rank_metric IS NULL
        OR fs.opp_team_id IN (SELECT opp_topbottom.team_id FROM opp_topbottom)
      )
      -- home/away filter
      AND (
        v_home_away = 'all'
        OR (v_home_away = 'home' AND fs.is_home IS TRUE)
        OR (v_home_away = 'away' AND fs.is_home IS FALSE)
      )
      -- outcome filter
      AND (
        v_outcome = 'all'
        OR (v_outcome = 'win'  AND fs.has_won IS TRUE)
        OR (v_outcome = 'loss' AND fs.has_won IS FALSE)
      )
  ),

  /* ------------------------------------------------------------
     Core aggregation — reads from pre-aggregated player_onoff_by_game
     instead of joining lineups_lookup × df_pts_poss_lineups_longer_mv
  ------------------------------------------------------------ */
  agg AS (
    SELECT
      p.player_id,
      p.team_id,
      p.is_on_key,
      p.type_lineup,
      p.game_year,
      SUM(p.total_pts)  AS total_pts,
      SUM(p.total_poss) AS total_poss,
      ROUND(
        SUM(p.total_pts) / NULLIF(SUM(p.total_poss), 0)::numeric * 100, 1
      ) AS ppp_calc
    FROM basketball_test.player_onoff_by_game p
    JOIN sched s ON s.game_id = p.game_id AND s.team_id = p.team_id
    GROUP BY p.player_id, p.team_id, p.is_on_key, p.type_lineup, p.game_year
  ),

  -- PPP percentile per (type_lineup, game_year)
  ppp_rank_base AS (
    SELECT
      a.*,
      PERCENT_RANK() OVER (
        PARTITION BY a.type_lineup, a.game_year
        ORDER BY a.ppp_calc
      ) AS pr_ppp_raw
    FROM agg a
  ),
  ppp_ranked AS (
    SELECT
      p.*,
      CASE
        WHEN p.type_lineup = 'defense' THEN 1 - p.pr_ppp_raw
        ELSE p.pr_ppp_raw
      END AS pr_ppp_better
    FROM ppp_rank_base p
  ),

  -- attach names + team_name + game_year
  with_names AS (
    SELECT
      a.player_id,
      a.team_id,
      a.game_year,
      a.is_on_key,
      a.type_lineup,
      a.total_pts,
      a.total_poss,
      a.ppp_calc,
      a.pr_ppp_raw,
      a.pr_ppp_better,
      r.firstname,
      r.lastname,
      r.team_name
    FROM ppp_ranked a
    JOIN (
      SELECT DISTINCT
        fr.player_id,
        fr.team_id,
        fr.firstname,
        fr.lastname,
        fr.team_name,
        fs.game_year
      FROM basketball_test.full_rosters fr
      JOIN basketball_test.final_schedule_mv fs
        ON fs.game_id = fr.game_id
       AND fs.team_id = fr.team_id
    ) r USING (player_id, team_id, game_year)
  ),

  -- eligibility per player-team-year
  elig AS (
    SELECT
      wn.player_id,
      wn.team_id,
      wn.game_year,
      MIN(wn.total_poss) AS min_poss_all,
      MAX(CASE WHEN wn.is_on_key = 1 THEN wn.total_poss ELSE 0 END) AS max_poss_on
    FROM with_names wn
    GROUP BY wn.player_id, wn.team_id, wn.game_year
  ),

  -- apply min_all/min_on filters
  filtered AS (
    SELECT
      wn.player_id,
      wn.team_id,
      wn.game_year,
      wn.is_on_key,
      wn.type_lineup,
      wn.total_pts,
      wn.total_poss,
      wn.ppp_calc,
      wn.pr_ppp_raw,
      wn.pr_ppp_better,
      wn.firstname,
      wn.lastname,
      wn.team_name
    FROM with_names wn
    JOIN elig e USING (player_id, team_id, game_year)
    WHERE e.min_poss_all >= p_min_all
      AND e.max_poss_on  >= p_min_on
  ),

  -- ON/OFF diff per type, per year
  step1 AS (
    SELECT
      f.player_id,
      f.team_id,
      f.game_year,
      f.is_on_key,
      f.type_lineup,
      f.total_pts,
      f.total_poss,
      f.ppp_calc,
      f.pr_ppp_raw,
      f.pr_ppp_better,
      f.firstname,
      f.lastname,
      f.team_name,
      CASE
        WHEN f.type_lineup = 'offense' THEN 1
        WHEN f.type_lineup = 'defense' THEN 2
        ELSE 3
      END AS type_key,
      f.ppp_calc
        - LAG(f.ppp_calc) OVER (
            PARTITION BY f.player_id, f.team_id, f.type_lineup, f.game_year
            ORDER BY f.is_on_key
          ) AS net_rtg
    FROM filtered f
  ),

  -- rank ON net_rtg within (type_lineup, game_year)
  step1_on_rank AS (
    SELECT
      s1.player_id,
      s1.team_id,
      s1.type_lineup,
      s1.game_year,
      s1.is_on_key,
      PERCENT_RANK() OVER (
        PARTITION BY s1.type_lineup, s1.game_year
        ORDER BY s1.net_rtg
      ) AS pr_net_rtg_raw,
      CASE
        WHEN s1.type_lineup = 'defense' THEN
          1 - PERCENT_RANK() OVER (
                PARTITION BY s1.type_lineup, s1.game_year
                ORDER BY s1.net_rtg
              )
        ELSE
          PERCENT_RANK() OVER (
            PARTITION BY s1.type_lineup, s1.game_year
            ORDER BY s1.net_rtg
          )
      END AS pr_net_rtg_better
    FROM step1 s1
    WHERE s1.is_on_key = 1
      AND s1.net_rtg IS NOT NULL
  ),

  step1_joined AS (
    SELECT
      s1.player_id,
      s1.team_id,
      s1.game_year,
      s1.is_on_key,
      s1.type_lineup,
      s1.total_pts,
      s1.total_poss,
      s1.ppp_calc,
      s1.pr_ppp_raw,
      s1.pr_ppp_better,
      s1.firstname,
      s1.lastname,
      s1.team_name,
      s1.type_key,
      s1.net_rtg,
      r.pr_net_rtg_raw,
      r.pr_net_rtg_better
    FROM step1 s1
    LEFT JOIN step1_on_rank r
      ON r.player_id   = s1.player_id
     AND r.team_id     = s1.team_id
     AND r.type_lineup = s1.type_lineup
     AND r.is_on_key   = s1.is_on_key
     AND r.game_year   = s1.game_year
  ),

  -- total_net_rtg = offense_net_rtg - defense_net_rtg
  step2 AS (
    SELECT
      s1j.player_id,
      s1j.team_id,
      s1j.game_year,
      s1j.is_on_key,
      s1j.type_lineup,
      s1j.total_pts,
      s1j.total_poss,
      s1j.ppp_calc,
      s1j.pr_ppp_raw,
      s1j.pr_ppp_better,
      s1j.firstname,
      s1j.lastname,
      s1j.team_name,
      s1j.type_key,
      s1j.net_rtg,
      s1j.pr_net_rtg_raw,
      s1j.pr_net_rtg_better,
      ROUND(
        LAG(s1j.net_rtg) OVER (
          PARTITION BY s1j.player_id, s1j.team_id, s1j.is_on_key, s1j.game_year
          ORDER BY s1j.type_key
        ) - s1j.net_rtg,
        2
      ) AS total_net_rtg
    FROM step1_joined s1j
  ),

  -- percentile of total_net_rtg per game_year
  step2_rank AS (
    SELECT
      s2.player_id,
      s2.team_id,
      s2.type_lineup,
      s2.game_year,
      s2.is_on_key,
      PERCENT_RANK() OVER (
        PARTITION BY s2.game_year
        ORDER BY s2.total_net_rtg
      ) AS pr_total_net
    FROM step2 s2
    WHERE s2.total_net_rtg IS NOT NULL
  ),

  step2_joined AS (
    SELECT
      s2.player_id,
      s2.team_id,
      s2.game_year,
      s2.is_on_key,
      s2.type_lineup,
      s2.total_pts,
      s2.total_poss,
      s2.ppp_calc,
      s2.pr_ppp_raw,
      s2.pr_ppp_better,
      s2.firstname,
      s2.lastname,
      s2.team_name,
      s2.type_key,
      s2.net_rtg,
      s2.pr_net_rtg_raw,
      s2.pr_net_rtg_better,
      s2.total_net_rtg,
      r.pr_total_net
    FROM step2 s2
    LEFT JOIN step2_rank r
      ON r.player_id   = s2.player_id
     AND r.team_id     = s2.team_id
     AND r.type_lineup = s2.type_lineup
     AND r.is_on_key   = s2.is_on_key
     AND r.game_year   = s2.game_year
  ),

  -- collapse to one row per (player, team, year)
  final_rows AS (
    SELECT
      s2j.player_id,
      s2j.team_id,
      s2j.game_year,
      s2j.team_name,
      s2j.firstname,
      s2j.lastname,

      MAX(CASE WHEN s2j.type_lineup = 'offense' AND s2j.is_on_key = 1 THEN s2j.ppp_calc END) AS offense_on_ppp,
      MAX(CASE WHEN s2j.type_lineup = 'offense' AND s2j.is_on_key = 0 THEN s2j.ppp_calc END) AS offense_off_ppp,
      MAX(CASE WHEN s2j.type_lineup = 'defense' AND s2j.is_on_key = 1 THEN s2j.ppp_calc END) AS defense_on_ppp,
      MAX(CASE WHEN s2j.type_lineup = 'defense' AND s2j.is_on_key = 0 THEN s2j.ppp_calc END) AS defense_off_ppp,

      MAX(CASE WHEN s2j.type_lineup = 'offense' AND s2j.is_on_key = 1 THEN s2j.pr_ppp_better END) AS pr_off_on,
      MAX(CASE WHEN s2j.type_lineup = 'offense' AND s2j.is_on_key = 0 THEN s2j.pr_ppp_better END) AS pr_off_off,
      MAX(CASE WHEN s2j.type_lineup = 'defense' AND s2j.is_on_key = 1 THEN s2j.pr_ppp_better END) AS pr_def_on_inv,
      MAX(CASE WHEN s2j.type_lineup = 'defense' AND s2j.is_on_key = 0 THEN s2j.pr_ppp_better END) AS pr_def_off_inv,

      MAX(CASE WHEN s2j.type_lineup = 'offense' AND s2j.is_on_key = 1 THEN s2j.net_rtg END) AS offense_on_diff,
      MAX(CASE WHEN s2j.type_lineup = 'defense' AND s2j.is_on_key = 1 THEN s2j.net_rtg END) AS defense_on_diff,

      MAX(CASE WHEN s2j.type_lineup = 'offense' AND s2j.is_on_key = 1 THEN s2j.pr_net_rtg_better END) AS pr_off_on_d,
      MAX(CASE WHEN s2j.type_lineup = 'defense' AND s2j.is_on_key = 1 THEN s2j.pr_net_rtg_raw END)    AS pr_def_on_d,
      MAX(CASE WHEN s2j.type_lineup = 'defense' AND s2j.is_on_key = 1 THEN s2j.pr_net_rtg_better END) AS pr_def_on_d_inv,

      MAX(s2j.total_net_rtg) AS total_net_rtg,
      MAX(s2j.pr_total_net)  AS pr_net,

      MAX(CASE WHEN s2j.is_on_key = 1 THEN s2j.total_poss END) AS on_poss,
      MAX(CASE WHEN s2j.is_on_key = 0 THEN s2j.total_poss END) AS off_poss
    FROM step2_joined s2j
    GROUP BY
      s2j.player_id,
      s2j.team_id,
      s2j.game_year,
      s2j.team_name,
      s2j.firstname,
      s2j.lastname
  ),

  final_scored AS (
    SELECT
      fr.player_id,
      fr.team_id,
      fr.game_year,
      fr.team_name,
      fr.firstname,
      fr.lastname,
      fr.offense_on_ppp,
      fr.offense_off_ppp,
      fr.defense_on_ppp,
      fr.defense_off_ppp,
      fr.pr_off_on,
      fr.pr_off_off,
      fr.pr_def_on_inv,
      fr.pr_def_off_inv,
      fr.offense_on_diff,
      fr.defense_on_diff,
      fr.pr_off_on_d,
      fr.pr_def_on_d,
      fr.pr_def_on_d_inv,
      fr.total_net_rtg,
      fr.pr_net,
      fr.on_poss,
      fr.off_poss,
      fr.offense_on_ppp  - fr.defense_on_ppp  AS on_net_rtg,
      fr.offense_off_ppp - fr.defense_off_ppp AS off_net_rtg,
      PERCENT_RANK() OVER (
        PARTITION BY fr.game_year
        ORDER BY (fr.offense_on_ppp - fr.defense_on_ppp)
      ) AS pr_on_net,
      PERCENT_RANK() OVER (
        PARTITION BY fr.game_year
        ORDER BY (fr.offense_off_ppp - fr.defense_off_ppp)
      ) AS pr_off_net
    FROM final_rows fr
  )
  SELECT
    fs.team_name   AS "Team",
    fs.firstname   AS "First Name",
    fs.lastname    AS "Last Name",
    fs.total_net_rtg AS "Net RTG Diff",
    fs.offense_on_diff AS "Off ON Diff",
    fs.defense_on_diff AS "Def ON Diff",
    fs.offense_on_ppp  AS "Off ON PPP",
    fs.defense_on_ppp  AS "Def ON PPP",
    fs.on_net_rtg      AS "On Net RTG",
    fs.offense_off_ppp AS "Off OFF PPP",
    fs.defense_off_ppp AS "Def OFF PPP",
    fs.off_net_rtg     AS "Off Net RTG",
    fs.on_poss         AS "ON Poss",
    fs.off_poss        AS "OFF Poss",
    fs.pr_net,
    fs.pr_off_on,
    fs.pr_off_off,
    fs.pr_def_on_inv,
    fs.pr_def_off_inv,
    fs.pr_off_on_d,
    fs.pr_def_on_d,
    fs.pr_def_on_d_inv,
    fs.pr_on_net,
    fs.pr_off_net,
    fs.player_id,
    fs.team_id
  FROM final_scored fs
  WHERE fs.total_net_rtg >= p_min_net
  ORDER BY "Net RTG Diff" DESC, "Team", "Last Name", "First Name";
END;
$$;
