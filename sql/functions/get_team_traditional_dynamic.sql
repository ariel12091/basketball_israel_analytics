-- Team traditional (box-score) totals for Tab 3's Traditional view, counted
-- from the raw action rows under the shared game-context and clutch filters.
-- p_side = 'offense' returns the team's own stats, 'defense' its opponents'.
--
-- Moved 2026-10-12 from an inline query in app/R/server_tab3.R
-- (run_team_traditional_dynamic). For the game filters NULL and '' both mean
-- "no filter", as they did there.
DROP FUNCTION IF EXISTS basketball_test.get_team_traditional_dynamic;

CREATE OR REPLACE FUNCTION basketball_test.get_team_traditional_dynamic(
    p_game_year          INT,
    p_start_date         DATE DEFAULT NULL,
    p_end_date           DATE DEFAULT NULL,
    p_game_type_csv      TEXT DEFAULT NULL,
    p_opp_team_ids_csv   TEXT DEFAULT NULL,
    p_home_away          TEXT DEFAULT NULL,
    p_outcome            TEXT DEFAULT NULL,
    p_opp_rank_side      TEXT DEFAULT NULL,
    p_opp_rank_n         INT  DEFAULT NULL,
    p_opp_rank_metric    TEXT DEFAULT NULL,
    p_max_margin         INT  DEFAULT NULL,
    p_margin_status      TEXT DEFAULT NULL,
    p_max_time_remaining INT  DEFAULT NULL,
    p_ot_margin_filter   BOOLEAN DEFAULT NULL,
    p_min_gn             INT  DEFAULT NULL,
    p_max_gn             INT  DEFAULT NULL,
    p_last_n_games       INT  DEFAULT NULL,
    p_side               TEXT DEFAULT 'offense'
)
RETURNS TABLE (
    team_id       INT,
    team_name     TEXT,
    gp            INT,
    poss_on_floor INT,
    minutes       NUMERIC,
    pts           INT,
    reb           INT,
    oreb          INT,
    dreb          INT,
    ast           INT,
    stl           INT,
    blk           INT,
    dfl           INT,
    tov           INT,
    fgm           INT,
    fga           INT,
    "2pm"         INT,
    "2pa"         INT,
    "3pm"         INT,
    "3pa"         INT,
    ftm           INT,
    fta           INT,
    fg_pct        NUMERIC,
    two_pct       NUMERIC,
    tp_pct        NUMERIC,
    ft_pct        NUMERIC,
    efg           NUMERIC,
    ts            NUMERIC
)
LANGUAGE sql
STABLE
SET plan_cache_mode = force_custom_plan
AS $$
  WITH params AS (
    SELECT
      CASE WHEN p_game_type_csv::text IS NULL OR btrim(p_game_type_csv::text) = '' THEN NULL::int4[]
           ELSE string_to_array(regexp_replace(p_game_type_csv::text, '\s+', '', 'g'), ',')::int4[] END AS game_types,
      CASE WHEN p_opp_team_ids_csv::text IS NULL OR btrim(p_opp_team_ids_csv::text) = '' THEN NULL::int4[]
           ELSE string_to_array(regexp_replace(p_opp_team_ids_csv::text, '\s+', '', 'g'), ',')::int4[] END AS opp_ids,
      (p_max_margin IS NOT NULL OR p_max_time_remaining IS NOT NULL
       OR COALESCE(NULLIF(btrim(p_margin_status), ''), 'all') <> 'all') AS clutch_active
  ),
  season_recency AS (
    SELECT
      fsr.game_id, fsr.team_id, fsr.game_year,
      ROW_NUMBER() OVER (PARTITION BY fsr.team_id, fsr.game_year ORDER BY fsr.game_date DESC NULLS LAST, fsr.game_id DESC) AS rn_recent
    FROM basketball_test.final_schedule_mv fsr
    WHERE fsr.game_year = p_game_year::int4
  ),
  games_base AS (
    SELECT
      fs.game_id, fs.team_id, fs.game_year, fs.opp_team_id, fs.game_date, sr.rn_recent
    FROM basketball_test.final_schedule_mv fs
    JOIN season_recency sr
      ON sr.game_id = fs.game_id AND sr.team_id = fs.team_id AND sr.game_year = fs.game_year
    CROSS JOIN params p0
    WHERE fs.game_year = p_game_year::int4
      AND (p_start_date::date IS NULL OR fs.game_date >= p_start_date::date)
      AND (p_end_date::date IS NULL OR fs.game_date <= p_end_date::date)
      AND (p0.game_types IS NULL OR fs.game_type = ANY(p0.game_types))
      AND (p0.opp_ids IS NULL OR fs.opp_team_id = ANY(p0.opp_ids))
      AND (p_home_away::text IS NULL OR p_home_away::text = '' OR (p_home_away::text = 'home' AND fs.is_home) OR (p_home_away::text = 'away' AND NOT fs.is_home))
      AND (p_outcome::text IS NULL OR p_outcome::text = '' OR (p_outcome::text = 'win' AND fs.has_won IS TRUE) OR (p_outcome::text = 'loss' AND fs.has_won IS FALSE))
      AND (p_min_gn::int4 IS NULL OR fs.gn >= p_min_gn::int4)
      AND (p_max_gn::int4 IS NULL OR fs.gn <= p_max_gn::int4)
  ),
  games_last_n AS (
    SELECT *
    FROM games_base
    WHERE (p_last_n_games::int4 IS NULL OR rn_recent <= p_last_n_games::int4)
  ),
  games_ranked AS (
    SELECT
      gb.game_id, gb.team_id, gb.game_year,
      CASE
        WHEN p_opp_rank_side::text IN ('top','bottom') THEN
          CASE COALESCE(p_opp_rank_metric::text, 'net')
            WHEN 'off' THEN r.rank_off_ppp
            WHEN 'def' THEN r.rank_def_ppp
            ELSE r.rank_net_rtg
          END
        ELSE NULL
      END AS opp_rank,
      CASE
        WHEN p_opp_rank_side::text = 'bottom' THEN
          MAX(
            CASE COALESCE(p_opp_rank_metric::text, 'net')
              WHEN 'off' THEN r.rank_off_ppp
              WHEN 'def' THEN r.rank_def_ppp
              ELSE r.rank_net_rtg
            END
          ) OVER (PARTITION BY gb.game_year)
        ELSE NULL
      END AS max_rank
    FROM games_last_n gb
    LEFT JOIN basketball_test.team_ppp_ratings_mv r
      ON r.game_year::int4 = gb.game_year
     AND r.team_id::int4 = gb.opp_team_id
     AND p_opp_rank_side::text IN ('top','bottom')
  ),
  games_filtered AS (
    SELECT gr.game_id, gr.team_id
    FROM games_ranked gr
    WHERE p_opp_rank_side::text IS NULL OR p_opp_rank_side::text = '' OR p_opp_rank_n::int4 IS NULL
       OR (p_opp_rank_side::text = 'top' AND gr.opp_rank <= p_opp_rank_n::int4)
       OR (p_opp_rank_side::text = 'bottom' AND gr.opp_rank >= (gr.max_rank - p_opp_rank_n::int4 + 1))
  ),
  acts AS (
     SELECT d.id, d.game_id, d.team_id, d.lineup_hash, d.segment_id, d.end_game_seconds_remaining, d.event_elapsed_seconds,
            d.segment_seconds,
            d.type, d.parameters_type, d.parameters_made, d.parameters_points, d.event_owner_side, d.type_lineup, d.final_end_poss, d.quarter,
            d.own_team_score, d.opp_team_score, d.parent_action_id
    FROM basketball_test.df_pts_poss_lineups_longer_mv d
    JOIN games_filtered gf ON gf.game_id = d.game_id AND gf.team_id = d.team_id
    -- Pre-shot margin, as in get_team_ratings_dynamic: the points scored on
    -- this row are taken back out of the scoring side's total, so a shot is
    -- judged by the score it was taken at, not the score it produced.
    WHERE (
      p_max_margin::int4 IS NULL
      OR ABS(CASE WHEN d.type_lineup = 'offense'
                  THEN (d.own_team_score - COALESCE(d.team_score, 0)) - d.opp_team_score
                  ELSE d.own_team_score - (d.opp_team_score - COALESCE(d.team_score, 0))
             END) <= p_max_margin::int4
      OR (d.quarter > 4 AND NOT COALESCE(p_ot_margin_filter::bool, FALSE))
    )
    AND (
      p_margin_status::text IS NULL OR p_margin_status::text = '' OR p_margin_status::text = 'all'
      OR (p_margin_status::text = 'leading' AND
          CASE WHEN d.type_lineup = 'offense'
               THEN (d.own_team_score - COALESCE(d.team_score, 0)) > d.opp_team_score
               ELSE d.own_team_score > (d.opp_team_score - COALESCE(d.team_score, 0))
          END)
      OR (p_margin_status::text = 'trailing' AND
          CASE WHEN d.type_lineup = 'offense'
               THEN (d.own_team_score - COALESCE(d.team_score, 0)) < d.opp_team_score
               ELSE d.own_team_score < (d.opp_team_score - COALESCE(d.team_score, 0))
          END)
      OR (p_margin_status::text = 'tied' AND
          CASE WHEN d.type_lineup = 'offense'
               THEN (d.own_team_score - COALESCE(d.team_score, 0)) = d.opp_team_score
               ELSE d.own_team_score = (d.opp_team_score - COALESCE(d.team_score, 0))
          END)
      OR (d.quarter > 4 AND NOT COALESCE(p_ot_margin_filter::bool, FALSE))
    )
    AND (p_max_time_remaining::int4 IS NULL OR d.end_game_seconds_remaining <= p_max_time_remaining::int4 OR d.quarter > 4)
  ),
  team_stats AS (
    SELECT
      a.team_id,
      (
         SUM(CASE WHEN a.type = 'shot' AND a.parameters_made = 'made' AND (
                (p_side::text = 'offense' AND a.type_lineup = 'offense') OR
                (p_side::text = 'defense' AND a.type_lineup = 'defense')
             ) THEN COALESCE(a.parameters_points, 0) ELSE 0 END)
         + SUM(CASE WHEN a.type = 'freeThrow' AND a.parameters_made = 'made' AND (
                (p_side::text = 'offense' AND a.type_lineup = 'offense') OR
                (p_side::text = 'defense' AND a.type_lineup = 'defense')
             ) THEN 1 ELSE 0 END)
        )::int AS pts,
        SUM(CASE WHEN a.type = 'rebound' AND (
                (p_side::text = 'offense' AND a.parameters_type = 'offensive' AND a.type_lineup = 'offense') OR
                (p_side::text = 'defense' AND a.parameters_type = 'offensive' AND a.type_lineup = 'defense')
              ) THEN 1 ELSE 0 END)::int AS oreb,
        SUM(CASE WHEN a.type = 'rebound' AND (
                (p_side::text = 'offense' AND a.parameters_type = 'defensive' AND a.type_lineup = 'defense') OR
                (p_side::text = 'defense' AND a.parameters_type = 'defensive' AND a.type_lineup = 'offense')
              ) THEN 1 ELSE 0 END)::int AS dreb,
       SUM(CASE WHEN a.type = 'assist' AND (
                (p_side::text = 'offense' AND a.type_lineup = 'offense') OR
                (p_side::text = 'defense' AND a.type_lineup = 'defense')
              ) THEN 1 ELSE 0 END)::int AS ast,
        SUM(CASE WHEN a.type = 'steal' AND (
                 (p_side::text = 'offense' AND a.type_lineup = 'defense') OR
                 (p_side::text = 'defense' AND a.type_lineup = 'offense')
               ) THEN 1 ELSE 0 END)::int AS stl,
        SUM(CASE WHEN a.type = 'block' AND (
                 (p_side::text = 'offense' AND a.type_lineup = 'defense') OR
                 (p_side::text = 'defense' AND a.type_lineup = 'offense')
               ) THEN 1 ELSE 0 END)::int AS blk,
        SUM(CASE WHEN a.type = 'deflection' AND (
                 (p_side::text = 'offense' AND a.type_lineup = 'defense') OR
                 (p_side::text = 'defense' AND a.type_lineup = 'offense')
               ) THEN 1 ELSE 0 END)::int AS dfl,
       SUM(CASE WHEN a.type = 'turnover' AND (
                (p_side::text = 'offense' AND a.type_lineup = 'offense') OR
                (p_side::text = 'defense' AND a.type_lineup = 'defense')
              ) THEN 1 ELSE 0 END)::int AS tov,
       SUM(CASE WHEN a.type = 'shot' AND a.parameters_made = 'made' AND (
                (p_side::text = 'offense' AND a.type_lineup = 'offense') OR
                (p_side::text = 'defense' AND a.type_lineup = 'defense')
              ) THEN 1 ELSE 0 END)::int AS fgm,
       SUM(CASE WHEN a.type = 'shot' AND (
                (p_side::text = 'offense' AND a.type_lineup = 'offense') OR
                (p_side::text = 'defense' AND a.type_lineup = 'defense')
              ) THEN 1 ELSE 0 END)::int AS fga,
       SUM(CASE WHEN a.type = 'shot' AND a.parameters_made = 'made' AND a.parameters_points = 3 AND (
                (p_side::text = 'offense' AND a.type_lineup = 'offense') OR
                (p_side::text = 'defense' AND a.type_lineup = 'defense')
              ) THEN 1 ELSE 0 END)::int AS "3pm",
       SUM(CASE WHEN a.type = 'shot' AND a.parameters_points = 3 AND (
                (p_side::text = 'offense' AND a.type_lineup = 'offense') OR
                (p_side::text = 'defense' AND a.type_lineup = 'defense')
              ) THEN 1 ELSE 0 END)::int AS "3pa",
       SUM(CASE WHEN a.type = 'freeThrow' AND a.parameters_made = 'made' AND (
                (p_side::text = 'offense' AND a.type_lineup = 'offense') OR
                (p_side::text = 'defense' AND a.type_lineup = 'defense')
              ) THEN 1 ELSE 0 END)::int AS ftm,
       SUM(CASE WHEN a.type = 'freeThrow' AND (
                (p_side::text = 'offense' AND a.type_lineup = 'offense') OR
                (p_side::text = 'defense' AND a.type_lineup = 'defense')
              ) THEN 1 ELSE 0 END)::int AS fta
    FROM acts a
    GROUP BY a.team_id
  ),
  ft_trips AS (
    SELECT a.team_id, COUNT(DISTINCT (a.game_id, a.parent_action_id))::int AS trips
    FROM acts a
    JOIN basketball_test.df_pts_poss_lineups_longer_mv parent
      ON parent.game_id = a.game_id AND parent.id = a.parent_action_id
     AND parent.team_id = a.team_id AND parent.type_lineup = a.type_lineup
     AND parent.type = 'foul' AND parent.parameters_type = 'personal'
    WHERE a.type = 'freeThrow' AND a.type_lineup = p_side::text
    GROUP BY a.team_id
  ),
  poss_end AS (
    SELECT DISTINCT a.game_id, a.team_id, a.id AS poss_end_id
    FROM acts a
    WHERE a.type_lineup = p_side::text AND a.final_end_poss AND a.id IS NOT NULL
  ),
  team_usage AS (
    SELECT
      pe.team_id,
      COUNT(DISTINCT (pe.game_id, pe.team_id, pe.poss_end_id))::int AS poss_on_floor
    FROM poss_end pe
    GROUP BY pe.team_id
  ),
  -- A game counts when any action survives the filters, as games_played does
  -- in get_team_ratings_dynamic. Counting only games with a completed
  -- possession dropped games where the team merely defended inside a clutch
  -- window, while their stats stayed in the totals.
  team_games AS (
    SELECT a.team_id, COUNT(DISTINCT a.game_id)::int AS gp
    FROM acts a
    GROUP BY a.team_id
  ),
  -- Floor time. With no clutch window the canonical segment clock applies: one
  -- MAX(segment_seconds) per segment, with type_lineup absent from the GROUP
  -- BY (CLAUDE.md, "Floor time"). A clutch window covers only part of a
  -- segment, so there the measure is the time between the first and last
  -- surviving event, as in get_player_traditional_custom_clutch.
  seg_times AS (
    SELECT
      a.team_id, a.game_id, a.lineup_hash, a.segment_id,
      CASE
        WHEN p0.clutch_active THEN
          GREATEST(
            (array_agg(a.event_elapsed_seconds ORDER BY a.id DESC))[1] -
            (array_agg(a.event_elapsed_seconds ORDER BY a.id))[1],
            0
          )::numeric
        ELSE MAX(a.segment_seconds)::numeric
      END AS seg_seconds
    FROM acts a
    CROSS JOIN params p0
    WHERE a.lineup_hash IS NOT NULL AND a.segment_id IS NOT NULL
      AND CASE WHEN p0.clutch_active THEN a.event_elapsed_seconds::numeric
               ELSE a.segment_seconds::numeric END IS NOT NULL
    GROUP BY a.team_id, a.game_id, a.lineup_hash, a.segment_id, p0.clutch_active
  ),
  team_minutes AS (
    SELECT st.team_id, ROUND(SUM(COALESCE(st.seg_seconds, 0))::numeric / 60.0, 1) AS minutes
    FROM seg_times st
    GROUP BY st.team_id
  ),
  team_names AS (
    SELECT fr.team_id, MIN(btrim(fr.team_name)) AS team_name
    FROM basketball_test.full_rosters fr
    WHERE fr.game_year = p_game_year::int4
    GROUP BY fr.team_id
  )
  SELECT
    ts.team_id, tn.team_name,
    COALESCE(tg.gp, 0)::int AS gp,
    COALESCE(tu.poss_on_floor, 0)::int AS poss_on_floor,
    COALESCE(tm.minutes, 0)::numeric AS minutes,
    ts.pts, (ts.oreb + ts.dreb)::int AS reb, ts.oreb, ts.dreb, ts.ast, ts.stl, ts.blk, ts.dfl, ts.tov,
    ts.fgm, ts.fga, (ts.fgm - ts."3pm")::int AS "2pm", (ts.fga - ts."3pa")::int AS "2pa",
    ts."3pm", ts."3pa", ts.ftm, ts.fta,
    CASE WHEN ts.fga > 0 THEN ROUND((ts.fgm::numeric / ts.fga::numeric) * 100, 1) ELSE NULL END AS fg_pct,
    CASE WHEN (ts.fga - ts."3pa") > 0 THEN ROUND(((ts.fgm - ts."3pm")::numeric / (ts.fga - ts."3pa")::numeric) * 100, 1) ELSE NULL END AS two_pct,
    CASE WHEN ts."3pa" > 0 THEN ROUND((ts."3pm"::numeric / ts."3pa"::numeric) * 100, 1) ELSE NULL END AS tp_pct,
    CASE WHEN ts.fta > 0 THEN ROUND((ts.ftm::numeric / ts.fta::numeric) * 100, 1) ELSE NULL END AS ft_pct,
    CASE WHEN ts.fga > 0 THEN ROUND(((ts.fgm::numeric + 0.5 * ts."3pm"::numeric) / ts.fga::numeric) * 100, 1) ELSE NULL END AS efg,
    CASE WHEN (ts.fga + COALESCE(ft.trips, 0)) > 0 THEN ROUND((ts.pts::numeric / (2.0 * (ts.fga + COALESCE(ft.trips, 0))::numeric)) * 100, 1) ELSE NULL END AS ts
  FROM team_stats ts
  LEFT JOIN team_usage tu ON tu.team_id = ts.team_id
  LEFT JOIN team_games tg ON tg.team_id = ts.team_id
  LEFT JOIN team_minutes tm ON tm.team_id = ts.team_id
  LEFT JOIN ft_trips ft ON ft.team_id = ts.team_id
  LEFT JOIN team_names tn ON tn.team_id = ts.team_id
  WHERE tn.team_name IS NOT NULL
  ORDER BY ts.pts DESC, tm.minutes DESC, tn.team_name
$$;
