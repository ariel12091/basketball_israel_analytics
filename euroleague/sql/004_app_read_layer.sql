-- EUROLEAGUE APP READ LAYER: player on/off summary and four factors.
--
-- Prepared 2026-08-06. Apply only to the isolated euroleague schema.
--
-- Purpose: give the Shiny app the same column contract as the Israeli
-- basketball_test.onoff_compute() / four_factors_compute() so Tab 1 rendering
-- can be reused, while sourcing everything from euroleague per-game facts.
--
-- Deliberate contract differences from the Israeli functions, all documented:
--   * p_competition ('E'/'U') is a EuroLeague-only filter.
--   * p_game_year is the PROVIDER season (2025 = the 2025-26 season). The
--     EuroLeague section owns its own season selector, so no +1 adapter applies.
--   * p_phase_csv replaces p_game_type_csv: EuroLeague phases are text
--     ('REGULAR SEASON', 'PLAYOFFS', ...), not Israeli integer game types.
--   * p_min_gn / p_max_gn filter schedule.round_number, the real analogue of
--     the Israeli game number. gamecode is a season-wide identifier, not a round.
--   * def_on_disruptions / def_off_disruptions carry STEALS ONLY. EuroLeague
--     play-by-play has no deflection event, so euroleague.player_four_factors_by_game
--     stores deflection_count = 0. The UI must label these "Steals", not
--     "Disruptions".
--   * Shot-profile columns (layup/dunk/corner-3) are intentionally absent. That
--     view mode gets its own design once the shots endpoint is collected; the
--     2PT/3PT splits below come from play_type and need no coordinates.

BEGIN;

-- ---------------------------------------------------------------------------
-- 1. Shared context view: per-game player facts joined to schedule context.
--    Both filtered-path functions and both season aggregates read this.
-- ---------------------------------------------------------------------------

CREATE OR REPLACE VIEW euroleague.player_game_context AS
SELECT
  pf.player_id,
  pf.team_id,
  pf.game_id,
  pf.game_year,
  pf.is_on_key,
  pf.type_lineup,
  pf.own_starters,
  pf.opp_starters,
  pf.total_points,
  pf.total_poss,
  pf.ts_poss_count,
  pf.oreb_count,
  pf.oreb_opportunities,
  pf.tov_count,
  pf.steal_count,
  pf.deflection_count,
  pf.total_ft_attempts,
  pf.total_fga,
  pf.total_fgm,
  pf.total_fg3_made,
  pf.fg2_made,
  pf.fg2_att,
  pf.fg3_made,
  pf.fg3_att,
  pf.onoff_minutes,
  s.competition,
  s.round_number,
  s.phase,
  s.scheduled_at::date AS game_date,
  fs.opp_team_id,
  fs.is_home,
  fs.has_won
FROM euroleague.player_four_factors_by_game pf
JOIN euroleague.schedule s ON s.game_id = pf.game_id
JOIN euroleague.final_schedule fs
  ON fs.game_id = pf.game_id AND fs.team_id = pf.team_id;

COMMENT ON VIEW euroleague.player_game_context IS
  'Per-game player ON/OFF facts with schedule context; single source for the app read layer.';

-- ---------------------------------------------------------------------------
-- 2. Filtered path: player on/off summary.
--    Mirrors basketball_test.onoff_compute column-for-column, minus the
--    shot-profile block.
-- ---------------------------------------------------------------------------

DROP FUNCTION IF EXISTS euroleague.onoff_compute(
  text, int4, date, date, text, text, text, text, text, text, int4, text,
  int4, int4, int4, int4, int4, int4, int4, numeric, int4, int4);

CREATE OR REPLACE FUNCTION euroleague.onoff_compute(
    p_competition          TEXT,
    p_game_year            INTEGER,
    p_start_date           DATE    DEFAULT NULL,
    p_end_date             DATE    DEFAULT NULL,
    p_team_ids_csv         TEXT    DEFAULT NULL,
    p_phase_csv            TEXT    DEFAULT NULL,
    p_opp_ids_csv          TEXT    DEFAULT NULL,
    p_home_away            TEXT    DEFAULT 'all',
    p_outcome              TEXT    DEFAULT 'all',
    p_opp_rank_side        TEXT    DEFAULT NULL,
    p_opp_rank_n           INTEGER DEFAULT NULL,
    p_opp_rank_metric      TEXT    DEFAULT NULL,
    p_min_gn               INTEGER DEFAULT NULL,
    p_max_gn               INTEGER DEFAULT NULL,
    p_last_n_games         INTEGER DEFAULT NULL,
    p_num_starters_off_min INTEGER DEFAULT NULL,
    p_num_starters_off_max INTEGER DEFAULT NULL,
    p_num_starters_def_min INTEGER DEFAULT NULL,
    p_num_starters_def_max INTEGER DEFAULT NULL,
    p_min_net              NUMERIC DEFAULT NULL,
    p_min_all              INTEGER DEFAULT 0,
    p_min_on               INTEGER DEFAULT 0
)
RETURNS TABLE (
    "Team" text, "First Name" text, "Last Name" text,
    "Net RTG Diff" numeric, "Off ON Diff" numeric, "Def ON Diff" numeric,
    "Off ON PPP" numeric, "Def ON PPP" numeric, "On Net RTG" numeric,
    "Off OFF PPP" numeric, "Def OFF PPP" numeric, "Off Net RTG" numeric,
    "ON Poss" numeric, "OFF Poss" numeric, minutes numeric,
    pr_net double precision, pr_off_on double precision, pr_off_off double precision,
    pr_def_on_inv double precision, pr_def_off_inv double precision,
    pr_off_on_d double precision, pr_def_on_d double precision, pr_def_on_d_inv double precision,
    pr_on_net double precision, pr_off_net double precision,
    player_id bigint, team_id bigint,
    off_on_fg2_made bigint, off_on_fg2_att bigint, off_on_fg3_made bigint, off_on_fg3_att bigint,
    off_off_fg2_made bigint, off_off_fg2_att bigint, off_off_fg3_made bigint, off_off_fg3_att bigint,
    def_on_fg2_made bigint, def_on_fg2_att bigint, def_on_fg3_made bigint, def_on_fg3_att bigint,
    def_off_fg2_made bigint, def_off_fg2_att bigint, def_off_fg3_made bigint, def_off_fg3_att bigint
)
LANGUAGE plpgsql
STABLE
SET plan_cache_mode = force_custom_plan
AS $function$
DECLARE
  v_competition   text;
  v_team_ids      bigint[];
  v_opp_ids       bigint[];
  v_phases        text[];
  v_home_away     text;
  v_outcome       text;
  v_rank_side     text;
  v_rank_metric   text;
BEGIN
  v_competition := COALESCE(NULLIF(btrim(p_competition), ''), 'E');
  v_home_away   := COALESCE(NULLIF(btrim(p_home_away), ''), 'all');
  v_outcome     := COALESCE(NULLIF(btrim(p_outcome), ''), 'all');
  v_rank_side   := NULLIF(btrim(p_opp_rank_side), '');
  v_rank_metric := COALESCE(NULLIF(btrim(p_opp_rank_metric), ''), 'net');

  IF p_team_ids_csv IS NOT NULL AND length(btrim(p_team_ids_csv)) > 0 THEN
    v_team_ids := ARRAY(
      SELECT DISTINCT x::bigint
      FROM unnest(string_to_array(regexp_replace(p_team_ids_csv, '\s+', '', 'g'), ',')) x
      WHERE x <> '' ORDER BY 1);
  END IF;

  IF p_opp_ids_csv IS NOT NULL AND length(btrim(p_opp_ids_csv)) > 0 THEN
    v_opp_ids := ARRAY(
      SELECT DISTINCT x::bigint
      FROM unnest(string_to_array(regexp_replace(p_opp_ids_csv, '\s+', '', 'g'), ',')) x
      WHERE x <> '' ORDER BY 1);
  END IF;

  IF p_phase_csv IS NOT NULL AND length(btrim(p_phase_csv)) > 0 THEN
    v_phases := ARRAY(
      SELECT DISTINCT btrim(x)
      FROM unnest(string_to_array(p_phase_csv, ',')) x
      WHERE btrim(x) <> '' ORDER BY 1);
  END IF;

  RETURN QUERY
  WITH schedule_ranked AS (
    -- Windowed last-N per team within the season (never a correlated subquery).
    SELECT
      fs.game_id, fs.team_id, fs.opp_team_id, fs.is_home, fs.has_won,
      s.round_number, s.phase, s.scheduled_at::date AS game_date,
      row_number() OVER (
        PARTITION BY fs.team_id
        ORDER BY s.scheduled_at DESC, fs.game_id DESC
      ) AS team_game_rank
    FROM euroleague.final_schedule fs
    JOIN euroleague.schedule s ON s.game_id = fs.game_id
    WHERE s.competition = v_competition
      AND s.season = p_game_year
  ),
  -- Opponent strength is recomputed from the selected window, matching the
  -- Israeli onoff_compute exception rather than a season-wide rank table.
  team_window AS (
    SELECT
      c.team_id,
      sum(c.total_points) FILTER (WHERE c.type_lineup = 'offense' AND c.is_on_key = 1) AS off_pts,
      sum(c.total_poss)   FILTER (WHERE c.type_lineup = 'offense' AND c.is_on_key = 1) AS off_poss,
      sum(c.total_points) FILTER (WHERE c.type_lineup = 'defense' AND c.is_on_key = 1) AS def_pts,
      sum(c.total_poss)   FILTER (WHERE c.type_lineup = 'defense' AND c.is_on_key = 1) AS def_poss
    FROM euroleague.player_game_context c
    JOIN schedule_ranked sr ON sr.game_id = c.game_id AND sr.team_id = c.team_id
    WHERE (p_start_date IS NULL OR sr.game_date >= p_start_date)
      AND (p_end_date   IS NULL OR sr.game_date <= p_end_date)
    GROUP BY c.team_id
  ),
  team_ranked AS (
    SELECT
      tw.team_id,
      dense_rank() OVER (ORDER BY 100.0 * tw.off_pts / NULLIF(tw.off_poss, 0) DESC) AS off_rank,
      dense_rank() OVER (ORDER BY 100.0 * tw.def_pts / NULLIF(tw.def_poss, 0) ASC)  AS def_rank,
      dense_rank() OVER (
        ORDER BY (100.0 * tw.off_pts / NULLIF(tw.off_poss, 0))
               - (100.0 * tw.def_pts / NULLIF(tw.def_poss, 0)) DESC
      ) AS net_rank
    FROM team_window tw
  ),
  games AS (
    SELECT sr.*
    FROM schedule_ranked sr
    LEFT JOIN team_ranked tr ON tr.team_id = sr.opp_team_id
    WHERE (p_start_date IS NULL OR sr.game_date >= p_start_date)
      AND (p_end_date   IS NULL OR sr.game_date <= p_end_date)
      AND (v_phases   IS NULL OR sr.phase = ANY(v_phases))
      AND (v_opp_ids  IS NULL OR sr.opp_team_id = ANY(v_opp_ids))
      AND (p_min_gn   IS NULL OR sr.round_number >= p_min_gn)
      AND (p_max_gn   IS NULL OR sr.round_number <= p_max_gn)
      AND (p_last_n_games IS NULL OR sr.team_game_rank <= p_last_n_games)
      AND (v_home_away = 'all'
           OR (v_home_away = 'home' AND sr.is_home)
           OR (v_home_away = 'away' AND NOT sr.is_home))
      AND (v_outcome = 'all'
           OR (v_outcome = 'win'  AND sr.has_won)
           OR (v_outcome = 'loss' AND NOT sr.has_won))
      AND (
        v_rank_side IS NULL OR p_opp_rank_n IS NULL
        OR (v_rank_side = 'top' AND
            CASE v_rank_metric WHEN 'off' THEN tr.off_rank
                               WHEN 'def' THEN tr.def_rank
                               ELSE tr.net_rank END <= p_opp_rank_n)
        OR (v_rank_side = 'bottom' AND
            CASE v_rank_metric WHEN 'off' THEN tr.off_rank
                               WHEN 'def' THEN tr.def_rank
                               ELSE tr.net_rank END
            > (SELECT count(*) FROM team_ranked) - p_opp_rank_n)
      )
  ),
  agg AS (
    SELECT
      c.player_id, c.team_id, c.is_on_key, c.type_lineup,
      sum(c.total_points)::numeric AS pts,
      sum(c.total_poss)::numeric   AS poss,
      sum(c.onoff_minutes)::numeric AS mins,
      sum(c.fg2_made)::bigint AS fg2_made,
      sum(c.fg2_att)::bigint  AS fg2_att,
      sum(c.fg3_made)::bigint AS fg3_made,
      sum(c.fg3_att)::bigint  AS fg3_att
    FROM euroleague.player_game_context c
    JOIN games g ON g.game_id = c.game_id AND g.team_id = c.team_id
    WHERE (p_num_starters_off_min IS NULL OR c.own_starters >= p_num_starters_off_min)
      AND (p_num_starters_off_max IS NULL OR c.own_starters <= p_num_starters_off_max)
      AND (p_num_starters_def_min IS NULL OR c.opp_starters >= p_num_starters_def_min)
      AND (p_num_starters_def_max IS NULL OR c.opp_starters <= p_num_starters_def_max)
      AND (v_team_ids IS NULL OR c.team_id = ANY(v_team_ids))
    GROUP BY c.player_id, c.team_id, c.is_on_key, c.type_lineup
  ),
  pivoted AS (
    SELECT
      a.player_id, a.team_id,
      max(a.pts)  FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 1) AS off_on_pts,
      max(a.poss) FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 1) AS off_on_poss,
      max(a.pts)  FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 0) AS off_off_pts,
      max(a.poss) FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 0) AS off_off_poss,
      max(a.pts)  FILTER (WHERE a.type_lineup = 'defense' AND a.is_on_key = 1) AS def_on_pts,
      max(a.poss) FILTER (WHERE a.type_lineup = 'defense' AND a.is_on_key = 1) AS def_on_poss,
      max(a.pts)  FILTER (WHERE a.type_lineup = 'defense' AND a.is_on_key = 0) AS def_off_pts,
      max(a.poss) FILTER (WHERE a.type_lineup = 'defense' AND a.is_on_key = 0) AS def_off_poss,
      max(a.mins) FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 1) AS mins_on,
      max(a.fg2_made) FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 1) AS off_on_fg2_made,
      max(a.fg2_att)  FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 1) AS off_on_fg2_att,
      max(a.fg3_made) FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 1) AS off_on_fg3_made,
      max(a.fg3_att)  FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 1) AS off_on_fg3_att,
      max(a.fg2_made) FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 0) AS off_off_fg2_made,
      max(a.fg2_att)  FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 0) AS off_off_fg2_att,
      max(a.fg3_made) FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 0) AS off_off_fg3_made,
      max(a.fg3_att)  FILTER (WHERE a.type_lineup = 'offense' AND a.is_on_key = 0) AS off_off_fg3_att,
      max(a.fg2_made) FILTER (WHERE a.type_lineup = 'defense' AND a.is_on_key = 1) AS def_on_fg2_made,
      max(a.fg2_att)  FILTER (WHERE a.type_lineup = 'defense' AND a.is_on_key = 1) AS def_on_fg2_att,
      max(a.fg3_made) FILTER (WHERE a.type_lineup = 'defense' AND a.is_on_key = 1) AS def_on_fg3_made,
      max(a.fg3_att)  FILTER (WHERE a.type_lineup = 'defense' AND a.is_on_key = 1) AS def_on_fg3_att,
      max(a.fg2_made) FILTER (WHERE a.type_lineup = 'defense' AND a.is_on_key = 0) AS def_off_fg2_made,
      max(a.fg2_att)  FILTER (WHERE a.type_lineup = 'defense' AND a.is_on_key = 0) AS def_off_fg2_att,
      max(a.fg3_made) FILTER (WHERE a.type_lineup = 'defense' AND a.is_on_key = 0) AS def_off_fg3_made,
      max(a.fg3_att)  FILTER (WHERE a.type_lineup = 'defense' AND a.is_on_key = 0) AS def_off_fg3_att
    FROM agg a
    GROUP BY a.player_id, a.team_id
  ),
  rated AS (
    SELECT
      p.*,
      round(100.0 * p.off_on_pts  / NULLIF(p.off_on_poss, 0), 1)  AS off_on_ppp,
      round(100.0 * p.off_off_pts / NULLIF(p.off_off_poss, 0), 1) AS off_off_ppp,
      round(100.0 * p.def_on_pts  / NULLIF(p.def_on_poss, 0), 1)  AS def_on_ppp,
      round(100.0 * p.def_off_pts / NULLIF(p.def_off_poss, 0), 1) AS def_off_ppp
    FROM pivoted p
    WHERE least(
            COALESCE(p.off_on_poss, 0), COALESCE(p.off_off_poss, 0),
            COALESCE(p.def_on_poss, 0), COALESCE(p.def_off_poss, 0)
          ) >= COALESCE(p_min_all, 0)
      AND COALESCE(p.off_on_poss, 0) >= COALESCE(p_min_on, 0)
  ),
  diffed AS (
    SELECT
      r.*,
      r.off_on_ppp - r.def_on_ppp   AS on_net,
      r.off_off_ppp - r.def_off_ppp AS off_net,
      r.off_on_ppp - r.off_off_ppp  AS off_diff,
      r.def_on_ppp - r.def_off_ppp  AS def_diff,
      (r.off_on_ppp - r.off_off_ppp) - (r.def_on_ppp - r.def_off_ppp) AS net_diff
    FROM rated r
  ),
  ranked AS (
  SELECT
    t.display_name AS "Team",
    btrim(split_part(pl.display_name, ',', 2)) AS "First Name",
    btrim(split_part(pl.display_name, ',', 1)) AS "Last Name",
    -- Aliased to the RETURNS TABLE names: those apply to the function result,
    -- not to an inner CTE, so the final WHERE/ORDER BY needs them here.
    d.net_diff            AS "Net RTG Diff",
    d.off_diff            AS "Off ON Diff",
    d.def_diff            AS "Def ON Diff",
    d.off_on_ppp          AS "Off ON PPP",
    d.def_on_ppp          AS "Def ON PPP",
    d.on_net              AS "On Net RTG",
    d.off_off_ppp         AS "Off OFF PPP",
    d.def_off_ppp         AS "Def OFF PPP",
    d.off_net             AS "Off Net RTG",
    d.off_on_poss         AS "ON Poss",
    d.off_off_poss        AS "OFF Poss",
    round(d.mins_on, 1)   AS minutes,
    percent_rank() OVER (ORDER BY d.net_diff)     AS pr_net,
    percent_rank() OVER (ORDER BY d.off_on_ppp)   AS pr_off_on,
    percent_rank() OVER (ORDER BY d.off_off_ppp)  AS pr_off_off,
    percent_rank() OVER (ORDER BY d.def_on_ppp DESC)  AS pr_def_on_inv,
    percent_rank() OVER (ORDER BY d.def_off_ppp DESC) AS pr_def_off_inv,
    percent_rank() OVER (ORDER BY d.off_diff)     AS pr_off_on_d,
    percent_rank() OVER (ORDER BY d.def_diff)     AS pr_def_on_d,
    percent_rank() OVER (ORDER BY d.def_diff DESC) AS pr_def_on_d_inv,
    percent_rank() OVER (ORDER BY d.on_net)       AS pr_on_net,
    percent_rank() OVER (ORDER BY d.off_net)      AS pr_off_net,
    d.player_id, d.team_id,
    d.off_on_fg2_made, d.off_on_fg2_att, d.off_on_fg3_made, d.off_on_fg3_att,
    d.off_off_fg2_made, d.off_off_fg2_att, d.off_off_fg3_made, d.off_off_fg3_att,
    d.def_on_fg2_made, d.def_on_fg2_att, d.def_on_fg3_made, d.def_on_fg3_att,
    d.def_off_fg2_made, d.def_off_fg2_att, d.def_off_fg3_made, d.def_off_fg3_att
  FROM diffed d
  JOIN euroleague.players pl ON pl.player_id = d.player_id
  JOIN euroleague.teams   t  ON t.team_id    = d.team_id
  )
  -- min_net is enforced AFTER the percentile window, matching the Israeli
  -- pipeline order: percentiles describe the full eligible population.
  SELECT * FROM ranked r
  WHERE p_min_net IS NULL OR r."Net RTG Diff" >= p_min_net
  ORDER BY r."Net RTG Diff" DESC NULLS LAST, r."Team", r."Last Name", r."First Name";
END;
$function$;

-- ---------------------------------------------------------------------------
-- 3. Filtered path: player ON/OFF four factors.
--    Mirrors basketball_test.four_factors_compute, with steals-only
--    "disruptions" as documented in the header.
-- ---------------------------------------------------------------------------

DROP FUNCTION IF EXISTS euroleague.four_factors_compute(
  text, int4, date, date, text, text, text, text, text, text, int4, text,
  int4, int4, int4, int4, int4, int4, int4);

CREATE OR REPLACE FUNCTION euroleague.four_factors_compute(
    p_competition          TEXT,
    p_game_year            INTEGER,
    p_start_date           DATE    DEFAULT NULL,
    p_end_date             DATE    DEFAULT NULL,
    p_team_ids_csv         TEXT    DEFAULT NULL,
    p_phase_csv            TEXT    DEFAULT NULL,
    p_opp_ids_csv          TEXT    DEFAULT NULL,
    p_home_away            TEXT    DEFAULT 'all',
    p_outcome              TEXT    DEFAULT 'all',
    p_opp_rank_side        TEXT    DEFAULT NULL,
    p_opp_rank_n           INTEGER DEFAULT NULL,
    p_opp_rank_metric      TEXT    DEFAULT NULL,
    p_min_gn               INTEGER DEFAULT NULL,
    p_max_gn               INTEGER DEFAULT NULL,
    p_last_n_games         INTEGER DEFAULT NULL,
    p_num_starters_off_min INTEGER DEFAULT NULL,
    p_num_starters_off_max INTEGER DEFAULT NULL,
    p_num_starters_def_min INTEGER DEFAULT NULL,
    p_num_starters_def_max INTEGER DEFAULT NULL
)
RETURNS TABLE (
    player_id BIGINT, team_id BIGINT,
    firstname TEXT, lastname TEXT, team_name TEXT, game_year INT,
    off_on_ts NUMERIC, off_off_ts NUMERIC, def_on_ts NUMERIC, def_off_ts NUMERIC,
    off_on_efg NUMERIC, off_off_efg NUMERIC, def_on_efg NUMERIC, def_off_efg NUMERIC,
    off_on_oreb NUMERIC, off_off_oreb NUMERIC, def_on_oreb NUMERIC, def_off_oreb NUMERIC,
    off_on_tov NUMERIC, off_off_tov NUMERIC, def_on_tov NUMERIC, def_off_tov NUMERIC,
    def_on_disruptions NUMERIC, def_off_disruptions NUMERIC,
    off_on_ftr NUMERIC, off_off_ftr NUMERIC, def_on_ftr NUMERIC, def_off_ftr NUMERIC,
    off_on_poss BIGINT, off_off_poss BIGINT, def_on_poss BIGINT, def_off_poss BIGINT,
    "Off eFG% Diff" NUMERIC, "Off TS% Diff" NUMERIC, "Off OREB% Diff" NUMERIC,
    "Off TOV% Diff" NUMERIC, "Off FTR Diff" NUMERIC,
    "Def eFG% Diff" NUMERIC, "Def TS% Diff" NUMERIC, "Def OREB% Diff" NUMERIC,
    "Def TOV% Diff" NUMERIC, "Def FTR Diff" NUMERIC,
    "Def Disruptions/100 Diff" NUMERIC
)
LANGUAGE plpgsql
STABLE
SET plan_cache_mode = force_custom_plan
AS $function$
DECLARE
  v_competition text;
  v_team_ids    bigint[];
  v_opp_ids     bigint[];
  v_phases      text[];
  v_home_away   text;
  v_outcome     text;
  v_rank_side   text;
  v_rank_metric text;
BEGIN
  v_competition := COALESCE(NULLIF(btrim(p_competition), ''), 'E');
  v_home_away   := COALESCE(NULLIF(btrim(p_home_away), ''), 'all');
  v_outcome     := COALESCE(NULLIF(btrim(p_outcome), ''), 'all');
  v_rank_side   := NULLIF(btrim(p_opp_rank_side), '');
  v_rank_metric := COALESCE(NULLIF(btrim(p_opp_rank_metric), ''), 'net');

  IF p_team_ids_csv IS NOT NULL AND length(btrim(p_team_ids_csv)) > 0 THEN
    v_team_ids := ARRAY(
      SELECT DISTINCT x::bigint
      FROM unnest(string_to_array(regexp_replace(p_team_ids_csv, '\s+', '', 'g'), ',')) x
      WHERE x <> '' ORDER BY 1);
  END IF;

  IF p_opp_ids_csv IS NOT NULL AND length(btrim(p_opp_ids_csv)) > 0 THEN
    v_opp_ids := ARRAY(
      SELECT DISTINCT x::bigint
      FROM unnest(string_to_array(regexp_replace(p_opp_ids_csv, '\s+', '', 'g'), ',')) x
      WHERE x <> '' ORDER BY 1);
  END IF;

  IF p_phase_csv IS NOT NULL AND length(btrim(p_phase_csv)) > 0 THEN
    v_phases := ARRAY(
      SELECT DISTINCT btrim(x)
      FROM unnest(string_to_array(p_phase_csv, ',')) x
      WHERE btrim(x) <> '' ORDER BY 1);
  END IF;

  RETURN QUERY
  WITH schedule_ranked AS (
    SELECT
      fs.game_id, fs.team_id, fs.opp_team_id, fs.is_home, fs.has_won,
      s.round_number, s.phase, s.scheduled_at::date AS game_date,
      row_number() OVER (
        PARTITION BY fs.team_id
        ORDER BY s.scheduled_at DESC, fs.game_id DESC
      ) AS team_game_rank
    FROM euroleague.final_schedule fs
    JOIN euroleague.schedule s ON s.game_id = fs.game_id
    WHERE s.competition = v_competition
      AND s.season = p_game_year
  ),
  team_window AS (
    SELECT
      c.team_id,
      sum(c.total_points) FILTER (WHERE c.type_lineup = 'offense' AND c.is_on_key = 1) AS off_pts,
      sum(c.total_poss)   FILTER (WHERE c.type_lineup = 'offense' AND c.is_on_key = 1) AS off_poss,
      sum(c.total_points) FILTER (WHERE c.type_lineup = 'defense' AND c.is_on_key = 1) AS def_pts,
      sum(c.total_poss)   FILTER (WHERE c.type_lineup = 'defense' AND c.is_on_key = 1) AS def_poss
    FROM euroleague.player_game_context c
    JOIN schedule_ranked sr ON sr.game_id = c.game_id AND sr.team_id = c.team_id
    WHERE (p_start_date IS NULL OR sr.game_date >= p_start_date)
      AND (p_end_date   IS NULL OR sr.game_date <= p_end_date)
    GROUP BY c.team_id
  ),
  team_ranked AS (
    SELECT
      tw.team_id,
      dense_rank() OVER (ORDER BY 100.0 * tw.off_pts / NULLIF(tw.off_poss, 0) DESC) AS off_rank,
      dense_rank() OVER (ORDER BY 100.0 * tw.def_pts / NULLIF(tw.def_poss, 0) ASC)  AS def_rank,
      dense_rank() OVER (
        ORDER BY (100.0 * tw.off_pts / NULLIF(tw.off_poss, 0))
               - (100.0 * tw.def_pts / NULLIF(tw.def_poss, 0)) DESC
      ) AS net_rank
    FROM team_window tw
  ),
  games AS (
    SELECT sr.*
    FROM schedule_ranked sr
    LEFT JOIN team_ranked tr ON tr.team_id = sr.opp_team_id
    WHERE (p_start_date IS NULL OR sr.game_date >= p_start_date)
      AND (p_end_date   IS NULL OR sr.game_date <= p_end_date)
      AND (v_phases   IS NULL OR sr.phase = ANY(v_phases))
      AND (v_opp_ids  IS NULL OR sr.opp_team_id = ANY(v_opp_ids))
      AND (p_min_gn   IS NULL OR sr.round_number >= p_min_gn)
      AND (p_max_gn   IS NULL OR sr.round_number <= p_max_gn)
      AND (p_last_n_games IS NULL OR sr.team_game_rank <= p_last_n_games)
      AND (v_home_away = 'all'
           OR (v_home_away = 'home' AND sr.is_home)
           OR (v_home_away = 'away' AND NOT sr.is_home))
      AND (v_outcome = 'all'
           OR (v_outcome = 'win'  AND sr.has_won)
           OR (v_outcome = 'loss' AND NOT sr.has_won))
      AND (
        v_rank_side IS NULL OR p_opp_rank_n IS NULL
        OR (v_rank_side = 'top' AND
            CASE v_rank_metric WHEN 'off' THEN tr.off_rank
                               WHEN 'def' THEN tr.def_rank
                               ELSE tr.net_rank END <= p_opp_rank_n)
        OR (v_rank_side = 'bottom' AND
            CASE v_rank_metric WHEN 'off' THEN tr.off_rank
                               WHEN 'def' THEN tr.def_rank
                               ELSE tr.net_rank END
            > (SELECT count(*) FROM team_ranked) - p_opp_rank_n)
      )
  ),
  agg AS (
    SELECT
      c.player_id, c.team_id, c.is_on_key, c.type_lineup,
      sum(c.total_points)::numeric      AS pts,
      sum(c.total_poss)::bigint         AS poss,
      sum(c.ts_poss_count)::bigint      AS ts_poss,
      sum(c.oreb_count)::bigint         AS oreb,
      sum(c.oreb_opportunities)::bigint AS oreb_opp,
      sum(c.tov_count)::bigint          AS tov,
      sum(c.steal_count)::bigint        AS steals,
      sum(c.deflection_count)::bigint   AS deflections,
      sum(c.total_ft_attempts)::bigint  AS fta,
      sum(c.total_fga)::bigint          AS fga,
      sum(c.total_fgm)::bigint          AS fgm,
      sum(c.total_fg3_made)::bigint     AS fg3m
    FROM euroleague.player_game_context c
    JOIN games g ON g.game_id = c.game_id AND g.team_id = c.team_id
    WHERE (p_num_starters_off_min IS NULL OR c.own_starters >= p_num_starters_off_min)
      AND (p_num_starters_off_max IS NULL OR c.own_starters <= p_num_starters_off_max)
      AND (p_num_starters_def_min IS NULL OR c.opp_starters >= p_num_starters_def_min)
      AND (p_num_starters_def_max IS NULL OR c.opp_starters <= p_num_starters_def_max)
      AND (v_team_ids IS NULL OR c.team_id = ANY(v_team_ids))
    GROUP BY c.player_id, c.team_id, c.is_on_key, c.type_lineup
  ),
  rates AS (
    SELECT
      a.player_id, a.team_id, a.is_on_key, a.type_lineup, a.poss,
      a.pts / NULLIF(2 * a.ts_poss, 0)::numeric                     AS ts_pct,
      (a.fgm + 0.5 * a.fg3m) / NULLIF(a.fga, 0)::numeric            AS efg_pct,
      a.oreb / NULLIF(a.oreb_opp, 0)::numeric                       AS oreb_pct,
      a.tov / NULLIF(a.poss, 0)::numeric                            AS tov_pct,
      (a.steals + a.deflections) / NULLIF(a.poss, 0)::numeric       AS disruption_rate,
      a.fta / NULLIF(a.fga, 0)::numeric                             AS ftr
    FROM agg a
  ),
  pivoted AS (
    SELECT
      r.player_id, r.team_id,
      max(r.ts_pct)  FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 1) AS off_on_ts,
      max(r.ts_pct)  FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 0) AS off_off_ts,
      max(r.ts_pct)  FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 1) AS def_on_ts,
      max(r.ts_pct)  FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 0) AS def_off_ts,
      max(r.efg_pct) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 1) AS off_on_efg,
      max(r.efg_pct) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 0) AS off_off_efg,
      max(r.efg_pct) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 1) AS def_on_efg,
      max(r.efg_pct) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 0) AS def_off_efg,
      max(r.oreb_pct) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 1) AS off_on_oreb,
      max(r.oreb_pct) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 0) AS off_off_oreb,
      max(r.oreb_pct) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 1) AS def_on_oreb,
      max(r.oreb_pct) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 0) AS def_off_oreb,
      max(r.tov_pct) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 1) AS off_on_tov,
      max(r.tov_pct) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 0) AS off_off_tov,
      max(r.tov_pct) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 1) AS def_on_tov,
      max(r.tov_pct) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 0) AS def_off_tov,
      max(r.disruption_rate) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 1) AS def_on_disr,
      max(r.disruption_rate) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 0) AS def_off_disr,
      max(r.ftr) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 1) AS off_on_ftr,
      max(r.ftr) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 0) AS off_off_ftr,
      max(r.ftr) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 1) AS def_on_ftr,
      max(r.ftr) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 0) AS def_off_ftr,
      max(r.poss) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 1) AS off_on_poss,
      max(r.poss) FILTER (WHERE r.type_lineup = 'offense' AND r.is_on_key = 0) AS off_off_poss,
      max(r.poss) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 1) AS def_on_poss,
      max(r.poss) FILTER (WHERE r.type_lineup = 'defense' AND r.is_on_key = 0) AS def_off_poss
    FROM rates r
    GROUP BY r.player_id, r.team_id
  )
  SELECT
    p.player_id, p.team_id,
    btrim(split_part(pl.display_name, ',', 2)) AS firstname,
    btrim(split_part(pl.display_name, ',', 1)) AS lastname,
    t.display_name AS team_name,
    p_game_year AS game_year,
    p.off_on_ts, p.off_off_ts, p.def_on_ts, p.def_off_ts,
    p.off_on_efg, p.off_off_efg, p.def_on_efg, p.def_off_efg,
    p.off_on_oreb, p.off_off_oreb, p.def_on_oreb, p.def_off_oreb,
    p.off_on_tov, p.off_off_tov, p.def_on_tov, p.def_off_tov,
    p.def_on_disr, p.def_off_disr,
    p.off_on_ftr, p.off_off_ftr, p.def_on_ftr, p.def_off_ftr,
    p.off_on_poss, p.off_off_poss, p.def_on_poss, p.def_off_poss,
    round(100 * (p.off_on_efg  - p.off_off_efg), 1),
    round(100 * (p.off_on_ts   - p.off_off_ts), 1),
    round(100 * (p.off_on_oreb - p.off_off_oreb), 1),
    round(100 * (p.off_on_tov  - p.off_off_tov), 1),
    round(100 * (p.off_on_ftr  - p.off_off_ftr), 1),
    round(100 * (p.def_on_efg  - p.def_off_efg), 1),
    round(100 * (p.def_on_ts   - p.def_off_ts), 1),
    round(100 * (p.def_on_oreb - p.def_off_oreb), 1),
    round(100 * (p.def_on_tov  - p.def_off_tov), 1),
    round(100 * (p.def_on_ftr  - p.def_off_ftr), 1),
    round(100 * (p.def_on_disr - p.def_off_disr), 1)
  FROM pivoted p
  JOIN euroleague.players pl ON pl.player_id = p.player_id
  JOIN euroleague.teams   t  ON t.team_id    = p.team_id;
END;
$function$;

-- ---------------------------------------------------------------------------
-- 4. Fast path: full-season materialized snapshots.
--    Explicit column lists, not SELECT *, so a later view change cannot
--    silently diverge from the materialized copy.
-- ---------------------------------------------------------------------------

-- Schedule dimension. Created here rather than in migration 003 so this file
-- is self-sufficient; 003 is superseded and must not be applied (see the
-- migration note at the end of this file).
DROP MATERIALIZED VIEW IF EXISTS euroleague.final_schedule_mv;

CREATE MATERIALIZED VIEW euroleague.final_schedule_mv AS
SELECT
  fs.game_id,
  fs.competition,
  fs.game_year,
  s.round_number,
  fs.gn AS gamecode,
  fs.game_date,
  fs.game_type AS phase,
  fs.team_id,
  fs.opp_team_id,
  fs.is_home,
  fs.has_won,
  fs.team_points,
  fs.opp_points,
  t.display_name     AS team_name,
  opp.display_name   AS opp_team_name
FROM euroleague.final_schedule fs
JOIN euroleague.schedule s ON s.game_id = fs.game_id
JOIN euroleague.teams t    ON t.team_id = fs.team_id
JOIN euroleague.teams opp  ON opp.team_id = fs.opp_team_id
WITH NO DATA;

CREATE UNIQUE INDEX euroleague_final_schedule_mv_pk
  ON euroleague.final_schedule_mv (game_id, team_id);

CREATE INDEX euroleague_final_schedule_mv_filter_idx
  ON euroleague.final_schedule_mv (competition, game_year, team_id, game_date, round_number);

DROP MATERIALIZED VIEW IF EXISTS euroleague.player_onoff_default_mv;

CREATE MATERIALIZED VIEW euroleague.player_onoff_default_mv AS
SELECT
  s.competition,
  s.season AS game_year,
  f."Team", f."First Name", f."Last Name",
  f."Net RTG Diff", f."Off ON Diff", f."Def ON Diff",
  f."Off ON PPP", f."Def ON PPP", f."On Net RTG",
  f."Off OFF PPP", f."Def OFF PPP", f."Off Net RTG",
  f."ON Poss", f."OFF Poss", f.minutes,
  f.pr_net, f.pr_off_on, f.pr_off_off, f.pr_def_on_inv, f.pr_def_off_inv,
  f.pr_off_on_d, f.pr_def_on_d, f.pr_def_on_d_inv, f.pr_on_net, f.pr_off_net,
  f.player_id, f.team_id,
  f.off_on_fg2_made, f.off_on_fg2_att, f.off_on_fg3_made, f.off_on_fg3_att,
  f.off_off_fg2_made, f.off_off_fg2_att, f.off_off_fg3_made, f.off_off_fg3_att,
  f.def_on_fg2_made, f.def_on_fg2_att, f.def_on_fg3_made, f.def_on_fg3_att,
  f.def_off_fg2_made, f.def_off_fg2_att, f.def_off_fg3_made, f.def_off_fg3_att
FROM (SELECT DISTINCT competition, season FROM euroleague.schedule) s
CROSS JOIN LATERAL euroleague.onoff_compute(s.competition, s.season) f
WITH NO DATA;

CREATE UNIQUE INDEX euroleague_player_onoff_default_mv_pk
  ON euroleague.player_onoff_default_mv (competition, game_year, team_id, player_id);

DROP MATERIALIZED VIEW IF EXISTS euroleague.player_advanced_stats_mv;

CREATE MATERIALIZED VIEW euroleague.player_advanced_stats_mv AS
SELECT
  s.competition,
  f.game_year, f.player_id, f.team_id, f.firstname, f.lastname, f.team_name,
  f.off_on_ts, f.off_off_ts, f.def_on_ts, f.def_off_ts,
  f.off_on_efg, f.off_off_efg, f.def_on_efg, f.def_off_efg,
  f.off_on_oreb, f.off_off_oreb, f.def_on_oreb, f.def_off_oreb,
  f.off_on_tov, f.off_off_tov, f.def_on_tov, f.def_off_tov,
  f.def_on_disruptions, f.def_off_disruptions,
  f.off_on_ftr, f.off_off_ftr, f.def_on_ftr, f.def_off_ftr,
  f.off_on_poss, f.off_off_poss, f.def_on_poss, f.def_off_poss,
  f."Off eFG% Diff", f."Off TS% Diff", f."Off OREB% Diff",
  f."Off TOV% Diff", f."Off FTR Diff",
  f."Def eFG% Diff", f."Def TS% Diff", f."Def OREB% Diff",
  f."Def TOV% Diff", f."Def FTR Diff", f."Def Disruptions/100 Diff"
FROM (SELECT DISTINCT competition, season FROM euroleague.schedule) s
CROSS JOIN LATERAL euroleague.four_factors_compute(s.competition, s.season) f
WITH NO DATA;

CREATE UNIQUE INDEX euroleague_player_advanced_stats_mv_pk
  ON euroleague.player_advanced_stats_mv (competition, game_year, team_id, player_id);

-- Extend the existing publication refresh entry point rather than adding a
-- second one; the load-run publication path already calls this function.
CREATE OR REPLACE FUNCTION euroleague.refresh_app_materialized_views()
RETURNS void
LANGUAGE plpgsql
AS $function$
BEGIN
  REFRESH MATERIALIZED VIEW euroleague.final_schedule_mv;
  REFRESH MATERIALIZED VIEW euroleague.player_onoff_default_mv;
  REFRESH MATERIALIZED VIEW euroleague.player_advanced_stats_mv;
END;
$function$;

-- ---------------------------------------------------------------------------
-- 5. Application read access.
--    Reuses the existing app_readonly role; no new role is created.
--    Functions are SECURITY INVOKER, so the role needs SELECT on the
--    underlying relations they touch.
-- ---------------------------------------------------------------------------

GRANT USAGE ON SCHEMA euroleague TO app_readonly;

GRANT SELECT ON
  euroleague.schedule,
  euroleague.teams,
  euroleague.players,
  euroleague.full_rosters,
  euroleague.player_four_factors_by_game,
  euroleague.final_schedule,
  euroleague.final_schedule_mv,
  euroleague.player_game_context,
  euroleague.player_onoff_default_mv,
  euroleague.player_advanced_stats_mv,
  -- The app derives its cache-busting token from the newest completed load
  -- run. Without this grant the lookup fails closed to a constant, and the
  -- season caches never invalidate after a publication.
  euroleague.load_runs
TO app_readonly;

GRANT EXECUTE ON FUNCTION
  euroleague.onoff_compute(text, int4, date, date, text, text, text, text, text,
                           text, int4, text, int4, int4, int4, int4, int4, int4,
                           int4, numeric, int4, int4)
TO app_readonly;

GRANT EXECUTE ON FUNCTION
  euroleague.four_factors_compute(text, int4, date, date, text, text, text, text,
                                  text, text, int4, text, int4, int4, int4, int4,
                                  int4, int4, int4)
TO app_readonly;

COMMIT;

-- ---------------------------------------------------------------------------
-- MIGRATION NOTE
--
-- This file SUPERSEDES 003_app_materialized_views.sql, which was never applied.
-- Apply order is 001 -> 002 -> 004. Do not apply 003: its three MVs were
-- defined as SELECT * over the ordinary views (column set frozen at creation)
-- and its two player MVs carry a column contract the app cannot use.
--
-- After applying, populate with:
--   SELECT euroleague.refresh_app_materialized_views();
--
-- Verify:
--   * all three _mv relations are populated and relkind = 'm';
--   * zero duplicate groups on each unique key;
--   * euroleague.onoff_compute('E', 2025) returns one row per team/player with
--     ON Poss > 0, and its team point/possession totals reconcile to
--     sql/analytics/player_onoff_ppp_readonly.sql;
--   * app_readonly can SELECT the three MVs and EXECUTE both functions;
--   * migrations 001/002 tables and the three controlled games are unchanged.
-- ---------------------------------------------------------------------------
