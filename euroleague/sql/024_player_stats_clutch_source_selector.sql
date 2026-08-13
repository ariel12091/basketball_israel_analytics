-- EuroLeague shadow schema -- migration 024: shared Player Stats clutch reader.
-- Mirrors select_team_game_facts(): one explicit cached/custom source branch,
-- followed by one shared player aggregation path.

BEGIN;
SET LOCAL search_path TO euroleague, public;

CREATE OR REPLACE FUNCTION euroleague.select_player_clutch_counts(
    p_game_ids BIGINT[], p_max_margin INTEGER, p_margin_status TEXT,
    p_max_time_remaining INTEGER, p_ot_margin_filter BOOLEAN
)
RETURNS TABLE (
    game_id BIGINT, team_id BIGINT, player_id BIGINT,
    pts NUMERIC, reb NUMERIC, oreb NUMERIC, dreb NUMERIC,
    ast NUMERIC, stl NUMERIC, blk NUMERIC, tov NUMERIC,
    fg2m NUMERIC, fg2a NUMERIC, fg3m NUMERIC, fg3a NUMERIC,
    ftm NUMERIC, fta NUMERIC, player_ts_poss NUMERIC, player_tov NUMERIC
)
LANGUAGE plpgsql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, euroleague, public
SET plan_cache_mode = force_custom_plan
AS $function$
DECLARE
  v_margin_status TEXT := coalesce(nullif(btrim(p_margin_status), ''), 'all');
BEGIN
  IF p_max_margin = 5 AND v_margin_status = 'all'
     AND p_max_time_remaining = 300
     AND NOT coalesce(p_ot_margin_filter, false) THEN
    RETURN QUERY
    SELECT c.game_id, c.team_id, c.player_id,
           c.pts, c.reb, c.oreb, c.dreb, c.ast, c.stl, c.blk, c.tov,
           c.fg2m, c.fg2a, c.fg3m, c.fg3a, c.ftm, c.fta,
           c.player_ts_poss, c.player_tov
    FROM euroleague.default_clutch_player_totals_by_game c
    WHERE c.game_id = ANY(coalesce(p_game_ids, ARRAY[]::bigint[]));
  ELSE
    RETURN QUERY
    SELECT atc.game_id, atc.team_id, atc.action_player_id,
           sum(atc.points)::numeric,
           sum(atc.orebounds + CASE WHEN atc.play_type = 'D' THEN 1 ELSE 0 END)::numeric,
           sum(atc.orebounds)::numeric,
           sum(CASE WHEN atc.play_type = 'D' THEN 1 ELSE 0 END)::numeric,
           sum(CASE WHEN atc.play_type = 'AS' THEN 1 ELSE 0 END)::numeric,
           sum(CASE WHEN atc.play_type = 'ST' THEN 1 ELSE 0 END)::numeric,
           sum(CASE WHEN atc.play_type = 'FV' THEN 1 ELSE 0 END)::numeric,
           sum(atc.turnovers)::numeric,
           sum(atc.fg2_made)::numeric, sum(atc.fg2_att)::numeric,
           sum(atc.fg3_made)::numeric, sum(atc.fg3_att)::numeric,
           sum(CASE WHEN atc.play_type = 'FTM' THEN 1 ELSE 0 END)::numeric,
           sum(atc.ft_attempts)::numeric,
           sum(atc.ts_possessions)::numeric, sum(atc.turnovers)::numeric
    FROM euroleague.action_team_context_actions atc
    JOIN euroleague.players p ON p.player_id = atc.action_player_id
    WHERE atc.game_id = ANY(coalesce(p_game_ids, ARRAY[]::bigint[]))
      AND atc.type_lineup = 'offense'
      AND atc.action_player_id IS NOT NULL
      AND lower(p.provider_player_id) NOT IN ('team', 'total')
      AND euroleague.clutch_event_qualifies(
        atc.period, atc.event_elapsed_seconds,
        atc.own_team_score
          - CASE WHEN atc.event_team_id = atc.team_id THEN atc.points ELSE 0 END,
        atc.opp_team_score
          - CASE WHEN atc.event_team_id = atc.opponent_team_id THEN atc.points ELSE 0 END,
        p_max_margin, v_margin_status, p_max_time_remaining,
        p_ot_margin_filter
      )
    GROUP BY atc.game_id, atc.team_id, atc.action_player_id;
  END IF;
END;
$function$;

CREATE OR REPLACE FUNCTION euroleague.get_player_traditional_clutch(
    p_competition TEXT, p_game_year INTEGER,
    p_start_date DATE DEFAULT NULL, p_end_date DATE DEFAULT NULL,
    p_team_ids_csv TEXT DEFAULT NULL, p_phase_csv TEXT DEFAULT NULL,
    p_opp_ids_csv TEXT DEFAULT NULL, p_home_away TEXT DEFAULT 'all',
    p_outcome TEXT DEFAULT 'all', p_opp_rank_side TEXT DEFAULT NULL,
    p_opp_rank_n INTEGER DEFAULT NULL, p_opp_rank_metric TEXT DEFAULT NULL,
    p_max_margin INTEGER DEFAULT NULL, p_margin_status TEXT DEFAULT NULL,
    p_max_time_remaining INTEGER DEFAULT NULL,
    p_ot_margin_filter BOOLEAN DEFAULT FALSE,
    p_min_gn INTEGER DEFAULT NULL, p_max_gn INTEGER DEFAULT NULL,
    p_last_n_games INTEGER DEFAULT NULL
)
RETURNS TABLE (
    team_id BIGINT, player_id BIGINT, team_name TEXT, "Player" TEXT,
    gp INTEGER, poss_on_floor NUMERIC, minutes NUMERIC,
    pts NUMERIC, reb NUMERIC, oreb NUMERIC, dreb NUMERIC,
    ast NUMERIC, stl NUMERIC, blk NUMERIC, dfl NUMERIC, tov NUMERIC,
    fgm NUMERIC, fga NUMERIC, fg_pct NUMERIC,
    "3pm" NUMERIC, "3pa" NUMERIC, tp_pct NUMERIC,
    ftm NUMERIC, fta NUMERIC, ft_pct NUMERIC, efg NUMERIC,
    ts NUMERIC, usg_pct NUMERIC
)
LANGUAGE sql
STABLE
SECURITY DEFINER
SET search_path = pg_catalog, euroleague, public
SET plan_cache_mode = force_custom_plan
AS $function$
WITH filtered_facts AS MATERIALIZED (
  SELECT f.*
  FROM euroleague.filtered_team_game_facts(
    p_competition, p_game_year, p_start_date, p_end_date,
    p_team_ids_csv, p_phase_csv, p_opp_ids_csv, p_home_away, p_outcome,
    p_opp_rank_side, p_opp_rank_n, p_opp_rank_metric,
    p_max_margin, p_margin_status, p_max_time_remaining,
    p_ot_margin_filter, p_min_gn, p_max_gn, p_last_n_games,
    NULL, NULL, NULL, NULL
  ) f
),
selected_games AS MATERIALIZED (
  SELECT DISTINCT game_id, team_id, team_name FROM filtered_facts
),
selected_game_ids AS MATERIALIZED (
  SELECT array_agg(DISTINCT game_id ORDER BY game_id) AS game_ids
  FROM selected_games
),
roster AS MATERIALIZED (
  SELECT sg.game_id, sg.team_id, sg.team_name,
         fr.player_id, fr.source_player_name,
         euroleague.person_display_name(p.display_name) AS player_name
  FROM selected_games sg
  JOIN euroleague.full_rosters fr
    ON fr.game_id = sg.game_id AND fr.team_id = sg.team_id
  JOIN euroleague.players p ON p.player_id = fr.player_id
  WHERE lower(p.provider_player_id) NOT IN ('team', 'total')
    AND lower(btrim(p.display_name)) NOT IN ('team', 'total')
),
player_counts AS MATERIALIZED (
  SELECT pc.*
  FROM selected_game_ids ids
  CROSS JOIN LATERAL euroleague.select_player_clutch_counts(
    ids.game_ids, p_max_margin, p_margin_status,
    p_max_time_remaining, p_ot_margin_filter
  ) pc
),
exposure AS (
  SELECT r.game_id, r.team_id, r.player_id,
         coalesce(sum(ff.possessions) FILTER (
           WHERE r.source_player_name = ANY(ff.own_lineup)), 0)::numeric AS poss_on_floor,
         coalesce(sum(ff.seconds) FILTER (
           WHERE r.source_player_name = ANY(ff.own_lineup)), 0)::numeric / 60.0 AS minutes
  FROM roster r
  LEFT JOIN filtered_facts ff
    ON ff.game_id = r.game_id AND ff.team_id = r.team_id
   AND ff.type_lineup = 'offense'
  GROUP BY r.game_id, r.team_id, r.player_id
),
team_usage AS (
  SELECT game_id, team_id,
         sum(ts_possessions)::numeric AS team_ts_poss,
         sum(turnovers)::numeric AS team_tov,
         sum(possessions)::numeric AS team_poss
  FROM filtered_facts WHERE type_lineup = 'offense'
  GROUP BY game_id, team_id
),
player_games AS (
  SELECT r.game_id, r.team_id, r.player_id, r.team_name, r.player_name,
         e.poss_on_floor, e.minutes,
         coalesce(c.player_ts_poss, 0)::numeric AS player_ts_poss,
         coalesce(c.player_tov, 0)::numeric AS player_tov,
         coalesce(tu.team_ts_poss, 0)::numeric AS team_ts_poss,
         coalesce(tu.team_tov, 0)::numeric AS team_tov,
         coalesce(tu.team_poss, 0)::numeric AS team_poss,
         coalesce(c.pts, 0)::numeric AS pts, coalesce(c.reb, 0)::numeric AS reb,
         coalesce(c.oreb, 0)::numeric AS oreb, coalesce(c.dreb, 0)::numeric AS dreb,
         coalesce(c.ast, 0)::numeric AS ast, coalesce(c.stl, 0)::numeric AS stl,
         coalesce(c.blk, 0)::numeric AS blk, coalesce(c.tov, 0)::numeric AS tov,
         coalesce(c.fg2m, 0)::numeric AS fg2m, coalesce(c.fg2a, 0)::numeric AS fg2a,
         coalesce(c.fg3m, 0)::numeric AS fg3m, coalesce(c.fg3a, 0)::numeric AS fg3a,
         coalesce(c.ftm, 0)::numeric AS ftm, coalesce(c.fta, 0)::numeric AS fta
  FROM roster r
  JOIN exposure e USING (game_id, team_id, player_id)
  LEFT JOIN player_counts c USING (game_id, team_id, player_id)
  LEFT JOIN team_usage tu USING (game_id, team_id)
  WHERE e.minutes > 0 OR e.poss_on_floor > 0
     OR coalesce(c.pts + c.reb + c.ast + c.stl + c.blk + c.tov
                 + c.fg2a + c.fg3a + c.fta, 0) > 0
),
agg AS (
  SELECT team_id, player_id, min(team_name) AS team_name,
         min(player_name) AS player_name, count(DISTINCT game_id)::integer AS gp,
         sum(poss_on_floor)::numeric AS poss_on_floor, sum(minutes)::numeric AS minutes,
         sum(player_ts_poss)::numeric AS player_ts_poss, sum(player_tov)::numeric AS player_tov,
         sum(team_ts_poss)::numeric AS team_ts_poss, sum(team_tov)::numeric AS team_tov,
         sum(team_poss)::numeric AS team_poss,
         sum(pts)::numeric AS pts, sum(reb)::numeric AS reb,
         sum(oreb)::numeric AS oreb, sum(dreb)::numeric AS dreb,
         sum(ast)::numeric AS ast, sum(stl)::numeric AS stl,
         sum(blk)::numeric AS blk, sum(tov)::numeric AS tov,
         sum(fg2m)::numeric AS fg2m, sum(fg2a)::numeric AS fg2a,
         sum(fg3m)::numeric AS fg3m, sum(fg3a)::numeric AS fg3a,
         sum(ftm)::numeric AS ftm, sum(fta)::numeric AS fta
  FROM player_games GROUP BY team_id, player_id
)
SELECT team_id, player_id, team_name, player_name AS "Player", gp,
       poss_on_floor, minutes, pts, reb, oreb, dreb, ast, stl, blk,
       NULL::numeric AS dfl, tov,
       fg2m + fg3m AS fgm, fg2a + fg3a AS fga,
       round(100 * (fg2m + fg3m) / nullif(fg2a + fg3a, 0), 1) AS fg_pct,
       fg3m AS "3pm", fg3a AS "3pa",
       round(100 * fg3m / nullif(fg3a, 0), 1) AS tp_pct,
       ftm, fta, round(100 * ftm / nullif(fta, 0), 1) AS ft_pct,
       round(100 * (fg2m + 1.5 * fg3m) / nullif(fg2a + fg3a, 0), 1) AS efg,
       round(100 * pts / nullif(2 * player_ts_poss, 0), 1) AS ts,
       round(100 * (player_ts_poss + player_tov) * team_poss
             / nullif((team_ts_poss + team_tov) * poss_on_floor, 0), 1) AS usg_pct
FROM agg
$function$;

REVOKE ALL ON FUNCTION euroleague.select_player_clutch_counts(
  bigint[], int4, text, int4, bool
) FROM PUBLIC;
REVOKE ALL ON FUNCTION euroleague.get_player_traditional_clutch(
  text, int4, date, date, text, text, text, text, text, text, int4, text,
  int4, text, int4, bool, int4, int4, int4
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION euroleague.get_player_traditional_clutch(
  text, int4, date, date, text, text, text, text, text, text, int4, text,
  int4, text, int4, bool, int4, int4, int4
) TO app_readonly;

COMMIT;
