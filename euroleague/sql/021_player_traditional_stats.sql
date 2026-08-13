-- EuroLeague shadow schema -- migration 021: traditional player statistics.
--
-- The official player/game box score is already persisted in
-- full_rosters.boxscore_stats, so this migration deliberately does not add a
-- duplicate player/game table. TS% and USG% use canonical PBP-derived shot/
-- free-throw-trip, turnover, team-possession, and on-court exposure terms. It
-- adds only the indexed season fast path required by the app contract and a
-- scoped dynamic reader for filtered and clutch requests.

BEGIN;

SET LOCAL search_path TO euroleague, public;

CREATE TABLE euroleague.default_clutch_player_totals_by_game (
  game_id BIGINT NOT NULL REFERENCES euroleague.schedule(game_id) ON DELETE CASCADE,
  team_id BIGINT NOT NULL REFERENCES euroleague.teams(team_id),
  player_id BIGINT NOT NULL REFERENCES euroleague.players(player_id),
  pts NUMERIC NOT NULL DEFAULT 0, reb NUMERIC NOT NULL DEFAULT 0,
  oreb NUMERIC NOT NULL DEFAULT 0, dreb NUMERIC NOT NULL DEFAULT 0,
  ast NUMERIC NOT NULL DEFAULT 0, stl NUMERIC NOT NULL DEFAULT 0,
  blk NUMERIC NOT NULL DEFAULT 0, tov NUMERIC NOT NULL DEFAULT 0,
  fg2m NUMERIC NOT NULL DEFAULT 0, fg2a NUMERIC NOT NULL DEFAULT 0,
  fg3m NUMERIC NOT NULL DEFAULT 0, fg3a NUMERIC NOT NULL DEFAULT 0,
  ftm NUMERIC NOT NULL DEFAULT 0, fta NUMERIC NOT NULL DEFAULT 0,
  player_ts_poss NUMERIC NOT NULL DEFAULT 0,
  player_tov NUMERIC NOT NULL DEFAULT 0,
  derivation_version TEXT NOT NULL DEFAULT 'default-clutch-player-v1',
  derived_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY (game_id, team_id, player_id)
);

CREATE INDEX euroleague_default_clutch_player_totals_game_team_idx
  ON euroleague.default_clutch_player_totals_by_game (game_id, team_id);

ALTER TABLE euroleague.default_clutch_player_totals_by_game ENABLE ROW LEVEL SECURITY;
CREATE POLICY app_readonly_select_all
  ON euroleague.default_clutch_player_totals_by_game
  FOR SELECT TO app_readonly USING (true);

CREATE MATERIALIZED VIEW euroleague.player_traditional_stats_mv AS
WITH exposure AS (
  SELECT
    pf.game_id,
    pf.team_id,
    pf.player_id,
    sum(pf.total_poss)::bigint AS poss_on_floor,
    sum(pf.minutes)::numeric AS minutes
  FROM euroleague.player_four_factors_by_game pf
  WHERE pf.type_lineup = 'offense' AND pf.is_on_key = 1
  GROUP BY pf.game_id, pf.team_id, pf.player_id
),
-- Default and game-filtered requests reuse the incrementally maintained
-- per-game facts. Only the clutch branch below needs action-grained reads.
player_usage AS (
  SELECT
    pf.game_id,
    pf.team_id,
    pf.player_id,
    sum(pf.player_ts_poss_count)::numeric AS player_ts_poss,
    sum(pf.player_tov_count)::numeric AS player_tov
  FROM euroleague.player_four_factors_by_game pf
  WHERE pf.type_lineup = 'offense'
  GROUP BY pf.game_id, pf.team_id, pf.player_id
),
team_usage AS (
  SELECT
    tf.game_id,
    tf.team_id,
    sum(tf.off_ts_poss)::numeric AS team_ts_poss,
    sum(tf.off_tov)::numeric AS team_tov,
    sum(tf.off_poss)::numeric AS team_poss
  FROM euroleague.team_four_factors_by_game tf
  GROUP BY tf.game_id, tf.team_id
),
player_games AS (
  SELECT
    s.competition,
    s.season AS game_year,
    fr.game_id,
    fr.team_id,
    fr.player_id,
    t.display_name AS team_name,
    euroleague.person_display_name(p.display_name) AS player_name,
    coalesce(e.poss_on_floor, 0)::bigint AS poss_on_floor,
    coalesce(e.minutes, fr.minutes_seconds / 60.0, 0)::numeric AS minutes,
    coalesce(pu.player_ts_poss, 0)::numeric AS player_ts_poss,
    coalesce(pu.player_tov, 0)::numeric AS player_tov,
    coalesce(tu.team_ts_poss, 0)::numeric AS team_ts_poss,
    coalesce(tu.team_tov, 0)::numeric AS team_tov,
    coalesce(tu.team_poss, 0)::numeric AS team_poss,
    coalesce((fr.boxscore_stats ->> 'Points')::numeric, 0) AS pts,
    coalesce((fr.boxscore_stats ->> 'TotalRebounds')::numeric, 0) AS reb,
    coalesce((fr.boxscore_stats ->> 'OffensiveRebounds')::numeric, 0) AS oreb,
    coalesce((fr.boxscore_stats ->> 'DefensiveRebounds')::numeric, 0) AS dreb,
    coalesce((fr.boxscore_stats ->> 'Assistances')::numeric, 0) AS ast,
    coalesce((fr.boxscore_stats ->> 'Steals')::numeric, 0) AS stl,
    coalesce((fr.boxscore_stats ->> 'BlocksFavour')::numeric, 0) AS blk,
    coalesce((fr.boxscore_stats ->> 'Turnovers')::numeric, 0) AS tov,
    coalesce((fr.boxscore_stats ->> 'FieldGoalsMade2')::numeric, 0) AS fg2m,
    coalesce((fr.boxscore_stats ->> 'FieldGoalsAttempted2')::numeric, 0) AS fg2a,
    coalesce((fr.boxscore_stats ->> 'FieldGoalsMade3')::numeric, 0) AS fg3m,
    coalesce((fr.boxscore_stats ->> 'FieldGoalsAttempted3')::numeric, 0) AS fg3a,
    coalesce((fr.boxscore_stats ->> 'FreeThrowsMade')::numeric, 0) AS ftm,
    coalesce((fr.boxscore_stats ->> 'FreeThrowsAttempted')::numeric, 0) AS fta
  FROM euroleague.full_rosters fr
  JOIN euroleague.schedule s ON s.game_id = fr.game_id
  JOIN euroleague.teams t ON t.team_id = fr.team_id
  JOIN euroleague.players p ON p.player_id = fr.player_id
  LEFT JOIN exposure e
    ON e.game_id = fr.game_id AND e.team_id = fr.team_id
   AND e.player_id = fr.player_id
  LEFT JOIN player_usage pu
    ON pu.game_id = fr.game_id AND pu.team_id = fr.team_id
   AND pu.player_id = fr.player_id
  LEFT JOIN team_usage tu
    ON tu.game_id = fr.game_id AND tu.team_id = fr.team_id
  WHERE lower(p.provider_player_id) NOT IN ('team', 'total')
    AND lower(btrim(p.display_name)) NOT IN ('team', 'total')
),
agg AS (
  SELECT
    competition, game_year, team_id, player_id,
    min(team_name) AS team_name,
    min(player_name) AS player_name,
    count(*) FILTER (WHERE minutes > 0)::integer AS gp,
    sum(poss_on_floor)::numeric AS poss_on_floor,
    sum(minutes)::numeric AS minutes,
    sum(player_ts_poss)::numeric AS player_ts_poss,
    sum(player_tov)::numeric AS player_tov,
    sum(team_ts_poss)::numeric AS team_ts_poss,
    sum(team_tov)::numeric AS team_tov,
    sum(team_poss)::numeric AS team_poss,
    sum(pts)::numeric AS pts,
    sum(reb)::numeric AS reb,
    sum(oreb)::numeric AS oreb,
    sum(dreb)::numeric AS dreb,
    sum(ast)::numeric AS ast,
    sum(stl)::numeric AS stl,
    sum(blk)::numeric AS blk,
    sum(tov)::numeric AS tov,
    sum(fg2m)::numeric AS fg2m,
    sum(fg2a)::numeric AS fg2a,
    sum(fg3m)::numeric AS fg3m,
    sum(fg3a)::numeric AS fg3a,
    sum(ftm)::numeric AS ftm,
    sum(fta)::numeric AS fta
  FROM player_games
  GROUP BY competition, game_year, team_id, player_id
)
SELECT
  competition, game_year, team_id, player_id, team_name,
  player_name AS "Player", gp, poss_on_floor, minutes,
  pts, reb, oreb, dreb, ast, stl, blk,
  NULL::numeric AS dfl,
  tov,
  fg2m + fg3m AS fgm,
  fg2a + fg3a AS fga,
  round(100 * (fg2m + fg3m) / nullif(fg2a + fg3a, 0), 1) AS fg_pct,
  fg3m AS "3pm", fg3a AS "3pa",
  round(100 * fg3m / nullif(fg3a, 0), 1) AS tp_pct,
  ftm, fta,
  round(100 * ftm / nullif(fta, 0), 1) AS ft_pct,
  round(100 * (fg2m + 1.5 * fg3m) / nullif(fg2a + fg3a, 0), 1) AS efg,
  round(100 * pts / nullif(2 * player_ts_poss, 0), 1) AS ts,
  round(
    100 * (player_ts_poss + player_tov) * team_poss
      / nullif((team_ts_poss + team_tov) * poss_on_floor, 0),
    1
  ) AS usg_pct
FROM agg
WHERE gp > 0
WITH NO DATA;

CREATE UNIQUE INDEX euroleague_player_traditional_stats_mv_pk
  ON euroleague.player_traditional_stats_mv
  (competition, game_year, team_id, player_id);

CREATE INDEX euroleague_player_traditional_stats_mv_team_idx
  ON euroleague.player_traditional_stats_mv
  (competition, game_year, team_id);

CREATE OR REPLACE FUNCTION euroleague.get_player_traditional_dynamic(
    p_competition TEXT,
    p_game_year INTEGER,
    p_start_date DATE DEFAULT NULL,
    p_end_date DATE DEFAULT NULL,
    p_team_ids_csv TEXT DEFAULT NULL,
    p_phase_csv TEXT DEFAULT NULL,
    p_opp_ids_csv TEXT DEFAULT NULL,
    p_home_away TEXT DEFAULT 'all',
    p_outcome TEXT DEFAULT 'all',
    p_opp_rank_side TEXT DEFAULT NULL,
    p_opp_rank_n INTEGER DEFAULT NULL,
    p_opp_rank_metric TEXT DEFAULT NULL,
    p_max_margin INTEGER DEFAULT NULL,
    p_margin_status TEXT DEFAULT NULL,
    p_max_time_remaining INTEGER DEFAULT NULL,
    p_ot_margin_filter BOOLEAN DEFAULT FALSE,
    p_min_gn INTEGER DEFAULT NULL,
    p_max_gn INTEGER DEFAULT NULL,
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
WITH params AS (
  SELECT
    p_max_margin IS NOT NULL
      OR nullif(btrim(p_margin_status), '') IS NOT NULL
      OR p_max_time_remaining IS NOT NULL AS is_clutch,
    p_max_margin = 5
      AND coalesce(nullif(btrim(p_margin_status), ''), 'all') = 'all'
      AND p_max_time_remaining = 300
      AND NOT coalesce(p_ot_margin_filter, false) AS is_standard_clutch
),
filtered_facts AS MATERIALIZED (
  SELECT f.*
  FROM euroleague.filtered_team_game_facts(
    p_competition, p_game_year, p_start_date, p_end_date,
    p_team_ids_csv, p_phase_csv, p_opp_ids_csv,
    p_home_away, p_outcome,
    p_opp_rank_side, p_opp_rank_n, p_opp_rank_metric,
    p_max_margin, p_margin_status, p_max_time_remaining,
    p_ot_margin_filter, p_min_gn, p_max_gn, p_last_n_games,
    NULL, NULL, NULL, NULL
  ) f
),
selected_games AS MATERIALIZED (
  SELECT DISTINCT game_id, team_id, team_name
  FROM filtered_facts
),
roster AS MATERIALIZED (
  SELECT
    sg.game_id, sg.team_id, sg.team_name,
    fr.player_id, fr.source_player_name, fr.boxscore_stats,
    euroleague.person_display_name(p.display_name) AS player_name
  FROM selected_games sg
  JOIN euroleague.full_rosters fr
    ON fr.game_id = sg.game_id AND fr.team_id = sg.team_id
  JOIN euroleague.players p ON p.player_id = fr.player_id
  WHERE lower(p.provider_player_id) NOT IN ('team', 'total')
    AND lower(btrim(p.display_name)) NOT IN ('team', 'total')
),
exposure AS (
  SELECT
    r.game_id, r.team_id, r.player_id,
    coalesce(sum(ff.possessions) FILTER (
      WHERE r.source_player_name = ANY(ff.own_lineup)
    ), 0)::numeric AS poss_on_floor,
    coalesce(sum(ff.seconds) FILTER (
      WHERE ff.type_lineup = 'offense'
        AND r.source_player_name = ANY(ff.own_lineup)
    ), 0)::numeric / 60.0 AS minutes
  FROM roster r
  LEFT JOIN filtered_facts ff
    ON ff.game_id = r.game_id AND ff.team_id = r.team_id
   AND ff.type_lineup = 'offense'
  GROUP BY r.game_id, r.team_id, r.player_id
),
team_usage AS (
  SELECT
    ff.game_id,
    ff.team_id,
    sum(ff.ts_possessions)::numeric AS team_ts_poss,
    sum(ff.turnovers)::numeric AS team_tov,
    sum(ff.possessions)::numeric AS team_poss
  FROM filtered_facts ff
  WHERE ff.type_lineup = 'offense'
  GROUP BY ff.game_id, ff.team_id
),
player_usage AS (
  -- Game-level filters can stay entirely on the existing per-game fact.
  SELECT
    sg.game_id,
    sg.team_id,
    pf.player_id,
    sum(pf.player_ts_poss_count)::numeric AS player_ts_poss,
    sum(pf.player_tov_count)::numeric AS player_tov
  FROM selected_games sg
  CROSS JOIN params p
  JOIN euroleague.player_four_factors_by_game pf
    ON pf.game_id = sg.game_id
   AND pf.team_id = sg.team_id
   AND pf.type_lineup = 'offense'
  WHERE NOT p.is_clutch
  GROUP BY sg.game_id, sg.team_id, pf.player_id

  UNION ALL

  SELECT
    sg.game_id, sg.team_id, c.player_id,
    c.player_ts_poss, c.player_tov
  FROM selected_games sg
  CROSS JOIN params p
  JOIN euroleague.default_clutch_player_totals_by_game c
    ON c.game_id = sg.game_id AND c.team_id = sg.team_id
  WHERE p.is_standard_clutch

  UNION ALL

  -- In-game predicates must be applied at event grain so FT-trip identity and
  -- player attribution remain exact.
  SELECT
    sg.game_id,
    sg.team_id,
    atc.action_player_id AS player_id,
    sum(atc.ts_possessions)::numeric AS player_ts_poss,
    sum(atc.turnovers)::numeric AS player_tov
  FROM selected_games sg
  CROSS JOIN params p
  JOIN euroleague.action_team_context_actions atc
    ON atc.game_id = sg.game_id
   AND atc.team_id = sg.team_id
   AND atc.type_lineup = 'offense'
   AND atc.action_player_id IS NOT NULL
   AND euroleague.clutch_event_qualifies(
         atc.period, atc.event_elapsed_seconds,
         atc.own_team_score
           - CASE WHEN atc.event_team_id = atc.team_id THEN atc.points ELSE 0 END,
         atc.opp_team_score
           - CASE WHEN atc.event_team_id = atc.opponent_team_id THEN atc.points ELSE 0 END,
         p_max_margin, p_margin_status, p_max_time_remaining,
         p_ot_margin_filter
       )
  WHERE p.is_clutch AND NOT p.is_standard_clutch
  GROUP BY sg.game_id, sg.team_id, atc.action_player_id
),
official_counts AS (
  SELECT
    r.game_id, r.team_id, r.player_id,
    coalesce((r.boxscore_stats ->> 'Points')::numeric, 0) AS pts,
    coalesce((r.boxscore_stats ->> 'TotalRebounds')::numeric, 0) AS reb,
    coalesce((r.boxscore_stats ->> 'OffensiveRebounds')::numeric, 0) AS oreb,
    coalesce((r.boxscore_stats ->> 'DefensiveRebounds')::numeric, 0) AS dreb,
    coalesce((r.boxscore_stats ->> 'Assistances')::numeric, 0) AS ast,
    coalesce((r.boxscore_stats ->> 'Steals')::numeric, 0) AS stl,
    coalesce((r.boxscore_stats ->> 'BlocksFavour')::numeric, 0) AS blk,
    coalesce((r.boxscore_stats ->> 'Turnovers')::numeric, 0) AS tov,
    coalesce((r.boxscore_stats ->> 'FieldGoalsMade2')::numeric, 0) AS fg2m,
    coalesce((r.boxscore_stats ->> 'FieldGoalsAttempted2')::numeric, 0) AS fg2a,
    coalesce((r.boxscore_stats ->> 'FieldGoalsMade3')::numeric, 0) AS fg3m,
    coalesce((r.boxscore_stats ->> 'FieldGoalsAttempted3')::numeric, 0) AS fg3a,
    coalesce((r.boxscore_stats ->> 'FreeThrowsMade')::numeric, 0) AS ftm,
    coalesce((r.boxscore_stats ->> 'FreeThrowsAttempted')::numeric, 0) AS fta
  FROM roster r CROSS JOIN params p
  WHERE NOT p.is_clutch
),
clutch_counts AS (
  SELECT
    r.game_id, r.team_id, r.player_id,
    c.pts, c.reb, c.oreb, c.dreb, c.ast, c.stl, c.blk, c.tov,
    c.fg2m, c.fg2a, c.fg3m, c.fg3a, c.ftm, c.fta
  FROM roster r
  CROSS JOIN params p
  JOIN euroleague.default_clutch_player_totals_by_game c
    ON c.game_id = r.game_id AND c.team_id = r.team_id
   AND c.player_id = r.player_id
  WHERE p.is_standard_clutch

  UNION ALL

  SELECT
    r.game_id, r.team_id, r.player_id,
    coalesce(sum(atc.points), 0)::numeric AS pts,
    coalesce(sum(atc.orebounds + CASE WHEN atc.play_type = 'D' THEN 1 ELSE 0 END), 0)::numeric AS reb,
    coalesce(sum(atc.orebounds), 0)::numeric AS oreb,
    coalesce(sum(CASE WHEN atc.play_type = 'D' THEN 1 ELSE 0 END), 0)::numeric AS dreb,
    coalesce(sum(CASE WHEN atc.play_type = 'AS' THEN 1 ELSE 0 END), 0)::numeric AS ast,
    coalesce(sum(CASE WHEN atc.play_type = 'ST' THEN 1 ELSE 0 END), 0)::numeric AS stl,
    coalesce(sum(CASE WHEN atc.play_type = 'FV' THEN 1 ELSE 0 END), 0)::numeric AS blk,
    coalesce(sum(atc.turnovers), 0)::numeric AS tov,
    coalesce(sum(atc.fg2_made), 0)::numeric AS fg2m,
    coalesce(sum(atc.fg2_att), 0)::numeric AS fg2a,
    coalesce(sum(atc.fg3_made), 0)::numeric AS fg3m,
    coalesce(sum(atc.fg3_att), 0)::numeric AS fg3a,
    coalesce(sum(CASE WHEN atc.play_type = 'FTM' THEN 1 ELSE 0 END), 0)::numeric AS ftm,
    coalesce(sum(atc.ft_attempts), 0)::numeric AS fta
  FROM roster r
  CROSS JOIN params p
  LEFT JOIN euroleague.action_team_context_actions atc
    ON atc.game_id = r.game_id
   AND atc.team_id = r.team_id
   AND atc.action_player_id = r.player_id
   AND euroleague.clutch_event_qualifies(
         atc.period, atc.event_elapsed_seconds,
         atc.own_team_score
           - CASE WHEN atc.event_team_id = atc.team_id THEN atc.points ELSE 0 END,
         atc.opp_team_score
           - CASE WHEN atc.event_team_id = atc.opponent_team_id THEN atc.points ELSE 0 END,
         p_max_margin, p_margin_status, p_max_time_remaining,
         p_ot_margin_filter
       )
  WHERE p.is_clutch AND NOT p.is_standard_clutch
  GROUP BY r.game_id, r.team_id, r.player_id
),
counts AS (
  SELECT * FROM official_counts
  UNION ALL
  SELECT * FROM clutch_counts
),
player_games AS (
  SELECT
    r.game_id, r.team_id, r.player_id, r.team_name, r.player_name,
    e.poss_on_floor, e.minutes,
    coalesce(pu.player_ts_poss, 0)::numeric AS player_ts_poss,
    coalesce(pu.player_tov, 0)::numeric AS player_tov,
    coalesce(tu.team_ts_poss, 0)::numeric AS team_ts_poss,
    coalesce(tu.team_tov, 0)::numeric AS team_tov,
    coalesce(tu.team_poss, 0)::numeric AS team_poss,
    c.pts, c.reb, c.oreb, c.dreb, c.ast, c.stl, c.blk, c.tov,
    c.fg2m, c.fg2a, c.fg3m, c.fg3a, c.ftm, c.fta
  FROM roster r
  JOIN exposure e USING (game_id, team_id, player_id)
  JOIN counts c USING (game_id, team_id, player_id)
  LEFT JOIN player_usage pu USING (game_id, team_id, player_id)
  LEFT JOIN team_usage tu USING (game_id, team_id)
  WHERE e.minutes > 0 OR e.poss_on_floor > 0
     OR c.pts + c.reb + c.ast + c.stl + c.blk + c.tov
        + c.fg2a + c.fg3a + c.fta > 0
),
agg AS (
  SELECT
    team_id, player_id, min(team_name) AS team_name,
    min(player_name) AS player_name, count(DISTINCT game_id)::integer AS gp,
    sum(poss_on_floor)::numeric AS poss_on_floor,
    sum(minutes)::numeric AS minutes,
    sum(player_ts_poss)::numeric AS player_ts_poss,
    sum(player_tov)::numeric AS player_tov,
    sum(team_ts_poss)::numeric AS team_ts_poss,
    sum(team_tov)::numeric AS team_tov,
    sum(team_poss)::numeric AS team_poss,
    sum(pts)::numeric AS pts, sum(reb)::numeric AS reb,
    sum(oreb)::numeric AS oreb, sum(dreb)::numeric AS dreb,
    sum(ast)::numeric AS ast, sum(stl)::numeric AS stl,
    sum(blk)::numeric AS blk, sum(tov)::numeric AS tov,
    sum(fg2m)::numeric AS fg2m, sum(fg2a)::numeric AS fg2a,
    sum(fg3m)::numeric AS fg3m, sum(fg3a)::numeric AS fg3a,
    sum(ftm)::numeric AS ftm, sum(fta)::numeric AS fta
  FROM player_games
  GROUP BY team_id, player_id
)
SELECT
  team_id, player_id, team_name, player_name AS "Player", gp,
  poss_on_floor, minutes, pts, reb, oreb, dreb, ast, stl, blk,
  NULL::numeric AS dfl, tov,
  fg2m + fg3m AS fgm, fg2a + fg3a AS fga,
  round(100 * (fg2m + fg3m) / nullif(fg2a + fg3a, 0), 1) AS fg_pct,
  fg3m AS "3pm", fg3a AS "3pa",
  round(100 * fg3m / nullif(fg3a, 0), 1) AS tp_pct,
  ftm, fta, round(100 * ftm / nullif(fta, 0), 1) AS ft_pct,
  round(100 * (fg2m + 1.5 * fg3m) / nullif(fg2a + fg3a, 0), 1) AS efg,
  round(100 * pts / nullif(2 * player_ts_poss, 0), 1) AS ts,
  round(
    100 * (player_ts_poss + player_tov) * team_poss
      / nullif((team_ts_poss + team_tov) * poss_on_floor, 0),
    1
  ) AS usg_pct
FROM agg
$function$;

CREATE OR REPLACE FUNCTION euroleague.refresh_app_materialized_views()
RETURNS void
LANGUAGE plpgsql
AS $function$
BEGIN
  REFRESH MATERIALIZED VIEW euroleague.final_schedule_mv;
  REFRESH MATERIALIZED VIEW euroleague.team_game_ratings_mv;
  REFRESH MATERIALIZED VIEW euroleague.team_ppp_ratings_mv;
  REFRESH MATERIALIZED VIEW euroleague.team_four_factors_mv;
  REFRESH MATERIALIZED VIEW euroleague.player_onoff_default_mv;
  REFRESH MATERIALIZED VIEW euroleague.player_advanced_stats_mv;
  REFRESH MATERIALIZED VIEW euroleague.sub_lineups_stats_mv;
  REFRESH MATERIALIZED VIEW euroleague.player_traditional_stats_mv;
END;
$function$;

GRANT SELECT ON euroleague.player_traditional_stats_mv TO app_readonly;
GRANT SELECT ON euroleague.default_clutch_player_totals_by_game TO app_readonly;

REVOKE ALL ON FUNCTION euroleague.get_player_traditional_dynamic(
  text, int4, date, date, text, text, text, text, text, text, int4,
  text, int4, text, int4, bool, int4, int4, int4
) FROM PUBLIC;

GRANT EXECUTE ON FUNCTION euroleague.get_player_traditional_dynamic(
  text, int4, date, date, text, text, text, text, text, text, int4,
  text, int4, text, int4, bool, int4, int4, int4
) TO app_readonly;

SELECT euroleague.refresh_app_materialized_views();

COMMIT;
