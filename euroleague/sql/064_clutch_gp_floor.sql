-- 064_clutch_gp_floor.sql
--
-- EuroLeague shadow schema. Closes D40 (docs/shared_definitions.md).
--
-- The standard-clutch player reader counted a clutch game played from clutch
-- minutes, a box-score stat, or CREDITED clutch possessions. Since migration
-- 062 a free throw is credited to the lineup that drew the foul, so a player
-- subbed in only for the free throws (0 clutch seconds, physically on the
-- floor at the possession end) lost that game, and one subbed out before them
-- could gain it. Rule R6: events follow credit, floor presence follows the
-- floor. The Israeli per-game player table counts a game when the player is
-- physically on the floor at one of his team's offensive possession ends
-- (compute_player_traditional_by_game, usage_floor).
--
-- 1. default_clutch_player_totals_by_game gains floor_possessions: the clutch
--    offensive possession ends the player was physically on the floor for.
--    A player on the floor with no action of his own now has a row with zero
--    box-score columns (sums are unchanged).
-- 2. refresh_default_clutch_player_for_games() fills it.
-- 3. get_player_traditional_standard_clutch() counts gp from floor_possessions
--    instead of credited possessions. Every sum keeps exactly the rows it had
--    (poss_on_floor, the usage denominator, still follows credit), so only gp
--    changes; a player left with gp 0 (credited, never on the floor at a
--    clutch possession end) is not listed.
--
-- Applied by scripts/apply_064_clutch_gp_floor.py (dry run by default).

ALTER TABLE euroleague.default_clutch_player_totals_by_game
  ADD COLUMN floor_possessions numeric NOT NULL DEFAULT 0;

CREATE OR REPLACE FUNCTION euroleague.refresh_default_clutch_player_for_games(game_ids bigint[])
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
DECLARE
  target_game_ids BIGINT[];
  inserted_count BIGINT := 0;
BEGIN
  IF game_ids IS NULL OR array_length(game_ids, 1) IS NULL THEN
    SELECT array_agg(s.game_id ORDER BY s.game_id) INTO target_game_ids
    FROM euroleague.schedule s;
    DELETE FROM euroleague.default_clutch_player_totals_by_game;
  ELSE
    SELECT array_agg(DISTINCT x ORDER BY x) INTO target_game_ids
    FROM unnest(game_ids) x;
    DELETE FROM euroleague.default_clutch_player_totals_by_game c
    WHERE c.game_id = ANY(target_game_ids);
  END IF;

  IF target_game_ids IS NULL OR array_length(target_game_ids, 1) IS NULL THEN
    RETURN 0;
  END IF;

  INSERT INTO euroleague.default_clutch_player_totals_by_game (
    game_id, team_id, player_id, pts, reb, oreb, dreb, ast, stl, blk, tov,
    fg2m, fg2a, fg3m, fg3a, ftm, fta, player_ts_poss, player_tov,
    floor_possessions, derivation_version
  )
  WITH clutch_rows AS MATERIALIZED (
    SELECT atc.*
    FROM euroleague.action_team_context_actions atc
    WHERE atc.game_id = ANY(target_game_ids)
      AND atc.type_lineup = 'offense'
      AND euroleague.clutch_event_qualifies(
        atc.period, atc.event_elapsed_seconds,
        atc.own_team_score - CASE WHEN atc.event_team_id = atc.team_id THEN atc.points ELSE 0 END,
        atc.opp_team_score - CASE WHEN atc.event_team_id = atc.opponent_team_id THEN atc.points ELSE 0 END,
        5, 'all', 300, false
      )
  ),
  box AS (
    SELECT
      atc.game_id,
      atc.team_id,
      atc.action_player_id AS player_id,
      sum(atc.points)::numeric AS pts,
      sum(atc.orebounds + CASE WHEN atc.play_type = 'D' THEN 1 ELSE 0 END)::numeric AS reb,
      sum(atc.orebounds)::numeric AS oreb,
      sum(CASE WHEN atc.play_type = 'D' THEN 1 ELSE 0 END)::numeric AS dreb,
      sum(CASE WHEN atc.play_type = 'AS' THEN 1 ELSE 0 END)::numeric AS ast,
      sum(CASE WHEN atc.play_type = 'ST' THEN 1 ELSE 0 END)::numeric AS stl,
      sum(CASE WHEN atc.play_type = 'FV' THEN 1 ELSE 0 END)::numeric AS blk,
      sum(atc.turnovers)::numeric AS tov,
      sum(atc.fg2_made)::numeric AS fg2m,
      sum(atc.fg2_att)::numeric AS fg2a,
      sum(atc.fg3_made)::numeric AS fg3m,
      sum(atc.fg3_att)::numeric AS fg3a,
      sum(CASE WHEN atc.play_type = 'FTM' THEN 1 ELSE 0 END)::numeric AS ftm,
      sum(atc.ft_attempts)::numeric AS fta,
      sum(atc.ts_possessions)::numeric AS player_ts_poss,
      sum(atc.turnovers)::numeric AS player_tov
    FROM clutch_rows atc
    JOIN euroleague.players p ON p.player_id = atc.action_player_id
    WHERE atc.action_player_id IS NOT NULL
      AND lower(p.provider_player_id) NOT IN ('team', 'total')
    GROUP BY atc.game_id, atc.team_id, atc.action_player_id
  ),
  -- Physical lineup (own_lineup, not credit_own_lineup) on each possession end.
  floor AS (
    SELECT atc.game_id, atc.team_id, fr.player_id,
           sum(atc.possession_flag)::numeric AS floor_possessions
    FROM clutch_rows atc
    CROSS JOIN LATERAL unnest(atc.own_lineup) AS on_floor(source_player_name)
    JOIN euroleague.full_rosters fr
      ON fr.game_id = atc.game_id AND fr.team_id = atc.team_id
     AND fr.source_player_name = on_floor.source_player_name
    JOIN euroleague.players p ON p.player_id = fr.player_id
    WHERE atc.possession_flag > 0
      AND lower(p.provider_player_id) NOT IN ('team', 'total')
    GROUP BY atc.game_id, atc.team_id, fr.player_id
  )
  SELECT
    game_id, team_id, player_id,
    coalesce(b.pts, 0), coalesce(b.reb, 0), coalesce(b.oreb, 0), coalesce(b.dreb, 0),
    coalesce(b.ast, 0), coalesce(b.stl, 0), coalesce(b.blk, 0), coalesce(b.tov, 0),
    coalesce(b.fg2m, 0), coalesce(b.fg2a, 0), coalesce(b.fg3m, 0), coalesce(b.fg3a, 0),
    coalesce(b.ftm, 0), coalesce(b.fta, 0), coalesce(b.player_ts_poss, 0),
    coalesce(b.player_tov, 0), coalesce(f.floor_possessions, 0),
    'default-clutch-player-v2'
  FROM box b
  FULL JOIN floor f USING (game_id, team_id, player_id);

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$function$;

CREATE OR REPLACE FUNCTION euroleague.get_player_traditional_standard_clutch(p_competition text, p_game_year integer, p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date, p_team_ids_csv text DEFAULT NULL::text, p_phase_csv text DEFAULT NULL::text, p_opp_ids_csv text DEFAULT NULL::text, p_home_away text DEFAULT 'all'::text, p_outcome text DEFAULT 'all'::text, p_opp_rank_side text DEFAULT NULL::text, p_opp_rank_n integer DEFAULT NULL::integer, p_opp_rank_metric text DEFAULT NULL::text, p_min_gn integer DEFAULT NULL::integer, p_max_gn integer DEFAULT NULL::integer, p_last_n_games integer DEFAULT NULL::integer)
 RETURNS TABLE(team_id bigint, player_id bigint, team_name text, "Player" text, gp integer, poss_on_floor numeric, minutes numeric, pts numeric, reb numeric, oreb numeric, dreb numeric, ast numeric, stl numeric, blk numeric, dfl numeric, tov numeric, fgm numeric, fga numeric, fg_pct numeric, "3pm" numeric, "3pa" numeric, tp_pct numeric, ftm numeric, fta numeric, ft_pct numeric, efg numeric, ts numeric, usg_pct numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'euroleague', 'public'
 SET plan_cache_mode TO 'force_custom_plan'
AS $function$
WITH filtered_facts AS MATERIALIZED (
  SELECT f.*
  FROM euroleague.filtered_team_game_facts(
    p_competition, p_game_year, p_start_date, p_end_date,
    p_team_ids_csv, p_phase_csv, p_opp_ids_csv, p_home_away, p_outcome,
    p_opp_rank_side, p_opp_rank_n, p_opp_rank_metric,
    5, 'all', 300, false, p_min_gn, p_max_gn, p_last_n_games,
    NULL, NULL, NULL, NULL
  ) f
),
selected_games AS MATERIALIZED (
  SELECT DISTINCT game_id, team_id, team_name FROM filtered_facts
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
  FROM filtered_facts
  WHERE type_lineup = 'offense'
  GROUP BY game_id, team_id
),
player_games AS (
  SELECT r.game_id, r.team_id, r.player_id, r.team_name, r.player_name,
         e.poss_on_floor, e.minutes,
         -- D40: the game counts towards gp from floor presence, not credited
         -- possessions; the sums keep the rows they always had (in_sums).
         (e.minutes > 0 OR coalesce(c.floor_possessions, 0) > 0
          OR coalesce(c.pts + c.reb + c.ast + c.stl + c.blk + c.tov
                      + c.fg2a + c.fg3a + c.fta, 0) > 0) AS plays,
         coalesce(c.player_ts_poss, 0)::numeric AS player_ts_poss,
         coalesce(c.player_tov, 0)::numeric AS player_tov,
         CASE WHEN in_sums THEN coalesce(tu.team_ts_poss, 0) ELSE 0 END::numeric AS team_ts_poss,
         CASE WHEN in_sums THEN coalesce(tu.team_tov, 0) ELSE 0 END::numeric AS team_tov,
         CASE WHEN in_sums THEN coalesce(tu.team_poss, 0) ELSE 0 END::numeric AS team_poss,
         coalesce(c.pts, 0)::numeric AS pts,
         coalesce(c.reb, 0)::numeric AS reb,
         coalesce(c.oreb, 0)::numeric AS oreb,
         coalesce(c.dreb, 0)::numeric AS dreb,
         coalesce(c.ast, 0)::numeric AS ast,
         coalesce(c.stl, 0)::numeric AS stl,
         coalesce(c.blk, 0)::numeric AS blk,
         coalesce(c.tov, 0)::numeric AS tov,
         coalesce(c.fg2m, 0)::numeric AS fg2m,
         coalesce(c.fg2a, 0)::numeric AS fg2a,
         coalesce(c.fg3m, 0)::numeric AS fg3m,
         coalesce(c.fg3a, 0)::numeric AS fg3a,
         coalesce(c.ftm, 0)::numeric AS ftm,
         coalesce(c.fta, 0)::numeric AS fta
  FROM roster r
  JOIN exposure e USING (game_id, team_id, player_id)
  LEFT JOIN euroleague.default_clutch_player_totals_by_game c
    USING (game_id, team_id, player_id)
  LEFT JOIN team_usage tu USING (game_id, team_id)
  CROSS JOIN LATERAL (SELECT e.minutes > 0 OR e.poss_on_floor > 0
     OR coalesce(c.pts + c.reb + c.ast + c.stl + c.blk + c.tov
                 + c.fg2a + c.fg3a + c.fta, 0) > 0 AS in_sums) s
  WHERE s.in_sums OR coalesce(c.floor_possessions, 0) > 0
),
agg AS (
  SELECT team_id, player_id, min(team_name) AS team_name,
         min(player_name) AS player_name,
         count(DISTINCT game_id) FILTER (WHERE plays)::integer AS gp,
         sum(poss_on_floor)::numeric AS poss_on_floor, sum(minutes)::numeric AS minutes,
         sum(player_ts_poss)::numeric AS player_ts_poss,
         sum(player_tov)::numeric AS player_tov,
         sum(team_ts_poss)::numeric AS team_ts_poss,
         sum(team_tov)::numeric AS team_tov, sum(team_poss)::numeric AS team_poss,
         sum(pts)::numeric AS pts, sum(reb)::numeric AS reb,
         sum(oreb)::numeric AS oreb, sum(dreb)::numeric AS dreb,
         sum(ast)::numeric AS ast, sum(stl)::numeric AS stl,
         sum(blk)::numeric AS blk, sum(tov)::numeric AS tov,
         sum(fg2m)::numeric AS fg2m, sum(fg2a)::numeric AS fg2a,
         sum(fg3m)::numeric AS fg3m, sum(fg3a)::numeric AS fg3a,
         sum(ftm)::numeric AS ftm, sum(fta)::numeric AS fta
  FROM player_games GROUP BY team_id, player_id
  HAVING count(DISTINCT game_id) FILTER (WHERE plays) > 0
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
