-- Credit-attribution definition of player_traditional_by_game, EuroLeague migration 062 (EUROLEAGUE SHADOW SCHEMA),
-- written by scripts/apply_062_readers.py from the live definition.

CREATE OR REPLACE FUNCTION euroleague.refresh_player_traditional_by_game_for_games(p_game_ids bigint[])
 RETURNS bigint
 LANGUAGE plpgsql
 SET search_path TO 'pg_catalog', 'euroleague', 'public'
AS $function$
DECLARE
  inserted_count bigint := 0;
BEGIN
  IF p_game_ids IS NULL OR array_length(p_game_ids, 1) IS NULL THEN
    DELETE FROM euroleague.player_traditional_by_game;
  ELSE
    DELETE FROM euroleague.player_traditional_by_game
    WHERE game_id = ANY(p_game_ids);
  END IF;

  INSERT INTO euroleague.player_traditional_by_game (
    competition, game_year, game_id, team_id, player_id, load_run_id,
    gp, poss_on_floor, minutes,
    pts, reb, oreb, dreb, ast, stl, blk, tov,
    fg2m, fg2a, fg3m, fg3a, ftm, fta,
    player_ts_poss, player_tov, team_ts_poss, team_tov, team_poss,
    derivation_version, derived_at
  )
  WITH roster AS MATERIALIZED (
    SELECT s.competition, s.season::integer AS game_year,
      fr.game_id, fr.team_id, fr.player_id, fr.load_run_id,
      coalesce((fr.boxscore_stats ->> 'Points')::numeric, 0) pts,
      coalesce((fr.boxscore_stats ->> 'TotalRebounds')::numeric, 0) reb,
      coalesce((fr.boxscore_stats ->> 'OffensiveRebounds')::numeric, 0) oreb,
      coalesce((fr.boxscore_stats ->> 'DefensiveRebounds')::numeric, 0) dreb,
      coalesce((fr.boxscore_stats ->> 'Assistances')::numeric, 0) ast,
      coalesce((fr.boxscore_stats ->> 'Steals')::numeric, 0) stl,
      coalesce((fr.boxscore_stats ->> 'BlocksFavour')::numeric, 0) blk,
      coalesce((fr.boxscore_stats ->> 'Turnovers')::numeric, 0) tov,
      coalesce((fr.boxscore_stats ->> 'FieldGoalsMade2')::numeric, 0) fg2m,
      coalesce((fr.boxscore_stats ->> 'FieldGoalsAttempted2')::numeric, 0) fg2a,
      coalesce((fr.boxscore_stats ->> 'FieldGoalsMade3')::numeric, 0) fg3m,
      coalesce((fr.boxscore_stats ->> 'FieldGoalsAttempted3')::numeric, 0) fg3a,
      coalesce((fr.boxscore_stats ->> 'FreeThrowsMade')::numeric, 0) ftm,
      coalesce((fr.boxscore_stats ->> 'FreeThrowsAttempted')::numeric, 0) fta
    FROM euroleague.full_rosters fr
    JOIN euroleague.schedule s ON s.game_id = fr.game_id
    JOIN euroleague.players p ON p.player_id = fr.player_id
    WHERE (p_game_ids IS NULL OR fr.game_id = ANY(p_game_ids))
      AND lower(p.provider_player_id) NOT IN ('team', 'total')
      AND lower(btrim(p.display_name)) NOT IN ('team', 'total')
  ), exposure AS MATERIALIZED (
    -- lineup_totals already carries the resolved five player IDs. Expanding
    -- those five IDs once during publication preserves the legacy reader's
    -- exact lineup seconds without crossing the roster against lineup names.
    SELECT l.game_id, l.team_id, u.player_id,
      coalesce(sum(l.possessions), 0)::numeric AS poss_on_floor,
      coalesce(sum(l.seconds), 0)::numeric / 60.0 AS minutes
    FROM euroleague.lineup_totals_by_game l
    CROSS JOIN LATERAL unnest(l.player_ids) AS u(player_id)
    WHERE l.type_lineup = 'offense'
      AND (p_game_ids IS NULL OR l.game_id = ANY(p_game_ids))
    GROUP BY l.game_id, l.team_id, u.player_id
  ), floor_poss AS MATERIALIZED (
    -- Migration 062: on the floor at a possession end = played the
    -- game, whichever lineup the possession is credited to.
    SELECT DISTINCT a.game_id, a.team_id, u.player_id
    FROM euroleague.player_stats_actions_by_game a
    JOIN euroleague.lineup_totals_by_game l
      ON l.game_id = a.game_id AND l.team_id = a.team_id
     AND l.own_lineup = a.own_lineup AND l.type_lineup = 'offense'
    CROSS JOIN LATERAL unnest(l.player_ids) AS u(player_id)
    WHERE a.type_lineup = 'offense' AND a.possession_flag = 1
      AND (p_game_ids IS NULL OR a.game_id = ANY(p_game_ids))
  ), player_usage AS MATERIALIZED (
    SELECT pf.game_id, pf.team_id, pf.player_id,
      coalesce(sum(pf.player_ts_poss_count), 0)::numeric AS player_ts_poss,
      coalesce(sum(pf.player_tov_count), 0)::numeric AS player_tov
    FROM euroleague.player_four_factors_by_game pf
    WHERE pf.type_lineup = 'offense'
      AND (p_game_ids IS NULL OR pf.game_id = ANY(p_game_ids))
    GROUP BY pf.game_id, pf.team_id, pf.player_id
  ), team_usage AS MATERIALIZED (
    SELECT tf.game_id, tf.team_id,
      coalesce(sum(tf.off_ts_poss), 0)::numeric AS team_ts_poss,
      coalesce(sum(tf.off_tov), 0)::numeric AS team_tov,
      coalesce(sum(tf.off_poss), 0)::numeric AS team_poss
    FROM euroleague.team_four_factors_by_game tf
    WHERE p_game_ids IS NULL OR tf.game_id = ANY(p_game_ids)
    GROUP BY tf.game_id, tf.team_id
  ), typed AS (
    SELECT r.*,
      coalesce(e.poss_on_floor, 0)::numeric AS poss_on_floor,
      coalesce(e.minutes, 0)::numeric AS minutes,
      coalesce(pu.player_ts_poss, 0)::numeric AS player_ts_poss,
      coalesce(pu.player_tov, 0)::numeric AS player_tov,
      coalesce(t.team_ts_poss, 0)::numeric AS team_ts_poss,
      coalesce(t.team_tov, 0)::numeric AS team_tov,
      coalesce(t.team_poss, 0)::numeric AS team_poss,
      (fp.player_id IS NOT NULL) AS on_floor_at_poss
    FROM roster r
    LEFT JOIN exposure e
      ON e.game_id = r.game_id AND e.team_id = r.team_id
     AND e.player_id = r.player_id
    LEFT JOIN player_usage pu
      ON pu.game_id = r.game_id AND pu.team_id = r.team_id
     AND pu.player_id = r.player_id
    LEFT JOIN team_usage t
      ON t.game_id = r.game_id AND t.team_id = r.team_id
    LEFT JOIN floor_poss fp
      ON fp.game_id = r.game_id AND fp.team_id = r.team_id AND fp.player_id = r.player_id
  )
  SELECT competition, game_year, game_id, team_id, player_id, load_run_id,
    1, poss_on_floor, minutes,
    pts, reb, oreb, dreb, ast, stl, blk, tov,
    fg2m, fg2a, fg3m, fg3a, ftm, fta,
    player_ts_poss, player_tov, team_ts_poss, team_tov, team_poss,
    'player-traditional-pergame-v1', now()
  FROM typed
  WHERE minutes > 0 OR poss_on_floor > 0 OR on_floor_at_poss
     OR pts + reb + ast + stl + blk + tov + fg2a + fg3a + fta > 0;

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$function$;
