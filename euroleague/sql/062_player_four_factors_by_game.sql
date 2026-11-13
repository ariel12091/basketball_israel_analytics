-- Credit-attribution definition of player_four_factors_by_game, EuroLeague migration 062 (EUROLEAGUE SHADOW SCHEMA),
-- written by scripts/apply_062_readers.py from the live definition.

CREATE OR REPLACE FUNCTION euroleague.refresh_player_four_factors_by_game_for_games(game_ids bigint[])
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
DECLARE
  inserted_count bigint := 0;
BEGIN
  IF game_ids IS NULL OR array_length(game_ids, 1) IS NULL THEN
    DELETE FROM euroleague.player_four_factors_by_game;
  ELSE
    DELETE FROM euroleague.player_four_factors_by_game
     WHERE game_id = ANY(game_ids);
  END IF;

  INSERT INTO euroleague.player_four_factors_by_game (
    game_id, team_id, player_id, is_on_key, type_lineup,
    game_year, num_starters, own_starters, opp_starters,
    total_points, total_poss, ts_poss_count, oreb_count,
    oreb_opportunities, tov_count, steal_count, total_ft_attempts,
    total_fga, total_fgm, total_fg3_made,
    player_ts_poss_count, player_tov_count, minutes,
    fg2_made, fg2_att, fg3_made, fg3_att,
    layup_made, layup_att, dunk_made, dunk_att, onoff_minutes,
    deflection_count, c3_made, c3_att, c3_known_att,
    load_run_id, derivation_version
  )
  WITH real_roster AS (
    SELECT fr.game_id, fr.team_id, fr.player_id, fr.source_player_name
      FROM euroleague.full_rosters fr
      JOIN euroleague.players p ON p.player_id = fr.player_id
     WHERE (game_ids IS NULL OR fr.game_id = ANY(game_ids))
       AND lower(p.provider_player_id) NOT IN ('team', 'total')
       AND lower(btrim(p.display_name)) NOT IN ('team', 'total')
  ),
  player_minutes AS MATERIALIZED (
    SELECT
      ms.game_id,
      ms.team_id,
      rr.player_id,
      CASE WHEN rr.source_player_name = ANY(ms.own_lineup)
           THEN 1 ELSE 0 END::smallint AS is_on_key,
      ms.own_starters,
      ms.opp_starters,
      round(sum(ms.segment_seconds) / 60.0, 3) AS minutes
    FROM euroleague.matchup_segments_actions ms
    JOIN real_roster rr
      ON rr.game_id = ms.game_id AND rr.team_id = ms.team_id
   WHERE game_ids IS NULL OR ms.game_id = ANY(game_ids)
   GROUP BY ms.game_id, ms.team_id, rr.player_id,
            CASE WHEN rr.source_player_name = ANY(ms.own_lineup)
                 THEN 1 ELSE 0 END,
            ms.own_starters, ms.opp_starters
  ),
  complete_grid AS (
    SELECT pm.game_id, pm.team_id, pm.player_id, pm.is_on_key,
           pm.own_starters, pm.opp_starters, pm.minutes,
           side.type_lineup
      FROM player_minutes pm
      CROSS JOIN (
        VALUES ('offense'::text), ('defense'::text)
      ) AS side(type_lineup)
  ),
  counts AS MATERIALIZED (
    SELECT
      atc.game_id,
      atc.team_id,
      rr.player_id,
      CASE WHEN rr.source_player_name = ANY(atc.credit_own_lineup)
           THEN 1 ELSE 0 END::smallint AS is_on_key,
      atc.type_lineup,
      atc.credit_own_starters AS own_starters,
      atc.credit_opp_starters AS opp_starters,
      sum(atc.points)::numeric AS total_points,
      sum(atc.possession_flag)::bigint AS total_poss,
      sum(atc.ts_possessions)::bigint AS ts_poss_count,
      sum(atc.orebounds)::bigint AS oreb_count,
      sum(atc.oreb_opportunities)::bigint AS oreb_opportunities,
      sum(atc.turnovers)::bigint AS tov_count,
      sum(atc.steals)::bigint AS steal_count,
      sum(atc.ft_attempts)::bigint AS total_ft_attempts,
      sum(atc.fga)::bigint AS total_fga,
      sum(atc.fgm)::bigint AS total_fgm,
      sum(atc.fg3_made)::bigint AS total_fg3_made,
      sum(CASE WHEN atc.type_lineup = 'offense'
                AND atc.action_player_id = rr.player_id
               THEN atc.ts_possessions ELSE 0 END)::bigint
        AS player_ts_poss_count,
      sum(CASE WHEN atc.type_lineup = 'offense'
                AND atc.action_player_id = rr.player_id
               THEN atc.turnovers ELSE 0 END)::bigint
        AS player_tov_count,
      sum(atc.fg2_made)::integer AS fg2_made,
      sum(atc.fg2_att)::integer AS fg2_att,
      sum(atc.fg3_made)::integer AS fg3_made,
      sum(atc.fg3_att)::integer AS fg3_att,
      sum(atc.layup_made)::integer AS layup_made,
      sum(atc.layup_att)::integer AS layup_att,
      sum(atc.dunk_made)::integer AS dunk_made,
      sum(atc.dunk_att)::integer AS dunk_att
    FROM euroleague.action_team_context_actions atc
    JOIN real_roster rr
      ON rr.game_id = atc.game_id AND rr.team_id = atc.team_id
   WHERE (game_ids IS NULL OR atc.game_id = ANY(game_ids))
     AND atc.type_lineup IS NOT NULL
   GROUP BY atc.game_id, atc.team_id, rr.player_id,
            CASE WHEN rr.source_player_name = ANY(atc.credit_own_lineup)
                 THEN 1 ELSE 0 END,
            atc.type_lineup, atc.credit_own_starters, atc.credit_opp_starters
  )
  SELECT
    cg.game_id,
    cg.team_id,
    cg.player_id,
    cg.is_on_key,
    cg.type_lineup,
    s.season AS game_year,
    cg.own_starters AS num_starters,
    cg.own_starters,
    cg.opp_starters,
    coalesce(c.total_points, 0)::numeric,
    coalesce(c.total_poss, 0)::bigint,
    coalesce(c.ts_poss_count, 0)::bigint,
    coalesce(c.oreb_count, 0)::bigint,
    coalesce(c.oreb_opportunities, 0)::bigint,
    coalesce(c.tov_count, 0)::bigint,
    coalesce(c.steal_count, 0)::bigint,
    coalesce(c.total_ft_attempts, 0)::bigint,
    coalesce(c.total_fga, 0)::bigint,
    coalesce(c.total_fgm, 0)::bigint,
    coalesce(c.total_fg3_made, 0)::bigint,
    coalesce(c.player_ts_poss_count, 0)::bigint,
    coalesce(c.player_tov_count, 0)::bigint,
    CASE WHEN cg.type_lineup = 'offense'
         THEN coalesce(cg.minutes, 0) ELSE 0 END::numeric,
    coalesce(c.fg2_made, 0)::integer,
    coalesce(c.fg2_att, 0)::integer,
    coalesce(c.fg3_made, 0)::integer,
    coalesce(c.fg3_att, 0)::integer,
    coalesce(c.layup_made, 0)::integer,
    coalesce(c.layup_att, 0)::integer,
    coalesce(c.dunk_made, 0)::integer,
    coalesce(c.dunk_att, 0)::integer,
    CASE WHEN cg.type_lineup = 'offense'
         THEN coalesce(cg.minutes, 0) ELSE 0 END::numeric,
    0, 0, 0, 0,
    s.last_seen_load_run_id,
    'actions-v1'
  FROM complete_grid cg
  JOIN euroleague.schedule s ON s.game_id = cg.game_id
  LEFT JOIN counts c
    ON c.game_id = cg.game_id
   AND c.team_id = cg.team_id
   AND c.player_id = cg.player_id
   AND c.is_on_key = cg.is_on_key
   AND c.type_lineup = cg.type_lineup
   AND c.own_starters = cg.own_starters
   AND c.opp_starters = cg.opp_starters;

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$function$;
