-- Definition before of lineup_totals_by_game, EuroLeague migration 062 (EUROLEAGUE SHADOW SCHEMA),
-- written by scripts/apply_062_readers.py from the live definition.

CREATE OR REPLACE FUNCTION euroleague.refresh_lineup_totals_by_game(game_ids bigint[])
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
DECLARE
  inserted_count bigint := 0;
BEGIN
  IF game_ids IS NULL OR array_length(game_ids, 1) IS NULL THEN
    DELETE FROM euroleague.lineup_totals_by_game;
  ELSE
    DELETE FROM euroleague.lineup_totals_by_game WHERE game_id = ANY(game_ids);
  END IF;

  INSERT INTO euroleague.lineup_totals_by_game (
    game_id, team_id, lineup_key, type_lineup, opp_starters,
    competition, game_year, own_starters, own_lineup, player_ids,
    possessions, points, fg2_made, fg2_att, fg3_made, fg3_att,
    ts_possessions, fgm, fga, ft_attempts,
    orebounds, oreb_opportunities, turnovers, steals, seconds,
    load_run_id, derivation_version, action_span_seconds
  )
  WITH real_roster AS (
    SELECT fr.game_id, fr.team_id, fr.player_id, fr.source_player_name
    FROM euroleague.full_rosters fr
    JOIN euroleague.players p ON p.player_id = fr.player_id
    WHERE (game_ids IS NULL OR fr.game_id = ANY(game_ids))
      AND lower(p.provider_player_id) NOT IN ('team', 'total')
      AND lower(btrim(p.display_name)) NOT IN ('team', 'total')
  ), segment_totals AS (
    SELECT ms.game_id, ms.team_id, ms.own_lineup, ms.opp_starters,
      max(ms.own_starters) AS own_starters,
      sum(ms.segment_seconds) AS seconds
    FROM euroleague.matchup_segments_actions ms
    WHERE game_ids IS NULL OR ms.game_id = ANY(game_ids)
    GROUP BY ms.game_id, ms.team_id, ms.own_lineup, ms.opp_starters
  ), action_segment_spans AS (
    SELECT a.game_id, a.team_id, a.segment_id,
      greatest(max(a.event_elapsed_seconds) - min(a.event_elapsed_seconds), 0::numeric) AS seconds
    FROM euroleague.player_stats_actions_by_game a
    WHERE (game_ids IS NULL OR a.game_id = ANY(game_ids))
      AND a.segment_id IS NOT NULL
      AND a.event_elapsed_seconds IS NOT NULL
    GROUP BY a.game_id, a.team_id, a.segment_id
  ), action_spans AS (
    SELECT x.game_id, x.team_id, ms.own_lineup, ms.opp_starters,
      sum(x.seconds) AS seconds
    FROM action_segment_spans x
    JOIN euroleague.matchup_segments_actions ms
      USING (game_id, team_id, segment_id)
    GROUP BY x.game_id, x.team_id, ms.own_lineup, ms.opp_starters
  ), distinct_lineups AS (
    SELECT DISTINCT s.game_id, s.team_id, s.own_lineup FROM segment_totals s
  ), keyed AS (
    SELECT d.game_id, d.team_id, d.own_lineup, ids.player_ids,
      md5(array_to_string(ids.player_ids, '_')) AS lineup_key
    FROM distinct_lineups d
    CROSS JOIN LATERAL (
      SELECT ARRAY(
        SELECT rr.player_id
        FROM real_roster rr
        WHERE rr.game_id = d.game_id
          AND rr.team_id = d.team_id
          AND rr.source_player_name = ANY(d.own_lineup)
        ORDER BY rr.player_id
      ) AS player_ids
    ) ids
  ), counts AS (
    SELECT atc.game_id, atc.team_id, atc.own_lineup, atc.type_lineup,
      atc.opp_starters,
      sum(atc.possession_flag)::integer AS possessions,
      sum(atc.points)::integer AS points,
      sum(atc.fg2_made)::integer AS fg2_made,
      sum(atc.fg2_att)::integer AS fg2_att,
      sum(atc.fg3_made)::integer AS fg3_made,
      sum(atc.fg3_att)::integer AS fg3_att,
      sum(atc.ts_possessions)::integer AS ts_possessions,
      sum(atc.fgm)::integer AS fgm,
      sum(atc.fga)::integer AS fga,
      sum(atc.ft_attempts)::integer AS ft_attempts,
      sum(atc.orebounds)::integer AS orebounds,
      sum(atc.oreb_opportunities)::integer AS oreb_opportunities,
      sum(atc.turnovers)::integer AS turnovers,
      sum(atc.steals)::integer AS steals
    FROM euroleague.action_team_context_actions atc
    WHERE (game_ids IS NULL OR atc.game_id = ANY(game_ids))
      AND atc.type_lineup IS NOT NULL
    GROUP BY atc.game_id, atc.team_id, atc.own_lineup,
      atc.type_lineup, atc.opp_starters
  ), game_run AS (
    SELECT a.game_id, max(a.load_run_id) AS load_run_id
    FROM euroleague.actions a
    WHERE game_ids IS NULL OR a.game_id = ANY(game_ids)
    GROUP BY a.game_id
  )
  SELECT s.game_id, s.team_id, k.lineup_key, side.type_lineup,
    s.opp_starters, sch.competition, sch.season, s.own_starters,
    s.own_lineup, k.player_ids,
    coalesce(c.possessions, 0), coalesce(c.points, 0),
    coalesce(c.fg2_made, 0), coalesce(c.fg2_att, 0),
    coalesce(c.fg3_made, 0), coalesce(c.fg3_att, 0),
    coalesce(c.ts_possessions, 0), coalesce(c.fgm, 0),
    coalesce(c.fga, 0), coalesce(c.ft_attempts, 0),
    coalesce(c.orebounds, 0), coalesce(c.oreb_opportunities, 0),
    coalesce(c.turnovers, 0), coalesce(c.steals, 0),
    CASE WHEN side.type_lineup = 'offense' THEN s.seconds END,
    gr.load_run_id, 'units-v2',
    CASE WHEN side.type_lineup = 'offense' THEN coalesce(span.seconds, 0) END
  FROM segment_totals s
  JOIN keyed k USING (game_id, team_id, own_lineup)
  JOIN euroleague.schedule sch ON sch.game_id = s.game_id
  LEFT JOIN game_run gr ON gr.game_id = s.game_id
  CROSS JOIN (VALUES ('offense'), ('defense')) AS side(type_lineup)
  LEFT JOIN counts c
    ON c.game_id = s.game_id AND c.team_id = s.team_id
   AND c.own_lineup = s.own_lineup
   AND c.opp_starters = s.opp_starters
   AND c.type_lineup = side.type_lineup
  LEFT JOIN action_spans span
    ON span.game_id = s.game_id AND span.team_id = s.team_id
   AND span.own_lineup = s.own_lineup
   AND span.opp_starters = s.opp_starters;

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$function$;
