-- 069_team_facts_ft_made_before.sql -- EUROLEAGUE SHADOW SCHEMA
-- Live definitions replaced by migration 069 (dumped 2026-11-20), for rollback.

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
    SELECT atc.game_id, atc.team_id, atc.credit_own_lineup AS own_lineup, atc.type_lineup,
      atc.credit_opp_starters AS opp_starters,
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
    GROUP BY atc.game_id, atc.team_id, atc.credit_own_lineup,
      atc.type_lineup, atc.credit_opp_starters
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

CREATE OR REPLACE FUNCTION euroleague.clutch_team_game_facts(p_game_ids bigint[], p_max_margin integer, p_margin_status text, p_max_time_remaining integer, p_ot_margin_filter boolean)
 RETURNS TABLE(game_id bigint, team_id bigint, own_lineup text[], own_starters smallint, opp_starters smallint, type_lineup text, possessions bigint, points bigint, fg2_made bigint, fg2_att bigint, fg3_made bigint, fg3_att bigint, ts_possessions bigint, fgm bigint, fga bigint, ft_attempts bigint, orebounds bigint, oreb_opportunities bigint, turnovers bigint, steals bigint, seconds numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'euroleague', 'public'
 SET plan_cache_mode TO 'force_custom_plan'
AS $function$
WITH eligible AS (
 SELECT a.*
 FROM euroleague.player_stats_actions_by_game a
 WHERE a.game_id = ANY(coalesce(p_game_ids, ARRAY[]::bigint[]))
   AND (a.is_overtime OR p_max_time_remaining IS NULL
        OR a.regulation_seconds_remaining <= p_max_time_remaining)
   AND (a.is_overtime AND NOT coalesce(p_ot_margin_filter,false)
        OR (p_max_margin IS NULL OR a.pre_abs_margin <= p_max_margin)
        AND (coalesce(nullif(btrim(p_margin_status),''),'all')='all'
          OR (p_margin_status='leading' AND a.pre_status>0)
          OR (p_margin_status='trailing' AND a.pre_status<0)
          OR (p_margin_status='tied' AND a.pre_status=0)))
), event_counts AS (
 SELECT game_id,team_id,credit_own_lineup AS own_lineup,credit_own_starters AS own_starters,
  credit_opp_starters AS opp_starters,type_lineup,
  sum(possession_flag)::bigint possessions,sum(points)::bigint points,
  sum(fg2_made)::bigint fg2_made,sum(fg2_att)::bigint fg2_att,
  sum(fg3_made)::bigint fg3_made,sum(fg3_att)::bigint fg3_att,
  sum(ts_possessions)::bigint ts_possessions,sum(fgm)::bigint fgm,sum(fga)::bigint fga,
  sum(ft_attempts)::bigint ft_attempts,sum(orebounds)::bigint orebounds,
  sum(oreb_opportunities)::bigint oreb_opportunities,sum(turnovers)::bigint turnovers,
  sum(steals)::bigint steals
 FROM eligible WHERE type_lineup IS NOT NULL
 GROUP BY game_id,team_id,credit_own_lineup,credit_own_starters,credit_opp_starters,type_lineup
), duration AS (
 SELECT d.game_id,d.team_id,ms.own_lineup,ms.own_starters,ms.opp_starters,
  sum(d.seconds)::numeric seconds
 FROM euroleague.clutch_segment_durations(
   p_game_ids,p_max_margin,p_margin_status,p_max_time_remaining,p_ot_margin_filter) d
 JOIN euroleague.matchup_segments_actions ms USING(game_id,team_id,segment_id)
 GROUP BY d.game_id,d.team_id,ms.own_lineup,ms.own_starters,ms.opp_starters
), identities AS (
 SELECT DISTINCT game_id,team_id,own_lineup,own_starters,opp_starters FROM duration
)
SELECT game_id,team_id,own_lineup,own_starters,opp_starters,type_lineup,
 coalesce(e.possessions,0),coalesce(e.points,0),coalesce(e.fg2_made,0),coalesce(e.fg2_att,0),
 coalesce(e.fg3_made,0),coalesce(e.fg3_att,0),coalesce(e.ts_possessions,0),coalesce(e.fgm,0),
 coalesce(e.fga,0),coalesce(e.ft_attempts,0),coalesce(e.orebounds,0),coalesce(e.oreb_opportunities,0),
 coalesce(e.turnovers,0),coalesce(e.steals,0),
 CASE WHEN type_lineup = 'offense' THEN coalesce(d.seconds, 0) END
FROM (SELECT i.*, s.type_lineup
        FROM duration i CROSS JOIN (VALUES ('offense'::text),('defense'::text)) s(type_lineup)) d
FULL JOIN event_counts e USING(game_id,team_id,own_lineup,own_starters,opp_starters,type_lineup)
WHERE d.seconds > 0 OR e.game_id IS NOT NULL;
$function$;

CREATE OR REPLACE FUNCTION euroleague.select_team_game_facts(p_game_ids bigint[], p_max_margin integer, p_margin_status text, p_max_time_remaining integer, p_ot_margin_filter boolean)
 RETURNS TABLE(game_id bigint, team_id bigint, own_lineup text[], own_starters smallint, opp_starters smallint, type_lineup text, possessions bigint, points bigint, fg2_made bigint, fg2_att bigint, fg3_made bigint, fg3_att bigint, ts_possessions bigint, fgm bigint, fga bigint, ft_attempts bigint, orebounds bigint, oreb_opportunities bigint, turnovers bigint, steals bigint, seconds numeric)
 LANGUAGE plpgsql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'euroleague', 'public'
 SET plan_cache_mode TO 'force_custom_plan'
AS $function$
DECLARE
  v_margin_status text := coalesce(nullif(btrim(p_margin_status), ''), 'all');
BEGIN
  IF p_max_margin IS NULL
     AND v_margin_status = 'all'
     AND p_max_time_remaining IS NULL THEN
    RETURN QUERY
    SELECT
      l.game_id,
      l.team_id,
      l.own_lineup,
      l.own_starters,
      l.opp_starters,
      l.type_lineup,
      l.possessions::bigint,
      l.points::bigint,
      l.fg2_made::bigint,
      l.fg2_att::bigint,
      l.fg3_made::bigint,
      l.fg3_att::bigint,
      l.ts_possessions::bigint,
      l.fgm::bigint,
      l.fga::bigint,
      l.ft_attempts::bigint,
      l.orebounds::bigint,
      l.oreb_opportunities::bigint,
      l.turnovers::bigint,
      l.steals::bigint,
      l.seconds
    FROM euroleague.lineup_totals_by_game l
    WHERE l.game_id = ANY(coalesce(p_game_ids, ARRAY[]::bigint[]));

  ELSIF p_max_margin = 5
        AND v_margin_status = 'all'
        AND p_max_time_remaining = 300
        AND NOT coalesce(p_ot_margin_filter, false) THEN
    RETURN QUERY
    SELECT
      c.game_id,
      c.team_id,
      c.own_lineup,
      c.own_starters,
      c.opp_starters,
      c.type_lineup,
      c.possessions,
      c.points,
      c.fg2_made,
      c.fg2_att,
      c.fg3_made,
      c.fg3_att,
      c.ts_possessions,
      c.fgm,
      c.fga,
      c.ft_attempts,
      c.orebounds,
      c.oreb_opportunities,
      c.turnovers,
      c.steals,
      c.seconds
    FROM euroleague.default_clutch_lineup_totals_by_game c
    WHERE c.game_id = ANY(coalesce(p_game_ids, ARRAY[]::bigint[]));

  ELSE
    RETURN QUERY
    SELECT f.*
    FROM euroleague.clutch_team_game_facts(
      p_game_ids,
      p_max_margin,
      v_margin_status,
      p_max_time_remaining,
      p_ot_margin_filter
    ) f;
  END IF;
END;
$function$;

CREATE OR REPLACE FUNCTION euroleague.filtered_team_game_facts(p_competition text, p_game_year integer, p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date, p_team_ids_csv text DEFAULT NULL::text, p_phase_csv text DEFAULT NULL::text, p_opp_ids_csv text DEFAULT NULL::text, p_home_away text DEFAULT 'all'::text, p_outcome text DEFAULT 'all'::text, p_opp_rank_side text DEFAULT NULL::text, p_opp_rank_n integer DEFAULT NULL::integer, p_opp_rank_metric text DEFAULT NULL::text, p_max_margin integer DEFAULT NULL::integer, p_margin_status text DEFAULT NULL::text, p_max_time_remaining integer DEFAULT NULL::integer, p_ot_margin_filter boolean DEFAULT false, p_min_gn integer DEFAULT NULL::integer, p_max_gn integer DEFAULT NULL::integer, p_last_n_games integer DEFAULT NULL::integer, p_num_starters_off_min integer DEFAULT NULL::integer, p_num_starters_off_max integer DEFAULT NULL::integer, p_num_starters_def_min integer DEFAULT NULL::integer, p_num_starters_def_max integer DEFAULT NULL::integer)
 RETURNS TABLE(game_id bigint, team_id bigint, team_name text, has_won boolean, own_lineup text[], own_starters smallint, opp_starters smallint, type_lineup text, possessions bigint, points bigint, fg2_made bigint, fg2_att bigint, fg3_made bigint, fg3_att bigint, ts_possessions bigint, fgm bigint, fga bigint, ft_attempts bigint, orebounds bigint, oreb_opportunities bigint, turnovers bigint, steals bigint, seconds numeric)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'euroleague', 'public'
 SET plan_cache_mode TO 'force_custom_plan'
AS $function$
  WITH normalized AS (
    SELECT
      coalesce(nullif(btrim(p_competition), ''), 'E') AS competition,
      CASE WHEN nullif(btrim(p_team_ids_csv), '') IS NULL THEN NULL::bigint[]
           ELSE string_to_array(regexp_replace(p_team_ids_csv, '\s+', '', 'g'), ',')::bigint[] END AS team_ids,
      CASE WHEN nullif(btrim(p_phase_csv), '') IS NULL THEN NULL::text[]
           ELSE string_to_array(p_phase_csv, ',') END AS phases,
      CASE WHEN nullif(btrim(p_opp_ids_csv), '') IS NULL THEN NULL::bigint[]
           ELSE string_to_array(regexp_replace(p_opp_ids_csv, '\s+', '', 'g'), ',')::bigint[] END AS opp_ids,
      coalesce(nullif(btrim(p_home_away), ''), 'all') AS home_away,
      coalesce(nullif(btrim(p_outcome), ''), 'all') AS outcome,
      nullif(btrim(p_opp_rank_side), '') AS rank_side,
      coalesce(nullif(btrim(p_opp_rank_metric), ''), 'net') AS rank_metric
  ),
  schedule_ranked AS (
    SELECT fs.*,
           row_number() OVER (
             PARTITION BY fs.team_id ORDER BY fs.game_date DESC, fs.game_id DESC
           ) AS team_game_rank
    FROM euroleague.final_schedule_mv fs
    CROSS JOIN normalized n
    WHERE fs.competition = n.competition AND fs.game_year = p_game_year
  ),
  opponent_ranks AS (
    SELECT r.team_id, r.off_rank, r.def_rank, r.net_rank,
           count(*) OVER () AS team_count
    FROM euroleague.team_ppp_ratings_mv r
    CROSS JOIN normalized n
    WHERE r.competition = n.competition AND r.game_year = p_game_year
  ),
  games AS MATERIALIZED (
    SELECT sr.game_id, sr.team_id, sr.team_name, sr.has_won
    FROM schedule_ranked sr
    CROSS JOIN normalized n
    LEFT JOIN opponent_ranks r ON r.team_id = sr.opp_team_id
    WHERE (p_start_date IS NULL OR sr.game_date >= p_start_date)
      AND (p_end_date IS NULL OR sr.game_date <= p_end_date)
      AND (n.team_ids IS NULL OR sr.team_id = ANY(n.team_ids))
      AND (n.phases IS NULL OR sr.phase = ANY(n.phases))
      AND (n.opp_ids IS NULL OR sr.opp_team_id = ANY(n.opp_ids))
      AND (n.home_away = 'all'
           OR (n.home_away = 'home' AND sr.is_home)
           OR (n.home_away = 'away' AND NOT sr.is_home))
      AND (n.outcome = 'all'
           OR (n.outcome = 'win' AND sr.has_won)
           OR (n.outcome = 'loss' AND NOT sr.has_won))
      AND (n.rank_side IS NULL OR p_opp_rank_n IS NULL
           OR (n.rank_side = 'top' AND
               CASE n.rank_metric WHEN 'off' THEN r.off_rank
                                  WHEN 'def' THEN r.def_rank
                                  ELSE r.net_rank END <= p_opp_rank_n)
           OR (n.rank_side = 'bottom' AND
               CASE n.rank_metric WHEN 'off' THEN r.off_rank
                                  WHEN 'def' THEN r.def_rank
                                  ELSE r.net_rank END > r.team_count - p_opp_rank_n))
      AND (p_min_gn IS NULL OR sr.round_number >= p_min_gn)
      AND (p_max_gn IS NULL OR sr.round_number <= p_max_gn)
      AND (p_last_n_games IS NULL OR sr.team_game_rank <= p_last_n_games)
  ),
  facts AS MATERIALIZED (
    SELECT f.*
    FROM euroleague.select_team_game_facts(
      ARRAY(SELECT DISTINCT g.game_id FROM games g),
      p_max_margin,
      p_margin_status,
      p_max_time_remaining,
      p_ot_margin_filter
    ) f
  )
  SELECT
    g.game_id, g.team_id, g.team_name, g.has_won,
    f.own_lineup, f.own_starters, f.opp_starters, f.type_lineup,
    f.possessions, f.points, f.fg2_made, f.fg2_att,
    f.fg3_made, f.fg3_att, f.ts_possessions, f.fgm, f.fga,
    f.ft_attempts, f.orebounds, f.oreb_opportunities,
    f.turnovers, f.steals, f.seconds
  FROM games g
  JOIN facts f ON f.game_id = g.game_id AND f.team_id = g.team_id
  WHERE (p_num_starters_off_min IS NULL OR f.own_starters >= p_num_starters_off_min)
    AND (p_num_starters_off_max IS NULL OR f.own_starters <= p_num_starters_off_max)
    AND (p_num_starters_def_min IS NULL OR f.opp_starters >= p_num_starters_def_min)
    AND (p_num_starters_def_max IS NULL OR f.opp_starters <= p_num_starters_def_max)
$function$;

CREATE OR REPLACE FUNCTION euroleague.refresh_default_clutch_for_games(game_ids bigint[])
 RETURNS bigint
 LANGUAGE plpgsql
AS $function$
DECLARE
  target_game_ids bigint[];
  inserted_count bigint := 0;
BEGIN
  IF game_ids IS NULL OR array_length(game_ids, 1) IS NULL THEN
    SELECT array_agg(s.game_id ORDER BY s.game_id)
      INTO target_game_ids
      FROM euroleague.schedule s;
    DELETE FROM euroleague.default_clutch_lineup_totals_by_game;
  ELSE
    SELECT array_agg(DISTINCT x ORDER BY x)
      INTO target_game_ids
      FROM unnest(game_ids) x;
    DELETE FROM euroleague.default_clutch_lineup_totals_by_game c
     WHERE c.game_id = ANY(target_game_ids);
  END IF;

  IF target_game_ids IS NULL OR array_length(target_game_ids, 1) IS NULL THEN
    RETURN 0;
  END IF;

  INSERT INTO euroleague.default_clutch_lineup_totals_by_game (
    game_id, team_id, own_lineup, own_starters, opp_starters, type_lineup,
    possessions, points, fg2_made, fg2_att, fg3_made, fg3_att,
    ts_possessions, fgm, fga, ft_attempts, orebounds,
    oreb_opportunities, turnovers, steals, seconds,
    derivation_version
  )
  SELECT
    f.game_id, f.team_id, f.own_lineup, f.own_starters, f.opp_starters,
    f.type_lineup, f.possessions, f.points, f.fg2_made, f.fg2_att,
    f.fg3_made, f.fg3_att, f.ts_possessions, f.fgm, f.fga,
    f.ft_attempts, f.orebounds, f.oreb_opportunities, f.turnovers,
    f.steals, f.seconds, 'default-clutch-v1'
  FROM euroleague.clutch_team_game_facts(
    target_game_ids, 5, 'all', 300, false
  ) f;

  GET DIAGNOSTICS inserted_count = ROW_COUNT;
  RETURN inserted_count;
END;
$function$;

CREATE OR REPLACE FUNCTION euroleague.get_team_ratings_dynamic(p_competition text, p_game_year integer, p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date, p_team_ids_csv text DEFAULT NULL::text, p_phase_csv text DEFAULT NULL::text, p_opp_ids_csv text DEFAULT NULL::text, p_home_away text DEFAULT 'all'::text, p_outcome text DEFAULT 'all'::text, p_opp_rank_side text DEFAULT NULL::text, p_opp_rank_n integer DEFAULT NULL::integer, p_opp_rank_metric text DEFAULT NULL::text, p_max_margin integer DEFAULT NULL::integer, p_margin_status text DEFAULT NULL::text, p_max_time_remaining integer DEFAULT NULL::integer, p_ot_margin_filter boolean DEFAULT false, p_min_gn integer DEFAULT NULL::integer, p_max_gn integer DEFAULT NULL::integer, p_last_n_games integer DEFAULT NULL::integer, p_num_starters_off_min integer DEFAULT NULL::integer, p_num_starters_off_max integer DEFAULT NULL::integer, p_num_starters_def_min integer DEFAULT NULL::integer, p_num_starters_def_max integer DEFAULT NULL::integer)
 RETURNS TABLE(game_year integer, team_id bigint, team_name text, off_ppp numeric, def_ppp numeric, net_rtg numeric, games_played bigint, wins bigint, losses bigint, off_poss bigint, def_poss bigint, rank_net_rtg bigint, rank_off_ppp bigint, rank_def_ppp bigint, off_fg3_made bigint, off_fg3_att bigint, def_fg3_made bigint, def_fg3_att bigint, off_ftm bigint, off_fta bigint, def_ftm bigint, def_fta bigint)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'euroleague', 'public'
AS $function$
  WITH facts AS (
    SELECT * FROM euroleague.filtered_team_game_facts(
      p_competition, p_game_year, p_start_date, p_end_date,
      p_team_ids_csv, p_phase_csv, p_opp_ids_csv, p_home_away, p_outcome,
      p_opp_rank_side, p_opp_rank_n, p_opp_rank_metric,
      p_max_margin, p_margin_status, p_max_time_remaining, p_ot_margin_filter,
      p_min_gn, p_max_gn, p_last_n_games,
      p_num_starters_off_min, p_num_starters_off_max,
      p_num_starters_def_min, p_num_starters_def_max
    )
  ),
  agg AS (
    SELECT
      f.team_id, max(f.team_name) AS team_name,
      count(DISTINCT f.game_id) AS games_played,
      count(DISTINCT f.game_id) FILTER (WHERE f.has_won) AS wins,
      count(DISTINCT f.game_id) FILTER (WHERE NOT f.has_won) AS losses,
      sum(f.points) FILTER (WHERE f.type_lineup = 'offense') AS off_pts,
      sum(f.possessions) FILTER (WHERE f.type_lineup = 'offense') AS off_poss,
      sum(f.points) FILTER (WHERE f.type_lineup = 'defense') AS def_pts,
      sum(f.possessions) FILTER (WHERE f.type_lineup = 'defense') AS def_poss,
      sum(f.fg3_made) FILTER (WHERE f.type_lineup = 'offense') AS off_fg3_made,
      sum(f.fg3_att) FILTER (WHERE f.type_lineup = 'offense') AS off_fg3_att,
      sum(f.fg3_made) FILTER (WHERE f.type_lineup = 'defense') AS def_fg3_made,
      sum(f.fg3_att) FILTER (WHERE f.type_lineup = 'defense') AS def_fg3_att,
      sum(f.points - 2 * f.fg2_made - 3 * f.fg3_made) FILTER (WHERE f.type_lineup = 'offense') AS off_ftm,
      sum(f.ft_attempts) FILTER (WHERE f.type_lineup = 'offense') AS off_fta,
      sum(f.points - 2 * f.fg2_made - 3 * f.fg3_made) FILTER (WHERE f.type_lineup = 'defense') AS def_ftm,
      sum(f.ft_attempts) FILTER (WHERE f.type_lineup = 'defense') AS def_fta
    FROM facts f GROUP BY f.team_id
  ),
  rated AS (
    SELECT a.*,
      round(100.0 * a.off_pts / nullif(a.off_poss, 0), 1) AS off_ppp,
      round(100.0 * a.def_pts / nullif(a.def_poss, 0), 1) AS def_ppp
    FROM agg a
  )
  SELECT
    p_game_year, r.team_id, r.team_name,
    r.off_ppp, r.def_ppp,
    round(100.0 * r.off_pts / nullif(r.off_poss, 0)
        - 100.0 * r.def_pts / nullif(r.def_poss, 0), 1),
    r.games_played::bigint, r.wins::bigint, r.losses::bigint,
    r.off_poss::bigint, r.def_poss::bigint,
    dense_rank() OVER (ORDER BY r.off_ppp - r.def_ppp DESC),
    dense_rank() OVER (ORDER BY r.off_ppp DESC),
    dense_rank() OVER (ORDER BY r.def_ppp ASC),
    r.off_fg3_made::bigint, r.off_fg3_att::bigint, r.def_fg3_made::bigint, r.def_fg3_att::bigint,
    r.off_ftm::bigint, r.off_fta::bigint, r.def_ftm::bigint, r.def_fta::bigint
  FROM rated r
  ORDER BY r.off_ppp - r.def_ppp DESC NULLS LAST
$function$;

CREATE OR REPLACE FUNCTION euroleague.get_team_dashboard_dynamic(p_competition text, p_game_year integer, p_start_date date DEFAULT NULL::date, p_end_date date DEFAULT NULL::date, p_team_ids_csv text DEFAULT NULL::text, p_phase_csv text DEFAULT NULL::text, p_opp_ids_csv text DEFAULT NULL::text, p_home_away text DEFAULT 'all'::text, p_outcome text DEFAULT 'all'::text, p_opp_rank_side text DEFAULT NULL::text, p_opp_rank_n integer DEFAULT NULL::integer, p_opp_rank_metric text DEFAULT NULL::text, p_max_margin integer DEFAULT NULL::integer, p_margin_status text DEFAULT NULL::text, p_max_time_remaining integer DEFAULT NULL::integer, p_ot_margin_filter boolean DEFAULT false, p_min_gn integer DEFAULT NULL::integer, p_max_gn integer DEFAULT NULL::integer, p_last_n_games integer DEFAULT NULL::integer, p_num_starters_off_min integer DEFAULT NULL::integer, p_num_starters_off_max integer DEFAULT NULL::integer, p_num_starters_def_min integer DEFAULT NULL::integer, p_num_starters_def_max integer DEFAULT NULL::integer)
 RETURNS TABLE(game_year integer, team_id bigint, team_name text, off_ppp numeric, def_ppp numeric, net_rtg numeric, games_played bigint, wins bigint, losses bigint, off_poss bigint, def_poss bigint, rank_net_rtg bigint, rank_off_ppp bigint, rank_def_ppp bigint, off_efg numeric, def_efg numeric, off_ts numeric, def_ts numeric, off_oreb numeric, def_oreb numeric, off_tov numeric, def_tov numeric, off_ftr numeric, def_ftr numeric, minutes numeric, off_fg3_made bigint, off_fg3_att bigint, def_fg3_made bigint, def_fg3_att bigint, off_ftm bigint, off_fta bigint, def_ftm bigint, def_fta bigint)
 LANGUAGE sql
 STABLE SECURITY DEFINER
 SET search_path TO 'pg_catalog', 'euroleague', 'public'
 SET plan_cache_mode TO 'force_custom_plan'
AS $function$
WITH facts AS MATERIALIZED (
  SELECT * FROM euroleague.filtered_team_game_facts(
    p_competition,p_game_year,p_start_date,p_end_date,
    p_team_ids_csv,p_phase_csv,p_opp_ids_csv,p_home_away,p_outcome,
    p_opp_rank_side,p_opp_rank_n,p_opp_rank_metric,
    p_max_margin,p_margin_status,p_max_time_remaining,p_ot_margin_filter,
    p_min_gn,p_max_gn,p_last_n_games,
    p_num_starters_off_min,p_num_starters_off_max,
    p_num_starters_def_min,p_num_starters_def_max
  )
), agg AS (
  SELECT f.team_id,max(f.team_name) team_name,
    count(DISTINCT f.game_id) games_played,
    count(DISTINCT f.game_id) FILTER(WHERE f.has_won) wins,
    count(DISTINCT f.game_id) FILTER(WHERE NOT f.has_won) losses,
    sum(f.points) FILTER(WHERE f.type_lineup='offense') off_pts,
    sum(f.possessions) FILTER(WHERE f.type_lineup='offense') off_poss,
    sum(f.ts_possessions) FILTER(WHERE f.type_lineup='offense') off_ts_poss,
    sum(f.orebounds) FILTER(WHERE f.type_lineup='offense') off_oreb,
    sum(f.oreb_opportunities) FILTER(WHERE f.type_lineup='offense') off_oreb_opp,
    sum(f.turnovers) FILTER(WHERE f.type_lineup='offense') off_tov,
    sum(f.ft_attempts) FILTER(WHERE f.type_lineup='offense') off_fta,
    sum(f.fga) FILTER(WHERE f.type_lineup='offense') off_fga,
    sum(f.fgm) FILTER(WHERE f.type_lineup='offense') off_fgm,
    sum(f.fg3_made) FILTER(WHERE f.type_lineup='offense') off_fg3m,
    sum(f.points) FILTER(WHERE f.type_lineup='defense') def_pts,
    sum(f.possessions) FILTER(WHERE f.type_lineup='defense') def_poss,
    sum(f.ts_possessions) FILTER(WHERE f.type_lineup='defense') def_ts_poss,
    sum(f.orebounds) FILTER(WHERE f.type_lineup='defense') def_oreb,
    sum(f.oreb_opportunities) FILTER(WHERE f.type_lineup='defense') def_oreb_opp,
    sum(f.turnovers) FILTER(WHERE f.type_lineup='defense') def_tov,
    sum(f.ft_attempts) FILTER(WHERE f.type_lineup='defense') def_fta,
    sum(f.fga) FILTER(WHERE f.type_lineup='defense') def_fga,
    sum(f.fgm) FILTER(WHERE f.type_lineup='defense') def_fgm,
    sum(f.fg3_made) FILTER(WHERE f.type_lineup='defense') def_fg3m,
    sum(f.seconds) FILTER(WHERE f.type_lineup='offense') seconds,
    sum(f.fg3_made) FILTER (WHERE f.type_lineup = 'offense') AS off_fg3_made,
    sum(f.fg3_att) FILTER (WHERE f.type_lineup = 'offense') AS off_fg3_att,
    sum(f.fg3_made) FILTER (WHERE f.type_lineup = 'defense') AS def_fg3_made,
    sum(f.fg3_att) FILTER (WHERE f.type_lineup = 'defense') AS def_fg3_att,
    sum(f.points - 2 * f.fg2_made - 3 * f.fg3_made) FILTER (WHERE f.type_lineup = 'offense') AS off_ftm,
    sum(f.points - 2 * f.fg2_made - 3 * f.fg3_made) FILTER (WHERE f.type_lineup = 'defense') AS def_ftm
    -- off_fta / def_fta: already summed above (ft_attempts)
  FROM facts f GROUP BY f.team_id
), rated AS (
  SELECT a.*,round(100.0*a.off_pts/nullif(a.off_poss,0),1) off_rating,
    round(100.0*a.def_pts/nullif(a.def_poss,0),1) def_rating FROM agg a
)
SELECT p_game_year,r.team_id,r.team_name,r.off_rating,r.def_rating,
  round(100.0*r.off_pts/nullif(r.off_poss,0)-100.0*r.def_pts/nullif(r.def_poss,0),1),
  r.games_played::bigint,r.wins::bigint,r.losses::bigint,
  r.off_poss::bigint,r.def_poss::bigint,
  dense_rank() OVER(ORDER BY r.off_rating-r.def_rating DESC),
  dense_rank() OVER(ORDER BY r.off_rating DESC),dense_rank() OVER(ORDER BY r.def_rating ASC),
  round(100.0*(r.off_fgm+0.5*r.off_fg3m)/nullif(r.off_fga,0),1),
  round(100.0*(r.def_fgm+0.5*r.def_fg3m)/nullif(r.def_fga,0),1),
  round(100.0*r.off_pts/nullif(2*r.off_ts_poss,0),1),
  round(100.0*r.def_pts/nullif(2*r.def_ts_poss,0),1),
  round(100.0*r.off_oreb/nullif(r.off_oreb_opp,0),1),
  round(100.0*r.def_oreb/nullif(r.def_oreb_opp,0),1),
  round(100.0*r.off_tov/nullif(r.off_poss,0),1),
  round(100.0*r.def_tov/nullif(r.def_poss,0),1),
  round(100.0*r.off_fta/nullif(r.off_fga,0),1),
  round(100.0*r.def_fta/nullif(r.def_fga,0),1),round(r.seconds/60.0,3)::numeric,
  r.off_fg3_made::bigint,r.off_fg3_att::bigint,r.def_fg3_made::bigint,r.def_fg3_att::bigint,
  r.off_ftm::bigint,r.off_fta::bigint,r.def_ftm::bigint,r.def_fta::bigint
FROM rated r ORDER BY r.off_rating-r.def_rating DESC NULLS LAST
$function$;
