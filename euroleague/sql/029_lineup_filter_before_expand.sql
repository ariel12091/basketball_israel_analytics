-- EuroLeague migration 029: filter five-player lineup identities before
-- expanding them to 2-5 player units. This follows the Israeli query order
-- and avoids resolving every lineup in the season for a filtered request.

BEGIN;
SET LOCAL search_path TO euroleague, public;

CREATE OR REPLACE FUNCTION euroleague.fetch_lineups_dynamic(
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
    p_last_n_games INTEGER DEFAULT NULL,
    p_num_starters_off_min INTEGER DEFAULT NULL,
    p_num_starters_off_max INTEGER DEFAULT NULL,
    p_num_starters_def_min INTEGER DEFAULT NULL,
    p_num_starters_def_max INTEGER DEFAULT NULL,
    p_unit_size INTEGER DEFAULT 5,
    p_players_on_csv TEXT DEFAULT NULL,
    p_players_off_csv TEXT DEFAULT NULL,
    p_min_poss INTEGER DEFAULT 0
)
RETURNS TABLE (
    team_id BIGINT, unit_key TEXT, unit_size SMALLINT,
    player_ids BIGINT[], player_names_str TEXT,
    off_poss BIGINT, off_pts BIGINT, off_fg2_made BIGINT, off_fg2_att BIGINT,
    off_fg3_made BIGINT, off_fg3_att BIGINT, off_ts_poss BIGINT,
    off_fgm BIGINT, off_fga BIGINT, off_fta BIGINT,
    off_oreb BIGINT, off_oreb_opp BIGINT, off_tov BIGINT, off_steals BIGINT,
    def_poss BIGINT, def_pts BIGINT, def_fg2_made BIGINT, def_fg2_att BIGINT,
    def_fg3_made BIGINT, def_fg3_att BIGINT, def_ts_poss BIGINT,
    def_fgm BIGINT, def_fga BIGINT, def_fta BIGINT,
    def_oreb BIGINT, def_oreb_opp BIGINT, def_tov BIGINT, def_steals BIGINT,
    minutes NUMERIC
)
LANGUAGE sql STABLE SECURITY DEFINER
SET search_path = pg_catalog, euroleague, public
SET plan_cache_mode = force_custom_plan
AS $function$
  WITH normalized AS (
    SELECT
      CASE WHEN nullif(btrim(p_players_on_csv), '') IS NULL THEN NULL::bigint[]
           ELSE string_to_array(regexp_replace(p_players_on_csv, '\s+', '', 'g'), ',')::bigint[] END AS players_on,
      CASE WHEN nullif(btrim(p_players_off_csv), '') IS NULL THEN NULL::bigint[]
           ELSE string_to_array(regexp_replace(p_players_off_csv, '\s+', '', 'g'), ',')::bigint[] END AS players_off
  ),
  facts AS MATERIALIZED (
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
  lineup_identity AS MATERIALIZED (
    SELECT DISTINCT
      l.game_id, l.team_id, l.own_lineup, l.lineup_key, l.player_ids
    FROM facts f
    JOIN euroleague.lineup_totals_by_game l
      ON l.game_id = f.game_id
     AND l.team_id = f.team_id
     AND l.own_lineup = f.own_lineup
    WHERE l.competition = coalesce(nullif(btrim(p_competition), ''), 'E')
      AND l.game_year = p_game_year
  ),
  unit_rows AS (
    SELECT
      sl.team_id, sl.unit_key, sl.unit_size, sl.player_ids,
      f.type_lineup, f.possessions, f.points,
      f.fg2_made, f.fg2_att, f.fg3_made, f.fg3_att,
      f.ts_possessions, f.fgm, f.fga, f.ft_attempts,
      f.orebounds, f.oreb_opportunities, f.turnovers, f.steals, f.seconds
    FROM facts f
    JOIN lineup_identity li
      ON li.game_id = f.game_id AND li.team_id = f.team_id
     AND li.own_lineup = f.own_lineup
    JOIN euroleague.sub_lineups sl
      ON sl.competition = coalesce(nullif(btrim(p_competition), ''), 'E')
     AND sl.game_year = p_game_year AND sl.team_id = li.team_id
     AND sl.lineup_key = li.lineup_key
    CROSS JOIN normalized n
    WHERE sl.unit_size = p_unit_size::smallint
      AND (n.players_on IS NULL OR sl.player_ids @> n.players_on)
      AND (n.players_off IS NULL OR NOT (sl.player_ids && n.players_off))
  ),
  agg AS (
    SELECT
      u.team_id, u.unit_key, u.unit_size, u.player_ids,
      sum(u.possessions) FILTER (WHERE u.type_lineup = 'offense') AS off_poss,
      sum(u.points) FILTER (WHERE u.type_lineup = 'offense') AS off_pts,
      sum(u.fg2_made) FILTER (WHERE u.type_lineup = 'offense') AS off_fg2_made,
      sum(u.fg2_att) FILTER (WHERE u.type_lineup = 'offense') AS off_fg2_att,
      sum(u.fg3_made) FILTER (WHERE u.type_lineup = 'offense') AS off_fg3_made,
      sum(u.fg3_att) FILTER (WHERE u.type_lineup = 'offense') AS off_fg3_att,
      sum(u.ts_possessions) FILTER (WHERE u.type_lineup = 'offense') AS off_ts_poss,
      sum(u.fgm) FILTER (WHERE u.type_lineup = 'offense') AS off_fgm,
      sum(u.fga) FILTER (WHERE u.type_lineup = 'offense') AS off_fga,
      sum(u.ft_attempts) FILTER (WHERE u.type_lineup = 'offense') AS off_fta,
      sum(u.orebounds) FILTER (WHERE u.type_lineup = 'offense') AS off_oreb,
      sum(u.oreb_opportunities) FILTER (WHERE u.type_lineup = 'offense') AS off_oreb_opp,
      sum(u.turnovers) FILTER (WHERE u.type_lineup = 'offense') AS off_tov,
      sum(u.steals) FILTER (WHERE u.type_lineup = 'offense') AS off_steals,
      sum(u.possessions) FILTER (WHERE u.type_lineup = 'defense') AS def_poss,
      sum(u.points) FILTER (WHERE u.type_lineup = 'defense') AS def_pts,
      sum(u.fg2_made) FILTER (WHERE u.type_lineup = 'defense') AS def_fg2_made,
      sum(u.fg2_att) FILTER (WHERE u.type_lineup = 'defense') AS def_fg2_att,
      sum(u.fg3_made) FILTER (WHERE u.type_lineup = 'defense') AS def_fg3_made,
      sum(u.fg3_att) FILTER (WHERE u.type_lineup = 'defense') AS def_fg3_att,
      sum(u.ts_possessions) FILTER (WHERE u.type_lineup = 'defense') AS def_ts_poss,
      sum(u.fgm) FILTER (WHERE u.type_lineup = 'defense') AS def_fgm,
      sum(u.fga) FILTER (WHERE u.type_lineup = 'defense') AS def_fga,
      sum(u.ft_attempts) FILTER (WHERE u.type_lineup = 'defense') AS def_fta,
      sum(u.orebounds) FILTER (WHERE u.type_lineup = 'defense') AS def_oreb,
      sum(u.oreb_opportunities) FILTER (WHERE u.type_lineup = 'defense') AS def_oreb_opp,
      sum(u.turnovers) FILTER (WHERE u.type_lineup = 'defense') AS def_tov,
      sum(u.steals) FILTER (WHERE u.type_lineup = 'defense') AS def_steals,
      sum(u.seconds) FILTER (WHERE u.type_lineup = 'offense') AS seconds
    FROM unit_rows u
    GROUP BY u.team_id, u.unit_key, u.unit_size, u.player_ids
  )
  SELECT
    a.team_id, a.unit_key, a.unit_size, a.player_ids,
    names.player_names_str,
    a.off_poss::bigint, a.off_pts::bigint,
    a.off_fg2_made::bigint, a.off_fg2_att::bigint,
    a.off_fg3_made::bigint, a.off_fg3_att::bigint,
    a.off_ts_poss::bigint, a.off_fgm::bigint, a.off_fga::bigint,
    a.off_fta::bigint, a.off_oreb::bigint, a.off_oreb_opp::bigint,
    a.off_tov::bigint, a.off_steals::bigint,
    a.def_poss::bigint, a.def_pts::bigint,
    a.def_fg2_made::bigint, a.def_fg2_att::bigint,
    a.def_fg3_made::bigint, a.def_fg3_att::bigint,
    a.def_ts_poss::bigint, a.def_fgm::bigint, a.def_fga::bigint,
    a.def_fta::bigint, a.def_oreb::bigint, a.def_oreb_opp::bigint,
    a.def_tov::bigint, a.def_steals::bigint,
    round(coalesce(a.seconds, 0) / 60.0, 1)
  FROM agg a
  CROSS JOIN LATERAL (
    SELECT string_agg(
      coalesce(euroleague.person_display_name(p.display_name), '#' || x.pid::text),
      ', ' ORDER BY x.ord
    ) AS player_names_str
    FROM unnest(a.player_ids) WITH ORDINALITY x(pid, ord)
    LEFT JOIN euroleague.players p ON p.player_id = x.pid
  ) names
  WHERE coalesce(a.off_poss, 0) + coalesce(a.def_poss, 0)
        >= coalesce(p_min_poss, 0)
$function$;

REVOKE ALL ON FUNCTION euroleague.fetch_lineups_dynamic(
  text, integer, date, date, text, text, text, text, text, text, integer,
  text, integer, text, integer, boolean, integer, integer, integer,
  integer, integer, integer, integer, integer, text, text, integer
) FROM PUBLIC;
GRANT EXECUTE ON FUNCTION euroleague.fetch_lineups_dynamic(
  text, integer, date, date, text, text, text, text, text, text, integer,
  text, integer, text, integer, boolean, integer, integer, integer,
  integer, integer, integer, integer, integer, text, text, integer
) TO app_readonly;

COMMIT;
