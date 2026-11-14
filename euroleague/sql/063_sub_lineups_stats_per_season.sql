-- 063_sub_lineups_stats_per_season.sql
--
-- EuroLeague shadow schema. Same rows, rebuilt one competition-season at a time.
--
-- sub_lineups_stats_mv (the 2-5 player Lineups table) was a materialized view
-- that refresh_app_materialized_views() rebuilt for every competition and
-- season after every load: a 4.2 million-row join sorted in 3.5 MB of
-- work_mem, ~85 s and ~2.8 GB of temp files per load, two loads a night. 73%
-- of those rows were the finished 2025 seasons, which never change (measured
-- 2026-11-14 from pg_stat_statements; docs/verification/supabase_disk_io_2026-11-13.md).
--
-- It becomes a table of the same name and columns, so Tab 10, the web
-- Lineups route and the security lists read it unchanged. The loader calls
-- refresh_sub_lineups_stats(game_ids) for the competition-seasons of the games
-- it published; NULL rebuilds every season. The query is the live view
-- definition (pg_get_viewdef, 2026-11-14) with only the season filter added;
-- scripts/apply_063_sub_lineups_stats_per_season.py proves the function
-- returns the view's rows exactly before the view is dropped.
--
-- A table named _mv: the name is what every reader uses; renaming it would
-- touch them all for no change in meaning (the Israeli schema has the same
-- convention, e.g. player_advanced_stats_mv).

BEGIN;

-- 1. The table, with the view's exact column types, filled from the view.
CREATE TABLE euroleague.sub_lineups_stats_mv__new
  AS SELECT * FROM euroleague.sub_lineups_stats_mv WITH NO DATA;
INSERT INTO euroleague.sub_lineups_stats_mv__new
  SELECT * FROM euroleague.sub_lineups_stats_mv;

-- 2. Swap names. Nothing in the database depends on the view (checked in the
-- catalog 2026-11-14; only refresh_app_materialized_views() names it, below).
DROP MATERIALIZED VIEW euroleague.sub_lineups_stats_mv;
ALTER TABLE euroleague.sub_lineups_stats_mv__new RENAME TO sub_lineups_stats_mv;

CREATE UNIQUE INDEX euroleague_sub_lineups_stats_mv_pk
  ON euroleague.sub_lineups_stats_mv (competition, game_year, team_id, unit_key);
CREATE INDEX euroleague_sub_lineups_stats_mv_size_idx
  ON euroleague.sub_lineups_stats_mv (competition, game_year, unit_size, team_id);

-- 3. Access in the same transaction, as sql/security/enable_readonly_rls.sql
-- sets it for a curated EuroLeague table: the app never loses the relation.
GRANT SELECT ON euroleague.sub_lineups_stats_mv TO app_readonly;
ALTER TABLE euroleague.sub_lineups_stats_mv ENABLE ROW LEVEL SECURITY;
CREATE POLICY app_readonly_select_all ON euroleague.sub_lineups_stats_mv
  FOR SELECT TO app_readonly USING (true);

-- 4. The refresh: delete and rebuild the competition-seasons of p_game_ids.
-- work_mem only for this function: the current season's join (~0.6 million
-- rows) then sorts in memory instead of spilling to disk.
CREATE OR REPLACE FUNCTION euroleague.refresh_sub_lineups_stats(p_game_ids bigint[] DEFAULT NULL)
RETURNS bigint
LANGUAGE plpgsql
SET work_mem = '64MB'
AS $function$
DECLARE
  v_rows bigint;
BEGIN
  -- Scope: the competition-seasons of p_game_ids (schedule.season is the
  -- provider season, the game_year of every EuroLeague read-layer table).
  DELETE FROM euroleague.sub_lineups_stats_mv t
  WHERE (t.competition, t.game_year) IN (
    SELECT s.competition, s.season FROM euroleague.schedule s
    WHERE p_game_ids IS NULL OR s.game_id = ANY (p_game_ids));

  INSERT INTO euroleague.sub_lineups_stats_mv
  WITH game_starters AS NOT MATERIALIZED (
         SELECT fr.game_id,
            fr.team_id,
            array_agg(DISTINCT fr.player_id) AS starter_ids
           FROM euroleague.full_rosters fr
          WHERE fr.is_starter
          GROUP BY fr.game_id, fr.team_id
        ), unit_totals AS (
         SELECT sl.competition,
            sl.game_year,
            sl.team_id,
            sl.unit_key,
            sl.unit_size,
            sl.player_ids,
            sum(l.possessions) FILTER (WHERE l.type_lineup = 'offense'::text) AS off_poss,
            sum(l.points) FILTER (WHERE l.type_lineup = 'offense'::text) AS off_pts,
            sum(l.fg2_made) FILTER (WHERE l.type_lineup = 'offense'::text) AS off_fg2_made,
            sum(l.fg2_att) FILTER (WHERE l.type_lineup = 'offense'::text) AS off_fg2_att,
            sum(l.fg3_made) FILTER (WHERE l.type_lineup = 'offense'::text) AS off_fg3_made,
            sum(l.fg3_att) FILTER (WHERE l.type_lineup = 'offense'::text) AS off_fg3_att,
            sum(l.ts_possessions) FILTER (WHERE l.type_lineup = 'offense'::text) AS off_ts_poss,
            sum(l.fgm) FILTER (WHERE l.type_lineup = 'offense'::text) AS off_fgm,
            sum(l.fga) FILTER (WHERE l.type_lineup = 'offense'::text) AS off_fga,
            sum(l.ft_attempts) FILTER (WHERE l.type_lineup = 'offense'::text) AS off_fta,
            sum(l.orebounds) FILTER (WHERE l.type_lineup = 'offense'::text) AS off_oreb,
            sum(l.oreb_opportunities) FILTER (WHERE l.type_lineup = 'offense'::text) AS off_oreb_opp,
            sum(l.turnovers) FILTER (WHERE l.type_lineup = 'offense'::text) AS off_tov,
            sum(l.steals) FILTER (WHERE l.type_lineup = 'offense'::text) AS off_steals,
            sum(l.possessions) FILTER (WHERE l.type_lineup = 'defense'::text) AS def_poss,
            sum(l.points) FILTER (WHERE l.type_lineup = 'defense'::text) AS def_pts,
            sum(l.fg2_made) FILTER (WHERE l.type_lineup = 'defense'::text) AS def_fg2_made,
            sum(l.fg2_att) FILTER (WHERE l.type_lineup = 'defense'::text) AS def_fg2_att,
            sum(l.fg3_made) FILTER (WHERE l.type_lineup = 'defense'::text) AS def_fg3_made,
            sum(l.fg3_att) FILTER (WHERE l.type_lineup = 'defense'::text) AS def_fg3_att,
            sum(l.ts_possessions) FILTER (WHERE l.type_lineup = 'defense'::text) AS def_ts_poss,
            sum(l.fgm) FILTER (WHERE l.type_lineup = 'defense'::text) AS def_fgm,
            sum(l.fga) FILTER (WHERE l.type_lineup = 'defense'::text) AS def_fga,
            sum(l.ft_attempts) FILTER (WHERE l.type_lineup = 'defense'::text) AS def_fta,
            sum(l.orebounds) FILTER (WHERE l.type_lineup = 'defense'::text) AS def_oreb,
            sum(l.oreb_opportunities) FILTER (WHERE l.type_lineup = 'defense'::text) AS def_oreb_opp,
            sum(l.turnovers) FILTER (WHERE l.type_lineup = 'defense'::text) AS def_tov,
            sum(l.steals) FILTER (WHERE l.type_lineup = 'defense'::text) AS def_steals,
            sum(l.seconds) FILTER (WHERE l.type_lineup = 'offense'::text) AS seconds,
            sum(l.possessions * (( SELECT count(*) AS count
                   FROM unnest(sl.player_ids) us(pid)
                  WHERE us.pid = ANY (gs.starter_ids)))) AS starters_poss_num
           FROM euroleague.sub_lineups sl
             JOIN ( SELECT DISTINCT s.competition, s.season AS game_year
                   FROM euroleague.schedule s
                  WHERE p_game_ids IS NULL OR s.game_id = ANY (p_game_ids)) sc
               ON sc.competition = sl.competition AND sc.game_year = sl.game_year
             JOIN euroleague.lineup_totals_by_game l ON l.competition = sl.competition AND l.game_year = sl.game_year AND l.team_id = sl.team_id AND l.lineup_key = sl.lineup_key
             LEFT JOIN game_starters gs ON gs.game_id = l.game_id AND gs.team_id = l.team_id
          GROUP BY sl.competition, sl.game_year, sl.team_id, sl.unit_key, sl.unit_size, sl.player_ids
        )
 SELECT ut.competition,
    ut.game_year,
    ut.team_id,
    ut.unit_key,
    ut.unit_size,
    ut.player_ids,
    names.player_names_str,
    ut.off_poss,
    ut.off_pts,
    ut.off_fg2_made,
    ut.off_fg2_att,
    ut.off_fg3_made,
    ut.off_fg3_att,
    ut.off_ts_poss,
    ut.off_fgm,
    ut.off_fga,
    ut.off_fta,
    ut.off_oreb,
    ut.off_oreb_opp,
    ut.off_tov,
    ut.off_steals,
    ut.def_poss,
    ut.def_pts,
    ut.def_fg2_made,
    ut.def_fg2_att,
    ut.def_fg3_made,
    ut.def_fg3_att,
    ut.def_ts_poss,
    ut.def_fgm,
    ut.def_fga,
    ut.def_fta,
    ut.def_oreb,
    ut.def_oreb_opp,
    ut.def_tov,
    ut.def_steals,
    round(COALESCE(ut.seconds, 0::numeric) / 60.0, 1) AS minutes,
    ut.starters_poss_num
   FROM unit_totals ut
     CROSS JOIN LATERAL ( SELECT string_agg(COALESCE(euroleague.person_display_name(p.display_name), '#'::text || u.pid::text), ', '::text ORDER BY u.ord) AS player_names_str
           FROM unnest(ut.player_ids) WITH ORDINALITY u(pid, ord)
             LEFT JOIN euroleague.players p ON p.player_id = u.pid) names;

  GET DIAGNOSTICS v_rows = ROW_COUNT;
  RETURN v_rows;
END;
$function$;

-- 5. The whole-schema refresh no longer rebuilds it.
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
  REFRESH MATERIALIZED VIEW euroleague.player_traditional_stats_mv;
END;
$function$;

COMMIT;
