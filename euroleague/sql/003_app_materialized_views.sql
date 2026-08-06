-- EUROLEAGUE APP-FACING MATERIALIZED VIEWS
-- Prepared on 2026-08-06; apply only to the isolated euroleague schema.
--
-- The ordinary views created by migration 002 remain the always-current,
-- inspectable semantic layer. These materialized views are the bounded,
-- indexed read layer intended for eventual application queries.

BEGIN;

CREATE MATERIALIZED VIEW IF NOT EXISTS euroleague.final_schedule_mv AS
SELECT *
FROM euroleague.final_schedule
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS euroleague_final_schedule_mv_pk
  ON euroleague.final_schedule_mv (game_id, team_id);

CREATE INDEX IF NOT EXISTS euroleague_final_schedule_mv_filter_idx
  ON euroleague.final_schedule_mv (game_year, team_id, game_date, gn);

CREATE MATERIALIZED VIEW IF NOT EXISTS euroleague.player_onoff_by_season_mv AS
SELECT *
FROM euroleague.player_onoff_by_season
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS euroleague_player_onoff_season_mv_pk
  ON euroleague.player_onoff_by_season_mv (game_year, team_id, player_id);

CREATE INDEX IF NOT EXISTS euroleague_player_onoff_season_mv_filter_idx
  ON euroleague.player_onoff_by_season_mv (game_year, team_id, minutes_on);

CREATE MATERIALIZED VIEW IF NOT EXISTS euroleague.player_four_factors_by_season_mv AS
SELECT *
FROM euroleague.player_four_factors_by_season
WITH NO DATA;

CREATE UNIQUE INDEX IF NOT EXISTS euroleague_player_four_factors_season_mv_pk
  ON euroleague.player_four_factors_by_season_mv
  (game_year, team_id, player_id);

CREATE INDEX IF NOT EXISTS euroleague_player_four_factors_season_mv_filter_idx
  ON euroleague.player_four_factors_by_season_mv (game_year, team_id);

CREATE OR REPLACE FUNCTION euroleague.refresh_app_materialized_views()
RETURNS void
LANGUAGE plpgsql
AS $function$
BEGIN
  REFRESH MATERIALIZED VIEW euroleague.final_schedule_mv;
  REFRESH MATERIALIZED VIEW euroleague.player_onoff_by_season_mv;
  REFRESH MATERIALIZED VIEW euroleague.player_four_factors_by_season_mv;
END;
$function$;

SELECT euroleague.refresh_app_materialized_views();

COMMIT;
