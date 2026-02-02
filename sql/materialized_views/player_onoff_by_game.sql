-- basketball_test.player_onoff_by_game source
-- Pre-aggregated player on/off stats per game.
-- Joins lineups_lookup (player-lineup mapping) with mv_lineup_totals_by_day
-- (pre-aggregated lineup pts/poss per game) to avoid the expensive
-- lineups_lookup × df_pts_poss_lineups_longer_mv join at query time.
-- Used by onoff_compute() for ~24x speedup.

CREATE MATERIALIZED VIEW basketball_test.player_onoff_by_game
TABLESPACE pg_default
AS WITH base0 AS (
    SELECT DISTINCT
      ll.player_id,
      ll.team_id,
      ll.lineup_hash,
      COALESCE(ll.is_on_verdict, 0::numeric)::integer AS is_on_key
    FROM basketball_test.lineups_lookup ll
  )
SELECT
  b0.player_id,
  b0.team_id,
  m.game_id,
  m.game_year,
  b0.is_on_key,
  m.type_lineup,
  SUM(m.total_pts)  AS total_pts,
  SUM(m.total_poss) AS total_poss
FROM base0 b0
JOIN basketball_test.mv_lineup_totals_by_day m
  ON m.lineup_hash = b0.lineup_hash
GROUP BY b0.player_id, b0.team_id, m.game_id, m.game_year, b0.is_on_key, m.type_lineup
WITH DATA;

-- View indexes:
CREATE INDEX idx_pobg_game_team ON basketball_test.player_onoff_by_game USING btree (game_id, team_id);
CREATE INDEX idx_pobg_gy ON basketball_test.player_onoff_by_game USING btree (game_year);
CREATE UNIQUE INDEX idx_pobg_pk ON basketball_test.player_onoff_by_game USING btree (player_id, team_id, game_id, is_on_key, type_lineup);
