-- Indexes to accelerate Tab 5 (Traditional Stats) filters and on-floor joins.
-- Safe to run multiple times.

-- 1) Fast lookup of actions by filtered game/team and lineup.
CREATE INDEX IF NOT EXISTS idx_df_longer_game_team_lineup
  ON basketball_test.df_pts_poss_lineups_longer_mv (game_id, team_id, lineup_hash);

-- 2) Narrow partial index for on-floor rows only (the query predicate used by Tab 5).
CREATE INDEX IF NOT EXISTS idx_lineups_lookup_ts_onfloor_partial
  ON basketball_test.lineups_lookup (game_year, game_id, team_id, lineup_hash, player_id)
  WHERE COALESCE(is_on_verdict, 0)::int = 1;

