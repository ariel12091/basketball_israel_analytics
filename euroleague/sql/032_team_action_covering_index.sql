-- EuroLeague migration 032: covering access path for the Israeli-shaped
-- direct Team readers. The source fact is 326 MB because it also retains
-- player and lineup fields; all heap pages are all-visible, so this index lets
-- Team Ratings and Four Factors use a materially narrower index-only scan.
--
-- Apply outside a transaction because CONCURRENTLY protects live app reads.

CREATE INDEX CONCURRENTLY IF NOT EXISTS euroleague_player_stats_actions_team_idx
ON euroleague.player_stats_actions_by_game (
  game_id,
  team_id,
  regulation_seconds_remaining,
  pre_abs_margin,
  pre_status,
  is_overtime,
  own_starters,
  opp_starters
)
INCLUDE (
  type_lineup,
  possession_flag,
  points,
  ts_possessions,
  orebounds,
  oreb_opportunities,
  turnovers,
  ft_attempts,
  fga,
  fgm,
  fg3_made
);

ANALYZE euroleague.player_stats_actions_by_game;
