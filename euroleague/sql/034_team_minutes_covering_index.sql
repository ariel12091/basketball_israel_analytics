-- Cover the Israeli-style custom duration scan without reading the wide
-- player/lineup heap. Apply outside a transaction.
CREATE INDEX CONCURRENTLY IF NOT EXISTS euroleague_player_stats_actions_minutes_idx
ON euroleague.player_stats_actions_by_game(
 game_id,team_id,regulation_seconds_remaining,pre_abs_margin,pre_status,
 is_overtime,own_starters,opp_starters,segment_id,event_elapsed_seconds
);
ANALYZE euroleague.player_stats_actions_by_game;
