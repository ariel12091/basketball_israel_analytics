-- Cover the direct Lineups action scan without reading the wide heap. A normal
-- build is intentional: it remains compatible with app SELECTs and avoids
-- CONCURRENTLY starvation from the continuously active read pool. Do not run
-- it during a EuroLeague publication.
CREATE INDEX IF NOT EXISTS euroleague_player_stats_actions_lineups_idx
ON euroleague.player_stats_actions_by_game(
 game_id,team_id,regulation_seconds_remaining,pre_abs_margin,pre_status,
 is_overtime,own_starters,opp_starters
)
INCLUDE(own_lineup,segment_id,event_elapsed_seconds,type_lineup,possession_flag,
 points,fg2_made,fg2_att,fg3_made,fg3_att,ts_possessions,fgm,fga,ft_attempts,
 orebounds,oreb_opportunities,turnovers,steals);
ANALYZE euroleague.player_stats_actions_by_game;
