-- EuroLeague migration 060a: partial index on the team-credited rows of the
-- action fact, for the EUROLEAGUE SHADOW SCHEMA.
--
-- get_team_traditional_pergame (060) adds team rebounds and team turnovers,
-- which are the action rows with no player: about 3,000 of 157,000 per
-- season-competition. Without this index the lookup walks every row of each
-- team-game (3.2 s for E 2025, 185k buffers). Apply outside a transaction:
-- CONCURRENTLY keeps app reads and loads running.

CREATE INDEX CONCURRENTLY IF NOT EXISTS euroleague_player_stats_actions_team_credit_idx
ON euroleague.player_stats_actions_by_game (game_id, team_id)
INCLUDE (play_type, type_lineup, turnovers)
WHERE action_player_id IS NULL;
