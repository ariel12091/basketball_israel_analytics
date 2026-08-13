-- EuroLeague migration 027: narrow physical action fact for interactive
-- custom Player Stats filters. Grain is unchanged: one action/team perspective.

BEGIN;
SET LOCAL search_path TO euroleague, public;

CREATE TABLE euroleague.player_stats_actions_by_game (
  game_id BIGINT NOT NULL REFERENCES euroleague.schedule(game_id) ON DELETE CASCADE,
  team_id BIGINT NOT NULL REFERENCES euroleague.teams(team_id),
  source_event_order INTEGER NOT NULL,
  own_lineup TEXT[], segment_id INTEGER, event_elapsed_seconds NUMERIC,
  type_lineup TEXT, possession_flag SMALLINT NOT NULL,
  action_player_id BIGINT, points INTEGER NOT NULL, play_type TEXT,
  turnovers INTEGER NOT NULL, fgm INTEGER NOT NULL, fga INTEGER NOT NULL,
  fg3_made INTEGER NOT NULL, fg3_att INTEGER NOT NULL,
  ft_attempts INTEGER NOT NULL, ts_possessions INTEGER NOT NULL,
  is_overtime BOOLEAN NOT NULL, regulation_seconds_remaining NUMERIC NOT NULL,
  pre_margin INTEGER NOT NULL, pre_abs_margin INTEGER NOT NULL,
  pre_status SMALLINT NOT NULL CHECK(pre_status BETWEEN -1 AND 1),
  derivation_version TEXT NOT NULL DEFAULT 'player-stats-actions-v1',
  derived_at TIMESTAMPTZ NOT NULL DEFAULT now(),
  PRIMARY KEY(game_id,source_event_order,team_id)
);

CREATE INDEX euroleague_player_stats_actions_filter_idx
  ON euroleague.player_stats_actions_by_game
  (game_id,team_id,regulation_seconds_remaining,pre_abs_margin,pre_status);

ALTER TABLE euroleague.player_stats_actions_by_game ENABLE ROW LEVEL SECURITY;

CREATE OR REPLACE FUNCTION euroleague.refresh_player_stats_actions_for_games(game_ids BIGINT[])
RETURNS BIGINT LANGUAGE plpgsql AS $function$
DECLARE target_ids BIGINT[]; inserted_count BIGINT:=0;
BEGIN
 IF game_ids IS NULL OR array_length(game_ids,1) IS NULL THEN
  SELECT array_agg(game_id ORDER BY game_id) INTO target_ids FROM euroleague.schedule;
  DELETE FROM euroleague.player_stats_actions_by_game;
 ELSE
  SELECT array_agg(DISTINCT x ORDER BY x) INTO target_ids FROM unnest(game_ids) x;
  DELETE FROM euroleague.player_stats_actions_by_game WHERE game_id=ANY(target_ids);
 END IF;
 IF target_ids IS NULL THEN RETURN 0; END IF;
 INSERT INTO euroleague.player_stats_actions_by_game(
  game_id,team_id,source_event_order,own_lineup,segment_id,event_elapsed_seconds,
  type_lineup,possession_flag,action_player_id,points,play_type,turnovers,fgm,fga,
  fg3_made,fg3_att,ft_attempts,ts_possessions,is_overtime,
  regulation_seconds_remaining,pre_margin,pre_abs_margin,pre_status)
 SELECT game_id,team_id,source_event_order,own_lineup,segment_id,event_elapsed_seconds,
  type_lineup,possession_flag,action_player_id,points,play_type,turnovers,fgm,fga,
  fg3_made,fg3_att,ft_attempts,ts_possessions,is_overtime,
  regulation_seconds_remaining,pre_margin,pre_abs_margin,pre_status
 FROM euroleague.player_stats_action_context WHERE game_id=ANY(target_ids);
 GET DIAGNOSTICS inserted_count=ROW_COUNT; RETURN inserted_count;
END;
$function$;

SELECT euroleague.refresh_player_stats_actions_for_games(NULL::bigint[]);

CREATE OR REPLACE VIEW euroleague.player_stats_action_context AS
SELECT game_id,team_id,own_lineup,segment_id,source_event_order,event_elapsed_seconds,
 type_lineup,possession_flag,action_player_id,points,play_type,turnovers,fgm,fga,
 fg3_made,fg3_att,ft_attempts,ts_possessions,is_overtime,
 regulation_seconds_remaining,pre_margin,pre_abs_margin,pre_status
FROM euroleague.player_stats_actions_by_game;

REVOKE ALL ON TABLE euroleague.player_stats_actions_by_game FROM PUBLIC;
REVOKE ALL ON TABLE euroleague.player_stats_actions_by_game FROM app_readonly;
REVOKE ALL ON FUNCTION euroleague.refresh_player_stats_actions_for_games(bigint[]) FROM PUBLIC;
COMMIT;
