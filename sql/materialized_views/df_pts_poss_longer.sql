-- basketball_test.df_pts_poss_lineups_longer_mv source

CREATE MATERIALIZED VIEW basketball_test.df_pts_poss_lineups_longer_mv
TABLESPACE pg_default
AS WITH cum_scores AS (
    SELECT game_id, id,
      SUM(COALESCE(team_score, 0)) OVER (PARTITION BY game_id ORDER BY id) AS total_cum,
      SUM(COALESCE(team_score, 0)) OVER (PARTITION BY game_id, team_id ORDER BY id) AS team_cum
    FROM possessions
)
SELECT quarter,
    parameters_type,
    parameters_points,
    parameters_made,
    id,
    parent_action_id,
    type,
    player_id,
    team_id,
    game_id,
    end_game_seconds_remaining,
    pct_ft,
    team_score,
    final_end_poss,
    segment_id,
    final_end_id,
    own_team_score,
    opp_team_score,
    type_lineup,
    lineup_hash
   FROM ( SELECT pws.quarter,
            pws.parameters_type,
            pws.parameters_points,
            pws.parameters_made,
            pws.id,
            pws.parent_action_id,
            pws.type,
            pws.player_id,
            pws.team_id,
            pws.game_id,
            pws.end_game_seconds_remaining,
            pws.pct_ft,
            pws.team_score,
            pws.final_end_poss,
            pws.segment_id,
            pws.final_end_id,
            cs.team_cum AS own_team_score,
            cs.total_cum - cs.team_cum AS opp_team_score,
            'offense'::text AS type_lineup,
            pws.lineup_hash_offense AS lineup_hash
           FROM pws
           LEFT JOIN cum_scores cs ON pws.game_id = cs.game_id AND pws.id = cs.id
          WHERE pws.game_id <> ALL (ARRAY[62527, 62541, 62522])
        UNION ALL
         SELECT pws.quarter,
            pws.parameters_type,
            pws.parameters_points,
            pws.parameters_made,
            pws.id,
            pws.parent_action_id,
            pws.type,
            pws.player_id,
            pws.team_id_defense,
            pws.game_id,
            pws.end_game_seconds_remaining,
            pws.pct_ft,
            pws.team_score,
            pws.final_end_poss,
            pws.segment_id,
            pws.final_end_id,
            cs.total_cum - cs.team_cum AS own_team_score,
            cs.team_cum AS opp_team_score,
            'defense'::text AS type_lineup,
            pws.lineup_hash_defense AS lineup_hash
           FROM pws
           LEFT JOIN cum_scores cs ON pws.game_id = cs.game_id AND pws.id = cs.id
          WHERE pws.game_id <> ALL (ARRAY[62527, 62541, 62522])) longer
  WHERE lineup_hash IS NOT NULL
WITH DATA;

-- View indexes:
CREATE INDEX dfppl_game_id_idx ON basketball_test.df_pts_poss_lineups_longer_mv USING btree (game_id);
CREATE INDEX dfppl_team_game_idx ON basketball_test.df_pts_poss_lineups_longer_mv USING btree (team_id, game_id);
CREATE INDEX dfppl_lineup_game_cover_idx ON basketball_test.df_pts_poss_lineups_longer_mv USING btree (lineup_hash, game_id, type_lineup) INCLUDE (team_score, final_end_poss, segment_id, end_game_seconds_remaining);
CREATE INDEX dfppl_id_game_type_idx ON basketball_test.df_pts_poss_lineups_longer_mv USING btree (game_id, id, type);
CREATE INDEX dfppl_parent_game_idx ON basketball_test.df_pts_poss_lineups_longer_mv USING btree (game_id, parent_action_id) WHERE (parent_action_id IS NOT NULL);
CREATE INDEX idx_df_longer_game_team_lineup ON basketball_test.df_pts_poss_lineups_longer_mv USING btree (game_id, team_id, lineup_hash);
