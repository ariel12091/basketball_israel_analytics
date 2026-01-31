-- basketball_test.final_schedule_mv source

CREATE MATERIALIZED VIEW basketball_test.final_schedule_mv
TABLESPACE pg_default
AS SELECT game_id,
    game_year,
    game_date,
    gn,
    game_type,
    team_id,
    team_name,
    opp_team_id,
    opp_team_name,
    team_score,
    opp_score,
    team_score - opp_score AS margin,
    team_score > opp_score AS has_won,
    team_slot = 'team1'::text AS is_home
   FROM sched_long s
WITH DATA;

-- View indexes:
CREATE INDEX final_sched_mv_date ON basketball_test.final_schedule_mv USING btree (game_date);
CREATE INDEX final_sched_mv_filters ON basketball_test.final_schedule_mv USING btree (game_year, game_type, is_home, has_won);
CREATE INDEX final_sched_mv_opp ON basketball_test.final_schedule_mv USING btree (game_year, opp_team_id);
CREATE INDEX final_sched_mv_team ON basketball_test.final_schedule_mv USING btree (game_year, team_id);