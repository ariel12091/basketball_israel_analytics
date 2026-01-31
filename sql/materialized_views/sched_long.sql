-- basketball_test.sched_long source

CREATE OR REPLACE VIEW basketball_test.sched_long
AS WITH base AS (
         SELECT DISTINCT s.game_id,
            s.game_year,
            s.game_date,
            s.gn,
            s.game_type,
            s.team1,
            s.team2,
            s.team_name_eng_1,
            s.team_name_eng_2,
            s.score_team1,
            s.score_team2
           FROM schedule s
        ), mapped AS (
         SELECT b.game_id,
            b.game_year,
            b.game_date,
            b.gn,
            b.game_type,
            b.team1,
            b.team2,
            b.team_name_eng_1,
            b.team_name_eng_2,
            b.score_team1,
            b.score_team2,
            d1.team_id_rosters AS team1_id,
            d2.team_id_rosters AS team2_id
           FROM base b
             JOIN schedule_team_dict d1 ON d1.game_year = b.game_year AND d1.team_id_schedule = b.team1
             JOIN schedule_team_dict d2 ON d2.game_year = b.game_year AND d2.team_id_schedule = b.team2
        )
 SELECT m.game_id,
    m.game_year,
    m.game_date,
    m.gn,
    m.game_type,
    'team1'::text AS team_slot,
    m.team1_id AS team_id,
    m.team_name_eng_1 AS team_name,
    m.score_team1 AS team_score,
    m.team2_id AS opp_team_id,
    m.team_name_eng_2 AS opp_team_name,
    m.score_team2 AS opp_score
   FROM mapped m
UNION ALL
 SELECT m.game_id,
    m.game_year,
    m.game_date,
    m.gn,
    m.game_type,
    'team2'::text AS team_slot,
    m.team2_id AS team_id,
    m.team_name_eng_2 AS team_name,
    m.score_team2 AS team_score,
    m.team1_id AS opp_team_id,
    m.team_name_eng_1 AS opp_team_name,
    m.score_team1 AS opp_score
   FROM mapped m;