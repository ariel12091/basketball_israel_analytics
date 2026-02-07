-- basketball_test.team_ppp_ratings_mv source

CREATE MATERIALIZED VIEW basketball_test.team_ppp_ratings_mv
TABLESPACE pg_default
AS WITH base AS (
         SELECT s.game_year,
            dppllm.team_id,
            dppllm.type_lineup,
            sum(dppllm.team_score) / NULLIF(sum(dppllm.final_end_poss::integer), 0)::numeric AS ppp,
            sum(dppllm.final_end_poss::integer) AS total_poss,
            COUNT(DISTINCT dppllm.game_id) AS games_count
           FROM df_pts_poss_lineups_longer_mv dppllm
             JOIN schedule s USING (game_id)
          GROUP BY s.game_year, dppllm.team_id, dppllm.type_lineup
        ), win_loss AS (
         SELECT fs.game_year,
            fs.team_id,
            COUNT(*) FILTER (WHERE fs.has_won = TRUE) AS wins,
            COUNT(*) FILTER (WHERE fs.has_won = FALSE) AS losses
           FROM final_schedule_mv fs
          GROUP BY fs.game_year, fs.team_id
        ), pivoted AS (
         SELECT base.game_year,
            base.team_id,
            max(base.ppp) FILTER (WHERE base.type_lineup = 'offense'::text) AS off_ppp_raw,
            max(base.ppp) FILTER (WHERE base.type_lineup = 'defense'::text) AS def_ppp_raw,
            max(base.games_count) AS games_played,
            max(base.total_poss) FILTER (WHERE base.type_lineup = 'offense'::text) AS off_poss,
            max(base.total_poss) FILTER (WHERE base.type_lineup = 'defense'::text) AS def_poss
           FROM base
          GROUP BY base.game_year, base.team_id
        ), teams AS (
         SELECT DISTINCT full_rosters.game_year,
            full_rosters.team_id,
            full_rosters.team_name
           FROM full_rosters
        ), final AS (
         SELECT p.game_year,
            t.team_id,
            t.team_name,
            round(p.off_ppp_raw, 3) * 100::numeric AS off_ppp,
            round(p.def_ppp_raw, 3) * 100::numeric AS def_ppp,
            round(p.off_ppp_raw - p.def_ppp_raw, 3) * 100::numeric AS net_rtg,
            p.games_played,
            wl.wins,
            wl.losses,
            p.off_poss,
            p.def_poss
           FROM pivoted p
             JOIN teams t ON t.game_year = p.game_year AND t.team_id = p.team_id
             LEFT JOIN win_loss wl ON wl.game_year = p.game_year AND wl.team_id = p.team_id
        )
 SELECT game_year,
    team_id,
    team_name,
    off_ppp,
    def_ppp,
    net_rtg,
    games_played,
    wins,
    losses,
    off_poss,
    def_poss,
    dense_rank() OVER (PARTITION BY game_year ORDER BY net_rtg DESC NULLS LAST) AS rank_net_rtg,
    dense_rank() OVER (PARTITION BY game_year ORDER BY off_ppp DESC NULLS LAST) AS rank_off_ppp,
    dense_rank() OVER (PARTITION BY game_year ORDER BY def_ppp) AS rank_def_ppp
   FROM final
WITH DATA;

-- View indexes:
CREATE INDEX team_ppp_ratings_mv_join_idx ON basketball_test.team_ppp_ratings_mv USING btree (game_year, team_id);
