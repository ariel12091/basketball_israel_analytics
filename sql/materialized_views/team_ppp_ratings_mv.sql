-- basketball_test.team_ppp_ratings_mv source

CREATE MATERIALIZED VIEW basketball_test.team_ppp_ratings_mv
TABLESPACE pg_default
AS WITH base AS (
         SELECT s.game_year,
            dppllm.team_id,
            dppllm.type_lineup,
            sum(dppllm.team_score) / NULLIF(sum(dppllm.final_end_poss::integer), 0)::numeric AS ppp
           FROM df_pts_poss_lineups_longer_mv dppllm
             JOIN schedule s USING (game_id)
          GROUP BY s.game_year, dppllm.team_id, dppllm.type_lineup
        ), pivoted AS (
         SELECT base.game_year,
            base.team_id,
            max(base.ppp) FILTER (WHERE base.type_lineup = 'offense'::text) AS off_ppp_raw,
            max(base.ppp) FILTER (WHERE base.type_lineup = 'defense'::text) AS def_ppp_raw
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
            round(p.off_ppp_raw - p.def_ppp_raw, 3) * 100::numeric AS net_rtg
           FROM pivoted p
             JOIN teams t ON t.game_year = p.game_year AND t.team_id = p.team_id
        )
 SELECT game_year,
    team_id,
    team_name,
    off_ppp,
    def_ppp,
    net_rtg,
    dense_rank() OVER (PARTITION BY game_year ORDER BY net_rtg DESC NULLS LAST) AS rank_net_rtg,
    dense_rank() OVER (PARTITION BY game_year ORDER BY off_ppp DESC NULLS LAST) AS rank_off_ppp,
    dense_rank() OVER (PARTITION BY game_year ORDER BY def_ppp) AS rank_def_ppp
   FROM final
WITH DATA;

-- View indexes:
CREATE INDEX team_ppp_ratings_mv_join_idx ON basketball_test.team_ppp_ratings_mv USING btree (game_year, team_id);