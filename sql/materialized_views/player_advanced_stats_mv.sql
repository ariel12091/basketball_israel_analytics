-- basketball_test.player_advanced_stats_mv source

CREATE MATERIALIZED VIEW basketball_test.player_advanced_stats_mv
TABLESPACE pg_default
AS WITH sched AS (
         SELECT DISTINCT s.game_id,
            s.game_year
           FROM schedule s
        ), base0 AS (
         SELECT DISTINCT ll.player_id,
            ll.team_id,
            ll.lineup_hash,
            COALESCE(ll.is_on_verdict, 0::numeric)::integer AS is_on_key
           FROM lineups_lookup ll
        ), clean_stats AS (
         SELECT d.id,
            d.game_id,
            d.lineup_hash,
            d.team_score,
            d.type,
            d.parameters_type,
            d.parameters_made,
            d.pct_ft,
            d.parent_action_id,
            d.type_lineup,
                CASE
                    WHEN d.final_end_poss IS TRUE THEN 1
                    ELSE 0
                END AS final_end_flag
           FROM df_pts_poss_lineups_longer_mv d
        ), complex_flags AS (
         SELECT DISTINCT ON (d.id) d.id AS main_id,
            t2.type AS parent_type,
            t2.parameters_type AS parent_param
           FROM df_pts_poss_lineups_longer_mv d
             JOIN df_pts_poss_lineups_longer_mv t2 ON t2.id = d.parent_action_id AND t2.game_id = d.game_id AND t2.type = 'foul'::text
          WHERE d.parent_action_id IS NOT NULL
          ORDER BY d.id
        ), combined_data AS (
         SELECT b0.player_id,
            b0.team_id,
            b0.is_on_key,
            s.game_year,
            cs.type_lineup,
            cs.team_score,
            cs.final_end_flag,
            cs.type,
            cs.parameters_type,
            cs.parameters_made,
            cs.pct_ft,
            cs.parent_action_id,
            cf.parent_type,
            cf.parent_param
           FROM base0 b0
             JOIN clean_stats cs ON b0.lineup_hash = cs.lineup_hash
             JOIN sched s ON cs.game_id = s.game_id
             LEFT JOIN complex_flags cf ON cs.id = cf.main_id
        ), agg AS (
         SELECT cd.player_id,
            cd.team_id,
            cd.game_year,
            cd.is_on_key,
            cd.type_lineup,
            sum(cd.team_score) AS total_points,
            sum(cd.final_end_flag) AS total_poss,
            count(
                CASE
                    WHEN cd.type = 'shot'::text THEN 1
                    ELSE NULL::integer
                END) + count(DISTINCT
                CASE
                    WHEN cd.type = 'freeThrow'::text AND cd.parent_type = 'foul'::text AND cd.parent_param = 'personal'::text THEN cd.parent_action_id
                    ELSE NULL::integer
                END) AS ts_poss_count,
            count(
                CASE
                    WHEN cd.type = 'rebound'::text AND cd.parameters_type = 'offensive'::text THEN 1
                    ELSE NULL::integer
                END) AS oreb_count,
            count(
                CASE
                    WHEN cd.type = 'shot'::text AND cd.parameters_made = 'missed'::text THEN 1
                    WHEN cd.type = 'freeThrow'::text AND cd.parameters_made = 'missed'::text AND cd.pct_ft = 1::numeric AND cd.parent_type = 'foul'::text AND cd.parent_param = 'personal'::text THEN 1
                    ELSE NULL::integer
                END) AS oreb_opportunities,
            count(
                CASE
                    WHEN cd.type = 'turnover'::text THEN 1
                    ELSE NULL::integer
                END) AS tov_count,
            count(
                CASE
                    WHEN cd.type = 'freeThrow'::text THEN 1
                    ELSE NULL::integer
                END) AS total_ft_attempts,
            count(
                CASE
                    WHEN cd.type = 'shot'::text THEN 1
                    ELSE NULL::integer
                END) AS total_fga
           FROM combined_data cd
          GROUP BY cd.player_id, cd.team_id, cd.game_year, cd.is_on_key, cd.type_lineup
        ), calc_rates AS (
         SELECT a.player_id,
            a.team_id,
            a.game_year,
            a.is_on_key,
            a.type_lineup,
            a.total_points,
            a.total_poss,
            a.ts_poss_count,
            a.oreb_count,
            a.oreb_opportunities,
            a.tov_count,
            a.total_ft_attempts,
            a.total_fga,
            a.total_points / (2.0 * NULLIF(a.ts_poss_count, 0)::numeric) AS ts_pct,
            a.oreb_count::numeric / NULLIF(a.oreb_opportunities, 0)::numeric AS oreb_pct,
            a.tov_count::numeric / NULLIF(a.total_poss, 0)::numeric AS tov_pct,
            a.total_ft_attempts::numeric / NULLIF(a.total_fga, 0)::numeric AS ft_rate
           FROM agg a
        ), pivoted AS (
         SELECT calc_rates.player_id,
            calc_rates.team_id,
            calc_rates.game_year,
            max(
                CASE
                    WHEN calc_rates.type_lineup = 'offense'::text AND calc_rates.is_on_key = 1 THEN calc_rates.ts_pct
                    ELSE NULL::numeric
                END) AS off_on_ts,
            max(
                CASE
                    WHEN calc_rates.type_lineup = 'offense'::text AND calc_rates.is_on_key = 1 THEN calc_rates.oreb_pct
                    ELSE NULL::numeric
                END) AS off_on_oreb,
            max(
                CASE
                    WHEN calc_rates.type_lineup = 'offense'::text AND calc_rates.is_on_key = 1 THEN calc_rates.tov_pct
                    ELSE NULL::numeric
                END) AS off_on_tov,
            max(
                CASE
                    WHEN calc_rates.type_lineup = 'offense'::text AND calc_rates.is_on_key = 1 THEN calc_rates.ft_rate
                    ELSE NULL::numeric
                END) AS off_on_ftr,
            max(
                CASE
                    WHEN calc_rates.type_lineup = 'offense'::text AND calc_rates.is_on_key = 1 THEN calc_rates.total_poss
                    ELSE NULL::bigint
                END) AS off_on_poss,
            max(
                CASE
                    WHEN calc_rates.type_lineup = 'offense'::text AND calc_rates.is_on_key = 0 THEN calc_rates.ts_pct
                    ELSE NULL::numeric
                END) AS off_off_ts,
            max(
                CASE
                    WHEN calc_rates.type_lineup = 'offense'::text AND calc_rates.is_on_key = 0 THEN calc_rates.oreb_pct
                    ELSE NULL::numeric
                END) AS off_off_oreb,
            max(
                CASE
                    WHEN calc_rates.type_lineup = 'offense'::text AND calc_rates.is_on_key = 0 THEN calc_rates.tov_pct
                    ELSE NULL::numeric
                END) AS off_off_tov,
            max(
                CASE
                    WHEN calc_rates.type_lineup = 'offense'::text AND calc_rates.is_on_key = 0 THEN calc_rates.ft_rate
                    ELSE NULL::numeric
                END) AS off_off_ftr,
            max(
                CASE
                    WHEN calc_rates.type_lineup = 'offense'::text AND calc_rates.is_on_key = 0 THEN calc_rates.total_poss
                    ELSE NULL::bigint
                END) AS off_off_poss,
            max(
                CASE
                    WHEN calc_rates.type_lineup = 'defense'::text AND calc_rates.is_on_key = 1 THEN calc_rates.ts_pct
                    ELSE NULL::numeric
                END) AS def_on_ts,
            max(
                CASE
                    WHEN calc_rates.type_lineup = 'defense'::text AND calc_rates.is_on_key = 1 THEN calc_rates.oreb_pct
                    ELSE NULL::numeric
                END) AS def_on_oreb,
            max(
                CASE
                    WHEN calc_rates.type_lineup = 'defense'::text AND calc_rates.is_on_key = 1 THEN calc_rates.tov_pct
                    ELSE NULL::numeric
                END) AS def_on_tov,
            max(
                CASE
                    WHEN calc_rates.type_lineup = 'defense'::text AND calc_rates.is_on_key = 1 THEN calc_rates.ft_rate
                    ELSE NULL::numeric
                END) AS def_on_ftr,
            max(
                CASE
                    WHEN calc_rates.type_lineup = 'defense'::text AND calc_rates.is_on_key = 1 THEN calc_rates.total_poss
                    ELSE NULL::bigint
                END) AS def_on_poss,
            max(
                CASE
                    WHEN calc_rates.type_lineup = 'defense'::text AND calc_rates.is_on_key = 0 THEN calc_rates.ts_pct
                    ELSE NULL::numeric
                END) AS def_off_ts,
            max(
                CASE
                    WHEN calc_rates.type_lineup = 'defense'::text AND calc_rates.is_on_key = 0 THEN calc_rates.oreb_pct
                    ELSE NULL::numeric
                END) AS def_off_oreb,
            max(
                CASE
                    WHEN calc_rates.type_lineup = 'defense'::text AND calc_rates.is_on_key = 0 THEN calc_rates.tov_pct
                    ELSE NULL::numeric
                END) AS def_off_tov,
            max(
                CASE
                    WHEN calc_rates.type_lineup = 'defense'::text AND calc_rates.is_on_key = 0 THEN calc_rates.ft_rate
                    ELSE NULL::numeric
                END) AS def_off_ftr,
            max(
                CASE
                    WHEN calc_rates.type_lineup = 'defense'::text AND calc_rates.is_on_key = 0 THEN calc_rates.total_poss
                    ELSE NULL::bigint
                END) AS def_off_poss
           FROM calc_rates
          GROUP BY calc_rates.player_id, calc_rates.team_id, calc_rates.game_year
        ), final_rows AS (
         SELECT p.player_id,
            p.team_id,
            p.game_year,
            p.off_on_ts,
            p.off_on_oreb,
            p.off_on_tov,
            p.off_on_ftr,
            p.off_on_poss,
            p.off_off_ts,
            p.off_off_oreb,
            p.off_off_tov,
            p.off_off_ftr,
            p.off_off_poss,
            p.def_on_ts,
            p.def_on_oreb,
            p.def_on_tov,
            p.def_on_ftr,
            p.def_on_poss,
            p.def_off_ts,
            p.def_off_oreb,
            p.def_off_tov,
            p.def_off_ftr,
            p.def_off_poss,
            p.off_on_ts - p.off_off_ts AS diff_off_ts,
            p.off_on_oreb - p.off_off_oreb AS diff_off_oreb,
            p.off_on_tov - p.off_off_tov AS diff_off_tov,
            p.off_on_ftr - p.off_off_ftr AS diff_off_ftr,
            p.def_on_ts - p.def_off_ts AS diff_def_ts,
            p.def_on_oreb - p.def_off_oreb AS diff_def_oreb,
            p.def_on_tov - p.def_off_tov AS diff_def_tov,
            p.def_on_ftr - p.def_off_ftr AS diff_def_ftr,
            percent_rank() OVER (PARTITION BY p.game_year ORDER BY (p.off_on_ts - p.off_off_ts)) AS pr_diff_off_ts,
            percent_rank() OVER (PARTITION BY p.game_year ORDER BY (p.off_on_oreb - p.off_off_oreb)) AS pr_diff_off_oreb,
            percent_rank() OVER (PARTITION BY p.game_year ORDER BY (p.def_on_ts - p.def_off_ts) DESC) AS pr_diff_def_ts
           FROM pivoted p
        ), final_names AS (
         SELECT fr.player_id,
            fr.team_id,
            r.firstname,
            r.lastname,
            r.team_name,
            fr.game_year,
            fr.off_on_ts,
            fr.off_off_ts,
            fr.def_on_ts,
            fr.def_off_ts,
            fr.off_on_oreb,
            fr.off_off_oreb,
            fr.def_on_oreb,
            fr.def_off_oreb,
            fr.off_on_tov,
            fr.off_off_tov,
            fr.def_on_tov,
            fr.def_off_tov,
            fr.off_on_ftr,
            fr.off_off_ftr,
            fr.def_on_ftr,
            fr.def_off_ftr,
            fr.off_on_poss,
            fr.off_off_poss,
            fr.def_on_poss,
            fr.def_off_poss,
            round(fr.diff_off_ts * 100::numeric, 1) AS "Off TS% Diff",
            round(fr.diff_off_oreb * 100::numeric, 1) AS "Off OREB% Diff",
            round(fr.diff_off_tov * 100::numeric, 1) AS "Off TOV% Diff",
            round(fr.diff_off_ftr * 100::numeric, 1) AS "Off FTR Diff",
            round(fr.diff_def_ts * 100::numeric, 1) AS "Def TS% Diff",
            round(fr.diff_def_oreb * 100::numeric, 1) AS "Def OREB% Diff",
            round(fr.diff_def_tov * 100::numeric, 1) AS "Def TOV% Diff",
            round(fr.diff_def_ftr * 100::numeric, 1) AS "Def FTR Diff",
            fr.pr_diff_off_ts,
            fr.pr_diff_off_oreb,
            fr.pr_diff_def_ts
           FROM final_rows fr
             JOIN ( SELECT DISTINCT full_rosters.player_id,
                    full_rosters.team_id,
                    full_rosters.firstname,
                    full_rosters.lastname,
                    full_rosters.team_name
                   FROM full_rosters) r ON fr.player_id = r.player_id AND fr.team_id = r.team_id
        )
 SELECT player_id,
    team_id,
    firstname,
    lastname,
    team_name,
    game_year,
    off_on_ts,
    off_off_ts,
    def_on_ts,
    def_off_ts,
    off_on_oreb,
    off_off_oreb,
    def_on_oreb,
    def_off_oreb,
    off_on_tov,
    off_off_tov,
    def_on_tov,
    def_off_tov,
    off_on_ftr,
    off_off_ftr,
    def_on_ftr,
    def_off_ftr,
    off_on_poss,
    off_off_poss,
    def_on_poss,
    def_off_poss,
    "Off TS% Diff",
    "Off OREB% Diff",
    "Off TOV% Diff",
    "Off FTR Diff",
    "Def TS% Diff",
    "Def OREB% Diff",
    "Def TOV% Diff",
    "Def FTR Diff",
    pr_diff_off_ts,
    pr_diff_off_oreb,
    pr_diff_def_ts
   FROM final_names
  ORDER BY "Off TS% Diff" DESC
WITH DATA;