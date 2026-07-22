-- basketball_test.onoff_default_mv source

DO $$
BEGIN
  IF EXISTS (
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'basketball_test'
      AND c.relname = 'onoff_default_mv'
      AND c.relkind = 'm'
  ) THEN
    EXECUTE 'DROP MATERIALIZED VIEW basketball_test.onoff_default_mv';
  ELSIF EXISTS (
    SELECT 1
    FROM pg_class c
    JOIN pg_namespace n ON n.oid = c.relnamespace
    WHERE n.nspname = 'basketball_test'
      AND c.relname = 'onoff_default_mv'
      AND c.relkind = 'r'
  ) THEN
    EXECUTE 'DROP TABLE basketball_test.onoff_default_mv';
  END IF;
END
$$;

CREATE TABLE basketball_test.onoff_default_mv AS
WITH sched AS (
         SELECT DISTINCT schedule.game_id,
            schedule.game_date,
            schedule.game_year
           FROM schedule
        ), base0 AS (
         SELECT DISTINCT lineups_lookup.player_id,
            lineups_lookup.team_id,
            lineups_lookup.lineup_hash,
            COALESCE(lineups_lookup.is_on_verdict, 0::numeric)::integer AS is_on_key
           FROM lineups_lookup
        ), base AS (
         SELECT b0.player_id,
            b0.team_id,
            b0.is_on_key,
            b0.lineup_hash,
            d.game_id,
            d.type_lineup,
            d.team_score,
                CASE
                    WHEN d.final_end_poss IS TRUE THEN 1
                    ELSE 0
                END AS final_end_flag,
            s.game_year,
            CASE WHEN d.type = 'shot' AND d.parameters_points = 2 AND d.parameters_made = 'made' THEN 1 ELSE 0 END AS fg2_made_flag,
            CASE WHEN d.type = 'shot' AND d.parameters_points = 2 THEN 1 ELSE 0 END AS fg2_att_flag,
            CASE WHEN d.type = 'shot' AND d.parameters_points = 3 AND d.parameters_made = 'made' THEN 1 ELSE 0 END AS fg3_made_flag,
            CASE WHEN d.type = 'shot' AND d.parameters_points = 3 THEN 1 ELSE 0 END AS fg3_att_flag,
            CASE WHEN d.type = 'shot' AND d.parameters_points = 2 AND d.parameters_type = 'lay-up' AND d.parameters_made = 'made' THEN 1 ELSE 0 END AS layup_made_flag,
            CASE WHEN d.type = 'shot' AND d.parameters_points = 2 AND d.parameters_type = 'lay-up' THEN 1 ELSE 0 END AS layup_att_flag,
            CASE WHEN d.type = 'shot' AND d.parameters_points = 2 AND d.parameters_type IN ('dunk', 'allyhoop') AND d.parameters_made = 'made' THEN 1 ELSE 0 END AS dunk_made_flag,
            CASE WHEN d.type = 'shot' AND d.parameters_points = 2 AND d.parameters_type IN ('dunk', 'allyhoop') THEN 1 ELSE 0 END AS dunk_att_flag,
            CASE WHEN d.type = 'shot' AND d.parameters_points = 3 AND z.is_corner3 IS TRUE AND d.parameters_made = 'made' THEN 1 ELSE 0 END AS c3_made_flag,
            CASE WHEN d.type = 'shot' AND d.parameters_points = 3 AND z.is_corner3 IS TRUE THEN 1 ELSE 0 END AS c3_att_flag,
            CASE WHEN d.type = 'shot' AND d.parameters_points = 3 AND z.is_corner3 IS NOT NULL THEN 1 ELSE 0 END AS c3_known_att_flag
           FROM base0 b0
             JOIN df_pts_poss_lineups_longer_mv d USING (lineup_hash)
             JOIN sched s USING (game_id)
             LEFT JOIN shot_zones z ON z.game_id = d.game_id AND z.id = d.id
        ), agg AS (
         SELECT base.player_id,
            base.team_id,
            base.is_on_key,
            base.type_lineup,
            base.game_year,
            sum(base.team_score) AS total_pts,
            sum(base.final_end_flag) AS total_poss,
            round(sum(base.team_score) / NULLIF(sum(base.final_end_flag), 0)::numeric * 100::numeric, 1) AS ppp_calc
           FROM base
          GROUP BY base.player_id, base.team_id, base.is_on_key, base.type_lineup, base.game_year
        ), shot_agg AS (
         SELECT base.player_id,
            base.team_id,
            base.is_on_key,
            base.type_lineup,
            base.game_year,
            sum(base.fg2_made_flag) AS fg2_made,
            sum(base.fg2_att_flag) AS fg2_att,
            sum(base.fg3_made_flag) AS fg3_made,
            sum(base.fg3_att_flag) AS fg3_att,
            sum(base.layup_made_flag) AS layup_made,
            sum(base.layup_att_flag) AS layup_att,
            sum(base.dunk_made_flag) AS dunk_made,
            sum(base.dunk_att_flag) AS dunk_att,
            sum(base.c3_made_flag) AS c3_made,
            sum(base.c3_att_flag) AS c3_att,
            sum(base.c3_known_att_flag) AS c3_known_att
           FROM base
          GROUP BY base.player_id, base.team_id, base.is_on_key, base.type_lineup, base.game_year
        ), player_segments AS (
         SELECT b0.player_id,
            b0.team_id,
            b0.is_on_key,
            s.game_year,
            d.game_id,
            d.lineup_hash,
            d.type_lineup,
            d.segment_id,
            MAX(d.segment_seconds)::numeric AS seg_seconds
           FROM base0 b0
             JOIN df_pts_poss_lineups_longer_mv d
               ON d.lineup_hash = b0.lineup_hash
              AND d.team_id = b0.team_id
             JOIN sched s USING (game_id)
          WHERE d.segment_id IS NOT NULL
            AND d.segment_seconds IS NOT NULL
          GROUP BY b0.player_id, b0.team_id, b0.is_on_key, s.game_year, d.game_id, d.lineup_hash, d.type_lineup, d.segment_id
        ), player_minutes AS (
         SELECT player_segments.player_id,
            player_segments.team_id,
            player_segments.game_year,
            ROUND(
              COALESCE(SUM(player_segments.seg_seconds) FILTER (
                WHERE player_segments.is_on_key = 1
                  AND player_segments.type_lineup = 'offense'::text
              ), 0) / 60.0,
              1
            )::numeric AS minutes
           FROM player_segments
          GROUP BY player_segments.player_id, player_segments.team_id, player_segments.game_year
        ), ppp_ranked AS (
         SELECT a.player_id,
            a.team_id,
            a.is_on_key,
            a.type_lineup,
            a.game_year,
            a.total_pts,
            a.total_poss,
            a.ppp_calc,
            percent_rank() OVER (PARTITION BY a.type_lineup, a.game_year ORDER BY a.ppp_calc) AS pr_ppp_raw,
                CASE
                    WHEN a.type_lineup = 'defense'::text THEN 1::double precision - percent_rank() OVER (PARTITION BY a.type_lineup, a.game_year ORDER BY a.ppp_calc)
                    ELSE percent_rank() OVER (PARTITION BY a.type_lineup, a.game_year ORDER BY a.ppp_calc)
                END AS pr_ppp_better
           FROM agg a
        ), roster_names AS (
         SELECT full_rosters.player_id,
            full_rosters.team_id,
            schedule.game_year,
            COALESCE(
              max(trim(regexp_replace(full_rosters.firstname, '\s+', ' ', 'g'))) FILTER (WHERE full_rosters.firstname ~ '^[A-Za-z .''-]+$'),
              max(trim(regexp_replace(full_rosters.firstname, '\s+', ' ', 'g')))
            ) AS firstname,
            COALESCE(
              max(trim(regexp_replace(regexp_replace(full_rosters.lastname, '\.\s+', '.', 'g'), '\s+', ' ', 'g'))) FILTER (WHERE full_rosters.lastname ~ '^[A-Za-z .''-]+$'),
              max(trim(regexp_replace(regexp_replace(full_rosters.lastname, '\.\s+', '.', 'g'), '\s+', ' ', 'g')))
            ) AS lastname,
            max(trim(regexp_replace(full_rosters.team_name, '\s+', ' ', 'g'))) AS team_name
           FROM full_rosters
             JOIN schedule ON full_rosters.game_id = schedule.game_id
          GROUP BY full_rosters.player_id, full_rosters.team_id, schedule.game_year
        ), with_names AS (
         SELECT a.player_id,
            a.team_id,
            a.game_year,
            a.is_on_key,
            a.type_lineup,
            a.total_pts,
            a.total_poss,
            a.ppp_calc,
            a.pr_ppp_raw,
            a.pr_ppp_better,
            r.firstname,
            r.lastname,
            r.team_name
           FROM ppp_ranked a
             JOIN roster_names r USING (player_id, team_id, game_year)
        ), elig AS (
         SELECT with_names.player_id,
            with_names.team_id,
            with_names.game_year,
            min(with_names.total_poss) AS min_poss_all,
            max(
                CASE
                    WHEN with_names.is_on_key = 1 THEN with_names.total_poss
                    ELSE 0::bigint
                END) AS max_poss_on
           FROM with_names
          GROUP BY with_names.player_id, with_names.team_id, with_names.game_year
        ), filtered AS (
         SELECT wn.player_id,
            wn.team_id,
            wn.game_year,
            wn.is_on_key,
            wn.type_lineup,
            wn.total_pts,
            wn.total_poss,
            wn.ppp_calc,
            wn.pr_ppp_raw,
            wn.pr_ppp_better,
            wn.firstname,
            wn.lastname,
            wn.team_name
           FROM with_names wn
             JOIN elig e USING (player_id, team_id, game_year)
        ), step1 AS (
         SELECT filtered.player_id,
            filtered.team_id,
            filtered.game_year,
            filtered.is_on_key,
            filtered.type_lineup,
            filtered.total_pts,
            filtered.total_poss,
            filtered.ppp_calc,
            filtered.pr_ppp_raw,
            filtered.pr_ppp_better,
            filtered.firstname,
            filtered.lastname,
            filtered.team_name,
                CASE
                    WHEN filtered.type_lineup = 'offense'::text THEN 1
                    WHEN filtered.type_lineup = 'defense'::text THEN 2
                    ELSE 3
                END AS type_key,
            filtered.ppp_calc - lag(filtered.ppp_calc) OVER (PARTITION BY filtered.player_id, filtered.team_id, filtered.type_lineup, filtered.game_year ORDER BY filtered.is_on_key) AS net_rtg
           FROM filtered
        ), step1_on_rank AS (
         SELECT s1.player_id,
            s1.team_id,
            s1.type_lineup,
            s1.game_year,
            s1.is_on_key,
            percent_rank() OVER (PARTITION BY s1.type_lineup, s1.game_year ORDER BY s1.net_rtg) AS pr_net_rtg_raw,
                CASE
                    WHEN s1.type_lineup = 'defense'::text THEN 1::double precision - percent_rank() OVER (PARTITION BY s1.type_lineup, s1.game_year ORDER BY s1.net_rtg)
                    ELSE percent_rank() OVER (PARTITION BY s1.type_lineup, s1.game_year ORDER BY s1.net_rtg)
                END AS pr_net_rtg_better
           FROM step1 s1
          WHERE s1.is_on_key = 1 AND s1.net_rtg IS NOT NULL
        ), step1_joined AS (
         SELECT s1.player_id,
            s1.team_id,
            s1.game_year,
            s1.is_on_key,
            s1.type_lineup,
            s1.total_pts,
            s1.total_poss,
            s1.ppp_calc,
            s1.pr_ppp_raw,
            s1.pr_ppp_better,
            s1.firstname,
            s1.lastname,
            s1.team_name,
            s1.type_key,
            s1.net_rtg,
            r.pr_net_rtg_raw,
            r.pr_net_rtg_better
           FROM step1 s1
             LEFT JOIN step1_on_rank r ON r.player_id = s1.player_id AND r.team_id = s1.team_id AND r.type_lineup = s1.type_lineup AND r.is_on_key = s1.is_on_key AND r.game_year = s1.game_year
        ), step2 AS (
         SELECT s1j.player_id,
            s1j.team_id,
            s1j.game_year,
            s1j.is_on_key,
            s1j.type_lineup,
            s1j.total_pts,
            s1j.total_poss,
            s1j.ppp_calc,
            s1j.pr_ppp_raw,
            s1j.pr_ppp_better,
            s1j.firstname,
            s1j.lastname,
            s1j.team_name,
            s1j.type_key,
            s1j.net_rtg,
            s1j.pr_net_rtg_raw,
            s1j.pr_net_rtg_better,
            round(lag(s1j.net_rtg) OVER (PARTITION BY s1j.player_id, s1j.team_id, s1j.is_on_key, s1j.game_year ORDER BY s1j.type_key) - s1j.net_rtg, 2) AS total_net_rtg
           FROM step1_joined s1j
        ), step2_rank AS (
         SELECT s2.player_id,
            s2.team_id,
            s2.type_lineup,
            s2.game_year,
            s2.is_on_key,
            percent_rank() OVER (PARTITION BY s2.game_year ORDER BY s2.total_net_rtg) AS pr_total_net
           FROM step2 s2
          WHERE s2.total_net_rtg IS NOT NULL
        ), step2_joined AS (
         SELECT s2.player_id,
            s2.team_id,
            s2.game_year,
            s2.is_on_key,
            s2.type_lineup,
            s2.total_pts,
            s2.total_poss,
            s2.ppp_calc,
            s2.pr_ppp_raw,
            s2.pr_ppp_better,
            s2.firstname,
            s2.lastname,
            s2.team_name,
            s2.type_key,
            s2.net_rtg,
            s2.pr_net_rtg_raw,
            s2.pr_net_rtg_better,
            s2.total_net_rtg,
            r.pr_total_net
           FROM step2 s2
             LEFT JOIN step2_rank r ON r.player_id = s2.player_id AND r.team_id = s2.team_id AND r.type_lineup = s2.type_lineup AND r.is_on_key = s2.is_on_key AND r.game_year = s2.game_year
        ), final_rows AS (
         SELECT s2j.player_id,
            s2j.team_id,
            s2j.game_year,
            s2j.team_name,
            s2j.firstname,
            s2j.lastname,
            max(
                CASE
                    WHEN s2j.type_lineup = 'offense'::text AND s2j.is_on_key = 1 THEN s2j.ppp_calc
                    ELSE NULL::numeric
                END) AS offense_on_ppp,
            max(
                CASE
                    WHEN s2j.type_lineup = 'offense'::text AND s2j.is_on_key = 0 THEN s2j.ppp_calc
                    ELSE NULL::numeric
                END) AS offense_off_ppp,
            max(
                CASE
                    WHEN s2j.type_lineup = 'defense'::text AND s2j.is_on_key = 1 THEN s2j.ppp_calc
                    ELSE NULL::numeric
                END) AS defense_on_ppp,
            max(
                CASE
                    WHEN s2j.type_lineup = 'defense'::text AND s2j.is_on_key = 0 THEN s2j.ppp_calc
                    ELSE NULL::numeric
                END) AS defense_off_ppp,
            max(
                CASE
                    WHEN s2j.type_lineup = 'offense'::text AND s2j.is_on_key = 1 THEN s2j.pr_ppp_better
                    ELSE NULL::double precision
                END) AS pr_off_on,
            max(
                CASE
                    WHEN s2j.type_lineup = 'offense'::text AND s2j.is_on_key = 0 THEN s2j.pr_ppp_better
                    ELSE NULL::double precision
                END) AS pr_off_off,
            max(
                CASE
                    WHEN s2j.type_lineup = 'defense'::text AND s2j.is_on_key = 1 THEN s2j.pr_ppp_better
                    ELSE NULL::double precision
                END) AS pr_def_on_inv,
            max(
                CASE
                    WHEN s2j.type_lineup = 'defense'::text AND s2j.is_on_key = 0 THEN s2j.pr_ppp_better
                    ELSE NULL::double precision
                END) AS pr_def_off_inv,
            max(
                CASE
                    WHEN s2j.type_lineup = 'offense'::text AND s2j.is_on_key = 1 THEN s2j.net_rtg
                    ELSE NULL::numeric
                END) AS offense_on_diff,
            max(
                CASE
                    WHEN s2j.type_lineup = 'defense'::text AND s2j.is_on_key = 1 THEN s2j.net_rtg
                    ELSE NULL::numeric
                END) AS defense_on_diff,
            max(
                CASE
                    WHEN s2j.type_lineup = 'offense'::text AND s2j.is_on_key = 1 THEN s2j.pr_net_rtg_better
                    ELSE NULL::double precision
                END) AS pr_off_on_d,
            max(
                CASE
                    WHEN s2j.type_lineup = 'defense'::text AND s2j.is_on_key = 1 THEN s2j.pr_net_rtg_raw
                    ELSE NULL::double precision
                END) AS pr_def_on_d,
            max(
                CASE
                    WHEN s2j.type_lineup = 'defense'::text AND s2j.is_on_key = 1 THEN s2j.pr_net_rtg_better
                    ELSE NULL::double precision
                END) AS pr_def_on_d_inv,
            max(s2j.total_net_rtg) AS total_net_rtg,
            max(s2j.pr_total_net) AS pr_net,
            max(
                CASE
                    WHEN s2j.is_on_key = 1 THEN s2j.total_poss
                    ELSE NULL::bigint
                END) AS on_poss,
            max(
                CASE
                    WHEN s2j.is_on_key = 0 THEN s2j.total_poss
                    ELSE NULL::bigint
                END) AS off_poss,
            -- Shooting splits (16 columns)
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 1 THEN sa.fg2_made END) AS off_on_fg2_made,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 1 THEN sa.fg2_att END)  AS off_on_fg2_att,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 1 THEN sa.fg3_made END) AS off_on_fg3_made,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 1 THEN sa.fg3_att END)  AS off_on_fg3_att,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 0 THEN sa.fg2_made END) AS off_off_fg2_made,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 0 THEN sa.fg2_att END)  AS off_off_fg2_att,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 0 THEN sa.fg3_made END) AS off_off_fg3_made,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 0 THEN sa.fg3_att END)  AS off_off_fg3_att,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 1 THEN sa.fg2_made END) AS def_on_fg2_made,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 1 THEN sa.fg2_att END)  AS def_on_fg2_att,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 1 THEN sa.fg3_made END) AS def_on_fg3_made,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 1 THEN sa.fg3_att END)  AS def_on_fg3_att,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 0 THEN sa.fg2_made END) AS def_off_fg2_made,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 0 THEN sa.fg2_att END)  AS def_off_fg2_att,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 0 THEN sa.fg3_made END) AS def_off_fg3_made,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 0 THEN sa.fg3_att END)  AS def_off_fg3_att,
            -- Shot Profile counts (28 columns, c3_known_att = 3PA with known corner flag)
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 1 THEN sa.layup_made END) AS off_on_layup_made,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 1 THEN sa.layup_att END)  AS off_on_layup_att,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 1 THEN sa.dunk_made END) AS off_on_dunk_made,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 1 THEN sa.dunk_att END)  AS off_on_dunk_att,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 1 THEN sa.c3_made END) AS off_on_c3_made,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 1 THEN sa.c3_att END)  AS off_on_c3_att,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 1 THEN sa.c3_known_att END) AS off_on_c3_known_att,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 0 THEN sa.layup_made END) AS off_off_layup_made,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 0 THEN sa.layup_att END)  AS off_off_layup_att,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 0 THEN sa.dunk_made END) AS off_off_dunk_made,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 0 THEN sa.dunk_att END)  AS off_off_dunk_att,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 0 THEN sa.c3_made END) AS off_off_c3_made,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 0 THEN sa.c3_att END)  AS off_off_c3_att,
            MAX(CASE WHEN sa.type_lineup = 'offense' AND sa.is_on_key = 0 THEN sa.c3_known_att END) AS off_off_c3_known_att,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 1 THEN sa.layup_made END) AS def_on_layup_made,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 1 THEN sa.layup_att END)  AS def_on_layup_att,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 1 THEN sa.dunk_made END) AS def_on_dunk_made,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 1 THEN sa.dunk_att END)  AS def_on_dunk_att,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 1 THEN sa.c3_made END) AS def_on_c3_made,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 1 THEN sa.c3_att END)  AS def_on_c3_att,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 1 THEN sa.c3_known_att END) AS def_on_c3_known_att,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 0 THEN sa.layup_made END) AS def_off_layup_made,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 0 THEN sa.layup_att END)  AS def_off_layup_att,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 0 THEN sa.dunk_made END) AS def_off_dunk_made,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 0 THEN sa.dunk_att END)  AS def_off_dunk_att,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 0 THEN sa.c3_made END) AS def_off_c3_made,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 0 THEN sa.c3_att END)  AS def_off_c3_att,
            MAX(CASE WHEN sa.type_lineup = 'defense' AND sa.is_on_key = 0 THEN sa.c3_known_att END) AS def_off_c3_known_att
           FROM step2_joined s2j
           LEFT JOIN shot_agg sa
             ON sa.player_id = s2j.player_id
            AND sa.team_id = s2j.team_id
            AND sa.is_on_key = s2j.is_on_key
            AND sa.type_lineup = s2j.type_lineup
            AND sa.game_year = s2j.game_year
          GROUP BY s2j.player_id, s2j.team_id, s2j.game_year, s2j.team_name, s2j.firstname, s2j.lastname
        ), final_scored AS (
         SELECT fr.player_id,
            fr.team_id,
            fr.game_year,
            fr.team_name,
            fr.firstname,
            fr.lastname,
            fr.offense_on_ppp,
            fr.offense_off_ppp,
            fr.defense_on_ppp,
            fr.defense_off_ppp,
            fr.pr_off_on,
            fr.pr_off_off,
            fr.pr_def_on_inv,
            fr.pr_def_off_inv,
            fr.offense_on_diff,
            fr.defense_on_diff,
            fr.pr_off_on_d,
            fr.pr_def_on_d,
            fr.pr_def_on_d_inv,
            fr.total_net_rtg,
            fr.pr_net,
            fr.on_poss,
            fr.off_poss,
            COALESCE(pm.minutes, 0)::numeric AS minutes,
            fr.off_on_fg2_made, fr.off_on_fg2_att, fr.off_on_fg3_made, fr.off_on_fg3_att,
            fr.off_off_fg2_made, fr.off_off_fg2_att, fr.off_off_fg3_made, fr.off_off_fg3_att,
            fr.def_on_fg2_made, fr.def_on_fg2_att, fr.def_on_fg3_made, fr.def_on_fg3_att,
            fr.def_off_fg2_made, fr.def_off_fg2_att, fr.def_off_fg3_made, fr.def_off_fg3_att,
            fr.off_on_layup_made, fr.off_on_layup_att, fr.off_on_dunk_made, fr.off_on_dunk_att,
            fr.off_on_c3_made, fr.off_on_c3_att, fr.off_on_c3_known_att,
            fr.off_off_layup_made, fr.off_off_layup_att, fr.off_off_dunk_made, fr.off_off_dunk_att,
            fr.off_off_c3_made, fr.off_off_c3_att, fr.off_off_c3_known_att,
            fr.def_on_layup_made, fr.def_on_layup_att, fr.def_on_dunk_made, fr.def_on_dunk_att,
            fr.def_on_c3_made, fr.def_on_c3_att, fr.def_on_c3_known_att,
            fr.def_off_layup_made, fr.def_off_layup_att, fr.def_off_dunk_made, fr.def_off_dunk_att,
            fr.def_off_c3_made, fr.def_off_c3_att, fr.def_off_c3_known_att,
            fr.offense_on_ppp - fr.defense_on_ppp AS on_net_rtg,
            fr.offense_off_ppp - fr.defense_off_ppp AS off_net_rtg,
            percent_rank() OVER (PARTITION BY fr.game_year ORDER BY (fr.offense_on_ppp - fr.defense_on_ppp)) AS pr_on_net,
            percent_rank() OVER (PARTITION BY fr.game_year ORDER BY (fr.offense_off_ppp - fr.defense_off_ppp)) AS pr_off_net
           FROM final_rows fr
           LEFT JOIN player_minutes pm
             ON pm.player_id = fr.player_id
            AND pm.team_id = fr.team_id
            AND pm.game_year = fr.game_year
        )
SELECT team_name AS "Team",
    game_year AS "Year",
    firstname AS "First Name",
    lastname AS "Last Name",
    total_net_rtg AS "Net RTG Diff",
    offense_on_diff AS "Off ON Diff",
    defense_on_diff AS "Def ON Diff",
    offense_on_ppp AS "Off ON PPP",
    defense_on_ppp AS "Def ON PPP",
    on_net_rtg AS "On Net RTG",
    offense_off_ppp AS "Off OFF PPP",
    defense_off_ppp AS "Def OFF PPP",
    off_net_rtg AS "Off Net RTG",
    on_poss AS "ON Poss",
    off_poss AS "OFF Poss",
    minutes,
    pr_net,
    pr_off_on,
    pr_off_off,
    pr_def_on_inv,
    pr_def_off_inv,
    pr_off_on_d,
    pr_def_on_d,
    pr_def_on_d_inv,
    pr_on_net,
    pr_off_net,
    off_on_fg2_made, off_on_fg2_att, off_on_fg3_made, off_on_fg3_att,
    off_off_fg2_made, off_off_fg2_att, off_off_fg3_made, off_off_fg3_att,
    def_on_fg2_made, def_on_fg2_att, def_on_fg3_made, def_on_fg3_att,
    def_off_fg2_made, def_off_fg2_att, def_off_fg3_made, def_off_fg3_att,
    off_on_layup_made, off_on_layup_att, off_on_dunk_made, off_on_dunk_att, off_on_c3_made, off_on_c3_att, off_on_c3_known_att,
    off_off_layup_made, off_off_layup_att, off_off_dunk_made, off_off_dunk_att, off_off_c3_made, off_off_c3_att, off_off_c3_known_att,
    def_on_layup_made, def_on_layup_att, def_on_dunk_made, def_on_dunk_att, def_on_c3_made, def_on_c3_att, def_on_c3_known_att,
    def_off_layup_made, def_off_layup_att, def_off_dunk_made, def_off_dunk_att, def_off_c3_made, def_off_c3_att, def_off_c3_known_att,
   player_id,
   team_id
  FROM final_scored
  ORDER BY total_net_rtg DESC, team_name, lastname, firstname
;

CREATE INDEX idx_onoff_default_year ON basketball_test.onoff_default_mv ("Year");
CREATE INDEX idx_onoff_default_team_player ON basketball_test.onoff_default_mv (team_id, player_id);
CREATE UNIQUE INDEX idx_onoff_default_pk ON basketball_test.onoff_default_mv ("Year", team_id, player_id);
