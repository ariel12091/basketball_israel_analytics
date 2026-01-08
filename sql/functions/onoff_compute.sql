-- DROP FUNCTION basketball_test.onoff_compute(date, date, text, int4, int4, numeric, text);

CREATE OR REPLACE FUNCTION basketball_test.onoff_compute(p_start_date date, p_end_date date, p_team_ids text, p_min_all integer, p_min_on integer, p_min_net numeric, p_game_year text)
 RETURNS TABLE("Team" text, "First Name" text, "Last Name" text, "Net RTG Diff" numeric, "Off ON Diff" numeric, "Def ON Diff" numeric, "Off ON PPP" numeric, "Def ON PPP" numeric, "On Net RTG" numeric, "Off OFF PPP" numeric, "Def OFF PPP" numeric, "Off Net RTG" numeric, "ON Poss" numeric, "OFF Poss" numeric, pr_net numeric, pr_off_on numeric, pr_off_off numeric, pr_def_on_inv numeric, pr_def_off_inv numeric, pr_off_on_d numeric, pr_def_on_d numeric, pr_def_on_d_inv numeric, pr_on_net numeric, pr_off_net numeric, player_id integer, team_id integer)
 LANGUAGE sql
AS $function$
WITH
-- optional team filter from CSV
team_filter AS (
  SELECT NULLIF(p_team_ids,'') AS csv
),
teams AS (
  SELECT unnest(string_to_array(csv, ','))::int AS team_id
  FROM team_filter
  WHERE csv IS NOT NULL
),

-- schedule filtered by date and optional game_year
sched AS (
  SELECT DISTINCT
    s.game_id,
    s.game_date,
    s.game_year
  FROM basketball_test.schedule s
  WHERE s.game_date BETWEEN p_start_date AND p_end_date
    AND (p_game_year IS NULL OR s.game_year::text = p_game_year)
),

-- base player–team–lineup–on/off
base0 AS (
  SELECT DISTINCT
    ll.player_id,
    ll.team_id,
    ll.lineup_hash,
    COALESCE(ll.is_on_verdict, 0::numeric)::integer AS is_on_key
  FROM basketball_test.lineups_lookup ll
  WHERE NOT EXISTS (SELECT 1 FROM team_filter tf WHERE tf.csv IS NOT NULL)
     OR ll.team_id IN (SELECT team_id FROM teams)
),

-- attach possessions + scores + season
base AS (
  SELECT
    b0.player_id,
    b0.team_id,
    b0.is_on_key,
    b0.lineup_hash,
    d.game_id,
    d.type_lineup,
    d.team_score,
    CASE WHEN COALESCE(d.final_end_poss, false) THEN 1 ELSE 0 END AS final_end_flag,
    s.game_year
  FROM base0 b0
  JOIN basketball_test.df_pts_poss_lineups_longer_mv d USING (lineup_hash)
  JOIN sched s USING (game_id)
),

-- per player–team–ON/OFF–type–year totals + PPP
agg AS (
  SELECT
    b.player_id,
    b.team_id,
    b.is_on_key,
    b.type_lineup,
    b.game_year,
    SUM(b.team_score) AS total_pts,
    SUM(b.final_end_flag) AS total_poss,
    ROUND(
      SUM(b.team_score) / NULLIF(SUM(b.final_end_flag), 0)::numeric * 100::numeric,
      1
    ) AS ppp_calc
  FROM base b
  GROUP BY
    b.player_id,
    b.team_id,
    b.is_on_key,
    b.type_lineup,
    b.game_year
),

-- PPP percentile per (type_lineup, game_year)
ppp_rank_base AS (
  SELECT
    a.*,
    PERCENT_RANK() OVER (
      PARTITION BY a.type_lineup, a.game_year
      ORDER BY a.ppp_calc
    ) AS pr_ppp_raw
  FROM agg a
),
ppp_ranked AS (
  SELECT
    p.*,
    CASE
      WHEN p.type_lineup = 'defense' THEN 1 - p.pr_ppp_raw
      ELSE p.pr_ppp_raw
    END AS pr_ppp_better
  FROM ppp_rank_base p
),

-- attach names + team_name + game_year (like MV)
with_names AS (
  SELECT
    a.player_id,
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
  JOIN (
    SELECT DISTINCT
      fr.player_id,
      fr.team_id,
      fr.firstname,
      fr.lastname,
      fr.team_name,
      s.game_year
    FROM basketball_test.full_rosters fr
    JOIN basketball_test.schedule s ON fr.game_id = s.game_id
  ) r USING (player_id, team_id, game_year)
),

-- eligibility per player–team–year
elig AS (
  SELECT
    wn.player_id,
    wn.team_id,
    wn.game_year,
    MIN(wn.total_poss) AS min_poss_all,
    MAX(CASE WHEN wn.is_on_key = 1 THEN wn.total_poss ELSE 0 END) AS max_poss_on
  FROM with_names wn
  GROUP BY
    wn.player_id,
    wn.team_id,
    wn.game_year
),

-- apply min_all/min_on filters
filtered AS (
  SELECT
    wn.player_id,
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
  WHERE e.min_poss_all >= p_min_all
    AND e.max_poss_on  >= p_min_on
),

-- ON/OFF diff per type, per year
step1 AS (
  SELECT
    f.player_id,
    f.team_id,
    f.game_year,
    f.is_on_key,
    f.type_lineup,
    f.total_pts,
    f.total_poss,
    f.ppp_calc,
    f.pr_ppp_raw,
    f.pr_ppp_better,
    f.firstname,
    f.lastname,
    f.team_name,
    CASE
      WHEN f.type_lineup = 'offense' THEN 1
      WHEN f.type_lineup = 'defense' THEN 2
      ELSE 3
    END AS type_key,
    f.ppp_calc
      - LAG(f.ppp_calc) OVER (
          PARTITION BY f.player_id, f.team_id, f.type_lineup, f.game_year
          ORDER BY f.is_on_key
        ) AS net_rtg
  FROM filtered f
),

-- rank ON net_rtg within (type_lineup, game_year)
step1_on_rank AS (
  SELECT
    s1.player_id,
    s1.team_id,
    s1.type_lineup,
    s1.game_year,
    s1.is_on_key,
    PERCENT_RANK() OVER (
      PARTITION BY s1.type_lineup, s1.game_year
      ORDER BY s1.net_rtg
    ) AS pr_net_rtg_raw,
    CASE
      WHEN s1.type_lineup = 'defense' THEN
        1 - PERCENT_RANK() OVER (
              PARTITION BY s1.type_lineup, s1.game_year
              ORDER BY s1.net_rtg
            )
      ELSE
        PERCENT_RANK() OVER (
          PARTITION BY s1.type_lineup, s1.game_year
          ORDER BY s1.net_rtg
        )
    END AS pr_net_rtg_better
  FROM step1 s1
  WHERE s1.is_on_key = 1
    AND s1.net_rtg IS NOT NULL
),

step1_joined AS (
  SELECT
    s1.player_id,
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
  LEFT JOIN step1_on_rank r
    ON r.player_id   = s1.player_id
   AND r.team_id     = s1.team_id
   AND r.type_lineup = s1.type_lineup
   AND r.is_on_key   = s1.is_on_key
   AND r.game_year   = s1.game_year
),

-- total_net_rtg = offense_net_rtg - defense_net_rtg (per ON/OFF, per year)
step2 AS (
  SELECT
    s1j.player_id,
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
    ROUND(
      LAG(s1j.net_rtg) OVER (
        PARTITION BY s1j.player_id, s1j.team_id, s1j.is_on_key, s1j.game_year
        ORDER BY s1j.type_key
      ) - s1j.net_rtg,
      2
    ) AS total_net_rtg
  FROM step1_joined s1j
),

-- percentile of total_net_rtg per game_year
step2_rank AS (
  SELECT
    s2.player_id,
    s2.team_id,
    s2.type_lineup,
    s2.game_year,
    s2.is_on_key,
    PERCENT_RANK() OVER (
      PARTITION BY s2.game_year
      ORDER BY s2.total_net_rtg
    ) AS pr_total_net
  FROM step2 s2
  WHERE s2.total_net_rtg IS NOT NULL
),

step2_joined AS (
  SELECT
    s2.player_id,
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
  LEFT JOIN step2_rank r
    ON r.player_id   = s2.player_id
   AND r.team_id     = s2.team_id
   AND r.type_lineup = s2.type_lineup
   AND r.is_on_key   = s2.is_on_key
   AND r.game_year   = s2.game_year
),

-- collapse to one row per (player, team, year)
final_rows AS (
  SELECT
    s2j.player_id,
    s2j.team_id,
    s2j.game_year,
    s2j.team_name,
    s2j.firstname,
    s2j.lastname,

    MAX(CASE WHEN s2j.type_lineup = 'offense' AND s2j.is_on_key = 1
             THEN s2j.ppp_calc END) AS offense_on_ppp,
    MAX(CASE WHEN s2j.type_lineup = 'offense' AND s2j.is_on_key = 0
             THEN s2j.ppp_calc END) AS offense_off_ppp,
    MAX(CASE WHEN s2j.type_lineup = 'defense' AND s2j.is_on_key = 1
             THEN s2j.ppp_calc END) AS defense_on_ppp,
    MAX(CASE WHEN s2j.type_lineup = 'defense' AND s2j.is_on_key = 0
             THEN s2j.ppp_calc END) AS defense_off_ppp,

    MAX(CASE WHEN s2j.type_lineup = 'offense' AND s2j.is_on_key = 1
             THEN s2j.pr_ppp_better END) AS pr_off_on,
    MAX(CASE WHEN s2j.type_lineup = 'offense' AND s2j.is_on_key = 0
             THEN s2j.pr_ppp_better END) AS pr_off_off,
    MAX(CASE WHEN s2j.type_lineup = 'defense' AND s2j.is_on_key = 1
             THEN s2j.pr_ppp_better END) AS pr_def_on_inv,
    MAX(CASE WHEN s2j.type_lineup = 'defense' AND s2j.is_on_key = 0
             THEN s2j.pr_ppp_better END) AS pr_def_off_inv,

    MAX(CASE WHEN s2j.type_lineup = 'offense' AND s2j.is_on_key = 1
             THEN s2j.net_rtg END) AS offense_on_diff,
    MAX(CASE WHEN s2j.type_lineup = 'defense' AND s2j.is_on_key = 1
             THEN s2j.net_rtg END) AS defense_on_diff,

    MAX(CASE WHEN s2j.type_lineup = 'offense' AND s2j.is_on_key = 1
             THEN s2j.pr_net_rtg_better END) AS pr_off_on_d,
    MAX(CASE WHEN s2j.type_lineup = 'defense' AND s2j.is_on_key = 1
             THEN s2j.pr_net_rtg_raw END)    AS pr_def_on_d,
    MAX(CASE WHEN s2j.type_lineup = 'defense' AND s2j.is_on_key = 1
             THEN s2j.pr_net_rtg_better END) AS pr_def_on_d_inv,

    MAX(s2j.total_net_rtg) AS total_net_rtg,
    MAX(s2j.pr_total_net)  AS pr_net,

    MAX(CASE WHEN s2j.is_on_key = 1 THEN s2j.total_poss END) AS on_poss,
    MAX(CASE WHEN s2j.is_on_key = 0 THEN s2j.total_poss END) AS off_poss
  FROM step2_joined s2j
  GROUP BY
    s2j.player_id,
    s2j.team_id,
    s2j.game_year,
    s2j.team_name,
    s2j.firstname,
    s2j.lastname
),

final_scored AS (
  SELECT
    fr.player_id,
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
    fr.offense_on_ppp  - fr.defense_on_ppp  AS on_net_rtg,
    fr.offense_off_ppp - fr.defense_off_ppp AS off_net_rtg,
    PERCENT_RANK() OVER (
      PARTITION BY fr.game_year
      ORDER BY (fr.offense_on_ppp - fr.defense_on_ppp)
    ) AS pr_on_net,
    PERCENT_RANK() OVER (
      PARTITION BY fr.game_year
      ORDER BY (fr.offense_off_ppp - fr.defense_off_ppp)
    ) AS pr_off_net
  FROM final_rows fr
)
SELECT
  team_name   AS "Team",
  firstname   AS "First Name",
  lastname    AS "Last Name",
  total_net_rtg AS "Net RTG Diff",
  offense_on_diff AS "Off ON Diff",
  defense_on_diff AS "Def ON Diff",
  offense_on_ppp  AS "Off ON PPP",
  defense_on_ppp  AS "Def ON PPP",
  on_net_rtg      AS "On Net RTG",
  offense_off_ppp AS "Off OFF PPP",
  defense_off_ppp AS "Def OFF PPP",
  off_net_rtg     AS "Off Net RTG",
  on_poss         AS "ON Poss",
  off_poss        AS "OFF Poss",
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
  player_id,
  team_id
FROM final_scored
WHERE total_net_rtg >= p_min_net
ORDER BY "Net RTG Diff" DESC, "Team", "Last Name", "First Name";
$function$
;
