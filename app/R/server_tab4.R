# server_tab4.R - Tab 4: Game Logs server logic

GL_SUMMARY_FILTERABLE_COLS <- c(
  "GN" = "gn",
  "Min" = "minutes",
  "Off PPP" = "off_ppp",
  "Def PPP" = "def_ppp",
  "Net" = "net_rtg",
  "Off Shot" = "Off Shot",
  "Def Shot" = "Def Shot",
  "Off Poss" = "off_poss",
  "Def Poss" = "def_poss"
)

GL_FF_FILTERABLE_COLS <- c(
  "GN" = "gn",
  "Min" = "minutes",
  "Off PPP" = "off_ppp",
  "Off eFG%" = "off_efg_pct",
  "Off OREB%" = "off_oreb_pct",
  "Off TOV%" = "off_tov_pct",
  "Off FTR" = "off_ftr_pct",
  "Def PPP" = "def_ppp",
  "Def eFG%" = "def_efg_pct",
  "Def OREB%" = "def_oreb_pct",
  "Def TOV%" = "def_tov_pct",
  "Def FTR" = "def_ftr_pct",
  "Off Poss" = "off_poss",
  "Def Poss" = "def_poss"
)

gl_pr_col_name <- function(metric_name) {
  paste0("pr_", gsub("[^A-Za-z0-9]+", "_", metric_name))
}

gl_filter_starters <- function(data, starters_bounds) {
  if (is.null(data) || !nrow(data) || is.null(starters_bounds)) return(data)

  data %>%
    filter(
      (type_lineup == "offense" &
         (is.na(starters_bounds$off_min) | num_starters >= starters_bounds$off_min) &
         (is.na(starters_bounds$off_max) | num_starters <= starters_bounds$off_max)) |
      (type_lineup == "defense" &
         (is.na(starters_bounds$def_min) | num_starters >= starters_bounds$def_min) &
         (is.na(starters_bounds$def_max) | num_starters <= starters_bounds$def_max))
    )
}

gl_join_schedule_info <- function(metrics_df, schedule_df) {
  if (is.null(metrics_df) || !nrow(metrics_df) || is.null(schedule_df) || !nrow(schedule_df)) return(NULL)

  sched_info <- schedule_df %>%
    select(game_id, team_id, team_name, gn, game_type, game_date, opp_team_name, team_score, opp_score, has_won) %>%
    mutate(
      game_type_label = dplyr::coalesce(unname(GAME_TYPE_LABELS[as.character(game_type)]), as.character(game_type)),
      result = ifelse(has_won, "W", "L"),
      score_display = paste0(team_score, "-", opp_score)
    )

  metrics_df %>%
    inner_join(sched_info, by = c("game_id", "team_id")) %>%
    arrange(desc(game_date), desc(gn), game_id, team_name)
}

gl_build_summary_metrics <- function(lineup_totals_df, schedule_df, starters_bounds = NULL, apply_starters = TRUE) {
  if (is.null(schedule_df) || !nrow(schedule_df) || is.null(lineup_totals_df) || !nrow(lineup_totals_df)) return(NULL)

  sched_pairs <- schedule_df %>% select(game_id, team_id) %>% distinct()
  lt <- lineup_totals_df %>% inner_join(sched_pairs, by = c("game_id", "team_id"))
  if (isTRUE(apply_starters)) {
    lt <- gl_filter_starters(lt, starters_bounds)
  }
  if (!nrow(lt)) return(NULL)
  if (!"minutes" %in% names(lt)) lt$minutes <- NA_real_

  game_stats <- lt %>%
    group_by(game_id, team_id, type_lineup) %>%
    summarise(
      poss = sum(total_poss, na.rm = TRUE),
      pts = sum(total_pts, na.rm = TRUE),
      fg2m = sum(fg2_made, na.rm = TRUE),
      fg2a = sum(fg2_att, na.rm = TRUE),
      fg3m = sum(fg3_made, na.rm = TRUE),
      fg3a = sum(fg3_att, na.rm = TRUE),
      minutes = sum(minutes, na.rm = TRUE),
      .groups = "drop"
    )

  off <- game_stats %>%
    filter(type_lineup == "offense") %>%
    rename(
      off_poss = poss,
      off_pts = pts,
      off_fg2m = fg2m,
      off_fg2a = fg2a,
      off_fg3m = fg3m,
      off_fg3a = fg3a
    ) %>%
    select(-type_lineup)
  def <- game_stats %>%
    filter(type_lineup == "defense") %>%
    rename(
      def_poss = poss,
      def_pts = pts,
      def_fg2m = fg2m,
      def_fg2a = fg2a,
      def_fg3m = fg3m,
      def_fg3a = fg3a
    ) %>%
    select(game_id, team_id, def_poss, def_pts, def_fg2m, def_fg2a, def_fg3m, def_fg3a)

  off %>%
    left_join(def, by = c("game_id", "team_id")) %>%
    mutate(
      off_ppp = ifelse(off_poss > 0, round(off_pts / off_poss * 100, 1), NA_real_),
      def_ppp = ifelse(def_poss > 0, round(def_pts / def_poss * 100, 1), NA_real_),
      net_rtg = round(coalesce(off_ppp, 0) - coalesce(def_ppp, 0), 1),
      minutes = round(coalesce(minutes, 0), 1)
    )
}

gl_build_ff_metrics <- function(lineup_ff_df, schedule_df, starters_bounds = NULL, apply_starters = TRUE) {
  if (is.null(schedule_df) || !nrow(schedule_df) || is.null(lineup_ff_df) || !nrow(lineup_ff_df)) return(NULL)

  sched_pairs <- schedule_df %>% select(game_id, team_id) %>% distinct()
  ff <- lineup_ff_df %>% inner_join(sched_pairs, by = c("game_id", "team_id"))
  if (isTRUE(apply_starters)) {
    ff <- gl_filter_starters(ff, starters_bounds)
  }
  if (!nrow(ff)) return(NULL)
  if (!"minutes" %in% names(ff)) ff$minutes <- NA_real_

  game_ff <- ff %>%
    group_by(game_id, team_id, type_lineup) %>%
    summarise(
      total_points = sum(total_points, na.rm = TRUE),
      total_poss = sum(total_poss, na.rm = TRUE),
      ts_poss_count = sum(ts_poss_count, na.rm = TRUE),
      oreb_count = sum(oreb_count, na.rm = TRUE),
      oreb_opportunities = sum(oreb_opportunities, na.rm = TRUE),
      tov_count = sum(tov_count, na.rm = TRUE),
      total_ft_attempts = sum(total_ft_attempts, na.rm = TRUE),
      total_fga = sum(total_fga, na.rm = TRUE),
      total_fgm = sum(total_fgm, na.rm = TRUE),
      total_fg3_made = sum(total_fg3_made, na.rm = TRUE),
      minutes = sum(minutes, na.rm = TRUE),
      .groups = "drop"
    )

  off <- game_ff %>%
    filter(type_lineup == "offense") %>%
    rename(
      off_pts = total_points,
      off_poss = total_poss,
      off_ts_poss = ts_poss_count,
      off_oreb = oreb_count,
      off_oreb_opp = oreb_opportunities,
      off_tov = tov_count,
      off_fta = total_ft_attempts,
      off_fga = total_fga,
      off_fgm = total_fgm,
      off_fg3m = total_fg3_made,
      off_minutes = minutes
    ) %>%
    select(-type_lineup)
  def <- game_ff %>%
    filter(type_lineup == "defense") %>%
    rename(
      def_pts = total_points,
      def_poss = total_poss,
      def_ts_poss = ts_poss_count,
      def_oreb = oreb_count,
      def_oreb_opp = oreb_opportunities,
      def_tov = tov_count,
      def_fta = total_ft_attempts,
      def_fga = total_fga,
      def_fgm = total_fgm,
      def_fg3m = total_fg3_made,
      def_minutes = minutes
    ) %>%
    select(game_id, team_id, def_pts, def_poss, def_ts_poss, def_oreb, def_oreb_opp, def_tov, def_fta, def_fga, def_fgm, def_fg3m)

  off %>%
    left_join(def, by = c("game_id", "team_id")) %>%
    mutate(
      off_ppp = ifelse(off_poss > 0, round(off_pts / off_poss * 100, 1), NA_real_),
      def_ppp = ifelse(def_poss > 0, round(def_pts / def_poss * 100, 1), NA_real_),
      off_ts_pct = ifelse(off_ts_poss > 0, round(off_pts / (2 * off_ts_poss) * 100, 1), NA_real_),
      off_efg_pct = ifelse(off_fga > 0, round((off_fgm + 0.5 * off_fg3m) / off_fga * 100, 1), NA_real_),
      off_oreb_pct = ifelse(off_oreb_opp > 0, round(off_oreb / off_oreb_opp * 100, 1), NA_real_),
      off_tov_pct = ifelse(off_poss > 0, round(off_tov / off_poss * 100, 1), NA_real_),
      off_ftr_pct = ifelse(off_fga > 0, round(off_fta / off_fga * 100, 1), NA_real_),
      def_ts_pct = ifelse(def_ts_poss > 0, round(def_pts / (2 * def_ts_poss) * 100, 1), NA_real_),
      def_efg_pct = ifelse(def_fga > 0, round((def_fgm + 0.5 * def_fg3m) / def_fga * 100, 1), NA_real_),
      def_oreb_pct = ifelse(def_oreb_opp > 0, round(def_oreb / def_oreb_opp * 100, 1), NA_real_),
      def_tov_pct = ifelse(def_poss > 0, round(def_tov / def_poss * 100, 1), NA_real_),
      def_ftr_pct = ifelse(def_fga > 0, round(def_fta / def_fga * 100, 1), NA_real_),
      minutes = round(coalesce(off_minutes, 0), 1)
    )
}

gl_attach_percentiles <- function(display_df, baseline_df, metric_names) {
  if (is.null(display_df) || !nrow(display_df) || is.null(baseline_df) || !nrow(baseline_df)) return(display_df)

  metric_names <- intersect(metric_names, intersect(names(display_df), names(baseline_df)))
  if (!length(metric_names)) return(display_df)

  rank_df <- baseline_df %>% select(game_id, team_id)
  pr_cols <- character(0)
  for (metric_name in metric_names) {
    pr_col <- gl_pr_col_name(metric_name)
    rank_df[[pr_col]] <- dplyr::percent_rank(suppressWarnings(as.numeric(baseline_df[[metric_name]])))
    pr_cols <- c(pr_cols, pr_col)
  }

  display_df %>% left_join(rank_df %>% select(game_id, team_id, all_of(pr_cols)), by = c("game_id", "team_id"))
}

gl_fetch_box_score <- function(pool, game_id, game_year) {
  db_get_query(
    pool,
    "WITH game_teams AS (
       SELECT DISTINCT
         fs.game_id,
         fs.team_id,
         fs.team_name,
         fs.opp_team_name,
         fs.team_score,
         fs.opp_score,
         fs.has_won,
         fs.is_home
       FROM basketball_test.final_schedule_mv fs
       WHERE fs.game_id = $1::int4
         AND fs.game_year = $2::int4
     ),
     acts AS (
       SELECT
         d.id,
         d.game_id,
         d.team_id,
         d.lineup_hash,
         d.segment_id,
         d.end_game_seconds_remaining,
         d.type,
         d.parameters_type,
         d.parameters_made,
         d.parameters_points,
         d.player_id,
         d.type_lineup,
         d.final_end_poss,
         d.parent_action_id,
         d.team_score
       FROM basketball_test.df_pts_poss_lineups_longer_mv d
       JOIN game_teams gt
         ON gt.game_id = d.game_id
        AND gt.team_id = d.team_id
     ),
     complex_flags AS (
       SELECT DISTINCT ON (a.id, a.game_id)
         a.id AS main_id,
         a.game_id,
         t2.type AS parent_type,
         t2.parameters_type AS parent_param
       FROM acts a
       JOIN basketball_test.df_pts_poss_lineups_longer_mv t2
         ON t2.id = a.parent_action_id
        AND t2.game_id = a.game_id
        AND t2.type = 'foul'::text
       WHERE a.parent_action_id IS NOT NULL
       ORDER BY a.id, a.game_id
     ),
     actions_enriched AS (
       SELECT
         a.*,
         cf.parent_type,
         cf.parent_param
       FROM acts a
       LEFT JOIN complex_flags cf
         ON cf.main_id = a.id
        AND cf.game_id = a.game_id
     ),
     lineup_map AS (
       SELECT DISTINCT
         ll.game_id,
         ll.team_id,
         ll.lineup_hash,
         ll.player_id
       FROM basketball_test.lineups_lookup ll
       JOIN game_teams gt
         ON gt.game_id = ll.game_id
        AND gt.team_id = ll.team_id
       WHERE COALESCE(ll.is_on_verdict, 0)::int = 1
     ),
     poss_end AS (
       SELECT DISTINCT
         a.game_id,
         a.team_id,
         a.lineup_hash,
         a.id AS poss_end_id
       FROM acts a
       WHERE a.type_lineup = 'offense'
         AND a.final_end_poss
         AND a.id IS NOT NULL
         AND a.lineup_hash IS NOT NULL
     ),
     player_usage AS (
       SELECT
         lm.player_id,
         pe.team_id,
         COUNT(DISTINCT (pe.game_id, pe.team_id, pe.poss_end_id))::int AS poss_on_floor
       FROM poss_end pe
       JOIN lineup_map lm
         ON lm.game_id = pe.game_id
        AND lm.team_id = pe.team_id
        AND lm.lineup_hash = pe.lineup_hash
       GROUP BY lm.player_id, pe.team_id
     ),
     team_possession_totals AS (
       SELECT
         pe.team_id,
         COUNT(DISTINCT (pe.game_id, pe.team_id, pe.poss_end_id))::numeric AS team_poss
       FROM poss_end pe
       GROUP BY pe.team_id
     ),
     seg_times AS (
       SELECT
         a.game_id,
         a.team_id,
         a.lineup_hash,
         a.segment_id,
         MAX(a.end_game_seconds_remaining) - MIN(a.end_game_seconds_remaining) AS seg_seconds
       FROM acts a
       WHERE a.lineup_hash IS NOT NULL
         AND a.segment_id IS NOT NULL
         AND a.end_game_seconds_remaining IS NOT NULL
       GROUP BY a.game_id, a.team_id, a.lineup_hash, a.segment_id
     ),
     player_minutes AS (
       SELECT
         lm.player_id,
         st.team_id,
         ROUND(SUM(COALESCE(st.seg_seconds, 0))::numeric / 60.0, 1) AS minutes
       FROM seg_times st
       JOIN lineup_map lm
         ON lm.game_id = st.game_id
        AND lm.team_id = st.team_id
        AND lm.lineup_hash = st.lineup_hash
       GROUP BY lm.player_id, st.team_id
     ),
     stats AS (
       SELECT
         a.player_id,
         a.team_id,
         (
           SUM(CASE WHEN a.type = 'shot' AND a.parameters_made = 'made' AND a.type_lineup = 'offense'
                    THEN COALESCE(a.parameters_points, 0) ELSE 0 END)
           + SUM(CASE WHEN a.type = 'freeThrow' AND a.parameters_made = 'made' AND a.type_lineup = 'offense'
                      THEN 1 ELSE 0 END)
         )::int AS pts,
         SUM(CASE WHEN a.type = 'rebound' AND a.type_lineup = 'offense' AND a.parameters_type = 'offensive' THEN 1 ELSE 0 END)::int AS oreb,
         SUM(CASE WHEN a.type = 'rebound' AND a.type_lineup = 'defense' AND a.parameters_type = 'defensive' THEN 1 ELSE 0 END)::int AS dreb,
         SUM(CASE WHEN a.type = 'assist' AND a.type_lineup = 'offense' THEN 1 ELSE 0 END)::int AS ast,
         SUM(CASE WHEN a.type = 'steal' AND a.type_lineup = 'defense' THEN 1 ELSE 0 END)::int AS stl,
         SUM(CASE WHEN a.type = 'block' AND a.type_lineup = 'defense' THEN 1 ELSE 0 END)::int AS blk,
         SUM(CASE WHEN a.type = 'turnover' AND a.type_lineup = 'offense' THEN 1 ELSE 0 END)::int AS tov,
         SUM(CASE WHEN a.type = 'shot' AND a.parameters_made = 'made' AND a.type_lineup = 'offense' THEN 1 ELSE 0 END)::int AS fgm,
         SUM(CASE WHEN a.type = 'shot' AND a.type_lineup = 'offense' THEN 1 ELSE 0 END)::int AS fga,
         SUM(CASE WHEN a.type = 'shot' AND a.parameters_made = 'made' AND a.parameters_points = 3 AND a.type_lineup = 'offense' THEN 1 ELSE 0 END)::int AS \"3pm\",
         SUM(CASE WHEN a.type = 'shot' AND a.parameters_points = 3 AND a.type_lineup = 'offense' THEN 1 ELSE 0 END)::int AS \"3pa\",
         SUM(CASE WHEN a.type = 'freeThrow' AND a.parameters_made = 'made' AND a.type_lineup = 'offense' THEN 1 ELSE 0 END)::int AS ftm,
         SUM(CASE WHEN a.type = 'freeThrow' AND a.type_lineup = 'offense' THEN 1 ELSE 0 END)::int AS fta,
         (
           COUNT(CASE WHEN a.type = 'shot' AND a.type_lineup = 'offense' THEN 1 END)
           + COUNT(DISTINCT CASE
               WHEN a.type = 'freeThrow'
                 AND a.type_lineup = 'offense'
                 AND a.parent_type = 'foul'
                 AND a.parent_param = 'personal'
               THEN a.parent_action_id
             END)
         )::int AS ts_poss_count
       FROM actions_enriched a
       WHERE a.player_id IS NOT NULL
         AND a.player_id > 0
       GROUP BY a.player_id, a.team_id
     ),
     player_plus_minus AS (
       SELECT
         lm.player_id,
         a.team_id,
         SUM(
           CASE
             WHEN a.type_lineup = 'offense' THEN COALESCE(a.team_score, 0)
             WHEN a.type_lineup = 'defense' THEN -COALESCE(a.team_score, 0)
             ELSE 0
           END
         )::int AS plus_minus
       FROM (
         SELECT DISTINCT game_id, team_id, lineup_hash, id, type_lineup, team_score
         FROM acts
         WHERE COALESCE(team_score, 0) <> 0
           AND lineup_hash IS NOT NULL
       ) a
       JOIN lineup_map lm
         ON lm.game_id = a.game_id
        AND lm.team_id = a.team_id
        AND lm.lineup_hash = a.lineup_hash
       GROUP BY lm.player_id, a.team_id
     ),
     names_df AS (
       SELECT
         fr.player_id,
         fr.team_id,
         MIN(btrim(fr.team_name)) AS team_name,
         MIN(btrim(CONCAT_WS(' ', fr.firstname, fr.lastname))) AS player_name,
         BOOL_OR(COALESCE(fr.starter, FALSE)) AS starter
       FROM basketball_test.full_rosters fr
       JOIN game_teams gt
         ON gt.game_id = fr.game_id
        AND gt.team_id = fr.team_id
       GROUP BY fr.player_id, fr.team_id
     ),
     player_base AS (
       SELECT player_id, team_id FROM names_df
       UNION
       SELECT player_id, team_id FROM stats
       UNION
       SELECT player_id, team_id FROM player_usage
       UNION
       SELECT player_id, team_id FROM player_minutes
     )
     SELECT
       gt.game_id,
       pb.team_id,
       COALESCE(nd.team_name, gt.team_name) AS team_name,
       gt.opp_team_name,
       gt.team_score,
       gt.opp_score,
       CASE WHEN gt.has_won THEN 'W' ELSE 'L' END AS result,
       pb.player_id,
       nd.player_name,
       COALESCE(nd.starter, FALSE) AS starter,
       COALESCE(pu.poss_on_floor, 0)::int AS poss_on_floor,
       COALESCE(tpt.team_poss, 0)::int AS team_poss,
       COALESCE(pm.minutes, 0)::numeric AS minutes,
       COALESCE(s.pts, 0)::int AS pts,
       (COALESCE(s.oreb, 0) + COALESCE(s.dreb, 0))::int AS reb,
       COALESCE(s.oreb, 0)::int AS oreb,
       COALESCE(s.dreb, 0)::int AS dreb,
       COALESCE(s.ast, 0)::int AS ast,
       COALESCE(s.stl, 0)::int AS stl,
       COALESCE(s.blk, 0)::int AS blk,
       COALESCE(s.tov, 0)::int AS tov,
       COALESCE(s.fgm, 0)::int AS fgm,
       COALESCE(s.fga, 0)::int AS fga,
       (COALESCE(s.fgm, 0) - COALESCE(s.\"3pm\", 0))::int AS \"2pm\",
       (COALESCE(s.fga, 0) - COALESCE(s.\"3pa\", 0))::int AS \"2pa\",
       COALESCE(s.\"3pm\", 0)::int AS \"3pm\",
       COALESCE(s.\"3pa\", 0)::int AS \"3pa\",
       COALESCE(s.ftm, 0)::int AS ftm,
       COALESCE(s.fta, 0)::int AS fta,
       CASE WHEN COALESCE(s.fga, 0) > 0 THEN ROUND((COALESCE(s.fgm, 0)::numeric / s.fga::numeric) * 100, 1) ELSE NULL END AS fg_pct,
       CASE WHEN (COALESCE(s.fga, 0) - COALESCE(s.\"3pa\", 0)) > 0
         THEN ROUND(((COALESCE(s.fgm, 0) - COALESCE(s.\"3pm\", 0))::numeric / (s.fga - COALESCE(s.\"3pa\", 0))::numeric) * 100, 1)
         ELSE NULL
       END AS two_pct,
       CASE WHEN COALESCE(s.\"3pa\", 0) > 0 THEN ROUND((COALESCE(s.\"3pm\", 0)::numeric / s.\"3pa\"::numeric) * 100, 1) ELSE NULL END AS tp_pct,
       CASE WHEN COALESCE(s.fta, 0) > 0 THEN ROUND((COALESCE(s.ftm, 0)::numeric / s.fta::numeric) * 100, 1) ELSE NULL END AS ft_pct,
       CASE WHEN COALESCE(s.fga, 0) > 0 THEN ROUND(((COALESCE(s.fgm, 0)::numeric + 0.5 * COALESCE(s.\"3pm\", 0)::numeric) / s.fga::numeric) * 100, 1) ELSE NULL END AS efg,
       CASE WHEN (COALESCE(s.fga, 0) + 0.44 * COALESCE(s.fta, 0)) > 0
         THEN ROUND((COALESCE(s.pts, 0)::numeric / (2.0 * (COALESCE(s.fga, 0)::numeric + 0.44 * COALESCE(s.fta, 0)::numeric))) * 100, 1)
         ELSE NULL
       END AS ts,
       CASE
         WHEN (COALESCE(s.ts_poss_count, 0) + COALESCE(s.tov, 0) + 0.33 * COALESCE(s.ast, 0)) > 0
          AND COALESCE(pu.poss_on_floor, 0) > 0
         THEN ROUND(
           100.0 * (COALESCE(s.ts_poss_count, 0) + COALESCE(s.tov, 0) + 0.33 * COALESCE(s.ast, 0))::numeric
           / NULLIF(pu.poss_on_floor::numeric, 0),
           1
         )
         ELSE NULL
       END AS usg_pct,
       COALESCE(ppm.plus_minus, 0)::int AS plus_minus
     FROM player_base pb
     JOIN game_teams gt
       ON gt.team_id = pb.team_id
     LEFT JOIN names_df nd
       ON nd.player_id = pb.player_id
      AND nd.team_id = pb.team_id
     LEFT JOIN stats s
       ON s.player_id = pb.player_id
      AND s.team_id = pb.team_id
     LEFT JOIN player_usage pu
       ON pu.player_id = pb.player_id
      AND pu.team_id = pb.team_id
     LEFT JOIN team_possession_totals tpt
       ON tpt.team_id = pb.team_id
     LEFT JOIN player_minutes pm
       ON pm.player_id = pb.player_id
      AND pm.team_id = pb.team_id
     LEFT JOIN player_plus_minus ppm
       ON ppm.player_id = pb.player_id
      AND ppm.team_id = pb.team_id
     WHERE COALESCE(nd.player_name, '') <> ''
       AND (
         COALESCE(pu.poss_on_floor, 0) > 0
         OR COALESCE(pm.minutes, 0) > 0
         OR COALESCE(s.pts, 0) > 0
         OR COALESCE(s.oreb, 0) > 0
         OR COALESCE(s.dreb, 0) > 0
         OR COALESCE(s.ast, 0) > 0
         OR COALESCE(s.stl, 0) > 0
         OR COALESCE(s.blk, 0) > 0
         OR COALESCE(s.tov, 0) > 0
       )
     ORDER BY gt.is_home DESC, COALESCE(nd.starter, FALSE) DESC, COALESCE(pm.minutes, 0) DESC, COALESCE(s.pts, 0) DESC, nd.player_name",
    params = list(as.integer(game_id), as.integer(game_year))
  )
}

gl_add_box_score_totals <- function(box_df, team_order_ids) {
  if (is.null(box_df) || !nrow(box_df)) return(box_df)

  ordered_df <- box_df %>%
    mutate(
      .team_order = match(as.integer(team_id), team_order_ids),
      .team_order = ifelse(is.na(.team_order), 99L, .team_order),
      .row_order = 1L,
      .starter_sort = ifelse(coalesce(starter, FALSE), 1L, 0L)
    )

  totals <- ordered_df %>%
    group_by(.team_order, team_id, team_name, team_score, opp_score, result) %>%
    summarise(
      player_id = NA_integer_,
      player_name = "TOTAL",
      starter = FALSE,
      poss_on_floor = max(team_poss, na.rm = TRUE),
      team_poss = max(team_poss, na.rm = TRUE),
      minutes = sum(minutes, na.rm = TRUE),
      pts = sum(pts, na.rm = TRUE),
      reb = sum(reb, na.rm = TRUE),
      oreb = sum(oreb, na.rm = TRUE),
      dreb = sum(dreb, na.rm = TRUE),
      ast = sum(ast, na.rm = TRUE),
      stl = sum(stl, na.rm = TRUE),
      blk = sum(blk, na.rm = TRUE),
      tov = sum(tov, na.rm = TRUE),
      fgm = sum(fgm, na.rm = TRUE),
      fga = sum(fga, na.rm = TRUE),
      `2pm` = sum(`2pm`, na.rm = TRUE),
      `2pa` = sum(`2pa`, na.rm = TRUE),
      `3pm` = sum(`3pm`, na.rm = TRUE),
      `3pa` = sum(`3pa`, na.rm = TRUE),
      ftm = sum(ftm, na.rm = TRUE),
      fta = sum(fta, na.rm = TRUE),
      plus_minus = max(team_score - opp_score, na.rm = TRUE),
      .groups = "drop"
    ) %>%
    mutate(
      game_id = unique(ordered_df$game_id)[1],
      opp_team_name = NA_character_,
      fg_pct = ifelse(fga > 0, round(fgm / fga * 100, 1), NA_real_),
      two_pct = ifelse(`2pa` > 0, round(`2pm` / `2pa` * 100, 1), NA_real_),
      tp_pct = ifelse(`3pa` > 0, round(`3pm` / `3pa` * 100, 1), NA_real_),
      ft_pct = ifelse(fta > 0, round(ftm / fta * 100, 1), NA_real_),
      efg = ifelse(fga > 0, round((fgm + 0.5 * `3pm`) / fga * 100, 1), NA_real_),
      ts = ifelse((fga + 0.44 * fta) > 0, round(pts / (2 * (fga + 0.44 * fta)) * 100, 1), NA_real_),
      usg_pct = NA_real_,
      .row_order = 2L,
      .starter_sort = 0L
    )

  bind_rows(ordered_df, totals) %>%
    arrange(.team_order, .row_order, desc(.starter_sort), desc(minutes), desc(pts), player_name)
}

gl_score_link <- function(game_id, team_id, score_display) {
  gid <- as.character(game_id)
  tid <- as.character(team_id)
  label <- as.character(score_display)
  gid[is.na(gid)] <- ""
  tid[is.na(tid)] <- ""
  label[is.na(label)] <- ""

  sprintf(
    '<a href="#" class="gl-game-link" data-game-id="%s" data-team-id="%s" title="Open box score">%s</a>',
    htmltools::htmlEscape(gid),
    htmltools::htmlEscape(tid),
    htmltools::htmlEscape(label)
  )
}

gl_game_link_callback <- DT::JS(
  "table.on('click', 'a.gl-game-link', function(e) {
     e.preventDefault();
     if (!window.Shiny || typeof window.Shiny.setInputValue !== 'function') return;
     var gameId = parseInt(this.getAttribute('data-game-id'), 10);
     var teamId = parseInt(this.getAttribute('data-team-id'), 10);
     if (isNaN(gameId)) return;
     window.Shiny.setInputValue('gl_game_click', {
       game_id: gameId,
       team_id: isNaN(teamId) ? null : teamId,
       ts: Date.now()
     }, { priority: 'event' });
   });"
)

GL_BOX_SCORE_HEAT_GOOD <- c(
  "PTS", "REB", "OREB", "DREB", "AST", "STL", "BLK",
  "FGM", "FGA", "FG%", "2PM", "2PA", "2P%", "3PM", "3PA", "3P%",
  "FTM", "FTA", "FT%", "+/-", "TS%", "USG%"
)

gl_box_score_pr_col <- function(col_name) {
  paste0("pr_", gsub("[^A-Za-z0-9]+", "_", col_name))
}

gl_add_box_score_percentiles <- function(display_df) {
  if (is.null(display_df) || !nrow(display_df)) return(display_df)

  eligible <- display_df$Player != "TOTAL" &
    !is.na(display_df$Player) &
    coalesce(suppressWarnings(as.numeric(display_df$Poss)), 0) > 0
  eligible[is.na(eligible)] <- FALSE

  add_pr <- function(data, col_name) {
    if (!(col_name %in% names(data))) return(data)
    vals <- suppressWarnings(as.numeric(data[[col_name]]))
    vals[!eligible] <- NA_real_
    data[[gl_box_score_pr_col(col_name)]] <- dplyr::percent_rank(vals)
    data
  }

  for (col_name in GL_BOX_SCORE_HEAT_GOOD) {
    display_df <- add_pr(display_df, col_name)
  }
  display_df <- add_pr(display_df, "TOV")
  display_df
}

server_tab4 <- function(input, output, session, shared) {

  gl_ref <- reactiveValues(teams = NULL)
  gl_stat_filter_state <- make_stat_filter_state()
  gl_stat_filter_cols <- reactive({
    if (identical(input$gl_view_mode, "Four Factors")) GL_FF_FILTERABLE_COLS else GL_SUMMARY_FILTERABLE_COLS
  })

  setup_stat_filter_handlers("gl", input, session, gl_stat_filter_cols, gl_stat_filter_state)

  # --- Team list for the season ---
  observeEvent(list(input$main_tabs, input$game_year), ignoreInit = TRUE, {
    if (!identical(input$main_tabs, "game_logs")) return(NULL)
    gy_int <- as.integer(input$game_year)
    teams_gl <- cached_ref_query(
      key = sprintf("gl_teams_%d", gy_int),
      query_fun = function() {
        db_get_query(
          pg_pool,
          "SELECT DISTINCT team_id, MIN(team_name) AS team_name
           FROM basketball_test.full_rosters
           WHERE game_year = $1
           GROUP BY team_id ORDER BY MIN(team_name)",
          params = list(gy_int)
        )
      }
    )
    gl_ref$teams <- teams_gl
    pending_team <- shared$pending_gl_team()
    if (!is.null(pending_team) && nzchar(pending_team)) {
      shared$pending_gl_team(NULL)
      update_single_team_selectize(session, "gl_team", teams_gl, selected = pending_team)
    } else {
      update_single_team_selectize(session, "gl_team", teams_gl, selected = "")
    }
    updateSelectizeInput(session, "gl_opponents", choices = teams_gl$team_name,
                         selected = character(0), server = TRUE)

    gn_df <- cached_ref_query(
      key = sprintf("gl_gn_%d", gy_int),
      query_fun = function() {
        db_get_query(
          pg_pool,
          "SELECT DISTINCT gn FROM basketball_test.final_schedule_mv WHERE game_year = $1 ORDER BY gn",
          params = list(gy_int)
        )
      }
    )
    gn_vals <- if (nrow(gn_df)) as.integer(gn_df$gn) else integer(0)
    update_gn_last_n_choices(session, "gl", gn_vals)
  })

  setup_gn_last_n_sync(session, input, "gl")

  observeEvent(input$game_year, {
    b <- shared$season_date_bounds(input$game_year)
    updateDateRangeInput(session, "gl_dates", start = b$start, end = b$end, min = b$start, max = b$end)
  }, ignoreInit = FALSE)

  gl_gn_params <- reactive({
    resolve_gn_last_n_params(input, "gl")
  }) %>% debounce(150)

  # --- Reset ---
  observeEvent(input$gl_reset, {
    updateRadioButtons(session, "gl_view_mode", selected = "Summary")
    b <- shared$season_date_bounds(input$game_year %||% DEFAULT_GAME_YEAR)
    updateDateRangeInput(session, "gl_dates", start = b$start, end = b$end, min = b$start, max = b$end)
    if (!is.null(gl_ref$teams)) {
      update_single_team_selectize(session, "gl_team", gl_ref$teams, selected = "")
    } else {
      updateSelectizeInput(session, "gl_team", selected = "", server = TRUE)
    }
    updateSelectizeInput(session, "gl_game_type", selected = character(0))
    updateSelectizeInput(session, "gl_opponents", selected = character(0))
    updateSelectInput(session, "gl_home_away", selected = "")
    updateSelectInput(session, "gl_outcome", selected = "")
    reset_starters_inputs(session, "gl")
    reset_gn_last_n_inputs(session, "gl")
    reset_stat_filters(gl_stat_filter_state)
  })

  observeEvent(input$gl_view_mode, {
    reset_stat_filters(gl_stat_filter_state)
  }, ignoreInit = TRUE)

  # --- Schedule cache per season ---
  gl_schedule <- reactive({
    req(identical(input$main_tabs, "game_logs"))
    gy_int <- as.integer(input$game_year)
    req(gy_int)
    db_get_query(pg_pool,
      "SELECT * FROM basketball_test.final_schedule_mv WHERE game_year = $1",
      params = list(gy_int))
  }) %>% bindEvent(input$game_year, input$main_tabs)

  # --- Filtered schedule rows (team is optional) ---
  gl_filtered_schedule <- reactive({
    sched <- gl_schedule()
    req(nrow(sched) > 0)

    df <- sched

    # Team filter (optional)
    team_id_str <- input$gl_team
    if (!is.null(team_id_str) && nzchar(team_id_str)) {
      team_id_val <- as.integer(team_id_str)
      df <- df %>% filter(team_id == !!team_id_val)
    }

    # Date filter
    start_d <- input$gl_dates[1]
    end_d <- input$gl_dates[2]
    if (!is.null(start_d) && !is.na(start_d)) df <- df %>% filter(game_date >= !!as.Date(start_d))
    if (!is.null(end_d) && !is.na(end_d)) df <- df %>% filter(game_date <= !!as.Date(end_d))

    # Game type filter
    gt <- input$gl_game_type
    if (!is.null(gt) && length(gt) > 0 && any(nzchar(gt))) {
      gt_vals <- as.integer(gt[nzchar(gt)])
      df <- df %>% filter(game_type %in% !!gt_vals)
    }

    # Opponent filter
    opp_names <- input$gl_opponents
    if (!is.null(opp_names) && length(opp_names) > 0) {
      df <- df %>% filter(opp_team_name %in% !!opp_names)
    }

    # Home/Away filter
    ha <- input$gl_home_away
    if (!is.null(ha) && nzchar(ha)) {
      if (ha == "home") df <- df %>% filter(is_home == TRUE)
      else df <- df %>% filter(is_home == FALSE)
    }

    # Outcome filter
    outcome <- input$gl_outcome
    if (!is.null(outcome) && nzchar(outcome)) {
      if (outcome == "win") df <- df %>% filter(has_won == TRUE)
      else df <- df %>% filter(has_won == FALSE)
    }

    # GN filter
    gp <- gl_gn_params()
    if (!is.na(gp$min_gn)) df <- df %>% filter(gn >= !!gp$min_gn)
    if (!is.na(gp$max_gn)) df <- df %>% filter(gn <= !!gp$max_gn)
    if (!is.na(gp$last_n)) {
      df <- df %>%
        group_by(team_id) %>%
        arrange(desc(game_date), desc(game_id), .by_group = TRUE) %>%
        mutate(rn_recent = row_number()) %>%
        ungroup() %>%
        filter(rn_recent <= gp$last_n) %>%
        select(-rn_recent)
    }

    df
  })

  gl_percentile_schedule <- reactive({
    sched <- gl_schedule()
    req(nrow(sched) > 0)

    df <- sched
    team_id_str <- input$gl_team
    if (!is.null(team_id_str) && nzchar(team_id_str)) {
      team_id_val <- as.integer(team_id_str)
      df <- df %>% filter(team_id == !!team_id_val)
    }

    df
  })

  gl_starters_filter <- reactive({
    off_mode <- input$gl_num_starters_off_mode %||% ""
    def_mode <- input$gl_num_starters_def_mode %||% ""
    off_val <- if (nzchar(off_mode) && nzchar(input$gl_num_starters_off %||% "")) as.integer(input$gl_num_starters_off) else NA_integer_
    def_val <- if (nzchar(def_mode) && nzchar(input$gl_num_starters_def %||% "")) as.integer(input$gl_num_starters_def) else NA_integer_
    list(
      off_min = if (identical(off_mode, "gte")) off_val else NA_integer_,
      off_max = if (identical(off_mode, "lte")) off_val else NA_integer_,
      def_min = if (identical(def_mode, "gte")) def_val else NA_integer_,
      def_max = if (identical(def_mode, "lte")) def_val else NA_integer_
    )
  })

  # --- Lineup totals cache per season ---
  gl_lineup_totals <- reactive({
    req(identical(input$main_tabs, "game_logs"))
    gy_int <- as.integer(input$game_year)
    req(gy_int)
    db_get_query(
      pg_pool,
      "SELECT team_id, lineup_hash, type_lineup, g_date, game_id, game_year,
              total_poss, total_pts, fg2_made, fg2_att, fg3_made, fg3_att, minutes, num_starters
       FROM basketball_test.mv_lineup_totals_by_day
       WHERE game_year = $1",
      params = list(gy_int)
    )
  }) %>% bindEvent(input$game_year, input$main_tabs)

  # --- Lineup FF cache per season ---
  gl_lineup_ff <- reactive({
    req(identical(input$main_tabs, "game_logs"))
    gy_int <- as.integer(input$game_year)
    req(gy_int)
    db_get_query(
      pg_pool,
      "SELECT lineup_hash, team_id, game_id, game_year, type_lineup,
              total_points, total_poss, ts_poss_count, oreb_count,
              oreb_opportunities, tov_count, total_ft_attempts, total_fga,
              total_fgm, total_fg3_made, minutes, num_starters
       FROM basketball_test.lineup_four_factors_by_game
       WHERE game_year = $1",
      params = list(gy_int)
    )
  }) %>% bindEvent(input$game_year, input$main_tabs)

  # ============================================================
  # TEAMS SUMMARY
  # ============================================================
  gl_teams_summary_baseline <- reactive({
    gl_build_summary_metrics(
      lineup_totals_df = gl_lineup_totals(),
      schedule_df = gl_percentile_schedule(),
      apply_starters = FALSE
    )
  })

  gl_teams_summary <- reactive({
    sched <- gl_filtered_schedule()
    req(nrow(sched) > 0)
    display_metrics <- gl_build_summary_metrics(
      lineup_totals_df = gl_lineup_totals(),
      schedule_df = sched,
      starters_bounds = gl_starters_filter()
    )
    if (is.null(display_metrics) || !nrow(display_metrics)) return(NULL)

    gl_join_schedule_info(
      gl_attach_percentiles(display_metrics, gl_teams_summary_baseline(), c("off_ppp", "def_ppp")),
      sched
    )
  })

  # ============================================================
  # TEAMS FOUR FACTORS
  # ============================================================
  gl_teams_ff_baseline <- reactive({
    gl_build_ff_metrics(
      lineup_ff_df = gl_lineup_ff(),
      schedule_df = gl_percentile_schedule(),
      apply_starters = FALSE
    )
  })

  gl_teams_ff <- reactive({
    sched <- gl_filtered_schedule()
    req(nrow(sched) > 0)
    display_metrics <- gl_build_ff_metrics(
      lineup_ff_df = gl_lineup_ff(),
      schedule_df = sched,
      starters_bounds = gl_starters_filter()
    )
    if (is.null(display_metrics) || !nrow(display_metrics)) return(NULL)

    gl_join_schedule_info(
      gl_attach_percentiles(
        display_metrics,
        gl_teams_ff_baseline(),
        c(
          "off_ppp", "off_efg_pct", "off_oreb_pct", "off_tov_pct", "off_ftr_pct",
          "def_ppp", "def_efg_pct", "def_oreb_pct", "def_tov_pct", "def_ftr_pct"
        )
      ),
      sched
    )
  })

  # ============================================================
  # RENDER TABLE
  # ============================================================
  output$gl_table <- DT::renderDataTable({
    req(identical(input$main_tabs, "game_logs"))

    view <- input$gl_view_mode

    if (identical(view, "Summary")) {
      # ------- TEAMS SUMMARY -------
      df <- gl_teams_summary()
      if (is.null(df) || nrow(df) == 0) return(NULL)

      shot_raw_cols <- c("off_fg2m", "off_fg2a", "off_fg3m", "off_fg3a",
                         "def_fg2m", "def_fg2a", "def_fg3m", "def_fg3a")
      has_shots <- all(c("off_fg2a", "off_fg3a") %in% names(df))
      if (has_shots) {
        df[["Off Shot"]] <- coalesce(df$off_fg2a, 0) + coalesce(df$off_fg3a, 0)
        df[["Def Shot"]] <- coalesce(df$def_fg2a, 0) + coalesce(df$def_fg3a, 0)
      }
      df <- apply_stat_filters(df, gl_stat_filter_state$filters())
      if (is.null(df) || nrow(df) == 0) return(NULL)
      df <- df %>% mutate(score_link = gl_score_link(game_id, team_id, score_display))

      disp <- df %>% select(
        gn, game_type_label, game_date, team_name, opp_team_name, result, score_link,
        minutes,
        off_ppp, def_ppp, net_rtg,
        any_of(c("Off Shot", "Def Shot")),
        off_poss, def_poss,
        any_of(shot_raw_cols),
        any_of(c("pr_off_ppp", "pr_def_ppp"))
      )

      hide_idx <- which(names(disp) %in% c(shot_raw_cols, "pr_off_ppp", "pr_def_ppp")) - 1L

      # Shooting column JS render
      make_shot_render_gl <- function(fg2m_col, fg2a_col, fg3m_col, fg3a_col,
                                      is_defense = FALSE, min_fga = 10, avg2 = 53, avg3 = 34) {
        fg2m_idx <- which(names(disp) == fg2m_col) - 1
        fg2a_idx <- which(names(disp) == fg2a_col) - 1
        fg3m_idx <- which(names(disp) == fg3m_col) - 1
        fg3a_idx <- which(names(disp) == fg3a_col) - 1
        sign_mult <- if (is_defense) -1 else 1
        js_str <- sprintf(
          "function(data, type, row, meta) {
             if (type !== 'display' || !row) return data;
             var fg2m = row[%d] || 0, fg2a = row[%d] || 0;
             var fg3m = row[%d] || 0, fg3a = row[%d] || 0;
             var totalFGA = fg2a + fg3a;
             if (!totalFGA) return '<div class=\"shot-acc-label\" style=\"color:#aaa;\">-</div>';
             var fg2pct = fg2a ? Math.round(fg2m / fg2a * 100) : 0;
             var fg3pct = fg3a ? Math.round(fg3m / fg3a * 100) : 0;
             var fg2freq = Math.round(fg2a / totalFGA * 100);
             var fg3freq = 100 - fg2freq;
             var minFGA = %d;
             var sign = %d;
             var avg2 = %d, avg3 = %d;
             function accColor(pct, avg) {
               var d = sign * (pct - avg) / avg;
               d = Math.max(-1, Math.min(1, d * 3));
               var r, g;
               if (d < 0) { r = 200; g = Math.round(200 + d * 120); }
               else       { g = 170; r = Math.round(200 - d * 150); }
               return 'rgb(' + r + ',' + g + ',60)';
             }
             var muted = totalFGA < minFGA;
             var c2 = muted ? '#bbb' : accColor(fg2pct, avg2);
             var c3 = muted ? '#bbb' : accColor(fg3pct, avg3);
             var barOpacity = muted ? 'opacity:0.3;' : '';
             var title2pct = '2PT accuracy: ' + fg2pct + '%% (' + fg2m + '/' + fg2a + ')';
             var title3pct = '3PT accuracy: ' + fg3pct + '%% (' + fg3m + '/' + fg3a + ')';
             var title2freq = '2PT frequency: ' + fg2freq + '%% of FGA (' + fg2a + '/' + totalFGA + ')';
             var title3freq = '3PT frequency: ' + fg3freq + '%% of FGA (' + fg3a + '/' + totalFGA + ')';
             return '<div class=\"shot-acc-label\">' +
               '<span title=\"' + title2pct + '\" style=\"color:' + c2 + '; font-weight:' + (muted ? '400' : '700') + '; cursor:help;\">' + fg2pct + '%%</span>' +
               ' <span style=\"opacity:0.3;\">|</span> ' +
               '<span title=\"' + title3pct + '\" style=\"color:' + c3 + '; font-weight:' + (muted ? '400' : '700') + '; cursor:help;\">' + fg3pct + '%%</span>' +
               '</div>' +
               '<div class=\"shot-bar-container\" style=\"' + barOpacity + '\">' +
               '<div class=\"shot-bar-2pt\" title=\"' + title2freq + '\" style=\"width:' + fg2freq + '%%; cursor:help;\">' + fg2freq + '%%</div>' +
               '<div class=\"shot-bar-3pt\" title=\"' + title3freq + '\" style=\"width:' + fg3freq + '%%; cursor:help;\">' + fg3freq + '%%</div>' +
               '</div>';
           }", fg2m_idx, fg2a_idx, fg3m_idx, fg3a_idx, min_fga, sign_mult, avg2, avg3
        )
        DT::JS(js_str)
      }

      # Dynamic shot averages from the data
      shot_col_defs <- list()
      if (has_shots) {
        shot_col_map <- list(
          "Off Shot" = c("off_fg2m", "off_fg2a", "off_fg3m", "off_fg3a"),
          "Def Shot" = c("def_fg2m", "def_fg2a", "def_fg3m", "def_fg3a")
        )
        SHOT_MIN_FGA <- 10L
        for (disp_name in names(shot_col_map)) {
          cols <- shot_col_map[[disp_name]]
          target_idx <- which(names(disp) == disp_name) - 1
          is_def <- grepl("^Def", disp_name)
          fg2a_sum <- sum(disp[[cols[2]]], na.rm = TRUE)
          fg3a_sum <- sum(disp[[cols[4]]], na.rm = TRUE)
          a2 <- if (fg2a_sum > 0) as.integer(round(sum(disp[[cols[1]]], na.rm = TRUE) / fg2a_sum * 100)) else 53L
          a3 <- if (fg3a_sum > 0) as.integer(round(sum(disp[[cols[3]]], na.rm = TRUE) / fg3a_sum * 100)) else 34L
          if (length(target_idx) && all(cols %in% names(disp))) {
            shot_col_defs[[length(shot_col_defs) + 1]] <- list(
              targets = target_idx,
              render = make_shot_render_gl(cols[1], cols[2], cols[3], cols[4],
                                           is_defense = is_def, min_fga = SHOT_MIN_FGA,
                                           avg2 = a2, avg3 = a3)
            )
          }
        }
      }

      # Result column color
      result_idx <- which(names(disp) == "result") - 1L
      result_render <- DT::JS(
        "function(data, type, row, meta) {
           if (type !== 'display' || !row) return data;
           var color = data === 'W' ? '#34d399' : '#f87171';
           return '<span style=\"font-weight:700; color:' + color + ';\">' + data + '</span>';
         }")

      col_defs <- c(
        list(
          list(targets = hide_idx, visible = FALSE),
          list(targets = "_all", className = "dt-center"),
          list(targets = result_idx, render = result_render)
        ),
        shot_col_defs
      )

      sketch <- htmltools::withTags(table(class = 'display', thead(
        tr(
          th(class = "sub-head", "GN"),
          th(class = "sub-head", "Game Type"),
          th(class = "sub-head", "Date"),
          th(class = "sub-head", "Team"),
          th(class = "sub-head", "Opponent"),
          th(class = "sub-head", "W/L"),
          th(class = "sub-head", "Score"),
          th(class = "sub-head", "Min"),
          th(class = "sub-head section-left-border", "Off PPP"),
          th(class = "sub-head", "Def PPP"),
          th(class = "sub-head", "Net"),
          if (has_shots) th(class = "sub-head section-left-border", "Off Shot"),
          if (has_shots) th(class = "sub-head", "Def Shot"),
          th(class = "sub-head section-left-border", "Off Poss"),
          th(class = "sub-head", "Def Poss")
        )
      )))

      off_ppp_idx <- which(names(disp) == "off_ppp") - 1L
      off_poss_idx <- which(names(disp) == "off_poss") - 1L
      off_shot_idx <- if (has_shots) which(names(disp) == "Off Shot") - 1L else integer(0)

      if (length(off_ppp_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = off_ppp_idx, className = "section-left-border dt-center")
      if (length(off_poss_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = off_poss_idx, className = "section-left-border dt-center")
      if (length(off_shot_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = off_shot_idx, className = "section-left-border dt-center")

      dt <- DT::datatable(disp, container = sketch, rownames = FALSE, escape = FALSE,
                          selection = "none",
                          callback = gl_game_link_callback,
                          options = list(
                            headerCallback = HEADER_TOOLTIP_JS,
                            dom = "tip", pageLength = 50,
                            deferRender = TRUE, scrollX = TRUE,
                            scrollY = "70vh", scrollCollapse = TRUE,
                            order = list(list(2, "desc"), list(0, "desc")),
                            columnDefs = col_defs
                          ))

      dt <- DT::formatRound(dt, c("off_ppp", "def_ppp", "net_rtg"), 1)
      if ("minutes" %in% names(disp)) dt <- DT::formatRound(dt, "minutes", 1)
      dt <- DT::formatCurrency(dt, c("off_poss", "def_poss"), currency = "", interval = 3, mark = ",", digits = 0)
      if ("pr_off_ppp" %in% names(disp)) {
        dt <- DT::formatStyle(dt, "off_ppp", backgroundColor = DT::styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_ppp")
      }
      if ("pr_def_ppp" %in% names(disp)) {
        dt <- DT::formatStyle(dt, "def_ppp", backgroundColor = DT::styleInterval(CUTS, COLS_REV), valueColumns = "pr_def_ppp")
      }

      return(dt)

    } else {
      # ------- TEAMS FOUR FACTORS -------
      df <- gl_teams_ff()
      if (is.null(df) || nrow(df) == 0) return(NULL)
      df <- apply_stat_filters(df, gl_stat_filter_state$filters())
      if (is.null(df) || nrow(df) == 0) return(NULL)
      df <- df %>% mutate(score_link = gl_score_link(game_id, team_id, score_display))

      disp <- df %>% select(
        gn, game_type_label, game_date, team_name, opp_team_name, result, score_link,
        minutes,
        off_ppp, off_efg_pct, off_oreb_pct, off_tov_pct, off_ftr_pct,
        def_ppp, def_efg_pct, def_oreb_pct, def_tov_pct, def_ftr_pct,
        off_poss, def_poss,
        any_of(c(
          "pr_off_ppp", "pr_off_efg_pct", "pr_off_oreb_pct", "pr_off_tov_pct", "pr_off_ftr_pct",
          "pr_def_ppp", "pr_def_efg_pct", "pr_def_oreb_pct", "pr_def_tov_pct", "pr_def_ftr_pct"
        ))
      )

      hidden_pr_cols <- names(disp)[grepl("^pr_", names(disp))]

      # Result column color
      result_idx <- which(names(disp) == "result") - 1L
      result_render <- DT::JS(
        "function(data, type, row, meta) {
           if (type !== 'display' || !row) return data;
           var color = data === 'W' ? '#34d399' : '#f87171';
           return '<span style=\"font-weight:700; color:' + color + ';\">' + data + '</span>';
         }")

      off_ppp_idx <- which(names(disp) == "off_ppp") - 1L
      def_ppp_idx <- which(names(disp) == "def_ppp") - 1L
      off_poss_idx <- which(names(disp) == "off_poss") - 1L

      col_defs <- list(
        list(targets = "_all", className = "dt-center"),
        list(targets = result_idx, render = result_render),
        list(targets = which(names(disp) %in% hidden_pr_cols) - 1L, visible = FALSE)
      )
      if (length(off_ppp_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = off_ppp_idx, className = "section-left-border dt-center")
      if (length(def_ppp_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = def_ppp_idx, className = "section-left-border dt-center")
      if (length(off_poss_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = off_poss_idx, className = "section-left-border dt-center")

      sketch <- htmltools::withTags(table(class = 'display', thead(
        tr(
          th(class = "group-head", colspan = 8, ""),
          th(class = "group-head section-left-border", colspan = 5, "Offense"),
          th(class = "group-head section-left-border", colspan = 5, "Defense"),
          th(class = "group-head section-left-border", colspan = 2, "Usage")
        ),
        tr(
          th(class = "sub-head", "GN"),
          th(class = "sub-head", "Game Type"),
          th(class = "sub-head", "Date"),
          th(class = "sub-head", "Team"),
          th(class = "sub-head", "Opponent"),
          th(class = "sub-head", "W/L"),
          th(class = "sub-head", "Score"),
          th(class = "sub-head", "Min"),
          th(class = "sub-head section-left-border", "PPP"),
          th(class = "sub-head", "eFG%"),
          th(class = "sub-head", title = OFF_OREB_TOOLTIP, "OREB%"),
          th(class = "sub-head", "TOV%"),
          th(class = "sub-head", "FTR"),
          th(class = "sub-head section-left-border", "PPP"),
          th(class = "sub-head", "eFG%"),
          th(class = "sub-head", title = DEF_OREB_TOOLTIP, "OREB%"),
          th(class = "sub-head", "TOV%"),
          th(class = "sub-head", "FTR"),
          th(class = "sub-head section-left-border", "Off Poss"),
          th(class = "sub-head", "Def Poss")
        )
      )))

      dt <- DT::datatable(disp, container = sketch, rownames = FALSE, escape = FALSE,
                          selection = "none",
                          callback = gl_game_link_callback,
                          options = list(
                            headerCallback = HEADER_TOOLTIP_JS,
                            dom = "tip", pageLength = 50,
                            deferRender = TRUE, scrollX = TRUE,
                            scrollY = "70vh", scrollCollapse = TRUE,
                            order = list(list(2, "desc"), list(0, "desc")),
                            columnDefs = col_defs
                          ))

      rate_cols <- c("off_efg_pct", "off_oreb_pct", "off_tov_pct", "off_ftr_pct",
                     "def_efg_pct", "def_oreb_pct", "def_tov_pct", "def_ftr_pct")
      ppp_cols <- c("off_ppp", "def_ppp")

      dt <- DT::formatRound(dt, intersect(c(rate_cols, ppp_cols), names(disp)), 1)
      if ("minutes" %in% names(disp)) dt <- DT::formatRound(dt, "minutes", 1)
      dt <- DT::formatCurrency(dt, c("off_poss", "def_poss"), currency = "", interval = 3, mark = ",", digits = 0)
      heat_reverse <- c(
        off_ppp = FALSE,
        off_efg_pct = FALSE,
        off_oreb_pct = FALSE,
        off_tov_pct = TRUE,
        off_ftr_pct = FALSE,
        def_ppp = TRUE,
        def_efg_pct = TRUE,
        def_oreb_pct = TRUE,
        def_tov_pct = FALSE,
        def_ftr_pct = TRUE
      )
      for (metric_name in names(heat_reverse)) {
        pr_col <- gl_pr_col_name(metric_name)
        if (!(metric_name %in% names(disp)) || !(pr_col %in% names(disp))) next
        dt <- DT::formatStyle(
          dt,
          metric_name,
          backgroundColor = DT::styleInterval(CUTS, if (isTRUE(heat_reverse[[metric_name]])) COLS_REV else COLS_GRAD),
          valueColumns = pr_col
        )
      }

      return(dt)
    }
  })

  # ============================================================
  # GAME CLICK -> PLAYER BOX SCORE
  # ============================================================
  observeEvent(input$gl_game_click, {
    click <- input$gl_game_click
    req(click$game_id)

    game_id_val <- suppressWarnings(as.integer(click$game_id %||% NA_integer_))
    clicked_team_id <- suppressWarnings(as.integer(click$team_id %||% NA_integer_))
    gy <- suppressWarnings(as.integer(input$game_year))
    req(!is.na(game_id_val), !is.na(gy))

    sched <- gl_schedule()
    game_sched <- sched %>% filter(game_id == !!game_id_val)
    if (is.null(game_sched) || !nrow(game_sched)) {
      showModal(modalDialog(title = "No box score", "No schedule row found for this game.", easyClose = TRUE))
      return()
    }

    clicked_row <- if (!is.na(clicked_team_id)) {
      game_sched %>% filter(team_id == !!clicked_team_id)
    } else {
      game_sched[0, , drop = FALSE]
    }
    if (!nrow(clicked_row)) clicked_row <- game_sched[1, , drop = FALSE]
    clicked_row <- clicked_row[1, , drop = FALSE]

    other_team_ids <- setdiff(as.integer(game_sched$team_id), as.integer(clicked_row$team_id[[1]]))
    team_order_ids <- c(as.integer(clicked_row$team_id[[1]]), other_team_ids)

    box_df <- tryCatch(
      gl_fetch_box_score(pg_pool, game_id_val, gy),
      error = function(e) {
        app_log("tab4_box_score_error", conditionMessage(e))
        NULL
      }
    )
    if (is.null(box_df) || !nrow(box_df)) {
      showModal(modalDialog(title = "No box score", "No player box score rows found for this game.", easyClose = TRUE))
      return()
    }

    box_df <- gl_add_box_score_totals(box_df, team_order_ids)

    disp <- box_df %>%
      transmute(
        .team_order,
        .row_order,
        .starter_sort,
        .team_id = team_id,
        Starter = ifelse(coalesce(starter, FALSE), "Y", ""),
        Player = player_name,
        Min = minutes,
        PTS = pts,
        REB = reb,
        OREB = oreb,
        DREB = dreb,
        AST = ast,
        STL = stl,
        BLK = blk,
        TOV = tov,
        FGM = fgm,
        FGA = fga,
        `FG%` = fg_pct,
        `2PM` = `2pm`,
        `2PA` = `2pa`,
        `2P%` = two_pct,
        `3PM` = `3pm`,
        `3PA` = `3pa`,
        `3P%` = tp_pct,
        FTM = ftm,
        FTA = fta,
        `FT%` = ft_pct,
        `+/-` = plus_minus,
        Poss = poss_on_floor,
        `TS%` = ts,
        `USG%` = usg_pct
      )
    disp <- gl_add_box_score_percentiles(disp)

    clicked_team_id <- as.integer(clicked_row$team_id[[1]])
    clicked_disp <- disp %>% filter(.team_id == !!clicked_team_id)
    opponent_disp <- disp %>% filter(.team_id != !!clicked_team_id)

    render_box_score_dt <- function(table_df) {
      pr_cols <- names(table_df)[grepl("^pr_", names(table_df))]
      hidden_cols <- c(".team_order", ".row_order", ".starter_sort", ".team_id", pr_cols)
      hidden_idx <- which(names(table_df) %in% hidden_cols) - 1L
      player_idx <- which(names(table_df) == "Player") - 1L
      min_idx <- which(names(table_df) == "Min") - 1L
      order_cols <- list(
        list(which(names(table_df) == ".row_order") - 1L, "asc"),
        list(which(names(table_df) == ".starter_sort") - 1L, "desc"),
        list(min_idx, "desc")
      )

      dt <- DT::datatable(
        table_df,
        rownames = FALSE,
        escape = FALSE,
        selection = "none",
        options = list(
          headerCallback = HEADER_TOOLTIP_JS,
          dom = "t",
          paging = FALSE,
          ordering = TRUE,
          deferRender = TRUE,
          scrollX = TRUE,
          scrollY = "65vh",
          scrollCollapse = TRUE,
          order = order_cols,
          columnDefs = list(
            list(visible = FALSE, targets = hidden_idx),
            list(className = "dt-center", targets = "_all"),
            list(className = "dt-left", targets = player_idx)
          )
        )
      )

      int_cols <- intersect(c(
        "PTS", "REB", "OREB", "DREB", "AST", "STL", "BLK", "TOV",
        "FGM", "FGA", "2PM", "2PA", "3PM", "3PA", "FTM", "FTA", "+/-", "Poss"
      ), names(table_df))
      pct_cols <- intersect(c("Min", "FG%", "2P%", "3P%", "FT%", "TS%", "USG%"), names(table_df))

      if (length(int_cols)) {
        dt <- DT::formatCurrency(dt, int_cols, currency = "", interval = 3, mark = ",", digits = 0)
      }
      if (length(pct_cols)) {
        dt <- DT::formatRound(dt, pct_cols, 1)
      }

      apply_heat <- function(dt_obj, col_name, reverse = FALSE) {
        pr_col <- gl_box_score_pr_col(col_name)
        if (!(col_name %in% names(table_df)) || !(pr_col %in% names(table_df))) return(dt_obj)
        DT::formatStyle(
          dt_obj,
          col_name,
          backgroundColor = DT::styleInterval(CUTS, if (isTRUE(reverse)) COLS_REV else COLS_GRAD),
          valueColumns = pr_col
        )
      }

      for (col_name in GL_BOX_SCORE_HEAT_GOOD) {
        dt <- apply_heat(dt, col_name, reverse = FALSE)
      }
      dt <- apply_heat(dt, "TOV", reverse = TRUE)

      dt %>%
        DT::formatStyle(
          "Player",
          target = "row",
          fontWeight = DT::styleEqual("TOTAL", "bold"),
          backgroundColor = DT::styleEqual("TOTAL", "#1a1f2b")
        )
    }

    output$gl_box_score_team_table <- DT::renderDataTable({
      render_box_score_dt(clicked_disp)
    })

    output$gl_box_score_opp_table <- DT::renderDataTable({
      render_box_score_dt(opponent_disp)
    })

    game_type_label <- unname(GAME_TYPE_LABELS[as.character(clicked_row$game_type[[1]])])
    if (!length(game_type_label) || is.na(game_type_label) || !nzchar(game_type_label)) {
      game_type_label <- as.character(clicked_row$game_type[[1]])
    }
    game_date_label <- format(as.Date(clicked_row$game_date[[1]]), "%b %d, %Y")
    title <- sprintf(
      "Box Score: %s %s-%s %s",
      clicked_row$team_name[[1]],
      clicked_row$team_score[[1]],
      clicked_row$opp_score[[1]],
      clicked_row$opp_team_name[[1]]
    )

    showModal(modalDialog(
      title = title,
      tags$div(
        class = "gl-box-score-modal",
        tags$div(
          class = "text-muted small mb-2",
          sprintf("GN %s | %s | %s", clicked_row$gn[[1]], game_date_label, game_type_label)
        ),
        tabsetPanel(
          id = "gl_box_score_team_tabs",
          type = "tabs",
          tabPanel(
            title = sprintf("%s %s", clicked_row$team_name[[1]], clicked_row$team_score[[1]]),
            value = "clicked_team",
            DTOutput("gl_box_score_team_table")
          ),
          tabPanel(
            title = sprintf("%s %s", clicked_row$opp_team_name[[1]], clicked_row$opp_score[[1]]),
            value = "opponent",
            DTOutput("gl_box_score_opp_table")
          )
        )
      ),
      size = "l",
      easyClose = TRUE
    ))
  })

  # ---- Filter Chips ----
  output$gl_filter_chips <- renderUI({
    team_map <- NULL
    tdf <- gl_ref$teams
    if (is.data.frame(tdf) && nrow(tdf) > 0 &&
        all(c("team_id", "team_name") %in% names(tdf))) {
      ids <- as.character(tdf$team_id)
      lbls <- as.character(tdf$team_name)
      keep <- !is.na(ids) & nzchar(ids)
      ids <- ids[keep]
      lbls <- lbls[keep]
      if (length(ids) > 0) {
        team_map <- stats::setNames(lbls, ids)
      }
    }
    build_filter_chips(
      "gl", input, shared$season_date_bounds,
      reset_btn_id = "gl_reset",
      team_label_map = team_map,
      extra_children = stat_filter_chips_ui("gl", gl_stat_filter_state, gl_stat_filter_cols)
    )
  })
  setup_chip_clears("gl", session, input, shared,
    game_type_id = "gl_game_type", opponents_id = "gl_opponents",
    home_away_id = "gl_home_away", outcome_id = "gl_outcome",
    gn_min_id = "gl_gn_min", gn_max_id = "gl_gn_max", last_n_id = "gl_last_n",
    opp_rank_ids = c(),
    date_id = "gl_dates", gy_input_id = "game_year",
    teams_ids = "gl_team",
    starters_ids = c("gl_num_starters_off_mode", "gl_num_starters_off",
                     "gl_num_starters_def_mode", "gl_num_starters_def"))

  invisible(list(
    gl_teams_summary = gl_teams_summary,
    gl_teams_ff = gl_teams_ff
  ))
}

