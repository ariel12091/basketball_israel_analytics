# svc_game_logs.R - Game Logs service layer: pure, Shiny-free logic shared by
# the Shiny tab and the API. Sourced before server_tab4.R.

GL_SUMMARY_FILTERABLE_COLS <- c(
  "GN" = "gn",
  "Min" = "minutes",
  "Off PPP" = "off_ppp",
  "Def PPP" = "def_ppp",
  "Net" = "net_rtg",
  "Off Shot" = "Off Shot",
  shot_split_metric_cols("Off", "off"),
  "Def Shot" = "Def Shot",
  shot_split_metric_cols("Def", "def"),
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
      net_rtg = round(off_ppp - def_ppp, 1),
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

