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


# Schedule rows that pass the Game Logs filters. `f` carries the filter values:
# team (character or ""), dates (length-2 Date or NULL), game_type, opponents,
# home_away, outcome, and gn (the resolve_gn_last_n_params() list).
gl_filter_schedule <- function(sched, f) {
  df <- sched

  # Last N counts each team's whole season, as the SQL readers'
  # schedule_ranked CTE does, so it runs before every other filter: "home +
  # last 5" is the home games among a team's last 5, not its last 5 home games.
  gp <- f$gn
  if (!is.na(gp$last_n)) {
    df <- df %>%
      group_by(team_id) %>%
      arrange(desc(game_date), desc(game_id), .by_group = TRUE) %>%
      mutate(rn_recent = row_number()) %>%
      ungroup() %>%
      filter(rn_recent <= gp$last_n) %>%
      select(-rn_recent)
  }

  # Team filter (optional)
  team_id_str <- f$team
  if (!is.null(team_id_str) && nzchar(team_id_str)) {
    team_id_val <- as.integer(team_id_str)
    df <- df %>% filter(team_id == !!team_id_val)
  }

  # Date filter
  start_d <- f$dates[1]
  end_d <- f$dates[2]
  if (!is.null(start_d) && !is.na(start_d)) df <- df %>% filter(game_date >= !!as.Date(start_d))
  if (!is.null(end_d) && !is.na(end_d)) df <- df %>% filter(game_date <= !!as.Date(end_d))

  # Game type filter
  gt <- f$game_type
  if (!is.null(gt) && length(gt) > 0 && any(nzchar(gt))) {
    gt_vals <- game_type_filter_values(gt)
    df <- df %>% filter(game_type %in% !!gt_vals)
  }

  # Opponent filter
  opp_ids <- suppressWarnings(as.integer(f$opponents))
  opp_ids <- opp_ids[is.finite(opp_ids)]
  if (length(opp_ids) > 0) {
    df <- df %>% filter(opp_team_id %in% !!opp_ids)
  }

  # Home/Away filter
  ha <- f$home_away
  if (!is.null(ha) && nzchar(ha)) {
    if (ha == "home") df <- df %>% filter(is_home == TRUE)
    else df <- df %>% filter(is_home == FALSE)
  }

  # Outcome filter
  outcome <- f$outcome
  if (!is.null(outcome) && nzchar(outcome)) {
    if (outcome == "win") df <- df %>% filter(has_won == TRUE)
    else df <- df %>% filter(has_won == FALSE)
  }

  # GN filter
  if (!is.na(gp$min_gn)) df <- df %>% filter(gn >= !!gp$min_gn)
  if (!is.na(gp$max_gn)) df <- df %>% filter(gn <= !!gp$max_gn)

  df
}

# "At least / at most N starters" bounds for each side, from the raw control values.
gl_starters_bounds <- function(off_mode, off_val, def_mode, def_val) {
  off_mode <- off_mode %||% ""
  def_mode <- def_mode %||% ""
  off_val <- if (nzchar(off_mode) && nzchar(off_val %||% "")) as.integer(off_val) else NA_integer_
  def_val <- if (nzchar(def_mode) && nzchar(def_val %||% "")) as.integer(def_val) else NA_integer_
  list(
    off_min = if (identical(off_mode, "gte")) off_val else NA_integer_,
    off_max = if (identical(off_mode, "lte")) off_val else NA_integer_,
    def_min = if (identical(def_mode, "gte")) def_val else NA_integer_,
    def_max = if (identical(def_mode, "lte")) def_val else NA_integer_
  )
}

# Summary display frame: shot totals and splits, the user's stat filters, and
# the scoreless-game flag. `scoreless` is only evaluated when rows survive.
gl_summary_display <- function(df, stat_filters, scoreless) {
  has_shots <- all(c("off_fg2a", "off_fg3a") %in% names(df))
  if (has_shots) {
    df[["Off Shot"]] <- coalesce(df$off_fg2a, 0) + coalesce(df$off_fg3a, 0)
    df[["Def Shot"]] <- coalesce(df$def_fg2a, 0) + coalesce(df$def_fg3a, 0)
    df <- add_shot_split_metrics(df, list(
      off = c("off_fg2m", "off_fg2a", "off_fg3m", "off_fg3a"),
      def = c("def_fg2m", "def_fg2a", "def_fg3m", "def_fg3a")
    ))
  }
  df <- apply_stat_filters(df, stat_filters)
  if (is.null(df) || nrow(df) == 0) return(NULL)

  attach_has_scores(df, scoreless)
}

gl_ff_display <- function(df, stat_filters, scoreless) {
  df <- apply_stat_filters(df, stat_filters)
  if (is.null(df) || nrow(df) == 0) return(NULL)

  attach_has_scores(df, scoreless)
}

# Raw shot columns (2PT made/att, 3PT made/att) behind each side's shot cell.
GL_SHOT_COLS <- list(
  off = c("off_fg2m", "off_fg2a", "off_fg3m", "off_fg3a"),
  def = c("def_fg2m", "def_fg2a", "def_fg3m", "def_fg3a")
)

# League 2PT/3PT accuracy (whole percent) over the displayed rows, per side;
# the shot cells colour against it. 53/34 when a side has no attempts.
gl_shot_averages <- function(disp) {
  one_side <- function(cols) {
    fg2a_sum <- sum(disp[[cols[2]]], na.rm = TRUE)
    fg3a_sum <- sum(disp[[cols[4]]], na.rm = TRUE)
    c(
      fg2 = if (fg2a_sum > 0) as.integer(round(sum(disp[[cols[1]]], na.rm = TRUE) / fg2a_sum * 100)) else 53L,
      fg3 = if (fg3a_sum > 0) as.integer(round(sum(disp[[cols[3]]], na.rm = TRUE) / fg3a_sum * 100)) else 34L
    )
  }
  lapply(GL_SHOT_COLS, one_side)
}

GL_EURO_PERCENTILE_METRICS <- c(
  "off_ppp", "def_ppp", "net_rtg",
  "off_efg_pct", "off_oreb_pct", "off_tov_pct", "off_ftr_pct",
  "def_efg_pct", "def_oreb_pct", "def_tov_pct", "def_ftr_pct"
)

gl_build_euro_games <- function(df, starters_bounds = NULL, apply_starters = TRUE) {
  if (is.null(df) || !nrow(df)) return(NULL)

  schedule_rows <- df %>%
    distinct(game_id, team_id, round_number, phase, game_date, opp_team_id,
             is_home, has_won, team_points, opp_points, team_name, opp_team_name)

  # Schema adapter only: Tab 4's shared calculator consumes one offense or
  # defense row at a time, while the EuroLeague fact stores both perspectives
  # on one row. All aggregation and basketball formulas remain in
  # gl_build_ff_metrics().
  metric_rows <- bind_rows(
    df %>% transmute(
      game_id, team_id, type_lineup = "offense", num_starters = own_starters,
      total_points = off_pts, total_poss = off_poss,
      ts_poss_count = off_ts_poss, oreb_count = off_oreb,
      oreb_opportunities = off_oreb_opp, tov_count = off_tov,
      total_ft_attempts = off_fta, total_fga = off_fga,
      total_fgm = off_fgm, total_fg3_made = off_fg3m, minutes
    ),
    df %>% transmute(
      game_id, team_id, type_lineup = "defense", num_starters = opp_starters,
      total_points = def_pts, total_poss = def_poss,
      ts_poss_count = def_ts_poss, oreb_count = def_oreb,
      oreb_opportunities = def_oreb_opp, tov_count = def_tov,
      total_ft_attempts = def_fta, total_fga = def_fga,
      total_fgm = def_fgm, total_fg3_made = def_fg3m, minutes = NA_real_
    )
  )

  games <- gl_build_ff_metrics(
    metric_rows, schedule_rows,
    starters_bounds = starters_bounds,
    apply_starters = apply_starters
  )
  if (is.null(games) || !nrow(games)) return(NULL)

  games %>%
    inner_join(schedule_rows, by = c("game_id", "team_id")) %>%
    mutate(result = ifelse(has_won, "W", "L"),
           score = paste0(team_points, "-", opp_points),
           phase_label = euro_phase_label(phase)) %>%
    # game_id before team_name, matching tab 4. DataTables sorts on Date
    # then Rd and its sort is stable, so every row of every game played on
    # the same date in the same round ties on both keys and falls back to
    # this order. Without the game_id tiebreak a game's two rows scatter
    # alphabetically across the whole date block -- 79 of 100 date+round
    # buckets hold more than one game.
    arrange(desc(game_date), desc(round_number), game_id, team_name)
}

gl_filter_euro_rows <- function(df, f) {
  if (is.null(df) || !nrow(df)) return(df)
  gn <- f$gn
  # Known defect: slices starter-split rows, not games -- see the deviation register.
  if (!is.na(gn$last_n)) df <- df %>% group_by(team_id) %>%
    arrange(desc(game_date), desc(game_id), .by_group = TRUE) %>%
    slice_head(n = gn$last_n) %>% ungroup()
  rng <- f$dates
  if (length(rng) == 2L) df <- df %>% filter(game_date >= as.Date(rng[1]), game_date <= as.Date(rng[2]))
  ids <- f$teams
  if (length(ids)) df <- df %>% filter(team_id %in% ids)
  opp <- suppressWarnings(as.numeric(f$opponents)); opp <- opp[is.finite(opp)]
  if (length(opp)) df <- df %>% filter(opp_team_id %in% opp)
  if (length(f$phase) && any(nzchar(f$phase))) df <- df %>% filter(phase %in% f$phase[nzchar(f$phase)])
  if (nzchar(f$home_away %||% "")) df <- df %>% filter(is_home == (f$home_away == "home"))
  if (nzchar(f$outcome %||% "")) df <- df %>% filter(has_won == (f$outcome == "win"))
  if (!is.na(gn$min_gn)) df <- df %>% filter(round_number >= gn$min_gn)
  if (!is.na(gn$max_gn)) df <- df %>% filter(round_number <= gn$max_gn)
  df
}
