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


# The league dimension of Game Logs. round_col and type_col name the round and
# stage columns (Israeli schedule game number and game-type code, EuroLeague
# round and phase text), team_multi says whether the team filter is `p$teams`
# (several) or `p$team` (one), type_values turns the stage filter into column
# values, and has_shots / has_stat_filters say what the league's display
# frame offers. date_na is what a blank end of the date range does: "open"
# lifts that bound, "match_none" shows no games -- a kept difference,
# deviation register D25. header_labels are the round and stage labels the
# table headers (gamelog_summary_header() / gamelog_ff_header()) are drawn
# with: none for Israeli, which takes the builders' defaults (GN, Game Type).
# Nothing else about a game log differs between the leagues.
GL_LEAGUE <- list(
  il = list(round_col = "gn", type_col = "game_type", team_multi = FALSE,
            has_shots = TRUE, has_stat_filters = TRUE,
            type_values = function(x) game_type_filter_values(x),
            date_na = "open",
            header_labels = list()),
  euroleague = list(round_col = "round_number", type_col = "phase", team_multi = TRUE,
                    has_shots = FALSE, has_stat_filters = FALSE,
                    type_values = function(x) x[nzchar(x)],
                    date_na = "match_none",
                    header_labels = list(first_label = "Rd", second_label = "Phase"))
)

# One league's table header for a view, as Tab 4 / Tab 11 draw it and the
# API reads its labels: `...` are the builder's other arguments.
gl_header <- function(league, view, ...) {
  builder <- if (identical(view, "summary")) gamelog_summary_header else gamelog_ff_header
  do.call(builder, c(GL_LEAGUE[[league]]$header_labels, list(...)))
}

# Game rows that pass the Game Logs filters, for either league: the Israeli
# schedule (one row per team-game) or EuroLeague's game-log rows. `p` is the
# svc_game_logs() parameter list; this reads team / teams, dates, game_type /
# phase, opponents, home_away, outcome and gn (resolve_gn_last_n_values()).
gl_filter_games <- function(df, p, league) {
  lg <- GL_LEAGUE[[league %||% ""]]
  if (is.null(lg)) stop("gl_filter_games: unknown league ", league, call. = FALSE)
  if (is.null(df) || !nrow(df)) return(df)
  gp <- p$gn

  # Last N counts each team's whole season, as the SQL readers'
  # schedule_ranked CTE does, so it runs before every other filter: "home +
  # last 5" is the home games among a team's last 5, not its last 5 home games.
  # Known defect, deviation register D24: it counts ROWS. EuroLeague's rows
  # are split by starters, so there it keeps a team's last N starter-split
  # rows of team_four_factors_by_game, not its last N games.
  if (!is.na(gp$last_n)) {
    df <- df %>%
      group_by(team_id) %>%
      arrange(desc(game_date), desc(game_id), .by_group = TRUE) %>%
      mutate(rn_recent = row_number()) %>%
      ungroup() %>%
      filter(rn_recent <= !!gp$last_n) %>%
      select(-rn_recent)
  }

  # Team filter (optional). A non-blank value that is not an id matches
  # nothing rather than lifting the filter.
  teams <- as.character(if (isTRUE(lg$team_multi)) p$teams else p$team)
  teams <- teams[nzchar(teams)]
  if (length(teams)) {
    team_ids <- suppressWarnings(as.numeric(teams))
    df <- df %>% filter(team_id %in% !!team_ids)
  }

  # Date filter. "open": a blank (NA) end lifts that bound. "match_none": a
  # two-ended range is applied as given, so a blank end matches no game (D25).
  if (identical(lg$date_na, "match_none")) {
    if (length(p$dates) == 2L) {
      df <- df %>% filter(game_date >= !!as.Date(p$dates[1]), game_date <= !!as.Date(p$dates[2]))
    }
  } else {
    start_d <- p$dates[1]
    end_d <- p$dates[2]
    if (!is.null(start_d) && !is.na(start_d)) df <- df %>% filter(game_date >= !!as.Date(start_d))
    if (!is.null(end_d) && !is.na(end_d)) df <- df %>% filter(game_date <= !!as.Date(end_d))
  }

  # Game type (Israeli) / phase (EuroLeague) filter
  type_sel <- p[[lg$type_col]]
  if (!is.null(type_sel) && length(type_sel) > 0 && any(nzchar(type_sel))) {
    type_vals <- lg$type_values(type_sel)
    df <- df %>% filter(.data[[lg$type_col]] %in% !!type_vals)
  }

  # Opponent filter
  opp_ids <- suppressWarnings(as.numeric(p$opponents))
  opp_ids <- opp_ids[is.finite(opp_ids)]
  if (length(opp_ids) > 0) df <- df %>% filter(opp_team_id %in% !!opp_ids)

  # Home/Away and outcome filters
  ha <- p$home_away %||% ""
  if (nzchar(ha)) df <- df %>% filter(is_home == !!identical(ha, "home"))
  outcome <- p$outcome %||% ""
  if (nzchar(outcome)) df <- df %>% filter(has_won == !!identical(outcome, "win"))

  # Round filter (Israeli GN, EuroLeague round)
  if (!is.na(gp$min_gn)) df <- df %>% filter(.data[[lg$round_col]] >= !!gp$min_gn)
  if (!is.na(gp$max_gn)) df <- df %>% filter(.data[[lg$round_col]] <= !!gp$max_gn)

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

# Heat polarity of each view's percentile-coloured columns, both leagues:
# TRUE colours a high value as bad (the reversed scale). Tab 4 and Tab 11
# colour from these, and the API reports them as each column's polarity.
GL_HEAT_REVERSE_SUMMARY <- c(off_ppp = FALSE, def_ppp = TRUE)
GL_HEAT_REVERSE_FF <- c(
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

# Columns each view shows to one decimal (DT::formatRound(..., 1)) in both
# leagues' tables, besides the minutes column, which both round on its own.
GL_ROUND_1 <- list(
  summary = c("off_ppp", "def_ppp", "net_rtg"),
  four_factors = c("off_efg_pct", "off_oreb_pct", "off_tov_pct", "off_ftr_pct",
                   "def_efg_pct", "def_oreb_pct", "def_tov_pct", "def_ftr_pct",
                   "off_ppp", "def_ppp", "net_rtg")
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

# ---------------- Composition: one view's game rows ----------------
# Shiny's reactives and svc_game_logs() both call these, so the steps between
# a season pull and a game-log row exist once.

# Israeli views: the season pull each reads, its metric builder, the columns
# ranked against the season, and its display step. Wrappers, not references,
# so the builders resolve when called.
GL_IL_VIEWS <- list(
  summary = list(
    reader = "lineup_totals",
    build = function(...) gl_build_summary_metrics(...),
    percentiles = c("off_ppp", "def_ppp"),
    display = function(...) gl_summary_display(...)
  ),
  four_factors = list(
    reader = "lineup_ff",
    build = function(...) gl_build_ff_metrics(...),
    percentiles = c(
      "off_ppp", "off_efg_pct", "off_oreb_pct", "off_tov_pct", "off_ftr_pct",
      "def_ppp", "def_efg_pct", "def_oreb_pct", "def_tov_pct", "def_ftr_pct"
    ),
    display = function(...) gl_ff_display(...)
  )
)

gl_il_view <- function(view) {
  spec <- GL_IL_VIEWS[[view %||% ""]]
  if (is.null(spec)) stop("Game Logs: unknown view ", view, call. = FALSE)
  spec
}

# Percentile baseline: the season's whole game population, deliberately
# unfiltered, so a game keeps its colour whatever the filters show.
gl_il_baseline <- function(view, lineup_df, sched) {
  gl_il_view(view)$build(lineup_df, sched, apply_starters = FALSE)
}

# One view's rows for the filtered schedule, ranked against the season.
# `baseline` is a function returning gl_il_baseline()'s frame (Shiny passes
# its cached reactive); it is called only when rows survive.
gl_il_games <- function(view, lineup_df, sched, starters, baseline) {
  spec <- gl_il_view(view)
  display_metrics <- spec$build(lineup_df, sched, starters_bounds = starters)
  if (is.null(display_metrics) || !nrow(display_metrics)) return(NULL)

  gl_join_schedule_info(
    gl_attach_percentiles(display_metrics, baseline(), spec$percentiles),
    sched
  )
}

# EuroLeague's season population, the baseline of its percentiles.
gl_el_baseline <- function(rows) {
  if (is.null(rows) || !nrow(rows)) return(rows)
  gl_build_euro_games(rows, apply_starters = FALSE)
}

# EuroLeague's rows for `p`, ranked against the season; `baseline` as in
# gl_il_games().
gl_el_games <- function(rows, p, baseline) {
  if (is.null(rows) || !nrow(rows)) return(rows)
  df <- gl_filter_games(rows, p, "euroleague")
  games <- gl_build_euro_games(df, starters_bounds = p$starters)
  if (is.null(games) || !nrow(games)) return(games)

  gl_attach_percentiles(games, baseline(), GL_EURO_PERCENTILE_METRICS)
}

# ---------------- Season pulls ----------------
# The queries Shiny's Game Logs reactives run, uncached: Shiny wraps the
# Israeli ones in its own bindCache, so they must not cache a second copy.

gl_query_schedule <- function(gy) {
  db_get_query(pg_pool,
    "SELECT * FROM basketball_test.final_schedule_mv WHERE game_year = $1",
    params = list(as.integer(gy)))
}

gl_query_lineup_totals <- function(gy) {
  db_get_query(
    pg_pool,
    "SELECT team_id, lineup_hash, type_lineup, g_date, game_id, game_year,
            total_poss, total_pts, fg2_made, fg2_att, fg3_made, fg3_att, minutes, num_starters
     FROM basketball_test.mv_lineup_totals_by_day
     WHERE game_year = $1",
    params = list(as.integer(gy))
  )
}

gl_query_lineup_ff <- function(gy) {
  db_get_query(
    pg_pool,
    "SELECT lineup_hash, team_id, game_id, game_year, type_lineup,
            total_points, total_poss, ts_poss_count, oreb_count,
            oreb_opportunities, tov_count, total_ft_attempts, total_fga,
            total_fgm, total_fg3_made, minutes, num_starters
     FROM basketball_test.lineup_four_factors_by_game
     WHERE game_year = $1",
    params = list(as.integer(gy))
  )
}

# The service's readers: each season pull cached process-wide by league,
# season and data version. EuroLeague's key also carries the competition,
# because EuroLeague and EuroCup share a season number.
gl_default_readers <- function() list(
  schedule = function(season, data_version) cached_season_df(
    list("gl_schedule", "il", season, data_version),
    function() gl_query_schedule(season)
  ),
  lineup_totals = function(season, data_version) cached_season_df(
    list("gl_lineup_totals", "il", season, data_version),
    function() gl_query_lineup_totals(season)
  ),
  lineup_ff = function(season, data_version) cached_season_df(
    list("gl_lineup_ff", "il", season, data_version),
    function() gl_query_lineup_ff(season)
  ),
  euro_rows = function(competition, season, data_version) cached_season_df(
    list("gl_euro_rows", "euroleague", competition, season, data_version),
    function() euro_fetch_game_log_rows(competition, season)
  ),
  scoreless = function(data_version) fetch_scoreless_games(data_version)
)

# ---------------- The service ----------------

gl_il_rows <- function(p, readers) {
  view <- p$view
  spec <- gl_il_view(view)
  sched <- readers$schedule(p$season, p$data_version)
  if (is.null(sched) || !nrow(sched)) return(NULL)
  filtered <- gl_filter_games(sched, p, "il")
  if (!nrow(filtered)) return(NULL)

  lineup <- readers[[spec$reader]](p$season, p$data_version)
  games <- gl_il_games(view, lineup, filtered, p$starters,
                       function() gl_il_baseline(view, lineup, sched))
  if (is.null(games) || !nrow(games)) return(NULL)
  spec$display(games, p$stat_filters, readers$scoreless(p$data_version))
}

gl_el_rows <- function(p, readers) {
  rows <- readers$euro_rows(p$competition, p$season, p$data_version)
  games <- gl_el_games(rows, p, function() gl_el_baseline(rows))
  if (is.null(games) || !nrow(games)) return(NULL)
  # EuroLeague has no scoreless games to gate (see Tab 11's renderer).
  attach_has_scores(games, NULL)
}

# Game Logs for either league: one row per team-game that passes the filters,
# percentiles ranked against the season's whole population.
#
# `p` -- the shape svc_game_logs_params() (Task 8) must return. That function
# validates every field (league, view, ids, dates, ...); the service does not,
# beyond stopping on an unknown league or view.
#   league        "il" | "euroleague" (a GL_LEAGUE name)
#   competition   EuroLeague competition code ("E", "U"); NULL for il
#   season        il: game_year (season-ending year, integer); euroleague:
#                 provider season (starting year)
#   view          "summary" | "four_factors" (il rows differ by view;
#                 EuroLeague returns one frame that carries both)
#   team          il: one team id as character, "" for all
#   teams         euroleague: numeric team ids, empty for all
#   dates         length-2 Date (either end may be NA) or NULL
#   game_type     il: game-type codes (character); NULL for euroleague
#   phase         euroleague: phase text; NULL for il
#   opponents     opponent team ids (character or numeric), empty for all
#   home_away     "" | "home" | "away"
#   outcome       "" | "win" | "loss"
#   starters      list(off_min, off_max, def_min, def_max), integer or NA
#   gn            list(min_gn, max_gn, last_n) from resolve_gn_last_n_values()
#                 (round_number bounds for euroleague)
#   stat_filters  list of list(col, op = "ge" | "le", value); il only
#   data_version  the league's ETL version, a cache key only
#
# `readers` -- a list of season pulls, each called as:
#   schedule(season, data_version), lineup_totals(season, data_version),
#   lineup_ff(season, data_version), euro_rows(competition, season,
#   data_version), scoreless(data_version).
#
# Returns list(rows, meta). `rows` is the frame Shiny's renderer selects its
# columns from (NULL when nothing matches); `meta` echoes `p` and gives the
# default order, the Summary shot averages (il only) and the percentile
# population.
svc_game_logs <- function(p, readers = gl_default_readers()) {
  lg <- GL_LEAGUE[[p$league %||% ""]]
  if (is.null(lg)) stop("svc_game_logs: unknown league ", p$league, call. = FALSE)

  rows <- if (identical(p$league, "il")) gl_il_rows(p, readers) else gl_el_rows(p, readers)
  list(
    rows = rows,
    meta = list(
      path = "fast",
      filters = p,
      default_sort = list(
        list(column = "game_date", direction = "desc"),
        list(column = lg$round_col, direction = "desc")
      ),
      shot_averages = if (isTRUE(lg$has_shots) && identical(p$view, "summary") && !is.null(rows)) {
        gl_shot_averages(rows)
      },
      percentile_population = "season"
    )
  )
}

# ---------------- Parameters from a query ----------------
# svc_game_logs_params() builds svc_game_logs()'s `p` from an untrusted query
# (svc_params.R has the parsers and the query shape). Shiny builds the same
# list from its inputs; for an equivalent query the two are identical, pinned
# by api/tests/testthat/test-api-params.R against preset_to_p().

# Parameter names per league. `sort` and `format` shape the response and are
# not part of `p`. The league-specific names are the GL_LEAGUE dimensions:
# one team or several, game type or phase, GN or round, stat filters or none.
GL_PARAMS_COMMON <- c(
  "season", "view", "date_from", "date_to", "opponents", "home_away",
  "outcome", "own_starters", "opp_starters", "last_n", "sort", "format"
)
GL_PARAMS_LEAGUE <- list(
  il = c("team", "game_type", "gn_min", "gn_max", "stat"),
  euroleague = c("teams", "phase", "round_min", "round_max")
)
GL_PARAMS_ROUND <- list(il = c("gn_min", "gn_max"), euroleague = c("round_min", "round_max"))

# The request guard's caps (guard_query_window()), which Shiny applies to its
# heavy readers: Last N up to 80, a GN range of at most 80 games.
GL_MAX_LAST_N <- 80L
GL_MAX_ROUND_SPAN <- 80L

# Israeli game-type codes -> the UI choice that carries them. One choice can
# carry several codes (Winner Cup is "10,34"), so either code selects it.
gl_game_type_choice <- function(codes) {
  choices <- unname(GAME_TYPE_CHOICES_UI[nzchar(GAME_TYPE_CHOICES_UI)])
  by_code <- unlist(lapply(choices, function(ch) {
    stats::setNames(rep(ch, length(game_type_filter_values(ch))), game_type_filter_values(ch))
  }))
  ints <- svc_int(codes, "game_type")
  hit <- by_code[as.character(ints)]
  if (anyNA(hit)) {
    svc_reject("game_type", "Unknown game type %d; known codes: %s.",
               ints[is.na(hit)][[1]], paste(names(by_code), collapse = ", "))
  }
  unique(unname(hit))
}

# own_starters / opp_starters = <gte|lte>:<0-5>, the starters filter's mode
# and value as Shiny's two inputs hold them.
gl_starters_value <- function(query, name) {
  val <- svc_one(query, name)
  if (is.null(val)) return(list(mode = NULL, value = NULL))
  m <- regmatches(val, regexec("^(gte|lte):([0-5])$", val))[[1]]
  if (!length(m)) svc_reject(name, "'%s' must be gte:<0-5> or lte:<0-5>, got '%s'.", name, svc_show(val))
  list(mode = m[[2]], value = m[[3]])
}

# GN (Israeli) or round (EuroLeague) bounds and Last N, which exclude each
# other (X-05); then Shiny's own resolver, so the triple has its shape.
gl_params_gn <- function(query, league) {
  ids <- GL_PARAMS_ROUND[[league]]
  lo <- svc_int(svc_one(query, ids[[1]]), ids[[1]], min = 1L)
  hi <- svc_int(svc_one(query, ids[[2]]), ids[[2]], min = 1L)
  last_n <- svc_int(svc_one(query, "last_n"), "last_n", min = 1L, max = GL_MAX_LAST_N)
  if (!is.null(last_n) && (!is.null(lo) || !is.null(hi))) {
    svc_reject("last_n", "'last_n' cannot be combined with '%s' / '%s'.", ids[[1]], ids[[2]])
  }
  if (!is.null(lo) && !is.null(hi)) {
    if (lo > hi) svc_reject(ids[[1]], "'%s' (%d) is greater than '%s' (%d).", ids[[1]], lo, ids[[2]], hi)
    if (hi - lo + 1L > GL_MAX_ROUND_SPAN) {
      svc_reject(ids[[1]], "A '%s'-'%s' range spans at most %d games.", ids[[1]], ids[[2]], GL_MAX_ROUND_SPAN)
    }
  }
  txt <- function(x) if (is.null(x)) NULL else as.character(x)
  resolve_gn_last_n_values(txt(lo), txt(hi), txt(last_n))
}

# stat=<col>:<ge|le>:<value>, repeatable; <col> a value of the view's
# filterable columns. Israeli only.
gl_params_stat <- function(query, view) {
  vals <- svc_values(query, "stat")
  filterable <- unname(if (identical(view, "four_factors")) GL_FF_FILTERABLE_COLS else GL_SUMMARY_FILTERABLE_COLS)
  lapply(vals, function(v) {
    m <- regmatches(v, regexec("^(.+):([^:]+):([^:]+)$", v))[[1]]
    if (!length(m)) svc_reject("stat", "'stat' must be <column>:<ge|le>:<value>, got '%s'.", svc_show(v))
    svc_choice(m[[2]], "stat", filterable)
    svc_choice(m[[3]], "stat", c("ge", "le"))
    list(col = m[[2]], op = m[[3]], value = svc_number(m[[4]], "stat"))
  })
}

# Team ids as given (validated as ids, not looked up): an id the league does
# not have filters to nothing, as a stale Shiny selection would.
gl_param_ids <- function(query, name) {
  ids <- svc_int(svc_list(query, name), name, min = 1L)
  if (is.null(ids)) NULL else ids
}

# `query` -- see svc_params.R; `league` -- an API league id ("il",
# "euroleague", "eurocup"); `data_version` -- the league's ETL version, read
# with svc_data_version() unless given. Returns svc_game_logs()'s `p`, or
# signals an api_error() naming the first bad parameter.
svc_game_logs_params <- function(query, league, data_version = NULL) {
  lg <- svc_league(league)
  is_il <- identical(lg$league, "il")
  query <- query %||% list()
  svc_check_names(query, c(GL_PARAMS_COMMON, GL_PARAMS_LEAGUE[[lg$league]]))

  season <- svc_season(query, league)
  view <- svc_choice(svc_one(query, "view"), "view", names(GL_IL_VIEWS)) %||% "summary"
  dates <- svc_dates(query, svc_season_bounds(league, season))
  svc_format(query)
  svc_sort(query)

  team <- gl_param_ids(query, "team")
  teams <- gl_param_ids(query, "teams")
  if (!is.null(team) && length(team) > 1L) svc_reject("team", "'team' takes one team id; got %d.", length(team))
  game_type <- if (is_il) {
    codes <- svc_list(query, "game_type")
    if (!is.null(codes)) gl_game_type_choice(codes)
  }
  phase <- if (!is_il) {
    phases <- svc_list(query, "phase")
    if (!is.null(phases)) {
      svc_choice(phases, "phase", as.character(euro_fetch_phases(lg$competition, season)$phase))
    }
  }
  opponents <- gl_param_ids(query, "opponents")

  own <- gl_starters_value(query, "own_starters")
  opp <- gl_starters_value(query, "opp_starters")
  starters <- if (is_il) {
    gl_starters_bounds(own$mode, own$value, opp$mode, opp$value)
  } else {
    sb <- resolve_starters_bounds(own$mode, own$value, opp$mode, opp$value)
    list(off_min = sb$num_starters_off_min, off_max = sb$num_starters_off_max,
         def_min = sb$num_starters_def_min, def_max = sb$num_starters_def_max)
  }

  list(
    league = lg$league,
    competition = lg$competition,
    season = if (is_il) season else as.character(season),
    view = view,
    team = if (is_il) (if (is.null(team)) "" else as.character(team)),
    teams = if (!is_il) as.numeric(teams %||% numeric(0)),
    dates = dates,
    game_type = game_type,
    phase = phase,
    opponents = if (!is.null(opponents)) as.character(opponents),
    home_away = svc_choice(svc_one(query, "home_away"), "home_away", c("home", "away")) %||% "",
    outcome = svc_choice(svc_one(query, "outcome"), "outcome", c("win", "loss")) %||% "",
    starters = starters,
    gn = gl_params_gn(query, lg$league),
    stat_filters = if (is_il) gl_params_stat(query, view) else list(),
    data_version = data_version %||% svc_data_version(league)
  )
}
