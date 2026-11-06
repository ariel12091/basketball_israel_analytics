# Fixed bars: testServer does not acknowledge browser-side auto-slider updates.
LINEUPS_PRESETS <- list()
for (euro in c(FALSE, TRUE)) for (view in c("summary", "four_factors")) {
  league <- if (euro) "euroleague" else "il"
  p <- if (euro) "euro_ld" else "ld"
  base <- list(main_tabs = if (euro) "euro_lineups" else "lineup_data",
    league_select = if (euro) "E" else "il", game_year = "2026", euro_game_year = "2025")
  base[[paste0(p, "_view_mode")]] <- if (view == "summary") "Summary" else "Four Factors"
  base[[paste0(p, if (euro) "_date_range" else "_dates")]] <- as.Date(c("2025-09-01", "2026-07-01"))
  base[[paste0(p, if (euro) "_group_size" else "_num")]] <- "5"
  base[[paste0(p, "_minposs")]] <- 20
  team <- if (euro) "19" else "5"
  players <- if (euro) c("224", "227", "229") else c("1064", "1066", "1986")
  cases <- list(default = list(), pair = list(size = "2"), triple = list(size = "3"),
    quad = list(size = "4"), last5_home = list(last_n = "5", home_away = "home"),
    starters = list(own_starters = "gte:3"),
    clutch = list(clutch = "true"),
    team_any = list(team = team, players_on = players[1], players_any = paste(players[-1], collapse = ","), any_exact = "true"),
    stat_filter = list(stats = "net_rtg"))
  for (name in names(cases)) {
    q <- c(list(season = if (euro) "2025" else "2026", view = view, min_poss = "20"), cases[[name]])
    inp <- base
    if (!is.null(q$size)) inp[[paste0(p, if (euro) "_group_size" else "_num")]] <- q$size
    for (key in c("last_n", "home_away")) if (!is.null(q[[key]])) inp[[paste0(p, "_", key)]] <- q[[key]]
    if (!is.null(q$own_starters)) {
      inp[[paste0(p, "_num_starters_off_mode")]] <- "gte"
      inp[[paste0(p, "_num_starters_off")]] <- "3"
    }
    if (!is.null(q$clutch)) for (key in c("enabled", "margin", "status", "minutes", "ot_margin"))
      inp[[paste0(p, "_clutch_", key)]] <- switch(key, enabled = TRUE, margin = 5, status = "all", minutes = 5, ot_margin = FALSE)
    if (!is.null(q$team)) {
      inp[[paste0(p, "_lineup_filter-team")]] <- team
      inp[[paste0(p, "_lineup_filter-players_on")]] <- players[1]
      inp[[paste0(p, "_lineup_filter-players_on_any")]] <- players[-1]
      inp[[paste0(p, "_lineup_filter-players_on_any_exact")]] <- TRUE
    }
    filters <- list()
    if (!is.null(q$stats)) {
      filters <- list(list(col = "net_rtg", op = "ge", value = 0))
      q$stats <- jsonlite::toJSON(filters, auto_unbox = TRUE)
      step <- setNames(list(if (view == "summary") "Net RTG" else "Net", "ge", 0), paste0(p, c("_stat_filter_col", "_stat_filter_op", "_stat_filter_value")))
      inp <- list(inp, step, setNames(list(1), paste0(p, "_add_stat_filter")))
    }
    LINEUPS_PRESETS[[paste(league, view, name, sep = "_")]] <- list(
      league = league, view = view, query = q, filters = filters, inputs = inp,
      has_col = if (view == "summary") "num_starters" else "off_efg",
      render_pattern = if (euro) "euro-ld-unit" else "ld-lineup-link",
      allow_empty = FALSE, empty = FALSE)
  }
}
rm(euro, view, league, p, base, team, players, cases, name, q, inp, filters, key, step)
