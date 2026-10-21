# Real Shiny On/Off frames. Explicit bars: testServer does not apply slider
# updates, so these presets deliberately test fixed 100/300 possession bars.
# Auto-min state transitions are covered separately by browser interactions.
onoff_base <- function(euro, view, ...) {
  if (euro) c(list(
    main_tabs = "euro", league_select = "E", euro_game_year = "2025", game_year = "2026",
    euro_view_mode = view, euro_date_range = as.Date(c("2025-09-01", "2026-07-01")),
    euro_min_all_poss = 100, euro_min_on_poss = 300
  ), list(...)) else c(list(
    main_tabs = "onoff", league_select = "il", game_year = "2026",
    onoff_view_mode = view, date_range = as.Date(c("2025-09-01", "2026-07-01")),
    min_all_poss = 100, min_on_poss = 300
  ), list(...))
}

ONOFF_PRESETS <- list()
for (euro in c(FALSE, TRUE)) {
  prefix <- if (euro) "euro" else "on"
  league <- if (euro) "euroleague" else "il"
  for (view in c("Summary", "Four Factors", if (!euro) "Shot Profile")) {
    key <- c(Summary = "summary", `Four Factors` = "four_factors", `Shot Profile` = "shot_profile")[[view]]
    has_col <- c(Summary = "Net RTG Diff", `Four Factors` = "Net Diff", `Shot Profile` = "Off Lay+Dunk Diff")[[view]]
    cases <- list(
      default = list(),
      home = setNames(list("home"), paste0(prefix, "_home_away")),
      teams = setNames(list(c("2", "3")), if (euro) "euro_teams" else "teams"),
      teams_win = c(setNames(list(c("2", "3")), if (euro) "euro_teams" else "teams"),
                    setNames(list("win"), paste0(prefix, "_outcome"))),
      last5 = setNames(list("5"), paste0(prefix, "_last_n")),
      starters = setNames(list("gte", "3"), paste0(prefix, c("_num_starters_off_mode", "_num_starters_off"))),
      opp_rank = setNames(list("top", "5", "net"), paste0(prefix, c("_opp_rank_side", "_opp_rank_n", "_opp_rank_metric")))
    )
    for (name in names(cases)) {
      ONOFF_PRESETS[[paste(league, key, name, sep = "_")]] <- list(
        league = league, view = key, covers = c("ON-01", "ON-02", "ON-03", "ON-04", "ON-05"),
        inputs = do.call(onoff_base, c(list(euro, view), cases[[name]])), has_col = has_col,
        allow_empty = euro && name == "last5"
      )
    }
    for (ps in c(FALSE, TRUE)) {
      filter_col <- if (ps) "PTS/G" else if (view == "Shot Profile") "Off Lay+Dunk Delta" else if (view == "Summary") "Net" else "Net Diff"
      inputs <- list(onoff_base(euro, view),
        setNames(list(filter_col, "ge", if (ps) 15 else 0), paste0(prefix, c("_stat_filter_col", "_stat_filter_op", "_stat_filter_value"))),
        setNames(list(1), paste0(prefix, "_add_stat_filter")))
      ONOFF_PRESETS[[paste(league, key, if (ps) "player_stats" else "stat_filter", sep = "_")]] <- list(
        league = league, view = key, covers = if (ps) "ON-12" else "ON-04",
        inputs = inputs, has_col = has_col
      )
    }
  }
}
rm(euro, prefix, league, view, key, has_col, cases, name, ps, filter_col, inputs)
