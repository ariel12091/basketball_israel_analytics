# The Game Logs presets as API queries: the one place the Shiny-input ->
# API-parameter mapping is written down. test-api-params.R proves each query
# gives exactly the `p` Shiny builds from the preset's inputs (preset_to_p());
# test-api-game-logs.R proves the endpoint's rows equal the preset's golden.

GL_PRESET_QUERIES <- c(
  il_summary_default = "season=2026",
  il_ff_default = "season=2026&view=four_factors",
  il_ff_away_loss_last10 = "season=2026&view=four_factors&home_away=away&outcome=loss&last_n=10",
  il_summary_empty = "season=2026&team=2&gn_min=30&gn_max=31",
  el_summary_default = "season=2025",
  el_ff_default = "season=2025&view=four_factors",
  il_summary_opps_statfilter =
    "season=2026&view=summary&opponents=2,3&date_from=2025-09-01&date_to=2026-07-01&stat=net_rtg:ge:0",
  il_ff_starters = "season=2026&view=four_factors&own_starters=gte:4&opp_starters=lte:2",
  il_summary_dates_type = "season=2026&game_type=5&date_from=2025-12-01&date_to=2026-01-31",
  il_ff_gn_win = "season=2026&view=four_factors&gn_min=5&gn_max=15&outcome=win",
  il_summary_last5_home = "season=2026&last_n=5&home_away=home",
  il_summary_team = "season=2026&team=2",
  el_ff_teams_phase = "season=2025&view=four_factors&teams=1,2&phase=RS",
  el_summary_round_starters = "season=2025&round_min=10&round_max=20&own_starters=gte:4",
  el_summary_last5_home = "season=2025&last_n=5&home_away=home"
)

# The API query for the preset named `name` (a GAME_LOGS_PRESETS name).
preset_to_query <- function(name) {
  q <- GL_PRESET_QUERIES[name]
  if (length(name) != 1L || is.na(q)) stop("preset_to_query: no query for preset '", name, "'")
  unname(q)
}

# The API league id a preset's league is served under.
preset_api_league <- function(preset) {
  if (identical(preset$league, "il")) "il" else "euroleague"
}
