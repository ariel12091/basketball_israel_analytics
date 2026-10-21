# Offline: export stored Shiny frames and their request inputs, never recompute
# or change the expected tables. Run after capture_goldens.R --tab onoff.
source("app/tests/goldens/harness.R")
source("app/tests/goldens/presets/onoff.R")
out <- "web/test/goldens/onoff"
dir.create(out, recursive = TRUE, showWarnings = FALSE)
for (id in names(ONOFF_PRESETS)) {
  p <- ONOFF_PRESETS[[id]]
  inp <- Reduce(utils::modifyList, golden_input_steps(p$inputs), init = list())
  euro <- p$league != "il"
  prefix <- if (euro) "euro" else "on"
  get <- function(suffix) inp[[paste0(prefix, "_", suffix)]]
  q <- list(season = if (euro) inp$euro_game_year else inp$game_year, view = p$view)
  dates <- if (euro) inp$euro_date_range else inp$date_range
  q$start <- as.character(dates[1]); q$end <- as.character(dates[2])
  teams <- if (euro) inp$euro_teams else inp$teams
  if (length(teams)) q$teams <- paste(teams, collapse = ",")
  for (key in c("home_away", "outcome", "opp_rank_side", "opp_rank_n", "opp_rank_metric", "last_n")) {
    v <- get(key)
    if (length(v)) q[[key]] <- as.character(v)
  }
  if (length(get("num_starters_off_mode")))
    q$own_starters <- paste(get("num_starters_off_mode"), get("num_starters_off"), sep = ":")
  filters <- list()
  if (length(get("add_stat_filter"))) {
    col <- if (grepl("player_stats$", id)) "ps_pts_pg" else p$has_col
    filters <- list(list(col = col, op = "ge", value = get("stat_filter_value")))
    q$stats <- jsonlite::toJSON(filters, auto_unbox = TRUE)
  }
  q$min_all <- "100"; q$min_on <- "300"
  expected <- readRDS(file.path("app/tests/goldens/out/onoff", paste0(id, ".rds")))
  jsonlite::write_json(list(id = id, league = p$league, view = p$view, query = q,
    filters = filters, expected = expected), file.path(out, paste0(id, ".json")),
    dataframe = "rows", auto_unbox = TRUE, digits = 17, na = "null", null = "null")
}
