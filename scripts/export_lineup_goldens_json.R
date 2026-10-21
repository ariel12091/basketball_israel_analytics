# Offline conversion of real Shiny-rendered Lineups frames.
source("app/tests/goldens/harness.R")
source("app/tests/goldens/presets/lineups.R")
out <- "web/test/goldens/lineups"
dir.create(out, recursive = TRUE, showWarnings = FALSE)
for (id in names(LINEUPS_PRESETS)) {
  p <- LINEUPS_PRESETS[[id]]
  expected <- readRDS(file.path("app/tests/goldens/out/lineups", paste0(id, ".rds")))
  jsonlite::write_json(list(id = id, league = p$league, view = p$view,
    query = p$query, filters = p$filters, expected = expected),
    file.path(out, paste0(id, ".json")), dataframe = "rows", auto_unbox = TRUE,
    digits = 17, na = "null", null = "null")
}
