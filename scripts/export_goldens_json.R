# Offline export: replay the exact Shiny query fixtures. Never changes goldens.
source('app/tests/goldens/harness.R')
source('app/tests/goldens/presets/game_logs.R')
conn <- golden_replay_conn('app/tests/goldens/fixtures/game_logs')
env <- golden_app_env(conn)
out <- 'web/test/goldens/game_logs'
dir.create(out, recursive = TRUE, showWarnings = FALSE)
readers <- env$gl_default_readers()
for (id in names(GAME_LOGS_PRESETS)) {
  preset <- GAME_LOGS_PRESETS[[id]]
  p <- preset_to_p(preset, env)
  input <- if (p$league == 'il') list(
    rows = readers$lineup_totals(p$season, 'golden'),
    ff = readers$lineup_ff(p$season, 'golden'),
    schedule = readers$schedule(p$season, 'golden'),
    scoreless = readers$scoreless('golden')
  ) else list(rows = readers$euro_rows(p$competition, p$season, 'golden'), scoreless = NULL)
  expected <- readRDS(file.path('app/tests/goldens/out/game_logs', paste0(id, '.rds')))
  service <- env$svc_game_logs(p, readers)
  jsonlite::write_json(list(preset=id, params=p, input=input, expected=expected, service=service),
    file.path(out, paste0(id,'.json')), dataframe='rows', auto_unbox=TRUE, digits=17, na='null', null='null')
  cat(id, '\n')
}
