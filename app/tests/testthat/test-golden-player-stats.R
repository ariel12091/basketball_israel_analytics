source(repo_file('tests','goldens','harness.R'),local=TRUE)
source(repo_file('tests','goldens','presets','player_stats.R'),local=TRUE)
for(id in names(PLAYER_STATS_PRESETS))local({
 name<-id;p<-PLAYER_STATS_PRESETS[[name]]
 test_that(paste('Player Stats golden replays:',name),{
  path<-repo_file('tests','goldens','out','player_stats',paste0(name,'.rds'))
  expect_true(file.exists(path),info=name)
  env<-golden_app_env(golden_replay_conn(repo_file('tests','goldens','fixtures','player_stats'),GOLDEN_STRICT_READERS_BY_TAB$player_stats))
  got<-golden_run_server(env,p$inputs,golden_capture_dt(p$slug,has_col=p$has_col))
  expect_identical(got,readRDS(path))
 })
})
