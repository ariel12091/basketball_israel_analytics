source(repo_file('tests','goldens','harness.R'),local=TRUE)
source(repo_file('tests','goldens','presets','team_ratings.R'),local=TRUE)
for(id in names(TEAM_RATINGS_PRESETS))local({
 name<-id;p<-TEAM_RATINGS_PRESETS[[name]]
 test_that(paste('Team Ratings golden replays:',name),{
  path<-repo_file('tests','goldens','out','team_ratings',paste0(name,'.rds'))
  expect_true(file.exists(path),info=name)
  env<-golden_app_env(golden_replay_conn(repo_file('tests','goldens','fixtures','team_ratings'),GOLDEN_STRICT_READERS_BY_TAB$team_ratings))
  got<-golden_run_server(env,p$inputs,golden_capture_dt(p$slug,has_col=p$has_col))
  expect_identical(got,readRDS(path))
 })
})
