# Real read-only Shiny query inputs and pure-transform outputs. --record is
# explicit; replay/export is offline and never synthesizes expected results.
source('scripts/gameflow_r_sources.R')
gameflow_export_sources()
root <- 'app/tests/goldens';dir.create(file.path(root,'fixtures/gameflow'),recursive=TRUE,showWarnings=FALSE)
dir.create(file.path(root,'out/gameflow'),recursive=TRUE,showWarnings=FALSE)
jsondir <- 'web/test/goldens/gameflow';dir.create(jsondir,recursive=TRUE,showWarnings=FALSE)
record <- '--record' %in% commandArgs(trailingOnly=TRUE)
refresh_output <- '--refresh-output' %in% commandArgs(trailingOnly=TRUE)
if(record) {
 readRenviron('app/.Renviron')
 con <- DBI::dbConnect(RPostgres::Postgres(),host=Sys.getenv('PG_HOST'),port=5432L,dbname=Sys.getenv('PG_DB'),user=Sys.getenv('PG_USER'),password=Sys.getenv('PG_PASS'),sslmode='require',bigint='numeric',connect_timeout=15L)
 stopifnot(identical(DBI::dbGetQuery(con,'SELECT current_user AS role')$role,'app_readonly'))
 DBI::dbExecute(con,'SET default_transaction_read_only=on');DBI::dbExecute(con,'SET statement_timeout=20000')
 cases <- list()
 for(league in c('il','euroleague','eurocup')) {
  euro <- league!='il';comp <- if(league=='eurocup')'U' else 'E'
  schedule <- if(euro)'euroleague.final_schedule_mv' else 'basketball_test.final_schedule_mv'
  where <- if(euro)sprintf("competition='%s' AND game_year=2025",comp) else 'game_year=2026'
  for(kind in c('regular','overtime')) {
   extra <- if(kind=='regular')'' else if(euro)' AND game_id IN (SELECT game_id FROM euroleague.ribbon_margin_v WHERE period>4)' else ' AND game_id IN (SELECT game_id FROM basketball_test.df_pts_poss_lineups_longer_mv WHERE quarter>4)'
   pick <- DBI::dbGetQuery(con,paste0('SELECT game_id,team_id,team_name,opp_team_name FROM ',schedule,' WHERE ',where,extra,' ORDER BY game_id DESC,team_id LIMIT 1'))
   if(nrow(pick))cases[[paste(league,kind,sep='_')]]<-c(list(league=league),as.list(pick[1,]))
  }
 }
 for(id in c(184,406)) {
  pick<-DBI::dbGetQuery(con,'SELECT game_id,team_id,team_name,opp_team_name FROM basketball_test.final_schedule_mv WHERE game_id=$1 ORDER BY team_id',params=list(as.integer(id)))
  for(i in seq_len(nrow(pick)))cases[[paste0('il_',id,'_',i)]]<-c(list(league='il'),as.list(pick[i,]))
 }
 for(name in names(cases)) {
  p<-cases[[name]]
  raw<-DBI::dbGetQuery(con,if(p$league=='il')gameflow_r$RIBBON_SQL_ISRAEL else gameflow_r$RIBBON_SQL_EURO,params=list(as.integer(p$game_id),as.integer(p$team_id)))
  saveRDS(list(case=p,raw=raw),file.path(root,'fixtures/gameflow',paste0(name,'.rds')),compress='xz')
  cat(name,'recorded\n')
 }
 DBI::dbDisconnect(con)
}
for(file in list.files(file.path(root,'fixtures/gameflow'),pattern='[.]rds$',full.names=TRUE)) {
 input<-readRDS(file);p<-input$case;raw<-input$raw
 lanes<-gameflow_r$ribbon_mark_starters(gameflow_r$ribbon_normalise_lanes(jsonlite::fromJSON(raw$lanes[[1]]),p$team_id))
 steps<-jsonlite::fromJSON(raw$margin[[1]])
 bounds<-gameflow_r$ribbon_period_bounds(raw$n_periods[[1]])
 margin<-gameflow_r$ribbon_complete_margin(steps,tail(bounds,1),bounds)
 bars<-gameflow_r$ribbon_lane_index(gameflow_r$ribbon_side_perspective(gameflow_r$ribbon_stint_points(gameflow_r$merge_adjacent_stints(lanes),steps)))
 dict<-gameflow_r$ribbon_lineup_dictionary(lanes)
 details<-lapply(seq_len(nrow(bars)),function(i)gameflow_r$ribbon_window_detail(lanes,steps,dict,bars$side[i],bars$player_key[i],bars$player_label[i],bars$start_elapsed[i],bars$end_elapsed[i]))
 period_details<-lapply(seq_len(nrow(bars)),function(i)jsonlite::fromJSON(gameflow_r$ribbon_period_details(lanes,steps,dict,bars$side[i],bars$player_key[i],bars$player_label[i],bars$start_elapsed[i],bars$end_elapsed[i],bounds),simplifyVector=FALSE))
 expected<-list(lanes=lanes,bars=bars,margin=margin,dict=dict,totals=gameflow_r$ribbon_player_totals(bars),details=details,periodDetails=period_details,
  health=gameflow_r$ribbon_game_warning(if(p$league=='il')'israel' else 'euroleague',p$game_id,gameflow_r$ribbon_health_message(raw$excluded_segments[[1]],raw$false_straddle_segments[[1]],raw$false_straddle_seconds[[1]])))
 path<-file.path(root,'out/gameflow',basename(file))
 if(record || refresh_output)saveRDS(expected,path,compress='xz') else stopifnot(identical(expected,readRDS(path)))
 jsonlite::write_json(list(case=p,raw=list(lanes=jsonlite::fromJSON(raw$lanes[[1]]),margin=steps,n_periods=raw$n_periods[[1]],excluded_segments=raw$excluded_segments[[1]],false_straddle_segments=raw$false_straddle_segments[[1]],false_straddle_seconds=raw$false_straddle_seconds[[1]]),expected=readRDS(path)),file.path(jsondir,sub('[.]rds$','.json',basename(file))),dataframe='rows',auto_unbox=TRUE,na='null',null='null',digits=17)
 cat(basename(file),'replay/export matches\n')
}
