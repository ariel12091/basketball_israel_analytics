# Record only with --record. Default/replay exports immutable recorded frames
# and original R helper results without contacting the database.
source('scripts/home_r_sources.R')
path<-'app/tests/goldens/out/home.rds'
if('--record' %in% commandArgs(trailingOnly=TRUE)) {
 suppressMessages({library(DBI);library(RPostgres)})
 readRenviron('app/.Renviron')
 con<-dbConnect(Postgres(),host=Sys.getenv('PG_HOST'),port=5432L,
  dbname=Sys.getenv('PG_DB'),user=Sys.getenv('PG_USER'),password=Sys.getenv('PG_PASS'),
  sslmode='require',bigint='numeric')
 stopifnot(identical(dbGetQuery(con,'SELECT current_user AS name')$name[[1]],'app_readonly'))
 dbExecute(con,'BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY')
 dbExecute(con,"SET LOCAL statement_timeout='20s'")
 frames<-list()
 for(season in c(2026L,2027L)) {
  games<-dbGetQuery(con,home_sql$hub_fetch_last_games,params=list(season))
  for(team in c(2L,5L)) {
   out<-dbGetQuery(con,home_sql$dashboard,params=list(season,team))
   stopifnot(nrow(out)==1L)
   df<-lapply(c('ratings','four_factors','onoff','traditional','lineups','storylines'),function(s)home_r$hub_parse_json_df(out[[paste0(s,'_json')]][[1]]))
   names(df)<-c('ratings','four_factors','onoff','traditional','lineups','storylines')
   df$last_games<-games
   frames[[paste(season,team,sep='_')]]<-list(season=season,team=as.character(team),frames=df,version=out$data_version[[1]])
  }
 }
 dbExecute(con,'COMMIT');dbDisconnect(con)
 dir.create(dirname(path),recursive=TRUE,showWarnings=FALSE)
 saveRDS(frames,path,compress='xz')
} else frames<-readRDS(path)
row_list<-function(r)if(is.null(r)||!nrow(r))NULL else as.list(r[1,,drop=FALSE])
expectations<-function(f,team) {
 identity<-home_r$hub_identity_data(f$ratings,f$four_factors,team)
 if(!is.null(identity)){identity$row<-row_list(identity$row);identity$ff<-row_list(identity$ff)}
 lineups<-home_r$hub_best_worst_lineups(f$lineups)
 if(!is.null(lineups))lineups<-lapply(lineups,row_list)
 pair<-function(id) {
  variants<-switch(id,starters_bench=c('starters_hi','starters_lo'),clutch=c('clutch','overall'),last10=c('last10','overall'),top_bottom_4=c('top4','bottom4'))
  result<-lapply(variants,function(v) {
   all<-if(v=='overall')f$ratings else f$storylines[f$storylines$hub_variant==v,,drop=FALSE]
   if(!nrow(all)||!('team_id'%in%names(all)))return(NULL)
   row<-all[as.integer(all$team_id)==as.integer(team),,drop=FALSE]
   if(!nrow(row))return(NULL)
   row<-row[1,,drop=FALSE];row$net_rtg_rank<-home_r$hub_net_rtg_rank(all,team);row
  });names(result)<-c('a','b');result
 }
 game<-home_r$hub_last_game(f$last_games,team)
 list(identity=identity,offense=home_r$hub_ff_mini(f$four_factors,team,'offense'),
  defense=home_r$hub_ff_mini(f$four_factors,team,'defense'),players=home_r$hub_key_players(f$onoff,team),
  scorer=row_list(home_r$hub_top_scorer(home_r$normalize_ts_result_cols(f$traditional),team)),lineups=lineups,
  storylines=home_r$hub_storyline_lines(home_r$hub_storyline_specs(),pair),
  lastGame=if(is.null(game))NULL else list(row=game,result=home_r$hub_last_game_result(game),context=home_r$hub_last_game_context(game)))
}
cases<-frames
base<-frames[[1]]
for(mode in c('empty','missing_team','below_threshold','ties','one_lineup','missing_values')) {
 g<-base
 if(mode=='empty')g$frames<-lapply(g$frames,function(d)d[FALSE,,drop=FALSE])
 if(mode=='missing_team')g$team<-'999'
 if(mode=='below_threshold') {
  g$frames$onoff[['ON Poss']]<-99
  g$frames$traditional$gp<-2
  g$frames$storylines$off_poss<-49;g$frames$storylines$def_poss<-50
 }
 if(mode=='ties') {
  g$frames$ratings$net_rtg<-5
  g$frames$storylines$net_rtg<-5
  g$frames$four_factors[,c('off_efg','off_tov','off_oreb','off_ftr','def_efg','def_tov','def_oreb','def_ftr')]<-50
  g$frames$lineups$net_rtg<-1
 }
 if(mode=='one_lineup')g$frames$lineups<-head(g$frames$lineups,1)
 if(mode=='missing_values') {
  g$frames$four_factors[1,c('off_efg','def_efg')]<-NA_real_
  g$frames$storylines$net_rtg[1]<-NA_real_
  if(nrow(g$frames$lineups))g$frames$lineups$off_poss[1]<-NA_real_
 }
 cases[[mode]]<-g
}
cases<-lapply(cases,function(g){g$expected<-expectations(g$frames,g$team);g})
dir.create('web/test/goldens/home',recursive=TRUE,showWarnings=FALSE)
write_json(cases,'web/test/goldens/home/cases.json',dataframe='rows',auto_unbox=TRUE,na='null',null='null',digits=17)
cat(length(cases),'Home rule cases exported from original R helpers\n')
