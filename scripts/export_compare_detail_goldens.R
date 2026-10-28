# Original Compare rules over stored Shiny frames; no live database reads.
path <- 'app/tests/goldens/out/compare_details.rds'
export <- function(x) {
 dir.create('web/test/goldens/compare',recursive=TRUE,showWarnings=FALSE)
 jsonlite::write_json(x,'web/test/goldens/compare/details.json',dataframe='rows',auto_unbox=TRUE,na='null',null='null',digits=17)
}
if('--replay' %in% commandArgs(trailingOnly=TRUE)) {export(readRDS(path));quit(status=0)}
source('scripts/compare_r_sources.R')
for(name in c('get_val','cmp_team_players_scored')) {
 if(name=='get_val')eval(compare_assignments[[name]],compare_r)
}
read <- function(file) jsonlite::fromJSON(paste0('web/test/goldens/',file))
players_a <- read('player_stats/il_per_game.answer.json')$rows
players_b <- read('player_stats/il_clutch.answer.json')$rows
a <- players_a[players_a$team_id==2 & !is.na(players_a$team_id),,drop=FALSE]
b <- players_b[players_b$team_id==2 & !is.na(players_b$team_id),,drop=FALSE]
joined <- dplyr::full_join(compare_r$prep_team_player_side(a,'a'),compare_r$prep_team_player_side(b,'b'),by=c('team_id','player_id'))
joined$team_name<-dplyr::coalesce(joined$team_name_a,joined$team_name_b)
joined$player_name<-dplyr::coalesce(joined$player_name_a,joined$player_name_b)
compare_r$cmp_team_players_joined<-function()joined
compare_r$selected_team_player_metric<-function()metric
compare_r$cmp_min_poss<-function()minimum
cases<-list()
for(metric in unname(compare_r$TEAM_PLAYER_METRICS)) for(rate in c('Per Game','Per 60 Possessions','Per 30 Minutes','Totals')) for(reverse in c(FALSE,TRUE)) for(minimum in c(0,500)) {
 compare_r$input<-list(cmp_team_player_rate_mode=rate,cmp_preset=if(reverse)'date_split' else '')
 result<-eval(compare_assignments$cmp_team_players_scored[[3]][[2]],compare_r)
 cols<-c('team_id','player_id','team_name','player_name','metric_a','metric_b','poss_a','poss_b','gap','gap_score','played_a','played_b','rank')
 cases[[paste(metric,rate,reverse,minimum,sep='_')]]<-list(metric=metric,rate=rate,reverse=reverse,minimum=minimum,expected=result[,cols,drop=FALSE])
}
player_cases<-list()
for(i in seq_len(min(8,nrow(players_a))))for(rate in c('Per Game','Per 75 Possessions','Totals')) {
 row<-players_a[i,,drop=FALSE]
 values<-lapply(compare_r$PVP_STATS,function(stat)compare_r$get_val(row,stat));names(values)<-vapply(compare_r$PVP_STATS,`[[`,character(1),'col')
 player_cases[[paste(i,rate,sep='_')]]<-list(row=row,rate=rate,expected=values)
}
ratings<-read('team_ratings/il_summary_default.answer.json')$rows
ff<-read('team_ratings/il_four_factors_default.answer.json')$rows
lineups<-read('lineups/il_summary_default.answer.json')$answer$rows
details<-list()
for(i in 1:4) {
 r<-ratings[i,,drop=FALSE];f<-ff[ff$team_id==r$team_id,,drop=FALSE];s<-lineups[lineups$team_id==r$team_id,,drop=FALSE]
 shooting<-compare_r$add_shooting_rates(s);shots<-compare_r$add_team_shot_profile_shares(r)
 expected<-list()
 for(section in compare_r$DETAIL_METRICS)for(m in section$metrics)expected[[m$label]]<-compare_r$detail_extract_value(shots,f,shooting,m)
 details[[as.character(i)]]<-list(ratings=r,ff=f,shooting=s,expected=expected)
}
for(name in c('get_swing','swing'))eval(compare_assignments[[name]],compare_r)
ff_rows<-read('onoff/il_four_factors_default.answer.json')$answer$rows
onoff_rows<-read('onoff/il_shot_profile_default.answer.json')$answer$rows
swings<-list();shots<-list()
for(i in 1:8) {
 f<-ff_rows[i,,drop=FALSE];o<-onoff_rows[onoff_rows$player_id==f$player_id,,drop=FALSE]
 if(nrow(o))o<-o[1,,drop=FALSE] else o<-NULL
 values<-lapply(compare_r$FF_SWING_STATS,function(stat)compare_r$get_swing(f,o,stat));names(values)<-vapply(compare_r$FF_SWING_STATS,`[[`,character(1),'col')
 swings[[as.character(i)]]<-list(ff=f,onoff=o,expected=values)
 raw<-onoff_rows[i,,drop=FALSE];derived<-compare_r$add_player_shot_profile_shares(raw);expected<-list()
 for(side in c('off','def'))for(metric in c('efg','layup_share','dunk_share','rim_share','fg3_share','c3_pct3','mid_share'))expected[[paste(side,metric,sep='.')]]<-compare_r$swing(derived,side,metric)
 shots[[as.character(i)]]<-list(raw=raw,expected=expected)
}
formatted<-list()
for(value in c(-0.02,0.02,-1.25,1.25))for(fmt in c('net','pct','rtg','num'))formatted[[paste(value,fmt,sep='_')]]<-list(value=value,fmt=fmt,expected=compare_r$detail_fmt(value,fmt))
out<-list(a=a,b=b,teamPlayers=cases,players=player_cases,details=details,swings=swings,shots=shots,formatted=formatted)
saveRDS(out,path,compress='xz');export(out)
cat(length(cases),'team-player cases,',length(player_cases),'PvP cases and',length(details),'team detail cases exported\n')
