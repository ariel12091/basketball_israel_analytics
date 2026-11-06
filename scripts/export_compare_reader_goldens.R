# Record the SQL signatures and parameter lists emitted by the unmodified R
# Compare readers. No database is contacted; empty frames stand in for answers.
source('scripts/compare_r_sources.R')
for(name in c('run_team_ratings','run_team_ff','cmp_lineup_query_args','merge_lineup_team_csv','cmp_apply_on_boxes','run_lineups_summary','run_lineups_ff','run_player_traditional','run_four_factors','run_onoff_impact','add_cmp_player_usage_pct'))
 eval(compare_assignments[[name]],compare_r)
for(e in parse('app/R/global.R')) if(is.call(e)&&identical(e[[1]],as.name('<-'))&&identical(e[[2]],as.name('DEFAULT_MIN_NET'))) eval(e,compare_r)
compare_r$shared <- list(season_date_bounds=function(gy)list(start=as.Date('2025-09-01'),end=as.Date('2026-07-01')))
compare_r$pg_pool <- NULL; compare_r$session <- NULL
compare_r$guard_heavy_request <- function(...) TRUE
compare_r$cmp_profile_time <- function(label,expr,...) force(expr)
calls <- list()
compare_r$db_get_query <- function(pool,sql,params) {
 calls[[length(calls)+1L]] <<- list(sql=sql,params=params)
 data.frame()
}
compare_r$run_compare_query <- function(key,p,sql,params) compare_r$db_get_query(NULL,sql,params)
contexts <- list(default=list(),home=list(home_away='home'),away_win=list(home_away='away',outcome='win'),
 own_hi=list(own_starters='gte:3'),own_lo=list(own_starters='lte:2'),opp=list(opp_starters='gte:2'),
 clutch=list(clutch='true',clutch_margin='5',clutch_minutes='5'),custom=list(clutch='true',clutch_margin='3',clutch_minutes='2'),
 top=list(opp_rank_side='top',opp_rank_n='4',opp_rank_metric='net'),bottom=list(opp_rank_side='bottom',opp_rank_n='6',opp_rank_metric='def'),
 dates=list(start='2025-11-01',end='2026-02-01',min_gn='3',max_gn='15'),
 context=list(game_type='5,16',opponents='2,5',teams='2',home_away='home'))
cases <- list()
for(kind in c('ratings','four_factors','lineups','lineup_ff','players','player_ff','onoff')) for(name in names(contexts)) {
 q <- c(list(season='2026',kind=kind),contexts[[name]])
 inp <- list(game_year='2026',main_tabs='compare',cmp_mode='Teams',
   cmp_dates=as.Date(c(q$start %||% '2025-09-01',q$end %||% '2026-07-01')),
   cmp_gn_min=q$min_gn,cmp_gn_max=q$max_gn,cmp_a_clutch=identical(q$clutch,'true'),
   cmp_a_clutch_margin=as.integer(q$clutch_margin %||% '5'),cmp_a_clutch_minutes=as.integer(q$clutch_minutes %||% '5'))
 for(k in c('home_away','outcome','opp_rank_side','opp_rank_n','opp_rank_metric'))inp[[paste0('cmp_a_',k)]]<-q[[k]]
 for(k in c('teams','opponents','game_type'))if(!is.null(q[[k]]))inp[[paste0('cmp_a_',k)]]<-strsplit(q[[k]],',',fixed=TRUE)[[1]]
 for(k in c('own','opp'))if(!is.null(q[[paste0(k,'_starters')]])) {
   xs<-strsplit(q[[paste0(k,'_starters')]],':',fixed=TRUE)[[1]];stem<-if(k=='own')'cmp_a_starters_' else 'cmp_a_opp_starters_'
   inp[[paste0(stem,'mode')]]<-xs[1];inp[[paste0(stem,'val')]]<-xs[2]
 }
 compare_r$input<-inp;p<-compare_r$collect_side_params('a')
 compare_r$cmp_lu_params<-function()list(num=5L,team_csv=NA_character_,player_csv=NA_character_,player_off_csv=NA_character_,exact=FALSE,has_any=FALSE)
 variant <- compare_r$team_ratings_preset_variant(p,compare_r$shared$season_date_bounds('2026'))
 calls<-list()
 if(kind!='ratings'||is.na(variant)) switch(kind,
  ratings=compare_r$run_team_ratings(p),four_factors=compare_r$run_team_ff(p),
  lineups=compare_r$run_lineups_summary(p,0L),lineup_ff=compare_r$run_lineups_ff(p,0L),
  players=compare_r$run_player_traditional(p,p$team_ids_csv,starters=TRUE),player_ff=compare_r$run_four_factors(p,p$team_ids_csv),onoff=compare_r$run_onoff_impact(p,p$team_ids_csv))
 if((kind!='ratings'||is.na(variant))&&!length(calls))stop('Reader did not execute: ',kind,'/',name)
 cases[[paste(kind,name,sep='_')]]<-list(query=q,variant=variant,calls=calls)
}
dir.create('web/test/goldens/compare',recursive=TRUE,showWarnings=FALSE)
saveRDS(cases,'app/tests/goldens/out/compare_readers.rds',compress='xz')
jsonlite::write_json(cases,'web/test/goldens/compare/readers.json',auto_unbox=TRUE,na='null',null='null',dataframe='rows',digits=17)
cat(length(cases),'Compare reader signatures exported\n')
