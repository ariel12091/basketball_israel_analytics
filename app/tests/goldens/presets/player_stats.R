PLAYER_STATS_PRESETS <- list()
for(league in c('il','euroleague','eurocup')) {
  euro <- league!='il'
  cases <- list(per_game=list(),totals=list(view='Totals'),per60=list(view='Per 60 Possessions'),per30=list(view='Per 30 Minutes'),show_ineligible=list(view='Per 60 Possessions',show_ineligible='true'))
  if(league!='eurocup') cases <- c(cases,list(last5_home=list(last_n='5',home_away='home'),clutch=list(clutch='true'),custom_clutch=list(clutch='true',clutch_margin='3',clutch_status='trailing',clutch_minutes='2',ot_margin='true'),min_gp=list(min_gp='10'),teams=list(teams=if(euro)'19,2' else '1,2'),stat_filter=list(stats='pts')))
  for(name in names(cases)) {
    q <- modifyList(list(season=if(euro)'2025' else '2026',view='Per Game'),cases[[name]])
    inp <- list(main_tabs='traditional_stats',league_select=if(league=='eurocup')'U' else if(euro)'E' else 'il',game_year='2026',euro_game_year='2025',ts_display_mode=q$view,ts_dates=as.Date(c('2025-09-01','2026-07-01')),ts_min_gp=as.numeric(q$min_gp %||% '1'),ts_min_gp_slider=as.numeric(q$min_gp %||% '1'),ts_show_ineligible=identical(q$show_ineligible,'true'))
    for(k in c('last_n','home_away','teams'))if(!is.null(q[[k]]))inp[[paste0('ts_',k)]]<-strsplit(q[[k]],',',fixed=TRUE)[[1]]
    if(!is.null(q$clutch))for(k in c('enabled','margin','status','minutes','ot_margin'))inp[[paste0('ts_clutch_',k)]]<-switch(k,enabled=TRUE,margin=as.numeric(q$clutch_margin %||% '5'),status=q$clutch_status %||% 'all',minutes=as.numeric(q$clutch_minutes %||% '5'),ot_margin=identical(q$ot_margin,'true'))
    filters <- list()
    if(!is.null(q$stats)) {
      filters <- list(list(col='pts',op='ge',value=10))
      q$stats <- jsonlite::toJSON(filters,auto_unbox=TRUE)
      inp <- list(inp,list(ts_stat_filter_col='PTS',ts_stat_filter_op='ge',ts_stat_filter_value=10),list(ts_add_stat_filter=1))
    }
    PLAYER_STATS_PRESETS[[paste(league,name,sep='_')]] <- list(league=league,view=q$view,query=q,filters=filters,inputs=inp,slug=paste0(if(euro)'euroleague' else 'israeli_league','_player_stats'),has_col='Player',allow_empty=TRUE,empty=FALSE)
  }
}
