TEAM_RATINGS_PRESETS <- list()
labels <- c(summary='Summary',four_factors='Four Factors',shot_profile='Shot Profile',traditional='Traditional')
for (league in c('il','euroleague','eurocup')) {
  euro <- league != 'il'; p <- if(euro) 'euroteam' else 'tr'
  views <- if(euro) c('summary','four_factors') else names(labels)
  for(view in views) {
    cases <- if(league=='eurocup') list(default=list()) else list(
      default=list(), last5_home=list(last_n='5',home_away='home'),
      clutch=list(clutch='true'), starters=list(own_starters='gte:3'),
      dates=list(start='2025-11-01',end='2026-02-01'))
    if(view=='four_factors' && league!='eurocup') cases$custom_clutch <- list(clutch='true',clutch_margin='3',clutch_status='trailing',clutch_minutes='2',ot_margin='true')
    if(!euro) cases$stat_filter <- list(stats=if(view=='traditional') 'pts' else if(view=='shot_profile') 'off_rim_share' else 'net_rtg')
    if(view=='traditional') {
      cases$per75 <- list(display='per75'); cases$per40 <- list(display='per40')
      cases$defense <- list(side='defense'); cases$defense75 <- list(side='defense',display='per75')
      cases$defense40 <- list(side='defense',display='per40')
    }
    if(euro && league!='eurocup') cases$teams <- list(teams='19,2')
    for(name in names(cases)) {
      q <- c(list(season=if(euro)'2025' else '2026',view=view),cases[[name]])
      inp <- list(main_tabs=if(euro)'euro_team' else 'team_ratings',league_select=if(league=='eurocup')'U' else if(euro)'E' else 'il',game_year='2026',euro_game_year='2025')
      inp[[paste0(p,'_view_mode')]] <- labels[[view]]
      inp[[paste0(p,'_dates')]] <- as.Date(c(q$start %||% '2025-09-01',q$end %||% '2026-07-01'))
      for(k in c('last_n','home_away','teams')) if(!is.null(q[[k]])) inp[[paste0(p,'_',k)]] <- strsplit(q[[k]],',',fixed=TRUE)[[1]]
      if(!is.null(q$own_starters)) {inp[[paste0(p,'_num_starters_off_mode')]] <- 'gte'; inp[[paste0(p,'_num_starters_off')]] <- '3'}
      if(!is.null(q$clutch)) for(k in c('enabled','margin','status','minutes','ot_margin')) inp[[paste0(p,'_clutch_',k)]] <- switch(k,enabled=TRUE,margin=as.numeric(q$clutch_margin %||% '5'),status=q$clutch_status %||% 'all',minutes=as.numeric(q$clutch_minutes %||% '5'),ot_margin=identical(q$ot_margin,'true'))
      inp$tr_trad_defense_mode <- identical(q$side,'defense')
      inp$tr_trad_display_mode <- switch(q$display %||% 'per_game',per_game='Per Game',per75='Per 75 Possessions',per40='Per 40 Minutes')
      filters <- list()
      if(!is.null(q$stats)) {
        col <- q$stats; filters <- list(list(col=col,op='ge',value=if(col=='pts')80 else if(col=='off_rim_share')35 else 0))
        q$stats <- jsonlite::toJSON(filters,auto_unbox=TRUE)
        lab <- if(col=='pts')'PTS' else if(col=='off_rim_share')'Off Lay+Dunk%' else if(view=='summary')'Net Rtg' else 'Net'
        step <- setNames(list(lab,'ge',filters[[1]]$value),paste0(p,c('_stat_filter_col','_stat_filter_op','_stat_filter_value')))
        inp <- list(inp,step,setNames(list(1),paste0(p,'_add_stat_filter')))
      }
      slug <- if(euro) if(view=='summary')'euroleague_team_ratings' else 'euroleague_team_four_factors' else paste0('team_ratings_',view)
      TEAM_RATINGS_PRESETS[[paste(league,view,name,sep='_')]] <- list(league=league,view=view,query=q,filters=filters,inputs=inp,slug=slug,has_col=if(view=='traditional')'GP' else if(view=='shot_profile')'off_rim' else if(view=='four_factors')'off_efg' else 'rank_net_rtg',allow_empty=TRUE,empty=FALSE)
    }
  }
}
