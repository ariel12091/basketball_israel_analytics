# Offline rule oracles from the original Compare functions and previously
# recorded Shiny SQL frames. This verifies derived rules, not Compare SQL wiring.
if('--replay' %in% commandArgs(trailingOnly=TRUE)) {
  dir.create('web/test/goldens/compare',recursive=TRUE,showWarnings=FALSE)
  jsonlite::write_json(readRDS('app/tests/goldens/out/compare_rules.rds'),'web/test/goldens/compare/rules.json',dataframe='rows',auto_unbox=TRUE,na='null',null='null',digits=17)
  quit(status=0)
}
source('scripts/compare_r_sources.R')
for(name in c('cmp_joined_inner','cmp_metric_lower_is_better','normalize_teams_ref'))
  eval(compare_assignments[[name]],compare_r)
compare_r$cmp_side_state <- function() list(a=list(side='a',team_ids_csv=NA_character_),b=list(side='b',team_ids_csv=NA_character_))
compare_r$run_team_ratings <- compare_r$run_team_ff <- compare_r$run_lineups_summary <- compare_r$run_lineups_ff <- function(p,...) datasets[[if(p$side=='a')source_a else source_b]]
compare_r$selected_metric <- function() metric
compare_r$cmp_joined <- function() result
compare_r$cmp_summary_stats <- function() summary
teams <- jsonlite::fromJSON('web/test/goldens/team_ratings/il_summary_default.answer.json')$rows[,c('team_id','team_name')]
compare_r$cmp_ref <- list(teams=teams)
datasets <- list(); cases <- list()
root <- 'web/test/goldens'
for(mode in c('Teams','Lineups')) for(view in c('summary','four_factors')) {
  variants <- if(mode=='Teams') c('default','dates','starters') else c('default','pair','triple','quad')
  for(variant in variants) {
    source_a <- paste0(if(mode=='Teams')'team_ratings/' else 'lineups/','il_',view,'_default.answer.json')
    source_b <- paste0(if(mode=='Teams')'team_ratings/' else 'lineups/','il_',view,'_',variant,'.answer.json')
    if(mode=='Lineups') {
      source_a <- source_b
      if(variant=='default') source_b <- paste0('lineups/il_',view,'_last5_home.answer.json')
    }
    for(file in c(source_a,source_b)) if(is.null(datasets[[file]])) {
      saved <- jsonlite::fromJSON(file.path(root,file))
      datasets[[file]] <- if(is.null(saved$answer)) saved$rows else saved$answer$rows
    }
    metrics <- if(view=='summary') c('net_rtg','off_ppp','def_ppp') else c('off_efg','off_tov','off_oreb','off_ftr')
    for(metric in metrics) for(reverse in c(FALSE,TRUE)) {
      compare_r$input <- list(main_tabs='compare',cmp_mode=mode)
      joined <- compare_r$cmp_joined_inner(FALSE,FALSE,0L)
      result <- compare_r$finalize_cmp_joined(joined,mode,reverse,min_poss=10L)
      if(is.null(result)) result <- data.frame()
      summary <- if(nrow(result)) eval(compare_assignments$cmp_summary_stats[[3]][[2]],compare_r) else NULL
      winner <- if(nrow(result)) eval(compare_assignments$cmp_summary_winner[[3]][[2]],compare_r) else NULL
      cols <- intersect(c('team_id','team_name','sub_lineup_hash','entity_name','metric_a','metric_b','poss_a','poss_b','gap','rank'),names(result))
      id <- paste(mode,view,variant,metric,reverse,sep='_')
      cases[[id]] <- list(mode=mode,metric=metric,reverse=reverse,a=source_a,b=source_b,minimum=10,
        autoMin=compare_r$cmp_auto_minposs_from_df(joined),expected=result[,cols,drop=FALSE],summary=summary,winner=winner)
    }
  }
}
out <- list(datasets=datasets,cases=cases,teams=teams)
dir.create('web/test/goldens/compare',recursive=TRUE,showWarnings=FALSE)
saveRDS(out,'app/tests/goldens/out/compare_rules.rds',compress='xz')
jsonlite::write_json(out,'web/test/goldens/compare/rules.json',dataframe='rows',auto_unbox=TRUE,na='null',null='null',digits=17)
cat(length(cases),'Compare rule cases exported\n')
