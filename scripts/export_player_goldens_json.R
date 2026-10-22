# Export actual Shiny frames, without recomputing the expected result.
source('app/tests/goldens/harness.R')
source('app/tests/goldens/presets/player_stats.R')
out <- 'web/test/goldens/player_stats';dir.create(out,recursive=TRUE,showWarnings=FALSE)
for(id in names(PLAYER_STATS_PRESETS)) {
 p <- PLAYER_STATS_PRESETS[[id]]
 if('--partial' %in% commandArgs(trailingOnly=TRUE) && !file.exists(file.path('app/tests/goldens/out/player_stats',paste0(id,'.rds'))))next
 jsonlite::write_json(list(id=id,league=p$league,view=p$view,query=p$query,filters=p$filters,expected=readRDS(file.path('app/tests/goldens/out/player_stats',paste0(id,'.rds')))),file.path(out,paste0(id,'.json')),dataframe='rows',auto_unbox=TRUE,digits=17,na='null',null='null')
}
