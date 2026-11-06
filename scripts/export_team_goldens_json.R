# Export the recorded Shiny frames without recomputing the expected result.
source('app/tests/goldens/harness.R')
source('app/tests/goldens/presets/team_ratings.R')
out <- 'web/test/goldens/team_ratings';dir.create(out,recursive=TRUE,showWarnings=FALSE)
partial <- '--partial' %in% commandArgs(trailingOnly=TRUE)
for(id in names(TEAM_RATINGS_PRESETS)) {
 p <- TEAM_RATINGS_PRESETS[[id]];path <- file.path('app/tests/goldens/out/team_ratings',paste0(id,'.rds'))
 if(partial&&!file.exists(path))next
 jsonlite::write_json(list(id=id,league=p$league,view=p$view,query=p$query,filters=p$filters,expected=readRDS(path)),file.path(out,paste0(id,'.json')),dataframe='rows',auto_unbox=TRUE,digits=17,na='null',null='null')
}
