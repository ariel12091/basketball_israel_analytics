library(testthat)
skip_if_not(Sys.getenv('RUN_DB_TESTS','0')=='1')
skip_if_not_installed('DBI')
skip_if_not_installed('RPostgres')
skip_if_not_installed('jsonlite')
if(!grepl('^app_readonly([.]|$)',Sys.getenv('PG_USER'))) stop('Run the API proof as app_readonly')
api_con <- DBI::dbConnect(RPostgres::Postgres(),host=Sys.getenv('PG_HOST'),port=5432L,
 dbname=Sys.getenv('PG_DB'),user=Sys.getenv('PG_USER'),password=Sys.getenv('PG_PASS'),
 sslmode='require',connect_timeout=15,bigint='numeric')
withr::defer(DBI::dbDisconnect(api_con))
get_payload <- function(sql,params) {
 jsonlite::fromJSON(DBI::dbGetQuery(api_con,sql,params=params)$payload[[1]])
}
test_that('version reports all league publications', {
 v <- DBI::dbGetQuery(api_con,'SELECT * FROM app_api.version()')
 expect_setequal(v$league,c('il','euroleague','eurocup'))
 expect_true(all(v$version>=1))
})
test_that('Israeli additive readers preserve both source totals and empty metadata', {
 p <- get_payload('SELECT app_api.game_logs_il($1)::text AS payload',list(2026L))
 expect_gt(nrow(p$rows),0L)
 expect_equal(p$row_count,nrow(p$rows))
 expect_equal(p$ctl_poss,sum(p$rows$total_poss,na.rm=TRUE))
 ref <- DBI::dbGetQuery(api_con,'SELECT sum(total_poss)::float8 AS poss,sum(total_pts)::float8 AS pts,sum(minutes)::float8 AS minutes FROM basketball_test.mv_lineup_totals_by_day WHERE game_year=2026')
 expect_equal(sum(p$rows$total_poss,na.rm=TRUE),ref$poss)
 expect_equal(sum(p$rows$total_pts,na.rm=TRUE),ref$pts)
 expect_equal(sum(p$rows$minutes,na.rm=TRUE),ref$minutes,tolerance=1e-10)
 ff <- DBI::dbGetQuery(api_con,'SELECT sum(total_poss)::float8 AS poss,sum(total_points)::float8 AS pts,sum(minutes)::float8 AS minutes FROM basketball_test.lineup_four_factors_by_game WHERE game_year=2026')
 expect_equal(sum(p$rows$ff_total_poss,na.rm=TRUE),ff$poss)
 expect_equal(sum(p$rows$total_points,na.rm=TRUE),ff$pts)
 expect_equal(sum(p$rows$ff_minutes,na.rm=TRUE),ff$minutes,tolerance=1e-10)
 e <- get_payload('SELECT app_api.game_logs_il($1)::text AS payload',list(1900L))
 expect_length(e$rows,0L); expect_equal(e$row_count,0); expect_equal(e$ctl_poss,0)
 expect_true(e$state %in% c('published','publishing','failed','stale'))
 expect_gte(e$version,1)
 cat('\n[API payload bytes] il/2026=',nchar(jsonlite::toJSON(p,auto_unbox=TRUE,digits=NA),type='bytes'),'\n',sep='')
})
test_that('European native readers preserve row multiplicity and league scope', {
 for(comp in c('E','U')) {
   p <- get_payload('SELECT app_api.game_logs_euro($1,$2)::text AS payload',list(comp,2025L))
   ref <- DBI::dbGetQuery(api_con,"SELECT count(*)::integer AS n,sum(f.off_poss)::float8 AS poss FROM euroleague.team_four_factors_by_game f JOIN euroleague.final_schedule_mv s USING(game_id,team_id) WHERE s.competition=$1 AND s.game_year=$2",params=list(comp,2025L))
   expect_equal(p$row_count,ref$n)
   expect_equal(p$ctl_poss,if(is.na(ref$poss)) 0 else ref$poss)
   if(p$row_count>0) expect_true(all(c('own_starters','opp_starters','off_poss','def_poss') %in% names(p$rows)))
   cat('\n[API payload bytes] ',comp,'/2025=',nchar(jsonlite::toJSON(p,auto_unbox=TRUE,digits=NA),type='bytes'),'\n',sep='')
 }
})
