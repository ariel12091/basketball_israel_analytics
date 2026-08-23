# App-role timing and privilege verification for the Israeli custom-clutch fact.
suppressPackageStartupMessages({library(DBI);library(RPostgres)})
file_arg<-grep("^--file=",commandArgs(trailingOnly=FALSE),value=TRUE)
script_path<-if(length(file_arg)) sub("^--file=","",file_arg[[1]]) else "scripts/x"
repo_root<-normalizePath(file.path(dirname(script_path),".."),winslash="/",mustWork=TRUE)
readRenviron(file.path(repo_root,"app",".Renviron"))
open_con<-function(){
 con<-dbConnect(Postgres(),host=Sys.getenv("PG_HOST"),port=6543L,dbname=Sys.getenv("PG_DB"),
  user=Sys.getenv("PG_USER"),password=Sys.getenv("PG_PASS"),sslmode=Sys.getenv("PG_SSLMODE","require"),
  bigint="numeric",connect_timeout=15L)
 dbBegin(con);dbExecute(con,"set local default_transaction_read_only=on");dbExecute(con,"set local statement_timeout='20s'");con
}
probe<-open_con();b<-dbGetQuery(probe,"select min(game_date) mn,max(game_date) mx from basketball_test.final_schedule_mv where game_year=2026")
profile<-dbGetQuery(probe,"select count(*) rows,count(distinct game_id) games,pg_total_relation_size('basketball_test.player_stats_actions_by_game') bytes from basketball_test.player_stats_actions_by_game")
print(profile,row.names=FALSE);dbRollback(probe);dbDisconnect(probe)
sig<-paste0("$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,$7::text,$8::text,",
 "$9::text,$10::int4,$11::text,$12::int4,$13::text,$14::int4,$15::bool,$16::int4,$17::int4,$18::int4")
sql<-paste0("select * from basketball_test.get_player_traditional_custom_clutch(",sig,")")
params<-function(margin,status,seconds,ot) list(2026L,as.Date(b$mn[[1]]),as.Date(b$mx[[1]]),
 NA_character_,NA_character_,NA_character_,"all","all","all",NA_integer_,"net",margin,status,seconds,ot,
 NA_integer_,NA_integer_,NA_integer_)
cases<-list(margin3_all_4m=params(3L,"all",240L,FALSE),trailing7_2m_filteredOT=params(7L,"trailing",120L,TRUE))
for(name in names(cases)) for(run in 1:3){
 con<-open_con();started<-proc.time()[["elapsed"]]
 x<-tryCatch(dbGetQuery(con,sql,params=cases[[name]]),error=function(e) structure(conditionMessage(e),class="bench_error"))
 seconds<-proc.time()[["elapsed"]]-started;try(dbRollback(con),silent=TRUE);dbDisconnect(con)
 if(inherits(x,"bench_error")) cat(sprintf("%s run=%d timeout_or_error=%s\n",name,run,x))
 else cat(sprintf("%s run=%d seconds=%.2f rows=%d\n",name,run,seconds,nrow(x)));flush.console()
}
deny<-function(label,sql){con<-open_con();ok<-tryCatch({dbGetQuery(con,sql);FALSE},error=function(e) grepl("permission denied",conditionMessage(e),fixed=TRUE));try(dbRollback(con),silent=TRUE);dbDisconnect(con);cat(sprintf("%s_denied=%s\n",label,ok));stopifnot(ok)}
deny("compute","select count(*) from basketball_test.compute_player_stats_actions_by_game(array[64942]::int4[])")
deny("refresh","select basketball_test.refresh_player_stats_actions_for_games(array[64942]::int4[])")
