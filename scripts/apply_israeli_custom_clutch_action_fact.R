# Validates and optionally applies the Israeli narrow action fact for custom
# Player Stats clutch reads. Default behavior rolls the transaction back.

suppressPackageStartupMessages({ library(DBI); library(RPostgres) })
file_arg <- grep("^--file=", commandArgs(trailingOnly=FALSE), value=TRUE)
script_path <- if(length(file_arg)) sub("^--file=","",file_arg[[1]]) else "scripts/x"
repo_root <- normalizePath(file.path(dirname(script_path),".."),winslash="/",mustWork=TRUE)
readRenviron(file.path(repo_root,"etl",".Renviron"))
read_sql <- function(path) paste(readLines(file.path(repo_root,path),warn=FALSE),collapse="\n")
execute_simple_file <- function(con,path) {
  statements <- trimws(strsplit(read_sql(path),";",fixed=TRUE)[[1]])
  statements <- statements[nzchar(statements)]
  invisible(lapply(statements,function(sql) dbExecute(con,sql)))
}
timed <- function(label,expr) {
  started <- proc.time()[["elapsed"]]; value <- force(expr)
  cat(sprintf("%-38s %.2fs\n",label,proc.time()[["elapsed"]]-started));flush.console();value
}
con <- dbConnect(Postgres(),host=Sys.getenv("PG_HOST"),port=5432L,
  dbname=Sys.getenv("PG_DB"),user=Sys.getenv("PG_USER"),password=Sys.getenv("PG_PASS"),
  sslmode=Sys.getenv("PG_SSLMODE","require"),bigint="numeric",connect_timeout=15L)
on.exit(if(dbIsValid(con)) dbDisconnect(con),add=TRUE)
confirm_apply <- identical(Sys.getenv("CONFIRM_ISRAELI_CUSTOM_CLUTCH_APPLY","0"),"1")
exists <- dbGetQuery(con,"select to_regclass('basketball_test.player_stats_actions_by_game') is not null present")$present[[1]]
if(isTRUE(exists) && !identical(Sys.getenv("ALLOW_CUSTOM_CLUTCH_REBUILD","0"),"1"))
  stop("player_stats_actions_by_game already exists; refusing accidental rebuild")

dbBegin(con); finished <- FALSE
on.exit(if(!finished) try(dbRollback(con),silent=TRUE),add=TRUE)
dbExecute(con,"SET LOCAL search_path TO basketball_test, public")
dbExecute(con,"SET LOCAL lock_timeout='5s'")
dbExecute(con,"SET LOCAL statement_timeout='240s'")
timed("install action compute",dbExecute(con,read_sql("sql/functions/compute_player_stats_actions_by_game.sql")))
timed("build narrow action fact",execute_simple_file(con,"sql/materialized_views/player_stats_actions_by_game.sql"))
timed("install incremental refresh",dbExecute(con,read_sql("sql/functions/refresh_player_stats_actions_for_games.sql")))
reader_sql <- read_sql("sql/functions/get_player_traditional_custom_clutch.sql")
create_at <- regexpr("CREATE OR REPLACE FUNCTION",reader_sql,fixed=TRUE)[[1]]
dbExecute(con,paste0("DROP FUNCTION IF EXISTS basketball_test.get_player_traditional_custom_clutch(",
  "INT,DATE,DATE,TEXT,TEXT,TEXT,TEXT,TEXT,TEXT,INT,TEXT,INT,TEXT,INT,BOOLEAN,INT,INT,INT)"))
timed("install custom reader",dbExecute(con,substring(reader_sql,create_at)))

profile <- dbGetQuery(con,"select count(*) rows,count(distinct game_id) games,
 pg_relation_size('basketball_test.player_stats_actions_by_game') table_bytes,
 pg_indexes_size('basketball_test.player_stats_actions_by_game') index_bytes,
 pg_total_relation_size('basketball_test.player_stats_actions_by_game') total_bytes
 from basketball_test.player_stats_actions_by_game")
print(profile,row.names=FALSE)
stopifnot(profile$rows[[1]]>0,profile$games[[1]]>0)

sig <- paste0("$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,$7::text,$8::text,",
 "$9::text,$10::int4,$11::text,$12::int4,$13::text,$14::int4,$15::bool,",
 "$16::int4,$17::int4,$18::int4")
old_sql <- paste0("select * from basketball_test.get_player_traditional_dynamic(",sig,")")
new_sql <- paste0("select * from basketball_test.get_player_traditional_custom_clutch(",sig,")")
bounds <- dbGetQuery(con,"select min(game_date) mn,max(game_date) mx from basketball_test.final_schedule_mv where game_year=2026")
params <- function(margin,status,seconds,ot,last_n=NA_integer_) list(2026L,as.Date(bounds$mn[[1]]),as.Date(bounds$mx[[1]]),
  NA_character_,NA_character_,NA_character_,"all","all","all",NA_integer_,"net",
  margin,status,seconds,ot,NA_integer_,NA_integer_,last_n)
compare_case <- function(label,p) {
  new <- timed(paste(label,"candidate"),dbGetQuery(con,new_sql,params=p))
  old <- timed(paste(label,"legacy"),dbGetQuery(con,old_sql,params=p))
  key <- function(x) {x<-x[order(x$team_id,x$player_id),,drop=FALSE];rownames(x)<-NULL;x}
  new<-key(new);old<-key(old)
  same <- isTRUE(all.equal(new,old,check.attributes=FALSE,tolerance=0))
  cat(sprintf("%-38s rows=%d exact=%s\n",paste(label,"parity"),nrow(new),same))
  if(!same){print(all.equal(new,old,check.attributes=FALSE,tolerance=0));stop("parity failed")}
}
compare_case("margin3_all_4m",params(3L,"all",240L,FALSE))
compare_case("trailing7_2m_filteredOT",params(7L,"trailing",120L,TRUE))
compare_case("leading5_5m_filteredOT_last5",params(5L,"leading",300L,TRUE,5L))
compare_case("time_only_2m_last5",params(NA_integer_,"all",120L,FALSE,5L))

sample_game <- dbGetQuery(con,"select max(game_id)::int game_id from basketball_test.player_stats_actions_by_game")$game_id[[1]]
touched <- timed("single-game refresh",dbGetQuery(con,
  "select basketball_test.refresh_player_stats_actions_for_games(array[$1]::int4[]) n",
  params=list(as.integer(sample_game))))$n[[1]]
cat(sprintf("incremental game_id=%d touched=%s\n",sample_game,touched))

if(confirm_apply) {
  dbExecute(con,"REVOKE ALL ON FUNCTION basketball_test.compute_player_stats_actions_by_game(int4[]) FROM PUBLIC")
  dbExecute(con,"REVOKE ALL ON FUNCTION basketball_test.refresh_player_stats_actions_for_games(int4[]) FROM PUBLIC")
  reader_sig <- paste0("basketball_test.get_player_traditional_custom_clutch(",
    "int4,date,date,text,text,text,text,text,text,int4,text,int4,text,int4,bool,int4,int4,int4)")
  dbExecute(con,sprintf("REVOKE ALL ON FUNCTION %s FROM PUBLIC",reader_sig))
  dbExecute(con,sprintf("GRANT EXECUTE ON FUNCTION %s TO app_readonly",reader_sig))
  dbExecute(con,"GRANT SELECT ON basketball_test.player_stats_actions_by_game TO app_readonly")
  dbExecute(con,"ALTER TABLE basketball_test.player_stats_actions_by_game ENABLE ROW LEVEL SECURITY")
  dbExecute(con,"CREATE POLICY app_readonly_select_all ON basketball_test.player_stats_actions_by_game FOR SELECT TO app_readonly USING(true)")
  dbCommit(con);cat("migration=committed\n")
} else {dbRollback(con);cat("migration=rolled_back persistent_changes=false\n")}
finished <- TRUE
