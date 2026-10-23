# Home's pure definitions and exact SQL, loaded without app startup or DB I/O.
suppressMessages({library(shiny);library(dplyr);library(htmltools);library(jsonlite)})
home_r <- new.env(parent=globalenv())
for(file in c('app/R/catalogues_generated.R','app/R/helpers.R','app/R/mod_team_hub.R','app/R/ui_tab0_home.R'))
 for(e in parse(file)) if(is.call(e)&&identical(e[[1]],as.name('<-'))&&is.name(e[[2]])) {
  name<-as.character(e[[2]])
  if((is.call(e[[3]])&&identical(e[[3]][[1]],as.name('function'))) ||
     name %in% c('STATIC_TEAM_ROSTERS','HUB_TOP_SCORER_MIN_GP')) eval(e,home_r)
 }
home_walk <- function(x, visit) {
 visit(x)
 if(is.call(x)||is.pairlist(x)||is.expression(x)) {
  a<-as.list(x)
  for(i in seq_along(a)) if(!identical(a[[i]],quote(expr=))) home_walk(a[[i]],visit)
 }
}
home_sql <- list()
home_r$cached_season_df <- function(key,fn) fn()
home_r$db_get_query <- home_r$hub_db_get_query_timed <- function(conn=NULL,statement=NULL,params=NULL,...) {
 # hub_db_get_query_timed() receives SQL as its first positional argument.
 if(is.null(statement)) statement<-conn
 home_sql[[length(home_sql)+1L]] <<- statement
 data.frame()
}
home_r$pg_pool <- NULL
for(name in c('hub_fetch_team_ratings','hub_fetch_last_games','hub_fetch_team_ff','hub_fetch_team_ratings_presets')) {
 home_r[[name]](2026L,'offline')
 home_sql[[name]] <- home_sql[[length(home_sql)]]
}
home_sql$dashboard <- home_r$hub_dashboard_query_sql()
home_sql$storylines_dynamic <- home_r$hub_storyline_variants_sql()
# The legacy readers are nested reactives. Extract their literal query calls.
home_assignments<-list()
home_walk(body(home_r$server_team_hub),function(x) {
 if(is.call(x)&&identical(x[[1]],as.name('<-'))&&is.name(x[[2]]))
  home_assignments[[as.character(x[[2]])]]<<-x
})
for(name in c('hub_onoff_df','hub_ts_df','hub_lineups_df')) {
 found<-NULL
 home_walk(home_assignments[[name]],function(x) {
  if(is.call(x)&&identical(x[[1]],as.name('db_get_query')))found<<-eval(x[[3]],home_r)
 })
 stopifnot(is.character(found),length(found)==1L)
 home_sql[[name]]<-found
}
home_sql<-home_sql[!is.na(names(home_sql)) & nzchar(names(home_sql))]

home_export_sources <- function() {
 # Evaluate card arguments straight from ui_tab0_home(), not copies of its text.
 cards<-list()
 home_walk(body(home_r$ui_tab0_home),function(x) {
  if(is.call(x)&&identical(x[[1]],as.name('home_nav_cards')))
   cards[[length(cards)+1L]]<<-eval(x[[2]],home_r)
 })
 texts<-character()
 for(fn in c('ui_tab0_home','team_hub_ui','server_team_hub','hub_last_game_ui','hub_top_scorer_label','hub_storyline_specs'))
  home_walk(body(home_r[[fn]]),function(x) if(is.character(x))texts<<-c(texts,x))
 texts<-unique(texts)
 pick<-function(pattern) {x<-grep(pattern,texts,value=TRUE);if(length(x)!=1L)stop('Expected one source literal for ',pattern,': ',paste(x,collapse=' | '));x}
 chooser<-list()
 home_walk(body(home_r$ui_tab0_home),function(x) {
  if(is.call(x)&&identical(x[[1]],quote(tags$button))&&'data-league-btn' %in% names(as.list(x))) {
   a<-as.list(x);label<-eval(a[[which(names(a)=='data-league-btn')]],home_r)
   spans<-a[vapply(a,function(v)is.call(v)&&identical(v[[1]],quote(tags$span)),logical(1))]
   chooser[[length(chooser)+1L]]<<-list(value=label,name=spans[[1]][[3]],sub=spans[[2]][[3]])
  }
 })
 keys<-c(title='^IBPL Analytics$',ilSubtitle='^Your go-to',euroSubtitle='^EuroLeague and EuroCup',
  euroNote='^EuroLeague possessions come',euroNoteEnd='^league\\. The two',
  defaultLabel='^Set as default$',defaultTitle='^Use this team',allTeams='^All teams$',
  investigate='^Investigate further$',players='^Key players',lineups='^Lineups \\(min',
  storylines='^Storylines$',emptyStorylines='^No qualified storylines',loading='^Analyzing team',
  teamLink='^Open team ratings$',onoffLink='^Open on/off impact$',lineupsLink='^Open lineups$',
  off='^Off PPP$',def='^Def PPP$',net='^Net$',offense='^Offense$',defense='^Defense$',
  best='^Best$',worst='^Worst$',lastGame='^Last game$',gameflow='^View Gameflow$',better='^better$',worse='^worse$',
  scorer='^Top scorer \\(min',starterHigh='^Starter-heavy',starterLow='^Bench-heavy',
  starterSentence='^%s by',clutchSentence='^Clutch net rating',last10Sentence='^Last 10 games:',topBottomSentence='^Net rating vs Top 4')
 text<-lapply(keys,pick)
 contracts<-NULL;miniLabels<-NULL
 home_walk(body(home_r$hub_dashboard_validation_error),function(x) {
  if(is.call(x)&&identical(x[[1]],as.name('<-'))&&identical(x[[2]],as.name('contracts')))contracts<<-eval(x[[3]],home_r)
 })
 home_walk(body(home_r$hub_ff_mini),function(x) {
  if(is.call(x)&&identical(x[[1]],as.name('<-'))&&identical(x[[2]],as.name('labels'))&&is.call(x[[3]])&&identical(x[[3]][[1]],as.name('c')))miniLabels<<-eval(x[[3]],home_r)
 })
 write_json(list(sql=home_sql,cards=list(euro=cards[[1]],il=cards[[2]]),chooser=chooser,
   text=text,contracts=contracts,ffLabels=miniLabels,months=month.abb,topScorerMinGp=home_r$HUB_TOP_SCORER_MIN_GP,
   storylines=lapply(home_r$hub_storyline_specs(),function(s)s[c('id','preset','min_poss')]),
   storage=list(team='ibplHubTeam',enabled='ibplHubTeamDefaultEnabled')),
   'web/src/data/homeUi.json',auto_unbox=TRUE,pretty=TRUE,digits=NA)
}
if('--export' %in% commandArgs(trailingOnly=TRUE)) home_export_sources()
