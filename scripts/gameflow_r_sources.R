# Load only Gameflow's own assignments, never the application/pool startup.
suppressMessages({library(dplyr);library(htmltools);library(jsonlite)})
gameflow_r <- new.env(parent=globalenv())
for (file in c('app/R/helpers.R','app/R/global.R','app/R/mod_ribbon_modal.R'))
 for(e in parse(file)) if(is.call(e)&&identical(e[[1]],as.name('<-'))&&is.name(e[[2]])) {
  name <- as.character(e[[2]])
  if(grepl('^(RIBBON_|ribbon_)',name)||name %in% c('merge_adjacent_stints','build_stint_ribbon_svg','%||%'))eval(e,gameflow_r)
 }
gameflow_export_sources <- function() {
 strings <- function(x) {
  if(is.character(x))return(x)
  if(is.call(x)||is.pairlist(x)||is.expression(x))return(unlist(lapply(as.list(x),strings),use.names=FALSE))
  character(0)
 }
 literal <- function(name,prefix) {
  values<-strings(body(gameflow_r[[name]]));out<-unique(values[startsWith(values,prefix)])
  stopifnot(length(out)==1L);out
 }
 mobile<-readLines('app/www/mobile.js',warn=FALSE,encoding='UTF-8')
 mobile_toggle<-function(prefix) {
  line<-unique(trimws(mobile[grepl(paste0('toggle.textContent = "',prefix),mobile,fixed=TRUE)]))
  stopifnot(length(line)==1L);sub('";$','',sub('^toggle.textContent = "','',line))
 }
 jsonlite::write_json(list(il=gameflow_r$RIBBON_SQL_ISRAEL,euro=gameflow_r$RIBBON_SQL_EURO),
  'web/src/data/gameflowSql.json',auto_unbox=TRUE,pretty=TRUE)
 jsonlite::write_json(list(desktop=gameflow_r$ribbon_layout(),compact=gameflow_r$ribbon_layout(TRUE),
  bandGap=gameflow_r$RIBBON_BAND_GAP,header=gameflow_r$RIBBON_HEADER,padTop=gameflow_r$RIBBON_PAD_TOP,padBottom=gameflow_r$RIBBON_PAD_BOTTOM,
  numberFont=gameflow_r$RIBBON_NUM_FONT,numberAdvance=gameflow_r$RIBBON_NUM_ADVANCE,numberPad=gameflow_r$RIBBON_NUM_PAD,
  text=list(clear=literal('ribbon_pair_controls','Clear players'),prompt=literal('ribbon_pair_controls','Select two'),
   hint=literal('ribbon_modal_server','Scroll through'),empty=literal('ribbon_modal_server','This game has no segment'),
   overview=literal('ribbon_mobile_overview_ui','Score margin across'),overviewAria=literal('ribbon_mobile_overview_ui','Full-game score margin;'),
   full=mobile_toggle('View full'),quarters=mobile_toggle('Show quarter'))),
  'web/src/data/gameflowLayout.json',auto_unbox=TRUE,pretty=TRUE,na='null')
}
