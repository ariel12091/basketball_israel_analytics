# Isolated Compare definitions: no app startup, session or database connection.
suppressMessages({library(shiny);library(dplyr);library(htmltools);library(jsonlite)})
compare_r <- new.env(parent=globalenv())
for(file in c('app/R/catalogues_generated.R','app/R/helpers.R','app/R/server_tab7_compare.R'))
 for(e in parse(file)) if(is.call(e)&&identical(e[[1]],as.name('<-'))&&is.name(e[[2]])) {
  name<-as.character(e[[2]])
  if(is.call(e[[3]])&&identical(e[[3]][[1]],as.name('function')))eval(e,compare_r)
  else if(name %in% c('TEAM_RATING_METRICS','CMP_FILTERABLE_COLS','FF_IMPACT_WEIGHTS'))eval(e,compare_r)
 }
compare_assignments <- list()
walk_compare <- function(x) {
 if(!is.call(x))return(invisible(NULL))
 if(identical(x[[1]],as.name('<-'))&&is.name(x[[2]]))compare_assignments[[as.character(x[[2]])]]<<-x
 args<-as.list(x)
 for(i in seq_along(args)[-1])if(!identical(args[[i]],quote(expr=)))walk_compare(args[[i]])
}
walk_compare(body(compare_r$server_tab7_compare))
compare_constants<-c('DETAIL_METRICS','TEAM_METRICS','PLAYER_METRICS','PLAYER_VIEWS','TEAM_PLAYER_METRICS','TEAM_PLAYER_PCT_COLS','TEAM_PLAYER_LOWER_BETTER','TEAM_PLAYER_RAW_COLS','PVP_STATS','FF_SWING_STATS','PRESET_MIRROR_BLOCKS','detail_specs','gt_labels')
for(name in compare_constants)eval(compare_assignments[[name]],compare_r)
compare_defs<-c('collect_side_params','cmp_player_compare_mode','parse_single_date','side_label_short','side_label_full','cmp_gap_direction','cmp_gap_after_minus_before','cmp_gap_value','cmp_compute_gap','cmp_detail_display_state','apply_side_team_filter','apply_min_poss_filter','cmp_auto_minposs_from_df','finalize_cmp_joined','weighted_or_mean','add_shooting_rates','add_team_shot_profile_shares','add_player_shot_profile_shares','detail_get_value','detail_fmt','detail_win_pct','detail_extract_value','calc_team_player_metric','team_player_played_side','normalize_team_player_rows','empty_team_player_side','prep_team_player_side','cmp_team_dynamic_params','cmp_team_shooting_params','cmp_team_csv_or_na')
for(name in compare_defs)eval(compare_assignments[[name]],compare_r)
compare_r$CMP_AUTO_TARGET_ROWS<-50L
compare_r$cmp_min_poss<-function()10L
compare_export_sources <- function() {
 out<-lapply(compare_constants,function(name) {
  x<-get(name,compare_r)
  if(is.character(x)&&!is.null(names(x)))as.list(x) else x
 });names(out)<-compare_constants
 literals<-function(x) {
  if(is.character(x))return(x)
  if(is.call(x)||is.pairlist(x)||is.expression(x)) {
   args<-as.list(x);out<-character(0)
   for(i in seq_along(args))if(!identical(args[[i]],quote(expr=)))out<-c(out,literals(args[[i]]))
   return(out)
  }
  character(0)
 }
 out$text<-unique(literals(body(compare_r$server_tab7_compare)))
 out$filterColumns<-as.list(compare_r$CMP_FILTERABLE_COLS)
 # Static dropdown choices are read from the R UI rather than maintained twice.
 choices_for<-function(id) {
  found<-NULL
  visit<-function(x) {
   if(!is.call(x))return(NULL)
   args<-as.list(x)
   if(length(args)>1&&is.character(args[[2]])&&identical(args[[2]],id)&&!is.null(args$choices))found<<-eval(args$choices,compare_r)
   for(i in seq_along(args)[-1])if(!identical(args[[i]],quote(expr=)))visit(args[[i]])
  }
  for(e in parse('app/R/ui_tab7_compare.R'))visit(e)
  stopifnot(!is.null(found))
  lapply(seq_along(found),function(i)list(value=unname(found[i]),label=if(is.null(names(found)))unname(found[i]) else names(found)[i]))
 }
 out$presetChoices<-choices_for('cmp_preset')
 get_sql<-function(name) {
  found<-character(0)
  walk<-function(x) {
   if(!is.call(x))return(NULL)
   if(identical(x[[1]],as.name('paste0'))) {
    args<-as.list(x)[-1]
    if(length(args)&&all(vapply(args,is.character,logical(1)))) {
     val<-eval(x)
     if(startsWith(val,'SELECT * FROM basketball_test.'))found<<-c(found,val)
    }
   }
   args<-as.list(x)
   for(i in seq_along(args)[-1])if(!identical(args[[i]],quote(expr=)))walk(args[[i]])
  }
  walk(compare_assignments[[name]])
  stopifnot(length(unique(found))==1);unique(found)
 }
 out$sql<-lapply(c(ratings='run_team_ratings',four_factors='run_team_ff',lineups='run_lineups_summary',lineup_ff='run_lineups_ff',player_ff='run_four_factors',onoff='run_onoff_impact'),get_sql)
 write_json(out,'web/src/data/compareUi.json',auto_unbox=TRUE,pretty=TRUE,na='null',null='null',digits=NA)
}
