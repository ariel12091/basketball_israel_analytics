# Generate the web copy from the same generated catalogue Shiny sources.
source('app/R/catalogues_generated.R')
env <- new.env()
for (expr in parse('app/R/global_euro.R')) {
  if (is.call(expr) && identical(expr[[1]], as.name('<-')) &&
      identical(expr[[2]], as.name('EURO_PHASE_LABELS'))) eval(expr, env)
}
dir.create('web/src/data', recursive=TRUE, showWarnings=FALSE)
jsonlite::write_json(list(gameTypes=as.list(GAME_TYPE_LABELS),
  gameTypeChoices=lapply(seq_along(GAME_TYPE_CHOICES_UI),function(i)
    list(label=names(GAME_TYPE_CHOICES_UI)[i],value=unname(GAME_TYPE_CHOICES_UI[i]))),
  phases=as.list(env$EURO_PHASE_LABELS)), 'web/src/data/catalogues.json',
  auto_unbox=TRUE,pretty=TRUE)
