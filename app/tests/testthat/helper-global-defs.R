# global.R cannot be sourced from a test: at source time it builds the DB pool,
# registers onStop() and schedules a later() prewarm. parse() does not execute
# anything, so a test can lift out just the definitions it needs and evaluate
# those. This keeps UI-builder tests asserting on rendered output instead of
# dropping to string-matching the source, which is all reading the file as text
# allows.
#
# catalogues_generated.R holds the constants generated from the catalogue
# tables; app.R sources it ahead of global.R, so it is read first here too.
global_defs <- function(...) {
  wanted <- c(...)
  exprs <- c(parse(repo_file("R", "catalogues_generated.R")),
             parse(repo_file("R", "global.R")))
  env <- new.env(parent = globalenv())

  for (e in exprs) {
    if (!is.call(e) || !identical(as.character(e[[1]]), "<-")) next
    nm <- as.character(e[[2]])
    if (length(nm) == 1 && nm %in% wanted) eval(e, envir = env)
  }

  missing <- setdiff(wanted, ls(env))
  if (length(missing)) {
    stop("not defined at top level in global.R or catalogues_generated.R: ",
         paste(missing, collapse = ", "))
  }
  env
}
