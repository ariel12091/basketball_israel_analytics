# API feature-coverage gate helpers (see app/tests/testthat/test-api-coverage-gate.R).
#
# A test claims a feature id by calling covers("GL-01", "GL-04", ...) inside a
# test_that() block. The scanner PARSES test files (comments never count) and
# collects every string literal inside any covers(...) call, recursing through
# c(...). Ids must therefore appear as string literals inside covers(...) in a
# test-*.R file; ids passed through a variable are invisible to it.

# Prefixes of inventory sections that are implemented; empty until Task 11 ("GL").
API_DONE_PREFIXES <- character()

# No-op marker; safe to source repeatedly.
covers <- function(...) invisible(NULL)

inventory_ids <- function(path) {
  lines <- readLines(path, warn = FALSE)
  rows <- lines[grepl("^[|]", lines)]
  cells <- trimws(vapply(strsplit(rows, "|", fixed = TRUE),
                         function(x) if (length(x) >= 2L) x[[2]] else "", ""))
  cells[grepl("^[A-Z]+-[0-9]{2}$", cells)]
}

.covers_literals <- function(expr) {
  if (is.character(expr)) return(expr)
  if (!is.call(expr)) return(character())
  unlist(lapply(as.list(expr)[-1], .covers_literals), use.names = FALSE) %||% character()
}

.covers_in_expr <- function(expr) {
  if (!is.call(expr)) return(character())
  own <- if (identical(expr[[1]], as.name("covers"))) .covers_literals(expr) else character()
  c(own, unlist(lapply(as.list(expr), .covers_in_expr), use.names = FALSE))
}

covered_ids <- function(test_dirs) {
  files <- unlist(lapply(test_dirs[dir.exists(test_dirs)], list.files,
                         pattern = "^test-.*[.]R$", full.names = TRUE))
  ids <- unlist(lapply(files, function(f) {
    exprs <- tryCatch(parse(f, keep.source = FALSE), error = function(e) expression())
    unlist(lapply(as.list(exprs), .covers_in_expr), use.names = FALSE)
  }), use.names = FALSE)
  sort(unique(as.character(ids)))
}

uncovered_ids <- function(ids, covered, done_prefixes) {
  want <- ids[sub("-.*", "", ids) %in% done_prefixes]
  sort(setdiff(want, covered))
}

if (!exists("%||%", mode = "function")) `%||%` <- function(a, b) if (is.null(a)) b else a
