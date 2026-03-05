repo_file <- function(...) {
  normalizePath(file.path("..", "..", ...), winslash = "/", mustWork = FALSE)
}

read_repo_txt <- function(...) {
  path <- repo_file(...)
  paste(readLines(path, warn = FALSE), collapse = "\n")
}
