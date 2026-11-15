# Runs the app suite (from app/); the process fails if it did. (The R API
# suite retired with api/ on 2026-11-15.)
owd <- getwd()
on.exit(setwd(owd), add = TRUE)

run_suite <- function(label, dir) {
  cat(sprintf("[tests] running %s testthat suite\n", label))
  setwd(file.path(owd, dir))
  on.exit(setwd(owd), add = TRUE)
  ok <- tryCatch({
    source("tests/testthat.R")
    TRUE
  }, error = function(e) {
    cat(sprintf("[tests] %s suite FAILED: %s\n", label, conditionMessage(e)))
    FALSE
  })
  ok
}

results <- c(app = run_suite("app", "app"))
cat("[tests] done\n")
if (!all(results)) {
  cat(sprintf("[tests] failed suites: %s\n", paste(names(results)[!results], collapse = ", ")))
  quit(save = "no", status = 1L)
}
