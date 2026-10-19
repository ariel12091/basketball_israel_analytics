# Runs the app suite (from app/) and then the API suite (from api/). Both run
# even when the first fails; the process fails if either did.
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

results <- c(app = run_suite("app", "app"), api = run_suite("api", "api"))
cat("[tests] done\n")
if (!all(results)) {
  cat(sprintf("[tests] failed suites: %s\n", paste(names(results)[!results], collapse = ", ")))
  quit(save = "no", status = 1L)
}
