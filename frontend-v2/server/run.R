# Start the Plumber API server for frontend-v2
# Usage (from repo root):  Rscript frontend-v2/server/run.R
# Usage (from frontend-v2): Rscript server/run.R
# Serves on http://127.0.0.1:3002 by default

library(plumber)

# Resolve plumber.R location
this_dir <- tryCatch(
  dirname(normalizePath(sys.frame(1)$ofile)),
  error = function(e) {
    candidates <- c(
      getwd(),
      file.path(getwd(), "server"),
      file.path(getwd(), "frontend-v2", "server")
    )
    for (d in candidates) {
      if (file.exists(file.path(d, "plumber.R"))) return(normalizePath(d))
    }
    stop("Cannot find server/plumber.R")
  }
)

pr <- plumb(file.path(this_dir, "plumber.R"))
host <- Sys.getenv("PLUMBER_HOST", "127.0.0.1")
port <- as.integer(Sys.getenv("PLUMBER_PORT", "3002"))
pr$run(host = host, port = port)
