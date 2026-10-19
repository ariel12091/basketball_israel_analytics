# respond.R - how the API writes a response: JSON or CSV bodies, and errors.
#
# api_error() itself lives in app/R/svc_params.R: the service layer's
# validators signal it, and the service layer must not depend on the API.

API_STATUS_TIMEOUT <- 504L

# Writes `payload` to `res` and returns `res` (Plumber sends a returned
# response object as it is, bypassing its serializers). JSON: rows as an
# array of objects, NA as null, numbers unrounded. CSV (format = "csv"): the
# rows only, as an attachment named <slug>_<csv_export_stamp()>.csv, the
# stamp Shiny's CSV buttons use.
api_respond <- function(res, payload, format = "json", slug = "export", status = 200L) {
  res$status <- as.integer(status)
  if (identical(format, "csv")) {
    rows <- payload$rows
    if (is.null(rows)) rows <- data.frame()
    out <- utils::capture.output(utils::write.csv(as.data.frame(rows), row.names = FALSE, na = ""))
    res$setHeader("Content-Type", "text/csv; charset=utf-8")
    res$setHeader("Content-Disposition",
                  sprintf("attachment; filename=\"%s_%s.csv\"", slug, csv_export_stamp()))
    res$body <- paste0(paste(out, collapse = "\n"), "\n")
  } else {
    res$setHeader("Content-Type", "application/json; charset=utf-8")
    res$body <- as.character(jsonlite::toJSON(
      payload, dataframe = "rows", na = "null", auto_unbox = TRUE, digits = NA,
      # A NULL field (e.g. `totals`) is null, not an empty object.
      null = "null"
    ))
  }
  res
}

# An error as a response, always JSON. A validation failure keeps its status
# and names its parameter; a statement timeout is a 504 saying so; anything
# else is a 500 whose body carries no detail -- the message can hold SQL, so
# it goes to the log only.
api_respond_error <- function(res, e) {
  msg <- conditionMessage(e)
  if (inherits(e, "api_error")) {
    return(api_respond(res, list(error = msg, param = e$param %||% NA), status = e$status))
  }
  if (grepl("statement timeout", msg, ignore.case = TRUE)) {
    api_log_error("timeout", msg)
    return(api_respond(res, list(
      error = "The database query exceeded the statement timeout. Narrow the filters and retry.",
      param = NA
    ), status = API_STATUS_TIMEOUT))
  }
  api_log_error("error", msg)
  api_respond(res, list(error = "Internal server error.", param = NA), status = 500L)
}

api_log_error <- function(kind, msg) {
  if (exists("app_log", mode = "function")) {
    app_log("api", sprintf("%s: %s", kind, msg), level = "ERROR")
  }
  invisible(NULL)
}

# Runs a handler body; any condition it signals becomes api_respond_error().
api_guard <- function(res, expr) {
  tryCatch(expr, error = function(e) api_respond_error(res, e))
}
