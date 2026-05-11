# logger.R - Centralized logging for the Shiny app.
#
# Routes every per-tab/per-startup line through one function so a single grep
# on the shinyapps.io log viewer surfaces it. APP_LOG_LEVEL gates output
# (DEBUG/INFO/WARN/ERROR); when a session is passed, the first 8 chars of
# session$token are attached so multi-line user reports can be reassembled.

LOG_LEVELS <- c(DEBUG = 10L, INFO = 20L, WARN = 30L, ERROR = 40L)

session_log_id <- function(session) {
  if (is.null(session)) return("")
  tok <- tryCatch(session$token, error = function(e) NULL)
  if (is.null(tok) || !is.character(tok) || !nzchar(tok)) return("")
  substr(tok, 1L, 8L)
}

app_log <- function(component, msg, level = "INFO", session = NULL,
                    file_env = "APP_LOG_FILE",
                    level_env = "APP_LOG_LEVEL") {
  level <- toupper(as.character(level))
  if (!level %in% names(LOG_LEVELS)) level <- "INFO"

  min_level_name <- toupper(Sys.getenv(level_env, "INFO"))
  if (!nzchar(min_level_name) || !min_level_name %in% names(LOG_LEVELS)) {
    min_level_name <- "INFO"
  }
  if (LOG_LEVELS[[level]] < LOG_LEVELS[[min_level_name]]) {
    return(invisible(NULL))
  }

  sid <- session_log_id(session)
  sid_part <- if (nzchar(sid)) sprintf(" sid=%s", sid) else ""

  line <- sprintf(
    "%s [%s] [%s]%s %s",
    format(Sys.time(), "%Y-%m-%d %H:%M:%S"),
    level,
    component,
    sid_part,
    msg
  )
  message(line)

  log_file <- Sys.getenv(file_env, "")
  if (nzchar(log_file)) {
    try(suppressWarnings(cat(line, "\n", file = log_file, append = TRUE)), silent = TRUE)
  }

  invisible(line)
}
