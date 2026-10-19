readRenviron("app/.Renviron")
Sys.setenv(IBPL_CACHE_UI = "false", IBPL_CACHE_UI_HTML = "false")
shiny::runApp("app", host = "127.0.0.1", port = 3878, launch.browser = FALSE)
