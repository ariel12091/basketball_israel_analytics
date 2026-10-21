# Generate the web app's copy of Shiny's presentation from Shiny's own sources.
#
# The migration spec says "no redesign": the web Game Logs page must look and
# read like the Shiny tab. Nothing here is retyped. This script
#   1. compiles the Bootstrap theme from the bs_theme() call in app/app.R,
#      plus the datepicker CSS Shiny compiles against the same theme;
#   2. copies the DataTables and Shiny base stylesheets from the installed
#      R packages the app deploys with;
#   3. renders the Game Logs tab UIs (Israeli tab 4, EuroLeague tab 11) and the
#      glossary modal, and stores their static fragments (explainers, example
#      boxes, legend, table hints) plus the tooltip and heat-colour constants
#      as JSON for the React components.
# app/www/app.css and mobile.css are copied by web/scripts/sync-shiny-styles.mjs
# and pinned byte-for-byte by web/src/styles/shiny.test.ts.
#
# Run from the repository root:
#   Rscript scripts/export_web_shiny_ui.R
suppressMessages({
  library(shiny); library(bslib); library(htmltools); library(jsonlite); library(xml2); library(DT)
})

out_css <- "web/src/styles/shiny/vendor"
out_data <- "web/src/data"
out_img <- "web/public/images"
dir.create(out_css, recursive = TRUE, showWarnings = FALSE)
dir.create(out_img, recursive = TRUE, showWarnings = FALSE)

# Top-level assignments only, as tests/testthat/helper-global-defs.R does:
# sourcing global.R would open the database pool.
env <- new.env(parent = globalenv())
files <- c("app/R/catalogues_generated.R", "app/R/helpers.R", "app/R/global.R",
           "app/R/global_euro.R", "app/R/svc_game_logs.R", "app/R/mod_ribbon_modal.R",
           "app/R/ui_tab4_gamelogs.R", "app/R/ui_tab11_euro_gamelogs.R")
for (f in files) for (e in parse(f)) {
  if (!is.call(e) || !identical(as.character(e[[1]]), "<-")) next
  if (!is.name(e[[2]])) next
  try(eval(e, envir = env), silent = TRUE)
}
setwd("app")  # app_image_src() and includeCSS() resolve relative to app/
on.exit(setwd(".."), add = TRUE)

# ---- 1. Theme CSS ---------------------------------------------------------
app_exprs <- parse("app.R")
find_call <- function(x, name) {
  if (is.call(x)) {
    if (identical(x[[1]], quote(bslib::bs_theme)) && identical(name, "bs_theme")) return(x)
    for (a in as.list(x)[-1]) { r <- find_call(a, name); if (!is.null(r)) return(r) }
  }
  NULL
}
theme_call <- NULL
for (e in app_exprs) { theme_call <- find_call(e, "bs_theme"); if (!is.null(theme_call)) break }
stopifnot(!is.null(theme_call))
theme <- eval(theme_call)
deps <- bslib::bs_theme_dependencies(theme)
boot <- Filter(function(d) identical(d$name, "bootstrap"), deps)[[1]]
# The compiled theme starts with @import url("font.css"): bslib's preset font
# faces (Open Sans, Lato, ...), ~3.4 MB that this theme never uses (base_font is
# DM Sans from Google Fonts). Declaring unused faces downloads nothing, so the
# import is dropped and only the glyphicon files the CSS references are kept.
css <- readLines(file.path(boot$src$file, boot$stylesheet), warn = FALSE)
stopifnot(startsWith(css[1], '@import url("font.css");'))
css[1] <- sub('@import url("font.css");', "", css[1], fixed = TRUE)
writeLines(css, file.path("..", out_css, "bootstrap.min.css"))
glyph <- file.path(boot$src$file, "fonts", "bootstrap")
dir.create(file.path("..", out_css, "fonts", "bootstrap"), recursive = TRUE, showWarnings = FALSE)
for (f in list.files(glyph)) file.copy(file.path(glyph, f), file.path("..", out_css, "fonts", "bootstrap", f), overwrite = TRUE)
# selectize and bslib's component styles (accordion, popover), compiled against
# the theme as Shiny serves them (selectize-0.15.2/selectize.css,
# bslib-component-css-0.9.0/bslib-component-css.min.css).
themed <- list("selectize.css" = shiny:::selectizeDependencyFunc(theme),
               "bslib-component-css.min.css" = bslib:::component_dependency_sass(theme))
for (nm in names(themed)) {
  d <- themed[[nm]]
  if (!inherits(d, "html_dependency")) d <- d[[1]]
  css_file <- Filter(function(s) endsWith(s, ".css"), unlist(d$stylesheet))[[1]]
  css <- readLines(file.path(d$src$file, css_file), warn = FALSE)
  # A Sass module rule the compiler leaves in place; browsers ignore it, the
  # Next.js CSS parser warns on it.
  css <- css[css != '@use "sass:math";']
  writeLines(css, file.path("..", out_css, nm))
}
# Shiny serves both: the static datepicker stylesheet and the copy compiled
# against the theme (loaded after it).
for (d in list(shiny:::datePickerCSS(NULL), shiny:::datePickerCSS(theme))) for (s in d$stylesheet) {
  src <- if (!is.null(d$package)) system.file(d$src$file, s, package = d$package) else file.path(d$src$file, s)
  file.copy(src, file.path("..", out_css, basename(s)), overwrite = TRUE)
}

# ---- 2. Package stylesheets ----------------------------------------------
pkg_css <- c(
  "shiny.min.css" = system.file("www/shared/shiny.min.css", package = "shiny"),
  "jquery.dataTables.min.css" = system.file("htmlwidgets/lib/datatables/css/jquery.dataTables.min.css", package = "DT"),
  "jquery.dataTables.extra.css" = system.file("htmlwidgets/lib/datatables/css/jquery.dataTables.extra.css", package = "DT"),
  "buttons.dataTables.min.css" = system.file("htmlwidgets/lib/datatables-extensions/Buttons/css/buttons.dataTables.min.css", package = "DT")
)
for (nm in names(pkg_css)) {
  stopifnot(nzchar(pkg_css[[nm]]))
  file.copy(pkg_css[[nm]], file.path("..", out_css, nm), overwrite = TRUE)
}

# ---- 3. Static UI fragments ----------------------------------------------
images <- character(0)
fragment <- function(node) {
  html <- as.character(node)
  # Example snippets are inlined as data URIs in Shiny; the web serves files.
  for (img in xml_find_all(node, ".//img")) {
    alt <- xml_attr(img, "alt")
    src <- xml_attr(img, "src")
    if (startsWith(src, "data:")) {
      file <- c("Game logs summary table snippet" = "gamelogs-row-snippet.png",
                "Game logs four factors table snippet" = "gamelogs-ff-row-snippet.png")[[alt]]
      images <<- c(images, file)
      html <- sub(src, paste0("/images/", file), html, fixed = TRUE)
    }
  }
  html
}
tab_fragments <- function(tab_ui, prefix) {
  doc <- read_html(as.character(tab_ui))
  main <- xml_find_first(doc, "//div[@role='main']")
  conds <- xml_find_all(main, "./div[contains(@class,'shiny-panel-conditional')]")
  pick <- function(view) {
    hits <- Filter(function(n) grepl(sprintf("'%s'", view), xml_attr(n, "data-display-if"), fixed = TRUE), conds)
    paste(vapply(hits, function(n) paste(vapply(xml_children(n), fragment, ""), collapse = "\n"), ""), collapse = "\n")
  }
  list(
    summary = pick("Summary"),
    four_factors = pick("Four Factors"),
    hint = as.character(xml_find_first(main, ".//div[@class='table-hint']"))
  )
}

ui_il <- env$ui_tab4_gamelogs()
ui_el <- env$ui_tab11_euro_gamelogs()

glossary_call <- NULL
walk <- function(x) {
  if (is.call(x)) {
    if (identical(x[[1]], as.name("modalDialog"))) { glossary_call <<- x; return(invisible()) }
    for (a in as.list(x)[-1]) if (is.null(glossary_call)) walk(a)
  }
}
for (e in app_exprs) walk(e)
stopifnot(!is.null(glossary_call))
glossary <- eval(glossary_call)
glossary_body <- xml_find_first(read_html(as.character(glossary)), "//div[@class='modal-body']")


ui <- list(
  il = tab_fragments(ui_il, "gl"),
  euro = tab_fragments(ui_el, "eurogl"),
  glossaryHtml = paste(vapply(xml_children(glossary_body), as.character, ""), collapse = "\n"),
  columnTooltips = as.list(env$COLUMN_TOOLTIPS),
  filterTooltips = as.list(env$FILTER_TOOLTIPS),
  offOrebTooltip = env$OFF_OREB_TOOLTIP,
  defOrebTooltip = env$DEF_OREB_TOOLTIP,
  heat = list(cuts = env$CUTS, grad = env$COLS_GRAD, rev = env$COLS_REV),
  euroCompetitionLabels = as.list(env$EURO_COMPETITION_LABELS),
  # The navbar's Israeli season choices are literal in app.R's
  # selectInput("game_year", ...); EuroLeague starts from EURO_STATIC_SEASONS
  # and is replaced by the loaded seasons, as euro_init_season_inputs() does.
  statFilterCols = lapply(list(summary = env$GL_SUMMARY_FILTERABLE_COLS,
                               four_factors = env$GL_FF_FILTERABLE_COLS),
                          function(cols) lapply(seq_along(cols), function(i)
                            list(label = names(cols)[i], col = unname(cols[i])))),
  round1 = env$GL_ROUND_1,
  # fetch_teams_min(): the Israeli team pickers and chips read the catalogue
  # roster (season_teams, generated into catalogues_generated.R).
  ilTeams = lapply(env$STATIC_TEAM_ROSTERS, function(r) {
    r <- r[order(r$team_name), , drop = FALSE]
    lapply(seq_len(nrow(r)), function(i) list(id = as.integer(r$team_id[i]), name = r$team_name[i]))
  }),
  defaultGameYear = env$DEFAULT_GAME_YEAR,
  ilSeasons = local({
    hit <- NULL
    walk_sel <- function(x) {
      if (is.call(x)) {
        if (identical(x[[1]], as.name("selectInput")) && identical(x[[2]], "game_year")) hit <<- x
        for (a in as.list(x)[-1]) if (is.null(hit)) walk_sel(a)
      }
    }
    for (e in app_exprs) walk_sel(e)
    unname(eval(hit$choices, env))
  }),
  euroDefaultSeason = env$EURO_DEFAULT_SEASON,
  euroStaticSeasons = env$EURO_STATIC_SEASONS,
  seasonDateBounds = lapply(env$SEASON_DATE_BOUNDS, function(b) lapply(b, format)),
  leagueChoices = lapply(seq_along(env$LEAGUE_SELECT_CHOICES), function(i)
    list(label = names(env$LEAGUE_SELECT_CHOICES)[i], value = unname(env$LEAGUE_SELECT_CHOICES[i])))
)
for (f in unique(images)) file.copy(file.path("www", f), file.path("..", out_img, f), overwrite = TRUE)
write_json(ui, file.path("..", out_data, "shinyUi.json"), auto_unbox = TRUE, pretty = TRUE, digits = NA)
cat("wrote", file.path(out_data, "shinyUi.json"), "and", length(list.files(file.path("..", out_css))), "stylesheets\n")
