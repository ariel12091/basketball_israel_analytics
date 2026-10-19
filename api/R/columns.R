# columns.R - meta$columns: what each column of a data endpoint's rows is.
#
# Labels and definitions are read off the table container Shiny itself draws
# (the header builders in helpers.R), and definitions fall back to
# COLUMN_TOOLTIPS exactly as HEADER_TOOLTIP_JS does in the browser: a header
# cell's own title wins, else the tooltip keyed by the cell's text. Nothing is
# retyped here.

# The header cells of a Shiny table container, one per displayed column, in
# order: list(label, title, group). The last header row holds the column
# labels; an earlier row, when present, holds the group headings over them
# (colspans expanded; a blank heading is no group).
api_header_cells <- function(container) {
  rows <- api_find_tags(container, "tr")
  if (!length(rows)) stop("api_header_cells: the container has no header row", call. = FALSE)
  cells_of <- function(tr) Filter(function(x) inherits(x, "shiny.tag") && identical(x$name, "th"),
                                  api_tag_children(tr))
  text_of <- function(th) {
    txt <- unlist(Filter(is.character, api_tag_children(th)), use.names = FALSE)
    if (length(txt)) paste(txt, collapse = "") else ""
  }
  leaf <- cells_of(rows[[length(rows)]])
  groups <- rep(NA_character_, length(leaf))
  if (length(rows) > 1L) {
    top <- cells_of(rows[[1]])
    spans <- vapply(top, function(th) as.integer(th$attribs$colspan %||% 1L), integer(1))
    labels <- rep(vapply(top, text_of, character(1)), spans)
    if (length(labels) != length(leaf)) {
      stop("api_header_cells: group colspans do not cover the header row", call. = FALSE)
    }
    groups <- ifelse(nzchar(labels), labels, NA_character_)
  }
  lapply(seq_along(leaf), function(i) list(
    label = text_of(leaf[[i]]),
    title = leaf[[i]]$attribs$title,
    group = groups[[i]]
  ))
}

api_tag_children <- function(tag) {
  flat <- function(x) {
    if (inherits(x, "shiny.tag")) return(list(x))
    if (is.list(x)) return(unlist(lapply(x, flat), recursive = FALSE))
    if (is.null(x)) return(list())
    list(x)
  }
  flat(tag$children)
}

api_find_tags <- function(tag, name) {
  if (!inherits(tag, "shiny.tag")) return(list())
  own <- if (identical(tag$name, name)) list(tag) else list()
  c(own, unlist(lapply(api_tag_children(tag), api_find_tags, name = name), recursive = FALSE))
}

# A header cell's definition: its own title, else COLUMN_TOOLTIPS for its
# text, else NULL.
api_definition <- function(label, title = NULL) {
  if (!is.null(title) && nzchar(title)) return(title)
  if (!is.null(label) && label %in% names(COLUMN_TOOLTIPS)) return(unname(COLUMN_TOOLTIPS[[label]]))
  NULL
}

# meta$columns. `view_cols` is a list of column specs, in row order, each
# list(key, label, definition, group, format, hidden); `polarity` is the
# renderer's heat map (named logical, TRUE = high is bad) keyed by row key.
# A column gets a polarity only when it is heat-coloured, and a
# percentile_key when its pr_<key> column is among the rows. best_direction
# is DataTables' first-click direction: the tables set no orderSequence, so
# it is the default, ascending, for every column.
api_columns <- function(view_cols, polarity) {
  keys <- vapply(view_cols, `[[`, character(1), "key")
  lapply(view_cols, function(col) {
    pr_key <- paste0("pr_", col$key)
    reverse <- if (col$key %in% names(polarity)) polarity[[col$key]]
    list(
      key = col$key,
      label = col$label,
      definition = col$definition,
      format = col$format,
      polarity = if (is.null(reverse)) NULL else if (isTRUE(reverse)) "lower_better" else "higher_better",
      best_direction = "asc",
      percentile_key = if (pr_key %in% keys) pr_key,
      group = col$group,
      hidden = isTRUE(col$hidden)
    )
  })
}
