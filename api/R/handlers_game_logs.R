# handlers_game_logs.R - GET /v1/{league}/game-logs (Tab 4 and Tab 11).
#
# svc_game_logs_params() validates the query and svc_game_logs() computes the
# rows; this file only shapes them for the response contract: the columns
# the renderer shows (and the hidden ones it carries), under stable
# snake_case keys, sorted, with meta$columns describing each.

# What the two leagues' tables call their stage and score columns, and the
# labels their header builders are given (Tab 11 passes "Rd" / "Phase"; Tab 4
# takes the builders' defaults). The round column is GL_LEAGUE's round_col.
GL_API_LEAGUE <- list(
  il = list(stage = "game_type_label", score = "score_display", header_args = list()),
  euroleague = list(stage = "phase_label", score = "score", header_args = list("Rd", "Phase"))
)

# Row keys for the renderer's display names: the shot cells get snake_case
# keys, and the Gameflow link is replaced by the has_scores flag it is drawn
# from (a link is offered exactly where has_scores is TRUE).
GL_API_KEYS <- c("Off Shot" = "off_shot_fga", "Def Shot" = "def_shot_fga", gameflow = "has_scores")

# Formats: Tab 4 / Tab 11 round the heat-coloured rates, net and minutes to
# one decimal (formatRound); pr_* are 0-1 percentiles; the text, date and
# flag columns are named; every other column is a whole count or an id.
GL_API_DECIMAL_1 <- c(names(GL_HEAT_REVERSE_FF), "net_rtg", "minutes")

gl_api_format <- function(key, lgx) {
  if (startsWith(key, "pr_")) return("percentile")
  if (key %in% GL_API_DECIMAL_1) return("decimal_1")
  if (identical(key, "game_date")) return("date")
  if (identical(key, "has_scores")) return("boolean")
  if (key %in% c(lgx$stage, lgx$score, "team_name", "opp_team_name", "result")) return("text")
  "integer"
}

# The column specs of one league's view, in row order: the columns the
# renderer shows, then those it carries hidden (raw shot counts behind the
# shot cells, the percentiles), then the row's game and team ids, which a
# client needs to ask for that game's Gameflow. `source` is the service
# frame's column, `key` the API's.
gl_api_specs <- function(league, view) {
  lg <- GL_LEAGUE[[league]]
  lgx <- GL_API_LEAGUE[[league]]
  summary <- identical(view, "summary")
  has_shots <- summary && isTRUE(lg$has_shots)
  heat <- if (summary) GL_HEAT_REVERSE_SUMMARY else GL_HEAT_REVERSE_FF

  context <- c(lg$round_col, lgx$stage, "game_date", "gameflow", "team_name", "opp_team_name",
               "result", lgx$score, "minutes")
  metrics <- if (summary) {
    c("off_ppp", "def_ppp", "net_rtg", if (has_shots) c("Off Shot", "Def Shot"), "off_poss", "def_poss")
  } else {
    c("net_rtg", names(GL_HEAT_REVERSE_FF)[startsWith(names(GL_HEAT_REVERSE_FF), "off_")],
      names(GL_HEAT_REVERSE_FF)[startsWith(names(GL_HEAT_REVERSE_FF), "def_")], "off_poss", "def_poss")
  }
  visible <- c(context, metrics)
  hidden <- c(if (has_shots) c(GL_SHOT_COLS$off, GL_SHOT_COLS$def),
              vapply(names(heat), gl_pr_col_name, character(1), USE.NAMES = FALSE),
              "game_id", "team_id")

  header_fn <- if (summary) gamelog_summary_header else gamelog_ff_header
  header_args <- c(lgx$header_args, list(show_gameflow = TRUE), if (summary) list(has_shots = has_shots))
  cells <- api_header_cells(do.call(header_fn, header_args))
  if (length(cells) != length(visible)) {
    stop(sprintf("gl_api_specs: %d header cells for %d visible columns (%s %s)",
                 length(cells), length(visible), league, view), call. = FALSE)
  }

  key_of <- function(src) if (src %in% names(GL_API_KEYS)) GL_API_KEYS[[src]] else src
  spec <- function(src, cell = NULL) {
    key <- key_of(src)
    list(source = if (identical(src, "gameflow")) "has_scores" else src, key = key,
         label = cell$label, definition = if (!is.null(cell)) api_definition(cell$label, cell$title),
         group = cell$group, format = gl_api_format(key, lgx), hidden = is.null(cell))
  }
  c(unname(Map(spec, visible, cells)), lapply(hidden, spec))
}

# The service frame -> the response rows: the specs' columns under their
# keys; a zero-row frame with the same keys when nothing matched.
gl_api_rows <- function(rows, specs) {
  keys <- vapply(specs, `[[`, character(1), "key")
  if (is.null(rows) || !nrow(rows)) {
    return(structure(stats::setNames(rep(list(logical(0)), length(keys)), keys),
                     class = "data.frame", row.names = integer(0)))
  }
  src <- vapply(specs, `[[`, character(1), "source")
  out <- as.data.frame(rows, stringsAsFactors = FALSE)[, src, drop = FALSE]
  names(out) <- keys
  rownames(out) <- NULL
  out
}

# sort = list of list(column, direction): a stable sort, NAs last in either
# direction; text compares byte-wise (C locale), so the order does not
# depend on the server's locale.
gl_api_sort <- function(df, sort) {
  if (!nrow(df) || !length(sort)) return(df)
  keys <- lapply(sort, function(s) {
    x <- df[[s$column]]
    if (is.character(x)) x else xtfrm(x)
  })
  decreasing <- vapply(sort, function(s) identical(s$direction, "desc"), logical(1))
  o <- do.call(order, c(unname(keys), list(decreasing = decreasing, na.last = TRUE, method = "radix")))
  out <- df[o, , drop = FALSE]
  rownames(out) <- NULL
  out
}

# The response for a validated query: list(payload, format, slug). The
# payload follows the design's response contract; `rows` carries every
# column (visible and hidden), the CSV only the visible ones, as Shiny's CSV
# button exports them.
gl_api_payload <- function(query, league) {
  p <- svc_game_logs_params(query, league)
  fmt <- svc_format(query)
  sort <- svc_sort(query)
  out <- svc_game_logs(p)

  specs <- gl_api_specs(p$league, p$view)
  rows <- gl_api_rows(out$rows, specs)
  sort <- sort %||% out$meta$default_sort
  unknown <- setdiff(vapply(sort, `[[`, character(1), "column"), names(rows))
  if (length(unknown)) {
    svc_reject("sort", "Unknown sort column '%s'; columns: %s.", svc_show(unknown[[1]]),
               paste(names(rows), collapse = ", "))
  }
  rows <- gl_api_sort(rows, sort)

  meta <- c(
    list(league = league, competition = p$competition, season = p$season, view = p$view,
         data_version = p$data_version),
    out$meta,
    list(sort = sort,
         columns = api_columns(specs, if (identical(p$view, "summary")) GL_HEAT_REVERSE_SUMMARY else GL_HEAT_REVERSE_FF),
         warnings = list())
  )
  # Named vectors would serialise as bare arrays.
  if (!is.null(meta$shot_averages)) meta$shot_averages <- lapply(meta$shot_averages, as.list)

  if (identical(fmt, "csv")) {
    visible <- vapply(Filter(function(s) !isTRUE(s$hidden), specs), `[[`, character(1), "key")
    rows <- rows[, visible, drop = FALSE]
  }
  list(
    payload = list(rows = rows, totals = NULL, meta = meta),
    format = fmt,
    slug = paste(league, "game_logs", p$view, sep = "_")
  )
}

# GET /v1/{league}/game-logs
handle_game_logs <- function(req, res, league) {
  api_guard(res, {
    out <- gl_api_payload(api_query(req), league)
    api_respond(res, out$payload, out$format, slug = out$slug)
  })
}
