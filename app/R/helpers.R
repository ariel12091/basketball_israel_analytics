# helpers.R - Pure helper functions shared by the app and the test suite.
#
# Rules for this file: no side effects at source time (no library() calls,
# no DB pool, no cache objects, no env reads). Functions may take a session
# or shared context as arguments. Impure infrastructure lives in global.R.
# Tests source this file directly (see tests/testthat/helper-server-mocks.R)
# and stub only the impure pieces, so app and tests share one implementation.

# Fail-closed HTML escaping policy for DT tables.
dt_escape_except <- function(data, html_cols = character()) {
  data_cols <- names(data)
  html_cols <- intersect(as.character(html_cols), data_cols)

  if (!length(html_cols)) {
    return(TRUE)
  }

  # DT interprets numeric values as the columns that must be escaped.
  # Positions remain stable when replacement display headers are supplied.
  # All callers use rownames = FALSE, so no row-name offset is required.
  which(!data_cols %in% html_cols)
}

# Adaptive baseline: use RANKING_BASELINE when enough data qualifies,
# otherwise lower to the 75th-percentile so ~25% still get colored.
adaptive_baseline <- function(poss_vec) {
  n <- sum(!is.na(poss_vec))
  if (n == 0) return(0)
  pct_above <- sum(poss_vec >= RANKING_BASELINE, na.rm = TRUE) / n
  if (pct_above >= RANKING_MIN_PCT) return(RANKING_BASELINE)
  unname(quantile(poss_vec, 1 - RANKING_MIN_PCT, na.rm = TRUE))
}

# Null coalescing operator
`%||%` <- function(a, b) if (!is.null(a)) a else b

apply_visible_col_order <- function(df, visible_order, hidden_cols = character()) {
  if (is.null(df) || !length(visible_order)) return(df)

  all_cols <- names(df)
  hidden_cols <- intersect(hidden_cols, all_cols)
  visible_cols <- setdiff(all_cols, hidden_cols)
  saved_visible <- intersect(as.character(visible_order), visible_cols)
  if (!length(saved_visible)) return(df)

  df[, c(saved_visible, setdiff(visible_cols, saved_visible), hidden_cols), drop = FALSE]
}

dt_col_order_init_callback <- function(input_id, storage_key) {
  input_id_json <- jsonlite::toJSON(input_id, auto_unbox = TRUE)
  restore_id_json <- jsonlite::toJSON(paste0(input_id, "_restore"), auto_unbox = TRUE)
  storage_key_json <- jsonlite::toJSON(storage_key, auto_unbox = TRUE)

  DT::JS(sprintf(
    "function(settings, json) {
      var api = this.api();
      var inputId = %s;
      var restoreId = %s;
      var storageKey = %s;
      var maxColumns = 80;

      var cleanOrder = function(order) {
        if (!Array.isArray(order)) return [];
        return order.filter(function(name) {
          return typeof name === 'string' && name.length > 0 && name.length <= 80;
        }).slice(0, maxColumns);
      };

      var visibleColumnNames = function() {
        return cleanOrder(api.columns(':visible').header().toArray().map(function(header) {
          return $(header).text().replace(/\\s+/g, ' ').trim();
        }));
      };

      var setShinyOrder = function(order) {
        if (window.Shiny) {
          window.Shiny.setInputValue(inputId, cleanOrder(order), {priority: 'event'});
        }
      };

      var loadOrder = function() {
        try {
          var raw = window.localStorage.getItem(storageKey);
          return cleanOrder(raw ? JSON.parse(raw) : []);
        } catch (e) {
          return [];
        }
      };

      var saveOrder = function() {
        var order = visibleColumnNames();
        try {
          window.localStorage.setItem(storageKey, JSON.stringify(order));
        } catch (e) {}
        setShinyOrder(order);
      };

      api.on('column-reorder.dt', function() {
        window.setTimeout(saveOrder, 0);
      });

      var savedOrder = loadOrder();
      if (!savedOrder.length || !window.Shiny) return;

      window.__onoffColumnOrderSeeded = window.__onoffColumnOrderSeeded || {};
      if (window.__onoffColumnOrderSeeded[storageKey]) return;
      window.__onoffColumnOrderSeeded[storageKey] = true;

      setShinyOrder(savedOrder);
      window.setTimeout(function() {
        window.Shiny.setInputValue(restoreId, new Date().getTime(), {priority: 'event'});
      }, 0);
    }",
    input_id_json,
    restore_id_json,
    storage_key_json
  ))
}

csv_export_stamp <- function(now = Sys.time()) {
  format(now, "%Y%m%d_%H%M%S")
}

# Normalized ETL data version from the shared context. Used in cache keys so
# season-level MV pulls are shared across sessions and invalidate after ETL.
shared_data_version <- function(shared) {
  version <- if (is.function(shared$data_version)) shared$data_version() else NA_character_
  version <- trimws(as.character(version %||% ""))
  if (!length(version) || is.na(version[[1]]) || !nzchar(version[[1]])) "unknown" else version[[1]]
}

# Return the persisted rating variant for an exact full-season preset shape.
# Team selection is intentionally ignored because Compare applies that filter
# locally after fetching league-wide ratings. NA means the dynamic path is
# required.
team_ratings_preset_variant <- function(p, season_bounds) {
  if (!is.list(p) || !is.list(season_bounds)) return(NA_character_)

  text_value <- function(name) {
    value <- as.character(p[[name]] %||% "")
    if (!length(value) || is.na(value[[1]])) "" else tolower(trimws(value[[1]]))
  }
  int_value <- function(name) {
    value <- suppressWarnings(as.integer(p[[name]] %||% NA_integer_))
    if (!length(value) || !is.finite(value[[1]])) NA_integer_ else value[[1]]
  }
  date_value <- function(value) {
    tryCatch({
      out <- as.Date(value)
      if (!length(out) || is.na(out[[1]])) as.Date(NA) else out[[1]]
    }, error = function(e) as.Date(NA))
  }
  text_unset <- function(name, allow_all = FALSE) {
    value <- text_value(name)
    !nzchar(value) || (isTRUE(allow_all) && identical(value, "all"))
  }
  int_unset <- function(name) is.na(int_value(name))

  start_d <- date_value(p$start_d)
  end_d <- date_value(p$end_d)
  season_start <- date_value(season_bounds$start)
  season_end <- date_value(season_bounds$end)
  if (is.na(start_d) || is.na(end_d) ||
      is.na(season_start) || is.na(season_end) ||
      !identical(start_d, season_start) ||
      !identical(end_d, season_end)) {
    return(NA_character_)
  }

  common_unfiltered <-
    text_unset("game_type_csv") &&
    text_unset("opp_ids_csv") &&
    text_unset("home_away", allow_all = TRUE) &&
    text_unset("outcome", allow_all = TRUE) &&
    text_unset("margin_status", allow_all = TRUE) &&
    !isTRUE(p$ot_margin_filter) &&
    int_unset("min_gn") &&
    int_unset("max_gn") &&
    int_unset("num_starters_off") &&
    int_unset("num_starters_def") &&
    int_unset("num_starters_def_min") &&
    int_unset("num_starters_def_max")
  if (!isTRUE(common_unfiltered)) return(NA_character_)

  no_rank <- text_unset("opp_rank_side", allow_all = TRUE) &&
    int_unset("opp_rank_n") &&
    (text_unset("opp_rank_metric") || identical(text_value("opp_rank_metric"), "net"))
  no_clutch <- int_unset("max_margin") && int_unset("max_time_remaining")
  no_last_n <- int_unset("last_n_games")
  no_off_starters <- int_unset("num_starters_off_min") &&
    int_unset("num_starters_off_max")

  if (no_rank && no_clutch && no_last_n && no_off_starters) {
    return("overall")
  }
  if (no_rank && no_clutch && no_last_n &&
      identical(int_value("num_starters_off_min"), 3L) &&
      identical(int_value("num_starters_off_max"), 5L)) {
    return("starters_hi")
  }
  if (no_rank && no_clutch && no_last_n &&
      identical(int_value("num_starters_off_min"), 0L) &&
      identical(int_value("num_starters_off_max"), 2L)) {
    return("starters_lo")
  }
  if (no_rank && no_last_n && no_off_starters &&
      identical(int_value("max_margin"), 5L) &&
      identical(int_value("max_time_remaining"), 300L)) {
    return("clutch")
  }
  if (no_rank && no_clutch && no_off_starters &&
      identical(int_value("last_n_games"), 10L)) {
    return("last10")
  }

  rank_metric <- text_value("opp_rank_metric")
  if (no_clutch && no_last_n && no_off_starters &&
      identical(int_value("opp_rank_n"), 4L) &&
      identical(rank_metric, "net")) {
    rank_side <- text_value("opp_rank_side")
    if (identical(rank_side, "top")) return("top4")
    if (identical(rank_side, "bottom")) return("bottom4")
  }

  NA_character_
}

normalize_stat_filter_cols <- function(filterable_cols) {
  cols <- if (is.function(filterable_cols)) filterable_cols() else filterable_cols
  if (is.null(cols)) return(stats::setNames(character(0), character(0)))
  if (is.list(cols) && !is.atomic(cols)) cols <- unlist(cols, use.names = TRUE)
  labels <- names(cols)
  cols <- as.character(cols)
  if (is.null(labels)) labels <- rep("", length(cols))
  keep <- nzchar(labels) & nzchar(cols)
  stats::setNames(cols[keep], labels[keep])
}

make_stat_filter_state <- function() {
  list(
    filters = reactiveVal(list()),
    next_id = reactiveVal(1L)
  )
}

reset_stat_filters <- function(state) {
  state$filters(list())
  state$next_id(1L)
  invisible(NULL)
}

apply_stat_filters <- function(df, filters) {
  if (is.null(df) || !nrow(df) || !length(filters)) return(df)
  for (f in filters) {
    col <- f$col
    if (!col %in% names(df)) next
    v <- suppressWarnings(as.numeric(df[[col]]))
    threshold <- suppressWarnings(as.numeric(f$value))
    if (length(threshold) != 1L || !is.finite(threshold)) next
    keep <- !is.na(v) & (if (identical(f$op, "le")) v <= threshold else v >= threshold)
    df <- df[keep, , drop = FALSE]
    if (!nrow(df)) break
  }
  df
}

shot_split_metric_cols <- function(label_prefix, col_prefix) {
  stats::setNames(
    paste0(col_prefix, c("_fg2_pct", "_fg2_freq", "_fg3_pct", "_fg3_freq")),
    paste(label_prefix, c("2PT%", "2PT Freq", "3PT%", "3PT Freq"))
  )
}

add_shot_split_metrics <- function(df, specs) {
  if (is.null(df) || !length(specs)) return(df)

  pct <- function(num, den) {
    out <- rep(NA_real_, length(den))
    ok <- is.finite(den) & den > 0
    out[ok] <- round(num[ok] / den[ok] * 100, 1)
    out
  }
  count_col <- function(col) {
    x <- suppressWarnings(as.numeric(df[[col]]))
    x[is.na(x)] <- 0
    x
  }

  for (prefix in names(specs)) {
    cols <- specs[[prefix]]
    if (length(cols) != 4L || !all(cols %in% names(df))) next

    fg2m <- count_col(cols[[1]])
    fg2a <- count_col(cols[[2]])
    fg3m <- count_col(cols[[3]])
    fg3a <- count_col(cols[[4]])
    total_fga <- fg2a + fg3a

    df[[paste0(prefix, "_fg2_pct")]] <- pct(fg2m, fg2a)
    df[[paste0(prefix, "_fg2_freq")]] <- pct(fg2a, total_fga)
    df[[paste0(prefix, "_fg3_pct")]] <- pct(fg3m, fg3a)
    df[[paste0(prefix, "_fg3_freq")]] <- pct(fg3a, total_fga)
  }

  df
}

# ---- Shot Profile (shot-diet) share metrics ---------------------------------
# Descriptive shares of total FGA (Plan C). Corner-3 share is of KNOWN-location
# 3PA (c3_known_att), never of all 3PA — unknown fails open to NA, not 0.

SHOT_PROFILE_METRIC_SUFFIXES <- c(
  "_layup_share", "_dunk_share", "_rim_share", "_fg3_share", "_c3_pct3", "_mid_share"
)
SHOT_PROFILE_METRIC_LABELS <- c("Lay-up%", "Dunk%", "Lay+Dunk%", "3PA%", "Corner 3 Share", "2PT Jumper%")

shot_profile_metric_cols <- function(label_prefix, col_prefix) {
  stats::setNames(
    paste0(col_prefix, SHOT_PROFILE_METRIC_SUFFIXES),
    paste(label_prefix, SHOT_PROFILE_METRIC_LABELS)
  )
}

add_shot_profile_metrics <- function(df, specs) {
  if (is.null(df) || !length(specs)) return(df)

  pct <- function(num, den) {
    out <- rep(NA_real_, length(den))
    ok <- is.finite(den) & den > 0
    out[ok] <- round(num[ok] / den[ok] * 100, 1)
    out
  }
  count_col <- function(col) {
    x <- suppressWarnings(as.numeric(df[[col]]))
    x[is.na(x)] <- 0
    x
  }

  for (prefix in names(specs)) {
    cols <- specs[[prefix]]
    if (length(cols) != 6L || !all(cols %in% names(df))) next

    layup <- count_col(cols[[1]])
    dunk  <- count_col(cols[[2]])
    fga   <- count_col(cols[[3]])
    fg3a  <- count_col(cols[[4]])
    c3a   <- count_col(cols[[5]])
    c3k   <- count_col(cols[[6]])
    rim   <- layup + dunk

    df[[paste0(prefix, "_layup_share")]] <- pct(layup, fga)
    df[[paste0(prefix, "_dunk_share")]]  <- pct(dunk, fga)
    df[[paste0(prefix, "_rim_share")]]   <- pct(rim, fga)
    df[[paste0(prefix, "_fg3_share")]]   <- pct(fg3a, fga)
    df[[paste0(prefix, "_mid_share")]]   <- pct(pmax(fga - rim - fg3a, 0), fga)
    df[[paste0(prefix, "_c3_pct3")]]     <- pct(c3a, c3k)
    df[[paste0(prefix, "_fga")]]         <- fga
  }
  df
}

setup_stat_filter_handlers <- function(prefix, input, session, filterable_cols, state) {
  add_id <- paste0(prefix, "_add_stat_filter")
  remove_id <- paste0(prefix, "_remove_stat_filter")
  col_id <- paste0(prefix, "_stat_filter_col")
  op_id <- paste0(prefix, "_stat_filter_op")
  value_id <- paste0(prefix, "_stat_filter_value")

  observeEvent(input[[add_id]], {
    cols <- normalize_stat_filter_cols(filterable_cols)
    col_label <- input[[col_id]] %||% ""
    op <- input[[op_id]] %||% "ge"
    raw_val <- input[[value_id]]
    val <- suppressWarnings(as.numeric(raw_val))
    if (!nzchar(col_label) || !col_label %in% names(cols)) return()
    if (!op %in% c("ge", "le")) return()
    if (length(val) != 1L || !is.finite(val)) return()

    new_id <- state$next_id()
    state$next_id(new_id + 1L)

    current <- state$filters()
    current[[length(current) + 1]] <- list(
      id = new_id,
      label = col_label,
      col = unname(cols[[col_label]]),
      op = op,
      value = val
    )
    state$filters(current)

    updateSelectInput(session, col_id, selected = "")
    updateRadioButtons(session, op_id, selected = "ge")
    updateNumericInput(session, value_id, value = NA)
  })

  observeEvent(input[[remove_id]], {
    rm_id <- suppressWarnings(as.integer(input[[remove_id]]))
    if (is.na(rm_id)) return()
    current <- state$filters()
    keep <- vapply(current, function(f) !identical(as.integer(f$id), rm_id), logical(1))
    state$filters(current[keep])
  }, ignoreInit = TRUE)
}

stat_filter_chips_ui <- function(prefix, state, filterable_cols, percent_hint = NULL) {
  cols <- normalize_stat_filter_cols(filterable_cols)
  choices <- names(cols)
  remove_id <- paste0(prefix, "_remove_stat_filter")
  filter_chips <- lapply(state$filters(), function(f) {
    op_sym <- if (identical(f$op, "ge")) "\u2265" else "\u2264"
    val_txt <- format(f$value, big.mark = ",", trim = TRUE)
    tags$span(
      class = "filter-chip chip-stat",
      sprintf("%s %s %s", f$label, op_sym, val_txt), " ",
      tags$a(
        href = "#",
        class = "js-shiny-event",
        `data-input-id` = remove_id,
        `data-shiny-value` = as.character(as.integer(f$id)),
        style = "margin-left:4px;color:inherit;",
        HTML("&times;")
      )
    )
  })

  pct_msg <- percent_hint
  if (is.null(pct_msg) && any(grepl("%", choices, fixed = TRUE))) {
    pct_msg <- "Percent columns: enter as 0-100."
  }

  add_btn <- bslib::popover(
    trigger = tags$span(
      class = "filter-chip filter-chip-add",
      id = paste0(prefix, "_stat_filter_add_btn"),
      tags$i(class = "bi bi-plus"), " Filter"
    ),
    title = "Add stat filter",
    placement = "bottom",
    div(
      class = paste0(prefix, "-stat-popover"),
      style = "min-width: 220px;",
      selectInput(
        paste0(prefix, "_stat_filter_col"), "Column",
        choices = c("Choose..." = "", choices),
        selected = "",
        width = "100%"
      ),
      radioButtons(
        paste0(prefix, "_stat_filter_op"), "Operator",
        choices = c("\u2265" = "ge", "\u2264" = "le"),
        selected = "ge",
        inline = TRUE
      ),
      numericInput(
        paste0(prefix, "_stat_filter_value"), "Value",
        value = NA,
        width = "100%"
      ),
      if (!is.null(pct_msg) && nzchar(pct_msg)) {
        tags$div(class = "small text-muted mb-2", pct_msg)
      },
      actionButton(
        paste0(prefix, "_add_stat_filter"), "Add",
        class = "btn-sm btn-primary w-100"
      )
    )
  )

  c(filter_chips, list(add_btn))
}

# Percentile rank in [0, 1], NA-preserving, with average ties. Shared by Tab 3
# (Israeli team ratings) and Tab 9 (EuroLeague), which held three byte-identical
# copies between them -- two inside server_tab3.R alone.
#
# invert = TRUE for metrics where low is good (defensive rating, turnover rate),
# so the colour ramp can stay in one direction everywhere.
pr_vec <- function(x, invert = FALSE) {
  n <- sum(!is.na(x))
  if (n <= 1) return(rep(NA_real_, length(x)))
  r <- rank(x, na.last = "keep", ties.method = "average")
  p <- (r - 1) / (n - 1)
  if (invert) p <- 1 - p
  as.numeric(p)
}

add_team_metric_ranks <- function(df) {
  if (is.null(df) || !NROW(df)) return(df)
  for (i in seq_len(nrow(TEAM_RATING_METRICS))) {
    source <- TEAM_RATING_METRICS$metric[[i]]
    target <- TEAM_RATING_METRICS$percentile[[i]]
    invert <- identical(TEAM_RATING_METRICS$best_direction[[i]], "asc")
    if (source %in% names(df)) df[[target]] <- pr_vec(df[[source]], invert = invert)
  }
  df
}

# Point a dateRangeInput at a season window, value and allowed range together.
# Nothing league-specific here: the caller supplies whichever bounds its league
# computes, Israeli season_date_bounds_for_year() or euro_season_date_bounds().
#
# `bounds` is a lazy argument and is forced inside the tryCatch, so a caller
# whose bounds lookup can fail keeps that protection.
#
# The guard is the documented pitfall: updateDateRangeInput() with a start
# outside min silently yields NA rather than erroring, so bad bounds must not
# reach it at all.
apply_season_date_bounds <- function(session, input_id, bounds) {
  b <- tryCatch(bounds, error = function(e) NULL)
  if (is.null(b) || !length(b$start) || !length(b$end) ||
      is.na(b$start) || is.na(b$end)) {
    return(invisible(FALSE))
  }
  updateDateRangeInput(session, input_id, start = b$start, end = b$end,
                       min = b$start, max = b$end)
  invisible(TRUE)
}

update_gn_last_n_choices <- function(session, prefix, gn_vals) {
  gn_vals <- suppressWarnings(as.integer(gn_vals))
  gn_vals <- gn_vals[is.finite(gn_vals)]
  gn_choices <- c("", as.character(gn_vals))
  last_choices <- if (length(gn_vals)) c("", as.character(seq_len(max(gn_vals, na.rm = TRUE)))) else ""
  # Cleared as before on every rebuild, except once in a restored session where
  # the bookmarked value is applied instead.
  targets <- list(gn_min = gn_choices, gn_max = gn_choices, last_n = last_choices)
  for (suffix in names(targets)) {
    id <- paste0(prefix, "_", suffix)
    choices <- targets[[suffix]]
    selected <- restore_once_selection(session, id, NULL, choices)
    updateSelectizeInput(
      session, id,
      choices = choices,
      selected = if (length(selected)) selected[[1]] else ""
    )
  }
}

resolve_gn_last_n_values <- function(min_gn = "", max_gn = "", last_n = "") {
  min_gn <- min_gn %||% ""
  max_gn <- max_gn %||% ""
  last_n <- last_n %||% ""
  min_gn <- if (nzchar(min_gn)) as.integer(min_gn) else NA_integer_
  max_gn <- if (nzchar(max_gn)) as.integer(max_gn) else NA_integer_
  last_n <- if (nzchar(last_n)) as.integer(last_n) else NA_integer_

  if (!is.na(last_n)) {
    min_gn <- NA_integer_
    max_gn <- NA_integer_
  }
  if (!is.na(min_gn) || !is.na(max_gn)) {
    last_n <- NA_integer_
  }
  if (!is.na(min_gn) && !is.na(max_gn) && min_gn > max_gn) {
    tmp <- min_gn
    min_gn <- max_gn
    max_gn <- tmp
  }

  list(min_gn = min_gn, max_gn = max_gn, last_n = last_n)
}

resolve_gn_last_n_params <- function(input, prefix) {
  resolve_gn_last_n_values(
    input[[paste0(prefix, "_gn_min")]],
    input[[paste0(prefix, "_gn_max")]],
    input[[paste0(prefix, "_last_n")]]
  )
}

setup_gn_last_n_sync <- function(session, input, prefix) {
  observeEvent(input[[paste0(prefix, "_last_n")]], {
    last_n <- input[[paste0(prefix, "_last_n")]]
    if (!is.null(last_n) && nzchar(last_n)) {
      updateSelectizeInput(session, paste0(prefix, "_gn_min"), selected = "")
      updateSelectizeInput(session, paste0(prefix, "_gn_max"), selected = "")
    }
  }, ignoreInit = TRUE)

  observeEvent(list(input[[paste0(prefix, "_gn_min")]], input[[paste0(prefix, "_gn_max")]]), {
    gn_min <- input[[paste0(prefix, "_gn_min")]] %||% ""
    gn_max <- input[[paste0(prefix, "_gn_max")]] %||% ""
    last_n <- input[[paste0(prefix, "_last_n")]] %||% ""
    if ((nzchar(gn_min) || nzchar(gn_max)) && nzchar(last_n)) {
      updateSelectizeInput(session, paste0(prefix, "_last_n"), selected = "")
    }
  }, ignoreInit = TRUE)
}

reset_gn_last_n_inputs <- function(session, prefix) {
  updateSelectizeInput(session, paste0(prefix, "_gn_min"), selected = "")
  updateSelectizeInput(session, paste0(prefix, "_gn_max"), selected = "")
  updateSelectizeInput(session, paste0(prefix, "_last_n"), selected = "")
}

reset_opp_rank_inputs <- function(session, prefix) {
  updateSelectInput(session, paste0(prefix, "_opp_rank_side"), selected = "")
  updateSelectInput(session, paste0(prefix, "_opp_rank_n"), selected = "")
  updateSelectInput(session, paste0(prefix, "_opp_rank_metric"), selected = "")
}

reset_starters_inputs <- function(session, prefix, own_prefix = "num_starters_off", opp_prefix = "num_starters_def") {
  updateSelectInput(session, paste0(prefix, "_", own_prefix, "_mode"), selected = "")
  updateSelectInput(session, paste0(prefix, "_", own_prefix), selected = "")
  updateSelectInput(session, paste0(prefix, "_", opp_prefix, "_mode"), selected = "")
  updateSelectInput(session, paste0(prefix, "_", opp_prefix), selected = "")
}

reset_clutch_inputs <- function(session, prefix, status_default = "all", margin_default = 5, minutes_default = 5) {
  updateCheckboxInput(session, paste0(prefix, "_clutch_enabled"), value = FALSE)
  updateSliderInput(session, paste0(prefix, "_clutch_margin"), value = margin_default)
  updateSelectInput(session, paste0(prefix, "_clutch_status"), selected = status_default)
  updateSliderInput(session, paste0(prefix, "_clutch_minutes"), value = minutes_default)
  updateCheckboxInput(session, paste0(prefix, "_clutch_ot_margin"), value = FALSE)
}

blank_to_na_character <- function(x) {
  val <- x %||% ""
  if (!nzchar(val)) NA_character_ else as.character(val)
}

# Which of the three EuroLeague clutch-capable readers answers a request.
# Shared by Tabs 9 and 10 -- the two surfaces that route the same four clutch
# parameters -- so the classification cannot drift between them.
#
#   "pergame" no margin/time predicate at all. The per-game facts (migrations
#             037 and 038) answer it without an action scan.
#   "dynamic" exactly the 5 / all / 5:00 preset, which has an incremental
#             per-game cache behind it.
#   "direct"  any other clutch window: an Israeli-shaped single action scan.
#
# The per-game readers deliberately take fewer parameters than the other two,
# so a caller chooses signature and parameter list together, never
# independently.
clutch_reader_kind <- function(p) {
  status <- blank_to_na_character(p$margin_status)
  status <- if (length(status) == 1L && !is.na(status)) status else "all"
  is_set <- function(x) {
    x <- suppressWarnings(as.integer(x))
    length(x) == 1L && is.finite(x)
  }
  if (!is_set(p$max_margin) && !is_set(p$max_time_remaining) &&
      identical(status, "all")) {
    return("pergame")
  }
  standard_clutch <- identical(suppressWarnings(as.integer(p$max_margin)), 5L) &&
    identical(status, "all") &&
    identical(suppressWarnings(as.integer(p$max_time_remaining)), 300L) &&
    !isTRUE(p$ot_margin_filter)
  if (isTRUE(standard_clutch)) "dynamic" else "direct"
}

blank_to_na_integer <- function(x) {
  val <- x %||% ""
  if (!nzchar(val)) {
    NA_integer_
  } else {
    suppressWarnings(as.integer(val))
  }
}

is_invalid_persisted_token <- function(x) {
  if (is.null(x)) return(logical(0))
  val <- trimws(tolower(as.character(x)))
  is.na(val) | val %in% c("undefined", "null", "nan", "na")
}

sanitize_persisted_choices <- function(x, max_len = 80L, numeric_only = FALSE) {
  if (is.null(x)) return(character(0))
  vals <- if (is.list(x)) unlist(x, recursive = FALSE, use.names = FALSE) else x
  vals <- trimws(as.character(vals))
  vals <- vals[!is.na(vals) & nzchar(vals)]
  vals <- vals[!is_invalid_persisted_token(vals)]
  if (isTRUE(numeric_only) && length(vals)) {
    nums <- suppressWarnings(as.integer(vals))
    vals <- vals[!is.na(nums)]
  }
  vals <- substr(vals, 1L, 200L)
  vals[seq_len(min(length(vals), max_len))]
}

sanitize_single_choice <- function(x, numeric_only = FALSE) {
  vals <- sanitize_persisted_choices(x, max_len = 1L, numeric_only = numeric_only)
  if (length(vals)) vals[[1]] else ""
}

# Inputs that must never enter a bookmark: one-shot actions, the idle
# heartbeat, and DataTable bookkeeping inputs. Everything else is filter
# state and is bookmarked automatically.
BOOKMARK_EXCLUDE_LITERALS <- c(
  "open_glossary", "idle_activity_ts", "hub_remembered_team",
  "home_set_default",
  "ibpl_restore_state", "ld_lineup_click", "cmp_table_row_click"
)

BOOKMARK_EXCLUDE_PATTERNS <- c(
  "^go_",
  "_reset$",
  "^ibpl_",
  paste0(
    "_(rows_current|rows_all|rows_selected|state|search|search_columns|",
    "cell_clicked|cells_selected|columns_selected|row_last_clicked)$"
  )
)

bookmark_excluded_ids <- function(input_ids) {
  ids <- as.character(input_ids %||% character(0))
  if (!length(ids)) return(character(0))
  hit <- ids %in% BOOKMARK_EXCLUDE_LITERALS
  for (pattern in BOOKMARK_EXCLUDE_PATTERNS) {
    hit <- hit | grepl(pattern, ids)
  }
  ids[hit]
}

# restoreContext$input$get() marks a value as used; force = TRUE lets the
# server re-read values that the UI already consumed via restoreInput().
restored_input_value <- function(session, id, default = character(0)) {
  ctx <- tryCatch(session$restoreContext, error = function(e) NULL)
  if (is.null(ctx) || !isTRUE(ctx$active)) return(default)
  namespaced_id <- tryCatch(session$ns(id), error = function(e) id)
  candidate_ids <- unique(c(as.character(namespaced_id), as.character(id)))
  for (candidate_id in candidate_ids) {
    val <- tryCatch(
      ctx$input$get(candidate_id, force = TRUE),
      error = function(e) NULL
    )
    if (!is.null(val) && length(val)) return(val)
  }
  default
}

restore_aware_selection <- function(session, id, current, choices) {
  candidate <- sanitize_persisted_choices(current)
  if (!length(candidate)) {
    candidate <- sanitize_persisted_choices(restored_input_value(session, id))
  }
  if (!length(candidate) || !length(choices)) return(character(0))
  intersect(candidate, as.character(unname(choices)))
}

# Some choice rebuilds deliberately clear their input (season change, tab
# re-entry). Those must still honour a bookmark once, but never resurrect it
# afterwards, so the restored value is consumed on first use per session.
restore_consumed_env <- function(session) {
  ud <- tryCatch(session$userData, error = function(e) NULL)
  if (!is.environment(ud)) return(NULL)
  if (!exists(".ibpl_restore_consumed", envir = ud, inherits = FALSE)) {
    assign(".ibpl_restore_consumed", new.env(parent = emptyenv()), envir = ud)
  }
  get(".ibpl_restore_consumed", envir = ud, inherits = FALSE)
}

restore_value_consumed <- function(session, id) {
  env <- restore_consumed_env(session)
  !is.null(env) && exists(id, envir = env, inherits = FALSE)
}

restore_once_selection <- function(session, id, current, choices) {
  candidate <- sanitize_persisted_choices(current)
  if (!length(candidate) && !restore_value_consumed(session, id)) {
    candidate <- sanitize_persisted_choices(restored_input_value(session, id))
  }
  env <- restore_consumed_env(session)
  if (!is.null(env)) assign(id, TRUE, envir = env)
  if (!length(candidate) || !length(choices)) return(character(0))
  intersect(candidate, as.character(unname(choices)))
}

csv_if_any <- function(x, integerize = FALSE) {
  if (is.null(x) || !length(x)) return(NA_character_)
  vals <- as.character(x)
  vals <- vals[nzchar(vals)]
  if (!length(vals)) return(NA_character_)
  if (isTRUE(integerize)) {
    vals <- suppressWarnings(as.integer(vals))
    vals <- vals[!is.na(vals)]
    if (!length(vals)) return(NA_character_)
  }
  paste(vals, collapse = ",")
}

resolve_clutch_params <- function(enabled, margin, status, minutes, ot_margin) {
  clutch_enabled <- isTRUE(enabled)
  list(
    max_margin = if (clutch_enabled) suppressWarnings(as.integer(margin)) else NA_integer_,
    margin_status = if (clutch_enabled) (status %||% "all") else NA_character_,
    max_time_remaining = if (clutch_enabled) suppressWarnings(as.integer(minutes)) * 60L else NA_integer_,
    ot_margin_filter = if (clutch_enabled) isTRUE(ot_margin) else FALSE
  )
}

resolve_starters_bounds <- function(off_mode, off_val, def_mode, def_val) {
  off_mode <- off_mode %||% ""
  def_mode <- def_mode %||% ""
  off_val <- if (nzchar(off_mode)) suppressWarnings(as.integer(off_val)) else NA_integer_
  def_val <- if (nzchar(def_mode)) suppressWarnings(as.integer(def_val)) else NA_integer_
  list(
    num_starters_off_min = if (identical(off_mode, "gte")) off_val else NA_integer_,
    num_starters_off_max = if (identical(off_mode, "lte")) off_val else NA_integer_,
    num_starters_def_min = if (identical(def_mode, "gte")) def_val else NA_integer_,
    num_starters_def_max = if (identical(def_mode, "lte")) def_val else NA_integer_
  )
}

# team_id -> team_name choices for a team selector. all_label = NULL omits the
# leading blank "all teams" entry, which is what a multi-select wants: an empty
# option there shows as a selectable blank tag. Both leagues use this.
team_select_choices_with_all <- function(teams_df, all_label = "\u2014 All teams \u2014") {
  if (is.null(teams_df) || !nrow(teams_df)) {
    if (is.null(all_label)) return(character(0))
    out <- ""
    names(out) <- all_label
    return(out)
  }
  ids <- as.character(teams_df$team_id)
  nms <- as.character(teams_df$team_name)
  if (is.null(all_label)) {
    out <- ids
    names(out) <- nms
    return(out)
  }
  out <- c("", ids)
  names(out) <- c(all_label, nms)
  out
}

update_single_team_selectize <- function(session, select_id, teams_df, selected = "", all_label = "\u2014 All teams \u2014") {
  updateSelectizeInput(
    session,
    select_id,
    choices = team_select_choices_with_all(teams_df, all_label = all_label),
    selected = selected,
    server = TRUE
  )
}

# ---- Four-factor point impact ----
# Estimated points per 100 poss. contributed by a 1pp change in each factor.
# Fit: weighted OLS off_ppp ~ off_efg + off_tov + off_oreb + off_ftr on
# team_metrics_by_game_mv (878 team-games, 2025-26 seasons, weights = off_poss,
# R^2 = 0.948). Stable per season and under team/opponent fixed effects.
# Refit after future seasons with scripts/fit_ff_impact_weights.R and update here.
FF_IMPACT_WEIGHTS <- c(efg = 1.45, tov = -1.36, oreb = 0.63, ftr = 0.13)

# Convert factor deltas (percentage points, natural column orientation) to
# estimated pts/100. Vectorized over delta and factor; NA in -> NA out.
# Output is the change in that side's rating: for defense columns this is
# points ALLOWED (negative = good) -- callers add the "allowed" wording.
ff_impact_pts <- function(delta, factor) {
  bad <- setdiff(unique(as.character(factor)), names(FF_IMPACT_WEIGHTS))
  if (length(bad)) stop("Unknown four-factor name(s): ", paste(bad, collapse = ", "))
  unname(FF_IMPACT_WEIGHTS[as.character(factor)] * delta)
}

# Short hover title for each est. annotation (full detail lives in the legend).
FF_IMPACT_EST_TITLE <- "Estimated points per 100 possessions"

# Shared legend/caption text for every four-factors surface: names all four
# weights once, so the per-cell annotations can stay terse.
ff_impact_legend <- function() {
  sprintf(
    "Estimated factor impact (est.): each 1pp of a factor \u2248 eFG %+.2f, TOV %+.2f, OREB %+.2f, FTR %+.2f pts per 100 poss. (league-calibrated regression weights \u2014 an approximation, not a measured stat).",
    FF_IMPACT_WEIGHTS[["efg"]], FF_IMPACT_WEIGHTS[["tov"]],
    FF_IMPACT_WEIGHTS[["oreb"]], FF_IMPACT_WEIGHTS[["ftr"]]
  )
}

# ---------------- Team hub (Tab 0) pure helpers ----------------

# Resolve the hub's default team: remembered id if it exists this season,
# else the season's net-rating leader, else the first team, else "".
hub_default_team <- function(remembered_id, teams_df, ratings_df) {
  rid <- trimws(as.character(remembered_id %||% ""))
  ids <- as.character(teams_df$team_id %||% character(0))
  if (!length(ids)) return("")
  if (length(rid) == 1 && nzchar(rid) && rid %in% ids) return(rid)
  if (!is.null(ratings_df) &&
      nrow(ratings_df) > 0 &&
      all(c("team_id", "rank_net_rtg") %in% names(ratings_df))) {
    ranks <- suppressWarnings(as.numeric(ratings_df$rank_net_rtg))
    if (any(is.finite(ranks))) {
      leader <- as.character(ratings_df$team_id[which.min(ranks)])
      if (length(leader) == 1 && leader %in% ids) return(leader)
    }
  }
  ids[[1]]
}

hub_identity_data <- function(ratings_df, ff_df, team_id) {
  tid <- suppressWarnings(as.integer(team_id))
  if (is.null(ratings_df) ||
      !nrow(ratings_df) ||
      !("team_id" %in% names(ratings_df)) ||
      !is.finite(tid)) {
    return(NULL)
  }
  row <- ratings_df[as.integer(ratings_df$team_id) == tid, , drop = FALSE]
  if (!nrow(row)) return(NULL)
  ff_row <- NULL
  if (!is.null(ff_df) && nrow(ff_df) && "team_id" %in% names(ff_df)) {
    fr <- ff_df[as.integer(ff_df$team_id) == tid, , drop = FALSE]
    if (nrow(fr)) ff_row <- fr[1, , drop = FALSE]
  }
  list(row = row[1, , drop = FALSE], n_teams = nrow(ratings_df), ff = ff_row)
}

# Four-factor mini-row with league ranks. On offense, high is good except TOV%;
# on defense, low opponent rates are good except TOV%.
hub_ff_mini <- function(ff_df, team_id, side = c("offense", "defense")) {
  side <- match.arg(side)
  prefix <- if (identical(side, "offense")) "off" else "def"
  tid <- suppressWarnings(as.integer(team_id))
  factor_names <- c("efg", "tov", "oreb", "ftr")
  metric_cols <- paste(prefix, factor_names, sep = "_")
  need <- c("team_id", metric_cols)
  if (is.null(ff_df) ||
      !nrow(ff_df) ||
      !all(need %in% names(ff_df)) ||
      !is.finite(tid)) {
    return(NULL)
  }
  idx <- which(as.integer(ff_df$team_id) == tid)
  if (!length(idx)) return(NULL)
  labels <- c("eFG%", "TOV%", "OREB%", "FTR")
  names(labels) <- metric_cols
  rows <- lapply(metric_cols, function(col) {
    x <- as.numeric(ff_df[[col]])
    higher_is_better <- if (identical(side, "offense")) {
      !identical(col, "off_tov")
    } else {
      identical(col, "def_tov")
    }
    ranked <- rank(
      if (higher_is_better) -x else x,
      ties.method = "min",
      na.last = "keep"
    )
    data.frame(
      label = labels[[col]],
      value = x[idx[[1]]],
      rank = as.integer(ranked[idx[[1]]]),
      n = sum(is.finite(x)),
      stringsAsFactors = FALSE
    )
  })
  do.call(rbind, rows)
}

hub_key_players <- function(onoff_df, team_id, min_on_poss = 100, top_n = 5) {
  need <- c("team_id", "ON Poss", "Net RTG Diff", "First Name", "Last Name")
  if (is.null(onoff_df) || !nrow(onoff_df) || !all(need %in% names(onoff_df))) {
    return(NULL)
  }
  tid <- suppressWarnings(as.integer(team_id))
  if (!is.finite(tid)) return(NULL)
  keep <- as.integer(onoff_df$team_id) == tid &
    dplyr::coalesce(as.numeric(onoff_df[["ON Poss"]]), 0) >= min_on_poss &
    is.finite(as.numeric(onoff_df[["Net RTG Diff"]]))
  df <- onoff_df[which(keep), , drop = FALSE]
  if (!nrow(df)) return(NULL)
  df <- df[order(-as.numeric(df[["Net RTG Diff"]])), , drop = FALSE]
  utils::head(df, top_n)
}

hub_top_scorer <- function(ts_df, team_id, min_gp = 3) {
  need <- c("team_id", "pts", "gp")
  if (is.null(ts_df) || !nrow(ts_df) || !all(need %in% names(ts_df))) {
    return(NULL)
  }
  tid <- suppressWarnings(as.integer(team_id))
  if (!is.finite(tid)) return(NULL)
  keep <- as.integer(ts_df$team_id) == tid &
    dplyr::coalesce(as.numeric(ts_df$gp), 0) >= min_gp
  df <- ts_df[which(keep), , drop = FALSE]
  if (!nrow(df)) return(NULL)
  df$ppg <- as.numeric(df$pts) / pmax(as.numeric(df$gp), 1)
  df <- df[is.finite(df$ppg), , drop = FALSE]
  if (!nrow(df)) return(NULL)
  df[which.max(df$ppg), , drop = FALSE]
}

hub_best_worst_lineups <- function(lineups_df) {
  need <- c("player_names_str", "net_rtg", "off_poss", "def_poss")
  if (is.null(lineups_df) ||
      !nrow(lineups_df) ||
      !all(need %in% names(lineups_df))) {
    return(NULL)
  }
  df <- lineups_df
  df$total_poss <- dplyr::coalesce(as.numeric(df$off_poss), 0) +
    dplyr::coalesce(as.numeric(df$def_poss), 0)
  df <- df[is.finite(as.numeric(df$net_rtg)), , drop = FALSE]
  if (!nrow(df)) return(NULL)
  list(
    best = df[which.max(as.numeric(df$net_rtg)), , drop = FALSE],
    worst = df[which.min(as.numeric(df$net_rtg)), , drop = FALSE]
  )
}

hub_ordinal <- function(n) {
  n <- as.integer(n)
  if (length(n) != 1L || is.na(n)) return("")
  suffix <- if (n %% 100 %in% 11:13) {
    "th"
  } else {
    switch(
      as.character(n %% 10),
      `1` = "st",
      `2` = "nd",
      `3` = "rd",
      "th"
    )
  }
  paste0(n, suffix)
}

# League rank for a team's net rating in a team-rating result set.
hub_net_rtg_rank <- function(ratings_df, team_id) {
  need <- c("team_id", "net_rtg")
  tid <- suppressWarnings(as.integer(team_id))
  if (is.null(ratings_df) ||
      !nrow(ratings_df) ||
      !all(need %in% names(ratings_df)) ||
      !is.finite(tid)) {
    return(NA_integer_)
  }
  idx <- which(as.integer(ratings_df$team_id) == tid)
  if (!length(idx)) return(NA_integer_)
  ranked <- rank(
    -suppressWarnings(as.numeric(ratings_df$net_rtg)),
    ties.method = "min",
    na.last = "keep"
  )
  value <- ranked[idx[[1]]]
  if (is.finite(value)) as.integer(value) else NA_integer_
}

# Storyline spec list. Each entry: id, Compare preset id ("" = no Compare
# preset; the line deep-links to Tab 3 instead), min sample size per side
# (total possessions), and a sentence builder over two result rows.
hub_storyline_specs <- function() {
  rank_value <- function(row) {
    value <- suppressWarnings(as.integer(row$net_rtg_rank %||% NA_integer_))
    if (length(value) != 1L || is.na(value)) {
      stop("Missing net-rating rank")
    }
    value
  }

  list(
    list(
      id = "starters_bench",
      preset = "starters_bench",
      min_poss = 100,
      sentence = function(a, b) {
        delta <- as.numeric(a$net_rtg) - as.numeric(b$net_rtg)
        who <- if (delta >= 0) {
          "Starter-heavy lineups (3+ starters) outscore bench-heavy ones"
        } else {
          "Bench-heavy lineups (2 or fewer starters) outscore starter-heavy ones"
        }
        sprintf("%s by %.1f pts per 100", who, abs(delta))
      }
    ),
    list(
      id = "clutch",
      preset = "clutch",
      min_poss = 100,
      sentence = function(a, b) {
        delta <- as.numeric(a$net_rtg) - as.numeric(b$net_rtg)
        sprintf(
          "Clutch net rating %+.1f — %.1f pts per 100 %s than overall",
          as.numeric(a$net_rtg),
          abs(delta),
          if (delta >= 0) "better" else "worse"
        )
      }
    ),
    list(
      id = "last10",
      preset = "",
      min_poss = 100,
      sentence = function(a, b) {
        sprintf(
          "Last 10 games: net rating %+.1f vs %+.1f on the season",
          as.numeric(a$net_rtg),
          as.numeric(b$net_rtg)
        )
      }
    ),
    list(
      id = "top_bottom_4",
      preset = "top_bottom_rank",
      min_poss = 100,
      sentence = function(a, b) {
        sprintf(
          "Net rating vs Top 4: %+.1f (%s in league) · vs Bottom 4: %+.1f (%s in league)",
          as.numeric(a$net_rtg),
          hub_ordinal(rank_value(a)),
          as.numeric(b$net_rtg),
          hub_ordinal(rank_value(b))
        )
      }
    )
  )
}

# Render qualified storylines. fetch_pair(id) returns list(a=row, b=row) or
# NULL; rows carry net_rtg/off_poss/def_poss. Lines that error, miss data, or
# fall under min_poss are skipped entirely.
hub_storyline_lines <- function(specs, fetch_pair) {
  out <- list()
  for (sp in specs) {
    pair <- tryCatch(fetch_pair(sp$id), error = function(e) NULL)
    if (is.null(pair) ||
        is.null(pair$a) ||
        is.null(pair$b) ||
        !nrow(pair$a) ||
        !nrow(pair$b)) {
      next
    }
    total_poss <- function(row) {
      dplyr::coalesce(as.numeric(row$off_poss), 0) +
        dplyr::coalesce(as.numeric(row$def_poss), 0)
    }
    if (total_poss(pair$a) < sp$min_poss ||
        total_poss(pair$b) < sp$min_poss) {
      next
    }
    txt <- tryCatch(sp$sentence(pair$a, pair$b), error = function(e) NULL)
    if (is.null(txt) || length(txt) != 1L || is.na(txt) || !nzchar(txt)) next
    out[[length(out) + 1L]] <- list(
      id = sp$id,
      preset = sp$preset,
      text = txt
    )
  }
  out
}

# Auto minimum-possessions threshold: the kth largest usage value, rounded up
# to `step`, where k is the row-count target. Shared by Tab 2 (Israeli lineups)
# and Tab 10 (EuroLeague lineups). Returns 0 when the population already fits
# under the target, and NA when there is nothing to rank.
auto_minposs_from_df <- function(df, usage_col = "total_poss", step = 10L,
                                 target_rows = 150L) {
  if (is.null(df) || !NROW(df)) return(NA_integer_)
  if (!usage_col %in% names(df)) return(NA_integer_)
  vals <- suppressWarnings(as.numeric(df[[usage_col]]))
  vals <- vals[is.finite(vals)]
  if (!length(vals)) return(NA_integer_)
  vals <- sort(vals, decreasing = TRUE)
  n <- length(vals)
  if (n <= target_rows) return(0L)
  kth <- vals[target_rows]
  if (!is.finite(kth)) return(NA_integer_)
  as.integer(ceiling(kth / step) * step)
}

# Lineup player-set filtering, shared by Tab 2 (Israeli lineups) and Tab 10
# (EuroLeague lineups). PostgreSQL hands an int array back as '{1,2,3}' text,
# so parse_player_ids() accepts that form as well as a list column.
#
# Semantics match the SQL predicates they stand in for on the fast path:
# players-on is "unit contains all of these" (@>), players-off is "unit
# overlaps none of these" (NOT &&).
parse_player_ids <- function(x) {
  if (is.null(x)) return(integer(0))
  if (is.list(x)) {
    vals <- suppressWarnings(as.integer(unlist(x, use.names = FALSE)))
    return(vals[!is.na(vals)])
  }
  s <- gsub("[{}\\s]", "", as.character(x))
  if (!nzchar(s)) return(integer(0))
  vals <- suppressWarnings(as.integer(strsplit(s, ",", fixed = TRUE)[[1]]))
  vals[!is.na(vals)]
}

ensure_player_ids_list <- function(df) {
  if (is.null(df) || NROW(df) == 0L || !("player_ids" %in% names(df))) return(df)
  if ("player_ids_list" %in% names(df)) return(df)
  df$player_ids_list <- lapply(df$player_ids, parse_player_ids)
  df
}

apply_local_lineup_filters <- function(df, p) {
  if (is.null(df) || NROW(df) == 0L) return(df)
  df <- ensure_player_ids_list(df)
  if (!is.na(p$team_csv) && nzchar(p$team_csv)) {
    team_ids <- as.integer(strsplit(p$team_csv, ",")[[1]])
    df <- df[df$team_id %in% team_ids, , drop = FALSE]
  }
  if (!is.na(p$player_csv) && nzchar(p$player_csv)) {
    on_ids <- as.integer(strsplit(p$player_csv, ",")[[1]])
    keep <- vapply(df$player_ids_list, function(x) all(on_ids %in% x), logical(1))
    df <- df[keep, , drop = FALSE]
  }
  if (!is.na(p$player_off_csv) && nzchar(p$player_off_csv)) {
    off_ids <- as.integer(strsplit(p$player_off_csv, ",")[[1]])
    keep <- vapply(df$player_ids_list, function(x) !any(off_ids %in% x), logical(1))
    df <- df[keep, , drop = FALSE]
  }
  df
}

# Auto minimum-possessions helpers for the on/off surfaces, shared by Tab 1
# (Israeli) and Tab 8 (EuroLeague). Moved here verbatim from both server
# files, which held byte-identical copies.
#
# auto_min_on_from_df()  keeps the top AUTO_TOP_PCT of rows by usage.
# auto_min_all_from_df() additionally requires BOTH the on and off
#   possession counts to clear the bar.
# resolve_poss_cols()    picks the usage columns for the active view mode,
#   because Summary and Four Factors label them differently.
AUTO_TOP_PCT <- 0.35

auto_min_on_from_df <- function(df, usage_col, step = 10L) {
  if (is.null(df) || !NROW(df)) return(NA_integer_)
  if (!usage_col %in% names(df)) return(NA_integer_)
  n <- nrow(df)
  top_n <- max(1L, ceiling(n * AUTO_TOP_PCT))
  df_ord <- df %>% arrange(desc(.data[[usage_col]]))
  df_top <- df_ord[seq_len(min(top_n, n)), , drop = FALSE]
  min_needed <- suppressWarnings(min(df_top[[usage_col]], na.rm = TRUE))
  if (!is.finite(min_needed)) return(NA_integer_)
  as.integer(floor(min_needed / step) * step)
}

auto_min_all_from_df <- function(df, usage_col, on_col, off_col, step = 10L) {
  if (is.null(df) || !NROW(df)) return(NA_integer_)
  if (!usage_col %in% names(df) || !on_col %in% names(df) || !off_col %in% names(df)) return(NA_integer_)
  n <- nrow(df)
  top_n <- max(1L, ceiling(n * AUTO_TOP_PCT))
  df_ord <- df %>% arrange(desc(.data[[usage_col]]))
  df_top <- df_ord[seq_len(min(top_n, n)), , drop = FALSE]
  poss_min <- pmin(df_top[[on_col]], df_top[[off_col]])
  min_needed <- suppressWarnings(min(poss_min, na.rm = TRUE))
  if (!is.finite(min_needed)) return(NA_integer_)
  as.integer(floor(min_needed / step) * step)
}

resolve_poss_cols <- function(df, mode) {
  if (identical(mode, "Four Factors")) {
    if (all(c("off_on_poss", "off_off_poss") %in% names(df))) {
      return(list(on = "off_on_poss", off = "off_off_poss"))
    }
  } else {
    if (all(c("ON Poss", "OFF Poss") %in% names(df))) {
      return(list(on = "ON Poss", off = "OFF Poss"))
    }
    if (all(c("off_on_poss", "off_off_poss") %in% names(df))) {
      return(list(on = "off_on_poss", off = "off_off_poss"))
    }
  }
  list(on = NA_character_, off = NA_character_)
}

# Four-factor derived display columns and percentile ranks for the on/off
# tabs, shared by Tab 1 (Israeli) and Tab 8 (EuroLeague). Moved here verbatim
# from both server files, which held byte-identical copies.
#
# Ranks are computed on the FULL population, before any team or min-poss
# filtering, so narrowing the table never reshuffles the percentiles the cells
# color by. adaptive_baseline() lowers the ranking threshold when the window is
# sparse; rows under it come out NA, which the renderers show as unranked.
onoff_add_ff_ranks <- function(df) {
    # Derived display columns
    df <- df %>% mutate(
      `Off Rtg Diff` = as.numeric(`Off ON Diff`),
      `Def Rtg Diff` = as.numeric(`Def ON Diff`),
      `Net Diff`     = round(`Net RTG Diff`, 1)
    )

    # Calculate ALL ranks on full unfiltered dataset
    # Adaptive baseline: lower threshold when data is sparse (narrow date ranges)
    rank_thresh <- adaptive_baseline(df$off_on_poss)

    # Background color ranks (pr_ prefix)
    df <- df %>% mutate(
      pr_net_diff = percent_rank(if_else(off_on_poss >= rank_thresh, coalesce(`Net Diff`, -999), NA_real_)),
      pr_off_rtg  = percent_rank(if_else(off_on_poss >= rank_thresh, coalesce(`Off Rtg Diff`, -999), NA_real_)),
      pr_def_rtg  = percent_rank(if_else(off_on_poss >= rank_thresh, coalesce(`Def Rtg Diff`, 999), NA_real_)),

      pr_diff_off_efg  = percent_rank(if_else(off_on_poss >= rank_thresh, off_on_efg - off_off_efg, NA_real_)),
      pr_diff_off_oreb = percent_rank(if_else(off_on_poss >= rank_thresh, off_on_oreb - off_off_oreb, NA_real_)),
      pr_diff_off_ftr  = percent_rank(if_else(off_on_poss >= rank_thresh, off_on_ftr - off_off_ftr, NA_real_)),
      pr_diff_off_tov  = percent_rank(if_else(off_on_poss >= rank_thresh, off_on_tov - off_off_tov, NA_real_)),

      pr_diff_def_efg  = percent_rank(if_else(off_on_poss >= rank_thresh, def_on_efg - def_off_efg, NA_real_)),
      pr_diff_def_oreb = percent_rank(if_else(off_on_poss >= rank_thresh, def_on_oreb - def_off_oreb, NA_real_)),
      pr_diff_def_ftr  = percent_rank(if_else(off_on_poss >= rank_thresh, def_on_ftr - def_off_ftr, NA_real_)),
      pr_diff_def_tov  = percent_rank(if_else(off_on_poss >= rank_thresh, def_on_tov - def_off_tov, NA_real_))
    )

    # Dot position ranks (_rank suffix) for range bar visuals
    raw_cols <- c("off_on_efg", "off_off_efg", "off_on_oreb", "off_off_oreb",
                  "off_on_tov", "off_off_tov", "off_on_ftr", "off_off_ftr",
                  "def_on_efg", "def_off_efg", "def_on_oreb", "def_off_oreb",
                  "def_on_tov", "def_off_tov", "def_on_ftr", "def_off_ftr")
    for (col in intersect(raw_cols, names(df))) {
      vals <- if_else(df$off_on_poss >= rank_thresh, coalesce(df[[col]], 0), NA_real_)
      df[[paste0(col, "_rank")]] <- percent_rank(vals) * 100
    }

    df
}

# Local row filters for the on/off tabs, shared by Tab 1 (Israeli) and Tab 8
# (EuroLeague). Both leagues apply the team and minimum-possession filters in R
# rather than in SQL, because the percentile ranks must be computed on the full
# population first -- see onoff_add_ff_ranks().
#
# The two view modes name their possession columns differently, hence two
# functions: Summary carries the display names, Four Factors the raw ones. The
# min_all bar is a floor on the WEAKEST side, so a player needs enough evidence
# both on and off the court to appear at all.
#
# Every filter is guarded on the columns being present. The Summary MV branch
# and the Four Factors branch previously left their final min_on filter
# unguarded, which errored rather than no-op'd on a frame without the column;
# the other call sites already guarded it, so this settles on the guarded form.
onoff_filter_summary_rows <- function(df, team_ids, min_all, min_on) {
  if (!is.null(team_ids) && length(team_ids) > 0) {
    df <- df %>% filter(team_id %in% !!team_ids)
  }
  if (all(c("ON Poss", "OFF Poss") %in% names(df))) {
    df <- df %>%
      filter(
        pmin(
          dplyr::coalesce(`ON Poss`, 0),
          dplyr::coalesce(`OFF Poss`, 0)
        ) >= !!min_all
      )
  }
  if ("ON Poss" %in% names(df)) {
    df <- df %>% filter(`ON Poss` >= !!min_on)
  }
  df
}

onoff_filter_ff_rows <- function(df, team_ids, min_all, min_on) {
  if (!is.null(team_ids) && length(team_ids) > 0) {
    df <- df %>% filter(team_id %in% !!team_ids)
  }
  if (all(c("off_on_poss", "off_off_poss", "def_on_poss", "def_off_poss") %in% names(df))) {
    df <- df %>%
      filter(
        pmin(
          dplyr::coalesce(off_on_poss, 0),
          dplyr::coalesce(off_off_poss, 0),
          dplyr::coalesce(def_on_poss, 0),
          dplyr::coalesce(def_off_poss, 0)
        ) >= !!min_all
      )
  }
  if ("off_on_poss" %in% names(df)) {
    df <- df %>% filter(off_on_poss >= !!min_on)
  }
  df
}

# Auto minimum-possessions wiring for the on/off tabs, shared by Tab 1
# (Israeli) and Tab 8 (EuroLeague), which held observer-for-observer copies of
# it. Five observers in the order the server bodies created them, because
# Shiny flushes observers in creation order:
#
#   1-2. manual override -- moving a slider by hand turns auto off, unless the
#        value equals the one this code just wrote (state$updating and the
#        last_auto slots exist to tell those two cases apart).
#   3.   re-arm -- any filter change turns auto back on, except during a reset.
#   4-5. the bars themselves, each triggered by the OTHER slider so setting one
#        can relax the other without the two chasing each other.
#
# Both bars only ever LOWER the slider (cur_val <= min_needed returns early):
# auto-min exists to stop a stale high threshold emptying the table, not to
# overrule a deliberately loose one.
#
# `sources` supplies the data each league fetches its own way -- see
# onoff_auto_min_base_df(). Every element is a function so the server body can
# call this before the reactives it names are assigned.
setup_onoff_auto_min <- function(input, session, min_on_id, min_all_id,
                                 state, auto_enabled, resetting,
                                 mode_r, triggers, sources) {
  manual <- list(list(id = min_on_id, slot = "last_auto"),
                 list(id = min_all_id, slot = "last_auto_all"))
  for (m in manual) {
    local({
      spec <- m
      observeEvent(input[[spec$id]], {
        if (isTRUE(state$updating)) return(invisible(NULL))
        cur_val <- as.integer(input[[spec$id]])
        last_auto <- as.integer(state[[spec$slot]])
        if (!is.na(cur_val) && !is.na(last_auto) && cur_val == last_auto) {
          return(invisible(NULL))
        }
        auto_enabled(FALSE)
      }, ignoreInit = TRUE)
    })
  }

  observeEvent(triggers(), {
    if (isTRUE(resetting())) return(invisible(NULL))
    auto_enabled(TRUE)
  }, ignoreInit = TRUE)

  bars <- list(
    list(
      id = min_on_id, trigger_id = min_all_id, slot = "last_auto",
      gate_min_on = FALSE,
      ready = function(cols) !is.na(cols$on),
      compute = function(df, cols) auto_min_on_from_df(df, usage_col = cols$on, step = 10L)
    ),
    list(
      id = min_all_id, trigger_id = min_on_id, slot = "last_auto_all",
      gate_min_on = TRUE,
      ready = function(cols) !is.na(cols$on) && !is.na(cols$off),
      compute = function(df, cols) {
        auto_min_all_from_df(df, usage_col = cols$on, on_col = cols$on,
                             off_col = cols$off, step = 10L)
      }
    )
  )

  for (b in bars) {
    local({
      spec <- b
      observeEvent(list(triggers(), input[[spec$trigger_id]]), {
        if (!isTRUE(auto_enabled())) return(invisible(NULL))

        mode <- mode_r()
        df_base <- onoff_auto_min_base_df(
          mode, sources,
          min_on = if (isTRUE(spec$gate_min_on)) input[[min_on_id]] else NULL
        )

        cols <- resolve_poss_cols(df_base, mode)
        if (!spec$ready(cols)) return(invisible(NULL))
        min_needed <- spec$compute(df_base, cols)
        cur_val <- as.integer(input[[spec$id]])
        if (is.na(min_needed) || is.na(cur_val)) return(invisible(NULL))
        if (cur_val <= min_needed) return(invisible(NULL))

        state$updating <- TRUE
        updateSliderInput(session, spec$id, value = min_needed)
        state$updating <- FALSE
        state[[spec$slot]] <- min_needed
      }, ignoreInit = TRUE)
    })
  }
}

# The population the auto-min bars measure, for the active view mode. Shared by
# Tab 1 and Tab 8; the league-specific parts arrive through `sources`:
#
#   ff()        ranked four-factor frame       mv()    season materialized view
#   fallback()  is the filtered path active?   live()  filtered-path pull with
#   team_ids()  currently selected teams               BOTH bars at zero
#
# live() must not pre-filter on the possession bars: the threshold is derived
# from the whole population, and a pre-filtered frame would ratchet it upward
# every time it ran.
onoff_auto_min_base_df <- function(mode, sources, min_on = NULL) {
  filter_teams <- function(df) {
    tids <- sources$team_ids()
    if (!is.null(tids) && length(tids) > 0) df <- df %>% filter(team_id %in% !!tids)
    df
  }

  if (identical(mode, "Four Factors")) {
    df <- filter_teams(sources$ff())
    if (!is.null(min_on) && "off_on_poss" %in% names(df)) {
      df <- df %>% filter(off_on_poss >= !!min_on)
    }
    return(df)
  }

  if (isTRUE(sources$fallback())) return(sources$live())
  filter_teams(sources$mv())
}

# Fast-path gate for the on/off tabs, shared by Tab 1 (Israeli) and Tab 8
# (EuroLeague). TRUE means the season materialized view cannot answer the
# question and the filtered SQL path has to run. Deliberately FALSE when only
# the team or min-poss controls moved: those are applied in R, so the MV still
# holds the answer.
#
# The raw GN inputs are re-read alongside the resolved ones because the
# resolved reactive is debounced: without this the tab would serve MV numbers
# for the moment between typing a round and the debounce firing.
#
# season_bounds, filters and gn are ordinary lazy arguments, and that is
# load-bearing: they are forced only after the date guards above them, exactly
# where the server bodies used to call them.
onoff_fallback_needed <- function(rng, season_bounds, filters, gn, input, prefix) {
  if (is.null(rng)) return(FALSE)
  start_d <- as.Date(rng[1])
  end_d <- as.Date(rng[2])
  if (is.na(start_d) || is.na(end_d)) return(FALSE)

  date_changed <- (start_d != season_bounds$start) || (end_d != season_bounds$end)

  f <- filters
  extra_filters <- (!is.null(f$game_type) && any(nzchar(f$game_type))) ||
    (!is.null(f$opp_ids) && length(f$opp_ids) > 0) ||
    nzchar(f$home_away %||% "") ||
    nzchar(f$outcome %||% "") ||
    nzchar(f$rank_side %||% "") ||
    (nzchar(f$num_starters_off_mode %||% "") && nzchar(f$num_starters_off %||% "")) ||
    (nzchar(f$num_starters_def_mode %||% "") && nzchar(f$num_starters_def %||% ""))

  gn_active <- !is.na(gn$min_gn) || !is.na(gn$max_gn) || !is.na(gn$last_n)
  gn_raw_active <- nzchar(input[[paste0(prefix, "_gn_min")]] %||% "") ||
    nzchar(input[[paste0(prefix, "_gn_max")]] %||% "") ||
    nzchar(input[[paste0(prefix, "_last_n")]] %||% "")
  gn_active <- gn_active || gn_raw_active

  date_changed || extra_filters || gn_active
}

# Common input-to-query mapping for the two on/off server modules. The caller
# supplies the one genuine naming difference (`on_game_type` versus
# `euro_phase`) and, where needed, its league-specific opponent-id reactive.
game_context_filter_values <- function(input, prefix,
                                       game_type_id = paste0(prefix, "_game_type")) {
  list(
    game_type = input[[game_type_id]],
    opp_ids = input[[paste0(prefix, "_opponents")]],
    home_away = input[[paste0(prefix, "_home_away")]],
    outcome = input[[paste0(prefix, "_outcome")]],
    rank_side = input[[paste0(prefix, "_opp_rank_side")]],
    rank_n = input[[paste0(prefix, "_opp_rank_n")]],
    metric = input[[paste0(prefix, "_opp_rank_metric")]],
    gn_min = input[[paste0(prefix, "_gn_min")]],
    gn_max = input[[paste0(prefix, "_gn_max")]],
    last_n = input[[paste0(prefix, "_last_n")]],
    num_starters_off_mode = input[[paste0(prefix, "_num_starters_off_mode")]],
    num_starters_off = input[[paste0(prefix, "_num_starters_off")]],
    num_starters_def_mode = input[[paste0(prefix, "_num_starters_def_mode")]],
    num_starters_def = input[[paste0(prefix, "_num_starters_def")]]
  )
}

game_context_db_args <- function(filters, gn, opponent_ids = filters$opp_ids,
                                 integerize_opponents = FALSE) {
  starters <- resolve_starters_bounds(
    off_mode = filters$num_starters_off_mode,
    off_val = filters$num_starters_off,
    def_mode = filters$num_starters_def_mode,
    def_val = filters$num_starters_def
  )

  list(
    game_type_csv = csv_if_any(filters$game_type),
    opp_ids_csv = csv_if_any(opponent_ids, integerize = integerize_opponents),
    home_away = blank_to_na_character(filters$home_away),
    outcome = blank_to_na_character(filters$outcome),
    opp_rank_side = blank_to_na_character(filters$rank_side),
    opp_rank_n = blank_to_na_integer(filters$rank_n),
    opp_rank_metric = blank_to_na_character(filters$metric),
    min_gn = gn$min_gn,
    max_gn = gn$max_gn,
    last_n_games = gn$last_n,
    num_starters_off_min = starters$num_starters_off_min,
    num_starters_off_max = starters$num_starters_off_max,
    num_starters_def_min = starters$num_starters_def_min,
    num_starters_def_max = starters$num_starters_def_max
  )
}

# Backward-compatible names for the first consumer pair. New lineup and team
# tabs use the neutral names above; these aliases keep existing call sites and
# bookmarks stable while the extraction proceeds incrementally.
onoff_filter_values <- game_context_filter_values
onoff_db_args <- game_context_db_args

# ---- Stat-filter column menus for the on/off tabs ----
# Both leagues offer the same Summary and Four Factors menus, so these vectors
# moved here verbatim from the two server files, which held byte-identical
# copies. Tab 1's Shot Profile menu stays in server_tab1.R: EuroLeague has no
# shot coordinates, so there is no second consumer to share it with.
ONOFF_SUMMARY_FILTERABLE_COLS <- c(
  "Net" = "Net RTG Diff",
  "Off" = "Off ON Diff",
  "Def" = "Def ON Diff",
  "On Off PPP" = "Off ON PPP",
  "On Def PPP" = "Def ON PPP",
  "On Net Rtg" = "On Net RTG",
  "On Off Shot" = "Off Shot ON",
  shot_split_metric_cols("On Off", "on_off"),
  "On Def Shot" = "Def Shot ON",
  shot_split_metric_cols("On Def", "on_def"),
  "Off Off PPP" = "Off OFF PPP",
  "Off Def PPP" = "Def OFF PPP",
  "Off Net Rtg" = "Off Net RTG",
  "Off Off Shot" = "Off Shot OFF",
  shot_split_metric_cols("Off Off", "off_off"),
  "Off Def Shot" = "Def Shot OFF",
  shot_split_metric_cols("Off Def", "off_def"),
  "Min" = "minutes",
  "On Poss" = "ON Poss",
  "Off Poss" = "OFF Poss"
)

ONOFF_FF_FILTERABLE_COLS <- c(
  "Net Diff" = "Net Diff",
  "Off Rtg Diff" = "Off Rtg Diff",
  "Off eFG% Diff" = "Off eFG% Diff",
  "Off OREB% Diff" = "Off OREB% Diff",
  "Off TOV% Diff" = "Off TOV% Diff",
  "Off FTR Diff" = "Off FTR Diff",
  "Def Rtg Diff" = "Def Rtg Diff",
  "Def eFG% Diff" = "Def eFG% Diff",
  "Def OREB% Diff" = "Def OREB% Diff",
  "Def TOV% Diff" = "Def TOV% Diff",
  "Def FTR Diff" = "Def FTR Diff",
  "Min" = "minutes",
  "On Poss" = "ON Poss",
  "Off Poss" = "OFF Poss"
)

# Four-factor ON/OFF diff cell for the on/off tabs, shared by Tab 1 (Israeli)
# and Tab 8 (EuroLeague). Renders the diff, the on/off values, a percentile
# range bar, and optionally an estimated points-impact line.
#
# Three-line team-ratings cell: value, rank, rank movement. Moved here verbatim
# from server_tab3.R, which is why show_delta is an argument rather than a
# closure over the render scope; Tab 9 had a second copy that differed only in
# always showing the delta, which is what the default reproduces.
fmt_rank_cell <- function(value, rank_now, delta = NA_integer_, digits = 1,
                          show_delta = TRUE) {
  v <- suppressWarnings(as.numeric(value))
  r <- suppressWarnings(as.integer(rank_now))
  d <- suppressWarnings(as.integer(delta))
  value_txt <- ifelse(is.na(v), "NA", format(round(v, digits), nsmall = digits, trim = TRUE))
  rank_txt <- ifelse(is.na(r), "#NA", paste0("#", r))
  delta_txt <- ifelse(
    !show_delta | is.na(d),
    "\u2014",
    ifelse(d > 0, paste0("\u25b2", abs(d)), ifelse(d < 0, paste0("\u25bc", abs(d)), "\u2194"))
  )
  paste0(value_txt, "<br>", rank_txt, "<br>", delta_txt)
}

# Shared Team Ratings metric contract. Both league tabs use these definitions
# for ranks, display columns, colour polarity, and best-first sorting. League
# modules remain responsible only for obtaining their correctly scoped facts.
TEAM_RATING_METRICS <- data.frame(
  metric = c(
    "off_ppp", "off_efg", "off_oreb", "off_tov", "off_ftr",
    "def_ppp", "def_efg", "def_oreb", "def_tov", "def_ftr", "net_rtg"
  ),
  percentile = c(
    "pr_off_ppp", "pr_off_efg", "pr_off_oreb", "pr_off_tov", "pr_off_ftr",
    "pr_def_ppp", "pr_def_efg", "pr_def_oreb", "pr_def_tov", "pr_def_ftr", "pr_net"
  ),
  best_direction = c(
    "desc", "desc", "desc", "asc", "desc",
    "asc", "asc", "asc", "desc", "asc", "desc"
  ),
  stringsAsFactors = FALSE
)

team_rating_rank_vectors <- function(df) {
  if (is.null(df) || !NROW(df)) return(list())
  out <- list()
  for (i in seq_len(nrow(TEAM_RATING_METRICS))) {
    metric <- TEAM_RATING_METRICS$metric[[i]]
    if (!metric %in% names(df)) next
    values <- suppressWarnings(as.numeric(df[[metric]]))
    out[[metric]] <- if (identical(TEAM_RATING_METRICS$best_direction[[i]], "asc")) {
      dplyr::min_rank(values)
    } else {
      dplyr::min_rank(dplyr::desc(values))
    }
  }
  out
}

team_rating_rank_deltas <- function(df, previous = NULL, show_delta = TRUE) {
  current <- team_rating_rank_vectors(df)
  deltas <- lapply(current, function(x) rep(NA_integer_, length(x)))
  if (!isTRUE(show_delta) || is.null(previous) || !NROW(previous) ||
      !"team_id" %in% names(df) || !"team_id" %in% names(previous)) {
    return(list(current = current, delta = deltas))
  }
  previous_ranks <- team_rating_rank_vectors(previous)
  current_keys <- as.character(df$team_id)
  previous_keys <- as.character(previous$team_id)
  for (metric in intersect(names(current), names(previous_ranks))) {
    previous_map <- stats::setNames(previous_ranks[[metric]], previous_keys)
    deltas[[metric]] <- as.integer(previous_map[current_keys]) - as.integer(current[[metric]])
  }
  list(current = current, delta = deltas)
}

team_rating_sort_columns <- function(display_df, source_df, metrics) {
  metrics <- intersect(metrics, intersect(names(display_df), names(source_df)))
  defs <- list()
  for (metric in metrics) {
    direction <- TEAM_RATING_METRICS$best_direction[
      match(metric, TEAM_RATING_METRICS$metric)
    ]
    sort_name <- paste0("sort__", metric)
    values <- suppressWarnings(as.numeric(source_df[[metric]]))
    values[is.na(values)] <- if (identical(direction, "asc")) Inf else -Inf
    display_df[[sort_name]] <- values
    defs[[length(defs) + 1L]] <- list(
      targets = which(names(display_df) == metric) - 1L,
      orderData = which(names(display_df) == sort_name) - 1L,
      orderSequence = if (identical(direction, "asc")) list("asc", "desc") else list("desc", "asc")
    )
  }
  list(data = display_df, definitions = defs)
}

add_team_pace_cols <- function(df, minutes_map = NULL, fallback_to_regulation = TRUE) {
  if (is.null(df) || !NROW(df)) return(df)
  gp_col <- if ("games_played" %in% names(df)) "games_played" else if ("gp" %in% names(df)) "gp" else NA_character_
  gp <- if (is.na(gp_col)) rep(NA_real_, nrow(df)) else suppressWarnings(as.numeric(df[[gp_col]]))
  gp[!is.finite(gp) | gp <= 0] <- NA_real_
  off_poss <- if ("off_poss" %in% names(df)) suppressWarnings(as.numeric(df$off_poss)) else rep(NA_real_, nrow(df))
  def_poss <- if ("def_poss" %in% names(df)) suppressWarnings(as.numeric(df$def_poss)) else rep(NA_real_, nrow(df))
  minutes_vec <- rep(NA_real_, nrow(df))
  if (!is.null(minutes_map) && "team_id" %in% names(df)) {
    minutes_vec <- suppressWarnings(as.numeric(minutes_map[as.character(df$team_id)]))
    minutes_vec[!is.finite(minutes_vec) | minutes_vec <= 0] <- NA_real_
  }
  missing_minutes <- is.na(minutes_vec) & !is.na(gp)
  if (isTRUE(fallback_to_regulation) && any(missing_minutes)) {
    minutes_vec[missing_minutes] <- gp[missing_minutes] * 40
  }
  df$minutes <- minutes_vec
  df$off_pace <- ifelse(is.na(minutes_vec), NA_real_, off_poss / minutes_vec * 40)
  df$def_pace <- ifelse(is.na(minutes_vec), NA_real_, def_poss / minutes_vec * 40)
  df
}

# show_impact is the one real league difference. The weights in
# FF_IMPACT_WEIGHTS were fitted on Israeli-league data, so EuroLeague passes
# FALSE: reusing those coefficients would state a points-per-100 impact that
# league's data never supported. Restore it there only after a refit. When
# FALSE the estimate is compiled out of the JS rather than hidden, so no
# weight can reach the rendered page.
#
# The template's internal indentation is deliberately left as it was in the
# server files: it is inside the emitted JS string, so re-indenting it would
# change the output and stop this being a provable move.
ff_diff_cell_js <- function(on_val_idx, off_val_idx, on_rank_idx, off_rank_idx,
                            impact_w = 0, impact_suffix = "", impact_tip = "",
                            show_impact = TRUE) {
  guard <- if (isTRUE(show_impact)) {
    "data !== null && data !== '' && !isNaN(parseFloat(data))"
  } else {
    "false"
  }
            JS(sprintf(
            "function(data, type, row, meta) {
               if (type === 'display') {
                 var w = %f;
                 var estLine = '';
                 if (%s) {
                   var est = parseFloat(data) * w;
                   estLine = '<div class=\"ff-impact-est\" title=\"%s\">est. ' +
                             (est >= 0 ? '+' : '\\u2212') + Math.abs(est).toFixed(1) +
                             '%s</div>';
                 }
                 var diffVal = (data === null) ? '-' : (parseFloat(data) > 0 ? '+' + data : data);
                 var onVal   = row[%d] || '-';
                 var offVal  = row[%d] || '-';
                 var onPct   = row[%d];
                 var offPct  = row[%d];

                 if (onPct === null || onPct === undefined) {
                    return '<div class=\"diff-val unranked\">' + diffVal + '</div>' +
                           '<div class=\"rank-bar-container hidden\"></div>' +
                           '<div class=\"sub-text\" style=\"opacity:0.5;\">' + onVal + ' | ' + offVal + '</div>' +
                           estLine;
                 }

                 var rangeLineLeft  = Math.min(onPct, offPct);
                 var rangeLineWidth = Math.abs(onPct - offPct);

                 return '<div class=\"diff-val\">' + diffVal + '</div>' +
                        '<div class=\"rank-bar-container\">' +
                          '<div class=\"rank-track\"></div>' +
                          '<div class=\"range-connect\" style=\"left:' + rangeLineLeft + '%%; width:' + rangeLineWidth + '%%;\"></div>' +
                          '<div class=\"dot-off\" style=\"left:' + offPct + '%%;\" title=\"Off: ' + offVal + '\"></div>' +
                          '<div class=\"dot-on\" style=\"left:' + onPct + '%%;\" title=\"On: ' + onVal + '\"></div>' +
                        '</div>' +
                        '<div class=\"sub-text\">' +
                          '<span style=\"font-weight:700; color:#222;\">' + onVal + '</span>' +
                          ' <span style=\"opacity:0.6;\">|</span> ' +
                          '<span style=\"color:#666;\">' + offVal + '</span>' +
                        '</div>' +
                        estLine;
               }
               return data;
             }", impact_w, guard, impact_tip, impact_suffix, on_val_idx, off_val_idx, on_rank_idx, off_rank_idx
  ))
}

# Summary-view DataTable for the on/off tabs, shared by Tab 1 (Israeli) and
# Tab 8 (EuroLeague). Builds the shot-split cell renderers with league averages
# computed from the supplied data, the grouped header, the column definitions
# and every percentile-rank heat colour, and returns the finished widget.
#
# It owns the whole Summary branch: the shooting-split column names, the four
# sortable display columns, the keep list, the stat filters, and the widget.
#
# Both tabs held this verbatim and byte-identical, so it moves unchanged. Its
# only free names are the CUTS / COLS_GRAD / COLS_REV / HEADER_TOOLTIP_JS
# globals from global.R; everything else is an argument or local.
#
# Indentation is left exactly as it was in the server files. make_shot_render()
# builds its JS with a multi-line sprintf template, so re-indenting the body
# would change the emitted JavaScript and stop this being a provable move.
onoff_summary_datatable <- function(df, stat_filters) {
      # Shooting split column names (16 raw + 4 display)
      shot_raw_cols <- c(
        "off_on_fg2_made", "off_on_fg2_att", "off_on_fg3_made", "off_on_fg3_att",
        "off_off_fg2_made", "off_off_fg2_att", "off_off_fg3_made", "off_off_fg3_att",
        "def_on_fg2_made", "def_on_fg2_att", "def_on_fg3_made", "def_on_fg3_att",
        "def_off_fg2_made", "def_off_fg2_att", "def_off_fg3_made", "def_off_fg3_att"
      )
      shot_display_cols <- c("Off Shot ON", "Def Shot ON", "Off Shot OFF", "Def Shot OFF")
      shot_filter_cols <- unname(c(
        shot_split_metric_cols("On Off", "on_off"),
        shot_split_metric_cols("On Def", "on_def"),
        shot_split_metric_cols("Off Off", "off_off"),
        shot_split_metric_cols("Off Def", "off_def")
      ))
      if (!"minutes" %in% names(df)) df$minutes <- NA_real_

      # Create display columns (sortable value = total FGA)
      has_shots <- all(c("off_on_fg2_att", "off_on_fg3_att") %in% names(df))
      if (has_shots) {
        df <- df %>% mutate(
          `Off Shot ON`  = coalesce(off_on_fg2_att, 0L) + coalesce(off_on_fg3_att, 0L),
          `Def Shot ON`  = coalesce(def_on_fg2_att, 0L) + coalesce(def_on_fg3_att, 0L),
          `Off Shot OFF` = coalesce(off_off_fg2_att, 0L) + coalesce(off_off_fg3_att, 0L),
          `Def Shot OFF` = coalesce(def_off_fg2_att, 0L) + coalesce(def_off_fg3_att, 0L)
        )
        df <- add_shot_split_metrics(df, list(
          on_off = c("off_on_fg2_made", "off_on_fg2_att", "off_on_fg3_made", "off_on_fg3_att"),
          on_def = c("def_on_fg2_made", "def_on_fg2_att", "def_on_fg3_made", "def_on_fg3_att"),
          off_off = c("off_off_fg2_made", "off_off_fg2_att", "off_off_fg3_made", "off_off_fg3_att"),
          off_def = c("def_off_fg2_made", "def_off_fg2_att", "def_off_fg3_made", "def_off_fg3_att")
        ))
      }

      keep_cols <- c(
        "Team", "Player",
        "Net RTG Diff", "Off ON Diff", "Def ON Diff",
        "Off ON PPP", "Def ON PPP", "On Net RTG", "Off Shot ON", "Def Shot ON",
        "Off OFF PPP", "Def OFF PPP", "Off Net RTG", "Off Shot OFF", "Def Shot OFF",
        "minutes", "ON Poss", "OFF Poss",
        shot_raw_cols,
        shot_filter_cols,
        "pr_net", "pr_off_on_d", "pr_def_on_d", "pr_off_on", "pr_def_on_inv", "pr_on_net", "pr_off_off", "pr_def_off_inv", "pr_off_net", "pr_def_on_d_inv"
      )
      df <- df[, intersect(keep_cols, names(df))]
      df <- apply_stat_filters(df, stat_filters)

      idx_net <- which(names(df) == "Net RTG Diff") - 1
      idx_on  <- which(names(df) == "Off ON PPP") - 1
      idx_off <- which(names(df) == "Off OFF PPP") - 1
      idx_use <- which(names(df) == "minutes") - 1

      diff_cols <- c("Net RTG Diff", "Off ON Diff", "Def ON Diff", "On Net RTG", "Off Net RTG")
      idx_diff <- which(names(df) %in% diff_cols) - 1

      pr_cols <- names(df)[grep("^pr_", names(df))]
      hide_idx <- which(names(df) %in% c(pr_cols, shot_raw_cols, shot_filter_cols)) - 1

      # Shooting column JS render function factory
      make_shot_render <- function(fg2m_col, fg2a_col, fg3m_col, fg3a_col,
                                   is_defense = FALSE, min_fga = 50, avg2 = 53, avg3 = 34) {
        fg2m_idx <- which(names(df) == fg2m_col) - 1
        fg2a_idx <- which(names(df) == fg2a_col) - 1
        fg3m_idx <- which(names(df) == fg3m_col) - 1
        fg3a_idx <- which(names(df) == fg3a_col) - 1
        sign_mult <- if (is_defense) -1 else 1
        js_str <- sprintf(
          "function(data, type, row, meta) {
             if (type !== 'display' || !row) return data;
             var fg2m = row[%d] || 0, fg2a = row[%d] || 0;
             var fg3m = row[%d] || 0, fg3a = row[%d] || 0;
             var totalFGA = fg2a + fg3a;
             if (!totalFGA) return '<div class=\"shot-acc-label\" style=\"color:#aaa;\">-</div>';
             var fg2pct = fg2a ? Math.round(fg2m / fg2a * 100) : 0;
             var fg3pct = fg3a ? Math.round(fg3m / fg3a * 100) : 0;
             var fg2freq = Math.round(fg2a / totalFGA * 100);
             var fg3freq = 100 - fg2freq;
             var minFGA = %d;
             var sign = %d;
             var avg2 = %d, avg3 = %d;
             function accColor(pct, avg) {
               var d = sign * (pct - avg) / avg;
               d = Math.max(-1, Math.min(1, d * 3));
               var r, g;
               if (d < 0) { r = 200; g = Math.round(200 + d * 120); }
               else       { g = 170; r = Math.round(200 - d * 150); }
               return 'rgb(' + r + ',' + g + ',60)';
             }
             var muted = totalFGA < minFGA;
             var c2 = muted ? '#bbb' : accColor(fg2pct, avg2);
             var c3 = muted ? '#bbb' : accColor(fg3pct, avg3);
             var barOpacity = muted ? 'opacity:0.3;' : '';
             var title2pct = '2PT accuracy: ' + fg2pct + '%% (' + fg2m + '/' + fg2a + ')';
             var title3pct = '3PT accuracy: ' + fg3pct + '%% (' + fg3m + '/' + fg3a + ')';
             var title2freq = '2PT frequency: ' + fg2freq + '%% of FGA (' + fg2a + '/' + totalFGA + ')';
             var title3freq = '3PT frequency: ' + fg3freq + '%% of FGA (' + fg3a + '/' + totalFGA + ')';
             return '<div class=\"shot-acc-label\">' +
               '<span title=\"' + title2pct + '\" style=\"color:' + c2 + '; font-weight:' + (muted ? '400' : '700') + '; cursor:help;\">' + fg2pct + '%%</span>' +
               ' <span style=\"opacity:0.3;\">|</span> ' +
               '<span title=\"' + title3pct + '\" style=\"color:' + c3 + '; font-weight:' + (muted ? '400' : '700') + '; cursor:help;\">' + fg3pct + '%%</span>' +
               '</div>' +
               '<div class=\"shot-bar-container\" style=\"' + barOpacity + '\">' +
               '<div class=\"shot-bar-2pt\" title=\"' + title2freq + '\" style=\"width:' + fg2freq + '%%; cursor:help;\">' + fg2freq + '%%</div>' +
               '<div class=\"shot-bar-3pt\" title=\"' + title3freq + '\" style=\"width:' + fg3freq + '%%; cursor:help;\">' + fg3freq + '%%</div>' +
               '</div>';
           }", fg2m_idx, fg2a_idx, fg3m_idx, fg3a_idx, min_fga, sign_mult, avg2, avg3
        )
        DT::JS(js_str)
      }

      # Build shot column defs with dynamic thresholds
      shot_col_defs <- list()
      if (has_shots) {
        shot_col_map <- list(
          "Off Shot ON"  = c("off_on_fg2_made", "off_on_fg2_att", "off_on_fg3_made", "off_on_fg3_att"),
          "Def Shot ON"  = c("def_on_fg2_made", "def_on_fg2_att", "def_on_fg3_made", "def_on_fg3_att"),
          "Off Shot OFF" = c("off_off_fg2_made", "off_off_fg2_att", "off_off_fg3_made", "off_off_fg3_att"),
          "Def Shot OFF" = c("def_off_fg2_made", "def_off_fg2_att", "def_off_fg3_made", "def_off_fg3_att")
        )
        # Compute per-column weighted averages from qualifying players (>= 50 FGA)
        SHOT_MIN_FGA <- 50L
        shot_avgs <- list()
        for (dn in names(shot_col_map)) {
          cols <- shot_col_map[[dn]]
          fga <- df[[dn]]
          qual <- if (is.null(fga)) rep(FALSE, nrow(df)) else (!is.na(fga) & fga >= SHOT_MIN_FGA)
          fg2a_sum <- sum(df[[cols[2]]][qual], na.rm = TRUE)
          fg3a_sum <- sum(df[[cols[4]]][qual], na.rm = TRUE)
          a2 <- if (fg2a_sum > 0) as.integer(round(sum(df[[cols[1]]][qual], na.rm = TRUE) / fg2a_sum * 100)) else 53L
          a3 <- if (fg3a_sum > 0) as.integer(round(sum(df[[cols[3]]][qual], na.rm = TRUE) / fg3a_sum * 100)) else 34L
          shot_avgs[[dn]] <- list(avg2 = a2, avg3 = a3)
        }
        for (disp_name in names(shot_col_map)) {
          cols <- shot_col_map[[disp_name]]
          target_idx <- which(names(df) == disp_name) - 1
          is_def <- grepl("^Def", disp_name)
          avgs <- shot_avgs[[disp_name]]
          if (length(target_idx) && all(cols %in% names(df))) {
            shot_col_defs[[length(shot_col_defs) + 1]] <- list(
              targets = target_idx,
              render = make_shot_render(cols[1], cols[2], cols[3], cols[4],
                                        is_defense = is_def, min_fga = SHOT_MIN_FGA,
                                        avg2 = avgs$avg2, avg3 = avgs$avg3)
            )
          }
        }
      }

      # Section border indices for shooting columns
      idx_shot_on  <- which(names(df) == "Off Shot ON") - 1
      idx_shot_off <- which(names(df) == "Off Shot OFF") - 1
      section_borders <- c(idx_net, idx_on, idx_off, idx_use)
      # Don't add shot borders - they sit inside on/off court groups

      # Header: On Court = Off PPP, Def PPP, Net Rtg, Off Shot, Def Shot (5 cols)
      # Off Court = Off PPP, Def PPP, Net Rtg, Off Shot, Def Shot (5 cols)
      sketch_summary <- htmltools::withTags(table(class = 'display', thead(
        tr(
          th(class="group-head", colspan=2, ""),
          th(class="group-head section-left-border", colspan=3, "Net Impact"),
          th(class="group-head section-left-border", colspan=5, "On Court Stats"),
          th(class="group-head section-left-border", colspan=5, "Off Court Stats"),
          th(class="group-head section-left-border", colspan=3, "Usage")
        ),
        tr(
          th(class="sub-head", "Team"), th(class="sub-head", "Player"),
          th(class="sub-head section-left-border", "Net"), th(class="sub-head", "Off"), th(class="sub-head", "Def"),
          th(class="sub-head section-left-border", "Off PPP"), th(class="sub-head", "Def PPP"), th(class="sub-head", "Net Rtg"), th(class="sub-head", "Off Shot"), th(class="sub-head", "Def Shot"),
          th(class="sub-head section-left-border", "Off PPP"), th(class="sub-head", "Def PPP"), th(class="sub-head", "Net Rtg"), th(class="sub-head", "Off Shot"), th(class="sub-head", "Def Shot"),
          th(class="sub-head section-left-border", "Min"), th(class="sub-head", "On Poss"), th(class="sub-head", "Off Poss")
        )
      )))

      dt <- datatable(df, container = sketch_summary, rownames = FALSE,
                      options = list(headerCallback = HEADER_TOOLTIP_JS, dom = "tip", pageLength = 30, scrollX = TRUE,
                                     scrollY = "70vh", scrollCollapse = TRUE,
                                     order = list(list(which(names(df) == "Net RTG Diff") - 1, "desc")),
                                     columnDefs = c(list(
                                       list(targets = section_borders, className = "section-left-border"),
                                       list(targets = hide_idx, visible = FALSE),
                                       list(targets = "_all", className = "dt-center"),
                                       list(targets = idx_diff, render = DT::JS(
                                         "function(data, type, row, meta) {",
                                         "  if (type !== 'display' || data === null) return data;",
                                         "  var val = parseFloat(data);",
                                         "  if (isNaN(val)) return data;",
                                         "  var formatted = val.toFixed(2);",
                                         "  return val > 0 ? '+' + formatted : formatted;",
                                         "}"
                                       ))
                                     ), shot_col_defs))) |>
        formatRound(c("Off ON PPP", "Def ON PPP", "Off OFF PPP", "Def OFF PPP"), 1) |>
        formatRound(intersect("minutes", names(df)), 1) |>
        formatCurrency(c("ON Poss", "OFF Poss"), currency = "", interval = 3, mark = ",", digits = 0)

      if("pr_net" %in% names(df)) dt <- formatStyle(dt, "Net RTG Diff", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_net")
      if("pr_off_on_d" %in% names(df)) dt <- formatStyle(dt, "Off ON Diff", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_on_d")
      if("pr_def_on_d" %in% names(df)) dt <- formatStyle(dt, "Def ON Diff", backgroundColor = styleInterval(CUTS, COLS_REV), valueColumns = "pr_def_on_d")

      if("pr_off_on" %in% names(df)) dt <- formatStyle(dt, "Off ON PPP", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_on")
      if("pr_def_on_inv" %in% names(df)) dt <- formatStyle(dt, "Def ON PPP", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_def_on_inv")
      if("pr_on_net" %in% names(df)) dt <- formatStyle(dt, "On Net RTG", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_on_net")

      if("pr_off_off" %in% names(df)) dt <- formatStyle(dt, "Off OFF PPP", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_off")
      if("pr_def_off_inv" %in% names(df)) dt <- formatStyle(dt, "Def OFF PPP", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_def_off_inv")
      if("pr_off_net" %in% names(df)) dt <- formatStyle(dt, "Off Net RTG", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_net")

      return(dt)
}

# ---- On/Off Four Factors DataTable (shared by the Israeli and EuroLeague tabs) ----
# The Four Factors branch of the on/off renderDT. Both leagues build the same
# table; they differ only in which stat-filter state feeds it and whether the
# diff cells carry the "est. +/-X pts" annotation.
#
# show_impact must stay FALSE for EuroLeague: FF_IMPACT_WEIGHTS were fitted on
# Israeli-league data, so reusing those coefficients would state a
# points-per-100 impact this league's data never supported. Restore only after
# refitting on EuroLeague possessions.
onoff_four_factors_datatable <- function(df, stat_filters, show_impact) {
      # === MODE 2: FOUR FACTORS ===

      metric_map <- list(
        "Off eFG% Diff"  = c("off_on_efg", "off_off_efg"),
        "Off OREB% Diff" = c("off_on_oreb", "off_off_oreb"),
        "Off TOV% Diff"  = c("off_on_tov", "off_off_tov"),
        "Off FTR Diff"   = c("off_on_ftr", "off_off_ftr"),
        "Def eFG% Diff"  = c("def_on_efg", "def_off_efg"),
        "Def OREB% Diff" = c("def_on_oreb", "def_off_oreb"),
        "Def TOV% Diff"  = c("def_on_tov", "def_off_tov"),
        "Def FTR Diff"   = c("def_on_ftr", "def_off_ftr")
      )

      # Factor key per FF diff column -> impact weight + defense wording.
      FF_METRIC_FACTOR <- c(
        "Off eFG% Diff" = "efg", "Off OREB% Diff" = "oreb",
        "Off TOV% Diff" = "tov", "Off FTR Diff" = "ftr",
        "Def eFG% Diff" = "efg", "Def OREB% Diff" = "oreb",
        "Def TOV% Diff" = "tov", "Def FTR Diff" = "ftr"
      )

      raw_cols_all <- unique(unlist(metric_map))

      # Rounding
      df <- df %>% mutate(across(all_of(intersect(raw_cols_all, names(df))), ~ round(as.numeric(.) * 100, 1)))
      df <- df %>% mutate(across(all_of(intersect(names(metric_map), names(df))), ~ round(as.numeric(.), 1)))

      # Dot position ranks (_rank columns) already computed in ff_ranked_df()

      # Rename poss columns for display
      df <- df %>% rename(`ON Poss` = off_on_poss, `OFF Poss` = off_off_poss)
      if (!"minutes" %in% names(df)) df$minutes <- NA_real_

      # 3. SELECT & ORDER COLUMNS
      vis_cols <- c("Team", "Player", "Net Diff", "Off Rtg Diff", "Def Rtg Diff", intersect(names(metric_map), names(df)), "minutes", "ON Poss", "OFF Poss")

      rank_cols <- intersect(c(
        "pr_net_diff", "pr_off_rtg", "pr_def_rtg",
        "pr_diff_off_efg", "pr_diff_off_oreb", "pr_diff_off_tov", "pr_diff_off_ftr",
        "pr_diff_def_efg", "pr_diff_def_oreb", "pr_diff_def_tov", "pr_diff_def_ftr"
      ), names(df))

      df_final <- df %>% select(all_of(vis_cols), any_of(rank_cols), ends_with("_rank"), all_of(raw_cols_all))

      final_vis_order <- c(
        "Team", "Player", "Net Diff",
        "Off Rtg Diff", "Off eFG% Diff", "Off OREB% Diff", "Off TOV% Diff", "Off FTR Diff",
        "Def Rtg Diff", "Def eFG% Diff", "Def OREB% Diff", "Def TOV% Diff", "Def FTR Diff",
        "minutes", "ON Poss", "OFF Poss"
      )

      final_vis_order <- intersect(final_vis_order, names(df_final))
      final_col_order <- c(final_vis_order, setdiff(names(df_final), final_vis_order))
      df_final <- df_final %>% select(all_of(final_col_order))
      df_final <- apply_stat_filters(df_final, stat_filters)

      defs <- list()

      for (i in seq_along(metric_map)) {
        diff_name <- names(metric_map)[i]
        if (!diff_name %in% names(df_final)) next
        target_idx <- which(names(df_final) == diff_name) - 1L

        on_col <- metric_map[[i]][1]
        off_col <- metric_map[[i]][2]

        if (on_col %in% names(df_final) && off_col %in% names(df_final)) {
          on_val_idx <- which(names(df_final) == on_col) - 1L
          off_val_idx <- which(names(df_final) == off_col) - 1L
          on_rank_idx <- which(names(df_final) == paste0(on_col, "_rank")) - 1L
          off_rank_idx <- which(names(df_final) == paste0(off_col, "_rank")) - 1L

          if (isTRUE(show_impact)) {
            impact_w <- FF_IMPACT_WEIGHTS[[FF_METRIC_FACTOR[[diff_name]]]]
            impact_suffix <- if (startsWith(diff_name, "Def")) " pts allowed" else " pts"
            impact_tip <- FF_IMPACT_EST_TITLE
          } else {
            impact_w <- 0
            impact_suffix <- ""
            impact_tip <- ""
          }

          js_func <- ff_diff_cell_js(
            on_val_idx, off_val_idx, on_rank_idx, off_rank_idx,
            impact_w, impact_suffix, impact_tip, show_impact = show_impact
          )
          defs[[length(defs) + 1]] <- list(targets = target_idx, render = js_func)
        }
      }

      # Hide auxiliary columns
      hide_cols <- c(rank_cols, raw_cols_all, names(df)[grep("_rank$", names(df))])
      hide_idx <- which(names(df_final) %in% hide_cols) - 1L
      if (length(hide_idx)) defs[[length(defs) + 1]] <- list(targets = hide_idx, visible = FALSE)

      # --- SEPARATORS (Thick borders for 3 sections) ---

      off_rtg_idx <- which(names(df_final) == "Off Rtg Diff") - 1L
      if(length(off_rtg_idx)) defs[[length(defs) + 1]] <- list(targets = off_rtg_idx, className = "section-left-border")

      def_rtg_idx <- which(names(df_final) == "Def Rtg Diff") - 1L
      if(length(def_rtg_idx)) defs[[length(defs) + 1]] <- list(targets = def_rtg_idx, className = "section-left-border")

      minutes_idx <- which(names(df_final) == "minutes") - 1L
      if(length(minutes_idx)) defs[[length(defs) + 1]] <- list(targets = minutes_idx, className = "section-left-border")

      # Net Diff Style
      net_diff_idx <- which(names(df_final) == "Net Diff") - 1L
      if(length(net_diff_idx)) {
        defs[[length(defs) + 1]] <- list(targets = net_diff_idx, className = "dt-center",
                                         render = JS("function(data, type, row) {
                                            if(type === 'display') {
                                              var v = (data !== null && parseFloat(data) > 0) ? '+' + data : data;
                                              return '<div style=\"font-weight:800; font-size:1.05em;\">' + v + '</div>';
                                            }
                                            return data;
                                         }"))
      }

      # '+' sign for Off Rtg Diff and Def Rtg Diff
      plus_sign_js <- JS(
        "function(data, type, row, meta) {",
        "  if (type !== 'display' || data === null) return data;",
        "  var val = parseFloat(data);",
        "  if (isNaN(val)) return data;",
        "  return val > 0 ? '+' + data : data;",
        "}"
      )
      off_rtg_diff_idx <- which(names(df_final) == "Off Rtg Diff") - 1L
      def_rtg_diff_idx <- which(names(df_final) == "Def Rtg Diff") - 1L
      rtg_diff_idx <- c(off_rtg_diff_idx, def_rtg_diff_idx)
      if (length(rtg_diff_idx)) defs[[length(defs) + 1]] <- list(targets = rtg_diff_idx, render = plus_sign_js)

      defs[[length(defs) + 1]] <- list(targets = "_all", className = "dt-center")

      sketch_ff <- htmltools::withTags(table(class = 'display', thead(
        tr(
          th(class = "group-head", colspan = 2, ""),
          th(class = "group-head", "Total"),
          th(class = "group-head section-left-border", colspan = 5, "Offense Impact (On-Off)"),
          th(class = "group-head section-left-border", colspan = 5, "Defense Impact (On-Off)"),
          th(class = "group-head section-left-border", colspan = 3, "Usage")
        ),
        tr(
          th(class = "sub-head", "Team"), th(class = "sub-head", "Player"),
          th(class = "sub-head", "Diff"),
          th(class = "sub-head section-left-border", "Diff"), th(class = "sub-head", "eFG%"), th(class = "sub-head", title = OFF_OREB_TOOLTIP, "OREB%"), th(class = "sub-head", "TOV%"), th(class = "sub-head", "FTR"),
          th(class = "sub-head section-left-border", "Diff"), th(class = "sub-head", "eFG%"), th(class = "sub-head", title = DEF_OREB_TOOLTIP, "OREB%"), th(class = "sub-head", "TOV%"), th(class = "sub-head", "FTR"),
          th(class = "sub-head section-left-border", "Min"), th(class = "sub-head", "On Poss"), th(class = "sub-head", "Off Poss")
        )
      )))

      dt <- datatable(df_final,
                      container = sketch_ff, rownames = FALSE,
                      escape = dt_escape_except(df_final),
                      options = list(
                        headerCallback = HEADER_TOOLTIP_JS,
                        dom = "t", pageLength = 50, deferRender = TRUE, scrollX = TRUE,
                        scrollY = "70vh", scrollCollapse = TRUE,
                        order = list(list(2, "desc")),
                        columnDefs = defs
                      )
      )

      # --- FORMAT POSS COLUMNS ---
      dt <- formatRound(dt, intersect("minutes", names(df_final)), 1)
      dt <- formatCurrency(dt, c("ON Poss", "OFF Poss"), currency = "", interval = 3, mark = ",", digits = 0)

      # --- COLOR LOGIC ---
      if ("pr_net_diff" %in% names(df_final)) dt <- formatStyle(dt, "Net Diff", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_net_diff")

      # Offense Ratings (High Diff = Good)
      if ("pr_off_rtg" %in% names(df_final)) dt <- formatStyle(dt, "Off Rtg Diff", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_rtg")

      # Defense Ratings (High Diff = Bad -> Reverse)
      if ("pr_def_rtg" %in% names(df_final)) dt <- formatStyle(dt, "Def Rtg Diff", backgroundColor = styleInterval(CUTS, COLS_REV), valueColumns = "pr_def_rtg")

      # Offense Factors
      if ("pr_diff_off_efg" %in% names(df_final)) dt <- formatStyle(dt, "Off eFG% Diff", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_diff_off_efg")
      if ("pr_diff_off_oreb" %in% names(df_final)) dt <- formatStyle(dt, "Off OREB% Diff", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_diff_off_oreb")
      if ("pr_diff_off_ftr" %in% names(df_final)) dt <- formatStyle(dt, "Off FTR Diff", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_diff_off_ftr")
      if ("pr_diff_off_tov" %in% names(df_final)) dt <- formatStyle(dt, "Off TOV% Diff", backgroundColor = styleInterval(CUTS, COLS_REV), valueColumns = "pr_diff_off_tov")

      # Defense Factors
      if ("pr_diff_def_efg" %in% names(df_final)) dt <- formatStyle(dt, "Def eFG% Diff", backgroundColor = styleInterval(CUTS, COLS_REV), valueColumns = "pr_diff_def_efg")
      if ("pr_diff_def_oreb" %in% names(df_final)) dt <- formatStyle(dt, "Def OREB% Diff", backgroundColor = styleInterval(CUTS, COLS_REV), valueColumns = "pr_diff_def_oreb")
      if ("pr_diff_def_ftr" %in% names(df_final)) dt <- formatStyle(dt, "Def FTR Diff", backgroundColor = styleInterval(CUTS, COLS_REV), valueColumns = "pr_diff_def_ftr")
      if ("pr_diff_def_tov" %in% names(df_final)) dt <- formatStyle(dt, "Def TOV% Diff", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_diff_def_tov")

      return(dt)
}

# ---- On/Off display-name cleanup (shared by the Israeli and EuroLeague tabs) ----
# Both leagues reach renderDT with either a ready "Player" column or the two
# name parts the fast and filtered paths return under different spellings.
onoff_clean_display_names <- function(df) {
    # Standard Name Cleanup
    if (!"Player" %in% names(df) && all(c("First Name", "Last Name") %in% names(df))) {
      df <- df %>% mutate(Player = paste(`First Name`, `Last Name`))
    } else if (!"Player" %in% names(df) && all(c("firstname", "lastname") %in% names(df))) {
      df <- df %>% mutate(Player = paste(firstname, lastname))
    }
    if ("team_name" %in% names(df)) df <- df %>% rename(Team = team_name)
  df
}

# Four-factors and summary DataTables for the lineup tabs, shared by Tab 2
# (Israeli) and Tab 10 (EuroLeague). Both tabs held this verbatim; the only
# difference between leagues is the clickable-Players-column anchor class and
# its click handler, carried by `spec` (`link_class`, `click_js`).
#
# `lineup_ff_datatable()` takes `raw` separately from `df` because Tab 2's
# TOTAL row sums the unfiltered reactive frame, which cannot move into a
# plain function; callers pass their un-selected display frame (default
# `raw = df` serves Tab 10, whose display frame is already un-selected).
#
# Indentation is left exactly as it was in the server files. make_shot_render()
# builds its JS with a multi-line sprintf template, so re-indenting the body
# would change the emitted JavaScript and stop this being a provable move.
lineup_ff_datatable <- function(df, stat_filters, spec, raw = NULL) {
      # raw defaults to NULL, not df, because R default-argument promises
      # evaluate lazily in THIS frame -- an implicit raw = df would be forced
      # only when first read below, by which point df has already been
      # reassigned by select()/arrange()/apply_stat_filters() and the count
      # columns raw needs would be gone. Forcing it here, before any
      # reassignment of df, guards every caller -- not just ones that
      # remember to pass raw explicitly.
      if (is.null(raw)) raw <- df
      # ============================================================
      # FOUR FACTORS LINEUP TABLE
      # Ranks are pre-computed on the full unfiltered population
      # in ld_ff_ranked_df(), so colors stay stable across local filters.
      # ============================================================

      pr_cols <- c("pr_off_ppp", "pr_off_efg", "pr_off_oreb", "pr_off_tov", "pr_off_ftr",
                   "pr_def_ppp", "pr_def_efg", "pr_def_oreb", "pr_def_tov", "pr_def_ftr", "pr_net")

      keep_cols <- c("Team", "Players",
                     "off_ppp", "off_efg", "off_oreb", "off_tov", "off_ftr", "off_poss",
                     "def_ppp", "def_efg", "def_oreb", "def_tov", "def_ftr", "def_poss",
                     "minutes", "total_poss", "net_rtg", "team_id", "sub_lineup_hash")
      df <- df %>% select(any_of(c(keep_cols, pr_cols)))
      df$is_total <- rep(1, nrow(df))
      df <- df %>% arrange(desc(total_poss))
      df <- apply_stat_filters(df, stat_filters)

      # --- TOTAL row (rates from summed raw counts) ---
      if (nrow(df) > 0) {
        if (all(c("team_id", "sub_lineup_hash") %in% names(raw)) &&
            all(c("team_id", "sub_lineup_hash") %in% names(df))) {
          raw <- raw %>%
            semi_join(
              df %>% select(team_id, sub_lineup_hash) %>% distinct(),
              by = c("team_id", "sub_lineup_hash")
            )
        }
        sum_off_poss <- sum(df$off_poss, na.rm = TRUE)
        sum_def_poss <- sum(df$def_poss, na.rm = TRUE)
        sum_off_pts  <- sum(raw$off_pts, na.rm = TRUE)
        sum_def_pts  <- sum(raw$def_pts, na.rm = TRUE)
        tot_off_ppp <- if (sum_off_poss > 0) round((sum_off_pts / sum_off_poss) * 100, 1) else NA_real_
        tot_def_ppp <- if (sum_def_poss > 0) round((sum_def_pts / sum_def_poss) * 100, 1) else NA_real_
        tot_net_rtg <- if (!is.na(tot_off_ppp) && !is.na(tot_def_ppp)) round(tot_off_ppp - tot_def_ppp, 1) else NA_real_

        # Sum raw counts for four-factor rates
        s_off_ts_poss   <- sum(raw$off_ts_poss, na.rm = TRUE)
        s_off_oreb_cnt  <- sum(raw$off_oreb_cnt, na.rm = TRUE)
        s_off_oreb_opps <- sum(raw$off_oreb_opps, na.rm = TRUE)
        s_off_tov_cnt   <- sum(raw$off_tov_cnt, na.rm = TRUE)
        s_off_fta       <- sum(raw$off_fta, na.rm = TRUE)
        s_off_fga       <- sum(raw$off_fga_cnt, na.rm = TRUE)
        s_off_fgm       <- sum(raw$off_fgm_cnt, na.rm = TRUE)
        s_off_fg3m      <- sum(raw$off_fg3m_cnt, na.rm = TRUE)
        s_def_ts_poss   <- sum(raw$def_ts_poss, na.rm = TRUE)
        s_def_oreb_cnt  <- sum(raw$def_oreb_cnt, na.rm = TRUE)
        s_def_oreb_opps <- sum(raw$def_oreb_opps, na.rm = TRUE)
        s_def_tov_cnt   <- sum(raw$def_tov_cnt, na.rm = TRUE)
        s_def_fta       <- sum(raw$def_fta, na.rm = TRUE)
        s_def_fga       <- sum(raw$def_fga_cnt, na.rm = TRUE)
        s_def_fgm       <- sum(raw$def_fgm_cnt, na.rm = TRUE)
        s_def_fg3m      <- sum(raw$def_fg3m_cnt, na.rm = TRUE)

        tot_off_efg  <- if (s_off_fga > 0) round((s_off_fgm + 0.5 * s_off_fg3m) / s_off_fga * 100, 1) else NA_real_
        tot_off_oreb <- if (s_off_oreb_opps > 0) round(s_off_oreb_cnt / s_off_oreb_opps * 100, 1) else NA_real_
        tot_off_tov  <- if (sum_off_poss > 0) round(s_off_tov_cnt / sum_off_poss * 100, 1) else NA_real_
        tot_off_ftr  <- if (s_off_fga > 0) round(s_off_fta / s_off_fga * 100, 1) else NA_real_
        tot_def_efg  <- if (s_def_fga > 0) round((s_def_fgm + 0.5 * s_def_fg3m) / s_def_fga * 100, 1) else NA_real_
        tot_def_oreb <- if (s_def_oreb_opps > 0) round(s_def_oreb_cnt / s_def_oreb_opps * 100, 1) else NA_real_
        tot_def_tov  <- if (sum_def_poss > 0) round(s_def_tov_cnt / sum_def_poss * 100, 1) else NA_real_
        tot_def_ftr  <- if (s_def_fga > 0) round(s_def_fta / s_def_fga * 100, 1) else NA_real_

        sum_minutes <- sum(raw$minutes, na.rm = TRUE)
        total_row <- data.frame(
          Team = "TOTAL", Players = "- All Lineups -",
          off_ppp = tot_off_ppp, off_efg = tot_off_efg, off_oreb = tot_off_oreb, off_tov = tot_off_tov, off_ftr = tot_off_ftr,
          off_poss = sum_off_poss,
          def_ppp = tot_def_ppp, def_efg = tot_def_efg, def_oreb = tot_def_oreb, def_tov = tot_def_tov, def_ftr = tot_def_ftr,
          def_poss = sum_def_poss,
          minutes = sum_minutes,
          total_poss = sum_off_poss + sum_def_poss,
          net_rtg = tot_net_rtg,
          team_id = NA_integer_, sub_lineup_hash = NA_character_,
          is_total = 0, stringsAsFactors = FALSE
        )
        df <- dplyr::bind_rows(total_row, as.data.frame(df, stringsAsFactors = FALSE))
      }

      df <- df %>% select(is_total, everything())

      # Build custom sketch header
      # Note: first th("") in each row accounts for hidden is_total column at position 0
      sketch_ff <- htmltools::withTags(table(class = 'display', thead(
        tr(
          th(""),
          th(class = "group-head", colspan = 2, ""),
          th(class = "group-head section-left-border", colspan = 6, "Offense"),
          th(class = "group-head section-left-border", colspan = 6, "Defense"),
          th(class = "group-head section-left-border", colspan = 3, "Usage")
        ),
        tr(
          th(""),
          th(class = "sub-head", "Team"), th(class = "sub-head", "Players"),
          th(class = "sub-head section-left-border", "PPP"), th(class = "sub-head", "eFG%"),
          th(class = "sub-head", title = OFF_OREB_TOOLTIP, "OREB%"), th(class = "sub-head", "TOV%"),
          th(class = "sub-head", "FTR"), th(class = "sub-head", "Poss"),
          th(class = "sub-head section-left-border", "PPP"), th(class = "sub-head", "eFG%"),
          th(class = "sub-head", title = DEF_OREB_TOOLTIP, "OREB%"), th(class = "sub-head", "TOV%"),
          th(class = "sub-head", "FTR"), th(class = "sub-head", "Poss"),
          th(class = "sub-head section-left-border", "Min"), th(class = "sub-head", "Poss"), th(class = "sub-head", "Net")
        )
      )))

      # Column indices for section borders
      ff_hash_idx <- which(names(df) == "sub_lineup_hash") - 1L
      ff_tid_idx  <- which(names(df) == "team_id") - 1L
      hide_idx <- c(0, which(colnames(df) %in% pr_cols) - 1L, ff_hash_idx, ff_tid_idx)
      off_ppp_idx  <- which(names(df) == "off_ppp") - 1L
      def_ppp_idx  <- which(names(df) == "def_ppp") - 1L
      minutes_idx  <- which(names(df) == "minutes") - 1L

      # Clickable Players column
      ff_players_idx <- which(names(df) == "Players") - 1L
      ff_players_render <- DT::JS(sprintf(
        "function(data, type, row, meta) {
           if (type !== 'display' || !row) return data;
           if (row[0] === 0) return data;
           var hash = row[%d];
           var tid = row[%d];
           var esc = function(x) { return $('<div/>').text(x == null ? '' : String(x)).html(); };
           return '<a href=\"#\" class=\"%s\" data-hash=\"' + esc(hash) + '\" data-team-id=\"' + esc(tid) + '\">' + esc(data) + '</a>';
         }", ff_hash_idx, ff_tid_idx, spec$link_class))

      col_defs <- list(
        list(targets = hide_idx, visible = FALSE),
        list(targets = "_all", className = "dt-center"),
        list(targets = ff_players_idx, render = ff_players_render)
      )
      if (length(off_ppp_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = off_ppp_idx, className = "section-left-border dt-center")
      if (length(def_ppp_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = def_ppp_idx, className = "section-left-border dt-center")
      if (length(minutes_idx)) col_defs[[length(col_defs) + 1]] <- list(targets = minutes_idx, className = "section-left-border dt-center")

      dt <- DT::datatable(
                          df, container = sketch_ff, rownames = FALSE,
                          escape = dt_escape_except(df, c("Players", "sub_lineup_hash")),
                          callback = DT::JS(sprintf(
                            "table.on('click', 'a.%s', function(e) {
                               e.preventDefault();
                               %s
                             });", spec$link_class, spec$click_js)
                          ),
                          options = list(
                            headerCallback = HEADER_TOOLTIP_JS,
                            dom = "tip", pageLength = 50,
                            lengthMenu = c(25, 50, 100, 200),
                            orderFixed = list(list(0, 'asc')),
                            deferRender = TRUE, scrollX = TRUE,
                            scrollY = "70vh", scrollCollapse = TRUE,
                            columnDefs = col_defs
                          ))

      # Format numbers
      rate_cols <- intersect(c("off_efg", "off_oreb", "off_tov", "off_ftr", "def_efg", "def_oreb", "def_tov", "def_ftr"), names(df))
      ppp_cols  <- intersect(c("off_ppp", "def_ppp", "net_rtg"), names(df))
      poss_cols <- intersect(c("off_poss", "def_poss", "total_poss"), names(df))
      min_cols  <- intersect(c("minutes"), names(df))

      if (length(rate_cols)) dt <- DT::formatRound(dt, rate_cols, 1)
      if (length(ppp_cols))  dt <- DT::formatRound(dt, ppp_cols, 1)
      if (length(poss_cols)) dt <- DT::formatCurrency(dt, poss_cols, currency = "", interval = 3, mark = ",", digits = 0)
      if (length(min_cols))  dt <- DT::formatRound(dt, min_cols, 1)

      # TOTAL row styling
      dt <- DT::formatStyle(dt, "Team", target = "row",
                            backgroundColor = styleEqual("TOTAL", "#1a1f2b"),
                            fontWeight = styleEqual("TOTAL", "bold"))

      # Color logic
      if ("pr_off_ppp"  %in% names(df)) dt <- DT::formatStyle(dt, "off_ppp",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_ppp")
      if ("pr_off_efg"  %in% names(df)) dt <- DT::formatStyle(dt, "off_efg",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_efg")
      if ("pr_off_oreb" %in% names(df)) dt <- DT::formatStyle(dt, "off_oreb", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_oreb")
      if ("pr_off_tov"  %in% names(df)) dt <- DT::formatStyle(dt, "off_tov",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_tov")
      if ("pr_off_ftr"  %in% names(df)) dt <- DT::formatStyle(dt, "off_ftr",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_off_ftr")
      if ("pr_def_ppp"  %in% names(df)) dt <- DT::formatStyle(dt, "def_ppp",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_def_ppp")
      if ("pr_def_efg"  %in% names(df)) dt <- DT::formatStyle(dt, "def_efg",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_def_efg")
      if ("pr_def_oreb" %in% names(df)) dt <- DT::formatStyle(dt, "def_oreb", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_def_oreb")
      if ("pr_def_tov"  %in% names(df)) dt <- DT::formatStyle(dt, "def_tov",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_def_tov")
      if ("pr_def_ftr"  %in% names(df)) dt <- DT::formatStyle(dt, "def_ftr",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_def_ftr")
      if ("pr_net"      %in% names(df)) dt <- DT::formatStyle(dt, "net_rtg",  backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_net")

      return(dt)
}

lineup_summary_datatable <- function(df, stat_filters, spec) {
      # ============================================================
      # SUMMARY LINEUP TABLE (existing behavior)
      # ============================================================

      pr_cols <- c("pr_ld_net", "pr_ld_off_ppp", "pr_ld_def_ppp_i")
      shot_raw_cols <- c("off_fg2_made", "off_fg2_att", "off_fg3_made", "off_fg3_att",
                         "def_fg2_made", "def_fg2_att", "def_fg3_made", "def_fg3_att")
      shot_filter_cols <- unname(c(
        shot_split_metric_cols("Off", "off"),
        shot_split_metric_cols("Def", "def")
      ))
      has_shots <- all(c("off_fg2_att", "off_fg3_att") %in% names(df))
      if (!("num_starters" %in% names(df)) && ("num_lineup" %in% names(df))) {
        df$num_starters <- df$num_lineup
      }

      # Create display columns for sorting (total FGA)
      if (has_shots) {
        df[["Off Shot"]] <- dplyr::coalesce(df$off_fg2_att, 0L) + dplyr::coalesce(df$off_fg3_att, 0L)
        df[["Def Shot"]] <- dplyr::coalesce(df$def_fg2_att, 0L) + dplyr::coalesce(df$def_fg3_att, 0L)
        df <- add_shot_split_metrics(df, list(
          off = c("off_fg2_made", "off_fg2_att", "off_fg3_made", "off_fg3_att"),
          def = c("def_fg2_made", "def_fg2_att", "def_fg3_made", "def_fg3_att")
        ))
      }

      keep_cols <- c("Team", "Players", "minutes", "total_poss", "plus_minus",
                     if (has_shots) c("Off Shot", "Def Shot"),
                     "off_poss", "def_poss", "off_pts", "def_pts", "off_ppp", "def_ppp", "net_rtg", "num_starters", "sub_lineup_hash", "team_id")
      df <- df %>% select(any_of(c(keep_cols, shot_raw_cols, shot_filter_cols, pr_cols)))
      if ("net_rtg" %in% names(df)) df <- df %>% arrange(desc(total_poss))
      df <- apply_stat_filters(df, stat_filters)
      df$is_total <- rep(1, nrow(df))
      if (nrow(df) > 0) {
        sum_off_poss <- sum(df$off_poss, na.rm = TRUE)
        sum_def_poss <- sum(df$def_poss, na.rm = TRUE)
        sum_off_pts <- sum(df$off_pts, na.rm = TRUE)
        sum_def_pts <- sum(df$def_pts, na.rm = TRUE)
        sum_minutes <- sum(df$minutes, na.rm = TRUE)
        tot_off_ppp <- if (sum_off_poss > 0) (sum_off_pts / sum_off_poss) * 100 else 0
        tot_def_ppp <- if (sum_def_poss > 0) (sum_def_pts / sum_def_poss) * 100 else 0
        tot_net_rtg <- tot_off_ppp - tot_def_ppp
        total_row <- data.frame(Team = "TOTAL", Players = "- All Lineups -", minutes = sum_minutes, total_poss = sum_off_poss + sum_def_poss, off_ppp = tot_off_ppp, def_ppp = tot_def_ppp, net_rtg = tot_net_rtg, plus_minus = sum_off_pts - sum_def_pts, off_poss = sum_off_poss, off_pts = sum_off_pts, def_poss = sum_def_poss, def_pts = sum_def_pts, num_starters = NA_real_, sub_lineup_hash = "TOTAL", team_id = NA_integer_, is_total = 0, stringsAsFactors = FALSE)
        # Add shooting totals
        if (has_shots) {
          for (sc in shot_raw_cols) total_row[[sc]] <- sum(df[[sc]], na.rm = TRUE)
          total_row[["Off Shot"]] <- total_row$off_fg2_att + total_row$off_fg3_att
          total_row[["Def Shot"]] <- total_row$def_fg2_att + total_row$def_fg3_att
        }
        df <- dplyr::bind_rows(total_row, as.data.frame(df, stringsAsFactors = FALSE))
      }
      df <- df %>% select(is_total, everything())
      show_cols <- c("Team", "Players", "minutes", "total_poss", "off_ppp", "def_ppp", "net_rtg", "plus_minus",
                     if (has_shots) c("Off Shot", "Def Shot"),
                     "off_poss", "off_pts", "def_poss", "def_pts", "num_starters", "sub_lineup_hash", "team_id")

      keep <- intersect(show_cols, names(df))
      df <- df[, unique(c("is_total", keep, shot_raw_cols[shot_raw_cols %in% names(df)], pr_cols[pr_cols %in% names(df)])), drop = FALSE]
      pretty_labels <- c(Team = "Team", Players = "Players", minutes = "Min", num_starters = "# Starters", total_poss = "Total Poss", net_rtg = "Net RTG", `plus_minus` = "+/-", off_ppp = "Off PPP", def_ppp = "Def PPP", off_poss = "Off Poss", off_pts = "Off Pts", def_poss = "Def Poss", def_pts = "Def Pts", sub_lineup_hash = "Lineup ID", team_id = "team_id", `Off Shot` = "Off Shot", `Def Shot` = "Def Shot")

      # Shooting column JS render function factory (same pattern as Tab 1)
      make_shot_render <- function(fg2m_col, fg2a_col, fg3m_col, fg3a_col,
                                   is_defense = FALSE, min_fga = 50, avg2 = 53, avg3 = 34) {
        fg2m_idx <- which(names(df) == fg2m_col) - 1
        fg2a_idx <- which(names(df) == fg2a_col) - 1
        fg3m_idx <- which(names(df) == fg3m_col) - 1
        fg3a_idx <- which(names(df) == fg3a_col) - 1
        sign_mult <- if (is_defense) -1 else 1
        js_str <- sprintf(
          "function(data, type, row, meta) {
             if (type !== 'display' || !row) return data;
             var fg2m = row[%d] || 0, fg2a = row[%d] || 0;
             var fg3m = row[%d] || 0, fg3a = row[%d] || 0;
             var totalFGA = fg2a + fg3a;
             if (!totalFGA) return '<div class=\"shot-acc-label\" style=\"color:#aaa;\">-</div>';
             var fg2pct = fg2a ? Math.round(fg2m / fg2a * 100) : 0;
             var fg3pct = fg3a ? Math.round(fg3m / fg3a * 100) : 0;
             var fg2freq = Math.round(fg2a / totalFGA * 100);
             var fg3freq = 100 - fg2freq;
             var minFGA = %d;
             var sign = %d;
             var avg2 = %d, avg3 = %d;
             function accColor(pct, avg) {
               var d = sign * (pct - avg) / avg;
               d = Math.max(-1, Math.min(1, d * 3));
               var r, g;
               if (d < 0) { r = 200; g = Math.round(200 + d * 120); }
               else       { g = 170; r = Math.round(200 - d * 150); }
               return 'rgb(' + r + ',' + g + ',60)';
             }
             var muted = totalFGA < minFGA;
             var c2 = muted ? '#bbb' : accColor(fg2pct, avg2);
             var c3 = muted ? '#bbb' : accColor(fg3pct, avg3);
             var barOpacity = muted ? 'opacity:0.3;' : '';
             var title2pct = '2PT accuracy: ' + fg2pct + '%% (' + fg2m + '/' + fg2a + ')';
             var title3pct = '3PT accuracy: ' + fg3pct + '%% (' + fg3m + '/' + fg3a + ')';
             var title2freq = '2PT frequency: ' + fg2freq + '%% of FGA (' + fg2a + '/' + totalFGA + ')';
             var title3freq = '3PT frequency: ' + fg3freq + '%% of FGA (' + fg3a + '/' + totalFGA + ')';
             return '<div class=\"shot-acc-label\">' +
               '<span title=\"' + title2pct + '\" style=\"color:' + c2 + '; font-weight:' + (muted ? '400' : '700') + '; cursor:help;\">' + fg2pct + '%%</span>' +
               ' <span style=\"opacity:0.3;\">|</span> ' +
               '<span title=\"' + title3pct + '\" style=\"color:' + c3 + '; font-weight:' + (muted ? '400' : '700') + '; cursor:help;\">' + fg3pct + '%%</span>' +
               '</div>' +
               '<div class=\"shot-bar-container\" style=\"' + barOpacity + '\">' +
               '<div class=\"shot-bar-2pt\" title=\"' + title2freq + '\" style=\"width:' + fg2freq + '%%; cursor:help;\">' + fg2freq + '%%</div>' +
               '<div class=\"shot-bar-3pt\" title=\"' + title3freq + '\" style=\"width:' + fg3freq + '%%; cursor:help;\">' + fg3freq + '%%</div>' +
               '</div>';
           }", fg2m_idx, fg2a_idx, fg3m_idx, fg3a_idx, min_fga, sign_mult, avg2, avg3
        )
        DT::JS(js_str)
      }

      # Build shot column defs with dynamic thresholds
      shot_col_defs <- list()
      if (has_shots) {
        shot_col_map <- list(
          "Off Shot" = c("off_fg2_made", "off_fg2_att", "off_fg3_made", "off_fg3_att"),
          "Def Shot" = c("def_fg2_made", "def_fg2_att", "def_fg3_made", "def_fg3_att")
        )
        SHOT_MIN_FGA <- 50L
        shot_avgs <- list()
        for (dn in names(shot_col_map)) {
          cols <- shot_col_map[[dn]]
          fga <- df[[dn]]
          qual <- if (is.null(fga)) rep(FALSE, nrow(df)) else (!is.na(fga) & fga >= SHOT_MIN_FGA)
          fg2a_sum <- sum(df[[cols[2]]][qual], na.rm = TRUE)
          fg3a_sum <- sum(df[[cols[4]]][qual], na.rm = TRUE)
          a2 <- if (fg2a_sum > 0) as.integer(round(sum(df[[cols[1]]][qual], na.rm = TRUE) / fg2a_sum * 100)) else 53L
          a3 <- if (fg3a_sum > 0) as.integer(round(sum(df[[cols[3]]][qual], na.rm = TRUE) / fg3a_sum * 100)) else 34L
          shot_avgs[[dn]] <- list(avg2 = a2, avg3 = a3)
        }
        for (disp_name in names(shot_col_map)) {
          cols <- shot_col_map[[disp_name]]
          target_idx <- which(names(df) == disp_name) - 1
          is_def <- grepl("^Def", disp_name)
          avgs <- shot_avgs[[disp_name]]
          if (length(target_idx) && all(cols %in% names(df))) {
            shot_col_defs[[length(shot_col_defs) + 1]] <- list(
              targets = target_idx,
              render = make_shot_render(cols[1], cols[2], cols[3], cols[4],
                                        is_defense = is_def, min_fga = SHOT_MIN_FGA,
                                        avg2 = avgs$avg2, avg3 = avgs$avg3)
            )
          }
        }
      }

      data_col_names <- colnames(df)[-1]
      data_col_names <- setdiff(data_col_names, c(pr_cols, shot_raw_cols))
      col_labels <- unname(pretty_labels[data_col_names])
      final_labels <- c("", col_labels)
      pr_indices <- which(colnames(df) %in% pr_cols) - 1L
      shot_raw_indices <- which(colnames(df) %in% shot_raw_cols) - 1L
      sum_hash_idx <- which(names(df) == "sub_lineup_hash") - 1L
      sum_tid_idx <- which(names(df) == "team_id") - 1L
      hidden_indices <- c(0, pr_indices, shot_raw_indices, sum_hash_idx, sum_tid_idx)

      # Clickable Players column
      sum_players_idx <- which(names(df) == "Players") - 1L
      sum_players_render <- DT::JS(sprintf(
        "function(data, type, row, meta) {
           if (type !== 'display' || !row) return data;
           if (row[0] === 0) return data;
           var hash = row[%d];
           var tid = row[%d];
           var esc = function(x) { return $('<div/>').text(x == null ? '' : String(x)).html(); };
           return '<a href=\"#\" class=\"%s\" data-hash=\"' + esc(hash) + '\" data-team-id=\"' + esc(tid) + '\">' + esc(data) + '</a>';
         }", sum_hash_idx, sum_tid_idx, spec$link_class))

      all_col_defs <- c(list(list(targets = hidden_indices, visible = FALSE),
                             list(targets = sum_players_idx, render = sum_players_render)),
                        shot_col_defs)

      dt <- DT::datatable(
        df,
        colnames = final_labels,
        rownames = FALSE,
        escape = dt_escape_except(df, c("Players", "sub_lineup_hash")),
        filter = "top",
        callback = DT::JS(sprintf(
          "table.on('click', 'a.%s', function(e) {
             e.preventDefault();
             %s
           });", spec$link_class, spec$click_js)
        ),
        options = list(headerCallback = HEADER_TOOLTIP_JS, pageLength = 50, lengthMenu = c(25, 50, 100, 200, 1000), orderFixed = list(list(0, 'asc')), deferRender = TRUE, scrollX = TRUE, scrollY = "70vh", scrollCollapse = TRUE, processing = TRUE, columnDefs = all_col_defs)
      ) |>
        DT::formatRound(c("off_ppp", "def_ppp", "net_rtg", "minutes")[c("off_ppp", "def_ppp", "net_rtg", "minutes") %in% names(df)], 1) |>
        # Displayed as a whole number: it counts people on the floor, and a
        # decimal invites reading a possession-weighted mean as a headcount.
        # Display only -- filtering and sorting still use the exact value.
        DT::formatRound(intersect("num_starters", names(df)), 0) |>
        DT::formatCurrency(c("total_poss", "off_poss", "def_poss")[c("total_poss", "off_poss", "def_poss") %in% names(df)], currency = "", interval = 3, mark = ",", digits = 0) |>
        DT::formatCurrency(c("off_pts", "def_pts", "plus_minus")[c("off_pts", "def_pts", "plus_minus") %in% names(df)], currency = "", interval = 3, mark = ",", digits = 0)
      dt <- DT::formatStyle(dt, "Team", target = "row", backgroundColor = styleEqual("TOTAL", "#1a1f2b"), fontWeight = styleEqual("TOTAL", "bold"))
      if (all(c("net_rtg", "pr_ld_net") %in% colnames(df))) dt <- DT::formatStyle(dt, "net_rtg", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_ld_net")
      if (all(c("off_ppp", "pr_ld_off_ppp") %in% colnames(df))) dt <- DT::formatStyle(dt, "off_ppp", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_ld_off_ppp")
      if (all(c("def_ppp", "pr_ld_def_ppp_i") %in% colnames(df))) dt <- DT::formatStyle(dt, "def_ppp", backgroundColor = styleInterval(CUTS, COLS_GRAD), valueColumns = "pr_ld_def_ppp_i")
      return(dt)
}

# The Israeli spec. Tab 2's anchors are handled by window.handleLineupLinkClick,
# defined in www/app.js, which sets input$ld_lineup_click.
LD_LINEUP_TABLE_SPEC <- list(
  link_class = "ld-lineup-link",
  click_js   = "window.handleLineupLinkClick(this);"
)

# The EuroLeague spec. Tab 10 has no equivalent of app.js's
# handleLineupLinkClick, so it sets its input directly. data-hash carries the
# unit_key, which the rename map surfaces as sub_lineup_hash.
EURO_LD_LINEUP_TABLE_SPEC <- list(
  link_class = "euro-ld-unit",
  click_js   = paste0(
    "Shiny.setInputValue('euro_ld_clicked_unit', ",
    "this.getAttribute('data-hash'), {priority: 'event'});"
  )
)
