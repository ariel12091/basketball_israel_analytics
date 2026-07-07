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

update_gn_last_n_choices <- function(session, prefix, gn_vals) {
  gn_vals <- suppressWarnings(as.integer(gn_vals))
  gn_vals <- gn_vals[is.finite(gn_vals)]
  gn_choices <- c("", as.character(gn_vals))
  last_choices <- if (length(gn_vals)) c("", as.character(seq_len(max(gn_vals, na.rm = TRUE)))) else ""
  updateSelectizeInput(session, paste0(prefix, "_gn_min"), choices = gn_choices, selected = "")
  updateSelectizeInput(session, paste0(prefix, "_gn_max"), choices = gn_choices, selected = "")
  updateSelectizeInput(session, paste0(prefix, "_last_n"), choices = last_choices, selected = "")
}

resolve_gn_last_n_params <- function(input, prefix) {
  min_gn <- input[[paste0(prefix, "_gn_min")]] %||% ""
  max_gn <- input[[paste0(prefix, "_gn_max")]] %||% ""
  last_n <- input[[paste0(prefix, "_last_n")]] %||% ""

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

team_select_choices_with_all <- function(teams_df, all_label = "\u2014 All teams \u2014") {
  if (is.null(teams_df) || !nrow(teams_df)) {
    out <- ""
    names(out) <- all_label
    return(out)
  }
  out <- c("", as.character(teams_df$team_id))
  names(out) <- c(all_label, teams_df$team_name)
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

# Header-tooltip sentence explaining the est. annotation for one factor.
ff_impact_tooltip <- function(factor) {
  bad <- setdiff(unique(as.character(factor)), names(FF_IMPACT_WEIGHTS))
  if (length(bad)) stop("Unknown four-factor name(s): ", paste(bad, collapse = ", "))
  sprintf(
    "Estimated impact: each 1pp of this factor \u2248 %+.2f pts per 100 poss. (league-calibrated regression weight; an approximation, not a measured stat).",
    FF_IMPACT_WEIGHTS[[as.character(factor)]]
  )
}
