# Per-run HTML report combining ETL outcome, data-quality results, and log evidence.

html_escape <- function(x) {
  x <- as.character(x)
  x[is.na(x)] <- ""
  x <- gsub("&", "&amp;", x, fixed = TRUE)
  x <- gsub("<", "&lt;", x, fixed = TRUE)
  x <- gsub(">", "&gt;", x, fixed = TRUE)
  x <- gsub('"', "&quot;", x, fixed = TRUE)
  x
}

fmt_count <- function(x) {
  if (!length(x) || is.na(x)) return("n/a")
  format(as.numeric(x), big.mark = ",", scientific = FALSE, trim = TRUE)
}

fmt_pct <- function(x, digits = 2L) {
  if (!length(x) || is.na(x)) return("n/a")
  sprintf(paste0("%.", digits, "f%%"), as.numeric(x))
}

read_dq_detail <- function(dq_result, check_id) {
  row <- dq_result$summary[dq_result$summary$check_id == check_id, , drop = FALSE]
  if (!nrow(row) || !nzchar(row$detail_file[[1]])) return(data.frame())

  path <- file.path(dirname(dq_result$latest_path), row$detail_file[[1]])
  if (!file.exists(path)) return(data.frame())
  utils::read.csv(path, stringsAsFactors = FALSE, check.names = FALSE)
}

metric_bar <- function(label, value, detail, color = "#dc3545") {
  width <- if (is.finite(value)) min(100, max(1, value)) else 0
  sprintf(
    paste0(
      '<div class="metric">',
      '<div class="metric-head"><span>%s</span><strong>%s</strong></div>',
      '<div class="bar"><span style="width:%.3f%%;background:%s"></span></div>',
      '<div class="metric-detail">%s</div>',
      '</div>'
    ),
    html_escape(label),
    html_escape(fmt_pct(value, 3L)),
    width,
    color,
    html_escape(detail)
  )
}

status_card <- function(label, value, class_name = "neutral") {
  sprintf(
    '<div class="card %s"><span>%s</span><strong>%s</strong></div>',
    class_name,
    html_escape(label),
    html_escape(value)
  )
}

render_table <- function(df, max_rows = 50L) {
  if (is.null(df) || !nrow(df)) return("<p class=\"muted\">No rows.</p>")

  out <- utils::head(df, max_rows)
  header <- paste(sprintf("<th>%s</th>", html_escape(names(out))), collapse = "")
  rows <- apply(out, 1L, function(row) {
    paste0(
      "<tr>",
      paste(sprintf("<td>%s</td>", html_escape(row)), collapse = ""),
      "</tr>"
    )
  })
  suffix <- if (nrow(df) > max_rows) {
    sprintf("<p class=\"muted\">Showing %d of %d rows.</p>", max_rows, nrow(df))
  } else {
    ""
  }
  paste0(
    '<div class="table-wrap"><table><thead><tr>',
    header,
    "</tr></thead><tbody>",
    paste(rows, collapse = ""),
    "</tbody></table></div>",
    suffix
  )
}

parse_log_evidence <- function(log_file) {
  if (!file.exists(log_file)) {
    return(list(
      phases = character(0),
      alerts = character(0),
      lineup_matches = character(0),
      ot_recovery = character(0)
    ))
  }

  lines <- readLines(log_file, warn = FALSE, encoding = "UTF-8")
  list(
    phases = unique(grep(
      "Phase [0-9]+ (complete|FAILED)|Skipping Phase|ETL Full pipeline finished",
      lines,
      value = TRUE
    )),
    alerts = unique(grep("\\] (WARN|ERROR):", lines, value = TRUE)),
    lineup_matches = unique(grep(
      "lineup/stint match coverage|Lineup/stint coverage FAILED",
      lines,
      value = TRUE
    )),
    ot_recovery = unique(grep(
      "OT lineup recovery",
      lines,
      value = TRUE
    ))
  )
}

write_etl_run_report <- function(
  etl_result,
  dq_result,
  overall_elapsed = NA_real_,
  output_dir = file.path("etl", "logs", "reports")
) {
  dir.create(output_dir, recursive = TRUE, showWarnings = FALSE)
  stamp <- format(Sys.time(), "%Y%m%d_%H%M%S")
  report_path <- file.path(output_dir, paste0("etl_report_", stamp, ".html"))
  latest_path <- file.path(output_dir, "latest.html")

  q <- read_dq_detail(dq_result, "Q_persisted_rows_without_lineup_match")
  r <- read_dq_detail(dq_result, "R_invalid_lineup_player_counts")
  s <- read_dq_detail(dq_result, "S_invalid_starter_counts")
  p <- read_dq_detail(dq_result, "P_app_invalid_or_nonparticipant_player_rows")
  reviewed_exceptions <- read_dq_detail(dq_result, "P1_reviewed_data_quality_exceptions")
  t <- read_dq_detail(dq_result, "T_invalid_team_minutes")
  u <- read_dq_detail(dq_result, "U_invalid_lineup_metric_values")
  v <- read_dq_detail(dq_result, "V_team_game_score_reconciliation")
  w <- read_dq_detail(dq_result, "W_team_game_possession_reconciliation")
  x <- read_dq_detail(dq_result, "X_player_minute_conservation")
  y <- read_dq_detail(dq_result, "Y_ot_period_start_lineup_coverage")
  z <- read_dq_detail(dq_result, "Z_ot_event_player_lineup_mismatches")
  log_evidence <- parse_log_evidence(etl_result$log_file)
  ot_audit <- if (!is.null(etl_result$ot_recovery_audit)) {
    etl_result$ot_recovery_audit
  } else {
    data.frame()
  }

  q_global_pct <- if (nrow(q)) q$overall_unmatched_pct[[1]] else NA_real_
  q_affected_pct <- if (nrow(q)) q$affected_games_unmatched_pct[[1]] else NA_real_
  q_unmatched <- if (nrow(q)) q$overall_unmatched_rows[[1]] else NA_real_
  q_global_total <- if (nrow(q)) q$overall_total_rows[[1]] else NA_real_
  q_affected_total <- if (nrow(q)) q$affected_total_rows[[1]] else NA_real_

  r_invalid <- if (nrow(r)) sum(r$invalid_states, na.rm = TRUE) else NA_real_
  r_total <- if (nrow(r)) r$overall_total_states[[1]] else NA_real_
  r_pct <- if (nrow(r)) r$overall_invalid_pct[[1]] else NA_real_

  s_persisted <- s[s$source_table == "df_pts_poss_lineups_longer_mv", , drop = FALSE]
  s_invalid <- if (nrow(s_persisted)) s_persisted$invalid_rows[[1]] else NA_real_
  s_total <- if (nrow(s_persisted)) s_persisted$total_rows[[1]] else NA_real_
  s_pct <- if (nrow(s_persisted)) s_persisted$invalid_pct[[1]] else NA_real_

  p_onoff <- p[p$source_table == "app_player_aggregates", , drop = FALSE]
  p_invalid <- if (nrow(p_onoff)) p_onoff$invalid_rows[[1]] else NA_real_
  p_total <- if (nrow(p_onoff)) p_onoff$total_rows[[1]] else NA_real_
  p_pct <- if (nrow(p_onoff)) p_onoff$invalid_pct[[1]] else NA_real_
  p_placeholders <- if (nrow(p_onoff)) p_onoff$placeholder_rows[[1]] else NA_real_

  t_invalid <- if (nrow(t)) t$overall_invalid_team_games[[1]] else 0
  t_total <- if (nrow(t)) t$overall_total_team_games[[1]] else NA_real_
  t_pct <- if (nrow(t)) t$overall_invalid_pct[[1]] else 0

  u_invalid <- if (nrow(u)) u$invalid_rows[[1]] else NA_real_
  u_total <- if (nrow(u)) u$total_rows[[1]] else NA_real_
  u_pct <- if (nrow(u)) u$invalid_pct[[1]] else NA_real_

  v_invalid <- if (nrow(v)) v$overall_invalid_team_games[[1]] else 0
  v_total <- if (nrow(v)) v$overall_total_team_games[[1]] else NA_real_
  v_pct <- if (nrow(v)) v$overall_invalid_pct[[1]] else 0

  w_invalid <- if (nrow(w)) w$overall_invalid_team_games[[1]] else 0
  w_total <- if (nrow(w)) w$overall_total_team_games[[1]] else NA_real_
  w_pct <- if (nrow(w)) w$overall_invalid_pct[[1]] else 0

  x_invalid <- if (nrow(x)) x$overall_invalid_team_games[[1]] else 0
  x_total <- if (nrow(x)) x$overall_total_team_games[[1]] else NA_real_
  x_pct <- if (nrow(x)) x$overall_invalid_pct[[1]] else 0

  y_invalid <- if (nrow(y)) y$overall_invalid_periods[[1]] else 0
  y_total <- if (nrow(y)) y$overall_total_periods[[1]] else NA_real_
  y_pct <- if (nrow(y)) y$overall_invalid_pct[[1]] else 0

  z_invalid <- if (nrow(z)) z$overall_unmatched_events[[1]] else 0
  z_total <- if (nrow(z)) z$overall_total_events[[1]] else NA_real_
  z_pct <- if (nrow(z)) z$overall_unmatched_pct[[1]] else 0

  ot_periods <- if (nrow(ot_audit)) {
    nrow(unique(ot_audit[, c("game_id", "quarter"), drop = FALSE]))
  } else {
    0L
  }
  ot_accepted <- if (nrow(ot_audit)) {
    sum(grepl("^accepted_", ot_audit$recovery_status), na.rm = TRUE)
  } else {
    0L
  }
  ot_rejected <- if (nrow(ot_audit)) {
    sum(!grepl("^accepted_", ot_audit$recovery_status), na.rm = TRUE)
  } else {
    0L
  }
  ot_warnings <- if (nrow(ot_audit)) {
    sum(ot_audit$ordering_warning_count, na.rm = TRUE)
  } else {
    0L
  }

  etl_ok <- isTRUE(etl_result$success)
  dq_ok <- identical(dq_result$status, "PASS")
  overall_class <- if (etl_ok && dq_ok) "pass" else "fail"
  overall_label <- if (etl_ok && dq_ok) "PASS" else "FAIL"

  unmatched_bars <- if (nrow(q)) {
    paste(vapply(seq_len(nrow(q)), function(i) {
      metric_bar(
        sprintf("Game %s", q$game_id[[i]]),
        q$unmatched_pct[[i]],
        sprintf(
          "%s unmatched / %s rows",
          fmt_count(q$unmatched_rows[[i]]),
          fmt_count(q$total_rows[[i]])
        ),
        "#b02a37"
      )
    }, character(1)), collapse = "")
  } else {
    '<p class="muted">No affected games.</p>'
  }

  summary_bars <- paste(
    metric_bar(
      "Unmatched lineups — affected games",
      q_affected_pct,
      sprintf("%s / %s rows", fmt_count(q_unmatched), fmt_count(q_affected_total))
    ),
    metric_bar(
      "Unmatched lineups — full dataset",
      q_global_pct,
      sprintf("%s / %s rows", fmt_count(q_unmatched), fmt_count(q_global_total)),
      "#fd7e14"
    ),
    metric_bar(
      "Invalid five-player lineup states",
      r_pct,
      sprintf("%s / %s states", fmt_count(r_invalid), fmt_count(r_total)),
      "#dc3545"
    ),
    metric_bar(
      "Missing starter context on statistical rows",
      s_pct,
      sprintf("%s / %s rows", fmt_count(s_invalid), fmt_count(s_total)),
      "#6f42c1"
    ),
    metric_bar(
      "Severely incomplete team timelines",
      t_pct,
      sprintf("%s / %s team-games", fmt_count(t_invalid), fmt_count(t_total)),
      "#0d6efd"
    ),
    metric_bar(
      "Placeholder player identities",
      p_pct,
      sprintf(
        "%s / %s rows; %s placeholders",
        fmt_count(p_invalid),
        fmt_count(p_total),
        fmt_count(p_placeholders)
      ),
      "#d63384"
    ),
    metric_bar(
      "Invalid lineup metric rows",
      u_pct,
      sprintf("%s / %s rows", fmt_count(u_invalid), fmt_count(u_total)),
      if (isTRUE(u_invalid == 0)) "#198754" else "#dc3545"
    ),
    metric_bar(
      "Score reconciliation failures",
      v_pct,
      sprintf("%s / %s team-games", fmt_count(v_invalid), fmt_count(v_total)),
      if (isTRUE(v_invalid == 0)) "#198754" else "#dc3545"
    ),
    metric_bar(
      "Possession reconciliation failures",
      w_pct,
      sprintf("%s / %s team-games", fmt_count(w_invalid), fmt_count(w_total)),
      if (isTRUE(w_invalid == 0)) "#198754" else "#dc3545"
    ),
    metric_bar(
      "Reconstructed player-minute conservation failures",
      x_pct,
      sprintf("%s / %s team-games", fmt_count(x_invalid), fmt_count(x_total)),
      if (isTRUE(x_invalid == 0)) "#198754" else "#dc3545"
    ),
    metric_bar(
      "Invalid OT period starts",
      y_pct,
      sprintf("%s / %s team-periods", fmt_count(y_invalid), fmt_count(y_total)),
      if (isTRUE(y_invalid == 0)) "#198754" else "#dc3545"
    ),
    metric_bar(
      "OT event-player lineup mismatches",
      z_pct,
      sprintf("%s / %s player events", fmt_count(z_invalid), fmt_count(z_total)),
      if (isTRUE(z_invalid == 0)) "#198754" else "#fd7e14"
    ),
    sep = ""
  )

  list_items <- function(x) {
    if (!length(x)) return('<li class="muted">None recorded.</li>')
    paste(sprintf("<li><code>%s</code></li>", html_escape(x)), collapse = "")
  }

  priorities <- c(
    "Raw PBP validation before cleaning: duplicate IDs, clock resets, and quarter-boundary conflicts.",
    "ETL publication funnel: scheduled, fetched, base-loaded, published, failed, skipped, and excluded games.",
    "Source schema and event-distribution drift: fields, null rates, event types, and rows per game.",
    "Substitution state validation: outgoing player on court, incoming player off court, same team.",
    "Possession-ending integrity: exactly one terminal event; offense/defense parity is now implemented.",
    "Incremental/full-rebuild, materialized-view/dynamic-query, and hot/cold-storage parity.",
    "Per-run failure trends: new findings, repeated games, pass rate, and unresolved age."
  )

  html <- paste0(
    '<!doctype html><html><head><meta charset="utf-8">',
    '<meta name="viewport" content="width=device-width,initial-scale=1">',
    "<title>ETL Data Quality Report</title>",
    "<style>",
    "body{font-family:Segoe UI,Arial,sans-serif;background:#f4f6f8;color:#202124;margin:0;padding:24px}",
    ".wrap{max-width:1200px;margin:auto}.hero{background:#17212b;color:white;border-radius:14px;padding:24px}",
    ".hero.pass{background:#155d3b}.hero.fail{background:#842029}",
    ".cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;margin:18px 0}",
    ".card{background:white;border-radius:10px;padding:14px;border-left:5px solid #6c757d;box-shadow:0 2px 7px #0001}",
    ".card span{display:block;color:#6c757d;font-size:12px;text-transform:uppercase}.card strong{font-size:22px}",
    ".card.pass{border-color:#198754}.card.fail{border-color:#dc3545}.card.warn{border-color:#fd7e14}",
    "section{background:white;margin-top:18px;padding:20px;border-radius:12px;box-shadow:0 2px 7px #0001}",
    "h1,h2,h3{margin-top:0}.metric{margin:15px 0}.metric-head{display:flex;justify-content:space-between;gap:12px}",
    ".bar{height:14px;background:#e9ecef;border-radius:9px;overflow:hidden}.bar span{display:block;height:100%;border-radius:9px}",
    ".metric-detail,.muted{color:#6c757d;font-size:13px;margin-top:4px}",
    ".table-wrap{overflow:auto}table{width:100%;border-collapse:collapse;font-size:13px}",
    "th,td{text-align:left;border-bottom:1px solid #dee2e6;padding:8px;white-space:nowrap}th{background:#f8f9fa}",
    "code{white-space:pre-wrap}ul{padding-left:20px}a{color:#0d6efd}",
    "</style></head><body><div class=\"wrap\">",
    sprintf(
      '<div class="hero %s"><h1>ETL Data Quality Report</h1><p>Generated %s · Schema %s</p><h2>%s</h2></div>',
      overall_class,
      html_escape(format(Sys.time(), "%Y-%m-%d %H:%M:%S %Z")),
      html_escape(if (exists("SCHEMA", inherits = TRUE)) get("SCHEMA", inherits = TRUE) else ""),
      overall_label
    ),
    '<div class="cards">',
    status_card("ETL", if (etl_ok) "PASS" else "FAIL", if (etl_ok) "pass" else "fail"),
    status_card("Data quality", dq_result$status, if (dq_ok) "pass" else "fail"),
    status_card("Base-loaded games", fmt_count(length(etl_result$base_loaded_game_ids))),
    status_card("Published games", fmt_count(length(etl_result$published_game_ids)), if (length(etl_result$published_game_ids)) "pass" else "neutral"),
    status_card("Failed games", fmt_count(length(etl_result$failed_game_ids)), if (length(etl_result$failed_game_ids)) "fail" else "pass"),
    status_card("OT periods recovered", fmt_count(ot_periods), if (ot_rejected) "fail" else if (ot_periods) "pass" else "neutral"),
    status_card("Elapsed", if (is.finite(overall_elapsed)) sprintf("%.1fs", overall_elapsed) else "n/a"),
    "</div>",
    "<section><h2>Quality overview</h2>", summary_bars, "</section>",
    "<section><h2>Unmatched lineup rows by affected game</h2>",
    '<p class="muted">Affected-game and full-dataset denominators are shown separately.</p>',
    unmatched_bars,
    "</section>",
    "<section><h2>App reconciliation details</h2>",
    "<h3>Score mismatches</h3>", render_table(v, 25L),
    "<h3>Possession mismatches</h3>", render_table(w, 25L),
    "<h3>Reconstructed player-minute conservation</h3>", render_table(x, 25L),
    "</section>",
    "<section><h2>OT lineup recovery</h2>",
    sprintf(
      paste0(
        '<p class="muted">%s accepted team-periods; %s rejected; ',
        '%s same-clock ordering warnings.</p>'
      ),
      fmt_count(ot_accepted),
      fmt_count(ot_rejected),
      fmt_count(ot_warnings)
    ),
    render_table(ot_audit, 50L),
    "<h3>Persisted OT start failures</h3>", render_table(y, 25L),
    "<h3>OT event-player mismatches</h3>", render_table(z, 25L),
    "</section>",
    "<section><h2>ETL log evidence</h2><h3>Phase outcomes</h3><ul>",
    list_items(log_evidence$phases),
    "</ul><h3>Lineup/stint coverage from this run</h3><ul>",
    list_items(log_evidence$lineup_matches),
    "</ul><h3>OT lineup recovery from this run</h3><ul>",
    list_items(log_evidence$ot_recovery),
    "</ul><h3>Warnings and errors</h3><ul>",
    list_items(log_evidence$alerts),
    "</ul></section>",
    "<section><h2>Open data-quality findings</h2>",
    render_table(
      dq_result$summary[
        dq_result$summary$status %in% c("fail", "warning", "query_error") &
          dq_result$summary$check_id != "P1_reviewed_data_quality_exceptions",
        c("check_id", "severity", "status", "issue_count", "title"),
        drop = FALSE
      ]
    ),
    "</section>",
    "<section><h2>Reviewed exceptions</h2>",
    '<p class="muted">These findings remain visible but require no current data repair.</p>',
    render_table(reviewed_exceptions, 25L),
    "</section>",
    "<section><h2>Priorities not yet addressed</h2><ol>",
    paste(sprintf("<li>%s</li>", html_escape(priorities)), collapse = ""),
    "</ol></section>",
    sprintf(
      '<section><h2>Artifacts</h2><ul><li>ETL log: <code>%s</code></li><li>Detailed DQ report: <code>%s</code></li></ul></section>',
      html_escape(etl_result$log_file),
      html_escape(dq_result$latest_path)
    ),
    "</div></body></html>"
  )

  writeLines(html, report_path, useBytes = TRUE)
  writeLines(html, latest_path, useBytes = TRUE)
  list(report_path = report_path, latest_path = latest_path)
}
