# Immutable, database-free interchange for report consumers (including Dagster).
# Keep this separate from presentation so validation rules have a single owner.
write_dq_export <- function(dq_result, output_dir, schema, started_at,
                            run_context = NULL) {
  parent <- file.path(output_dir, "dagster")
  dir.create(parent, recursive = TRUE, showWarnings = FALSE)
  bundle <- tempfile(paste0(format(started_at, "%Y%m%d_%H%M%S"), "_"), parent)
  dir.create(file.path(bundle, "details"), recursive = TRUE)
  bundle <- normalizePath(bundle, winslash = "/", mustWork = TRUE)
  complete <- FALSE
  on.exit(if (!complete) unlink(bundle, recursive = TRUE), add = TRUE)

  snapshot <- function(path, target) {
    if (is.null(path) || !length(path) || is.na(path) || !nzchar(path) || !file.exists(path)) {
      return(NULL)
    }
    dest <- file.path(bundle, target)
    if (!file.copy(path, dest)) stop("Could not snapshot data-quality evidence: ", path)
    normalizePath(dest, winslash = "/", mustWork = TRUE)
  }
  summary <- dq_result$summary
  checks <- lapply(seq_len(nrow(summary)), function(i) {
    row <- summary[i, , drop = FALSE]
    detail_path <- NULL
    game_ids <- integer()
    if (!is.na(row$detail_file) && nzchar(row$detail_file)) {
      source_path <- file.path(dirname(dq_result$latest_path), row$detail_file)
      detail_path <- snapshot(source_path, file.path("details", paste0(row$check_id, ".csv")))
      if (is.null(detail_path)) stop("Missing data-quality evidence: ", source_path)
      details <- utils::read.csv(detail_path, stringsAsFactors = FALSE)
      if ("game_id" %in% names(details)) {
        game_ids <- sort(unique(suppressWarnings(as.integer(details$game_id))))
        game_ids <- game_ids[!is.na(game_ids)]
      }
    }
    list(
      check_id = row$check_id, title = row$title, severity = row$severity,
      status = row$status, row_count = row$row_count, issue_count = row$issue_count,
      error_message = row$error_message, detail_path = detail_path,
      # These are evidence rows across the dataset, not necessarily new issues
      # introduced by this run. Some detail CSVs also contain contextual rows.
      evidence_game_ids = unname(as.list(game_ids))
    )
  })
  ids <- function(key) unname(as.list(as.integer(run_context[[key]])))
  payload <- list(
    format_version = 1L, league = "il", schema = schema,
    run_id = basename(bundle),
    generated_at = format(started_at, "%Y-%m-%dT%H:%M:%SZ", tz = "UTC"),
    scope = "whole_dataset", status = dq_result$status,
    etl = if (is.null(run_context)) NULL else list(
      success = isTRUE(run_context$success),
      published_game_ids = ids("published_game_ids"),
      failed_game_ids = ids("failed_game_ids"),
      base_loaded_game_ids = ids("base_loaded_game_ids"),
      phase_failures = unname(as.list(run_context$phase_failures))
    ),
    artifacts = list(
      markdown = snapshot(dq_result$report_path, "report.md"),
      findings = snapshot(dq_result$findings_path, "findings.html"),
      summary = snapshot(dq_result$summary_path, "summary.csv")
    ),
    checks = checks
  )
  # report.json appears last: the sensor never sees a partially written bundle.
  pending <- file.path(bundle, "report.pending")
  jsonlite::write_json(payload, pending, auto_unbox = TRUE, na = "null", null = "null",
                       pretty = TRUE, digits = NA)
  path <- file.path(bundle, "report.json")
  if (!file.rename(pending, path)) stop("Could not finalize data-quality export")
  complete <- TRUE
  path
}
