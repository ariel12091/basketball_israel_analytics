# Israeli gameplay coverage gate. Run after any lineup recovery and before
# committing pws. Boundary/reset rows can be incomplete; gameplay cannot.
LINEUP_COVERAGE_GAMEPLAY_TYPES <- c(
  "shot", "freeThrow", "rebound", "assist", "steal", "block",
  "turnover", "foul", "foul-drawn", "deflection", "jumpBall"
)

lineup_coverage_require_columns <- function(df, columns, label) {
  missing <- setdiff(columns, names(df))
  if (length(missing)) {
    stop(sprintf("Lineup coverage: %s missing column(s): %s", label,
                 paste(missing, collapse = ", ")), call. = FALSE)
  }
}

# Validate the actual ON players, rather than trusting n_on or a non-null hash.
lineup_coverage_dictionary <- function(lineups) {
  lineup_coverage_require_columns(lineups, c(
    "game_id", "team_id", "player_id", "is_on_verdict", "n_on", "lineup_hash"
  ), "lineups")
  lineups |>
    dplyr::filter(!is.na(lineup_hash), nzchar(lineup_hash)) |>
    dplyr::group_by(game_id, team_id, lineup_hash) |>
    dplyr::summarise(
      .valid = {
        players <- sort(unique(player_id[which(is_on_verdict == 1)]), na.last = TRUE)
        length(players) == 5L && all(!is.na(players) & players > 0) &&
          all(!is.na(n_on) & n_on == 5) &&
          identical(as.character(dplyr::first(lineup_hash)),
                    digest::digest(paste(players, collapse = "_"),
                                   algo = "md5", serialize = FALSE))
      },
      .groups = "drop"
    )
}

# Compare to cleaned SOURCE actions: checking pws alone misses rows lost before
# the join. Count mappings before validating hashes so duplicate matches fail.
gameplay_lineup_coverage_failures <- function(actions, pws, lineups) {
  key <- c("game_id", "id", "team_id")
  lineup_coverage_require_columns(actions,
    c(key, "quarter", "quarter_time", "type"), "actions")
  lineup_coverage_require_columns(pws, c(
    key, "quarter", "type", "segment_id", "team_id_defense",
    "lineup_hash_offense", "lineup_hash_defense"
  ), "pws")
  dictionary <- lineup_coverage_dictionary(lineups)
  expected <- actions |>
    dplyr::filter(type %in% LINEUP_COVERAGE_GAMEPLAY_TYPES) |>
    dplyr::select(dplyr::all_of(c(key, "quarter", "quarter_time", "type"))) |>
    dplyr::add_count(game_id, id, team_id, name = ".source_rows") |>
    dplyr::distinct()
  mapped <- pws |>
    dplyr::select(dplyr::all_of(c(
      key, "quarter", "type", "segment_id", "team_id_defense",
      "lineup_hash_offense", "lineup_hash_defense"
    ))) |>
    dplyr::add_count(game_id, id, team_id, name = ".mapped_rows") |>
    dplyr::rename(.mapped_quarter = quarter, .mapped_type = type) |>
    dplyr::left_join(
      dplyr::rename(dictionary, lineup_hash_offense = lineup_hash,
                    .offense_valid = .valid),
      by = c("game_id", "team_id", "lineup_hash_offense"),
      na_matches = "never"
    ) |>
    dplyr::left_join(
      dplyr::rename(dictionary, team_id_defense = team_id,
                    lineup_hash_defense = lineup_hash, .defense_valid = .valid),
      by = c("game_id", "team_id_defense", "lineup_hash_defense"),
      na_matches = "never"
    )
  checked <- dplyr::left_join(expected, mapped, by = key,
                             relationship = "many-to-many", na_matches = "never")
  if (!nrow(checked)) {
    checked$reason <- character(0)
    return(checked[, c(key, "quarter", "quarter_time", "type", "reason")])
  }
  checked$reason <- vapply(seq_len(nrow(checked)), function(i) {
    row <- checked[i, , drop = FALSE]
    reasons <- character(0)
    if (row$.source_rows != 1L) reasons <- c(reasons, "duplicate source action")
    if (is.na(row$.mapped_rows)) {
      reasons <- c(reasons, "missing staged action")
    } else {
      if (row$.mapped_rows != 1L) reasons <- c(reasons, "duplicate stint mapping")
      if (is.na(row$.mapped_quarter) || row$.mapped_quarter != row$quarter ||
          is.na(row$.mapped_type) || row$.mapped_type != row$type) {
        reasons <- c(reasons, "staged period/type differs from source")
      }
      if (is.na(row$segment_id)) reasons <- c(reasons, "missing segment")
      if (is.na(row$team_id_defense) || row$team_id_defense <= 0 ||
          is.na(row$team_id) || row$team_id <= 0 ||
          row$team_id_defense == row$team_id) {
        reasons <- c(reasons, "invalid opposing team")
      }
      if (!isTRUE(row$.offense_valid)) reasons <- c(reasons, "invalid offense lineup")
      if (!isTRUE(row$.defense_valid)) reasons <- c(reasons, "invalid defense lineup")
    }
    paste(reasons, collapse = "; ")
  }, character(1))
  checked |>
    dplyr::filter(nzchar(reason)) |>
    dplyr::select(dplyr::all_of(c(key, "quarter", "quarter_time", "type", "reason"))) |>
    dplyr::distinct() |>
    dplyr::arrange(game_id, quarter, id, team_id)
}

assert_gameplay_lineup_coverage <- function(actions, pws, lineups,
                                           log_msg = NULL, diagnostic_dir = NULL) {
  failures <- gameplay_lineup_coverage_failures(actions, pws, lineups)
  if (!nrow(failures)) return(invisible(TRUE))
  log <- if (is.null(log_msg)) function(msg, level = "ERROR") invisible(NULL) else log_msg
  for (i in seq_len(min(nrow(failures), 20L))) {
    row <- failures[i, , drop = FALSE]
    log(sprintf("  lineup coverage failed: game=%s Q%s clock=%s team=%s id=%s type=%s: %s",
                row$game_id, row$quarter, row$quarter_time, row$team_id,
                row$id, row$type, row$reason), "ERROR")
  }
  if (!is.null(diagnostic_dir)) {
    tryCatch({
      dir.create(diagnostic_dir, recursive = TRUE, showWarnings = FALSE)
      for (gid in unique(failures$game_id)) {
        path <- file.path(diagnostic_dir, sprintf("lineup_coverage_game_%s.csv", gid))
        utils::write.csv(failures[failures$game_id == gid, , drop = FALSE],
                         path, row.names = FALSE, na = "")
        log(sprintf("  lineup coverage diagnostic: %s", path), "ERROR")
      }
    }, error = function(e) {
      log(sprintf("  could not write lineup coverage diagnostic: %s", conditionMessage(e)), "WARN")
    })
  }
  examples <- utils::head(failures, 5L)
  message <- sprintf(
    "Gameplay lineup coverage failed: %d offending action(s); %s",
    nrow(dplyr::distinct(failures, game_id, id, team_id)),
    paste(sprintf("game=%s Q%s team=%s id=%s (%s)", examples$game_id,
                  examples$quarter, examples$team_id, examples$id, examples$reason),
          collapse = " | ")
  )
  stop(structure(list(message = message, call = NULL, failures = failures),
                 class = c("lineup_coverage_error", "error", "condition")))
}
