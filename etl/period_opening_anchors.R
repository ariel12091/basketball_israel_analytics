# Period-opening lineup anchors.
#
# compute_lineups_lookup() materialises a lineup state only at a substitution
# action id, so a team that does not substitute at a period boundary has no
# opening segment for that period. compute_stints() then starts the shared
# stint at the LATER of the two teams' first substitutions, and every action
# before that overlap is dropped by df_pts_poss_longer.sql's
# `WHERE lineup_hash IS NOT NULL`.
#
# An anchor is one extra row per team per period, carrying no substitution
# payload. The existing fill(is_on, "down") supplies the carried-forward
# lineup, so the anchor state is the previous period's closing five with no
# separate seed lookup.
#
# The anchor names its team explicitly. compute_lineups_lookup() joins
# full_rosters to subs ON team_id AND game_id, and a NULL team_id matches no
# roster row at all, so a team-less anchor would join to nothing.
#
# See docs/plans/2026-09-19-period-opening-lineup-anchors-design.md.

# Columns an anchor must not claim: it asserts that a period started, not that
# a substitution happened, so is_on falls through to the filled-down value.
PERIOD_ANCHOR_NULL_COLUMNS <- c(
  "player_id", "parameters_player_in", "parameters_player_out"
)

period_opening_anchors <- function(actions, teams, min_quarter = 2L) {
  required <- c("id", "game_id", "quarter")
  missing <- setdiff(required, names(actions))
  if (length(missing)) {
    stop(
      sprintf("actions is missing required columns: %s", paste(missing, collapse = ", ")),
      call. = FALSE
    )
  }
  team_required <- c("game_id", "team_id")
  team_missing <- setdiff(team_required, names(teams))
  if (length(team_missing)) {
    stop(
      sprintf("teams is missing required columns: %s", paste(team_missing, collapse = ", ")),
      call. = FALSE
    )
  }

  empty <- actions[0, , drop = FALSE]
  eligible <- actions[!is.na(actions$quarter) & actions$quarter >= min_quarter, , drop = FALSE]
  if (!nrow(eligible)) return(empty)

  # Canonical ordering is by action id, never by clock: the provider clock can
  # regress inside a period, so the lowest id is the period's first action even
  # when a later row carries a higher remaining-seconds value.
  eligible <- eligible[order(eligible$game_id, eligible$quarter, eligible$id), , drop = FALSE]
  period_key <- paste(eligible$game_id, eligible$quarter, sep = "/")
  openings <- eligible[!duplicated(period_key), , drop = FALSE]

  teams <- unique(teams[, team_required, drop = FALSE])
  rows <- lapply(seq_len(nrow(openings)), function(i) {
    opening <- openings[i, , drop = FALSE]
    game_teams <- teams$team_id[teams$game_id == opening$game_id]
    if (!length(game_teams)) return(NULL)

    # The period's first action is often itself a substitution. Anchoring that
    # team again at the same id would give one player two states at one
    # instant, so only the other team is anchored there.
    if (identical(as.character(opening$type), "substitution")) {
      game_teams <- setdiff(game_teams, opening$team_id)
    }
    if (!length(game_teams)) return(NULL)

    out <- opening[rep(1L, length(game_teams)), , drop = FALSE]
    out$team_id <- as.integer(game_teams)
    out
  })

  rows <- Filter(Negate(is.null), rows)
  if (!length(rows)) return(empty)
  anchors <- do.call(rbind, rows)

  for (column in intersect(PERIOD_ANCHOR_NULL_COLUMNS, names(anchors))) {
    anchors[[column]] <- NA
    storage.mode(anchors[[column]]) <- "integer"
  }

  rownames(anchors) <- NULL
  anchors
}

# Gate 4 of the design: the period's lowest action id must also carry the
# period's maximum remaining-seconds value. A violation means the provider
# stamped the period opening with a later clock -- the games 398/399 defect
# class -- so seeding from that row would anchor the period at the wrong
# moment. Returns one row per offending period; zero rows means the gate
# passes.
period_anchor_clock_violations <- function(actions, teams, min_quarter = 2L,
                                           clock_column = "end_game_seconds_remaining") {
  anchors <- period_opening_anchors(actions, teams, min_quarter = min_quarter)
  empty <- data.frame(
    game_id = integer(0), quarter = integer(0), anchor_id = integer(0),
    anchor_clock = numeric(0), period_max_clock = numeric(0),
    stringsAsFactors = FALSE
  )
  if (!nrow(anchors)) return(empty)
  if (!clock_column %in% names(actions)) {
    stop(sprintf("actions has no column '%s'", clock_column), call. = FALSE)
  }

  periods <- unique(anchors[, c("game_id", "quarter", "id", clock_column), drop = FALSE])
  period_max <- vapply(seq_len(nrow(periods)), function(i) {
    rows <- actions[
      actions$game_id == periods$game_id[[i]] & actions$quarter == periods$quarter[[i]],
      ,
      drop = FALSE
    ]
    max(as.numeric(rows[[clock_column]]), na.rm = TRUE)
  }, numeric(1))

  out <- data.frame(
    game_id = as.integer(periods$game_id),
    quarter = as.integer(periods$quarter),
    anchor_id = as.integer(periods$id),
    anchor_clock = as.numeric(periods[[clock_column]]),
    period_max_clock = period_max,
    stringsAsFactors = FALSE
  )
  out <- out[out$anchor_clock < out$period_max_clock, , drop = FALSE]
  rownames(out) <- NULL
  out
}

# dbplyr twin of period_opening_anchors(), applied inside
# compute_lineups_lookup() against Postgres. Same rule: the lowest action id
# per (game_id, quarter >= min_quarter), one row per roster team, skipping the
# team whose own substitution is that lowest id, with the substitution payload
# nulled. Parity with the pure helper is tested on Postgres and re-checked at
# runtime by apply_period_anchor_gates().
#
# The NULLs are cast explicitly: an untyped NULL in a subquery resolves to
# text in Postgres, and the UNION ALL with the integer substitution columns
# would then fail.
period_opening_anchors_tbl <- function(actions, teams, min_quarter = 2L) {
  min_quarter <- as.integer(min_quarter)
  action_cols <- colnames(actions)
  null_cols <- intersect(PERIOD_ANCHOR_NULL_COLUMNS, action_cols)
  typed_nulls <- stats::setNames(
    rep(list(dbplyr::sql("CAST(NULL AS INTEGER)")), length(null_cols)),
    null_cols
  )

  actions |>
    dplyr::filter(!is.na(quarter), quarter >= !!min_quarter) |>
    dplyr::group_by(game_id, quarter) |>
    dplyr::summarise(id = min(id, na.rm = TRUE), .groups = "drop") |>
    dplyr::inner_join(actions, by = c("game_id", "quarter", "id")) |>
    dplyr::rename(.opening_team_id = team_id) |>
    dplyr::inner_join(dplyr::distinct(teams, game_id, team_id), by = "game_id") |>
    dplyr::filter(!dplyr::coalesce(
      type == "substitution" & team_id == .opening_team_id, FALSE
    )) |>
    dplyr::mutate(!!!typed_nulls) |>
    dplyr::select(dplyr::all_of(action_cols))
}

# ---- Runtime gates on the computed lineups_lookup rows ----
# compute_lineups_lookup() marks every row with period_anchor. After the
# slice_max de-duplication, an anchor survives only where the team had no
# substitution at that clock -- exactly the rows these gates inspect.

PERIOD_ANCHOR_KEY <- c("game_id", "team_id", "quarter", "id")

period_anchor_key_strings <- function(df) {
  do.call(paste, c(
    lapply(PERIOD_ANCHOR_KEY, function(col) as.character(as.integer(df[[col]]))),
    sep = "/"
  ))
}

# Gate 1 degrades rather than rejects: the base load is one transaction per
# game, so rejecting would leave the game with no rows at all. An anchor whose
# carried-forward state is not five players is dropped, and that period loads
# exactly as it did before anchors existed. Provider-derived rows are never
# touched, even when malformed.
drop_malformed_period_anchors <- function(lineups) {
  bad <- (lineups$period_anchor %in% TRUE) & !(lineups$n_on %in% 5)
  dropped <- unique(lineups[bad, c(PERIOD_ANCHOR_KEY, "n_on"), drop = FALSE])
  rownames(dropped) <- NULL
  list(lineups = lineups[!bad, , drop = FALSE], dropped = dropped)
}

# Gates 2 and 3 as a runtime parity check: every anchor the SQL retained must
# be one the pure helper selects from the same game's actions. That covers
# "the anchor id exists in actions_clean" and the per-team skip rule, on the
# real engine and real data, for every processed game. Returns offending
# anchor keys; zero rows means the gate passes.
period_anchor_parity_errors <- function(lineups, actions, teams) {
  retained <- unique(lineups[lineups$period_anchor %in% TRUE, PERIOD_ANCHOR_KEY, drop = FALSE])
  expected <- period_opening_anchors(actions, teams)
  out <- retained[
    !period_anchor_key_strings(retained) %in% period_anchor_key_strings(expected),
    ,
    drop = FALSE
  ]
  rownames(out) <- NULL
  out
}

apply_period_anchor_gates <- function(lineups, actions, teams, log_msg = NULL) {
  log <- if (is.null(log_msg)) function(msg, level = "INFO") invisible(NULL) else log_msg

  clock <- period_anchor_clock_violations(actions, teams)
  if (nrow(clock)) {
    stop(sprintf(
      "period anchor clock gate failed (lowest id is not the period's opening clock): %s",
      paste(sprintf("game %d Q%d id %d at %s < %s", clock$game_id, clock$quarter,
                    clock$anchor_id, clock$anchor_clock, clock$period_max_clock),
            collapse = "; ")
    ), call. = FALSE)
  }

  parity <- period_anchor_parity_errors(lineups, actions, teams)
  if (nrow(parity)) {
    stop(sprintf(
      "period anchor parity failed (SQL anchor not chosen by the helper): %s",
      paste(period_anchor_key_strings(parity), collapse = "; ")
    ), call. = FALSE)
  }

  gate1 <- drop_malformed_period_anchors(lineups)
  d <- gate1$dropped
  for (i in seq_len(nrow(d))) {
    log(sprintf(
      "  period anchor dropped (Gate 1): game %d team %d Q%d id %d n_on=%s",
      as.integer(d$game_id[i]), as.integer(d$team_id[i]), as.integer(d$quarter[i]),
      as.integer(d$id[i]), format(d$n_on[i])
    ), "WARN")
  }

  out <- gate1$lineups
  out$period_anchor <- NULL
  out
}
