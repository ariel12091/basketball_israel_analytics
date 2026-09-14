# =============================================================================
# etl/dq_findings_html.R
#
# Readable companion to the data quality report: one HTML page that leads
# with a summary, then lists the games, players and events that need a
# change, each with its effect on the app and the fix, ordered by severity.
#
# Sourced by run_data_quality_report(), which passes the check results it
# already holds plus name lookups from the database. No queries run here.
# =============================================================================

DQ_TIERS <- c("critical", "high", "medium", "low")
DQ_TIER_LABELS <- c(
  critical = "Critical",
  high = "High",
  medium = "Medium",
  low = "Low"
)
DQ_TIER_MEANING <- c(
  critical = "Numbers people read in the app are wrong or split",
  high = "Time-based views and filters misplace events",
  medium = "Narrow or likely-harmless, worth a look",
  low = "Offline, cosmetic or informational"
)

dq_escape <- function(x) {
  x <- as.character(x)
  x[is.na(x)] <- ""
  x <- gsub("&", "&amp;", x, fixed = TRUE)
  x <- gsub("<", "&lt;", x, fixed = TRUE)
  x <- gsub(">", "&gt;", x, fixed = TRUE)
  gsub('"', "&quot;", x, fixed = TRUE)
}

dq_num <- function(x, digits = 0L) {
  x <- suppressWarnings(as.numeric(x))
  if (!length(x) || is.na(x)) return("n/a")
  formatC(x, format = "f", digits = digits, big.mark = ",")
}

dq_signed <- function(x, digits = 2L) {
  x <- suppressWarnings(as.numeric(x))
  if (!length(x) || is.na(x)) return("n/a")
  paste0(if (x > 0) "+" else "", formatC(x, format = "f", digits = digits))
}

# Game seconds remaining -> period label and "m:ss" left in that period.
dq_period_name <- function(quarter) {
  q <- as.integer(quarter)
  if (is.na(q)) return("?")
  if (q <= 4L) paste0("Q", q) else paste0("OT", q - 4L)
}

dq_period_clock <- function(game_clock, quarter) {
  secs <- suppressWarnings(as.numeric(game_clock))
  q <- suppressWarnings(as.integer(quarter))
  if (is.na(secs) || is.na(q)) return("?")
  if (q <= 4L) secs <- secs - (4L - q) * 600
  secs <- max(0, round(secs))
  sprintf("%d:%02d", secs %/% 60, secs %% 60)
}

dq_team <- function(ctx, team_id) {
  name <- ctx$teams$team_name[match(as.integer(team_id), ctx$teams$team_id)]
  if (is.na(name)) paste("team", team_id) else name
}

dq_player <- function(ctx, team_id, player_id) {
  p <- ctx$players
  i <- which(p$team_id == as.integer(team_id) & p$player_id == as.integer(player_id))
  if (!length(i)) i <- which(p$player_id == as.integer(player_id))
  if (!length(i)) return(paste("player", player_id))
  trimws(paste(p$firstname[i[1]], p$lastname[i[1]]))
}

dq_split_ids <- function(x) {
  ids <- suppressWarnings(as.integer(unlist(strsplit(as.character(x), "[^0-9]+"))))
  ids[!is.na(ids)]
}

# -----------------------------------------------------------------------------
# Catalog: one entry per check. `entity` says where a row belongs (a game, a
# player, or the whole dataset); `describe(row, ctx)` turns one detail row into
# a sentence; `effect` is what the app shows wrongly; `fix` is the change.
# test_dq_findings_html.R fails if a check in the report has no entry here.
# -----------------------------------------------------------------------------

DQ_FINDING_CATALOG <- list(
  A_same_player_id_multiple_roster_names = list(
    tier = "critical", entity = "dataset",
    effect = "One player ID shows under two names, so stats for two people can merge.",
    fix = "Split the reused provider ID or undo the bad canonical merge."
  ),
  B_same_roster_name_multiple_player_ids = list(
    headline = "Same player name under several IDs",
    tier = "medium", entity = "player",
    describe = function(r, ctx) sprintf(
      "%s on %s appears under %s IDs (%s).",
      r$player_name, dq_team(ctx, r$team_id), r$distinct_player_ids, r$id_games
    ),
    effect = "If these IDs are one person, the player's games are split across separate rows in Player Stats, on/off and lineups.",
    fix = "If they are one person, add a season alias; if two people, record them as distinct so the check stops flagging them."
  ),
  C_active_correction_residue_game_scoped_tables = list(
    headline = "Old player IDs left in rosters and lineups",
    tier = "critical", entity = "player",
    describe = function(r, ctx) sprintf(
      "Old ID %s is still in %s: %s rows over %s games (should be ID %s).",
      r$alias_player_id, r$source_table, dq_num(r$rows), dq_num(r$games), r$canonical_player_id
    ),
    effect = "Part of this player's games still sit under the old ID, so on/off and lineup numbers can be split between two identities.",
    fix = "Re-apply the season alias to these rows (see etl/backfill_player_id_aliases.R), then rebuild the lineup tables that still hold the old ID."
  ),
  D_active_game_overrides_without_canonical_roster_row = list(
    tier = "medium", entity = "dataset",
    effect = "A player correction points at a roster row that no longer exists, so it silently stops applying.",
    fix = "Update or retire the stale game override."
  ),
  E_aggregate_names_not_roster_valid = list(
    tier = "critical", entity = "dataset",
    effect = "The app shows a name stitched from two different players.",
    fix = "Rebuild the player aggregates from roster-valid names."
  ),
  F_lineup_derivative_active_alias_residue = list(
    headline = "Old player IDs left in lineup tables",
    tier = "critical", entity = "player",
    describe = function(r, ctx) sprintf(
      "Old ID %s is still in %s: %s rows (should be ID %s).",
      r$alias_player_id, r$source_table, dq_num(r$rows), r$canonical_player_id
    ),
    effect = "Lineup Data and the players-on/off filters look up the new ID, so lineups stored under the old ID are missed.",
    fix = "Rebuild lineups_lookup_on and sub_lineups for this season after the alias is applied."
  ),
  G_cleaned_action_duplicate_ids = list(
    tier = "critical", entity = "dataset",
    effect = "Duplicate events double points, shots and possessions.",
    fix = "Drop the duplicated source events before cleaning."
  ),
  H_base_loaded_games_missing_processed_marker = list(
    tier = "high", entity = "dataset",
    effect = "A loaded game is not marked processed, so the next ETL run may load it again.",
    fix = "Mark the game processed or reload it cleanly."
  ),
  I_processed_games_missing_base_rows = list(
    tier = "critical", entity = "dataset",
    effect = "A game is marked processed but its rows are missing, so it never reaches the app.",
    fix = "Clear the processed marker and reload the game."
  ),
  J_processed_base_games_missing_downstream_game_rows = list(
    tier = "critical", entity = "dataset",
    effect = "A processed game is missing from the per-game tables the app reads.",
    fix = "Re-run the game-grain refresh for the game."
  ),
  K_app_aggregate_duplicate_keys = list(
    tier = "critical", entity = "dataset",
    effect = "A player appears twice in an app table.",
    fix = "Rebuild the aggregate with its key enforced."
  ),
  L_raw_pbp_duplicate_action_ids = list(
    tier = "low", entity = "dataset",
    effect = "Not checked yet: duplicate IDs in the raw feed would only surface after cleaning.",
    fix = "Add a raw-payload check to the ETL."
  ),
  M_identity_dictionary_mapping_ambiguities = list(
    tier = "critical", entity = "dataset",
    effect = "A source identity maps to more than one player.",
    fix = "Keep one active mapping per source context."
  ),
  N_identity_compatibility_missing_legacy_corrections = list(
    tier = "critical", entity = "dataset",
    effect = "An older player correction is no longer applied by the ETL.",
    fix = "Restore the missing correction in the identity dictionary."
  ),
  O_identity_unresolved_source_contexts = list(
    tier = "medium", entity = "dataset",
    effect = "A roster identity fell back to the provider's raw ID and may split across seasons.",
    fix = "Add the identity to the dictionary."
  ),
  P0_source_placeholder_roster_identities = list(
    headline = "Placeholder players on source rosters",
    tier = "low", entity = "player",
    describe = function(r, ctx) sprintf(
      "Placeholder \"%s\" (ID %s) on %s's roster in game %s.",
      r$player_name, r$player_id, dq_team(ctx, r$team_id), r$game_ids
    ),
    effect = "None on stats: the provider's placeholder never played and is left out of app aggregates.",
    fix = "No change needed."
  ),
  P_app_invalid_or_nonparticipant_player_rows = list(
    headline = "Placeholder players in app player lists",
    tier = "low", entity = "dataset",
    describe = function(r, ctx) sprintf(
      "%s placeholder rows among %s rows in %s.",
      dq_num(r$placeholder_rows), dq_num(r$total_rows), r$source_table
    ),
    effect = "Placeholder names can appear in player lists; they carry no minutes, points or possessions.",
    fix = "Filter placeholders out of the aggregate if they show up in a dropdown."
  ),
  P1_reviewed_data_quality_exceptions = list(
    tier = "low", entity = "exception",
    describe = function(r, ctx) sprintf("Game %s: %s", r$game_id, r$reason),
    effect = "Reviewed and expected.",
    fix = "None."
  ),
  Q_persisted_rows_without_lineup_match = list(
    headline = "Events with no lineup attached",
    tier = "critical", entity = "game",
    describe = function(r, ctx) sprintf(
      "%s of %s event rows (%s%%) have no five-player lineup attached.",
      dq_num(r$unmatched_rows), dq_num(r$total_rows), dq_num(r$unmatched_pct, 1L)
    ),
    effect = "Those events drop out of on/off, Lineup Data and lineup-based ratings for this game.",
    fix = "Trace the substitution chain around the unmatched events; a missed or duplicated substitution breaks the lineup."
  ),
  R_invalid_lineup_player_counts = list(
    headline = "Lineups without exactly five players",
    tier = "critical", entity = "game",
    describe = function(r, ctx) sprintf(
      "%s: %s of %s lineup states don't have five players on court (seen %s to %s).",
      dq_team(ctx, r$team_id), dq_num(r$invalid_states), dq_num(r$total_states),
      r$min_reported_n_on, r$max_reported_n_on
    ),
    effect = "Minutes and points in those states go to lineups that aren't real fives, in on/off and Lineup Data.",
    fix = "Fix the substitutions that leave too many or too few players on court."
  ),
  S_invalid_starter_counts = list(
    headline = "Rows missing starter context",
    tier = "medium", entity = "game",
    games = function(r) dq_split_ids(r$affected_game_ids),
    describe = function(r, ctx) sprintf(
      "%s of %s rows in %s have no valid starter context.",
      dq_num(r$invalid_rows), dq_num(r$total_rows), r$source_table
    ),
    effect = "Those rows fall outside the starters/bench filters.",
    fix = "Check the game's starter declarations."
  ),
  T_invalid_team_minutes = list(
    headline = "Team minutes don't match the game's length",
    tier = "critical", entity = "game",
    describe = function(r, ctx) sprintf(
      "%s logged %s team minutes; the game lasted %s (%s).",
      dq_team(ctx, r$team_id), dq_num(r$minutes, 2L), dq_num(r$expected_minutes),
      dq_signed(r$minute_difference)
    ),
    effect = "Minutes totals and per-minute rates for this team-game are off by the difference.",
    fix = "Look for a stretch with no lineup (often a missing substitution or a clock gap)."
  ),
  U_invalid_lineup_metric_values = list(
    tier = "critical", entity = "dataset",
    effect = "Lineup rows with impossible counts or minutes reach the app.",
    fix = "Rebuild the affected lineup rows."
  ),
  V_team_game_score_reconciliation = list(
    headline = "Scores don't match the reconstructed points",
    tier = "critical", entity = "game",
    describe = function(r, ctx) sprintf(
      "%s scored %s officially; the box score has %s and lineup offense %s. The opponent scored %s officially; lineup defense counts %s.",
      r$team_name, dq_num(r$team_score), dq_num(r$traditional_points), dq_num(r$lineup_offense_points),
      dq_num(r$opp_score), dq_num(r$lineup_defense_points)
    ),
    effect = "Points in Player Stats, on/off and Lineup Data don't add up to the final score.",
    fix = "Compare the play-by-play with the official box score for a missing, duplicated or rescinded basket."
  ),
  W_team_game_possession_reconciliation = list(
    headline = "Possessions don't mirror between the teams",
    tier = "critical", entity = "game",
    describe = function(r, ctx) sprintf(
      "%s: %s offensive possessions, but %s's defense counts %s.",
      r$team_name, dq_num(r$team_off_poss), r$opp_team_name, dq_num(r$opponent_def_poss)
    ),
    effect = "Offensive and defensive ratings for this game don't mirror each other.",
    fix = "Check the possession-ending events around the mismatch."
  ),
  X_player_minute_conservation = list(
    headline = "Player minutes don't add up",
    tier = "critical", entity = "game",
    describe = function(r, ctx) sprintf(
      "%s: player minutes add up to %s, expected %s (%s).",
      r$team_name, dq_num(r$actual_player_minutes, 2L), dq_num(r$expected_player_minutes, 2L),
      dq_signed(r$minute_difference)
    ),
    effect = "Individual minutes in Player Stats and on/off are wrong for this team-game.",
    fix = "Fix the substitutions that put a player on court twice or leave a slot empty."
  ),
  Y_ot_period_start_lineup_coverage = list(
    headline = "Overtime starts without a lineup",
    tier = "high", entity = "game",
    describe = function(r, ctx) sprintf(
      "%s: %s starts without a valid lineup; the first one appears with %s left.",
      dq_team(ctx, r$team_id), dq_period_name(r$quarter), dq_period_clock(r$first_valid_clock, 5L)
    ),
    effect = "The start of overtime has no lineup, so those minutes and possessions miss on/off and lineups.",
    fix = "Recover the OT starting five (see etl/ot_lineup_recovery.R)."
  ),
  Z_ot_event_player_lineup_mismatches = list(
    headline = "Overtime plays credited to benched players",
    tier = "medium", entity = "game",
    describe = function(r, ctx) sprintf(
      "%s event %s: %s is credited but isn't in %s's lineup.",
      dq_period_name(r$quarter), r$event_id, dq_player(ctx, r$team_id, r$player_id), dq_team(ctx, r$team_id)
    ),
    effect = "An overtime play is credited to a player the lineup data has on the bench.",
    fix = "Check the substitution just before the event; same-clock ordering can explain it."
  ),
  AA_material_clock_order_anomalies = list(
    headline = "Events out of period or clock order",
    tier = "high", entity = "game",
    describe = function(r, ctx) {
      parts <- c(
        if (as.numeric(r$quarter_regression_rows) > 0)
          sprintf("the period goes backwards %s time(s)", r$quarter_regression_rows),
        if (as.numeric(r$reversal_gt24_rows) > 0 && !(r$game_id %in% ctx$clock_run_games))
          sprintf("the clock jumps back %ss", dq_num(r$max_reversal_seconds)),
        if (as.numeric(r$out_of_range_rows) > 0)
          sprintf("%s clock values fall outside their period", r$out_of_range_rows)
      )
      if (!length(parts)) return(NULL)
      paste0(toupper(substr(parts[1], 1, 1)), substring(paste(parts, collapse = "; "), 2), ".")
    },
    effect = "Events land in the wrong period or out of order in time-based views (gameflow, quarter splits, clutch).",
    fix = "Move each flagged event to its real period or clock in the ETL."
  ),
  AB_clock_order_jitter = list(
    headline = "Small backward clock steps",
    tier = "low", entity = "game",
    describe = function(r, ctx) sprintf(
      "The clock steps back up to %ss, %s time(s), at events %s.",
      dq_num(r$max_reversal_seconds), r$reversal_rows, r$action_transitions
    ),
    effect = "Usually provider rounding. Matters only if a step crosses a quarter or clutch boundary.",
    fix = "Review only if the step crosses a boundary."
  ),
  AK_misplaced_clock_runs = list(
    headline = "Events stamped with the wrong game clock",
    tier = "high", entity = "game",
    describe = function(r, ctx) {
      before <- identical(r$likely_misplaced_side, "before_jump")
      sprintf(
        "%s: %s event(s) stamped %s left (%s scoring) sit %s a run stamped %s left.%s",
        r$period, r$misplaced_events,
        if (before) r$before_clock_left else r$after_clock_left,
        r$misplaced_scoring_plays,
        if (before) "before" else "after",
        if (before) r$after_clock_left else r$before_clock_left,
        switch(as.character(r$review_status),
               verified = " Verified against the feed.",
               likely = " Likely; not confirmable.",
               "")
      )
    },
    events = function(r) {
      if (identical(r$likely_misplaced_side, "before_jump")) {
        sprintf("%s to %s", r$before_first_id, r$before_last_id)
      } else {
        sprintf("%s to %s", r$after_first_id, r$after_last_id)
      }
    },
    note = function(r) r$diagnosis,
    effect = "Gameflow, quarter cards, quarter and clutch splits place these events at the wrong time; scores and counts still add up.",
    fix = "Correct the clock on the misplaced events in the ETL (like the game-381 source correction in etl_onoff.R) and reload the game."
  ),
  AC_missing_regulation_period_coverage = list(
    headline = "A regulation quarter is missing",
    tier = "critical", entity = "game",
    describe = function(r, ctx) "A regulation quarter has no events.",
    effect = "A whole quarter is missing from every stat for this game.",
    fix = "Reload the game's play-by-play."
  ),
  AD_clutch_clock_exposure = list(
    headline = "Plays that can slip in or out of clutch",
    tier = "high", entity = "game",
    describe = function(r, ctx) sprintf(
      "Event %s follows %s but is stamped %s %s left, %s.",
      r$action_id, r$prev_id, dq_period_name(r$quarter), dq_period_clock(r$game_clock, r$quarter),
      if (identical(r$exposure_type, "period_regressed_out_of_clutch_scope"))
        sprintf("after a %s event", dq_period_name(r$prev_quarter))
      else "re-entering Q4 outside the last five minutes"
    ),
    events = function(r) as.character(r$action_id),
    effect = "The clutch filter (Lineup Data, Team Ratings) can leave this play out of a clutch window, or count it in one.",
    fix = "Correct the event's period or clock so it falls in its real window."
  ),
  AE_duplicate_persisted_action_stint_keys = list(
    tier = "critical", entity = "dataset",
    effect = "One event counts twice for a team: points, shots and possessions double.",
    fix = "Rebuild the event table for the game."
  ),
  AF_invalid_persisted_segment_ids = list(
    tier = "critical", entity = "dataset",
    effect = "Segments without an ID collapse, so minutes go missing.",
    fix = "Rebuild segment IDs for the game."
  ),
  AH_canonical_segment_timing = list(
    headline = "Segment minutes a few seconds off",
    tier = "medium", entity = "game",
    describe = function(r, ctx) sprintf(
      "%s: segments total %ss against a %ss game timeline (%ss off).",
      dq_team(ctx, r$team_id), dq_num(r$total_segment_seconds), dq_num(r$game_end_elapsed_seconds),
      dq_num(r$conservation_difference_seconds)
    ),
    effect = "Minutes for this team-game are off by a few seconds.",
    fix = "Re-run the canonical segment timing refresh for the game."
  ),
  AI_team_game_minute_mirror = list(
    tier = "critical", entity = "dataset",
    effect = "A team's offense and defense minutes disagree.",
    fix = "Rebuild the team minute rows."
  ),
  AJ_free_throw_progress_domain = list(
    tier = "high", entity = "dataset",
    effect = "An impossible free-throw progress value can break possession endings.",
    fix = "Clamp or correct the provider's attempt metadata."
  ),
  AG_cold_storage_snapshot_consistency = list(
    headline = "Cold-storage snapshot inconsistencies",
    tier = "low", entity = "dataset",
    group = "issue_type",
    describe = function(r, ctx) sprintf(
      "%s: %s rows across %s game(s).",
      gsub("_", " ", r$issue_type), dq_num(r$affected_rows), r$games
    ),
    effect = "Offline audits and cold-storage restores only; the live app is unaffected.",
    fix = "Re-export the affected cold-storage tables."
  )
)

# -----------------------------------------------------------------------------
# Findings table
# -----------------------------------------------------------------------------

dq_rows <- function(df) {
  if (is.null(df) || !nrow(df)) return(list())
  lapply(seq_len(nrow(df)), function(i) as.list(df[i, , drop = FALSE]))
}

dq_build_findings <- function(summary_df, details_by_check, ctx) {
  open <- summary_df$check_id[summary_df$status %in% c("fail", "warning")]
  out <- list()
  add <- function(...) out[[length(out) + 1L]] <<- data.frame(..., stringsAsFactors = FALSE)
  for (check_id in open) {
    entry <- DQ_FINDING_CATALOG[[check_id]]
    details <- details_by_check[[check_id]]
    if (is.null(entry) || is.null(details) || !nrow(details) || is.null(entry$describe)) next
    if (!is.null(entry$group)) {
      key <- details[[entry$group]]
      kinds <- unique(key)
      details <- data.frame(
        issue_type = kinds,
        affected_rows = vapply(kinds, function(k) sum(as.numeric(details$affected_rows[key == k])), numeric(1)),
        games = vapply(kinds, function(k) length(unique(details$game_id[key == k])), integer(1)),
        stringsAsFactors = FALSE
      )
    }
    for (r in dq_rows(details)) {
      text <- entry$describe(r, ctx)
      if (is.null(text) || !nzchar(text)) next
      games <- if (identical(entry$entity, "game")) {
        if (is.function(entry$games)) entry$games(r) else as.integer(r$game_id)
      } else NA_integer_
      player_name <- if (identical(entry$entity, "player")) {
        r$player_name %||% dq_player(ctx, r$team_id, r$canonical_player_id %||% r$player_id)
      } else NA_character_
      player_key <- if (identical(entry$entity, "player")) {
        paste(r$team_id %||% "", toupper(trimws(player_name)), sep = ":")
      } else NA_character_
      for (g in games) {
        add(
          check_id = check_id, tier = entry$tier, entity = entry$entity,
          game_id = g, player_key = player_key, player_name = player_name,
          team_id = suppressWarnings(as.integer(r$team_id %||% NA)),
          player_id = suppressWarnings(as.integer(r$canonical_player_id %||% r$player_id %||% NA)),
          text = text,
          events = if (is.function(entry$events)) entry$events(r) else "",
          note = if (is.function(entry$note)) as.character(entry$note(r) %||% "") else "",
          effect = entry$effect, fix = entry$fix
        )
      }
    }
  }
  if (!length(out)) {
    return(data.frame(check_id = character(), tier = character(), entity = character(),
                      game_id = integer(), player_key = character(), player_name = character(), team_id = integer(),
                      player_id = integer(), text = character(), events = character(),
                      note = character(), effect = character(), fix = character(),
                      stringsAsFactors = FALSE))
  }
  findings <- do.call(rbind, out)
  findings$rank <- match(findings$tier, DQ_TIERS)
  findings[order(findings$rank, findings$check_id), , drop = FALSE]
}

# -----------------------------------------------------------------------------
# HTML
# -----------------------------------------------------------------------------

dq_pill <- function(tier) {
  sprintf('<span class="pill %s">%s</span>', tier, DQ_TIER_LABELS[[tier]])
}

dq_game_heading <- function(game_id, ctx) {
  g <- ctx$games[ctx$games$game_id == game_id, , drop = FALSE]
  if (!nrow(g)) return(list(title = sprintf("Game %s", game_id), meta = ""))
  g <- g[order(!g$is_home), , drop = FALSE]
  d <- as.Date(g$game_date[1])
  date_txt <- if (is.na(d)) "" else sprintf("%d %s %s", as.integer(format(d, "%d")), month.abb[as.integer(format(d, "%m"))], format(d, "%Y"))
  score <- if (nrow(g) >= 2) {
    sprintf("%s %s&ndash;%s %s", dq_escape(g$team_name[1]), dq_num(g$team_score[1]), dq_num(g$team_score[2]), dq_escape(g$team_name[2]))
  } else dq_escape(g$team_name[1])
  list(title = score, meta = sprintf("Game %s &middot; %s", game_id, date_txt))
}

dq_finding_items <- function(f) {
  paste(vapply(seq_len(nrow(f)), function(i) {
    r <- f[i, ]
    sprintf(
      paste0(
        '<li class="finding"><div class="finding-head">%s<p>%s</p></div>',
        '<dl><div><dt>In the app</dt><dd>%s</dd></div><div><dt>Fix</dt><dd>%s</dd></div>%s</dl>%s</li>'
      ),
      dq_pill(r$tier), dq_escape(r$text), dq_escape(r$effect), dq_escape(r$fix),
      if (nzchar(r$events)) sprintf('<div><dt>Events</dt><dd class="mono">%s</dd></div>', dq_escape(r$events)) else "",
      if (nzchar(r$note)) sprintf('<p class="note">%s</p>', dq_escape(r$note)) else ""
    )
  }, character(1)), collapse = "")
}

write_dq_findings_html <- function(summary_df, details_by_check, ctx, path, meta) {
  ctx$clock_run_games <- as.integer(details_by_check[["AK_misplaced_clock_runs"]]$game_id)
  findings <- dq_build_findings(summary_df, details_by_check, ctx)
  exceptions <- findings[findings$entity == "exception", , drop = FALSE]
  findings <- findings[findings$entity != "exception", , drop = FALSE]
  game_f <- findings[findings$entity == "game", , drop = FALSE]
  player_f <- findings[findings$entity == "player", , drop = FALSE]
  data_f <- findings[findings$entity == "dataset", , drop = FALSE]

  # Summary: one line per open check, worst first, with its reach.
  open_checks <- unique(findings$check_id)
  summary_lines <- vapply(open_checks, function(id) {
    f <- findings[findings$check_id == id, , drop = FALSE]
    reach <- if (all(f$entity == "game")) {
      sprintf("%d game%s", length(unique(f$game_id)), if (length(unique(f$game_id)) == 1) "" else "s")
    } else if (all(f$entity == "player")) {
      n <- length(unique(f$player_key)); sprintf("%d player%s", n, if (n == 1) "" else "s")
    } else {
      sprintf("%d item%s", nrow(f), if (nrow(f) == 1) "" else "s")
    }
    title <- DQ_FINDING_CATALOG[[id]]$headline %||% summary_df$title[summary_df$check_id == id][1]
    sprintf('<li>%s<span class="line">%s</span><span class="reach mono">%s</span></li>',
            dq_pill(f$tier[1]), dq_escape(title), reach)
  }, character(1))
  tier_counts <- vapply(DQ_TIERS, function(t) {
    f <- findings[findings$tier == t, , drop = FALSE]
    sprintf(
      '<div class="tally %s"><span class="tally-label">%s</span><strong class="mono">%d</strong><span class="tally-unit">problem types</span><span class="tally-sub">%s</span></div>',
      t, DQ_TIER_LABELS[[t]], length(unique(f$check_id)),
      dq_escape(sprintf("%s. %d game%s.", DQ_TIER_MEANING[[t]],
                        length(unique(stats::na.omit(f$game_id))),
                        if (length(unique(stats::na.omit(f$game_id))) == 1) "" else "s"))
    )
  }, character(1))

  # Games, worst first: best tier, then number of problems.
  game_ids <- unique(game_f$game_id)
  game_order <- if (length(game_ids)) {
    best <- vapply(game_ids, function(g) min(game_f$rank[game_f$game_id == g]), numeric(1))
    n <- vapply(game_ids, function(g) sum(game_f$game_id == g), numeric(1))
    game_ids[order(best, -n, -game_ids)]
  } else integer()
  game_cards <- vapply(game_order, function(g) {
    f <- game_f[game_f$game_id == g, , drop = FALSE]
    h <- dq_game_heading(g, ctx)
    sprintf(
      '<article class="entry %s" id="game-%s"><header><p class="meta mono">%s</p><h3>%s</h3><p class="count">%d problem%s</p></header><ul class="findings">%s</ul></article>',
      f$tier[1], g, h$meta, h$title, nrow(f), if (nrow(f) == 1) "" else "s", dq_finding_items(f)
    )
  }, character(1))

  players <- unique(player_f$player_key)
  player_cards <- vapply(players, function(k) {
    f <- player_f[player_f$player_key == k, , drop = FALSE]
    ids <- stats::na.omit(f$player_id)
    name <- if (length(ids)) dq_player(ctx, f$team_id[1], ids[1]) else f$player_name[1]
    sprintf(
      '<article class="entry %s"><header><p class="meta mono">%s%s</p><h3>%s</h3><p class="count">%d problem%s</p></header><ul class="findings">%s</ul></article>',
      f$tier[1], dq_escape(if (!is.na(f$team_id[1])) dq_team(ctx, f$team_id[1]) else ""),
      if (length(ids)) sprintf(" &middot; ID %s", ids[1]) else "",
      dq_escape(name), nrow(f), if (nrow(f) == 1) "" else "s", dq_finding_items(f)
    )
  }, character(1))
  player_cards <- player_cards[order(match(vapply(players, function(k) player_f$tier[player_f$player_key == k][1], ""), DQ_TIERS))]

  data_items <- if (nrow(data_f)) dq_finding_items(data_f) else ""
  exception_items <- if (nrow(exceptions)) paste(sprintf("<li>%s</li>", dq_escape(exceptions$text)), collapse = "") else ""

  all_checks <- summary_df
  all_checks$tier <- vapply(all_checks$check_id, function(id) DQ_FINDING_CATALOG[[id]]$tier %||% "low", "")
  all_checks <- all_checks[order(match(all_checks$status, c("fail", "warning", "query_error", "pass", "skipped", "not_automated")),
                                 match(all_checks$tier, DQ_TIERS)), , drop = FALSE]
  check_rows <- paste(sprintf(
    '<tr><td class="mono">%s</td><td>%s</td><td><span class="status %s">%s</span></td><td class="num mono">%s</td><td class="mono dim">%s</td></tr>',
    dq_escape(all_checks$check_id), dq_escape(all_checks$title), dq_escape(all_checks$status), dq_escape(gsub("_", " ", all_checks$status)),
    ifelse(is.na(all_checks$issue_count), "", dq_escape(all_checks$issue_count)), dq_escape(all_checks$detail_file)
  ), collapse = "")

  worst_game_tier <- vapply(game_ids, function(g) DQ_TIERS[min(game_f$rank[game_f$game_id == g])], "")
  n_critical <- sum(worst_game_tier == "critical")
  n_high <- sum(worst_game_tier == "high")
  n_players <- length(unique(player_f$player_key[player_f$tier %in% c("critical", "high")]))
  lede <- sprintf(
    "%d game%s a data fix: %d with critical problems, %d with high. %d player identit%s to repair.",
    n_critical + n_high, if (n_critical + n_high == 1) " needs" else "s need", n_critical, n_high,
    n_players, if (n_players == 1) "y" else "ies"
  )
  sublede <- sprintf(
    "Another %d game%s only medium or low findings. %d of %d checks pass.",
    length(game_ids) - n_critical - n_high, if (length(game_ids) - n_critical - n_high == 1) " carries" else "s carry",
    sum(summary_df$status == "pass"), nrow(summary_df)
  )

  section <- function(id, title, sub, body) {
    if (!nzchar(paste(body, collapse = ""))) return("")
    sprintf('<section id="%s"><div class="section-head"><h2>%s</h2><p>%s</p></div>%s</section>', id, title, sub, paste(body, collapse = ""))
  }

  html <- paste0(
    '<!doctype html><html lang="en"><head><meta charset="utf-8">',
    '<meta name="viewport" content="width=device-width,initial-scale=1">',
    "<title>IBPL Data Health</title>",
    '<link rel="preconnect" href="https://fonts.googleapis.com">',
    '<link rel="stylesheet" href="https://fonts.googleapis.com/css2?family=DM+Sans:opsz,wght@9..40,400;9..40,500;9..40,600;9..40,700&family=JetBrains+Mono:wght@400;500&display=swap">',
    "<style>", DQ_FINDINGS_CSS, "</style></head><body><main>",
    '<header class="masthead"><p class="brand">IBPL Analytics &middot; Data health</p>',
    sprintf('<h1>%s</h1><p class="sublede">%s</p>', dq_escape(lede), dq_escape(sublede)),
    sprintf('<p class="run mono">Run %s &middot; schema %s &middot; overall <span class="status %s">%s</span></p></header>',
            dq_escape(meta$run_time), dq_escape(meta$schema), tolower(meta$status), dq_escape(meta$status)),
    '<section class="summary"><div class="tallies">', paste(tier_counts, collapse = ""), "</div>",
    '<h2 class="summary-title">What needs attention, worst first</h2><ol class="summary-list">', paste(summary_lines, collapse = ""), "</ol></section>",
    section("games", "Games", "Each game lists every problem found in it, worst game first.", game_cards),
    section("players", "Players", "Identities that are split, stale or placeholders.", player_cards),
    section("dataset", "Dataset-wide", "Problems that aren't tied to one game or player.", sprintf('<ul class="findings flat">%s</ul>', data_items)),
    if (nzchar(exception_items)) sprintf('<section id="exceptions"><div class="section-head"><h2>Reviewed exceptions</h2><p>Known and expected; no change needed.</p></div><ul class="plain">%s</ul></section>', exception_items) else "",
    '<section id="checks"><details><summary>All ', nrow(summary_df), ' checks</summary><div class="table-wrap"><table><thead><tr><th>Check</th><th>What it tests</th><th>Status</th><th class="num">Issues</th><th>Detail CSV</th></tr></thead><tbody>',
    check_rows, "</tbody></table></div></details></section>",
    "</main></body></html>"
  )
  writeLines(enc2utf8(html), path, useBytes = TRUE)
  invisible(list(path = path, findings = findings))
}

DQ_FINDINGS_CSS <- "
:root{
  --ground:#f5f5f3;--surface:#ffffff;--ink:#1b1d22;--muted:#5b6270;--rule:#e1e2e4;--sunken:#eeefec;
  --brand:#a86c12;
  --critical:#c23a33;--critical-bg:#fbeceb;
  --high:#c9661d;--high-bg:#fcf0e6;
  --medium:#94761a;--medium-bg:#f7f1df;
  --low:#66707d;--low-bg:#eef0f2;
  --pass:#2f7d4f;
}
@media (prefers-color-scheme: dark){
  :root:not([data-theme=\"light\"]){
    --ground:#121417;--surface:#1a1d22;--ink:#e8e9eb;--muted:#9aa1ab;--rule:#2b2f36;--sunken:#16181c;
    --brand:#e8a435;
    --critical:#ef6b61;--critical-bg:#2c1a1a;
    --high:#f0914a;--high-bg:#2c2117;
    --medium:#d8b547;--medium-bg:#29251a;
    --low:#9aa3ae;--low-bg:#1f2227;
    --pass:#5fbf85;
  }
}
:root[data-theme=\"dark\"]{
  --ground:#121417;--surface:#1a1d22;--ink:#e8e9eb;--muted:#9aa1ab;--rule:#2b2f36;--sunken:#16181c;
  --brand:#e8a435;
  --critical:#ef6b61;--critical-bg:#2c1a1a;
  --high:#f0914a;--high-bg:#2c2117;
  --medium:#d8b547;--medium-bg:#29251a;
  --low:#9aa3ae;--low-bg:#1f2227;
  --pass:#5fbf85;
}
*{box-sizing:border-box}
body{margin:0;background:var(--ground);color:var(--ink);font:15px/1.55 'DM Sans',system-ui,-apple-system,'Segoe UI',sans-serif;padding-inline:20px;padding-block:32px 64px}
main{max-width:1040px;margin:0 auto;display:grid;gap:40px}
.mono{font-family:'JetBrains Mono',ui-monospace,Consolas,monospace;font-variant-numeric:tabular-nums}
h1,h2,h3{text-wrap:balance;margin:0}
.masthead{display:grid;gap:10px}
.brand{margin:0;color:var(--brand);font-size:12px;font-weight:600;letter-spacing:.09em;text-transform:uppercase}
.masthead h1{font-size:clamp(24px,3.4vw,34px);line-height:1.2;font-weight:600;max-width:32ch}
.sublede{margin:0;color:var(--muted);font-size:16px;max-width:60ch}
.run{margin:0;color:var(--muted);font-size:12.5px}
.status{font-weight:600}
.status.fail{color:var(--critical)}.status.warning{color:var(--high)}.status.query_error{color:var(--critical)}
.status.pass{color:var(--pass)}.status.skipped,.status.not_automated{color:var(--muted)}
.summary{display:grid;gap:18px}
.tallies{display:grid;grid-template-columns:repeat(4,minmax(0,1fr));gap:1px;background:var(--rule);border:1px solid var(--rule);border-radius:10px;overflow:hidden}
.tally{background:var(--surface);padding:14px 16px;display:grid;gap:2px;align-content:start}
.tally-label{font-size:12px;font-weight:600;letter-spacing:.06em;text-transform:uppercase}
.tally strong{font-size:28px;font-weight:500;line-height:1.1}
.tally-unit{font-size:12px;color:var(--muted)}
.tally-sub{color:var(--muted);font-size:12.5px;line-height:1.35;margin-top:4px}
.tally.critical .tally-label{color:var(--critical)}.tally.high .tally-label{color:var(--high)}
.tally.medium .tally-label{color:var(--medium)}.tally.low .tally-label{color:var(--low)}
.summary-title{font-size:13px;font-weight:600;letter-spacing:.06em;text-transform:uppercase;color:var(--muted)}
.summary-list{list-style:none;margin:0;padding:0;display:grid;border-top:1px solid var(--rule)}
.summary-list li{display:grid;grid-template-columns:78px 1fr auto;gap:12px;align-items:baseline;padding:9px 0;border-bottom:1px solid var(--rule)}
.summary-list .reach{color:var(--muted);font-size:12.5px;white-space:nowrap}
.pill{display:inline-block;justify-self:start;font-size:11px;font-weight:600;letter-spacing:.05em;text-transform:uppercase;padding:2px 8px;border-radius:999px;white-space:nowrap}
.pill.critical{color:var(--critical);background:var(--critical-bg)}
.pill.high{color:var(--high);background:var(--high-bg)}
.pill.medium{color:var(--medium);background:var(--medium-bg)}
.pill.low{color:var(--low);background:var(--low-bg)}
section{display:grid;gap:14px}
.section-head{display:grid;gap:2px;border-bottom:1px solid var(--rule);padding-bottom:8px}
.section-head h2{font-size:20px;font-weight:600}
.section-head p{margin:0;color:var(--muted);font-size:13.5px}
.entry{background:var(--surface);border:1px solid var(--rule);border-left:4px solid var(--low);border-radius:8px;padding:16px 18px;display:grid;gap:10px}
.entry.critical{border-left-color:var(--critical)}.entry.high{border-left-color:var(--high)}.entry.medium{border-left-color:var(--medium)}
.entry header{display:grid;grid-template-columns:1fr auto;gap:2px 16px;align-items:baseline}
.entry .meta{grid-column:1/-1;margin:0;color:var(--muted);font-size:12px}
.entry h3{font-size:17px;font-weight:600}
.entry .count{margin:0;color:var(--muted);font-size:12.5px;white-space:nowrap}
.findings{list-style:none;margin:0;padding:0;display:grid;gap:12px}
.findings.flat{background:var(--surface);border:1px solid var(--rule);border-radius:8px;padding:16px 18px}
.finding{display:grid;gap:6px;padding-top:12px;border-top:1px solid var(--rule)}
.findings > .finding:first-child{padding-top:0;border-top:0}
.finding-head{display:grid;grid-template-columns:78px 1fr;gap:12px;align-items:baseline}
.finding-head p{margin:0;max-width:72ch}
.finding dl{margin:0 0 0 90px;display:grid;gap:3px;font-size:13.5px}
.finding dl div{display:grid;grid-template-columns:84px 1fr;gap:10px}
.finding dt{color:var(--muted);font-size:12px;font-weight:600;letter-spacing:.04em;text-transform:uppercase;padding-top:1px}
.finding dd{margin:0;max-width:68ch}
.note{margin:4px 0 0 90px;padding:10px 12px;background:var(--sunken);border-radius:6px;font-size:13px;color:var(--muted);max-width:76ch}
.plain{margin:0;padding-left:18px;color:var(--muted);display:grid;gap:4px}
details summary{cursor:pointer;font-weight:600;font-size:15px;padding:6px 0}
details summary:focus-visible{outline:2px solid var(--brand);outline-offset:3px;border-radius:4px}
.table-wrap{overflow-x:auto;border:1px solid var(--rule);border-radius:8px;background:var(--surface);margin-top:10px}
table{width:100%;border-collapse:collapse;font-size:13px}
th,td{text-align:left;padding:8px 12px;border-bottom:1px solid var(--rule);vertical-align:top}
th{font-size:11.5px;letter-spacing:.05em;text-transform:uppercase;color:var(--muted);font-weight:600;background:var(--sunken)}
td.num,th.num{text-align:right}
.dim{color:var(--muted)}
@media (max-width:640px){
  .tallies{grid-template-columns:repeat(2,minmax(0,1fr))}
  .summary-list li{grid-template-columns:auto 1fr;}
  .summary-list .reach{grid-column:2}
  .finding-head{grid-template-columns:1fr;gap:6px}
  .finding dl,.note{margin-left:0}
  .finding dl div{grid-template-columns:1fr;gap:0}
}
"
