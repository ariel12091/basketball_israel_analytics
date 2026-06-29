# Conservative overtime lineup recovery.
#
# This module operates on already-cleaned in-memory ETL data frames. It is
# intentionally separate from compute_lineups_lookup(): provider-derived
# lineup states remain the default, and this fallback is invoked only when the
# provisional PWS has a leading OT lineup/stint gap.

OT_LINEUP_RECOVERY_EXCLUDED_GAMES <- c(
  211L # Regulation and OT action IDs overlap; see the design backlog.
)

ot_truthy <- function(x) {
  if (is.logical(x)) return(!is.na(x) & x)
  val <- tolower(trimws(as.character(x)))
  !is.na(val) & val %in% c("1", "true", "t", "yes", "y")
}

ot_sorted_players <- function(x) {
  sort(unique(as.integer(x[!is.na(x)])))
}

ot_lineup_id <- function(players) {
  paste(ot_sorted_players(players), collapse = "_")
}

ot_lineup_hash <- function(players) {
  digest::digest(ot_lineup_id(players), algo = "md5", serialize = FALSE)
}

ot_valid_lineup_states <- function(lineup_df) {
  if (is.null(lineup_df) || !nrow(lineup_df)) {
    return(data.frame())
  }

  required <- c(
    "game_id", "team_id", "quarter", "id",
    "end_quarter_seconds_remaining", "player_id", "is_on_verdict",
    "lineup_hash"
  )
  missing <- setdiff(required, names(lineup_df))
  if (length(missing)) {
    stop(
      sprintf("lineup_df is missing required columns: %s", paste(missing, collapse = ", ")),
      call. = FALSE
    )
  }

  keys <- unique(lineup_df[, c(
    "game_id", "team_id", "quarter", "id",
    "end_quarter_seconds_remaining"
  ), drop = FALSE])
  keys <- keys[order(keys$game_id, keys$team_id, keys$quarter, keys$id), , drop = FALSE]

  out <- lapply(seq_len(nrow(keys)), function(i) {
    key <- keys[i, , drop = FALSE]
    rows <- lineup_df[
      lineup_df$game_id == key$game_id &
        lineup_df$team_id == key$team_id &
        lineup_df$quarter == key$quarter &
        lineup_df$id == key$id &
        lineup_df$end_quarter_seconds_remaining == key$end_quarter_seconds_remaining,
      ,
      drop = FALSE
    ]
    players <- ot_sorted_players(rows$player_id[rows$is_on_verdict == 1])
    hashes <- unique(rows$lineup_hash[!is.na(rows$lineup_hash) & nzchar(rows$lineup_hash)])
    if (length(players) != 5L || length(hashes) != 1L) return(NULL)

    data.frame(
      game_id = as.integer(key$game_id),
      team_id = as.integer(key$team_id),
      quarter = as.integer(key$quarter),
      id = as.integer(key$id),
      end_quarter_seconds_remaining = as.numeric(key$end_quarter_seconds_remaining),
      lineup_hash = as.character(hashes[[1]]),
      players = I(list(players)),
      stringsAsFactors = FALSE
    )
  })

  out <- Filter(Negate(is.null), out)
  if (!length(out)) data.frame() else do.call(rbind, out)
}

ot_latest_valid_lineup <- function(lineup_df, game_id, team_id, quarter) {
  states <- ot_valid_lineup_states(
    lineup_df[
      lineup_df$game_id == game_id &
        lineup_df$team_id == team_id &
        lineup_df$quarter == quarter,
      ,
      drop = FALSE
    ]
  )
  if (!nrow(states)) return(integer(0))
  states <- states[order(states$id, states$end_quarter_seconds_remaining), , drop = FALSE]
  ot_sorted_players(states$players[[nrow(states)]])
}

ot_substitution_sets <- function(rows) {
  if (!nrow(rows)) {
    return(list(
      ins = integer(0),
      outs = integer(0),
      sub_ids = integer(0),
      operations = data.frame()
    ))
  }
  subs <- rows[rows$type == "substitution", , drop = FALSE]
  if (!nrow(subs)) {
    return(list(
      ins = integer(0),
      outs = integer(0),
      sub_ids = integer(0),
      operations = data.frame()
    ))
  }
  subs <- subs[order(subs$id), , drop = FALSE]

  in_flag <- if ("parameters_player_in" %in% names(subs)) {
    !is.na(subs$parameters_player_in)
  } else {
    rep(FALSE, nrow(subs))
  }
  out_flag <- if ("parameters_player_out" %in% names(subs)) {
    !is.na(subs$parameters_player_out)
  } else {
    rep(FALSE, nrow(subs))
  }

  operations <- rbind(
    data.frame(
      id = as.integer(subs$id[out_flag]),
      player_id = as.integer(subs$player_id[out_flag]),
      direction = rep("out", sum(out_flag)),
      stringsAsFactors = FALSE
    ),
    data.frame(
      id = as.integer(subs$id[in_flag]),
      player_id = as.integer(subs$player_id[in_flag]),
      direction = rep("in", sum(in_flag)),
      stringsAsFactors = FALSE
    )
  )
  if (nrow(operations)) {
    operations <- operations[order(operations$id, operations$direction == "in"), , drop = FALSE]
  }

  list(
    ins = ot_sorted_players(subs$player_id[in_flag]),
    outs = ot_sorted_players(subs$player_id[out_flag]),
    sub_ids = sort(unique(as.integer(subs$id))),
    operations = operations
  )
}

ot_apply_period_start_reset <- function(current, subs, roster_players) {
  current <- ot_sorted_players(current)
  roster_players <- ot_sorted_players(roster_players)

  if (!length(subs$ins) && !length(subs$outs)) {
    return(list(ok = length(current) == 5L, players = current, reset_type = "carry_forward", reason = ""))
  }

  if (length(setdiff(c(subs$ins, subs$outs), roster_players))) {
    return(list(
      ok = FALSE,
      players = current,
      reset_type = "invalid",
      reason = "period-start substitution references a player outside the game roster"
    ))
  }

  sequential <- current
  if (nrow(subs$operations)) {
    for (i in seq_len(nrow(subs$operations))) {
      player_id <- as.integer(subs$operations$player_id[[i]])
      if (subs$operations$direction[[i]] == "out") {
        sequential <- setdiff(sequential, player_id)
      } else {
        sequential <- unique(c(sequential, player_id))
      }
    }
  }
  sequential <- ot_sorted_players(sequential)
  if (length(sequential) == 5L) {
    reset_type <- if (length(subs$outs) >= 5L) "full_out_in_reset" else "period_start_substitutions"
    return(list(ok = TRUE, players = sequential, reset_type = reset_type, reason = ""))
  }

  if (length(subs$ins) == 5L) {
    return(list(
      ok = TRUE,
      players = ot_sorted_players(subs$ins),
      reset_type = "atomic_five_in_declaration",
      reason = ""
    ))
  }

  list(
    ok = FALSE,
    players = sequential,
    reset_type = "invalid",
    reason = sprintf(
      "period-start reset resolves to %d players (ins=%d, outs=%d)",
      length(sequential), length(subs$ins), length(subs$outs)
    )
  )
}

ot_apply_substitution_group <- function(current, subs, roster_players) {
  current <- ot_sorted_players(current)
  roster_players <- ot_sorted_players(roster_players)

  if (!length(subs$ins) && !length(subs$outs)) {
    return(list(ok = TRUE, players = current, reason = ""))
  }
  if (length(setdiff(c(subs$ins, subs$outs), roster_players))) {
    return(list(
      ok = FALSE,
      players = current,
      reason = "substitution references a player outside the game roster"
    ))
  }
  next_players <- current
  if (nrow(subs$operations)) {
    for (i in seq_len(nrow(subs$operations))) {
      player_id <- as.integer(subs$operations$player_id[[i]])
      direction <- subs$operations$direction[[i]]
      if (direction == "out") {
        if (!player_id %in% next_players) {
          return(list(
            ok = FALSE,
            players = next_players,
            reason = sprintf("outgoing player not on court: %s", player_id)
          ))
        }
        next_players <- setdiff(next_players, player_id)
      } else {
        if (player_id %in% next_players) {
          return(list(
            ok = FALSE,
            players = next_players,
            reason = sprintf("incoming player already on court: %s", player_id)
          ))
        }
        next_players <- unique(c(next_players, player_id))
      }
    }
  }
  next_players <- ot_sorted_players(next_players)
  if (length(next_players) != 5L) {
    return(list(
      ok = FALSE,
      players = next_players,
      reason = sprintf("substitution group resolves to %d players", length(next_players))
    ))
  }

  list(ok = TRUE, players = next_players, reason = "")
}

ot_event_participants <- function(rows, roster_game, team_id) {
  if (!nrow(rows)) return(data.frame())

  participant_types <- c(
    "shot", "freeThrow", "rebound", "assist", "steal", "block",
    "turnover", "foul", "foul-drawn", "deflection"
  )
  event_rows <- rows[rows$type %in% participant_types, , drop = FALSE]
  if (!nrow(event_rows)) return(data.frame())

  coach <- if ("parameters_is_coach_foul" %in% names(event_rows)) {
    ot_truthy(event_rows$parameters_is_coach_foul)
  } else {
    rep(FALSE, nrow(event_rows))
  }
  bench <- if ("parameters_is_bench_foul" %in% names(event_rows)) {
    ot_truthy(event_rows$parameters_is_bench_foul)
  } else {
    rep(FALSE, nrow(event_rows))
  }
  event_rows <- event_rows[!(coach | bench), , drop = FALSE]
  if (!nrow(event_rows)) return(data.frame())

  roster_map <- unique(roster_game[, c("player_id", "team_id"), drop = FALSE])
  roster_map$player_id <- as.integer(roster_map$player_id)
  roster_map$team_id <- as.integer(roster_map$team_id)

  primary <- data.frame(
    event_id = as.integer(event_rows$id),
    player_id = as.integer(event_rows$player_id),
    source = "player_id",
    action_team_id = as.integer(event_rows$team_id),
    stringsAsFactors = FALSE
  )
  primary <- primary[!is.na(primary$player_id) & primary$player_id > 0L, , drop = FALSE]

  fouled <- data.frame()
  if ("parameters_fouled_on" %in% names(event_rows)) {
    fouled <- data.frame(
      event_id = as.integer(event_rows$id),
      player_id = suppressWarnings(as.integer(event_rows$parameters_fouled_on)),
      source = "parameters_fouled_on",
      action_team_id = as.integer(event_rows$team_id),
      stringsAsFactors = FALSE
    )
    fouled <- fouled[!is.na(fouled$player_id) & fouled$player_id > 0L, , drop = FALSE]
  }

  participants <- rbind(primary, fouled)
  if (!nrow(participants)) return(participants)
  participants <- merge(
    participants,
    roster_map,
    by = "player_id",
    all.x = TRUE,
    suffixes = c("", "_roster")
  )

  participants$participant_team_id <- participants$team_id
  primary_unknown <- participants$source == "player_id" &
    is.na(participants$participant_team_id) &
    participants$action_team_id == team_id
  keep <- (
    !is.na(participants$participant_team_id) &
      participants$participant_team_id == team_id
  ) | (
    !is.na(primary_unknown) & primary_unknown
  )
  participants <- participants[
    keep,
    c("event_id", "player_id", "source", "participant_team_id"),
    drop = FALSE
  ]
  unique(participants)
}

ot_make_lineup_state_rows <- function(
  roster_team,
  game_id,
  team_id,
  quarter,
  action_id,
  quarter_seconds,
  players
) {
  players <- ot_sorted_players(players)
  if (length(players) != 5L) {
    stop("Cannot create an OT lineup state without exactly five players.", call. = FALSE)
  }

  roster_team <- roster_team[order(roster_team$player_id), , drop = FALSE]
  lineup_id <- ot_lineup_id(players)
  starters <- if ("starter" %in% names(roster_team)) {
    as.logical(roster_team$starter)
  } else {
    rep(FALSE, nrow(roster_team))
  }
  starters[is.na(starters)] <- FALSE

  data.frame(
    id = as.integer(action_id),
    game_id = as.integer(game_id),
    player_id = as.integer(roster_team$player_id),
    team_id = as.integer(team_id),
    quarter = as.integer(quarter),
    quarter_time = sprintf(
      "%02d:%02d",
      floor(as.numeric(quarter_seconds) / 60),
      round(as.numeric(quarter_seconds) %% 60)
    ),
    end_game_seconds_remaining = as.integer(round(quarter_seconds)),
    end_quarter_seconds_remaining = as.numeric(quarter_seconds),
    is_on_verdict = as.numeric(roster_team$player_id %in% players),
    lineup_id = lineup_id,
    n_on = 5L,
    lineup_hash = ot_lineup_hash(players),
    game_year = as.integer(roster_team$game_year),
    num_starters = sum(roster_team$player_id %in% players & starters),
    stringsAsFactors = FALSE
  )
}

ot_reconstruct_team_period <- function(
  actions_period,
  roster_game,
  normal_lineups,
  previous_players,
  team_id
) {
  game_id <- unique(as.integer(actions_period$game_id))
  quarter <- unique(as.integer(actions_period$quarter))
  if (length(game_id) != 1L || length(quarter) != 1L) {
    stop("actions_period must contain one game and one quarter.", call. = FALSE)
  }

  roster_team <- roster_game[roster_game$team_id == team_id, , drop = FALSE]
  roster_players <- ot_sorted_players(roster_team$player_id)
  previous_players <- ot_sorted_players(previous_players)
  if (length(previous_players) != 5L) {
    return(list(
      ok = FALSE,
      reason = "previous period has no valid five-player finishing lineup",
      status = "rejected_missing_previous_lineup",
      rows = data.frame(),
      warnings = data.frame(),
      unexplained = data.frame(),
      reset_type = NA_character_
    ))
  }
  if (length(setdiff(previous_players, roster_players))) {
    return(list(
      ok = FALSE,
      reason = "previous-period lineup contains a player outside the game roster",
      status = "rejected_invalid_player_count",
      rows = data.frame(),
      warnings = data.frame(),
      unexplained = data.frame(),
      reset_type = NA_character_
    ))
  }

  actions_period <- actions_period[order(actions_period$id), , drop = FALSE]
  actions_period$.clock <- as.numeric(actions_period$end_quarter_seconds_remaining)
  actions_period$.clock_key <- sprintf("%.3f", actions_period$.clock)
  clock_order <- unique(actions_period$.clock_key)
  first_action_id <- min(as.integer(actions_period$id), na.rm = TRUE)

  current <- previous_players
  reconstructed_rows <- list()
  state_players_by_id <- list()
  warnings <- list()
  unexplained <- list()
  reset_type <- "carry_forward"

  for (clock_index in seq_along(clock_order)) {
    clock_key <- clock_order[[clock_index]]
    group_rows <- actions_period[actions_period$.clock_key == clock_key, , drop = FALSE]
    clock_seconds <- as.numeric(group_rows$.clock[[1]])
    before <- current
    team_group_rows <- group_rows[group_rows$team_id == team_id, , drop = FALSE]
    subs <- ot_substitution_sets(team_group_rows)

    is_period_start <- clock_index == 1L && is.finite(clock_seconds) && clock_seconds >= 299
    applied <- if (is_period_start) {
      ot_apply_period_start_reset(before, subs, roster_players)
    } else {
      ot_apply_substitution_group(before, subs, roster_players)
    }
    if (!isTRUE(applied$ok)) {
      return(list(
        ok = FALSE,
        reason = applied$reason,
        status = "rejected_invalid_substitution",
        rows = data.frame(),
        warnings = if (length(warnings)) do.call(rbind, warnings) else data.frame(),
        unexplained = if (length(unexplained)) do.call(rbind, unexplained) else data.frame(),
        reset_type = reset_type
      ))
    }
    current <- ot_sorted_players(applied$players)
    if (is_period_start) reset_type <- applied$reset_type

    participants <- ot_event_participants(group_rows, roster_game, team_id)
    if (nrow(participants)) {
      participants$in_before <- participants$player_id %in% before
      participants$in_after <- participants$player_id %in% current
      participants$game_id <- game_id
      participants$team_id <- as.integer(team_id)
      participants$quarter <- quarter
      participants$clock_seconds <- clock_seconds

      group_unexplained <- participants[
        is.na(participants$participant_team_id) |
          (!participants$in_before & !participants$in_after),
        ,
        drop = FALSE
      ]
      if (nrow(group_unexplained)) {
        unexplained[[length(unexplained) + 1L]] <- group_unexplained
      }

      group_warnings <- participants[
        !is.na(participants$participant_team_id) &
          xor(participants$in_before, participants$in_after),
        ,
        drop = FALSE
      ]
      if (nrow(group_warnings)) {
        warnings[[length(warnings) + 1L]] <- group_warnings
      }
    }

    if (clock_index == 1L) {
      seed_players <- if (is_period_start) current else before
      seed <- ot_make_lineup_state_rows(
        roster_team = roster_team,
        game_id = game_id,
        team_id = team_id,
        quarter = quarter,
        action_id = first_action_id,
        quarter_seconds = 300,
        players = seed_players
      )
      reconstructed_rows[[length(reconstructed_rows) + 1L]] <- seed
      state_players_by_id[[as.character(first_action_id)]] <- seed_players
    }

    if (length(subs$sub_ids)) {
      for (sub_id in subs$sub_ids) {
        state <- ot_make_lineup_state_rows(
          roster_team = roster_team,
          game_id = game_id,
          team_id = team_id,
          quarter = quarter,
          action_id = sub_id,
          quarter_seconds = clock_seconds,
          players = current
        )
        reconstructed_rows[[length(reconstructed_rows) + 1L]] <- state
        state_players_by_id[[as.character(sub_id)]] <- current
      }
    }
  }

  unexplained_df <- if (length(unexplained)) do.call(rbind, unexplained) else data.frame()
  warning_df <- if (length(warnings)) do.call(rbind, warnings) else data.frame()
  if (nrow(unexplained_df)) {
    return(list(
      ok = FALSE,
      reason = sprintf(
        "%d unexplained OT participant event(s): %s",
        nrow(unexplained_df),
        paste(unique(unexplained_df$event_id), collapse = ",")
      ),
      status = "rejected_unexplained_participant",
      rows = data.frame(),
      warnings = warning_df,
      unexplained = unexplained_df,
      reset_type = reset_type
    ))
  }

  provider_states <- ot_valid_lineup_states(normal_lineups)
  if (nrow(provider_states)) {
    provider_states <- provider_states[
      order(
        provider_states$end_quarter_seconds_remaining,
        provider_states$id,
        decreasing = TRUE
      ),
      ,
      drop = FALSE
    ]
    provider_states <- provider_states[
      !duplicated(provider_states$end_quarter_seconds_remaining),
      ,
      drop = FALSE
    ]
    for (i in seq_len(nrow(provider_states))) {
      provider_id <- as.character(provider_states$id[[i]])
      reconstructed <- state_players_by_id[[provider_id]]
      if (!is.null(reconstructed) &&
          !identical(ot_sorted_players(reconstructed), ot_sorted_players(provider_states$players[[i]]))) {
        return(list(
          ok = FALSE,
          reason = sprintf("provider lineup disagrees with reconstruction at action %s", provider_id),
          status = "rejected_provider_disagreement",
          rows = data.frame(),
          warnings = warning_df,
          unexplained = data.frame(),
          reset_type = reset_type
        ))
      }
    }
  }

  rows <- do.call(rbind, reconstructed_rows)
  rows <- rows[
    !duplicated(rows[, c(
      "id", "game_id", "team_id", "quarter",
      "end_game_seconds_remaining", "player_id"
    )]),
    ,
    drop = FALSE
  ]

  list(
    ok = TRUE,
    reason = "",
    status = if (reset_type == "carry_forward") {
      "accepted_carry_forward"
    } else {
      "accepted_period_reset"
    },
    rows = rows,
    warnings = warning_df,
    unexplained = data.frame(),
    reset_type = reset_type
  )
}

detect_ot_leading_lineup_gaps <- function(pws_df) {
  if (is.null(pws_df) || !nrow(pws_df) || !"quarter" %in% names(pws_df)) {
    return(data.frame(game_id = integer(0), quarter = integer(0)))
  }
  ot <- pws_df[pws_df$quarter >= 5, , drop = FALSE]
  if (!nrow(ot)) {
    return(data.frame(game_id = integer(0), quarter = integer(0)))
  }

  periods <- unique(ot[, c("game_id", "quarter"), drop = FALSE])
  gaps <- lapply(seq_len(nrow(periods)), function(i) {
    game_id <- periods$game_id[[i]]
    quarter <- periods$quarter[[i]]
    rows <- ot[ot$game_id == game_id & ot$quarter == quarter, , drop = FALSE]
    gameplay_rows <- if ("type" %in% names(rows)) {
      rows[
        !rows$type %in% c(
          "substitution", "start-of-quarter", "end-of-quarter", "clock",
          "quarter", "game"
        ),
        ,
        drop = FALSE
      ]
    } else {
      rows
    }
    if (!nrow(gameplay_rows)) return(NULL)
    first_id <- min(gameplay_rows$id, na.rm = TRUE)
    first_rows <- gameplay_rows[gameplay_rows$id == first_id, , drop = FALSE]
    required <- c(
      "segment_id", "team_id_defense",
      "lineup_hash_offense", "lineup_hash_defense"
    )
    missing_cols <- setdiff(required, names(first_rows))
    if (length(missing_cols)) {
      stop(
        sprintf("pws_df is missing required columns: %s", paste(missing_cols, collapse = ", ")),
        call. = FALSE
      )
    }
    invalid <- any(
      is.na(first_rows$segment_id) |
        is.na(first_rows$team_id_defense) |
        is.na(first_rows$lineup_hash_offense) |
        is.na(first_rows$lineup_hash_defense)
    )
    if (!invalid) return(NULL)
    data.frame(game_id = as.integer(game_id), quarter = as.integer(quarter))
  })

  gaps <- Filter(Negate(is.null), gaps)
  if (!length(gaps)) {
    data.frame(game_id = integer(0), quarter = integer(0))
  } else {
    unique(do.call(rbind, gaps))
  }
}

recover_ot_lineup_periods <- function(
  actions_df,
  roster_df,
  lineup_df,
  periods
) {
  if (is.null(periods) || !nrow(periods)) {
    return(list(lineups = lineup_df, audit = data.frame(), replacement_rows = data.frame()))
  }

  final_lineups <- lineup_df
  audit <- list()
  replacements <- list()
  periods <- periods[order(periods$game_id, periods$quarter), , drop = FALSE]

  for (period_index in seq_len(nrow(periods))) {
    game_id <- as.integer(periods$game_id[[period_index]])
    quarter <- as.integer(periods$quarter[[period_index]])
    actions_period <- actions_df[
      actions_df$game_id == game_id & actions_df$quarter == quarter,
      ,
      drop = FALSE
    ]
    roster_game <- roster_df[roster_df$game_id == game_id, , drop = FALSE]
    teams <- sort(unique(as.integer(roster_game$team_id)))

    if (!nrow(actions_period) || length(teams) != 2L) {
      reason <- if (!nrow(actions_period)) {
        "OT period has no cleaned actions"
      } else {
        sprintf("expected two roster teams, found %d", length(teams))
      }
      audit[[length(audit) + 1L]] <- data.frame(
        game_id = game_id,
        team_id = NA_integer_,
        quarter = quarter,
        recovery_required = TRUE,
        recovery_status = "rejected_invalid_period",
        source_previous_quarter = quarter - 1L,
        seed_lineup_hash = NA_character_,
        resolved_lineup_hash = NA_character_,
        period_start_reset_type = NA_character_,
        recovered_action_rows = 0L,
        ordering_warning_count = 0L,
        unexplained_event_count = 0L,
        unexplained_event_ids = NA_character_,
        reason = reason,
        stringsAsFactors = FALSE
      )
      next
    }

    team_results <- vector("list", length(teams))
    names(team_results) <- as.character(teams)
    for (team_id in teams) {
      previous_players <- ot_latest_valid_lineup(
        final_lineups,
        game_id = game_id,
        team_id = team_id,
        quarter = quarter - 1L
      )
      normal_period <- final_lineups[
        final_lineups$game_id == game_id &
          final_lineups$team_id == team_id &
          final_lineups$quarter == quarter,
        ,
        drop = FALSE
      ]
      result <- ot_reconstruct_team_period(
        actions_period = actions_period,
        roster_game = roster_game,
        normal_lineups = normal_period,
        previous_players = previous_players,
        team_id = team_id
      )
      team_results[[as.character(team_id)]] <- result

      resolved_players <- if (isTRUE(result$ok) && nrow(result$rows)) {
        ot_latest_valid_lineup(result$rows, game_id, team_id, quarter)
      } else {
        integer(0)
      }
      unexplained_ids <- if (nrow(result$unexplained)) {
        paste(sort(unique(result$unexplained$event_id)), collapse = ",")
      } else {
        NA_character_
      }
      audit[[length(audit) + 1L]] <- data.frame(
        game_id = game_id,
        team_id = team_id,
        quarter = quarter,
        recovery_required = TRUE,
        recovery_status = result$status,
        source_previous_quarter = quarter - 1L,
        seed_lineup_hash = if (length(previous_players) == 5L) ot_lineup_hash(previous_players) else NA_character_,
        resolved_lineup_hash = if (length(resolved_players) == 5L) ot_lineup_hash(resolved_players) else NA_character_,
        period_start_reset_type = result$reset_type,
        recovered_action_rows = if (isTRUE(result$ok)) length(unique(result$rows$id)) else 0L,
        ordering_warning_count = nrow(result$warnings),
        unexplained_event_count = nrow(result$unexplained),
        unexplained_event_ids = unexplained_ids,
        reason = result$reason,
        stringsAsFactors = FALSE
      )
    }

    period_ok <- all(vapply(team_results, function(x) isTRUE(x$ok), logical(1)))
    if (!period_ok) next

    period_rows <- do.call(rbind, lapply(team_results, `[[`, "rows"))
    final_lineups <- final_lineups[
      !(final_lineups$game_id == game_id & final_lineups$quarter == quarter),
      ,
      drop = FALSE
    ]
    final_lineups <- rbind(final_lineups, period_rows)
    replacements[[length(replacements) + 1L]] <- period_rows
  }

  audit_df <- if (length(audit)) do.call(rbind, audit) else data.frame()
  replacement_df <- if (length(replacements)) do.call(rbind, replacements) else data.frame()
  list(lineups = final_lineups, audit = audit_df, replacement_rows = replacement_df)
}
