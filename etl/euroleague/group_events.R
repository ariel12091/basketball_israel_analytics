# Provider-specific EuroLeague play-by-play grouping.
#
# This module is intentionally pure: it does not read from or write to the
# database. It accepts package-shaped or normalized play-by-play rows and adds
# synthetic event relationships used by the canonical possession pipeline.

.el_first_column <- function(data, candidates, required = TRUE) {
  found <- candidates[candidates %in% names(data)]
  if (length(found) > 0L) {
    return(data[[found[[1L]]]])
  }
  if (required) {
    stop(
      sprintf("Missing required EuroLeague column: %s", candidates[[1L]]),
      call. = FALSE
    )
  }
  rep(NA, nrow(data))
}

normalize_euroleague_events <- function(events) {
  stopifnot(is.data.frame(events))

  result <- events
  result$season <- as.integer(.el_first_column(events, c("season", "Season")))
  result$gamecode <- as.integer(.el_first_column(events, c("gamecode", "Gamecode")))
  result$source_event_order <- as.integer(.el_first_column(
    events,
    c("source_event_order", "TRUE_NUMBEROFPLAY")
  ))
  result$provider_number_of_play <- as.integer(.el_first_column(
    events,
    c("provider_number_of_play", "NUMBEROFPLAY"),
    required = FALSE
  ))
  result$period <- as.integer(.el_first_column(events, c("period", "PERIOD")))
  result$clock <- as.character(.el_first_column(
    events,
    c("clock", "MARKERTIME"),
    required = FALSE
  ))
  result$team_code <- trimws(as.character(.el_first_column(
    events,
    c("team_code", "CODETEAM"),
    required = FALSE
  )))
  result$play_type <- toupper(trimws(as.character(.el_first_column(
    events,
    c("play_type", "PLAYTYPE")
  ))))
  result$player_id <- trimws(as.character(.el_first_column(
    events,
    c("player_id", "PLAYER_ID"),
    required = FALSE
  )))
  result$player_name <- as.character(.el_first_column(
    events,
    c("player_name", "PLAYER"),
    required = FALSE
  ))
  result$play_info <- as.character(.el_first_column(
    events,
    c("play_info", "PLAYINFO"),
    required = FALSE
  ))
  result$score_a <- suppressWarnings(as.numeric(.el_first_column(
    events,
    c("score_a", "POINTS_A"),
    required = FALSE
  )))
  result$score_b <- suppressWarnings(as.numeric(.el_first_column(
    events,
    c("score_b", "POINTS_B"),
    required = FALSE
  )))

  result$clock[result$clock %in% c("", "NA")] <- NA_character_
  result$team_code[result$team_code %in% c("", "NA")] <- NA_character_
  result$player_id[result$player_id %in% c("", "NA")] <- NA_character_
  result
}

.el_after <- function(index, n) {
  if (index >= n) integer() else seq.int(index + 1L, n)
}

.el_before <- function(index) {
  if (index <= 1L) integer() else seq.int(index - 1L, 1L)
}

.el_other_team <- function(teams, team) {
  candidates <- setdiff(teams, team)
  if (length(candidates) == 1L) candidates[[1L]] else NA_character_
}

.group_euroleague_period <- function(period_events) {
  x <- period_events[order(period_events$source_event_order), , drop = FALSE]
  n <- nrow(x)
  type <- x$play_type
  team <- x$team_code
  player <- x$player_id
  clock <- x$clock
  order_id <- x$source_event_order

  made_shots <- c("2FGM", "3FGM")
  missed_shots <- c("2FGA", "3FGA")
  shots <- c(made_shots, missed_shots)
  rebounds <- c("D", "O")
  committed_fouls <- c("CM", "CMT", "CMU", "B", "C", "CMTI", "OF")
  special_fouls <- c("CMT", "CMU", "B", "C", "CMTI")
  ft_types <- c("FTA", "FTM")
  administrative <- c(
    "IN", "OUT", "CCH", "TOUT", "TOUT_TV", "TIMEOUT", "TV", "BP"
  )
  period_end <- c("EP", "EG")
  live_boundaries <- c(shots, rebounds, "TO", "OF")

  parent <- order_id
  ft_trip <- rep(NA_character_, n)
  assignment_status <- rep("confirmed", n)
  assignment_confidence <- rep(100, n)
  final_end_poss <- rep(FALSE, n)
  end_reason <- rep(NA_character_, n)

  # Shot children: assists, block annotations, and the resolving rebound.
  shot_rows <- which(type %in% shots)
  for (i in shot_rows) {
    root <- order_id[[i]]

    if (type[[i]] %in% made_shots) {
      for (j in .el_after(i, n)) {
        if (type[[j]] == "AS" && identical(team[[j]], team[[i]])) {
          parent[[j]] <- root
          next
        }
        if (type[[j]] %in% c(administrative, "AS")) next
        if (type[[j]] %in% c("CM", "RV", ft_types)) next
        break
      }

      window <- integer()
      for (j in .el_after(i, n)) {
        # An intervening FT belongs to an already-open penalty and closes the
        # search for a new and-one foul after this basket. Substitutions may
        # occur between the actual and-one foul and its FT, so they remain
        # transparent.
        if (type[[j]] %in% c(live_boundaries, ft_types, period_end)) break
        window <- c(window, j)
      }
      rv <- window[
        type[window] == "RV" &
          !is.na(team[window]) & team[window] == team[[i]] &
          !is.na(player[window]) & player[window] == player[[i]]
      ]
      rv_same_team <- window[
        type[window] == "RV" &
          !is.na(team[window]) & team[window] == team[[i]]
      ]
      cm <- window[
        type[window] == "CM" &
          !is.na(team[window]) & team[window] != team[[i]]
      ]
      if (length(rv) > 0L && length(cm) > 0L) {
        parent[c(rv[[1L]], cm[[1L]])] <- root
      } else if (length(rv_same_team) > 0L && length(cm) > 0L) {
        # A dead-ball foul on a teammate after a made basket can produce an FT
        # trip before the opponent ever gains live-ball control. Treat the
        # basket and that trip as one extended possession, while requiring the
        # FT shooter to match the fouled player to avoid merging retained-ball
        # sequences that have no FT penalty.
        fouled_row <- rv_same_team[[1L]]
        ft_match <- FALSE
        for (j in .el_after(max(fouled_row, cm[[1L]]), n)) {
          if (type[[j]] %in% live_boundaries || type[[j]] %in% period_end) break
          if (type[[j]] %in% ft_types &&
              identical(team[[j]], team[[i]]) &&
              identical(player[[j]], player[[fouled_row]])) {
            ft_match <- TRUE
            break
          }
        }
        if (ft_match) parent[c(fouled_row, cm[[1L]])] <- root
      }
    } else {
      for (j in .el_after(i, n)) {
        if (type[[j]] %in% c("AG", "FV")) {
          parent[[j]] <- root
          next
        }
        if (type[[j]] %in% administrative) next
        if (type[[j]] %in% rebounds) {
          parent[[j]] <- root
        }
        break
      }
    }
  }

  # Offensive-foul bundles and ordinary turnover/steal bundles.
  for (i in which(type == "CMU")) {
    root <- order_id[[i]]
    for (j in .el_after(i, n)) {
      if (type[[j]] == "TO" && identical(team[[j]], team[[i]])) {
        parent[[j]] <- root
        next
      }
      if (type[[j]] == "RV" && !identical(team[[j]], team[[i]])) {
        parent[[j]] <- root
        next
      }
      if (type[[j]] %in% administrative) next
      break
    }
  }

  for (i in which(type == "OF")) {
    root <- order_id[[i]]
    for (j in .el_after(i, n)) {
      if (type[[j]] == "TO" && identical(team[[j]], team[[i]])) {
        parent[[j]] <- root
        next
      }
      if (type[[j]] %in% c("RV", "ST") &&
          !identical(team[[j]], team[[i]])) {
        parent[[j]] <- root
        next
      }
      if (type[[j]] %in% administrative) next
      break
    }
  }

  for (i in which(type == "TO")) {
    if (parent[[i]] != order_id[[i]]) next
    for (j in .el_after(i, n)) {
      if (type[[j]] == "ST" && !identical(team[[j]], team[[i]])) {
        parent[[j]] <- order_id[[i]]
      }
      if (!type[[j]] %in% administrative) break
    }
  }

  # Pair standalone committed/drawn foul rows. Fouls already attached to a
  # made shot remain children of that shot.
  for (i in which(type %in% setdiff(committed_fouls, "OF"))) {
    if (parent[[i]] != order_id[[i]]) next
    paired <- FALSE

    for (j in .el_after(i, n)) {
      if (type[[j]] == "RV" && parent[[j]] == order_id[[j]] &&
          !identical(team[[j]], team[[i]])) {
        parent[[j]] <- order_id[[i]]
        paired <- TRUE
        break
      }
      if (type[[j]] %in% administrative) next
      if (type[[j]] %in% c(committed_fouls, shots, rebounds, "TO", ft_types,
                           period_end)) break
    }

    if (!paired) {
      for (j in .el_before(i)) {
        if (type[[j]] %in% live_boundaries) break
        if (type[[j]] == "RV" && parent[[j]] == order_id[[j]] &&
            !identical(team[[j]], team[[i]]) &&
            (is.na(clock[[i]]) || is.na(clock[[j]]) ||
             identical(clock[[i]], clock[[j]]))) {
          parent[[j]] <- order_id[[i]]
          break
        }
      }
    }
  }

  game_teams <- unique(team[!is.na(team)])

  # Opposing player technicals recorded together are offsetting annotations.
  # They do not supersede an ordinary personal-foul trip awarded in the same
  # stoppage.
  offsetting_technical <- rep(FALSE, n)
  technical_rows <- which(type == "CMT")
  if (length(technical_rows) > 1L) {
    technical_clock_groups <- split(
      technical_rows,
      ifelse(is.na(clock[technical_rows]),
             paste0("order_", order_id[technical_rows]),
             clock[technical_rows])
    )
    for (rows in technical_clock_groups) {
      technical_teams <- unique(team[rows][!is.na(team[rows])])
      if (length(rows) == 2L && length(technical_teams) == 2L) {
        offsetting_technical[rows] <- TRUE
      }
    }
  }

  # Assign each FT to the best compatible open foul. Clock equality is only a
  # weak score bonus; team, shooter, and source order control the match.
  ft_rows <- which(type %in% ft_types)
  for (i in ft_rows) {
    # Rebounds do not close a foul penalty that was already called, and a TO
    # attached to an unsportsmanlike/offensive-foul incident is an annotation
    # rather than a new search boundary.
    ft_search_boundary <- type %in% c(shots, "TO", "OF") &
      !(type == "TO" & parent != order_id)
    previous_live <- which(seq_len(n) < i & ft_search_boundary)
    lower <- if (length(previous_live) == 0L) 0L else max(previous_live)
    candidates <- which(
      seq_len(n) > lower & seq_len(n) < i &
        type %in% setdiff(committed_fouls, "OF")
    )

    best_root <- NA_integer_
    best_score <- -Inf
    best_exact_player <- FALSE
    if (length(candidates) > 0L) {
      for (candidate in candidates) {
        if (offsetting_technical[[candidate]]) next
        root <- parent[[candidate]]
        group_rows <- which(parent == root)
        rv_rows <- group_rows[type[group_rows] == "RV"]
        benefit_team <- if (length(rv_rows) > 0L) {
          team[[rv_rows[[1L]]]]
        } else {
          .el_other_team(game_teams, team[[candidate]])
        }
        benefit_player <- if (length(rv_rows) > 0L) {
          player[[rv_rows[[1L]]]]
        } else {
          NA_character_
        }

        if (is.na(benefit_team) || is.na(team[[i]]) ||
            benefit_team != team[[i]]) next

        # One foul parent creates one FT trip. It can continue through multiple
        # attempts by the same shooter, but cannot absorb a later trip by a
        # different shooter in the same dead-ball sequence.
        prior_root_fts <- which(
          seq_len(n) < i & type %in% ft_types & parent == root
        )
        if (length(prior_root_fts) > 0L) {
          last_root_ft <- max(prior_root_fts)
          prior_shooter <- player[[last_root_ft]]
          prior_team <- team[[last_root_ft]]
          if (!identical(prior_shooter, player[[i]]) ||
              !identical(prior_team, team[[i]])) next
          intervening_foul <- which(
            seq_len(n) > last_root_ft & seq_len(n) < i &
              type %in% committed_fouls & parent != root
          )
          if (length(intervening_foul) > 0L) next
        }

        exact_player <- !is.na(benefit_player) && !is.na(player[[i]]) &&
          benefit_player == player[[i]]
        score <- 1000 - (i - candidate)
        if (exact_player) score <- score + 500
        # When a technical/special foul and a non-shooting personal foul award
        # the same team, the immediately pending special penalty is the source
        # of the next FT. Its retained-possession semantics are material.
        if (type[[candidate]] %in% special_fouls) score <- score + 600
        if (!is.na(clock[[i]]) && !is.na(clock[[candidate]]) &&
            clock[[i]] == clock[[candidate]]) score <- score + 10

        if (score > best_score) {
          best_score <- score
          best_root <- root
          best_exact_player <- exact_player
        }
      }
    }

    if (!is.na(best_root)) {
      parent[[i]] <- best_root
      assignment_confidence[[i]] <- if (best_exact_player) 99 else 95
    } else {
      prior_fts <- ft_rows[ft_rows < i]
      prior_ft <- if (length(prior_fts) == 0L) NA_integer_ else max(prior_fts)
      crossed_live <- if (is.na(prior_ft)) TRUE else any(
        type[seq.int(prior_ft + 1L, i - 1L)] %in% live_boundaries
      )
      can_continue <- !is.na(prior_ft) && !crossed_live &&
        identical(team[[prior_ft]], team[[i]]) &&
        identical(player[[prior_ft]], player[[i]])
      if (can_continue) {
        parent[[i]] <- parent[[prior_ft]]
      } else {
        assignment_status[[i]] <- "unresolved"
        assignment_confidence[[i]] <- 0
      }
    }
  }

  # Split FT trips by parent, team, shooter, and intervening live-ball control.
  trip_number <- 0L
  previous_ft <- NA_integer_
  for (i in ft_rows) {
    same_trip <- FALSE
    if (!is.na(previous_ft)) {
      between <- if (i - previous_ft <= 1L) integer() else
        seq.int(previous_ft + 1L, i - 1L)
      same_trip <- parent[[i]] == parent[[previous_ft]] &&
        identical(team[[i]], team[[previous_ft]]) &&
        identical(player[[i]], player[[previous_ft]]) &&
        !any(type[between] %in% live_boundaries)
    }
    if (!same_trip) trip_number <- trip_number + 1L
    ft_trip[[i]] <- sprintf(
      "EL:%d:%d:%d:%d:%d",
      x$season[[i]], x$gamecode[[i]], x$period[[i]], parent[[i]], trip_number
    )
    previous_ft <- i
  }

  # A rebound after a missed final FT belongs to the foul/FT incident.
  for (trip in unique(stats::na.omit(ft_trip))) {
    rows <- which(ft_trip == trip)
    final <- max(rows)
    if (type[[final]] != "FTA") next
    for (j in .el_after(final, n)) {
      if (type[[j]] %in% c(administrative, "CM", "RV")) next
      if (type[[j]] %in% rebounds) {
        if (parent[[j]] == order_id[[j]]) parent[[j]] <- parent[[final]]
        next
      }
      break
    }
  }

  set_endpoint <- function(index, reason) {
    root <- parent[[index]]
    existing <- which(parent == root & final_end_poss)
    if (length(existing) == 0L) {
      final_end_poss[[index]] <<- TRUE
      end_reason[[index]] <<- reason
    }
  }

  for (i in which(type == "TO")) {
    set_endpoint(i, "turnover")
  }

  for (i in which(type %in% made_shots)) {
    if (!any(type == "FTM" & parent == parent[[i]]) &&
        !any(type == "FTA" & parent == parent[[i]])) {
      set_endpoint(i, "made_field_goal")
    }
  }

  for (i in which(type %in% missed_shots)) {
    has_block <- FALSE
    outcome <- NA_character_
    rebound_team <- NA_character_
    rebound_type <- NA_character_
    for (j in .el_after(i, n)) {
      if (type[[j]] %in% c("AG", "FV")) {
        has_block <- TRUE
        next
      }
      # Loose-ball foul annotations can be recorded between a miss and the
      # rebound that establishes control. They do not erase the shot outcome;
      # an ensuing FT still stops this scan and resolves the possession later.
      if (type[[j]] %in% c(administrative, "CM", "RV")) next
      if (type[[j]] %in% rebounds) {
        rebound_team <- team[[j]]
        rebound_type <- type[[j]]
        next
      } else if (type[[j]] %in% period_end) {
        outcome <- "PERIOD_END"
      } else if (type[[j]] %in% live_boundaries &&
                 !identical(team[[j]], team[[i]])) {
        outcome <- "OPPONENT_CONTROL"
      }
      break
    }

    if (!is.na(rebound_type)) {
      outcome <- if (!is.na(rebound_team) && !is.na(team[[i]])) {
        if (rebound_team == team[[i]]) "O" else "D"
      } else {
        rebound_type
      }
    }

    if (identical(outcome, "D") || identical(outcome, "OPPONENT_CONTROL")) {
      set_endpoint(
        i,
        if (has_block) "blocked_shot_defensive_rebound" else
          "miss_defensive_rebound"
      )
    } else if (identical(outcome, "PERIOD_END")) {
      set_endpoint(
        i,
        if (has_block) "period_end_blocked_miss" else "period_end_miss"
      )
    }
  }

  # An offensive rebound normally continues the possession, but the period
  # boundary closes it when no later action can resolve the retained control.
  for (i in which(type %in% period_end)) {
    for (j in .el_before(i)) {
      if (type[[j]] %in% c(administrative, "AG", "FV")) next
      if (type[[j]] == "O") {
        set_endpoint(j, "period_end_offensive_rebound")
      }
      break
    }
  }

  # Resolve ordinary and and-one FT trips. Special penalties retain the
  # entitled inbound and therefore do not independently end a possession.
  for (trip in unique(stats::na.omit(ft_trip))) {
    rows <- which(ft_trip == trip)
    final <- max(rows)
    root <- parent[[final]]
    group_rows <- which(parent == root)
    group_types <- type[group_rows]
    is_and_one <- any(group_types %in% made_shots)
    is_special <- any(group_types %in% special_fouls)
    if (is_special) next

    if (type[[final]] == "FTM") {
      between <- seq.int(min(group_rows), final)
      is_compound <- any(type[between] %in% special_fouls & parent[between] != root)
      reason <- if (is_and_one) {
        made_row <- group_rows[type[group_rows] %in% made_shots][[1L]]
        if (identical(player[[made_row]], player[[final]])) {
          "and_one_final_ft"
        } else {
          "made_basket_dead_ball_ft"
        }
      } else if (is_compound) {
        earlier <- between[between < final]
        earlier_same_team_endpoint <- any(
          final_end_poss[earlier] &
            !is.na(team[earlier]) &
            !is.na(team[[final]]) &
            team[earlier] == team[[final]]
        )
        if (earlier_same_team_endpoint) NA_character_ else
          "compound_penalty_offense_resolved"
      } else {
        "ordinary_ft_trip_final_make"
      }
      if (!is.na(reason)) set_endpoint(final, reason)
    } else {
      outcome <- NA_character_
      rebound_team <- NA_character_
      rebound_type <- NA_character_
      for (j in .el_after(final, n)) {
        if (type[[j]] %in% c(administrative, "CM", "RV")) next
        if (type[[j]] %in% rebounds) {
          rebound_team <- team[[j]]
          rebound_type <- type[[j]]
          next
        } else if (type[[j]] %in% period_end) {
          outcome <- "PERIOD_END"
        } else if (type[[j]] %in% live_boundaries &&
                   !identical(team[[j]], team[[final]])) {
          outcome <- "OPPONENT_CONTROL"
        }
        break
      }
      if (!is.na(rebound_type)) {
        outcome <- if (!is.na(rebound_team) && !is.na(team[[final]])) {
          if (rebound_team == team[[final]]) "O" else "D"
        } else {
          rebound_type
        }
      }
      if (outcome %in% c("D", "PERIOD_END", "OPPONENT_CONTROL")) {
        set_endpoint(
          final,
          if (identical(outcome, "D"))
            "final_ft_miss_defensive_rebound" else "final_ft_miss_end"
        )
      }
    }
  }

  # Compound personal + special penalties require entitlement inference even
  # when their individual FT-to-parent links are deterministic. Define the
  # incident by live-play boundaries, not an arbitrary number of provider rows:
  # substitutions and annotations are transparent, while a shot, rebound,
  # turnover, offensive foul, or period end closes the penalty cluster.
  special_rows <- which(type %in% special_fouls)
  if (length(special_rows) > 0L) {
    for (i in special_rows) {
      cluster <- i
      for (j in .el_before(i)) {
        if (type[[j]] %in% c(live_boundaries, period_end)) break
        cluster <- c(cluster, j)
      }
      for (j in .el_after(i, n)) {
        if (type[[j]] %in% c(live_boundaries, period_end)) break
        cluster <- c(cluster, j)
      }
      nearby <- cluster[type[cluster] %in% c("CM", ft_types)]
      assignment_status[nearby] <- ifelse(
        assignment_status[nearby] == "unresolved",
        "unresolved",
        "provisional"
      )
      assignment_confidence[nearby] <- pmin(assignment_confidence[nearby], 90)
    }
  }

  x$synthetic_parent_order <- as.integer(parent)
  x$synthetic_ft_trip_id <- ft_trip
  x$final_end_poss <- final_end_poss
  x$end_reason <- end_reason
  x$grouping_status <- assignment_status
  x$grouping_confidence_pct <- as.numeric(assignment_confidence)
  x
}

group_euroleague_events <- function(events) {
  normalized <- normalize_euroleague_events(events)
  normalized$.input_row <- seq_len(nrow(normalized))
  normalized <- normalized[order(
    normalized$season,
    normalized$gamecode,
    normalized$period,
    normalized$source_event_order
  ), , drop = FALSE]

  group_key <- interaction(
    normalized$season,
    normalized$gamecode,
    normalized$period,
    drop = TRUE,
    lex.order = TRUE
  )
  grouped <- lapply(split(normalized, group_key), .group_euroleague_period)
  result <- do.call(rbind, grouped)
  result <- result[order(result$.input_row), , drop = FALSE]
  result$.input_row <- NULL
  rownames(result) <- NULL
  result
}

.el_safe_rate <- function(numerator, denominator) {
  if (denominator == 0) NA_real_ else numerator / denominator
}

evaluate_euroleague_grouping <- function(predicted, labelled) {
  key <- function(x) paste(
    x$season, x$gamecode, x$source_event_order, sep = ":"
  )
  match_rows <- match(key(labelled), key(predicted))
  if (anyNA(match_rows)) {
    stop("Predictions do not contain every labelled event", call. = FALSE)
  }
  p <- predicted[match_rows, , drop = FALSE]
  truth_end <- as.logical(labelled$expected_possession_end)
  pred_end <- as.logical(p$final_end_poss)
  ft <- labelled$play_type %in% c("FTA", "FTM")
  child <- labelled$expected_parent_order != labelled$source_event_order
  tp <- sum(pred_end & truth_end)
  fp <- sum(pred_end & !truth_end)
  fn <- sum(!pred_end & truth_end)
  precision <- .el_safe_rate(tp, tp + fp)
  recall <- .el_safe_rate(tp, tp + fn)

  ft_pairs_correct <- NA_real_
  if (sum(ft) >= 2L) {
    pair <- utils::combn(which(ft), 2L)
    truth_same <- labelled$expected_ft_trip_id[pair[1L, ]] ==
      labelled$expected_ft_trip_id[pair[2L, ]]
    pred_same <- p$synthetic_ft_trip_id[pair[1L, ]] ==
      p$synthetic_ft_trip_id[pair[2L, ]]
    ft_pairs_correct <- mean(truth_same == pred_same)
  }

  data.frame(
    labelled_events = nrow(labelled),
    labelled_ft_rows = sum(ft),
    parent_accuracy = mean(
      p$synthetic_parent_order == labelled$expected_parent_order
    ),
    child_parent_accuracy = mean(
      p$synthetic_parent_order[child] == labelled$expected_parent_order[child]
    ),
    ft_parent_accuracy = mean(
      p$synthetic_parent_order[ft] == labelled$expected_parent_order[ft]
    ),
    ft_trip_partition_accuracy = ft_pairs_correct,
    ft_resolved_rate = mean(p$grouping_status[ft] != "unresolved"),
    endpoint_accuracy = mean(pred_end == truth_end),
    endpoint_precision = precision,
    endpoint_recall = recall,
    endpoint_f1 = if (is.na(precision) || is.na(recall) ||
                      precision + recall == 0) {
      NA_real_
    } else {
      2 * precision * recall / (precision + recall)
    },
    endpoint_reason_accuracy = mean(
      p$end_reason[truth_end] == labelled$expected_end_reason[truth_end]
    ),
    stringsAsFactors = FALSE
  )
}
