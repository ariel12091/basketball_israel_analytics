# Deterministic EuroLeague possession counting.
#
# This layer converts grouped play-by-play endpoints into auditable possession
# rows, team/game totals, and structural QA signals. It performs no database IO.

if (!exists("group_euroleague_events", mode = "function")) {
  group_script_candidates <- c(
    file.path("etl", "euroleague", "group_events.R"),
    "group_events.R",
    file.path("..", "euroleague", "group_events.R")
  )
  group_script <- group_script_candidates[file.exists(group_script_candidates)]
  if (length(group_script) == 0L) {
    stop("Cannot locate etl/euroleague/group_events.R", call. = FALSE)
  }
  source(group_script[[1L]])
}

.el_sequence_within <- function(data, columns) {
  key <- interaction(data[columns], drop = TRUE, lex.order = TRUE)
  as.integer(ave(seq_len(nrow(data)), key, FUN = seq_along))
}

.el_count_same_team_transitions <- function(possessions) {
  if (nrow(possessions) == 0L) return(integer())
  groups <- split(
    possessions,
    interaction(
      possessions$season,
      possessions$gamecode,
      possessions$period,
      drop = TRUE,
      lex.order = TRUE
    )
  )
  counts <- vapply(groups, function(events) {
    if (nrow(events) < 2L) return(0L)
    as.integer(sum(
      events$offense_team[-1L] == events$offense_team[-nrow(events)]
    ))
  }, integer(1L))

  keys <- do.call(rbind, lapply(groups, function(events) {
    events[1L, c("season", "gamecode"), drop = FALSE]
  }))
  aggregate(
    counts,
    by = list(season = keys$season, gamecode = keys$gamecode),
    FUN = sum
  )
}

count_euroleague_possessions <- function(events) {
  # Always recompute grouping from raw columns. This prevents a caller from
  # passing stale derived fields and makes the result a pure function of input.
  grouped <- group_euroleague_events(events)
  grouped <- grouped[order(
    grouped$season,
    grouped$gamecode,
    grouped$period,
    grouped$source_event_order
  ), , drop = FALSE]
  rownames(grouped) <- NULL

  endpoint_rows <- which(grouped$final_end_poss)
  possessions <- grouped[endpoint_rows, c(
    "season", "gamecode", "period", "clock", "source_event_order",
    "team_code", "play_type", "player_id", "synthetic_parent_order",
    "synthetic_ft_trip_id", "end_reason", "grouping_status",
    "grouping_confidence_pct"
  ), drop = FALSE]
  names(possessions)[names(possessions) == "team_code"] <- "offense_team"

  if (nrow(possessions) > 0L) {
    possessions$game_possession_number <- .el_sequence_within(
      possessions,
      c("season", "gamecode")
    )
    possessions$team_possession_number <- .el_sequence_within(
      possessions,
      c("season", "gamecode", "offense_team")
    )
  } else {
    possessions$game_possession_number <- integer()
    possessions$team_possession_number <- integer()
  }

  grouped$game_possession_number <- NA_integer_
  grouped$team_possession_number <- NA_integer_
  if (length(endpoint_rows) > 0L) {
    grouped$game_possession_number[endpoint_rows] <-
      possessions$game_possession_number
    grouped$team_possession_number[endpoint_rows] <-
      possessions$team_possession_number
  }

  teams <- unique(grouped[
    !is.na(grouped$team_code),
    c("season", "gamecode", "team_code"),
    drop = FALSE
  ])
  names(teams)[names(teams) == "team_code"] <- "offense_team"

  if (nrow(possessions) > 0L) {
    team_totals <- aggregate(
      rep(1L, nrow(possessions)),
      by = possessions[c("season", "gamecode", "offense_team")],
      FUN = sum
    )
    names(team_totals)[names(team_totals) == "x"] <- "possessions"

    provisional <- aggregate(
      as.integer(possessions$grouping_status != "confirmed"),
      by = possessions[c("season", "gamecode", "offense_team")],
      FUN = sum
    )
    names(provisional)[names(provisional) == "x"] <-
      "provisional_possessions"
    team_totals <- merge(
      teams,
      merge(
        team_totals,
        provisional,
        by = c("season", "gamecode", "offense_team"),
        all = TRUE,
        sort = FALSE
      ),
      by = c("season", "gamecode", "offense_team"),
      all.x = TRUE,
      sort = FALSE
    )
    team_totals$possessions[is.na(team_totals$possessions)] <- 0L
    team_totals$provisional_possessions[
      is.na(team_totals$provisional_possessions)
    ] <- 0L

    reason_totals <- aggregate(
      rep(1L, nrow(possessions)),
      by = possessions[c(
        "season", "gamecode", "offense_team", "end_reason"
      )],
      FUN = sum
    )
    names(reason_totals)[names(reason_totals) == "x"] <- "possessions"
  } else {
    team_totals <- transform(
      teams,
      possessions = 0L,
      provisional_possessions = 0L
    )
    reason_totals <- data.frame(
      season = integer(), gamecode = integer(), offense_team = character(),
      end_reason = character(), possessions = integer(),
      stringsAsFactors = FALSE
    )
  }

  team_totals <- team_totals[order(
    team_totals$season,
    team_totals$gamecode,
    team_totals$offense_team
  ), , drop = FALSE]
  rownames(team_totals) <- NULL

  game_base <- unique(grouped[c("season", "gamecode")])
  game_qa <- do.call(rbind, lapply(seq_len(nrow(game_base)), function(i) {
    season_value <- game_base$season[[i]]
    game_value <- game_base$gamecode[[i]]
    game_rows <- grouped$season == season_value &
      grouped$gamecode == game_value
    game_teams <- team_totals[
      team_totals$season == season_value &
        team_totals$gamecode == game_value,
      , drop = FALSE
    ]
    game_possessions <- possessions[
      possessions$season == season_value &
        possessions$gamecode == game_value,
      , drop = FALSE
    ]

    incident_counts <- aggregate(
      final_end_poss ~ period + synthetic_parent_order,
      grouped[game_rows, , drop = FALSE],
      sum
    )
    source_key <- paste(
      grouped$period[game_rows],
      grouped$source_event_order[game_rows],
      sep = ":"
    )
    parent_key <- paste(
      grouped$period[game_rows],
      grouped$synthetic_parent_order[game_rows],
      sep = ":"
    )
    ft_rows <- game_rows & grouped$play_type %in% c("FTA", "FTM")

    same_team <- 0L
    if (nrow(game_possessions) > 0L) {
      period_groups <- split(game_possessions, game_possessions$period)
      same_team <- sum(vapply(period_groups, function(period_events) {
        if (nrow(period_events) < 2L) return(0L)
        as.integer(sum(
          period_events$offense_team[-1L] ==
            period_events$offense_team[-nrow(period_events)]
        ))
      }, integer(1L)))
    }

    possession_difference <- if (nrow(game_teams) < 2L) {
      NA_integer_
    } else {
      as.integer(max(game_teams$possessions) - min(game_teams$possessions))
    }
    unresolved_ft <- sum(
      grouped$grouping_status[ft_rows] == "unresolved"
    )
    provisional_ft <- sum(
      grouped$grouping_status[ft_rows] == "provisional"
    )
    duplicate_endpoints <- sum(incident_counts$final_end_poss > 1L)
    missing_parents <- sum(!parent_key %in% source_key)
    hard_failure <- unresolved_ft > 0L || duplicate_endpoints > 0L ||
      missing_parents > 0L
    needs_review <- hard_failure || provisional_ft > 0L ||
      same_team > 0L || (!is.na(possession_difference) &&
                          possession_difference > 1L)

    data.frame(
      season = season_value,
      gamecode = game_value,
      total_possessions = nrow(game_possessions),
      possession_difference = possession_difference,
      same_team_transitions = same_team,
      provisional_ft_rows = provisional_ft,
      unresolved_ft_rows = unresolved_ft,
      duplicate_endpoint_incidents = duplicate_endpoints,
      missing_parent_targets = missing_parents,
      structural_status = if (hard_failure) "fail" else "pass",
      review_status = if (needs_review) "review" else "clear",
      stringsAsFactors = FALSE
    )
  }))
  game_qa <- game_qa[order(game_qa$season, game_qa$gamecode), , drop = FALSE]
  rownames(game_qa) <- NULL

  list(
    events = grouped,
    possessions = possessions,
    team_totals = team_totals,
    reason_totals = reason_totals,
    game_qa = game_qa
  )
}
