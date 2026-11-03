# Possession-end derivation for the Israeli ETL (moved out of etl_onoff.R so it can be
# tested without a database connection; etl_onoff.R sources it).

# Fouls whose free throws do not end the possession: the shooting team keeps the
# ball. Matched by word, because the provider's labels vary (unsportsmanlike,
# OffensiveFoulUnsportsmanlike, technical-behavioral, unsportsmanlike-flagrant).
SPECIAL_FOUL_WORDS <- c("technical", "unsportsmanlike", "disqualif")

special_foul_predicate <- function() {
  Reduce(function(a, b) rlang::expr(!!a | !!b),
         lapply(paste0("%", SPECIAL_FOUL_WORDS, "%"),
                function(p) rlang::expr(tolower(parameters_type) %LIKE% !!p)))
}

compute_possessions <- function(actions_tbl) {
  special_foul <- special_foul_predicate()
  poss0 <- actions_tbl |>
    mutate(
      # The provider occasionally reports an awarded total below the current
      # attempt number (for example, attempt 2 with awarded = 1). The attempt
      # number is a hard lower bound on trip size, so use it as the effective
      # denominator only for that impossible combination. Raw source fields
      # remain unchanged for auditability.
      .effective_ft_awarded = case_when(
        !is.na(parameters_free_throw_number) &
          !is.na(parameters_free_throws_awarded) &
          parameters_free_throw_number > parameters_free_throws_awarded ~
            parameters_free_throw_number,
        TRUE ~ parameters_free_throws_awarded
      )
    ) |>
    mutate(
      pct_ft = round(parameters_free_throw_number / NULLIF(.effective_ft_awarded, 0), 2),
      team_score = case_when(parameters_made == "made" ~ parameters_points, TRUE ~ NULL)
    ) |>
    mutate(q_bucket = as.integer(quarter)) %>%
    group_by(game_id) |>
    dbplyr::window_order(id) |>
    ungroup()
  
  base <- poss0 |>
    group_by(game_id, parent_action_id) |>
    dbplyr::window_order(id, quarter, desc(end_game_seconds_remaining), user_time) |>
    mutate(
      next_type      = lead(type),
      next_paramtype = lead(parameters_type),
      end_poss = case_when(
        type == "shot"      & parameters_made == "made"             ~ TRUE,
        next_paramtype == "defensive" & next_type == "rebound"      ~ TRUE,
        type == "freeThrow" & parameters_made == "made" & pct_ft==1 ~ TRUE,
        type == "turnover"                                           ~ TRUE,
        TRUE                                                         ~ FALSE
      ),
      sum_poss_poss = sum(if_else(end_poss, 1L, 0L),                            na.rm = TRUE),
      sum_block     = sum(if_else(parameters_made == "blocked",   1L, 0L),      na.rm = TRUE),
      sum_tech      = sum(if_else(!!special_foul,  1L, 0L),      na.rm = TRUE),
      final_end_poss_base = case_when(
        sum_poss_poss >= 2 & id == parent_action_id ~ NULL,
        sum_block >= 1                               ~ lead(end_poss),
        sum_tech  >= 1 & type != "turnover"          ~ NULL,
        TRUE ~ end_poss
      )
    ) |>
    # And-one candidate: a made shot whose group's last free throw was missed and then
    # rebounded by the offense. The rebound test reads the last free-throw id, so it
    # needs its own mutate().
    mutate(.last_ft_id = max(if_else(type == "freeThrow", id, NA_integer_), na.rm = TRUE)) |>
    mutate(
      .own_rebound_after = max(if_else(type == "rebound" & parameters_type == "offensive" &
                                         id > .last_ft_id, 1L, 0L), na.rm = TRUE),
      .last_ft_missed    = max(if_else(type == "freeThrow" & id == .last_ft_id &
                                         parameters_made == "missed", 1L, 0L), na.rm = TRUE)
    ) |>
    ungroup() |>
    mutate(.own_rebound_candidate = case_when(
      id == parent_action_id & type == "shot" & parameters_made == "made" &
        .last_ft_missed == 1L & .own_rebound_after == 1L ~ TRUE,
      TRUE ~ FALSE
    )) |>
    select(-next_type, -next_paramtype, -.last_ft_id, -.own_rebound_after, -.last_ft_missed)
  
  eoq_targets <- poss0 |>
    group_by(game_id, quarter) |>
    dbplyr::window_order(id) |>
    mutate(
      next_paramtype = lead(parameters_type),
      prev_id        = lag(id),
      prev_parent    = lag(parent_action_id),
      id_to_mark = case_when(
        next_paramtype == "end-of-quarter" & type == "shot"    & parameters_made == "missed"    ~ id,
        next_paramtype == "end-of-quarter" & type == "rebound" & parameters_type == "offensive" ~ id,
        next_paramtype == "end-of-quarter" & type == "block" & prev_parent == parent_action_id  ~ prev_id,
        TRUE ~ NA_integer_
      )
    ) |>
    filter(!is.na(id_to_mark)) |>
    distinct(game_id, id = id_to_mark) |>
    mutate(eoq_override = TRUE) |>
    ungroup()
  
  # A special-foul trip keeps the ball, but when it is the last event of the
  # period the possession ends there all the same.
  eoq_ft <- poss0 |>
    filter(!type %in% c("substitution", "timeout")) |>
    group_by(game_id, quarter) |>
    dbplyr::window_order(id) |>
    mutate(next_paramtype = lead(parameters_type)) |>
    filter(next_paramtype == "end-of-quarter", type == "freeThrow") |>
    ungroup() |>
    distinct(game_id, id) |>
    mutate(eoq_special_ft = TRUE)
  with_ends <- base |>
    left_join(eoq_targets, by = c("game_id", "id")) |>
    left_join(eoq_ft, by = c("game_id", "id")) |>
    mutate(final_end_poss = case_when(
      eoq_override                     ~ TRUE,
      eoq_special_ft & sum_tech >= 1L  ~ TRUE,
      TRUE                             ~ final_end_poss_base
    ))
  # The next end after each end, by game and quarter in id order; only candidates are
  # kept. (eoq_targets also carries a quarter column, so the join renames it quarter.x.)
  next_end <- with_ends |>
    filter(final_end_poss == TRUE) |>
    group_by(game_id, quarter.x) |>
    dbplyr::window_order(id) |>
    mutate(.next_end_team = lead(team_id)) |>
    ungroup() |>
    filter(.own_rebound_candidate) |>
    select(game_id, id, .next_end_team)

  finished <- with_ends |>
    # This runs last, on the finished ends: the end-of-quarter overrides above decide
    # which ends exist, and the made shot gives its end up only when the next end after
    # it (same game and quarter) is the same team's. A rebound that closes the period
    # is itself an end, so it keeps the made shot's end off.
    left_join(next_end, by = c("game_id", "id")) |>
    mutate(final_end_poss = case_when(
      .own_rebound_candidate & .next_end_team == team_id ~ FALSE,
      TRUE                                              ~ final_end_poss
    ))

  # Phantom ends. This runs after the and-one step because both tests below read the
  # finished ends: the previous end of a row is the one the and-one step left, and the
  # rows removed here never change whether another row is removed.
  #  (1) A put-back's rebound: the rebound of a missed shot is logged after the made
  #      put-back, so the same team's made shot is the previous end and the rebound
  #      (an end only by the end-of-quarter override) would count a possession twice.
  #  (2) A foul turnover (OffensiveFoulUnsportsmanlike) is a phantom when the fouling
  #      team did not have the ball: its own previous end with no shot or offensive
  #      rebound of its own since, or a second foul turnover in the same second with
  #      no live play between (a brawl logged as one burst).
  foul_ids <- finished |>
    filter(type == "foul", parameters_type == "OffensiveFoulUnsportsmanlike")
  foul_keys <- union_all(
    foul_ids |> transmute(game_id, parent_action_id = id),
    foul_ids |> transmute(game_id, parent_action_id = parent_action_id)
  ) |>
    distinct() |>
    mutate(.foul_key = 1L)
  end_lookup <- finished |>
    transmute(game_id, .prev_end_id = id, .prev_end_team = team_id,
              .prev_end_type = type, .prev_end_made = parameters_made)
  ftt_lookup <- finished |>
    transmute(game_id, .prev_ftt_id = id, .prev_ftt_secs = end_game_seconds_remaining)

  finished |>
    left_join(foul_keys, by = c("game_id", "parent_action_id")) |>
    mutate(
      .is_end = coalesce(final_end_poss, FALSE),
      .foul_turnover = case_when(type == "turnover" & !is.na(.foul_key) ~ TRUE, TRUE ~ FALSE),
      .end_id = if_else(.is_end, id, NA_integer_),
      .ftt_end_id = if_else(.is_end & .foul_turnover, id, NA_integer_),
      .live_id = case_when(
        type %in% c("shot", "freeThrow", "rebound") ~ id,
        type == "turnover" & !.foul_turnover        ~ id,
        TRUE                                        ~ NA_integer_
      ),
      .own_id = case_when(
        type == "shot"                                     ~ id,
        type == "rebound" & parameters_type == "offensive" ~ id,
        TRUE                                               ~ NA_integer_
      )
    ) |>
    group_by(game_id, quarter.x) |>
    dbplyr::window_order(id) |>
    mutate(.end_cum = cummax(.end_id), .ftt_cum = cummax(.ftt_end_id),
           .live_cum = cummax(.live_id)) |>
    mutate(.prev_end_id = lag(.end_cum), .prev_ftt_id = lag(.ftt_cum),
           .live_before = lag(.live_cum)) |>
    ungroup() |>
    group_by(game_id, quarter.x, team_id) |>
    dbplyr::window_order(id) |>
    mutate(.own_cum = cummax(.own_id)) |>
    mutate(.own_before = lag(.own_cum)) |>
    ungroup() |>
    left_join(end_lookup, by = c("game_id", ".prev_end_id")) |>
    left_join(ftt_lookup, by = c("game_id", ".prev_ftt_id")) |>
    mutate(
      .same_team_prev = case_when(.prev_end_team == team_id ~ TRUE, TRUE ~ FALSE),
      .putback_rebound = case_when(
        .is_end & type == "rebound" & parameters_type == "offensive" & .same_team_prev &
          .prev_end_type == "shot" & .prev_end_made == "made" &
          parent_action_id < .prev_end_id ~ TRUE,
        TRUE ~ FALSE
      ),
      .foul_no_ball = case_when(
        .is_end & .foul_turnover & .same_team_prev &
          (is.na(.own_before) | .own_before <= .prev_end_id) ~ TRUE,
        TRUE ~ FALSE
      ),
      .foul_same_second = case_when(
        .is_end & .foul_turnover & !is.na(.prev_ftt_id) &
          .prev_ftt_secs == end_game_seconds_remaining &
          (is.na(.live_before) | .live_before < .prev_ftt_id) ~ TRUE,
        TRUE ~ FALSE
      ),
      final_end_poss = case_when(
        .putback_rebound | .foul_no_ball | .foul_same_second ~ FALSE,
        TRUE                                                 ~ final_end_poss
      )
    ) |>
    select(-eoq_override, -eoq_special_ft, -final_end_poss_base, -.effective_ft_awarded,
           -.own_rebound_candidate, -.next_end_team, -.foul_key, -.is_end, -.foul_turnover,
           -.end_id, -.ftt_end_id, -.live_id, -.own_id, -.end_cum, -.ftt_cum, -.live_cum,
           -.prev_end_id, -.prev_ftt_id, -.live_before, -.own_cum, -.own_before,
           -.prev_end_team, -.prev_end_type, -.prev_end_made, -.prev_ftt_secs,
           -.same_team_prev, -.putback_rebound, -.foul_no_ball, -.foul_same_second)
}
