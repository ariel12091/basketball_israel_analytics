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
    ungroup() |>
    select(-next_type, -next_paramtype)
  
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
  base |>
    left_join(eoq_targets, by = c("game_id", "id")) |>
    left_join(eoq_ft, by = c("game_id", "id")) |>
    mutate(final_end_poss = case_when(
      eoq_override                     ~ TRUE,
      eoq_special_ft & sum_tech >= 1L  ~ TRUE,
      TRUE                             ~ final_end_poss_base
    )) |>
    select(-eoq_override, -eoq_special_ft, -final_end_poss_base, -.effective_ft_awarded)
}
