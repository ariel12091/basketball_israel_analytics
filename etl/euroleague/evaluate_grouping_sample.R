# Diagnostic runner for unlabelled EuroLeague play-by-play samples.
# Run from the repository root:
# Rscript etl/euroleague/evaluate_grouping_sample.R <raw-pbp.csv>

args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 1L) {
  stop(
    "Usage: Rscript etl/euroleague/evaluate_grouping_sample.R <raw-pbp.csv>",
    call. = FALSE
  )
}

source(file.path("etl", "euroleague", "group_events.R"))

raw <- read.csv(
  args[[1L]],
  stringsAsFactors = FALSE,
  check.names = FALSE
)
timing <- system.time(grouped <- group_euroleague_events(raw))

ft <- grouped$play_type %in% c("FTA", "FTM")
endpoints <- grouped[grouped$final_end_poss, , drop = FALSE]

incident_endpoints <- aggregate(
  final_end_poss ~ season + gamecode + period + synthetic_parent_order,
  grouped,
  sum
)

parent_key <- paste(
  grouped$season,
  grouped$gamecode,
  grouped$period,
  grouped$source_event_order,
  sep = ":"
)
target_key <- paste(
  grouped$season,
  grouped$gamecode,
  grouped$period,
  grouped$synthetic_parent_order,
  sep = ":"
)

endpoint_groups <- split(
  endpoints,
  interaction(endpoints$gamecode, endpoints$period, drop = TRUE)
)
transition_counts <- vapply(
  endpoint_groups,
  function(events) {
    if (nrow(events) < 2L) return(c(same = 0, total = 0))
    c(
      same = sum(events$team_code[-1L] ==
                   events$team_code[-nrow(events)]),
      total = nrow(events) - 1L
    )
  },
  numeric(2L)
)

same_transition_rows <- lapply(endpoint_groups, function(events) {
  if (nrow(events) < 2L) return(NULL)
  hit <- which(
    events$team_code[-1L] == events$team_code[-nrow(events)]
  ) + 1L
  if (length(hit) == 0L) return(NULL)
  data.frame(
    gamecode = events$gamecode[hit],
    period = events$period[hit],
    previous_order = events$source_event_order[hit - 1L],
    previous_clock = events$clock[hit - 1L],
    previous_team = events$team_code[hit - 1L],
    previous_type = events$play_type[hit - 1L],
    previous_reason = events$end_reason[hit - 1L],
    current_order = events$source_event_order[hit],
    current_clock = events$clock[hit],
    current_team = events$team_code[hit],
    current_type = events$play_type[hit],
    current_reason = events$end_reason[hit],
    stringsAsFactors = FALSE
  )
})
same_transition_rows <- do.call(rbind, same_transition_rows)

team_counts <- aggregate(
  final_end_poss ~ gamecode + team_code,
  endpoints,
  sum
)
game_balance <- aggregate(
  final_end_poss ~ gamecode,
  team_counts,
  function(counts) max(counts) - min(counts)
)
names(game_balance)[names(game_balance) == "final_end_poss"] <-
  "possession_difference"

same_count <- sum(transition_counts["same", ])
transition_total <- sum(transition_counts["total", ])

cat(sprintf("rows: %d\n", nrow(grouped)))
cat(sprintf("games: %d\n", length(unique(grouped$gamecode))))
cat(sprintf("elapsed_seconds: %.3f\n", unname(timing[["elapsed"]])))
cat(sprintf("ft_rows: %d\n", sum(ft)))
cat(sprintf(
  "ft_resolved: %d (%.3f%%)\n",
  sum(grouped$grouping_status[ft] != "unresolved"),
  100 * mean(grouped$grouping_status[ft] != "unresolved")
))
cat(sprintf("ft_provisional: %d\n", sum(
  grouped$grouping_status[ft] == "provisional"
)))
cat(sprintf("endpoints: %d\n", nrow(endpoints)))
cat(sprintf("duplicate_endpoint_incidents: %d\n", sum(
  incident_endpoints$final_end_poss > 1L
)))
cat(sprintf("missing_parent_targets: %d\n", sum(!target_key %in% parent_key)))
cat(sprintf(
  "same_team_endpoint_transitions: %d/%d (%.3f%%)\n",
  same_count,
  transition_total,
  100 * same_count / transition_total
))

cat("\nGame possession-difference distribution:\n")
print(table(game_balance$possession_difference))

cat("\nSame-team endpoint transitions:\n")
if (is.null(same_transition_rows)) {
  cat("none\n")
} else {
  print(same_transition_rows, row.names = FALSE)
}

cat("\nGames with possession difference above one:\n")
print(
  game_balance[game_balance$possession_difference > 1L, , drop = FALSE],
  row.names = FALSE
)

unresolved <- grouped[ft & grouped$grouping_status == "unresolved", c(
  "gamecode", "period", "source_event_order", "clock", "team_code",
  "play_type", "player_id", "play_info"
), drop = FALSE]
cat("\nUnresolved FT rows:\n")
if (nrow(unresolved) == 0L) {
  cat("none\n")
} else {
  print(unresolved, row.names = FALSE)
}
