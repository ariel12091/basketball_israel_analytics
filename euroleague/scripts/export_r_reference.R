# Export the R reference decisions used for Python parity checks.
# Run from the repository root:
# Rscript euroleague/scripts/export_r_reference.R <raw.csv> <reference.csv>

args <- commandArgs(trailingOnly = TRUE)
if (length(args) != 2L) {
  stop(
    "Usage: export_r_reference.R <raw.csv> <reference.csv>",
    call. = FALSE
  )
}

source(file.path("etl", "euroleague", "group_events.R"))
raw <- read.csv(args[[1L]], stringsAsFactors = FALSE, check.names = FALSE)
grouped <- group_euroleague_events(raw)
columns <- c(
  "season", "gamecode", "period", "source_event_order",
  "synthetic_parent_order", "synthetic_ft_trip_id", "final_end_poss",
  "end_reason", "grouping_status", "grouping_confidence_pct"
)
grouped <- grouped[order(
  grouped$season,
  grouped$gamecode,
  grouped$period,
  grouped$source_event_order
), columns, drop = FALSE]
write.csv(grouped, args[[2L]], row.names = FALSE, na = "")
