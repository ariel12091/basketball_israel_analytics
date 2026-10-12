# Pure comparison logic for scripts/verify_ft_credit.R. No database access and
# no side effects at source time, so it can be unit-tested
# (app/tests/testthat/test-verify-ft-credit.R). Each function returns a
# character vector of failure messages; an empty vector means the gate passed.

# Compare one reader result with its snapshot.
#   spec$allowed  columns that may change ("*" = any column)
#   spec$frozen   columns that may never change, even under "*"
#   spec$frozen_where  named: frozen column -> the column it is reported with
#   spec$conserved  columns whose total over all rows may not change
#   spec$rows     "same" (default) or "any" when the reader's row set is not stable
fn_result_failures <- function(old, new, key, spec) {
  allowed <- spec$allowed
  frozen <- spec$frozen
  rows_any <- identical(spec$rows, "any")
  fails <- character(0)

  gone <- setdiff(names(old), names(new))
  added <- setdiff(names(new), names(old))
  if (length(gone)) fails <- c(fails, paste("columns no longer returned:", paste(gone, collapse = ", ")))
  if (length(added)) fails <- c(fails, paste("columns newly returned:", paste(added, collapse = ", ")))
  cols <- setdiff(intersect(names(old), names(new)), key)

  # Totals over the whole result, before any row matching: a conserved column
  # may be redistributed between rows but never created or lost.
  for (cc in intersect(spec$conserved, cols)) {
    before <- sum(as.numeric(old[[cc]]), na.rm = TRUE)
    after <- sum(as.numeric(new[[cc]]), na.rm = TRUE)
    if (!isTRUE(all.equal(before, after))) {
      fails <- c(fails, sprintf("total %s not conserved: %s before, %s after", cc, format(before), format(after)))
    }
  }

  same_rows <- nrow(old) == nrow(new)
  if (!same_rows && !rows_any) {
    return(c(fails, sprintf("rows: %d before, %d after", nrow(old), nrow(new))))
  }

  row_key <- function(d) do.call(paste, c(lapply(key, function(k) as.character(d[[k]])), sep = "\r"))
  ko <- row_key(old); kn <- row_key(new)
  if (anyDuplicated(ko) || anyDuplicated(kn)) {
    return(c(fails, paste("key is not unique:", paste(key, collapse = ", "))))
  }
  if (same_rows && !setequal(ko, kn) && !rows_any) {
    return(c(fails, "rows: same count but different keys"))
  }
  shared <- intersect(ko, kn)
  o <- old[match(shared, ko), , drop = FALSE]
  n <- new[match(shared, kn), , drop = FALSE]

  # A frozen column the reader reports only when another column exists is
  # compared on the rows where that column exists on both sides.
  comparable <- function(cc) {
    gate <- spec$frozen_where[cc]
    if (is.null(gate) || is.na(gate)) return(rep(TRUE, nrow(o)))
    !is.na(o[[gate]]) & !is.na(n[[gate]])
  }
  differs <- function(cc) {
    keep <- comparable(cc)
    !isTRUE(all.equal(o[[cc]][keep], n[[cc]][keep], check.attributes = FALSE))
  }
  no_gate <- setdiff(spec$frozen_where, cols)
  if (length(no_gate)) return(c(fails, paste("frozen_where names a column not returned:", paste(no_gate, collapse = ", "))))
  changed <- Filter(differs, cols)
  # When the row set itself moved, only the frozen columns are comparable.
  permitted <- if (identical(allowed, "*") || !same_rows) cols else allowed
  unexpected <- union(setdiff(changed, permitted), intersect(changed, frozen))
  if (length(unexpected)) {
    fails <- c(fails, paste("changed but not permitted:", paste(unexpected, collapse = ", ")))
  }
  fails
}

# Every case a reader is expected to support must have a snapshot.
missing_case_failures <- function(expected, available, unsupported) {
  missing <- setdiff(setdiff(expected, unsupported), available)
  if (length(missing)) paste("no snapshot for case:", paste(missing, collapse = ", ")) else character(0)
}

# `got` (game_id, team_id, got) against `expected` (game_id, team_id, expected):
# every expected team-game must be present with the right count, nothing else
# may be present, and an empty comparison is not a pass.
trip_coverage_failures <- function(got, expected) {
  if (!nrow(expected)) return("nothing to compare: no expected team-games")
  key <- function(d) paste(d$game_id, d$team_id)
  kg <- key(got); ke <- key(expected)
  fails <- character(0)
  missing <- setdiff(ke, kg)
  # A team-game with no actions in the base table (a game with no play-by-play)
  # may be present, but only with an empty count.
  has_count <- !is.na(got$got) & as.numeric(got$got) != 0
  extra <- setdiff(kg[has_count], ke)
  if (length(missing)) fails <- c(fails, sprintf("%d expected team-game(s) missing", length(missing)))
  if (length(extra)) fails <- c(fails, sprintf("%d team-game(s) present but not expected", length(extra)))
  shared <- intersect(ke, kg)
  wrong <- sum(as.numeric(got$got[match(shared, kg)]) != as.numeric(expected$expected[match(shared, ke)]), na.rm = TRUE) +
    sum(is.na(got$got[match(shared, kg)]))
  if (wrong) fails <- c(fails, sprintf("%d of %d team-games differ from one count per trip", wrong, length(ke)))
  fails
}
