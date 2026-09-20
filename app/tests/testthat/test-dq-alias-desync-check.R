# Contract tests for AL_reader_lineup_hash_unresolved_in_on_table.
#
# The check exists because the app's lineup readers join
# df_pts_poss_lineups_longer_mv to lineups_lookup_on, while
# Q_persisted_rows_without_lineup_match joins the SOURCE table
# lineups_lookup. A scoped alias backfill can clean the derived table while
# the source and the MV keep raw player ids, which makes a segment
# unresolvable for the reader although the source holds a valid five. On
# 2026-09-20 that silently cost the stint ribbon 2588 seconds across four
# games and Q reported nothing.

dq_alias_check_source <- function() {
  paste(
    readLines(repo_file("..", "etl", "run_data_quality_report.R"), warn = FALSE),
    collapse = "\n"
  )
}

test_that("the alias-desync check is registered", {
  dq_r <- dq_alias_check_source()
  expect_true(grepl("AL_reader_lineup_hash_unresolved_in_on_table", dq_r, fixed = TRUE))
})

test_that("the check reads lineups_lookup_on, which is what the reader joins", {
  dq_r <- dq_alias_check_source()
  al <- sub(".*AL_reader_lineup_hash_unresolved_in_on_table", "", dq_r)

  # Resolution is judged against the derived table, keyed as the reader keys
  # it. Reading only lineups_lookup here would reproduce Q and see nothing.
  expect_true(grepl("on_counts AS (", al, fixed = TRUE))
  expect_true(grepl("count(DISTINCT player_id)::int AS players_on", al, fixed = TRUE))
  expect_true(grepl("coalesce(o.players_on, 0) <> 5", al, fixed = TRUE))
})

test_that("alias_resolvable requires a rewritten FIVE-player twin", {
  dq_r <- dq_alias_check_source()
  al <- sub(".*AL_reader_lineup_hash_unresolved_in_on_table", "", dq_r)

  # Without all three conditions a 6- or 7-player feed defect matches its own
  # unchanged set and is misreported as fixable by a reprocess. That was the
  # first version's bug: every row came back alias_resolvable = TRUE.
  expect_true(grepl("c.canon_pids IS DISTINCT FROM c.pids", al, fixed = TRUE))
  expect_true(grepl("cardinality(c.canon_pids) = 5", al, fixed = TRUE))
  expect_true(grepl("cardinality(o.pids) = 5", al, fixed = TRUE))
})

test_that("the check counts only alias-resolvable seconds", {
  dq_r <- dq_alias_check_source()
  al <- sub(".*AL_reader_lineup_hash_unresolved_in_on_table", "", dq_r)

  # Feed defects belong to Q and R. If this check counted them it could never
  # return to pass once the aliases were consistent, and would stop being a
  # signal that a backfill left the two id spaces out of step.
  expect_true(grepl('problem_count_col = "actionable_seconds"', al, fixed = TRUE))
  expect_true(grepl(
    "coalesce(sum(segment_seconds) FILTER (WHERE alias_resolvable), 0)",
    al,
    fixed = TRUE
  ))
  expect_true(grepl("AS actionable_seconds,", al, fixed = TRUE))
})

test_that("the check mirrors the ribbon reader's segment definition", {
  dq_r <- dq_alias_check_source()
  al <- sub(".*AL_reader_lineup_hash_unresolved_in_on_table", "", dq_r)

  # RIBBON_SQL_ISRAEL's segs CTE: gameplay only, non-zero duration, and
  # game_year taken from a DISTINCT lookup because final_schedule_mv is
  # sched_long-derived and carries two rows per game_id.
  expect_true(grepl("bool_or(d.type IS DISTINCT FROM 'substitution')", al, fixed = TRUE))
  expect_true(grepl("HAVING max(d.segment_seconds) > 0", al, fixed = TRUE))
  expect_true(grepl("SELECT DISTINCT game_id, game_year FROM", al, fixed = TRUE))
})
