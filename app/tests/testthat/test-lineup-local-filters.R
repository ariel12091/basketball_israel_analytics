# Players On is one selector whose chips each carry a required/optional flag:
#   star (required) -> the lineup must contain this player
#   hollow          -> the lineup must contain AT LEAST ONE of these
# so the selection "A required, B and C optional" reads A AND (B OR C).
#
# The two degenerate cases have to keep working, because they are what every
# pre-existing caller and every untouched call site produces:
#   all required -> contains all of them (the historical behaviour)
#   none required -> contains at least one of them

ld_fixture <- function() {
  data.frame(
    team_id = c(1L, 1L, 1L, 1L, 2L),
    player_ids = c("{10,11,12,13,14}",
                   "{10,11,20,21,22}",
                   "{10,30,31,32,33}",
                   "{11,40,41,42,43}",
                   "{50,51,52,53,54}"),
    total_poss = c(100L, 90L, 80L, 70L, 60L),
    stringsAsFactors = FALSE
  )
}

no_filters <- list(
  team_csv = NA_character_,
  player_csv = NA_character_,
  player_off_csv = NA_character_
)

keep_poss <- function(...) {
  p <- modifyList(no_filters, list(...))
  apply_local_lineup_filters(ld_fixture(), p)$total_poss
}

test_that("an absent required list keeps the historical all-of behaviour", {
  expect_equal(keep_poss(player_csv = "10,11"), c(100L, 90L))
})

test_that("every player required means the lineup must contain all of them", {
  expect_equal(
    keep_poss(player_csv = "10,11", player_required_csv = "10,11"),
    c(100L, 90L)
  )
})

test_that("no player required means at least one of them", {
  # 10 is in rows 1-3, 11 is in rows 1, 2 and 4; row 5 shares neither.
  expect_equal(
    keep_poss(player_csv = "10,11", player_required_csv = ""),
    c(100L, 90L, 80L, 70L)
  )
})

test_that("a required player plus optional ones reads A AND (B OR C)", {
  # 10 required; 20 or 30 optional. Row 1 has 10 but neither 20 nor 30.
  expect_equal(
    keep_poss(player_csv = "10,20,30", player_required_csv = "10"),
    c(90L, 80L)
  )
})

test_that("the optional clause cannot rescue a lineup missing a required player", {
  # 10 required, 11 and 40 optional. Row 4 holds BOTH optional players and is
  # still dropped, because it does not hold 10. Row 3 holds 10 but neither
  # optional player, so marking a player optional adds an "at least one of"
  # constraint rather than removing one.
  expect_equal(
    keep_poss(player_csv = "10,11,40", player_required_csv = "10"),
    c(100L, 90L)
  )
})

test_that("two required players intersect and still take the optional clause", {
  # 10 and 11 required, 12 and 20 optional: row 1 qualifies on 12, row 2 on 20.
  expect_equal(
    keep_poss(player_csv = "10,11,12,20", player_required_csv = "10,11"),
    c(100L, 90L)
  )
})

test_that("an unsatisfiable optional clause empties the result", {
  # Rows 1-2 hold both required players but neither holds 30, so requiring at
  # least one optional player leaves nothing.
  expect_equal(
    keep_poss(player_csv = "10,11,30", player_required_csv = "10,11"),
    integer(0)
  )
})

test_that("a lone optional player behaves as if it were required", {
  # "at least one of {30}" and "must contain 30" are the same statement.
  expect_equal(
    keep_poss(player_csv = "30", player_required_csv = ""),
    keep_poss(player_csv = "30", player_required_csv = "30")
  )
})

test_that("required ids not actually selected are ignored", {
  # A stale flag for a player who has since been removed must not filter.
  expect_equal(
    keep_poss(player_csv = "10", player_required_csv = "10,99"),
    c(100L, 90L, 80L)
  )
})

test_that("the required list is inert when no player is selected", {
  expect_equal(keep_poss(player_required_csv = "10"), c(100L, 90L, 80L, 70L, 60L))
})

test_that("Players Off still excludes regardless of the required split", {
  # Rows 2 and 3 hold 10; row 2 also holds 11, so the exclusion drops it.
  expect_equal(
    keep_poss(player_csv = "10,20,30", player_required_csv = "10",
              player_off_csv = "11"),
    80L
  )
})

test_that("the team filter composes with a required/optional split", {
  expect_equal(
    keep_poss(team_csv = "1", player_csv = "10,20,50", player_required_csv = "10"),
    90L
  )
})
