# The three player boxes are mutually exclusive, so a player claimed by one
# must not be OFFERED by the others. Narrowing the option pool is the fix;
# the alternative -- offering the player and then silently pulling them out of
# the box that already had them -- is what this replaced.

roster <- function() c("Avdija" = "1", "Sorkin" = "2", "Madar" = "3", "Colson" = "4")

test_that("a box offers the whole roster when nothing else is claimed", {
  pool <- lineup_box_pool(roster(), own_selected = character(0),
                          other_selected = character(0))

  expect_equal(pool, roster())
})

test_that("players claimed by another box are withheld", {
  pool <- lineup_box_pool(roster(), own_selected = character(0),
                          other_selected = c("2", "3"))

  expect_equal(unname(pool), c("1", "4"))
  expect_equal(names(pool), c("Avdija", "Colson"))
})

test_that("a box keeps its own selection in its pool", {
  # The narrowing update passes selected = own, so dropping the chosen option
  # from the pool would clear the box the moment another box changed.
  pool <- lineup_box_pool(roster(), own_selected = "1",
                          other_selected = c("1", "2"))

  expect_true("1" %in% unname(pool))
  expect_false("2" %in% unname(pool))
})

test_that("releasing a player returns them to the other pools", {
  before <- lineup_box_pool(roster(), character(0), other_selected = "3")
  after  <- lineup_box_pool(roster(), character(0), other_selected = character(0))

  expect_false("3" %in% unname(before))
  expect_true("3" %in% unname(after))
})

test_that("an empty roster stays empty", {
  pool <- lineup_box_pool(setNames(character(0), character(0)), "1", "2")

  expect_length(pool, 0L)
})
