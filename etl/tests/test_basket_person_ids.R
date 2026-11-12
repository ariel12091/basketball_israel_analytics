library(testthat)

source(file.path("..", "..", "etl", "basket_person_ids.R"), local = TRUE)

regs <- function(ids, links, dob = NA, year = 2027L) {
  data.frame(game_year = year, basket_player_id = as.integer(ids),
             date_of_birth = as.Date(dob),
             linked_basket_player_ids = I(lapply(links, as.integer)))
}
person_of <- function(out, id) out$basket_person_id[out$basket_player_id == id]

test_that("a chain of links is one person, keyed by its smallest id", {
  out <- assign_basket_person_ids(regs(c(10, 20, 30), list(integer(), 10, 20)))
  expect_equal(out$basket_person_id, c(10L, 10L, 10L))
})

test_that("a link that bridges two persons merges them under the smaller key", {
  out <- assign_basket_person_ids(regs(c(10, 20, 5, 40), list(integer(), 10, integer(), c(5, 20))))
  expect_equal(out$basket_person_id, rep(5L, 4))
})

test_that("an id with no registration (before 2022) still joins and can be the key", {
  out <- assign_basket_person_ids(regs(20, list(7)))
  expect_equal(person_of(out, 20), 7L)
})

test_that("two registrations of one person in one season share his key", {
  out <- assign_basket_person_ids(regs(c(26116, 28237), list(21808, 21808)))
  expect_equal(out$basket_person_id, c(21808L, 21808L))
})

test_that("links that join two dates of birth stop the write", {
  r <- regs(c(10, 20), list(integer(), 10), dob = c("1999-01-01", "2000-01-01"))
  expect_error(assign_basket_person_ids(r), "10.*20|20.*10")
})

test_that("a registration with no links, read or not, is a person of its own", {
  r <- regs(c(10, 20), list(integer(), 1))
  r$linked_basket_player_ids[1] <- list(NULL)
  r$linked_basket_player_ids[2] <- list(integer())
  out <- assign_basket_person_ids(r)
  expect_equal(out$basket_person_id, c(10L, 20L))
})
