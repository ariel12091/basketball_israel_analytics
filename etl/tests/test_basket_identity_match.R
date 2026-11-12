library(testthat)

source(file.path("..", "..", "etl", "basket_identity_match.R"), local = TRUE)

# As fetch_basket_registrations() builds a row.
reg <- function(name_en, borrowed = NA, name_he = "רועי פריצקי") {
  r <- registration_english_names(data.frame(
    game_year = 2027L, basket_player_id = 28237L, team_id = 7L,
    player_name = name_he, player_name_en = name_en, borrowed_name_en = borrowed,
    stringsAsFactors = FALSE))
  r$norm_he <- normalize_person_name(r$player_name)
  r
}
pool <- function(names) {
  data.frame(game_year = 2027L, team_id = 7L, identity_id = seq_along(names) + 100,
             canonical_player_id = seq_along(names) + 500L, display_name = names,
             norm_en = normalize_person_name(names), norm_he = NA_character_,
             stringsAsFactors = FALSE)
}

test_that("a registration with no English name matches on the person's English name from another season", {
  r <- reg(NA, borrowed = "Roy Paretsky")
  expect_true(r$name_en_borrowed)
  hit <- match_registration(r, pool(c("ROY PARETSKY", "ROY ELIA")))
  expect_equal(nrow(hit), 1)
  expect_equal(hit$display_name, "ROY PARETSKY")
  expect_equal(hit$matched_on, "name_en")
})

test_that("a borrowed English name is only a candidate", {
  # no hit: nothing matched
  expect_equal(nrow(match_registration(reg(NA, borrowed = "Roy Paretsky"), pool("ROY ELIA"))), 0)
  # two identities with that name: both come back, as for a row's own name
  two <- match_registration(reg(NA, borrowed = "Roy Paretsky"), pool(c("ROY PARETSKY", "ROY PARETSKY")))
  expect_equal(nrow(two), 2)
  # a row's own English name wins over a borrowed one
  own <- reg("Roy Elia", borrowed = "Roy Paretsky")
  expect_false(own$name_en_borrowed)
  expect_equal(match_registration(own, pool(c("ROY PARETSKY", "ROY ELIA")))$display_name, "ROY ELIA")
})
