# Fixtures and helpers for compute_possessions() (etl/possession_rules.R).
# testthat sources helper-*.R automatically; a plain source() works too and opens
# no database connection until ends() is first called.
# POSSESSION_FIXTURES is a named list of list(rows = <frame>, ends = <expected ids>),
# so a later script can run the same rows through another implementation.

# One row per action, in the actions_clean columns compute_possessions() reads.
act <- function(id, type, parent = id, ptype = NA_character_, made = NA_character_,
                pts = NA_real_, ft_no = NA_integer_, ft_of = NA_integer_, team = 1L, q = 1L) {
  tibble::tibble(game_id = 1L, id = as.integer(id), parent_action_id = as.integer(parent),
                 quarter = as.integer(q), end_game_seconds_remaining = 2400 - as.numeric(id),
                 user_time = sprintf("20:%02d:00", as.integer(id)), team_id = as.integer(team),
                 type = type, parameters_type = ptype, parameters_made = made,
                 parameters_points = pts, parameters_free_throw_number = as.integer(ft_no),
                 parameters_free_throws_awarded = as.integer(ft_of))
}
ft <- function(id, parent, no, of, made = "made", team = 1L)
  act(id, "freeThrow", parent, made = made, pts = if (made == "made") 1 else NA_real_,
      ft_no = no, ft_of = of, team = team)
shot <- function(id, parent = id, made = "made", ptype = "lay-up", team = 1L, q = 1L)
  act(id, "shot", parent, ptype = ptype, made = made, pts = 2, team = team, q = q)
foul <- function(id, kind, parent = id, team = 2L) act(id, "foul", parent, ptype = kind, team = team)
reb <- function(id, parent, kind, team = 1L) act(id, "rebound", parent, ptype = kind, team = team)
assist <- function(id, parent, team = 1L) act(id, "assist", parent, team = team)

# The in-memory database is opened on first use, so sourcing this file connects to nothing.
.possession_fixture_db <- new.env()

# Ids whose final_end_poss is TRUE (NA and FALSE are both "not an end").
ends <- function(rows) {
  if (is.null(.possession_fixture_db$con)) {
    .possession_fixture_db$con <- DBI::dbConnect(duckdb::duckdb())
    reg.finalizer(.possession_fixture_db, function(e) {
      try(DBI::dbDisconnect(e$con, shutdown = TRUE), silent = TRUE)
    }, onexit = TRUE)
  }
  con <- .possession_fixture_db$con
  name <- paste0("a", sample.int(1e9, 1))
  dplyr::copy_to(con, rows, name, overwrite = TRUE)
  out <- compute_possessions(dplyr::tbl(con, name)) |> dplyr::collect()
  sort(out$id[out$final_end_poss %in% TRUE])
}

# Team 2 fouls team 1, two made free throws, then team 1 scores.
trip <- function(kind) dplyr::bind_rows(foul(1, kind), ft(2, 1, 1, 2), ft(3, 1, 2, 2), shot(4))

# Made and-one, team 2 fouls, team 1 misses the free throw.
.s <- dplyr::bind_rows(shot(1), foul(2, "personal", parent = 1, team = 2L),
                       ft(3, 1, 1, 1, made = "missed"))
.fx <- function(rows, ends, base = .s) list(rows = dplyr::bind_rows(base, rows), ends = as.integer(ends))

POSSESSION_FIXTURES <- list(
  own_rebound_putback = .fx(dplyr::bind_rows(reb(4, 1, "offensive"), shot(5)), 5),
  two_own_rebounds = .fx(dplyr::bind_rows(reb(4, 1, "offensive"), shot(5, made = "missed"),
                                          reb(6, 5, "offensive"), shot(7)), 7),
  assist_before_rebound = .fx(dplyr::bind_rows(act(4, "assist", 1), reb(5, 1, "offensive"),
                                               shot(6)), 6),
  substitution_before_rebound = .fx(dplyr::bind_rows(act(4, "substitution"),
                                                     reb(5, 1, "offensive"), shot(6)), 6),
  other_group_end_before_ft = .fx(
    dplyr::bind_rows(shot(1), act(2, "turnover", ptype = "bad-pass", team = 2L),
                     foul(3, "personal", parent = 1, team = 2L),
                     ft(4, 1, 1, 1, made = "missed"), reb(5, 1, "offensive"), shot(6)),
    c(1, 2, 6), base = NULL),
  rebound_logged_after_putback = .fx(dplyr::bind_rows(shot(4), reb(5, 1, "offensive")), 4),
  own_rebound_turnover = .fx(dplyr::bind_rows(reb(4, 1, "offensive"),
                                              act(5, "turnover", ptype = "bad-pass")), 5),
  own_rebound_period_ends = .fx(dplyr::bind_rows(reb(4, 1, "offensive"),
                                                 act(5, "period", ptype = "end-of-quarter")), 4),
  chain = .fx(dplyr::bind_rows(reb(4, 1, "offensive"), shot(5),
                               foul(6, "personal", parent = 5, team = 2L),
                               ft(7, 5, 1, 1, made = "missed"), reb(8, 5, "offensive"),
                               shot(9)), 9),
  no_later_end_in_period = .fx(dplyr::bind_rows(reb(4, 1, "offensive"),
                                                shot(5, made = "missed"),
                                                act(6, "turnover", ptype = "bad-pass",
                                                    team = 2L, q = 2L)), c(1, 6)),
  opponent_end_next = .fx(dplyr::bind_rows(reb(4, 1, "offensive"), shot(5, team = 2L)), c(1, 5)),
  made_ft_unchanged = .fx(dplyr::bind_rows(shot(1), foul(2, "personal", parent = 1, team = 2L),
                                           ft(3, 1, 1, 1), shot(4, team = 2L)),
                          c(3, 4), base = NULL),
  defensive_rebound_unchanged = .fx(dplyr::bind_rows(reb(4, 1, "defensive", team = 2L),
                                                     shot(5, team = 2L)), c(3, 5)),
  other_group_own_rebound = .fx(dplyr::bind_rows(reb(4, 99, "offensive"),
                                                 reb(5, 1, "defensive", team = 2L),
                                                 act(6, "turnover", ptype = "bad-pass")),
                                c(3, 6)),
  special_foul_on_basket = .fx(dplyr::bind_rows(shot(1),
                                                foul(2, "unsportsmanlike", parent = 1, team = 2L),
                                                ft(3, 1, 1, 1, made = "missed"),
                                                reb(4, 1, "offensive"), shot(5)),
                               5, base = NULL),
  no_made_shot_in_group = .fx(dplyr::bind_rows(foul(1, "personal", team = 2L), ft(2, 1, 1, 2),
                                               ft(3, 1, 2, 2, made = "missed"),
                                               reb(4, 1, "offensive"), shot(5)),
                              5, base = NULL),
  next_quarter_end_is_same_team = .fx(dplyr::bind_rows(reb(4, 1, "offensive"),
                                                       shot(5, made = "missed"),
                                                       act(6, "turnover", ptype = "bad-pass", q = 2L)),
                                      c(1, 6)),
  rebound_before_made_last_ft = .fx(dplyr::bind_rows(shot(1),
                                                     foul(2, "personal", parent = 1, team = 2L),
                                                     ft(3, 1, 1, 2, made = "missed"),
                                                     reb(4, 1, "offensive"), ft(5, 1, 2, 2)),
                                    5, base = NULL),
  rebound_before_missed_last_ft = .fx(dplyr::bind_rows(shot(1),
                                                       foul(2, "personal", parent = 1, team = 2L),
                                                       reb(3, 1, "offensive"),
                                                       ft(4, 1, 1, 1, made = "missed"),
                                                       shot(5)),
                                      c(1, 5), base = NULL)
)

# Phantom possession ends (docs/superpowers/plans/2026-11-03-phantom-possession-ends.md).
# Each fixture is list(rows, ends) like POSSESSION_FIXTURES. Ids follow the foul block
# layout: .F(a) is foul a, foul-drawn a + 1, turnover a + 2 (team 1 fouls; .F2 swaps the teams).
.OFU <- "OffensiveFoulUnsportsmanlike"
.bye <- act(99, "period", ptype = "end-of-quarter")
.F <- function(a, kind = .OFU)
  dplyr::bind_rows(foul(a, kind, team = 1L), act(a + 1, "foul-drawn", a, team = 2L),
                   act(a + 2, "turnover", a, ptype = "other", team = 1L))
.F2 <- function(a, kind = .OFU)
  dplyr::bind_rows(foul(a, kind, team = 2L), act(a + 1, "foul-drawn", a, team = 1L),
                   act(a + 2, "turnover", a, ptype = "other", team = 2L))
.FTS <- dplyr::bind_rows(ft(5, 2, 1, 2, team = 2L), ft(6, 2, 2, 2, team = 2L))

# Sets end_game_seconds_remaining on the given ids (the default is 2400 - id).
at_second <- function(rows, ids, rem) {
  rows$end_game_seconds_remaining[rows$id %in% ids] <- as.numeric(rem)
  rows
}
.px <- function(ends, ...) list(rows = dplyr::bind_rows(...), ends = as.integer(ends))

PHANTOM_END_FIXTURES <- list(
  putback_logged_before_rebound = .px(2, shot(1, made = "missed"), shot(2),
                                      reb(3, 1, "offensive"), .bye),
  putback_and_one_then_rebound = .px(2, shot(1, made = "missed"), shot(2),
                                     foul(3, "personal", parent = 2, team = 2L),
                                     ft(4, 2, 1, 1, made = "missed"),
                                     reb(5, 1, "offensive"), .bye),
  rebound_in_order_then_putback = .px(3, shot(1, made = "missed"), reb(2, 1, "offensive"),
                                      shot(3), .bye),
  buzzer_rebound_no_made_shot = .px(2, shot(1, made = "missed"), reb(2, 1, "offensive"), .bye),
  earlier_basket_later_own_rebound = .px(c(1, 3), shot(1), shot(2, made = "missed"),
                                         reb(3, 2, "offensive"), .bye),
  opponent_scored_before_buzzer_rebound = .px(c(2, 3), shot(1, made = "missed"),
                                              shot(2, team = 2L), reb(3, 1, "offensive"), .bye),
  foul_after_own_turnover = .px(c(1, 7), act(1, "turnover", ptype = "ball-handling"), .F(2),
                                .FTS, shot(7, team = 2L)),
  foul_after_own_basket = .px(c(1, 7), shot(1), .F(2), .FTS, shot(7, team = 2L)),
  foul_no_free_throws = .px(c(1, 6), shot(1, made = "missed"),
                            reb(2, 1, "defensive", team = 2L), .F(3), shot(6, team = 2L)),
  real_foul_after_opponent_end = .px(c(1, 4, 7), shot(1, team = 2L), .F(2), .FTS,
                                     shot(7, team = 2L)),
  team_regained_ball = .px(c(1, 6, 7), act(1, "turnover", ptype = "bad-pass"),
                           shot(2, made = "missed"), reb(3, 2, "offensive"), .F(4),
                           shot(7, team = 2L)),
  first_in_period = .px(c(3, 4), .F(1), shot(4, team = 2L)),
  ordinary_offensive_foul_stays = .px(c(1, 4, 5), shot(1), .F(2, "personal"),
                                      shot(5, team = 2L)),
  brawl = .px(c(1, 5, 14),
              at_second(dplyr::bind_rows(
                shot(1, made = "missed", team = 2L), reb(2, 1, "defensive"),
                .F(3), .F2(6), .F(9), ft(12, 3, 1, 2, team = 2L), ft(13, 3, 2, 2, team = 2L),
                shot(14, team = 2L)), 3:13, 1500)),
  ordinary_same_second_stays = .px(c(1, 5, 8, 9),
              at_second(dplyr::bind_rows(
                shot(1, made = "missed", team = 2L), reb(2, 1, "defensive"),
                .F(3, "personal"), .F2(6, "personal"), shot(9)), 3:8, 1500)),
  same_second_after_live_play = .px(c(1, 4, 8, 9),
              at_second(dplyr::bind_rows(
                shot(1, team = 2L), .F(2), ft(5, 2, 1, 1, team = 2L), .F2(6), shot(9)),
                2:8, 1500)),
  and_one_then_foul_turnover = .px(c(5, 9), shot(1),
                                   foul(2, "personal", parent = 1, team = 2L),
                                   ft(3, 1, 1, 1, made = "missed"), reb(4, 1, "offensive"),
                                   shot(5), .F(6), shot(9, team = 2L))
)
