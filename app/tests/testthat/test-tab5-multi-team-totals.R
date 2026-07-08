test_that("build_ts_total_row sums counts and recomputes rates from totals", {
  rows <- data.frame(
    team_id = c(1L, 2L), player_id = c(11L, 11L),
    Player = c("Jordan Bone", "Jordan Bone"), team_name = c("Team A", "Team B"),
    gp = c(10, 5), poss_on_floor = c(300, 150), minutes = c(150, 70),
    pts = c(100, 40), reb = c(40, 15), oreb = c(10, 5), dreb = c(30, 10),
    ast = c(30, 12), stl = c(10, 4), blk = c(6, 2), dfl = c(5, 3), tov = c(14, 6),
    fgm = c(38, 16), fga = c(70, 30), `3pm` = c(11, 5), `3pa` = c(29, 12),
    ftm = c(13, 6), fta = c(16, 8), two_pct = c(NA_real_, NA_real_),
    fg_pct = c(54.3, 53.3), tp_pct = c(37.9, 41.7), ft_pct = c(81.3, 75),
    efg = c(62, 60), ts = c(60, 58), usg_pct = c(25, 20),
    check.names = FALSE, stringsAsFactors = FALSE
  )

  out <- build_ts_total_row(rows, "1000000000001", "Jordan Bone")

  expect_equal(nrow(out), 1L)
  expect_equal(out$team_name, "TOTAL")
  expect_equal(out$Player, "Jordan Bone")
  expect_true(is.na(out$team_id))
  expect_true(is.na(out$player_id))
  expect_true(isTRUE(out$is_multi_team_total))
  expect_equal(out$.identity_id, "1000000000001")

  # Summed counts
  expect_equal(out$gp, 15)
  expect_equal(out$poss_on_floor, 450)
  expect_equal(out$minutes, 220)
  expect_equal(out$pts, 140)
  expect_equal(out$reb, 55)
  expect_equal(out$dfl, 8)
  expect_equal(out$fgm, 54)
  expect_equal(out$fga, 100)

  # Rates recomputed from summed counts
  expect_equal(out$fg_pct, 54.0)               # 54 / 100
  expect_equal(out$tp_pct, 39.0)               # 16 / 41
  expect_equal(out$two_pct, 64.4)              # 38 / 59
  expect_equal(out$ft_pct, 79.2)               # 19 / 24
  expect_equal(out$efg, 62.0)                  # (54 + 0.5*16) / 100
  expect_equal(out$ts, 63.3)                   # 140 / (2 * (100 + 0.44*24))
  # USG% is possession-weighted: (25*300 + 20*150) / 450
  expect_equal(out$usg_pct, 23.3)
})

test_that("add_ts_multi_team_totals appends one total per >=2-team identity only", {
  df <- data.frame(
    team_id = c(1L, 2L, 1L, 3L, 4L),
    player_id = c(11L, 11L, 12L, 99L, 99L),
    Player = c("A", "A", "B", "C", "C"),
    team_name = c("T1", "T2", "T1", "T3", "T4"),
    gp = c(10, 5, 8, 4, 4),
    poss_on_floor = c(300, 150, 200, 100, 100),
    minutes = c(150, 70, 120, 50, 50),
    pts = c(100, 40, 72, 30, 30),
    reb = c(40, 15, 24, 10, 10), oreb = c(10, 5, 7, 3, 3), dreb = c(30, 10, 17, 7, 7),
    ast = c(30, 12, 18, 8, 8), stl = c(10, 4, 7, 5, 5), blk = c(6, 2, 4, 3, 3),
    dfl = c(5, 3, 4, 2, 2),
    tov = c(14, 6, 11, 5, 5),
    fgm = c(38, 16, 28, 12, 12), fga = c(70, 30, 55, 25, 25),
    `3pm` = c(11, 5, 8, 3, 3), `3pa` = c(29, 12, 22, 10, 10),
    ftm = c(13, 6, 8, 5, 5), fta = c(16, 8, 10, 6, 6),
    fg_pct = c(54, 53, 51, 48, 48), tp_pct = c(38, 42, 36, 30, 30),
    ft_pct = c(81, 75, 80, 83, 83), efg = c(62, 60, 58, 54, 54),
    ts = c(60, 58, 57, 55, 55), usg_pct = c(25, 20, 22, 18, 18),
    check.names = FALSE, stringsAsFactors = FALSE
  )
  # Player A (11) resolves to one identity across teams 1 & 2 -> eligible.
  # Player B (12) is single-team -> not combined.
  # Player C (99) is absent from the lookup (unresolved) -> not combined.
  lookup <- data.frame(
    team_id = c(1L, 2L, 1L), player_id = c(11L, 11L, 12L),
    identity_id = c("idA", "idA", "idB"),
    display_name = c("A", "A", "B"),
    stringsAsFactors = FALSE
  )

  out <- add_ts_multi_team_totals(df, lookup)

  expect_equal(nrow(out), 6L)                 # 5 original + 1 total
  expect_equal(sum(out$is_multi_team_total), 1L)

  total <- out[out$is_multi_team_total, , drop = FALSE]
  expect_equal(nrow(total), 1L)
  expect_equal(total$team_name, "TOTAL")
  expect_equal(total$Player, "A")
  expect_equal(total$.identity_id, "idA")
  expect_equal(total$gp, 15)
  expect_equal(total$pts, 140)
  expect_equal(total$dfl, 8)

  # Original rows preserved and untouched
  originals <- out[!out$is_multi_team_total, , drop = FALSE]
  expect_equal(nrow(originals), 5L)
  expect_equal(sort(originals$team_name), sort(c("T1", "T2", "T1", "T3", "T4")))
})

test_that("add_ts_multi_team_totals leaves data unchanged when lookup is empty", {
  df <- data.frame(
    team_id = c(1L, 2L), player_id = c(11L, 11L),
    Player = c("A", "A"), team_name = c("T1", "T2"),
    gp = c(10, 5), poss_on_floor = c(300, 150), minutes = c(150, 70),
    pts = c(100, 40), fgm = c(38, 16), fga = c(70, 30),
    `3pm` = c(11, 5), `3pa` = c(29, 12), ftm = c(13, 6), fta = c(16, 8),
    check.names = FALSE, stringsAsFactors = FALSE
  )
  out <- add_ts_multi_team_totals(df, data.frame())
  expect_equal(nrow(out), 2L)
  expect_true(all(!out$is_multi_team_total))
})

test_that("add_ts_multi_team_totals normalizes per-team names to the canonical name", {
  df <- data.frame(
    team_id = c(14L, 6L, 9L),
    player_id = c(1143L, 1982L, 555L),
    Player = c("DJ BURNS", "D.J. BURNS", "SOLO GUY"),
    team_name = c("Rishon", "Bnei H", "Ness Z"),
    gp = c(27, 17, 20),
    poss_on_floor = c(1448, 692, 400), minutes = c(900, 500, 600),
    pts = c(341, 166, 200), reb = c(178, 71, 50), oreb = c(52, 24, 10),
    dreb = c(126, 47, 40), ast = c(61, 38, 30), stl = c(25, 13, 5),
    blk = c(8, 6, 2), dfl = c(12, 7, 3), tov = c(43, 29, 10),
    fgm = c(124, 72, 80), fga = c(242, 132, 150),
    `3pm` = c(19, 1, 10), `3pa` = c(64, 1, 30), ftm = c(74, 21, 20), fta = c(98, 32, 25),
    fg_pct = c(51, 54, 53), tp_pct = c(30, 100, 33), ft_pct = c(75, 66, 80),
    efg = c(55, 55, 56), ts = c(58, 60, 57), usg_pct = c(20, 23, 19),
    check.names = FALSE, stringsAsFactors = FALSE
  )
  # 1143 + 1982 are one identity (canonical "DJ BURNS"); 555 is single-team.
  lookup <- data.frame(
    team_id = c(14L, 6L, 9L), player_id = c(1143L, 1982L, 555L),
    identity_id = c("idBurns", "idBurns", "idSolo"),
    display_name = c("DJ BURNS", "DJ BURNS", "SOLO GUY"),
    stringsAsFactors = FALSE
  )

  out <- add_ts_multi_team_totals(df, lookup)

  burns <- out[out$.identity_id == "idBurns", , drop = FALSE]
  # Both per-team rows AND the TOTAL row all read the canonical "DJ BURNS".
  expect_true(all(burns$Player == "DJ BURNS"))
  expect_false(any(grepl("D\\.J\\.", burns$Player)))
  # Single-team player is untouched.
  solo <- out[out$.identity_id == "idSolo" & !out$is_multi_team_total, , drop = FALSE]
  expect_equal(solo$Player, "SOLO GUY")
})

test_that("filter_ts_players keeps a TOTAL row when any component entry is selected", {
  df <- data.frame(
    team_id = c(1L, 2L, NA_integer_),
    player_id = c(11L, 11L, NA_integer_),
    Player = c("A", "A", "A"),
    team_name = c("T1", "T2", "TOTAL"),
    is_multi_team_total = c(FALSE, FALSE, TRUE),
    .identity_id = c("idA", "idA", "idA"),
    pts = c(100, 40, 140),
    check.names = FALSE, stringsAsFactors = FALSE
  )
  lookup <- data.frame(
    team_id = c(1L, 2L), player_id = c(11L, 11L),
    identity_id = c("idA", "idA"), display_name = c("A", "A"),
    stringsAsFactors = FALSE
  )

  # Selecting player A on team 1 keeps that team row AND the TOTAL row.
  out <- filter_ts_players(df, "1:11", lookup)
  expect_equal(sort(out$team_name), sort(c("T1", "TOTAL")))

  # Without a lookup, a TOTAL row is not surfaced by a component selection.
  out_no_lookup <- filter_ts_players(df, "1:11")
  expect_equal(out_no_lookup$team_name, "T1")
})

test_that("ts_group_display_order groups team rows with their TOTAL by combined PTS", {
  df <- data.frame(
    team_id = c(1L, 2L, NA_integer_, 5L, 6L, 7L),
    player_id = c(11L, 11L, NA_integer_, 51L, 61L, 71L),
    Player = c("A", "A", "A", "Hi", "Mid", "Lo"),
    team_name = c("T1", "T2", "TOTAL", "T5", "T6", "T7"),
    pts = c(100, 40, 140, 130, 120, 30),
    is_multi_team_total = c(FALSE, FALSE, TRUE, FALSE, FALSE, FALSE),
    .identity_id = c("idA", "idA", "idA", "idHi", "idMid", "idLo"),
    check.names = FALSE, stringsAsFactors = FALSE
  )

  out <- ts_group_display_order(df)

  # idA's combined PTS (140) ranks the whole group first; rows contiguous; TOTAL last.
  expect_equal(out$team_name[1:3], c("T1", "T2", "TOTAL"))
  expect_equal(out$pts[1:3], c(100, 40, 140))
  # Singles follow by PTS desc.
  expect_equal(out$pts[4:6], c(130, 120, 30))
  # The 40-PTS team row is grouped at the top, NOT placed beside the 30-PTS single.
  expect_lt(which(out$team_name == "T2"), which(out$team_name == "T7"))
})

test_that("ts_group_display_order is a no-op without pts / on empty input", {
  expect_equal(nrow(ts_group_display_order(data.frame())), 0L)
  d <- data.frame(a = 1:3)
  expect_identical(ts_group_display_order(d), d)
})

test_that("ts_group_display_order never collapses distinct unresolved-identity rows", {
  df <- data.frame(
    team_id = c(1L, 2L, 3L),
    player_id = c(11L, 22L, 33L),
    Player = c("NA hi", "single", "NA lo"),
    team_name = c("Nhi", "Single", "Nlo"),
    pts = c(50, 40, 25),
    is_multi_team_total = c(FALSE, FALSE, FALSE),
    .identity_id = c(NA_character_, "idMid", NA_character_),
    check.names = FALSE, stringsAsFactors = FALSE
  )

  out <- ts_group_display_order(df)

  # Each unresolved (NA identity) row gets a unique token, so it forms its own
  # group and orders purely by PTS desc -- the two NA rows must NOT clump
  # together: the resolved single (40) sits between them (50, 40, 25).
  expect_equal(out$team_name, c("Nhi", "Single", "Nlo"))
  expect_gt(which(out$team_name == "Single"), which(out$team_name == "Nhi"))
  expect_lt(which(out$team_name == "Single"), which(out$team_name == "Nlo"))
})

test_that("ts_player_choices uses the canonical identity name across a player's teams", {
  players <- data.frame(
    team_id = c(14L, 6L, 9L),
    player_id = c(1143L, 1982L, 555L),
    player_name = c("DJ BURNS", "D.J. BURNS", "SOLO GUY"),
    team_name = c("Rishon", "Bnei H", "Ness Z"),
    stringsAsFactors = FALSE
  )
  lookup <- data.frame(
    team_id = c(14L, 6L), player_id = c(1143L, 1982L),
    identity_id = c("id1", "id1"), display_name = c("DJ BURNS", "DJ BURNS"),
    stringsAsFactors = FALSE
  )

  ch <- ts_player_choices(players, lookup = lookup)

  # Keys remain one entry per (team, player).
  expect_true(all(c("14:1143", "6:1982", "9:555") %in% unname(ch)))
  # Both DJ Burns entries read the canonical name; no "D.J." spelling remains.
  expect_true("DJ BURNS (Rishon)" %in% names(ch))
  expect_true("DJ BURNS (Bnei H)" %in% names(ch))
  expect_false(any(grepl("D\\.J\\.", names(ch))))
  # Unmatched player keeps its original name.
  expect_true("SOLO GUY (Ness Z)" %in% names(ch))
})

test_that("ts_player_choices without a lookup is unchanged", {
  players <- data.frame(
    team_id = 6L, player_id = 1982L,
    player_name = "D.J. BURNS", team_name = "Bnei H",
    stringsAsFactors = FALSE
  )
  ch <- ts_player_choices(players)
  expect_equal(unname(ch), "6:1982")
  expect_equal(names(ch), "D.J. BURNS (Bnei H)")
})
