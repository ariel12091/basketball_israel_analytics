# server_tab5_traditional.R - Tab 5: Traditional Player Stats server logic

# Display label -> column name in result_df()
TS_FILTERABLE_COLS <- list(
  "GP"    = "gp",
  "MIN"   = "minutes",
  "Poss"  = "poss_on_floor",
  "Total Poss" = "total_poss",
  "PTS"   = "pts",
  "REB"   = "reb",
  "OREB"  = "oreb",
  "DREB"  = "dreb",
  "AST"   = "ast",
  "STL"   = "stl",
  "BLK"   = "blk",
  "TOV"   = "tov",
  "FGM"   = "fgm",
  "FGA"   = "fga",
  "FG%"   = "fg_pct",
  "2PM"   = "2pm",
  "2PA"   = "2pa",
  "2P%"   = "two_pct",
  "3PM"   = "3pm",
  "3PA"   = "3pa",
  "3P%"   = "tp_pct",
  "FTM"   = "ftm",
  "FTA"   = "fta",
  "FT%"   = "ft_pct",
  "eFG%"  = "efg",
  "TS%"   = "ts",
  "USG%"  = "usg_pct"
)

TS_PERCENT_COLS <- c("fg_pct", "two_pct", "tp_pct", "ft_pct", "efg", "ts", "usg_pct")

# Display column -> source (snake_case) column used for percentile/heat coloring,
# in display order. TOV is reverse-polarity (handled at render time via COLS_REV).
TS_HEAT_SRC <- list(
  PTS = "pts", REB = "reb", OREB = "oreb", DREB = "dreb", AST = "ast",
  STL = "stl", BLK = "blk", FGM = "fgm", FGA = "fga", `FG%` = "fg_pct",
  `2PM` = "2pm", `2PA` = "2pa", `2P%` = "two_pct", `3PM` = "3pm",
  `3PA` = "3pa", `3P%` = "tp_pct", FTM = "ftm", FTA = "fta",
  `FT%` = "ft_pct", `eFG%` = "efg", `TS%` = "ts", `USG%` = "usg_pct",
  TOV = "tov"
)

# Name of the hidden percentile-rank column carrying a heat column's color value.
# Must match the lookup used at render time (apply_heat) exactly.
ts_pr_colname <- function(display_name) {
  paste0("pr_", gsub("[^A-Za-z0-9]+", "_", display_name))
}

# Which "actual playing-time base" context column to show for a display mode. Rate
# modes normalize their per-rate column to a constant (Poss On Floor = 60, Min =
# 30), hiding the real sample size, so the table surfaces the matching per-game
# base: Min/G in minutes modes, Poss/G in possession modes. Per Game already shows
# per-game Min and Poss On Floor, and Totals has no rate, so both return NULL.
# Returns NULL or list(label, src column, after column).
ts_rate_base_col <- function(mode) {
  if (mode %in% c("Per 30 Minutes", "Per X Minutes")) {
    return(list(label = "Min/G", src = "base_min_pg", after = "Min"))
  }
  if (mode %in% c("Per 60 Possessions", "Per X Possessions")) {
    return(list(label = "Poss/G", src = "base_poss_pg", after = "Poss On Floor"))
  }
  NULL
}

# Possession floor for the Per 60 / Per 30 small-sample trim: the (1 - keep_pct)
# quantile of possessions, but only once the population is large enough that
# trimming the noisy low-possession tail is worthwhile. Below min_n eligible
# players the trim is disabled (returns 0), so small or heavily filtered result
# sets show everyone instead of always dropping the bottom (1 - keep_pct).
ts_rate_threshold <- function(poss_vec, keep_pct = 0.85, min_n = 120L) {
  poss_vec <- suppressWarnings(as.numeric(poss_vec))
  poss_vec <- poss_vec[is.finite(poss_vec) & poss_vec > 0]
  if (length(poss_vec) < min_n) return(0)
  as.numeric(stats::quantile(poss_vec, probs = 1 - keep_pct, na.rm = TRUE, type = 7))
}

# Compute percentile-rank (pr_*) columns over the FULL population passed in, so
# coloring is league-relative and stays fixed no matter how the displayed rows are
# later narrowed (player selection, Min GP, stat filters). Rows below the adaptive
# possession baseline (per `.poss_rank_base`) are set to NA before ranking, so
# low-possession / 0-pt outliers neither receive a color nor skew the distribution
# (dplyr::percent_rank drops NAs from the denominator). Pure; safe on missing cols.
add_ts_percentile_cols <- function(df) {
  if (is.null(df) || !nrow(df)) return(df)
  base <- suppressWarnings(as.numeric(df$`.poss_rank_base`))
  if (length(base) != nrow(df) || all(is.na(base))) base <- rep(0, nrow(df))
  mask <- dplyr::coalesce(base, 0) >= adaptive_baseline(base)
  mask[!is.finite(mask)] <- FALSE

  present <- Filter(function(d) TS_HEAT_SRC[[d]] %in% names(df), names(TS_HEAT_SRC))
  if (!length(present)) return(df)

  # Blank out sub-threshold rows across every stat in one shot, then percent-rank
  # the table column-wise over the full population (masked NAs drop out of each
  # column's rank denominator, so low-possession outliers neither color nor skew).
  src <- df[unlist(TS_HEAT_SRC[present], use.names = FALSE)]
  src[!mask, ] <- NA_real_
  df[vapply(present, ts_pr_colname, character(1))] <- lapply(src, dplyr::percent_rank)
  df
}

apply_ts_stat_filters <- function(df, filters) {
  if (is.null(df) || !nrow(df) || !length(filters)) return(df)
  for (f in filters) {
    col <- f$col
    if (!col %in% names(df)) next
    v <- suppressWarnings(as.numeric(df[[col]]))
    keep <- !is.na(v) & (if (identical(f$op, "ge")) v >= f$value else v <= f$value)
    df <- df[keep, , drop = FALSE]
    if (!nrow(df)) break
  }
  df
}

add_ts_two_point_stats <- function(df) {
  if (is.null(df) || !nrow(df)) return(df)
  needed <- c("fgm", "fga", "3pm", "3pa")
  if (!all(needed %in% names(df))) return(df)

  fgm <- suppressWarnings(as.numeric(df$fgm))
  fga <- suppressWarnings(as.numeric(df$fga))
  fg3m <- suppressWarnings(as.numeric(df$`3pm`))
  fg3a <- suppressWarnings(as.numeric(df$`3pa`))

  df$`2pm` <- fgm - fg3m
  df$`2pa` <- fga - fg3a
  df$two_pct <- ifelse(df$`2pa` > 0, round((df$`2pm` / df$`2pa`) * 100, 1), NA_real_)
  df
}

normalize_ts_result_cols <- function(df) {
  if (is.null(df) || !nrow(df)) return(df)
  if ("player_name" %in% names(df) && !("Player" %in% names(df))) {
    names(df)[names(df) == "player_name"] <- "Player"
  }

  if (!("oreb" %in% names(df))) df$oreb <- NA_real_
  if (!("dreb" %in% names(df))) df$dreb <- NA_real_
  if (!("reb" %in% names(df))) {
    oreb <- suppressWarnings(as.numeric(df$oreb))
    dreb <- suppressWarnings(as.numeric(df$dreb))
    df$reb <- ifelse(is.na(oreb) & is.na(dreb), NA_real_, coalesce(oreb, 0) + coalesce(dreb, 0))
  }
  df
}

ts_player_key <- function(team_id, player_id) {
  paste0(as.integer(team_id), ":", as.integer(player_id))
}

normalize_ts_players <- function(players_df, teams_df = NULL) {
  if (is.null(players_df) || !nrow(players_df) || is.null(names(players_df))) {
    return(data.frame())
  }
  if (!all(c("team_id", "player_id") %in% names(players_df))) {
    return(data.frame())
  }

  player_name <- if ("player_name" %in% names(players_df)) {
    as.character(players_df$player_name)
  } else if ("name" %in% names(players_df)) {
    as.character(players_df$name)
  } else if (all(c("firstname", "lastname") %in% names(players_df))) {
    trimws(paste(players_df$firstname, players_df$lastname))
  } else {
    rep("", nrow(players_df))
  }

  team_name <- if ("team_name" %in% names(players_df)) {
    as.character(players_df$team_name)
  } else {
    rep("", nrow(players_df))
  }
  if (is.data.frame(teams_df) && nrow(teams_df) && all(c("team_id", "team_name") %in% names(teams_df))) {
    team_map <- stats::setNames(as.character(teams_df$team_name), as.character(teams_df$team_id))
    missing_team <- is.na(team_name) | !nzchar(team_name)
    team_name[missing_team] <- unname(team_map[as.character(players_df$team_id[missing_team])])
  }

  out <- data.frame(
    team_id = suppressWarnings(as.integer(players_df$team_id)),
    player_id = suppressWarnings(as.integer(players_df$player_id)),
    player_name = trimws(player_name),
    team_name = trimws(team_name),
    stringsAsFactors = FALSE
  )
  out <- out[
    is.finite(out$team_id) &
      is.finite(out$player_id) &
      nzchar(out$player_name),
    ,
    drop = FALSE
  ]
  if (!nrow(out)) return(out)
  out$key <- ts_player_key(out$team_id, out$player_id)
  out <- out[!duplicated(out$key), , drop = FALSE]
  out[order(tolower(out$player_name), tolower(out$team_name)), , drop = FALSE]
}

ts_player_choices <- function(players_df, teams_df = NULL, team_ids = NULL, lookup = NULL) {
  players <- normalize_ts_players(players_df, teams_df)
  if (!nrow(players)) return(stats::setNames(character(0), character(0)))
  if (!is.null(team_ids) && length(team_ids)) {
    players <- players[players$team_id %in% as.integer(team_ids), , drop = FALSE]
  }
  if (!nrow(players)) return(stats::setNames(character(0), character(0)))

  # Use the canonical identity display name when the lookup resolves a
  # (team, player), so a player who appears under different provider ids/teams
  # reads consistently. Keys stay "<team_id>:<player_id>".
  if (!is.null(lookup) && nrow(lookup) &&
      all(c("team_id", "player_id", "display_name") %in% names(lookup))) {
    dispmap <- stats::setNames(as.character(lookup$display_name),
                               paste0(lookup$team_id, ":", lookup$player_id))
    canon <- unname(dispmap[players$key])
    ok <- !is.na(canon) & nzchar(canon)
    players$player_name[ok] <- canon[ok]
  }

  labels <- ifelse(
    nzchar(players$team_name),
    sprintf("%s (%s)", players$player_name, players$team_name),
    players$player_name
  )
  stats::setNames(players$key, labels)
}

filter_ts_players <- function(df, selected_player_keys, lookup = NULL) {
  if (is.null(df) || !nrow(df) || is.null(selected_player_keys) || !length(selected_player_keys)) {
    return(df)
  }
  if (!all(c("team_id", "player_id") %in% names(df))) return(df)
  sel <- as.character(selected_player_keys)

  is_total <- if ("is_multi_team_total" %in% names(df)) {
    vapply(df$is_multi_team_total, isTRUE, logical(1))
  } else {
    rep(FALSE, nrow(df))
  }

  row_keys <- ts_player_key(df$team_id, df$player_id)
  keep_regular <- !is_total & (row_keys %in% sel)

  # A TOTAL row is kept when ANY of its component (team, player) entries is
  # selected, so picking a multi-team player on any one team surfaces the total.
  keep_total <- rep(FALSE, nrow(df))
  if (any(is_total) && ".identity_id" %in% names(df) &&
      !is.null(lookup) && nrow(lookup) &&
      all(c("team_id", "player_id", "identity_id") %in% names(lookup))) {
    lkmap <- stats::setNames(as.character(lookup$identity_id),
                             paste0(lookup$team_id, ":", lookup$player_id))
    sel_idents <- unique(unname(lkmap[sel]))
    sel_idents <- sel_idents[!is.na(sel_idents)]
    if (length(sel_idents)) {
      keep_total <- is_total & (as.character(df$.identity_id) %in% sel_idents)
    }
  }

  df[keep_regular | keep_total, , drop = FALSE]
}

# Build a single combined row for one player identity from that identity's
# per-team rows. Counting stats are summed; percentages are recomputed from the
# summed counts; USG% is a possession-weighted average of the component values.
build_ts_total_row <- function(rows, identity_id, display_name = NULL) {
  if (is.null(rows) || !nrow(rows)) return(NULL)
  s <- function(col) if (col %in% names(rows)) sum(suppressWarnings(as.numeric(rows[[col]])), na.rm = TRUE) else NA_real_
  pct <- function(n, d) if (is.finite(n) && is.finite(d) && d > 0) round(n / d * 100, 1) else NA_real_

  # Summed counting stats, then derived two-point and percentage columns.
  sums <- vapply(
    c("gp", "poss_on_floor", "minutes", "pts", "reb", "oreb", "dreb", "ast", "stl", "blk",
      "tov", "fgm", "fga", "3pm", "3pa", "ftm", "fta"),
    s, numeric(1)
  )
  fg2m <- sums[["fgm"]] - sums[["3pm"]]
  fg2a <- sums[["fga"]] - sums[["3pa"]]
  derived <- c(
    `2pm` = fg2m, `2pa` = fg2a,
    fg_pct  = pct(sums[["fgm"]], sums[["fga"]]),
    two_pct = pct(fg2m, fg2a),
    tp_pct  = pct(sums[["3pm"]], sums[["3pa"]]),
    ft_pct  = pct(sums[["ftm"]], sums[["fta"]]),
    efg     = pct(sums[["fgm"]] + 0.5 * sums[["3pm"]], sums[["fga"]]),
    ts      = pct(sums[["pts"]], 2 * (sums[["fga"]] + 0.44 * sums[["fta"]])),
    usg_pct = ts_weighted_usg(rows)
  )

  name <- display_name
  if (is.null(name) || is.na(name) || !nzchar(name)) {
    name <- if ("Player" %in% names(rows)) as.character(rows$Player[1]) else NA_character_
  }

  out <- rows[1, , drop = FALSE]
  out$team_id <- NA_integer_
  out$player_id <- NA_integer_
  out$team_name <- "TOTAL"
  if ("Player" %in% names(out)) out$Player <- name
  for (col in names(sums))    if (col %in% names(out)) out[[col]] <- sums[[col]]
  for (col in names(derived)) if (col %in% names(out)) out[[col]] <- derived[[col]]
  out$is_multi_team_total <- TRUE
  out$.identity_id <- as.character(identity_id)
  out
}

# Possession-weighted average of component USG% values (USG% is a rate, so it is
# averaged by playing time rather than summed).
ts_weighted_usg <- function(rows) {
  if (!all(c("usg_pct", "poss_on_floor") %in% names(rows))) return(NA_real_)
  u <- suppressWarnings(as.numeric(rows$usg_pct))
  w <- suppressWarnings(as.numeric(rows$poss_on_floor))
  ok <- is.finite(u) & is.finite(w) & w > 0
  if (!any(ok)) return(NA_real_)
  round(sum(u[ok] * w[ok]) / sum(w[ok]), 1)
}

# Append one combined TOTAL row per player identity that appears on >= min_teams
# distinct teams in the current (already team/db-filtered) result set. Identity
# is resolved via the stable identity dictionary; unresolved or ambiguous rows
# are never combined. Original per-team rows are preserved.
add_ts_multi_team_totals <- function(df, lookup, min_teams = 2L) {
  if (is.null(df) || !nrow(df)) return(df)
  if (!all(c("team_id", "player_id") %in% names(df))) return(df)
  if (!("is_multi_team_total" %in% names(df))) df$is_multi_team_total <- FALSE
  if (!(".identity_id" %in% names(df))) df$.identity_id <- NA_character_

  if (is.null(lookup) || !nrow(lookup) ||
      !all(c("team_id", "player_id", "identity_id") %in% names(lookup))) {
    return(df)
  }

  lkmap <- stats::setNames(as.character(lookup$identity_id),
                           paste0(lookup$team_id, ":", lookup$player_id))
  dispmap <- NULL
  if ("display_name" %in% names(lookup)) {
    dl <- lookup[!duplicated(lookup$identity_id), , drop = FALSE]
    dispmap <- stats::setNames(as.character(dl$display_name), as.character(dl$identity_id))
  }

  df$.identity_id <- unname(lkmap[paste0(df$team_id, ":", df$player_id)])

  resolved <- df[!is.na(df$.identity_id), , drop = FALSE]
  if (!nrow(resolved)) return(df)

  team_counts <- tapply(resolved$team_id, resolved$.identity_id,
                        function(t) length(unique(t[!is.na(t)])))
  multi_ids <- names(team_counts[team_counts >= min_teams])
  if (!length(multi_ids)) return(df)

  # Per-team rows of a multi-team identity display the canonical identity name,
  # so the same person reads consistently across teams (and matches the TOTAL).
  if (!is.null(dispmap) && "Player" %in% names(df)) {
    is_total_row <- vapply(df$is_multi_team_total, isTRUE, logical(1))
    member <- !is.na(df$.identity_id) & df$.identity_id %in% multi_ids & !is_total_row
    if (any(member)) {
      canon <- unname(dispmap[df$.identity_id[member]])
      ok <- !is.na(canon) & nzchar(canon)
      idx <- which(member)[ok]
      df$Player[idx] <- canon[ok]
    }
  }

  totals <- lapply(multi_ids, function(id) {
    id_rows <- resolved[resolved$.identity_id == id, , drop = FALSE]
    nm <- if (!is.null(dispmap)) unname(dispmap[id]) else NULL
    build_ts_total_row(id_rows, id, nm)
  })
  totals <- totals[!vapply(totals, is.null, logical(1))]
  if (!length(totals)) return(df)

  dplyr::bind_rows(df, do.call(rbind, totals))
}

# Rows excluding appended multi-team TOTAL rows. Used for the eligibility/rate
# quantile population, where a derived TOTAL would double-count its player.
ts_drop_totals <- function(df) {
  if (is.null(df) || !nrow(df) || !("is_multi_team_total" %in% names(df))) return(df)
  df[!vapply(df$is_multi_team_total, isTRUE, logical(1)), , drop = FALSE]
}

# Order rows for display so each player's per-team rows sit together followed by
# its multi-team TOTAL row. The whole group is positioned at the group's combined
# PTS (the TOTAL's PTS = sum of its parts = the group max), so single-team players
# and multi-team groups interleave by PTS desc and the table still reads PTS-first.
# Rows with no resolved identity never group. Pure; safe on missing columns.
ts_group_display_order <- function(df) {
  if (is.null(df) || !nrow(df) || !("pts" %in% names(df))) return(df)
  n <- nrow(df)
  ident <- if (".identity_id" %in% names(df)) as.character(df$.identity_id) else rep(NA_character_, n)
  is_total <- if ("is_multi_team_total" %in% names(df)) {
    vapply(df$is_multi_team_total, isTRUE, logical(1))
  } else {
    rep(FALSE, n)
  }
  # Group key: the identity when present, else a unique per-row token (so
  # unresolved rows never collapse together).
  g <- ifelse(is.na(ident) | !nzchar(ident), paste0(".row", seq_len(n)), ident)
  pts <- suppressWarnings(as.numeric(df$pts))
  pts[is.na(pts)] <- -Inf
  grp_pts <- stats::ave(pts, g, FUN = function(p) max(p, na.rm = TRUE))
  ord <- order(-grp_pts, g, is_total, -pts)
  df[ord, , drop = FALSE]
}

ts_no_data_message <- function(selected_player_keys) {
  "No data for current filters"
}

server_tab5_traditional <- function(input, output, session, shared) {

  TS_NORM_MIN_GP <- 3L
  TS_NORM_PCT <- 75
  TS_RATE_KEEP_PCT <- 0.85
  # Minimum eligible-player population before the Per 60 / Per 30 bottom-tail trim
  # engages; below this everyone is shown (small/filtered result sets).
  TS_RATE_MIN_N <- 120L

  clean_ts_rows <- function(df) {
    if (is.null(df) || !nrow(df)) return(df)
    df %>%
      filter(
        !is.na(Player), nzchar(trimws(Player)),
        !is.na(team_name), nzchar(trimws(team_name))
      ) %>%
      filter(
        coalesce(gp, 0) > 0 |
          coalesce(poss_on_floor, 0) > 0 |
          coalesce(minutes, 0) > 0
      )
  }

  add_ts_usage_pct <- function(df) {
    if (is.null(df) || !nrow(df)) return(df)
    if (!("usg_pct" %in% names(df))) df$usg_pct <- NA_real_
    needed <- c("fga", "fta", "tov", "poss_on_floor")
    if (!all(needed %in% names(df))) return(df)

    as_num <- function(col) suppressWarnings(as.numeric(df[[col]]))
    zero_na <- function(x) {
      x[!is.finite(x)] <- 0
      x
    }

    fga <- zero_na(as_num("fga"))
    fta <- zero_na(as_num("fta"))
    tov <- zero_na(as_num("tov"))
    poss_on_floor <- as_num("poss_on_floor")
    pts <- if ("pts" %in% names(df)) as_num("pts") else rep(NA_real_, nrow(df))
    ts <- if ("ts" %in% names(df)) as_num("ts") else rep(NA_real_, nrow(df))

    shot_term <- fga + 0.44 * fta
    can_imply_ts_term <- is.finite(pts) & pts > 0 & is.finite(ts) & ts > 0
    shot_term[can_imply_ts_term] <- pts[can_imply_ts_term] / (2 * (ts[can_imply_ts_term] / 100))
    player_term <- shot_term + tov

    out <- suppressWarnings(as.numeric(df$usg_pct))
    team_key <- if ("team_id" %in% names(df)) as.character(df$team_id) else rep("all", nrow(df))
    team_key[is.na(team_key) | !nzchar(team_key)] <- "all"

    for (key in unique(team_key)) {
      idx <- which(team_key == key)
      team_term <- sum(player_term[idx], na.rm = TRUE)
      team_poss <- sum(poss_on_floor[idx], na.rm = TRUE) / 5
      ok <- !is.finite(out[idx]) &
        is.finite(player_term[idx]) & player_term[idx] >= 0 &
        is.finite(poss_on_floor[idx]) & poss_on_floor[idx] > 0 &
        is.finite(team_term) & team_term > 0 &
        is.finite(team_poss) & team_poss > 0
      if (any(ok)) {
        out[idx[ok]] <- 100 * player_term[idx][ok] * team_poss / (team_term * poss_on_floor[idx][ok])
      }
    }

    df$usg_pct <- round(out, 1)
    df
  }

  ts_ref <- reactiveValues(teams = NULL, players = NULL)

  ts_stat_filters <- reactiveVal(list())
  ts_stat_filter_next_id <- reactiveVal(1L)

  selected_team_ids_now <- function() {
    ids <- suppressWarnings(as.integer(input$ts_teams %||% character(0)))
    ids <- ids[is.finite(ids)]
    if (length(ids)) ids else NULL
  }

  refresh_ts_player_choices <- function() {
    gy_int <- suppressWarnings(as.integer(input$game_year))
    lk <- if (length(gy_int) && is.finite(gy_int)) load_ts_identity_lookup(gy_int) else NULL
    choices <- ts_player_choices(ts_ref$players, ts_ref$teams, selected_team_ids_now(), lookup = lk)
    selected <- intersect(input$ts_players %||% character(0), unname(choices))
    updateSelectizeInput(session, "ts_players", choices = choices, selected = selected, server = TRUE)
  }

  observeEvent(list(input$main_tabs, input$game_year), ignoreInit = TRUE, {
    if (!identical(input$main_tabs, "traditional_stats")) return(NULL)
    gy_int <- as.integer(input$game_year)
    req(gy_int)

    teams_df <- cached_ref_query(
      key = sprintf("ts_teams_%d", gy_int),
      query_fun = function() {
        db_get_query(
          pg_pool,
          "SELECT DISTINCT team_id, team_name
           FROM basketball_test.full_rosters
           WHERE game_year = $1
           ORDER BY team_name",
          params = list(gy_int)
        )
      }
    )
    ts_ref$teams <- teams_df
    team_choices <- stats::setNames(as.character(teams_df$team_id), as.character(teams_df$team_name))
    updateSelectizeInput(session, "ts_teams", choices = team_choices, selected = character(0), server = TRUE)
    updateSelectizeInput(session, "ts_opponents", choices = team_choices, selected = character(0), server = TRUE)

    players_df <- cached_ref_query(
      key = sprintf("ts_players_%d", gy_int),
      query_fun = function() {
        db_get_query(
          pg_pool,
          "SELECT
             fr.team_id,
             fr.player_id,
             MIN(btrim(fr.team_name)) AS team_name,
             MIN(NULLIF(btrim(CONCAT_WS(' ', fr.firstname, fr.lastname)), '')) AS player_name
           FROM basketball_test.full_rosters fr
           WHERE fr.game_year = $1
             AND fr.player_id IS NOT NULL
             AND fr.player_id > 0
           GROUP BY fr.team_id, fr.player_id
           HAVING MIN(NULLIF(btrim(CONCAT_WS(' ', fr.firstname, fr.lastname)), '')) IS NOT NULL
           ORDER BY player_name, team_name",
          params = list(gy_int)
        )
      }
    )
    ts_ref$players <- players_df
    refresh_ts_player_choices()

    gn_df <- cached_ref_query(
      key = sprintf("ts_gn_%d", gy_int),
      query_fun = function() {
        db_get_query(
          pg_pool,
          "SELECT DISTINCT gn
           FROM basketball_test.final_schedule_mv
           WHERE game_year = $1
           ORDER BY gn",
          params = list(gy_int)
        )
      }
    )
    gn_vals <- if (nrow(gn_df)) as.integer(gn_df$gn) else integer(0)
    update_gn_last_n_choices(session, "ts", gn_vals)
  })

  observeEvent(input$game_year, {
    b <- shared$season_date_bounds(input$game_year)
    updateDateRangeInput(session, "ts_dates", start = b$start, end = b$end, min = b$start, max = b$end)
  }, ignoreInit = FALSE)

  setup_gn_last_n_sync(session, input, "ts")

  # ignoreNULL = FALSE so clearing the last team (input$ts_teams -> NULL) still
  # fires and restores the full player list; otherwise the player dropdown stays
  # filtered to the removed team.
  observeEvent(input$ts_teams, {
    refresh_ts_player_choices()
  }, ignoreInit = TRUE, ignoreNULL = FALSE)

  observeEvent(input$ts_reset, {
    b <- shared$season_date_bounds(input$game_year %||% DEFAULT_GAME_YEAR)
    updateDateRangeInput(session, "ts_dates", start = b$start, end = b$end, min = b$start, max = b$end)
    updateSelectizeInput(session, "ts_teams", selected = character(0))
    updateSelectizeInput(session, "ts_players", selected = character(0))
    updateSelectizeInput(session, "ts_game_type", selected = character(0))
    updateSelectizeInput(session, "ts_opponents", selected = character(0))
    updateSelectInput(session, "ts_home_away", selected = "")
    updateSelectInput(session, "ts_outcome", selected = "")
    reset_opp_rank_inputs(session, "ts")
    updateSelectInput(session, "ts_display_mode", selected = "Per Game")
    updateSliderInput(session, "ts_min_gp_slider", value = 1, min = 1, max = 40)
    updateNumericInput(session, "ts_min_gp", value = 1, min = 1, max = 40)
    updateCheckboxInput(session, "ts_show_ineligible", value = FALSE)
    reset_clutch_inputs(session, "ts")
    reset_gn_last_n_inputs(session, "ts")
    ts_stat_filters(list())
  })

  observeEvent(input$ts_clear_players, {
    updateSelectizeInput(session, "ts_players", selected = character(0))
  }, ignoreInit = TRUE)

  observeEvent(input$ts_add_stat_filter, {
    col_label <- input$ts_stat_filter_col
    op <- input$ts_stat_filter_op %||% "ge"
    raw_val <- input$ts_stat_filter_value
    if (is.null(col_label) || !nzchar(col_label)) return()
    if (is.null(raw_val) || is.na(suppressWarnings(as.numeric(raw_val)))) return()
    if (!col_label %in% names(TS_FILTERABLE_COLS)) return()
    if (!op %in% c("ge", "le")) return()

    new_id <- ts_stat_filter_next_id()
    ts_stat_filter_next_id(new_id + 1L)

    current <- ts_stat_filters()
    current[[length(current) + 1]] <- list(
      id = new_id,
      label = col_label,
      col = TS_FILTERABLE_COLS[[col_label]],
      op = op,
      value = as.numeric(raw_val)
    )
    ts_stat_filters(current)

    updateSelectInput(session, "ts_stat_filter_col", selected = "")
    updateRadioButtons(session, "ts_stat_filter_op", selected = "ge")
    updateNumericInput(session, "ts_stat_filter_value", value = NA)
  })

  observeEvent(input$ts_remove_stat_filter, {
    rm_id <- suppressWarnings(as.integer(input$ts_remove_stat_filter))
    if (is.na(rm_id)) return()
    current <- ts_stat_filters()
    keep <- vapply(current, function(f) !identical(as.integer(f$id), rm_id), logical(1))
    ts_stat_filters(current[keep])
  }, ignoreInit = TRUE)

  observeEvent(input$ts_min_gp_slider, ignoreInit = TRUE, {
    s <- suppressWarnings(as.integer(input$ts_min_gp_slider))
    n <- suppressWarnings(as.integer(input$ts_min_gp))
    if (is.na(s)) return(NULL)
    if (is.na(n) || s != n) {
      updateNumericInput(session, "ts_min_gp", value = s)
    }
  })

  observeEvent(input$ts_min_gp, ignoreInit = TRUE, {
    n <- suppressWarnings(as.integer(input$ts_min_gp))
    s <- suppressWarnings(as.integer(input$ts_min_gp_slider))
    if (is.na(n)) return(NULL)
    if (is.na(s) || n != s) {
      updateSliderInput(session, "ts_min_gp_slider", value = n)
    }
  })

  apply_ts_mode <- function(df, mode, x_poss = NA_real_, x_min = NA_real_) {
    if (is.null(df) || !nrow(df)) return(df)

    count_cols <- c("pts", "reb", "oreb", "dreb", "ast", "stl", "blk", "tov", "fgm", "fga", "2pm", "2pa", "3pm", "3pa", "ftm", "fta")
    mode <- mode %||% "Per Game"

    if (identical(mode, "Per Game")) {
      for (col in count_cols) {
        if (col %in% names(df)) df[[col]] <- ifelse(df$gp > 0, df[[col]] / df$gp, NA_real_)
      }
      if ("poss_on_floor" %in% names(df)) df$poss_on_floor <- ifelse(df$gp > 0, df$poss_on_floor / df$gp, NA_real_)
      if ("minutes" %in% names(df)) df$minutes <- ifelse(df$gp > 0, df$minutes / df$gp, NA_real_)
      return(df)
    }

    if (identical(mode, "Per 60 Possessions")) {
      base_poss <- df$poss_on_floor
      for (col in count_cols) {
        if (col %in% names(df)) df[[col]] <- ifelse(base_poss > 0, df[[col]] / base_poss * 60, NA_real_)
      }
      if ("minutes" %in% names(df)) df$minutes <- ifelse(base_poss > 0, df$minutes / base_poss * 60, NA_real_)
      if ("poss_on_floor" %in% names(df)) df$poss_on_floor <- ifelse(base_poss > 0, base_poss / base_poss * 60, NA_real_)
      return(df)
    }

    if (identical(mode, "Per 30 Minutes")) {
      base_minutes <- df$minutes
      for (col in count_cols) {
        if (col %in% names(df)) df[[col]] <- ifelse(base_minutes > 0, df[[col]] / base_minutes * 30, NA_real_)
      }
      if ("poss_on_floor" %in% names(df)) df$poss_on_floor <- ifelse(base_minutes > 0, df$poss_on_floor / base_minutes * 30, NA_real_)
      if ("minutes" %in% names(df)) df$minutes <- ifelse(base_minutes > 0, base_minutes / base_minutes * 30, NA_real_)
      return(df)
    }

    if (identical(mode, "Per X Possessions")) {
      if (!is.finite(x_poss) || x_poss <= 0) return(df)
      for (col in count_cols) {
        if (col %in% names(df)) df[[col]] <- ifelse(df$poss_on_floor > 0, df[[col]] / df$poss_on_floor * x_poss, NA_real_)
      }
      if ("minutes" %in% names(df)) df$minutes <- ifelse(df$poss_on_floor > 0, df$minutes / df$poss_on_floor * x_poss, NA_real_)
      if ("poss_on_floor" %in% names(df)) df$poss_on_floor <- ifelse(df$poss_on_floor > 0, df$poss_on_floor / df$poss_on_floor * x_poss, NA_real_)
      return(df)
    }

    if (identical(mode, "Per X Minutes")) {
      if (!is.finite(x_min) || x_min <= 0) return(df)
      for (col in count_cols) {
        if (col %in% names(df)) df[[col]] <- ifelse(df$minutes > 0, df[[col]] / df$minutes * x_min, NA_real_)
      }
      if ("poss_on_floor" %in% names(df)) df$poss_on_floor <- ifelse(df$minutes > 0, df$poss_on_floor / df$minutes * x_min, NA_real_)
      return(df)
    }

    df
  }

  debounced_range <- reactive(input$ts_dates) %>% debounce(300)
  debounced_teams <- reactive(input$ts_teams) %>% debounce(300)
  debounced_players <- reactive(input$ts_players) %>% debounce(150)
  debounced_ts_filters <- reactive(list(
    game_type = input$ts_game_type,
    opp_ids = input$ts_opponents,
    home_away = input$ts_home_away,
    outcome = input$ts_outcome,
    rank_side = input$ts_opp_rank_side,
    rank_n = input$ts_opp_rank_n,
    metric = input$ts_opp_rank_metric,
    clutch_enabled = input$ts_clutch_enabled,
    clutch_margin = input$ts_clutch_margin,
    clutch_status = input$ts_clutch_status,
    clutch_minutes = input$ts_clutch_minutes,
    clutch_ot_margin = input$ts_clutch_ot_margin
  )) %>% debounce(300)

  gn_params <- reactive({
    resolve_gn_last_n_params(input, "ts")
  }) %>% debounce(150)

  selected_team_ids <- reactive({
    ids <- suppressWarnings(as.integer(debounced_teams()))
    ids <- ids[is.finite(ids)]
    if (length(ids)) ids else NULL
  })

  selected_opp_ids <- reactive({
    ids <- suppressWarnings(as.integer(debounced_ts_filters()$opp_ids))
    ids <- ids[is.finite(ids)]
    if (length(ids)) ids else NULL
  })

  build_ts_db_args <- function() {
    f <- debounced_ts_filters()
    tids <- selected_team_ids()
    opp_ids <- selected_opp_ids()
    gp <- gn_params()
    clutch <- resolve_clutch_params(
      enabled = f$clutch_enabled,
      margin = f$clutch_margin,
      status = f$clutch_status,
      minutes = f$clutch_minutes,
      ot_margin = f$clutch_ot_margin
    )

    list(
      team_ids_csv = csv_if_any(tids, integerize = TRUE),
      game_type_csv = csv_if_any(f$game_type, integerize = TRUE),
      opp_ids_csv = csv_if_any(opp_ids, integerize = TRUE),
      opp_rank_side = blank_to_na_character(f$rank_side),
      opp_rank_n = blank_to_na_integer(f$rank_n),
      opp_rank_metric = blank_to_na_character(f$metric),
      home_away = blank_to_na_character(f$home_away),
      outcome = blank_to_na_character(f$outcome),
      max_margin = clutch$max_margin,
      margin_status = clutch$margin_status,
      max_time_remaining = clutch$max_time_remaining,
      ot_margin_filter = clutch$ot_margin_filter,
      min_gn = gp$min_gn,
      max_gn = gp$max_gn,
      last_n_games = gp$last_n
    )
  }

  run_player_traditional_dynamic <- function(pool, game_year, start_d, end_d,
                                             team_ids_csv, game_type_csv, opp_ids_csv,
                                             home_away, outcome, opp_rank_side, opp_rank_n, opp_rank_metric,
                                             max_margin, margin_status, max_time_remaining, ot_margin_filter,
                                             min_gn, max_gn, last_n_games) {
    allowed <- guard_heavy_request(
      session, key = "tab5_player_traditional",
      start_d = start_d, end_d = end_d,
      min_gn = min_gn, max_gn = max_gn, last_n = last_n_games,
      max_calls = 35L, window_sec = 60L
    )
    if (!isTRUE(allowed)) return(data.frame())
    db_get_query(
      pool,
      paste0(
        "SELECT * FROM basketball_test.get_player_traditional_dynamic(",
        "$1::int4,$2::date,$3::date,$4::text,$5::text,$6::text,$7::text,$8::text,$9::text,$10::int4,$11::text,",
        "$12::int4,$13::text,$14::int4,$15::bool,$16::int4,$17::int4,$18::int4",
        ")"
      ),
      params = list(
        as.integer(game_year),
        if (!is.na(start_d)) as.Date(start_d) else NA,
        if (!is.na(end_d)) as.Date(end_d) else NA,
        team_ids_csv,
        game_type_csv,
        opp_ids_csv,
        home_away,
        outcome,
        opp_rank_side,
        opp_rank_n,
        opp_rank_metric,
        max_margin,
        margin_status,
        max_time_remaining,
        ot_margin_filter,
        min_gn,
        max_gn,
        last_n_games
      )
    )
  }

  fallback_needed <- reactive({
    rng <- debounced_range()
    if (is.null(rng)) return(FALSE)

    start_d <- as.Date(rng[1])
    end_d <- as.Date(rng[2])
    if (is.na(start_d) || is.na(end_d)) return(FALSE)

    gy <- as.integer(input$game_year)
    season_bounds <- shared$season_date_bounds(gy)
    date_changed <- (start_d != season_bounds$start) || (end_d != season_bounds$end)

    f <- debounced_ts_filters()
    extra_filters <- (!is.null(f$game_type) && any(nzchar(f$game_type))) ||
      (!is.null(f$opp_ids) && length(f$opp_ids) > 0) ||
      nzchar(f$home_away %||% "") ||
      nzchar(f$outcome %||% "") ||
      nzchar(f$rank_side %||% "") ||
      isTRUE(f$clutch_enabled)

    gp <- gn_params()
    gn_active <- !is.na(gp$min_gn) || !is.na(gp$max_gn) || !is.na(gp$last_n)
    gn_raw_active <- nzchar(input$ts_gn_min %||% "") ||
      nzchar(input$ts_gn_max %||% "") ||
      nzchar(input$ts_last_n %||% "")

    date_changed || extra_filters || gn_active || gn_raw_active
  })

  mv_result_df <- reactive({
    req(identical(input$main_tabs, "traditional_stats"))
    gy_int <- as.integer(input$game_year)
    req(gy_int)

    out <- tryCatch(
      db_get_query(
        pg_pool,
        "SELECT *
         FROM basketball_test.player_traditional_stats_mv
         WHERE game_year = $1",
        params = list(gy_int)
      ),
      error = function(e) NULL
    )
    if (is.null(out)) return(NULL)
    out <- normalize_ts_result_cols(out)

    team_ids <- selected_team_ids()
    if (!is.null(team_ids) && length(team_ids) > 0) {
      out <- out %>% filter(team_id %in% !!team_ids)
    }

    out %>%
      clean_ts_rows() %>%
      add_ts_two_point_stats() %>%
      add_ts_usage_pct() %>%
      arrange(desc(pts), desc(minutes), team_name, Player)
  }) %>% bindEvent(input$main_tabs, input$game_year, debounced_teams())

  live_result_df <- reactive({
    req(identical(input$main_tabs, "traditional_stats"))

    gy_int <- as.integer(input$game_year)
    req(gy_int)
    rng <- debounced_range()
    req(rng)
    req(!is.na(rng[1]), !is.na(rng[2]))

    db_args <- build_ts_db_args()

    out <- tryCatch(
      run_player_traditional_dynamic(
        pg_pool,
        game_year = gy_int,
        start_d = as.Date(rng[1]),
        end_d = as.Date(rng[2]),
        team_ids_csv = db_args$team_ids_csv,
        game_type_csv = db_args$game_type_csv,
        opp_ids_csv = db_args$opp_ids_csv,
        home_away = db_args$home_away,
        outcome = db_args$outcome,
        opp_rank_side = db_args$opp_rank_side,
        opp_rank_n = db_args$opp_rank_n,
        opp_rank_metric = db_args$opp_rank_metric,
        max_margin = db_args$max_margin,
        margin_status = db_args$margin_status,
        max_time_remaining = db_args$max_time_remaining,
        ot_margin_filter = db_args$ot_margin_filter,
        min_gn = db_args$min_gn,
        max_gn = db_args$max_gn,
        last_n_games = db_args$last_n_games
      ),
      error = function(e) NULL
    )

    if (is.null(out) || !nrow(out)) return(NULL)

    out %>%
      normalize_ts_result_cols() %>%
      clean_ts_rows() %>%
      add_ts_two_point_stats() %>%
      add_ts_usage_pct() %>%
      arrange(desc(pts), desc(minutes), team_name, Player)
    }) %>% bindEvent(
    input$main_tabs,
    input$game_year,
    debounced_range(),
    debounced_teams(),
    debounced_ts_filters(),
    gn_params()
  )

  # Season-level (team, player) -> stable identity_id lookup, used to combine a
  # player's rows across teams. Keyed on source_player_id because the MV/dynamic
  # results carry provider (source) player ids. Ambiguous (team, player) pairs
  # that resolve to more than one identity are dropped (fail-safe).
  load_ts_identity_lookup <- function(gy_int) {
    raw <- tryCatch(
      cached_ref_query(
        key = sprintf("ts_identity_%d", gy_int),
        query_fun = function() {
          db_get_query(
            pg_pool,
            "SELECT team_id,
                    source_player_id AS player_id,
                    identity_id::text AS identity_id,
                    MIN(display_name) AS display_name
             FROM basketball_test.resolved_player_identity_v
             WHERE game_year = $1
             GROUP BY team_id, source_player_id, identity_id",
            params = list(gy_int)
          )
        }
      ),
      error = function(e) NULL
    )
    if (is.null(raw) || !nrow(raw) ||
        !all(c("team_id", "player_id", "identity_id") %in% names(raw))) {
      return(data.frame())
    }
    raw$team_id <- suppressWarnings(as.integer(raw$team_id))
    raw$player_id <- suppressWarnings(as.integer(raw$player_id))
    raw$identity_id <- as.character(raw$identity_id)
    raw <- raw[is.finite(raw$team_id) & is.finite(raw$player_id) & !is.na(raw$identity_id), , drop = FALSE]
    if (!nrow(raw)) return(raw)
    tp <- paste0(raw$team_id, ":", raw$player_id)
    ambiguous <- unique(tp[duplicated(tp)])
    raw[!(tp %in% ambiguous), , drop = FALSE]
  }

  # Full ranking population for the current data scope (season + team/date/game
  # filters), with multi-team TOTAL rows appended. The player-selection filter is
  # deliberately NOT applied here: percentiles are computed over this population so
  # coloring stays league-relative when the display is later narrowed to a few
  # players. Player selection is applied downstream in ts_display_context().
  population_df <- reactive({
    req(identical(input$main_tabs, "traditional_stats"))
    df <- NULL
    if (!isTRUE(fallback_needed())) {
      mv_df <- mv_result_df()
      if (!is.null(mv_df)) df <- mv_df
    }
    if (is.null(df)) df <- live_result_df()
    gy_int <- as.integer(input$game_year)
    lookup <- if (is.finite(gy_int)) load_ts_identity_lookup(gy_int) else data.frame()
    add_ts_multi_team_totals(df, lookup)
  }) %>% bindEvent(
    input$main_tabs,
    input$game_year,
    debounced_range(),
    debounced_teams(),
    debounced_ts_filters(),
    gn_params()
  )

  observeEvent(population_df(), ignoreInit = FALSE, {
    df <- population_df()
    max_gp <- 1L
    if (!is.null(df) && nrow(df) && "gp" %in% names(df)) {
      max_gp <- suppressWarnings(as.integer(max(df$gp, na.rm = TRUE)))
      if (!is.finite(max_gp) || is.na(max_gp) || max_gp < 1L) max_gp <- 1L
    }
    cur_num <- suppressWarnings(as.integer(input$ts_min_gp))
    cur_sld <- suppressWarnings(as.integer(input$ts_min_gp_slider))
    target <- max(1L, min(max_gp, dplyr::coalesce(cur_num, cur_sld, 1L)))
    updateSliderInput(session, "ts_min_gp_slider", min = 1, max = max_gp, value = target)
    updateNumericInput(session, "ts_min_gp", min = 1, max = max_gp, value = target)
  })

  ts_mode_context <- reactive({
    base_df <- population_df()
    if (is.null(base_df) || !nrow(base_df)) {
      return(list(df = base_df, x_poss = NA_real_, x_min = NA_real_, rate_threshold = 0))
    }

    min_gp <- suppressWarnings(as.integer(TS_NORM_MIN_GP))
    if (!is.finite(min_gp) || min_gp < 1) min_gp <- 1L
    pct <- suppressWarnings(as.numeric(TS_NORM_PCT))
    if (!is.finite(pct)) pct <- 75
    pct <- max(70, min(90, pct))

    # Derived TOTAL rows are excluded from the eligibility/rate population so a
    # multi-team player is not counted twice (per-team rows + their TOTAL).
    pop_df <- ts_drop_totals(base_df)
    if (!nrow(pop_df)) pop_df <- base_df

    df0 <- pop_df %>%
      mutate(
        poss_pg = ifelse(gp > 0, poss_on_floor / gp, NA_real_),
        min_pg = ifelse(gp > 0, minutes / gp, NA_real_)
      )

    eligible <- df0 %>%
      filter(gp >= min_gp, !is.na(poss_pg), !is.na(min_pg), poss_pg > 0, min_pg > 0)

    if (!nrow(eligible)) {
      eligible <- df0 %>%
        filter(gp > 0, !is.na(poss_pg), !is.na(min_pg), poss_pg > 0, min_pg > 0)
    }

    x_poss <- if (nrow(eligible)) as.numeric(stats::quantile(eligible$poss_pg, probs = pct / 100, na.rm = TRUE, type = 7)) else NA_real_
    x_min <- if (nrow(eligible)) as.numeric(stats::quantile(eligible$min_pg, probs = pct / 100, na.rm = TRUE, type = 7)) else NA_real_
    rate_threshold <- ts_rate_threshold(pop_df$poss_on_floor, TS_RATE_KEEP_PCT, TS_RATE_MIN_N)

    list(
      df = base_df,
      x_poss = x_poss,
      x_min = x_min,
      rate_threshold = rate_threshold
    )
  }) %>% bindEvent(population_df())

  # Mode-transform and rank the FULL population. Percentiles (pr_* columns) are
  # computed here, before any display narrowing, so each row's color reflects its
  # standing in the whole league for the current data scope. Raw possessions are
  # captured in `.poss_rank_base`/`total_poss` before the mode transform so the
  # rate-eligibility gate and the ranking baseline use season totals, not the
  # per-game/per-possession transformed values.
  ts_ranked_df <- reactive({
    ctx <- ts_mode_context()
    df <- ctx$df
    mode <- input$ts_display_mode %||% "Per Game"
    poss_threshold <- as.numeric(ctx$rate_threshold %||% 0)
    if (is.null(df) || !nrow(df)) {
      return(list(df = df, mode = mode, threshold = poss_threshold))
    }

    df$rate_eligible <- TRUE
    df$total_poss <- suppressWarnings(as.numeric(df$poss_on_floor))
    df$.poss_rank_base <- suppressWarnings(as.numeric(df$poss_on_floor))
    # Per-game playing-time base shown in rate modes (see ts_rate_base_col). Uses
    # raw possessions/minutes over gp before the mode transform; for a multi-team
    # TOTAL row gp is the summed games, so this reads as combined per-game.
    gp_n <- suppressWarnings(as.numeric(df$gp))
    raw_min <- suppressWarnings(as.numeric(df$minutes))
    df$base_poss_pg <- ifelse(is.finite(gp_n) & gp_n > 0, df$total_poss / gp_n, NA_real_)
    df$base_min_pg  <- ifelse(is.finite(gp_n) & gp_n > 0, raw_min / gp_n, NA_real_)
    if (identical(mode, "Per 60 Possessions") || identical(mode, "Per 30 Minutes")) {
      df$rate_eligible <- !is.na(df$.poss_rank_base) & df$.poss_rank_base >= poss_threshold
    }

    df <- apply_ts_mode(df, mode, x_poss = ctx$x_poss, x_min = ctx$x_min)
    df <- add_ts_percentile_cols(df)

    list(df = df, mode = mode, threshold = poss_threshold)
  }) %>% bindEvent(ts_mode_context(), input$ts_display_mode)

  ts_display_context <- reactive({
    ranked <- ts_ranked_df()
    df <- ranked$df
    mode <- ranked$mode
    show_ineligible <- isTRUE(input$ts_show_ineligible)
    poss_threshold <- as.numeric(ranked$threshold %||% 0)
    if (is.null(df) || !nrow(df)) {
      return(list(df = df, mode = mode, removed = 0L, ineligible = 0L, threshold = poss_threshold, show_ineligible = show_ineligible))
    }

    # Narrow the displayed rows (player selection, then Min GP). The pr_* columns
    # are already baked in from the full population, so these only hide rows.
    gy_int <- suppressWarnings(as.integer(input$game_year))
    lookup <- if (is.finite(gy_int)) load_ts_identity_lookup(gy_int) else data.frame()
    df <- filter_ts_players(df, debounced_players(), lookup)
    if (is.null(df) || !nrow(df)) {
      return(list(df = df, mode = mode, removed = 0L, ineligible = 0L, threshold = poss_threshold, show_ineligible = show_ineligible))
    }

    min_gp <- suppressWarnings(as.integer(input$ts_min_gp))
    if (!is.finite(min_gp) || is.na(min_gp) || min_gp < 1L) min_gp <- 1L
    df <- df %>% filter(coalesce(gp, 0L) >= min_gp)
    if (is.null(df) || !nrow(df)) {
      return(list(df = df, mode = mode, removed = 0L, ineligible = 0L, threshold = poss_threshold, show_ineligible = show_ineligible))
    }

    removed <- 0L
    ineligible <- 0L
    if (identical(mode, "Per 60 Possessions") || identical(mode, "Per 30 Minutes")) {
      keep <- !is.na(df$rate_eligible) & df$rate_eligible
      ineligible <- sum(!keep, na.rm = TRUE)
      if (!show_ineligible) {
        removed <- ineligible
        df <- df[keep, , drop = FALSE]
      }
    }

    df <- apply_ts_stat_filters(df, ts_stat_filters())

    list(df = df, mode = mode, removed = removed, ineligible = ineligible, threshold = poss_threshold, show_ineligible = show_ineligible)
  }) %>% bindEvent(ts_ranked_df(), input$ts_show_ineligible, input$ts_min_gp, input$ts_min_gp_slider, ts_stat_filters(), debounced_players())

  output$ts_mode_warning <- renderUI({
    disp_ctx <- ts_display_context()
    req(!is.null(disp_ctx$mode))
    if (!(identical(disp_ctx$mode, "Per 60 Possessions") || identical(disp_ctx$mode, "Per 30 Minutes"))) return(NULL)
    if (isTRUE(disp_ctx$show_ineligible)) {
      if (!isTRUE(disp_ctx$ineligible > 0)) return(NULL)
      return(
        div(
          class = "alert alert-info py-2 mb-2",
          sprintf(
            "%d non-eligible players shown in gray in %s (below %s total possessions).",
            as.integer(disp_ctx$ineligible),
            disp_ctx$mode,
            format(as.integer(disp_ctx$threshold), big.mark = ",")
          )
        )
      )
    }
    if (!isTRUE(disp_ctx$removed > 0)) return(NULL)
    div(class = "alert alert-warning py-2 mb-2",
        sprintf("%d players hidden in %s (below %s total possessions).",
                as.integer(disp_ctx$removed), disp_ctx$mode, format(as.integer(disp_ctx$threshold), big.mark = ",")))
  }) %>% bindEvent(ts_display_context(), input$main_tabs)

  output$ts_table <- DT::renderDataTable({
    req(identical(input$main_tabs, "traditional_stats"))
    disp_ctx <- ts_display_context()
    df <- disp_ctx$df
    if (is.null(df) || nrow(df) == 0) {
      return(DT::datatable(
        data.frame(Info = ts_no_data_message(input$ts_players), check.names = FALSE),
        rownames = FALSE,
        options = list(headerCallback = HEADER_TOOLTIP_JS, dom = "t", ordering = FALSE)
      ))
    }
    mode <- disp_ctx$mode
    if (!("is_multi_team_total" %in% names(df))) df$is_multi_team_total <- FALSE
    df <- ts_group_display_order(df)

    disp <- df %>%
      transmute(
        Team = team_name,
        Player,
        GP = gp,
        `Poss On Floor` = poss_on_floor,
        Min = minutes,
        PTS = pts,
        REB = reb,
        OREB = oreb,
        DREB = dreb,
        AST = ast,
        STL = stl,
        BLK = blk,
        TOV = tov,
        FGM = fgm,
        FGA = fga,
        `FG%` = fg_pct,
        `2PM` = `2pm`,
        `2PA` = `2pa`,
        `2P%` = two_pct,
        `3PM` = `3pm`,
        `3PA` = `3pa`,
        `3P%` = tp_pct,
        FTM = ftm,
        FTA = fta,
        `FT%` = ft_pct,
        `eFG%` = efg,
        `TS%` = ts,
        `USG%` = usg_pct,
        `.poss_rank_base` = coalesce(.poss_rank_base, NA_real_),
        `.eligible_rate` = coalesce(rate_eligible, TRUE),
        `.is_total` = coalesce(is_multi_team_total, FALSE)
      )
    # Surface the player's real accumulated base for the mode (Total Min in
    # minutes modes, Total Poss otherwise), since rate modes normalize the
    # per-rate column to a constant and hide the actual sample size.
    base_col <- ts_rate_base_col(mode)
    if (!is.null(base_col) && base_col$src %in% names(df) && base_col$after %in% names(disp)) {
      disp[[base_col$label]] <- df[[base_col$src]]
      disp <- dplyr::relocate(disp, dplyr::all_of(base_col$label),
                              .after = dplyr::all_of(base_col$after))
    }

    # Percentile (pr_*) columns were already computed over the full population in
    # ts_ranked_df(); carry them onto the row-aligned display frame so coloring is
    # league-relative and unchanged by the player/Min GP/stat narrowing above.
    heat_good <- c("PTS", "REB", "OREB", "DREB", "AST", "STL", "BLK", "FGM", "FGA", "FG%", "2PM", "2PA", "2P%", "3PM", "3PA", "3P%", "FTM", "FTA", "FT%", "eFG%", "TS%", "USG%")
    src_pr_cols <- grep("^pr_", names(df), value = TRUE)
    disp[src_pr_cols] <- df[src_pr_cols]

    pr_cols <- names(disp)[grepl("^pr_", names(disp))]
    hidden_cols <- c(".eligible_rate", ".poss_rank_base", ".is_total", pr_cols)
    disp <- apply_visible_col_order(disp, isolate(input$ts_visible_col_order), hidden_cols)

    round_cols <- setdiff(names(disp), c("Team", "Player", "GP", ".eligible_rate", ".poss_rank_base", ".is_total", pr_cols))
    style_cols <- setdiff(names(disp), c(".eligible_rate", ".is_total"))

    dt <- DT::datatable(
      disp,
      rownames = FALSE,
      extensions = c("Buttons", "ColReorder"),
      options = list(
        headerCallback = HEADER_TOOLTIP_JS,
        initComplete = dt_col_order_init_callback("ts_visible_col_order", "onoff.ts.visible_col_order.v1"),
        colReorder = TRUE,
        dom = "Btip",
        buttons = list(
          list(
            extend = "csv",
            text = "Download CSV",
            filename = sprintf("traditional_player_stats_%s", csv_export_stamp()),
            exportOptions = list(columns = ":visible", stripHtml = TRUE)
          )
        ),
        pageLength = 50,
        deferRender = TRUE,
        scrollX = TRUE,
        scrollY = "70vh",
        scrollCollapse = TRUE,
        order = list(),
        columnDefs = list(
          list(className = "dt-center", targets = "_all"),
          list(visible = FALSE, targets = which(names(disp) %in% hidden_cols) - 1L)
        )
      )
    ) %>%
      DT::formatRound(intersect(round_cols, names(disp)), 1)

    apply_heat <- function(dt_obj, col_name, reverse = FALSE) {
      pr_col <- paste0("pr_", gsub("[^A-Za-z0-9]+", "_", col_name))
      if (!(col_name %in% names(disp)) || !(pr_col %in% names(disp))) return(dt_obj)
      DT::formatStyle(
        dt_obj,
        col_name,
        backgroundColor = DT::styleInterval(CUTS, if (isTRUE(reverse)) COLS_REV else COLS_GRAD),
        valueColumns = pr_col
      )
    }

    for (col_name in heat_good) dt <- apply_heat(dt, col_name, reverse = FALSE)
    dt <- apply_heat(dt, "TOV", reverse = TRUE)

    dt <- dt %>%
      DT::formatStyle(
        columns = style_cols,
        valueColumns = ".eligible_rate",
        color = DT::styleEqual(c(TRUE, FALSE), c("inherit", "#6e7681"))
      ) %>%
      DT::formatStyle(
        columns = style_cols,
        valueColumns = ".is_total",
        fontWeight = DT::styleEqual(c(TRUE, FALSE), c("bold", "normal"))
      )

    # Multi-team TOTAL rows keep heat coloring but are distinguished by bold text
    # and a subtle amber tint on the identity (Team/Player) cells.
    tint_cols <- intersect(c("Team", "Player"), names(disp))
    if (length(tint_cols)) {
      dt <- dt %>%
        DT::formatStyle(
          columns = tint_cols,
          valueColumns = ".is_total",
          backgroundColor = DT::styleEqual(TRUE, "rgba(232, 164, 53, 0.18)")
        )
    }
    dt
  }, server = FALSE) %>% bindEvent(ts_display_context(), input$main_tabs, input$ts_visible_col_order_restore, ignoreNULL = FALSE)

  # ---- Filter Chips ----
  output$ts_filter_chips <- renderUI({
    team_map <- if (!is.null(ts_ref$teams) && nrow(ts_ref$teams)) {
      stats::setNames(as.character(ts_ref$teams$team_name), as.character(ts_ref$teams$team_id))
    } else {
      NULL
    }
    stat_filter_choices <- names(TS_FILTERABLE_COLS)
    if (identical(input$ts_display_mode %||% "Per Game", "Totals")) {
      stat_filter_choices <- setdiff(stat_filter_choices, "Total Poss")
    }
    stat_chips <- lapply(ts_stat_filters(), function(f) {
      op_sym <- if (identical(f$op, "ge")) "\u2265" else "\u2264"
      val_txt <- format(f$value, big.mark = ",", trim = TRUE)
      label <- sprintf("%s %s %s", f$label, op_sym, val_txt)
      tags$span(
        class = "filter-chip chip-stat",
        label, " ",
        tags$a(
          href = "#",
          class = "js-shiny-event",
          `data-input-id` = "ts_remove_stat_filter",
          `data-shiny-value` = as.character(as.integer(f$id)),
          style = "margin-left:4px;color:inherit;",
          "\u00d7"
        )
      )
    })

    player_chips <- list()
    selected_players <- input$ts_players %||% character(0)
    if (length(selected_players)) {
      gy_int <- suppressWarnings(as.integer(input$game_year))
      lk <- if (length(gy_int) && is.finite(gy_int)) load_ts_identity_lookup(gy_int) else NULL
      choice_map <- ts_player_choices(ts_ref$players, ts_ref$teams, lookup = lk)
      label_map <- stats::setNames(names(choice_map), unname(choice_map))
      selected_labels <- unname(label_map[as.character(selected_players)])
      selected_labels[is.na(selected_labels) | !nzchar(selected_labels)] <- as.character(selected_players)[is.na(selected_labels) | !nzchar(selected_labels)]
      chip_label <- if (length(selected_labels) == 1) {
        paste("Player:", selected_labels[1])
      } else {
        paste0(length(selected_labels), " players")
      }
      player_chips <- list(make_chip(chip_label, "ts_clear_players", "chip-game"))
    }

    add_btn <- bslib::popover(
      trigger = tags$span(
        class = "filter-chip filter-chip-add",
        id = "ts_stat_filter_add_btn",
        tags$i(class = "bi bi-plus"), " Filter"
      ),
      title = "Add stat filter",
      placement = "bottom",
      div(
        class = "ts-stat-popover",
        style = "min-width: 220px;",
        selectInput(
          "ts_stat_filter_col", "Column",
          choices = c("Choose..." = "", stat_filter_choices),
          selected = "",
          width = "100%"
        ),
        radioButtons(
          "ts_stat_filter_op", "Operator",
          choices = c("\u2265" = "ge", "\u2264" = "le"),
          selected = "ge",
          inline = TRUE
        ),
        numericInput(
          "ts_stat_filter_value", "Value",
          value = NA, width = "100%"
        ),
        tags$div(
          class = "small text-muted mb-2",
          "Percent columns (FG%, 2P%, 3P%, FT%, eFG%, TS%, USG%): enter as 0\u2013100."
        ),
        actionButton(
          "ts_add_stat_filter", "Add",
          class = "btn-sm btn-primary w-100"
        )
      )
    )

    build_filter_chips(
      "ts",
      input,
      shared$season_date_bounds,
      reset_btn_id = "ts_reset",
      team_label_map = team_map,
      opponent_label_map = team_map,
      extra_children = c(player_chips, stat_chips, list(add_btn))
    )
  })
  setup_chip_clears("ts", session, input, shared,
    game_type_id = "ts_game_type", opponents_id = "ts_opponents",
    home_away_id = "ts_home_away", outcome_id = "ts_outcome",
    gn_min_id = "ts_gn_min", gn_max_id = "ts_gn_max", last_n_id = "ts_last_n",
    opp_rank_ids = c("ts_opp_rank_side", "ts_opp_rank_n", "ts_opp_rank_metric"),
    date_id = "ts_dates", gy_input_id = "game_year",
    teams_ids = "ts_teams",
    clutch_enabled_id = "ts_clutch_enabled")
}


