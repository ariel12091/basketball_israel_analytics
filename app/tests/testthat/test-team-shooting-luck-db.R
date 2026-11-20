library(testthat)

# Team Ratings shooting luck (docs/shared_definitions.md, "Shooting luck
# (Team Ratings)"): the web derives it from six counts the Team Ratings
# readers return -- own and opponents' 3PM, FTM and FTA. These pins hold
# those counts to the Traditional reader, which counts the same actions,
# and the season view to the filtered function (rule 10).

skip_if_not(identical(Sys.getenv("RUN_DB_TESTS"), "1"))
skip_if_not_installed("DBI")
skip_if_not_installed("RPostgres")

pg_env <- c("PG_HOST", "PG_PORT", "PG_DB", "PG_USER", "PG_PASS")
missing_env <- pg_env[!nzchar(Sys.getenv(pg_env))]
if (length(missing_env)) {
  skip(paste("Missing DB env:", paste(missing_env, collapse = ", ")))
}

con <- db_test_connect(
  RPostgres::Postgres(),
  host = Sys.getenv("PG_HOST"),
  port = as.integer(Sys.getenv("PG_PORT")),
  dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE", unset = "require")
)
on.exit(DBI::dbDisconnect(con), add = TRUE)

# Fixed SQL, immediate mode (see test-team-minutes-db.R).
dbq <- function(sql) DBI::dbGetQuery(con, sql, immediate = TRUE)

LUCK_COUNTS <- c("off_fg3_made", "def_fg3_made", "off_ftm", "off_fta",
                 "def_ftm", "def_fta")
IL_ARGS <- c(full = "", clutch = ", p_max_margin := 5, p_max_time_remaining := 300")

il_ratings <- function(season, args) {
  dbq(sprintf("SELECT * FROM basketball_test.get_team_ratings_dynamic(%d%s)",
              season, args))
}
il_traditional <- function(season, args, side) {
  dbq(sprintf(
    "SELECT team_id, \"3pm\" AS fg3m, ftm, fta FROM basketball_test.get_team_traditional_dynamic(%d%s, p_side := '%s')",
    season, args, side))
}

test_that("Israeli ratings function returns 3PM and FT counts equal to the Traditional reader's", {
  for (season in db_test_seasons()) for (k in names(IL_ARGS)) {
    r <- il_ratings(season, IL_ARGS[[k]])
    label <- paste(season, k)
    expect_true(all(LUCK_COUNTS %in% names(r)), label = paste(label, "has the counts"))
    if (!all(LUCK_COUNTS %in% names(r))) next
    for (side in c("off", "def")) {
      t <- il_traditional(season, IL_ARGS[[k]], if (side == "off") "offense" else "defense")
      m <- merge(r, t, by = "team_id")
      expect_equal(nrow(m), nrow(r), label = paste(label, side, "teams"))
      expect_equal(as.numeric(m[[paste0(side, "_fg3_made")]]), as.numeric(m$fg3m), label = paste(label, side, "3PM"))
      expect_equal(as.numeric(m[[paste0(side, "_ftm")]]), as.numeric(m$ftm), label = paste(label, side, "FTM"))
      expect_equal(as.numeric(m[[paste0(side, "_fta")]]), as.numeric(m$fta), label = paste(label, side, "FTA"))
    }
  }
})

# No date window: 2026-27 began on 8 September (cup games), before the
# 1 October start of the "explicit full-season window".
test_that("Israeli season view counts equal the ratings function over the whole season", {
  for (season in db_test_seasons()) {
    v <- dbq(sprintf("SELECT * FROM basketball_test.team_ppp_ratings_mv WHERE game_year = %d", season))
    expect_true(all(LUCK_COUNTS %in% names(v)), label = paste(season, "view has the counts"))
    if (!all(LUCK_COUNTS %in% names(v))) next
    f <- il_ratings(season, "")
    m <- merge(v, f, by = "team_id", suffixes = c(".v", ".f"))
    expect_equal(nrow(m), nrow(v), label = paste(season, "teams"))
    for (col in c(LUCK_COUNTS, "off_poss", "def_poss"))
      expect_equal(as.numeric(m[[paste0(col, ".v")]]), as.numeric(m[[paste0(col, ".f")]]),
                   label = paste(season, col))
  }
})

# EuroLeague (migration 068): eight counts, 3PA included (its readers had none).
EL_COUNTS <- c("off_fg3_made", "off_fg3_att", "def_fg3_made", "def_fg3_att",
               "off_ftm", "off_fta", "def_ftm", "def_fta")
EL_CLUTCH <- ", p_max_margin => 5, p_margin_status => 'all', p_max_time_remaining => 300"
el_read <- function(fn, comp, season, args = "", cols = "*") {
  dbq(sprintf("SELECT %s FROM euroleague.%s('%s', %d%s)", cols, fn, comp, season, args))
}
same_counts <- function(a, b, cols, label) {
  m <- merge(a, b, by = "team_id", suffixes = c(".a", ".b"))
  expect_equal(nrow(m), nrow(a), label = paste(label, "teams"))
  for (col in cols)
    expect_equal(as.numeric(m[[paste0(col, ".a")]]), as.numeric(m[[paste0(col, ".b")]]),
                 label = paste(label, col))
}

test_that("EuroLeague Team Ratings readers return the same eight counts on every path", {
  for (comp in c("E", "U")) for (season in db_test_euro_seasons()) {
    label <- paste(comp, season)
    pg <- el_read("get_team_ratings_pergame", comp, season)
    expect_true(all(EL_COUNTS %in% names(pg)), label = paste(label, "pergame has the counts"))
    if (!all(EL_COUNTS %in% names(pg))) next
    mv <- dbq(sprintf("SELECT * FROM euroleague.team_ppp_ratings_mv WHERE competition = '%s' AND game_year = %d", comp, season))
    same_counts(mv, pg, c(EL_COUNTS, "off_poss", "def_poss"), paste(label, "view vs pergame"))
    same_counts(el_read("get_team_ratings_direct", comp, season), pg, EL_COUNTS, paste(label, "direct vs pergame"))
    dyn <- el_read("get_team_ratings_dynamic", comp, season, EL_CLUTCH)
    same_counts(el_read("get_team_dashboard_dynamic", comp, season, EL_CLUTCH), dyn, EL_COUNTS,
                paste(label, "dashboard vs dynamic, standard clutch"))
    same_counts(el_read("get_team_ratings_direct", comp, season, EL_CLUTCH), dyn, EL_COUNTS,
                paste(label, "direct vs dynamic, standard clutch"))
  }
})

test_that("EuroLeague ratings counts equal the Traditional reader's", {
  trad_cols <- "team_id, \"3pm\" AS fg3_made, \"3pa\" AS fg3_att, ftm, fta"
  for (comp in c("E", "U")) for (season in db_test_euro_seasons()) for (side in c("off", "def")) {
    label <- paste(comp, season, side)
    p_side <- sprintf(", p_side => '%s'", if (side == "off") "offense" else "defense")
    rename <- function(d) { names(d)[-1] <- paste0(side, "_", names(d)[-1]); d }
    cols <- paste0(side, "_", c("fg3_made", "fg3_att", "ftm", "fta"))
    pg <- el_read("get_team_ratings_pergame", comp, season)
    if (!all(cols %in% names(pg))) { expect_true(FALSE, label = paste(label, "has the counts")); next }
    same_counts(pg, rename(el_read("get_team_traditional_pergame", comp, season, p_side, trad_cols)), cols,
                paste(label, "pergame vs Traditional"))
    same_counts(el_read("get_team_ratings_dynamic", comp, season, EL_CLUTCH),
                rename(el_read("get_team_traditional_direct", comp, season, paste0(EL_CLUTCH, p_side), trad_cols)), cols,
                paste(label, "standard clutch vs Traditional"))
  }
})
