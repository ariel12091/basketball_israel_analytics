# global_euro.R - EuroLeague/EuroCup constants and reference lookups (Tab 8).
#
# Deliberately SEPARATE from the four Israeli canonical lookups
# (fetch_teams_distinct / fetch_teams_min / fetch_gn_values /
# fetch_players_basic). Those key their caches on game_year alone, so making
# them league-aware would let the two leagues serve each other's dropdowns.
# Every key here is scoped by competition AND season.
#
# Contract deviations from the Israeli side, all owned by migration 004:
#   * p_game_year is the PROVIDER season (2025 = the 2025-26 season), not the
#     season-ending year. This section owns its own selector, so no adapter.
#   * GN filters round_number, not gamecode. A gamecode range would mean
#     "league games 5-10", which is not what a GN range promises.
#   * Phase is provider text ('RS', 'PLAYOFFS', ...), not an integer code.

EURO_DEFAULT_COMPETITION <- "E"
EURO_DEFAULT_SEASON      <- "2025"

EURO_COMPETITION_LABELS <- c("E" = "EuroLeague", "U" = "EuroCup")

# Season window for a provider season: Sep 1 (Y) through Jul 1 (Y+1).
# Wider than the Israeli Oct 1 start because EuroLeague tips off in late Sep.
euro_season_date_bounds <- function(season = EURO_DEFAULT_SEASON) {
  y <- suppressWarnings(as.integer(season))
  if (length(y) != 1L || is.na(y)) y <- as.integer(EURO_DEFAULT_SEASON)
  list(start = as.Date(sprintf("%04d-09-01", y)),
       end   = as.Date(sprintf("%04d-07-01", y + 1L)))
}

EURO_DEFAULT_START <- euro_season_date_bounds(EURO_DEFAULT_SEASON)$start
EURO_DEFAULT_END   <- euro_season_date_bounds(EURO_DEFAULT_SEASON)$end

# Label a provider season the way the rest of the app labels seasons.
euro_season_label <- function(season) {
  y <- suppressWarnings(as.integer(season))
  if (length(y) != 1L || is.na(y)) return(as.character(season))
  sprintf("%02d-%02d", y %% 100L, (y + 1L) %% 100L)
}

# ---------------- Reference lookups ----------------
# One cached key per dataset per (competition, season). All read the app-facing
# MV, never the base tables, so they stay valid while cold-storage-style
# intermediate relations are empty.

euro_fetch_competitions <- function() {
  cached_ref_query(
    key = "euro_competitions",
    query_fun = function() db_get_query(
      pg_pool,
      "SELECT DISTINCT competition
         FROM euroleague.final_schedule_mv
        ORDER BY competition"
    )
  )
}

euro_fetch_seasons <- function(competition = EURO_DEFAULT_COMPETITION) {
  competition <- as.character(competition)
  cached_ref_query(
    key = sprintf("euro_seasons_%s", competition),
    query_fun = function() db_get_query(
      pg_pool,
      "SELECT DISTINCT game_year
         FROM euroleague.final_schedule_mv
        WHERE competition = $1::text
        ORDER BY game_year DESC",
      params = list(competition)
    )
  )
}

euro_fetch_teams <- function(competition, season) {
  competition <- as.character(competition)
  season <- as.integer(season)
  cached_ref_query(
    key = sprintf("euro_teams_%s_%d", competition, season),
    query_fun = function() db_get_query(
      pg_pool,
      "SELECT team_id, MIN(team_name) AS team_name
         FROM euroleague.final_schedule_mv
        WHERE competition = $1::text AND game_year = $2::int4
        GROUP BY team_id
        ORDER BY MIN(team_name)",
      params = list(competition, season)
    )
  )
}

# Players for the Tab 10 players-on/off pool. Its own cache key: sharing the
# Israeli fetch_players_basic() key would serve one league's roster to the
# other.
euro_fetch_players_basic <- function(competition, season) {
  competition <- as.character(competition)
  season <- as.integer(season)
  cached_ref_query(
    key = sprintf("euro_players_%s_%d", competition, season),
    query_fun = function() db_get_query(
      pg_pool,
      "SELECT DISTINCT fr.player_id, p.display_name AS player_name, fr.team_id
         FROM euroleague.full_rosters fr
         JOIN euroleague.players p ON p.player_id = fr.player_id
         JOIN euroleague.schedule s ON s.game_id = fr.game_id
        WHERE s.competition = $1::text AND s.season = $2::int4
          AND lower(p.provider_player_id) NOT IN ('team', 'total')
        ORDER BY p.display_name",
      params = list(competition, season)
    )
  )
}

# Distinct ROUND numbers for the GN / Last-N dropdowns.
euro_fetch_round_values <- function(competition, season) {
  competition <- as.character(competition)
  season <- as.integer(season)
  cached_ref_query(
    key = sprintf("euro_rounds_%s_%d", competition, season),
    query_fun = function() db_get_query(
      pg_pool,
      "SELECT DISTINCT round_number AS gn
         FROM euroleague.final_schedule_mv
        WHERE competition = $1::text AND game_year = $2::int4
          AND round_number IS NOT NULL
        ORDER BY round_number",
      params = list(competition, season)
    )
  )
}

euro_fetch_phases <- function(competition, season) {
  competition <- as.character(competition)
  season <- as.integer(season)
  cached_ref_query(
    key = sprintf("euro_phases_%s_%d", competition, season),
    query_fun = function() db_get_query(
      pg_pool,
      "SELECT DISTINCT phase
         FROM euroleague.final_schedule_mv
        WHERE competition = $1::text AND game_year = $2::int4
          AND phase IS NOT NULL
        ORDER BY phase",
      params = list(competition, season)
    )
  )
}

# Cache-busting token for the season-level MV pulls. The Israeli
# shared$data_version tracks the Israeli ETL only, so a EuroLeague publication
# would not invalidate anything keyed on it.
euro_data_version <- function() {
  res <- tryCatch(
    cached_ref_query(
      key = "euro_data_version",
      query_fun = function() db_get_query(
        pg_pool,
        "SELECT COALESCE(MAX(completed_at), MAX(started_at)) AS v
           FROM euroleague.load_runs
          WHERE status = 'completed'"
      )
    ),
    error = function(e) NULL
  )
  if (is.null(res) || !NROW(res) || is.na(res$v[[1]])) return("na")
  as.character(res$v[[1]])
}

# Phase codes are provider text; label the ones we know, pass through the rest
# so a new phase never renders as a blank option.
# ---------------- Shared section-level selectors ----------------
# ONE competition + season pair for the whole EuroLeague section, living in the
# navbar beside the Israeli season selector. Every EuroLeague tab reads
# input$euro_competition / input$euro_game_year, so changing season once
# changes it everywhere -- the Israeli app's global-season behaviour, scoped to
# this league. Visibility is by league class; see app.css.

euro_navbar_season_ui <- function() {
  tags$div(
    class = "navbar-season-select league-nav-el",
    selectInput("euro_competition", NULL,
                choices = stats::setNames(EURO_DEFAULT_COMPETITION,
                                          EURO_COMPETITION_LABELS[[EURO_DEFAULT_COMPETITION]]),
                selected = EURO_DEFAULT_COMPETITION),
    selectInput("euro_game_year", NULL,
                choices = stats::setNames(EURO_DEFAULT_SEASON,
                                          euro_season_label(EURO_DEFAULT_SEASON)),
                selected = EURO_DEFAULT_SEASON)
  )
}

# Populate those two from what is actually loaded. Called ONCE from app.R --
# if each tab did this they would fight over the same inputs.
euro_init_season_inputs <- function(input, session) {
  observe({
    comps <- tryCatch(euro_fetch_competitions(), error = function(e) NULL)
    codes <- if (!is.null(comps) && nrow(comps)) as.character(comps$competition) else EURO_DEFAULT_COMPETITION
    labels <- unname(EURO_COMPETITION_LABELS[codes])
    labels[is.na(labels)] <- codes[is.na(labels)]
    updateSelectInput(session, "euro_competition",
                      choices = stats::setNames(codes, labels),
                      selected = isolate(input$euro_competition) %||% EURO_DEFAULT_COMPETITION)
  })

  observeEvent(input$euro_competition, {
    comp <- input$euro_competition %||% EURO_DEFAULT_COMPETITION
    seasons <- tryCatch(euro_fetch_seasons(comp), error = function(e) NULL)
    vals <- if (!is.null(seasons) && nrow(seasons)) as.character(seasons$game_year) else EURO_DEFAULT_SEASON
    sel <- isolate(input$euro_game_year) %||% EURO_DEFAULT_SEASON
    if (!sel %in% vals) sel <- vals[[1]]
    updateSelectInput(session, "euro_game_year",
                      choices = stats::setNames(vals, euro_season_label(vals)),
                      selected = sel)
  }, ignoreInit = FALSE)
}

# Read helpers, so no tab has to repeat the defaulting logic.
euro_selected_competition <- function(input) {
  val <- input$euro_competition %||% EURO_DEFAULT_COMPETITION
  if (!nzchar(val)) EURO_DEFAULT_COMPETITION else as.character(val)
}

euro_selected_game_year <- function(input) {
  val <- input$euro_game_year %||% EURO_DEFAULT_SEASON
  if (!nzchar(val)) EURO_DEFAULT_SEASON else as.character(val)
}

EURO_PHASE_LABELS <- c(
  "RS" = "Regular Season",
  "PLAYOFFS" = "Playoffs",
  "FF" = "Final Four",
  "TS" = "Top 16"
)

euro_phase_label <- function(x) {
  x <- as.character(x)
  lbl <- unname(EURO_PHASE_LABELS[x])
  lbl[is.na(lbl)] <- x[is.na(lbl)]
  lbl
}
