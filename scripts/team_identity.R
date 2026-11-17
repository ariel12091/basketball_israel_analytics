# Team logos for basketball_test.season_teams: the one owner of the rule that
# picks a team's logo in a season. Sourced by scripts/apply_catalogue_migrations.R,
# scripts/sync_catalogues.R and scripts/export_web_images.R.
#
# A team's logo is the icon on its latest game that season in the schedule
# feed (team_icon_1/2), keyed by the roster team id through
# schedule_team_dict, made absolute by basket_absolute_url().

if (!exists("basket_absolute_url")) source("scripts/basket_rosters.R")

#' One row per (game_year, team_id) with a logo: game_year, team_id, url.
season_team_logos <- function(con) {
  logos <- DBI::dbGetQuery(con, "
    WITH sides AS (
      SELECT game_year, game_date, id, team1 AS team_id_schedule, team_icon_1 AS icon
      FROM basketball_test.schedule
      UNION ALL
      SELECT game_year, game_date, id, team2, team_icon_2
      FROM basketball_test.schedule
    )
    SELECT DISTINCT ON (s.game_year, d.team_id_rosters)
           s.game_year, d.team_id_rosters AS team_id, s.icon
    FROM sides s
    JOIN basketball_test.schedule_team_dict d
      ON d.game_year = s.game_year AND d.team_id_schedule = s.team_id_schedule
    WHERE NULLIF(btrim(s.icon), '') IS NOT NULL
    ORDER BY s.game_year, d.team_id_rosters, s.game_date DESC NULLS LAST, s.id DESC")
  logos$url <- basket_absolute_url(logos$icon)
  logos[!is.na(logos$url), c("game_year", "team_id", "url")]
}

#' Writes logo_url where season_teams has none (a hand-corrected URL is never
#' overwritten). Returns the number of rows filled.
fill_season_team_logos <- function(con) {
  logos <- season_team_logos(con)
  filled <- 0L
  for (i in seq_len(nrow(logos))) {
    filled <- filled + DBI::dbExecute(
      con,
      "UPDATE basketball_test.season_teams
         SET logo_url = $3
       WHERE game_year = $1 AND team_id = $2 AND logo_url IS NULL",
      params = list(
        as.integer(logos$game_year[i]),
        as.integer(logos$team_id[i]),
        logos$url[i]
      )
    )
  }
  filled
}
