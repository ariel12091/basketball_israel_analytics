# =============================================================================
# etl/dq_consecutive_possessions.R
#
# Data quality: a team ends two possessions in a row in one period, with no
# opponent possession between them. Both leagues' reports run this one rule; a
# league supplies only how to read its possession ends and how to label a game.
# Read-only. Pinned by app/tests/testthat/test-dq-consecutive-possessions.R.
# =============================================================================

# `ends_sql` yields one row per possession end:
# game_id, period, event_id, team_id, end_type, clock.
consecutive_possession_pairs_sql <- function(ends_sql) {
  paste0(
    "WITH ends AS (", ends_sql, "),
     ordered AS (
       SELECT e.*,
              LAG(e.team_id)  OVER w AS previous_team_id,
              LAG(e.event_id) OVER w AS first_end_event_id,
              LAG(e.clock)    OVER w AS first_end_clock,
              LAG(e.end_type) OVER w AS first_end
         FROM ends e
       WINDOW w AS (PARTITION BY e.game_id, e.period ORDER BY e.event_id)
     )
     SELECT o.game_id, o.period, o.team_id,
            o.first_end_clock, o.clock AS second_end_clock,
            o.first_end, o.end_type AS second_end,
            o.first_end_event_id, o.event_id AS second_end_event_id
       FROM ordered o
      WHERE o.team_id = o.previous_team_id"
  )
}

CONSECUTIVE_POSSESSION_LEAGUES <- list(
  israeli = list(
    schema = "basketball_test",
    ends = function(q) paste0(
      "SELECT d.game_id, d.quarter::integer AS period, d.id AS event_id, d.team_id,
              CONCAT_WS(' ', d.type, d.parameters_type, d.parameters_made) AS end_type,
              TO_CHAR(MAKE_INTERVAL(secs => GREATEST(0, ROUND(
                CASE WHEN d.quarter <= 4 THEN d.quarter * 600
                     ELSE 2400 + (d.quarter - 4) * 300 END
                - d.event_elapsed_seconds))::double precision), 'FMMI:SS') AS clock
         FROM ", q("df_pts_poss_lineups_longer_mv"), " d
        WHERE d.final_end_poss AND d.type_lineup = 'offense'"
    ),
    labelled = function(q, pairs) paste0(
      "SELECT s.game_year, s.game_date, p.game_id, p.team_id, t.team_name, p.period,
              p.first_end_clock, p.second_end_clock, p.first_end, p.second_end,
              p.first_end_event_id, p.second_end_event_id
         FROM (", pairs, ") p
         LEFT JOIN (SELECT DISTINCT ON (game_id) game_id, game_year, game_date
                      FROM ", q("schedule"), " ORDER BY game_id) s ON s.game_id = p.game_id
         LEFT JOIN ", q("season_teams"), " t ON t.game_year = s.game_year AND t.team_id = p.team_id
        ORDER BY s.game_date DESC NULLS LAST, p.game_id, p.period, p.second_end_event_id"
    )
  ),
  euroleague = list(
    schema = "euroleague",
    # The provider keeps PERIOD = 5 for every overtime; the context fact has the
    # effective period. It has no rows for a game whose derived build failed, and
    # such a game still has possession ends worth checking, hence the LEFT JOIN.
    ends = function(q) paste0(
      "SELECT a.game_id, COALESCE(c.period, a.period) AS period, a.source_event_order AS event_id,
              a.possession_offense_team_id AS team_id,
              CONCAT_WS(' ', a.play_type, a.endpoint_reason) AS end_type,
              a.marker_time AS clock
         FROM ", q("actions"), " a
         LEFT JOIN ", q("action_team_context_actions"), " c
           ON c.game_id = a.game_id AND c.source_event_order = a.source_event_order
          AND c.team_id = a.possession_offense_team_id AND c.type_lineup = 'offense'
        WHERE a.end_possession"
    ),
    labelled = function(q, pairs) paste0(
      "SELECT s.competition, s.season, s.gamecode, s.scheduled_at::date AS game_date,
              p.game_id, p.team_id, t.display_name AS team_name, p.period,
              p.first_end_clock, p.second_end_clock, p.first_end, p.second_end,
              p.first_end_event_id, p.second_end_event_id
         FROM (", pairs, ") p
         LEFT JOIN ", q("schedule"), " s ON s.game_id = p.game_id
         LEFT JOIN ", q("teams"), " t ON t.team_id = p.team_id
        ORDER BY s.season DESC, s.competition, s.gamecode, p.period, p.second_end_event_id"
    )
  )
)

# One row per repeat, with the game and both time stamps.
consecutive_possession_details <- function(con, league, schema = NULL) {
  spec <- CONSECUTIVE_POSSESSION_LEAGUES[[league]]
  if (is.null(spec)) stop("Unknown league: ", league)
  schema <- schema %||% spec$schema
  q <- function(table) as.character(DBI::dbQuoteIdentifier(con, DBI::Id(schema = schema, table = table)))
  DBI::dbGetQuery(con, spec$labelled(q, consecutive_possession_pairs_sql(spec$ends(q))))
}
