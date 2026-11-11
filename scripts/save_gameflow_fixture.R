# Save one Gameflow reader row as a RawRibbon JSON fixture for the web tests.
#
#   Rscript scripts/save_gameflow_fixture.R <game_id> <team_id> [israel|euroleague]
#
# Runs the real reader constant from app/R/global.R (never a copy) as
# app_readonly on port 5432 and writes
# web/src/rules/__fixtures__/gameflow-<game_id>.json. Also prints each of the
# team's players' ON offensive/defensive points and possessions from
# player_four_factors_by_game, the numbers the fixture test pins.

args <- commandArgs(TRUE)
stopifnot(length(args) >= 2)
game_id <- as.integer(args[1]); team_id <- as.integer(args[2])
league <- if (length(args) >= 3) args[3] else "israel"
stopifnot(!is.na(game_id), !is.na(team_id), league %in% c("israel", "euroleague"))

readRenviron("app/.Renviron")
con <- DBI::dbConnect(RPostgres::Postgres(),
  host = Sys.getenv("PG_HOST"), port = 5432L, dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"), password = Sys.getenv("PG_PASS"),
  sslmode = "require", bigint = "numeric")
on.exit(DBI::dbDisconnect(con))

const <- if (league == "israel") "RIBBON_SQL_ISRAEL" else "RIBBON_SQL_EURO"
src <- paste(readLines("app/R/global.R", warn = FALSE), collapse = "\n")
eval(parse(text = regmatches(src, regexpr(paste0(const, ' <- "(.|\n)*?"\n'),
                                          src, perl = TRUE))))
sql <- get(const)
sql <- gsub("$2", "?team_id", gsub("$1", "?game_id", sql, fixed = TRUE), fixed = TRUE)
row <- DBI::dbGetQuery(con, DBI::sqlInterpolate(con, sql,
                                                game_id = game_id, team_id = team_id))

js <- function(x) if (is.na(x)) NULL else jsonlite::fromJSON(x, simplifyVector = FALSE)
out <- list(
  lanes = js(row$lanes[1]), margin = js(row$margin[1]), poss = js(row$poss[1]),
  events = js(row$events[1]), n_periods = as.numeric(row$n_periods[1]),
  excluded_segments = as.numeric(row$excluded_segments[1]),
  false_straddle_segments = as.numeric(row$false_straddle_segments[1]),
  false_straddle_seconds = as.numeric(row$false_straddle_seconds[1]))
path <- file.path("web", "src", "rules", "__fixtures__",
                  sprintf("gameflow-%d.json", game_id))
dir.create(dirname(path), showWarnings = FALSE, recursive = TRUE)
writeLines(jsonlite::toJSON(out, auto_unbox = TRUE, null = "null", digits = NA),
           path, useBytes = TRUE)
cat("wrote", path, file.size(path), "bytes\n")

schema <- if (league == "israel") "basketball_test" else "euroleague"
ff <- DBI::dbGetQuery(con, sprintf("
  SELECT player_id, type_lineup, SUM(total_poss) AS poss, SUM(total_points) AS pts
  FROM %s.player_four_factors_by_game
  WHERE game_id = %d AND team_id = %d AND is_on_key = 1
    AND type_lineup IN ('offense', 'defense')
  GROUP BY 1, 2 ORDER BY 1, 2", schema, game_id, team_id))
labels <- unique(do.call(rbind, lapply(out$lanes, function(l)
  data.frame(player_id = l$player_id, label = l$player_label))))
print(merge(ff, labels, by = "player_id", all.x = TRUE))
