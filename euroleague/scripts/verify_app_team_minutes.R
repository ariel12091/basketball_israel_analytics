readRenviron(file.path("app", ".Renviron"))

suppressPackageStartupMessages({
  library(DBI)
  library(RPostgres)
})

con <- dbConnect(
  Postgres(),
  host = Sys.getenv("PG_HOST"),
  port = as.integer(Sys.getenv("PG_PORT")),
  dbname = Sys.getenv("PG_DB"),
  user = Sys.getenv("PG_USER"),
  password = Sys.getenv("PG_PASS"),
  sslmode = Sys.getenv("PG_SSLMODE", "require")
)
on.exit(dbDisconnect(con), add = TRUE)

access <- dbGetQuery(
  con,
  paste(
    "SELECT current_user,",
    "has_table_privilege(current_user,",
    "'euroleague.matchup_segments_actions', 'select') AS can_segments"
  )
)
print(access)

sql <- paste0(
  "SELECT team_id, minutes AS game_minutes ",
  "FROM euroleague.get_team_minutes_dynamic(",
  "$1::text,$2::int4,$3::date,$4::date,$5::text,$6::text,$7::text,",
  "$8::text,$9::text,$10::text,$11::int4,$12::text,",
  "$13::int4,$14::int4,$15::int4,$16::int4,$17::int4,$18::int4,$19::int4)"
)

rows <- dbGetQuery(
  con,
  sql,
  params = list(
    "E", 2025L, as.Date("2025-09-01"), as.Date("2026-07-01"),
    NA_character_, NA_character_, NA_character_, NA_character_, NA_character_,
    NA_character_, NA_integer_, NA_character_,
    NA_integer_, NA_integer_, NA_integer_, NA_integer_, NA_integer_,
    NA_integer_, NA_integer_
  )
)

print(summary(rows$game_minutes))
print(utils::head(rows))
