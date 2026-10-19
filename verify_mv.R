readRenviron("etl/.Renviron")
library(DBI); library(RPostgres)
con <- dbConnect(Postgres(), host=Sys.getenv("PG_HOST"), port=5432L,
  dbname=Sys.getenv("PG_DB"), user=Sys.getenv("PG_USER"),
  password=Sys.getenv("PG_PASS"), sslmode="require")
n <- dbGetQuery(con, "SELECT count(*) AS n FROM basketball_test.onoff_default_mv")$n
cat("MV rows:", as.integer(n), "\n")
sample <- dbGetQuery(con, "SELECT \"ON Poss\" FROM basketball_test.onoff_default_mv ORDER BY \"ON Poss\" ASC LIMIT 5")
cat("Lowest ON Poss values:", paste(sample[[1]], collapse=", "), "\n")
dbDisconnect(con)
