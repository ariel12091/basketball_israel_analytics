# Validate the shot-type "interior" proxy against real coordinates.
# Reads exports/cold/actions_clean.parquet (coords never reach the live MV),
# classifies shots into basic zones via scripts/analysis/fiba_court_zones.R,
# and cross-tabs parameters_type x shot_zone_basic.
suppressMessages({
  library(arrow); library(sf); library(dplyr)
  library(DBI); library(RPostgres)
})
source("scripts/analysis/fiba_court_zones.R")

out <- file.path(Sys.getenv("SCRATCH"), "shot_zone_validation_out.txt")
sink(out)
options(width = 220)

shots <- read_parquet(
  "exports/cold/actions_clean.parquet",
  col_select = c("game_id", "id", "type", "parameters_type", "parameters_points",
                 "parameters_made", "parameters_coord_x", "parameters_coord_y")
) |>
  filter(type == "shot") |>
  distinct(game_id, id, .keep_all = TRUE)

# season map (tiny query; parquet has no game_year)
readRenviron("app/.Renviron")
con <- dbConnect(RPostgres::Postgres(),
  host=Sys.getenv("PG_HOST"), port=as.integer(Sys.getenv("PG_PORT")),
  dbname=Sys.getenv("PG_DB"), user=Sys.getenv("PG_USER"),
  password=Sys.getenv("PG_PASS"), sslmode=Sys.getenv("PG_SSLMODE"),
  connect_timeout=15L, bigint="numeric")
yr_map <- dbGetQuery(con, "SELECT DISTINCT game_id, game_year FROM basketball_test.schedule")
dbDisconnect(con)
shots <- left_join(shots, yr_map, by = "game_id")

cat("== 1. COORDINATE COVERAGE ==\n")
cat("total shot rows (deduped):", nrow(shots), "\n")
cov <- shots |>
  group_by(game_year) |>
  summarise(shots = n(),
            with_coords = sum(!is.na(parameters_coord_x) & !is.na(parameters_coord_y)),
            pct = round(100 * with_coords / shots, 1), .groups = "drop")
print(as.data.frame(cov), row.names = FALSE)
cov_t <- shots |>
  group_by(parameters_type) |>
  summarise(shots = n(),
            pct_with_coords = round(100 * mean(!is.na(parameters_coord_x)), 1),
            .groups = "drop")
cat("\nby shot type:\n"); print(as.data.frame(cov_t), row.names = FALSE)

sc <- shots |>
  filter(!is.na(parameters_coord_x), !is.na(parameters_coord_y)) |>
  mutate(x = parameters_coord_x / 100, y = parameters_coord_y / 100)

cat("\ncoordinate ranges (/100): x", round(range(sc$x), 2), " y", round(range(sc$y), 2), "\n")
cat("out of half-court box [0,15]x[0,14]:",
    sum(sc$x < 0 | sc$x > 15 | sc$y < 0 | sc$y > 14), "shots\n")

# zone classification (v2: points-first, radial rim disc)
sc_df <- sc |>
  mutate(shot_zone_basic = classify_shot_zone(x, y, parameters_points),
         magnet = is_magnet_spot(x, y))
cat("\nmagnet-spot shots (suspected default clicks):", sum(sc_df$magnet),
    "(", round(100 * mean(sc_df$magnet), 1), "% )\n")

cat("\n== 2. SHOT TYPE x ZONE (row %) ==\n")
ct <- sc_df |>
  mutate(type4 = case_when(
    parameters_type == "jump-shot" & parameters_points == 2 ~ "jump-shot 2pt",
    parameters_type == "jump-shot" & parameters_points == 3 ~ "jump-shot 3pt",
    TRUE ~ parameters_type)) |>
  count(type4, shot_zone_basic) |>
  group_by(type4) |>
  mutate(pct = round(100 * n / sum(n), 1)) |>
  ungroup()

wide <- ct |>
  select(type4, shot_zone_basic, pct) |>
  tidyr::pivot_wider(names_from = shot_zone_basic, values_from = pct, values_fill = 0)
cat("row-% matrix:\n"); print(as.data.frame(wide), row.names = FALSE)

cat("\n== 3. INTERIOR PROXY VALIDATION ==\n")
prox <- sc_df |>
  mutate(is_int_proxy = parameters_type %in% c("lay-up", "dunk", "allyhoop"),
         is_rim_zone = shot_zone_basic == "Rim (r<=1.45)",
         is_paint = shot_zone_basic %in% c("Rim (r<=1.45)", "In The Paint (Non-Rim)"))
cat("proxy (lay-up/dunk/oop) shots in rim disc:",
    round(100 * mean(prox$is_rim_zone[prox$is_int_proxy]), 1), "%\n")
cat("proxy shots in rim+paint:",
    round(100 * mean(prox$is_paint[prox$is_int_proxy]), 1), "%\n")
cat("rim-disc shots that are proxy-tagged:",
    round(100 * mean(prox$is_int_proxy[prox$is_rim_zone]), 1), "%\n")
cat("dunks in rim disc:",
    round(100 * mean(prox$is_rim_zone[prox$parameters_type == "dunk"]), 1), "%\n")

cat("\n== 4. 2PT JUMP-SHOT BY ZONE (n, FG%, blocked=miss) ==\n")
j2 <- sc_df |>
  filter(parameters_type == "jump-shot", parameters_points == 2) |>
  group_by(shot_zone_basic) |>
  summarise(att = n(), fg_pct = round(100 * mean(parameters_made == "made"), 1),
            .groups = "drop") |>
  arrange(desc(att))
print(as.data.frame(j2), row.names = FALSE)

cat("\n== 5. LAY-UP BY ZONE (n, FG%) ==\n")
lz <- sc_df |>
  filter(parameters_type == "lay-up") |>
  group_by(shot_zone_basic) |>
  summarise(att = n(), fg_pct = round(100 * mean(parameters_made == "made"), 1),
            .groups = "drop") |>
  arrange(desc(att))
print(as.data.frame(lz), row.names = FALSE)

write.csv(ct, file.path("output", "shot_type_zone_crosstab.csv"), row.names = FALSE)
cat("\n-> output/shot_type_zone_crosstab.csv\n")

sink()
cat("done\n")
