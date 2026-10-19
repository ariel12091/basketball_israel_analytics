# Shot-generation regressions with LOCATION-based zones (parquet x/y ->
# classify_shot_zone v2) instead of provider tags. Tests whether rim-disc
# share carries the offense signal that tag-based lay-up share lacked.
# Team level: zone shares vs off_ppp/off_efg/off_ts (28 team-seasons).
# Player level (2026): ON-OFF zone-share deltas vs d_ppp/d_efg/d_ts,
# merged from output/shotgen_player_onoff_2026.csv (tag-based run).
# Caveat: parquet lags the MV by ~8 recent games; outcomes cover all games.
suppressMessages({
  library(arrow); library(sf); library(dplyr)
  library(DBI); library(RPostgres)
})
source("scripts/analysis/fiba_court_zones.R")

out <- file.path(Sys.getenv("SCRATCH"), "shotgen_zone_out.txt")
sink(out)
options(width = 220)

acts <- read_parquet("exports/cold/actions_clean.parquet",
  col_select = c("game_id", "id", "type", "parameters_points", "parameters_made",
                 "parameters_coord_x", "parameters_coord_y")) |>
  distinct(game_id, id, .keep_all = TRUE) |>
  filter(type == "shot", !is.na(parameters_coord_x)) |>
  mutate(x = parameters_coord_x / 100, y = parameters_coord_y / 100,
         zone = classify_shot_zone(x, y, parameters_points),
         zone_grp = case_when(
           zone == "Rim (r<=1.45)" ~ "ra",
           zone == "In The Paint (Non-Rim)" ~ "paint_nonra",
           zone == "Mid-Range" ~ "mid",
           zone %in% c("Left Corner 3", "Right Corner 3") ~ "c3",
           zone == "Above Break 3" ~ "ab3"),
         made = parameters_made == "made")

readRenviron("app/.Renviron")
con <- dbConnect(RPostgres::Postgres(),
  host=Sys.getenv("PG_HOST"), port=as.integer(Sys.getenv("PG_PORT")),
  dbname=Sys.getenv("PG_DB"), user=Sys.getenv("PG_USER"),
  password=Sys.getenv("PG_PASS"), sslmode=Sys.getenv("PG_SSLMODE"),
  connect_timeout=15L, bigint="numeric")

mv_shots <- dbGetQuery(con, "
  SELECT d.game_id, d.id, d.team_id, d.lineup_hash, s.game_year AS yr
  FROM basketball_test.df_pts_poss_lineups_longer_mv d
  JOIN basketball_test.schedule s ON s.game_id = d.game_id
  WHERE d.type = 'shot' AND d.type_lineup = 'offense'")
rtg <- dbGetQuery(con, "
  SELECT game_year AS yr, team_id, team_name, off_ppp
  FROM basketball_test.team_ppp_ratings_mv")
ff <- dbGetQuery(con, "
  SELECT game_year AS yr, team_id, off_efg, off_ts
  FROM basketball_test.team_four_factors_mv")
ll <- dbGetQuery(con, "
  SELECT DISTINCT l.player_id, l.team_id, l.lineup_hash,
         COALESCE(l.is_on_verdict, 0)::int AS is_on
  FROM basketball_test.lineups_lookup l
  JOIN (SELECT DISTINCT d.lineup_hash
        FROM basketball_test.df_pts_poss_lineups_longer_mv d
        JOIN basketball_test.schedule s ON s.game_id = d.game_id
        WHERE s.game_year = 2026 AND d.type_lineup = 'offense') h
    USING (lineup_hash)")
dbDisconnect(con)

sz <- acts |> inner_join(mv_shots, by = c("game_id", "id"))
cat("== JOIN CHECK ==\n")
cat("parquet shots:", nrow(acts), " matched to MV offense rows:", nrow(sz), "\n\n")

vif <- function(df, vars) sapply(vars, function(v) {
  r2 <- summary(lm(reformulate(setdiff(vars, v), v), data = df))$r.squared
  round(1 / (1 - r2), 2)
})
show <- function(m, n) {
  s <- summary(m)
  co <- round(coef(s), 3); colnames(co) <- c("est","se","t","p")
  print(co)
  cat(sprintf("R2 = %.3f  adjR2 = %.3f  n = %d\n\n", s$r.squared, s$adj.r.squared, n))
}

# ---------- TEAM LEVEL ----------
tz <- sz |>
  group_by(yr, team_id) |>
  summarise(fga = n(),
            ra_share = 100 * sum(zone_grp == "ra") / n(),
            paint_nonra_share   = 100 * sum(zone_grp == "paint_nonra") / n(),
            mid_share     = 100 * sum(zone_grp == "mid") / n(),
            c3_share      = 100 * sum(zone_grp == "c3") / n(),
            ab3_share     = 100 * sum(zone_grp == "ab3") / n(),
            .groups = "drop") |>
  inner_join(rtg, by = c("yr", "team_id")) |>
  inner_join(ff,  by = c("yr", "team_id"))

cat("== LEAGUE ZONE PPS (per season; for reference) ==\n")
print(sz |> group_by(yr, zone_grp) |>
  summarise(att = n(), fg = round(100 * mean(made), 1),
            pps = round(sum(parameters_points[made]) / n(), 3), .groups = "drop") |>
  as.data.frame(), row.names = FALSE)

cat("\n== TEAM ZONE SHARES (n =", nrow(tz), "team-seasons) ==\n")
print(tz |> arrange(desc(off_ppp)) |>
  mutate(across(ends_with("_share"), \(v) round(v, 1))) |>
  select(yr, team_name, off_ppp, ra_share, paint_nonra_share, mid_share,
         c3_share, ab3_share) |> as.data.frame(), row.names = FALSE)

zv <- c("ra_share", "paint_nonra_share", "c3_share", "ab3_share")
cat("\n== predictor correlations + VIFs (baseline = mid_share) ==\n")
print(round(cor(tz[, c(zv, "mid_share")]), 2))
print(vif(tz, zv))

for (o in c("off_ppp", "off_efg", "off_ts")) {
  cat("\n==", o, "~ ra + paint + c3 + ab3 + factor(yr)  [baseline = mid] ==\n")
  show(lm(reformulate(c(zv, "factor(yr)"), o), tz), nrow(tz))
}
cat("== off_ppp ~ pooled zones: rim disc + all 3s (paint+mid baseline) ==\n")
tz$fg3z_share <- tz$c3_share + tz$ab3_share
show(lm(off_ppp ~ ra_share + fg3z_share + factor(yr), tz), nrow(tz))

# ---------- PLAYER LEVEL (2026) ----------
ps <- sz |>
  filter(yr == 2026) |>
  inner_join(ll, by = c("lineup_hash", "team_id"),
             relationship = "many-to-many") |>
  group_by(player_id, team_id, is_on) |>
  summarise(fga = n(),
            ra = sum(zone_grp == "ra"),
            paint_nonra = sum(zone_grp == "paint_nonra"),
            fg3z    = sum(zone_grp %in% c("c3", "ab3")),
            ra_made = sum(zone_grp == "ra" & made),
            .groups = "drop")

pw <- ps |>
  tidyr::pivot_wider(names_from = is_on,
                     values_from = c(fga, ra, paint_nonra, fg3z, ra_made),
                     names_sep = "_") |>
  mutate(d_ra_share = 100 * (ra_1/fga_1 - ra_0/fga_0),
         d_paint_nonra_share = 100 * (paint_nonra_1/fga_1 - paint_nonra_0/fga_0),
         d_fg3z_share    = 100 * (fg3z_1/fga_1 - fg3z_0/fga_0),
         d_ra_fg    = 100 * (ra_made_1/ra_1 - ra_made_0/ra_0))

tag <- read.csv("output/shotgen_player_onoff_2026.csv")
r <- merge(tag, pw, by = c("player_id", "team_id"))
cat("\n== PLAYER LEVEL: zone-based deltas, same 137-player panel ==\n")
cat("matched:", nrow(r), "of", nrow(tag), "\n")
cat("tag-vs-zone share delta agreement: cor(d_rim_share, d_ra_share+d_paint_nonra_share) =",
    round(cor(r$d_rim_share, r$d_ra_share + r$d_paint_nonra_share, use = "complete.obs"), 2),
    "\n  cor(d_fg3_share [tag], d_fg3z_share [zone]) =",
    round(cor(r$d_fg3_share, r$d_fg3z_share, use = "complete.obs"), 2), "\n\n")

cat("== PEARSON r: zone generation deltas vs outcome deltas ==\n")
preds <- c("d_ra_share", "d_paint_nonra_share", "d_fg3z_share", "d_ra_fg")
print(t(sapply(preds, function(p) {
  unlist(lapply(c("d_ppp", "d_efg", "d_ts"), function(o) {
    ok <- is.finite(r[[p]]) & is.finite(r[[o]])
    setNames(round(cor(r[[p]][ok], r[[o]][ok]), 2), o)
  }))
})))

for (o in c("d_ppp", "d_efg", "d_ts")) {
  cat("\n==", o, "~ zone generation only ==\n")
  show(lm(reformulate(c("d_ra_share", "d_paint_nonra_share", "d_fg3z_share"), o), r), nrow(r))
}
cat("== d_ppp ~ zone generation + finishing (zone rim FG + tag 3pt FG) ==\n")
show(lm(d_ppp ~ d_ra_share + d_paint_nonra_share + d_fg3z_share +
          d_ra_fg + d_fg3_fg, r), nrow(r))

write.csv(tz, "output/shotgen_team_zone_2025_2026.csv", row.names = FALSE)
write.csv(r[, c("player_name","team_name","player_id","team_id","on_poss",
                "d_ra_share","d_paint_nonra_share","d_fg3z_share","d_ra_fg",
                "d_rim_share","d_fg3_share","d_ppp","d_efg","d_ts")],
          "output/shotgen_player_zone_2026.csv", row.names = FALSE)
sink()
cat("done\n")
