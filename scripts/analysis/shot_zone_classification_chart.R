# Full-court shot chart: EVERY shot, colored by classify_shot_zone() (v2).
# Emphasis: the classifier is points-first — 3pt-tagged shots clicking inside
# the arc stay classified as 3s (they render as blue/green dots in mid-range
# territory), and the rim zone is a radial disc, not the painted RA polygon.
# Coords are a ~0.15-unit click grid: aggregate per (spot, zone), size ~ count.
# A spot can host TWO zones (2s and misplaced 3s at the same click cell).
# Output: docs/shot_zone_classification_map.png + stats to $SCRATCH.
suppressMessages({
  library(arrow); library(dplyr); library(ggplot2); library(sf)
})
source("scripts/analysis/fiba_court_zones.R")

out <- file.path(Sys.getenv("SCRATCH"), "shot_zone_classification_out.txt")
sink(out)
options(width = 220)

shots <- read_parquet(
  "exports/cold/actions_clean.parquet",
  col_select = c("game_id", "id", "type", "parameters_type", "parameters_points",
                 "parameters_made", "parameters_coord_x", "parameters_coord_y")
) |>
  filter(type == "shot") |>
  distinct(game_id, id, .keep_all = TRUE) |>
  filter(!is.na(parameters_coord_x), !is.na(parameters_coord_y)) |>
  mutate(x = parameters_coord_x / 100, y = parameters_coord_y / 100,
         zone = classify_shot_zone(x, y, parameters_points),
         zone = ifelse(zone %in% c("Left Corner 3", "Right Corner 3"),
                       "Corner 3", zone),
         made = parameters_made == "made")

zone_levels <- c("Rim (r<=1.45)", "In The Paint (Non-Rim)", "Mid-Range",
                 "Corner 3", "Above Break 3")
# validated on #0F172A: amber, violet, red, blue, aqua (fixed assignment)
zone_cols <- c("Rim (r<=1.45)"          = "#c98500",
               "In The Paint (Non-Rim)" = "#9085e9",
               "Mid-Range"              = "#e66767",
               "Corner 3"               = "#3987e5",
               "Above Break 3"          = "#199e70")

zone_stats <- shots |>
  group_by(zone) |>
  summarise(att = n(), fg = 100 * mean(made), .groups = "drop") |>
  mutate(zone = factor(zone, zone_levels)) |>
  arrange(zone)
cat("== zone totals ==\n"); print(as.data.frame(zone_stats), row.names = FALSE)

# points-first overrides: 3s clicking in 2pt territory / 2s beyond the arc
r_all <- sqrt((shots$x - cx)^2 + (shots$y - hoop_center_y)^2)
in2 <- ifelse(shots$y <= three_point_side_height,
              shots$x > three_point_side_offset & shots$x < width - three_point_side_offset,
              r_all < three_point_radius)
cat("\n3s clicking inside 2pt territory:", sum(shots$parameters_points == 3 & in2),
    "\n2s clicking outside the arc:", sum(shots$parameters_points == 2 & !in2), "\n")

# legend labels carry the stats (relief rule: identity never color-alone)
zone_labs <- with(zone_stats, setNames(
  sprintf("%s  ·  %s att  ·  %.1f FG%%", zone, format(att, big.mark = ","), fg),
  as.character(zone)))

spots <- shots |>
  count(x, y, zone) |>
  mutate(zone = factor(zone, zone_levels)) |>
  arrange(desc(n))   # big dots drawn first, small (incl. overridden 3s) on top

col_surface <- "#0F172A"; col_lines <- "#8b97ab"
col_text <- "#E5E7EB"; col_muted <- "#94a3b8"

p <- ggplot() +
  geom_point(data = spots, aes(x, y, color = zone, size = n), alpha = 0.75, stroke = 0) +
  geom_sf(data = court_sf, fill = col_lines, color = NA, alpha = 0.9) +
  scale_color_manual(values = zone_cols, labels = zone_labs, name = NULL,
                     guide = guide_legend(override.aes = list(size = 4, alpha = 1),
                                          ncol = 1)) +
  scale_size_area(max_size = 6, guide = "none") +
  labs(
    title = "Every shot, classified — classify_shot_zone() v2",
    subtitle = paste0("one dot per click-grid spot, sized by shots there · 2-vs-3 by scoreboard points,\n",
                      "so blue/green dots inside the arc are real 3s with a misplaced click · ",
                      "rim = radial disc r≤1.45, not the painted RA"),
    caption = paste0("Seasons 2025–26, ", format(nrow(shots), big.mark = ","),
                     " shots (actions_clean.parquet, deduped) · ",
                     format(nrow(count(shots, x, y)), big.mark = ","),
                     " distinct grid spots · corner 3 = L+R merged")
  ) +
  theme_void() +
  theme(
    plot.background  = element_rect(fill = col_surface, color = col_surface),
    panel.background = element_rect(fill = col_surface, color = col_surface),
    legend.position  = "right",
    legend.text      = element_text(color = col_text, size = 10,
                                    margin = margin(t = 4, b = 4)),
    plot.title    = element_text(color = col_text, size = 16, face = "bold",
                                 hjust = 0.5, margin = margin(b = 4)),
    plot.subtitle = element_text(color = col_muted, size = 9.5, hjust = 0.5,
                                 margin = margin(b = 10)),
    plot.caption  = element_text(color = col_muted, size = 8, hjust = 0.5,
                                 margin = margin(t = 8)),
    plot.margin   = margin(12, 16, 8, 16)
  )

ggsave("docs/shot_zone_classification_map.png", p,
       width = 12.5, height = 9, dpi = 150, bg = col_surface)
cat("\n-> docs/shot_zone_classification_map.png\n")
sink()
cat("done\n")
