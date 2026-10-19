# Shot chart court dimensions: basket.co.il vs the tracker's coordinates (2026-10-01)

**Question:** does the basket.co.il livestats shot chart
(`https://basket.co.il/livestats/game.html?game_id=406`) draw its court in the
same system the play-by-play coordinates live in, and where do shots land in
the wrong zone?

**Short answer:** no. The site draws a FIBA-true court (except for a
hand-fitted three-point line), while the tracker's coordinates are a stretched
grid. The three-point line can be recovered exactly from the data; the paint
and restricted area cannot be verified.

## 1. The coordinate system (play-by-play)

- Columns: `parameters_coord_x` / `parameters_coord_y` in `actions_clean`
  (Segev API: `parameters.coordX` / `coordY`). Only `type = 'shot'` rows have them.
- `coord_x` = across the court, 0-1500, **steps of 15**, centre = 750.
- `coord_y` = depth from the attacking baseline, 0-1400, **steps of 28**.
- So a 100 x 50 grid over a half court. Not centimetres.
- The rim sits at about `coord_y` 112-140 (dunks cluster there), `coord_x` 735-750.

**The 2/3 label is derived from the position.** In 2025 no grid cell held both
2s and 3s; in 2026 three shots out of 29,763 did. The operator taps a spot on
the tracker's court image and the tracker assigns 2 or 3 from it, so the 2/3
boundary in the data **is** the tracker's three-point line.

## 2. What basket.co.il draws

Source: `courtLines()` and `shotPos()` in `https://basket.co.il/livestats/assets/game.js?v=35`.
SVG viewBox is 2800 x 1500 (cm).

| Element | basket.co.il | FIBA | Note |
|---|---|---|---|
| Court | 2800 x 1500 | 2800 x 1500 | ok |
| Basket centre | 157.5 from baseline | 157.5 | ok |
| Backboard | 120 from baseline, 180 wide | 120, 180 | ok |
| Rim | r 22.5 | 45 diameter | ok |
| Paint | 580 deep x 490 wide | 580 x 490 | FIBA-true, **never adjusted to the data** |
| Free-throw / centre circle | r 180 | 180 | FIBA-true |
| Restricted area | arc r 125 only | r 125 + two 37.5 straight lines | lines missing; **never adjusted** |
| Three-point line | **ellipse 900 (depth) x 600 (width)** around the basket; corner lines 580 from centre up to depth 388; apex at 1057.5 | circle r 675, corner lines 660 from centre, apex 832.5 | hand-fitted to the data, not FIBA |

Plotting: depth plotted 1:1 (`x = coordY`, mirrored for the other basket);
width stretched `WIDTH_SCALE = 1.12` about the centre line
(`y = 750 + (coordX - 750) * 1.12`). The code comment "832 = 3pt" is stale --
it contradicts the ellipse the same code draws.

**Only the three-point line was adjusted.** Paint, restricted area and circles
are FIBA-true while the shots are in a stretched system (along the court
roughly 1.4x), so interior zones on the site's chart are too small for the data.

## 3. Our own script (`scripts/analysis/fiba_court_zones.R`)

- Adjusted: three-point circle r 910, corner lines 210 from the sideline (540
  from centre), restricted area 145 (FIBA 125), rim at depth 140.
- Not adjusted: paint 580 x 490, free-throw circle 180.
- It is the weakest of the fitted lines: its corner line at 540 counts corner
  3s at 510-525 as 2s.

## 4. The tracker's three-point line, fitted from the data

2025-26 season (game_year 2026, 221 games, 29,763 shots):

- Straight-on: farthest 2 at `coord_y` 1036, nearest 3 at 1092 -> apex ~1071-1078.
- Sides: almost straight at **|coord_x - 750| ~ 495-510** from the baseline up
  to depth ~800 (FIBA's straight corner ends at 299).
- Then it rounds off: approx. a circle r ~615 centred at depth ~463, or an
  ellipse around the rim ~945 deep x 680-710 wide with corner lines at 495.
- It is **not** a uniformly stretched FIBA line (best uniform stretch still
  misses 229 shots vs 16).
- Same boundary in 2025, 2026 and 2027 -- a fixed property of the tracker.

Shots on the wrong side of each candidate line:

| Line | 2025 | 2026 | 2027 (22 games, API) |
|---|---|---|---|
| FIBA circle | 3,217 | 3,168 | 350 |
| Our R script (910 / 540) | 450 | 668 | 41 |
| basket.co.il ellipse | 174 | 178 | 18 |
| Fitted ellipse (rim 126, depth 945, width 680, corner 495) | 3 | 13 | 1 |

## 5. Interior zones -- no ground truth

- The play-by-play has no per-shot "paint" / "restricted area" label.
- The box score's `Points_in_the_paint` (Segev `getBoxScore`) was tested as a
  check and **does not work**: the best paint rectangle matched only 131 of 438
  team-games exactly (~2.5 pts/team-game off); shot type (lay-up/dunk) did no
  better. The value is always even and never exceeds made-2 points (~82% of
  them), but it is not a clean function of position or type. Dropped.

## 6. Zone split with the site's full measurements

Geometry (site plot space, cm): depth = `coord_y`; width = `|coord_x - 750| * 1.12`;
RA = within 125 of (157.5, centre) or behind the basket within 125 wide;
paint = depth <= 580 and width <= 245; three = width > 580 for depth <= 388,
else outside ellipse `((depth-157.5)/900)^2 + (width/600)^2 > 1`.

2025-26 (game_year 2026, 29,763 shots):

| Zone | Att | Share | FG% |
|---|---|---|---|
| Restricted area | 3,269 | 11.0% | 77.1 |
| Paint (non-RA) | 9,914 | 33.3% | 53.2 |
| Mid-range | 4,321 | 14.5% | 39.3 |
| Corner 3 | 1,804 | 6.1% | 37.9 |
| Above-break 3 | 10,455 | 35.1% | 34.3 |

- 173 recorded 2s plot beyond the drawn arc (122 at depth 700-900: the wings,
  where the tracker's line is still straight and the site's ellipse has curved
  in); 5 recorded 3s plot inside it (corners, depth 252-364).
- Lay-ups: 2,146 in RA, 9,137 in paint outside RA, 690 in mid-range.
  Dunks: 1,069 in RA, 60 in paint, 3 mid-range. 95% of dunks are inside r 125,
  so the basket position is right -- the **scale** of the interior zones is wrong.

2026-27 (game_year 2027, 22 games via API, 2,868 shots): same pattern --
RA 10.1%, paint 33.8%, mid 13.3%, corner 3 7.9%, above-break 35.0%;
18 wrong-side shots (17 wing 2s beyond the arc, 1 corner 3 inside).

## 7. Buggy shots, 2026-27 (in-house data)

Built from cold-storage parquet only (local `exports/cold/actions_clean.parquet`
+ GitHub release `cold-storage/latest`): **18 of 22 games, 2,393 shots**. Full
list with names: `docs/shot_chart_buggy_shots_2027.csv`.

| Issue | Count |
|---|---|
| Lay-up outside the paint | 49 |
| Dunk / alley-oop outside RA | 21 |
| 2 plotted beyond the arc | 13 |
| 3 plotted inside the arc | 1 |

Clear tagging errors (wrong under any drawing):

| Issue | Game | Q | Clock | Player | x | y | Dist |
|---|---|---|---|---|---|---|---|
| Dunk | 395 | 4 | 01:24 | Kenneth Lofton Jr. | 600 | 504 | 3.85 m |
| Dunk | 623 | 3 | 03:32 | Tyreek Smith | 735 | 420 | 2.63 m |
| Dunk | 403 | 2 | 07:27 | Rivaldo Soares | 735 | 392 | 2.35 m |
| Lay-up | 622 | 2 | 09:33 | Markis McDuffie | 1005 | 644 | 5.64 m |
| Lay-up | 621 | 4 | 02:47 | Ahmad Caver | 615 | 644 | 5.09 m |
| Lay-up | 395 | 3 | 04:13 | Dusan Miletic | 615 | 616 | 4.83 m |

Site-drawing artefacts (the tracker's label is consistent with its own line):
all 13 "2 beyond the arc" are wing shots at depth 700-924 (e.g. game 406 Q3
04:58 Chaunce Jenkins; game 396 Q1 09:40 Tomer Asayag), and the one "3 inside"
is a corner shot (game 403 Q3 05:20 Nike Sibande, x 240, y 336).

Most of the 21 dunks are 1.3-2.2 m out and most of the 49 lay-ups are 2.5-3.3 m
out at the paint's sides -- consistent with the interior zones being drawn too
small for the stretched data, not with bad tagging.

Also seen in the API data (not in-house): game 400 Q4 04:34 Ron Zipper, 2 at
`coord_y` 1064 (~10.6 m, near half court); game 399 Q3 04:08 Vincent Amsellem,
lay-up ~5 m.

## 8. Data availability note

2026-27 games **393, 397, 399, 400** are processed in the DB
(`etl_processed_games`, `df_pts_poss_lineups_longer_mv`) but their raw actions
(and so shot coordinates) are in neither local parquet nor the release -- the
known cold-storage gap (CLAUDE.md Backlogs, Security/Resilience #3). The
release also holds 621, 626, 648, 651, which the local folder lacks; merging it
into `exports/cold/` was offered but not done.

## 9. Open next steps

1. Decide the interior-zone definition in the data's own system (options:
   stretch paint/RA by the measured ~1.4x along the court; distance bands from
   the rim; or shot type as an RA proxy).
2. Replace the three-point line in `fiba_court_zones.R` with the fitted one.
3. Optionally merge the release parquet into `exports/cold/`.
