// Shot-cell accuracy text for the stats-tab layout (design 2026-10-28 § 5):
// muted green/red against the league average, used with the grey/amber
// frequency bar. Cell shading keeps Shiny's heat colours (user 2026-10-28:
// they explain a cell's rank better than a muted red/green).

const lerp = (a: number, b: number, t: number) => a + (b - a) * t;
const NEUTRAL = [0xcf, 0xc8, 0xbb],
  GOOD = [0x7f, 0xc7, 0x9a],
  POOR = [0xd9, 0x8b, 0x7e];
const hex = (rgb: number[]) =>
  "#" + rgb.map((v) => Math.round(v).toString(16).padStart(2, "0")).join("");

/** Shot-cell accuracy text: `d` in [-1, 1] is how far above (+) or below (-)
 * the league average the percentage is, already sign-flipped for defense. */
export function mutedAccuracy(d: number): string {
  const t = Math.max(-1, Math.min(1, d));
  const to = t >= 0 ? GOOD : POOR;
  return hex(NEUTRAL.map((v, i) => lerp(v, to[i], Math.abs(t))));
}
