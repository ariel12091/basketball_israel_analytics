// The stats tabs' muted good/bad colours (design 2026-10-28 § 5). Web-owned:
// the band cut points still come from Shiny's ui.heat.cuts, so a value lands
// in the same band as before; only the colours are softer. Opt-in per tab via
// heat(..., "muted") and shotCell({ palette: "muted" }).

const BANDS = 20;
const lerp = (a: number, b: number, t: number) => a + (b - a) * t;
const round = (x: number) => Math.round(x * 1000) / 1000;

/** 20 backgrounds, worst to best: muted red fading to clear, then clear
 * deepening to muted green. Index-aligned with heat()'s band index. */
export const MUTED_HEAT: string[] = Array.from({ length: BANDS }, (_, i) =>
  i < BANDS / 2
    ? `rgba(210,120,105,${round(lerp(0.3, 0.04, i / (BANDS / 2 - 1)))})`
    : `rgba(110,180,130,${round(lerp(0.04, 0.38, (i - BANDS / 2) / (BANDS / 2 - 1)))})`,
);

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
