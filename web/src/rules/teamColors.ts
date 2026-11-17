// Gameflow draws each side in its own club's colours (user 2026-11-17: Maccabi
// is never red, Hapoel never yellow). The colours live on the team catalogue
// rows (season_teams / euroleague.teams, docs/shared_definitions.md "Team
// colours"); this file only pairs two of them for one game and keeps each
// readable on the chart. Plan: docs/superpowers/plans/2026-11-17-gameflow-phone-focus-layout.md.

export type TeamIdentity = {
  logo: string | null;
  primary: string | null;
  secondary: string | null;
};
export type GamePalette = { own: string; opp: string; ownText: string; oppText: string };
type Theme = "light" | "dark";

/** Two colours closer than this (CIE76, in Lab) read as the same team. */
export const MIN_DISTANCE = 30;
/** A bar's contrast with the chart: 2:1, not 3:1, so a yellow club stays
 * yellow on the light theme (3:1 turned Maccabi mustard, 2026-11-17). */
export const MIN_BG_CONTRAST = 2;
/** The chart's background in each theme (--ibpl-surface: light.css, app.css). */
export const CHART_BG = { light: "#FFFFFF", dark: "#1F1A14" } as const;
/** The bar numbers' two inks, dark and light. */
const INK = { dark: "#14100C", light: "#FFF8EA" } as const;
/** The neutral a side falls back to when no pair of club colours differs. */
const NEUTRAL = "#8C8278";

type Rgb = [number, number, number];
const hexToRgb = (h: string): Rgb => [1, 3, 5].map((i) => parseInt(h.slice(i, i + 2), 16)) as Rgb;
const rgbToHex = (c: Rgb) =>
  "#" + c.map((v) => Math.round(Math.min(255, Math.max(0, v))).toString(16).padStart(2, "0")).join("").toUpperCase();
const lin = (v: number) => ((v /= 255) <= 0.04045 ? v / 12.92 : ((v + 0.055) / 1.055) ** 2.4);
const unlin = (v: number) => 255 * (v <= 0.0031308 ? 12.92 * v : 1.055 * v ** (1 / 2.4) - 0.055);
const WHITE = [0.95047, 1, 1.08883];
const f = (t: number) => (t > 216 / 24389 ? Math.cbrt(t) : (24389 / 27 * t + 16) / 116);
const fInv = (t: number) => (t ** 3 > 216 / 24389 ? t ** 3 : (116 * t - 16) / (24389 / 27));

function toLab(hex: string): Rgb {
  const [r, g, b] = hexToRgb(hex).map(lin);
  const xyz = [
    0.4124 * r + 0.3576 * g + 0.1805 * b,
    0.2126 * r + 0.7152 * g + 0.0722 * b,
    0.0193 * r + 0.1192 * g + 0.9505 * b,
  ].map((v, i) => f(v / WHITE[i]));
  return [116 * xyz[1] - 16, 500 * (xyz[0] - xyz[1]), 200 * (xyz[1] - xyz[2])];
}
function fromLab([L, a, b]: Rgb): string {
  const fy = (L + 16) / 116,
    [x, y, z] = [fInv(fy + a / 500), fInv(fy), fInv(fy - b / 200)].map((v, i) => v * WHITE[i]);
  return rgbToHex([
    unlin(3.2406 * x - 1.5372 * y - 0.4986 * z),
    unlin(-0.9689 * x + 1.8758 * y + 0.0415 * z),
    unlin(0.0557 * x - 0.204 * y + 1.057 * z),
  ].map((v) => (Number.isFinite(v) ? v : 0)) as Rgb);
}

/** CIE76 distance between two hex colours. */
export function colourDistance(a: string, b: string): number {
  const [p, q] = [toLab(a), toLab(b)];
  return Math.hypot(p[0] - q[0], p[1] - q[1], p[2] - q[2]);
}
const luminance = (hex: string) => {
  const [r, g, b] = hexToRgb(hex).map(lin);
  return 0.2126 * r + 0.7152 * g + 0.0722 * b;
};
/** WCAG contrast ratio. */
export function contrast(a: string, b: string): number {
  const [x, y] = [luminance(a), luminance(b)].sort((m, n) => n - m);
  return (x + 0.05) / (y + 0.05);
}
const ink = (fill: string) => (contrast(INK.dark, fill) >= contrast(INK.light, fill) ? INK.dark : INK.light);

/** The club colour as drawn: lightness moved (hue kept) until it shows 2:1 on
 * the chart and its numbers 4.5:1 on it. */
function drawable(hex: string, theme: Theme): string {
  const bg = CHART_BG[theme],
    lab = toLab(hex);
  let out = hex;
  for (let i = 0; i < 100; i++) {
    out = fromLab(lab);
    const bgOk = contrast(out, bg) >= MIN_BG_CONTRAST,
      text = ink(out);
    if (bgOk && contrast(text, out) >= 4.5) return out;
    // Away from the background first, then away from the ink's opposite.
    const step = !bgOk ? (theme === "dark" ? 1 : -1) : text === INK.dark ? 1 : -1;
    lab[0] = Math.min(100, Math.max(0, lab[0] + step));
  }
  return out;
}

/** A team's name in its club colour: lightness moved (hue kept) until the
 * text shows 4.5:1 on the chart. */
function readable(hex: string, theme: Theme): string {
  const bg = CHART_BG[theme],
    lab = toLab(hex);
  let out = hex;
  for (let i = 0; i < 100 && contrast(out, bg) < 4.5; i++) {
    lab[0] = Math.min(100, Math.max(0, lab[0] + (theme === "dark" ? 1 : -1)));
    out = fromLab(lab);
  }
  return out;
}

/** The two sides' colours for one game, or null when either club has no
 * colours (the chart then keeps its own two). Never the same, or a near-same,
 * colour for both sides. */
export function gamePalette(
  own: TeamIdentity | undefined,
  opp: TeamIdentity | undefined,
  theme: Theme,
): GamePalette | null {
  if (!own?.primary || !own.secondary || !opp?.primary || !opp.secondary) return null;
  const d = (h: string) => drawable(h, theme);
  const pairs: [string, string][] = [
    [own.primary, opp.primary],
    [own.primary, opp.secondary],
    [own.secondary, opp.primary],
    [own.primary, NEUTRAL],
  ];
  const [a, b] = pairs.map(([x, y]) => [d(x), d(y)] as const).find(([x, y]) => colourDistance(x, y) >= MIN_DISTANCE) ??
    [d(own.primary), d(NEUTRAL)];
  return { own: a, opp: b, ownText: ink(a), oppText: ink(b) };
}

/** Both themes' colours as CSS variables (--gf-own-l / --gf-own-d, ...), for
 * the Gameflow root; web.css picks the theme's pair. Empty without a palette,
 * so the chart keeps its own two colours. */
export function paletteVars(
  own: TeamIdentity | undefined,
  opp: TeamIdentity | undefined,
): Record<string, string> {
  const out: Record<string, string> = {};
  for (const [theme, t] of [["light", "l"], ["dark", "d"]] as const) {
    const p = gamePalette(own, opp, theme);
    if (!p) return {};
    out[`--gf-own-${t}`] = p.own;
    out[`--gf-opp-${t}`] = p.opp;
    out[`--gf-own-text-${t}`] = p.ownText;
    out[`--gf-opp-text-${t}`] = p.oppText;
    out[`--gf-own-label-${t}`] = readable(p.own, theme);
    out[`--gf-opp-label-${t}`] = readable(p.opp, theme);
  }
  return out;
}
