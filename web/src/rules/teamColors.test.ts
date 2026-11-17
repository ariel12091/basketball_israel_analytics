import { expect, test } from "vitest";
import {
  CHART_BG,
  colourDistance,
  contrast,
  gamePalette,
  MIN_BG_CONTRAST,
  MIN_DISTANCE,
  paletteVars,
  type TeamIdentity,
} from "./teamColors";

// Colours as stored on the catalogue rows (basket.co.il's, approved 2026-11-17).
const club = (primary: string, secondary: string): TeamIdentity => ({ logo: null, primary, secondary });
const MACCABI = club("#FFD700", "#003087"),
  HAPOEL_TA = club("#E8001C", "#FFFFFF"),
  HAPOEL_J = club("#E8001C", "#FFFFFF"),
  RAMAT_GAN = club("#0EA5E9", "#003087"),
  EILAT = club("#4FC3F7", "#003087"),
  PARTIZAN = club("#1A1A1A", "#FFFFFF"),
  VIRTUS = club("#1A1A1A", "#FFFFFF");
const THEMES = ["light", "dark"] as const;

test("yellow stays yellow on the light theme (2:1, not a 3:1 mustard)", () => {
  expect(MIN_BG_CONTRAST).toBe(2);
  const own = gamePalette(MACCABI, HAPOEL_TA, "light")!.own;
  expect(contrast(own, CHART_BG.light)).toBeLessThan(2.3);
});
test("each club keeps its own colour: Maccabi yellow, Hapoel red", () => {
  for (const theme of THEMES) {
    const p = gamePalette(MACCABI, HAPOEL_TA, theme)!;
    // Same hue family: only the lightness may move, for contrast.
    expect(colourDistance(p.own, "#FFD700")).toBeLessThan(40);
    expect(colourDistance(p.opp, "#E8001C")).toBeLessThan(40);
    expect(colourDistance(p.own, "#E8001C")).toBeGreaterThan(MIN_DISTANCE);
  }
});

test("two red clubs are told apart: the opponent takes its second colour", () => {
  for (const theme of THEMES) {
    const p = gamePalette(HAPOEL_TA, HAPOEL_J, theme)!;
    expect(colourDistance(p.own, "#E8001C")).toBeLessThan(40);
    expect(colourDistance(p.own, p.opp)).toBeGreaterThanOrEqual(MIN_DISTANCE);
    // Never the other club's red.
    expect(colourDistance(p.opp, "#E8001C")).toBeGreaterThanOrEqual(MIN_DISTANCE);
  }
});

test("near-same blues are told apart (Ramat Gan v Eilat)", () => {
  for (const theme of THEMES)
    for (const [a, b] of [[RAMAT_GAN, EILAT], [EILAT, RAMAT_GAN]]) {
      const p = gamePalette(a, b, theme)!;
      expect(colourDistance(p.own, p.opp)).toBeGreaterThanOrEqual(MIN_DISTANCE);
    }
});

test("two black-and-white clubs still differ", () => {
  for (const theme of THEMES) {
    const p = gamePalette(PARTIZAN, VIRTUS, theme)!;
    expect(colourDistance(p.own, p.opp)).toBeGreaterThanOrEqual(MIN_DISTANCE);
  }
});

test("a club without colours: no palette, the chart keeps its own two colours", () => {
  expect(gamePalette(MACCABI, undefined, "dark")).toBeNull();
  expect(gamePalette({ logo: null, primary: null, secondary: null }, MACCABI, "light")).toBeNull();
});

test("every colour stays visible on the chart, and its numbers readable", () => {
  for (const theme of THEMES)
    for (const [a, b] of [
      [MACCABI, HAPOEL_TA],
      [HAPOEL_TA, HAPOEL_J],
      [PARTIZAN, VIRTUS],
      [EILAT, MACCABI],
    ]) {
      const p = gamePalette(a, b, theme)!;
      for (const [fill, text] of [
        [p.own, p.ownText],
        [p.opp, p.oppText],
      ]) {
        expect(contrast(fill, CHART_BG[theme])).toBeGreaterThanOrEqual(MIN_BG_CONTRAST);
        expect(contrast(text, fill)).toBeGreaterThanOrEqual(4.5);
      }
    }
});

test("paletteVars: both themes' colours as CSS variables, none without a palette", () => {
  const v = paletteVars(MACCABI, HAPOEL_TA);
  expect(Object.keys(v).sort()).toEqual([
    "--gf-opp-d", "--gf-opp-l", "--gf-opp-label-d", "--gf-opp-label-l", "--gf-opp-text-d", "--gf-opp-text-l",
    "--gf-own-d", "--gf-own-l", "--gf-own-label-d", "--gf-own-label-l", "--gf-own-text-d", "--gf-own-text-l",
  ]);
  expect(v["--gf-own-l"]).toBe(gamePalette(MACCABI, HAPOEL_TA, "light")!.own);
  expect(v["--gf-opp-d"]).toBe(gamePalette(MACCABI, HAPOEL_TA, "dark")!.opp);
  expect(paletteVars(MACCABI, undefined)).toEqual({});
});

test("team names read in their club colour: 4.5:1 on the chart, same hue", () => {
  for (const theme of THEMES)
    for (const [a, b] of [[MACCABI, HAPOEL_TA], [EILAT, MACCABI], [PARTIZAN, VIRTUS]]) {
      const v = paletteVars(a, b), t = theme === "light" ? "l" : "d";
      for (const side of ["own", "opp"]) {
        const label = v[`--gf-${side}-label-${t}`];
        expect(contrast(label, CHART_BG[theme])).toBeGreaterThanOrEqual(4.5);
        // Only lightness moves: still near its bar's colour.
        expect(colourDistance(label, v[`--gf-${side}-${t}`])).toBeLessThan(60);
      }
    }
});
