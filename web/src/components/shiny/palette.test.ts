import { describe, expect, it } from "vitest";
import ui from "@/data/shinyUi.json";
import { heat } from "./DataTable";
import { MUTED_HEAT, mutedAccuracy } from "./palette";

const band = (colour: string | undefined, palette: string[]) =>
  colour === undefined ? -1 : palette.indexOf(colour);
const alpha = (rgba: string) => Number(/,\s*([\d.]+)\)$/.exec(rgba)![1]);

describe("muted heat palette", () => {
  it("runs from reddest to greenest, reversed for lower-is-better", () => {
    expect(MUTED_HEAT).toHaveLength(ui.heat.cuts.length + 1);
    expect(heat(0, false, "muted")).toBe(MUTED_HEAT[0]);
    expect(heat(1, false, "muted")).toBe(MUTED_HEAT[19]);
    expect(heat(1, true, "muted")).toBe(MUTED_HEAT[0]);
  });
  it("keeps the Shiny band cut points, so the ranking is unchanged", () => {
    for (const c of [...ui.heat.cuts, 0.03, 0.51, 0.97]) {
      expect(band(heat(c, false, "muted"), MUTED_HEAT)).toBe(
        band(heat(c, false), ui.heat.grad),
      );
    }
  });
  it("returns nothing for missing or non-numeric ranks", () => {
    expect(heat(null, false, "muted")).toBeUndefined();
    expect(heat("x", false, "muted")).toBeUndefined();
  });
  it("leaves the Shiny palette as the default", () => {
    expect(heat(0.5, false)).toBe(ui.heat.grad[9]);
  });
  it("reads neutral in the middle", () => {
    expect(alpha(MUTED_HEAT[9])).toBeLessThanOrEqual(0.06);
    expect(alpha(MUTED_HEAT[10])).toBeLessThanOrEqual(0.06);
  });
});

describe("muted shot accuracy text", () => {
  it("is neutral at the league average, green above, red below", () => {
    expect(mutedAccuracy(0)).toBe("#cfc8bb");
    expect(mutedAccuracy(1)).toBe("#7fc79a");
    expect(mutedAccuracy(-1)).toBe("#d98b7e");
  });
});
