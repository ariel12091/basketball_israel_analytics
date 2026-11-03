import { describe, it, expect } from "vitest";
import { chartHeight, fitGameflowLayout } from "./gameflow";
import geometry from "../data/gameflowLayout.json";

const shown = (lanes: number, box: { width: number; height: number }) => {
  const L = fitGameflowLayout(lanes, box),
    scale = box.width / L.width;
  return { L, scale, height: chartHeight(L, lanes) * scale };
};

describe("fitGameflowLayout", () => {
  it("draws the chart's own height", () => {
    // 8 + 9 lanes as GameflowChart lays them out (il_regular: viewBox height 491).
    expect(chartHeight(geometry.desktop, 17)).toBe(491);
  });
  it("fits a full roster on a laptop screen, filling the width", () => {
    const box = { width: 1206, height: 488 },
      { L, scale, height } = shown(26, box);
    expect(height).toBeLessThanOrEqual(box.height + 0.01);
    expect(L.lane_height).toBe(geometry.tight.lane_height);
    expect(scale).toBeGreaterThan(geometry.minScale);
    // Shown width stays the column's: spare room goes to the time axis.
    expect(L.width * scale).toBeCloseTo(box.width);
  });
  it("keeps today's drawing when it already fits", () => {
    const L = fitGameflowLayout(17, { width: 1366, height: 700 });
    expect(L).toEqual(geometry.desktop);
  });
  it("never enlarges past the width's natural scale", () => {
    const box = { width: 1000, height: 5000 };
    expect(shown(12, box).scale).toBeCloseTo(box.width / geometry.desktop.width);
  });
  it("stops shrinking at the legible minimum and lets the page scroll", () => {
    const box = { width: 1206, height: 200 },
      { scale, height } = shown(26, box);
    expect(scale).toBe(geometry.minScale);
    expect(height).toBeGreaterThan(box.height);
  });
  it("leaves a narrow column alone", () => {
    expect(fitGameflowLayout(21, { width: 582, height: 0 })).toEqual(
      geometry.desktop,
    );
  });
});
