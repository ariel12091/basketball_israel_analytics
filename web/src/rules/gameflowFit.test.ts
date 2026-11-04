import { describe, it, expect } from "vitest";
import {
  aggregateSegments,
  chartHeight,
  clock,
  fitGameflowLayout,
  lineupsRoom,
  windowSegments,
} from "./gameflow";
import { gameflowFiles, loadGameflow } from "../../test/gameflowGoldens";
import geometry from "../data/gameflowLayout.json";

const shown = (lanes: number, box: { width: number; height: number }) => {
  const L = fitGameflowLayout(lanes, box),
    scale = box.width / L.width;
  return { L, scale, height: chartHeight(L, lanes) * scale };
};

describe("lineup time ranges", () => {
  const pieces = (start: number, end: number, dictIndex = 0) => ({
    side: "own" as const,
    dictIndex,
    start,
    end,
    pm: 1,
  });
  it("joins back-to-back pieces and keeps real gaps", () => {
    const [g] = aggregateSegments(
      [pieces(964, 1017), pieces(1017, 1097), pieces(1500, 1600)] as never,
    );
    expect(g.windows).toEqual([
      { start: 964, end: 1097 },
      { start: 1500, end: 1600 },
    ]);
    expect(g.duration).toBe(233);
    expect(g.pm).toBe(3);
  });
  // Real games: no range may end at the clock reading the next one starts at,
  // or the join would still read as two ranges.
  for (const file of gameflowFiles)
    it(`leaves no touching ranges in ${file}`, () => {
      const { answer: a } = loadGameflow(file);
      for (const b of a.bars) {
        const groups = aggregateSegments(
          windowSegments(
            a.lanes,
            a.steps,
            a.dict,
            b.side,
            b.player_key,
            b.start_elapsed,
            b.end_elapsed,
          ),
        );
        for (const g of groups) {
          g.windows.slice(1).forEach((w, i) => {
            expect(clock(w.start)).not.toBe(clock(g.windows[i].end));
          });
          const covered = g.windows.reduce((s, w) => s + w.end - w.start, 0);
          expect(covered).toBeCloseTo(g.duration, 6);
        }
      }
    });
});

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
    expect(lineupsRoom(21, { width: 582, height: 500 })).toBeNull();
  });
  it("makes room for the lineups and the chart still fits above them", () => {
    const space = { width: 1292, height: 536 },
      room = lineupsRoom(21, space)!,
      box = { ...space, height: space.height - room },
      L = fitGameflowLayout(21, box, true),
      scale = box.width / L.width;
    expect(room).toBeGreaterThan(geometry.lineupsMinRoom);
    expect(L.lane_height).toBe(geometry.squeezed.lane_height);
    expect(scale).toBeGreaterThanOrEqual(geometry.minScale);
    expect(chartHeight(L, 21) * scale).toBeLessThanOrEqual(box.height + 0.01);
  });
  it("takes only the squeezed rows when the lineups are open", () => {
    const box = { width: 1292, height: 300 };
    expect(fitGameflowLayout(21, box).lane_height).toBe(
      geometry.tight.lane_height,
    );
    expect(fitGameflowLayout(21, box, true).lane_height).toBe(
      geometry.squeezed.lane_height,
    );
  });
  it("gives the lineups a share of a large window, a floor in a tiny one", () => {
    expect(lineupsRoom(12, { width: 1846, height: 896 })).toBeCloseTo(
      896 * geometry.lineupsShare,
    );
    expect(lineupsRoom(26, { width: 1206, height: 300 })).toBe(
      geometry.lineupsMinRoom,
    );
  });
});
