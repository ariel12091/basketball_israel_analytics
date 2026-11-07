// Web On/Off auto min-poss (deviation D34): both bars fit the rows in view so
// about three in four of them show, raising as well as lowering.
import { describe, expect, it } from "vitest";
import { autoBars, filterFfRows, filterSummaryRows } from "./onoff";

const summaryRows = (n: number, team = 2, scale = 10) =>
  Array.from({ length: n }, (_, i) => ({
    team_id: team,
    "ON Poss": (i + 1) * scale,
    "OFF Poss": 5000,
  }));

describe("autoBars", () => {
  it("shows three in four of the rows in view (Summary)", () => {
    const rows = summaryRows(20);
    const bars = autoBars(rows, "summary")!;
    const shown = filterSummaryRows(rows, [], bars.minAll, bars.minOn);
    expect(shown.length / rows.length).toBeGreaterThanOrEqual(0.75);
    expect(shown.length / rows.length).toBeLessThanOrEqual(0.8);
  });

  it("raises the bar above the 300 default for a team of heavy-minute players", () => {
    const rows = summaryRows(16, 10, 100); // 100..1600 ON possessions
    const bars = autoBars(rows, "summary")!;
    expect(bars.minOn).toBeGreaterThan(300);
    const shown = filterSummaryRows(rows, [], bars.minAll, bars.minOn);
    expect(shown.length).toBe(12);
  });

  it("measures Four Factors' all-side bar on the rows that clear the ON bar", () => {
    const rows = Array.from({ length: 8 }, (_, i) => ({
      team_id: 2,
      off_on_poss: (i + 1) * 100,
      off_off_poss: 900,
      def_on_poss: (i + 1) * 100,
      def_off_poss: 900,
    }));
    const bars = autoBars(rows, "four_factors")!;
    const shown = filterFfRows(rows, [], bars.minAll, bars.minOn);
    expect(shown).toHaveLength(6);
  });

  it("returns null when there is nothing to measure", () => {
    expect(autoBars([], "summary")).toBeNull();
  });
});
