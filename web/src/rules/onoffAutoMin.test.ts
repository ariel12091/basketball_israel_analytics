// Web On/Off auto min-poss (deviation D34): both bars fit the rows in view,
// raising as well as lowering -- the top 60% of the league, relaxed to the
// top 75% under a team filter.
import { describe, expect, it } from "vitest";
import {
  AUTO_TOP_PCT,
  AUTO_TOP_PCT_TEAMS,
  autoBars,
  autoShare,
  filterFfRows,
  filterSummaryRows,
} from "./onoff";

const summaryRows = (n: number, team = 2, scale = 10) =>
  Array.from({ length: n }, (_, i) => ({
    team_id: team,
    "ON Poss": (i + 1) * scale,
    "OFF Poss": 5000,
  }));

describe("autoShare", () => {
  it("keeps 60% of the league and relaxes to 75% under a team filter", () => {
    expect(AUTO_TOP_PCT).toBe(0.6);
    expect(autoShare([])).toBe(AUTO_TOP_PCT);
    expect(autoShare([3])).toBe(AUTO_TOP_PCT_TEAMS);
    expect(autoShare([3, 10])).toBe(0.75);
  });
});

describe("autoBars", () => {
  it("shows three in four of a team's rows (Summary)", () => {
    const rows = summaryRows(20);
    const bars = autoBars(rows, "summary", autoShare([2]))!;
    const shown = filterSummaryRows(rows, [], bars.minAll, bars.minOn);
    expect(shown.length / rows.length).toBeGreaterThanOrEqual(0.75);
    expect(shown.length / rows.length).toBeLessThanOrEqual(0.8);
  });

  it("holds the whole league to the harder 60% floor", () => {
    const rows = summaryRows(200); // 10..2000 ON possessions
    const league = autoBars(rows, "summary", autoShare([]))!;
    const team = autoBars(rows, "summary", autoShare([2]))!;
    expect(league.minOn).toBeGreaterThan(team.minOn);
    const shown = filterSummaryRows(rows, [], league.minAll, league.minOn);
    expect(shown).toHaveLength(120); // ceil(200 x 0.6)
  });

  it("raises the bar above the 300 default for a team of heavy-minute players", () => {
    const rows = summaryRows(16, 10, 100); // 100..1600 ON possessions
    const bars = autoBars(rows, "summary", autoShare([10]))!;
    expect(bars.minOn).toBeGreaterThan(300);
    const shown = filterSummaryRows(rows, [], bars.minAll, bars.minOn);
    expect(shown.length).toBe(12);
  });

  it("measures Four Factors' all-side bar on the same top rows as the ON bar", () => {
    const rows = Array.from({ length: 8 }, (_, i) => ({
      team_id: 2,
      off_on_poss: (i + 1) * 100,
      off_off_poss: 900,
      def_on_poss: (i + 1) * 100,
      def_off_poss: 900,
    }));
    const bars = autoBars(rows, "four_factors", autoShare([2]))!;
    const shown = filterFfRows(rows, [], bars.minAll, bars.minOn);
    expect(shown).toHaveLength(6);
  });

  it("returns null when there is nothing to measure", () => {
    expect(autoBars([], "summary", autoShare([]))).toBeNull();
  });
});
