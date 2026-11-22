import { describe, it, expect } from "vitest";
import {
  applyStatFilters,
  needsOtherView,
  viewsForFilters,
  withOtherView,
} from "./statFilters";
import { teamRatingsTable } from "../lib/pageState/teamRatings";
import { lineupTableRows } from "./lineups";
import type { Row } from "./gameLogs";

describe("stat filters on another view's columns", () => {
  const own = ["off_efg", "off_ppp"],
    other = ["off_fg3_pct", "off_ppp"];
  it("asks for the other view only for a column this view lacks", () => {
    expect(
      needsOtherView([{ col: "off_efg", op: "ge", value: 1 }], own, other),
    ).toBe(false);
    expect(
      needsOtherView([{ col: "off_ppp", op: "ge", value: 1 }], own, other),
    ).toBe(false);
    expect(
      needsOtherView([{ col: "off_fg3_pct", op: "ge", value: 1 }], own, other),
    ).toBe(true);
    expect(
      needsOtherView([{ col: "nope", op: "ge", value: 1 }], own, other),
    ).toBe(false);
  });
  it("adds only missing fields, by key, and an unmatched row fails the filter", () => {
    const key = (r: Row) => `${r.team_id}|${r.sub_lineup_hash}`;
    const ff: Row[] = [
      { team_id: 1, sub_lineup_hash: "a", off_ppp: 1.1, total_poss: 100 },
      { team_id: 1, sub_lineup_hash: "b", off_ppp: 0.9, total_poss: 100 },
    ];
    const summary: Row[] = [
      {
        team_id: 1,
        sub_lineup_hash: "a",
        off_ppp: 9,
        off_fg3_made: 4,
        off_fg3_att: 10,
      },
      { team_id: 2, sub_lineup_hash: "b", off_fg3_made: 9, off_fg3_att: 10 },
    ];
    const joined = withOtherView(ff, summary, key);
    expect(joined[0].off_ppp).toBe(1.1);
    expect(joined[0].off_fg3_att).toBe(10);
    expect(joined[1].off_fg3_att).toBeUndefined();
    // Four Factors rows filtered by Summary's derived 3PT%.
    const rows = lineupTableRows(joined, "four_factors", 0, [
      { col: "off_fg3_pct", op: "ge", value: 30 },
    ]);
    expect(
      rows
        .filter((r) => r.sub_lineup_hash !== "TOTAL")
        .map((r) => r.sub_lineup_hash),
    ).toEqual(["a"]);
    expect(
      applyStatFilters(joined, [{ col: "off_fg3_att", op: "ge", value: 0 }]),
    ).toHaveLength(1);
  });
});

describe("views a page reads for its stat filters", () => {
  const cols = {
    summary: ["off_ppp", "net_rtg"],
    four_factors: ["off_ppp", "off_efg"],
    traditional: ["pts", "tp_pct", "off_efg"],
  };
  const f = (col: string) => ({ col, op: "ge" as const, value: 1 });
  it("names each other view once, the first that offers the column", () => {
    expect(viewsForFilters([f("off_ppp")], cols, "summary")).toEqual([]);
    expect(viewsForFilters([f("off_efg"), f("pts")], cols, "summary")).toEqual([
      "four_factors",
      "traditional",
    ]);
    expect(viewsForFilters([f("tp_pct"), f("pts")], cols, "summary")).toEqual([
      "traditional",
    ]);
  });
  it("Teams: Four Factors filtered by Traditional's 3P%, joined by team", () => {
    const answer = {
      rows: [
        { team_id: 1, Team: "A", off_efg: 50 },
        { team_id: 2, Team: "B", off_efg: 55 },
      ] as Row[],
      meta: {},
    };
    const traditional: Row[] = [
      { team_id: 1, tp_pct: 38 },
      { team_id: 2, tp_pct: 31 },
    ];
    const out = teamRatingsTable(
      answer,
      {
        view: "four_factors",
        stats: JSON.stringify([{ col: "tp_pct", op: "ge", value: 35 }]),
      },
      [traditional],
    );
    expect(out.rows.map((r) => r.team_id)).toEqual([1]);
  });
});
