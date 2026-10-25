import { describe, expect, it } from "vitest";
import { flatten } from "./flatten";
import { cellText } from "./display";

const payload = {
  rows: [
    { team_id: 2, player_id: 1039, name: "YAM MADAR", minutes: 300 },
    { team_id: 3, player_id: 7, name: "OTHER", minutes: 200 },
  ],
};

describe("lineup_roster tool", () => {
  it("keeps one team's players when team is given, every team otherwise", () => {
    const one = flatten("lineup_roster", "il", payload, new URLSearchParams("season=2027&team=2"));
    expect(one.rows.map((r) => r.player_id)).toEqual([1039]);
    const all = flatten("lineup_roster", "il", payload, new URLSearchParams("season=2027"));
    expect(all.rows).toHaveLength(2);
  });

  it("player names are quotable, minutes are not", () => {
    const row = { id: "row1", cells: payload.rows[0] };
    expect(cellText("lineup_roster", "il", null, "name", row)).toBe("YAM MADAR");
    expect(cellText("lineup_roster", "il", null, "minutes", row)).toBeNull();
  });
});
