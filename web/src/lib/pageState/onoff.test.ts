import { describe, expect, it } from "vitest";
import ui from "@/data/shinyUi.json";
import { autoBars, autoShare, onoffTableRows } from "@/rules/onoff";
import { onoffPageOptions, onoffPsPath, onoffQueryDefaults, onoffTable } from "./onoff";

const rows = Array.from({ length: 40 }, (_, i) => ({
  team_id: i % 2 ? 2 : 3,
  player_id: i,
  "ON Poss": (i + 1) * 40,
  "OFF Poss": 4000,
  "Net RTG Diff": i - 20,
}));
const payload = { rows, league3p: null, meta: { view: "summary" as const, filtered: false } };
const q = (o: Record<string, string>) => ({ ...onoffQueryDefaults("il"), ...o });

describe("onoffPageOptions", () => {
  it("auto sets both bars when the link does not turn it off (the page's fresh link)", () => {
    const bars = autoBars(rows, "summary", autoShare([]))!;
    const o = onoffPageOptions(q({ min_on: "50" }), payload, true);
    expect(o.minOn).toBe(bars.minOn);
    expect(o.minAll).toBe(bars.minAll);
  });
  it("auto=false keeps the link's bars", () => {
    const o = onoffPageOptions(q({ auto: "false", min_on: "50", min_all: "7" }), payload, true);
    expect([o.minOn, o.minAll]).toEqual([50, 7]);
  });
  it("the page passes auto off: its own effect has already written the bars", () => {
    const o = onoffPageOptions(q({ min_on: "50" }), payload, false);
    expect(o.minOn).toBe(50);
  });
  it("auto's population is the selected teams' rows, and its share follows the team filter", () => {
    const team = rows.filter((r) => r.team_id === 2);
    const o = onoffPageOptions(q({ teams: "2" }), payload, true);
    expect(o.teams).toEqual([2]);
    expect(o.minOn).toBe(autoBars(team, "summary", autoShare([2]))!.minOn);
  });
  it("with nothing to measure, the defaults stand", () => {
    const o = onoffPageOptions(q({}), { ...payload, rows: [] }, true);
    expect([o.minOn, o.minAll]).toEqual([ui.onoffDefaults.israel.minOn, ui.onoffDefaults.israel.minAll]);
  });
  it("splits Player Stats chips (ps_*) from the table's own filters", () => {
    const stats = '[{"col":"ps_pts","op":"ge","value":10},{"col":"ON Poss","op":"ge","value":100}]';
    const o = onoffPageOptions(q({ stats }), payload, true);
    expect(o.psFilters.map((f) => f.col)).toEqual(["ps_pts"]);
    expect(o.filters).toHaveLength(2);
  });
});

describe("onoffTable", () => {
  it("is the page's table for those options, shape marked", () => {
    const query = q({ teams: "2" });
    const o = onoffPageOptions(query, payload, true);
    const got = onoffTable(payload, query);
    expect(got.rows).toEqual(
      onoffTableRows({ rows, league3p: null, view: "summary" }, { teams: o.teams, minAll: o.minAll, minOn: o.minOn, filters: o.filters }),
    );
    expect(got.rows.every((r) => r.team_id === 2)).toBe(true);
    expect(got.meta.shape).toBe("table");
  });
  it("the page's call (auto already written) uses the query's bars", () => {
    const query = q({ min_on: "900", min_all: "0" });
    const got = onoffTable(payload, query, [], false);
    expect(got.rows.every((r) => Number(r["ON Poss"]) >= 900)).toBe(true);
    expect(got.rows.length).toBe(rows.filter((r) => r["ON Poss"] >= 900).length);
  });
});

describe("onoffPsPath", () => {
  it("is the Player Stats frame for the same game context, without the table's keys", () => {
    const path = onoffPsPath("il", q({ season: "2027", teams: "2", view: "four_factors", stats: "[]", home_away: "home" }));
    const s = new URL(path, "http://x").searchParams;
    expect(path.startsWith("/api/v1/il/player-stat-filters?")).toBe(true);
    expect(s.get("home_away")).toBe("home");
    for (const k of ["view", "teams", "team", "stats", "own_starters", "opp_starters"]) expect(s.has(k), k).toBe(false);
  });
});
