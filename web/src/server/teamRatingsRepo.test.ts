import { describe, it, expect, vi } from "vitest";
import { applyShootingLuck, parseTeamRatings, readFacts } from "./teamRatingsRepo";
import type { Query } from "./db";
describe("Team Ratings Traditional readers", () => {
  it("accepts Traditional in every league and Shot Profile only in Israel", () => {
    for (const league of ["il", "euroleague", "eurocup"] as const)
      expect(
        parseTeamRatings(new URLSearchParams("view=traditional"), league).view,
      ).toBe("traditional");
    expect(() =>
      parseTeamRatings(new URLSearchParams("view=shot_profile"), "euroleague"),
    ).toThrow();
  });
  it("keeps the eighteen Israeli arguments of get_team_traditional_dynamic", async () => {
    const query = vi.fn<Query>(async () => []);
    await readFacts(
      query as Query,
      parseTeamRatings(
        new URLSearchParams("view=traditional&side=defense&last_n=5"),
        "il",
      ),
      "traditional",
    );
    expect(query.mock.calls[0][0]).toContain(
      "basketball_test.get_team_traditional_dynamic(",
    );
    expect(query.mock.calls[0][1]).toHaveLength(18);
    expect(query.mock.calls[0][1]?.slice(-4)).toEqual([
      null,
      null,
      5,
      "defense",
    ]);
  });
  for (const comp of ["euroleague", "eurocup"] as const)
    for (const [extra, owner, length, clutch] of [
      ["", "pergame", 16, []],
      // The standard preset has no box-score cache: it reads the action fact.
      ["&clutch=true", "direct", 20, [5, "all", 300, false]],
      [
        "&clutch=true&clutch_margin=3&clutch_status=trailing",
        "direct",
        20,
        [3, "trailing", 300, false],
      ],
    ] as const)
      it(`${comp}: ${owner} signature${extra && ` (${extra.slice(1)})`}`, async () => {
        const query = vi.fn<Query>(async () => []);
        await readFacts(
          query as Query,
          parseTeamRatings(
            new URLSearchParams(
              `view=traditional&side=defense&season=2025&teams=19,2&phase=RS&last_n=5${extra}`,
            ),
            comp,
          ),
          "traditional",
        );
        expect(query.mock.calls[0][0]).toContain(
          `euroleague.get_team_traditional_${owner}(`,
        );
        const args = query.mock.calls[0][1]!;
        expect(args).toHaveLength(length);
        expect(args.slice(0, 7)).toEqual([
          comp === "eurocup" ? "U" : "E",
          2025,
          "2025-09-01",
          "2026-07-01",
          "19,2",
          "RS",
          null,
        ]);
        expect(args.slice(12, 12 + clutch.length)).toEqual(clutch);
        expect(args.slice(-4)).toEqual([null, null, 5, "defense"]);
      });
});

describe("Team Ratings shooting luck", () => {
  const team = (id: number, net: number, fg3m: number, ftm: number) => ({
    team_id: id,
    net_rtg: net,
    off_poss: 1000,
    def_poss: 1000,
    off_fg3_made: fg3m,
    off_fg3_att: 300,
    def_fg3_made: 100,
    def_fg3_att: 300,
    off_ftm: ftm,
    off_fta: 200,
    def_ftm: 150,
    def_fta: 200,
  });
  const rows = [team(1, 2, 120, 150), team(2, -2, 90, 150)];
  it("adds the luck to Summary rows at the rows' league rates", async () => {
    const query = vi.fn<Query>(async () => []);
    const p = parseTeamRatings(new URLSearchParams(""), "il");
    const out = await applyShootingLuck(query as Query, p, rows, "ratings");
    expect(query).not.toHaveBeenCalled();
    // r3 = 210 / 600 = 0.35: team 1 made 15 threes over the rate
    expect(out[0].luck_3pt_ours as number).toBeCloseTo(4.5, 6);
    expect(out[0].luck_flag).toBe(true);
  });
  it("leaves the other views alone", async () => {
    const query = vi.fn<Query>(async () => []);
    for (const view of ["four_factors", "traditional"]) {
      const p = parseTeamRatings(new URLSearchParams(`view=${view}`), "il");
      expect(await applyShootingLuck(query as Query, p, rows, "ratings")).toBe(rows);
    }
  });
  it("takes the league rates from all teams when a EuroLeague team filter narrows the rows", async () => {
    const league = [team(1, 2, 120, 150), team(2, -2, 90, 150), team(3, 0, 30, 100)];
    const mock = vi.fn<(sql: string, args?: unknown[]) => Promise<typeof league>>(
      async () => league,
    );
    const query = mock as unknown as Query;
    const p = parseTeamRatings(new URLSearchParams("season=2025&teams=1,2"), "euroleague");
    const out = await applyShootingLuck(query, p, rows, "ratings");
    expect(mock).toHaveBeenCalledTimes(1);
    expect(mock.mock.calls[0][0]).toContain("euroleague.get_team_ratings_pergame(");
    expect(mock.mock.calls[0][1]?.[4]).toBeNull();
    // league r3 = 240 / 900: team 1 made 120 - 80 = 40 over the rate
    expect(out[0].luck_3pt_ours as number).toBeCloseTo(12, 6);
    expect(out).toHaveLength(2);
  });
});
