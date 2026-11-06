import { describe, it, expect, vi } from "vitest";
import { parseTeamRatings, readFacts } from "./teamRatingsRepo";
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
