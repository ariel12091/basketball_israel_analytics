import { describe, it, expect, vi } from "vitest";
import { parsePlayerStats, readPlayerFacts } from "./playerStatsRepo";
import type { Query } from "./db";
describe("Player Stats existing SQL contracts (helpers.R)", () => {
  for (const league of ["il", "euroleague", "eurocup"] as const)
    it(`${league}: team-only requests keep the season MV fast path`, async () => {
      const query = vi.fn<Query>(async () => []),
        p = parsePlayerStats(
          new URLSearchParams(
            `teams=1,2&season=${league === "il" ? 2026 : 2025}`,
          ),
          league,
        );
      await readPlayerFacts(query as Query, p);
      expect(query.mock.calls[0][0]).toContain("player_traditional_stats_mv");
      expect(query.mock.calls[0][1]).toEqual(
        league === "il" ? [2026] : [league === "eurocup" ? "U" : "E", 2025],
      );
    });
  it("preserves all eighteen Israeli arguments and selects the custom-clutch owner", async () => {
    const query = vi.fn<Query>(async () => []),
      p = parsePlayerStats(
        new URLSearchParams(
          "season=2026&teams=1,2&start=2025-11-01&end=2026-02-01&game_type=1&opponents=3&home_away=home&outcome=win&opp_rank_side=top&opp_rank_n=3&opp_rank_metric=net&clutch=true&clutch_margin=3&clutch_status=trailing&clutch_minutes=2&ot_margin=true&min_gn=3&max_gn=8",
        ),
        "il",
      );
    await readPlayerFacts(query as Query, p);
    expect(query.mock.calls[0][0]).toContain(
      "basketball_test.get_player_traditional_custom_clutch(",
    );
    expect(query.mock.calls[0][1]).toEqual([
      2026,
      "2025-11-01",
      "2026-02-01",
      "1,2",
      "1",
      "3",
      "home",
      "win",
      "top",
      3,
      "net",
      3,
      "trailing",
      120,
      true,
      3,
      8,
      null,
    ]);
  });
  it("sends standard Israeli clutch through from_games", async () => {
    const query = vi.fn<Query>(async () => []);
    await readPlayerFacts(
      query as Query,
      parsePlayerStats(new URLSearchParams("clutch=true&last_n=5"), "il"),
    );
    expect(query.mock.calls[0][0]).toContain(
      "get_player_traditional_from_games(",
    );
    expect(query.mock.calls[0][1]?.slice(11)).toEqual([
      5,
      "all",
      300,
      false,
      null,
      null,
      5,
    ]);
  });
  for (const comp of ["euroleague", "eurocup"] as const)
    for (const [extra, owner, length] of [
      ["", "pergame", 15],
      ["&clutch=true", "standard_clutch", 15],
      ["&clutch=true&clutch_margin=3", "custom_clutch", 19],
    ] as const)
      it(`${comp}: ${owner} signature`, async () => {
        const query = vi.fn<Query>(async () => []);
        await readPlayerFacts(
          query as Query,
          parsePlayerStats(
            new URLSearchParams(
              `season=2025&teams=19,2&phase=RS&last_n=5${extra}`,
            ),
            comp,
          ),
        );
        expect(query.mock.calls[0][0]).toContain(
          `euroleague.get_player_traditional_${owner}(`,
        );
        expect(query.mock.calls[0][1]).toHaveLength(length);
        expect(query.mock.calls[0][1]?.slice(0, 7)).toEqual([
          comp === "eurocup" ? "U" : "E",
          2025,
          "2025-09-01",
          "2026-07-01",
          "19,2",
          "RS",
          null,
        ]);
        expect(query.mock.calls[0][1]?.slice(-3)).toEqual([null, null, 5]);
      });
});
