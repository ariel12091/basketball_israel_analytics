// shape=table on the table routes: the page's finished table; no shape is
// today's population answer; any other shape is a 400 (no silent default).
import { describe, expect, it, vi } from "vitest";

const calls: { fn: string; search: string }[] = [];
const answer = (fn: string) => async (_league: string, s: URLSearchParams) => {
  calls.push({ fn, search: s.toString() });
  return { rows: [{ fn }], meta: { version: "v1" } };
};
vi.mock("@/server/playerStatsRepo", () => ({
  playerStatsAnswer: answer("playerStatsAnswer"),
  playerStatsPageAnswer: answer("playerStatsPageAnswer"),
}));
vi.mock("@/server/teamRatingsRepo", () => ({
  teamRatingsAnswer: answer("teamRatingsAnswer"),
  teamRatingsPageAnswer: answer("teamRatingsPageAnswer"),
}));

const routes = {
  "player-stats": await import("@/app/api/v1/[league]/player-stats/route"),
  "team-ratings": await import("@/app/api/v1/[league]/team-ratings/route"),
};
const get = (route: keyof typeof routes, q: string) =>
  routes[route].GET(new Request(`https://x.test/api/v1/il/${route}?${q}`), {
    params: Promise.resolve({ league: "il" }),
  });

describe.each([
  ["player-stats", "playerStats"],
  ["team-ratings", "teamRatings"],
] as const)("%s", (route, name) => {
  it("shape=table answers with the page answer, without the shape key", async () => {
    calls.length = 0;
    const res = await get(route, "season=2027&shape=table&view=x");
    expect(res.status).toBe(200);
    expect(calls).toEqual([{ fn: `${name}PageAnswer`, search: "season=2027&view=x" }]);
  });
  it("no shape keeps the population answer", async () => {
    calls.length = 0;
    await get(route, "season=2027");
    expect(calls.map((c) => c.fn)).toEqual([`${name}Answer`]);
  });
  it("an unknown shape is a 400 naming the parameter", async () => {
    calls.length = 0;
    const res = await get(route, "season=2027&shape=full");
    expect(res.status).toBe(400);
    expect(JSON.stringify(await res.json())).toContain("shape");
    expect(calls).toEqual([]);
  });
});
