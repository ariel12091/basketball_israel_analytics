// shape=table on the table routes: the page's finished table; no shape is
// today's population answer; any other shape is a 400 (no silent default).
import { describe, expect, it, vi } from "vitest";

const calls: { fn: string; search: string }[] = [];
const answer = (fn: string) => async (_league: string, s: URLSearchParams) => {
  calls.push({ fn, search: s.toString() });
  return { rows: [{ fn }], meta: { version: "v1" } };
};
vi.mock("@/server/playerStatsRepo", () => ({ playerStatsAnswer: answer("playerStatsAnswer") }));
vi.mock("@/server/teamRatingsRepo", () => ({ teamRatingsAnswer: answer("teamRatingsAnswer") }));
vi.mock("@/server/pageAnswers", () => ({
  playerStatsPageAnswer: answer("playerStatsPageAnswer"),
  teamRatingsPageAnswer: answer("teamRatingsPageAnswer"),
  onoffPageAnswer: answer("onoffPageAnswer"),
  lineupsPageAnswer: answer("lineupsPageAnswer"),
}));
vi.mock("@/server/gameLogsApi", () => ({ gameLogsAnswer: answer("gameLogsAnswer") }));

vi.mock("@/server/onoffApi", () => ({ onoffAnswer: answer("onoffAnswer") }));

// Compare's readers take the query only (Israeli League only).
const one = (fn: string) => async (s: URLSearchParams) => answer(fn)("il", s);
vi.mock("@/server/compareRepo", () => ({ compareAnswer: one("compareAnswer") }));
vi.mock("@/server/comparePage", () => ({ comparePageAnswer: one("comparePageAnswer") }));

const routes = {
  compare: await import("@/app/api/v1/[league]/compare/route"),
  "player-stats": await import("@/app/api/v1/[league]/player-stats/route"),
  "team-ratings": await import("@/app/api/v1/[league]/team-ratings/route"),
  onoff: await import("@/app/api/v1/[league]/onoff/route"),
  lineups: await import("@/app/api/v1/[league]/lineups/route"),
  "game-logs": await import("@/app/api/v1/[league]/game-logs/route"),
};
const get = (route: keyof typeof routes, q: string) =>
  routes[route].GET(new Request(`https://x.test/api/v1/il/${route}?${q}`), {
    params: Promise.resolve({ league: "il" }),
  });

describe.each([
  ["player-stats", "playerStats"],
  ["team-ratings", "teamRatings"],
  ["onoff", "onoff"],
  ["compare", "compare"],
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

// Lineups and Game Logs already answer with the page's table: shape=table is
// accepted as a no-op and any other shape is still refused.
describe.each([
  ["lineups", "lineupsPageAnswer"],
  ["game-logs", "gameLogsAnswer"],
] as const)("%s shape", (route, fn) => {
  it("accepts shape=table (already the table) and drops the key", async () => {
    calls.length = 0;
    const res = await get(route, "season=2027&shape=table");
    expect(res.status).toBe(200);
    expect(calls).toEqual([{ fn, search: "season=2027" }]);
  });
  it("refuses any other shape", async () => {
    calls.length = 0;
    const res = await get(route, "season=2027&shape=full");
    expect(res.status).toBe(400);
    expect(calls).toEqual([]);
  });
});

describe.each([
  ["player-stats", "playerStatsPageAnswer", "Team"],
  ["team-ratings", "teamRatingsPageAnswer", "Team"],
  ["onoff", "onoffPageAnswer", "Team"],
  ["compare", "comparePageAnswer", "#"],
  ["lineups", "lineupsPageAnswer", "Team"],
  ["game-logs", "gameLogsAnswer", "Game Type"],
] as const)("%s format=csv", (route, fn, first) => {
  it("serves the finished table as the page's CSV, named for a download", async () => {
    calls.length = 0;
    const res = await get(route, "season=2027&format=csv");
    expect(res.status).toBe(200);
    expect(calls.map((c) => c.fn)).toEqual([fn]);
    expect(calls[0].search).not.toContain("format");
    expect(res.headers.get("Content-Type")).toBe("text/csv; charset=utf-8");
    expect(res.headers.get("Content-Disposition")).toBe(`attachment; filename="${route}_il_2027.csv"`);
    expect((await res.text()).startsWith(`"${first}"`)).toBe(true);
  });
  it("refuses any other format", async () => {
    const res = await get(route, "season=2027&format=xlsx");
    expect(res.status).toBe(400);
  });
});
