import { beforeEach, expect, it, vi } from "vitest";
import { readFileSync } from "node:fs";
import type { Row } from "../rules/gameLogs";
const { query } = vi.hoisted(() => ({ query: vi.fn() }));
vi.mock("./db", async (original) => ({
  ...(await original<typeof import("./db")>()),
  withSnapshot: (_deadline: unknown, read: (q: typeof query) => unknown) =>
    read(query),
}));
import { fetchSeason, QUERIES } from "./gameLogsRepo";
import { gameLogsAnswer } from "./gameLogsApi";
beforeEach(() => {
  query.mockReset();
});
for (const view of ["summary", "four_factors"])
  it(`[GL-03][GL-06] Israeli ${view}: SQL score availability reaches the API rows`, async () => {
    vi.resetModules();
    const { gameLogsAnswer: answer } = await import("./gameLogsApi");
    const file = view === "summary" ? "summary" : "ff";
    const g = JSON.parse(
      readFileSync(`test/goldens/game_logs/il_${file}_default.json`, "utf8"),
    );
    const absent = g.input.schedule[0] as Row;
    query.mockImplementation(async (sql: string, args: unknown[]) => {
      if (sql === QUERIES.ilVersion) return [{ value: "audit-v1" }];
      if (sql === QUERIES.schedule) return g.input.schedule;
      if (sql === QUERIES.summary || sql === QUERIES.ff) return g.input.rows;
      if (sql === QUERIES.scoreless) {
        expect(args).toEqual([
          [...new Set(g.input.schedule.map((r: Row) => Number(r.game_id)))],
        ]);
        return [{ game_id: absent.game_id, team_id: absent.team_id }];
      }
      throw Error(`Unexpected SQL owner: ${String(sql)}`);
    });
    const result = await answer(
      "il",
      new URLSearchParams({ season: "2026", view }),
    );
    expect(result.rows.length).toBeGreaterThan(0);
    const gated = result.rows.filter(
      (r) => r.game_id === absent.game_id && r.team_id === absent.team_id,
    );
    expect(gated).toHaveLength(1);
    expect(gated[0].has_scores).toBe(false);
    expect(
      result.rows
        .filter((r) => !gated.includes(r))
        .every((r) => r.has_scores === true),
    ).toBe(true);
    expect(
      query.mock.calls.filter(([sql]) => sql === QUERIES.scoreless),
    ).toHaveLength(1);
  });
for (const league of ["euroleague", "eurocup"] as const)
  it(`[GL-06] ${league} retains Shiny's ungated Gameflow links`, async () => {
    query.mockImplementation(async (sql: string) => {
      if (sql === QUERIES.euroVersion) return [{ value: "audit-v1" }];
      if (sql === QUERIES.euroSeasons) return [{ game_year: 2025 }];
      if (sql === QUERIES.euro) return [];
      throw Error(`Unexpected Israeli score-feed read: ${String(sql)}`);
    });
    const result = await gameLogsAnswer(
      league,
      new URLSearchParams({ season: "2025" }),
    );
    expect(result.rows).toEqual([]);
    expect(query.mock.calls.some(([sql]) => sql === QUERIES.scoreless)).toBe(
      false,
    );
  });
it("empty Israeli schedules do not query unrelated scoring feeds", async () => {
  query.mockResolvedValue([]);
  const result = await fetchSeason(query, {
    league: "il",
    season: 2026,
    view: "summary",
  });
  expect(result.scoreless).toEqual([]);
  expect(query.mock.calls.some(([sql]) => sql === QUERIES.scoreless)).toBe(
    false,
  );
});
