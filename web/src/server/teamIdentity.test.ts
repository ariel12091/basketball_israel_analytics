import { describe, expect, it } from "vitest";
import { readTeamIdentity } from "./teamIdentity";

/** A query stub that records what it was asked and answers with fixed rows. */
const stub = (rows: Record<string, unknown>[]) => {
  const calls: { sql: string; params: unknown[] }[] = [];
  const q = (async (sql: string, params: unknown[]) => {
    calls.push({ sql, params });
    return rows;
  }) as Parameters<typeof readTeamIdentity>[0];
  return { q, calls };
};

describe("readTeamIdentity", () => {
  it("reads the Israeli season's rows for the asked teams", async () => {
    const { q, calls } = stub([
      { team_id: 2, logo: "https://basket.co.il/m.png", primary_color: "#FFD700", secondary_color: "#003087" },
      { team_id: "3", logo: null, primary_color: null, secondary_color: null },
    ]);
    const m = await readTeamIdentity(q, "il", 2027, [2, 3]);
    expect(calls[0].sql).toContain("basketball_test.season_teams");
    expect(calls[0].params).toEqual([2027, [2, 3]]);
    expect(m.get(2)).toEqual({ logo: "https://basket.co.il/m.png", primary: "#FFD700", secondary: "#003087" });
    expect(m.get(3)).toEqual({ logo: null, primary: null, secondary: null });
  });

  it("reads EuroLeague clubs by their team id, crest as the logo", async () => {
    const { q, calls } = stub([{ team_id: 7, logo: "https://x/c.png", primary_color: "#891631", secondary_color: "#09213F" }]);
    const m = await readTeamIdentity(q, "euroleague", 2026, [7, 8]);
    expect(calls[0].sql).toContain("euroleague.teams");
    expect(calls[0].sql).toContain("crest_url");
    expect(calls[0].params).toEqual([[7, 8]]);
    expect(m.get(7)?.logo).toBe("https://x/c.png");
    expect(m.has(8)).toBe(false);
  });

  it("asks nothing for no teams", async () => {
    const { q, calls } = stub([]);
    expect((await readTeamIdentity(q, "il", 2027, [])).size).toBe(0);
    expect(calls).toHaveLength(0);
  });
});
