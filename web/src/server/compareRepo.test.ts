import { readFileSync } from "node:fs";
import { expect, it } from "vitest";
import {
  compareDynamicArgs,
  parseCompare,
  presetVariant,
  readCompareFacts,
} from "./compareRepo";
import data from "../data/compareUi.json";
import type { Query } from "./db";

const cases = JSON.parse(
  readFileSync("test/goldens/compare/readers.json", "utf8"),
) as Record<
  string,
  {
    query: Record<string, string>;
    variant: string | null;
    calls: { sql: string; params: unknown[] }[];
  }
>;
const normalized = (sql: string) => sql.replace(/\s/g, "").toLowerCase();
for (const [name, g] of Object.entries(cases)) {
  const presetOnly =
    g.variant !== null &&
    parseCompare(new URLSearchParams(g.query)).kind === "ratings";
  it(`Compare R ${presetOnly ? "preset classification" : "SQL signature"} ${name}`, async () => {
    const p = parseCompare(new URLSearchParams(g.query));
    if (p.kind === "ratings") {
      expect(presetVariant(p)).toBe(g.variant);
      if (g.variant !== null) return;
    }
    const calls: { sql: string; params: unknown[] }[] = [];
    const q: Query = async (sql, params) => {
      calls.push({ sql, params });
      return [];
    };
    await readCompareFacts(q, p);
    expect(calls.map((c) => ({ ...c, sql: normalized(c.sql) }))).toEqual(
      g.calls.map((c) => ({ ...c, sql: normalized(c.sql) })),
    );
  });
  if (presetOnly)
    it(`Compare preset reader executes ${name}`, async () => {
      const p = parseCompare(new URLSearchParams(g.query));
      const calls: { sql: string; params: unknown[] }[] = [];
      const rows = [{ team_id: 2, net_rtg: 7 }];
      const q: Query = async <R>(sql: string, params: unknown[]) => {
        calls.push({ sql, params });
        return (sql.startsWith("SELECT") ? rows : []) as R[];
      };
      expect(await readCompareFacts(q, p)).toEqual(rows);
      expect(calls).toEqual([
        { sql: "SAVEPOINT cmp_preset", params: [] },
        {
          sql:
            g.variant === "overall"
              ? "SELECT * FROM basketball_test.team_ppp_ratings_mv WHERE game_year=$1 ORDER BY rank_net_rtg"
              : "SELECT * FROM basketball_test.team_ratings_preset_cache WHERE game_year=$1 AND preset_variant=$2 ORDER BY rank_net_rtg",
          params:
            g.variant === "overall" ? [p.c.season] : [p.c.season, g.variant],
        },
        { sql: "RELEASE SAVEPOINT cmp_preset", params: [] },
      ]);
    });
}

it("Compare missing preset rolls back before executing the dynamic reader", async () => {
  const p = parseCompare(
    new URLSearchParams({ season: "2026", kind: "ratings" }),
  );
  const calls: { sql: string; params: unknown[] }[] = [];
  const rows = [{ team_id: 2 }];
  const q: Query = async <R>(sql: string, params: unknown[]) => {
    calls.push({ sql, params });
    if (sql.includes("team_ppp_ratings_mv"))
      throw new Error("relation unavailable");
    return (sql === data.sql.ratings ? rows : []) as R[];
  };
  expect(await readCompareFacts(q, p)).toEqual(rows);
  expect(calls.map((c) => c.sql)).toEqual([
    "SAVEPOINT cmp_preset",
    "SELECT * FROM basketball_test.team_ppp_ratings_mv WHERE game_year=$1 ORDER BY rank_net_rtg",
    "ROLLBACK TO SAVEPOINT cmp_preset",
    "RELEASE SAVEPOINT cmp_preset",
    data.sql.ratings,
  ]);
  expect(calls.at(-1)!.params).toEqual(compareDynamicArgs(p));
});
