import { readFileSync } from "node:fs";
import { expect, it } from "vitest";
import {
  compareDynamicArgs,
  parseCompare,
  presetVariant,
  readCompareFacts,
} from "./compareRepo";
import data from "../data/compareUi.json";
import { needsFilteredPath, seasonBounds } from "./contextParams";
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
// Shiny's default dates are the whole season; the web's default window starts
// on 1 October (defaultDates). The goldens record Shiny's calls, so give each
// golden query the dates Shiny used.
const shinyQuery = (query: Record<string, string>) => {
  const b = seasonBounds("il", Number(query.season));
  return new URLSearchParams({ start: b.start, end: b.end, ...query });
};
for (const [name, g] of Object.entries(cases)) {
  const presetOnly =
    g.variant !== null && parseCompare(shinyQuery(g.query)).kind === "ratings";
  it(`Compare R ${presetOnly ? "preset classification" : "SQL signature"} ${name}`, async () => {
    const p = parseCompare(shinyQuery(g.query));
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
    // With no filter set, player Four Factors reads On/Off's season views
    // (seasonFourFactors), not the reader Shiny called; their equality is
    // pinned in app/tests/testthat/test-player-reader-paths-db.R.
    if (p.kind === "player_ff" && !needsFilteredPath(p.c)) {
      expect(calls).toEqual([
        {
          sql: "SELECT * FROM basketball_test.player_advanced_stats_mv WHERE game_year = $1::int4",
          params: [p.c.season],
        },
        {
          sql: expect.stringContaining("FROM basketball_test.onoff_default_mv"),
          params: [p.c.season],
        },
      ]);
      return;
    }
    expect(calls.map((c) => ({ ...c, sql: normalized(c.sql) }))).toEqual(
      g.calls.map((c) => ({ ...c, sql: normalized(c.sql) })),
    );
  });
  if (presetOnly)
    it(`Compare preset reader executes ${name}`, async () => {
      const p = parseCompare(shinyQuery(g.query));
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
  const p = parseCompare(shinyQuery({ season: "2026", kind: "ratings" }));
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

it("EuroLeague player Four Factors: season views with no filter, the reader otherwise", async () => {
  const read = async (query: Record<string, string>) => {
    const calls: string[] = [];
    const q: Query = async <R>(sql: string) => {
      calls.push(sql);
      return (sql.includes("player_advanced_stats_mv")
        ? [{ player_id: 1, team_id: 7, game_year: 2025 }, { player_id: 2, team_id: 9, game_year: 2025 }]
        : []) as R[];
    };
    const rows = await readCompareFacts(
      q,
      parseCompare(new URLSearchParams({ kind: "player_ff", season: "2025", ...query }), "euroleague"),
    );
    return { calls, rows };
  };
  const season = await read({ teams: "7" });
  expect(season.calls).toEqual([
    expect.stringContaining("FROM euroleague.player_advanced_stats_mv"),
    expect.stringContaining("FROM euroleague.player_onoff_default_mv"),
  ]);
  expect(season.rows.map((r) => r.player_id)).toEqual([1]);
  const filtered = await read({ home_away: "home" });
  expect(filtered.calls).toEqual([
    expect.stringContaining("euroleague.four_factors_dashboard_compute"),
  ]);
});
