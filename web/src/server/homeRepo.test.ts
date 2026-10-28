import { readFileSync } from "node:fs";
import { expect, it } from "vitest";
import { parseHome, parseHomeDashboard, readHome } from "./homeRepo";
import type { Query } from "./db";
import type { HomeFrames } from "../rules/home";
import ui from "../data/homeUi.json";
const fixture = JSON.parse(
  readFileSync("test/goldens/home/cases.json", "utf8"),
)["2026_2"] as { frames: HomeFrames };
const combined = () =>
  Object.fromEntries(
    Object.keys(ui.contracts).map((k) => [
      `${k}_json`,
      JSON.stringify(fixture.frames[k as keyof HomeFrames]),
    ]),
  );
function mock(fail: string[] = [], badCombined = false) {
  const calls: { sql: string; params: unknown[] }[] = [],
    sql = ui.sql;
  const query: Query = async <T>(statement: string, params: unknown[]) => {
    calls.push({ sql: statement, params });
    if (fail.some((s) => statement === s))
      throw Object.assign(Error("fixture failure"), { code: "42P01" });
    let rows: unknown[] = [];
    if (statement.includes("key='etl_full_last_success'"))
      rows = [{ value: "v1" }];
    if (statement === sql.dashboard) rows = [badCombined ? {} : combined()];
    for (const [source, section] of [
      ["hub_fetch_team_ratings", "ratings"],
      ["hub_fetch_team_ff", "four_factors"],
      ["hub_onoff_df", "onoff"],
      ["hub_ts_df", "traditional"],
      ["hub_lineups_df", "lineups"],
      ["hub_fetch_team_ratings_presets", "storylines"],
      ["storylines_dynamic", "storylines"],
      ["hub_fetch_last_games", "last_games"],
    ] as const)
      if (statement === sql[source]) rows = fixture.frames[section];
    return rows as T[];
  };
  return { query, calls };
}
const p = { season: 2026, team: 2 };
it("uses the original combined dashboard and independent last-game query", async () => {
  const { query, calls } = mock(),
    data = await readHome(query, p);
  expect(data.meta).toMatchObject({
    source: "combined",
    version: "v1",
    errors: [],
  });
  expect(data.frames).toEqual(fixture.frames);
  expect(calls.filter((c) => c.sql === ui.sql.dashboard)).toEqual([
    { sql: ui.sql.dashboard, params: [2026, 2] },
  ]);
  expect(calls.some((c) => c.sql === ui.sql.hub_lineups_df)).toBe(false);
});
it("recovers an SQL failure with a savepoint before using legacy readers", async () => {
  const { query, calls } = mock([
      ui.sql.dashboard,
      ui.sql.hub_fetch_team_ratings_presets,
    ]),
    data = await readHome(query, p);
  expect(data.meta).toMatchObject({ source: "fallback", errors: [] });
  expect(data.frames).toEqual(fixture.frames);
  const failure = calls.findIndex((c) => c.sql === ui.sql.dashboard);
  expect(calls[failure + 1].sql).toBe("ROLLBACK TO SAVEPOINT home_0");
  expect(calls.find((c) => c.sql === ui.sql.hub_lineups_df)?.params).toEqual([
    5,
    "2",
    null,
    null,
    false,
    "2025-09-01",
    "2026-07-01",
    100,
    2026,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    false,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
    null,
  ]);
  expect(calls.some((c) => c.sql === ui.sql.storylines_dynamic)).toBe(true);
});
it("falls back for invalid combined sections and preserves healthy sections on failure", async () => {
  const { query } = mock([ui.sql.hub_onoff_df], true),
    data = await readHome(query, p);
  expect(data.meta).toMatchObject({ source: "fallback", errors: ["onoff"] });
  expect(data.frames.onoff).toEqual([]);
  expect(data.frames.ratings).toEqual(fixture.frames.ratings);
  expect(data.frames.lineups).toEqual(fixture.frames.lineups);
});
it("accepts empty selected-team data but requires the league-wide frames", () => {
  const row = combined();
  row.onoff_json = "[]";
  row.lineups_json = "[]";
  expect(parseHomeDashboard(row)?.onoff).toEqual([]);
  row.ratings_json = "[]";
  expect(parseHomeDashboard(row)).toBeNull();
  expect(
    parseHomeDashboard({ ...combined(), four_factors_json: '[{"team_id":2}]' }),
  ).toBeNull();
  expect(
    parseHomeDashboard({ ...combined(), traditional_json: "not JSON" }),
  ).toBeNull();
});
it("does not turn a cancelled expensive query into a second heavy query", async () => {
  const calls: string[] = [];
  const query: Query = async (sql) => {
    calls.push(sql);
    if (sql === ui.sql.dashboard)
      throw Object.assign(Error("timeout"), { code: "57014" });
    return [];
  };
  await expect(readHome(query, p)).rejects.toMatchObject({ code: "57014" });
  expect(calls).not.toContain(ui.sql.hub_lineups_df);
});
it("validates season and team before reading", () => {
  expect(
    parseHome(new URLSearchParams({ season: "2026", team_id: "2" })),
  ).toEqual(p);
  for (const q of [
    { season: "2026", team_id: "2;SELECT" },
    { season: "2026", team_id: "999" },
    { season: "1900", team_id: "2" },
  ])
    expect(() => parseHome(new URLSearchParams(q))).toThrow();
});
