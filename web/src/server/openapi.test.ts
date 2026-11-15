// "Are we all covered?": the OpenAPI description of /api/v1 is generated from
// the code (server/openapi.ts), and these checks hold it to the routes.
import { readdirSync, statSync } from "node:fs";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import catalogues from "@/data/catalogues.json";
import { compareDefaults, presetChoices } from "@/lib/compareQuery";
import { onoffQueryDefaults } from "@/lib/pageState/onoff";
import { TEAM_VIEWS } from "@/lib/pageState/teamRatings";
import { PLAYER_QUERY_DEFAULTS, TEAM_QUERY_DEFAULTS, queryFromSearch } from "@/lib/tabQuery";
import { PLAYER_MODES } from "@/rules/playerStatsTable";

vi.mock("server-only", () => ({}));
const { openApiSpec } = await import("./openapi");
const { TOOLS } = await import("@/ask/registry");

type Param = { name: string; in: string; description?: string; schema?: { type?: string; enum?: string[] } };
type Op = { operationId: string; parameters: Param[]; responses: Record<string, unknown>; description?: string };
const spec = openApiSpec() as unknown as { openapi: string; info: { title: string }; paths: Record<string, { get: Op }> };
const api = path.resolve("src/app/api/v1");
const op = (route: string) => spec.paths[`/api/v1/{league}/${route}`]?.get;
const query = (route: string) => op(route).parameters.filter((p) => p.in === "query");
const names = (route: string) => query(route).map((p) => p.name).sort();
const RESPONSE_KEYS = ["shape", "format", "v"];
const pageKeys = (defaults: Record<string, string>) => Object.keys(queryFromSearch("il", {}, defaults));

describe("OpenAPI description of /api/v1", () => {
  it("is OpenAPI 3.1 with a title, and every operation has a unique id and 200/400 answers", () => {
    expect(spec.openapi).toBe("3.1.0");
    expect(spec.info.title).toBeTruthy();
    const ids = Object.values(spec.paths).map((p) => p.get.operationId);
    expect(new Set(ids).size).toBe(ids.length);
    for (const [p, item] of Object.entries(spec.paths)) {
      expect(item.get.responses, p).toHaveProperty("200");
      if (p.includes("{league}")) expect(item.get.responses, p).toHaveProperty("400");
    }
  });

  it("has a path for every route the app serves", () => {
    for (const d of readdirSync(path.join(api, "[league]")))
      expect(spec.paths, d).toHaveProperty([`/api/v1/{league}/${d}`]);
    for (const d of readdirSync(api).filter((d) => !["[league]", "ask"].includes(d) && statSync(path.join(api, d)).isDirectory()))
      expect(spec.paths, d).toHaveProperty([`/api/v1/${d}`]);
  });

  it("describes every parameter, with a type", () => {
    for (const [p, item] of Object.entries(spec.paths))
      for (const prm of item.get.parameters ?? []) {
        expect(prm.description, `${p} ${prm.name}`).toBeTruthy();
        expect(prm.schema?.type, `${p} ${prm.name}`).toBeTruthy();
      }
  });

  it("lists exactly what each table route accepts: the page's query, plus shape, format and v", () => {
    expect(names("team-ratings")).toEqual([...pageKeys(TEAM_QUERY_DEFAULTS), ...RESPONSE_KEYS].sort());
    expect(names("player-stats")).toEqual([...pageKeys(PLAYER_QUERY_DEFAULTS), ...RESPONSE_KEYS].sort());
    expect(names("onoff")).toEqual([...pageKeys(onoffQueryDefaults("il")), ...RESPONSE_KEYS].sort());
    expect(names("compare")).toEqual([...pageKeys(compareDefaults), "kind", ...RESPONSE_KEYS].sort());
  });

  it("lists exactly the keys the smaller routes take", () => {
    for (const t of Object.values(TOOLS))
      if (t.route && t.keys) expect(names(t.route), t.route).toEqual([...t.keys, "v"].sort());
  });

  it("takes allowed values from the catalogues and the routes' own lists", () => {
    const codes = query("game-logs").find((p) => p.name === "game_type")!.schema!;
    for (const c of catalogues.gameTypeChoices.filter((c) => c.value !== "")) expect(codes.enum).toContain(c.value);
    expect(query("team-ratings").find((p) => p.name === "view")!.schema!.enum).toEqual(TEAM_VIEWS);
    expect(query("player-stats").find((p) => p.name === "view")!.schema!.enum).toEqual([...PLAYER_MODES]);
    expect(query("compare").find((p) => p.name === "preset")!.schema!.enum).toEqual(["", ...presetChoices.map((c) => c.value)].filter((v, i, a) => a.indexOf(v) === i));
    expect(query("team-ratings").find((p) => p.name === "shape")!.schema!.enum).toEqual(["table"]);
    expect(query("team-ratings").find((p) => p.name === "format")!.schema!.enum).toEqual(["json", "csv"]);
  });

  it("names the leagues each route serves (Compare and the Home hub are Israeli only)", () => {
    const league = (route: string) => op(route).parameters.find((p) => p.in === "path")!.schema!.enum;
    expect(league("team-ratings")).toEqual(["il", "euroleague", "eurocup"]);
    expect(league("compare")).toEqual(["il"]);
    expect(league("home")).toEqual(["il"]);
  });
});
