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
const { NOT_TOOLS, TOOLS } = await import("@/ask/registry");

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
    // Internal helpers (the registry's NOT_TOOLS) are left out on purpose.
    for (const d of readdirSync(path.join(api, "[league]")).filter((d) => !(d in NOT_TOOLS)))
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
    // Comma separated, so the codes are listed in the description, not as an enum.
    const codes = query("game-logs").find((p) => p.name === "game_type")!.description!;
    for (const c of catalogues.gameTypeChoices.filter((c) => c.value !== "")) expect(codes).toContain(`${c.value} ${c.label}`);
    expect(query("team-ratings").find((p) => p.name === "view")!.schema!.enum).toEqual(TEAM_VIEWS);
    expect(query("player-stats").find((p) => p.name === "view")!.schema!.enum).toEqual([...PLAYER_MODES]);
    expect(query("compare").find((p) => p.name === "preset")!.schema!.enum).toEqual([...new Set(presetChoices.map((c) => c.value).filter(Boolean))]);
    expect(query("team-ratings").find((p) => p.name === "shape")!.schema!.enum).toEqual(["table"]);
    expect(query("team-ratings").find((p) => p.name === "format")!.schema!.enum).toEqual(["json", "csv"]);
  });

  it("marks the parameters a route cannot answer without as required", () => {
    const required = (route: string) => op(route).parameters.filter((p) => p.in === "query" && (p as { required?: boolean }).required).map((p) => p.name).sort();
    expect(required("gameflow")).toEqual(["game_id", "team_id"]);
    expect(required("home")).toEqual(["team_id"]);
    expect(required("lineup-games")).toEqual(["hash", "season", "team"]);
    expect(required("guide")).toEqual(["topic"]);
    expect(required("team-ratings")).toEqual([]);
    expect(query("gameflow").find((p) => p.name === "team_id")!.schema!.type).toBe("integer");
  });

  it("allows what the routes accept: comma lists are free text, and no empty enum value", () => {
    const gt = query("game-logs").find((p) => p.name === "game_type")!;
    expect(gt.schema!.enum).toBeUndefined();
    expect(gt.description).toMatch(/5 Regular season/);
    for (const [p, item] of Object.entries(spec.paths))
      for (const prm of item.get.parameters) expect(prm.schema?.enum ?? [], `${p} ${prm.name}`).not.toContain("");
  });

  it("states the page's defaults", () => {
    const d = (route: string, name: string) => (query(route).find((p) => p.name === name)!.schema as { default?: string }).default;
    expect(d("team-ratings", "view")).toBe("summary");
    expect(d("team-ratings", "clutch_margin")).toBe("5");
    expect(d("player-stats", "view")).toBe("Per Game");
    expect(d("lineups", "size")).toBe("5");
    expect(d("onoff", "auto")).toBe("true");
  });

  it("documents the busy and timeout answers, with valid media types only", () => {
    const allowed = new Set(["schema", "example", "examples", "encoding"]);
    for (const [p, item] of Object.entries(spec.paths)) {
      if (p.includes("{league}")) for (const code of ["503", "504"]) expect(item.get.responses, `${p} ${code}`).toHaveProperty(code);
      for (const r of Object.values(item.get.responses) as { content?: Record<string, object> }[])
        for (const media of Object.values(r.content ?? {})) for (const k of Object.keys(media)) expect(allowed.has(k), `${p} ${k}`).toBe(true);
    }
  });

  it("leaves internal helpers out and names no fixed host", () => {
    expect(spec.paths).not.toHaveProperty(["/api/v1/{league}/player-stat-filters"]);
    expect((spec as unknown as { servers: { url: string }[] }).servers).toEqual([{ url: "/" }]);
  });

  it("names the leagues each route serves (Compare and the Home hub are Israeli only)", () => {
    const league = (route: string) => op(route).parameters.find((p) => p.in === "path")!.schema!.enum;
    expect(league("team-ratings")).toEqual(["il", "euroleague", "eurocup"]);
    expect(league("compare")).toEqual(["il"]);
    expect(league("home")).toEqual(["il"]);
  });
});
