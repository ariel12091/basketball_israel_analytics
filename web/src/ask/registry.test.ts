import { readdirSync, readFileSync, statSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";
import { NOT_TOOLS, TOOLS } from "./registry";

const api = path.resolve("src/app/api/v1");
const tools = Object.values(TOOLS);

describe("ask tool registry (spec: API mapping)", () => {
  it("every route dir is a tool or excluded", () => {
    const routes = readdirSync(path.join(api, "[league]")).sort();
    const mapped = [
      ...tools.flatMap((t) => (t.route ? [t.route] : [])),
      ...Object.keys(NOT_TOOLS),
    ].sort();
    expect(routes).toEqual(mapped);
    expect(Object.keys(NOT_TOOLS)).toEqual(["player-stat-filters"]);
    const top = readdirSync(api).filter((d) =>
      statSync(path.join(api, d)).isDirectory(),
    );
    expect(top.filter((d) => !["[league]", "version", "ask"].includes(d))).toEqual(
      [],
    );
  });

  it("each tool names the function its route imports", () => {
    for (const t of tools.filter((x) => x.route)) {
      const src = readFileSync(
        path.join(api, "[league]", t.route!, "route.ts"),
        "utf8",
      );
      // Lineups: the route serves the page's table, lineupsPageAnswer, which
      // is the tool's lineupsAnswer plus the lineupPage that pageRows applies.
      const name =
        t.answerName === "lineupsAnswer" ? "lineupsPageAnswer" : t.answerName;
      expect(src, t.name).toMatch(
        new RegExp(`import \\{[^}]*\\b${name}\\b[^}]*\\}`),
      );
      expect(typeof t.answer, t.name).toBe("function");
    }
  });

  it("inventory ids exist", () => {
    const inv = readFileSync(
      path.resolve("../docs/superpowers/specs/2026-10-19-api-mcp-feature-inventory.md"),
      "utf8",
    );
    for (const t of tools) {
      expect(t.inventory.length, t.name).toBeGreaterThan(0);
      for (const id of t.inventory)
        expect(inv.includes(`| ${id} |`), `${t.name}: ${id}`).toBe(true);
    }
  });

  it("ask code never touches the database", () => {
    const files: string[] = [];
    const walk = (d: string) => {
      for (const f of readdirSync(d)) {
        const p = path.join(d, f);
        if (statSync(p).isDirectory()) walk(p);
        else if (/\.tsx?$/.test(f) && !/\.test\.tsx?$/.test(f)) files.push(p);
      }
    };
    walk(path.resolve("src/ask"));
    const registry = readFileSync(path.resolve("src/ask/registry.ts"), "utf8");
    // Allowed: exactly the server modules some API route imports.
    const routeImports = new Set<string>();
    for (const d of readdirSync(path.join(api, "[league]")))
      for (const m of readFileSync(path.join(api, "[league]", d, "route.ts"), "utf8").matchAll(
        /from "@\/server\/(\w+)"/g,
      ))
        routeImports.add(m[1]);
    const allowedRepos = [...registry.matchAll(/from "@\/server\/(\w+(?:Repo|Api))"/g)].map(
      (m) => m[1],
    );
    for (const m of allowedRepos) expect(routeImports, m).toContain(m);
    for (const f of files) {
      const src = readFileSync(f, "utf8");
      expect(src, f).not.toMatch(/(from|import\(|require\()\s*"(@\/server\/db[^"]*|[^"]*\/db|pg)"/);
      expect(src, f).not.toMatch(/\bimport\(|\brequire\(/);
      expect(src, f).not.toMatch(/\bSELECT\b|\bFROM basketball/);
      for (const m of src.matchAll(/from "@\/server\/(\w+(?:Repo|Api))"/g)) {
        expect(path.basename(f), `${f} imports ${m[1]}`).toBe("registry.ts");
        expect(allowedRepos).toContain(m[1]);
      }
    }
  });
});
