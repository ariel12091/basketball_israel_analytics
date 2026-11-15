// Every feature in the inventory (docs/superpowers/specs/
// 2026-10-19-api-mcp-feature-inventory.md) is served by the API -- through a
// route the Ask registry maps it to, or a route-level entry in API_COVERAGE --
// or is named page-only with a reason. A new inventory row fails this until it
// is placed.
import { readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, it, vi } from "vitest";
import { API_COVERAGE, CLIENT_ONLY } from "./apiCoverage";

vi.mock("server-only", () => ({}));
const { TOOLS } = await import("@/ask/registry");

const inventory = [
  ...readFileSync(path.resolve("../docs/superpowers/specs/2026-10-19-api-mcp-feature-inventory.md"), "utf8").matchAll(
    /^\| \*{0,2}([A-Z]+-\d+)\*{0,2} \|/gm,
  ),
].map((m) => m[1]);
const viaTools = new Set(Object.values(TOOLS).flatMap((t) => t.inventory));
const viaRoutes = Object.values(API_COVERAGE).flat();

describe("API coverage of the feature inventory", () => {
  it("reads the whole inventory", () => {
    expect(new Set(inventory).size).toBe(109);
  });
  it("places every feature exactly once: served (tools or routes) or page-only", () => {
    for (const id of new Set(inventory)) {
      const served = viaTools.has(id) || viaRoutes.includes(id);
      expect(served !== id in CLIENT_ONLY, id).toBe(true);
    }
  });
  it("route-level entries add features the tools do not already list, once each", () => {
    for (const id of viaRoutes) expect(viaTools.has(id), id).toBe(false);
    expect(new Set(viaRoutes).size).toBe(viaRoutes.length);
  });
  it("names only real features, each page-only one with a reason", () => {
    for (const id of [...viaRoutes, ...Object.keys(CLIENT_ONLY)]) expect(inventory, id).toContain(id);
    for (const [id, why] of Object.entries(CLIENT_ONLY)) expect(why.length, id).toBeGreaterThan(10);
  });
});
