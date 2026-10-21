import { readFileSync } from "node:fs";
import path from "node:path";
import { describe, expect, it } from "vitest";

// Rule 4 of docs/shared_definitions.md: no twin without a pin. The web app
// renders with Shiny's stylesheets; a copy that differs is a redesign by drift.
describe("Shiny stylesheets", () => {
  for (const name of ["app.css", "mobile.css"]) {
    it(`${name} equals app/www/${name}`, () => {
      const copy = readFileSync(path.resolve("src/styles/shiny", name));
      const original = readFileSync(path.resolve("../app/www", name));
      expect(copy.equals(original), "run npm run sync-styles").toBe(true);
    });
  }
});
