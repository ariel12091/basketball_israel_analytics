// Stored Shiny tables are the oracle, separately captured API responses the
// inputs. Never derive the expected values by running the TypeScript rules.
import { expect, test } from "vitest";
import { presets, loadGolden } from "../../test/onoffGoldens";

for (const file of presets)
  test(`On/Off Shiny parity: ${file}`, () => {
    const { g, rows } = loadGolden(file);
    const key = (r: Record<string, unknown>) => `${r.Team}:${r.Player}`;
    const actual = new Map(rows.map((r) => [key(r), r]));
    expect(actual.size).toBe(rows.length);
    expect(rows).toHaveLength(g.expected.data.length);
    for (const want of g.expected.data) {
      const got = actual.get(key(want));
      expect(got, key(want)).toBeDefined();
      expect(got!.Team).toBe(want.Team);
      expect(got!.Player).toBe(want.Player);
      // DT frames contain HTML-rendered presentation columns too. Those are
      // compared by the browser suite; every scalar numeric field is checked here.
      for (const [col, value] of Object.entries(want)) {
        if (typeof value === "number") {
          expect(typeof got![col], `${key(want)} ${col}`).toBe("number");
          expect(got![col], `${key(want)} ${col}`).toBeCloseTo(value, 9);
        } else if (value === null || typeof value === "boolean") {
          expect(got![col] ?? null, `${key(want)} ${col}`).toBe(value);
        }
      }
    }
  });
