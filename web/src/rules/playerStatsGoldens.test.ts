import { describe, it, expect } from "vitest";
import {
  loadPlayer,
  playerPresets,
  PLAYER_ORACLE_MAP,
} from "../../test/playerGoldens";
describe("Player Stats recorded Shiny parity", () => {
  it("requires all 27 independently recorded Shiny tables", () =>
    expect(playerPresets).toHaveLength(27));
  for (const file of playerPresets)
    it(file, () => {
      const { g, rows } = loadPlayer(file);
      expect(rows).toHaveLength(g.expected.data.length);
      for (let i = 0; i < rows.length; i++)
        for (const [label, value] of Object.entries(g.expected.data[i])) {
          const actual = rows[i][PLAYER_ORACLE_MAP[label] ?? label] ?? null;
          if (typeof value === "number")
            expect(actual, `${i} ${label}`).toBeCloseTo(value, 8);
          else expect(actual, `${i} ${label}`).toEqual(value);
        }
    });
});
