import { describe, it, expect } from "vitest";
import { readFileSync, readdirSync } from "node:fs";
import {
  gameLogs,
  percentRank,
  type GameLogParams,
  type SeasonInput,
  type Row,
} from "./gameLogs";
import { rRound } from "./round";
describe("R parity", () => {
  it("rounds ties to even", () => {
    expect([0.5, 1.5, 2.5, -0.5].map((x) => rRound(x))).toEqual([0, 2, 2, 0]);
  });
  it("percent rank preserves missing values", () =>
    expect(percentRank([3, 1, null, 3, 2])).toEqual([
      2 / 3,
      0,
      null,
      2 / 3,
      1 / 3,
    ]));
  for (const file of readdirSync("test/goldens/game_logs").filter((x) =>
    x.endsWith(".json"),
  )) {
    it(
      file,
      () => {
        const g = JSON.parse(
          readFileSync("test/goldens/game_logs/" + file, "utf8"),
        ) as {
          input: SeasonInput;
          params: GameLogParams;
          expected: { data: Row[] } | null;
        };
        const result = gameLogs(g.input, g.input.scoreless ?? null, g.params);
        const expected = g.expected?.data ?? [];
        expect(result.rows.length).toBe(expected.length);
        const stored = expected.map((row) =>
          Object.fromEntries(
            Object.entries(row).filter(([col]) => col !== "gameflow"),
          ),
        );
        const actual = result.rows.map((row, i) =>
          Object.fromEntries(
            Object.keys(stored[i]).map((col) => [col, row[col]]),
          ),
        );
        expect(actual).toEqual(stored);
      },
      30000,
    );
  }
});
