import { readFileSync } from "node:fs";
import { describe, expect, it } from "vitest";
import {
  autoCompareMin,
  compareSummary,
  finalizeCompare,
  joinCompare,
} from "./compare";
import type { Row } from "./gameLogs";

// Regenerate with Rscript scripts/export_compare_rule_goldens.R. Expected
// results come from the unmodified server_tab7_compare.R functions.
const golden = JSON.parse(
  readFileSync("test/goldens/compare/rules.json", "utf8"),
) as {
  datasets: Record<string, Row[]>;
  teams: { team_id: number; team_name: string }[];
  cases: Record<
    string,
    {
      mode: string;
      metric: string;
      reverse: boolean;
      a: string;
      b: string;
      minimum: number;
      autoMin: number | null;
      expected: Row[];
      summary: Record<string, number> | null;
      winner: string | null;
    }
  >;
};
describe("Compare recorded R derived rules (SQL wiring tested separately)", () => {
  for (const [name, g] of Object.entries(golden.cases))
    it(name, () => {
      const joined = joinCompare(
        golden.datasets[g.a],
        golden.datasets[g.b],
        g.mode,
        g.metric,
        golden.teams.map((t) => ({
          value: String(t.team_id),
          label: t.team_name,
        })),
      );
      expect(autoCompareMin(joined)).toBe(g.autoMin);
      const rows = finalizeCompare(
        joined,
        g.mode,
        g.reverse ? "date_split" : "",
        g.minimum,
      );
      expect(rows).toHaveLength(g.expected.length);
      rows.forEach((row, i) => {
        for (const [key, expected] of Object.entries(g.expected[i])) {
          if (typeof expected === "number")
            expect(row[key], `${i}.${key}`).toBeCloseTo(expected, 10);
          else expect(row[key], `${i}.${key}`).toEqual(expected);
        }
      });
      if (g.summary) {
        const summary = compareSummary(rows, g.mode, g.metric);
        for (const [key, expected] of Object.entries(g.summary))
          expect(summary[key as keyof typeof summary]).toBeCloseTo(
            expected,
            10,
          );
        expect(summary.winner).toBe(g.winner);
      }
    });
});
