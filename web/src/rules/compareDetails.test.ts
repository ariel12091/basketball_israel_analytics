import { readFileSync } from "node:fs";
import { expect, it } from "vitest";
import {
  detailMetric,
  detailSpecs,
  joinTeamPlayers,
  playerMetric,
  shootingRates,
  withTeamShots,
  playerSwingValue,
  playerShotSwing,
  playerShotRows,
  formatCompare,
} from "./compare";
import type { Row } from "./gameLogs";
import data from "../data/compareUi.json";
const golden = JSON.parse(
  readFileSync("test/goldens/compare/details.json", "utf8"),
) as {
  a: Row[];
  b: Row[];
  teamPlayers: Record<
    string,
    {
      metric: string;
      rate: string;
      reverse: boolean;
      minimum: number;
      expected: Row[];
    }
  >;
  players: Record<
    string,
    { row: Row[]; rate: string; expected: Record<string, number | null> }
  >;
  details: Record<
    string,
    {
      ratings: Row[];
      ff: Row[];
      shooting: Row[];
      expected: Record<string, number | null>;
    }
  >;
  swings: Record<
    string,
    { ff: Row[]; onoff: Row[] | null; expected: Record<string, number | null> }
  >;
  shots: Record<
    string,
    {
      raw: Row[];
      expected: Record<
        string,
        { d: number | null; on: number | null; off: number | null }
      >;
    }
  >;
  formatted: Record<string,{value: number; fmt: string; expected: string}>;
};
function check(actual: unknown, expected: unknown, label: string) {
  if (typeof expected === "number")
    expect(actual, label).toBeCloseTo(expected, 9);
  else expect(actual, label).toEqual(expected);
}
it("Compare display matches R, including small negative values rounded to zero", () => {
  for(const g of Object.values(golden.formatted))expect(formatCompare(g.value,g.fmt,g.fmt === "net")).toBe(g.expected);
});
// Avoid the full metric x rate x direction x minimum cross-product:
// - Percentages return their raw value in every rate (pinned directly below).
// - The minimum filters raw possessions, independently of metric/rate. Keep
//   its complete rate/direction matrix for points, not every other metric.
// Retain all metrics/directions and all rate-scaled metrics in every rate.
const percentageMetrics = ["fg_pct", "tp_pct", "ft_pct", "efg", "ts", "usg_pct"];
const teamPlayerCases = Object.entries(golden.teamPlayers).filter(([, g]) =>
  (!percentageMetrics.includes(g.metric) || g.rate === "Per Game") &&
  (g.minimum !== 500 || g.metric === "pts"),
);
it("percentage metrics retain their value in every rate, even with zero denominators", () => {
  const row = { gp: 0, poss_on_floor: 0, minutes: 0,
    fg_pct: 40, tp_pct: 35, ft_pct: 80, efg: 52.5, ts: 60, usg_pct: 22 };
  for (const metric of percentageMetrics)
    for (const rate of ["Per Game", "Per 60 Possessions", "Per 75 Possessions", "Per 30 Minutes", "Totals"])
      expect(playerMetric(row, metric, rate), `${metric}/${rate}`).toBe(row[metric as keyof typeof row]);
});
for (const [name, g] of teamPlayerCases)
  it(`Compare R team players ${name}`, () => {
    const rows = joinTeamPlayers(
      golden.a,
      golden.b,
      g.metric,
      g.rate,
      g.minimum,
      g.reverse,
    );
    expect(rows).toHaveLength(g.expected.length);
    rows.forEach((r, i) => {
      for (const [key, value] of Object.entries(g.expected[i]))
        check(r[key], value, `${i}.${key}`);
    });
  });
for (const [name, g] of Object.entries(golden.players))
  it(`Compare R PvP ${name}`, () => {
    for (const [metric, value] of Object.entries(g.expected))
      check(playerMetric(g.row[0], metric, g.rate), value, metric);
  });
for (const [name, g] of Object.entries(golden.details))
  it(`Compare R team detail ${name}`, () => {
    const ratings = withTeamShots(g.ratings[0]),
      shooting = shootingRates(g.shooting);
    for (const section of Object.values(detailSpecs))
      for (const metric of section.metrics)
        check(
          detailMetric(ratings, g.ff[0], shooting, metric),
          g.expected[metric.label],
          metric.label,
        );
  });
for (const [name, g] of Object.entries(golden.swings))
  it(`Compare R FF swing ${name}`, () => {
    for (const stat of data.FF_SWING_STATS)
      check(
        playerSwingValue(g.ff[0], g.onoff?.[0] ?? null, stat),
        g.expected[stat.col],
        stat.col,
      );
  });
for (const [name, g] of Object.entries(golden.shots))
  it(`Compare R Shot Profile swing ${name}`, () => {
    const row = playerShotRows(g.raw[0]);
    for (const [key, expected] of Object.entries(g.expected)) {
      const [side, metric] = key.split("."),
        actual = playerShotSwing(row, side, metric);
      for (const k of ["d", "on", "off"] as const)
        check(actual[k], expected[k], key + "." + k);
    }
  });
