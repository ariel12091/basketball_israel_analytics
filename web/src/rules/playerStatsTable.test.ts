import { describe, it, expect } from "vitest";
import {
  playerDisplay,
  playerPopulation,
  playerPr,
  playerRateThreshold,
  type Identity,
} from "./playerStatsTable";
import type { Row } from "./gameLogs";
const lookup: Identity[] = [
  {
    team_id: 1,
    player_id: 7,
    identity_id: "person",
    display_name: "Canonical Name",
  },
  {
    team_id: 2,
    player_id: 8,
    identity_id: "person",
    display_name: "Canonical Name",
  },
];
const row = (patch: Row): Row => ({
  team_id: 1,
  player_id: 7,
  team_name: "A",
  Player: "Provider Name",
  gp: 2,
  minutes: 40,
  poss_on_floor: 100,
  pts: 20,
  fgm: 8,
  fga: 16,
  "3pm": 2,
  "3pa": 6,
  ftm: 2,
  fta: 2,
  ts_att: 18,
  usg_pct: 20,
  reb: 8,
  oreb: 3,
  dreb: 5,
  ast: 4,
  stl: 2,
  blk: 1,
  dfl: 1,
  tov: 2,
  ...patch,
});
describe("Player Stats Tab 5 rules", () => {
  it("preserves R's floating cutoff at an integer possession boundary", () => {
    expect(
      playerRateThreshold(Array.from({ length: 321 }, (_, i) => i + 1)),
    ).toBeGreaterThan(49);
  });
  it("combines resolved identities with trip-based TS and possession-weighted USG", () => {
    const population = playerPopulation(
      [
        row({}),
        row({
          team_id: 2,
          player_id: 8,
          team_name: "B",
          poss_on_floor: 200,
          pts: 30,
          usg_pct: 35,
          ts_att: 20,
        }),
      ],
      lookup,
    );
    expect(population).toHaveLength(3);
    expect(population[0].Player).toBe("Canonical Name");
    expect(population[2]).toMatchObject({
      team_name: "TOTAL",
      gp: 4,
      pts: 50,
      poss_on_floor: 300,
      minutes: 80,
      usg_pct: 30,
      ts: 65.8,
      is_multi_team_total: true,
    });
    const shown = playerDisplay(population, lookup, {
      mode: "Per Game",
      players: ["1:7"],
    }).rows;
    expect(shown.map((r) => r.team_name)).toEqual(["A", "TOTAL"]);
    expect(shown[1].pts).toBe(12.5);
  });
  it("never joins unresolved players by name; team narrowing precedes TOTAL construction", () => {
    expect(
      playerPopulation([row({}), row({ team_id: 2, player_id: 8 })], []),
    ).toHaveLength(2);
    expect(
      playerPopulation(
        [row({}), row({ team_id: 2, player_id: 8 })],
        lookup,
        [1],
      ),
    ).toHaveLength(1);
  });
  it("keeps percentages and actual sample sizes when scaling rate modes", () => {
    const population = playerPopulation([row({ fg_pct: 50 })], []);
    expect(
      playerDisplay(population, [], { mode: "Per 60 Possessions" }).rows[0],
    ).toMatchObject({
      pts: 12,
      minutes: 24,
      poss_on_floor: 60,
      total_poss: 100,
      base_poss_pg: 50,
      fg_pct: 50,
    });
    expect(
      playerDisplay(population, [], { mode: "Per 30 Minutes" }).rows[0],
    ).toMatchObject({
      pts: 15,
      minutes: 30,
      poss_on_floor: 75,
      base_min_pg: 20,
    });
    expect(playerDisplay(population, [], { mode: "Totals" }).rows[0].pts).toBe(
      20,
    );
  });
  it("ranks the full population before player/GP/stat narrowing", () => {
    const population = playerPopulation(
      [
        row({ pts: 10, poss_on_floor: 1000 }),
        row({ player_id: 9, pts: 30, poss_on_floor: 1000 }),
        row({ player_id: 10, pts: 20, poss_on_floor: 1000 }),
      ],
      [],
    );
    const options = { mode: "Per Game" as const };
    const full = playerDisplay(population, [], options).rows;
    const narrowed = playerDisplay(population, [], {
      ...options,
      players: ["1:10"],
      filters: [{ col: "pts", op: "ge", value: 9 }],
    }).rows;
    expect(narrowed).toHaveLength(1);
    expect(narrowed[0][playerPr("PTS")]).toBe(0.5);
    expect(narrowed[0][playerPr("PTS")]).toBe(
      full.find((r) => r.player_id === 10)?.[playerPr("PTS")],
    );
  });
  it("trims the bottom 15% only for a rate population of at least 120, excluding totals", () => {
    const population = Array.from({ length: 120 }, (_, i) =>
      row({ player_id: i + 1, poss_on_floor: i + 1 }),
    );
    const hidden = playerDisplay(population, [], {
      mode: "Per 60 Possessions",
    });
    expect(hidden.threshold).toBeCloseTo(18.85);
    expect(hidden.ineligible).toBe(18);
    expect(hidden.rows).toHaveLength(102);
    expect(
      playerDisplay(population, [], {
        mode: "Per 60 Possessions",
        showIneligible: true,
      }).rows,
    ).toHaveLength(120);
    expect(
      playerDisplay(population.slice(1), [], { mode: "Per 60 Possessions" })
        .threshold,
    ).toBe(0);
  });
});
