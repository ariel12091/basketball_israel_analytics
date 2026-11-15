import { describe, expect, it, vi } from "vitest";
import { compareDefaults, resolveCompareQuery } from "@/lib/compareQuery";
import type { Query } from "@/lib/tabQuery";
import { finalizeCompare, joinCompare, joinTeamPlayers, reverseGap } from "./compare";
import { compareLeagueRows, comparePlayerRows } from "./compareTable";

vi.mock("server-only", () => ({}));

const rowsA = [
  { team_id: 2, team_name: "A", net_rtg: 8, off_poss: 300 },
  { team_id: 3, team_name: "B", net_rtg: 1, off_poss: 300 },
];
const rowsB = [
  { team_id: 2, team_name: "A", net_rtg: -2, off_poss: 280 },
  { team_id: 3, team_name: "B", net_rtg: 4, off_poss: 5 },
];
const teams = [
  { value: "2", label: "A" },
  { value: "3", label: "B" },
];
const q = (o: Record<string, string>) =>
  resolveCompareQuery({ ...compareDefaults, season: "2026", ...o } as Query);

describe("compareLeagueRows: the league table the page and the API show", () => {
  it("is joinCompare + finalizeCompare on the query's fields", () => {
    const query = q({ preset: "top_bottom_rank" });
    expect(compareLeagueRows(query, rowsA, rowsB, teams)).toEqual(
      finalizeCompare(joinCompare(rowsA, rowsB, "Teams", "net_rtg", teams), "Teams", query.preset, 10, []),
    );
    // Team 3's side B has 5 possessions, under the default minimum of 10.
    expect(compareLeagueRows(query, rowsA, rowsB, teams).map((r) => r.team_id)).toEqual([2]);
  });
  it("keeps each side's team selection", () => {
    const got = compareLeagueRows(q({ a_teams: "3", b_teams: "3", min_poss: "0" }), rowsA, rowsB, teams);
    expect(got.map((r) => r.team_id)).toEqual([3]);
  });
  it("applies the stat filter", () => {
    const stats = '[{"col":"gap","op":"ge","value":5}]';
    const got = compareLeagueRows(q({ min_poss: "0", stats }), rowsA, rowsB, teams);
    expect(got.every((r) => Number(r.gap) >= 5)).toBe(true);
  });
});

describe("comparePlayerRows: one team's players under both conditions", () => {
  it("is joinTeamPlayers on the query's metric, rate, minimum and preset", () => {
    const a = [{ team_id: 2, player_id: 1, player_name: "P", gp: 3, pts: 30, minutes: 60, poss_on_floor: 100 }];
    const b = [{ team_id: 2, player_id: 1, player_name: "P", gp: 2, pts: 10, minutes: 40, poss_on_floor: 80 }];
    const query = q({ preset: "home_away", team_metric: "pts" });
    expect(comparePlayerRows(query, a, b)).toEqual(
      joinTeamPlayers(a, b, "pts", query.team_rate, 10, reverseGap("home_away")),
    );
  });
});
