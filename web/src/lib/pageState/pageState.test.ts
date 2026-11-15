// The tables' page state, read once for the page, the API (shape=table) and
// the chat: each options function maps a page query to its rule's options.
import { describe, expect, it } from "vitest";
import { playerDisplay } from "@/rules/playerStatsTable";
import { teamTableRows } from "@/rules/teamRatings";
import { playerStatsPageOptions, playerStatsTable } from "./playerStats";
import { teamRatingsPageOptions, teamRatingsTable } from "./teamRatings";

describe("the finished tables (shape=table)", () => {
  const meta = { version: "v1", league: "il" };
  it("Player Stats: the page's rows, the rest of the answer kept, shape marked", () => {
    const answer = { rows, identities: [], players: [{ team_id: 2 }], meta };
    const query = { view: "Totals", min_gp: "4" };
    const got = playerStatsTable(answer, query);
    expect(got.rows).toEqual(playerDisplay(rows, [], playerStatsPageOptions(query, rows)).rows);
    expect(got.rows.length).toBeLessThan(rows.length);
    expect(got.players).toEqual(answer.players);
    expect(got.meta).toEqual({ ...meta, shape: "table" });
  });
  it("Team Ratings: the page's rows; empty stays empty", () => {
    const teams = [
      { team_id: 2, team_name: "A", net_rtg: 5 },
      { team_id: 3, team_name: "B", net_rtg: -5 },
    ];
    const query = { stats: '[{"col":"net_rtg","op":"ge","value":0}]' };
    const got = teamRatingsTable({ rows: teams, previous: [], meta: { ...meta, showDelta: false } }, query);
    const o = teamRatingsPageOptions(query);
    expect(got.rows).toEqual(teamTableRows(teams, [], o.view, o.filters, false, o.defense));
    expect(got.rows.map((r) => r.team_id)).toEqual([2]);
    expect(got.meta.shape).toBe("table");
    expect(teamRatingsTable({ rows: [], previous: [], meta: { ...meta, showDelta: false } }, {}).rows).toEqual([]);
  });
});
import { pageRows } from "@/ask/pageRows";

const rows = [
  { team_id: 2, player_id: 10, player_name: "A", gp: 8, pts: 80, minutes: 200, poss_on_floor: 400 },
  { team_id: 2, player_id: 11, player_name: "B", gp: 3, pts: 12, minutes: 40, poss_on_floor: 80 },
  { team_id: 3, player_id: 12, player_name: "C", gp: 6, pts: 66, minutes: 150, poss_on_floor: 300 },
];

describe("playerStatsPageOptions", () => {
  it("reads the rate view, falling back to Per Game", () => {
    expect(playerStatsPageOptions({ view: "Totals" }, rows).mode).toBe("Totals");
    expect(playerStatsPageOptions({ view: "Per Week" }, rows).mode).toBe("Per Game");
    expect(playerStatsPageOptions({}, rows).mode).toBe("Per Game");
  });
  it("caps min_gp at the most games anyone played, at least 1", () => {
    expect(playerStatsPageOptions({ min_gp: "99" }, rows).minGp).toBe(8);
    expect(playerStatsPageOptions({ min_gp: "3" }, rows).minGp).toBe(3);
    expect(playerStatsPageOptions({}, rows).minGp).toBe(1);
    // An empty season (Review Focus 5).
    expect(playerStatsPageOptions({ min_gp: "5" }, []).minGp).toBe(1);
  });
  it("reads players, show_ineligible and the stat filters", () => {
    const o = playerStatsPageOptions(
      { players: "2:10,3:12", show_ineligible: "true", stats: '[{"col":"pts","op":"ge","value":10}]' },
      rows,
    );
    expect(o.players).toEqual(["2:10", "3:12"]);
    expect(o.showIneligible).toBe(true);
    expect(o.filters).toEqual([{ col: "pts", op: "ge", value: 10 }]);
  });
  it("gives the chat's table today (parity pin until pageRows goes)", () => {
    const query = { view: "Totals", min_gp: "4", stats: '[{"col":"pts","op":"ge","value":20}]' };
    expect(playerDisplay(rows, [], playerStatsPageOptions(query, rows)).rows).toEqual(
      pageRows("player_stats", "il", { rows }, new URLSearchParams(query)),
    );
  });
});

describe("teamRatingsPageOptions", () => {
  it("reads the view, side and stat filters", () => {
    const o = teamRatingsPageOptions({
      view: "four_factors",
      side: "defense",
      stats: '[{"col":"net_rtg","op":"ge","value":0}]',
    });
    expect(o).toEqual({
      view: "four_factors",
      defense: true,
      filters: [{ col: "net_rtg", op: "ge", value: 0 }],
    });
  });
  it("falls back to the summary view and offense", () => {
    expect(teamRatingsPageOptions({ view: "nonsense" })).toEqual({ view: "summary", defense: false, filters: [] });
  });
  it("an empty population is an empty table", () => {
    const o = teamRatingsPageOptions({});
    expect(teamTableRows([], [], o.view, o.filters, false, o.defense)).toEqual([]);
  });
});
