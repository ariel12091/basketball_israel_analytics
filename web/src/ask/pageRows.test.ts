import { describe, expect, it } from "vitest";
import ui from "@/data/shinyUi.json";
import { lineupAutoMin, lineupTableRows, localLineups } from "@/rules/lineups";
import { autoMinOn, possCols } from "@/rules/onoff";
import { pageRows, lowerOnly, withTeamNetRank } from "./pageRows";

const q = (s: string) => new URLSearchParams(s);

describe("pageRows: the rows a freshly opened link shows", () => {
  const lineups = [
    { team_id: 2, total_poss: 120, net_rtg: 5, is_total: 1 },
    { team_id: 2, total_poss: 8, net_rtg: 40, is_total: 1 },
    { team_id: 3, total_poss: 300, net_rtg: 9, is_total: 1 },
  ];

  it("lineups: team filter, then the page's auto minimum, then table rows", () => {
    const got = pageRows("lineups", "il", { rows: lineups, meta: {} }, q("team=2"));
    const sel = { on: [], any: [], off: [], count: 1, exact: false };
    const local = localLineups(lineups, "2", sel);
    expect(got).toEqual(
      lineupTableRows(local, "summary", lineupAutoMin(local) ?? ui.lineupDefaults.minPoss),
    );
    expect(got.filter((r) => r.is_total !== 0).every((r) => r.team_id === 2)).toBe(true);
  });

  it("lineups: a min_poss in the link is overridden by auto, as on the page", () => {
    const many = Array.from({ length: 200 }, (_, i) => ({
      team_id: 2,
      total_poss: i * 5,
      net_rtg: i,
      is_total: 1,
    }));
    const got = pageRows("lineups", "il", { rows: many, meta: {} }, q("min_poss=0"));
    const min = lineupAutoMin(many)!;
    expect(min).toBeGreaterThan(0);
    expect(got.every((r) => Number(r.total_poss) >= min)).toBe(true);
  });

  it("onoff auto minimum only lowers the bar on first load", () => {
    expect(lowerOnly(300, 120)).toBe(120);
    expect(lowerOnly(100, 250)).toBe(100);
    expect(lowerOnly(300, null)).toBe(300);
  });

  it("onoff: lowered ON bar comes from autoMinOn over the team-filtered base", () => {
    const rows = Array.from({ length: 20 }, (_, i) => ({
      team_id: i % 2 ? 2 : 3,
      "ON Poss": 50 + i * 10,
      "OFF Poss": 400,
    }));
    const cols = possCols("summary");
    const base = rows.filter((r) => r.team_id === 2);
    const needed = autoMinOn(base, cols.on);
    expect(needed).not.toBeNull();
    // pageRows passes min_on = lowerOnly(300, needed) to onoffTableRows; with
    // no qualifying ranks the rows carry through, so check the bar it applied.
    const got = pageRows(
      "onoff",
      "il",
      { rows, league3p: null, meta: { view: "summary", filtered: false } },
      q("teams=2"),
    );
    expect(got.every((r) => Number(r[cols.on]) >= lowerOnly(300, needed))).toBe(true);
  });

  it("onoff: Player Stats chips filter by the player's own stats, as the page does", () => {
    const rows = [1, 2, 3].map((i) => ({
      team_id: 2,
      player_id: i,
      "ON Poss": 900,
      "OFF Poss": 900,
    }));
    const psRows = [
      { team_id: 2, player_id: 1, ps_min_pg: 30 },
      { team_id: 2, player_id: 2, ps_min_pg: 12 },
    ];
    const stats = encodeURIComponent(JSON.stringify([{ col: "ps_min_pg", op: "ge", value: 20 }]));
    const got = pageRows(
      "onoff",
      "il",
      { rows, psRows, league3p: null, meta: { view: "summary", filtered: false } },
      q(`teams=2&min_all=0&min_on=0&stats=${stats}`),
    );
    // Player 3 has no Player Stats row: NA, which an active chip excludes.
    expect(got.map((r) => r.player_id)).toEqual([1]);
  });

  it("player stats: Per Game by default, Totals when asked", () => {
    const rows = [
      { team_id: 2, player_id: 1, Player: "A", gp: 4, pts: 80, poss_on_floor: 400, minutes: 120 },
    ];
    const pg = pageRows("player_stats", "il", { rows, identities: [], meta: {} }, q(""));
    const tot = pageRows("player_stats", "il", { rows, identities: [], meta: {} }, q("view=Totals"));
    expect(pg[0].pts).toBe(20);
    expect(tot[0].pts).toBe(80);
  });

  it("other tools pass their rows through", () => {
    expect(pageRows("game_logs", "il", { rows: [{ a: 1 }] }, q(""))).toEqual([{ a: 1 }]);
  });
});

describe("withTeamNetRank: the chat's on-team net impact rank", () => {
  it("ranks within each team, ties share the better rank, blanks are unranked", () => {
    const got = withTeamNetRank(
      [
        { team_id: 2, "Net RTG Diff": 3 },
        { team_id: 2, "Net RTG Diff": 9 },
        { team_id: 3, "Net RTG Diff": -1 },
        { team_id: 2, "Net RTG Diff": 3 },
        { team_id: 2, "Net RTG Diff": null },
      ],
      "Net RTG Diff",
    );
    expect(got.map((r) => [r.team_rank_net, r.team_ranked])).toEqual([
      [2, 3],
      [1, 3],
      [1, 1],
      [2, 3],
      [undefined, undefined],
    ]);
  });
});
